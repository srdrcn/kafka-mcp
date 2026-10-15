# kafka-mcp

An [MCP](https://modelcontextprotocol.io) server for Apache Kafka fleets. It follows the operating model of
[openshift-mcp-server](https://github.com/openshift/openshift-mcp-server): a single executable jar, one YAML config
file, tools grouped into toolsets, a read-only switch, and Streamable HTTP or stdio as transport. It talks to Kafka
directly with the Java `AdminClient` and `KafkaConsumer` (no CLI wrapping), and to Kafka Connect and Schema Registry
through their REST APIs.

Built with Java 21, Spring Boot 3.5, the official MCP Java SDK 2.0.1 and kafka-clients 4.1.1.

**Highlights**

- **108 tools in 8 toolsets**: `core`, `messages`, `consumers`, `acl`, `connect`, `schemaregistry`, `report`,
  `diagnose`.
- **Mixed fleets**: ZooKeeper and KRaft clusters work side by side with the same tools. The mode is detected per
  cluster, so there is nothing to configure. Tested against KRaft Kafka 4.1.1 and ZooKeeper-mode Kafka 3.9
  (Confluent Platform 7.9).
- **Kerberos**: one `kerberos` block (keytab + principal) becomes `SASL_PLAINTEXT` + `GSSAPI` with a generated
  `sasl.jaas.config` for every cluster. Per-cluster `properties` override the client settings (only a few
  read-safety settings of the message consumers always win).
- **Read-only in the example config**: `readOnly: true` hides every write tool (one sandbox cluster opts back in),
  and `disableDestructive: true` hides the delete/reset tools.
- **No JMX needed**: rates and trends come from two offset samples taken a few seconds apart within the same call.
- **Message reading without consumer groups**: it uses `assign()` only, with no `group.id` and no offset commits,
  and returns at most 500 records per call.
- **Bounded calls**: every tool call has a 15 s limit, and message reads have memory and response size limits.
  Every `*_list` tool takes `filter` (regex) and `limit`.
- **Simple to operate**: a single static API key, plain JSON logs, a non-root UBI9 image and kustomize manifests
  for OpenShift (`restricted-v2` SCC).

## Contents

- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Toolsets](#toolsets)
- [Tool reference](#tool-reference)
- [Security](#security)
- [Logging](#logging)
- [Container image](#container-image)
- [OpenShift deployment](#openshift-deployment)
- [Claude Code](#claude-code)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## Quick start

Run it either with Java (JDK 21) or as a container with Docker/Podman (no JDK needed on the host). Docker or Podman
is also needed for the tests.

### With Java

```bash
make build                                       # ./mvnw -B package -DskipTests -> target/kafka-mcp.jar
cp kafka-mcp.example.yaml kafka-mcp.local.yaml   # git-ignored; fill in the <...> values and local paths
java -jar target/kafka-mcp.jar --config kafka-mcp.local.yaml
```

The server refuses to start while `apiKey` still holds the `<api-key>` placeholder: set a real key, or an empty
value (`apiKey: ""`) to turn authentication off for a local run. The example config sets `port: 8080`, so the server
listens on `http://localhost:8080/mcp`. To run over stdio instead (for a local MCP client that starts the process
itself):

```bash
java -jar target/kafka-mcp.jar --config kafka-mcp.local.yaml --transport stdio
# or: make run-stdio CONFIG=kafka-mcp.local.yaml
```

### With Docker

The image is built in a multi-stage build, so the host needs no JDK or Maven. The default base images come from
`registry.access.redhat.com`; to use a mirror, pass `BUILD_ARGS` (see [Container image](#container-image)).

```bash
make image                                       # or: docker build -t kafka-mcp:latest .
cp kafka-mcp.example.yaml kafka-mcp.local.yaml   # git-ignored; fill in the <...> values
```

Keep the container paths from the example config (`krb5Conf: /etc/kafka-mcp/krb5/krb5.conf`,
`keytab: /etc/kafka-mcp/keytabs/svc.keytab`) and mount the files there:

| File | Path in the container |
|---|---|
| config | `/etc/kafka-mcp/config/kafka-mcp.yaml` (the image's default `--config`) |
| `krb5.conf` | `/etc/kafka-mcp/krb5/krb5.conf` (the image's `JAVA_TOOL_OPTIONS` points the JVM there) |
| keytab | `/etc/kafka-mcp/keytabs/svc.keytab` |

Streamable HTTP on `http://localhost:8080/mcp`:

```bash
docker run --rm -p 8080:8080 \
  -v "$PWD/kafka-mcp.local.yaml:/etc/kafka-mcp/config/kafka-mcp.yaml:ro" \
  -v "$PWD/krb5.conf:/etc/kafka-mcp/krb5/krb5.conf:ro" \
  -v "$PWD/svc.keytab:/etc/kafka-mcp/keytabs/svc.keytab:ro" \
  kafka-mcp:latest

curl -s localhost:8080/actuator/health/readiness   # {"status":"UP"}
```

stdio (`-i` keeps stdin open; arguments after the image name replace the default command, so pass `--config` too):

```bash
docker run --rm -i \
  -v "$PWD/kafka-mcp.local.yaml:/etc/kafka-mcp/config/kafka-mcp.yaml:ro" \
  -v "$PWD/krb5.conf:/etc/kafka-mcp/krb5/krb5.conf:ro" \
  -v "$PWD/svc.keytab:/etc/kafka-mcp/keytabs/svc.keytab:ro" \
  kafka-mcp:latest --config /etc/kafka-mcp/config/kafka-mcp.yaml --transport stdio
```

Notes:

- The process runs as UID 185 (group 0) and must be able to read the mounted files. A keytab with mode `600` owned
  by your user is not readable by UID 185; run the container as your UID with `--user "$(id -u):0"` so the keytab
  can stay private. `kerberos_self_check` shows whether the keytab is readable.
- Without Kerberos (no `kerberos` block, e.g. a local test cluster), mount only the config.
- Inside the container `localhost` is the container itself. To reach a broker on the host, use
  `--network host` (Linux; then `-p` is not needed) or the host's address, and make sure the brokers'
  advertised listeners are reachable from the container.
- With Podman, use the same commands with `podman`; on SELinux hosts add `:Z` to the volume options
  (e.g. `:ro,Z`).
- To mimic OpenShift's read-only root filesystem, add `--read-only --tmpfs /tmp`.

### Command line

| Option | Meaning |
|---|---|
| `--config <file>` | Main config file (YAML). |
| `--config-dir <dir>` | Directory of drop-in `.yaml`/`.yml` files merged over the main config (see [Drop-in files](#drop-in-files---config-dir)). |
| `--port <port>` | HTTP port (1–65535). Overrides `port` from the config. |
| `--transport <http\|stdio>` | Transport. Defaults to `http` when a port is set (> 0) and to `stdio` otherwise. `--transport http` without a port uses 8080. |
| `--help` | Print usage. |

At least one of `--config` or `--config-dir` is required. Both `--option value` and `--option=value` work. An
invalid config or command line is reported on stderr as `kafka-mcp: ...` and the process exits with code 2. An
unknown toolset name is found only while the server starts, so in that case the JSON log also shows
`Application run failed` before the `kafka-mcp: Unknown toolset: [...]` line. Any other argument goes to Spring Boot,
for example `--logging.level.org.apache.kafka=INFO`.

### Transports

- **Streamable HTTP** on `/mcp` (stateless). The server handles each request on its own and keeps no MCP session,
  so a pod restart does not break connected clients. Only `POST` is served: `GET /mcp` returns 405 because there is
  no server-initiated stream. Health endpoints are `/actuator/health/liveness` and `/actuator/health/readiness`.
- **stdio**: JSON-RPC on stdin/stdout, with logs on stderr. The process exits when stdin closes. There is no HTTP
  listener and no API key check.

### Make targets

| Target | What it does |
|---|---|
| `make build` | `./mvnw -B package -DskipTests` produces `target/kafka-mcp.jar` |
| `make test` | `./mvnw -B verify`: all tests (Testcontainers needs Docker or Podman) |
| `make image` | Builds the container image `$(IMAGE)` (default `kafka-mcp:latest`) with podman or docker (`CONTAINER_TOOL`); `BUILD_ARGS` is passed to the build |
| `make run` / `make run-stdio` | Builds, then runs with `$(CONFIG)` (default `kafka-mcp.yaml`, which is git-ignored) over the configured transport or stdio |
| `make snapshot` | Rewrites the tool schema snapshots (see [Development](#development)) |
| `make clean` | `./mvnw clean` |

## Configuration

This is the content of `kafka-mcp.example.yaml` without its header comment
(`deploy/openshift/kafka-mcp.yaml.example` is identical). Never commit real values. Keep them in
`kafka-mcp.local.yaml` or `kafka-mcp.yaml` (both git-ignored) or in the OpenShift ConfigMap.

```yaml
port: 8080
apiKey: <api-key>                # X-Authorization value; use a long random key (openssl rand -hex 32); "" = no auth
readOnly: true
disableDestructive: true
toolsets: [core, messages, consumers, acl, connect, report, diagnose]

kerberos:
  krb5Conf: /etc/kafka-mcp/krb5/krb5.conf
  keytab: /etc/kafka-mcp/keytabs/svc.keytab
  principal: <principal>@<REALM>
  serviceName: kafka

clusters:
  - name: kafka-dev
    bootstrapServers: kafka-dev.example.com:9092
    connectUrl: http://kafka-dev.example.com:8083
  - name: kafka-test
    bootstrapServers: kafka-test.example.com:9092
  - name: kafka-uat
    bootstrapServers: kafka-uat.example.com:9092
    connectUrl: http://kafka-uat.example.com:8083
  - name: kafka-sandbox
    bootstrapServers: kafka-sandbox.example.com:9092
    connectUrl: http://kafka-sandbox.example.com:8083
    readOnly: false
    # schemaRegistryUrl: http://...:8081
    # properties:                   # Kafka client property overrides
    #   client.dns.lookup: use_all_dns_ips
```

### Top-level keys

| Key | Type | Default | Meaning |
|---|---|---|---|
| `port` | integer (0–65535) | not set | HTTP port. When it is set (> 0) and `--transport` is not given, the server runs Streamable HTTP, otherwise stdio. `--port` overrides it. |
| `apiKey` | string | not set | Static API key. HTTP requests must send it in the `X-Authorization` header. Not set or empty (`""`) turns the check off. A non-empty key must not start or end with whitespace and must not be a `<...>` or `${...}` placeholder, otherwise the server does not start; a key shorter than 16 characters only logs a warning. See [Security](#security). |
| `readOnly` | boolean | `false` | `true` hides every write tool on clusters that do not override it with their own `readOnly`. |
| `disableDestructive` | boolean | `false` | `true` hides the destructive tools (names ending in `_delete` or `_reset`, plus `schemas_delete_version`) on every cluster. It is global only and does not affect the other write tools. |
| `toolsets` | list of strings | `[core]` | Toolsets to enable, which also sets the order of `tools/list`. An unknown or duplicate name stops the server at startup. |
| `kerberos` | object | not set | Kerberos settings applied to every cluster (see below). Without this block, the clients use the Kafka defaults (`PLAINTEXT`) unless a cluster's `properties` say otherwise. |
| `clusters` | list | (required) | Kafka clusters. At least one is required. |

The defaults in the code are permissive (`readOnly: false`, `disableDestructive: false`). The example config turns
both on.

YAML numbers keep the text they are written with wherever a string is expected, so `apiKey: 0123456701234567` or
`ssl.truststore.password: 012345` are read exactly as written (plain YAML 1.1 would turn them into octal numbers).
Quoting such values is still a good habit.

### Cluster keys

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | (required) | Unique name of 1–63 characters from `A-Z a-z 0-9 _ . -`, starting with a letter or digit. It becomes a value of the `cluster` enum of every tool that is available on this cluster (see [Which tools are listed](#which-tools-are-listed)). |
| `bootstrapServers` | string | (required) | Kafka bootstrap list (`host:port,host:port`). With Kerberos, use the broker FQDNs. |
| `connectUrl` | URL | not set | Kafka Connect REST base URL (`http://` or `https://`). It enables the `connect` tools, plus `topics_connectors` and `diagnose_connector`, for this cluster. |
| `schemaRegistryUrl` | URL | not set | Schema Registry base URL (`http://` or `https://`). It enables the `schemaregistry` tools and `avro` decoding in the `messages` tools for this cluster. |
| `readOnly` | boolean | global `readOnly` | Per-cluster override of `readOnly`, in either direction. |
| `properties` | map (string → string) | `{}` | Kafka client properties for the AdminClient and the message consumers, applied after the defaults and the settings derived from `kerberos` (they override both). Only the consumer safety settings listed under [Kafka client properties](#kafka-client-properties) take precedence over them. |

Connect and Schema Registry are called without authentication, so their URLs must not contain credentials
(`user:password@`): such a URL is rejected at startup, because the credentials would not be used and would show up
in tool output.

### Kerberos keys

| Key | Type | Default | Meaning |
|---|---|---|---|
| `krb5Conf` | path | not set | Path to `krb5.conf`. It sets the `java.security.krb5.conf` system property only if that property is not already set. In the container image, `JAVA_TOOL_OPTIONS` already sets it to `/etc/kafka-mcp/krb5/krb5.conf`, and that value wins (`kerberos_self_check` notes a mismatch). |
| `keytab` | path | (required) | Keytab of the service principal. |
| `principal` | string | (required) | Kerberos principal, for example `<principal>@<REALM>`. |
| `serviceName` | string | `kafka` | Kerberos service name of the brokers (`sasl.kerberos.service.name`). |

### Kafka client properties

The client properties of each cluster are built in this order (later entries win):

```properties
# 1. defaults
bootstrap.servers=<bootstrapServers>
client.id=kafka-mcp-<name>
request.timeout.ms=15000
default.api.timeout.ms=15000
socket.connection.setup.timeout.ms=5000
socket.connection.setup.timeout.max.ms=10000
reconnect.backoff.max.ms=5000
metadata.max.age.ms=60000
# 2. only when the kerberos block is present
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=<serviceName>
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true useTicketCache=false keyTab="<keytab>" principal="<principal>";
# 3. the cluster's properties: map
```

- **AdminClient**: there is one per cluster. It is created on first use and then reused. Its own defaults are
  `client.id=kafka-mcp-<name>-admin` and `retries=3`, and the cluster's `properties` can override both. Each cluster
  has its own client, so a Kerberos login or connection failure on one cluster does not affect the others. Clients
  are created on a separate platform thread and a call waits at most 15 s for that (the Kerberos login runs during
  creation), so a slow or unreachable KDC for one cluster cannot stall the calls for other clusters. If creating a
  client fails, the next call tries again.
- **Message consumers**: each call creates a new consumer and closes it at the end. Their defaults, which the
  cluster's `properties` can override, are `max.poll.records=500` and `fetch.max.bytes=8388608` (8 MiB instead of
  Kafka's 50 MiB). The following settings always win, because the reads must stay read-only and the offset windows
  depend on them: no `group.id` and no `group.instance.id` (removed even when `properties` set them),
  `enable.auto.commit=false`, `allow.auto.create.topics=false` (a read never recreates a deleted topic),
  `auto.offset.reset=none`, `isolation.level=read_uncommitted`, and byte-array deserializers. The `client.id` gets a
  unique suffix: `<client.id>-reader-<n>`, so `kafka-mcp-<name>-reader-<n>` by default.
- **`diagnose_connectivity`** is the one deliberate exception to "one AdminClient per cluster": to test a fresh
  connection it creates a short-lived extra AdminClient on each call (the cluster's client properties with shorter
  timeouts) and closes it afterwards. The cached client is not touched.

An example of per-cluster overrides:

```yaml
clusters:
  - name: kafka-test
    bootstrapServers: kafka-test.example.com:9092
    properties:
      client.dns.lookup: use_all_dns_ips
      # a different principal for this cluster only
      sasl.jaas.config: 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true useTicketCache=false keyTab="/etc/kafka-mcp/keytabs/other.keytab" principal="<principal>@<REALM>";'
```

### Drop-in files (`--config-dir`)

- The server reads the regular `*.yaml` and `*.yml` files directly inside the directory. Hidden files are skipped
  and subdirectories are not read. The files are sorted by name and merged over `--config` one after another.
  `--config-dir` can also be used on its own.
- Maps are merged deeply. `clusters` is merged **by `name`**: an entry with an existing name overrides only the
  fields it sets (its `properties` are merged key by key), and an entry with a new name is appended. Every other
  list, such as `toolsets`, replaces the earlier value entirely. Scalars replace the earlier value.
- Validation runs after the merge. Unknown keys are errors, and the error message names the field.

```yaml
# conf.d/10-sandbox.yaml: add a Schema Registry to an existing cluster
clusters:
  - name: kafka-sandbox
    schemaRegistryUrl: http://kafka-sandbox.example.com:8081
```

```yaml
# conf.d/20-toolsets.yaml: replaces the whole toolsets list
toolsets: [core, messages, consumers, acl, connect, schemaregistry, report, diagnose]
```

The config is read once at startup, so restart the server to apply changes.

## Toolsets

| Toolset | Tools | What it covers | Available on |
|---|---:|---|---|
| `core` (enabled by default) | 28 | Clusters, brokers (configs, config drift, log dirs, leader/replica skew, preferred leader election), partition reassignments, and topics (details, configs, find by config, size, offsets, last write time, write rate, consumers, connectors, producers, transactions, create/clone/config update/partition increase/delete) | Every cluster. `topics_connectors` needs `connectUrl`. |
| `messages` | 8 | Reading messages without a consumer group, header summaries, offset for a timestamp, message counts, the partition for a key, and value structure inference. Formats: `string`, `json`, `hex`, `long`, `avro` | Every cluster. `avro` needs `schemaRegistryUrl`. |
| `consumers` | 12 | Consumer groups: details, lag, groups with lag, lag trend, stuck groups, Empty groups, group ACLs, member search, offset reset/delete, group delete | Every cluster |
| `acl` | 13 | ACLs (list, explain, who can access a topic, principal permissions, audit, create/delete), client quotas, SCRAM users, delegation tokens, `security_summary`, `kerberos_self_check` | Every cluster |
| `connect` | 19 | Kafka Connect: worker reachability, plugins and config validation, connectors (status, failures, topics, offsets, task distribution, config diff), and pause/resume/stop/restart/create/delete/offset reset | Clusters with `connectUrl` |
| `schemaregistry` | 10 | Schema Registry: subjects, versions, schemas, diffs, compatibility get/check/set, register, delete version | Clusters with `schemaRegistryUrl` |
| `report` | 12 | Cluster-wide reports and audits (all read-only): summary, top topics, empty/idle/unconsumed topics, replication and retention audits, partition outliers, naming rules, duplicate-like names, team inventory, cleanup candidates | Every cluster |
| `diagnose` | 6 | One-call diagnoses with findings for a topic, a consumer group, a connector, the cluster, a principal's access (`diagnose_access` suggests the missing ACLs as bindings to pass to `acls_create`) and connectivity | Every cluster. `diagnose_connector` needs `connectUrl`. |

### Which tools are listed

For every tool, the server works out which clusters it may run on:

1. Its toolset must be in `toolsets`.
2. `connect` tools need a cluster with `connectUrl`, and `schemaregistry` tools need a cluster with
   `schemaRegistryUrl`.
3. **Write** tools (descriptions starting with `WRITE:`) need the cluster's effective `readOnly` to be `false`. The
   cluster's own `readOnly` applies if it is set, otherwise the global one does.
4. **Destructive** tools also need `disableDestructive: false`.

A tool with no eligible cluster is **not listed at all**. Every other tool is listed with its `cluster` enum
**narrowed to the eligible clusters**, and a call that names any other cluster is rejected by schema validation.
With the example config, 91 of the 108 tools are listed. For example:

| Tool | `cluster` enum |
|---|---|
| `topics_list` (read) | `kafka-dev`, `kafka-test`, `kafka-uat`, `kafka-sandbox` |
| `connectors_list` (read, needs Connect) | `kafka-dev`, `kafka-uat`, `kafka-sandbox` |
| `topics_create`, `connectors_pause` (write) | `kafka-sandbox` |
| `topics_delete`, `connectors_delete` (destructive) | not listed |

Tools also carry MCP annotations: `readOnlyHint: true` on read tools and `destructiveHint: true` on destructive
tools.

### Common parameters and limits

- **`cluster`**: always present, as an enum of the eligible cluster names. It is required everywhere except `clusters_list`,
  `connect_clusters_list` and `serdes_list`.
- **`filter` / `limit`**: every `*_list` tool takes both, and most other listing tools do too (`topics_producers`
  and `report_team_inventory` take only `limit`). `filter` is a Java regex with partial matching (use `(?i)` for
  case-insensitive matching). `limit` is 1–5000 and usually defaults to 100 (`brokers_logdirs`, `acls_audit` and
  some reports default to 20 or 50; `schemas_versions_list` keeps the newest versions). List results usually
  include `total`, `returned` and `truncated`.
- **Regex parameters** (`filter`, `keyRegex`, `valueRegex`, `headerValueRegex`, `host`, `clientId`, ...) are
  stopped when the call is cancelled or reaches its time limit, so a pathological pattern cannot keep the server
  busy. `keyRegex`/`valueRegex`/`headerValueRegex` search only the first 262144 characters of each decoded
  key/value/header; a scan that runs out of time returns what it found so far as `partial`.
- **`maxRecords`** on message-reading tools: defaults to 50, maximum 500. `messages_consume` starts from `latest`
  by default (the last `maxRecords` records, without waiting for new data), or from `earliest`, an `offset` or a
  `timestamp`. It scans at most `maxScan` records (default 10000) with filters or from `latest`, and truncates values at
  `maxValueChars` (default 4096). `avro` decoding expects the Confluent wire format and fetches schemas from the
  cluster's Schema Registry.
- **Size limits on message reads**: the raw records a call holds are limited to 32 MiB. From `latest` the newest
  records within that budget are kept; the other reads stop once it is exceeded. Either way the result is marked
  `partial` with a note. The records of a `messages_consume` response are also limited to 2,000,000 characters of
  JSON: the rest is left out (from `latest`, the oldest), the result has `truncated: true` and a note, and
  `nextOffset` (single partition) points at the first record that was left out.
- **`intervalSeconds`** on rate and trend tools (`topics_write_rate`, `consumer_groups_lag_trend`,
  `consumer_groups_stuck`, `report_top_topics` with `by=write_rate`, and `diagnose_topic`,
  `diagnose_consumer_group`, `diagnose_connector`): the time between the two offset samples, 1–10 s (default 5,
  or 3 for `diagnose_topic`).
- **15 s per call**: Kafka client timeouts are set to match. A call that runs longer returns the error
  `Tool did not finish within 15 s (timeout). Try a narrower filter/limit.`
- When a broker is too old for an API, many tools report `Broker does not support this operation (version)` for
  the affected item instead of failing the whole call.

## Tool reference

The tables match the tool schema snapshots in `src/test/resources/snapshots/*.json`, which also hold the full
input schemas. **Kind**: *read* tools are always listed, *write* tools (the description starts with `WRITE:`, which is
left out below) are hidden by `readOnly`, and *destructive* tools are hidden by `readOnly` or `disableDestructive`.

| Toolset | Tools | Read | Write | Destructive |
|---|---:|---:|---:|---:|
| [core](#core-28-tools) | 28 | 20 | 7 | 1 |
| [messages](#messages-8-tools) | 8 | 8 | 0 | 0 |
| [consumers](#consumers-12-tools) | 12 | 9 | 0 | 3 |
| [acl](#acl-13-tools) | 13 | 10 | 2 | 1 |
| [connect](#connect-19-tools) | 19 | 11 | 6 | 2 |
| [schemaregistry](#schemaregistry-10-tools) | 10 | 7 | 2 | 1 |
| [report](#report-12-tools) | 12 | 12 | 0 | 0 |
| [diagnose](#diagnose-6-tools) | 6 | 6 | 0 | 0 |
| **Total** | **108** | **83** | **17** | **8** |

### core (28 tools)

| Tool | Kind | Description |
|---|---|---|
| `clusters_list` | read | List configured clusters with reachability and mode (KRaft/ZooKeeper) |
| `clusters_get` | read | Cluster summary: id, mode, controller/quorum, min Kafka version, features or ZooKeeper connection, authorizer, broker/topic/partition counts, URP/offline partitions |
| `brokers_list` | read | List brokers: host, port, rack, controller flag, leader and replica counts |
| `brokers_config_get` | read | Configs of a broker (by default only non-default values) |
| `brokers_config_drift` | read | Configs whose values differ between brokers (skips broker-specific keys, and mode-specific keys on mixed ZooKeeper/KRaft clusters) |
| `brokers_logdirs` | read | Broker log dirs: path, disk and used size, error, partition count and the largest partitions |
| `brokers_partition_skew` | read | Leader/replica counts per broker, deviation from the average (%) and non-preferred leader count |
| `brokers_config_update` | write | Change dynamic broker configs (cluster-wide default if brokerId is omitted); returns before/after values |
| `brokers_elect_preferred_leaders` | write | Run preferred leader election; without a topic, on every partition whose leader is not the preferred replica |
| `partition_reassignments_list` | read | List ongoing partition reassignments: current, adding and removing replicas |
| `partition_reassignments_alter` | write | Start or cancel partition reassignments (empty replicas cancels); returns a result per partition |
| `topics_list` | read | List topics with partition count and replication factor |
| `topics_get` | read | Topic details: partitions (leader, replicas, ISR, ELR on KRaft, URP), non-default configs, size, offsets |
| `topics_config_get` | read | Topic configs: value, source, default and read-only flags |
| `topics_find_by_config` | read | Find topics by config value (e.g. retention.ms gt 604800000, cleanup.policy eq compact) |
| `topics_size` | read | Topic size: leader replicas (logical) and all replicas; without a topic, the largest topics |
| `topics_offsets` | read | Earliest/latest offsets and approximate message count (latest - earliest) per partition, with totals |
| `topics_last_write_time` | read | Last write time of a topic (per partition and overall, as ISO-8601 and age) |
| `topics_write_rate` | read | Write rate (messages/s) from two offset samples; without a topic, the busiest topics |
| `topics_consumers` | read | Consumer groups with committed offsets on the topic: type (Classic/Consumer), state, member count, lag on this topic |
| `topics_connectors` | read | Connect connectors that use the topic (active topics, or topic/topics/regex/prefix in the config) |
| `topics_producers` | read | Active producers on the topic partitions: producerId, epoch, last sequence/timestamp, open transaction |
| `topics_transactions` | read | Open transactions touching the topic, with hints of possibly hanging transactions |
| `topics_create` | write | Create a topic (partitions and RF default to the broker defaults) |
| `topics_clone` | write | Create a topic with the partition count, RF and topic-level configs of a source topic (data is not copied) |
| `topics_config_update` | write | Change topic configs (set/delete); returns before/after values |
| `topics_partitions_increase` | write | Increase the partition count of a topic (cannot be decreased; changes the key→partition mapping of keyed data) |
| `topics_delete` | destructive | Delete a topic and all its data (internal and system topics are refused) |

### messages (8 tools)

| Tool | Kind | Description |
|---|---|---|
| `messages_consume` | read | Read messages without a consumer group or commit: from latest/earliest/offset/timestamp, key/value/header filters, format decoding |
| `messages_get` | read | Get a single message (topic/partition/offset) with its full value |
| `messages_headers` | read | Summarize headers of the latest (or earliest) messages: count per key, distinct value count, sample values |
| `messages_offset_for_timestamp` | read | Find, per partition, the offset and time of the first message at or after the given time |
| `messages_count` | read | Count messages in an offset or time range (offset difference per partition; approximate) |
| `messages_partition_for_key` | read | Compute the partition a key maps to with Kafka's default partitioner (murmur2) |
| `messages_sample_schema` | read | Infer the value structure from recent messages: JSON paths, types, null/missing ratios, example values |
| `serdes_list` | read | List supported message formats (string/json/hex/long/avro) and avro support per cluster |

### consumers (12 tools)

| Tool | Kind | Description |
|---|---|---|
| `consumer_groups_list` | read | List consumer groups (groupId, state, type, protocol) |
| `consumer_groups_get` | read | Group details: state, coordinator, assignor, members and assignments, per-partition committed/end offset, lag and owner |
| `consumer_groups_find_by_member` | read | Find groups and members whose host or clientId matches a regex (at least one is required) |
| `consumer_groups_empty` | read | Groups in Empty state with the topics they still have committed offsets on and their total lag (likely stale groups) |
| `consumer_groups_acls` | read | ACLs that apply to the group (literal, prefixed, wildcard): principal, host, operation, permission |
| `consumer_groups_lag` | read | Group lag: per-topic totals and per-partition committed, end, lag, owner clientId/host (by lag, descending) |
| `consumer_groups_with_lag` | read | Groups whose total lag is above the threshold (descending), with state and the 5 topics with the most lag |
| `consumer_groups_lag_trend` | read | Lag trend from two samples: per-topic lag, consume/produce rate (msg/s), trend and estimated catch-up time |
| `consumer_groups_stuck` | read | Stuck groups: partitions with lag whose commit did not advance between two samples while the end offset grew |
| `consumer_groups_offsets_reset` | destructive | Move the group's offsets on a topic to earliest/latest/offset/timestamp/shift; the group must be Empty (no active members) |
| `consumer_groups_offsets_delete` | destructive | Delete the group's committed offsets on a topic (all or selected partitions) |
| `consumer_groups_delete` | destructive | Delete a consumer group; the group must be empty (no active members) |

### acl (13 tools)

| Tool | Kind | Description |
|---|---|---|
| `acls_list` | read | List ACLs, filtered by principal, resource type/name, pattern, operation, permission and host |
| `acls_explain` | read | Explain whether a principal may perform an operation on a resource under the built-in Kafka authorizer rules (ALLOW/DENY and why) |
| `acls_who_can_access_topic` | read | Show who can access a topic (ALLOW), who is explicitly denied (DENY), and the super users |
| `acls_principal_permissions` | read | Group the ACLs that apply to a principal by resource and summarize the effective ALLOW/DENY operations |
| `acls_audit` | read | Find risky and orphaned ACLs: User:\*, wildcard resource, ALL, CLUSTER ALLOW, missing topic/group, unused prefix |
| `quotas_list` | read | List client quotas (user/client-id/ip, &lt;default&gt; included) and their values |
| `scram_users_list` | read | List SCRAM users with mechanism and iteration count (no secrets returned) |
| `delegation_tokens_list` | read | List delegation tokens: id, owner, renewers, timestamps, expired or not (no HMAC returned) |
| `kerberos_self_check` | read | Check this server's Kerberos setup (krb5.conf, keytab, principal); try a login and a Kafka connection |
| `security_summary` | read | Security summary: metadata mode (KRaft/ZooKeeper), listener/SASL/authorizer settings, ACL/SCRAM/quota/token counts, this server's connection |
| `acls_create` | write | Create ACLs: a single binding or a preset (producer, consumer, streams); returns the created bindings |
| `quotas_update` | write | Set or remove client quotas (user, clientId, ip; &lt;default&gt; supported); returns the before/after values |
| `acls_delete` | destructive | Delete ACLs matching the filter (principal, resourceType, resourceName required); returns the deleted bindings |

### connect (19 tools)

| Tool | Kind | Description |
|---|---|---|
| `connect_clusters_list` | read | Connect reachability, version and connector count for clusters with a Connect URL |
| `connect_plugins_list` | read | Installed Connect plugins (class, type, version) |
| `connect_plugins_validate` | read | Validate a connector config against its plugin; returns only the invalid fields |
| `connectors_list` | read | List connectors with state, type, class, worker and task counts |
| `connectors_get` | read | Connector details: type, class, masked config, tasks and status summary |
| `connectors_status` | read | Status, worker and error summary (first 3 trace lines) of a connector and its tasks |
| `connectors_failed` | read | FAILED connectors and connectors with FAILED tasks, with an error summary |
| `connectors_topics` | read | Topics actively used by a connector |
| `connectors_offsets` | read | Connector offsets (source: partition/offset, sink: topic/partition/offset) |
| `connectors_task_distribution` | read | Connector/task distribution per worker, imbalance and non-RUNNING tasks |
| `connectors_config_diff` | read | Compare a connector's config with a given config or with another connector |
| `connectors_pause` | write | Pause a connector and its tasks (PAUSED) |
| `connectors_resume` | write | Resume a paused/stopped connector (RUNNING) |
| `connectors_stop` | write | Stop a connector (STOPPED: its tasks shut down, the config is kept) |
| `connectors_restart` | write | Restart a connector (optionally with its tasks, or only the FAILED ones) |
| `connectors_task_restart` | write | Restart a single task of a connector |
| `connectors_create_or_update` | write | Create a connector or replace its whole config |
| `connectors_delete` | destructive | Delete a connector (irreversible) |
| `connectors_offsets_reset` | destructive | Reset the offsets of a STOPPED connector (irreversible) |

### schemaregistry (10 tools)

| Tool | Kind | Description |
|---|---|---|
| `subjects_list` | read | List Schema Registry subject names (sorted); deleted=true also returns soft-deleted ones |
| `subjects_for_topic` | read | Find the subjects of a topic (&lt;topic&gt;-key/-value and &lt;topic&gt;-&lt;RecordName&gt;) with the latest version, id and type of each |
| `schemas_versions_list` | read | List the version numbers of a subject; with deleted=true soft-deleted versions are included and marked |
| `schemas_get` | read | Get the schema of a subject version: id, version, schemaType, references and schema text (AVRO/JSON pretty-printed) |
| `schemas_diff` | read | Compare two schema versions: field-level diff and compatibility hint for AVRO, line diff for JSON/PROTOBUF |
| `schemas_register` | write | Register a schema under a subject; returns its id and version, or the SR message for an incompatible (409) or invalid (422) schema |
| `schemas_delete_version` | destructive | Delete a schema version (soft); permanent=true deletes it permanently (soft-deleting it first if needed) |
| `schemas_compatibility_get` | read | Get the compatibility level: subject level, global level and which one applies |
| `schemas_compatibility_check` | read | Test whether a schema is compatible with a subject version without registering it; returns the detailed SR messages |
| `schemas_compatibility_set` | write | Change the compatibility level of a subject (or the global level if omitted); returns the old and new value |

### report (12 tools)

| Tool | Kind | Description |
|---|---|---|
| `report_cluster_summary` | read | Cluster summary: mode/version, brokers, topics/partitions, size, URP/offline, group states and lag, largest topics, connectors/subjects |
| `report_top_topics` | read | Top N topics by size, partition count, consumer group count or write rate |
| `report_empty_topics` | read | Topics with no messages (earliest == latest on every partition); never-written topics are flagged |
| `report_idle_topics` | read | Topics whose last write (maxTimestamp) is older than N days, oldest first; empty topics are listed separately |
| `report_topics_without_consumers` | read | Non-internal topics without committed offsets from any consumer group, with size and last write age |
| `report_replication_audit` | read | Replication audit: RF=1, min.insync.replicas ≥ RF, URP/offline, unclean election, RF below default, single rack |
| `report_retention_audit` | read | Retention audit: infinite/very short retention, retention.bytes, compaction settings, segment outliers (value + source) |
| `report_partition_outliers` | read | Partition outliers: partition counts far above the median, size skew within topics, leader imbalance across brokers |
| `report_naming_check` | read | Topics that do not match a naming rule (full-match regex), with counts and the most common prefixes |
| `report_duplicate_like_topics` | read | Suspected duplicate topic names: groups that collide after normalization and pairs with Levenshtein distance ≤ 2 |
| `report_team_inventory` | read | Team inventory by topic prefix: topics, size, empty/idle flags, consumer groups and lag, connectors, ACL principals |
| `report_cleanup_candidates` | read | Cleanup candidates (deletes nothing): empty/idle/unconsumed topics, Empty groups, orphan LITERAL ACLs, with reasons and risk |

### diagnose (6 tools)

| Tool | Kind | Description |
|---|---|---|
| `diagnose_topic` | read | Diagnose topic health: partitions/ISR, config, size/skew, writes, consumer lag/stuck, transactions, connectors, ACLs |
| `diagnose_consumer_group` | read | Diagnose a consumer group: state/type, members and assignment, lag and trend, stuck partitions, missing topics, coordinator, ACLs |
| `diagnose_connector` | read | Diagnose a connector: connector/task state and error summaries, config/plugin/converters, topics, sink lag, source writes, workers |
| `diagnose_cluster` | read | Diagnose the cluster: mode/version, controller/quorum, brokers, URP/offline/min ISR, disks, balance, config drift, lag, connectors |
| `diagnose_access` | read | Check if a principal can produce/consume/transactional-produce/admin-topic on a topic; suggests missing ACLs for acls_create |
| `diagnose_connectivity` | read | Diagnose connectivity step by step with timings: DNS, TCP, security/Kerberos, Kafka with a new client, advertised addresses, Connect/SR |

## Security

- **API key (HTTP only)**: when `apiKey` is set, every request to `/mcp` (and anything below it) must carry the
  header `X-Authorization: <api-key>` with exactly that value (compared in constant time). A missing or different
  value gets **HTTP 401** with
  `{"error":"unauthorized","message":"X-Authorization header is missing or the API key does not match"}`.
  The header is `X-Authorization`, not `Authorization`, and takes the raw key (no `Bearer` prefix). An empty
  `apiKey` turns the check off. There are no users, roles or tokens. The server checks the key at startup: a blank
  value, surrounding whitespace or a `<...>`/`${...}` placeholder stops it; a key shorter than 16 characters only
  logs a warning. The startup log says `API key authentication enabled`, or warns `API key authentication DISABLED`
  when the key is empty.
- **Browser requests are refused**: a request to `/mcp` that carries an `Origin` header (that is, one sent by a web
  page) gets **HTTP 403** `Invalid Origin header`, even with the right key. This stops a web page from driving the
  tools through the user's browser (CSRF, DNS rebinding), which matters most when `apiKey` is empty. MCP clients such
  as Claude Code send no `Origin` header; browser-based clients cannot connect.
- The health endpoints `/actuator/health/liveness` and `/actuator/health/readiness` are not behind the key, so
  probes work. Only the health endpoint is exposed, and it shows no details.
- Run HTTP behind TLS. The OpenShift Route terminates TLS at the edge.
- On the Kafka side, the server acts as one principal (the Kerberos `principal`, or whatever a cluster's
  `properties` configure), so what it can do is bounded by that principal's ACLs. `readOnly` controls which tools
  are listed. It does not replace ACLs.
- **Secret masking in tool output**:
  - Kafka configs that the broker flags as sensitive are returned as `******`.
  - In connector configs (`connectors_get` including task configs, `connectors_config_diff`,
    `connect_plugins_validate`, `diagnose_connector`), values are returned as `******` when the key contains
    `password`, `passwd`, `passphrase`, `secret`, `token`, `credential`, `jaas`, `user.info`, `authorization` or
    `connection.string`, when one of its segments is `pwd`, `sas`, `auth` or `private`, when its last segment is
    `header` or `headers`, or when its last dotted segment contains `key` (for example `api.key`, but not
    `key.converter`).
  - In every other connector config value, and in connector error traces and validation messages, embedded
    credentials are replaced: URL user info (`mongodb://***@host`), Oracle `user/password@`, `password=`, `pwd=`,
    `SharedAccessKey=`, `AccountKey=`, `sig=`, `token=` pairs (JDBC URLs, connection strings) and `Bearer`/`Basic`
    credentials. Masking is pattern-based, so treat it as a safety net, not a guarantee.
  - JAAS passwords are masked wherever the client security settings are shown (`security_summary`,
    `kerberos_self_check`, `diagnose_connectivity`).
  - `kerberos_self_check` reports the keytab path, readability and size, never its content.
  - `scram_users_list` returns no secrets, and `delegation_tokens_list` returns no HMACs.
- Keep secrets out of the repository. `*.keytab`, `*.local.yaml`, `kafka-mcp.yaml` and `krb5.conf` in the
  repository root, `conf.d/`, and `deploy/openshift/kafka-mcp.yaml`/`krb5.conf` are git-ignored; the keytabs and
  config files are also excluded from the image build context. Examples use `<...>` placeholders.
- The pod does not mount a service account token (`automountServiceAccountToken: false`), because the server never
  calls the Kubernetes API.

## Logging

Logs are plain JSON, one event per line (Spring Boot structured logging, Logstash layout). In HTTP mode they go to
**stdout**. In stdio mode they go to **stderr**, because stdout carries the JSON-RPC stream (a test checks that
nothing else reaches stdout). The only non-JSON line is the JVM's own `Picked up JAVA_TOOL_OPTIONS: ...` on stderr at
startup, printed before logging starts.

Every tool call that passes the input schema validation produces one line with `tool`, `cluster`, `durationMs`,
`outcome` (`ok`/`error`) and `error`, for example:

```json
{"@timestamp":"2026-01-01T10:00:00.000Z","@version":"1","message":"tool call","logger_name":"io.github.srdrcn.kafkamcp.mcp.ToolRegistry","thread_name":"boundedElastic-1","level":"INFO","level_value":20000,"tool":"topics_list","cluster":"kafka-dev","durationMs":42,"outcome":"ok","error":null}
```

Calls that the MCP SDK rejects before they reach the server's code (a `cluster` outside the enum, a missing
required parameter, a wrong type, an unknown tool) produce no `tool call` line; the SDK logs
`Tool (<name>) input validation failed: ...` at WARN for the schema cases. A rejected request logs
`unauthorized request` at WARN with `remoteAddr` and `headerPresent`, never the key itself.

The log levels are `INFO` for the root logger and `WARN` for `org.apache.kafka` and `io.modelcontextprotocol`. You
can change them with Spring Boot arguments such as `--logging.level.org.apache.kafka=INFO`.

## Container image

`Dockerfile` is a multi-stage build:

- **Build stage**: `registry.access.redhat.com/ubi9/openjdk-21` runs `./mvnw package -DskipTests`. It downloads
  dependencies from Maven Central.
- **Runtime stage**: `registry.access.redhat.com/ubi9/openjdk-21-runtime`, with the jar at
  `/deployments/kafka-mcp.jar`.
- The base images default to the `latest` tag. To use a mirror, or to pin a version tag or digest for reproducible
  builds, override them with the build args `BUILDER_IMAGE` and `RUNTIME_IMAGE`, for example
  `make image BUILD_ARGS="--build-arg RUNTIME_IMAGE=registry.access.redhat.com/ubi9/openjdk-21-runtime@sha256:<digest>"`.

It is built to run under the OpenShift **restricted-v2** SCC:

- It runs as non-root user 185. `/deployments` and `/etc/kafka-mcp` are group 0 with `g=u`, so a random UID in group
  0 works.
- It writes only to `/tmp` (`-Djava.io.tmpdir=/tmp`), so the root filesystem can be read-only, with an `emptyDir`
  mounted at `/tmp`.
- `JAVA_TOOL_OPTIONS="-Djava.security.krb5.conf=/etc/kafka-mcp/krb5/krb5.conf -Djava.io.tmpdir=/tmp -XX:MaxRAMPercentage=75"`
- `ENTRYPOINT ["java", "-XX:+ExitOnOutOfMemoryError", "-jar", "/deployments/kafka-mcp.jar"]` with
  `CMD ["--config", "/etc/kafka-mcp/config/kafka-mcp.yaml"]`. After an `OutOfMemoryError` the JVM exits and
  OpenShift restarts the pod, instead of a half-working process that still passes its probes. Port 8080 is exposed.
- Mount points: `/etc/kafka-mcp/config` (kafka-mcp.yaml), `/etc/kafka-mcp/krb5` (krb5.conf),
  `/etc/kafka-mcp/keytabs` (keytab).

```bash
make image IMAGE=kafka-mcp:latest
podman run --rm -p 8080:8080 --read-only --tmpfs /tmp \
  -v "$PWD/kafka-mcp.local.yaml:/etc/kafka-mcp/config/kafka-mcp.yaml:ro" \
  -v "$PWD/krb5.conf:/etc/kafka-mcp/krb5/krb5.conf:ro" \
  -v "$PWD/svc.keytab:/etc/kafka-mcp/keytabs/svc.keytab:ro" \
  kafka-mcp:latest
```

The image has no `--transport` argument, so keep `port: 8080` in the config to get HTTP.

## OpenShift deployment

`deploy/openshift/` is a kustomize directory:

| File | Content |
|---|---|
| `deployment.yaml` | 1 replica (`Recreate`). Startup and liveness probes on `/actuator/health/liveness`, readiness probe on `/actuator/health/readiness`. A restricted-v2 security context (non-root, no privilege escalation, all capabilities dropped, `RuntimeDefault` seccomp, read-only root filesystem). No service account token is mounted. A `/tmp` `emptyDir` (256Mi). Requests of 100m CPU and 512Mi memory, with a 1Gi memory limit. |
| `service.yaml` | `kafka-mcp` Service on port 8080 (`http`). |
| `route.yaml` | Edge TLS (HTTP redirects to HTTPS), `haproxy.router.openshift.io/timeout: 60s`. |
| `kustomization.yaml` | The resources above, labels, and the `images:` entry that sets the image. |
| `kafka-mcp.yaml.example`, `krb5.conf.example` | Templates for the ConfigMap. |

The ConfigMap `kafka-mcp-config` (keys `kafka-mcp.yaml` and `krb5.conf`) and the Secret `kafka-mcp-keytab` hold
environment-specific data, so `oc apply -k` does not create them. You create them with `oc create`. Neither probe
depends on Kafka: a Kafka or KDC outage does not restart the pod or take it out of the Service. Use `clusters_list`
or `diagnose_connectivity` to check the clusters.

**1. Build and push the image**, either to a registry the cluster can pull from:

```bash
make image IMAGE=<registry>/<namespace>/kafka-mcp:<tag>
podman push <registry>/<namespace>/kafka-mcp:<tag>
```

or inside the cluster, into the internal registry (the build pod needs access to Maven Central):

```bash
oc new-build --name kafka-mcp --binary --strategy docker
oc start-build kafka-mcp --from-repo . --follow   # -> image-registry.openshift-image-registry.svc:5000/<namespace>/kafka-mcp:latest
```

`--from-repo` uploads the committed files of the clone (the HEAD commit), so local config files and keytabs are not
sent to the cluster.

**2. Set the image** in `deploy/openshift/kustomization.yaml`:

```yaml
images:
  - name: kafka-mcp
    newName: image-registry.openshift-image-registry.svc:5000/<namespace>/kafka-mcp   # or <registry>/<namespace>/kafka-mcp
    newTag: latest
```

**3. Prepare the config files** from the templates. Keep `port: 8080`, and keep the paths that match the mounts:
`krb5Conf: /etc/kafka-mcp/krb5/krb5.conf` and `keytab: /etc/kafka-mcp/keytabs/svc.keytab`. Generate the API key
with something like `openssl rand -hex 32` (the server does not start with the `<api-key>` placeholder; a key shorter
than 16 characters only logs a warning).

```bash
cp deploy/openshift/kafka-mcp.yaml.example kafka-mcp.yaml   # set apiKey, principal, clusters (git-ignored)
cp deploy/openshift/krb5.conf.example krb5.conf             # set the realm and the KDCs (git-ignored)
```

**4. Create the ConfigMap and the Secret, then apply the manifests.** The ConfigMap needs both keys,
`kafka-mcp.yaml` and `krb5.conf`. The Secret key must be `svc.keytab`, whatever the keytab file is called locally,
because the config points at `/etc/kafka-mcp/keytabs/svc.keytab`:

```bash
oc create configmap kafka-mcp-config --from-file=kafka-mcp.yaml --from-file=krb5.conf
oc create secret generic kafka-mcp-keytab --from-file=svc.keytab=/path/to/<principal>.keytab
oc apply -k deploy/openshift
oc rollout status deployment/kafka-mcp
oc get route kafka-mcp
```

The Route has no fixed host, so OpenShift generates `kafka-mcp-<namespace>.<apps-domain>`. To use
`kafka-mcp.<apps-domain>`, uncomment `spec.host` in `route.yaml`. The 60 s router timeout leaves room above the 15 s
tool limit.

The API key sits in the ConfigMap together with the rest of `kafka-mcp.yaml`, so anyone who can read ConfigMaps
in the namespace can read it. Limit access to the namespace accordingly.

**Smoke test:**

```bash
curl -s https://kafka-mcp.<apps-domain>/mcp \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -H "X-Authorization: $KAFKA_MCP_KEY" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

**Updating the config or the keytab.** The config is read only at startup, so replace the object and restart the
pod:

```bash
oc create configmap kafka-mcp-config --from-file=kafka-mcp.yaml --from-file=krb5.conf \
  --dry-run=client -o yaml | oc replace -f -
oc create secret generic kafka-mcp-keytab --from-file=svc.keytab=/path/to/<principal>.keytab \
  --dry-run=client -o yaml | oc replace -f -
oc rollout restart deployment/kafka-mcp
```

## Claude Code

Add the server to `.mcp.json` (project scope). The key stays in an environment variable and never in the file:

```json
{
  "mcpServers": {
    "kafka": {
      "type": "http",
      "url": "https://kafka-mcp.<apps-domain>/mcp",
      "headers": {
        "X-Authorization": "${KAFKA_MCP_KEY}"
      }
    }
  }
}
```

```bash
export KAFKA_MCP_KEY=<api-key>
claude
```

If the Route certificate is signed by a corporate CA, Node.js (which Claude Code runs on) must trust that CA. Point
`NODE_EXTRA_CA_CERTS` to the CA bundle in PEM format before you start `claude`:

```bash
export NODE_EXTRA_CA_CERTS=/path/to/corporate-ca.pem
```

Run `/mcp` inside Claude Code to check the connection. The tools appear as `mcp__kafka__<tool>`.

**stdio alternative.** Claude Code can also start the jar locally. In that case the keytab, `krb5.conf` and the
clusters must be reachable from your machine, and `apiKey` is not used. Pass `--transport stdio` explicitly,
because a config with `port` set would otherwise start HTTP:

```json
{
  "mcpServers": {
    "kafka": {
      "type": "stdio",
      "command": "java",
      "args": ["-jar", "/path/to/kafka-mcp.jar", "--config", "/path/to/kafka-mcp.local.yaml", "--transport", "stdio"]
    }
  }
}
```

For a local run, set `kerberos.krb5Conf` (or add `-Djava.security.krb5.conf=/path/to/krb5.conf` before `-jar`).

The same with the container image (absolute host paths; see [With Docker](#with-docker) for the mounts and file
permissions):

```json
{
  "mcpServers": {
    "kafka": {
      "type": "stdio",
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-v", "/path/to/kafka-mcp.local.yaml:/etc/kafka-mcp/config/kafka-mcp.yaml:ro",
        "-v", "/path/to/krb5.conf:/etc/kafka-mcp/krb5/krb5.conf:ro",
        "-v", "/path/to/svc.keytab:/etc/kafka-mcp/keytabs/svc.keytab:ro",
        "kafka-mcp:latest",
        "--config", "/etc/kafka-mcp/config/kafka-mcp.yaml", "--transport", "stdio"
      ]
    }
  }
}
```

## Troubleshooting

Start with `clusters_list` (reachability and mode of every cluster), then `diagnose_connectivity` (`diagnose`
toolset), which checks DNS, TCP, the security settings and Kerberos, connects with a new Kafka client, and checks
the brokers' advertised addresses and Connect/Schema Registry, with timings for each step. `kerberos_self_check`
(`acl` toolset) checks `krb5.conf`, the keytab and the principal, and tries a real login. Kafka and Kerberos errors
are translated into short explanations, with the original message in parentheses.

| Symptom | Cause and fix |
|---|---|
| HTTP 401 `X-Authorization header is missing or the API key does not match` | The key does not match `apiKey` exactly. Check that `KAFKA_MCP_KEY` is exported in the shell that starts Claude Code and that it equals `apiKey` in the ConfigMap (no extra whitespace, no `Bearer` prefix, header name `X-Authorization`). The server log shows `unauthorized request` with `headerPresent`. |
| HTTP 403 `Invalid Origin header` | The request came from a web page or a browser-based client (it carried an `Origin` header). Use a non-browser MCP client such as Claude Code. |
| `Kerberos clock skew: ...` | The pod clock differs from the KDC by more than the allowed skew (usually 5 minutes). Fix NTP on the nodes or the KDC. |
| `Broker service principal (kafka/<host>@REALM) not found in the KDC ...` | Usually reverse DNS turns the broker address into a name with no service principal. Set `rdns = false` (and `dns_canonicalize_hostname = false`) in `[libdefaults]` of `krb5.conf`, as in `krb5.conf.example`, and use broker FQDNs in `bootstrapServers`. |
| `Cannot reach the KDC (...)` | Check the `kdc` entries in `krb5.conf`, DNS, and the firewall/egress to port 88. The example sets `udp_preference_limit = 1` to force TCP. |
| `Keytab not found or unreadable, or the principal is not in the keytab` | `kerberos.keytab` must match the mounted file (`/etc/kafka-mcp/keytabs/svc.keytab`, so the Secret key must be `svc.keytab`), and the principal must be in the keytab. The Secret is mounted with mode `0440`. |
| `Keytab key is invalid (password/kvno changed; regenerate the keytab)` | The principal's password or kvno changed. Create a new keytab and update the Secret. |
| `krb5.conf not found or default_realm missing` | Check the `krb5.conf` key in the ConfigMap and `-Djava.security.krb5.conf` in `JAVA_TOOL_OPTIONS`. |
| Bootstrap works, but calls time out or fail for some brokers | The brokers advertise addresses the pod cannot resolve or reach. `diagnose_connectivity` tests every advertised address. |
| `Tool did not finish within 15 s (timeout). Try a narrower filter/limit.` | On very large clusters, narrow `filter` or lower `limit`, pass a specific `topic`/`group`, or use a shorter `intervalSeconds`. |
| `Regex match stopped at the tool time limit; use a simpler or more specific regex` | A `filter`/regex parameter was too expensive (for example `.*a.*b.*` on long values). Anchor it or make it more specific. |
| `Creating the Kafka client for cluster ... took longer than 15 s (slow Kerberos login or KDC?)` | The Kerberos login for that cluster is slow or hangs; the creation continues in the background. Other clusters are not affected. Check the KDC with `kerberos_self_check`. |
| `Kafka timeout: brokers unreachable or not responding` or `No response from Kafka within 15 s (timeout)` | Check `bootstrapServers`, the network path from the pod, and Kerberos (see above). |
| A tool or cluster is missing from the list | Its toolset is not in `toolsets`, the cluster is read-only (write tools), `disableDestructive` is on (delete/reset tools), or the cluster has no `connectUrl`/`schemaRegistryUrl`. |
| `format=avro requires a schemaRegistryUrl on cluster ...` | Add `schemaRegistryUrl` to the cluster. |
| Startup fails with `Unknown toolset: [...]` or `Invalid config: ...` | Fix the named key. Valid toolsets are `acl`, `connect`, `consumers`, `core`, `diagnose`, `messages`, `report` and `schemaregistry`. `apiKey still holds a placeholder` means the key was not filled in; `must not contain credentials` means a `connectUrl`/`schemaRegistryUrl` has `user:password@`. |

## Development

```
src/main/java/io/github/srdrcn/kafkamcp/
  KafkaMcpApplication.java   CLI parsing, transport selection
  config/                    YAML loading, --config-dir merge, validation
  kafka/                     per-cluster client cache (KafkaClients), Kafka helpers, error translation
  connect/                   Kafka Connect REST client
  schemaregistry/            Schema Registry REST client
  mcp/                       MCP server (HTTP + stdio), API key filter, readOnly/destructive tool filter, tool registry
  toolsets/<toolset>/        one package per toolset
```

- `make test` (`./mvnw -B verify`) runs the full test suite (over 400 tests). The integration tests use
  **Testcontainers**, which needs a working Docker or Podman. Images used:
  - `apache/kafka:4.1.1`: a KRaft broker with SASL/PLAIN and the StandardAuthorizer. The same image also runs the
    Kafka Connect worker.
  - `confluentinc/cp-kafka:7.9.1` + `confluentinc/cp-zookeeper:7.9.1`: a ZooKeeper-mode broker (Kafka 3.9) with
    SASL/PLAIN and the AclAuthorizer, for the mixed-fleet tests.
  - `confluentinc/cp-schema-registry:7.9.1`.
- `ToolSchemaSnapshotTest` checks every tool schema against `src/test/resources/snapshots/<toolset>.json` and
  enforces the Claude Code compatibility rules:
  - every schema is `type: object` with `properties` and a `required` array (even when it is empty), and contains
    no null values;
  - the description of a tool starts with `WRITE:` exactly when it is a write tool;
  - a tool is destructive exactly when its name ends in `_delete` or `_reset`, optionally followed by one more
    segment (for example `schemas_delete_version`);
  - every `*_list` tool takes `filter` and `limit`.

  After an intended schema change, update the snapshots and review the diff:

  ```bash
  ./mvnw test -Dtest=ToolSchemaSnapshotTest -Dsnapshot.update=true   # or: make snapshot
  ```

  Then update the [Tool reference](#tool-reference) tables above by hand to match the new snapshots: the tool
  name, the kind (from the annotations: `readOnlyHint` is read, `destructiveHint` is destructive, otherwise write)
  and the description without the `WRITE:` prefix.
- Other focused tests: `ToolFilterTest` (readOnly, per-cluster override, disableDestructive), `ApiKeyHttpTest`
  (correct, wrong and empty key, `/mcp/` variants, `Origin`, exposed actuator endpoints), `StdioTransportTest`
  (stdout carries only JSON-RPC), `McpHttpEndToEndTest`, `ConfigLoaderTest` (drop-in merge and validation),
  `MixedClustersTest` and the `*MixedCluster*` tests (ZooKeeper + KRaft).
- To add a tool, declare a `ToolSpec` with `ToolSpec.read`, `ToolSpec.write` or `ToolSpec.destructive` in a
  `*Tools` component in the toolset's package, and return it from that component's `tools()`, which the toolset
  class (for example `CoreToolset`) includes in its own `tools()`. Call `.connect()` or `.schemaRegistry()` if the
  tool needs Kafka Connect or Schema Registry, so that it is offered only on clusters that have them, and
  `.listParams()` for a `*_list` tool. The `cluster` parameter and the `WRITE:` prefix are added automatically.
  Then run `make snapshot` and add the tool to the [Tool reference](#tool-reference).
