package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static io.github.srdrcn.kafkamcp.toolsets.diagnose.Diagnosis.evidence;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KerberosConfig;
import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient;
import io.github.srdrcn.kafkamcp.toolsets.acl.Acls;
import io.github.srdrcn.kafkamcp.toolsets.acl.ClientSecurity;
import io.github.srdrcn.kafkamcp.toolsets.acl.KerberosCheck;

/**
 * diagnose_connectivity: step-by-step connectivity diagnosis from this server to the cluster (with timings):
 * bootstrap DNS and TCP, client security settings, Kerberos (for GSSAPI), describeCluster with a NEW AdminClient
 * (not the cached one), TCP to the brokers' advertised addresses, Connect and Schema Registry REST.
 */
@Component
public class DiagnoseConnectivityTools {

	static final long DNS_TIMEOUT_MS = 3_000;

	static final long TCP_TIMEOUT_MS = 2_000;

	static final long KERBEROS_LOGIN_MS = 4_000;

	static final long KAFKA_TIMEOUT_MS = 5_000;

	static final long REST_TIMEOUT_MS = 4_000;

	/** Maximum wait for the bootstrap metadata (advertised addresses) when describeCluster times out. */
	static final long BOOTSTRAP_METADATA_MS = 3_000;

	private static final AtomicLong SEQ = new AtomicLong();

	private final ConnectClient connect;

	private final SchemaRegistryClient schemaRegistry;

	public DiagnoseConnectivityTools(ConnectClient connect, SchemaRegistryClient schemaRegistry) {
		this.connect = connect;
		this.schemaRegistry = schemaRegistry;
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("diagnose_connectivity",
					"Diagnose connectivity step by step with timings: DNS, TCP, security/Kerberos, Kafka with a new client, advertised addresses, Connect/SR")
			.handler(this::diagnose));
	}

	record EndpointCheck(Net.Endpoint endpoint, Net.Resolution dns, Net.Probe tcp) {
	}

	record KafkaStep(Map<String, Object> step, List<Node> nodes, String clusterId, String mode, Throwable error) {
	}

	/** Result of the REST request and its duration measured inside the task (other steps' time is not included). */
	record RestResult(JsonNode value, Throwable error, long durationMs) {
	}

	Object diagnose(ToolCall call) {
		ClusterConfig cluster = call.clusterConfig();
		Diagnosis dx = new Diagnosis();
		List<Map<String, Object>> steps = new ArrayList<>();
		Map<String, Object> facts = new LinkedHashMap<>();
		facts.put("bootstrapServers", cluster.getBootstrapServers());
		ClientSecurity security = ClientSecurity.of(call.clients(), cluster);
		facts.put("client", security.render());
		ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
		String mode = null;
		try {
			Net.Parsed parsed = Net.parseServers(cluster.getBootstrapServers());
			if (!parsed.invalid().isEmpty() || parsed.endpoints().isEmpty()) {
				dx.critical("INVALID_BOOTSTRAP", "bootstrapServers contains invalid entries",
						evidence("invalid", parsed.invalid(), "value", cluster.getBootstrapServers()),
						"Use a comma-separated host:port list (e.g. kafka-0.kafka:9092,kafka-1.kafka:9092)");
			}
			Map<Net.Endpoint, Future<EndpointCheck>> endpoints = new LinkedHashMap<>();
			for (Net.Endpoint e : new LinkedHashSet<>(parsed.endpoints())) {
				endpoints.put(e, pool.submit(() -> {
					Net.Resolution r = Net.resolve(e.host(), DNS_TIMEOUT_MS);
					Net.Probe p = r.ok() ? Net.connect(e, r.addresses().get(0), TCP_TIMEOUT_MS) : null;
					return new EndpointCheck(e, r, p);
				}));
			}
			KerberosCheck.Settings krb = security.gssapi() ? kerberosSettings(call, security) : null;
			Future<Map<String, Object>> kerberos = krb == null ? null
					: pool.submit(() -> KerberosCheck.check(krb, KERBEROS_LOGIN_MS));
			Future<RestResult> connectRoot = cluster.hasConnect()
					? pool.submit(timed(() -> connect.get(cluster.getName(), "/"))) : null;
			Future<RestResult> srConfig = cluster.hasSchemaRegistry()
					? pool.submit(timed(() -> schemaRegistry.get(cluster.getName(), "/config"))) : null;
			long restStart = System.nanoTime();

			// 1-2) bootstrap DNS + TCP
			List<EndpointCheck> checks = new ArrayList<>();
			for (Map.Entry<Net.Endpoint, Future<EndpointCheck>> f : endpoints.entrySet()) {
				EndpointCheck ec = await(call, f.getValue(), DNS_TIMEOUT_MS + TCP_TIMEOUT_MS + 500);
				if (ec == null) {
					ec = new EndpointCheck(f.getKey(),
							new Net.Resolution(f.getKey().host(), List.of(), DNS_TIMEOUT_MS, Net.DNS_TIMEOUT,
									Net.explain(Net.DNS_TIMEOUT, f.getKey().host(), DNS_TIMEOUT_MS, null)),
							null);
				}
				checks.add(ec);
			}
			bootstrapFindings(dx, steps, checks);
			boolean anyTcp = checks.stream().anyMatch(c -> c.tcp() != null && c.tcp().ok());

			// 3) security settings
			steps.add(Results.map("step", "clientSecurity", "status", "ok", "securityProtocol",
					security.securityProtocol(), "saslMechanism", security.saslMechanism()));
			if ("PLAINTEXT".equals(security.securityProtocol()) || "SSL".equals(security.securityProtocol())) {
				dx.info("NO_SASL", "This server connects to the cluster with " + security.securityProtocol() + " (no SASL)",
						null, null);
			}

			// 4) Kerberos
			boolean kerberosOk = true;
			if (kerberos != null) {
				Map<String, Object> k = await(call, kerberos, KERBEROS_LOGIN_MS + 2_000);
				kerberosOk = kerberosFindings(dx, steps, facts, k,
						"true".equalsIgnoreCase(security.jaasOption("useTicketCache")));
			}

			// 5) Kafka (new AdminClient) + 6) advertised addresses
			KafkaStep kafka;
			if (!anyTcp) {
				kafka = skipped("No TCP connection to any bootstrap address; the Kafka request was not attempted");
			}
			else if (!kerberosOk) {
				kafka = skipped("Kerberos login failed; the client cannot log in with GSSAPI either, so the Kafka request was not attempted");
			}
			else {
				kafka = kafkaStep(call, cluster);
			}
			steps.add(kafka.step());
			if (kafka.error() != null) {
				kafkaFindings(dx, kafka, anyTcp);
			}
			if (kafka.mode() != null) {
				mode = kafka.mode();
			}
			if (kafka.nodes() != null) {
				facts.put("clusterId", kafka.clusterId());
				advertised(call, dx, steps, facts, pool, kafka.nodes(), checks);
			}
			else if (kafka.error() != null
					&& Acls.causedBy(kafka.error(), org.apache.kafka.common.errors.TimeoutException.class)) {
				// typical OpenShift failure: bootstrap is reachable, the advertised addresses are not → describeCluster times out.
				// Read the advertised addresses from metadata fetched over the bootstrap connection only, and probe them.
				KafkaStep md = bootstrapMetadata(call, cluster);
				steps.add(md.step());
				if (md.nodes() != null && !md.nodes().isEmpty()) {
					advertised(call, dx, steps, facts, pool, md.nodes(), checks);
				}
			}

			// 7) Connect and Schema Registry
			if (connectRoot != null) {
				rest(call, dx, steps, connectRoot, restStart, "connect", cluster.getConnectUrl(), "CONNECT_UNREACHABLE",
						"Kafka Connect REST", n -> Results.map("version", n.path("version").asText(null),
								"kafkaClusterId", n.path("kafka_cluster_id").asText(null)));
			}
			if (srConfig != null) {
				rest(call, dx, steps, srConfig, restStart, "schemaRegistry", cluster.getSchemaRegistryUrl(),
						"SCHEMA_REGISTRY_UNREACHABLE", "Schema Registry REST",
						n -> Results.map("compatibilityLevel", n.path("compatibilityLevel").asText(null)));
			}
		}
		finally {
			pool.shutdownNow();
		}
		Map<String, Object> extra = new LinkedHashMap<>();
		extra.put("steps", steps);
		return dx.result(Results.map("cluster", cluster.getName(), "mode", mode), extra, facts);
	}

	// ---- steps ----

	private static void bootstrapFindings(Diagnosis dx, List<Map<String, Object>> steps, List<EndpointCheck> checks) {
		List<String> dnsFailed = new ArrayList<>();
		Map<String, List<String>> tcpFailed = new LinkedHashMap<>();
		List<String> tcpErrors = new ArrayList<>();
		for (EndpointCheck c : checks) {
			Map<String, Object> dns = c.dns().toMap();
			dns.put("step", "dns");
			steps.add(reorder(dns));
			if (!c.dns().ok()) {
				dnsFailed.add(c.dns().error());
				continue;
			}
			Map<String, Object> tcp = c.tcp().toMap();
			tcp.put("step", "tcp");
			steps.add(reorder(tcp));
			if (!c.tcp().ok()) {
				tcpFailed.computeIfAbsent(c.tcp().errorType(), k -> new ArrayList<>()).add(c.endpoint().address());
				tcpErrors.add(c.tcp().error());
			}
		}
		int total = checks.size();
		boolean anyTcp = checks.stream().anyMatch(c -> c.tcp() != null && c.tcp().ok());
		if (!dnsFailed.isEmpty()) {
			boolean all = dnsFailed.size() == total;
			String msg = (all ? "No bootstrap address could be resolved" : dnsFailed.size() + "/" + total
					+ " bootstrap addresses could not be resolved") + ": " + dnsFailed.get(0);
			Map<String, Object> ev = evidence("errors", DiagnoseParams.first(dnsFailed, DiagnoseParams.MAX_EVIDENCE));
			String rec = "Check the names in bootstrapServers; try nslookup/dig from inside the pod. On OpenShift a "
					+ "Service name must look like <svc>.<namespace>.svc(.cluster.local); for external addresses check DNS/egress";
			if (all) {
				dx.critical("DNS_UNRESOLVED", msg, ev, rec);
			}
			else {
				dx.warning("DNS_UNRESOLVED", msg, ev, rec);
			}
		}
		for (Map.Entry<String, List<String>> e : tcpFailed.entrySet()) {
			String code = switch (e.getKey()) {
				case Net.REFUSED -> "CONNECTION_REFUSED";
				case Net.TIMEOUT -> "CONNECTION_TIMEOUT";
				case Net.NO_ROUTE -> "NO_ROUTE_TO_HOST";
				default -> "CONNECTION_FAILED";
			};
			String rec = switch (e.getKey()) {
				case Net.REFUSED -> "Check the port number and that the broker listens on this address (listeners); "
						+ "is the broker/pod up?";
				case Net.TIMEOUT -> "A firewall, OpenShift NetworkPolicy or egress rule may block this pod from reaching "
						+ "the broker port; verify with the network team";
				default -> "Check the network path (routing/firewall)";
			};
			boolean all = !anyTcp;
			String msg = tcpErrors.stream().filter(t -> e.getValue().stream().anyMatch(t::contains)).findFirst()
				.orElse(e.getKey());
			Map<String, Object> ev = evidence("endpoints", e.getValue());
			if (all) {
				dx.critical(code, msg, ev, rec);
			}
			else {
				dx.warning(code, msg + " (" + e.getValue().size() + "/" + total + " addresses)", ev, rec);
			}
		}
	}

	private static Map<String, Object> reorder(Map<String, Object> m) {
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("step", m.remove("step"));
		out.putAll(m);
		return out;
	}

	private static KerberosCheck.Settings kerberosSettings(ToolCall call, ClientSecurity security) {
		KerberosConfig k = call.clients().config().getKerberos();
		String keytab = security.jaasOption("keyTab");
		if (k != null && (keytab == null || keytab.equals(k.getKeytab()))) {
			return new KerberosCheck.Settings("kerberos", k.getKrb5Conf(), k.getKeytab(), k.getPrincipal(),
					k.getServiceName());
		}
		return new KerberosCheck.Settings("cluster properties (sasl.jaas.config)", null, keytab,
				security.jaasOption("principal"),
				security.kerberosServiceName() == null ? "kafka" : security.kerberosServiceName());
	}

	/**
	 * Kerberos findings; true if the login did not fail (succeeded or was not attempted, e.g. ticket cache): the Kafka
	 * step is attempted anyway. ticketCache: sasl.jaas.config useTicketCache=true (kinit sidecar; no keytab needed).
	 * Problems are classified by their leading technical token (keytab, principal, krb5.conf, Kerberos login) and the
	 * structured keytab/principal fields of the check result, not by their wording.
	 */
	@SuppressWarnings("unchecked")
	private static boolean kerberosFindings(Diagnosis dx, List<Map<String, Object>> steps, Map<String, Object> facts,
			Map<String, Object> k, boolean ticketCache) {
		if (k == null) {
			steps.add(Results.map("step", "kerberos", "status", "timeout", "error", "Kerberos check did not finish within the time limit"));
			dx.critical("KERBEROS_TIMEOUT", KafkaErrors.kerberos("cannot locate kdc") + " (the check did not finish within the time limit)",
					null, "Check that this pod can reach the kdc addresses in krb5.conf (port 88); kerberos_self_check");
			return false;
		}
		Map<String, Object> login = k.get("login") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
		Map<String, Object> keytab = k.get("keytab") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
		Object loginStatus = login.get("status");
		boolean loginFailed = "error".equals(loginStatus) || "timeout".equals(loginStatus);
		String stepStatus = Boolean.TRUE.equals(k.get("ok")) ? "ok" : "skipped".equals(loginStatus) ? "skipped" : "error";
		steps.add(Results.map("step", "kerberos", "status", stepStatus, "durationMs", login.get("durationMs"),
				"principal", k.get("principal") instanceof Map<?, ?> p ? p.get("name") : null, "keytab",
				keytab.get("path"), "login", loginStatus, "reason", login.get("reason"), "ticketCache",
				ticketCache ? true : null, "error", login.get("error")));
		facts.put("kerberos", Results.map("krb5Conf", k.get("krb5Conf"), "keytab", keytab, "principal",
				k.get("principal"), "serviceName", k.get("serviceName"), "notes", k.get("notes")));
		List<String> problems = k.get("problems") instanceof List<?> l ? (List<String>) l : List.of();
		Map<String, Object> principal = k.get("principal") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
		boolean keytabUnset = keytab.get("path") == null;
		boolean principalUnset = principal.get("name") == null;
		boolean keytabMissing = false;
		for (String p : problems) {
			boolean keytabProblem = p.startsWith("keytab");
			if (ticketCache && (keytabProblem && keytabUnset || p.startsWith("principal") && principalUnset)) {
				dx.info("KERBEROS_TICKET_CACHE",
						p + ": sasl.jaas.config useTicketCache=true, the ticket cache (kinit) is used; the login was not "
								+ "tried in advance, see the result of the Kafka step",
						null, "If the Kafka step reports a Kerberos error, check the ticket cache (klist, KRB5CCNAME) and the "
								+ "kinit sidecar");
			}
			else if (keytabProblem && !keytabUnset && Boolean.FALSE.equals(keytab.get("exists"))) {
				keytabMissing = true;
				dx.critical("KEYTAB_NOT_FOUND", "Keytab not found: " + keytab.get("path"), evidence("problem", p),
						"Check the kerberos.keytab path and that the keytab Secret is mounted into the pod");
			}
			else if (keytabProblem && (Boolean.FALSE.equals(keytab.get("readable"))
					|| keytab.get("sizeBytes") instanceof Number size && size.longValue() == 0)) {
				keytabMissing = true;
				dx.critical("KEYTAB_UNREADABLE", p, null, "Check the file permissions (pod user) and the Secret content");
			}
			else if (keytabProblem && Boolean.FALSE.equals(keytab.get("principalInKeytab"))) {
				dx.critical("KEYTAB_PRINCIPAL_MISSING", p, null,
						"Match kerberos.principal exactly to the name in the keytab (klist -kt; upper-case realm)");
			}
			else if (p.startsWith("krb5.conf")) {
				dx.critical("KRB5_CONF_PROBLEM", p, null,
						"Mount krb5.conf as a ConfigMap and set its path with kerberos.krb5Conf");
			}
			else if (p.startsWith("Kerberos login")) {
				String err = String.valueOf(login.get("error"));
				if (keytabMissing && err.toLowerCase().contains("keytab")) {
					continue;
				}
				dx.critical("KERBEROS_LOGIN_FAILED", p, evidence("detail", login.get("detail")), loginHint(err));
			}
			else if (p.contains("realm")) {
				dx.warning("KERBEROS_REALM", p, null, "Match the principal realm to the krb5.conf default_realm");
			}
			else {
				dx.critical("KERBEROS_PROBLEM", p, null, "Run a detailed check with kerberos_self_check");
			}
		}
		return !loginFailed;
	}

	private static String loginHint(String error) {
		String e = error == null ? "" : error;
		if (e.contains("KDC")) {
			return "Check that this pod can reach the kdc addresses in krb5.conf (TCP/UDP 88), DNS and egress/NetworkPolicy";
		}
		if (e.contains("clock skew")) {
			return "Synchronize the pod and KDC clocks (NTP); the difference must be under 5 minutes";
		}
		if (e.contains("keytab")) {
			return "Regenerate the keytab (the password/kvno may have changed) and check the principal name";
		}
		return "Run a detailed check with kerberos_self_check";
	}

	private static KafkaStep skipped(String reason) {
		return new KafkaStep(Results.map("step", "kafka", "status", "skipped", "reason", reason), null, null, null,
				null);
	}

	/** describeCluster with a new, short-lived AdminClient instead of the cached one (closed afterwards). */
	private static KafkaStep kafkaStep(ToolCall call, ClusterConfig cluster) {
		long timeout = Math.max(1_000, Math.min(KAFKA_TIMEOUT_MS, call.remainingMillis() - 5_000));
		Map<String, Object> props = new HashMap<>(call.clients().clientProperties(cluster));
		props.put(CommonClientConfigs.CLIENT_ID_CONFIG,
				"kafka-mcp-" + cluster.getName() + "-connectivity-" + SEQ.incrementAndGet());
		props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, (int) timeout);
		props.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeout);
		props.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 3_000);
		props.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, 3_000);
		props.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1_000);
		props.put(AdminClientConfig.RETRIES_CONFIG, 2);
		long start = System.nanoTime();
		FutureTask<Object[]> task = new FutureTask<>(() -> {
			Admin admin = Admin.create(props);
			try {
				DescribeClusterResult r = admin.describeCluster(new DescribeClusterOptions().timeoutMs((int) timeout));
				String id = r.clusterId().get(timeout + 500, TimeUnit.MILLISECONDS);
				Collection<Node> nodes = r.nodes().get(timeout + 500, TimeUnit.MILLISECONDS);
				return new Object[] { id, new ArrayList<>(nodes), mode(admin, timeout) };
			}
			finally {
				admin.close(Duration.ofSeconds(1));
			}
		});
		Thread.ofPlatform().daemon().name("diagnose-connectivity").start(task);
		try {
			Object[] r = task.get(Math.max(1, Math.min(call.waitMillis() - 3_000, timeout + 1_500)), TimeUnit.MILLISECONDS);
			@SuppressWarnings("unchecked")
			List<Node> nodes = (List<Node>) r[1];
			nodes.sort(java.util.Comparator.comparingInt(Node::id));
			return new KafkaStep(Results.map("step", "kafka", "status", "ok", "durationMs", elapsed(start), "clusterId",
					r[0], "brokers", nodes.size(), "mode", r[2], "client", "new AdminClient (not the cached one)"), nodes,
					(String) r[0], (String) r[2], null);
		}
		catch (TimeoutException e) {
			task.cancel(true);
			return new KafkaStep(Results.map("step", "kafka", "status", "timeout", "durationMs", elapsed(start), "error",
					"describeCluster got no response within " + timeout + " ms"), null, null, null,
					new org.apache.kafka.common.errors.TimeoutException("describeCluster timed out"));
		}
		catch (ExecutionException e) {
			Throwable cause = KafkaErrors.unwrap(e.getCause());
			return new KafkaStep(Results.map("step", "kafka", "status", "error", "durationMs", elapsed(start), "error",
					KafkaErrors.explain(cause)), null, null, null, cause);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Tool was cancelled");
		}
	}

	/**
	 * When describeCluster times out, learns the brokers' advertised addresses ONLY over the bootstrap connection: the
	 * first MetadataRequest of a new, short-lived assign-only consumer goes to the bootstrap address; the partition
	 * leader/replica Nodes it returns are the advertised host:ports (closed afterwards).
	 */
	private static KafkaStep bootstrapMetadata(ToolCall call, ClusterConfig cluster) {
		long timeout = Math.max(500, Math.min(BOOTSTRAP_METADATA_MS, call.remainingMillis() - 5_000));
		Map<String, Object> overrides = new HashMap<>();
		overrides.put(CommonClientConfigs.CLIENT_ID_CONFIG,
				"kafka-mcp-" + cluster.getName() + "-connectivity-md-" + SEQ.incrementAndGet());
		overrides.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, (int) timeout);
		overrides.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeout);
		overrides.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 2_000);
		overrides.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, 2_000);
		long start = System.nanoTime();
		FutureTask<List<Node>> task = new FutureTask<>(() -> {
			try (KafkaConsumer<byte[], byte[]> consumer = call.clients().newConsumer(cluster.getName(), overrides)) {
				Map<Integer, Node> nodes = new TreeMap<>();
				for (List<PartitionInfo> partitions : consumer.listTopics(Duration.ofMillis(timeout)).values()) {
					for (PartitionInfo p : partitions) {
						addNode(nodes, p.leader());
						if (p.replicas() != null) {
							for (Node n : p.replicas()) {
								addNode(nodes, n);
							}
						}
					}
				}
				return new ArrayList<>(nodes.values());
			}
		});
		Thread.ofPlatform().daemon().name("diagnose-connectivity-metadata").start(task);
		String note = "describeCluster timed out: advertised addresses come only from metadata fetched over the bootstrap connection";
		try {
			List<Node> nodes = task.get(Math.max(1, Math.min(call.waitMillis() - 3_000, timeout + 1_500)),
					TimeUnit.MILLISECONDS);
			if (nodes.isEmpty()) {
				return new KafkaStep(Results.map("step", "bootstrapMetadata", "status", "error", "durationMs",
						elapsed(start), "error", "Metadata has no partitions; the advertised addresses could not be determined"), null, null,
						null, null);
			}
			return new KafkaStep(Results.map("step", "bootstrapMetadata", "status", "ok", "durationMs", elapsed(start),
					"brokers", nodes.size(), "note", note), nodes, null, null, null);
		}
		catch (TimeoutException e) {
			task.cancel(true);
			return new KafkaStep(Results.map("step", "bootstrapMetadata", "status", "timeout", "durationMs",
					elapsed(start), "error", "bootstrap metadata request got no response within " + timeout + " ms"), null, null,
					null, null);
		}
		catch (ExecutionException e) {
			Throwable cause = KafkaErrors.unwrap(e.getCause());
			return new KafkaStep(Results.map("step", "bootstrapMetadata", "status", "error", "durationMs",
					elapsed(start), "error", cause instanceof ToolException ? cause.getMessage() : KafkaErrors.explain(cause)),
					null, null, null, null);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Tool was cancelled");
		}
	}

	private static void addNode(Map<Integer, Node> nodes, Node n) {
		if (n != null && !n.isEmpty() && n.id() >= 0 && n.host() != null && !n.host().isBlank()) {
			nodes.putIfAbsent(n.id(), n);
		}
	}

	/** KRaft (the metadata quorum responded) or ZooKeeper (API not supported); null if it cannot be determined. */
	private static String mode(Admin admin, long timeout) {
		try {
			admin.describeMetadataQuorum().quorumInfo().get(Math.min(2_000, timeout), TimeUnit.MILLISECONDS);
			return ClusterMeta.KRAFT;
		}
		catch (ExecutionException e) {
			return KafkaErrors.unwrap(e.getCause()) instanceof UnsupportedVersionException ? ClusterMeta.ZOOKEEPER : null;
		}
		catch (TimeoutException e) {
			return null;
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return null;
		}
	}

	private static void kafkaFindings(Diagnosis dx, KafkaStep k, boolean anyTcp) {
		Throwable e = k.error();
		String chain = KafkaErrors.explain(e);
		String kerberos = KafkaErrors.kerberos(chain);
		if (kerberos != null) {
			boolean serviceMissing = chain.toLowerCase().contains("server not found in kerberos database");
			dx.critical(serviceMissing ? "KERBEROS_SERVICE_PRINCIPAL" : "KERBEROS_ERROR", kerberos,
					evidence("error", chain),
					serviceMissing ? "Use the brokers' FQDNs in bootstrapServers (not IPs/short names) and set "
							+ "rdns = false in krb5.conf [libdefaults]; the service principal must be kafka/<fqdn>@REALM"
							: loginHint(kerberos));
		}
		else if (Acls.causedBy(e, SaslAuthenticationException.class)) {
			dx.critical("SASL_AUTH_FAILED", "SASL authentication failed", evidence("error", chain),
					"Check the user/password (sasl.jaas.config), sasl.mechanism (PLAIN/SCRAM-SHA-512/GSSAPI) and that "
							+ "the user exists on the broker");
		}
		else if (Acls.causedBy(e, SslAuthenticationException.class)) {
			dx.critical("SSL_HANDSHAKE_FAILED", "TLS handshake failed", evidence("error", chain),
					"Does the truststore contain the broker CA certificate, and does hostname verification (ssl.endpoint.identification.algorithm) match?");
		}
		else if (Acls.causedBy(e, org.apache.kafka.common.errors.TimeoutException.class)) {
			dx.critical("KAFKA_TIMEOUT",
					anyTcp ? "TCP connection is open but the Kafka request got no response in time"
							: "Kafka request got no response in time",
					evidence("error", chain),
					"Do security.protocol/sasl.mechanism match the broker listener (e.g. PLAINTEXT to a SASL port, plain "
							+ "connection to an SSL port)? Are the brokers' advertised.listeners addresses reachable from this pod?");
		}
		else {
			dx.critical("KAFKA_ERROR", "Kafka request failed: " + chain, null,
					"Check the network/security settings according to the error message");
		}
	}

	private static void advertised(ToolCall call, Diagnosis dx, List<Map<String, Object>> steps,
			Map<String, Object> facts, ExecutorService pool, List<Node> nodes, List<EndpointCheck> bootstrap) {
		long budget = Math.max(500, Math.min(DNS_TIMEOUT_MS + TCP_TIMEOUT_MS, call.remainingMillis() - 2_500));
		long dnsTimeout = Math.max(250, Math.min(DNS_TIMEOUT_MS, budget / 2));
		long tcpTimeout = Math.max(250, Math.min(TCP_TIMEOUT_MS, budget / 2));
		Map<Node, Future<Net.Probe>> probes = new LinkedHashMap<>();
		for (Node n : nodes) {
			Net.Endpoint e = new Net.Endpoint(n.host(), n.port());
			probes.put(n, pool.submit(() -> Net.resolveAndConnect(e, dnsTimeout, tcpTimeout)));
		}
		List<Map<String, Object>> brokers = new ArrayList<>();
		List<Map<String, Object>> unreachable = new ArrayList<>();
		for (Map.Entry<Node, Future<Net.Probe>> p : probes.entrySet()) {
			Node n = p.getKey();
			Net.Probe probe = await(call, p.getValue(), dnsTimeout + tcpTimeout + 500);
			String address = new Net.Endpoint(n.host(), n.port()).address();
			Map<String, Object> step = probe == null
					? Results.map("step", "advertised", "brokerId", n.id(), "target", address, "status", "timeout")
					: Results.map("step", "advertised", "brokerId", n.id(), "target", address, "status",
							probe.ok() ? "ok" : "error", "durationMs", probe.durationMs(), "error", probe.error());
			steps.add(step);
			brokers.add(Results.map("id", n.id(), "advertised", address, "rack", n.hasRack() ? n.rack() : null,
					"reachable", probe != null && probe.ok()));
			if (probe == null || !probe.ok()) {
				unreachable.add(Results.map("brokerId", n.id(), "advertised", address, "error",
						probe == null ? "timeout" : probe.error()));
			}
		}
		facts.put("brokers", brokers);
		if (!unreachable.isEmpty()) {
			Set<String> bootstrapHosts = new LinkedHashSet<>();
			bootstrap.forEach(b -> bootstrapHosts.add(b.endpoint().host()));
			dx.critical("ADVERTISED_UNREACHABLE",
					unreachable.size() + "/" + nodes.size()
							+ " brokers' advertised addresses are unreachable from this pod: even though bootstrap works, clients "
							+ "cannot talk to these brokers",
					evidence("brokers", unreachable, "bootstrapHosts", List.copyOf(bootstrapHosts)),
					"The broker's advertised.listeners must be a name clients can resolve and reach (OpenShift: the "
							+ "Route/LoadBalancer address for external access, the Service name for internal access; separate internal/external listeners)");
		}
	}

	interface Extract {

		Map<String, Object> apply(JsonNode n);

	}

	/** Runs the REST call and measures its duration inside the task (errors are returned as a result too). */
	private static Callable<RestResult> timed(Callable<JsonNode> request) {
		return () -> {
			long start = System.nanoTime();
			try {
				return new RestResult(request.call(), null, elapsed(start));
			}
			catch (Exception e) {
				return new RestResult(null, e, elapsed(start));
			}
		};
	}

	static void rest(ToolCall call, Diagnosis dx, List<Map<String, Object>> steps, Future<RestResult> f,
			long submitted, String step, String url, String code, String what, Extract extract) {
		long wait = Math.max(1, Math.min(REST_TIMEOUT_MS - (System.nanoTime() - submitted) / 1_000_000,
				call.remainingMillis() - 1_000));
		RestResult r;
		try {
			r = f.get(wait, TimeUnit.MILLISECONDS);
		}
		catch (TimeoutException e) {
			f.cancel(true);
			steps.add(Results.map("step", step, "target", url, "status", "timeout", "error",
					what + " did not respond within " + REST_TIMEOUT_MS + " ms"));
			dx.warning(code, what + " did not respond within " + REST_TIMEOUT_MS + " ms: " + url, null,
					"Check the URL, that the service is up and that this pod can reach it (NetworkPolicy/egress)");
			return;
		}
		catch (ExecutionException e) {
			r = new RestResult(null, e.getCause(), -1);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Tool was cancelled");
		}
		Long duration = r.durationMs() >= 0 ? r.durationMs() : null;
		if (r.error() == null) {
			Map<String, Object> m = Results.map("step", step, "target", url, "status", "ok", "durationMs", duration);
			m.putAll(extract.apply(r.value()));
			steps.add(m);
			return;
		}
		Throwable cause = r.error();
		String err = cause instanceof ToolException te ? te.getMessage() : KafkaErrors.explain(cause);
		steps.add(Results.map("step", step, "target", url, "status", "error", "durationMs", duration, "error", err));
		String type = Net.errorType(cause);
		String detail = Net.ERROR.equals(type) ? err : Net.explain(type, url, REST_TIMEOUT_MS, null);
		dx.warning(code, what + " unreachable: " + detail, evidence("error", err),
				err.contains("401") || err.contains("403")
						? "Credentials/authorization required: check the user info in the URL or the proxy settings"
						: "Check the URL, that the service is up and that this pod can reach it (NetworkPolicy/egress)");
	}

	private static <T> T await(ToolCall call, Future<T> f, long maxMs) {
		try {
			return f.get(Math.max(1, Math.min(maxMs, call.remainingMillis() - 1_000)), TimeUnit.MILLISECONDS);
		}
		catch (TimeoutException e) {
			f.cancel(true);
			return null;
		}
		catch (ExecutionException e) {
			Throwable c = e.getCause();
			throw c instanceof ToolException te ? te : new ToolException(KafkaErrors.explain(c), c);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Tool was cancelled");
		}
	}

	private static long elapsed(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

}
