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
 * diagnose_connectivity: bu sunucudan cluster'a adım adım bağlantı teşhisi (zamanlamalarla): bootstrap DNS ve
 * TCP, istemci güvenlik ayarları, Kerberos (GSSAPI ise), önbellekteki değil YENİ bir AdminClient ile
 * describeCluster, broker'ların advertised adreslerine TCP, Connect ve Schema Registry REST.
 */
@Component
public class DiagnoseConnectivityTools {

	static final long DNS_TIMEOUT_MS = 3_000;

	static final long TCP_TIMEOUT_MS = 2_000;

	static final long KERBEROS_LOGIN_MS = 4_000;

	static final long KAFKA_TIMEOUT_MS = 5_000;

	static final long REST_TIMEOUT_MS = 4_000;

	/** describeCluster zaman aşımında bootstrap metadata'sı (advertised adresler) için en fazla bekleme. */
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
					"Bağlantı teşhisi (adım adım, süreli): DNS, TCP, güvenlik/Kerberos, yeni client ile Kafka, advertised adresler, Connect/SR")
			.handler(this::diagnose));
	}

	record EndpointCheck(Net.Endpoint endpoint, Net.Resolution dns, Net.Probe tcp) {
	}

	record KafkaStep(Map<String, Object> step, List<Node> nodes, String clusterId, String mode, Throwable error) {
	}

	/** REST isteğinin sonucu ve görev içinde ölçülen süresi (diğer adımların süresi karışmaz). */
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
				dx.critical("INVALID_BOOTSTRAP", "bootstrapServers geçersiz girdi içeriyor",
						evidence("invalid", parsed.invalid(), "value", cluster.getBootstrapServers()),
						"host:port biçiminde virgülle ayrılmış liste verin (ör. kafka-0.kafka:9092,kafka-1.kafka:9092)");
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

			// 3) güvenlik ayarları
			steps.add(Results.map("step", "clientSecurity", "status", "ok", "securityProtocol",
					security.securityProtocol(), "saslMechanism", security.saslMechanism()));
			if ("PLAINTEXT".equals(security.securityProtocol()) || "SSL".equals(security.securityProtocol())) {
				dx.info("NO_SASL", "Bu sunucu cluster'a " + security.securityProtocol() + " ile bağlanıyor (SASL yok)",
						null, null);
			}

			// 4) Kerberos
			boolean kerberosOk = true;
			if (kerberos != null) {
				Map<String, Object> k = await(call, kerberos, KERBEROS_LOGIN_MS + 2_000);
				kerberosOk = kerberosFindings(dx, steps, facts, k,
						"true".equalsIgnoreCase(security.jaasOption("useTicketCache")));
			}

			// 5) Kafka (yeni AdminClient) + 6) advertised adresler
			KafkaStep kafka;
			if (!anyTcp) {
				kafka = skipped("Hiçbir bootstrap adresine TCP bağlantısı kurulamadı; Kafka isteği denenmedi");
			}
			else if (!kerberosOk) {
				kafka = skipped("Kerberos login başarısız; client da GSSAPI login olamayacağı için Kafka isteği denenmedi");
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
				// tipik OpenShift hatası: bootstrap erişilebilir, advertised adresler değil → describeCluster zaman aşımı.
				// Advertised adresleri sadece bootstrap bağlantısıyla alınan metadata'dan okuyup yokla.
				KafkaStep md = bootstrapMetadata(call, cluster);
				steps.add(md.step());
				if (md.nodes() != null && !md.nodes().isEmpty()) {
					advertised(call, dx, steps, facts, pool, md.nodes(), checks);
				}
			}

			// 7) Connect ve Schema Registry
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

	// ---- adımlar ----

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
			String msg = (all ? "Hiçbir bootstrap adresi çözümlenemedi" : dnsFailed.size() + "/" + total
					+ " bootstrap adresi çözümlenemedi") + ": " + dnsFailed.get(0);
			Map<String, Object> ev = evidence("errors", DiagnoseParams.first(dnsFailed, DiagnoseParams.MAX_EVIDENCE));
			String rec = "bootstrapServers'taki adları kontrol edin; pod içinden nslookup/dig ile deneyin. OpenShift'te "
					+ "Service adı <svc>.<namespace>.svc(.cluster.local) biçiminde olmalı; dış adreslerde DNS/egress";
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
				case Net.REFUSED -> "Port numarasını ve broker'ın bu adreste dinlediğini (listeners) kontrol edin; "
						+ "broker/pod ayakta mı?";
				case Net.TIMEOUT -> "Firewall, OpenShift NetworkPolicy ya da egress kuralı bu pod'dan broker portuna "
						+ "erişimi engelliyor olabilir; ağ ekibiyle doğrulayın";
				default -> "Ağ yolunu (routing/firewall) kontrol edin";
			};
			boolean all = !anyTcp;
			String msg = tcpErrors.stream().filter(t -> e.getValue().stream().anyMatch(t::contains)).findFirst()
				.orElse(e.getKey());
			Map<String, Object> ev = evidence("endpoints", e.getValue());
			if (all) {
				dx.critical(code, msg, ev, rec);
			}
			else {
				dx.warning(code, msg + " (" + e.getValue().size() + "/" + total + " adres)", ev, rec);
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
	 * Kerberos bulguları; login başarısız olmadıysa (başarılı ya da denenmedi, ör. bilet önbelleği) true: Kafka adımı
	 * yine de denenir. ticketCache: sasl.jaas.config useTicketCache=true (kinit sidecar; keytab gerekmez).
	 */
	@SuppressWarnings("unchecked")
	private static boolean kerberosFindings(Diagnosis dx, List<Map<String, Object>> steps, Map<String, Object> facts,
			Map<String, Object> k, boolean ticketCache) {
		if (k == null) {
			steps.add(Results.map("step", "kerberos", "status", "timeout", "error", "Kerberos kontrolü süre sınırında bitmedi"));
			dx.critical("KERBEROS_TIMEOUT", KafkaErrors.kerberos("cannot locate kdc") + " (kontrol süre sınırında bitmedi)",
					null, "krb5.conf'taki kdc adreslerine (port 88) bu pod'dan erişimi kontrol edin; kerberos_self_check");
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
		boolean keytabMissing = false;
		for (String p : problems) {
			if (ticketCache && (p.startsWith("keytab tanımlı değil") || p.startsWith("principal tanımlı değil"))) {
				dx.info("KERBEROS_TICKET_CACHE",
						p + ": sasl.jaas.config useTicketCache=true, bilet önbelleği (kinit) kullanılıyor; login önceden "
								+ "denenmedi, Kafka adımı sonucuna bakın",
						null, "Kafka adımı Kerberos hatası verirse bilet önbelleğini (klist, KRB5CCNAME) ve kinit sidecar'ını "
								+ "kontrol edin");
			}
			else if (p.startsWith("keytab dosyası yok")) {
				keytabMissing = true;
				dx.critical("KEYTAB_NOT_FOUND", "keytab bulunamadı: " + keytab.get("path"), evidence("problem", p),
						"kerberos.keytab yolunu ve keytab Secret'ının pod'a mount edildiğini kontrol edin");
			}
			else if (p.startsWith("keytab okunamıyor") || p.startsWith("keytab boş")) {
				keytabMissing = true;
				dx.critical("KEYTAB_UNREADABLE", p, null, "Dosya izinlerini (pod kullanıcısı) ve Secret içeriğini kontrol edin");
			}
			else if (p.startsWith("keytab'da")) {
				dx.critical("KEYTAB_PRINCIPAL_MISSING", p, null,
						"kerberos.principal'ı keytab'daki adla (klist -kt) birebir eşleyin (realm büyük harf)");
			}
			else if (p.startsWith("krb5.conf")) {
				dx.critical("KRB5_CONF_PROBLEM", p, null,
						"krb5.conf'u ConfigMap olarak mount edin ve kerberos.krb5Conf ile yolunu verin");
			}
			else if (p.startsWith("Kerberos login başarısız")) {
				String err = String.valueOf(login.get("error"));
				if (keytabMissing && err.toLowerCase().contains("keytab")) {
					continue;
				}
				dx.critical("KERBEROS_LOGIN_FAILED", p, evidence("detail", login.get("detail")), loginHint(err));
			}
			else if (p.contains("realm")) {
				dx.warning("KERBEROS_REALM", p, null, "Principal realm'ini krb5.conf default_realm ile eşleyin");
			}
			else {
				dx.critical("KERBEROS_PROBLEM", p, null, "kerberos_self_check ile ayrıntılı kontrol edin");
			}
		}
		return !loginFailed;
	}

	private static String loginHint(String error) {
		String e = error == null ? "" : error;
		if (e.contains("KDC")) {
			return "krb5.conf'taki kdc adreslerine (TCP/UDP 88) bu pod'dan erişimi, DNS'i ve egress/NetworkPolicy'yi kontrol edin";
		}
		if (e.contains("clock skew")) {
			return "Pod ve KDC saatlerini eşitleyin (NTP); fark 5 dakikadan az olmalı";
		}
		if (e.contains("keytab")) {
			return "Keytab'ı yenileyin (parola/kvno değişmiş olabilir) ve principal adını kontrol edin";
		}
		return "kerberos_self_check ile ayrıntılı kontrol edin";
	}

	private static KafkaStep skipped(String reason) {
		return new KafkaStep(Results.map("step", "kafka", "status", "skipped", "reason", reason), null, null, null,
				null);
	}

	/** Önbellekteki AdminClient yerine yeni, kısa ömürlü bir AdminClient ile describeCluster (sonra kapatılır). */
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
					r[0], "brokers", nodes.size(), "mode", r[2], "client", "yeni AdminClient (önbellekteki değil)"), nodes,
					(String) r[0], (String) r[2], null);
		}
		catch (TimeoutException e) {
			task.cancel(true);
			return new KafkaStep(Results.map("step", "kafka", "status", "timeout", "durationMs", elapsed(start), "error",
					"describeCluster " + timeout + " ms içinde yanıtlanmadı"), null, null, null,
					new org.apache.kafka.common.errors.TimeoutException("describeCluster zaman aşımı"));
		}
		catch (ExecutionException e) {
			Throwable cause = KafkaErrors.unwrap(e.getCause());
			return new KafkaStep(Results.map("step", "kafka", "status", "error", "durationMs", elapsed(start), "error",
					KafkaErrors.explain(cause)), null, null, null, cause);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Araç iptal edildi");
		}
	}

	/**
	 * describeCluster zaman aşımına uğradığında broker'ların advertised adreslerini SADECE bootstrap bağlantısı
	 * üzerinden öğrenir: yeni, kısa ömürlü assign-only consumer'ın ilk MetadataRequest'i bootstrap adresine gider;
	 * dönen partition lider/replika Node'ları advertised host:port'lardır (sonra kapatılır).
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
		String note = "describeCluster zaman aşımı: advertised adresler sadece bootstrap bağlantısıyla alınan metadata'dan";
		try {
			List<Node> nodes = task.get(Math.max(1, Math.min(call.waitMillis() - 3_000, timeout + 1_500)),
					TimeUnit.MILLISECONDS);
			if (nodes.isEmpty()) {
				return new KafkaStep(Results.map("step", "bootstrapMetadata", "status", "error", "durationMs",
						elapsed(start), "error", "Metadata'da partition yok; advertised adresler belirlenemedi"), null, null,
						null, null);
			}
			return new KafkaStep(Results.map("step", "bootstrapMetadata", "status", "ok", "durationMs", elapsed(start),
					"brokers", nodes.size(), "note", note), nodes, null, null, null);
		}
		catch (TimeoutException e) {
			task.cancel(true);
			return new KafkaStep(Results.map("step", "bootstrapMetadata", "status", "timeout", "durationMs",
					elapsed(start), "error", "bootstrap metadata isteği " + timeout + " ms içinde yanıtlanmadı"), null, null,
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
			throw new ToolException("Araç iptal edildi");
		}
	}

	private static void addNode(Map<Integer, Node> nodes, Node n) {
		if (n != null && !n.isEmpty() && n.id() >= 0 && n.host() != null && !n.host().isBlank()) {
			nodes.putIfAbsent(n.id(), n);
		}
	}

	/** KRaft (metadata quorum yanıt verdi) ya da ZooKeeper (API desteklenmiyor); belirlenemezse null. */
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
					serviceMissing ? "bootstrapServers'ta broker'ların FQDN adlarını kullanın (IP/kısa ad değil) ve "
							+ "krb5.conf [libdefaults] rdns = false ayarlayın; servis principal'ı kafka/<fqdn>@REALM olmalı"
							: loginHint(kerberos));
		}
		else if (Acls.causedBy(e, SaslAuthenticationException.class)) {
			dx.critical("SASL_AUTH_FAILED", "SASL kimlik doğrulaması başarısız", evidence("error", chain),
					"Kullanıcı/parola (sasl.jaas.config), sasl.mechanism (PLAIN/SCRAM-SHA-512/GSSAPI) ve broker'da "
							+ "kullanıcının tanımlı olduğunu kontrol edin");
		}
		else if (Acls.causedBy(e, SslAuthenticationException.class)) {
			dx.critical("SSL_HANDSHAKE_FAILED", "TLS el sıkışması başarısız", evidence("error", chain),
					"Truststore'da broker CA sertifikası var mı, hostname doğrulaması (ssl.endpoint.identification.algorithm) uyumlu mu?");
		}
		else if (Acls.causedBy(e, org.apache.kafka.common.errors.TimeoutException.class)) {
			dx.critical("KAFKA_TIMEOUT",
					anyTcp ? "TCP bağlantısı açık ama Kafka isteği zamanında yanıtlanmadı"
							: "Kafka isteği zamanında yanıtlanmadı",
					evidence("error", chain),
					"security.protocol/sasl.mechanism broker listener'ıyla uyumlu mu (ör. SASL portuna PLAINTEXT, SSL "
							+ "portuna düz bağlantı)? Broker'ların advertised.listeners adresleri bu pod'dan erişilebilir mi?");
		}
		else {
			dx.critical("KAFKA_ERROR", "Kafka isteği başarısız: " + chain, null,
					"Hata mesajına göre ağ/güvenlik ayarlarını kontrol edin");
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
						probe == null ? "zaman aşımı" : probe.error()));
			}
		}
		facts.put("brokers", brokers);
		if (!unreachable.isEmpty()) {
			Set<String> bootstrapHosts = new LinkedHashSet<>();
			bootstrap.forEach(b -> bootstrapHosts.add(b.endpoint().host()));
			dx.critical("ADVERTISED_UNREACHABLE",
					unreachable.size() + "/" + nodes.size()
							+ " broker'ın advertised adresine bu pod'dan erişilemiyor: bootstrap çalışsa da istemciler bu "
							+ "broker'larla konuşamaz",
					evidence("brokers", unreachable, "bootstrapHosts", List.copyOf(bootstrapHosts)),
					"Broker'ın advertised.listeners değeri istemcilerin çözüp erişebildiği bir ad olmalı (OpenShift: "
							+ "dış erişim için Route/LoadBalancer adresi, iç erişim için Service adı; iç/dış listener'ları ayırın)");
		}
	}

	interface Extract {

		Map<String, Object> apply(JsonNode n);

	}

	/** REST çağrısını görev içinde süre ölçerek çalıştırır (hata da sonuç olarak döner). */
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
					what + " " + REST_TIMEOUT_MS + " ms içinde yanıt vermedi"));
			dx.warning(code, what + " " + REST_TIMEOUT_MS + " ms içinde yanıt vermedi: " + url, null,
					"URL'yi, servisin ayakta olduğunu ve bu pod'dan erişimi (NetworkPolicy/egress) kontrol edin");
			return;
		}
		catch (ExecutionException e) {
			r = new RestResult(null, e.getCause(), -1);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Araç iptal edildi");
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
		dx.warning(code, what + " erişilemedi: " + detail, evidence("error", err),
				err.contains("401") || err.contains("403")
						? "Kimlik bilgisi/yetki gerekiyor: URL'de kullanıcı bilgisi ya da proxy ayarlarını kontrol edin"
						: "URL'yi, servisin ayakta olduğunu ve bu pod'dan erişimi (NetworkPolicy/egress) kontrol edin");
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
			throw new ToolException("Araç iptal edildi");
		}
	}

	private static long elapsed(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

}
