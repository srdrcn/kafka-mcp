package io.github.srdrcn.kafkamcp.toolsets.acl;

import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.aclsOf;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.createTopic;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.deleteAllFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.support.MixedClusterIntegrationTest;
import io.github.srdrcn.kafkamcp.support.TestConfigs;
import io.github.srdrcn.kafkamcp.support.ZkKafkaTestCluster;

/**
 * Sürüme ve gizli broker ayarlarına bağlı ACL davranışı karışık ortamda: producer/streams preset'lerinde
 * CLUSTER IDEMPOTENT_WRITE (Kafka 2.8 öncesi ZooKeeper broker'ları, KIP-679), acls_explain'in
 * IDEMPOTENT_WRITE notu ve gizli allow.everyone.if.no.acl.found için allowEveryoneIfNoAclFound parametresi.
 * Eski broker, describeConfigs'i inter.broker.protocol.version=2.7-IV2 döndüren Admin proxy'siyle
 * canlandırılır; diğer her istek gerçek ZooKeeper (3.9) broker'ına gider.
 */
class AclVersionAwareMixedClusterTest extends MixedClusterIntegrationTest {

	private static final ObjectMapper JSON = new ObjectMapper();

	private static final ResourcePattern CLUSTER = new ResourcePattern(ResourceType.CLUSTER, Acls.CLUSTER_NAME,
			PatternType.LITERAL);

	@Autowired
	private KafkaClients clients;

	@Autowired
	private AclWriteTools writeTools;

	@Autowired
	private AclAccessTools accessTools;

	private static boolean isIdempotentWrite(AclBinding b) {
		return b.pattern().resourceType() == ResourceType.CLUSTER
				&& b.entry().operation() == AclOperation.IDEMPOTENT_WRITE;
	}

	/** ZooKeeper cluster'ına kullanıcı olarak idempotent producer ile tek mesaj: başarılıysa null, değilse kök hata. */
	private static Throwable zkIdempotentProduceError(String user, String topic) {
		Map<String, Object> o = new HashMap<>(KafkaTestCluster.saslProperties(user));
		o.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		o.put(ProducerConfig.ACKS_CONFIG, "all");
		o.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
		o.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
		o.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
		try (KafkaProducer<byte[], byte[]> producer = ZkKafkaTestCluster.producer(o)) {
			producer.send(new ProducerRecord<>(topic, "v".getBytes(StandardCharsets.UTF_8))).get(15, TimeUnit.SECONDS);
			return null;
		}
		catch (Exception e) {
			Throwable t = e;
			while (t.getCause() != null && !(t instanceof KafkaException)) {
				t = t.getCause();
			}
			return t;
		}
	}

	/**
	 * ZooKeeper 3.9 (IBP 3.9-IV0) ve KRaft: preset'e IDEMPOTENT_WRITE eklenmez; gerçek idempotent producer
	 * sadece topic WRITE/DESCRIBE ile yazabilir (KIP-679). acls_explain IDEMPOTENT_WRITE için DENY der ama
	 * bunun 2.8+ broker'da producer'ı engellemediğini not eder.
	 */
	@Test
	void modernBrokersNeedNoIdempotentWriteInPresets() throws Exception {
		String user = "alice";
		String p = "User:" + user;
		String topic = unique("mxidem");
		createTopic(zkAdmin(), topic);
		try {
			// hiçbir topic'te WRITE yokken InitProducerId (Cluster) ya da metadata (Topic) yetkisi reddedilir
			assertThat(zkIdempotentProduceError(user, topic)).isInstanceOfAny(ClusterAuthorizationException.class,
					TopicAuthorizationException.class);
			JsonNode producer = ok("acls_create", zkArgs("principal", user, "preset", "producer", "topic", topic));
			assertThat(producer.get("created").asInt()).isEqualTo(2);
			assertThat(producer.has("notes")).isFalse();
			assertThat(aclsOf(zkAdmin(), p)).noneMatch(AclVersionAwareMixedClusterTest::isIdempotentWrite);
			await().atMost(Duration.ofSeconds(30))
				.pollInterval(Duration.ofMillis(500))
				.untilAsserted(() -> assertThat(zkIdempotentProduceError(user, topic)).isNull());

			JsonNode explain = ok("acls_explain", zkArgs("principal", user, "operation", "IDEMPOTENT_WRITE",
					"resourceType", "CLUSTER", "resourceName", Acls.CLUSTER_NAME));
			assertThat(explain.get("decision").asText()).isEqualTo("DENY");
			assertThat(explain.get("notes").toString()).contains("KIP-679").contains("WRITE yetkisi yeterli");
		}
		finally {
			for (AclBinding b : aclsOf(zkAdmin(), p)) {
				if (b.pattern().name().equals(topic)) {
					AclTestSupport.delete(zkAdmin(), b);
				}
			}
			try {
				zkAdmin().deleteTopics(List.of(topic)).all().get(15, TimeUnit.SECONDS);
			}
			catch (Exception ignored) {
				// temizlik
			}
		}

		String kraftPrincipal = "User:" + unique("mxidemk");
		try {
			JsonNode streams = ok("acls_create", args("principal", kraftPrincipal, "preset", "streams", "topic",
					unique("mxin"), "applicationId", unique("mxapp")));
			assertThat(streams.get("created").asInt()).isEqualTo(5);
			assertThat(streams.has("notes")).isFalse();
			assertThat(aclsOf(testAdmin(), kraftPrincipal)).hasSize(5)
				.noneMatch(AclVersionAwareMixedClusterTest::isIdempotentWrite);
			JsonNode explain = ok("acls_explain", args("principal", kraftPrincipal, "operation", "IDEMPOTENT_WRITE",
					"resourceType", "CLUSTER", "resourceName", Acls.CLUSTER_NAME));
			assertThat(explain.get("notes").toString()).contains("KIP-679");
		}
		finally {
			deleteAllFor(testAdmin(), kraftPrincipal);
		}
	}

	/**
	 * Kafka 2.8 öncesi ZooKeeper broker'ı (IBP 2.7-IV2): idempotent producer CLUSTER IDEMPOTENT_WRITE ister.
	 * producer (transactionalId'siz) ve streams preset'leri bunu ekler; transactional producer eklemez.
	 */
	@Test
	void pre28ZooKeeperBrokerGetsIdempotentWrite() throws Exception {
		KafkaClients old = clientsWith(Map.of("inter.broker.protocol.version", "2.7-IV2"));
		String p = "User:" + unique("mxold");
		String topic = unique("mxoldt");
		try {
			JsonNode producer = run(writeTools::create, old,
					zkArgs("principal", p, "preset", "producer", "topic", topic));
			assertThat(producer.get("created").asInt()).isEqualTo(3);
			assertThat(producer.get("notes").toString()).contains("2.7-IV2").contains("IDEMPOTENT_WRITE");
			JsonNode cluster = AclTestSupport.find(producer.get("bindings"), "resourceType", "CLUSTER");
			assertThat(cluster.get("operation").asText()).isEqualTo("IDEMPOTENT_WRITE");
			assertThat(cluster.get("resourceName").asText()).isEqualTo(Acls.CLUSTER_NAME);
			assertThat(aclsOf(zkAdmin(), p)).contains(new AclBinding(CLUSTER,
					new AccessControlEntry(p, "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW)));

			// eski broker'da IDEMPOTENT_WRITE kararı gerçekten gerekli: KIP-679 notu yok
			JsonNode explain = run(accessTools::explain, old, zkArgs("principal", "bob", "operation",
					"IDEMPOTENT_WRITE", "resourceType", "CLUSTER", "resourceName", Acls.CLUSTER_NAME));
			assertThat(explain.get("decision").asText()).isEqualTo("DENY");
			assertThat(explain.path("notes").toString()).doesNotContain("KIP-679");

			JsonNode streams = run(writeTools::create, old, zkArgs("principal", p, "preset", "streams", "topic", topic,
					"applicationId", topic + "-app"));
			assertThat(streams.get("created").asInt()).isEqualTo(6);
			assertThat(streams.get("notes").toString()).contains("IDEMPOTENT_WRITE");

			String txPrincipal = p + "-tx";
			try {
				JsonNode tx = run(writeTools::create, old, zkArgs("principal", txPrincipal, "preset", "producer",
						"topic", topic, "transactionalId", topic + "-tx"));
				assertThat(tx.get("created").asInt()).isEqualTo(4);
				assertThat(tx.has("notes")).isFalse();
				assertThat(aclsOf(zkAdmin(), txPrincipal)).noneMatch(AclVersionAwareMixedClusterTest::isIdempotentWrite);
			}
			finally {
				deleteAllFor(zkAdmin(), txPrincipal);
			}

			// mod bilinmiyor (process.roles ve zookeeper.connect okunamadı): binding yok, not var
			KafkaClients unknown = clientsWith(Map.of("zookeeper.connect", "", "process.roles", ""));
			String q = p + "-unknown";
			try {
				JsonNode u = run(writeTools::create, unknown, zkArgs("principal", q, "preset", "producer", "topic", topic));
				assertThat(u.get("created").asInt()).isEqualTo(2);
				assertThat(u.get("notes").toString()).contains("okunamadı").contains("enable.idempotence=false");
			}
			finally {
				deleteAllFor(zkAdmin(), q);
			}
		}
		finally {
			deleteAllFor(zkAdmin(), p);
		}
	}

	/**
	 * İki mod da allow.everyone.if.no.acl.found'u gizli döndürür (her ikisinde de false): parametre verilmezse
	 * karar varsayım olarak ifade edilir; verilirse o değer kullanılır.
	 */
	@ParameterizedTest
	@ValueSource(strings = { ZK_CLUSTER, TestConfigs.CLUSTER })
	void allowEveryoneParameterWhenBrokerHidesIt(String cluster) throws Exception {
		boolean zk = ZK_CLUSTER.equals(cluster);
		Admin admin = zk ? zkAdmin() : testAdmin();
		String topic = unique("mxnoacl");
		createTopic(admin, topic);
		try {
			Map<String, Object> props = zk ? ZkKafkaTestCluster.clientProperties("bob")
					: KafkaTestCluster.clientProperties("bob");
			props.put("default.api.timeout.ms", 10_000);
			props.put("request.timeout.ms", 5_000);
			try (Admin bob = Admin.create(props)) {
				// gerçek broker: ACL'siz kaynakta reddeder (allow.everyone.if.no.acl.found=false)
				Throwable error = null;
				try {
					bob.describeTopics(List.of(topic)).allTopicNames().get(15, TimeUnit.SECONDS);
				}
				catch (Exception e) {
					error = e.getCause();
				}
				assertThat(error).isInstanceOf(TopicAuthorizationException.class);
			}

			JsonNode assumed = ok("acls_explain", argsOn(cluster, "principal", "bob", "operation", "READ",
					"resourceType", "TOPIC", "resourceName", topic));
			assertThat(assumed.get("decision").asText()).isEqualTo("DENY");
			assertThat(assumed.get("reason").asText()).contains("false varsayıldı").contains("true ise ALLOW");
			assertThat(assumed.get("allowEveryoneIfNoAclFound").asBoolean()).isFalse();
			assertThat(assumed.get("notes").toString()).contains("allowEveryoneIfNoAclFound parametresiyle");

			JsonNode open = ok("acls_explain", argsOn(cluster, "principal", "bob", "operation", "READ",
					"resourceType", "TOPIC", "resourceName", topic, "allowEveryoneIfNoAclFound", true));
			assertThat(open.get("decision").asText()).isEqualTo("ALLOW");
			assertThat(open.get("reason").asText()).contains("allow.everyone.if.no.acl.found=true");
			assertThat(open.get("allowEveryoneIfNoAclFound").asBoolean()).isTrue();
			assertThat(open.get("notes").toString()).contains("parametresindeki değer (true)");

			JsonNode closed = ok("acls_explain", argsOn(cluster, "principal", "bob", "operation", "READ",
					"resourceType", "TOPIC", "resourceName", topic, "allowEveryoneIfNoAclFound", false));
			assertThat(closed.get("decision").asText()).isEqualTo("DENY");
			assertThat(closed.get("reason").asText()).contains("allow.everyone.if.no.acl.found=false")
				.doesNotContain("varsayıldı");

			JsonNode who = ok("acls_who_can_access_topic", argsOn(cluster, "topic", topic));
			assertThat(who.get("aclCount").asInt()).isZero();
			assertThat(who.get("notes").toString()).contains("false varsayıldı")
				.contains("true ise herkes erişebilir")
				.doesNotContain("kimse erişemez (allow.everyone.if.no.acl.found=false)");
			JsonNode whoOpen = ok("acls_who_can_access_topic",
					argsOn(cluster, "topic", topic, "allowEveryoneIfNoAclFound", true));
			assertThat(whoOpen.get("allowEveryoneIfNoAclFound").asBoolean()).isTrue();
			assertThat(whoOpen.get("notes").toString()).contains("=true: herkes erişebilir")
				.doesNotContain("varsayıldı");
			JsonNode whoClosed = ok("acls_who_can_access_topic",
					argsOn(cluster, "topic", topic, "allowEveryoneIfNoAclFound", false));
			assertThat(whoClosed.get("notes").toString())
				.contains("kimse erişemez (allow.everyone.if.no.acl.found=false)")
				.doesNotContain("varsayıldı");
		}
		finally {
			try {
				admin.deleteTopics(List.of(topic)).all().get(15, TimeUnit.SECONDS);
			}
			catch (Exception ignored) {
				// temizlik
			}
		}
	}

	private static Map<String, Object> argsOn(String cluster, Object... kv) {
		Map<String, Object> m = args(kv);
		m.put("cluster", cluster);
		return m;
	}

	private static JsonNode run(java.util.function.Function<ToolCall, Object> tool, KafkaClients clients,
			Map<String, Object> args) {
		return JSON.valueToTree(tool.apply(new ToolCall("test", args, clients, Instant.now().plusSeconds(30))));
	}

	/** ZooKeeper cluster'ının broker config'lerinde verilen anahtarları değiştiren Admin ile KafkaClients. */
	private KafkaClients clientsWith(Map<String, String> overrides) {
		Admin proxied = brokerConfigOverride(zkAdmin(), overrides);
		return new KafkaClients(clients.config()) {
			@Override
			public Admin admin(String cluster) {
				return proxied;
			}
		};
	}

	private static Admin brokerConfigOverride(Admin real, Map<String, String> overrides) {
		return (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] { Admin.class },
				(proxy, method, methodArgs) -> {
					Object result;
					try {
						result = method.invoke(real, methodArgs);
					}
					catch (InvocationTargetException e) {
						throw e.getCause();
					}
					if (!"describeConfigs".equals(method.getName())) {
						return result;
					}
					// KafkaIntegrationTest.Config ile çakışmaması için tam ad
					Map<ConfigResource, KafkaFuture<org.apache.kafka.clients.admin.Config>> futures = new HashMap<>();
					((DescribeConfigsResult) result).all().get(15, TimeUnit.SECONDS).forEach((resource, config) -> {
						List<ConfigEntry> entries = new ArrayList<>();
						for (ConfigEntry e : config.entries()) {
							String name = e.name();
							entries.add(overrides.containsKey(name) ? new ConfigEntry(name, overrides.get(name)) : e);
						}
						futures.put(resource,
								KafkaFuture.completedFuture(new org.apache.kafka.clients.admin.Config(entries)));
					});
					Constructor<DescribeConfigsResult> c = DescribeConfigsResult.class.getDeclaredConstructor(Map.class);
					c.setAccessible(true);
					return c.newInstance(futures);
				});
	}

}
