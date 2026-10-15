package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.support.TestConfigs;

/**
 * diagnose_access: eksik ACL'ler acls_create biçiminde önerilir; tam olarak önerilenler oluşturulunca araç izin
 * verildiğini söyler ve alice gerçek producer/consumer ile yazıp okuyabilir (KRaft ve ZooKeeper).
 */
class DiagnoseAccessTest extends DiagnoseTestBase {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static Map<String, Object> binding(String type, String name, String op) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("principal", "User:alice");
		m.put("resourceType", type);
		m.put("resourceName", name);
		m.put("patternType", "LITERAL");
		m.put("operation", op);
		m.put("permission", "ALLOW");
		m.put("host", "*");
		return m;
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> missing(JsonNode r) {
		List<Map<String, Object>> out = new ArrayList<>();
		r.get("missingAcls").forEach(n -> out.add(MAPPER.convertValue(n, Map.class)));
		return out;
	}

	private static AclBinding toBinding(Map<String, Object> m) {
		return new AclBinding(
				new ResourcePattern(ResourceType.valueOf((String) m.get("resourceType")), (String) m.get("resourceName"),
						PatternType.valueOf((String) m.get("patternType"))),
				new AccessControlEntry((String) m.get("principal"), (String) m.get("host"),
						AclOperation.valueOf((String) m.get("operation")),
						AclPermissionType.valueOf((String) m.get("permission"))));
	}

	/** Önerilen her binding'i olduğu gibi acls_create'e verir (parametre biçimi birebir aynı). */
	private void createSuggested(String cluster, List<Map<String, Object>> suggested, List<AclBinding> created) {
		for (Map<String, Object> b : suggested) {
			Map<String, Object> a = new LinkedHashMap<>(b);
			a.put("cluster", cluster);
			JsonNode r = ok("acls_create", a);
			assertThat(r.get("created").asInt()).isEqualTo(1);
			created.add(toBinding(b));
		}
	}

	@Test
	void aliceGetsExactlyTheSuggestedAclsAndThenProducesAndConsumesOnBothClusters() throws Exception {
		for (String cluster : CLUSTERS) {
			String topic = unique("diag-access");
			String group = topic + "-g";
			createTopic(cluster, topic, 1, Map.of());
			List<AclBinding> created = new ArrayList<>();
			try {
				// produce: TOPIC WRITE + DESCRIBE eksik
				JsonNode r = ok("diagnose_access",
						on(cluster, "principal", "alice", "intent", "produce", "topic", topic));
				assertThat(r.get("allowed").asBoolean()).isFalse();
				assertThat(r.get("status").asText()).isEqualTo("critical");
				assertThat(r.get("principal").asText()).isEqualTo("User:alice");
				assertThat(r.get("mode").asText()).isEqualTo(zk(cluster) ? "ZooKeeper" : "KRaft");
				assertThat(missing(r)).containsExactly(binding("TOPIC", topic, "WRITE"),
						binding("TOPIC", topic, "DESCRIBE"));
				assertThat(findings(r, "MISSING_PERMISSION")).hasSize(2);
				assertThat(r.at("/preset/tool").asText()).isEqualTo("acls_create");
				assertThat(r.at("/preset/args/preset").asText()).isEqualTo("producer");
				assertThat(r.at("/preset/args/topic").asText()).isEqualTo(topic);
				assertThat(r.get("explanation").asText()).contains("yapamaz", "WRITE");
				assertThat(r.at("/facts/idempotentWrite").asText()).contains("gerekmez");
				assertThat(r.at("/facts/topicExists").asBoolean()).isTrue();
				assertThat(r.toString()).doesNotContain("Jikkou", "apiVersion");

				createSuggested(cluster, missing(r), created);
				JsonNode allowed = ok("diagnose_access",
						on(cluster, "principal", "alice", "intent", "produce", "topic", topic));
				assertThat(allowed.get("allowed").asBoolean()).as(allowed.toString()).isTrue();
				assertThat(allowed.get("missingAcls")).isEmpty();
				assertThat(codes(allowed)).doesNotContain("MISSING_PERMISSION", "DENIED_BY_ACL");
				assertThat(allowed.get("status").asText()).isNotEqualTo("critical");
				assertThat(allowed.get("explanation").asText()).contains("yapabilir");
				produceAs(cluster, "alice", topic);

				// consume: TOPIC DESCRIBE produce adımında verildi; TOPIC READ + GROUP READ eksik
				JsonNode c = ok("diagnose_access",
						on(cluster, "principal", "alice", "intent", "consume", "topic", topic, "group", group));
				assertThat(c.get("allowed").asBoolean()).isFalse();
				assertThat(missing(c)).containsExactly(binding("TOPIC", topic, "READ"), binding("GROUP", group, "READ"));
				JsonNode describe = null;
				for (JsonNode check : c.get("checks")) {
					if ("DESCRIBE".equals(check.get("operation").asText())) {
						describe = check;
					}
				}
				assertThat(describe.get("allowed").asBoolean()).isTrue();
				assertThat(describe.get("matchedAllow").findValuesAsText("operation")).contains("DESCRIBE", "WRITE");
				assertThat(c.at("/preset/args/preset").asText()).isEqualTo("consumer");
				assertThat(c.at("/preset/args/group").asText()).isEqualTo(group);

				createSuggested(cluster, missing(c), created);
				JsonNode c2 = ok("diagnose_access",
						on(cluster, "principal", "alice", "intent", "consume", "topic", topic, "group", group));
				assertThat(c2.get("allowed").asBoolean()).as(c2.toString()).isTrue();
				assertThat(c2.get("missingAcls")).isEmpty();
				consumeAs(cluster, "alice", topic, group);
			}
			finally {
				deleteAcls(cluster, created);
			}
		}
	}

	@Test
	void denyBindingBlocksAndIsReportedSeparately() throws Exception {
		String cluster = TestConfigs.CLUSTER;
		String topic = unique("diag-deny");
		String group = topic + "-g";
		createTopic(cluster, topic, 1, Map.of());
		List<AclBinding> acls = List.of(
				new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
						new AccessControlEntry("User:bob", "*", AclOperation.READ, AclPermissionType.ALLOW)),
				new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
						new AccessControlEntry("User:bob", "*", AclOperation.READ, AclPermissionType.DENY)));
		testAdmin().createAcls(acls).all().get(30, TimeUnit.SECONDS);
		try {
			waitVisible(cluster, acls);
			JsonNode r = ok("diagnose_access",
					on(cluster, "principal", "User:bob", "intent", "consume", "topic", topic, "group", group));
			assertThat(r.get("allowed").asBoolean()).isFalse();
			JsonNode deny = finding(r, "DENIED_BY_ACL");
			assertThat(deny.get("recommendation").asText()).contains("acls_delete");
			assertThat(deny.at("/evidence/deny/0/permission").asText()).isEqualTo("DENY");
			assertThat(r.at("/blockingDenies/0/operation").asText()).isEqualTo("READ");
			// READ için ALLOW zaten var: eklenecek tek binding GROUP READ; DESCRIBE, READ ALLOW'undan çıkarılır
			assertThat(r.get("missingAcls")).hasSize(1);
			assertThat(r.at("/missingAcls/0/resourceType").asText()).isEqualTo("GROUP");
			assertThat(r.at("/missingAcls/0/principal").asText()).isEqualTo("User:bob");
		}
		finally {
			testAdmin().deleteAcls(acls.stream().map(AclBinding::toFilter).toList()).all().get(30, TimeUnit.SECONDS);
		}
	}

	@Test
	void transactionalAndAdminTopicIntents() throws Exception {
		String cluster = TestConfigs.CLUSTER;
		String topic = unique("diag-txn");
		String txId = topic + "-tx";
		JsonNode tx = ok("diagnose_access",
				on(cluster, "principal", "alice", "intent", "transactional-produce", "topic", topic, "transactionalId", txId));
		assertThat(missing(tx)).containsExactly(binding("TOPIC", topic, "WRITE"), binding("TOPIC", topic, "DESCRIBE"),
				binding("TRANSACTIONAL_ID", txId, "WRITE"), binding("TRANSACTIONAL_ID", txId, "DESCRIBE"));
		assertThat(tx.at("/preset/args/transactionalId").asText()).isEqualTo(txId);
		assertThat(finding(tx, "TOPIC_NOT_FOUND").get("severity").asText()).isEqualTo("warning");

		JsonNode admin = ok("diagnose_access", on(cluster, "principal", "alice", "intent", "admin-topic", "topic", topic));
		assertThat(missing(admin)).extracting(m -> m.get("operation"))
			.containsExactly("CREATE", "ALTER", "DELETE", "DESCRIBE_CONFIGS", "ALTER_CONFIGS");
		assertThat(admin.at("/preset/note").asText()).contains("preset yok");
		assertThat(finding(admin, "TOPIC_NOT_FOUND").get("severity").asText()).isEqualTo("info");

		AclBinding clusterCreate = new AclBinding(
				new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL),
				new AccessControlEntry("User:alice", "*", AclOperation.CREATE, AclPermissionType.ALLOW));
		testAdmin().createAcls(List.of(clusterCreate)).all().get(30, TimeUnit.SECONDS);
		try {
			waitVisible(cluster, List.of(clusterCreate));
			JsonNode again = ok("diagnose_access",
					on(cluster, "principal", "alice", "intent", "admin-topic", "topic", topic));
			assertThat(missing(again)).extracting(m -> m.get("operation"))
				.containsExactly("ALTER", "DELETE", "DESCRIBE_CONFIGS", "ALTER_CONFIGS");
			assertThat(again.at("/checks/0/allowed").asBoolean()).isTrue();
			assertThat(again.at("/checks/0/via").asText()).contains("CLUSTER CREATE");
		}
		finally {
			testAdmin().deleteAcls(List.of(clusterCreate.toFilter())).all().get(30, TimeUnit.SECONDS);
		}
	}

	@Test
	void hiddenSuperUsersAndParameterValidation() {
		JsonNode r = ok("diagnose_access", zkArgs("principal", "nobody", "intent", "produce", "topic", unique("x")));
		assertThat(r.get("allowed").asBoolean()).isFalse();
		if (!r.at("/facts/superUsersKnown").asBoolean()) {
			assertThat(finding(r, "SUPER_USERS_HIDDEN").get("message").asText()).contains("super.users");
		}
		assertThat(error("diagnose_access", args("principal", "alice", "intent", "consume", "topic", "t")))
			.contains("group zorunlu");
		assertThat(error("diagnose_access",
				args("principal", "alice", "intent", "transactional-produce", "topic", "t"))).contains("transactionalId");
		assertThat(error("diagnose_access", args("principal", "alice", "intent", "nope", "topic", "t")))
			.contains("Geçersiz intent");
	}

	private static void waitVisible(String cluster, List<AclBinding> acls) {
		waitUntil(() -> {
			try {
				return admin(cluster).describeAcls(AclBindingFilter.ANY).values().get(30, TimeUnit.SECONDS)
					.containsAll(acls);
			}
			catch (Exception e) {
				return false;
			}
		}, "ACL'ler görünmedi");
	}

	private static void deleteAcls(String cluster, List<AclBinding> acls) throws Exception {
		if (!acls.isEmpty()) {
			admin(cluster).deleteAcls(acls.stream().map(AclBinding::toFilter).toList()).all().get(30, TimeUnit.SECONDS);
		}
	}

	/** alice olarak gerçek producer ile yazar (ACL yayılımı için kısa yeniden deneme). */
	private static void produceAs(String cluster, String user, String topic) throws Exception {
		Map<String, Object> p = clientProperties(cluster, user);
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
		Exception last = null;
		for (int i = 0; i < 6; i++) {
			try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p)) {
				producer.send(new ProducerRecord<>(topic, "from-alice".getBytes())).get(15, TimeUnit.SECONDS);
				return;
			}
			catch (Exception e) {
				last = e;
				Thread.sleep(1_000);
			}
		}
		throw new AssertionError(user + " yazamadı", last);
	}

	/** alice olarak gerçek consumer (group) ile okur ve commit eder. */
	private static void consumeAs(String cluster, String user, String topic, String group) {
		Map<String, Object> p = clientProperties(cluster, user);
		p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
			consumer.subscribe(List.of(topic));
			long deadline = System.currentTimeMillis() + 60_000;
			int read = 0;
			while (read == 0 && System.currentTimeMillis() < deadline) {
				ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
				read += records.count();
			}
			assertThat(read).as(user + " okuyamadı").isPositive();
			consumer.commitSync(Duration.ofSeconds(30));
		}
	}

}
