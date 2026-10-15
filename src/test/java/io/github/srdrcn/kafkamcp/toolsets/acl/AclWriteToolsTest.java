package io.github.srdrcn.kafkamcp.toolsets.acl;

import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.aclsOf;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.allowTopic;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.create;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.createTopic;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.deleteAllFor;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.denyTopic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.support.TestConfigs;

/** acls_create (single binding, presets, validation), acls_delete and readOnly rejection. */
class AclWriteToolsTest extends KafkaIntegrationTest {

	@Test
	void singleBindingIsCreated() throws Exception {
		String p = "User:" + unique("single");
		String topic = unique("single-topic");
		JsonNode r = ok("acls_create", args("principal", p, "resourceType", "TOPIC", "resourceName", topic, "operation",
				"WRITE", "permission", "DENY", "host", "10.0.0.9"));
		assertThat(r.get("created").asInt()).isEqualTo(1);
		assertThat(r.at("/bindings/0/permission").asText()).isEqualTo("DENY");
		Collection<AclBinding> acls = aclsOf(testAdmin(), p);
		assertThat(acls).containsExactly(AclTestSupport.binding(ResourceType.TOPIC, topic, PatternType.LITERAL, p,
				"10.0.0.9", AclOperation.WRITE, AclPermissionType.DENY));

		// bare name → User:, CLUSTER name defaults to kafka-cluster
		String bare = unique("bare");
		JsonNode cluster = ok("acls_create", args("principal", bare, "resourceType", "CLUSTER", "operation", "DESCRIBE"));
		assertThat(cluster.at("/bindings/0/principal").asText()).isEqualTo("User:" + bare);
		assertThat(cluster.at("/bindings/0/resourceName").asText()).isEqualTo("kafka-cluster");
		deleteAllFor(testAdmin(), "User:" + bare);
	}

	@Test
	void presetsCreateExpectedBindings() throws Exception {
		String p = "User:" + unique("preset");
		String topic = unique("preset-in");
		JsonNode consumer = ok("acls_create",
				args("principal", p, "preset", "consumer", "topic", topic, "group", topic + "-g"));
		assertThat(consumer.get("created").asInt()).isEqualTo(3);
		assertThat(consumer.get("bindings").findValuesAsText("operation")).containsExactlyInAnyOrder("READ", "DESCRIBE",
				"READ");
		assertThat(consumer.get("bindings").findValuesAsText("resourceType")).contains("GROUP");

		JsonNode producer = ok("acls_create",
				args("principal", p, "preset", "producer", "topic", topic, "transactionalId", topic + "-tx"));
		assertThat(producer.get("created").asInt()).isEqualTo(4);
		assertThat(producer.get("bindings").findValuesAsText("resourceType")).containsOnly("TOPIC", "TRANSACTIONAL_ID");

		String app = unique("app");
		JsonNode streams = ok("acls_create", args("principal", p, "preset", "streams", "topic", topic, "applicationId",
				app, "outputTopic", topic + "-out"));
		assertThat(streams.get("created").asInt()).isEqualTo(7);
		Collection<AclBinding> acls = aclsOf(testAdmin(), p);
		assertThat(acls).contains(
				AclTestSupport.binding(ResourceType.TRANSACTIONAL_ID, app, PatternType.PREFIXED, p, AclOperation.ALL,
						AclPermissionType.ALLOW),
				AclTestSupport.binding(ResourceType.GROUP, app, PatternType.PREFIXED, p, AclOperation.READ,
						AclPermissionType.ALLOW),
				AclTestSupport.binding(ResourceType.TOPIC, app, PatternType.PREFIXED, p, AclOperation.ALL,
						AclPermissionType.ALLOW),
				AclTestSupport.binding(ResourceType.TOPIC, topic + "-out", PatternType.LITERAL, p, AclOperation.WRITE,
						AclPermissionType.ALLOW));
		// 3 + 4 + 7 bindings; duplicate READ/DESCRIBE/DESCRIBE on the same topic count once
		assertThat(acls).hasSize(11);
	}

	@Test
	void parameterCombinationsAreValidated() {
		String p = "User:" + unique("bad");
		assertThat(error("acls_create", args("principal", p, "preset", "producer", "topic", "t", "resourceType", "TOPIC")))
			.contains("cannot be used with preset");
		assertThat(error("acls_create", args("principal", p, "resourceType", "TOPIC", "resourceName", "t")))
			.contains("Missing parameter: operation");
		assertThat(error("acls_create", args("principal", p, "resourceType", "TOPIC", "operation", "READ")))
			.contains("resourceName");
		assertThat(error("acls_create", args("principal", p, "preset", "consumer", "topic", "t")))
			.contains("group is required");
		assertThat(error("acls_create", args("principal", p, "preset", "streams", "topic", "t")))
			.contains("applicationId is required");
		assertThat(error("acls_create", args("principal", p, "preset", "producer"))).contains("topic is required");
		assertThat(error("acls_create",
				args("principal", p, "resourceType", "TOPIC", "resourceName", "t", "operation", "READ", "group", "g")))
			.contains("can only be used with preset");
		assertThat(error("acls_create",
				args("principal", p, "preset", "consumer", "topic", "t", "group", "g", "transactionalId", "x")))
			.contains("preset=producer");
		assertThat(error("acls_create", args("principal", p, "preset", "producer", "topic", "t", "permission", "DENY")))
			.contains("only generate ALLOW");
		assertThat(error("acls_create",
				args("principal", p, "resourceType", "CLUSTER", "resourceName", "x", "operation", "ALTER")))
			.contains("kafka-cluster");
		assertThat(error("acls_create", args("principal", p, "resourceType", "ANY", "resourceName", "t", "operation",
				"READ"))).contains("Invalid resourceType");
		assertThat(error("acls_create", args("principal", p, "preset", "admin", "topic", "t"))).contains("Invalid preset");
	}

	@Test
	void producerPresetMatchesRealAuthorization() throws Exception {
		String topic = unique("alice-out");
		createTopic(testAdmin(), topic);
		JsonNode before = ok("acls_explain",
				args("principal", "User:alice", "operation", "WRITE", "resourceType", "TOPIC", "resourceName", topic));
		assertThat(before.get("decision").asText()).isEqualTo("DENY");
		assertThat(produceError("alice", topic)).isInstanceOf(TopicAuthorizationException.class);

		JsonNode created = ok("acls_create", args("principal", "alice", "preset", "producer", "topic", topic));
		assertThat(created.get("bindings").findValuesAsText("operation")).containsExactlyInAnyOrder("WRITE",
				"DESCRIBE");
		JsonNode after = ok("acls_explain",
				args("principal", "User:alice", "operation", "WRITE", "resourceType", "TOPIC", "resourceName", topic));
		assertThat(after.get("decision").asText()).isEqualTo("ALLOW");
		await().atMost(Duration.ofSeconds(30))
			.pollInterval(Duration.ofMillis(500))
			.untilAsserted(() -> assertThat(produceError("alice", topic)).isNull());
		// bob is still unauthorized: explain matches the real broker
		assertThat(ok("acls_explain",
				args("principal", "bob", "operation", "WRITE", "resourceType", "TOPIC", "resourceName", topic))
			.get("decision")
			.asText()).isEqualTo("DENY");
		assertThat(produceError("bob", topic)).isInstanceOf(TopicAuthorizationException.class);
	}

	/** Produces a single message as the user; null if there is no error, otherwise the root Kafka error. */
	private static Throwable produceError(String user, String topic) {
		Map<String, Object> props = KafkaTestCluster.clientProperties(user);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
		props.put(ProducerConfig.ACKS_CONFIG, "1");
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
		props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
		try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
			producer.send(new ProducerRecord<>(topic, "v".getBytes(StandardCharsets.UTF_8))).get(15, TimeUnit.SECONDS);
			return null;
		}
		catch (Exception e) {
			Throwable t = e;
			while (t.getCause() != null && !(t instanceof org.apache.kafka.common.KafkaException)) {
				t = t.getCause();
			}
			return t;
		}
	}

	@Test
	void deleteRemovesOnlyMatchingBindings() throws Exception {
		String p = "User:" + unique("del");
		String topic = unique("del-topic");
		String other = unique("del-other");
		create(testAdmin(), allowTopic(topic, p, AclOperation.READ), allowTopic(topic, p, AclOperation.WRITE),
				denyTopic(topic, p, AclOperation.READ), allowTopic(other, p, AclOperation.READ));

		JsonNode one = ok("acls_delete", args("principal", p, "resourceType", "TOPIC", "resourceName", topic,
				"operation", "READ", "permission", "ALLOW"));
		assertThat(one.get("deleted").asInt()).isEqualTo(1);
		assertThat(one.at("/bindings/0/operation").asText()).isEqualTo("READ");
		assertThat(one.at("/bindings/0/permission").asText()).isEqualTo("ALLOW");
		assertThat(one.at("/filter/host").asText()).isEqualTo("ANY");
		assertThat(aclsOf(testAdmin(), p)).hasSize(3);

		JsonNode rest = ok("acls_delete", args("principal", p.substring(5), "resourceType", "TOPIC", "resourceName",
				topic));
		assertThat(rest.get("deleted").asInt()).isEqualTo(2);
		assertThat(aclsOf(testAdmin(), p)).hasSize(1);

		JsonNode none = ok("acls_delete", args("principal", p, "resourceType", "TOPIC", "resourceName", topic));
		assertThat(none.get("deleted").asInt()).isZero();
		assertThat(none.get("note").asText()).contains("nothing was deleted");
		assertThat(aclsOf(testAdmin(), p)).hasSize(1);
	}

	/** MATCH would also delete the "*" and PREFIXED ACLs covering the resource: rejected; ANY only deletes this name. */
	@Test
	void deleteRefusesMatchAndAnyKeepsWildcardAndPrefixed() throws Exception {
		String p = "User:" + unique("delm");
		String topic = unique("delm-topic");
		AclBinding literal = allowTopic(topic, p, AclOperation.READ);
		AclBinding prefixed = AclTestSupport.binding(ResourceType.TOPIC, topic.substring(0, topic.length() - 2),
				PatternType.PREFIXED, p, AclOperation.READ, AclPermissionType.ALLOW);
		AclBinding wildcard = AclTestSupport.binding(ResourceType.TOPIC, "*", PatternType.LITERAL, p,
				AclOperation.DESCRIBE, AclPermissionType.ALLOW);
		create(testAdmin(), literal, prefixed, wildcard);
		try {
			assertThat(error("acls_delete", args("principal", p, "resourceType", "TOPIC", "resourceName", topic,
					"patternType", "MATCH")))
				.contains("MATCH");
			assertThat(aclsOf(testAdmin(), p)).hasSize(3);

			JsonNode any = ok("acls_delete", args("principal", p, "resourceType", "TOPIC", "resourceName", topic,
					"patternType", "ANY"));
			assertThat(any.get("deleted").asInt()).isEqualTo(1);
			assertThat(any.at("/bindings/0/patternType").asText()).isEqualTo("LITERAL");
			assertThat(aclsOf(testAdmin(), p)).containsExactlyInAnyOrder(prefixed, wildcard);
		}
		finally {
			deleteAllFor(testAdmin(), p);
		}
	}

	@Test
	void deleteRefusesWideFilters() {
		assertThat(error("acls_delete", args("principal", "User:x", "resourceType", "TOPIC"))).contains("resourceName");
		assertThat(error("acls_delete", args("principal", "User:x", "resourceType", "ANY", "resourceName", "t")))
			.contains("broad delete");
		assertThat(error("acls_delete", args("resourceType", "TOPIC", "resourceName", "t"))).contains("principal");
		assertThat(error("acls_delete", args("principal", "User:x", "resourceName", "t"))).contains("resourceType");
	}

	@Test
	void writeToolsAreRejectedOnReadOnlyCluster() {
		assertThat(error("acls_create", args("cluster", TestConfigs.READ_ONLY_CLUSTER, "principal", "User:x",
				"resourceType", "TOPIC", "resourceName", "t", "operation", "READ")))
			.contains("readOnly");
		assertThat(error("acls_delete", args("cluster", TestConfigs.READ_ONLY_CLUSTER, "principal", "User:x",
				"resourceType", "TOPIC", "resourceName", "t")))
			.contains("readOnly");
		// read tools work on the readOnly cluster
		assertThat(ok("acls_list", args("cluster", TestConfigs.READ_ONLY_CLUSTER, "limit", 1)).has("total")).isTrue();
	}

	@Test
	void principalValidation() {
		assertThat(error("acls_create", args("principal", "User:", "resourceType", "TOPIC", "resourceName", "t",
				"operation", "READ"))).contains("Invalid principal");
		assertThat(List.of(error("acls_create", args("resourceType", "TOPIC")))).anyMatch(e -> e.contains("principal"));
	}

}
