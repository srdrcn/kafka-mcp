package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;

class BrokerWriteToolsTest extends KafkaIntegrationTest {

	private static final String KEY = "log.cleaner.backoff.ms";

	@Test
	void brokersConfigUpdateSetsAndDeletesPerBrokerAndClusterDefault() throws Exception {
		ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "1");
		JsonNode set = ok("brokers_config_update", args("brokerId", 1, "set", Map.of(KEY, "16000")));
		assertThat(set.get("scope").asText()).isEqualTo("broker");
		assertThat(set.get("brokerId").asInt()).isEqualTo(1);
		JsonNode change = set.get("changes").get(0);
		assertThat(change.get("name").asText()).isEqualTo(KEY);
		assertThat(change.get("op").asText()).isEqualTo("set");
		assertThat(change.get("before").asText()).isEqualTo("15000");
		assertThat(change.get("after").asText()).isEqualTo("16000");
		assertThat(change.get("afterSource").asText()).isEqualTo("DYNAMIC_BROKER_CONFIG");
		ConfigEntry e = describe(broker).get(KEY);
		assertThat(e.value()).isEqualTo("16000");
		assertThat(e.source()).isEqualTo(ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);

		JsonNode del = ok("brokers_config_update", args("brokerId", 1, "delete", List.of(KEY)));
		assertThat(del.at("/changes/0/op").asText()).isEqualTo("delete");
		assertThat(del.at("/changes/0/before").asText()).isEqualTo("16000");
		assertThat(del.at("/changes/0/after").asText()).isEqualTo("15000");
		assertThat(describe(broker).get(KEY).source()).isNotEqualTo(ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);

		ConfigResource clusterDefault = new ConfigResource(ConfigResource.Type.BROKER, "");
		JsonNode def = ok("brokers_config_update", args("set", Map.of(KEY, "17000")));
		assertThat(def.get("scope").asText()).isEqualTo("cluster-default");
		assertThat(def.has("brokerId")).isFalse();
		assertThat(def.at("/changes/0/after").asText()).isEqualTo("17000");
		assertThat(def.at("/changes/0/afterSource").asText()).isEqualTo("DYNAMIC_DEFAULT_BROKER_CONFIG");
		assertThat(describe(clusterDefault).get(KEY).value()).isEqualTo("17000");

		JsonNode defDel = ok("brokers_config_update", args("delete", List.of(KEY)));
		assertThat(defDel.at("/changes/0/before").asText()).isEqualTo("17000");
		assertThat(defDel.at("/changes/0").has("after")).isFalse();
		assertThat(describe(clusterDefault).get(KEY)).isNull();
	}

	@Test
	void brokersConfigUpdateValidatesInputAndRespectsReadOnly() {
		assertThat(error("brokers_config_update", args("brokerId", 1))).contains("set or delete");
		assertThat(error("brokers_config_update", args("set", Map.of(KEY, "1"), "delete", List.of(KEY))))
			.contains("both set and delete");
		assertThat(error("brokers_config_update", args("brokerId", 7, "set", Map.of(KEY, "1"))))
			.contains("Broker not found: 7");
		assertThat(error("brokers_config_update", args("brokerId", 1, "set", Map.of(KEY, "abc"))))
			.contains("Invalid request/config");
		assertThat(error("brokers_config_update", args("cluster", "test-ro", "set", Map.of(KEY, "16000"))))
			.contains("readOnly");
	}

	@Test
	void electPreferredLeadersReportsPerPartitionResult() throws Exception {
		String topic = unique("elect");
		TestData.createTopic(testAdmin(), topic, 2, Map.of());
		JsonNode r = ok("brokers_elect_preferred_leaders", args("topic", topic, "partitions", List.of(0, 1)));
		assertThat(r.get("requested").asInt()).isEqualTo(2);
		assertThat(r.get("failed").asInt()).isZero();
		assertThat(r.get("notNeeded").asInt() + r.get("elected").asInt()).isEqualTo(2);
		assertThat(r.get("results")).hasSize(2);
		assertThat(r.at("/results/0/topic").asText()).isEqualTo(topic);
		TopicDescription d = testAdmin().describeTopics(List.of(topic)).allTopicNames().get().get(topic);
		d.partitions().forEach(p -> assertThat(p.leader().id()).isEqualTo(p.replicas().get(0).id()));

		JsonNode whole = ok("brokers_elect_preferred_leaders", args("topic", topic));
		assertThat(whole.get("requested").asInt()).isEqualTo(2);

		JsonNode all = ok("brokers_elect_preferred_leaders", args());
		assertThat(all.get("requested").asInt()).isZero();
		assertThat(all.get("message").asText()).contains("preferred replica");

		assertThat(error("brokers_elect_preferred_leaders", args("partitions", List.of(0)))).contains("topic");
		assertThat(error("brokers_elect_preferred_leaders", args("topic", topic, "partitions", List.of(5))))
			.contains("Invalid partition 5");
		assertThat(error("brokers_elect_preferred_leaders", args("cluster", "test-ro", "topic", topic)))
			.contains("readOnly");
	}

	@Test
	void partitionReassignmentsAlterReassignsAndReportsCancelErrors() throws Exception {
		String topic = unique("reassign");
		TestData.createTopic(testAdmin(), topic, 1, Map.of());
		JsonNode r = ok("partition_reassignments_alter",
				args("reassignments", List.of(Map.of("topic", topic, "partition", 0, "replicas", List.of(1)))));
		assertThat(r.get("requested").asInt()).isEqualTo(1);
		assertThat(r.get("ok").asInt()).isEqualTo(1);
		JsonNode row = r.get("results").get(0);
		assertThat(row.get("action").asText()).isEqualTo("reassign");
		assertThat(row.get("result").asText()).isEqualTo("ok");
		assertThat(row.get("replicas").get(0).asInt()).isEqualTo(1);
		TopicDescription d = testAdmin().describeTopics(List.of(topic)).allTopicNames().get().get(topic);
		assertThat(d.partitions().get(0).replicas()).extracting(Node::id).containsExactly(1);

		// non-array replicas do not mean cancel: "1", "[1]" and 1 are parsed as a reassignment
		for (Object replicas : List.of("1", "[1]", 1)) {
			JsonNode parsed = ok("partition_reassignments_alter",
					args("reassignments", List.of(Map.of("topic", topic, "partition", 0, "replicas", replicas))));
			assertThat(parsed.at("/results/0/action").asText()).as("replicas=%s", replicas).isEqualTo("reassign");
			assertThat(parsed.at("/results/0/result").asText()).isEqualTo("ok");
			assertThat(parsed.at("/results/0/replicas/0").asInt()).isEqualTo(1);
		}
		assertThat(error("partition_reassignments_alter",
				args("reassignments", List.of(Map.of("topic", topic, "partition", 0, "replicas", "abc")))))
			.contains("Expected an integer");

		JsonNode cancel = ok("partition_reassignments_alter",
				args("reassignments", List.of(Map.of("topic", topic, "partition", 0))));
		assertThat(cancel.at("/results/0/action").asText()).isEqualTo("cancel");
		assertThat(cancel.at("/results/0/result").asText()).isEqualTo("error");
		assertThat(cancel.at("/results/0/error").asText()).containsIgnoringCase("reassignment");
		assertThat(cancel.get("failed").asInt()).isEqualTo(1);

		assertThat(error("partition_reassignments_alter",
				args("reassignments", List.of(Map.of("topic", topic, "partition", 0, "replicas", List.of(1, 99))))))
			.contains("Broker not found: 99");
		assertThat(error("partition_reassignments_alter",
				args("reassignments", List.of(Map.of("topic", topic, "partition", 3, "replicas", List.of(1))))))
			.contains("Invalid partition");
		assertThat(error("partition_reassignments_alter",
				args("reassignments", List.of(Map.of("topic", unique("missing"), "partition", 0)))))
			.contains("Topic not found");
		assertThat(error("partition_reassignments_alter", args("reassignments", List.of()))).contains("non-empty");
		assertThat(error("partition_reassignments_alter", args("cluster", "test-ro", "reassignments",
				List.of(Map.of("topic", topic, "partition", 0)))))
			.contains("readOnly");
		assertThat(testAdmin().listPartitionReassignments().reassignments().get().keySet()).doesNotContain(
				new TopicPartition(topic, 0));
	}

	private static org.apache.kafka.clients.admin.Config describe(ConfigResource r) throws Exception {
		return testAdmin().describeConfigs(List.of(r)).all().get().get(r);
	}

}
