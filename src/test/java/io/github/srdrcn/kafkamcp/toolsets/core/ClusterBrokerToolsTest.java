package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;

class ClusterBrokerToolsTest extends KafkaIntegrationTest {

	private static String topic;

	@BeforeAll
	static void setUp() throws Exception {
		topic = unique("cb");
		TestData.createTopic(testAdmin(), topic, 3, Map.of());
		TestData.produce(topic, 0, 20, null);
	}

	@Test
	void clustersGetSummarizesKraftCluster() {
		JsonNode r = ok("clusters_get", args());
		assertThat(r.get("cluster").asText()).isEqualTo("test");
		assertThat(r.get("clusterId").asText()).isNotBlank();
		assertThat(r.get("mode").asText()).isEqualTo("KRaft");
		assertThat(r.get("controllerId").asInt()).isEqualTo(1);
		assertThat(r.get("minKafkaVersion").asText()).isEqualTo("4.1");
		assertThat(r.get("versionSource").asText()).startsWith("metadata.version=").contains("4.1-IV");
		assertThat(r.get("brokerCount").asInt()).isEqualTo(1);
		assertThat(r.get("topicCount").asInt()).isPositive();
		assertThat(r.get("partitionCount").asInt()).isGreaterThanOrEqualTo(3);
		assertThat(r.get("underReplicatedPartitions").asInt()).isZero();
		assertThat(r.get("offlinePartitions").asInt()).isZero();
		assertThat(r.get("underMinIsrPartitions").asInt()).isZero();
		assertThat(r.at("/quorum/leaderId").asInt()).isEqualTo(1);
		assertThat(r.at("/quorum/voters/0/id").asInt()).isEqualTo(1);
		assertThat(r.at("/quorum/voters/0/lag").asLong()).isGreaterThanOrEqualTo(0);
	}

	@Test
	void brokersListCountsLeadersAndReplicas() {
		JsonNode r = ok("brokers_list", args());
		assertThat(r.get("controllerId").asInt()).isEqualTo(1);
		assertThat(r.get("total").asInt()).isEqualTo(1);
		JsonNode b = r.get("brokers").get(0);
		assertThat(b.get("id").asInt()).isEqualTo(1);
		assertThat(b.get("host").asText()).isEqualTo("localhost");
		assertThat(b.get("port").asInt()).isPositive();
		assertThat(b.get("isController").asBoolean()).isTrue();
		assertThat(b.get("leaders").asInt()).isGreaterThanOrEqualTo(3);
		assertThat(b.get("replicas").asInt()).isEqualTo(b.get("leaders").asInt());

		assertThat(ok("brokers_list", args("filter", "^yok$")).get("total").asInt()).isZero();
	}

	@Test
	void brokersConfigGetShowsNonDefaultsAndMasksSecrets() {
		JsonNode r = ok("brokers_config_get", args("limit", 1000));
		assertThat(r.get("brokerId").asInt()).isEqualTo(1);
		assertThat(r.get("includeDefaults").asBoolean()).isFalse();
		List<String> sources = new ArrayList<>();
		r.get("configs").forEach(c -> sources.add(c.get("source").asText()));
		assertThat(sources).isNotEmpty().doesNotContain("DEFAULT_CONFIG");
		JsonNode nodeId = find(r.get("configs"), "node.id");
		assertThat(nodeId.get("value").asText()).isEqualTo("1");
		assertThat(nodeId.get("source").asText()).isEqualTo("STATIC_BROKER_CONFIG");
		assertThat(nodeId.has("readOnly")).isTrue();
		JsonNode jaas = find(r.get("configs"), "listener.name.sasl.plain.sasl.jaas.config");
		assertThat(jaas.get("value").asText()).isEqualTo("******");

		JsonNode defaults = ok("brokers_config_get",
				args("brokerId", 1, "includeDefaults", true, "filter", "^num\\.io\\.threads$"));
		assertThat(defaults.get("total").asInt()).isEqualTo(1);
		assertThat(defaults.at("/configs/0/source").asText()).isEqualTo("DEFAULT_CONFIG");
		assertThat(defaults.at("/configs/0/value").asText()).isEqualTo("8");

		assertThat(error("brokers_config_get", args("brokerId", 99))).contains("Broker bulunamadı: 99");
	}

	@Test
	void brokersConfigDriftWithSingleBrokerHasNoDifferences() {
		JsonNode r = ok("brokers_config_drift", args());
		assertThat(r.get("brokersCompared")).hasSize(1);
		assertThat(r.get("total").asInt()).isZero();
		assertThat(r.get("differences")).isEmpty();
		assertThat(r.get("note").asText()).contains("en az 2 broker");
		List<String> excluded = new ArrayList<>();
		r.get("excludedKeys").forEach(k -> excluded.add(k.asText()));
		assertThat(excluded).contains("broker.id", "node.id", "advertised.listeners", "listeners", "broker.rack");
	}

	@Test
	void brokersLogdirsShowsDirsAndLargestPartitions() {
		JsonNode r = ok("brokers_logdirs", args("filter", "^" + topic + "$"));
		JsonNode broker = r.get("brokers").get(0);
		assertThat(broker.get("brokerId").asInt()).isEqualTo(1);
		JsonNode dir = broker.get("dirs").get(0);
		assertThat(dir.get("path").asText()).isNotBlank();
		assertThat(dir.get("partitions").asInt()).isGreaterThanOrEqualTo(3);
		assertThat(dir.get("sizeBytes").asLong()).isPositive();
		assertThat(dir.has("error")).isFalse();
		assertThat(dir.get("totalBytes").asLong()).isPositive();
		assertThat(r.get("total").asInt()).isEqualTo(3);
		JsonNode largest = r.get("largestPartitions").get(0);
		assertThat(largest.get("topic").asText()).isEqualTo(topic);
		assertThat(largest.get("partition").asInt()).isZero();
		assertThat(largest.get("sizeBytes").asLong()).isPositive();
		assertThat(largest.get("size").asText()).endsWith("B");

		assertThat(ok("brokers_logdirs", args("brokerId", 1, "limit", 1)).get("largestPartitions")).hasSize(1);
		assertThat(error("brokers_logdirs", args("brokerId", 42))).contains("Broker bulunamadı");
	}

	@Test
	void brokersPartitionSkewIsBalancedOnSingleBroker() {
		JsonNode r = ok("brokers_partition_skew", args());
		JsonNode s = r.get("summary");
		assertThat(s.get("brokers").asInt()).isEqualTo(1);
		assertThat(s.get("partitions").asInt()).isGreaterThanOrEqualTo(3);
		assertThat(s.get("nonPreferredLeaderPartitions").asInt()).isZero();
		assertThat(s.get("maxLeaderDeviationPct").asDouble()).isZero();
		assertThat(s.get("skewed").asBoolean()).isFalse();
		JsonNode b = r.get("brokers").get(0);
		assertThat(b.get("brokerId").asInt()).isEqualTo(1);
		assertThat(b.get("leaders").asInt()).isEqualTo(s.get("partitions").asInt() - s.get("offlinePartitions").asInt());
		assertThat(b.get("preferredLeaderOf").asInt()).isEqualTo(s.get("partitions").asInt());
		assertThat(b.get("nonPreferredLeaders").asInt()).isZero();
		assertThat(r.has("hint")).isFalse();
	}

	@Test
	void partitionReassignmentsListIsEmptyWhenNothingRuns() {
		JsonNode r = ok("partition_reassignments_list", args());
		assertThat(r.get("total").asInt()).isZero();
		assertThat(r.get("truncated").asBoolean()).isFalse();
		assertThat(r.get("reassignments").isArray()).isTrue();
		assertThat(ok("partition_reassignments_list", args("filter", topic)).get("reassignments")).isEmpty();
	}

	static JsonNode find(JsonNode array, String name) {
		for (JsonNode n : array) {
			if (name.equals(n.path("name").asText())) {
				return n;
			}
		}
		throw new AssertionError(name + " bulunamadı: " + array);
	}

}
