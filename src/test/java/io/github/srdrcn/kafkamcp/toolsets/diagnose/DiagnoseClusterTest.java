package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

/** diagnose_cluster: KRaft (quorum) ve ZooKeeper (controller) cluster'larında sağlık özeti. */
class DiagnoseClusterTest extends DiagnoseTestBase {

	@Test
	void modeControllerPartitionsDiskAndLagOnBothClusters() throws Exception {
		for (String cluster : CLUSTERS) {
			String bad = unique("diag-cl-minisr");
			createTopic(cluster, bad, 1, Map.of("min.insync.replicas", "2"));
			String lagging = unique("diag-cl-lag");
			String group = lagging + "-g";
			createTopic(cluster, lagging, 1, Map.of());
			try (KafkaProducer<byte[], byte[]> p = producer(cluster)) {
				for (int i = 0; i < 12_000; i++) {
					p.send(new ProducerRecord<>(lagging, 0, null, new byte[1]));
				}
				p.flush();
			}
			commit(cluster, group, lagging, Map.of(0, 0L));

			long start = System.nanoTime();
			JsonNode r = ok("diagnose_cluster", on(cluster));
			assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(15_000);
			assertThat(r.has("partial")).as(r.toString()).isFalse();
			assertThat(sortedBySeverity(r)).isTrue();
			assertThat(r.get("status").asText()).isEqualTo("critical");
			JsonNode facts = r.get("facts");
			assertThat(facts.get("brokerCount").asInt()).isEqualTo(1);
			assertThat(facts.get("controllerId").asInt()).isEqualTo(1);
			assertThat(finding(r, "SINGLE_BROKER").get("severity").asText()).isEqualTo("info");
			if (zk(cluster)) {
				assertThat(r.get("mode").asText()).isEqualTo("ZooKeeper");
				assertThat(facts.get("kafkaVersion").asText()).isEqualTo("3.9");
				assertThat(facts.get("versionSource").asText()).startsWith("inter.broker.protocol.version");
				assertThat(finding(r, "ZOOKEEPER_MODE").get("recommendation").asText()).contains("KRaft");
				assertThat(facts.has("quorum")).isFalse();
			}
			else {
				assertThat(r.get("mode").asText()).isEqualTo("KRaft");
				assertThat(facts.get("kafkaVersion").asText()).isEqualTo("4.1");
				assertThat(facts.at("/quorum/leaderId").asInt()).isEqualTo(1);
				assertThat(facts.at("/quorum/voters/0/leader").asBoolean()).isTrue();
				assertThat(facts.at("/quorum/voters/0/lag").asLong()).isZero();
				assertThat(codes(r)).contains("SINGLE_CONTROLLER").doesNotContain("QUORUM_VOTER_LAGGING");
			}
			assertThat(codes(r)).doesNotContain("NO_ACTIVE_CONTROLLER", "BROKERS_DOWN", "OFFLINE_PARTITIONS",
					"LOG_DIR_ERROR", "CHECK_FAILED", "CHECK_TIMEOUT");

			JsonNode minIsr = finding(r, "MIN_ISR_EXCEEDS_RF");
			assertThat(texts(minIsr.at("/evidence/topics"))).contains(bad);
			assertThat(facts.at("/partitionHealth/partitions").asInt()).isGreaterThanOrEqualTo(2);
			assertThat(facts.at("/partitionHealth/offline").asInt()).isZero();
			JsonNode dir = facts.at("/disk/logDirs/0");
			assertThat(dir.get("brokerId").asInt()).isEqualTo(1);
			assertThat(dir.get("sizeBytes").asLong()).isPositive();
			assertThat(dir.get("usedPct").asDouble()).isBetween(0.0, 100.0);
			assertThat(facts.at("/configDrift/brokersCompared").asInt()).isEqualTo(1);

			assertThat(facts.at("/consumerGroups/total").asInt()).isGreaterThanOrEqualTo(1);
			JsonNode top = null;
			for (JsonNode g : facts.at("/consumerGroups/topLag")) {
				if (group.equals(g.get("groupId").asText())) {
					top = g;
				}
			}
			assertThat(top).as(facts.get("consumerGroups").toString()).isNotNull();
			assertThat(top.get("lag").asLong()).isEqualTo(12_000);
			assertThat(top.get("state").asText()).isEqualTo("Empty");
			JsonNode large = finding(r, "LARGE_LAG_EMPTY_GROUPS");
			assertThat(large.at("/evidence/groups").findValuesAsText("groupId")).contains(group);
			assertThat(large.get("recommendation").asText()).contains("consumer_groups_delete");
		}
	}

}
