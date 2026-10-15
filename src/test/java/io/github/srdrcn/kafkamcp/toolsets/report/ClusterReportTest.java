package io.github.srdrcn.kafkamcp.toolsets.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.mcp.RegisteredTool;
import io.github.srdrcn.kafkamcp.mcp.ToolKind;
import io.github.srdrcn.kafkamcp.support.TestConfigs;

/** report_cluster_summary and report_top_topics (KRaft and ZooKeeper). */
class ClusterReportTest extends ReportTestBase {

	@ParameterizedTest(name = "zk={0}")
	@ValueSource(booleans = { false, true })
	void clusterSummaryReportsModeVersionSizesAndGroups(boolean zk) throws Exception {
		String p = unique("rep-sum");
		String big = p + "-big";
		createTopic(zk, big, 2, Map.of());
		produce(zk, big, 0, 200, null, 1000);
		commit(zk, p + "-lagging", big, 0, 10);
		commit(zk, p + "-done", big, 0, 200);
		// min.insync.replicas (2) > RF (1): the broker caps the effective min ISR at 1, acks=all writes are accepted
		String minIsr = p + "-minisr";
		createTopic(zk, minIsr, 1, Map.of("min.insync.replicas", "2"));
		produce(zk, minIsr, 0, 1, null);

		JsonNode r = ok("report_cluster_summary", on(zk));
		assertThat(r.get("cluster").asText()).isEqualTo(zk ? "zk" : "test");
		assertThat(r.get("mode").asText()).isEqualTo(zk ? "ZooKeeper" : "KRaft");
		assertThat(r.get("kafkaVersion").asText()).isEqualTo(zk ? "3.9" : "4.1");
		assertThat(r.get("versionSource").asText())
			.startsWith(zk ? "inter.broker.protocol.version" : "metadata.version");
		assertThat(r.get("brokerCount").asInt()).isEqualTo(1);
		assertThat(r.get("controllerId").asInt()).isEqualTo(1);
		assertThat(r.at("/topics/user").asInt()).isPositive();
		assertThat(r.at("/topics/internal").asInt()).isPositive();
		assertThat(r.get("partitions").asInt()).isGreaterThanOrEqualTo(2);
		assertThat(r.at("/size/logicalBytes").asLong()).isGreaterThan(200_000);
		assertThat(r.at("/size/allReplicasBytes").asLong()).isGreaterThanOrEqualTo(r.at("/size/logicalBytes").asLong());
		assertThat(r.at("/replication/underReplicatedPartitions").asInt()).isZero();
		assertThat(r.at("/replication/offlinePartitions").asInt()).isZero();
		// single broker, RF=1: no partition is below the effective min ISR; min ISR > RF topics are counted separately
		assertThat(r.at("/replication/underMinIsrPartitions").asInt()).isZero();
		assertThat(r.at("/replication/minIsrAboveRfTopics").asInt()).isGreaterThanOrEqualTo(1);
		assertThat(r.get("notes").toString()).contains("minIsrAboveRfTopics").contains("report_replication_audit");
		assertThat(r.at("/consumerGroups/total").asInt()).isGreaterThanOrEqualTo(2);
		assertThat(r.at("/consumerGroups/byState/Empty").asInt()).isGreaterThanOrEqualTo(2);
		assertThat(r.at("/consumerGroups/withLag").asInt()).isGreaterThanOrEqualTo(1);
		assertThat(r.at("/consumerGroups/lagExamined").asInt()).isGreaterThanOrEqualTo(2);
		JsonNode largest = r.get("largestTopics");
		assertThat(largest.size()).isBetween(1, 5);
		for (int i = 1; i < largest.size(); i++) {
			assertThat(largest.get(i).get("sizeBytes").asLong()).isLessThanOrEqualTo(largest.get(i - 1).get("sizeBytes").asLong());
		}
		assertThat(r.get("emptyTopics").asInt()).isGreaterThanOrEqualTo(0);
		assertThat(r.at("/examined/topics").asInt()).isEqualTo(r.at("/topics/user").asInt() + r.at("/topics/internal").asInt());
		assertThat(r.has("connect")).isFalse();
		assertThat(r.has("schemaRegistry")).isFalse();
	}

	@ParameterizedTest(name = "zk={0}")
	@ValueSource(booleans = { false, true })
	void topTopicsBySizePartitionsAndConsumers(boolean zk) throws Exception {
		String p = unique("rep-top");
		String a = p + "-a";
		String b = p + "-b";
		String c = p + "-c";
		createTopic(zk, a, 3, Map.of());
		createTopic(zk, b, 1, Map.of());
		createTopic(zk, c, 6, Map.of());
		produce(zk, a, 0, 50, null, 1000);
		produce(zk, b, 0, 5, null);
		commit(zk, p + "-g1", b, 0, 1);
		commit(zk, p + "-g2", b, 0, 2);
		commit(zk, p + "-g3", a, 0, 3);

		JsonNode size = ok("report_top_topics", on(zk, "by", "size", "filter", p));
		assertThat(texts(size.get("topics"), "topic")).containsExactly(a, b, c);
		JsonNode first = size.at("/topics/0");
		assertThat(first.get("rank").asInt()).isEqualTo(1);
		assertThat(first.get("partitions").asInt()).isEqualTo(3);
		assertThat(first.get("replicationFactor").asInt()).isEqualTo(1);
		assertThat(first.get("sizeBytes").asLong()).isGreaterThan(50_000);
		assertThat(size.at("/examined/topics").asInt()).isEqualTo(3);

		JsonNode partitions = ok("report_top_topics", on(zk, "by", "partitions", "filter", p, "limit", 2));
		assertThat(texts(partitions.get("topics"), "topic")).containsExactly(c, a);
		assertThat(partitions.get("total").asInt()).isEqualTo(3);
		assertThat(partitions.get("truncated").asBoolean()).isTrue();

		JsonNode consumers = ok("report_top_topics", on(zk, "by", "consumers", "filter", p));
		assertThat(texts(consumers.get("topics"), "topic")).containsExactly(b, a, c);
		assertThat(consumers.at("/topics/0/consumerGroups").asInt()).isEqualTo(2);
		assertThat(texts(consumers.at("/topics/0/sampleGroups"), null)).containsExactly(p + "-g1", p + "-g2");
		assertThat(consumers.at("/topics/2/consumerGroups").asInt()).isZero();
		assertThat(consumers.at("/examined/groups").asInt()).isGreaterThanOrEqualTo(3);

		String bad = error("report_top_topics", on(zk, "by", "nope"));
		assertThat(bad).contains("Invalid by");
	}

	@ParameterizedTest(name = "zk={0}")
	@ValueSource(booleans = { false, true })
	void topTopicsByWriteRateSamplesTwice(boolean zk) throws Exception {
		String p = unique("rep-rate");
		String hot = p + "-hot";
		String cold = p + "-cold";
		createTopic(zk, hot, 1, Map.of());
		createTopic(zk, cold, 1, Map.of());
		produce(zk, cold, 0, 3, null);
		AtomicBoolean stop = new AtomicBoolean();
		List<Throwable> errors = new ArrayList<>();
		Thread writer = new Thread(() -> {
			try {
				while (!stop.get()) {
					producer(zk).send(new ProducerRecord<>(hot, 0, null, "k".getBytes(), "v".getBytes())).get();
					Thread.sleep(20);
				}
			}
			catch (Exception e) {
				errors.add(e);
			}
		});
		writer.start();
		JsonNode r;
		try {
			r = ok("report_top_topics", on(zk, "by", "write_rate", "filter", p, "intervalSeconds", 2));
		}
		finally {
			stop.set(true);
			writer.join(10_000);
		}
		assertThat(errors).isEmpty();
		assertThat(r.get("by").asText()).isEqualTo("write_rate");
		assertThat(r.get("intervalSeconds").asDouble()).isGreaterThanOrEqualTo(1.5);
		assertThat(texts(r.get("topics"), "topic")).containsExactly(hot, cold);
		assertThat(r.at("/topics/0/msgsPerSec").asDouble()).isPositive();
		assertThat(r.at("/topics/0/messages").asLong()).isPositive();
		assertThat(r.at("/topics/1/messages").asLong()).isZero();
		assertThat(r.get("activeTopics").asInt()).isEqualTo(1);
		assertThat(r.get("idleTopics").asInt()).isEqualTo(1);
	}

	@Test
	void reportToolsAreReadOnlyAndWorkOnReadOnlyCluster() {
		List<RegisteredTool> report = registry.tools().stream().filter(t -> t.toolset().equals("report")).toList();
		assertThat(report).extracting(t -> t.spec().name())
			.containsExactly("report_cluster_summary", "report_top_topics", "report_empty_topics", "report_idle_topics",
					"report_topics_without_consumers", "report_replication_audit", "report_retention_audit",
					"report_partition_outliers", "report_naming_check", "report_duplicate_like_topics",
					"report_team_inventory", "report_cleanup_candidates");
		assertThat(report).allMatch(t -> t.spec().kind() == ToolKind.READ);
		assertThat(report).allMatch(t -> t.clusters().contains(TestConfigs.READ_ONLY_CLUSTER));
		JsonNode r = ok("report_cluster_summary", args("cluster", TestConfigs.READ_ONLY_CLUSTER));
		assertThat(r.get("cluster").asText()).isEqualTo(TestConfigs.READ_ONLY_CLUSTER);
		// write tools are rejected on a readOnly cluster (report has no write tools; framework behaviour)
		String err = error("topics_create", args("cluster", TestConfigs.READ_ONLY_CLUSTER, "topic", unique("rep-ro")));
		assertThat(err).contains("readOnly");
	}

}
