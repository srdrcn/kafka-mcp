package io.github.srdrcn.kafkamcp.toolsets.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;

/** report_replication_audit, report_retention_audit, report_partition_outliers (KRaft and ZooKeeper). */
class AuditReportTest extends ReportTestBase {

	@ParameterizedTest(name = "zk={0}")
	@ValueSource(booleans = { false, true })
	void replicationAuditFindsRf1MinIsrAboveRfAndUncleanElection(boolean zk) throws Exception {
		String p = unique("rep-repl");
		String plain = p + "-plain";
		String minIsr = p + "-minisr";
		String unclean = p + "-unclean";
		createTopic(zk, plain, 2, Map.of());
		createTopic(zk, minIsr, 1, Map.of("min.insync.replicas", "2"));
		createTopic(zk, unclean, 1, Map.of("unclean.leader.election.enable", "true"));

		JsonNode r = ok("report_replication_audit", on(zk, "filter", p));
		assertThat(r.at("/examined/topics").asInt()).isEqualTo(3);
		assertThat(r.get("brokers").asInt()).isEqualTo(1);
		assertThat(r.get("defaultReplicationFactor").asInt()).isEqualTo(1);
		assertThat(r.get("rackCheck").asText()).startsWith("skipped");
		JsonNode findings = r.get("findings");
		JsonNode rf1 = find(findings, "check", "replication_factor_1");
		assertThat(rf1.get("severity").asText()).isEqualTo("high");
		assertThat(rf1.get("count").asInt()).isEqualTo(3);
		assertThat(texts(rf1.get("items"), "topic")).containsExactlyInAnyOrder(plain, minIsr, unclean);
		JsonNode above = find(findings, "check", "min_isr_above_rf");
		assertThat(above.get("severity").asText()).isEqualTo("critical");
		assertThat(above.at("/items/0/topic").asText()).isEqualTo(minIsr);
		assertThat(above.at("/items/0/minInsyncReplicas").asInt()).isEqualTo(2);
		assertThat(above.at("/items/0/replicationFactor").asInt()).isEqualTo(1);
		assertThat(above.at("/items/0/source").asText()).isEqualTo("topic");
		JsonNode election = find(findings, "check", "unclean_leader_election");
		assertThat(election.at("/items/0/topic").asText()).isEqualTo(unclean);
		assertThat(election.at("/items/0/value").asText()).isEqualTo("true");
		assertThat(election.at("/items/0/source").asText()).isEqualTo("topic");
		assertThat(find(findings, "check", "under_replicated").isMissingNode()).isTrue();
		assertThat(find(findings, "check", "offline_partitions").isMissingNode()).isTrue();
		// severity order: critical first
		assertThat(findings.get(0).get("severity").asText()).isEqualTo("critical");
		assertThat(r.at("/bySeverity/critical").asInt()).isEqualTo(1);
		assertThat(r.get("findingsTotal").asInt()).isEqualTo(5);

		// item limit: the remaining count is reported
		JsonNode limited = ok("report_replication_audit", on(zk, "filter", p, "limit", 1));
		JsonNode rf1Limited = find(limited.get("findings"), "check", "replication_factor_1");
		assertThat(rf1Limited.get("items").size()).isEqualTo(1);
		assertThat(rf1Limited.get("itemsOmitted").asInt()).isEqualTo(2);
	}

	@ParameterizedTest(name = "zk={0}")
	@ValueSource(booleans = { false, true })
	void retentionAuditReportsValuesWithSources(boolean zk) throws Exception {
		String p = unique("rep-ret");
		String infinite = p + "-inf";
		String shortRet = p + "-short";
		String bytes = p + "-bytes";
		String compact = p + "-compact";
		String segments = p + "-seg";
		createTopic(zk, infinite, 1, Map.of("retention.ms", "-1"));
		createTopic(zk, shortRet, 1, Map.of("retention.ms", "600000"));
		createTopic(zk, bytes, 2, Map.of("retention.bytes", "1048576"));
		createTopic(zk, compact, 1, Map.of("cleanup.policy", "compact", "delete.retention.ms", "60000",
				"min.cleanable.dirty.ratio", "0.05"));
		createTopic(zk, segments, 1, Map.of("segment.bytes", "1048576", "segment.ms", "600000"));
		produce(zk, infinite, 0, 20, null, 100);

		JsonNode r = ok("report_retention_audit", on(zk, "filter", p, "largeSizeMb", 0));
		assertThat(r.at("/examined/topics").asInt()).isEqualTo(5);
		JsonNode f = r.get("findings");
		JsonNode inf = find(f, "check", "infinite_retention_large");
		assertThat(inf.get("severity").asText()).isEqualTo("high");
		assertThat(inf.at("/items/0/topic").asText()).isEqualTo(infinite);
		assertThat(inf.at("/items/0/value").asText()).isEqualTo("-1");
		assertThat(inf.at("/items/0/source").asText()).isEqualTo("topic");
		assertThat(inf.at("/items/0/sizeBytes").asLong()).isPositive();
		JsonNode shortFinding = find(f, "check", "short_retention");
		assertThat(shortFinding.at("/items/0/topic").asText()).isEqualTo(shortRet);
		assertThat(shortFinding.at("/items/0/value").asText()).isEqualTo("600000");
		JsonNode segAbove = find(f, "check", "segment_ms_above_retention");
		JsonNode segAboveItem = find(segAbove.get("items"), "topic", shortRet);
		assertThat(segAboveItem.get("source").asText()).isIn("default", "broker");
		assertThat(segAboveItem.get("retentionMs").asLong()).isEqualTo(600_000);
		JsonNode retBytes = find(f, "check", "retention_bytes_set");
		assertThat(retBytes.at("/items/0/topic").asText()).isEqualTo(bytes);
		assertThat(retBytes.at("/items/0/maxTopicBytes").asLong()).isEqualTo(2 * 1048576L);
		assertThat(find(f, "check", "short_delete_retention").at("/items/0/topic").asText()).isEqualTo(compact);
		assertThat(find(f, "check", "dirty_ratio_extreme").at("/items/0/value").asText()).startsWith("0.05");
		assertThat(find(f, "check", "small_segment_bytes").at("/items/0/topic").asText()).isEqualTo(segments);
		JsonNode shortSeg = find(f, "check", "short_segment_ms");
		assertThat(texts(shortSeg.get("items"), "topic")).containsExactly(segments);
		assertThat(r.get("notes").toString()).contains("topic override");

		// default threshold (10 GiB): a small topic with infinite retention is info only
		JsonNode def = ok("report_retention_audit", on(zk, "filter", p));
		assertThat(find(def.get("findings"), "check", "infinite_retention_large").isMissingNode()).isTrue();
		JsonNode info = find(def.get("findings"), "check", "infinite_retention");
		assertThat(info.get("severity").asText()).isEqualTo("info");
		assertThat(texts(info.get("items"), "topic")).containsExactly(infinite);
	}

	@ParameterizedTest(name = "zk={0}")
	@ValueSource(booleans = { false, true })
	void partitionOutliersFindWideTopicsSkewAndLeaderBalance(boolean zk) throws Exception {
		String p = unique("rep-out");
		String wide = p + "-wide";
		String skewed = p + "-skew";
		createTopic(zk, wide, 60, Map.of());
		createTopic(zk, skewed, 6, Map.of());
		produce(zk, skewed, 0, 300, null, 1000);
		produce(zk, skewed, 1, 2, null, 10);

		JsonNode r = ok("report_partition_outliers", on(zk, "filter", p, "minSizeMb", 0));
		assertThat(r.get("reportedTopics").asInt()).isEqualTo(2);
		assertThat(r.get("medianPartitions").asDouble()).isPositive().isLessThan(15);
		JsonNode wideRow = find(r.at("/partitionCountOutliers/topics"), "topic", wide);
		assertThat(wideRow.get("partitions").asInt()).isEqualTo(60);
		assertThat(wideRow.get("timesMedian").asDouble()).isGreaterThan(4);
		assertThat(r.at("/partitionCountOutliers/total").asInt()).isEqualTo(1);
		JsonNode skewRow = find(r.at("/skewedTopics/topics"), "topic", skewed);
		assertThat(skewRow.get("partitions").asInt()).isEqualTo(6);
		assertThat(skewRow.get("skewedCount").asInt()).isEqualTo(1);
		assertThat(skewRow.at("/skewedPartitions/0/partition").asInt()).isZero();
		assertThat(skewRow.at("/skewedPartitions/0/timesAverage").asDouble()).isGreaterThan(3);
		JsonNode brokers = r.at("/leaderBalance/brokers");
		assertThat(brokers.size()).isEqualTo(1);
		assertThat(brokers.get(0).get("leaders").asInt()).isGreaterThanOrEqualTo(66);
		assertThat(brokers.get(0).get("deviationPct").asDouble()).isZero();
		assertThat(r.at("/leaderBalance/imbalancedBrokers").size()).isZero();

		// default minSizeMb=1: a 300 KB skew is not reported; threshold parameters are echoed
		JsonNode def = ok("report_partition_outliers", on(zk, "filter", p, "minPartitions", 100));
		assertThat(def.at("/skewedTopics/total").asInt()).isZero();
		assertThat(def.at("/partitionCountOutliers/total").asInt()).isZero();
		assertThat(def.at("/thresholds/minPartitions").asInt()).isEqualTo(100);
	}

}
