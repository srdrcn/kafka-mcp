package io.github.srdrcn.kafkamcp.toolsets.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.kafka.LogDirs;
import io.github.srdrcn.kafkamcp.kafka.Topics;

/** Replication/retention audit and partition outlier rules (synthetic metadata, no broker). */
class AuditRulesTest {

	static Node node(int id) {
		return new Node(id, "b" + id, 9092);
	}

	static List<Node> nodes(int... ids) {
		List<Node> out = new ArrayList<>();
		for (int id : ids) {
			out.add(node(id));
		}
		return out;
	}

	/** leader -1: offline. */
	static TopicPartitionInfo part(int p, int leader, int[] replicas, int... isr) {
		return new TopicPartitionInfo(p, leader < 0 ? Node.noNode() : node(leader), nodes(replicas), nodes(isr));
	}

	static TopicDescription topic(String name, TopicPartitionInfo... parts) {
		return new TopicDescription(name, false, List.of(parts));
	}

	static ConfigEntry entry(String name, String value, ConfigSource source) {
		return new ConfigEntry(name, value, source, false, false, List.of(), ConfigEntry.ConfigType.UNKNOWN, null);
	}

	/** overrides: topic overrides; defaults: Kafka defaults. */
	static Config config(Map<String, String> overrides, Map<String, String> defaults) {
		List<ConfigEntry> entries = new ArrayList<>();
		defaults.forEach((k, v) -> {
			if (!overrides.containsKey(k)) {
				entries.add(entry(k, v, ConfigSource.DEFAULT_CONFIG));
			}
		});
		overrides.forEach((k, v) -> entries.add(entry(k, v, ConfigSource.DYNAMIC_TOPIC_CONFIG)));
		return new Config(entries);
	}

	static final Map<String, String> DEFAULTS = Map.of("min.insync.replicas", "1", "unclean.leader.election.enable",
			"false", "cleanup.policy", "delete", "retention.ms", "604800000", "retention.bytes", "-1", "segment.ms",
			"604800000", "segment.bytes", "1073741824", "delete.retention.ms", "86400000", "min.cleanable.dirty.ratio",
			"0.5");

	private static Findings replication(Map<String, TopicDescription> topics, Map<String, Config> configs,
			Integer defaultRf, Map<Integer, String> racks, Topics.Reassignments r) {
		Findings f = new Findings(50);
		ReplicationAudit.evaluate(new ReplicationAudit.Input(topics, configs, defaultRf, racks, r), f);
		return f;
	}

	@Test
	void replicationAuditClassifiesPartitionsAndConfigs() {
		int[] r123 = { 1, 2, 3 };
		Map<String, TopicDescription> topics = new TreeMap<>();
		topics.put("off", topic("off", part(0, -1, r123), part(1, 1, r123, 1, 2, 3)));
		topics.put("urp", topic("urp", part(0, 1, r123, 1, 2)));
		topics.put("moving", topic("moving", part(0, 1, new int[] { 1, 2, 3, 4 }, 1, 2, 3)));
		topics.put("single", topic("single", part(0, 1, new int[] { 1 }, 1)));
		topics.put("strict", topic("strict", part(0, 1, new int[] { 1, 2 }, 1, 2)));
		topics.put("broken", topic("broken", part(0, 1, new int[] { 1, 2 }, 1, 2)));
		topics.put("degraded", topic("degraded", part(0, 1, r123, 1)));
		topics.put("unclean", topic("unclean", part(0, 1, r123, 1, 2, 3)));
		topics.put("noconfig", topic("noconfig", part(0, 1, r123, 1, 2, 3)));
		Map<String, Config> configs = new TreeMap<>();
		for (String t : topics.keySet()) {
			configs.put(t, config(Map.of(), DEFAULTS));
		}
		configs.put("strict", config(Map.of("min.insync.replicas", "2"), DEFAULTS));
		configs.put("broken", config(Map.of("min.insync.replicas", "3"), DEFAULTS));
		configs.put("degraded", config(Map.of("min.insync.replicas", "2"), DEFAULTS));
		configs.put("unclean", config(Map.of("unclean.leader.election.enable", "true"), DEFAULTS));
		configs.remove("noconfig");
		Topics.Reassignments reassign = new Topics.Reassignments(
				Map.of(new TopicPartition("moving", 0), new PartitionReassignment(List.of(1, 2, 3, 4), List.of(4), List.of(1))),
				null);

		Findings f = new Findings(50);
		int withoutConfig = ReplicationAudit.evaluate(
				new ReplicationAudit.Input(topics, configs, 3, Map.of(), reassign), f);
		assertThat(withoutConfig).isEqualTo(1);
		assertThat(topicsOf(f, "offline_partitions")).containsExactly("off");
		assertThat(f.items("offline_partitions").get(0).get("partitions")).isEqualTo(List.of(0));
		assertThat(topicsOf(f, "under_replicated")).containsExactly("degraded", "urp");
		assertThat(topicsOf(f, "replication_factor_1")).containsExactly("single");
		assertThat(topicsOf(f, "min_isr_equals_rf")).containsExactly("strict");
		assertThat(topicsOf(f, "min_isr_above_rf")).containsExactly("broken");
		assertThat(f.items("min_isr_above_rf").get(0)).containsEntry("source", "topic");
		assertThat(topicsOf(f, "under_min_isr")).containsExactly("degraded");
		assertThat(topicsOf(f, "unclean_leader_election")).containsExactly("unclean");
		assertThat(f.items("unclean_leader_election").get(0)).containsEntry("value", "true").containsEntry("source",
				"topic");
		assertThat(topicsOf(f, "rf_below_default")).containsExactly("broken", "single", "strict");
		assertThat(f.items("single_rack_replicas")).isEmpty();
		assertThat(f.render().get(0).get("severity")).isEqualTo("critical");
		assertThat(f.bySeverity()).containsEntry("critical", 3).containsEntry("high", 3);
	}

	@Test
	void underMinIsrUsesTheEffectiveMinIsrCappedByReplicaCount() {
		int[] r12 = { 1, 2 };
		Map<String, TopicDescription> topics = new TreeMap<>();
		// RF=1, min.insync=2: the broker caps min ISR at 1, writable (not under min ISR, but above RF)
		topics.put("single", topic("single", part(0, 1, new int[] { 1 }, 1)));
		// RF=2, min.insync=3, 4 partitions, only one URP: only that partition is below the effective min ISR (2)
		topics.put("above", topic("above", part(0, 1, r12, 1), part(1, 1, r12, 1, 2), part(2, 2, r12, 1, 2),
				part(3, 2, r12, 1, 2)));
		topics.put("normal", topic("normal", part(0, 1, new int[] { 1, 2, 3 }, 1), part(1, -1, new int[] { 1, 2, 3 })));
		topics.put("noconfig", topic("noconfig", part(0, 1, r12, 1)));
		Map<String, Config> configs = new TreeMap<>();
		configs.put("single", config(Map.of("min.insync.replicas", "2"), DEFAULTS));
		configs.put("above", config(Map.of("min.insync.replicas", "3"), DEFAULTS));
		configs.put("normal", config(Map.of("min.insync.replicas", "2"), DEFAULTS));

		ReplicationAudit.MinIsrCounts counts = ReplicationAudit.minIsrCounts(topics.values(), configs,
				Topics.Reassignments.NONE);
		// above/0 and normal/0; offline partitions are not counted
		assertThat(counts.underMinIsrPartitions()).isEqualTo(2);
		assertThat(counts.aboveRfTopics()).isEqualTo(2);
		assertThat(counts.unknownTopics()).isEqualTo(1);

		// the audit uses the same rule: it does not contradict the summary
		Findings f = replication(topics, configs, null, Map.of(), Topics.Reassignments.NONE);
		assertThat(topicsOf(f, "under_min_isr")).containsExactly("above", "normal");
		assertThat(f.items("under_min_isr").get(0)).containsEntry("partitions", List.of(0));
		assertThat(topicsOf(f, "min_isr_above_rf")).containsExactly("above", "single");
		assertThat(f.render()).filteredOn(m -> m.get("check").equals("min_isr_above_rf"))
			.singleElement()
			.satisfies(m -> assertThat(m.get("message").toString()).contains("older brokers"));
	}

	@Test
	void singleRackCheckRunsOnlyWithAtLeastTwoRacks() {
		Map<String, TopicDescription> topics = Map.of("a", topic("a", part(0, 1, new int[] { 1, 2 }, 1, 2),
				part(1, 1, new int[] { 1, 3 }, 1, 3), part(2, 2, new int[] { 2, 4 }, 2, 4)));
		Map<Integer, String> racks = Map.of(1, "a", 2, "a", 3, "b");
		Findings f = replication(topics, Map.of(), null, racks, Topics.Reassignments.NONE);
		assertThat(f.items("single_rack_replicas")).hasSize(1);
		assertThat(f.items("single_rack_replicas").get(0)).containsEntry("rack", "a")
			.containsEntry("partitions", List.of(0));
		Findings oneRack = replication(topics, Map.of(), null, Map.of(1, "a", 2, "a"), Topics.Reassignments.NONE);
		assertThat(oneRack.items("single_rack_replicas")).isEmpty();
		assertThat(ReplicationAudit.sameRack(nodes(1, 2), racks)).isEqualTo("a");
		assertThat(ReplicationAudit.sameRack(nodes(1, 4), racks)).isNull();
	}

	@Test
	void retentionAuditReportsValueAndSource() {
		Findings f = new Findings(50);
		RetentionAudit.Thresholds th = new RetentionAudit.Thresholds(10L << 30, 3_600_000);
		RetentionAudit.evaluate("big", config(Map.of("retention.ms", "-1"), DEFAULTS), 20L << 30, true, 4, th, f);
		RetentionAudit.evaluate("small", config(Map.of("retention.ms", "-1"), DEFAULTS), 1024L, true, 1, th, f);
		RetentionAudit.evaluate("capped", config(Map.of("retention.ms", "-1", "retention.bytes", "1000"), DEFAULTS), 10L,
				true, 3, th, f);
		RetentionAudit.evaluate("compacted", config(Map.of("cleanup.policy", "compact", "retention.ms", "-1",
				"delete.retention.ms", "0", "min.cleanable.dirty.ratio", "0.95"), DEFAULTS), 10L, true, 1, th, f);
		RetentionAudit.evaluate("short", config(Map.of("retention.ms", "1000"), DEFAULTS), 10L, true, 1, th, f);
		RetentionAudit.evaluate("segments", config(Map.of("segment.bytes", "1048576", "segment.ms", "60000"), DEFAULTS),
				null, false, 1, th, f);
		RetentionAudit.evaluate("fine", config(Map.of(), DEFAULTS), 10L, true, 1, th, f);
		// an infinite-retention topic of unknown or incomplete (lower bound) size is not considered "small"
		RetentionAudit.evaluate("unknown", config(Map.of("retention.ms", "-1"), DEFAULTS), null, false, 1, th, f);
		RetentionAudit.evaluate("partial", config(Map.of("retention.ms", "-1"), DEFAULTS), 2048L, false, 1, th, f);
		RetentionAudit.evaluate("partialBig", config(Map.of("retention.ms", "-1"), DEFAULTS), 30L << 30, false, 1, th,
				f);

		assertThat(topicsOf(f, "infinite_retention_large")).containsExactly("big", "partialBig");
		assertThat(f.items("infinite_retention_large").get(1)).containsEntry("sizeLowerBound", true);
		assertThat(topicsOf(f, "infinite_retention_size_unknown")).containsExactly("unknown", "partial");
		assertThat(f.items("infinite_retention_size_unknown").get(0)).doesNotContainKey("sizeBytes");
		assertThat(f.items("infinite_retention_size_unknown").get(1)).containsEntry("sizeBytes", 2048L)
			.containsEntry("sizeLowerBound", true);
		assertThat(f.render()).filteredOn(m -> m.get("check").equals("infinite_retention_size_unknown"))
			.singleElement()
			.satisfies(m -> assertThat(m).containsEntry("severity", "medium"));
		assertThat(f.items("infinite_retention_large").get(0)).containsEntry("value", "-1")
			.containsEntry("source", "topic")
			.containsEntry("sizeBytes", 20L << 30);
		assertThat(topicsOf(f, "infinite_retention")).containsExactly("small");
		assertThat(topicsOf(f, "retention_bytes_set")).containsExactly("capped");
		assertThat(f.items("retention_bytes_set").get(0)).containsEntry("maxTopicBytes", 3000L);
		assertThat(topicsOf(f, "short_retention")).containsExactly("short");
		assertThat(topicsOf(f, "segment_ms_above_retention")).containsExactly("short");
		assertThat(f.items("segment_ms_above_retention").get(0)).containsEntry("source", "default")
			.containsEntry("value", "604800000");
		assertThat(topicsOf(f, "short_delete_retention")).containsExactly("compacted");
		assertThat(topicsOf(f, "dirty_ratio_extreme")).containsExactly("compacted");
		assertThat(topicsOf(f, "small_segment_bytes")).containsExactly("segments");
		assertThat(topicsOf(f, "short_segment_ms")).containsExactly("segments");
		assertThat(f.items("small_segment_bytes").get(0)).doesNotContainKey("sizeBytes");
		assertThat(f.render()).extracting(m -> m.get("severity")).first().isEqualTo("high");
	}

	@Test
	void configSourcesAreClassified() {
		assertThat(ConfigValues.source(entry("x", "1", ConfigSource.DYNAMIC_TOPIC_CONFIG))).isEqualTo("topic");
		assertThat(ConfigValues.source(entry("x", "1", ConfigSource.STATIC_BROKER_CONFIG))).isEqualTo("broker");
		assertThat(ConfigValues.source(entry("x", "1", ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG))).isEqualTo("broker");
		assertThat(ConfigValues.source(entry("x", "1", ConfigSource.DEFAULT_CONFIG))).isEqualTo("default");
		assertThat(ConfigValues.source(null)).isNull();
		Config c = config(Map.of("cleanup.policy", "compact,delete"), DEFAULTS);
		assertThat(ConfigValues.compacted(c)).isTrue();
		assertThat(ConfigValues.deletes(c)).isTrue();
		assertThat(ConfigValues.longValue(c, "missing")).isNull();
	}

	@Test
	void partitionOutliersMedianSkewAndLeaderBalance() {
		assertThat(PartitionOutliers.median(List.of())).isZero();
		assertThat(PartitionOutliers.median(List.of(5))).isEqualTo(5);
		assertThat(PartitionOutliers.median(List.of(1, 2, 3, 100))).isEqualTo(2.5);
		PartitionOutliers.Thresholds th = new PartitionOutliers.Thresholds(4, 50, 3, 1024, 25);
		Map<String, Integer> counts = new LinkedHashMap<>();
		counts.put("wide", 200);
		counts.put("medium", 20);
		counts.put("tiny", 3);
		counts.put("wideButFew", 13);
		assertThat(PartitionOutliers.countOutliers(counts, 3, th)).extracting(PartitionOutliers.CountOutlier::topic)
			.containsExactly("wide");

		int[] r1 = { 1 };
		TopicDescription skewed = topic("skewed", part(0, 1, r1, 1), part(1, 1, r1, 1), part(2, 1, r1, 1),
				part(3, 1, r1, 1));
		TopicDescription even = topic("even", part(0, 1, r1, 1), part(1, 1, r1, 1));
		TopicDescription small = topic("small", part(0, 1, r1, 1), part(1, 1, r1, 1), part(2, 1, r1, 1),
				part(3, 1, r1, 1));
		Map<TopicPartition, LogDirs.PartitionSize> sizes = new TreeMap<>(
				java.util.Comparator.comparing(TopicPartition::toString));
		long[] skewSizes = { 100_000, 1_000, 1_000, 1_000 };
		for (int i = 0; i < 4; i++) {
			sizes.put(new TopicPartition("skewed", i), new LogDirs.PartitionSize(skewSizes[i], skewSizes[i], 1));
			sizes.put(new TopicPartition("small", i), new LogDirs.PartitionSize(i == 0 ? 900 : 1, 1, 1));
		}
		sizes.put(new TopicPartition("even", 0), new LogDirs.PartitionSize(5_000, 5_000, 1));
		sizes.put(new TopicPartition("even", 1), new LogDirs.PartitionSize(5_000, 5_000, 1));
		List<PartitionOutliers.SkewedTopic> skew = PartitionOutliers.skew(List.of(skewed, even, small), sizes, th);
		assertThat(skew).extracting(PartitionOutliers.SkewedTopic::topic).containsExactly("skewed");
		assertThat(skew.get(0).skewed()).extracting(PartitionOutliers.SkewedPartition::partition).containsExactly(0);
		assertThat(skew.get(0).averageBytes()).isEqualTo(25_750);

		TopicDescription t = topic("t", part(0, 1, new int[] { 1, 2 }, 1, 2), part(1, 1, new int[] { 1, 2 }, 1, 2),
				part(2, 1, new int[] { 2, 1 }, 1, 2), part(3, 1, new int[] { 1, 2 }, 1, 2),
				part(4, 2, new int[] { 2, 1 }, 1, 2), part(5, 2, new int[] { 2, 1 }, 1, 2));
		List<PartitionOutliers.BrokerLoad> loads = PartitionOutliers.leaderBalance(List.of(t), List.of(1, 2, 3), 25);
		assertThat(loads).extracting(PartitionOutliers.BrokerLoad::id).containsExactly(1, 2, 3);
		assertThat(loads).extracting(PartitionOutliers.BrokerLoad::leaders).containsExactly(4, 2, 0);
		assertThat(loads).extracting(PartitionOutliers.BrokerLoad::deviationPct).containsExactly(100.0, 0.0, -100.0);
		assertThat(loads).extracting(PartitionOutliers.BrokerLoad::imbalanced).containsExactly(true, false, true);
		assertThat(loads.get(0).nonPreferredLeaders()).isEqualTo(1);
	}

	private static List<String> topicsOf(Findings f, String check) {
		return f.items(check).stream().map(m -> (String) m.get("topic")).toList();
	}

}
