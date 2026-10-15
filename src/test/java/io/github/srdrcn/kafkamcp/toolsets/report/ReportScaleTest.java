package io.github.srdrcn.kafkamcp.toolsets.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.kafka.KafkaClients;

/**
 * Ölçek testi: tek createTopics ile ~300 topic; ağır raporlar zaman sınırı içinde biter, incelenen sayıları
 * bildirir ve kırpılan listeleri açıkça işaretler.
 */
class ReportScaleTest extends ReportTestBase {

	static final int TOPICS = 300;

	static String prefix;

	static List<String> names;

	@BeforeAll
	static void createManyTopics() throws Exception {
		prefix = unique("rep-scale") + ".";
		names = IntStream.range(0, TOPICS).mapToObj(i -> prefix + "t" + i).toList();
		createTopics(false, names.stream().map(n -> new NewTopic(n, 1, (short) 1)).toList());
		for (int i = 0; i < 30; i++) {
			produce(false, names.get(i), 0, 5, null);
		}
		for (int i = 0; i < 10; i++) {
			commit(false, prefix + "g" + i, names.get(i), 0, 2);
		}
	}

	@AfterAll
	static void deleteManyTopics() {
		if (names != null) {
			deleteTopicsQuietly(false, names);
		}
	}

	/** Aracı çağırır; sürenin araç sınırının (15 sn) altında kaldığını doğrular. */
	private JsonNode timed(String tool, Map<String, Object> args) {
		long t0 = System.nanoTime();
		JsonNode r = ok(tool, args);
		long ms = (System.nanoTime() - t0) / 1_000_000;
		assertThat(ms).as("%s süresi", tool).isLessThan(KafkaClients.TIMEOUT_MS);
		assertThat(r.has("partial")).as("%s partial", tool).isFalse();
		return r;
	}

	@Test
	void fleetWideReportsCoverAllTopicsWithinTheDeadline() {
		JsonNode summary = timed("report_cluster_summary", args());
		assertThat(summary.at("/examined/topics").asInt()).isGreaterThanOrEqualTo(TOPICS);
		assertThat(summary.at("/topics/user").asInt()).isGreaterThanOrEqualTo(TOPICS);
		assertThat(summary.get("emptyTopics").asInt()).isGreaterThanOrEqualTo(TOPICS - 30);

		JsonNode top = timed("report_top_topics", args("by", "consumers"));
		assertThat(top.at("/examined/topics").asInt()).isGreaterThanOrEqualTo(TOPICS);
		assertThat(top.get("returned").asInt()).isEqualTo(20);
		assertThat(top.get("truncated").asBoolean()).isTrue();

		JsonNode replication = timed("report_replication_audit", args());
		assertThat(replication.at("/examined/topics").asInt()).isGreaterThanOrEqualTo(TOPICS);
		JsonNode rf1 = find(replication.get("findings"), "check", "replication_factor_1");
		assertThat(rf1.get("count").asInt()).isGreaterThanOrEqualTo(TOPICS);
		assertThat(rf1.get("items").size()).isEqualTo(50);
		assertThat(rf1.get("itemsOmitted").asInt()).isEqualTo(rf1.get("count").asInt() - 50);

		JsonNode retention = timed("report_retention_audit", args());
		assertThat(retention.at("/examined/topics").asInt()).isGreaterThanOrEqualTo(TOPICS);

		JsonNode outliers = timed("report_partition_outliers", args());
		assertThat(outliers.at("/examined/topics").asInt()).isGreaterThanOrEqualTo(TOPICS);
		assertThat(outliers.at("/leaderBalance/brokers/0/leaders").asInt()).isGreaterThanOrEqualTo(TOPICS);

		JsonNode cleanup = timed("report_cleanup_candidates", args());
		assertThat(cleanup.at("/examined/topics").asInt()).isGreaterThanOrEqualTo(TOPICS);
		assertThat(cleanup.get("total").asInt()).isGreaterThanOrEqualTo(TOPICS - 10);
		assertThat(cleanup.get("returned").asInt()).isEqualTo(100);
		assertThat(cleanup.get("truncated").asBoolean()).isTrue();

		JsonNode duplicates = timed("report_duplicate_like_topics", args());
		assertThat(duplicates.at("/examined/topics").asInt()).isGreaterThanOrEqualTo(TOPICS);
		assertThat(duplicates.at("/similarPairs/comparisons").asLong()).isPositive();
	}

	@Test
	void prefixAndFilterScopedReportsCountExactly() {
		JsonNode team = timed("report_team_inventory", args("prefix", prefix));
		assertThat(team.at("/examined/topics").asInt()).isEqualTo(TOPICS);
		assertThat(team.at("/topics/count").asInt()).isEqualTo(TOPICS);
		assertThat(team.at("/topics/emptyCount").asInt()).isEqualTo(TOPICS - 30);
		assertThat(team.at("/topics/returned").asInt()).isEqualTo(100);
		assertThat(team.at("/topics/truncated").asBoolean()).isTrue();
		assertThat(team.at("/consumerGroups/count").asInt()).isEqualTo(10);
		assertThat(team.at("/consumerGroups/totalLag").asLong()).isEqualTo(30);
		assertThat(team.at("/skipped/notMatchingPrefix").asInt()).isPositive();

		String filter = java.util.regex.Pattern.quote(prefix);
		JsonNode empty = timed("report_empty_topics", args("filter", filter));
		assertThat(empty.get("emptyCount").asInt()).isEqualTo(TOPICS - 30);
		assertThat(empty.get("total").asInt()).isEqualTo(TOPICS - 30);

		JsonNode noConsumers = timed("report_topics_without_consumers", args("filter", filter, "limit", 5000));
		assertThat(noConsumers.get("count").asInt()).isEqualTo(TOPICS - 10);
		assertThat(noConsumers.get("returned").asInt()).isEqualTo(TOPICS - 10);

		JsonNode idle = timed("report_idle_topics", args("filter", filter));
		assertThat(idle.get("idleCount").asInt()).isZero();
		assertThat(idle.at("/empty/count").asInt()).isEqualTo(TOPICS - 30);
		assertThat(idle.at("/empty/omitted").asInt()).isEqualTo(TOPICS - 30 - 100);

		JsonNode duplicates = timed("report_duplicate_like_topics", args("filter", filter));
		assertThat(duplicates.at("/similarPairs/total").asInt()).isZero();
		assertThat(duplicates.at("/similarPairs/numericVariantsIgnored").asInt()).isPositive();

		JsonNode naming = timed("report_naming_check", args("pattern", filter + "t[0-9]{1,2}", "filter", filter));
		assertThat(naming.get("matching").asInt()).isEqualTo(100);
		assertThat(naming.get("nonMatching").asInt()).isEqualTo(TOPICS - 100);

		JsonNode rate = timed("report_top_topics", args("by", "write_rate", "filter", filter, "intervalSeconds", 1));
		assertThat(rate.at("/examined/partitions").asInt()).isEqualTo(TOPICS);
		assertThat(rate.get("idleTopics").asInt()).isEqualTo(TOPICS);
	}

}
