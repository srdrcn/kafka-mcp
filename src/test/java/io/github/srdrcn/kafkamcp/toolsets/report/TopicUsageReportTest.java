package io.github.srdrcn.kafkamcp.toolsets.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;

/** report_empty_topics, report_idle_topics, report_topics_without_consumers (KRaft ve ZooKeeper). */
class TopicUsageReportTest extends ReportTestBase {

	@ParameterizedTest(name = "zk={0}")
	@ValueSource(booleans = { false, true })
	void emptyTopicsDistinguishNeverWrittenFromPurged(boolean zk) throws Exception {
		String p = unique("rep-empty");
		String never = p + "-never";
		String purged = p + "-purged";
		String full = p + "-full";
		String system = p + "-connect-offsets";
		createTopic(zk, never, 2, Map.of());
		createTopic(zk, system, 1, Map.of());
		createTopic(zk, purged, 1, Map.of());
		createTopic(zk, full, 1, Map.of());
		produce(zk, purged, 0, 3, null);
		produce(zk, full, 0, 2, null);
		admin(zk).deleteRecords(Map.of(new TopicPartition(purged, 0), RecordsToDelete.beforeOffset(3)))
			.all()
			.get(30, TimeUnit.SECONDS);

		JsonNode r = ok("report_empty_topics", on(zk, "filter", p));
		assertThat(r.at("/examined/topics").asInt()).isEqualTo(3);
		assertThat(r.at("/examined/partitions").asInt()).isEqualTo(4);
		assertThat(r.get("emptyCount").asInt()).isEqualTo(2);
		assertThat(r.get("neverWrittenCount").asInt()).isEqualTo(1);
		assertThat(texts(r.get("topics"), "topic")).containsExactlyInAnyOrder(never, purged);
		assertThat(find(r.get("topics"), "topic", never).get("neverWritten").asBoolean()).isTrue();
		assertThat(find(r.get("topics"), "topic", never).get("partitions").asInt()).isEqualTo(2);
		assertThat(find(r.get("topics"), "topic", purged).get("neverWritten").asBoolean()).isFalse();
		assertThat(r.has("partial")).isFalse();

		// iç/sistem topic'leri (ör. Connect offset topic'i) varsayılan olarak hariç; atlanan sayısı belirtilir
		assertThat(r.at("/skipped/internalOrSystemTopics").asInt()).isPositive();
		JsonNode all = ok("report_empty_topics", on(zk, "filter", p, "includeInternal", true, "limit", 1));
		assertThat(all.get("total").asInt()).isEqualTo(3);
		assertThat(all.get("returned").asInt()).isEqualTo(1);
		assertThat(all.get("truncated").asBoolean()).isTrue();
	}

	@ParameterizedTest(name = "zk={0}")
	@ValueSource(booleans = { false, true })
	void idleTopicsAreSortedByAgeAndEmptyOnesListedSeparately(boolean zk) throws Exception {
		String p = unique("rep-idle");
		String old = p + "-old";
		String older = p + "-older";
		String fresh = p + "-fresh";
		String empty = p + "-empty";
		Map<String, String> keep = Map.of("retention.ms", "-1");
		createTopic(zk, old, 1, keep);
		createTopic(zk, older, 2, keep);
		createTopic(zk, fresh, 1, Map.of());
		createTopic(zk, empty, 1, Map.of());
		long now = System.currentTimeMillis();
		produce(zk, old, 0, 3, now - 40 * DAY_MS);
		produce(zk, older, 0, 2, now - 90 * DAY_MS);
		produce(zk, older, 1, 2, now - 100 * DAY_MS);
		produce(zk, fresh, 0, 2, null);

		JsonNode r = ok("report_idle_topics", on(zk, "filter", p, "olderThanDays", 30));
		assertThat(r.get("olderThanDays").asInt()).isEqualTo(30);
		assertThat(r.get("idleCount").asInt()).isEqualTo(2);
		assertThat(texts(r.get("topics"), "topic")).containsExactly(older, old);
		JsonNode olderRow = find(r.get("topics"), "topic", older);
		assertThat(olderRow.get("lastWriteAgeDays").asLong()).isBetween(89L, 91L);
		assertThat(olderRow.get("lastWriteTime").asText()).isNotBlank();
		assertThat(olderRow.get("partitions").asInt()).isEqualTo(2);
		assertThat(olderRow.get("sizeBytes").asLong()).isPositive();
		assertThat(find(r.get("topics"), "topic", old).get("lastWriteAgeDays").asLong()).isBetween(39L, 41L);
		assertThat(r.at("/empty/count").asInt()).isEqualTo(1);
		assertThat(texts(r.at("/empty/topics"), null)).containsExactly(empty);
		assertThat(r.at("/examined/topics").asInt()).isEqualTo(4);
		assertThat(r.has("unknown")).isFalse();

		JsonNode stricter = ok("report_idle_topics", on(zk, "filter", p, "olderThanDays", 60));
		assertThat(texts(stricter.get("topics"), "topic")).containsExactly(older);
	}

	@ParameterizedTest(name = "zk={0}")
	@ValueSource(booleans = { false, true })
	void topicsWithoutCommittedConsumersAreListed(boolean zk) throws Exception {
		String p = unique("rep-nocons");
		String consumed = p + "-consumed";
		String orphan = p + "-orphan";
		String empty = p + "-empty";
		createTopic(zk, consumed, 1, Map.of());
		createTopic(zk, orphan, 1, Map.of());
		createTopic(zk, empty, 1, Map.of());
		produce(zk, consumed, 0, 3, null);
		produce(zk, orphan, 0, 3, null, 1000);
		commit(zk, p + "-group", consumed, 0, 1);

		JsonNode r = ok("report_topics_without_consumers", on(zk, "filter", p));
		assertThat(texts(r.get("topics"), "topic")).containsExactly(orphan, empty);
		assertThat(r.get("count").asInt()).isEqualTo(2);
		assertThat(r.get("topicsWithConsumers").asInt()).isEqualTo(1);
		JsonNode row = find(r.get("topics"), "topic", orphan);
		assertThat(row.get("sizeBytes").asLong()).isGreaterThan(3000);
		assertThat(row.get("lastWriteAgeDays").asLong()).isZero();
		assertThat(find(r.get("topics"), "topic", empty).get("empty").asBoolean()).isTrue();
		assertThat(r.at("/examined/groups").asInt()).isPositive();
		assertThat(r.has("uncertain")).isFalse();
		assertThat(r.get("notes").toString()).contains("assign()");
	}

}
