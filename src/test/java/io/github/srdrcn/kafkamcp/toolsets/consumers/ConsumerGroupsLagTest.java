package io.github.srdrcn.kafkamcp.toolsets.consumers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

/** consumer_groups_lag, _with_lag, _lag_trend, _stuck. */
class ConsumerGroupsLagTest extends ConsumersTestBase {

	private static List<String> groupIds(JsonNode r) {
		List<String> out = new ArrayList<>();
		r.get("groups").forEach(g -> out.add(g.get("groupId").asText()));
		return out;
	}

	@Test
	void lagPerTopicAndPartitionSortedDescWithOwner() throws Exception {
		String prefix = unique("lag");
		String t1 = prefix + "-a";
		String t2 = prefix + "-b";
		createTopic(t1, 2);
		createTopic(t2, 1);
		produce(t1, 0, 10);
		produce(t1, 1, 10);
		produce(t2, 0, 30);
		String group = prefix + "-g";
		String clientId = prefix + "-cid";
		var consumer = join(group, clientId, t1);
		commit(consumer, t1, Map.of(0, 9L, 1, 2L));
		alter(prefix + "-other", t2, Map.of(0, 0L));

		JsonNode r = ok("consumer_groups_lag", args("group", group));
		assertThat(r.get("state").asText()).isEqualTo("Stable");
		assertThat(r.get("members").asInt()).isEqualTo(1);
		assertThat(r.get("totalLag").asLong()).isEqualTo(9);
		JsonNode topic = r.at("/topics/0");
		assertThat(topic.get("topic").asText()).isEqualTo(t1);
		assertThat(topic.get("lag").asLong()).isEqualTo(9);
		JsonNode first = topic.at("/partitions/0");
		assertThat(first.get("partition").asInt()).isEqualTo(1);
		assertThat(first.get("committed").asLong()).isEqualTo(2);
		assertThat(first.get("end").asLong()).isEqualTo(10);
		assertThat(first.get("lag").asLong()).isEqualTo(8);
		assertThat(first.get("clientId").asText()).isEqualTo(clientId);
		assertThat(first.get("host").asText()).isNotBlank();
		assertThat(topic.at("/partitions/1/lag").asLong()).isEqualTo(1);

		JsonNode filtered = ok("consumer_groups_lag", args("group", group, "topic", t1));
		assertThat(filtered.get("totalLag").asLong()).isEqualTo(9);
		assertThat(error("consumer_groups_lag", args("group", group, "topic", t2))).contains("commit'i ya da ataması yok");
		assertThat(error("consumer_groups_lag", args("group", unique("nope")))).contains("bulunamadı");
	}

	@Test
	void withLagReturnsGroupsAboveThresholdSortedDesc() throws Exception {
		String prefix = unique("wl");
		String big = prefix + "-big";
		String small = prefix + "-small";
		createTopic(big, 1);
		createTopic(small, 1);
		produce(big, 0, 1500);
		produce(small, 0, 60);
		String g1 = prefix + "-g1";
		String g2 = prefix + "-g2";
		String g3 = prefix + "-g3";
		emptyGroup(g1, big, Map.of(0, 0L));
		alter(g1, small, Map.of(0, 10L));
		alter(g2, small, Map.of(0, 0L));
		alter(g3, small, Map.of(0, 60L));

		JsonNode def = ok("consumer_groups_with_lag", args("filter", "^" + prefix));
		assertThat(def.get("threshold").asLong()).isEqualTo(1000);
		assertThat(def.get("scannedGroups").asInt()).isEqualTo(3);
		assertThat(groupIds(def)).containsExactly(g1);
		JsonNode row = def.at("/groups/0");
		assertThat(row.get("totalLag").asLong()).isEqualTo(1550);
		assertThat(row.get("state").asText()).isEqualTo("Empty");
		assertThat(row.get("topicCount").asInt()).isEqualTo(2);
		assertThat(row.at("/topTopics/0/topic").asText()).isEqualTo(big);
		assertThat(row.at("/topTopics/0/lag").asLong()).isEqualTo(1500);
		assertThat(row.at("/topTopics/1/lag").asLong()).isEqualTo(50);

		JsonNode low = ok("consumer_groups_with_lag", args("filter", "^" + prefix, "threshold", 1));
		assertThat(groupIds(low)).containsExactly(g1, g2);

		JsonNode onlySmall = ok("consumer_groups_with_lag",
				args("filter", "^" + prefix, "threshold", 1, "topic", "-small$"));
		assertThat(groupIds(onlySmall)).containsExactly(g2, g1);
		assertThat(onlySmall.at("/groups/0/totalLag").asLong()).isEqualTo(60);
		assertThat(onlySmall.at("/groups/1/totalLag").asLong()).isEqualTo(50);
	}

	@Test
	void lagTrendGrowingWhileProducing() throws Exception {
		String prefix = unique("trg");
		String topic = prefix + "-t";
		createTopic(topic, 1);
		produce(topic, 0, 10);
		String group = prefix + "-g";
		emptyGroup(group, topic, Map.of(0, 5L));

		JsonNode r;
		try (Background ignored = new Background(() -> produce(topic, 0, 3), 100)) {
			r = ok("consumer_groups_lag_trend", args("group", group, "intervalSeconds", 2));
		}
		assertThat(r.get("intervalSeconds").asInt()).isEqualTo(2);
		assertThat(r.get("elapsedSeconds").asDouble()).isGreaterThan(1.5);
		JsonNode total = r.get("total");
		assertThat(total.get("trend").asText()).isEqualTo("growing");
		assertThat(total.get("lagT1").asLong()).isGreaterThan(total.get("lagT0").asLong());
		assertThat(total.get("produceRate").asDouble()).isPositive();
		assertThat(total.get("consumeRate").asDouble()).isZero();
		assertThat(total.has("etaSeconds")).isFalse();
		JsonNode t = r.at("/topics/0");
		assertThat(t.get("topic").asText()).isEqualTo(topic);
		assertThat(t.get("trend").asText()).isEqualTo("growing");
		assertThat(t.get("partitions").asInt()).isEqualTo(1);
	}

	@Test
	void lagTrendShrinkingWhileCommitting() throws Exception {
		String prefix = unique("trs");
		String topic = prefix + "-t";
		createTopic(topic, 1);
		produce(topic, 0, 1000);
		String group = prefix + "-g";
		emptyGroup(group, topic, Map.of(0, 0L));

		AtomicLong position = new AtomicLong();
		JsonNode r;
		try (Background ignored = new Background(() -> {
			long next = Math.min(900, position.addAndGet(5));
			alter(group, topic, Map.of(0, next));
		}, 100)) {
			r = ok("consumer_groups_lag_trend", args("group", group, "intervalSeconds", 2));
		}
		JsonNode total = r.get("total");
		assertThat(total.get("trend").asText()).isEqualTo("shrinking");
		assertThat(total.get("consumeRate").asDouble()).isPositive();
		assertThat(total.get("produceRate").asDouble()).isZero();
		assertThat(total.get("etaSeconds").asLong()).isPositive();
		assertThat(r.get("state").asText()).isEqualTo("Empty");
	}

	@Test
	void lagTrendErrors() throws Exception {
		assertThat(error("consumer_groups_lag_trend", args("group", unique("nope"), "intervalSeconds", 1)))
			.contains("bulunamadı");
	}

	@Test
	void stuckFindsGroupsWhoseCommitsDoNotAdvance() throws Exception {
		String prefix = unique("stk");
		String busy = prefix + "-busy";
		String quiet = prefix + "-quiet";
		createTopic(busy, 1);
		createTopic(quiet, 1);
		produce(busy, 0, 10);
		produce(quiet, 0, 10);
		String stuck = prefix + "-stuck";
		String idle = prefix + "-idle";
		String caughtUp = prefix + "-caughtup";
		emptyGroup(stuck, busy, Map.of(0, 3L));
		alter(idle, quiet, Map.of(0, 4L));
		alter(caughtUp, quiet, Map.of(0, 10L));

		JsonNode r;
		try (Background ignored = new Background(() -> produce(busy, 0, 2), 100)) {
			r = ok("consumer_groups_stuck", args("filter", "^" + prefix, "intervalSeconds", 2));
		}
		assertThat(r.get("scannedGroups").asInt()).isEqualTo(3);
		assertThat(groupIds(r)).containsExactly(stuck);
		JsonNode g = r.at("/groups/0");
		assertThat(g.get("state").asText()).isEqualTo("Empty");
		assertThat(g.get("members").asInt()).isZero();
		assertThat(g.get("stuckPartitions").asInt()).isEqualTo(1);
		assertThat(g.get("fullyStuck").asBoolean()).isTrue();
		assertThat(g.get("stuckLag").asLong()).isGreaterThan(7);
		JsonNode p = g.at("/partitions/0");
		assertThat(p.get("topic").asText()).isEqualTo(busy);
		assertThat(p.get("committed").asLong()).isEqualTo(3);
		assertThat(p.get("endDelta").asLong()).isPositive();
	}

}
