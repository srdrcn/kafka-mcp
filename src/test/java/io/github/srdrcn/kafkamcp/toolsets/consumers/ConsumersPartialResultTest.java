package io.github.srdrcn.kafkamcp.toolsets.consumers;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolHandler;

/**
 * Yanıt vermeyen grup koordinatörü ve leader'ı olmayan partition (bkz. {@link FaultyAdmin}) aracı
 * düşürmemeli: araç deadline'dan önce biter, sağlam grupları döner ve eksikleri {@code partial} ile sayar.
 */
class ConsumersPartialResultTest extends ConsumersTestBase {

	/** okGroup: A'da lag 20; hangGroup: A'da lag 15, koordinatörü yanıt vermez; partGroup: A'da lag 10 + H'de commit. */
	private record Scenario(String prefix, String topicA, String topicH, String okGroup, String hangGroup,
			String partGroup) {
	}

	private static Scenario scenario(String prefix) throws Exception {
		Scenario s = new Scenario(prefix, prefix + "-a", prefix + "-h", prefix + "-ok", prefix + "-hang",
				prefix + "-part");
		createTopic(s.topicA(), 1);
		createTopic(s.topicH(), 1);
		produce(s.topicA(), 0, 20);
		produce(s.topicH(), 0, 10);
		alter(s.okGroup(), s.topicA(), Map.of(0, 0L));
		alter(s.hangGroup(), s.topicA(), Map.of(0, 5L));
		alter(s.partGroup(), s.topicA(), Map.of(0, 10L));
		alter(s.partGroup(), s.topicH(), Map.of(0, 2L));
		return s;
	}

	/** Handler'ı arızalı admin'le ve verilen deadline ile çağırır; deadline'dan önce bitmeli. */
	private JsonNode run(ToolHandler handler, Map<String, Object> args, long deadlineMillis, Set<String> hangGroups,
			Set<String> hangTopics) throws Exception {
		Admin faulty = FaultyAdmin.wrap(testAdmin(), hangGroups, hangTopics);
		KafkaClients faultyClients = new KafkaClients(clients.config()) {
			@Override
			public Admin admin(String cluster) {
				return faulty;
			}
		};
		Instant deadline = Instant.now().plusMillis(deadlineMillis);
		ToolCall call = new ToolCall("test", args, faultyClients, deadline);
		Object result = handler.handle(call);
		assertThat(Instant.now()).isBefore(deadline);
		return json(result);
	}

	private static List<String> ids(JsonNode array) {
		List<String> out = new ArrayList<>();
		array.forEach(n -> out.add(n.get("groupId").asText()));
		return out;
	}

	@Test
	void withLagReturnsAnsweredGroupsAndCountsTheRest() throws Exception {
		Scenario s = scenario(unique("pwl"));
		JsonNode r = run(lagTools::withLag, args("filter", "^" + s.prefix(), "threshold", 1), 8_000,
				Set.of(s.hangGroup()), Set.of(s.topicH()));
		assertThat(ids(r.get("groups"))).containsExactly(s.okGroup(), s.partGroup());
		assertThat(r.get("partial").asBoolean()).isTrue();
		assertThat(r.get("scannedGroups").asInt()).isEqualTo(3);
		assertThat(r.get("groupsWithoutResponse").asInt()).isEqualTo(1);
		assertThat(r.get("groupsWithUnknownLag").asInt()).isEqualTo(1);
		assertThat(r.get("partitionsWithoutEndOffset").asInt()).isEqualTo(1);
		assertThat(r.at("/endOffsetErrors/" + s.topicH()).asText()).contains("yanıt gelmedi");
		JsonNode part = r.at("/groups/1");
		assertThat(part.get("totalLag").asLong()).isEqualTo(10);
		assertThat(part.get("partitionsWithoutEndOffset").asInt()).isEqualTo(1);
	}

	@Test
	void withLagReportsGroupsWhoseLagIsUnknownEvenBelowThreshold() throws Exception {
		Scenario s = scenario(unique("pwu"));
		JsonNode r = run(lagTools::withLag, args("filter", "^" + s.prefix(), "topic", s.topicH() + "$"), 6_000,
				Set.of(), Set.of(s.topicH()));
		assertThat(r.get("total").asInt()).isZero();
		assertThat(r.get("partial").asBoolean()).isTrue();
		assertThat(r.get("groupsWithUnknownLag").asInt()).isEqualTo(1);
		assertThat(r.get("partitionsWithoutEndOffset").asInt()).isEqualTo(1);
		assertThat(r.at("/endOffsetErrors/" + s.topicH()).asText()).isNotBlank();
	}

	@Test
	void emptyReturnsAnsweredGroupsAndCountsTheRest() throws Exception {
		Scenario s = scenario(unique("pem"));
		JsonNode r = run(groupTools::empty, args("filter", "^" + s.prefix()), 8_000, Set.of(s.hangGroup()),
				Set.of(s.topicH()));
		assertThat(ids(r.get("groups"))).containsExactly(s.okGroup(), s.partGroup());
		assertThat(r.get("partial").asBoolean()).isTrue();
		assertThat(r.get("groupsWithoutResponse").asInt()).isEqualTo(1);
		assertThat(r.get("partitionsWithoutEndOffset").asInt()).isEqualTo(1);
		assertThat(r.at("/groups/1/partitionsWithoutEndOffset").asInt()).isEqualTo(1);
		assertThat(r.at("/groups/0/totalLag").asLong()).isEqualTo(20);
	}

	@Test
	void lagAndGetAnswerWhenOnePartitionHasNoLeader() throws Exception {
		Scenario s = scenario(unique("plg"));
		JsonNode lag = run(lagTools::lag, args("group", s.partGroup()), 6_000, Set.of(), Set.of(s.topicH()));
		assertThat(lag.get("partial").asBoolean()).isTrue();
		assertThat(lag.get("totalLag").asLong()).isEqualTo(10);
		assertThat(lag.get("partitionsWithoutEndOffset").asInt()).isEqualTo(1);
		JsonNode h = lag.at("/topics/1");
		assertThat(h.get("topic").asText()).isEqualTo(s.topicH());
		assertThat(h.at("/partitions/0/committed").asLong()).isEqualTo(2);
		assertThat(h.at("/partitions/0").has("end")).isFalse();
		assertThat(h.at("/partitions/0/error").asText()).contains("yanıt gelmedi");

		JsonNode get = run(groupTools::get, args("group", s.partGroup()), 6_000, Set.of(), Set.of(s.topicH()));
		assertThat(get.get("partial").asBoolean()).isTrue();
		assertThat(get.at("/totals/lag").asLong()).isEqualTo(10);
		assertThat(get.at("/totals/partitionsWithoutEndOffset").asInt()).isEqualTo(1);
	}

	@Test
	void stuckDetectsStuckGroupsDespiteUnansweredGroupAndPartition() throws Exception {
		Scenario s = scenario(unique("pst"));
		JsonNode r;
		try (Background ignored = new Background(() -> produce(s.topicA(), 0, 2), 100)) {
			r = run(lagTools::stuck, args("filter", "^" + s.prefix(), "intervalSeconds", 1), 10_000,
					Set.of(s.hangGroup()), Set.of(s.topicH()));
		}
		assertThat(r.get("partial").asBoolean()).isTrue();
		assertThat(r.get("groupsWithoutResponse").asInt()).isEqualTo(1);
		assertThat(r.get("partitionsWithoutEndOffset").asInt()).isEqualTo(1);
		assertThat(r.get("scannedGroups").asInt()).isEqualTo(2);
		assertThat(ids(r.get("groups"))).containsExactlyInAnyOrder(s.okGroup(), s.partGroup());
		JsonNode part = r.get("groups").get(0).get("groupId").asText().equals(s.partGroup()) ? r.at("/groups/0")
				: r.at("/groups/1");
		assertThat(part.get("uncheckedPartitions").asInt()).isEqualTo(1);
		assertThat(part.get("fullyStuck").asBoolean()).isFalse();
		assertThat(part.at("/partitions/0/topic").asText()).isEqualTo(s.topicA());
	}

	@Test
	void stuckWithoutTimeForSecondSampleReturnsFirstSample() throws Exception {
		Scenario s = scenario(unique("pss"));
		JsonNode r = run(lagTools::stuck, args("filter", "^" + s.prefix(), "intervalSeconds", 1), 4_300, Set.of(),
				Set.of());
		assertThat(r.get("partial").asBoolean()).isTrue();
		assertThat(r.get("secondSample").asBoolean()).isFalse();
		assertThat(r.get("note").asText()).contains("İkinci örnek");
		assertThat(ids(r.get("laggingAtFirstSample"))).containsExactly(s.okGroup(), s.partGroup(), s.hangGroup());
		assertThat(r.at("/laggingAtFirstSample/0/lag").asLong()).isEqualTo(20);
		assertThat(r.at("/laggingAtFirstSample/1/lag").asLong()).isEqualTo(18);
	}

	@Test
	void lagTrendSkipsPartitionWithoutLeader() throws Exception {
		Scenario s = scenario(unique("ptr"));
		JsonNode r = run(lagTools::lagTrend, args("group", s.partGroup(), "intervalSeconds", 1), 10_000, Set.of(),
				Set.of(s.topicH()));
		assertThat(r.get("partial").asBoolean()).isTrue();
		assertThat(r.get("partitionsSkipped").asInt()).isEqualTo(1);
		assertThat(r.get("partitionsWithoutEndOffset").asInt()).isEqualTo(1);
		assertThat(r.get("topics").size()).isEqualTo(1);
		assertThat(r.at("/topics/0/topic").asText()).isEqualTo(s.topicA());
		assertThat(r.at("/total/lagT0").asLong()).isEqualTo(10);
	}

	@Test
	void findByMemberCountsGroupsWithoutResponse() throws Exception {
		String prefix = unique("pfm");
		String topic = prefix + "-t";
		createTopic(topic, 1);
		String ok = prefix + "-ok";
		String hang = prefix + "-hang";
		join(ok, prefix + "-cid-ok", topic);
		join(hang, prefix + "-cid-hang", topic);
		JsonNode r = run(groupTools::findByMember, args("clientId", "^" + prefix + "-cid"), 6_000, Set.of(hang),
				Set.of());
		assertThat(r.get("partial").asBoolean()).isTrue();
		assertThat(r.get("groupsWithoutResponse").asInt()).isEqualTo(1);
		assertThat(r.get("matchedGroups").asInt()).isEqualTo(1);
		assertThat(r.at("/groups/0/groupId").asText()).isEqualTo(ok);
	}

}
