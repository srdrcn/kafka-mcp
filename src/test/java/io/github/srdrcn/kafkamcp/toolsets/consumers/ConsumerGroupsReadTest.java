package io.github.srdrcn.kafkamcp.toolsets.consumers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

/** consumer_groups_list, _get, _find_by_member, _empty. */
class ConsumerGroupsReadTest extends ConsumersTestBase {

	private static List<String> texts(JsonNode array, String field) {
		List<String> out = new ArrayList<>();
		array.forEach(n -> out.add(n.path(field).asText()));
		return out;
	}

	@Test
	void listFiltersByGroupIdAndState() throws Exception {
		String prefix = unique("cgl");
		String topic = prefix + "-t";
		createTopic(topic, 1);
		produce(topic, 0, 5);
		String active = prefix + "-active";
		String empty = prefix + "-empty";
		join(active, prefix + "-c1", topic);
		emptyGroup(empty, topic, Map.of(0, 2L));

		JsonNode all = ok("consumer_groups_list", args("filter", "^" + prefix));
		assertThat(all.get("total").asInt()).isEqualTo(2);
		assertThat(texts(all.get("groups"), "groupId")).containsExactly(active, empty);
		JsonNode a = all.get("groups").get(0);
		assertThat(a.get("state").asText()).isEqualTo("Stable");
		assertThat(a.get("type").asText()).isEqualTo("classic");
		assertThat(a.get("protocol").asText()).isEqualTo("consumer");
		assertThat(all.at("/byState/Stable").asInt()).isEqualTo(1);
		assertThat(all.at("/byState/Empty").asInt()).isEqualTo(1);

		JsonNode onlyEmpty = ok("consumer_groups_list", args("filter", "^" + prefix, "state", "Empty"));
		assertThat(onlyEmpty.get("total").asInt()).isEqualTo(1);
		assertThat(onlyEmpty.at("/groups/0/groupId").asText()).isEqualTo(empty);

		JsonNode limited = ok("consumer_groups_list", args("filter", "^" + prefix, "limit", 1));
		assertThat(limited.get("returned").asInt()).isEqualTo(1);
		assertThat(limited.get("truncated").asBoolean()).isTrue();

		assertThat(ok("consumer_groups_list", args("cluster", "test-ro", "filter", "^" + prefix)).get("total").asInt())
			.isEqualTo(2);
		assertThat(error("consumer_groups_list", args("state", "Sleeping"))).contains("Geçersiz state");
	}

	@Test
	void getShowsMembersOffsetsLagAndUncommittedPartitions() throws Exception {
		String prefix = unique("cgg");
		String topic = prefix + "-t";
		createTopic(topic, 3);
		for (int p = 0; p < 3; p++) {
			produce(topic, p, 10);
		}
		String group = prefix + "-g";
		String clientId = prefix + "-client";
		var consumer = join(group, clientId, topic);
		commit(consumer, topic, Map.of(0, 4L, 1, 10L));

		JsonNode r = ok("consumer_groups_get", args("group", group));
		assertThat(r.get("groupId").asText()).isEqualTo(group);
		assertThat(r.get("state").asText()).isEqualTo("Stable");
		assertThat(r.get("type").asText()).isEqualTo("classic");
		assertThat(r.at("/coordinator/id").asInt()).isEqualTo(1);
		assertThat(r.get("partitionAssignor").asText()).isNotBlank();
		assertThat(r.get("memberCount").asInt()).isEqualTo(1);
		JsonNode member = r.at("/members/0");
		assertThat(member.get("clientId").asText()).isEqualTo(clientId);
		assertThat(member.get("host").asText()).isNotBlank();
		assertThat(member.at("/assignment/" + topic).toString()).isEqualTo("[0,1,2]");
		String memberId = member.get("memberId").asText();

		JsonNode t = r.at("/offsets/0");
		assertThat(t.get("topic").asText()).isEqualTo(topic);
		assertThat(t.get("lag").asLong()).isEqualTo(6);
		JsonNode p0 = t.at("/partitions/0");
		assertThat(p0.get("partition").asInt()).isZero();
		assertThat(p0.get("committed").asLong()).isEqualTo(4);
		assertThat(p0.get("end").asLong()).isEqualTo(10);
		assertThat(p0.get("lag").asLong()).isEqualTo(6);
		assertThat(p0.get("owner").asText()).isEqualTo(memberId);
		JsonNode p1 = t.at("/partitions/1");
		assertThat(p1.get("lag").asLong()).isZero();
		JsonNode p2 = t.at("/partitions/2");
		assertThat(p2.get("noCommit").asBoolean()).isTrue();
		assertThat(p2.has("committed")).isFalse();
		assertThat(p2.has("lag")).isFalse();
		assertThat(p2.get("end").asLong()).isEqualTo(10);

		JsonNode totals = r.get("totals");
		assertThat(totals.get("lag").asLong()).isEqualTo(6);
		assertThat(totals.at("/lagByTopic/" + topic).asLong()).isEqualTo(6);
		assertThat(totals.get("partitions").asInt()).isEqualTo(3);
		assertThat(totals.get("partitionsWithoutCommit").asInt()).isEqualTo(1);
		assertThat(r.get("note").asText()).contains("noCommit");
	}

	@Test
	void getUnknownGroupFails() {
		assertThat(error("consumer_groups_get", args("group", unique("missing-group")))).contains("bulunamadı");
		assertThat(error("consumer_groups_get", args())).contains("group");
	}

	@Test
	void getEmptyGroupWithOffsets() throws Exception {
		String prefix = unique("cgge");
		String topic = prefix + "-t";
		createTopic(topic, 1);
		produce(topic, 0, 8);
		String group = prefix + "-g";
		emptyGroup(group, topic, Map.of(0, 3L));
		JsonNode r = ok("consumer_groups_get", args("group", group));
		assertThat(r.get("state").asText()).isEqualTo("Empty");
		assertThat(r.get("memberCount").asInt()).isZero();
		assertThat(r.at("/offsets/0/partitions/0/lag").asLong()).isEqualTo(5);
		assertThat(r.at("/offsets/0/partitions/0").has("owner")).isFalse();
		assertThat(r.at("/totals/lag").asLong()).isEqualTo(5);
	}

	@Test
	void findByMemberMatchesClientIdAndHost() throws Exception {
		String prefix = unique("cgf");
		String topic = prefix + "-t";
		createTopic(topic, 2);
		String group = prefix + "-g";
		String clientId = prefix + "-cid";
		join(group, clientId, topic);

		JsonNode byClient = ok("consumer_groups_find_by_member", args("clientId", "^" + clientId + "$"));
		assertThat(byClient.get("matchedGroups").asInt()).isEqualTo(1);
		assertThat(byClient.get("matchedMembers").asInt()).isEqualTo(1);
		assertThat(byClient.at("/groups/0/groupId").asText()).isEqualTo(group);
		assertThat(byClient.at("/groups/0/state").asText()).isEqualTo("Stable");
		JsonNode m = byClient.at("/groups/0/members/0");
		assertThat(m.get("clientId").asText()).isEqualTo(clientId);
		assertThat(m.get("memberId").asText()).isNotBlank();
		assertThat(m.get("topics").toString()).contains(topic);
		String host = m.get("host").asText();

		JsonNode both = ok("consumer_groups_find_by_member",
				args("clientId", clientId, "host", java.util.regex.Pattern.quote(host)));
		assertThat(both.get("matchedGroups").asInt()).isEqualTo(1);
		JsonNode none = ok("consumer_groups_find_by_member", args("clientId", clientId, "host", "^no-such-host$"));
		assertThat(none.get("matchedGroups").asInt()).isZero();
		assertThat(none.get("groups").size()).isZero();

		assertThat(error("consumer_groups_find_by_member", args())).contains("en az biri");
		assertThat(error("consumer_groups_find_by_member", args("clientId", "(["))).contains("Geçersiz regex");
	}

	@Test
	void emptyListsStaleGroupsWithLag() throws Exception {
		String prefix = unique("cge");
		String t1 = prefix + "-a";
		String t2 = prefix + "-b";
		createTopic(t1, 1);
		createTopic(t2, 1);
		produce(t1, 0, 20);
		produce(t2, 0, 5);
		String stale = prefix + "-stale";
		emptyGroup(stale, t1, Map.of(0, 5L));
		alter(stale, t2, Map.of(0, 5L));
		String active = prefix + "-active";
		join(active, prefix + "-c", t1);

		JsonNode r = ok("consumer_groups_empty", args("filter", "^" + prefix));
		assertThat(r.get("total").asInt()).isEqualTo(1);
		JsonNode g = r.at("/groups/0");
		assertThat(g.get("groupId").asText()).isEqualTo(stale);
		assertThat(g.get("totalLag").asLong()).isEqualTo(15);
		assertThat(g.get("topicCount").asInt()).isEqualTo(2);
		assertThat(texts(g.get("topics"), "topic")).containsExactly(t1, t2);
		assertThat(g.at("/topics/0/lag").asLong()).isEqualTo(15);
		assertThat(g.at("/topics/1/lag").asLong()).isZero();
	}

}
