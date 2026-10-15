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
	void findByMemberCapsGroupsMembersAndTopics() throws Exception {
		String prefix = unique("cgfc");
		String t1 = prefix + "-a";
		String t2 = prefix + "-b";
		createTopic(t1, 2);
		createTopic(t2, 2);
		String big = prefix + "-g0";
		joinAll(big, prefix + "-cid-big", 2, List.of(t1, t2));
		join(prefix + "-g1", prefix + "-cid-1", t1);
		join(prefix + "-g2", prefix + "-cid-2", t1);
		Map<String, Object> a = args("clientId", "^" + prefix + "-cid");

		JsonNode all = json(groupTools.findByMember(call("consumer_groups_find_by_member", a)));
		assertThat(all.get("matchedGroups").asInt()).isEqualTo(3);
		assertThat(all.get("matchedMembers").asInt()).isEqualTo(4);
		assertThat(all.has("groupsOmitted")).isFalse();
		assertThat(all.at("/groups/0/members").size()).isEqualTo(2);

		JsonNode capped = json(groupTools.findByMember(call("consumer_groups_find_by_member", a), 2, 1, 1));
		assertThat(capped.get("matchedGroups").asInt()).isEqualTo(3);
		assertThat(capped.get("matchedMembers").asInt()).isEqualTo(4);
		assertThat(capped.get("groupsOmitted").asInt()).isEqualTo(1);
		assertThat(texts(capped.get("groups"), "groupId")).containsExactly(big, prefix + "-g1");
		JsonNode g0 = capped.at("/groups/0");
		assertThat(g0.get("matchedMembers").asInt()).isEqualTo(2);
		assertThat(g0.get("members").size()).isEqualTo(1);
		assertThat(g0.get("membersOmitted").asInt()).isEqualTo(1);
		assertThat(g0.at("/members/0/topics").size()).isEqualTo(1);
		assertThat(g0.at("/members/0/topicsOmitted").asInt()).isEqualTo(1);
	}

	@Test
	void getAndLagBoundOutputForLargeAssignments() throws Exception {
		String prefix = unique("cgb");
		String t1 = prefix + "-a";
		String t2 = prefix + "-b";
		createTopic(t1, 3);
		createTopic(t2, 2);
		for (int p = 0; p < 3; p++) {
			produce(t1, p, 10);
		}
		produce(t2, 0, 50);
		String group = prefix + "-g";
		var consumer = joinAll(group, prefix + "-cid", 1, List.of(t1, t2)).get(0);
		commit(consumer, t1, Map.of(0, 1L, 1, 2L, 2, 3L));
		commit(consumer, t2, Map.of(0, 0L, 1, 0L));

		JsonNode full = json(groupTools.get(call("consumer_groups_get", args("group", group))));
		assertThat(full.has("partitionRowsOmitted")).isFalse();
		assertThat(full.at("/members/0").has("assignmentPartitionsOmitted")).isFalse();
		assertThat(full.at("/totals/lagByTopic").size()).isEqualTo(2);

		JsonNode get = json(groupTools.get(call("consumer_groups_get", args("group", group)), 2, 1));
		JsonNode member = get.at("/members/0");
		assertThat(member.get("assignedPartitions").asInt()).isEqualTo(5);
		assertThat(member.get("assignmentPartitionsOmitted").asInt()).isEqualTo(3);
		assertThat(member.at("/assignment/" + t1).toString()).isEqualTo("[0,1]");
		assertThat(member.at("/assignment").has(t2)).isFalse();
		assertThat(get.get("assignmentPartitionsOmitted").asInt()).isEqualTo(3);
		assertThat(get.get("offsets").size()).as("bütçe bitince kalan topic'ler boş dizi olarak eklenmemeli").isEqualTo(1);
		assertThat(get.at("/offsets/0/topic").asText()).isEqualTo(t1);
		assertThat(get.at("/offsets/0/partitions").size()).isEqualTo(2);
		assertThat(get.at("/offsets/0/partitionsOmitted").asInt()).isEqualTo(1);
		assertThat(get.get("partitionRowsOmitted").asInt()).isEqualTo(3);
		assertThat(get.get("topicsOmitted").asInt()).isEqualTo(1);
		assertThat(get.at("/totals/lag").asLong()).isEqualTo(74);
		assertThat(get.at("/totals/lagByTopic").size()).isEqualTo(1);
		assertThat(get.at("/totals/lagByTopic/" + t2).asLong()).isEqualTo(50);
		assertThat(get.at("/totals/lagByTopicOmitted").asInt()).isEqualTo(1);

		JsonNode lag = json(lagTools.lag(call("consumer_groups_lag", args("group", group)), 1));
		assertThat(lag.get("totalLag").asLong()).isEqualTo(74);
		assertThat(lag.get("topics").size()).isEqualTo(1);
		assertThat(lag.at("/topics/0/topic").asText()).isEqualTo(t2);
		assertThat(lag.at("/topics/0/partitions/0/lag").asLong()).isEqualTo(50);
		assertThat(lag.at("/topics/0/partitionsOmitted").asInt()).isEqualTo(1);
		assertThat(lag.get("topicsOmitted").asInt()).isEqualTo(1);
		assertThat(lag.get("omittedTopicsLag").asLong()).isEqualTo(24);
		assertThat(lag.get("partitionRowsOmitted").asInt()).isEqualTo(4);
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
