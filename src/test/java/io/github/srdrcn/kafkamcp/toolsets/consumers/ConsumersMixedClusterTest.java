package io.github.srdrcn.kafkamcp.toolsets.consumers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * consumers read tools on a ZooKeeper-mode cluster (Kafka 3.9, classic group coordinator, AclAuthorizer): list,
 * get, find_by_member, empty, acls, lag, with_lag, lag_trend, stuck. With real (classic protocol) consumer
 * groups, "simple" groups committed via admin, and real data.
 */
class ConsumersMixedClusterTest extends ConsumersMixedTestBase {

	/** Commits through the member that owns the partition (the classic protocol validates the generation). */
	private static void commitByOwner(List<KafkaConsumer<byte[], byte[]>> members, String topic,
			Map<Integer, Long> offsets) {
		for (KafkaConsumer<byte[], byte[]> c : members) {
			Map<Integer, Long> owned = new HashMap<>();
			offsets.forEach((p, o) -> {
				if (c.assignment().contains(new TopicPartition(topic, p))) {
					owned.put(p, o);
				}
			});
			if (!owned.isEmpty()) {
				commit(c, topic, owned);
			}
		}
	}

	/**
	 * On the ZooKeeper cluster: a topic with 2 partitions (p0: 10, p1: 6 messages); groups active (2 members,
	 * Stable, commit {0:4, 1:1}), empty (Empty, commit {0:2, 1:6}) and simple (admin commit {0:10}).
	 */
	private record Fixture(String prefix, String topic, String active, String empty, String simple,
			List<KafkaConsumer<byte[], byte[]>> members) {
	}

	private Fixture zkFixture() throws Exception {
		String prefix = unique("zkc");
		String topic = prefix + "-t";
		createTopic(ZK, topic, 2);
		produce(ZK, topic, 0, 10);
		produce(ZK, topic, 1, 6);
		String empty = prefix + "-b-empty";
		emptyGroup(ZK, CLASSIC, empty, topic, Map.of(0, 2L, 1, 6L));
		String simple = prefix + "-c-simple";
		alter(ZK, simple, topic, Map.of(0, 10L));
		String active = prefix + "-a-active";
		List<KafkaConsumer<byte[], byte[]>> members = joinAll(ZK, CLASSIC, active, prefix + "-cli", 2, List.of(topic));
		commitByOwner(members, topic, Map.of(0, 4L, 1, 1L));
		return new Fixture(prefix, topic, active, empty, simple, members);
	}

	@Test
	void listGetFindByMemberAndEmptyOnZooKeeper() throws Exception {
		Fixture f = zkFixture();

		JsonNode list = ok("consumer_groups_list", on(ZK, "filter", Pattern.quote(f.prefix())));
		assertThat(list.get("total").asInt()).isEqualTo(3);
		assertThat(list.has("partial")).isFalse();
		assertThat(list.get("byState").get("Stable").asInt()).isEqualTo(1);
		assertThat(list.get("byState").get("Empty").asInt()).isEqualTo(2);
		JsonNode active = list.at("/groups/0");
		assertThat(active.get("groupId").asText()).isEqualTo(f.active());
		assertThat(active.get("state").asText()).isEqualTo("Stable");
		// On ZooKeeper clusters consumer groups always use the classic protocol.
		assertThat(active.get("type").asText()).isEqualTo("classic");
		assertThat(active.get("protocol").asText()).isEqualTo("consumer");
		assertThat(active.has("simple")).isFalse();
		JsonNode empty = list.at("/groups/1");
		assertThat(empty.get("groupId").asText()).isEqualTo(f.empty());
		assertThat(empty.get("state").asText()).isEqualTo("Empty");
		assertThat(empty.get("type").asText()).isEqualTo("classic");
		JsonNode simple = list.at("/groups/2");
		assertThat(simple.get("groupId").asText()).isEqualTo(f.simple());
		assertThat(simple.get("state").asText()).isEqualTo("Empty");
		assertThat(simple.get("simple").asBoolean()).isTrue();
		assertThat(simple.has("protocol")).isFalse();

		JsonNode stable = ok("consumer_groups_list", on(ZK, "filter", Pattern.quote(f.prefix()), "state", "stable"));
		assertThat(stable.get("total").asInt()).isEqualTo(1);
		assertThat(stable.at("/groups/0/groupId").asText()).isEqualTo(f.active());
		// KIP-848 states never occur on ZooKeeper; the filter returns an empty result without an error.
		JsonNode reconciling = ok("consumer_groups_list",
				on(ZK, "filter", Pattern.quote(f.prefix()), "state", "Reconciling"));
		assertThat(reconciling.get("total").asInt()).isZero();

		JsonNode get = ok("consumer_groups_get", on(ZK, "group", f.active()));
		assertThat(get.get("state").asText()).isEqualTo("Stable");
		assertThat(get.get("type").asText()).isEqualTo("classic");
		assertThat(get.get("partitionAssignor").asText()).isEqualTo("range");
		assertThat(get.at("/coordinator/id").asInt()).isEqualTo(1);
		assertThat(get.at("/coordinator/host").asText()).isEqualTo("localhost");
		// the classic protocol has no KIP-848 epoch fields.
		assertThat(get.has("groupEpoch")).isFalse();
		assertThat(get.has("targetAssignmentEpoch")).isFalse();
		assertThat(get.get("memberCount").asInt()).isEqualTo(2);
		List<String> clientIds = new ArrayList<>();
		List<Integer> assigned = new ArrayList<>();
		for (JsonNode m : get.get("members")) {
			clientIds.add(m.get("clientId").asText());
			assertThat(m.get("memberId").asText()).startsWith(m.get("clientId").asText());
			assertThat(m.get("host").asText()).isNotBlank();
			assertThat(m.has("memberEpoch")).isFalse();
			assertThat(m.get("assignment").get(f.topic())).hasSize(1);
			assigned.add(m.get("assignment").get(f.topic()).get(0).asInt());
		}
		assertThat(clientIds).containsExactly(f.prefix() + "-cli-0", f.prefix() + "-cli-1");
		assertThat(assigned).containsExactlyInAnyOrder(0, 1);
		JsonNode offsets = get.at("/offsets/0");
		assertThat(offsets.get("topic").asText()).isEqualTo(f.topic());
		assertThat(offsets.get("lag").asLong()).isEqualTo(11);
		JsonNode p0 = offsets.at("/partitions/0");
		assertThat(p0.get("partition").asInt()).isZero();
		assertThat(p0.get("committed").asLong()).isEqualTo(4);
		assertThat(p0.get("end").asLong()).isEqualTo(10);
		assertThat(p0.get("lag").asLong()).isEqualTo(6);
		assertThat(p0.get("owner").asText()).startsWith(f.prefix() + "-cli-");
		JsonNode p1 = offsets.at("/partitions/1");
		assertThat(p1.get("committed").asLong()).isEqualTo(1);
		assertThat(p1.get("end").asLong()).isEqualTo(6);
		assertThat(p1.get("lag").asLong()).isEqualTo(5);
		assertThat(get.at("/totals/lag").asLong()).isEqualTo(11);
		assertThat(get.at("/totals/partitions").asInt()).isEqualTo(2);
		assertThat(get.has("partial")).isFalse();

		JsonNode simpleGet = ok("consumer_groups_get", on(ZK, "group", f.simple()));
		assertThat(simpleGet.get("state").asText()).isEqualTo("Empty");
		assertThat(simpleGet.get("simple").asBoolean()).isTrue();
		assertThat(simpleGet.get("memberCount").asInt()).isZero();
		assertThat(simpleGet.at("/offsets/0/partitions/0/lag").asLong()).isZero();

		JsonNode byClient = ok("consumer_groups_find_by_member",
				on(ZK, "clientId", "^" + Pattern.quote(f.prefix() + "-cli-1") + "$"));
		assertThat(byClient.get("matchedGroups").asInt()).isEqualTo(1);
		assertThat(byClient.get("matchedMembers").asInt()).isEqualTo(1);
		assertThat(byClient.at("/groups/0/groupId").asText()).isEqualTo(f.active());
		assertThat(byClient.at("/groups/0/state").asText()).isEqualTo("Stable");
		JsonNode member = byClient.at("/groups/0/members/0");
		assertThat(member.get("clientId").asText()).isEqualTo(f.prefix() + "-cli-1");
		assertThat(member.get("topics")).hasSize(1);
		assertThat(member.at("/topics/0").asText()).isEqualTo(f.topic());
		String host = member.get("host").asText();
		JsonNode byHost = ok("consumer_groups_find_by_member", on(ZK, "host", "^" + Pattern.quote(host) + "$",
				"clientId", "^" + Pattern.quote(f.prefix() + "-cli-")));
		assertThat(byHost.get("matchedMembers").asInt()).isEqualTo(2);
		assertThat(byHost.at("/groups/0/members")).hasSize(2);

		JsonNode emptyGroups = ok("consumer_groups_empty", on(ZK, "filter", Pattern.quote(f.prefix())));
		assertThat(emptyGroups.get("total").asInt()).isEqualTo(2);
		assertThat(emptyGroups.has("partial")).isFalse();
		JsonNode e0 = emptyGroups.at("/groups/0");
		assertThat(e0.get("groupId").asText()).isEqualTo(f.empty());
		assertThat(e0.get("type").asText()).isEqualTo("classic");
		assertThat(e0.get("totalLag").asLong()).isEqualTo(8);
		assertThat(e0.get("partitions").asInt()).isEqualTo(2);
		assertThat(e0.at("/topics/0/topic").asText()).isEqualTo(f.topic());
		assertThat(e0.at("/topics/0/lag").asLong()).isEqualTo(8);
		JsonNode e1 = emptyGroups.at("/groups/1");
		assertThat(e1.get("groupId").asText()).isEqualTo(f.simple());
		assertThat(e1.get("simple").asBoolean()).isTrue();
		assertThat(e1.get("totalLag").asLong()).isZero();
	}

	@Test
	void lagAndWithLagOnZooKeeper() throws Exception {
		Fixture f = zkFixture();

		JsonNode lag = ok("consumer_groups_lag", on(ZK, "group", f.active()));
		assertThat(lag.get("state").asText()).isEqualTo("Stable");
		assertThat(lag.get("members").asInt()).isEqualTo(2);
		assertThat(lag.get("totalLag").asLong()).isEqualTo(11);
		assertThat(lag.has("partial")).isFalse();
		JsonNode topic = lag.at("/topics/0");
		assertThat(topic.get("topic").asText()).isEqualTo(f.topic());
		assertThat(topic.get("lag").asLong()).isEqualTo(11);
		// partitions by lag, descending: p0 (6), p1 (5)
		JsonNode p0 = topic.at("/partitions/0");
		assertThat(p0.get("partition").asInt()).isZero();
		assertThat(p0.get("committed").asLong()).isEqualTo(4);
		assertThat(p0.get("end").asLong()).isEqualTo(10);
		assertThat(p0.get("lag").asLong()).isEqualTo(6);
		assertThat(p0.get("clientId").asText()).startsWith(f.prefix() + "-cli-");
		assertThat(p0.get("host").asText()).isNotBlank();
		assertThat(topic.at("/partitions/1/partition").asInt()).isEqualTo(1);
		assertThat(topic.at("/partitions/1/lag").asLong()).isEqualTo(5);

		JsonNode emptyLag = ok("consumer_groups_lag", on(ZK, "group", f.empty(), "topic", f.topic()));
		assertThat(emptyLag.get("state").asText()).isEqualTo("Empty");
		assertThat(emptyLag.get("members").asInt()).isZero();
		assertThat(emptyLag.get("totalLag").asLong()).isEqualTo(8);
		assertThat(emptyLag.at("/topics/0/partitions/0/lag").asLong()).isEqualTo(8);
		assertThat(emptyLag.at("/topics/0/partitions/0").has("clientId")).isFalse();
		assertThat(emptyLag.at("/topics/0/partitions/1/lag").asLong()).isZero();
		assertThat(error("consumer_groups_lag", on(ZK, "group", f.empty(), "topic", f.topic() + "-other")))
			.contains("has no commits or assignments");

		JsonNode withLag = ok("consumer_groups_with_lag",
				on(ZK, "filter", Pattern.quote(f.prefix()), "threshold", 5));
		assertThat(withLag.get("scannedGroups").asInt()).isEqualTo(3);
		assertThat(withLag.get("total").asInt()).isEqualTo(2);
		assertThat(withLag.has("partial")).isFalse();
		assertThat(withLag.at("/groups/0/groupId").asText()).isEqualTo(f.active());
		assertThat(withLag.at("/groups/0/state").asText()).isEqualTo("Stable");
		assertThat(withLag.at("/groups/0/totalLag").asLong()).isEqualTo(11);
		assertThat(withLag.at("/groups/0/topTopics/0/topic").asText()).isEqualTo(f.topic());
		assertThat(withLag.at("/groups/1/groupId").asText()).isEqualTo(f.empty());
		assertThat(withLag.at("/groups/1/totalLag").asLong()).isEqualTo(8);

		JsonNode high = ok("consumer_groups_with_lag", on(ZK, "filter", Pattern.quote(f.prefix()), "threshold", 9));
		assertThat(high.get("total").asInt()).isEqualTo(1);
		assertThat(high.at("/groups/0/groupId").asText()).isEqualTo(f.active());

		assertThat(error("consumer_groups_lag", on(ZK, "group", unique("zk-missing")))).contains("not found");
		assertThat(error("consumer_groups_get", on(ZK, "group", unique("zk-missing")))).contains("not found");
		assertThat(error("consumer_groups_lag_trend", on(ZK, "group", unique("zk-missing"), "intervalSeconds", 1)))
			.contains("not found");
	}

	@Test
	void lagTrendAndStuckOnZooKeeper() throws Exception {
		String prefix = unique("zks");
		String topic = prefix + "-t";
		createTopic(ZK, topic, 2);
		produce(ZK, topic, 0, 20);
		produce(ZK, topic, 1, 5);
		String group = prefix + "-g";
		emptyGroup(ZK, CLASSIC, group, topic, Map.of(0, 5L, 1, 5L));

		JsonNode trend;
		JsonNode stuck;
		try (Background b = new Background(() -> produce(ZK, topic, 0, 5), 100)) {
			trend = ok("consumer_groups_lag_trend", on(ZK, "group", group, "intervalSeconds", 2));
			stuck = ok("consumer_groups_stuck", on(ZK, "filter", Pattern.quote(prefix), "intervalSeconds", 2));
		}

		assertThat(trend.get("state").asText()).isEqualTo("Empty");
		assertThat(trend.get("members").asInt()).isZero();
		assertThat(trend.get("elapsedSeconds").asDouble()).isPositive();
		JsonNode total = trend.get("total");
		assertThat(total.get("lagT0").asLong()).isGreaterThanOrEqualTo(15);
		assertThat(total.get("lagT1").asLong()).isGreaterThan(total.get("lagT0").asLong());
		assertThat(total.get("trend").asText()).isEqualTo("growing");
		assertThat(total.get("consumeRate").asDouble()).isZero();
		assertThat(total.get("produceRate").asDouble()).isPositive();
		assertThat(trend.at("/topics/0/topic").asText()).isEqualTo(topic);
		assertThat(trend.at("/topics/0/partitions").asInt()).isEqualTo(2);

		assertThat(stuck.get("scannedGroups").asInt()).isEqualTo(1);
		assertThat(stuck.get("total").asInt()).isEqualTo(1);
		assertThat(stuck.has("partial")).isFalse();
		JsonNode g = stuck.at("/groups/0");
		assertThat(g.get("groupId").asText()).isEqualTo(group);
		assertThat(g.get("state").asText()).isEqualTo("Empty");
		assertThat(g.get("members").asInt()).isZero();
		assertThat(g.get("stuckPartitions").asInt()).isEqualTo(1);
		assertThat(g.get("fullyStuck").asBoolean()).isTrue();
		assertThat(g.at("/partitions/0/topic").asText()).isEqualTo(topic);
		assertThat(g.at("/partitions/0/partition").asInt()).isZero();
		assertThat(g.at("/partitions/0/committed").asLong()).isEqualTo(5);
		assertThat(g.at("/partitions/0/endDelta").asLong()).isPositive();
		assertThat(g.at("/partitions/0/lag").asLong()).isGreaterThan(15);
	}

	/** Literal, prefixed and wildcard ACLs: AclAuthorizer (ZooKeeper) and StandardAuthorizer (KRaft) agree. */
	@Test
	void aclsMatchLiteralPrefixedAndWildcardOnBothAuthorizers() throws Exception {
		String prefix = unique("zka");
		String group = prefix + "-g";
		List<AclBinding> bindings = List.of(
				new AclBinding(new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
						new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW)),
				new AclBinding(new ResourcePattern(ResourceType.GROUP, prefix, PatternType.PREFIXED),
						new AccessControlEntry("User:bob", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
				new AclBinding(new ResourcePattern(ResourceType.GROUP, "*", PatternType.LITERAL),
						new AccessControlEntry("User:" + prefix, "10.0.0.1", AclOperation.READ,
								AclPermissionType.DENY)));
		Map<String, JsonNode> results = new HashMap<>();
		for (String cluster : List.of(ZK, KRAFT)) {
			Admin admin = admin(cluster);
			admin.createAcls(bindings).all().get(30, TimeUnit.SECONDS);
			try {
				JsonNode r = awaitAcls(cluster, group, 3);
				results.put(cluster, r);
				JsonNode none = ok("consumer_groups_acls", on(cluster, "group", unique("zka-none")));
				// the wildcard ACL matches every group
				assertThat(none.get("count").asInt()).as(cluster).isEqualTo(1);
				assertThat(none.at("/acls/0/resourceName").asText()).isEqualTo("*");
			}
			finally {
				admin.deleteAcls(bindings.stream().map(AclBinding::toFilter).toList())
					.all()
					.get(30, TimeUnit.SECONDS);
			}
		}
		JsonNode zk = results.get(ZK);
		assertThat(zk.get("group").asText()).isEqualTo(group);
		assertThat(zk.get("count").asInt()).isEqualTo(3);
		assertThat(zk.get("principals")).hasSize(3);
		assertThat(zk.at("/byPatternType/LITERAL").asInt()).isEqualTo(2);
		assertThat(zk.at("/byPatternType/PREFIXED").asInt()).isEqualTo(1);
		JsonNode alice = zk.at("/acls/0");
		assertThat(alice.get("principal").asText()).isEqualTo("User:alice");
		assertThat(alice.get("host").asText()).isEqualTo("*");
		assertThat(alice.get("operation").asText()).isEqualTo("READ");
		assertThat(alice.get("permission").asText()).isEqualTo("ALLOW");
		assertThat(alice.get("patternType").asText()).isEqualTo("LITERAL");
		assertThat(alice.get("resourceName").asText()).isEqualTo(group);
		JsonNode bob = zk.at("/acls/1");
		assertThat(bob.get("principal").asText()).isEqualTo("User:bob");
		assertThat(bob.get("operation").asText()).isEqualTo("DESCRIBE");
		assertThat(bob.get("patternType").asText()).isEqualTo("PREFIXED");
		assertThat(bob.get("resourceName").asText()).isEqualTo(prefix);
		JsonNode wildcard = zk.at("/acls/2");
		assertThat(wildcard.get("principal").asText()).isEqualTo("User:" + prefix);
		assertThat(wildcard.get("host").asText()).isEqualTo("10.0.0.1");
		assertThat(wildcard.get("permission").asText()).isEqualTo("DENY");
		assertThat(wildcard.get("patternType").asText()).isEqualTo("LITERAL");
		assertThat(wildcard.get("resourceName").asText()).isEqualTo("*");
		assertThat(zk.has("note")).isFalse();
		assertThat(results.get(KRAFT)).isEqualTo(zk);

		JsonNode afterDelete = awaitAcls(ZK, group, 0);
		assertThat(afterDelete.get("note").asText()).contains("No ACLs match");
	}

	/** ACL changes reach the authorizer asynchronously (ZooKeeper watch, KRaft metadata log). */
	private JsonNode awaitAcls(String cluster, String group, int expected) {
		JsonNode[] last = new JsonNode[1];
		waitUntil(() -> {
			last[0] = ok("consumer_groups_acls", on(cluster, "group", group));
			return last[0].get("count").asInt() == expected;
		}, "ACLs did not propagate: " + cluster);
		return last[0];
	}

}
