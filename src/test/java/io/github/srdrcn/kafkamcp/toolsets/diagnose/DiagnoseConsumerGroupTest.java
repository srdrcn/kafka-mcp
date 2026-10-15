package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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

import io.github.srdrcn.kafkamcp.support.TestConfigs;

/**
 * diagnose_consumer_group: detection of stuck/empty groups, a healthy group that commits rarely (no false
 * positive), lag, log start and ACLs on KRaft and ZooKeeper clusters.
 */
class DiagnoseConsumerGroupTest extends DiagnoseTestBase {

	@Test
	void activeGroupWhoseCommitDoesNotAdvanceIsCriticalOnBothClusters() throws Exception {
		for (String cluster : CLUSTERS) {
			String topic = unique("diag-gstuck");
			String group = topic + "-g";
			createTopic(cluster, topic, 1, Map.of());
			// backlog written 30 min ago: the consumer whose commit stays at 3 is really stuck (record age as evidence)
			produceAged(cluster, topic, 0, 10, 30 * 60_000L);
			joinAndCommit(cluster, group, topic, Map.of(0, 3L));
			produceInBackground(cluster, topic, 0);

			JsonNode r = ok("diagnose_consumer_group", on(cluster, "group", group, "intervalSeconds", 2));
			assertThat(r.get("status").asText()).as(r.toString()).isEqualTo("critical");
			assertThat(r.get("mode").asText()).isEqualTo(zk(cluster) ? "ZooKeeper" : "KRaft");
			JsonNode stuck = finding(r, "STUCK_PARTITIONS");
			assertThat(stuck.get("severity").asText()).isEqualTo("critical");
			assertThat(stuck.at("/evidence/members").asInt()).isEqualTo(1);
			JsonNode p = stuck.at("/evidence/partitions/0");
			assertThat(p.get("topic").asText()).isEqualTo(topic);
			assertThat(p.get("committed").asLong()).isEqualTo(3);
			assertThat(p.get("endDelta").asLong()).isPositive();
			assertThat(p.get("ageBasis").asText()).isEqualTo("recordAge");
			assertThat(p.get("oldestPendingAgeSeconds").asLong()).isGreaterThanOrEqualTo(StuckCheck.STUCK_SECONDS);
			assertThat(stuck.at("/evidence/windowSeconds").asDouble()).isPositive();
			assertThat(stuck.get("recommendation").asText()).contains("max.poll.interval.ms");
			assertThat(codes(r)).doesNotContain("NO_ACTIVE_MEMBERS", "LAG_GROWING", "STUCK_SUSPECT");

			JsonNode facts = r.get("facts");
			assertThat(facts.get("state").asText()).isEqualTo("Stable");
			assertThat(facts.get("type").asText()).isEqualTo("classic");
			assertThat(facts.get("memberCount").asInt()).isEqualTo(1);
			assertThat(facts.at("/members/0/clientId").asText()).isEqualTo(group + "-client");
			assertThat(facts.at("/members/0/assignedPartitions").asInt()).isEqualTo(1);
			assertThat(facts.at("/coordinator/id").asInt()).isEqualTo(1);
			assertThat(facts.at("/lag/trend").asText()).isEqualTo("growing");
			assertThat(facts.at("/lag/stuckPartitions").asInt()).isEqualTo(1);
			assertThat(facts.at("/lag/commitNotAdvancedPartitions").asInt()).as(r.toString()).isEqualTo(1);
			assertThat(facts.at("/lag/produceRate").asDouble()).isPositive();
			assertThat(facts.at("/lagByTopic/0/topic").asText()).isEqualTo(topic);
			closeResources();
		}
	}

	/**
	 * Healthy group that commits rarely (like Streams 30 s, Connect sink 60 s): the commit does not advance in the
	 * measurement window and the lag looks like growing; since the pending records are recent, there is no critical
	 * stuck finding or growing-lag warning, only an info note.
	 */
	@Test
	void healthyGroupThatCommitsRarelyIsNotReportedAsStuckOnBothClusters() throws Exception {
		for (String cluster : CLUSTERS) {
			String topic = unique("diag-grare");
			String group = topic + "-g";
			createTopic(cluster, topic, 1, Map.of());
			produceInBackground(cluster, topic, 0);
			consumeWithRareCommits(cluster, group, topic);

			// no intervalSeconds: default window (5 s)
			JsonNode r = ok("diagnose_consumer_group", on(cluster, "group", group));
			assertThat(r.get("status").asText()).as(r.toString()).isNotEqualTo("critical");
			assertThat(codes(r)).as(r.toString())
				.doesNotContain("STUCK_PARTITIONS", "STUCK_SUSPECT", "LAG_GROWING", "NO_ACTIVE_MEMBERS");
			JsonNode facts = r.get("facts");
			assertThat(facts.get("state").asText()).isEqualTo("Stable");
			assertThat(facts.at("/lag/stuckPartitions").asInt()).isZero();
			// no commit in the window, new messages arrive: stuck candidate but the pending records are recent → info note only
			assertThat(facts.at("/lag/commitNotAdvancedPartitions").asInt()).as(r.toString()).isEqualTo(1);
			JsonNode note = finding(r, "COMMIT_NOT_ADVANCED");
			assertThat(note.get("severity").asText()).isEqualTo("info");
			assertThat(note.get("message").asText()).contains("periodic commit");
			assertThat(note.at("/evidence/partitions/0/ageBasis").asText()).isEqualTo("recordAge");
			assertThat(note.at("/evidence/partitions/0/oldestPendingAgeSeconds").asLong())
				.isLessThan(StuckCheck.COMMIT_ALLOWANCE_SECONDS);
			// the topic diagnosis does not count the same group as stuck/growing either
			JsonNode t = ok("diagnose_topic", on(cluster, "topic", topic));
			assertThat(codes(t)).as(t.toString())
				.doesNotContain("CONSUMER_STUCK", "CONSUMER_STUCK_SUSPECT", "CONSUMER_LAG_GROWING");
			assertThat(t.at("/facts/consumers/groups/0/groupId").asText()).isEqualTo(group);
			assertThat(t.at("/facts/consumers/groups/0/stuckPartitions").asInt()).isZero();
			closeResources();
		}
	}

	/** The ACL check evaluates at most 20 topics; the rest and the lists not shown are reported as counts. */
	@Test
	void aclCheckReportsTopicsItDidNotEvaluate() throws Exception {
		String cluster = TestConfigs.CLUSTER;
		String prefix = unique("diag-gacl");
		String group = prefix + "-g";
		List<NewTopic> topics = new ArrayList<>();
		Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
		for (int i = 0; i <= DiagnoseGroupTools.MAX_ACL_TOPICS; i++) {
			String t = prefix + "-" + String.format("%02d", i);
			topics.add(new NewTopic(t, 1, (short) 1));
			offsets.put(new TopicPartition(t, 0), new OffsetAndMetadata(0));
		}
		admin(cluster).createTopics(topics).all().get(60, TimeUnit.SECONDS);
		admin(cluster).alterConsumerGroupOffsets(group, offsets).all().get(60, TimeUnit.SECONDS);
		AclBinding groupRead = new AclBinding(new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
				new AccessControlEntry("User:bob", "*", AclOperation.READ, AclPermissionType.ALLOW));
		admin(cluster).createAcls(List.of(groupRead)).all().get(30, TimeUnit.SECONDS);
		try {
			waitUntil(() -> {
				try {
					return admin(cluster).describeAcls(groupRead.toFilter()).values().get(30, TimeUnit.SECONDS)
						.size() == 1;
				}
				catch (Exception e) {
					return false;
				}
			}, "ACL not visible");
			JsonNode r = ok("diagnose_consumer_group", on(cluster, "group", group, "intervalSeconds", 1));
			String last = prefix + "-" + String.format("%02d", DiagnoseGroupTools.MAX_ACL_TOPICS);
			JsonNode skipped = finding(r, "ACL_TOPICS_NOT_EVALUATED");
			assertThat(skipped.get("severity").asText()).isEqualTo("info");
			assertThat(skipped.get("message").asText()).startsWith("1 topic").contains("at most 20");
			assertThat(texts(skipped.at("/evidence/topics"))).containsExactly(last);
			JsonNode missing = finding(r, "ACL_TOPIC_READ_MISSING");
			assertThat(missing.at("/evidence/principals/0/principal").asText()).isEqualTo("User:bob");
			assertThat(missing.at("/evidence/principals/0/topics").size()).isEqualTo(DiagnoseParams.MAX_EVIDENCE);
			assertThat(missing.at("/evidence/principals/0/topicsOmitted").asInt())
				.isEqualTo(DiagnoseGroupTools.MAX_ACL_TOPICS - DiagnoseParams.MAX_EVIDENCE);
			JsonNode facts = r.get("facts");
			assertThat(facts.at("/access/topicsEvaluated").asInt()).isEqualTo(DiagnoseGroupTools.MAX_ACL_TOPICS);
			assertThat(facts.at("/access/topicsNotEvaluated").asInt()).isEqualTo(1);
			assertThat(facts.get("topics").size()).isEqualTo(DiagnoseGroupTools.MAX_TOPICS);
			assertThat(facts.get("topicsOmitted").asInt()).isEqualTo(1);
			assertThat(r.has("partial")).as("the cap is sampling, not a timeout").isFalse();
		}
		finally {
			admin(cluster).deleteAcls(List.of(groupRead.toFilter())).all().get(30, TimeUnit.SECONDS);
		}
	}

	@Test
	void emptyGroupBehindLogStartWithLagAndAclGapsOnBothClusters() throws Exception {
		for (String cluster : CLUSTERS) {
			String topic = unique("diag-gempty");
			String group = topic + "-g";
			createTopic(cluster, topic, 1, Map.of());
			produce(cluster, topic, 0, 10);
			admin(cluster).deleteRecords(Map.of(new TopicPartition(topic, 0), RecordsToDelete.beforeOffset(5)))
				.all()
				.get(30, TimeUnit.SECONDS);
			commit(cluster, group, topic, Map.of(0, 2L));
			AclBinding groupRead = new AclBinding(new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
					new AccessControlEntry("User:bob", "*", AclOperation.READ, AclPermissionType.ALLOW));
			admin(cluster).createAcls(List.of(groupRead)).all().get(30, TimeUnit.SECONDS);
			try {
				waitUntil(() -> {
					try {
						return admin(cluster).describeAcls(groupRead.toFilter()).values().get(30, TimeUnit.SECONDS)
							.size() == 1;
					}
					catch (Exception e) {
						return false;
					}
				}, "ACL not visible");

				JsonNode r = ok("diagnose_consumer_group", on(cluster, "group", group, "intervalSeconds", 1));
				assertThat(r.get("status").asText()).as(r.toString()).isEqualTo("warning");
				JsonNode empty = finding(r, "NO_ACTIVE_MEMBERS");
				assertThat(empty.get("message").asText()).contains("EMPTY", "8");
				assertThat(empty.get("recommendation").asText()).contains("consumer_groups_offsets_reset", "EMPTY");
				JsonNode lost = finding(r, "COMMITTED_BEFORE_LOG_START");
				assertThat(texts(lost.at("/evidence/partitions"))).containsExactly(topic + "-0");
				assertThat(lost.get("recommendation").asText()).contains("to=earliest");
				JsonNode acl = finding(r, "ACL_TOPIC_READ_MISSING");
				assertThat(acl.at("/evidence/principals/0/principal").asText()).isEqualTo("User:bob");
				assertThat(texts(acl.at("/evidence/principals/0/topics"))).containsExactly(topic);
				assertThat(codes(r)).doesNotContain("STUCK_PARTITIONS", "NO_GROUP_ACLS");

				JsonNode facts = r.get("facts");
				assertThat(facts.get("state").asText()).isEqualTo("Empty");
				assertThat(facts.get("memberCount").asInt()).isZero();
				assertThat(facts.at("/lag/lag").asLong()).isEqualTo(8);
				assertThat(facts.at("/lag/trend").asText()).isEqualTo("stable");
				assertThat(texts(facts.get("topics"))).containsExactly(topic);
				assertThat(texts(facts.at("/access/groupReaders"))).containsExactly("User:bob");
			}
			finally {
				admin(cluster).deleteAcls(List.of(groupRead.toFilter())).all().get(30, TimeUnit.SECONDS);
			}
		}
	}

	@Test
	void unknownGroupIsAnError() {
		// the message comes from the consumers toolset; the group name and the hint tool name are wording-independent
		String group = unique("diag-nogroup");
		assertThat(error("diagnose_consumer_group", args("group", group))).contains(group, "consumer_groups_list");
		String zkGroup = unique("diag-nogroup");
		assertThat(error("diagnose_consumer_group", zkArgs("group", zkGroup))).contains(zkGroup, "consumer_groups_list");
	}

}
