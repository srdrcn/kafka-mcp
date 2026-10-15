package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.RecordsToDelete;
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

/** diagnose_consumer_group: KRaft ve ZooKeeper cluster'larında takılı/boş grup, lag, log başı ve ACL tespiti. */
class DiagnoseConsumerGroupTest extends DiagnoseTestBase {

	@Test
	void activeGroupWhoseCommitDoesNotAdvanceIsCriticalOnBothClusters() throws Exception {
		for (String cluster : CLUSTERS) {
			String topic = unique("diag-gstuck");
			String group = topic + "-g";
			createTopic(cluster, topic, 1, Map.of());
			produce(cluster, topic, 0, 10);
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
			assertThat(stuck.get("recommendation").asText()).contains("max.poll.interval.ms");
			assertThat(codes(r)).doesNotContain("NO_ACTIVE_MEMBERS", "LAG_GROWING");

			JsonNode facts = r.get("facts");
			assertThat(facts.get("state").asText()).isEqualTo("Stable");
			assertThat(facts.get("type").asText()).isEqualTo("classic");
			assertThat(facts.get("memberCount").asInt()).isEqualTo(1);
			assertThat(facts.at("/members/0/clientId").asText()).isEqualTo(group + "-client");
			assertThat(facts.at("/members/0/assignedPartitions").asInt()).isEqualTo(1);
			assertThat(facts.at("/coordinator/id").asInt()).isEqualTo(1);
			assertThat(facts.at("/lag/trend").asText()).isEqualTo("growing");
			assertThat(facts.at("/lag/stuckPartitions").asInt()).isEqualTo(1);
			assertThat(facts.at("/lag/produceRate").asDouble()).isPositive();
			assertThat(facts.at("/lagByTopic/0/topic").asText()).isEqualTo(topic);
			closeResources();
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
				}, "ACL görünmedi");

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
		assertThat(error("diagnose_consumer_group", args("group", unique("diag-nogroup"))))
			.contains("Consumer group bulunamadı");
		assertThat(error("diagnose_consumer_group", zkArgs("group", unique("diag-nogroup"))))
			.contains("Consumer group bulunamadı");
	}

}
