package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

/** diagnose_topic: KRaft ve ZooKeeper cluster'larında gerçek sorunların tespiti. */
class DiagnoseTopicTest extends DiagnoseTestBase {

	@Test
	void minInsyncReplicasAboveReplicationFactorIsCriticalOnBothClusters() throws Exception {
		for (String cluster : CLUSTERS) {
			String topic = unique("diag-minisr");
			createTopic(cluster, topic, 2, Map.of("min.insync.replicas", "2"));
			produce(cluster, topic, 0, 5);

			JsonNode r = ok("diagnose_topic", on(cluster, "topic", topic, "intervalSeconds", 1));
			assertThat(r.get("status").asText()).as(cluster).isEqualTo("critical");
			assertThat(r.get("mode").asText()).isEqualTo(zk(cluster) ? "ZooKeeper" : "KRaft");
			assertThat(sortedBySeverity(r)).as(r.toString()).isTrue();
			JsonNode f = finding(r, "MIN_ISR_EXCEEDS_RF");
			assertThat(f.get("severity").asText()).isEqualTo("critical");
			assertThat(f.at("/evidence/minInsyncReplicas").asInt()).isEqualTo(2);
			assertThat(f.at("/evidence/replicationFactor").asInt()).isEqualTo(1);
			assertThat(f.get("recommendation").asText()).contains("topics_config_update", "min.insync.replicas=1");
			// ISR'ı tam olan partition'lar replika kaybı (UNDER_MIN_ISR) diye ayrıca raporlanmaz
			assertThat(codes(r)).doesNotContain("UNDER_MIN_ISR", "OFFLINE_PARTITIONS");

			JsonNode facts = r.get("facts");
			assertThat(facts.at("/config/min.insync.replicas").asInt()).isEqualTo(2);
			assertThat(facts.at("/config/replicationFactor").asInt()).isEqualTo(1);
			assertThat(facts.at("/health/partitions").asInt()).isEqualTo(2);
			assertThat(facts.at("/health/offline").asInt()).isZero();
			if (zk(cluster)) {
				assertThat(facts.at("/health/elr").asText()).contains("ZooKeeper");
			}
			else {
				assertThat(facts.at("/health/elrPartitions").asInt()).isZero();
			}
			assertThat(facts.at("/data/approxMessages").asLong()).isEqualTo(5);
			assertThat(facts.at("/data/lastWriteTime").asText()).isNotBlank();
			assertThat(facts.at("/size/sizeBytes").asLong()).isPositive();
			assertThat(facts.at("/access/authorizer").asBoolean()).isTrue();
			assertThat(r.has("partial")).as(r.toString()).isFalse();
		}
	}

	@Test
	void stuckConsumerGroupAndWriteRateAreDetectedOnBothClusters() throws Exception {
		for (String cluster : CLUSTERS) {
			String topic = unique("diag-stuck");
			String group = topic + "-g";
			createTopic(cluster, topic, 2, Map.of());
			produce(cluster, topic, 0, 10);
			commit(cluster, group, topic, Map.of(0, 3L));
			produceInBackground(cluster, topic, 0);

			JsonNode r = ok("diagnose_topic", on(cluster, "topic", topic, "intervalSeconds", 2));
			JsonNode stuck = finding(r, "CONSUMER_STUCK");
			assertThat(stuck.get("severity").asText()).as("üyesiz grup: uyarı").isEqualTo("warning");
			JsonNode g = stuck.at("/evidence/groups/0");
			assertThat(g.get("groupId").asText()).isEqualTo(group);
			assertThat(g.get("members").asInt()).isZero();
			assertThat(texts(g.get("stuckPartitions"))).containsExactly("0");
			assertThat(stuck.get("recommendation").asText()).contains("consumer_groups_offsets_reset");

			JsonNode facts = r.get("facts");
			assertThat(facts.at("/writeRate/msgsPerSec").asDouble()).as(cluster).isPositive();
			assertThat(facts.at("/writeRate/messages").asLong()).isPositive();
			assertThat(facts.at("/consumers/groupsOnTopic").asInt()).isEqualTo(1);
			JsonNode row = facts.at("/consumers/groups/0");
			assertThat(row.get("groupId").asText()).isEqualTo(group);
			assertThat(row.get("trend").asText()).isEqualTo("growing");
			assertThat(row.get("stuckPartitions").asInt()).isEqualTo(1);
			assertThat(row.get("lag").asLong()).isGreaterThan(7);
			closeResources();
		}
	}

	@Test
	void aclSummaryAndFactsOfHealthyTopic() throws Exception {
		String topic = unique("diag-acl");
		createTopic(TestConfigs.CLUSTER, topic, 1, Map.of());
		produce(TestConfigs.CLUSTER, topic, 0, 3);
		AclBinding bobRead = new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
				new AccessControlEntry("User:bob", "*", AclOperation.READ, AclPermissionType.ALLOW));
		AclBinding bobDenyWrite = new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
				new AccessControlEntry("User:bob", "*", AclOperation.WRITE, AclPermissionType.DENY));
		testAdmin().createAcls(List.of(bobRead, bobDenyWrite)).all().get(30, TimeUnit.SECONDS);
		try {
			waitUntil(() -> {
				try {
					return testAdmin().describeAcls(bobRead.toFilter()).values().get(30, TimeUnit.SECONDS).size() == 1;
				}
				catch (Exception e) {
					return false;
				}
			}, "ACL görünmedi");
			// salt okunur cluster'da da okuma araçları çalışır
			JsonNode r = ok("diagnose_topic", args("cluster", TestConfigs.READ_ONLY_CLUSTER, "topic", topic,
					"intervalSeconds", 1));
			assertThat(r.get("cluster").asText()).isEqualTo(TestConfigs.READ_ONLY_CLUSTER);
			JsonNode access = r.at("/facts/access");
			assertThat(texts(access.get("readers"))).containsExactly("User:bob");
			assertThat(texts(access.get("writers"))).isEmpty();
			assertThat(texts(access.get("deniedWrite"))).containsExactly("User:bob");
			assertThat(access.get("aclCount").asInt()).isEqualTo(2);
			assertThat(codes(r)).doesNotContain("NO_TOPIC_ACLS", "MIN_ISR_EXCEEDS_RF", "CONSUMER_STUCK");
			assertThat(codes(r)).contains("NO_CONSUMER_GROUPS");
			assertThat(r.at("/facts/producers/activeProducers").asInt()).isGreaterThanOrEqualTo(0);
			assertThat(r.at("/facts/config/cleanup.policy").asText()).isEqualTo("delete");
		}
		finally {
			testAdmin().deleteAcls(List.of(bobRead.toFilter(), bobDenyWrite.toFilter())).all().get(30, TimeUnit.SECONDS);
		}
	}

	@Test
	void unknownTopicIsAnError() {
		assertThat(error("diagnose_topic", args("topic", unique("diag-none")))).contains("Topic bulunamadı");
		assertThat(error("diagnose_topic", zkArgs("topic", unique("diag-none")))).contains("Topic bulunamadı");
	}

}
