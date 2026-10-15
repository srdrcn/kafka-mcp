package io.github.srdrcn.kafkamcp.toolsets.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;

/** report_team_inventory ve report_cleanup_candidates (KRaft ve ZooKeeper). */
class InventoryReportTest extends ReportTestBase {

	static AclBinding acl(ResourceType type, String name, PatternType pattern, String principal, AclOperation op) {
		return new AclBinding(new ResourcePattern(type, name, pattern),
				new AccessControlEntry(principal, "*", op, AclPermissionType.ALLOW));
	}

	/** ACL'leri oluşturur ve describeAcls'te görünene kadar bekler. */
	static void createAcls(Admin admin, List<AclBinding> bindings) throws Exception {
		admin.createAcls(bindings).all().get(30, TimeUnit.SECONDS);
		long deadline = System.currentTimeMillis() + 30_000;
		while (!admin.describeAcls(AclBindingFilter.ANY).values().get(30, TimeUnit.SECONDS).containsAll(bindings)) {
			if (System.currentTimeMillis() > deadline) {
				throw new IllegalStateException("ACL'ler görünmedi");
			}
			Thread.sleep(100);
		}
	}

	static void deleteAcls(Admin admin, List<AclBinding> bindings) {
		try {
			admin.deleteAcls(bindings.stream().map(AclBinding::toFilter).toList()).all().get(30, TimeUnit.SECONDS);
		}
		catch (Exception ignored) {
			// temizlik
		}
	}

	@ParameterizedTest(name = "zk={0}")
	@ValueSource(booleans = { false, true })
	void teamInventoryCollectsTopicsGroupsAndAclPrincipals(boolean zk) throws Exception {
		String token = unique("rep-team");
		String prefix = token + ".";
		String orders = prefix + "orders";
		String empty = prefix + "empty";
		String old = prefix + "old";
		String outside = unique("rep-outside");
		createTopic(zk, orders, 2, Map.of());
		createTopic(zk, empty, 1, Map.of());
		createTopic(zk, old, 1, Map.of("retention.ms", "-1"));
		createTopic(zk, outside, 1, Map.of());
		produce(zk, orders, 0, 5, null, 100);
		produce(zk, old, 0, 2, System.currentTimeMillis() - 45 * DAY_MS);
		produce(zk, outside, 0, 2, null);
		commit(zk, prefix + "svc", orders, 0, 2);
		String external = unique("rep-ext");
		commit(zk, external, orders, 0, 5);
		commit(zk, unique("rep-unrelated"), outside, 0, 1);
		// önekle adlandırılmış ama takım topic'lerini tüketmeyen gruplar
		commit(zk, prefix + "other1", outside, 0, 1);
		commit(zk, prefix + "other2", outside, 0, 1);
		String alice = "User:" + token + "-alice";
		String bob = "User:" + token + "-bob";
		String carol = "User:" + token + "-carol";
		String wild = "User:" + token + "-wild";
		List<AclBinding> acls = List.of(acl(ResourceType.TOPIC, orders, PatternType.LITERAL, alice, AclOperation.READ),
				acl(ResourceType.TOPIC, prefix, PatternType.PREFIXED, bob, AclOperation.WRITE),
				acl(ResourceType.TOPIC, prefix + "ghost", PatternType.LITERAL, carol, AclOperation.DESCRIBE),
				acl(ResourceType.TOPIC, "*", PatternType.LITERAL, wild, AclOperation.DESCRIBE),
				acl(ResourceType.TOPIC, outside, PatternType.LITERAL, alice, AclOperation.WRITE));
		createAcls(admin(zk), acls);
		try {
			JsonNode r = ok("report_team_inventory", on(zk, "prefix", prefix));
			assertThat(r.get("prefix").asText()).isEqualTo(prefix);
			JsonNode topics = r.get("topics");
			assertThat(topics.get("count").asInt()).isEqualTo(3);
			assertThat(topics.get("partitions").asInt()).isEqualTo(4);
			assertThat(topics.get("sizeBytes").asLong()).isPositive();
			assertThat(topics.get("emptyCount").asInt()).isEqualTo(1);
			assertThat(topics.get("idleCount").asInt()).isEqualTo(1);
			assertThat(texts(topics.get("list"), "topic")).containsExactly(empty, old, orders);
			assertThat(find(topics.get("list"), "topic", empty).get("empty").asBoolean()).isTrue();
			assertThat(find(topics.get("list"), "topic", old).get("idle").asBoolean()).isTrue();
			assertThat(find(topics.get("list"), "topic", old).get("lastWriteAgeDays").asLong()).isBetween(44L, 46L);
			assertThat(find(topics.get("list"), "topic", orders).has("idle")).isFalse();

			JsonNode groups = r.get("consumerGroups");
			assertThat(groups.get("count").asInt()).isEqualTo(2);
			assertThat(groups.get("totalLag").asLong()).isEqualTo(3);
			assertThat(texts(groups.get("groups"), "groupId")).containsExactly(prefix + "svc", external);
			assertThat(groups.at("/groups/0/totalLag").asLong()).isEqualTo(3);
			assertThat(groups.at("/groups/0/state").asText()).isEqualTo("Empty");
			assertThat(groups.get("otherGroupsWithPrefixCount").asInt()).isEqualTo(2);
			assertThat(texts(groups.get("otherGroupsWithPrefix"), null)).containsExactly(prefix + "other1",
					prefix + "other2");
			assertThat(groups.has("otherGroupsWithPrefixOmitted")).isFalse();

			JsonNode principals = r.at("/acls/principals");
			assertThat(texts(principals, "principal")).containsExactlyInAnyOrder(alice, bob, carol, wild);
			JsonNode a = find(principals, "principal", alice);
			assertThat(texts(a.get("operations"), null)).containsExactly("READ");
			assertThat(a.get("topicsCovered").asInt()).isEqualTo(1);
			assertThat(texts(a.get("patterns"), null)).containsExactly("LITERAL:" + orders);
			JsonNode b = find(principals, "principal", bob);
			assertThat(b.get("topicsCovered").asInt()).isEqualTo(3);
			assertThat(b.get("coversAllTopics").asBoolean()).isTrue();
			assertThat(texts(b.get("patterns"), null)).containsExactly("PREFIXED:" + prefix);
			assertThat(find(principals, "principal", carol).get("topicsCovered").asInt()).isZero();
			assertThat(find(principals, "principal", wild).get("topicsCovered").asInt()).isEqualTo(3);
			assertThat(texts(find(principals, "principal", wild).get("patterns"), null))
				.containsExactly("LITERAL:* (wildcard)");
			assertThat(r.has("connectors")).isFalse();
			assertThat(r.at("/examined/topics").asInt()).isEqualTo(3);

			// kırpılan liste sayısıyla birlikte döner
			JsonNode limited = ok("report_team_inventory", on(zk, "prefix", prefix, "limit", 1));
			JsonNode limitedGroups = limited.get("consumerGroups");
			assertThat(limitedGroups.get("otherGroupsWithPrefixCount").asInt()).isEqualTo(2);
			assertThat(texts(limitedGroups.get("otherGroupsWithPrefix"), null)).containsExactly(prefix + "other1");
			assertThat(limitedGroups.get("otherGroupsWithPrefixOmitted").asInt()).isEqualTo(1);
		}
		finally {
			deleteAcls(admin(zk), acls);
		}
	}

	@ParameterizedTest(name = "zk={0}")
	@ValueSource(booleans = { false, true })
	void cleanupCandidatesCoverTopicsGroupsAndOrphanAcls(boolean zk) throws Exception {
		String p = unique("rep-clean");
		String never = p + "-never";
		String active = p + "-active";
		String idle = p + "-idle";
		String busy = p + "-busy";
		createTopic(zk, never, 1, Map.of());
		createTopic(zk, active, 1, Map.of());
		createTopic(zk, idle, 1, Map.of("retention.ms", "-1"));
		createTopic(zk, busy, 1, Map.of());
		produce(zk, active, 0, 3, null);
		produce(zk, idle, 0, 4, System.currentTimeMillis() - 60 * DAY_MS);
		produce(zk, busy, 0, 3, null);
		commit(zk, p + "-idlegrp", idle, 0, 4);
		commit(zk, p + "-busygrp", busy, 0, 1);
		// filtreye uymayan topic/grup/ACL: atlananlar sayılır
		String out = unique("rep-cleanout");
		createTopic(zk, out, 1, Map.of());
		commit(zk, out + "-grp", out, 0, 0);
		String ghostTopic = p + "-ghost";
		String ghostGroup = p + "-ghostgrp";
		List<AclBinding> acls = new ArrayList<>(List.of(
				acl(ResourceType.TOPIC, ghostTopic, PatternType.LITERAL, "User:" + p + "-x", AclOperation.READ),
				acl(ResourceType.GROUP, ghostGroup, PatternType.LITERAL, "User:" + p + "-x", AclOperation.READ),
				acl(ResourceType.TOPIC, busy, PatternType.LITERAL, "User:" + p + "-x", AclOperation.READ),
				acl(ResourceType.GROUP, p + "-busygrp", PatternType.LITERAL, "User:" + p + "-x", AclOperation.READ),
				acl(ResourceType.TOPIC, p + "-pre", PatternType.PREFIXED, "User:" + p + "-x", AclOperation.READ),
				acl(ResourceType.TOPIC, out + "-ghost", PatternType.LITERAL, "User:" + p + "-x", AclOperation.READ)));
		createAcls(admin(zk), acls);
		try {
			JsonNode r = ok("report_cleanup_candidates", on(zk, "filter", p, "olderThanDays", 30));
			JsonNode c = r.get("candidates");
			assertThat(texts(c, "name")).containsExactlyInAnyOrder(never, active, idle, p + "-idlegrp", ghostTopic,
					ghostGroup);
			JsonNode neverRow = find(c, "name", never);
			assertThat(neverRow.get("type").asText()).isEqualTo("topic");
			assertThat(texts(neverRow.get("reasons"), null)).containsExactly("never_written", "no_consumers");
			assertThat(neverRow.get("risk").asText()).isEqualTo("low");
			JsonNode activeRow = find(c, "name", active);
			assertThat(texts(activeRow.get("reasons"), null)).containsExactly("no_consumers");
			assertThat(activeRow.get("risk").asText()).isEqualTo("high");
			assertThat(activeRow.get("lastWriteAgeDays").asLong()).isZero();
			JsonNode idleRow = find(c, "name", idle);
			assertThat(texts(idleRow.get("reasons"), null)).containsExactly("idle");
			assertThat(idleRow.get("risk").asText()).isEqualTo("medium");
			assertThat(idleRow.get("consumerGroups").asInt()).isEqualTo(1);
			JsonNode group = find(c, "name", p + "-idlegrp");
			assertThat(group.get("type").asText()).isEqualTo("group");
			assertThat(texts(group.get("reasons"), null)).containsExactly("empty_group", "no_lag", "topics_idle");
			JsonNode topicAcl = find(c, "name", ghostTopic);
			assertThat(topicAcl.get("type").asText()).isEqualTo("acl");
			assertThat(texts(topicAcl.get("reasons"), null)).containsExactly("orphan_topic_acl");
			assertThat(topicAcl.get("risk").asText()).isEqualTo("low");
			assertThat(topicAcl.get("resourceType").asText()).isEqualTo("TOPIC");
			JsonNode groupAcl = find(c, "name", ghostGroup);
			assertThat(texts(groupAcl.get("reasons"), null)).containsExactly("orphan_group_acl");
			assertThat(groupAcl.get("risk").asText()).isEqualTo("medium");
			// sıralama: önce düşük risk
			assertThat(c.get(0).get("risk").asText()).isEqualTo("low");
			assertThat(r.at("/byType/topic").asInt()).isEqualTo(3);
			assertThat(r.at("/byType/group").asInt()).isEqualTo(1);
			assertThat(r.at("/byType/acl").asInt()).isEqualTo(2);
			assertThat(r.at("/byRisk/high").asInt()).isEqualTo(1);
			assertThat(r.get("notes").toString()).contains("hiçbir şey silinmez");
			assertThat(r.at("/examined/aclBindings").asInt()).isGreaterThanOrEqualTo(6);
			assertThat(r.at("/skipped/topicsNotMatchingFilter").asInt()).isGreaterThanOrEqualTo(1);
			assertThat(r.at("/skipped/groupsNotMatchingFilter").asInt()).isGreaterThanOrEqualTo(1);
			assertThat(r.at("/skipped/orphanAclResourcesNotMatchingFilter").asInt()).isGreaterThanOrEqualTo(1);
			assertThat(r.at("/examined/topics").asInt())
				.isGreaterThanOrEqualTo(4 + r.at("/skipped/topicsNotMatchingFilter").asInt());

			// eşik büyüyünce idle nedeni kalkar
			JsonNode longer = ok("report_cleanup_candidates", on(zk, "filter", p, "olderThanDays", 90));
			assertThat(texts(longer.get("candidates"), "name")).doesNotContain(idle, p + "-idlegrp");
		}
		finally {
			deleteAcls(admin(zk), acls);
		}
	}

}
