package io.github.srdrcn.kafkamcp.toolsets.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.kafka.Offsets;

/** Temizlik adayı kuralları, önek ACL erişimi, connector topic referansları ve küçük yardımcılar (broker'sız). */
class InventoryRulesTest {

	static final long NOW = 1_800_000_000_000L;

	static final long THRESHOLD = TopicActivity.threshold(NOW, 30);

	static TopicActivity activity(Boolean empty, boolean neverWritten, Long lastWrite, boolean complete) {
		return new TopicActivity("t", empty, neverWritten, null, lastWrite, complete, complete ? null : "bilinmiyor");
	}

	static AclBinding acl(ResourceType type, String name, PatternType pattern, String principal, AclOperation op,
			AclPermissionType perm) {
		return new AclBinding(new ResourcePattern(type, name, pattern), new AccessControlEntry(principal, "*", op, perm));
	}

	@Test
	void topicCandidateReasonsAndRisk() {
		long old = NOW - 90 * 86_400_000L;
		long recent = NOW - 86_400_000L;
		CleanupRules.Candidate never = CleanupRules.topic("a", activity(true, true, null, true), THRESHOLD, 0, Map.of());
		assertThat(never.reasons()).containsExactly("never_written", "no_consumers");
		assertThat(never.risk()).isEqualTo(CleanupRules.LOW);
		CleanupRules.Candidate purged = CleanupRules.topic("b", activity(true, false, null, true), THRESHOLD, 2, Map.of());
		assertThat(purged.reasons()).containsExactly("empty");
		assertThat(purged.risk()).isEqualTo(CleanupRules.MEDIUM);
		assertThat(purged.details()).containsEntry("consumerGroups", 2);
		CleanupRules.Candidate idle = CleanupRules.topic("c", activity(false, false, old, true), THRESHOLD, 0, Map.of());
		assertThat(idle.reasons()).containsExactly("idle", "no_consumers");
		assertThat(idle.risk()).isEqualTo(CleanupRules.LOW);
		CleanupRules.Candidate active = CleanupRules.topic("d", activity(false, false, recent, true), THRESHOLD, 0,
				Map.of());
		assertThat(active.reasons()).containsExactly("no_consumers");
		assertThat(active.risk()).isEqualTo(CleanupRules.HIGH);
		CleanupRules.Candidate unknown = CleanupRules.topic("e", activity(false, false, null, false), THRESHOLD, 0,
				Map.of());
		assertThat(unknown.risk()).isEqualTo(CleanupRules.MEDIUM);
		// eksik son yazma (bazı partition'lar okunamadı) eski görünse de idle sayılmaz
		assertThat(CleanupRules.topic("f", activity(false, false, old, false), THRESHOLD, 1, Map.of())).isNull();
		assertThat(CleanupRules.topic("g", activity(false, false, recent, true), THRESHOLD, 1, Map.of())).isNull();
		// grup taraması başarısız (consumer sayısı bilinmiyor): boş/boşta topic yine aday, risk en az medium
		CleanupRules.Candidate neverUnknown = CleanupRules.topic("h", activity(true, true, null, true), THRESHOLD,
				null, Map.of());
		assertThat(neverUnknown.reasons()).containsExactly("never_written");
		assertThat(neverUnknown.risk()).isEqualTo(CleanupRules.MEDIUM);
		assertThat(neverUnknown.details()).containsEntry("consumersUnknown", true).doesNotContainKey("consumerGroups");
		assertThat(CleanupRules.topic("i", activity(false, false, old, true), THRESHOLD, null, Map.of()).reasons())
			.containsExactly("idle");
		assertThat(CleanupRules.topic("j", activity(false, false, recent, true), THRESHOLD, null, Map.of())).isNull();
	}

	@Test
	void groupCandidates() {
		TopicPartition a0 = new TopicPartition("a", 0);
		TopicPartition gone0 = new TopicPartition("gone", 0);
		Map<TopicPartition, Offsets.Range> ranges = Map.of(a0, new Offsets.Range(0, 10));
		Set<String> existing = Set.of("a");
		assertThat(CleanupRules.group("g", "Stable", Map.of(gone0, 1L), existing, ranges, t -> true)).isNull();
		assertThat(CleanupRules.group("g", "Empty", Map.of(), existing, ranges, t -> true).reasons())
			.containsExactly("empty_group", "no_offsets");
		CleanupRules.Candidate allGone = CleanupRules.group("g", "Empty", Map.of(gone0, 1L), existing, ranges, t -> false);
		assertThat(allGone.reasons()).containsExactly("empty_group", "offsets_on_deleted_topics");
		assertThat(allGone.risk()).isEqualTo(CleanupRules.LOW);
		assertThat(allGone.details()).containsEntry("deletedTopics", List.of("gone"));
		CleanupRules.Candidate someGone = CleanupRules.group("g", "Empty", Map.of(gone0, 1L, a0, 10L), existing, ranges,
				t -> false);
		assertThat(someGone.risk()).isEqualTo(CleanupRules.MEDIUM);
		CleanupRules.Candidate caughtUp = CleanupRules.group("g", "Empty", Map.of(a0, 10L), existing, ranges, t -> true);
		assertThat(caughtUp.reasons()).containsExactly("empty_group", "no_lag", "topics_idle");
		assertThat(CleanupRules.group("g", "Empty", Map.of(a0, 10L), existing, ranges, t -> false)).isNull();
		assertThat(CleanupRules.group("g", "Empty", Map.of(a0, 3L), existing, ranges, t -> true)).isNull();
		assertThat(CleanupRules.group("g", "Empty", Map.of(new TopicPartition("a", 1), 3L), existing, ranges, t -> true))
			.as("end offset'i bilinmeyen partition").isNull();
	}

	@Test
	void orphanAclsAreLiteralOnlyAndGroupedByResource() {
		AclPermissionType allow = AclPermissionType.ALLOW;
		List<AclBinding> bindings = List.of(
				acl(ResourceType.TOPIC, "ghost", PatternType.LITERAL, "User:b", AclOperation.READ, allow),
				acl(ResourceType.TOPIC, "ghost", PatternType.LITERAL, "User:a", AclOperation.WRITE, allow),
				acl(ResourceType.TOPIC, "live", PatternType.LITERAL, "User:a", AclOperation.READ, allow),
				acl(ResourceType.TOPIC, "*", PatternType.LITERAL, "User:a", AclOperation.READ, allow),
				acl(ResourceType.TOPIC, "gh", PatternType.PREFIXED, "User:a", AclOperation.READ, allow),
				acl(ResourceType.GROUP, "nogroup", PatternType.LITERAL, "User:a", AclOperation.READ, allow),
				acl(ResourceType.GROUP, "grp", PatternType.LITERAL, "User:a", AclOperation.READ, allow));
		List<CleanupRules.Candidate> c = CleanupRules.orphanAcls(bindings, Set.of("live"), Set.of("grp"), true);
		assertThat(c).extracting(CleanupRules.Candidate::name).containsExactly("nogroup", "ghost");
		CleanupRules.Candidate ghost = c.get(1);
		assertThat(ghost.risk()).isEqualTo(CleanupRules.LOW);
		assertThat(ghost.details()).containsEntry("bindings", 2)
			.containsEntry("principalCount", 2)
			.containsEntry("principals", List.of("User:a", "User:b"))
			.containsEntry("operations", List.of("ALLOW:READ", "ALLOW:WRITE"))
			.doesNotContainKey("principalsOmitted");
		assertThat(c.get(0).risk()).isEqualTo(CleanupRules.MEDIUM);
		assertThat(c.get(0).reasons()).containsExactly("orphan_group_acl");
		// çok principal: ilk 10 ad, toplam ve kalan sayı
		List<AclBinding> many = new java.util.ArrayList<>();
		for (int i = 0; i < 25; i++) {
			many.add(acl(ResourceType.TOPIC, "crowded", PatternType.LITERAL, "User:p" + (char) ('a' + i), AclOperation.READ,
					allow));
		}
		CleanupRules.Candidate crowded = CleanupRules.orphanAcls(many, Set.of(), Set.of(), false).get(0);
		assertThat(crowded.details()).containsEntry("bindings", 25)
			.containsEntry("principalCount", 25)
			.containsEntry("principalsOmitted", 15);
		assertThat((List<?>) crowded.details().get("principals")).hasSize(10);
		// grup listesi bilinmiyorsa grup ACL'leri aday olmaz
		assertThat(CleanupRules.orphanAcls(bindings, Set.of("live"), Set.of(), false))
			.extracting(CleanupRules.Candidate::name)
			.containsExactly("ghost");
	}

	@Test
	void prefixAccessCoversLiteralPrefixedAndWildcardBindings() {
		AclPermissionType allow = AclPermissionType.ALLOW;
		List<String> topics = List.of("pay.a", "pay.b", "pay.sub.c");
		List<AclBinding> bindings = List.of(
				acl(ResourceType.TOPIC, "pay.a", PatternType.LITERAL, "User:lit", AclOperation.READ, allow),
				acl(ResourceType.TOPIC, "pay.ghost", PatternType.LITERAL, "User:ghost", AclOperation.READ, allow),
				acl(ResourceType.TOPIC, "pa", PatternType.PREFIXED, "User:broad", AclOperation.ALL, allow),
				acl(ResourceType.TOPIC, "pay.sub.", PatternType.PREFIXED, "User:narrow", AclOperation.WRITE, allow),
				acl(ResourceType.TOPIC, "*", PatternType.LITERAL, "User:any", AclOperation.DESCRIBE, allow),
				acl(ResourceType.TOPIC, "pay.a", PatternType.LITERAL, "User:lit", AclOperation.WRITE,
						AclPermissionType.DENY),
				acl(ResourceType.TOPIC, "other", PatternType.LITERAL, "User:other", AclOperation.READ, allow),
				acl(ResourceType.GROUP, "pay.", PatternType.PREFIXED, "User:group", AclOperation.READ, allow));
		List<Map<String, Object>> rows = PrefixAccess.principals(bindings, "pay.", topics);
		assertThat(rows).extracting(m -> m.get("principal") + "/" + m.get("permission"))
			.containsExactly("User:any/ALLOW", "User:broad/ALLOW", "User:ghost/ALLOW", "User:lit/ALLOW", "User:lit/DENY",
					"User:narrow/ALLOW");
		assertThat(rows.get(1)).containsEntry("topicsCovered", 3).containsEntry("coversAllTopics", true);
		assertThat(rows.get(2)).containsEntry("topicsCovered", 0);
		assertThat(rows.get(3)).containsEntry("topicsCovered", 1).containsEntry("operations", List.of("READ"));
		assertThat(rows.get(5)).containsEntry("topicsCovered", 1).containsEntry("patterns", List.of("PREFIXED:pay.sub."));
		assertThat(rows.get(0)).containsEntry("patterns", List.of("LITERAL:* (wildcard)"));
	}

	@Test
	void connectorTopicReferences() {
		ConnectorTopics.References refs = ConnectorTopics.parse(Map.of("topics", "pay.a, pay.b", "topics.regex",
				"pay\\.x.*", "topic.prefix", "pay.db.", "errors.deadletterqueue.topic.name", "pay.dlq",
				"topic.creation.default.replication.factor", "3", "bad.topic.regex", "([", "tasks.max", "1"));
		assertThat(refs.literals()).containsExactly("pay.a", "pay.b", "pay.dlq");
		assertThat(refs.prefixes()).containsExactly("pay.db.");
		assertThat(refs.covers("pay.xyz")).isTrue();
		assertThat(refs.covers("pay.db.orders")).isTrue();
		assertThat(refs.covers("pay.c")).isFalse();
		assertThat(ConnectorTopics.covered(refs, List.of("pay.a", "pay.c", "pay.x1"))).containsExactly("pay.a", "pay.x1");
		assertThat(refs.keys()).doesNotContain("tasks.max", "topic.creation.default.replication.factor", "bad.topic.regex");
		assertThat(ConnectorTopics.parse(null).isEmpty()).isTrue();
	}

	@Test
	void findingsAreOrderedAndItemListsBounded() {
		Findings f = new Findings(2);
		f.add("low_one", Findings.Severity.LOW, "x", Map.of("topic", "a"));
		for (int i = 0; i < 5; i++) {
			f.add("crit", Findings.Severity.CRITICAL, "y", Map.of("topic", "t" + i));
		}
		List<Map<String, Object>> r = f.render();
		assertThat(r).extracting(m -> m.get("check")).containsExactly("crit", "low_one");
		assertThat(r.get(0)).containsEntry("count", 5).containsEntry("itemsOmitted", 3);
		assertThat((List<?>) r.get(0).get("items")).hasSize(2);
		assertThat(r.get(1)).doesNotContainKey("itemsOmitted");
		assertThat(f.bySeverity()).containsExactly(Map.entry("critical", 5), Map.entry("low", 1));
		assertThat(f.total()).isEqualTo(6);
	}

	@Test
	void topicActivityIdleness() {
		long old = NOW - 40 * 86_400_000L;
		assertThat(activity(false, false, old, true).idleSince(THRESHOLD)).isTrue();
		assertThat(activity(false, false, NOW, false).idleSince(THRESHOLD)).isFalse();
		assertThat(activity(false, false, old, false).idleSince(THRESHOLD)).isNull();
		assertThat(activity(true, true, null, true).idleSince(THRESHOLD)).isNull();
		assertThat(activity(null, false, old, true).idleSince(THRESHOLD)).isNull();
		assertThat(activity(false, false, old, true).ageDays(NOW)).isEqualTo(40);
		assertThat(activity(false, false, old, false).lastWriteFields(NOW)).containsEntry("lastWriteIncomplete", true);
	}

	@Test
	void groupSamplingPrefersActiveGroups() {
		assertThat(GroupScan.priority("Stable")).isLessThan(GroupScan.priority("PreparingRebalance"));
		assertThat(GroupScan.priority("CompletingRebalance")).isLessThan(GroupScan.priority("Empty"));
		assertThat(GroupScan.priority("Empty")).isLessThan(GroupScan.priority("Dead"));
		assertThat(GroupScan.priority(null)).isEqualTo(GroupScan.priority("Unknown"));
	}

}
