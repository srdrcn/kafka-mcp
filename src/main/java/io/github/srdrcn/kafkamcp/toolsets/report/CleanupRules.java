package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;

import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.toolsets.acl.Acls;

/**
 * Cleanup candidate rules (pure computation, testable): topics (empty/idle/without consumers), Empty consumer
 * groups (offsets on deleted topics, no offsets, or no lag with idle topics) and orphan LITERAL ACLs. Every
 * candidate comes with reasons and a risk hint (low/medium/high); nothing is deleted.
 */
public final class CleanupRules {

	public static final String LOW = "low";

	public static final String MEDIUM = "medium";

	public static final String HIGH = "high";

	private static final int MAX_NAMES = 10;

	private CleanupRules() {
	}

	/** Cleanup candidate: type (topic/group/acl), name, reasons, risk and short details. */
	public record Candidate(String type, String name, List<String> reasons, String risk, Map<String, Object> details) {

		public Map<String, Object> toMap() {
			Map<String, Object> m = Results.map("type", type, "name", name, "reasons", reasons, "risk", risk);
			m.putAll(details);
			return m;
		}

	}

	/**
	 * Topic candidate: empty (never written), idle (last write older than the threshold) or without consumers.
	 * Risk: low if there is no data and no consumer; medium if there is no data but there are consumers (or the
	 * consumer count is {@code null}, unknown, because the group scan failed); high if it only lacks consumers and
	 * was written recently (active producer). Null if not a candidate.
	 */
	public static Candidate topic(String name, TopicActivity a, long thresholdMs, Integer consumerGroups,
			Map<String, Object> details) {
		boolean empty = Boolean.TRUE.equals(a.empty());
		Boolean idle = a.idleSince(thresholdMs);
		boolean noConsumers = consumerGroups != null && consumerGroups == 0;
		List<String> reasons = new ArrayList<>();
		if (empty) {
			reasons.add(a.neverWritten() ? "never_written" : "empty");
		}
		if (Boolean.TRUE.equals(idle)) {
			reasons.add("idle");
		}
		if (noConsumers) {
			reasons.add("no_consumers");
		}
		if (reasons.isEmpty()) {
			return null;
		}
		boolean inactive = empty || Boolean.TRUE.equals(idle);
		String risk;
		if (inactive) {
			risk = noConsumers ? LOW : MEDIUM;
		}
		else {
			risk = Boolean.FALSE.equals(idle) ? HIGH : MEDIUM;
		}
		Map<String, Object> d = new LinkedHashMap<>(details);
		if (consumerGroups == null) {
			d.put("consumersUnknown", true);
		}
		else {
			d.put("consumerGroups", consumerGroups);
		}
		return new Candidate("topic", name, reasons, risk, d);
	}

	/**
	 * Candidate for a consumer group in the Empty state. Reasons: no offsets; offsets on deleted topics (low if all
	 * of them are deleted, medium if some are); or zero lag with all committed topics idle (medium). Null if not a
	 * candidate.
	 */
	public static Candidate group(String groupId, String state, Map<TopicPartition, Long> committed,
			Set<String> existingTopics, Map<TopicPartition, Offsets.Range> ranges, Predicate<String> topicInactive) {
		if (!"Empty".equals(state)) {
			return null;
		}
		Set<String> topics = new TreeSet<>();
		committed.keySet().forEach(tp -> topics.add(tp.topic()));
		if (topics.isEmpty()) {
			return new Candidate("group", groupId, List.of("empty_group", "no_offsets"), LOW, Map.of());
		}
		List<String> deleted = topics.stream().filter(t -> !existingTopics.contains(t)).toList();
		if (!deleted.isEmpty()) {
			return new Candidate("group", groupId, List.of("empty_group", "offsets_on_deleted_topics"),
					deleted.size() == topics.size() ? LOW : MEDIUM,
					Results.map("committedTopics", topics.size(), "deletedTopics", head(deleted), "deletedTopicCount",
							deleted.size()));
		}
		long lag = 0;
		for (Map.Entry<TopicPartition, Long> e : committed.entrySet()) {
			Offsets.Range r = ranges.get(e.getKey());
			if (r == null) {
				return null;
			}
			lag += Math.max(0, r.latest() - e.getValue());
		}
		if (lag == 0 && topics.stream().allMatch(topicInactive)) {
			return new Candidate("group", groupId, List.of("empty_group", "no_lag", "topics_idle"), MEDIUM,
					Results.map("committedTopics", topics.size(), "topics", head(new ArrayList<>(topics))));
		}
		return null;
	}

	/**
	 * Orphan LITERAL ACLs: bindings on a topic that does not exist or (if the group list is complete) a group that
	 * does not exist, one candidate per resource. Risk is low for topics and medium for groups (a group is only
	 * listed while it has offsets/members; it may be the ACL of a stopped application).
	 */
	public static List<Candidate> orphanAcls(Collection<AclBinding> bindings, Set<String> topics, Set<String> groups,
			boolean groupsKnown) {
		Map<String, List<AclBinding>> byResource = new TreeMap<>();
		for (AclBinding b : bindings) {
			if (b.pattern().patternType() != PatternType.LITERAL || Acls.WILDCARD.equals(b.pattern().name())) {
				continue;
			}
			ResourceType type = b.pattern().resourceType();
			String name = b.pattern().name();
			boolean orphan = type == ResourceType.TOPIC && !topics.contains(name)
					|| type == ResourceType.GROUP && groupsKnown && !groups.contains(name);
			if (orphan) {
				byResource.computeIfAbsent(type + "\u0000" + name, k -> new ArrayList<>()).add(b);
			}
		}
		List<Candidate> out = new ArrayList<>();
		byResource.forEach((k, list) -> {
			AclBinding first = list.get(0);
			boolean topic = first.pattern().resourceType() == ResourceType.TOPIC;
			Set<String> principals = new TreeSet<>();
			Set<String> ops = new TreeSet<>();
			list.forEach(b -> {
				principals.add(b.entry().principal());
				ops.add(b.entry().permissionType() + ":" + b.entry().operation());
			});
			out.add(new Candidate("acl", first.pattern().name(),
					List.of(topic ? "orphan_topic_acl" : "orphan_group_acl"), topic ? LOW : MEDIUM,
					Results.map("resourceType", first.pattern().resourceType().name(), "patternType", "LITERAL",
							"bindings", list.size(), "principalCount", principals.size(), "principals",
							head(new ArrayList<>(principals)), "principalsOmitted",
							principals.size() > MAX_NAMES ? principals.size() - MAX_NAMES : null, "operations",
							new ArrayList<>(ops))));
		});
		return out;
	}

	private static List<String> head(List<String> names) {
		return names.subList(0, Math.min(MAX_NAMES, names.size()));
	}

}
