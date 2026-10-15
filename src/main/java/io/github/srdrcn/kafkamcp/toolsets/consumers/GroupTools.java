package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.consumers.ConsumerGroups.GroupList;
import io.github.srdrcn.kafkamcp.toolsets.consumers.ConsumerGroups.GroupSummary;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.GroupLagInfo;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.PartitionLag;

/** Consumer group listing/detail tools: list, get, find_by_member, empty, acls. */
@Component
public class GroupTools {

	/**
	 * Maximum partition rows shown in consumer_groups_get/_lag; in get, also a separate budget for the partition
	 * numbers in member assignments (the rest is counted and omitted).
	 */
	static final int MAX_PARTITION_ROWS = 1000;

	/** Maximum topics shown in consumer_groups_get totals.lagByTopic (by lag, descending). */
	static final int MAX_LAG_BY_TOPIC = 50;

	/** Maximum topics shown per group in consumer_groups_empty. */
	static final int MAX_TOPICS_PER_GROUP = 20;

	/** consumer_groups_find_by_member limits: groups, members per group, topics per member. */
	static final int MAX_MEMBER_GROUPS = 100;

	static final int MAX_MEMBERS_PER_GROUP = 10;

	static final int MAX_TOPICS_PER_MEMBER = 10;

	/** consumer_groups_list: share kept after listGroups, only for the output. */
	private static final long LIST_ONLY_RESERVE_MS = 1_000;

	/** consumer_groups_find_by_member: share kept after listGroups for describeConsumerGroups + output. */
	private static final long LIST_BEFORE_DESCRIBE_RESERVE_MS = 2_000;

	private static final String NO_ACL_NOTE = "No ACLs match this group "
			+ "(if allow.everyone.if.no.acl.found=false, only super users have access)";

	static final List<String> STATES = List.of("Stable", "Empty", "PreparingRebalance", "CompletingRebalance", "Dead",
			"Assigning", "Reconciling", "Unknown");

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("consumer_groups_list", "List consumer groups (groupId, state, type, protocol)")
					.listParams()
					.param("state", P.enumOf(
							"Only groups in this state (Assigning/Reconciling only occur in consumer-type groups on KRaft)",
							STATES))
					.handler(this::list),
				ToolSpec
					.read("consumer_groups_get",
							"Group details: state, coordinator, assignor, members and assignments, per-partition committed/end offset, lag and owner")
					.required("group", Params.group())
					.handler(this::get),
				ToolSpec
					.read("consumer_groups_find_by_member",
							"Find groups and members whose host or clientId matches a regex (at least one is required)")
					.param("host", P.string("Member host regex (e.g. 10\\.1\\.2\\.)"))
					.param("clientId", P.string("Member clientId regex"))
					.handler(this::findByMember),
				ToolSpec
					.read("consumer_groups_empty",
							"Groups in Empty state with the topics they still have committed offsets on and their total lag (likely stale groups)")
					.listParams()
					.handler(this::empty),
				ToolSpec
					.read("consumer_groups_acls",
							"ACLs that apply to the group (literal, prefixed, wildcard): principal, host, operation, permission")
					.required("group", Params.group())
					.handler(this::acls));
	}

	Object list(ToolCall call) {
		String state = call.str("state", null);
		if (state != null && STATES.stream().noneMatch(state::equalsIgnoreCase)) {
			throw new ToolException("Invalid state: " + state + " (valid: " + STATES + ")");
		}
		GroupList list = ConsumerGroups.list(call, LIST_ONLY_RESERVE_MS, call::matches);
		Map<String, Integer> byState = new TreeMap<>();
		List<Map<String, Object>> rows = new ArrayList<>();
		for (GroupSummary g : list.groups()) {
			byState.merge(g.state() == null ? "Unknown" : g.state(), 1, Integer::sum);
			if (state == null || state.equalsIgnoreCase(g.state() == null ? "Unknown" : g.state())) {
				rows.add(g.toMap());
			}
		}
		Map<String, Object> out = Results.map("byState", byState, "partial", list.partial() ? Boolean.TRUE : null,
				"listErrors", list.errors().isEmpty() ? null : list.errors());
		out.putAll(Results.page("groups", rows, call.limit()));
		return out;
	}

	Object get(ToolCall call) {
		return get(call, MAX_PARTITION_ROWS, MAX_LAG_BY_TOPIC);
	}

	/**
	 * maxRows: budget for the partition rows in the offsets section and the partition numbers in member
	 * assignments; maxLagByTopic: maximum topics in totals.lagByTopic.
	 */
	Object get(ToolCall call, int maxRows, int maxLagByTopic) {
		String group = call.str("group");
		GroupDetail g = GroupDetail.load(call, group, null);
		ConsumerGroupDescription d = g.description();
		List<Map<String, Object>> members = new ArrayList<>();
		int assignmentBudget = maxRows;
		int assignmentOmitted = 0;
		for (MemberDescription m : ConsumerGroups.sortedMembers(d)) {
			int assigned = ConsumerGroups.assignedCount(m);
			members.add(ConsumerGroups.memberMap(m, assignmentBudget));
			assignmentOmitted += Math.max(0, assigned - assignmentBudget);
			assignmentBudget = Math.max(0, assignmentBudget - assigned);
		}
		Map<TopicPartition, MemberDescription> incoming = ConsumerGroups.incomingOwners(d);
		int reassigning = 0;
		Map<String, List<PartitionLag>> byTopic = new TreeMap<>();
		for (PartitionLag p : g.lag().partitions()) {
			byTopic.computeIfAbsent(p.topic(), k -> new ArrayList<>()).add(p);
		}
		Map<String, Long> lagByTopic = g.lag().lagByTopic();
		List<Map<String, Object>> topics = new ArrayList<>();
		int budget = maxRows;
		int omitted = 0;
		int topicsOmitted = 0;
		for (Map.Entry<String, List<PartitionLag>> e : byTopic.entrySet()) {
			if (budget <= 0) {
				topicsOmitted++;
				omitted += e.getValue().size();
				continue;
			}
			List<Map<String, Object>> rows = new ArrayList<>();
			int topicOmitted = 0;
			for (PartitionLag p : e.getValue()) {
				if (budget <= 0) {
					topicOmitted++;
					continue;
				}
				budget--;
				MemberDescription owner = g.owners().get(p.topicPartition());
				MemberDescription target = owner == null ? incoming.get(p.topicPartition()) : null;
				reassigning += target == null ? 0 : 1;
				rows.add(Results.map("partition", p.partition(), "committed", p.committed(), "end", p.end(), "lag",
						p.lag(), "owner", owner == null ? null : owner.consumerId(), "reassigningTo",
						target == null ? null : target.consumerId(), "noCommit",
						p.committed() == null ? Boolean.TRUE : null, "error", p.error()));
			}
			omitted += topicOmitted;
			topics.add(Results.map("topic", e.getKey(), "lag", lagByTopic.get(e.getKey()), "partitions", rows,
					"partitionsOmitted", topicOmitted > 0 ? topicOmitted : null));
		}
		int withoutCommit = g.lag().withoutCommit();
		int withoutEnd = (int) g.lag().partitions().stream().filter(p -> p.end() == null).count();
		Map<String, Object> totals = Results.map("lag", g.lag().totalLag(), "lagByTopic",
				first(lagByTopic, maxLagByTopic), "lagByTopicOmitted",
				lagByTopic.size() > maxLagByTopic ? lagByTopic.size() - maxLagByTopic : null, "topics",
				byTopic.size(), "partitions", g.lag().partitions().size(), "partitionsWithoutCommit",
				withoutCommit > 0 ? withoutCommit : null, "partitionsWithoutEndOffset",
				withoutEnd > 0 ? withoutEnd : null);
		String assignor = d.partitionAssignor() == null || d.partitionAssignor().isBlank() ? null
				: d.partitionAssignor();
		List<String> notes = new ArrayList<>();
		if (withoutCommit > 0) {
			notes.add("noCommit: partition is assigned but has no committed offset (lag not computed)");
		}
		if (reassigning > 0) {
			notes.add(ConsumerGroups.REASSIGNING_NOTE);
		}
		Map<String, Object> out = Results.map("groupId", d.groupId(), "state", g.state(), "type",
				ConsumerGroups.type(d.type()), "simple", d.isSimpleConsumerGroup() ? Boolean.TRUE : null,
				"coordinator", ConsumerGroups.node(d.coordinator()), "partitionAssignor", assignor);
		// KIP-848 consumer-type groups (KRaft only): groupEpoch, targetAssignmentEpoch.
		out.putAll(ConsumerGroups.consumerProtocolFields(d));
		out.putAll(Results.map("memberCount", members.size(), "members", members, "assignmentPartitionsOmitted",
				assignmentOmitted > 0 ? assignmentOmitted : null, "offsets", topics, "totals", totals, "partitionRowsOmitted", omitted > 0 ? omitted : null,
				"topicsOmitted", topicsOmitted > 0 ? topicsOmitted : null, "partial",
				withoutEnd > 0 ? Boolean.TRUE : null, "note", notes.isEmpty() ? null : String.join("; ", notes)));
		return out;
	}

	/** First max entries of an ordered map. */
	static <K, V> Map<K, V> first(Map<K, V> map, int max) {
		if (map.size() <= max) {
			return map;
		}
		Map<K, V> out = new LinkedHashMap<>();
		map.entrySet().stream().limit(max).forEach(e -> out.put(e.getKey(), e.getValue()));
		return out;
	}

	Object findByMember(ToolCall call) {
		return findByMember(call, MAX_MEMBER_GROUPS, MAX_MEMBERS_PER_GROUP, MAX_TOPICS_PER_MEMBER);
	}

	/** maxGroups/maxMembers/maxTopics: limits on groups, members per group and topics per member in the output. */
	Object findByMember(ToolCall call, int maxGroups, int maxMembers, int maxTopics) {
		Pattern host = call.regex("host");
		Pattern clientId = call.regex("clientId");
		if (host == null && clientId == null) {
			throw new ToolException("At least one of the host or clientId parameters is required");
		}
		GroupList list = ConsumerGroups.list(call, LIST_BEFORE_DESCRIBE_RESERVE_MS);
		List<GroupSummary> candidates = list.groups().stream().filter(GroupSummary::mayHaveMembers).toList();
		// On an old broker (listing without state) list() has already described the groups: they are not asked again.
		Map<String, ConsumerGroupDescription> known = list.descriptions();
		Map<String, Settled<ConsumerGroupDescription>> fresh = ConsumerGroups.describe(call,
				candidates.stream().map(GroupSummary::groupId).filter(id -> !known.containsKey(id)).toList());
		List<Map<String, Object>> groups = new ArrayList<>();
		int matchedGroups = 0;
		int matchedMembers = 0;
		int skipped = 0;
		int noResponse = 0;
		for (GroupSummary candidate : candidates) {
			ConsumerGroupDescription d = known.get(candidate.groupId());
			if (d == null) {
				Settled<ConsumerGroupDescription> s = fresh.get(candidate.groupId());
				if (s == null || !s.ok()) {
					if (s == null || s.timedOut()) {
						noResponse++;
					}
					else {
						skipped++;
					}
					continue;
				}
				d = s.value();
			}
			List<MemberDescription> matching = new ArrayList<>();
			for (MemberDescription m : ConsumerGroups.sortedMembers(d)) {
				if (host != null && (m.host() == null || !host.matcher(m.host()).find())) {
					continue;
				}
				if (clientId != null && (m.clientId() == null || !clientId.matcher(m.clientId()).find())) {
					continue;
				}
				matching.add(m);
			}
			if (matching.isEmpty()) {
				continue;
			}
			matchedGroups++;
			matchedMembers += matching.size();
			if (groups.size() >= maxGroups) {
				continue;
			}
			List<Map<String, Object>> members = new ArrayList<>();
			for (MemberDescription m : matching.subList(0, Math.min(maxMembers, matching.size()))) {
				List<String> topics = m.assignment() == null ? List.of()
						: new ArrayList<>(ConsumerGroups.byTopic(m.assignment().topicPartitions()).keySet());
				members.add(Results.map("memberId", m.consumerId(), "groupInstanceId",
						m.groupInstanceId().orElse(null), "clientId", m.clientId(), "host", m.host(), "topics",
						topics.subList(0, Math.min(maxTopics, topics.size())), "topicsOmitted",
						topics.size() > maxTopics ? topics.size() - maxTopics : null));
			}
			groups.add(Results.map("groupId", d.groupId(), "state", ConsumerGroups.state(d.groupState()),
					"matchedMembers", matching.size(), "members", members, "membersOmitted",
					matching.size() > maxMembers ? matching.size() - maxMembers : null));
		}
		return Results.map("scannedGroups", candidates.size(), "skippedGroups", skipped > 0 ? skipped : null,
				"groupsWithoutResponse", noResponse > 0 ? noResponse : null, "partial",
				noResponse > 0 || list.partial() ? Boolean.TRUE : null, "matchedGroups", matchedGroups,
				"matchedMembers", matchedMembers, "groupsOmitted",
				matchedGroups > groups.size() ? matchedGroups - groups.size() : null, "listErrors",
				list.errors().isEmpty() ? null : list.errors(), "groups", groups);
	}

	Object empty(ToolCall call) {
		GroupList list = ConsumerGroups.list(call, ConsumerGroups.LIST_RESERVE_MS, call::matches);
		List<GroupSummary> empty = list.groups().stream().filter(GroupSummary::isEmptyState).toList();
		GroupLag.Snapshot snapshot = GroupLag.lagSnapshot(call, empty.stream().map(GroupSummary::groupId).toList(),
				null);
		Map<String, GroupLagInfo> lags = snapshot.groups();
		List<Map<String, Object>> rows = new ArrayList<>();
		int disappeared = 0;
		ScanIssues issues = new ScanIssues();
		for (GroupSummary g : empty) {
			GroupLagInfo info = lags.get(g.groupId());
			if (info == null) {
				continue;
			}
			if (info.error() != null) {
				if (info.notFound()) {
					disappeared++;
				}
				else {
					issues.groupError(info);
				}
				continue;
			}
			issues.unknownLag(info);
			Map<String, Long> byTopic = new TreeMap<>(info.lagByTopic());
			List<Map<String, Object>> topics = new ArrayList<>();
			byTopic.entrySet()
				.stream()
				.limit(MAX_TOPICS_PER_GROUP)
				.forEach(e -> topics.add(Results.map("topic", e.getKey(), "lag", e.getValue())));
			int withoutEnd = info.withoutEnd();
			rows.add(Results.map("groupId", g.groupId(), "type", g.type(), "simple", g.simple() ? Boolean.TRUE : null,
					"totalLag", info.totalLag(), "topicCount", byTopic.size(), "partitions", info.partitions().size(),
					"topics", topics, "topicsOmitted",
					byTopic.size() > MAX_TOPICS_PER_GROUP ? byTopic.size() - MAX_TOPICS_PER_GROUP : null,
					"partitionsWithoutEndOffset", withoutEnd > 0 ? withoutEnd : null));
		}
		Map<String, Object> out = Results.map("skippedGroups", disappeared > 0 ? disappeared : null);
		issues.endErrors(snapshot.end());
		out.putAll(issues.toMap(list));
		out.putAll(Results.page("groups", rows, call.limit()));
		return out;
	}

	Object acls(ToolCall call) {
		String group = call.str("group");
		AclBindingFilter filter = new AclBindingFilter(
				new ResourcePatternFilter(ResourceType.GROUP, group, PatternType.MATCH), AccessControlEntryFilter.ANY);
		Settled<Collection<AclBinding>> s = Settled.of(call, call.admin().describeAcls(filter).values());
		if (!s.ok()) {
			if (s.error() instanceof SecurityDisabledException) {
				return Results.map("group", group, "authorizer", false, "acls", List.of(), "note",
						"Authorizer is disabled on the cluster: ACLs cannot be defined or enforced "
								+ "(access to the group is not restricted)");
			}
			throw new ToolException(s.errorMessage(), s.error());
		}
		List<AclBinding> bindings = new ArrayList<>(s.value());
		bindings.sort(Comparator.comparing((AclBinding b) -> b.entry().principal())
			.thenComparing(b -> b.pattern().patternType().toString())
			.thenComparing(b -> b.pattern().name())
			.thenComparing(b -> b.entry().operation().toString())
			.thenComparing(b -> b.entry().permissionType().toString()));
		List<Map<String, Object>> rows = new ArrayList<>();
		Map<String, Integer> byPatternType = new LinkedHashMap<>();
		TreeSet<String> principals = new TreeSet<>();
		for (AclBinding b : bindings) {
			rows.add(Results.map("principal", b.entry().principal(), "host", b.entry().host(), "operation",
					b.entry().operation().toString(), "permission", b.entry().permissionType().toString(),
					"patternType", b.pattern().patternType().toString(), "resourceName", b.pattern().name()));
			byPatternType.merge(b.pattern().patternType().toString(), 1, Integer::sum);
			principals.add(b.entry().principal());
		}
		return Results.map("group", group, "count", rows.size(), "principals", new ArrayList<>(principals),
				"byPatternType", byPatternType, "acls", rows, "note",
				rows.isEmpty() ? NO_ACL_NOTE : null);
	}

}
