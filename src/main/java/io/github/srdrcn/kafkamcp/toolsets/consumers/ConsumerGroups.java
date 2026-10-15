package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.SupportedVersionRange;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Helpers for listing and bulk-describing consumer groups (state/type names, members, partition owners). Each
 * call is a single admin request; an error or a missing response for one group does not affect the others.
 */
public final class ConsumerGroups {

	/** list(): default share of the deadline kept for the committed + end offset queries that follow. */
	public static final long LIST_RESERVE_MS = 6_000;

	/** describe(): default share of the deadline kept for the output. */
	public static final long DESCRIBE_RESERVE_MS = 1_000;

	/** Explanation of the reassigningTo field in the get/lag output. */
	public static final String REASSIGNING_NOTE = "reassigningTo: owner unknown; the partition is being moved to this "
			+ "member (KIP-848 reconciling). The previous owner is not shown in describe until it finishes revoking "
			+ "and may still be consuming";

	private static final String METADATA_VERSION = "metadata.version";

	/** metadata.version 3.7-IV0: first version with KIP-848 consumer-type groups (early access). */
	private static final short CONSUMER_GROUPS_MIN_METADATA_LEVEL = 15;

	/** A row of the group list. state/type are null if the broker does not report them. */
	public record GroupSummary(String groupId, String state, String type, String protocol, boolean simple) {

		public boolean isEmptyState() {
			return GroupState.EMPTY.toString().equals(state);
		}

		/** Can the group have members (not Empty/Dead)? */
		public boolean mayHaveMembers() {
			return !GroupState.EMPTY.toString().equals(state) && !GroupState.DEAD.toString().equals(state);
		}

		public Map<String, Object> toMap() {
			return Results.map("groupId", groupId, "state", state, "type", type, "protocol", protocol, "simple",
					simple ? Boolean.TRUE : null);
		}

	}

	/**
	 * listGroups result: the groups listed successfully (sorted by groupId), broker errors (if any, the list may be
	 * incomplete; on old brokers groups whose state could not be completed are also counted here) and the
	 * descriptions of groups already described while completing the list (old brokers), so they are not described
	 * again.
	 */
	public record GroupList(List<GroupSummary> groups, List<String> errors,
			Map<String, ConsumerGroupDescription> descriptions) {

		public GroupList(List<GroupSummary> groups, List<String> errors) {
			this(groups, errors, Map.of());
		}

		/** May the list be incomplete (brokers without response or with errors, groups with unknown state)? */
		public boolean partial() {
			return !errors.isEmpty();
		}

		public Map<String, GroupSummary> byId() {
			Map<String, GroupSummary> m = new LinkedHashMap<>();
			groups.forEach(g -> m.put(g.groupId(), g));
			return m;
		}

	}

	private ConsumerGroups() {
	}

	/**
	 * All consumer groups (classic + consumer protocol) with a single listGroups call; keeps a share of
	 * {@link #LIST_RESERVE_MS} of the deadline.
	 */
	public static GroupList list(ToolCall call) {
		return list(call, LIST_RESERVE_MS);
	}

	/**
	 * All consumer groups; the request finishes within (remaining time - reserveMillis) at the latest. Brokers that
	 * do not respond end up in {@link GroupList#errors()}.
	 */
	public static GroupList list(ToolCall call, long reserveMillis) {
		return list(call, reserveMillis, null);
	}

	/**
	 * {@link #list(ToolCall, long)}; if groupFilter is not null, only matching groups are returned (on old brokers
	 * only those are described to complete the missing state/type).
	 */
	public static GroupList list(ToolCall call, long reserveMillis, Predicate<String> groupFilter) {
		ListGroupsResult r = call.admin()
			.listGroups(ListGroupsOptions.forConsumerGroups().timeoutMs(Settled.timeoutMs(call, reserveMillis)));
		Settled<Collection<GroupListing>> valid = Settled.within(call, r.valid(), reserveMillis);
		Settled<Collection<Throwable>> errors = Settled.within(call, r.errors(), reserveMillis);
		if (!valid.ok()) {
			if (valid.timedOut()) {
				throw new ToolException("Could not list consumer groups in time (brokers did not respond)",
						valid.error());
			}
			throw new ToolException(valid.errorMessage(), valid.error());
		}
		List<String> errorMessages = new ArrayList<>();
		if (errors.ok() && errors.value() != null) {
			errors.value().forEach(e -> errorMessages.add(Settled.explain(e)));
		}
		if (valid.value().isEmpty() && !errorMessages.isEmpty()) {
			throw new ToolException(errorMessages.get(0));
		}
		List<GroupSummary> groups = new ArrayList<>();
		for (GroupListing g : valid.value()) {
			if (groupFilter != null && !groupFilter.test(g.groupId())) {
				continue;
			}
			String protocol = g.protocol() == null || g.protocol().isBlank() ? null : g.protocol();
			// GroupListing.isSimpleConsumerGroup() requires the broker to report the type (ListGroups v5+, Kafka
			// 3.8+); on old (ZooKeeper) brokers simple groups were never marked. An empty protocol type only occurs
			// in classic groups: without a type the group also counts as simple.
			boolean simple = protocol == null && g.type().map(t -> t == GroupType.CLASSIC).orElse(true);
			groups.add(new GroupSummary(g.groupId(), g.groupState().map(ConsumerGroups::state).orElse(null),
					g.type().map(ConsumerGroups::type).orElse(null), protocol, simple));
		}
		groups.sort(Comparator.comparing(GroupSummary::groupId));
		return complete(call, groups, errorMessages, reserveMillis);
	}

	/**
	 * Completes the state/type fields missing from the list:
	 * <ul>
	 * <li>Brokers before Kafka 2.6 (before ListGroups v4) do not return the state: groups with an unknown state are
	 * described so that the Empty/Stable filters and consumer_groups_empty do not silently miss them. Those without a
	 * response or with an error stay without a state and are counted in {@link GroupList#errors()} (partial).</li>
	 * <li>Brokers before Kafka 3.8 (before ListGroups v5; most 2.6-3.7 ZooKeeper fleets) do not return the type.
	 * Simple groups (empty protocol) and classic-/consumer-specific states determine the type; for the rest a
	 * single describeFeatures: on ZooKeeper (no finalized metadata.version) and on KRaft before 3.7 there cannot be
	 * consumer-type groups, all are classic. Only on KRaft 3.7 (KIP-848 early access) are they described.</li>
	 * </ul>
	 */
	private static GroupList complete(ToolCall call, List<GroupSummary> groups, List<String> errors,
			long reserveMillis) {
		List<GroupSummary> typed = new ArrayList<>(groups.size());
		for (GroupSummary g : groups) {
			String type = g.type() != null ? g.type() : inferType(g);
			typed.add(new GroupSummary(g.groupId(), g.state(), type, g.protocol(), g.simple()));
		}
		Set<String> toDescribe = new LinkedHashSet<>();
		typed.stream().filter(g -> g.state() == null).forEach(g -> toDescribe.add(g.groupId()));
		boolean typeless = typed.stream().anyMatch(g -> g.type() == null);
		Boolean consumerTypePossible = typeless ? consumerTypeGroupsPossible(call, reserveMillis) : null;
		if (Boolean.TRUE.equals(consumerTypePossible)) {
			typed.stream().filter(g -> g.type() == null).forEach(g -> toDescribe.add(g.groupId()));
		}
		Map<String, Settled<ConsumerGroupDescription>> described = describe(call, toDescribe, reserveMillis);
		Map<String, ConsumerGroupDescription> descriptions = new LinkedHashMap<>();
		List<GroupSummary> out = new ArrayList<>(typed.size());
		List<String> outErrors = new ArrayList<>(errors);
		int stateUnknown = 0;
		String firstError = null;
		for (GroupSummary g : typed) {
			// The broker does not report the type and consumer-type groups are impossible: classic.
			String type = g.type() == null && Boolean.FALSE.equals(consumerTypePossible) ? type(GroupType.CLASSIC)
					: g.type();
			String state = g.state();
			Settled<ConsumerGroupDescription> s = described.get(g.groupId());
			if (s != null && s.ok()) {
				ConsumerGroupDescription d = s.value();
				descriptions.put(g.groupId(), d);
				state = state != null ? state : state(d.groupState());
				type = type != null ? type : type(d.type());
			}
			else if (s != null && state == null) {
				if (notFound(s.error())) {
					// deleted after the listing: Dead, as old brokers return for a missing group
					state = state(GroupState.DEAD);
				}
				else {
					stateUnknown++;
					firstError = firstError != null ? firstError : s.errorMessage();
				}
			}
			out.add(new GroupSummary(g.groupId(), state, type, g.protocol(), g.simple()));
		}
		if (stateUnknown > 0) {
			outErrors.add("Groups whose state could not be fetched: " + stateUnknown
					+ " (the broker does not report the state in the listing; describe: " + firstError + ")");
		}
		return new GroupList(out, outErrors, descriptions);
	}

	/** Type that can be determined without describe if the listing has no type (Kafka &lt; 3.8); otherwise null. */
	private static String inferType(GroupSummary g) {
		if (g.simple()) {
			// an empty protocol type only occurs in classic groups
			return type(GroupType.CLASSIC);
		}
		if (g.state() == null) {
			return null;
		}
		GroupState s = GroupState.parse(g.state());
		if (s == GroupState.PREPARING_REBALANCE || s == GroupState.COMPLETING_REBALANCE) {
			return type(GroupType.CLASSIC);
		}
		if (s == GroupState.ASSIGNING || s == GroupState.RECONCILING) {
			return type(GroupType.CONSUMER);
		}
		return null;
	}

	/**
	 * Can a cluster that does not report the type (Kafka &lt; 3.8) have KIP-848 consumer-type groups? Only on KRaft
	 * 3.7 (early access): ZooKeeper has no finalized metadata.version (describeFeatures is empty), and on KRaft it
	 * cannot if the highest metadata.version the broker supports is below 3.7-IV0. null if the query is not
	 * answered in time (the type stays unknown; no heavy describe is done).
	 */
	private static Boolean consumerTypeGroupsPossible(ToolCall call, long reserveMillis) {
		DescribeFeaturesOptions options = new DescribeFeaturesOptions().timeoutMs(Settled.timeoutMs(call, reserveMillis));
		Settled<FeatureMetadata> s = Settled.within(call, call.admin().describeFeatures(options).featureMetadata(),
				reserveMillis);
		if (!s.ok() || s.value() == null) {
			return null;
		}
		if (!s.value().finalizedFeatures().containsKey(METADATA_VERSION)) {
			return false;
		}
		SupportedVersionRange supported = s.value().supportedFeatures().get(METADATA_VERSION);
		return supported == null || supported.maxVersion() >= CONSUMER_GROUPS_MIN_METADATA_LEVEL;
	}

	/**
	 * Describes the groups with a single describeConsumerGroups call; groups that disappeared come back as errors,
	 * those without a response as {@link Settled#timedOut()}. Keeps a share of {@link #DESCRIBE_RESERVE_MS}.
	 */
	public static Map<String, Settled<ConsumerGroupDescription>> describe(ToolCall call, Collection<String> groups) {
		return describe(call, groups, DESCRIBE_RESERVE_MS);
	}

	/** {@link #describe(ToolCall, Collection)}; the request finishes within (remaining time - reserveMillis). */
	public static Map<String, Settled<ConsumerGroupDescription>> describe(ToolCall call, Collection<String> groups,
			long reserveMillis) {
		Map<String, Settled<ConsumerGroupDescription>> out = new LinkedHashMap<>();
		if (groups.isEmpty()) {
			return out;
		}
		List<String> ids = new ArrayList<>(new LinkedHashSet<>(groups));
		DescribeConsumerGroupsOptions options = new DescribeConsumerGroupsOptions()
			.timeoutMs(Settled.timeoutMs(call, reserveMillis));
		Map<String, Settled<ConsumerGroupDescription>> settled = Settled.all(call,
				call.admin().describeConsumerGroups(ids, options).describedGroups(), reserveMillis);
		for (String id : ids) {
			out.put(id, settled.get(id));
		}
		return out;
	}

	/** Describes a single group; {@link ToolException} if the group does not exist. */
	public static ConsumerGroupDescription describeExisting(ToolCall call, String group) {
		return describeExisting(call, group, DESCRIBE_RESERVE_MS);
	}

	/** {@link #describeExisting(ToolCall, String)}; keeps a share of reserveMillis of the deadline. */
	public static ConsumerGroupDescription describeExisting(ToolCall call, String group, long reserveMillis) {
		Settled<ConsumerGroupDescription> s = describe(call, List.of(group), reserveMillis).get(group);
		if (s == null || !s.ok()) {
			if (s != null && notFound(s.error())) {
				throw notFoundError(group);
			}
			throw new ToolException(s == null ? "Could not describe group: " + group : s.errorMessage(),
					s == null ? null : s.error());
		}
		return s.value();
	}

	public static ToolException notFoundError(String group) {
		return new ToolException("Consumer group not found: " + group + " (see consumer_groups_list)");
	}

	public static boolean notFound(Throwable e) {
		return KafkaErrors.unwrap(e) instanceof GroupIdNotFoundException;
	}

	/** Old brokers return Dead with no members for a missing group. */
	public static boolean deadOrUnknown(ConsumerGroupDescription d) {
		GroupState s = d.groupState();
		return (s == null || s == GroupState.DEAD || s == GroupState.UNKNOWN) && d.members().isEmpty();
	}

	/** "Stable", "Empty", ... (Kafka's own names). */
	public static String state(GroupState s) {
		return s == null ? null : s.toString();
	}

	/** "classic" or "consumer" (KIP-848). */
	public static String type(GroupType t) {
		return t == null ? null : t.toString().toLowerCase(Locale.ROOT);
	}

	/** Coordinator broker {id, host, port}; null if unknown. */
	public static Map<String, Object> node(Node n) {
		if (n == null || n.isEmpty()) {
			return null;
		}
		return Results.map("id", n.id(), "host", n.host(), "port", n.port());
	}

	/** Groups a partition list as topic → sorted partition numbers (sorted by topic name). */
	public static Map<String, List<Integer>> byTopic(Collection<TopicPartition> partitions) {
		Map<String, List<Integer>> m = new TreeMap<>();
		for (TopicPartition tp : partitions) {
			m.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp.partition());
		}
		m.values().forEach(l -> l.sort(Comparator.naturalOrder()));
		return m;
	}

	/** Assigned partition → owner member. */
	public static Map<TopicPartition, MemberDescription> owners(ConsumerGroupDescription d) {
		Map<TopicPartition, MemberDescription> m = new HashMap<>();
		if (d == null) {
			return m;
		}
		for (MemberDescription member : d.members()) {
			if (member.assignment() != null) {
				member.assignment().topicPartitions().forEach(tp -> m.put(tp, member));
			}
		}
		return m;
	}

	/** Members sorted by clientId, then memberId. */
	public static List<MemberDescription> sortedMembers(ConsumerGroupDescription d) {
		List<MemberDescription> members = new ArrayList<>(d.members());
		members.sort(Comparator.comparing((MemberDescription m) -> String.valueOf(m.clientId()))
			.thenComparing(m -> String.valueOf(m.consumerId())));
		return members;
	}

	/** Member row: memberId, groupInstanceId, clientId, host and topic → partition assignment. */
	public static Map<String, Object> memberMap(MemberDescription m) {
		return memberMap(m, Integer.MAX_VALUE);
	}

	/** Number of partitions assigned to the member. */
	public static int assignedCount(MemberDescription m) {
		return m.assignment() == null ? 0 : m.assignment().topicPartitions().size();
	}

	/**
	 * Member row; at most maxPartitions partition numbers are shown in the assignment (in topic name order; the last
	 * topic may be cut). If truncated, the assignedPartitions and assignmentPartitionsOmitted counts are added.
	 */
	public static Map<String, Object> memberMap(MemberDescription m, int maxPartitions) {
		Map<String, List<Integer>> full = m.assignment() == null ? Map.of() : byTopic(m.assignment().topicPartitions());
		int total = assignedCount(m);
		Map<String, List<Integer>> assignment = full;
		if (total > maxPartitions) {
			assignment = new LinkedHashMap<>();
			int budget = Math.max(0, maxPartitions);
			for (Map.Entry<String, List<Integer>> e : full.entrySet()) {
				if (budget <= 0) {
					break;
				}
				List<Integer> shown = e.getValue().subList(0, Math.min(budget, e.getValue().size()));
				assignment.put(e.getKey(), shown);
				budget -= shown.size();
			}
		}
		int omitted = total - assignment.values().stream().mapToInt(List::size).sum();
		Map<String, Object> out = Results.map("memberId", m.consumerId(), "groupInstanceId",
				m.groupInstanceId().orElse(null), "clientId", m.clientId(), "host", m.host(), "assignment",
				assignment.isEmpty() && omitted > 0 ? null : assignment, "assignedPartitions",
				omitted > 0 ? total : null, "assignmentPartitionsOmitted", omitted > 0 ? omitted : null);
		out.putAll(consumerProtocolFields(m));
		return out;
	}

	/**
	 * KIP-848 (consumer type, KRaft only) member fields: memberEpoch; pendingPartitions, the number of partitions in
	 * the target assignment not yet given to the member (waiting until another member releases them); classicMember
	 * for a member still connected to a consumer-type group with the classic protocol (online migration). Classic
	 * groups (always the case on ZooKeeper) have none of these fields; an empty map is returned.
	 * <p>
	 * Partitions to be released are not reported: the assignment in ConsumerGroupDescribe is the broker's
	 * assignedPartitions (current ∩ target); partitions being revoked (partitionsPendingRevocation) are not in the
	 * response. The assignment \ target difference is only non-empty in the short moment between a new target epoch
	 * and the member's next heartbeat; during an actual revoke it is empty.
	 */
	public static Map<String, Object> consumerProtocolFields(MemberDescription m) {
		Map<String, Object> out = new LinkedHashMap<>();
		m.memberEpoch().ifPresent(e -> out.put("memberEpoch", e));
		if (m.upgraded().isPresent() && !m.upgraded().get()) {
			out.put("classicMember", Boolean.TRUE);
		}
		long pending = incoming(m).size();
		if (pending > 0) {
			out.put("pendingPartitions", pending);
		}
		return out;
	}

	/** Partitions in the member's target assignment but not in its current assignment (KIP-848); empty for classic. */
	private static Set<TopicPartition> incoming(MemberDescription m) {
		if (m.targetAssignment().isEmpty()) {
			return Set.of();
		}
		Set<TopicPartition> current = m.assignment() == null ? Set.of() : m.assignment().topicPartitions();
		Set<TopicPartition> out = new LinkedHashSet<>(m.targetAssignment().get().topicPartitions());
		out.removeAll(current);
		return out;
	}

	/**
	 * In a KIP-848 consumer-type group (KRaft only): partition given to a member in the target assignment but not yet
	 * moved to it → target member. The current owner of these partitions (the member revoking them) is not visible
	 * in the describe response: {@link #owners} leaves them without an owner, but consumption may still be going on.
	 * Empty for classic groups.
	 */
	public static Map<TopicPartition, MemberDescription> incomingOwners(ConsumerGroupDescription d) {
		Map<TopicPartition, MemberDescription> m = new HashMap<>();
		if (d == null) {
			return m;
		}
		for (MemberDescription member : d.members()) {
			incoming(member).forEach(tp -> m.put(tp, member));
		}
		return m;
	}

	/** KIP-848 group epochs (groupEpoch, targetAssignmentEpoch); empty map for classic groups. */
	public static Map<String, Object> consumerProtocolFields(ConsumerGroupDescription d) {
		Map<String, Object> out = new LinkedHashMap<>();
		d.groupEpoch().ifPresent(e -> out.put("groupEpoch", e));
		d.targetAssignmentEpoch().ifPresent(e -> out.put("targetAssignmentEpoch", e));
		return out;
	}

}
