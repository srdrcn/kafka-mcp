package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.GroupSubscribedToTopicException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.Committed;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.OffsetLookup;

/** Irreversible offset/group tools: offsets_reset, offsets_delete, delete. */
@Component
public class OffsetTools {

	static final List<String> TARGETS = List.of("earliest", "latest", "offset", "timestamp", "shift");

	/**
	 * Shares of the deadline kept by the read steps of reset: a partition without a leader cannot use up all the
	 * time in the read steps; at least ~3 s is left for the final request that changes the offsets.
	 */
	private static final long RESET_DESCRIBE_RESERVE_MS = 10_000;

	private static final long RESET_EARLIEST_RESERVE_MS = 8_000;

	private static final long RESET_LATEST_RESERVE_MS = 6_500;

	private static final long RESET_TIMESTAMP_RESERVE_MS = 5_000;

	private static final long RESET_COMMITTED_RESERVE_MS = 3_500;

	/** Target offset of a partition (before clamping) or the reason it was skipped. */
	record Target(Long offset, String skipped) {

		static Target of(long offset) {
			return new Target(offset, null);
		}

		static Target skip(String reason) {
			return new Target(null, reason);
		}

	}

	public List<ToolSpec> tools() {
		Map<String, Object> offset = P.integer("Target offset for to=offset");
		offset.put("minimum", 0);
		return List.of(
				ToolSpec
					.destructive("consumer_groups_offsets_reset",
							"Move the group's offsets on a topic to earliest/latest/offset/timestamp/shift; the group must be Empty (no active members)")
					.required("group", Params.group())
					.required("topic", P.string("Topic name"))
					.param("partitions", Params.intArray("Partition numbers (default: all partitions of the topic)"))
					.required("to", P.enumOf("Target: earliest, latest, offset, timestamp or shift", TARGETS))
					.param("offset", offset)
					.param("timestamp", P.string("Time for to=timestamp (ISO-8601 or epoch ms)"))
					.param("shiftBy", P.integer("Amount added to the current commit for to=shift (can be negative)"))
					.handler(this::reset),
				ToolSpec
					.destructive("consumer_groups_offsets_delete",
							"Delete the group's committed offsets on a topic (all or selected partitions)")
					.required("group", Params.group())
					.required("topic", P.string("Topic name"))
					.param("partitions", Params.intArray("Partition numbers (default: all partitions with a commit)"))
					.handler(this::deleteOffsets),
				ToolSpec
					.destructive("consumer_groups_delete",
							"Delete a consumer group; the group must be empty (no active members)")
					.required("group", Params.group())
					.handler(this::deleteGroup));
	}

	Object reset(ToolCall call) {
		String group = call.str("group");
		String topic = call.str("topic");
		String to = call.str("to").toLowerCase(Locale.ROOT);
		if (!TARGETS.contains(to)) {
			throw new ToolException("Invalid to: " + to + " (valid: " + TARGETS + ")");
		}
		Long offset = null;
		Long shiftBy = null;
		Instant timestamp = null;
		switch (to) {
			case "offset" -> {
				offset = call.optLong("offset");
				if (offset == null) {
					throw new ToolException("The offset parameter is required for to=offset");
				}
			}
			case "shift" -> {
				shiftBy = call.optLong("shiftBy");
				if (shiftBy == null) {
					throw new ToolException("The shiftBy parameter is required for to=shift");
				}
			}
			case "timestamp" -> timestamp = parseTimestamp(call.str("timestamp", null));
			default -> {
			}
		}
		String groupNote = checkResettable(call, group);
		List<TopicPartition> tps = targetPartitions(call, topic);
		OffsetLookup earliest = GroupLag.offsets(call, tps, OffsetSpec.earliest(), RESET_EARLIEST_RESERVE_MS);
		OffsetLookup latest = GroupLag.endOffsets(call, tps, RESET_LATEST_RESERVE_MS);
		OffsetLookup byTime = timestamp == null ? null
				: GroupLag.offsets(call, tps, OffsetSpec.forTimestamp(timestamp.toEpochMilli()),
						RESET_TIMESTAMP_RESERVE_MS);
		Committed committed = GroupLag.committed(call, List.of(group), topic::equals, RESET_COMMITTED_RESERVE_MS)
			.get(group);
		if (committed.error() != null && !committed.notFound()) {
			throw new ToolException("Could not read the current offsets of group '" + group
					+ "'; the offsets were not changed: " + committed.error());
		}
		Map<TopicPartition, Long> before = committed.error() == null ? committed.offsets() : Map.of();

		Map<TopicPartition, OffsetAndMetadata> targets = new LinkedHashMap<>();
		Map<Integer, Map<String, Object>> rows = new TreeMap<>();
		for (TopicPartition tp : tps) {
			Long lo = earliest.offsets().get(tp);
			Long hi = latest.offsets().get(tp);
			Long current = before.get(tp);
			if (lo == null || hi == null) {
				String err = lo == null ? earliest.errors().get(tp) : latest.errors().get(tp);
				rows.put(tp.partition(), Results.map("partition", tp.partition(), "before", current, "skipped",
						"Could not fetch the offset range: " + err));
				continue;
			}
			Target target = target(to, tp, lo, hi, current, offset, shiftBy, byTime);
			if (target.skipped() != null) {
				rows.put(tp.partition(),
						Results.map("partition", tp.partition(), "before", current, "skipped", target.skipped()));
				continue;
			}
			long clamped = Math.max(lo, Math.min(hi, target.offset()));
			targets.put(tp, new OffsetAndMetadata(clamped));
			rows.put(tp.partition(), Results.map("partition", tp.partition(), "before", current, "after", clamped,
					"earliest", lo, "latest", hi, "clamped", clamped != target.offset() ? Boolean.TRUE : null));
		}
		if (targets.isEmpty()) {
			throw new ToolException("Could not compute a target offset for any partition: " + rows.values());
		}
		AlterConsumerGroupOffsetsResult r = call.admin().alterConsumerGroupOffsets(group, targets);
		Settled<Void> all = Settled.of(call, r.all());
		int changed = targets.size();
		if (!all.ok()) {
			changed = 0;
			Throwable first = null;
			for (TopicPartition tp : targets.keySet()) {
				Settled<Void> s = Settled.of(call, r.partitionResult(tp));
				if (s.ok()) {
					changed++;
				}
				else {
					first = first == null ? s.error() : first;
					Map<String, Object> row = rows.get(tp.partition());
					row.remove("after");
					row.put("error", resetError(group, s.error()));
				}
			}
			if (changed == 0) {
				Throwable e = first == null ? all.error() : first;
				throw new ToolException("Could not change the offsets: " + resetError(group, e), e);
			}
		}
		return Results.map("group", group, "topic", topic, "to", to, "timestamp",
				timestamp == null ? null : timestamp.toString(), "groupState", groupNote,
				"note",
				groupNote == null
						? "The group did not exist; the offset commit created a new group (without members)" : null,
				"changedPartitions", changed, "partitions", new ArrayList<>(rows.values()));
	}

	/**
	 * Target offset of the partition (before clamping). timestamp: latest if there is no record after that time
	 * (offset -1); skipped if the timestamp query failed for that partition (never silently moved to latest).
	 */
	static Target target(String to, TopicPartition tp, long earliest, long latest, Long current, Long offset,
			Long shiftBy, OffsetLookup byTime) {
		return switch (to) {
			case "earliest" -> Target.of(earliest);
			case "latest" -> Target.of(latest);
			case "offset" -> Target.of(offset);
			case "timestamp" -> {
				Long t = byTime.offsets().get(tp);
				if (t == null) {
					String err = byTime.errors().get(tp);
					yield Target.skip("Could not fetch the timestamp offset: " + (err == null ? "no response" : err));
				}
				yield Target.of(t < 0 ? latest : t);
			}
			default -> current == null ? Target.skip("No committed offset; cannot apply shift")
					: Target.of(current + shiftBy);
		};
	}

	/** Can the group's offsets be reset; ToolException if not. Returns the group state, null if there is no group. */
	private static String checkResettable(ToolCall call, String group) {
		Settled<ConsumerGroupDescription> s = ConsumerGroups.describe(call, List.of(group), RESET_DESCRIBE_RESERVE_MS)
			.get(group);
		if (!s.ok()) {
			if (ConsumerGroups.notFound(s.error())) {
				return null;
			}
			throw new ToolException(s.errorMessage(), s.error());
		}
		ConsumerGroupDescription d = s.value();
		String state = ConsumerGroups.state(d.groupState());
		if (!d.members().isEmpty()) {
			throw new ToolException("Group '" + group + "' has " + d.members().size() + " active member(s) (state="
					+ state + "); the group must be Empty to reset offsets. Stop the consumers first.");
		}
		GroupState gs = d.groupState();
		if (gs == GroupState.EMPTY) {
			return state;
		}
		if (ConsumerGroups.deadOrUnknown(d)) {
			return null;
		}
		throw new ToolException("Group '" + group + "' is in state " + state
				+ "; the group must be Empty (no active members) to reset offsets.");
	}

	/** The partitions parameter, or all partitions of the topic (single describeTopics). */
	private static List<TopicPartition> targetPartitions(ToolCall call, String topic) {
		TopicDescription td = call.await(call.admin().describeTopics(List.of(topic)).topicNameValues().get(topic));
		Set<Integer> existing = new TreeSet<>();
		for (TopicPartitionInfo p : td.partitions()) {
			existing.add(p.partition());
		}
		List<Integer> requested = Params.partitions(call);
		if (requested.isEmpty()) {
			requested = new ArrayList<>(existing);
		}
		List<Integer> missing = requested.stream().filter(p -> !existing.contains(p)).toList();
		if (!missing.isEmpty()) {
			throw new ToolException("Partitions not in topic '" + topic + "': " + missing + " (partition count "
					+ existing.size() + ")");
		}
		return requested.stream().map(p -> new TopicPartition(topic, p)).toList();
	}

	private static String resetError(String group, Throwable e) {
		Throwable root = KafkaErrors.unwrap(e);
		// classic: UnknownMemberId/RebalanceInProgress/IllegalGeneration/FencedInstanceId; KIP-848 consumer-type
		// groups (KRaft): StaleMemberEpoch/FencedMemberEpoch.
		if (root instanceof UnknownMemberIdException || root instanceof RebalanceInProgressException
				|| root instanceof FencedInstanceIdException || root instanceof IllegalGenerationException
				|| root instanceof StaleMemberEpochException || root instanceof FencedMemberEpochException) {
			return "An active member joined group '" + group + "' in the meantime; the group must be Empty ("
					+ Settled.explain(root) + ")";
		}
		return Settled.explain(root);
	}

	static Instant parseTimestamp(String value) {
		if (value == null) {
			throw new ToolException("The timestamp parameter is required for to=timestamp (ISO-8601 or epoch ms)");
		}
		String v = value.trim();
		if (v.matches("-?\\d+")) {
			return Instant.ofEpochMilli(Long.parseLong(v));
		}
		try {
			return Instant.parse(v);
		}
		catch (DateTimeParseException ignored) {
			// other formats
		}
		try {
			return OffsetDateTime.parse(v).toInstant();
		}
		catch (DateTimeParseException ignored) {
			// other formats
		}
		try {
			return ZonedDateTime.parse(v).toInstant();
		}
		catch (DateTimeParseException ignored) {
			// other formats
		}
		try {
			return LocalDateTime.parse(v).toInstant(ZoneOffset.UTC);
		}
		catch (DateTimeParseException ignored) {
			// other formats
		}
		try {
			return LocalDate.parse(v).atStartOfDay().toInstant(ZoneOffset.UTC);
		}
		catch (DateTimeParseException e) {
			throw new ToolException("Invalid timestamp: " + v
					+ " (ISO-8601, e.g. 2024-05-01T10:00:00Z / 2024-05-01T13:00:00+03:00, or epoch ms)");
		}
	}

	Object deleteOffsets(ToolCall call) {
		String group = call.str("group");
		String topic = call.str("topic");
		Committed committed = GroupLag.committed(call, List.of(group), topic::equals).get(group);
		if (committed.error() != null) {
			throw committed.notFound() ? ConsumerGroups.notFoundError(group) : new ToolException(committed.error());
		}
		// Only partitions with a commit are deleted: the broker also returns without an error for a partition without
		// a commit (or one that does not exist); these must not be reported as "deleted".
		Set<TopicPartition> tps = new TreeSet<>(
				Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
		List<Integer> requested = Params.partitions(call);
		Map<Integer, Map<String, Object>> rows = new TreeMap<>();
		if (requested.isEmpty()) {
			tps.addAll(committed.offsets().keySet());
		}
		else {
			for (int p : requested) {
				TopicPartition tp = new TopicPartition(topic, p);
				if (committed.offsets().containsKey(tp)) {
					tps.add(tp);
				}
				else {
					rows.put(p, Results.map("partition", p, "deleted", false, "note",
							"No committed offset (nothing to delete)"));
				}
			}
		}
		if (tps.isEmpty()) {
			// ZooKeeper (classic coordinator) returns an empty result instead of an error for the offsets of a missing
			// group: the group's existence is checked so that "group not found" is reported, as on KRaft, instead of
			// "no commit".
			if (committed.isEmpty() && !groupExists(call, group)) {
				throw ConsumerGroups.notFoundError(group);
			}
			throw new ToolException("Group '" + group + "' has no committed offsets on topic '" + topic + "'"
					+ (requested.isEmpty() ? "" : " in the requested partitions " + requested));
		}
		DeleteConsumerGroupOffsetsResult r = call.admin().deleteConsumerGroupOffsets(group, tps);
		Settled<Void> all = Settled.of(call, r.all());
		int deleted = 0;
		Throwable first = null;
		for (TopicPartition tp : tps) {
			Settled<Void> s = all.ok() ? all : Settled.of(call, r.partitionResult(tp));
			Long before = committed.offsets().get(tp);
			if (s.ok()) {
				deleted++;
				rows.put(tp.partition(),
						Results.map("partition", tp.partition(), "committedBefore", before, "deleted", true));
			}
			else {
				first = first == null ? s.error() : first;
				rows.put(tp.partition(), Results.map("partition", tp.partition(), "committedBefore", before, "deleted",
						false, "error", deleteOffsetsError(group, topic, s.error())));
			}
		}
		if (deleted == 0) {
			Throwable e = first == null ? all.error() : first;
			if (ConsumerGroups.notFound(e)) {
				throw ConsumerGroups.notFoundError(group);
			}
			throw new ToolException("Could not delete the offsets: " + deleteOffsetsError(group, topic, e), e);
		}
		int withoutCommit = rows.size() - tps.size();
		return Results.map("group", group, "topic", topic, "deletedPartitions", deleted, "failedPartitions",
				tps.size() - deleted, "partitionsWithoutCommit", withoutCommit > 0 ? withoutCommit : null,
				"partitions", new ArrayList<>(rows.values()));
	}

	/**
	 * Does the group exist: KRaft (4.x) returns GroupIdNotFound for a missing group, ZooKeeper/old brokers return
	 * Dead with no members. If describe fails (authorization, no response), the group is assumed to exist, so "not
	 * found" is never reported by mistake.
	 */
	private static boolean groupExists(ToolCall call, String group) {
		Settled<ConsumerGroupDescription> s = ConsumerGroups.describe(call, List.of(group)).get(group);
		if (s == null) {
			return true;
		}
		if (!s.ok()) {
			return !ConsumerGroups.notFound(s.error());
		}
		return !ConsumerGroups.deadOrUnknown(s.value());
	}

	private static String deleteOffsetsError(String group, String topic, Throwable e) {
		Throwable root = KafkaErrors.unwrap(e);
		if (root instanceof GroupSubscribedToTopicException) {
			return "Group '" + group + "' is still subscribed to topic '" + topic
					+ "' (active members are consuming it); Kafka rejected the delete. Stop the consumers first";
		}
		return Settled.explain(root);
	}

	Object deleteGroup(ToolCall call) {
		String group = call.str("group");
		Committed committed = GroupLag.committed(call, List.of(group), null).get(group);
		Settled<Void> s = Settled.of(call, call.admin().deleteConsumerGroups(List.of(group)).deletedGroups().get(group));
		if (!s.ok()) {
			Throwable root = KafkaErrors.unwrap(s.error());
			if (ConsumerGroups.notFound(root)) {
				throw ConsumerGroups.notFoundError(group);
			}
			if (root instanceof GroupNotEmptyException) {
				Settled<ConsumerGroupDescription> d = ConsumerGroups.describe(call, List.of(group)).get(group);
				String detail = d != null && d.ok() ? " (state=" + ConsumerGroups.state(d.value().groupState())
						+ ", members=" + d.value().members().size() + ")" : "";
				throw new ToolException("Could not delete group '" + group
						+ "': the group is not empty, it has active members" + detail
						+ ". Stop the consumers first; retry when the group is Empty.", root);
			}
			throw new ToolException("Could not delete group '" + group + "': " + Settled.explain(root), root);
		}
		Map<String, Object> offsets = null;
		if (committed != null && committed.error() == null) {
			offsets = Results.map("topics", new ArrayList<>(ConsumerGroups.byTopic(committed.offsets().keySet()).keySet()),
					"partitions", committed.offsets().size());
		}
		return Results.map("group", group, "deleted", true, "deletedOffsets", offsets);
	}

}
