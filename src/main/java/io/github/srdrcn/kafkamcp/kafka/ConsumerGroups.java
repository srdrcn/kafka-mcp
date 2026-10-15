package io.github.srdrcn.kafkamcp.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Consumer group and lag helpers. A single listConsumerGroupOffsets / describeConsumerGroups is sent for many
 * groups; per-group errors do not fail the tool.
 */
public final class ConsumerGroups {

	private ConsumerGroups() {
	}

	/**
	 * Summary of a group on a topic: state, member count, total lag, number of committed partitions and group
	 * type ({@link #type}: "Classic" or KIP-848 "Consumer" on KRaft 4.x; null if unknown).
	 */
	public record TopicLag(String groupId, String state, Integer members, long lag, int partitions, String type) {

		/** Without type info (backward compatibility). */
		public TopicLag(String groupId, String state, Integer members, long lag, int partitions) {
			this(groupId, state, members, lag, partitions, null);
		}

	}

	/**
	 * {@link #forTopic} result: groups (by lag, descending), number of groups scanned, errors (keyed by group id
	 * or listGroups#n) and the number of partitions left out of the lag because their latest offset was not available.
	 */
	public record TopicConsumers(List<TopicLag> groups, int groupsScanned, Map<String, String> errors,
			int lagUnknownPartitions) {
	}

	/**
	 * Share of the remaining time that {@link #forTopic} spends waiting for committed offsets, so a loading or
	 * unreachable group coordinator cannot use up all the time and drop the results of the other groups.
	 */
	static final double COMMITTED_WAIT_SHARE = 0.6;

	/** Group list and the error messages of brokers that did not respond. */
	public record GroupList(Map<String, GroupListing> groups, List<String> errors) {
	}

	/**
	 * Consumer groups (classic + consumer protocol; share/streams excluded), sorted by id. If some brokers fail,
	 * the groups from the others are returned and the errors are reported separately; if none responds, an
	 * error is thrown.
	 */
	public static GroupList list(ToolCall call) {
		ListGroupsResult r = call.admin().listGroups();
		Collection<GroupListing> valid = call.await(r.valid());
		List<String> errors = call.await(r.errors()).stream().map(KafkaErrors::explain).toList();
		if (valid.isEmpty() && !errors.isEmpty()) {
			throw new ToolException(errors.get(0));
		}
		Map<String, GroupListing> out = new TreeMap<>();
		for (GroupListing g : valid) {
			GroupType type = g.type().orElse(GroupType.UNKNOWN);
			if (type == GroupType.SHARE || type == GroupType.STREAMS) {
				continue;
			}
			String protocol = g.protocol() == null ? "" : g.protocol();
			if (protocol.isEmpty() || "consumer".equals(protocol)) {
				out.put(g.groupId(), g);
			}
		}
		return new GroupList(out, errors);
	}

	/**
	 * Committed offsets of the groups (a single listConsumerGroupOffsets). If {@code tps} is empty, all
	 * partitions of the group; partitions without a commit are absent from the result.
	 */
	public static Futures.Each<String, Map<TopicPartition, Long>> committed(ToolCall call, Collection<String> groupIds,
			Collection<TopicPartition> tps) {
		return committed(call, groupIds, tps, Long.MAX_VALUE);
	}

	/**
	 * {@link #committed(ToolCall, Collection, Collection)}; waits at most {@code maxMillis}, groups that do not
	 * respond end up in the errors.
	 */
	public static Futures.Each<String, Map<TopicPartition, Long>> committed(ToolCall call, Collection<String> groupIds,
			Collection<TopicPartition> tps, long maxMillis) {
		Map<String, ListConsumerGroupOffsetsSpec> specs = new LinkedHashMap<>();
		for (String g : groupIds) {
			ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec();
			if (tps != null && !tps.isEmpty()) {
				spec.topicPartitions(tps);
			}
			specs.put(g, spec);
		}
		if (specs.isEmpty()) {
			return new Futures.Each<>(new LinkedHashMap<>(), new LinkedHashMap<>());
		}
		ListConsumerGroupOffsetsResult r = call.admin().listConsumerGroupOffsets(specs);
		Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = new LinkedHashMap<>();
		for (String g : specs.keySet()) {
			futures.put(g, r.partitionsToOffsetAndMetadata(g));
		}
		Futures.Each<String, Map<TopicPartition, OffsetAndMetadata>> raw = Futures.awaitEach(call, futures, maxMillis);
		Map<String, Map<TopicPartition, Long>> values = new LinkedHashMap<>();
		raw.values().forEach((g, m) -> {
			Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
			if (m != null) {
				m.forEach((tp, om) -> {
					if (om != null && om.offset() >= 0) {
						offsets.put(tp, om.offset());
					}
				});
			}
			values.put(g, offsets);
		});
		return new Futures.Each<>(values, raw.errors());
	}

	/**
	 * Group type: "Classic" (always on ZooKeeper clusters) or "Consumer" (KRaft 4.x, KIP-848); null if unknown.
	 * The description is used first, otherwise the listGroups entry (older brokers return no type in the list).
	 */
	public static String type(ConsumerGroupDescription description, GroupListing listing) {
		GroupType t = description != null ? description.type() : null;
		if ((t == null || t == GroupType.UNKNOWN) && listing != null) {
			t = listing.type().orElse(GroupType.UNKNOWN);
		}
		return t == null || t == GroupType.UNKNOWN ? null : t.toString();
	}

	/** describeConsumerGroups (a single call); per-group errors are kept separately. */
	public static Futures.Each<String, ConsumerGroupDescription> describe(ToolCall call, Collection<String> groupIds) {
		if (groupIds.isEmpty()) {
			return new Futures.Each<>(new LinkedHashMap<>(), new LinkedHashMap<>());
		}
		return Futures.awaitEach(call, call.admin().describeConsumerGroups(groupIds).describedGroups());
	}

	/** lag = latest - committed (at least 0). */
	public static long lag(long committed, long latest) {
		return Math.max(0, latest - committed);
	}

	/** Lag per partition; partitions with an unknown latest offset are skipped. */
	public static Map<TopicPartition, Long> lagByPartition(Map<TopicPartition, Long> committed,
			Map<TopicPartition, Long> latest) {
		Map<TopicPartition, Long> out = new LinkedHashMap<>();
		committed.forEach((tp, c) -> {
			Long l = latest.get(tp);
			if (l != null) {
				out.put(tp, lag(c, l));
			}
		});
		return out;
	}

	/**
	 * Consumer groups with committed offsets on the topic and their total lag on this topic. A single listGroups,
	 * a single listConsumerGroupOffsets (only this topic's partitions), a single listOffsets and a single
	 * describeConsumerGroups for the matching groups. Latest offsets are requested in parallel with the committed
	 * offsets; the commit wait is limited to a share of the remaining time and groups that do not respond are
	 * returned in the errors (partial result).
	 */
	public static TopicConsumers forTopic(ToolCall call, TopicDescription topic) {
		List<TopicPartition> tps = Topics.partitions(topic);
		List<TopicPartition> online = Topics.onlinePartitions(List.of(topic));
		ListOffsetsResult pendingLatest = Offsets.submit(call, online, OffsetSpec.latest());
		GroupList listing = list(call);
		Map<String, GroupListing> groups = listing.groups();
		Futures.Each<String, Map<TopicPartition, Long>> committed = committed(call, groups.keySet(), tps,
				(long) (call.waitMillis() * COMMITTED_WAIT_SHARE));
		Map<String, Map<TopicPartition, Long>> matching = new LinkedHashMap<>();
		committed.values().forEach((g, offsets) -> {
			if (!offsets.isEmpty()) {
				matching.put(g, offsets);
			}
		});
		Map<String, String> errors = new LinkedHashMap<>(committed.errors());
		for (int i = 0; i < listing.errors().size(); i++) {
			errors.put("listGroups#" + (i + 1), listing.errors().get(i));
		}
		if (matching.isEmpty()) {
			return new TopicConsumers(List.of(), groups.size(), errors, 0);
		}
		Map<TopicPartition, Long> latest = new LinkedHashMap<>();
		Offsets.awaitEach(call, online, pendingLatest).values().forEach((tp, i) -> latest.put(tp, i.offset()));
		Futures.Each<String, ConsumerGroupDescription> described = describe(call, matching.keySet());
		List<TopicLag> out = new ArrayList<>();
		matching.forEach((g, offsets) -> {
			long total = lagByPartition(offsets, latest).values().stream().mapToLong(Long::longValue).sum();
			ConsumerGroupDescription d = described.values().get(g);
			String state = d != null ? String.valueOf(d.groupState())
					: groups.get(g).groupState().map(String::valueOf).orElse(null);
			Integer members = d != null ? d.members().size() : null;
			out.add(new TopicLag(g, state, members, total, offsets.size(), type(d, groups.get(g))));
		});
		out.sort(Comparator.comparingLong(TopicLag::lag).reversed().thenComparing(TopicLag::groupId));
		return new TopicConsumers(out, groups.size(), errors, tps.size() - latest.size());
	}

}
