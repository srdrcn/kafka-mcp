package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * Batch building blocks for consumer group lag: the committed offsets of many groups are fetched with a single
 * {@code listConsumerGroupOffsets} call, the end offsets of all relevant partitions with a single
 * {@code listOffsets} call; lag = max(0, end - committed). Partitions without a commit do not count as lag 0;
 * they are reported as {@code committed == null}. Groups that disappeared become per-group errors. Each request
 * is sent with a timeoutMs that leaves a share for the following steps: a loading coordinator or a partition
 * without a leader does not fail the tool; the group/partition is returned as "no response" (timedOut).
 */
public final class GroupLag {

	/** committed(): default share of the deadline kept for the following end offset query and the output. */
	public static final long COMMITTED_RESERVE_MS = 3_000;

	/** offsets()/endOffsets(): default share of the deadline kept for the output. */
	public static final long OFFSETS_RESERVE_MS = 1_000;

	/**
	 * Lag row of a single partition. committed null: the group has not committed on this partition; end null: the
	 * end offset could not be fetched (reason in error); lag is set if both are known.
	 */
	public record PartitionLag(String topic, int partition, Long committed, Long end, Long lag, String error) {

		public TopicPartition topicPartition() {
			return new TopicPartition(topic, partition);
		}

		/** By lag, descending (rows without lag last), then topic/partition. */
		public static final Comparator<PartitionLag> BY_LAG_DESC = Comparator
			.comparing((PartitionLag p) -> p.lag() == null ? -1L : p.lag(), Comparator.reverseOrder())
			.thenComparing(PartitionLag::topic)
			.thenComparingInt(PartitionLag::partition);

		public static final Comparator<PartitionLag> BY_TOPIC_PARTITION = Comparator.comparing(PartitionLag::topic)
			.thenComparingInt(PartitionLag::partition);

	}

	/**
	 * Committed offsets of a group; partitions returned with a null offset are in withoutOffset; a group error is in
	 * error (notFound: the group does not exist; timedOut: no response in time).
	 */
	public record Committed(Map<TopicPartition, Long> offsets, Set<TopicPartition> withoutOffset, String error,
			boolean notFound, boolean timedOut) {

		public boolean isEmpty() {
			return offsets.isEmpty() && withoutOffset.isEmpty();
		}

	}

	/**
	 * listOffsets result: the fetched offsets and the error messages of the partitions that could not be fetched;
	 * timedOut: how many of these got no response in time.
	 */
	public record OffsetLookup(Map<TopicPartition, Long> offsets, Map<TopicPartition, String> errors, int timedOut) {

		/** Deduplicates the errors per topic (topic → first error message). */
		public Map<String, String> errorsByTopic() {
			Map<String, String> m = new TreeMap<>();
			errors.forEach((tp, e) -> m.putIfAbsent(tp.topic(), e));
			return m;
		}

	}

	/**
	 * Lag summary of a group (partitions in topic/partition order). error: per-group error; notFound: the group was
	 * deleted in the meantime; timedOut: no response in time for the group's offsets.
	 */
	public record GroupLagInfo(String groupId, List<PartitionLag> partitions, String error, boolean notFound,
			boolean timedOut) {

		public long totalLag() {
			long sum = 0;
			for (PartitionLag p : partitions) {
				if (p.lag() != null) {
					sum += p.lag();
				}
			}
			return sum;
		}

		/** topic → total lag, by lag, descending. */
		public Map<String, Long> lagByTopic() {
			Map<String, Long> sums = new HashMap<>();
			for (PartitionLag p : partitions) {
				sums.merge(p.topic(), p.lag() == null ? 0L : p.lag(), Long::sum);
			}
			Map<String, Long> sorted = new LinkedHashMap<>();
			sums.entrySet()
				.stream()
				.sorted(Map.Entry.<String, Long>comparingByValue(Comparator.reverseOrder())
					.thenComparing(Map.Entry.comparingByKey()))
				.forEach(e -> sorted.put(e.getKey(), e.getValue()));
			return sorted;
		}

		/** Number of partitions without a commit. */
		public int withoutCommit() {
			return (int) partitions.stream().filter(p -> p.committed() == null).count();
		}

		/** Number of partitions with a commit whose end offset could not be fetched. */
		public int withoutEnd() {
			return (int) partitions.stream().filter(p -> p.committed() != null && p.end() == null).count();
		}

		/** Partitions with a commit but without an end offset (lag unknown; the total lag is a lower bound). */
		public List<TopicPartition> withoutEndPartitions() {
			return partitions.stream()
				.filter(p -> p.committed() != null && p.end() == null)
				.map(PartitionLag::topicPartition)
				.toList();
		}

		public boolean hasCommits() {
			return partitions.stream().anyMatch(p -> p.committed() != null);
		}

		public Set<String> topics() {
			Set<String> s = new LinkedHashSet<>();
			partitions.forEach(p -> s.add(p.topic()));
			return s;
		}

	}

	private GroupLag() {
	}

	public static long lag(long end, long committed) {
		return Math.max(0, end - committed);
	}

	/**
	 * Committed offsets of the groups with a single listConsumerGroupOffsets call. If topicFilter is not null, only
	 * matching topics are kept. Keeps a share of {@link #COMMITTED_RESERVE_MS} of the deadline.
	 */
	public static Map<String, Committed> committed(ToolCall call, Collection<String> groups,
			Predicate<String> topicFilter) {
		return committed(call, groups, topicFilter, COMMITTED_RESERVE_MS);
	}

	/**
	 * {@link #committed(ToolCall, Collection, Predicate)}; the request finishes within (remaining time -
	 * reserveMillis) at the latest; groups without a response come back as {@code timedOut}.
	 */
	public static Map<String, Committed> committed(ToolCall call, Collection<String> groups,
			Predicate<String> topicFilter, long reserveMillis) {
		Map<String, Committed> out = new LinkedHashMap<>();
		if (groups.isEmpty()) {
			return out;
		}
		Map<String, ListConsumerGroupOffsetsSpec> specs = new LinkedHashMap<>();
		for (String g : groups) {
			specs.put(g, new ListConsumerGroupOffsetsSpec());
		}
		ListConsumerGroupOffsetsResult r = call.admin()
			.listConsumerGroupOffsets(specs,
					new ListConsumerGroupOffsetsOptions().timeoutMs(Settled.timeoutMs(call, reserveMillis)));
		Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = new LinkedHashMap<>();
		for (String g : specs.keySet()) {
			futures.put(g, r.partitionsToOffsetAndMetadata(g));
		}
		Map<String, Settled<Map<TopicPartition, OffsetAndMetadata>>> settled = Settled.all(call, futures,
				reserveMillis);
		for (String g : specs.keySet()) {
			Settled<Map<TopicPartition, OffsetAndMetadata>> s = settled.get(g);
			if (!s.ok()) {
				out.put(g, new Committed(Map.of(), Set.of(), s.errorMessage(), ConsumerGroups.notFound(s.error()),
						s.timedOut()));
				continue;
			}
			Map<TopicPartition, Long> offsets = new HashMap<>();
			Set<TopicPartition> without = new LinkedHashSet<>();
			if (s.value() != null) {
				s.value().forEach((tp, om) -> {
					if (topicFilter != null && !topicFilter.test(tp.topic())) {
						return;
					}
					if (om == null || om.offset() < 0) {
						without.add(tp);
					}
					else {
						offsets.put(tp, om.offset());
					}
				});
			}
			out.put(g, new Committed(offsets, without, null, false, false));
		}
		return out;
	}

	/** Latest (end) offsets of the partitions with a single listOffsets call. */
	public static OffsetLookup endOffsets(ToolCall call, Collection<TopicPartition> partitions) {
		return offsets(call, partitions, OffsetSpec.latest(), OFFSETS_RESERVE_MS);
	}

	/** {@link #endOffsets(ToolCall, Collection)}; keeps a share of reserveMillis of the deadline. */
	public static OffsetLookup endOffsets(ToolCall call, Collection<TopicPartition> partitions, long reserveMillis) {
		return offsets(call, partitions, OffsetSpec.latest(), reserveMillis);
	}

	/** {@link #offsets(ToolCall, Collection, OffsetSpec, long)}; keeps {@link #OFFSETS_RESERVE_MS} of the deadline. */
	public static OffsetLookup offsets(ToolCall call, Collection<TopicPartition> partitions, OffsetSpec spec) {
		return offsets(call, partitions, spec, OFFSETS_RESERVE_MS);
	}

	/**
	 * Offsets of the partitions for the given spec with a single listOffsets call; those that cannot be fetched
	 * (deleted topic, authorization, partition without a leader) end up in errors per partition. For a timestamp
	 * spec, offset -1 is returned if there is no such record. The request finishes within (remaining time -
	 * reserveMillis) at the latest.
	 */
	public static OffsetLookup offsets(ToolCall call, Collection<TopicPartition> partitions, OffsetSpec spec,
			long reserveMillis) {
		Map<TopicPartition, Long> offsets = new HashMap<>();
		Map<TopicPartition, String> errors = new HashMap<>();
		if (partitions.isEmpty()) {
			return new OffsetLookup(offsets, errors, 0);
		}
		Map<TopicPartition, OffsetSpec> request = new HashMap<>();
		for (TopicPartition tp : partitions) {
			request.put(tp, spec);
		}
		ListOffsetsResult r = call.admin()
			.listOffsets(request, new ListOffsetsOptions().timeoutMs(Settled.timeoutMs(call, reserveMillis)));
		Settled<Map<TopicPartition, ListOffsetsResultInfo>> all = Settled.within(call, r.all(), reserveMillis);
		if (all.ok()) {
			all.value().forEach((tp, info) -> offsets.put(tp, info.offset()));
			return new OffsetLookup(offsets, errors, 0);
		}
		int timedOut = 0;
		for (TopicPartition tp : request.keySet()) {
			Settled<ListOffsetsResultInfo> s = Settled.now(r.partitionResult(tp));
			if (s.ok()) {
				offsets.put(tp, s.value().offset());
			}
			else {
				errors.put(tp, s.errorMessage());
				timedOut += s.timedOut() ? 1 : 0;
			}
		}
		return new OffsetLookup(offsets, errors, timedOut);
	}

	/** All partitions in the committed offsets (including those without an offset). */
	public static Set<TopicPartition> partitionsOf(Map<String, Committed> committed) {
		Set<TopicPartition> tps = new LinkedHashSet<>();
		for (Committed c : committed.values()) {
			tps.addAll(c.offsets().keySet());
			tps.addAll(c.withoutOffset());
		}
		return tps;
	}

	/** Bulk lag scan: per-group lag and the end offset query itself (for the partitions that could not be fetched). */
	public record Snapshot(Map<String, GroupLagInfo> groups, OffsetLookup end) {
	}

	/**
	 * Lag of the groups: committed (one call) + end offsets (one call). If topicFilter is not null, only matching
	 * topics. A group that fails (e.g. deleted in the meantime) or does not respond is returned with an error in its
	 * own row.
	 */
	public static Map<String, GroupLagInfo> snapshot(ToolCall call, Collection<String> groups,
			Predicate<String> topicFilter) {
		return lagSnapshot(call, groups, topicFilter).groups();
	}

	/** {@link #snapshot}; together with the error/no-response counts of the end offset query. */
	public static Snapshot lagSnapshot(ToolCall call, Collection<String> groups, Predicate<String> topicFilter) {
		Map<String, Committed> committed = committed(call, groups, topicFilter);
		OffsetLookup end = endOffsets(call, partitionsOf(committed));
		return new Snapshot(compute(committed, end, Map.of()), end);
	}

	/**
	 * Computes the lag from committed + end offsets. extraPartitions: partitions of a group to show even without a
	 * commit (e.g. those assigned to members); their end offsets must also be in {@code end}.
	 */
	public static Map<String, GroupLagInfo> compute(Map<String, Committed> committed, OffsetLookup end,
			Map<String, ? extends Collection<TopicPartition>> extraPartitions) {
		Map<String, GroupLagInfo> out = new LinkedHashMap<>();
		for (Map.Entry<String, Committed> e : committed.entrySet()) {
			Committed c = e.getValue();
			Set<TopicPartition> tps = new LinkedHashSet<>(c.offsets().keySet());
			tps.addAll(c.withoutOffset());
			Collection<TopicPartition> extra = extraPartitions.get(e.getKey());
			if (extra != null) {
				tps.addAll(extra);
			}
			List<PartitionLag> rows = new ArrayList<>();
			for (TopicPartition tp : tps) {
				rows.add(row(tp, c.offsets().get(tp), end));
			}
			rows.sort(PartitionLag.BY_TOPIC_PARTITION);
			out.put(e.getKey(), new GroupLagInfo(e.getKey(), rows, c.error(), c.notFound(), c.timedOut()));
		}
		return out;
	}

	/** Row of a single partition. */
	public static PartitionLag row(TopicPartition tp, Long committed, OffsetLookup end) {
		Long endOffset = end.offsets().get(tp);
		Long lag = committed != null && endOffset != null ? lag(endOffset, committed) : null;
		String error = endOffset == null ? end.errors().get(tp) : null;
		return new PartitionLag(tp.topic(), tp.partition(), committed, endOffset, lag, error);
	}

}
