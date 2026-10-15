package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Resolves a topic's partitions and fetches offsets in bulk with a single {@code listOffsets} call instead of one
 * per partition (earliest/latest/forTimestamp). Several requests can be started first and awaited later.
 * <p>
 * Mixed environment (KRaft + ZooKeeper): describeTopics/listOffsets/fetch behave the same in both modes. Time
 * lookups, however, may be rejected per partition by old ZooKeeper brokers (v0 message format:
 * UnsupportedForMessageFormatException; old protocol: UnsupportedVersionException); {@link #awaitEach} turns these
 * into a per-partition note instead of failing the tool.
 */
public final class PartitionOffsets {

	private PartitionOffsets() {
	}

	/** Offset and the time of the record at that offset (-1 if none). */
	public record OffsetAt(long offset, long timestamp) {

		public boolean found() {
			return offset >= 0;
		}

	}

	/**
	 * Per-partition result: the offsets found, and the reason for the partitions without a result because the broker
	 * does not support the lookup (old version/message format).
	 */
	public record Lookup(Map<TopicPartition, OffsetAt> offsets, Map<TopicPartition, String> unsupported) {

		/** Partitions grouped by reason (in id order); used to build notes. */
		public Map<String, List<Integer>> unsupportedByReason() {
			Map<String, List<Integer>> out = new LinkedHashMap<>();
			unsupported.entrySet()
				.stream()
				.sorted(Comparator.comparingInt(e -> e.getKey().partition()))
				.forEach(e -> out.computeIfAbsent(e.getValue(), k -> new ArrayList<>()).add(e.getKey().partition()));
			return out;
		}

	}

	/** [earliest, latest) range per partition. */
	public record Bounds(Map<TopicPartition, Long> earliest, Map<TopicPartition, Long> latest) {

		public long earliest(TopicPartition tp) {
			return earliest.getOrDefault(tp, 0L);
		}

		public long latest(TopicPartition tp) {
			return latest.getOrDefault(tp, 0L);
		}

		/** latest - earliest (rough record count). */
		public long size(TopicPartition tp) {
			return Math.max(0, latest(tp) - earliest(tp));
		}

	}

	/** The topic's partition count (including leaderless ones; the default partitioner counts them all too). */
	public static int partitionCount(ToolCall call, String topic) {
		return describe(call, topic).partitions().size();
	}

	/**
	 * The topic's selected partitions: those with a leader ({@code online}, in id order) and the ids of leaderless
	 * (offline) partitions. Offline partitions are left out of listOffsets/fetch requests: the client would retry a
	 * leaderless partition until the timeout and the whole tool would fail.
	 */
	public record Selection(String topic, List<TopicPartition> online, List<Integer> offline) {

		/** Output note; null if there are no offline partitions. */
		public String note() {
			if (offline.isEmpty()) {
				return null;
			}
			return offline.size() + " partition(s) have no leader (offline) and were skipped: " + offline
					+ "; results cover only partitions with a leader";
		}

	}

	/**
	 * Selects the topic's partitions. If {@code partition} is given, only that partition; an error if the topic does
	 * not have it or it has no leader.
	 */
	public static Selection select(ToolCall call, String topic, Integer partition) {
		return select(describe(call, topic), partition);
	}

	static Selection select(TopicDescription d, Integer partition) {
		String topic = d.name();
		List<TopicPartitionInfo> infos = new ArrayList<>(d.partitions());
		infos.sort(Comparator.comparingInt(TopicPartitionInfo::partition));
		if (partition != null) {
			for (TopicPartitionInfo i : infos) {
				if (i.partition() == partition) {
					if (!hasLeader(i)) {
						throw new ToolException("Partition " + partition + " offline: no leader (broker down or no "
								+ "replica in the ISR); offsets/messages cannot be read" + elrHint(i));
					}
					return new Selection(topic, List.of(new TopicPartition(topic, partition)), List.of());
				}
			}
			throw new ToolException("Partition " + partition + " does not exist: topic " + topic + " has "
					+ infos.size() + " partitions (0.." + (infos.size() - 1) + ")");
		}
		List<TopicPartition> online = new ArrayList<>();
		List<Integer> offline = new ArrayList<>();
		for (TopicPartitionInfo i : infos) {
			if (hasLeader(i)) {
				online.add(new TopicPartition(topic, i.partition()));
			}
			else {
				offline.add(i.partition());
			}
		}
		return new Selection(topic, online, offline);
	}

	private static boolean hasLeader(TopicPartitionInfo i) {
		return i.leader() != null && !i.leader().isEmpty();
	}

	/**
	 * ELR details for a leaderless partition (KIP-966, KRaft only). On ZooKeeper brokers (and with ELR disabled)
	 * elr()/lastKnownElr() are null or empty: no extra details.
	 */
	static String elrHint(TopicPartitionInfo i) {
		List<Integer> elr = ids(i.elr());
		List<Integer> lastKnown = ids(i.lastKnownElr());
		if (elr.isEmpty() && lastKnown.isEmpty()) {
			return "";
		}
		StringBuilder sb = new StringBuilder(" (KRaft");
		if (!elr.isEmpty()) {
			sb.append("; ELR: ").append(elr).append(", a leader is elected without data loss when one of these replicas returns");
		}
		if (!lastKnown.isEmpty()) {
			sb.append("; last known ELR: ").append(lastKnown);
		}
		return sb.append(')').toString();
	}

	private static List<Integer> ids(List<Node> nodes) {
		List<Integer> out = new ArrayList<>();
		if (nodes != null) {
			for (Node n : nodes) {
				if (n != null) {
					out.add(n.id());
				}
			}
		}
		return out;
	}

	private static TopicDescription describe(ToolCall call, String topic) {
		TopicDescription d = call.await(call.admin().describeTopics(List.of(topic)).allTopicNames()).get(topic);
		if (d == null) {
			throw new ToolException("Topic not found: " + topic);
		}
		return d;
	}

	/** Starts the listOffsets request (does not wait); get the result with {@link #await(ToolCall, ListOffsetsResult)}. */
	public static ListOffsetsResult request(ToolCall call, Collection<TopicPartition> tps, OffsetSpec spec) {
		Map<TopicPartition, OffsetSpec> specs = new LinkedHashMap<>();
		for (TopicPartition tp : tps) {
			specs.put(tp, spec);
		}
		return call.admin().listOffsets(specs);
	}

	public static Map<TopicPartition, OffsetAt> await(ToolCall call, ListOffsetsResult result) {
		Map<TopicPartition, OffsetAt> out = new HashMap<>();
		call.await(result.all()).forEach((tp, info) -> out.put(tp, new OffsetAt(info.offset(), info.timestamp())));
		return out;
	}

	/**
	 * Awaits the result of {@link #request} per partition. Partitions the broker does not support
	 * (UnsupportedVersionException / UnsupportedForMessageFormatException; old ZooKeeper brokers) go to
	 * {@link Lookup#unsupported()} with a reason; other errors and timeouts still become a {@link ToolException}.
	 */
	public static Lookup awaitEach(ToolCall call, Collection<TopicPartition> tps, ListOffsetsResult result) {
		Map<TopicPartition, OffsetAt> offsets = new HashMap<>();
		Map<TopicPartition, String> unsupported = new LinkedHashMap<>();
		for (TopicPartition tp : tps) {
			KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> f = result.partitionResult(tp);
			try {
				ListOffsetsResult.ListOffsetsResultInfo info = call.await(f);
				offsets.put(tp, new OffsetAt(info.offset(), info.timestamp()));
			}
			catch (ToolException e) {
				String reason = unsupportedReason(e.getCause());
				if (reason == null) {
					throw e;
				}
				unsupported.put(tp, reason);
			}
		}
		return new Lookup(offsets, unsupported);
	}

	/** Reason for an error showing that the broker does not support this request; null for any other error. */
	static String unsupportedReason(Throwable error) {
		if (error instanceof UnsupportedForMessageFormatException) {
			return "Broker does not support this operation (old message format: v0 records have no timestamp)";
		}
		if (error instanceof UnsupportedVersionException) {
			return KafkaErrors.UNSUPPORTED;
		}
		return null;
	}

	/** earliest and latest offsets (two requests in parallel). */
	public static Bounds bounds(ToolCall call, Collection<TopicPartition> tps) {
		if (tps.isEmpty()) {
			return new Bounds(Map.of(), Map.of());
		}
		ListOffsetsResult e = request(call, tps, OffsetSpec.earliest());
		ListOffsetsResult l = request(call, tps, OffsetSpec.latest());
		return new Bounds(offsets(await(call, e)), offsets(await(call, l)));
	}

	/** First offset per partition whose time is {@code >= timestamp} (offset -1 if none). */
	public static Map<TopicPartition, OffsetAt> forTimestamp(ToolCall call, Collection<TopicPartition> tps,
			long timestamp) {
		return tps.isEmpty() ? Map.of() : await(call, request(call, tps, OffsetSpec.forTimestamp(timestamp)));
	}

	/**
	 * Like {@link #forTimestamp}, but partitions that do not support time lookups (old ZooKeeper broker/message
	 * format) go to {@link Lookup#unsupported()} instead of failing.
	 */
	public static Lookup forTimestampEach(ToolCall call, Collection<TopicPartition> tps, long timestamp) {
		if (tps.isEmpty()) {
			return new Lookup(Map.of(), Map.of());
		}
		return awaitEach(call, tps, request(call, tps, OffsetSpec.forTimestamp(timestamp)));
	}

	public static Map<TopicPartition, Long> offsets(Map<TopicPartition, OffsetAt> m) {
		Map<TopicPartition, Long> out = new HashMap<>();
		m.forEach((tp, o) -> out.put(tp, o.offset()));
		return out;
	}

}
