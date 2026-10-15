package io.github.srdrcn.kafkamcp.kafka;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * Offset queries: each call sends a SINGLE listOffsets for all partitions. Failed partitions are left out of
 * the result (an error is thrown if none succeeded); the caller should report the missing ones.
 */
public final class Offsets {

	private Offsets() {
	}

	/** earliest/latest range; count = latest - earliest. */
	public record Range(long earliest, long latest) {

		/** Approximate message count (an upper bound because of compaction/transaction markers). */
		public long count() {
			return Math.max(0, latest - earliest);
		}

	}

	/** A single listOffsets call (same spec for all partitions). */
	public static Map<TopicPartition, ListOffsetsResultInfo> list(ToolCall call, Collection<TopicPartition> tps,
			OffsetSpec spec) {
		return awaitEach(call, tps, submit(call, tps, spec)).requireAny().values();
	}

	/** Latest (log end) offset per partition. */
	public static Map<TopicPartition, Long> latest(ToolCall call, Collection<TopicPartition> tps) {
		return offsets(list(call, tps, OffsetSpec.latest()));
	}

	/** Earliest offset per partition. */
	public static Map<TopicPartition, Long> earliest(ToolCall call, Collection<TopicPartition> tps) {
		return offsets(list(call, tps, OffsetSpec.earliest()));
	}

	/** earliest + latest; both listOffsets are sent at once. Only partitions with both results are returned. */
	public static Map<TopicPartition, Range> ranges(ToolCall call, Collection<TopicPartition> tps) {
		return submitRanges(call, tps).await(call);
	}

	/** Sends the earliest + latest requests (does not wait); for running them in parallel with other requests. */
	public static PendingRanges submitRanges(ToolCall call, Collection<TopicPartition> tps) {
		return new PendingRanges(tps, submit(call, tps, OffsetSpec.earliest()), submit(call, tps, OffsetSpec.latest()));
	}

	/** Pending earliest/latest requests. */
	public record PendingRanges(Collection<TopicPartition> tps, ListOffsetsResult earliest, ListOffsetsResult latest) {

		/** Waits for the results; only partitions with both results are returned (error if none arrives). */
		public Map<TopicPartition, Range> await(ToolCall call) {
			Map<TopicPartition, Long> e = offsets(awaitEach(call, tps, earliest).requireAny().values());
			Map<TopicPartition, Long> l = offsets(awaitEach(call, tps, latest).requireAny().values());
			Map<TopicPartition, Range> out = new LinkedHashMap<>();
			for (TopicPartition tp : tps) {
				Long a = e.get(tp);
				Long b = l.get(tp);
				if (a != null && b != null) {
					out.put(tp, new Range(a, b));
				}
			}
			return out;
		}

	}

	/** Sends the listOffsets request (does not wait); for sending several specs in parallel. */
	public static ListOffsetsResult submit(ToolCall call, Collection<TopicPartition> tps, OffsetSpec spec) {
		Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
		for (TopicPartition tp : tps) {
			specs.put(tp, spec);
		}
		return call.admin().listOffsets(specs);
	}

	/** Waits for the {@link #submit} result; successful results and errors per partition. */
	public static Futures.Each<TopicPartition, ListOffsetsResultInfo> awaitEach(ToolCall call,
			Collection<TopicPartition> tps, ListOffsetsResult result) {
		Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new LinkedHashMap<>();
		for (TopicPartition tp : tps) {
			futures.put(tp, result.partitionResult(tp));
		}
		return Futures.awaitEach(call, futures);
	}

	private static Map<TopicPartition, Long> offsets(Map<TopicPartition, ListOffsetsResultInfo> infos) {
		Map<TopicPartition, Long> out = new LinkedHashMap<>();
		infos.forEach((tp, i) -> out.put(tp, i.offset()));
		return out;
	}

}
