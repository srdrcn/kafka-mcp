package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * Reads per-partition offset windows ({@code [start, end)}) with an assign()-only consumer. Does not wait for new
 * data; stops when the deadline gets close and returns {@code deadlineReached} (the caller marks the result
 * partial).
 */
public final class RecordScanner {

	/** Margin left before the deadline to leave the loop and build the result. */
	public static final long DEFAULT_RESERVE_MS = 2_000;

	private static final long MAX_POLL_MS = 500;

	private RecordScanner() {
	}

	/** Range to read: {@code [start, end)}. */
	public record Window(TopicPartition partition, long start, long end) {

		public long size() {
			return Math.max(0, end - start);
		}

	}

	/** The visitor's decision: continue, this partition is done, stop entirely. */
	public enum Next {

		CONTINUE, PARTITION_DONE, STOP

	}

	@FunctionalInterface
	public interface Visitor {

		Next visit(ConsumerRecord<byte[], byte[]> record);

	}

	/**
	 * Scan result. {@code positions}: the next offset to read per partition (at least {@code end} if the window end
	 * was reached).
	 */
	public record Result(long scanned, boolean completed, boolean deadlineReached, boolean stopped,
			Map<TopicPartition, Long> positions) {

		/** Did every window reach its end (or the given offsets)? */
		public boolean reached(Map<TopicPartition, Long> ends) {
			for (Map.Entry<TopicPartition, Long> e : ends.entrySet()) {
				Long p = positions.get(e.getKey());
				if (p == null || p < e.getValue()) {
					return false;
				}
			}
			return true;
		}

	}

	public static Result scan(ToolCall call, KafkaConsumer<byte[], byte[]> consumer, Collection<Window> windows,
			Visitor visitor) {
		return scan(call, consumer, windows, DEFAULT_RESERVE_MS, visitor);
	}

	public static Result scan(ToolCall call, KafkaConsumer<byte[], byte[]> consumer, Collection<Window> windows,
			long reserveMs, Visitor visitor) {
		Map<TopicPartition, Window> active = new LinkedHashMap<>();
		Map<TopicPartition, Long> positions = new HashMap<>();
		for (Window w : windows) {
			positions.put(w.partition(), w.start());
			if (w.size() > 0) {
				active.put(w.partition(), w);
			}
			else {
				positions.put(w.partition(), Math.max(w.start(), w.end()));
			}
		}
		if (active.isEmpty()) {
			return new Result(0, true, false, false, positions);
		}
		consumer.assign(active.keySet());
		active.values().forEach(w -> consumer.seek(w.partition(), w.start()));
		// second scan with the same consumer: partitions finished in the previous scan may still be paused (assign()
		// does not reset the state for the same set)
		consumer.resume(active.keySet());
		long scanned = 0;
		while (!active.isEmpty()) {
			if (call.nearDeadline(reserveMs)) {
				return new Result(scanned, false, true, false, positions);
			}
			long pollMs = Math.max(1, Math.min(MAX_POLL_MS, call.remainingMillis() - reserveMs - 300));
			ConsumerRecords<byte[], byte[]> records;
			try {
				records = consumer.poll(Duration.ofMillis(pollMs));
			}
			catch (OffsetOutOfRangeException e) {
				// retention moved the log start forward: continue from the oldest remaining record
				consumer.seekToBeginning(e.partitions());
				continue;
			}
			Set<TopicPartition> done = new HashSet<>();
			for (TopicPartition tp : records.partitions()) {
				Window w = active.get(tp);
				if (w == null) {
					continue;
				}
				for (ConsumerRecord<byte[], byte[]> r : records.records(tp)) {
					if (r.offset() >= w.end()) {
						break;
					}
					scanned++;
					positions.put(tp, r.offset() + 1);
					Next next = visitor.visit(r);
					if (next == Next.STOP) {
						return new Result(scanned, false, false, true, positions);
					}
					if (next == Next.PARTITION_DONE) {
						done.add(tp);
						break;
					}
				}
			}
			for (Window w : active.values()) {
				if (done.contains(w.partition())) {
					continue;
				}
				long pos = consumer.position(w.partition(), Duration.ofMillis(call.waitMillis()));
				if (pos >= w.end()) {
					// the consumer may have fetched past the window; the processed part ends at the window end
					positions.put(w.partition(), w.end());
					done.add(w.partition());
				}
			}
			if (!done.isEmpty()) {
				consumer.pause(done);
				done.forEach(active::remove);
			}
		}
		return new Result(scanned, true, false, false, positions);
	}

	/**
	 * Splits the budget fairly across partitions (water-filling): a partition with few records takes only what it
	 * needs and the remainder goes to the others. Total = min(budget, total demand). The result is deterministic.
	 */
	public static Map<TopicPartition, Long> fairShare(Map<TopicPartition, Long> demand, long budget) {
		List<Map.Entry<TopicPartition, Long>> entries = new ArrayList<>(demand.entrySet());
		entries.sort(Comparator.comparingLong((Map.Entry<TopicPartition, Long> e) -> Math.max(0, e.getValue()))
			.thenComparing(e -> e.getKey().partition()));
		Map<TopicPartition, Long> out = new HashMap<>();
		long remaining = Math.max(0, budget);
		int left = entries.size();
		for (Map.Entry<TopicPartition, Long> e : entries) {
			long share = remaining / left;
			long give = Math.min(Math.max(0, e.getValue()), share);
			out.put(e.getKey(), give);
			remaining -= give;
			left--;
		}
		return out;
	}

	public static long sum(Map<TopicPartition, Long> m) {
		long s = 0;
		for (long v : m.values()) {
			s += Math.max(0, v);
		}
		return s;
	}

}
