package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.toolsets.messages.RecordScanner.Next;
import io.github.srdrcn.kafkamcp.toolsets.messages.RecordScanner.Window;

/**
 * Reading engine without a consumer group: earliest/latest/offset/timestamp start, optional filter,
 * maxRecords/maxScan limits; does not wait for new data and returns what it has read as partial when the deadline
 * gets close.
 * <ul>
 * <li>latest: the last records of the selected partitions (computed from the end offsets), the newest maxRecords
 * records by time. The total window read is limited by maxScan. Partitions whose window yields fewer records
 * because of transaction markers/compaction gaps get their window extended backwards. Records without a timestamp
 * (v0 message format on old ZooKeeper brokers; timestamp -1) count as older than timed ones and are picked fairly
 * among themselves from the partition ends (result approximate + note).</li>
 * <li>earliest/offset/timestamp: forward from the start; without a filter maxRecords is split fairly across
 * partitions, with a filter up to maxScan records are scanned and the first maxRecords matches are returned.</li>
 * </ul>
 */
public final class MessageReader {

	public static final int DEFAULT_MAX_SCAN = 10_000;

	public static final int MAX_SCAN_LIMIT = 100_000;

	private static final int MAX_LISTED_PARTITIONS = 20;

	/** Maximum rounds of extending backwards the latest windows of partitions that yielded too few records. */
	private static final int MAX_EXTEND_ROUNDS = 6;

	/** latest order: time, partition, offset. */
	public static final Comparator<ConsumerRecord<byte[], byte[]>> BY_TIME = Comparator
		.comparingLong((ConsumerRecord<byte[], byte[]> r) -> r.timestamp())
		.thenComparingInt(ConsumerRecord::partition)
		.thenComparingLong(ConsumerRecord::offset);

	public static final Comparator<ConsumerRecord<byte[], byte[]>> BY_OFFSET = Comparator
		.comparingInt((ConsumerRecord<byte[], byte[]> r) -> r.partition())
		.thenComparingLong(ConsumerRecord::offset);

	private MessageReader() {
	}

	public enum From {

		EARLIEST, LATEST, OFFSET, TIMESTAMP;

		public String id() {
			return name().toLowerCase(Locale.ROOT);
		}

		public static From parse(String value, From defaultValue) {
			if (value == null || value.isBlank()) {
				return defaultValue;
			}
			for (From f : values()) {
				if (f.id().equals(value.trim().toLowerCase(Locale.ROOT))) {
					return f;
				}
			}
			throw new ToolException("Invalid from: " + value + " (valid: earliest|latest|offset|timestamp)");
		}

	}

	/**
	 * Read request. {@code offset} is for from=offset, {@code timestamp} (epoch ms) for from=timestamp; no filter if
	 * {@code filter} is null.
	 */
	public record Request(String topic, Integer partition, From from, Long offset, Long timestamp, int maxRecords,
			int maxScan, Predicate<ConsumerRecord<byte[], byte[]>> filter) {
	}

	/**
	 * Read result. {@code records}: sorted by time for latest, by partition/offset otherwise. {@code partitions}:
	 * the partitions read (with a leader); {@code offlinePartitions}: skipped because they have no leader;
	 * {@code skippedPartitions}: not read because their start could not be found although they have a leader (old
	 * broker without time lookup support): they are left out of {@code partitions} and {@code nextOffsets}, and
	 * {@code reachedEnd} is false. {@code nextOffsets}: the next offset to read for each partition read.
	 * {@code partial}: hit the time limit or there are skippedPartitions (the reason is in notes).
	 */
	public record Result(List<ConsumerRecord<byte[], byte[]>> records, List<TopicPartition> partitions,
			List<Integer> offlinePartitions, long scanned, long matched, boolean reachedEnd, boolean partial,
			boolean scanLimitReached, boolean approximate, Map<TopicPartition, Long> nextOffsets, List<String> notes,
			List<Integer> skippedPartitions) {

		/** Result without skipped partitions (previous signature). */
		public Result(List<ConsumerRecord<byte[], byte[]>> records, List<TopicPartition> partitions,
				List<Integer> offlinePartitions, long scanned, long matched, boolean reachedEnd, boolean partial,
				boolean scanLimitReached, boolean approximate, Map<TopicPartition, Long> nextOffsets,
				List<String> notes) {
			this(records, partitions, offlinePartitions, scanned, matched, reachedEnd, partial, scanLimitReached,
					approximate, nextOffsets, notes, List.of());
		}

	}

	public static Result read(ToolCall call, Request req) {
		PartitionOffsets.Selection sel = PartitionOffsets.select(call, req.topic(), req.partition());
		List<TopicPartition> tps = sel.online();
		List<String> notes = new ArrayList<>();
		if (sel.note() != null) {
			notes.add(sel.note());
		}
		PartitionOffsets.Bounds b = PartitionOffsets.bounds(call, tps);
		if (req.from() == From.LATEST) {
			return latest(call, req, sel, b, notes);
		}
		List<Integer> skipped = new ArrayList<>();
		Map<TopicPartition, Long> starts = starts(call, req, tps, b, notes, skipped);
		return forward(call, req, sel, b, starts, notes, skipped);
	}

	/**
	 * Start offset per partition. Partitions that will not be read because their start could not be found (old
	 * broker without time lookup support) are added to {@code skipped}.
	 */
	private static Map<TopicPartition, Long> starts(ToolCall call, Request req, List<TopicPartition> tps,
			PartitionOffsets.Bounds b, List<String> notes, List<Integer> skipped) {
		Map<TopicPartition, Long> starts = new LinkedHashMap<>();
		switch (req.from()) {
			case EARLIEST -> tps.forEach(tp -> starts.put(tp, b.earliest(tp)));
			case OFFSET -> {
				long o = req.offset();
				List<Integer> below = new ArrayList<>();
				List<Integer> above = new ArrayList<>();
				for (TopicPartition tp : tps) {
					if (o < b.earliest(tp)) {
						below.add(tp.partition());
					}
					else if (o > b.latest(tp)) {
						above.add(tp.partition());
					}
					starts.put(tp, Math.max(b.earliest(tp), Math.min(o, b.latest(tp))));
				}
				if (!below.isEmpty()) {
					notes.add("offset " + o + " is below earliest, started from earliest: partition " + list(below));
				}
				if (!above.isEmpty()) {
					notes.add("offset " + o + " is beyond latest, no records to read: partition " + list(above));
				}
			}
			case TIMESTAMP -> {
				// old ZooKeeper brokers (v0 message format / old protocol) may reject the time lookup per
				// partition: those partitions are not read and get a note, the others are read
				PartitionOffsets.Lookup at = PartitionOffsets.forTimestampEach(call, tps, req.timestamp());
				List<Integer> none = new ArrayList<>();
				for (TopicPartition tp : tps) {
					PartitionOffsets.OffsetAt a = at.offsets().get(tp);
					if (at.unsupported().containsKey(tp)) {
						// not read: there is no start (treating it as started at latest would make it look finished)
						skipped.add(tp.partition());
					}
					else if (a == null || !a.found()) {
						none.add(tp.partition());
						starts.put(tp, b.latest(tp));
					}
					else {
						starts.put(tp, Math.max(b.earliest(tp), a.offset()));
					}
				}
				if (!none.isEmpty()) {
					notes.add("No records at or after this time: partition " + list(none));
				}
				at.unsupportedByReason()
					.forEach((reason, ps) -> notes.add("Could not find a start by time, not read: partition "
							+ list(ps) + " (" + reason + ")"));
			}
			default -> throw new IllegalStateException();
		}
		return starts;
	}

	private static Result latest(ToolCall call, Request req, PartitionOffsets.Selection sel, PartitionOffsets.Bounds b,
			List<String> notes) {
		List<TopicPartition> tps = sel.online();
		boolean filtered = req.filter() != null;
		Map<TopicPartition, Long> demand = new HashMap<>();
		for (TopicPartition tp : tps) {
			demand.put(tp, filtered ? b.size(tp) : Math.min(b.size(tp), req.maxRecords()));
		}
		Map<TopicPartition, Long> size = RecordScanner.fairShare(demand, req.maxScan());
		boolean reduced = RecordScanner.sum(size) < RecordScanner.sum(demand);
		Map<TopicPartition, Long> starts = new HashMap<>();
		List<Window> windows = new ArrayList<>();
		for (TopicPartition tp : tps) {
			long start = b.latest(tp) - size.get(tp);
			starts.put(tp, start);
			windows.add(new Window(tp, start, b.latest(tp)));
		}
		PriorityQueue<ConsumerRecord<byte[], byte[]>> heap = new PriorityQueue<>(newestLast(tps, b));
		Map<TopicPartition, Long> got = new HashMap<>();
		long[] matched = { 0 };
		RecordScanner.Visitor visitor = r -> {
			got.merge(new TopicPartition(r.topic(), r.partition()), 1L, Long::sum);
			if (filtered && !req.filter().test(r)) {
				return Next.CONTINUE;
			}
			matched[0]++;
			heap.add(r);
			if (heap.size() > req.maxRecords()) {
				heap.poll();
			}
			return Next.CONTINUE;
		};
		RecordScanner.Result scan;
		long scanned;
		boolean completed;
		boolean deadline;
		List<Integer> shortfall = List.of();
		try (BoundedConsumer c = BoundedConsumer.open(call)) {
			scan = RecordScanner.scan(call, c.consumer(), windows, visitor);
			scanned = scan.scanned();
			completed = scan.completed();
			deadline = scan.deadlineReached();
			if (!filtered) {
				// transaction markers and compaction gaps take up offsets: partitions whose window yielded too few
				// records get their window extended backwards (within the maxScan budget)
				long budget = req.maxScan() - RecordScanner.sum(size);
				for (int round = 0; round < MAX_EXTEND_ROUNDS && completed; round++) {
					Map<TopicPartition, Long> grant = RecordScanner.fairShare(extension(tps, b, size, starts, got),
							budget);
					if (RecordScanner.sum(grant) == 0) {
						break;
					}
					budget -= RecordScanner.sum(grant);
					List<Window> more = new ArrayList<>();
					for (TopicPartition tp : tps) {
						long g = grant.getOrDefault(tp, 0L);
						if (g > 0) {
							long start = starts.get(tp);
							more.add(new Window(tp, start - g, start));
							starts.put(tp, start - g);
						}
					}
					RecordScanner.Result next = RecordScanner.scan(call, c.consumer(), more, visitor);
					scanned += next.scanned();
					completed = next.completed();
					deadline = next.deadlineReached();
				}
				if (completed) {
					shortfall = shortfall(tps, b, size, starts, got);
				}
			}
		}
		List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>(heap);
		records.sort(BY_TIME);
		long untimed = records.stream().filter(MessageReader::untimed).count();
		boolean approximate = (!filtered && (reduced || !shortfall.isEmpty())) || untimed > 0;
		if (untimed > 0) {
			notes.add(untimed + " records have no timestamp (old v0 message format): the newest records could not be "
					+ "picked by time, so they were picked fairly from the end of each partition; these records are listed "
					+ "before timed records, in partition/offset order");
		}
		if (!filtered && reduced) {
			notes.add("The window read per partition was limited by maxScan (" + req.maxScan()
					+ "); the selection of the newest records is approximate (increase maxScan for a more exact result)");
		}
		if (!shortfall.isEmpty()) {
			notes.add("Partition " + list(shortfall) + ": the last offsets held fewer records than requested because of "
					+ "transaction marker/compaction gaps; the backward extension hit its limit (maxScan=" + req.maxScan()
					+ "), so older records of these partitions may be missing");
		}
		if (filtered && reduced) {
			notes.add("Only the last " + RecordScanner.sum(size) + " records were scanned (maxScan); there may be older matches");
		}
		if (deadline) {
			notes.add("Approaching the time limit; returned the records read so far");
		}
		return new Result(records, tps, sel.offline(), scanned, matched[0], completed, deadline, filtered && reduced,
				approximate, scan.positions(), notes);
	}

	/** No timestamp: v0 message format (or a record written with NO_TIMESTAMP); timestamp -1. */
	static boolean untimed(ConsumerRecord<?, ?> r) {
		return r.timestamp() < 0;
	}

	/**
	 * latest selection order (smaller = older; the heap drops the oldest). {@link #BY_TIME} for timed records.
	 * Records without a timestamp count as older than every timed record and are ordered among themselves by distance
	 * from the partition end (latest): the selection is made fairly from the tail of each partition, not by
	 * partition id.
	 */
	static Comparator<ConsumerRecord<byte[], byte[]>> newestLast(List<TopicPartition> tps, PartitionOffsets.Bounds b) {
		Map<Integer, Long> ends = new HashMap<>();
		tps.forEach(tp -> ends.put(tp.partition(), b.latest(tp)));
		return Comparator.comparingLong((ConsumerRecord<byte[], byte[]> r) -> untimed(r) ? -1L : r.timestamp())
			.thenComparingLong(r -> untimed(r) ? r.offset() - ends.getOrDefault(r.partition(), 0L) : 0L)
			.thenComparingInt(ConsumerRecord::partition)
			.thenComparingLong(ConsumerRecord::offset);
	}

	/**
	 * Extra window size (in offsets) for partitions whose window yielded fewer records than the target
	 * ({@code size}) and that have room backwards: estimated from the missing record count and the observed
	 * offset/record ratio.
	 */
	private static Map<TopicPartition, Long> extension(List<TopicPartition> tps, PartitionOffsets.Bounds b,
			Map<TopicPartition, Long> size, Map<TopicPartition, Long> starts, Map<TopicPartition, Long> got) {
		Map<TopicPartition, Long> ext = new HashMap<>();
		for (TopicPartition tp : tps) {
			long have = got.getOrDefault(tp, 0L);
			long deficit = size.get(tp) - have;
			long room = starts.get(tp) - b.earliest(tp);
			if (deficit <= 0 || room <= 0) {
				continue;
			}
			long read = b.latest(tp) - starts.get(tp);
			long need = have == 0 ? 2 * read : Math.ceilDiv(deficit * read, have);
			ext.put(tp, Math.min(room, Math.max(deficit, need)));
		}
		return ext;
	}

	/** Partitions that yielded fewer records than the target but still have unread offsets further back. */
	private static List<Integer> shortfall(List<TopicPartition> tps, PartitionOffsets.Bounds b,
			Map<TopicPartition, Long> size, Map<TopicPartition, Long> starts, Map<TopicPartition, Long> got) {
		List<Integer> out = new ArrayList<>();
		for (TopicPartition tp : tps) {
			if (got.getOrDefault(tp, 0L) < size.get(tp) && starts.get(tp) > b.earliest(tp)) {
				out.add(tp.partition());
			}
		}
		return out;
	}

	/**
	 * {@code skipped}: partitions that will not be read because their start could not be found; they are not
	 * scanned, reachedEnd is false and the result is marked partial.
	 */
	private static Result forward(ToolCall call, Request req, PartitionOffsets.Selection sel,
			PartitionOffsets.Bounds b, Map<TopicPartition, Long> starts, List<String> notes, List<Integer> skipped) {
		List<TopicPartition> tps = new ArrayList<>();
		for (TopicPartition tp : sel.online()) {
			if (!skipped.contains(tp.partition())) {
				tps.add(tp);
			}
		}
		boolean filtered = req.filter() != null;
		Map<TopicPartition, Long> avail = new HashMap<>();
		for (TopicPartition tp : tps) {
			avail.put(tp, Math.max(0, b.latest(tp) - starts.get(tp)));
		}
		List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
		List<Window> windows = new ArrayList<>();
		RecordScanner.Visitor visitor;
		boolean scanLimited = false;
		if (!filtered) {
			Map<TopicPartition, Long> quota = RecordScanner.fairShare(avail, req.maxRecords());
			Map<TopicPartition, Long> taken = new HashMap<>();
			for (TopicPartition tp : tps) {
				long start = starts.get(tp);
				windows.add(new Window(tp, start, quota.get(tp) > 0 ? b.latest(tp) : start));
			}
			visitor = r -> {
				records.add(r);
				TopicPartition tp = new TopicPartition(r.topic(), r.partition());
				long n = taken.merge(tp, 1L, Long::sum);
				return n >= quota.get(tp) ? Next.PARTITION_DONE : Next.CONTINUE;
			};
		}
		else {
			Map<TopicPartition, Long> size = RecordScanner.fairShare(avail, req.maxScan());
			scanLimited = RecordScanner.sum(size) < RecordScanner.sum(avail);
			for (TopicPartition tp : tps) {
				long start = starts.get(tp);
				windows.add(new Window(tp, start, start + size.get(tp)));
			}
			visitor = r -> {
				if (!req.filter().test(r)) {
					return Next.CONTINUE;
				}
				records.add(r);
				return records.size() >= req.maxRecords() ? Next.STOP : Next.CONTINUE;
			};
		}
		RecordScanner.Result scan;
		try (BoundedConsumer c = BoundedConsumer.open(call)) {
			scan = RecordScanner.scan(call, c.consumer(), windows, visitor);
		}
		records.sort(BY_OFFSET);
		Map<TopicPartition, Long> ends = new HashMap<>();
		tps.forEach(tp -> ends.put(tp, b.latest(tp)));
		boolean readToEnd = scan.reached(ends);
		boolean reachedEnd = skipped.isEmpty() && readToEnd;
		boolean scanLimitReached = filtered && scanLimited && scan.completed() && !readToEnd;
		if (scanLimitReached) {
			notes.add("Scanned up to maxScan (" + req.maxScan() + ") records; the rest was not scanned");
		}
		if (scan.deadlineReached()) {
			notes.add("Approaching the time limit; returned the records read so far");
		}
		return new Result(records, tps, sel.offline(), scan.scanned(), records.size(), reachedEnd,
				scan.deadlineReached() || !skipped.isEmpty(), scanLimitReached, false, scan.positions(), notes,
				List.copyOf(skipped));
	}

	private static String list(List<Integer> partitions) {
		if (partitions.size() <= MAX_LISTED_PARTITIONS) {
			return partitions.toString();
		}
		return partitions.subList(0, MAX_LISTED_PARTITIONS) + " (+" + (partitions.size() - MAX_LISTED_PARTITIONS)
				+ " more)";
	}

}
