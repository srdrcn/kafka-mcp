package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Tells partitions whose commit did not advance in the measurement window apart from really stuck ones. Clients that
 * commit periodically (auto commit 5 s, Kafka Streams 30 s, Connect sink offset.flush.interval.ms 60 s) may not
 * commit at all within a window of a few seconds; so the verdict is based on the age of the record at the committed
 * offset (the oldest pending one), read with an assign-only consumer (one read per partition). If the record cannot
 * be read, the age is estimated as "lag / partition produce rate"; an estimate alone is not enough for a critical
 * verdict.
 */
public final class StuckCheck {

	/** Allowance for periodic commits (s): if the oldest pending record is younger, there is no evidence of being stuck. */
	public static final long COMMIT_ALLOWANCE_SECONDS = 90;

	/** Consumption counts as stuck if the oldest pending record is older than this (s) and the commit did not advance in the window. */
	public static final long STUCK_SECONDS = 300;

	/** The record age is read for at most this many (partition, offset) pairs; the rest are estimated. */
	public static final int MAX_AGE_READS = 50;

	/** Maximum time spent reading record ages (ms). */
	public static final long READ_BUDGET_MS = 2_000;

	/** Source of the age: the timestamp of the record read, or the lag / produce rate estimate. */
	public static final String RECORD_AGE = "recordAge";

	public static final String ESTIMATE = "lagRateEstimate";

	/** Assessment level: periodic commit likely, suspect (warning), stuck (proven). */
	public enum Level {

		PERIODIC, SUSPECT, STUCK

	}

	/** (partition, offset) pair. */
	public record Position(TopicPartition partition, long offset) {
	}

	/** Assessment of one partition: progress, age of the oldest pending record (s), its source and the level. */
	public record Assessed(OffsetWindow.PartitionProgress progress, long behindSeconds, String basis, Level level) {

		public TopicPartition partition() {
			return progress.partition();
		}

		public Map<String, Object> toMap() {
			Map<String, Object> m = progress.toMap();
			m.put("oldestPendingAgeSeconds", behindSeconds);
			m.put("ageBasis", basis);
			return m;
		}

	}

	/** Assessment of a group's partitions whose commit did not advance. */
	public record Verdict(List<Assessed> partitions, double windowSeconds) {

		public List<Assessed> at(Level level) {
			return partitions.stream().filter(a -> a.level() == level).toList();
		}

		/** Proven stuck (the oldest pending record is {@link #STUCK_SECONDS}+ s old and its age was read). */
		public List<Assessed> stuck() {
			return at(Level.STUCK);
		}

		/** Suspect: beyond the periodic commit allowance but not proven stuck. */
		public List<Assessed> suspect() {
			return at(Level.SUSPECT);
		}

		/** No commit seen in the window but the pending records are young: periodic commit likely. */
		public List<Assessed> periodic() {
			return at(Level.PERIODIC);
		}

		public boolean isEmpty() {
			return partitions.isEmpty();
		}

		/** Highest level (null if empty). */
		public Level worst() {
			return partitions.stream().map(Assessed::level).max(Comparator.naturalOrder()).orElse(null);
		}

		/** Age of the oldest pending record at the level (s; 0 if none). */
		public long maxBehindSeconds(Level level) {
			return at(level).stream().mapToLong(Assessed::behindSeconds).max().orElse(0);
		}

		/** Evidence rows of the partitions at the level (oldest first, first n). */
		public List<Map<String, Object>> evidence(Level level, int n) {
			return at(level).stream()
				.sorted(Comparator.comparingLong(Assessed::behindSeconds).reversed())
				.limit(n)
				.map(Assessed::toMap)
				.toList();
		}

		/** Window and threshold info added to the finding evidence. */
		public Map<String, Object> window() {
			return Results.map("windowSeconds", Math.round(windowSeconds * 10) / 10.0, "commitAllowanceSeconds",
					COMMIT_ALLOWANCE_SECONDS, "stuckAfterSeconds", STUCK_SECONDS);
		}

	}

	private final OffsetWindow window;

	private final Map<Position, Long> timestamps;

	private final int ageNotRead;

	private final long nowMs;

	private StuckCheck(OffsetWindow window, Map<Position, Long> timestamps, int ageNotRead, long nowMs) {
		this.window = window;
		this.timestamps = timestamps;
		this.ageNotRead = ageNotRead;
		this.nowMs = nowMs;
	}

	/**
	 * Collects the groups' partitions whose commit did not advance and reads the timestamp of the record at the
	 * committed offset for at most {@link #MAX_AGE_READS} (partition, offset) pairs (largest lag first; at most
	 * budgetMs).
	 */
	public static StuckCheck of(ToolCall call, OffsetWindow window, Collection<String> groups, long budgetMs) {
		Map<Position, Long> lagByPosition = new HashMap<>();
		for (String g : new LinkedHashSet<>(groups)) {
			for (OffsetWindow.PartitionProgress p : window.progress(g).stuck()) {
				lagByPosition.merge(new Position(p.partition(), p.committed0()), p.lag1(), Math::max);
			}
		}
		List<Position> ordered = new ArrayList<>(lagByPosition.keySet());
		ordered.sort(Comparator.comparingLong((Position p) -> -lagByPosition.get(p))
			.thenComparing(p -> p.partition().toString())
			.thenComparingLong(Position::offset));
		List<Position> toRead = ordered.subList(0, Math.min(MAX_AGE_READS, ordered.size()));
		Map<Position, Long> ts = toRead.isEmpty() ? Map.of() : readTimestamps(call, toRead, window.end1(), budgetMs);
		return new StuckCheck(window, ts, ordered.size() - toRead.size(), System.currentTimeMillis());
	}

	/** For tests: an assessor with given timestamps (position → record time, ms). */
	static StuckCheck of(OffsetWindow window, Map<Position, Long> timestamps, long nowMs) {
		return new StuckCheck(window, timestamps, 0, nowMs);
	}

	/** Number of positions whose age was not read due to the read limit (assessed by estimate). */
	public int ageNotRead() {
		return ageNotRead;
	}

	/** Assessment of the group's partitions whose commit did not advance in the window. */
	public Verdict verdict(String group) {
		List<Assessed> out = new ArrayList<>();
		for (OffsetWindow.PartitionProgress p : window.progress(group).stuck()) {
			Long ts = timestamps.get(new Position(p.partition(), p.committed0()));
			long behind;
			String basis;
			if (ts != null && ts >= 0) {
				behind = Math.max(0, nowMs - ts) / 1000;
				basis = RECORD_AGE;
			}
			else {
				double rate = window.elapsedSeconds() > 0 ? (p.end1() - p.end0()) / window.elapsedSeconds() : 0;
				behind = rate > 0 ? Math.round(p.lag0() / rate) : 0;
				basis = ESTIMATE;
			}
			Level level = behind >= STUCK_SECONDS && RECORD_AGE.equals(basis) ? Level.STUCK
					: behind >= COMMIT_ALLOWANCE_SECONDS ? Level.SUSPECT : Level.PERIODIC;
			out.add(new Assessed(p, behind, basis, level));
		}
		return new Verdict(out, window.elapsedSeconds());
	}

	/**
	 * Timestamp of the record at the given positions (or the first one after them): assign-only consumer, one position
	 * per partition per round. Positions that cannot be read (time, log start passed, authorization) are absent.
	 */
	static Map<Position, Long> readTimestamps(ToolCall call, Collection<Position> positions,
			Map<TopicPartition, Long> endHints, long budgetMs) {
		Map<Position, Long> out = new HashMap<>();
		Map<TopicPartition, Deque<Long>> pending = new LinkedHashMap<>();
		Map<TopicPartition, Set<Long>> sorted = new LinkedHashMap<>();
		positions.forEach(p -> sorted.computeIfAbsent(p.partition(), k -> new TreeSet<>()).add(p.offset()));
		sorted.forEach((tp, offsets) -> pending.put(tp, new ArrayDeque<>(offsets)));
		long end = System.currentTimeMillis() + Math.max(0, Math.min(budgetMs, call.remainingMillis() - 1_500));
		Map<String, Object> overrides = Map.of(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 64 * 1024,
				ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 4 * 1024 * 1024, ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100);
		try (KafkaConsumer<byte[], byte[]> consumer = call.clients().newConsumer(call.cluster(), overrides)) {
			while (!pending.isEmpty() && System.currentTimeMillis() < end) {
				Map<TopicPartition, Long> round = new LinkedHashMap<>();
				pending.forEach((tp, q) -> round.put(tp, q.peekFirst()));
				consumer.resume(consumer.paused());
				consumer.assign(round.keySet());
				round.forEach(consumer::seek);
				Set<TopicPartition> waiting = new HashSet<>(round.keySet());
				while (!waiting.isEmpty()) {
					long left = end - System.currentTimeMillis();
					if (left <= 0) {
						break;
					}
					ConsumerRecords<byte[], byte[]> records;
					try {
						records = consumer.poll(Duration.ofMillis(Math.min(300, left)));
					}
					catch (OffsetOutOfRangeException e) {
						Set<TopicPartition> gone = e.partitions();
						waiting.removeAll(gone);
						consumer.pause(gone);
						continue;
					}
					for (ConsumerRecord<byte[], byte[]> r : records) {
						TopicPartition tp = new TopicPartition(r.topic(), r.partition());
						Long target = round.get(tp);
						if (waiting.contains(tp) && target != null && r.offset() >= target) {
							out.put(new Position(tp, target), r.timestamp());
							waiting.remove(tp);
							consumer.pause(List.of(tp));
						}
					}
					// stop waiting if there are only control records from the position to the log end
					for (TopicPartition tp : List.copyOf(waiting)) {
						Long hint = endHints.get(tp);
						if (hint != null && position(consumer, tp) >= hint) {
							waiting.remove(tp);
							consumer.pause(List.of(tp));
						}
					}
				}
				round.keySet().forEach(tp -> {
					Deque<Long> q = pending.get(tp);
					q.pollFirst();
					if (q.isEmpty()) {
						pending.remove(tp);
					}
				});
			}
		}
		catch (KafkaException | ToolException e) {
			// partial result: what could be read is returned; the rest is estimated
		}
		return out;
	}

	private static long position(KafkaConsumer<byte[], byte[]> consumer, TopicPartition tp) {
		try {
			return consumer.position(tp, Duration.ZERO);
		}
		catch (KafkaException e) {
			return -1;
		}
	}

}
