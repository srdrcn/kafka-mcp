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
 * Commit'i ölçüm penceresinde ilerlemeyen partition'ları gerçek takılmadan ayırır. Periyodik commit eden istemciler
 * (auto commit 5 sn, Kafka Streams 30 sn, Connect sink offset.flush.interval.ms 60 sn) birkaç saniyelik pencerede
 * hiç commit etmeyebilir; bu yüzden karar commit'li offset'teki (bekleyen en eski) kaydın yaşına göre verilir
 * (assign-only consumer ile partition başına tek okuma). Kayıt okunamazsa yaş "lag / partition yazma hızı" ile
 * tahmin edilir; tahmin tek başına kritik karar için yeterli sayılmaz.
 */
public final class StuckCheck {

	/** Periyodik commit için tanınan pay (sn): bekleyen en eski kayıt bundan gençse takılma kanıtı yok. */
	public static final long COMMIT_ALLOWANCE_SECONDS = 90;

	/** Bekleyen en eski kayıt bundan eskiyse (sn) ve commit pencerede ilerlemediyse tüketim takılmış sayılır. */
	public static final long STUCK_SECONDS = 300;

	/** Kayıt yaşı en fazla bu kadar (partition, offset) için okunur; kalanlar tahminle değerlendirilir. */
	public static final int MAX_AGE_READS = 50;

	/** Kayıt yaşı okumasına ayrılan en fazla süre (ms). */
	public static final long READ_BUDGET_MS = 2_000;

	/** Yaşın kaynağı: okunan kaydın zaman damgası ya da lag / yazma hızı tahmini. */
	public static final String RECORD_AGE = "recordAge";

	public static final String ESTIMATE = "lagRateEstimate";

	/** Değerlendirme düzeyi: periyodik commit olası, şüpheli (uyarı), takılmış (kanıtlı). */
	public enum Level {

		PERIODIC, SUSPECT, STUCK

	}

	/** (partition, offset) çifti. */
	public record Position(TopicPartition partition, long offset) {
	}

	/** Bir partition'ın değerlendirmesi: ilerleme, bekleyen en eski kaydın yaşı (sn), kaynağı ve düzey. */
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

	/** Bir grubun commit'i ilerlemeyen partition'larının değerlendirmesi. */
	public record Verdict(List<Assessed> partitions, double windowSeconds) {

		public List<Assessed> at(Level level) {
			return partitions.stream().filter(a -> a.level() == level).toList();
		}

		/** Kanıtlı takılma (bekleyen en eski kayıt {@link #STUCK_SECONDS}+ sn ve yaşı okundu). */
		public List<Assessed> stuck() {
			return at(Level.STUCK);
		}

		/** Şüpheli: periyodik commit payını aşmış ama kanıtlı takılma değil. */
		public List<Assessed> suspect() {
			return at(Level.SUSPECT);
		}

		/** Pencerede commit görülmedi ama bekleyen kayıtlar genç: periyodik commit olası. */
		public List<Assessed> periodic() {
			return at(Level.PERIODIC);
		}

		public boolean isEmpty() {
			return partitions.isEmpty();
		}

		/** En yüksek düzey (boşsa null). */
		public Level worst() {
			return partitions.stream().map(Assessed::level).max(Comparator.naturalOrder()).orElse(null);
		}

		/** Düzeydeki en yaşlı bekleyen kaydın yaşı (sn; yoksa 0). */
		public long maxBehindSeconds(Level level) {
			return at(level).stream().mapToLong(Assessed::behindSeconds).max().orElse(0);
		}

		/** Düzeydeki partition'ların kanıt satırları (en yaşlı önce, ilk n). */
		public List<Map<String, Object>> evidence(Level level, int n) {
			return at(level).stream()
				.sorted(Comparator.comparingLong(Assessed::behindSeconds).reversed())
				.limit(n)
				.map(Assessed::toMap)
				.toList();
		}

		/** Bulgu kanıtına eklenen pencere ve eşik bilgisi. */
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
	 * Gruplarda commit'i ilerlemeyen partition'ları toplar ve en fazla {@link #MAX_AGE_READS} (partition, offset)
	 * için commit'li offset'teki kaydın zaman damgasını okur (lag'i büyük olanlar önce; en fazla budgetMs).
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

	/** Testler için: okunmuş zaman damgalarıyla (konum → kayıt zamanı, ms) değerlendirici. */
	static StuckCheck of(OffsetWindow window, Map<Position, Long> timestamps, long nowMs) {
		return new StuckCheck(window, timestamps, 0, nowMs);
	}

	/** Yaşı okuma sınırı yüzünden okunmayan (tahminle değerlendirilen) konum sayısı. */
	public int ageNotRead() {
		return ageNotRead;
	}

	/** Grubun commit'i pencerede ilerlemeyen partition'larının değerlendirmesi. */
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
	 * Verilen konumlardaki (ya da sonrasındaki ilk) kaydın zaman damgası: assign-only consumer, partition başına
	 * tur başına bir konum. Okunamayanlar (süre, log başı geçilmiş, yetki) sonuçta yoktur.
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
					// konumdan log sonuna kadar sadece kontrol kaydı varsa beklemeyi bırak
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
			// kısmi sonuç: okunabilenler döner; kalanlar tahminle değerlendirilir
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
