package io.github.srdrcn.kafkamcp.toolsets.diagnose;

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

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.kafka.ConsumerGroups;
import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * Aynı araç çağrısında iki örnekli offset ölçümü: partition'ların end offset'leri ve grupların commit'li offset'leri
 * t0 ve t1'de okunur (her örnek tek listOffsets + tek listConsumerGroupOffsets). Yazma hızı, lag eğilimi ve takılı
 * (commit'i ilerlemeyen, end offset'i artan) partition tespiti için. Yanıt vermeyen grup/partition hatalarda
 * döner; bekleme kalan süreye göre kısaltılır, ikinci örneğe süre kalmazsa {@link #complete()} false olur.
 */
public final class OffsetWindow {

	/** İlk örneğin her adımı için en fazla bekleme. */
	public static final long FIRST_SAMPLE_MAX_MS = 4_000;

	/** İkinci örneğin her adımı için en fazla bekleme (bekleme süresi buna göre kısaltılır). */
	public static final long SECOND_SAMPLE_MAX_MS = 3_000;

	/** Bundan kısa bekleme anlamsız: ikinci örnek alınmaz. */
	public static final long MIN_PAUSE_MS = 500;

	private final Map<TopicPartition, Long> end0;

	private final Map<TopicPartition, Long> end1;

	private final Map<String, Map<TopicPartition, Long>> committed0;

	private final Map<String, Map<TopicPartition, Long>> committed1;

	private final Map<String, String> groupErrors;

	private final Map<TopicPartition, String> endErrors;

	private final long requestedMs;

	private final long pausedMs;

	private final double elapsedSeconds;

	private OffsetWindow(Map<TopicPartition, Long> end0, Map<TopicPartition, Long> end1,
			Map<String, Map<TopicPartition, Long>> committed0, Map<String, Map<TopicPartition, Long>> committed1,
			Map<String, String> groupErrors, Map<TopicPartition, String> endErrors, long requestedMs, long pausedMs,
			double elapsedSeconds) {
		this.end0 = end0;
		this.end1 = end1;
		this.committed0 = committed0;
		this.committed1 = committed1;
		this.groupErrors = groupErrors;
		this.endErrors = endErrors;
		this.requestedMs = requestedMs;
		this.pausedMs = pausedMs;
		this.elapsedSeconds = elapsedSeconds;
	}

	/**
	 * İki örnek alır.
	 * @param partitions end offset'i izlenecek partition'lar
	 * @param groups commit'li offset'leri izlenecek gruplar
	 * @param onlyGivenPartitions true: grupların sadece {@code partitions} içindeki commit'leri sorulur (topic
	 * odaklı); false: grupların tüm commit'leri sorulur ve bu partition'ların end offset'leri de izlenir
	 * @param intervalMs istenen bekleme (kalan süreye göre kısaltılabilir)
	 * @param reserveMs ikinci örnekten sonra araca bırakılacak süre
	 */
	public static OffsetWindow measure(ToolCall call, Collection<TopicPartition> partitions, Collection<String> groups,
			boolean onlyGivenPartitions, long intervalMs, long reserveMs) {
		List<TopicPartition> given = List.copyOf(new LinkedHashSet<>(partitions));
		List<String> groupIds = List.copyOf(new LinkedHashSet<>(groups));
		boolean queryGroups = !groupIds.isEmpty() && (!onlyGivenPartitions || !given.isEmpty());
		Map<String, String> groupErrors = new TreeMap<>();
		Map<TopicPartition, String> endErrors = new HashMap<>();

		long s0 = System.nanoTime();
		ListOffsetsResult early = !given.isEmpty() && (onlyGivenPartitions || !queryGroups)
				? Offsets.submit(call, given, OffsetSpec.latest()) : null;
		Map<String, Map<TopicPartition, Long>> c0 = new TreeMap<>();
		if (queryGroups) {
			Futures.Each<String, Map<TopicPartition, Long>> each = ConsumerGroups.committed(call, groupIds,
					onlyGivenPartitions ? given : List.of(), FIRST_SAMPLE_MAX_MS);
			c0.putAll(each.values());
			groupErrors.putAll(each.errors());
		}
		Set<TopicPartition> tps = new LinkedHashSet<>(given);
		if (!onlyGivenPartitions) {
			c0.values().forEach(m -> tps.addAll(m.keySet()));
		}
		Map<TopicPartition, Long> e0 = new HashMap<>();
		if (early != null) {
			collect(call, given, early, FIRST_SAMPLE_MAX_MS, e0, endErrors);
		}
		else if (!tps.isEmpty()) {
			collect(call, tps, Offsets.submit(call, tps, OffsetSpec.latest()), FIRST_SAMPLE_MAX_MS, e0, endErrors);
		}
		long f0 = System.nanoTime();

		List<String> withCommits = new ArrayList<>();
		c0.forEach((g, m) -> {
			if (!m.isEmpty()) {
				withCommits.add(g);
			}
		});
		long pause = Math.min(intervalMs, call.remainingMillis() - SECOND_SAMPLE_MAX_MS - reserveMs);
		if (pause < MIN_PAUSE_MS || e0.isEmpty() && withCommits.isEmpty()) {
			return new OffsetWindow(e0, Map.of(), c0, Map.of(), groupErrors, endErrors, intervalMs, -1, 0);
		}
		call.sleep(pause);

		long s1 = System.nanoTime();
		ListOffsetsResult late = e0.isEmpty() ? null : Offsets.submit(call, e0.keySet(), OffsetSpec.latest());
		Map<String, Map<TopicPartition, Long>> c1 = new TreeMap<>();
		if (!withCommits.isEmpty()) {
			Futures.Each<String, Map<TopicPartition, Long>> each = ConsumerGroups.committed(call, withCommits,
					onlyGivenPartitions ? given : List.of(), SECOND_SAMPLE_MAX_MS);
			c1.putAll(each.values());
			each.errors().forEach((g, e) -> groupErrors.putIfAbsent(g, "ikinci örnek: " + e));
		}
		Map<TopicPartition, Long> e1 = new HashMap<>();
		if (late != null) {
			Map<TopicPartition, String> lateErrors = new HashMap<>();
			collect(call, e0.keySet(), late, SECOND_SAMPLE_MAX_MS, e1, lateErrors);
			lateErrors.forEach(endErrors::putIfAbsent);
		}
		long f1 = System.nanoTime();
		double elapsed = ((s1 + f1) / 2.0 - (s0 + f0) / 2.0) / 1e9;
		return new OffsetWindow(e0, e1, c0, c1, groupErrors, endErrors, intervalMs, pause, elapsed);
	}

	private static void collect(ToolCall call, Collection<TopicPartition> tps, ListOffsetsResult result, long maxMs,
			Map<TopicPartition, Long> offsets, Map<TopicPartition, String> errors) {
		Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new LinkedHashMap<>();
		for (TopicPartition tp : tps) {
			futures.put(tp, result.partitionResult(tp));
		}
		Futures.Each<TopicPartition, ListOffsetsResultInfo> each = Futures.awaitEach(call, futures, maxMs);
		each.values().forEach((tp, info) -> offsets.put(tp, info.offset()));
		errors.putAll(each.errors());
	}

	/** İkinci örnek alındı mı? */
	public boolean complete() {
		return pausedMs >= 0;
	}

	/** İki örneğin orta noktaları arası saniye (ikinci örnek yoksa 0). */
	public double elapsedSeconds() {
		return elapsedSeconds;
	}

	/** Bekleme istenenden kısa tutuldu mu (süre sınırı)? */
	public boolean intervalReduced() {
		return complete() && pausedMs < requestedMs;
	}

	public Map<TopicPartition, Long> end0() {
		return end0;
	}

	public Map<TopicPartition, Long> end1() {
		return end1;
	}

	/** Grup → t0'daki commit'li offset'ler (commit'i olmayan partition'lar yok). */
	public Map<String, Map<TopicPartition, Long>> committed0() {
		return committed0;
	}

	/** Offset'leri alınamayan gruplar (grup → Türkçe hata). */
	public Map<String, String> groupErrors() {
		return groupErrors;
	}

	/** End offset'i alınamayan partition'lar. */
	public Map<TopicPartition, String> endErrors() {
		return endErrors;
	}

	/** t0'da commit'i olan gruplar. */
	public List<String> groupsWithCommits() {
		List<String> out = new ArrayList<>();
		committed0.forEach((g, m) -> {
			if (!m.isEmpty()) {
				out.add(g);
			}
		});
		return out;
	}

	/** İki örnekte de end offset'i bilinen partition'ların artışı. */
	public Map<TopicPartition, Long> endDelta() {
		Map<TopicPartition, Long> out = new LinkedHashMap<>();
		end0.forEach((tp, a) -> {
			Long b = end1.get(tp);
			if (b != null) {
				out.put(tp, Math.max(0, b - a));
			}
		});
		return out;
	}

	/** delta / geçen süre, 2 ondalık (ikinci örnek yoksa null). */
	public Double rate(long delta) {
		return complete() && elapsedSeconds > 0 ? Format.round(delta / elapsedSeconds, 2) : null;
	}

	/** Pencere bilgisi: {intervalSeconds, intervalReduced, secondSample}. */
	public Map<String, Object> describe() {
		return Results.map("intervalSeconds", complete() ? Format.round(elapsedSeconds, 2) : null, "intervalReduced",
				intervalReduced() ? true : null, "secondSample", complete() ? null : false);
	}

	/** Grubun t0..t1 ilerlemesi (sadece t0'da commit'i olan partition'lar). */
	public GroupProgress progress(String group) {
		Map<TopicPartition, Long> a = committed0.getOrDefault(group, Map.of());
		Map<TopicPartition, Long> b = committed1.getOrDefault(group, Map.of());
		List<PartitionProgress> rows = new ArrayList<>();
		for (Map.Entry<TopicPartition, Long> e : a.entrySet()) {
			TopicPartition tp = e.getKey();
			rows.add(new PartitionProgress(tp, e.getValue(), complete() ? b.get(tp) : null, end0.get(tp),
					complete() ? end1.get(tp) : null));
		}
		rows.sort(Comparator.comparing((PartitionProgress p) -> p.partition().topic())
			.thenComparingInt(p -> p.partition().partition()));
		return new GroupProgress(group, rows, complete(), elapsedSeconds);
	}

	/** Tek partition'ın iki örneği (bilinmeyenler null). */
	public record PartitionProgress(TopicPartition partition, Long committed0, Long committed1, Long end0, Long end1) {

		public Long lag0() {
			return committed0 != null && end0 != null ? Math.max(0, end0 - committed0) : null;
		}

		public Long lag1() {
			return committed1 != null && end1 != null ? Math.max(0, end1 - committed1) : null;
		}

		/** İki örnek de tam mı (commit ve end offset her iki anda biliniyor)? */
		public boolean measured() {
			return committed0 != null && committed1 != null && end0 != null && end1 != null;
		}

		/** Commit ilerlemedi, end offset arttı ve lag var. */
		public boolean stuck() {
			return measured() && committed1.equals(committed0) && end1 > end0 && lag1() > 0;
		}

		/** Commit geri gitti (offset reset). */
		public boolean reset() {
			return measured() && committed1 < committed0;
		}

		public Map<String, Object> toMap() {
			return Results.map("topic", partition.topic(), "partition", partition.partition(), "committed",
					committed1 != null ? committed1 : committed0, "end", end1 != null ? end1 : end0, "lag",
					lag1() != null ? lag1() : lag0(), "endDelta", measured() ? end1 - end0 : null);
		}

	}

	/** Bir topic'in (grup içinde) toplamları. */
	public record TopicProgress(String topic, long lag0, long lag1, long consumed, long produced, int partitions,
			int stuck) {
	}

	/** Grubun iki örnek arası ilerlemesi. */
	public record GroupProgress(String groupId, List<PartitionProgress> partitions, boolean complete,
			double elapsedSeconds) {

		/** t0 toplam lag'i (end offset'i bilinen partition'lar). */
		public long lag0() {
			return partitions.stream().map(PartitionProgress::lag0).filter(l -> l != null).mapToLong(Long::longValue)
				.sum();
		}

		/** Son bilinen toplam lag (t1, yoksa t0). */
		public long lag() {
			if (!complete) {
				return lag0();
			}
			long sum = 0;
			for (PartitionProgress p : partitions) {
				Long l = p.lag1() != null ? p.lag1() : p.lag0();
				sum += l == null ? 0 : l;
			}
			return sum;
		}

		public List<PartitionProgress> stuck() {
			return partitions.stream().filter(PartitionProgress::stuck).toList();
		}

		/** End offset'i bilinmeyen (lag'i hesaplanamayan) partition sayısı. */
		public int unknown() {
			return (int) partitions.stream().filter(p -> p.end0() == null).count();
		}

		public boolean resetDetected() {
			return partitions.stream().anyMatch(PartitionProgress::reset);
		}

		/** [lag0, lag1, consumed, produced] sadece iki örneği tam partition'larda. */
		private long[] measured(List<PartitionProgress> rows) {
			long[] s = new long[4];
			for (PartitionProgress p : rows) {
				if (p.measured()) {
					s[0] += p.lag0();
					s[1] += p.lag1();
					s[2] += p.committed1() - p.committed0();
					s[3] += p.end1() - p.end0();
				}
			}
			return s;
		}

		/** growing | shrinking | stable; ikinci örnek yoksa null. */
		public String trend() {
			if (!complete) {
				return null;
			}
			long[] s = measured(partitions);
			return s[1] > s[0] ? "growing" : s[1] < s[0] ? "shrinking" : "stable";
		}

		public Double consumeRate() {
			return complete && elapsedSeconds > 0 ? Format.round(measured(partitions)[2] / elapsedSeconds, 2) : null;
		}

		public Double produceRate() {
			return complete && elapsedSeconds > 0 ? Format.round(measured(partitions)[3] / elapsedSeconds, 2) : null;
		}

		/** Lag azalıyorsa tahmini yetişme süresi (sn). */
		public Long etaSeconds() {
			if (!complete || elapsedSeconds <= 0) {
				return null;
			}
			long[] s = measured(partitions);
			if (s[1] >= s[0]) {
				return null;
			}
			double perSecond = (s[0] - s[1]) / elapsedSeconds;
			return Math.round(s[1] / perSecond);
		}

		/** Topic → toplamlar (lag'e göre azalan). */
		public List<TopicProgress> byTopic() {
			Map<String, List<PartitionProgress>> grouped = new TreeMap<>();
			partitions.forEach(p -> grouped.computeIfAbsent(p.partition().topic(), k -> new ArrayList<>()).add(p));
			List<TopicProgress> out = new ArrayList<>();
			grouped.forEach((topic, rows) -> {
				long[] s = measured(rows);
				long lag0 = rows.stream().map(PartitionProgress::lag0).filter(l -> l != null).mapToLong(Long::longValue)
					.sum();
				long lag1 = complete ? rows.stream()
					.map(p -> p.lag1() != null ? p.lag1() : p.lag0())
					.filter(l -> l != null)
					.mapToLong(Long::longValue)
					.sum() : lag0;
				int stuck = (int) rows.stream().filter(PartitionProgress::stuck).count();
				out.add(new TopicProgress(topic, lag0, lag1, s[2], s[3], rows.size(), stuck));
			});
			out.sort(Comparator.comparingLong(TopicProgress::lag1).reversed().thenComparing(TopicProgress::topic));
			return out;
		}

		/** Özet: lag, trend, hızlar, eta, takılı partition sayısı. */
		public Map<String, Object> summary() {
			return Results.map("lag", lag(), "lagAtStart", complete ? lag0() : null, "trend", trend(), "consumeRate",
					consumeRate(), "produceRate", produceRate(), "etaSeconds", etaSeconds(), "stuckPartitions",
					complete ? stuck().size() : null, "lagUnknownPartitions", unknown() > 0 ? unknown() : null,
					"resetDetected", resetDetected() ? true : null);
		}

	}

}
