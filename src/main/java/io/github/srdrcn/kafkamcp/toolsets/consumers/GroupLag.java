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
 * Consumer group lag'i için toplu (batch) yapı taşları: çok sayıda grubun committed offset'leri tek
 * {@code listConsumerGroupOffsets} çağrısıyla, ilgili tüm partition'ların end offset'leri tek
 * {@code listOffsets} çağrısıyla alınır; lag = max(0, end - committed). Commit'i olmayan partition'lar
 * lag 0 sayılmaz, {@code committed == null} olarak raporlanır. Kaybolan gruplar grup bazında hata olur.
 * Her istek sonraki adımlara pay bırakan bir timeoutMs ile gönderilir: yüklenen koordinatör ya da leader'ı
 * olmayan partition aracı düşürmez, ilgili grup/partition "yanıt gelmedi" (timedOut) olarak döner.
 */
public final class GroupLag {

	/** committed(): ardından gelen end offset sorgusu ve çıktı için deadline'dan ayrılan varsayılan pay. */
	public static final long COMMITTED_RESERVE_MS = 3_000;

	/** offsets()/endOffsets(): çıktı için deadline'dan ayrılan varsayılan pay. */
	public static final long OFFSETS_RESERVE_MS = 1_000;

	/**
	 * Tek partition'ın lag satırı. committed null: grup bu partition'a commit etmemiş; end null: end offset
	 * alınamadı (error'da sebep); lag ikisi de biliniyorsa dolu.
	 */
	public record PartitionLag(String topic, int partition, Long committed, Long end, Long lag, String error) {

		public TopicPartition topicPartition() {
			return new TopicPartition(topic, partition);
		}

		/** Lag'e göre azalan (lag'siz satırlar sonda), sonra topic/partition. */
		public static final Comparator<PartitionLag> BY_LAG_DESC = Comparator
			.comparing((PartitionLag p) -> p.lag() == null ? -1L : p.lag(), Comparator.reverseOrder())
			.thenComparing(PartitionLag::topic)
			.thenComparingInt(PartitionLag::partition);

		public static final Comparator<PartitionLag> BY_TOPIC_PARTITION = Comparator.comparing(PartitionLag::topic)
			.thenComparingInt(PartitionLag::partition);

	}

	/**
	 * Bir grubun committed offset'leri; offset'i null dönen partition'lar withoutOffset'te; grup hatası error'da
	 * (notFound: grup yok; timedOut: süre içinde yanıt gelmedi).
	 */
	public record Committed(Map<TopicPartition, Long> offsets, Set<TopicPartition> withoutOffset, String error,
			boolean notFound, boolean timedOut) {

		public boolean isEmpty() {
			return offsets.isEmpty() && withoutOffset.isEmpty();
		}

	}

	/**
	 * listOffsets sonucu: alınan offset'ler ve alınamayan partition'ların hata mesajları; timedOut: bunlardan
	 * süre içinde yanıt gelmeyenlerin sayısı.
	 */
	public record OffsetLookup(Map<TopicPartition, Long> offsets, Map<TopicPartition, String> errors, int timedOut) {

		/** Hataları topic bazında tekilleştirir (topic → ilk hata mesajı). */
		public Map<String, String> errorsByTopic() {
			Map<String, String> m = new TreeMap<>();
			errors.forEach((tp, e) -> m.putIfAbsent(tp.topic(), e));
			return m;
		}

	}

	/**
	 * Bir grubun lag özeti (partition'lar topic/partition sırasıyla). error: grup bazında hata; notFound: grup
	 * arada silinmiş; timedOut: grubun offset'leri için süre içinde yanıt gelmedi.
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

		/** topic → toplam lag, lag'e göre azalan. */
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

		/** Commit'i olmayan partition sayısı. */
		public int withoutCommit() {
			return (int) partitions.stream().filter(p -> p.committed() == null).count();
		}

		/** Commit'i olup end offset'i alınamayan partition sayısı. */
		public int withoutEnd() {
			return (int) partitions.stream().filter(p -> p.committed() != null && p.end() == null).count();
		}

		/** Commit'i olup end offset'i alınamayan partition'lar (lag'i bilinmiyor; toplam lag alt sınırdır). */
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
	 * Grupların committed offset'leri tek listConsumerGroupOffsets çağrısıyla. topicFilter null değilse sadece
	 * uyan topic'ler tutulur. Deadline'dan {@link #COMMITTED_RESERVE_MS} pay bırakır.
	 */
	public static Map<String, Committed> committed(ToolCall call, Collection<String> groups,
			Predicate<String> topicFilter) {
		return committed(call, groups, topicFilter, COMMITTED_RESERVE_MS);
	}

	/**
	 * {@link #committed(ToolCall, Collection, Predicate)}; istek en geç (kalan süre - reserveMillis) içinde biter,
	 * yanıt gelmeyen gruplar {@code timedOut} olarak döner.
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

	/** Partition'ların latest (end) offset'leri tek listOffsets çağrısıyla. */
	public static OffsetLookup endOffsets(ToolCall call, Collection<TopicPartition> partitions) {
		return offsets(call, partitions, OffsetSpec.latest(), OFFSETS_RESERVE_MS);
	}

	/** {@link #endOffsets(ToolCall, Collection)}; deadline'dan reserveMillis pay bırakır. */
	public static OffsetLookup endOffsets(ToolCall call, Collection<TopicPartition> partitions, long reserveMillis) {
		return offsets(call, partitions, OffsetSpec.latest(), reserveMillis);
	}

	/** {@link #offsets(ToolCall, Collection, OffsetSpec, long)}; deadline'dan {@link #OFFSETS_RESERVE_MS} pay. */
	public static OffsetLookup offsets(ToolCall call, Collection<TopicPartition> partitions, OffsetSpec spec) {
		return offsets(call, partitions, spec, OFFSETS_RESERVE_MS);
	}

	/**
	 * Partition'ların verilen spec'e göre offset'leri tek listOffsets çağrısıyla; alınamayanlar (silinmiş
	 * topic, yetki, leader'ı olmayan partition) partition bazında errors'a düşer. Timestamp spec'inde kayıt
	 * yoksa offset -1 döner. İstek en geç (kalan süre - reserveMillis) içinde biter.
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

	/** Committed offset'lerdeki (ve varsa extra'daki) tüm partition'lar. */
	public static Set<TopicPartition> partitionsOf(Map<String, Committed> committed) {
		Set<TopicPartition> tps = new LinkedHashSet<>();
		for (Committed c : committed.values()) {
			tps.addAll(c.offsets().keySet());
			tps.addAll(c.withoutOffset());
		}
		return tps;
	}

	/** Toplu lag taraması: grup bazında lag ve end offset sorgusunun kendisi (alınamayan partition'lar için). */
	public record Snapshot(Map<String, GroupLagInfo> groups, OffsetLookup end) {
	}

	/**
	 * Grupların lag'i: committed (tek çağrı) + end offset (tek çağrı). topicFilter null değilse sadece uyan
	 * topic'ler. Hata veren (ör. arada silinen) ya da yanıt vermeyen grup kendi satırında error ile döner.
	 */
	public static Map<String, GroupLagInfo> snapshot(ToolCall call, Collection<String> groups,
			Predicate<String> topicFilter) {
		return lagSnapshot(call, groups, topicFilter).groups();
	}

	/** {@link #snapshot}; end offset sorgusunun hata/yanıtsız sayılarıyla birlikte. */
	public static Snapshot lagSnapshot(ToolCall call, Collection<String> groups, Predicate<String> topicFilter) {
		Map<String, Committed> committed = committed(call, groups, topicFilter);
		OffsetLookup end = endOffsets(call, partitionsOf(committed));
		return new Snapshot(compute(committed, end, Map.of()), end);
	}

	/**
	 * Committed + end offset'lerden lag hesaplar. extraPartitions: grubun commit'i olmasa da gösterilecek
	 * partition'ları (ör. üyelere atanmış olanlar); bunların end offset'leri de {@code end}'de olmalıdır.
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

	/** Tek partition satırı. */
	public static PartitionLag row(TopicPartition tp, Long committed, OffsetLookup end) {
		Long endOffset = end.offsets().get(tp);
		Long lag = committed != null && endOffset != null ? lag(endOffset, committed) : null;
		String error = endOffset == null ? end.errors().get(tp) : null;
		return new PartitionLag(tp.topic(), tp.partition(), committed, endOffset, lag, error);
	}

}
