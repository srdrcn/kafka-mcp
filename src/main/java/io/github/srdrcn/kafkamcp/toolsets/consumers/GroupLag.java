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

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * Consumer group lag'i için toplu (batch) yapı taşları: çok sayıda grubun committed offset'leri tek
 * {@code listConsumerGroupOffsets} çağrısıyla, ilgili tüm partition'ların end offset'leri tek
 * {@code listOffsets} çağrısıyla alınır; lag = max(0, end - committed). Commit'i olmayan partition'lar
 * lag 0 sayılmaz, {@code committed == null} olarak raporlanır. Kaybolan gruplar grup bazında hata olur.
 */
public final class GroupLag {

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

	/** Bir grubun committed offset'leri; offset'i null dönen partition'lar withoutOffset'te; grup hatası error'da. */
	public record Committed(Map<TopicPartition, Long> offsets, Set<TopicPartition> withoutOffset, String error,
			boolean notFound) {

		public boolean isEmpty() {
			return offsets.isEmpty() && withoutOffset.isEmpty();
		}

	}

	/** listOffsets sonucu: alınan offset'ler ve alınamayan partition'ların hata mesajları. */
	public record OffsetLookup(Map<TopicPartition, Long> offsets, Map<TopicPartition, String> errors) {

		/** Hataları topic bazında tekilleştirir (topic → ilk hata mesajı). */
		public Map<String, String> errorsByTopic() {
			Map<String, String> m = new TreeMap<>();
			errors.forEach((tp, e) -> m.putIfAbsent(tp.topic(), e));
			return m;
		}

	}

	/**
	 * Bir grubun lag özeti (partition'lar topic/partition sırasıyla). error: grup bazında hata; notFound: grup
	 * arada silinmiş.
	 */
	public record GroupLagInfo(String groupId, List<PartitionLag> partitions, String error, boolean notFound) {

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
	 * uyan topic'ler tutulur.
	 */
	public static Map<String, Committed> committed(ToolCall call, Collection<String> groups,
			Predicate<String> topicFilter) {
		Map<String, Committed> out = new LinkedHashMap<>();
		if (groups.isEmpty()) {
			return out;
		}
		Map<String, ListConsumerGroupOffsetsSpec> specs = new LinkedHashMap<>();
		for (String g : groups) {
			specs.put(g, new ListConsumerGroupOffsetsSpec());
		}
		ListConsumerGroupOffsetsResult r = call.admin().listConsumerGroupOffsets(specs);
		for (String g : specs.keySet()) {
			Settled<Map<TopicPartition, OffsetAndMetadata>> s = Settled.of(call, r.partitionsToOffsetAndMetadata(g));
			if (!s.ok()) {
				out.put(g, new Committed(Map.of(), Set.of(), s.errorMessage(), ConsumerGroups.notFound(s.error())));
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
			out.put(g, new Committed(offsets, without, null, false));
		}
		return out;
	}

	/** Partition'ların latest (end) offset'leri tek listOffsets çağrısıyla. */
	public static OffsetLookup endOffsets(ToolCall call, Collection<TopicPartition> partitions) {
		return offsets(call, partitions, OffsetSpec.latest());
	}

	/**
	 * Partition'ların verilen spec'e göre offset'leri tek listOffsets çağrısıyla; alınamayanlar (silinmiş
	 * topic, yetki) partition bazında errors'a düşer. Timestamp spec'inde kayıt yoksa offset -1 döner.
	 */
	public static OffsetLookup offsets(ToolCall call, Collection<TopicPartition> partitions, OffsetSpec spec) {
		Map<TopicPartition, Long> offsets = new HashMap<>();
		Map<TopicPartition, String> errors = new HashMap<>();
		if (partitions.isEmpty()) {
			return new OffsetLookup(offsets, errors);
		}
		Map<TopicPartition, OffsetSpec> request = new HashMap<>();
		for (TopicPartition tp : partitions) {
			request.put(tp, spec);
		}
		ListOffsetsResult r = call.admin().listOffsets(request);
		Settled<Map<TopicPartition, ListOffsetsResultInfo>> all = Settled.of(call, r.all());
		if (all.ok()) {
			all.value().forEach((tp, info) -> offsets.put(tp, info.offset()));
			return new OffsetLookup(offsets, errors);
		}
		for (TopicPartition tp : request.keySet()) {
			Settled<ListOffsetsResultInfo> s = Settled.of(call, r.partitionResult(tp));
			if (s.ok()) {
				offsets.put(tp, s.value().offset());
			}
			else {
				errors.put(tp, s.errorMessage());
			}
		}
		return new OffsetLookup(offsets, errors);
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

	/**
	 * Grupların lag'i: committed (tek çağrı) + end offset (tek çağrı). topicFilter null değilse sadece uyan
	 * topic'ler. Hata veren (ör. arada silinen) grup kendi satırında error ile döner.
	 */
	public static Map<String, GroupLagInfo> snapshot(ToolCall call, Collection<String> groups,
			Predicate<String> topicFilter) {
		Map<String, Committed> committed = committed(call, groups, topicFilter);
		OffsetLookup end = endOffsets(call, partitionsOf(committed));
		return compute(committed, end, Map.of());
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
			out.put(e.getKey(), new GroupLagInfo(e.getKey(), rows, c.error(), c.notFound()));
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
