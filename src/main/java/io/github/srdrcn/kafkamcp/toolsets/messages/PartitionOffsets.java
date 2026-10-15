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

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Topic partition'larını çözer ve offset'leri partition başına değil tek {@code listOffsets} çağrısıyla
 * toplu alır (earliest/latest/forTimestamp). Birden çok istek önce başlatılıp sonra beklenebilir.
 * <p>
 * Karışık ortam (KRaft + ZooKeeper): describeTopics/listOffsets/fetch her iki modda aynıdır. Zamana göre arama
 * ise eski ZooKeeper broker'larında partition bazında reddedilebilir (v0 mesaj formatı:
 * UnsupportedForMessageFormatException; eski protokol: UnsupportedVersionException); bunlar
 * {@link #awaitEach} ile partition başına Türkçe not olur, aracı düşürmez.
 */
public final class PartitionOffsets {

	private PartitionOffsets() {
	}

	/** Offset ve o offset'teki kaydın zamanı (yoksa -1). */
	public record OffsetAt(long offset, long timestamp) {

		public boolean found() {
			return offset >= 0;
		}

	}

	/**
	 * Partition başına sonuç: bulunan offset'ler ve broker'ın desteklemediği (eski sürüm/mesaj formatı) için
	 * sonucu olmayan partition'ların Türkçe nedeni.
	 */
	public record Lookup(Map<TopicPartition, OffsetAt> offsets, Map<TopicPartition, String> unsupported) {

		/** Nedeni aynı olan partition'lar (id sırasıyla); not üretmek için. */
		public Map<String, List<Integer>> unsupportedByReason() {
			Map<String, List<Integer>> out = new LinkedHashMap<>();
			unsupported.entrySet()
				.stream()
				.sorted(Comparator.comparingInt(e -> e.getKey().partition()))
				.forEach(e -> out.computeIfAbsent(e.getValue(), k -> new ArrayList<>()).add(e.getKey().partition()));
			return out;
		}

	}

	/** Partition başına [earliest, latest) aralığı. */
	public record Bounds(Map<TopicPartition, Long> earliest, Map<TopicPartition, Long> latest) {

		public long earliest(TopicPartition tp) {
			return earliest.getOrDefault(tp, 0L);
		}

		public long latest(TopicPartition tp) {
			return latest.getOrDefault(tp, 0L);
		}

		/** latest - earliest (kaba kayıt sayısı). */
		public long size(TopicPartition tp) {
			return Math.max(0, latest(tp) - earliest(tp));
		}

	}

	/** Topic'in partition sayısı (lideri olmayanlar dahil; varsayılan partitioner da hepsini sayar). */
	public static int partitionCount(ToolCall call, String topic) {
		return describe(call, topic).partitions().size();
	}

	/**
	 * Topic'in seçilen partition'ları: lideri olanlar ({@code online}, id sırasıyla) ve lideri olmayan (offline)
	 * partition id'leri. Offline partition'lar listOffsets/fetch isteklerine konmaz: client lidersiz partition'ı
	 * zaman aşımına kadar yeniden dener ve tüm araç düşerdi.
	 */
	public record Selection(String topic, List<TopicPartition> online, List<Integer> offline) {

		/** Çıktı notu; offline partition yoksa null. */
		public String note() {
			if (offline.isEmpty()) {
				return null;
			}
			return offline.size() + " partition'ın lideri yok (offline), atlandı: " + offline
					+ "; sonuçlar sadece lideri olan partition'ları kapsar";
		}

	}

	/**
	 * Topic'in partition'larını seçer. {@code partition} verilirse sadece o partition; topic'te yoksa ya da
	 * lideri yoksa Türkçe hata.
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
						throw new ToolException("Partition " + partition + " offline: lideri yok (broker kapalı ya da "
								+ "ISR'da replika yok); offset/mesaj okunamaz" + elrHint(i));
					}
					return new Selection(topic, List.of(new TopicPartition(topic, partition)), List.of());
				}
			}
			throw new ToolException("Partition " + partition + " yok: " + topic + " topic'inde " + infos.size()
					+ " partition var (0.." + (infos.size() - 1) + ")");
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
	 * Lidersiz partition için ELR bilgisi (KIP-966, sadece KRaft). ZooKeeper broker'larında (ve ELR kapalıyken)
	 * elr()/lastKnownElr() null ya da boştur: ek bilgi yok.
	 */
	static String elrHint(TopicPartitionInfo i) {
		List<Integer> elr = ids(i.elr());
		List<Integer> lastKnown = ids(i.lastKnownElr());
		if (elr.isEmpty() && lastKnown.isEmpty()) {
			return "";
		}
		StringBuilder sb = new StringBuilder(" (KRaft");
		if (!elr.isEmpty()) {
			sb.append("; ELR: ").append(elr).append(", bu replikalardan biri dönünce veri kaybı olmadan lider seçilir");
		}
		if (!lastKnown.isEmpty()) {
			sb.append("; son bilinen ELR: ").append(lastKnown);
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
			throw new ToolException("Topic bulunamadı: " + topic);
		}
		return d;
	}

	/** listOffsets isteğini başlatır (beklemez); sonucu {@link #await(ToolCall, ListOffsetsResult)} ile alın. */
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
	 * {@link #request} sonucunu partition başına bekler. Broker'ın desteklemediği partition'lar
	 * (UnsupportedVersionException / UnsupportedForMessageFormatException; eski ZooKeeper broker'ları)
	 * {@link Lookup#unsupported()}'a Türkçe nedenle düşer; diğer hatalar ve zaman aşımı yine Türkçe
	 * {@link ToolException} olur.
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

	/** Broker'ın bu isteği desteklemediğini gösteren hata için Türkçe neden; başka hata ise null. */
	static String unsupportedReason(Throwable error) {
		if (error instanceof UnsupportedForMessageFormatException) {
			return "broker bu işlemi desteklemiyor (eski mesaj formatı: v0 kayıtlarda zaman damgası yok)";
		}
		if (error instanceof UnsupportedVersionException) {
			return "broker bu işlemi desteklemiyor (sürüm)";
		}
		return null;
	}

	/** earliest ve latest offset'leri (iki istek paralel). */
	public static Bounds bounds(ToolCall call, Collection<TopicPartition> tps) {
		if (tps.isEmpty()) {
			return new Bounds(Map.of(), Map.of());
		}
		ListOffsetsResult e = request(call, tps, OffsetSpec.earliest());
		ListOffsetsResult l = request(call, tps, OffsetSpec.latest());
		return new Bounds(offsets(await(call, e)), offsets(await(call, l)));
	}

	/** Partition başına zamanı {@code >= timestamp} olan ilk offset (yoksa offset -1). */
	public static Map<TopicPartition, OffsetAt> forTimestamp(ToolCall call, Collection<TopicPartition> tps,
			long timestamp) {
		return tps.isEmpty() ? Map.of() : await(call, request(call, tps, OffsetSpec.forTimestamp(timestamp)));
	}

	/**
	 * {@link #forTimestamp} gibi, ama zamana göre aramayı desteklemeyen partition'lar (eski ZooKeeper
	 * broker'ı/mesaj formatı) hata yerine {@link Lookup#unsupported()}'a düşer.
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
