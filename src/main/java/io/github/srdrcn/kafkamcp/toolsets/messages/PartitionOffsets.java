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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Topic partition'larını çözer ve offset'leri partition başına değil tek {@code listOffsets} çağrısıyla
 * toplu alır (earliest/latest/forTimestamp). Birden çok istek önce başlatılıp sonra beklenebilir.
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

	/** Topic'in partition sayısı. */
	public static int partitionCount(ToolCall call, String topic) {
		return describe(call, topic).partitions().size();
	}

	/**
	 * Topic'in partition'ları (id sırasıyla). {@code partition} verilirse sadece o partition; topic'te yoksa
	 * Türkçe hata.
	 */
	public static List<TopicPartition> partitions(ToolCall call, String topic, Integer partition) {
		List<TopicPartitionInfo> infos = new ArrayList<>(describe(call, topic).partitions());
		infos.sort(Comparator.comparingInt(TopicPartitionInfo::partition));
		if (partition != null) {
			for (TopicPartitionInfo i : infos) {
				if (i.partition() == partition) {
					return List.of(new TopicPartition(topic, partition));
				}
			}
			throw new ToolException("Partition " + partition + " yok: " + topic + " topic'inde " + infos.size()
					+ " partition var (0.." + (infos.size() - 1) + ")");
		}
		return infos.stream().map(i -> new TopicPartition(topic, i.partition())).toList();
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

	public static Map<TopicPartition, Long> offsets(Map<TopicPartition, OffsetAt> m) {
		Map<TopicPartition, Long> out = new HashMap<>();
		m.forEach((tp, o) -> out.put(tp, o.offset()));
		return out;
	}

}
