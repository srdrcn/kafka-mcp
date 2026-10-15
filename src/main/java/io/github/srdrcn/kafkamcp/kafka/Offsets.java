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
 * Offset sorguları: her çağrı tüm partition'lar için TEK listOffsets gönderir. Başarısız partition'lar
 * sonuçta yer almaz (hiçbiri başarılı değilse hata fırlatılır); çağıran eksikleri raporlamalıdır.
 */
public final class Offsets {

	private Offsets() {
	}

	/** earliest/latest aralığı; count = latest - earliest. */
	public record Range(long earliest, long latest) {

		/** Yaklaşık mesaj sayısı (compaction/transaction marker'ları nedeniyle üst sınır). */
		public long count() {
			return Math.max(0, latest - earliest);
		}

	}

	/** Tek listOffsets çağrısı (spec tüm partition'lar için aynı). */
	public static Map<TopicPartition, ListOffsetsResultInfo> list(ToolCall call, Collection<TopicPartition> tps,
			OffsetSpec spec) {
		return awaitEach(call, tps, submit(call, tps, spec)).requireAny().values();
	}

	/** Partition başına latest (log end) offset. */
	public static Map<TopicPartition, Long> latest(ToolCall call, Collection<TopicPartition> tps) {
		return offsets(list(call, tps, OffsetSpec.latest()));
	}

	/** Partition başına earliest offset. */
	public static Map<TopicPartition, Long> earliest(ToolCall call, Collection<TopicPartition> tps) {
		return offsets(list(call, tps, OffsetSpec.earliest()));
	}

	/** earliest + latest; iki listOffsets aynı anda gönderilir. Sadece ikisi de gelen partition'lar döner. */
	public static Map<TopicPartition, Range> ranges(ToolCall call, Collection<TopicPartition> tps) {
		return submitRanges(call, tps).await(call);
	}

	/** earliest + latest isteklerini gönderir (beklemez); başka isteklerle paralel çalıştırmak için. */
	public static PendingRanges submitRanges(ToolCall call, Collection<TopicPartition> tps) {
		return new PendingRanges(tps, submit(call, tps, OffsetSpec.earliest()), submit(call, tps, OffsetSpec.latest()));
	}

	/** Gönderilmiş earliest/latest istekleri. */
	public record PendingRanges(Collection<TopicPartition> tps, ListOffsetsResult earliest, ListOffsetsResult latest) {

		/** Sonuçları bekler; sadece ikisi de gelen partition'lar döner (hiçbiri gelmezse hata). */
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

	/** listOffsets isteğini gönderir (beklemez); birden çok spec'i paralel göndermek için. */
	public static ListOffsetsResult submit(ToolCall call, Collection<TopicPartition> tps, OffsetSpec spec) {
		Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
		for (TopicPartition tp : tps) {
			specs.put(tp, spec);
		}
		return call.admin().listOffsets(specs);
	}

	/** {@link #submit} sonucunu bekler; partition bazında başarılı sonuçlar ve hatalar. */
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
