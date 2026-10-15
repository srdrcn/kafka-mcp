package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.Map;

import org.apache.kafka.clients.admin.Config;

import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.toolsets.report.Findings.Severity;

/**
 * Retention/segment denetimi kuralları (saf hesap, test edilebilir): sınırsız retention (büyükse yüksek önem),
 * çok kısa retention, retention.bytes, compact topic'lerde kısa delete.retention.ms ve uç dirty ratio, segment
 * boyutu/süresi aykırılıkları. Her bulgu config değeri ve kaynağıyla (topic/broker/default) döner.
 */
public final class RetentionAudit {

	/** Bu boyutun altındaki segment.bytes çok küçük sayılır (çok dosya/açık handle). */
	static final long SMALL_SEGMENT_BYTES = 16L * 1024 * 1024;

	private RetentionAudit() {
	}

	/** Eşikler: büyük topic (bayt) ve kısa retention/segment süresi (ms). */
	public record Thresholds(long largeBytes, long shortMs) {
	}

	/** Kuralları tek topic'e uygular; sizeBytes bilinmiyorsa null. */
	public static void evaluate(String topic, Config c, Long sizeBytes, int partitions, Thresholds th, Findings f) {
		boolean deletes = ConfigValues.deletes(c);
		boolean compacted = ConfigValues.compacted(c);
		Long retentionMs = ConfigValues.longValue(c, "retention.ms");
		Long retentionBytes = ConfigValues.longValue(c, "retention.bytes");
		Long segmentMs = ConfigValues.longValue(c, "segment.ms");
		Long segmentBytes = ConfigValues.longValue(c, "segment.bytes");
		if (deletes && retentionMs != null) {
			boolean unboundedBytes = retentionBytes == null || retentionBytes < 0;
			if (retentionMs < 0 && unboundedBytes) {
				Map<String, Object> item = withSize(ConfigValues.item(topic, c, "retention.ms"), sizeBytes);
				if (sizeBytes != null && sizeBytes >= th.largeBytes()) {
					f.add("infinite_retention_large", Severity.HIGH,
							"Sınırsız retention (retention.ms=-1, retention.bytes=-1) ve büyük boyut: disk sürekli büyür", item);
				}
				else {
					f.add("infinite_retention", Severity.INFO,
							"Sınırsız retention (retention.ms=-1, retention.bytes=-1); şimdilik küçük", item);
				}
			}
			else if (retentionMs >= 0 && retentionMs < th.shortMs()) {
				f.add("short_retention", Severity.MEDIUM,
						"Çok kısa retention: geride kalan/duran consumer'lar veri kaybeder",
						ConfigValues.item(topic, c, "retention.ms"));
			}
			if (retentionMs >= 0 && segmentMs != null && segmentMs > retentionMs) {
				Map<String, Object> item = ConfigValues.item(topic, c, "segment.ms");
				item.put("retentionMs", retentionMs);
				f.add("segment_ms_above_retention", Severity.LOW,
						"segment.ms > retention.ms: segment kapanmadan silinemez, veri retention'dan uzun tutulur", item);
			}
		}
		if (deletes && retentionBytes != null && retentionBytes > 0) {
			Map<String, Object> item = withSize(ConfigValues.item(topic, c, "retention.bytes"), sizeBytes);
			long cap = retentionBytes * Math.max(1, partitions);
			item.put("partitions", partitions);
			item.put("maxTopicBytes", cap);
			item.put("maxTopicSize", Format.bytes(cap));
			f.add("retention_bytes_set", Severity.INFO,
					"retention.bytes partition başına uygulanır; topic üst sınırı ≈ değer × partition sayısı", item);
		}
		if (compacted) {
			Long deleteRetention = ConfigValues.longValue(c, "delete.retention.ms");
			if (deleteRetention != null && deleteRetention < th.shortMs()) {
				f.add("short_delete_retention", Severity.MEDIUM,
						"Compact topic'te kısa delete.retention.ms: yavaş consumer'lar tombstone'ları göremeyebilir",
						ConfigValues.item(topic, c, "delete.retention.ms"));
			}
			Double ratio = ConfigValues.doubleValue(c, "min.cleanable.dirty.ratio");
			if (ratio != null && (ratio < 0.1 || ratio > 0.9)) {
				f.add("dirty_ratio_extreme", Severity.LOW,
						"min.cleanable.dirty.ratio uç değerde: < 0.1 sık compaction (CPU/disk IO), > 0.9 tekrar eden anahtarlar uzun kalır",
						ConfigValues.item(topic, c, "min.cleanable.dirty.ratio"));
			}
		}
		if (segmentBytes != null && segmentBytes < SMALL_SEGMENT_BYTES) {
			f.add("small_segment_bytes", Severity.MEDIUM,
					"segment.bytes < 16 MiB: çok sayıda segment dosyası ve açık dosya/mmap", ConfigValues.item(topic, c,
							"segment.bytes"));
		}
		if (segmentMs != null && segmentMs < th.shortMs()) {
			f.add("short_segment_ms", Severity.MEDIUM, "segment.ms çok kısa: sık segment açılır, dosya sayısı artar",
					ConfigValues.item(topic, c, "segment.ms"));
		}
	}

	private static Map<String, Object> withSize(Map<String, Object> item, Long sizeBytes) {
		if (sizeBytes != null) {
			item.put("sizeBytes", sizeBytes);
			item.put("size", Format.bytes(sizeBytes));
		}
		return item;
	}

}
