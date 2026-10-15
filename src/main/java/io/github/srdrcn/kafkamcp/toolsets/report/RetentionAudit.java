package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.Map;

import org.apache.kafka.clients.admin.Config;

import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.toolsets.report.Findings.Severity;

/**
 * Retention/segment audit rules (pure computation, testable): infinite retention (high severity if large, medium
 * if the size is unknown), very short retention, retention.bytes, short delete.retention.ms and extreme dirty ratio
 * on compacted topics, segment size/time outliers. Every finding carries the config value and its source
 * (topic/broker/default).
 */
public final class RetentionAudit {

	/** segment.bytes below this size is considered too small (many files/open handles). */
	static final long SMALL_SEGMENT_BYTES = 16L * 1024 * 1024;

	private RetentionAudit() {
	}

	/** Thresholds: large topic (bytes) and short retention/segment time (ms). */
	public record Thresholds(long largeBytes, long shortMs) {
	}

	/**
	 * Applies the rules to one topic; sizeBytes is null if unknown and a lower bound if sizeComplete is false (the
	 * log dirs of some brokers could not be read).
	 */
	public static void evaluate(String topic, Config c, Long sizeBytes, boolean sizeComplete, int partitions,
			Thresholds th, Findings f) {
		boolean deletes = ConfigValues.deletes(c);
		boolean compacted = ConfigValues.compacted(c);
		Long retentionMs = ConfigValues.longValue(c, "retention.ms");
		Long retentionBytes = ConfigValues.longValue(c, "retention.bytes");
		Long segmentMs = ConfigValues.longValue(c, "segment.ms");
		Long segmentBytes = ConfigValues.longValue(c, "segment.bytes");
		if (deletes && retentionMs != null) {
			boolean unboundedBytes = retentionBytes == null || retentionBytes < 0;
			if (retentionMs < 0 && unboundedBytes) {
				Map<String, Object> item = withSize(ConfigValues.item(topic, c, "retention.ms"), sizeBytes, sizeComplete);
				if (sizeBytes != null && sizeBytes >= th.largeBytes()) {
					f.add("infinite_retention_large", Severity.HIGH,
							"Infinite retention (retention.ms=-1, retention.bytes=-1) and a large size: disk usage keeps growing", item);
				}
				else if (sizeBytes == null || !sizeComplete) {
					f.add("infinite_retention_size_unknown", Severity.MEDIUM,
							"Infinite retention (retention.ms=-1, retention.bytes=-1); size unknown or incomplete (log dirs could not be read): may be large",
							item);
				}
				else {
					f.add("infinite_retention", Severity.INFO,
							"Infinite retention (retention.ms=-1, retention.bytes=-1); small for now", item);
				}
			}
			else if (retentionMs >= 0 && retentionMs < th.shortMs()) {
				f.add("short_retention", Severity.MEDIUM,
						"Very short retention: lagging/stopped consumers lose data",
						ConfigValues.item(topic, c, "retention.ms"));
			}
			if (retentionMs >= 0 && segmentMs != null && segmentMs > retentionMs) {
				Map<String, Object> item = ConfigValues.item(topic, c, "segment.ms");
				item.put("retentionMs", retentionMs);
				f.add("segment_ms_above_retention", Severity.LOW,
						"segment.ms > retention.ms: a segment cannot be deleted before it is rolled, so data is kept longer than retention", item);
			}
		}
		if (deletes && retentionBytes != null && retentionBytes > 0) {
			Map<String, Object> item = withSize(ConfigValues.item(topic, c, "retention.bytes"), sizeBytes, sizeComplete);
			long cap = retentionBytes * Math.max(1, partitions);
			item.put("partitions", partitions);
			item.put("maxTopicBytes", cap);
			item.put("maxTopicSize", Format.bytes(cap));
			f.add("retention_bytes_set", Severity.INFO,
					"retention.bytes applies per partition; topic limit ≈ value times the partition count", item);
		}
		if (compacted) {
			Long deleteRetention = ConfigValues.longValue(c, "delete.retention.ms");
			if (deleteRetention != null && deleteRetention < th.shortMs()) {
				f.add("short_delete_retention", Severity.MEDIUM,
						"Short delete.retention.ms on a compacted topic: slow consumers may miss tombstones",
						ConfigValues.item(topic, c, "delete.retention.ms"));
			}
			Double ratio = ConfigValues.doubleValue(c, "min.cleanable.dirty.ratio");
			if (ratio != null && (ratio < 0.1 || ratio > 0.9)) {
				f.add("dirty_ratio_extreme", Severity.LOW,
						"Extreme min.cleanable.dirty.ratio: < 0.1 means frequent compaction (CPU/disk IO), > 0.9 keeps duplicate keys for a long time",
						ConfigValues.item(topic, c, "min.cleanable.dirty.ratio"));
			}
		}
		if (segmentBytes != null && segmentBytes < SMALL_SEGMENT_BYTES) {
			f.add("small_segment_bytes", Severity.MEDIUM,
					"segment.bytes < 16 MiB: many segment files and open files/mmaps", ConfigValues.item(topic, c,
							"segment.bytes"));
		}
		if (segmentMs != null && segmentMs < th.shortMs()) {
			f.add("short_segment_ms", Severity.MEDIUM, "segment.ms is very short: segments roll often and the file count grows",
					ConfigValues.item(topic, c, "segment.ms"));
		}
	}

	private static Map<String, Object> withSize(Map<String, Object> item, Long sizeBytes, boolean sizeComplete) {
		if (sizeBytes != null) {
			item.put("sizeBytes", sizeBytes);
			item.put("size", Format.bytes(sizeBytes));
			if (!sizeComplete) {
				item.put("sizeLowerBound", true);
			}
		}
		return item;
	}

}
