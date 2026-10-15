package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.Map;

import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Data/activity summary of a topic ({@link FleetScan#activity}): whether it is empty (earliest == latest on every
 * partition), whether it was never written (latest == 0), the message count and the last write time (the largest
 * maxTimestamp of the partitions). Unknown values are null; if {@code lastWriteComplete} is false the last write
 * is a lower bound.
 */
public record TopicActivity(String topic, Boolean empty, boolean neverWritten, Long messages, Long lastWriteMs,
		boolean lastWriteComplete, String unknownReason) {

	private static final long DAY_MS = 86_400_000L;

	/** Is the last write definitely older than the threshold? Null for empty topics and uncertain cases. */
	public Boolean idleSince(long thresholdMs) {
		if (Boolean.TRUE.equals(empty)) {
			return null;
		}
		if (lastWriteMs != null && lastWriteMs >= thresholdMs) {
			return false;
		}
		if (lastWriteComplete && lastWriteMs != null && Boolean.FALSE.equals(empty)) {
			return true;
		}
		return null;
	}

	/** Whole days since the last write; null if unknown. */
	public Long ageDays(long nowMs) {
		return lastWriteMs == null ? null : Math.max(0, (nowMs - lastWriteMs) / DAY_MS);
	}

	/** Output fields: lastWriteTime, lastWriteAgeDays (+ lastWriteIncomplete if incomplete). */
	public Map<String, Object> lastWriteFields(long nowMs) {
		return Results.map("lastWriteTime", lastWriteMs == null ? null : Format.iso(lastWriteMs), "lastWriteAgeDays",
				ageDays(nowMs), "lastWriteIncomplete", lastWriteMs != null && !lastWriteComplete ? true : null);
	}

	/** Threshold: {@code days} days before now (epoch ms). */
	public static long threshold(long nowMs, int days) {
		return nowMs - days * DAY_MS;
	}

}
