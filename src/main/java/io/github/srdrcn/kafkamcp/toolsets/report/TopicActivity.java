package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.Map;

import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Topic'in veri/aktivite özeti ({@link FleetScan#activity}): boş mu (earliest == latest her partition'da),
 * hiç yazılmamış mı (latest == 0), mesaj sayısı ve son yazma zamanı (partition'ların maxTimestamp'lerinin en
 * büyüğü). Bilinmeyen değerler null; {@code lastWriteComplete} false ise son yazma alt sınırdır.
 */
public record TopicActivity(String topic, Boolean empty, boolean neverWritten, Long messages, Long lastWriteMs,
		boolean lastWriteComplete, String unknownReason) {

	private static final long DAY_MS = 86_400_000L;

	/** Son yazma kesin olarak eşikten eski mi? Boş topic'ler ve belirsizler için null. */
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

	/** Son yazmadan bu yana geçen gün (tam sayı); bilinmiyorsa null. */
	public Long ageDays(long nowMs) {
		return lastWriteMs == null ? null : Math.max(0, (nowMs - lastWriteMs) / DAY_MS);
	}

	/** Çıktı alanları: lastWriteTime, lastWriteAgeDays (+ eksikse lastWriteIncomplete). */
	public Map<String, Object> lastWriteFields(long nowMs) {
		return Results.map("lastWriteTime", lastWriteMs == null ? null : Format.iso(lastWriteMs), "lastWriteAgeDays",
				ageDays(nowMs), "lastWriteIncomplete", lastWriteMs != null && !lastWriteComplete ? true : null);
	}

	/** Eşik: şimdiden {@code days} gün önce (epoch ms). */
	public static long threshold(long nowMs, int days) {
		return nowMs - days * DAY_MS;
	}

}
