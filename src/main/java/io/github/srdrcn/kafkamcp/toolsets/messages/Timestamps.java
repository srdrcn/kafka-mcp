package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Zaman parametreleri (ISO-8601 ya da epoch ms) ve ISO-8601 UTC çıktı. */
public final class Timestamps {

	private Timestamps() {
	}

	/** Zorunlu zaman parametresi (epoch ms). */
	public static long required(ToolCall call, String name) {
		return parse(name, call.str(name));
	}

	/** Opsiyonel zaman parametresi (epoch ms) ya da null. */
	public static Long optional(ToolCall call, String name) {
		String v = call.str(name, null);
		return v == null ? null : parse(name, v);
	}

	/**
	 * ISO-8601 (2025-01-31T10:00:00Z, +03:00 ofsetli, ofsetsiz = UTC, sadece tarih = gün başı UTC) ya da epoch ms.
	 */
	public static long parse(String name, String value) {
		String v = value.trim();
		if (v.matches("-?\\d+")) {
			try {
				return Long.parseLong(v);
			}
			catch (NumberFormatException e) {
				throw invalid(name, value);
			}
		}
		String iso = v.length() > 10 && v.charAt(10) == ' ' ? v.substring(0, 10) + 'T' + v.substring(11) : v;
		try {
			return Instant.parse(iso).toEpochMilli();
		}
		catch (DateTimeParseException ignored) {
			// sıradaki biçim
		}
		try {
			return OffsetDateTime.parse(iso).toInstant().toEpochMilli();
		}
		catch (DateTimeParseException ignored) {
			// sıradaki biçim
		}
		try {
			return ZonedDateTime.parse(iso).toInstant().toEpochMilli();
		}
		catch (DateTimeParseException ignored) {
			// sıradaki biçim
		}
		try {
			return LocalDateTime.parse(iso).toInstant(ZoneOffset.UTC).toEpochMilli();
		}
		catch (DateTimeParseException ignored) {
			// sıradaki biçim
		}
		try {
			return LocalDate.parse(iso).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
		}
		catch (DateTimeParseException e) {
			throw invalid(name, value);
		}
	}

	/** Epoch ms → ISO-8601 UTC; negatifse (zaman yok) null. */
	public static String iso(long epochMillis) {
		return epochMillis < 0 ? null : Instant.ofEpochMilli(epochMillis).toString();
	}

	private static ToolException invalid(String name, String value) {
		return new ToolException("Geçersiz zaman (" + name + "): " + value
				+ " (ISO-8601 ör. 2025-01-31T10:00:00Z ya da epoch milisaniye)");
	}

}
