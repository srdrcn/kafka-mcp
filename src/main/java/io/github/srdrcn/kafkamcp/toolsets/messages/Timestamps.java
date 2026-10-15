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

/** Time parameters (ISO-8601 or epoch ms) and ISO-8601 UTC output. */
public final class Timestamps {

	private Timestamps() {
	}

	/** Required time parameter (epoch ms). */
	public static long required(ToolCall call, String name) {
		return parse(name, call.str(name));
	}

	/** Optional time parameter (epoch ms), or null. */
	public static Long optional(ToolCall call, String name) {
		String v = call.str(name, null);
		return v == null ? null : parse(name, v);
	}

	/**
	 * ISO-8601 (2025-01-31T10:00:00Z, with an offset such as +03:00, without an offset = UTC, date only = start of
	 * day UTC) or epoch ms. Negative values (before 1970) are rejected: in listOffsets -1/-2/-3... have special
	 * meanings such as LATEST/EARLIEST/MAX_TIMESTAMP and give different results depending on the broker version (or
	 * an UnsupportedVersionException on an old broker).
	 */
	public static long parse(String name, String value) {
		long ms = parseAny(name, value);
		if (ms < 0) {
			throw new ToolException("Invalid time (" + name + "): " + value
					+ " — time cannot be negative/before 1970 (epoch milliseconds >= 0)");
		}
		return ms;
	}

	private static long parseAny(String name, String value) {
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
			// try the next format
		}
		try {
			return OffsetDateTime.parse(iso).toInstant().toEpochMilli();
		}
		catch (DateTimeParseException ignored) {
			// try the next format
		}
		try {
			return ZonedDateTime.parse(iso).toInstant().toEpochMilli();
		}
		catch (DateTimeParseException ignored) {
			// try the next format
		}
		try {
			return LocalDateTime.parse(iso).toInstant(ZoneOffset.UTC).toEpochMilli();
		}
		catch (DateTimeParseException ignored) {
			// try the next format
		}
		try {
			return LocalDate.parse(iso).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
		}
		catch (DateTimeParseException e) {
			throw invalid(name, value);
		}
	}

	/** Epoch ms → ISO-8601 UTC; null if negative (no timestamp). */
	public static String iso(long epochMillis) {
		return epochMillis < 0 ? null : Instant.ofEpochMilli(epochMillis).toString();
	}

	private static ToolException invalid(String name, String value) {
		return new ToolException("Invalid time (" + name + "): " + value
				+ " (use ISO-8601, e.g. 2025-01-31T10:00:00Z, or epoch milliseconds)");
	}

}
