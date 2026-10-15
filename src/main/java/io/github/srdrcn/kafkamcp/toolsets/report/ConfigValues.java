package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.Map;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

import io.github.srdrcn.kafkamcp.kafka.Configs;
import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Reading config values: number/boolean parsing and classifying the source ({@code topic}: topic override,
 * {@code broker}: broker config, {@code default}: Kafka default).
 */
public final class ConfigValues {

	private ConfigValues() {
	}

	/** Numeric value; null if missing or not a number. */
	public static Long longValue(Config c, String key) {
		ConfigEntry e = c == null ? null : c.get(key);
		if (e == null || e.value() == null || e.isSensitive()) {
			return null;
		}
		try {
			return Long.parseLong(e.value().trim());
		}
		catch (NumberFormatException ex) {
			return null;
		}
	}

	/** Decimal value (e.g. min.cleanable.dirty.ratio); null if missing. */
	public static Double doubleValue(Config c, String key) {
		ConfigEntry e = c == null ? null : c.get(key);
		if (e == null || e.value() == null || e.isSensitive()) {
			return null;
		}
		try {
			return Double.parseDouble(e.value().trim());
		}
		catch (NumberFormatException ex) {
			return null;
		}
	}

	/** Is the value "true"? */
	public static boolean isTrue(Config c, String key) {
		ConfigEntry e = c == null ? null : c.get(key);
		return e != null && "true".equalsIgnoreCase(String.valueOf(e.value()).trim());
	}

	/** Text value (masked if sensitive); null if missing. */
	public static String text(Config c, String key) {
		return c == null ? null : Configs.value(c.get(key));
	}

	/** Does cleanup.policy contain compact? */
	public static boolean compacted(Config c) {
		String p = text(c, "cleanup.policy");
		return p != null && p.contains("compact");
	}

	/** Does cleanup.policy contain delete (delete by default if missing)? */
	public static boolean deletes(Config c) {
		String p = text(c, "cleanup.policy");
		return p == null || p.contains("delete");
	}

	/** Source class: topic (override), broker (broker config), default (Kafka default) or unknown. */
	public static String source(ConfigEntry e) {
		if (e == null || e.source() == null) {
			return null;
		}
		return switch (e.source()) {
			case DYNAMIC_TOPIC_CONFIG -> "topic";
			case DYNAMIC_BROKER_CONFIG, DYNAMIC_DEFAULT_BROKER_CONFIG, STATIC_BROKER_CONFIG,
					DYNAMIC_BROKER_LOGGER_CONFIG -> "broker";
			case DEFAULT_CONFIG -> "default";
			default -> "unknown";
		};
	}

	/** Finding item: {topic, config, value, source}. */
	public static Map<String, Object> item(String topic, Config c, String key) {
		ConfigEntry e = c == null ? null : c.get(key);
		return Results.map("topic", topic, "config", key, "value", Configs.value(e), "source", source(e));
	}

}
