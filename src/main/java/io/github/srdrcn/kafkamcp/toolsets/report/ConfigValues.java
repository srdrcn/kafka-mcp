package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.Map;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

import io.github.srdrcn.kafkamcp.kafka.Configs;
import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Config değerlerini okuma: sayı/bool ayrıştırma ve kaynağın sınıflandırılması ({@code topic}: topic
 * override'ı, {@code broker}: broker config'i, {@code default}: Kafka varsayılanı).
 */
public final class ConfigValues {

	private ConfigValues() {
	}

	/** Sayısal değer; yoksa ya da sayı değilse null. */
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

	/** Ondalık değer (ör. min.cleanable.dirty.ratio); yoksa null. */
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

	/** Değer "true" mu? */
	public static boolean isTrue(Config c, String key) {
		ConfigEntry e = c == null ? null : c.get(key);
		return e != null && "true".equalsIgnoreCase(String.valueOf(e.value()).trim());
	}

	/** Metin değeri (hassas ise maske); yoksa null. */
	public static String text(Config c, String key) {
		return c == null ? null : Configs.value(c.get(key));
	}

	/** cleanup.policy compact içeriyor mu? */
	public static boolean compacted(Config c) {
		String p = text(c, "cleanup.policy");
		return p != null && p.contains("compact");
	}

	/** cleanup.policy delete içeriyor mu (yoksa varsayılan delete)? */
	public static boolean deletes(Config c) {
		String p = text(c, "cleanup.policy");
		return p == null || p.contains("delete");
	}

	/** Kaynak sınıfı: topic (override), broker (broker config'i), default (Kafka varsayılanı) ya da unknown. */
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

	/** Bulgu öğesi: {topic, config, value, source}. */
	public static Map<String, Object> item(String topic, Config c, String key) {
		ConfigEntry e = c == null ? null : c.get(key);
		return Results.map("topic", topic, "config", key, "value", Configs.value(e), "source", source(e));
	}

}
