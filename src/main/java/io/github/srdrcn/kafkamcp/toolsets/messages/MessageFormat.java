package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Mesaj key/value çözme formatları; {@code format}/{@code keyFormat} parametrelerinin değerleri. */
public enum MessageFormat {

	STRING("string", "UTF-8 metin; geçersiz UTF-8 baytlar hex gösterilir"),

	JSON("json", "UTF-8 JSON, ayrıştırılmış nesne olarak; geçersizse ham metin döner"),

	HEX("hex", "Ham baytlar hex olarak"),

	LONG("long", "8 baytlık big-endian long (Kafka LongSerializer)"),

	AVRO("avro", "Confluent wire format (0x0 + 4 bayt şema id + Avro binary); cluster'da schemaRegistryUrl gerekir");

	private final String id;

	private final String description;

	MessageFormat(String id, String description) {
		this.id = id;
		this.description = description;
	}

	public String id() {
		return id;
	}

	public String description() {
		return description;
	}

	public boolean needsSchemaRegistry() {
		return this == AVRO;
	}

	public static List<String> ids() {
		return Arrays.stream(values()).map(MessageFormat::id).toList();
	}

	/** Parametre değerini çözer; boşsa varsayılan, bilinmiyorsa Türkçe hata. */
	public static MessageFormat parse(String param, String value, MessageFormat defaultValue) {
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		String v = value.trim().toLowerCase(Locale.ROOT);
		for (MessageFormat f : values()) {
			if (f.id.equals(v)) {
				return f;
			}
		}
		throw new ToolException("Geçersiz " + param + ": " + value + " (geçerli: " + String.join("|", ids()) + ")");
	}

}
