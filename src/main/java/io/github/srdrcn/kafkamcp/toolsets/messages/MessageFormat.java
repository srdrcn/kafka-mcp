package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Message key/value decoding formats; the values of the {@code format}/{@code keyFormat} parameters. */
public enum MessageFormat {

	STRING("string", "UTF-8 text; invalid UTF-8 bytes are shown as hex"),

	JSON("json", "UTF-8 JSON, returned as a parsed object; raw text if invalid"),

	HEX("hex", "Raw bytes as hex"),

	LONG("long", "8-byte big-endian long (Kafka LongSerializer)"),

	AVRO("avro", "Confluent wire format (0x0 + 4-byte schema id + Avro binary); requires schemaRegistryUrl on the cluster");

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

	/** Parses the parameter value; the default if blank, an error if unknown. */
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
		throw new ToolException("Invalid " + param + ": " + value + " (valid: " + String.join("|", ids()) + ")");
	}

}
