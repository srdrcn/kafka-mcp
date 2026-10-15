package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.util.List;
import java.util.Locale;

import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.Separators;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Schema text helpers: schemaType normalization, pretty-printed JSON and truncation of long text. */
public final class SchemaTexts {

	public static final String AVRO = "AVRO";

	public static final String JSON = "JSON";

	public static final String PROTOBUF = "PROTOBUF";

	public static final List<String> TYPES = List.of(AVRO, JSON, PROTOBUF);

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static final ObjectWriter PRETTY;

	static {
		DefaultIndenter indenter = new DefaultIndenter("  ", "\n");
		DefaultPrettyPrinter pp = new DefaultPrettyPrinter(
				Separators.createDefaultInstance().withObjectFieldValueSpacing(Separators.Spacing.AFTER));
		pp.indentObjectsWith(indenter);
		pp.indentArraysWith(indenter);
		PRETTY = MAPPER.writer(pp);
	}

	private SchemaTexts() {
	}

	/** null/blank → AVRO; converts to upper case; error for values other than AVRO/JSON/PROTOBUF. */
	public static String normalizeType(String schemaType) {
		if (schemaType == null || schemaType.isBlank()) {
			return AVRO;
		}
		String t = schemaType.trim().toUpperCase(Locale.ROOT);
		if (!TYPES.contains(t)) {
			throw new ToolException("Invalid schemaType: " + schemaType + " (valid: " + TYPES + ")");
		}
		return t;
	}

	/** schemaType from an SR response: AVRO if absent, otherwise the upper-case value (unknown types are kept). */
	public static String typeOf(String schemaType) {
		return schemaType == null || schemaType.isBlank() ? AVRO : schemaType.trim().toUpperCase(Locale.ROOT);
	}

	/** Pretty-prints AVRO/JSON schemas; PROTOBUF or text that cannot be parsed is returned as is. */
	public static String pretty(String schemaType, String schema) {
		if (schema == null || PROTOBUF.equals(schemaType)) {
			return schema;
		}
		try {
			JsonNode node = MAPPER.readTree(schema);
			return node == null ? schema : PRETTY.writeValueAsString(node);
		}
		catch (Exception e) {
			return schema;
		}
	}

	/** Truncates the text to at most maxChars characters ("…" is appended if it was truncated). */
	public static String truncate(String text, int maxChars) {
		if (text == null || text.length() <= maxChars) {
			return text;
		}
		return text.substring(0, Math.max(0, maxChars)) + "…";
	}

}
