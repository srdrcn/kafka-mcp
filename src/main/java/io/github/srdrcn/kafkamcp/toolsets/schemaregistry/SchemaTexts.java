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

/** Şema metni yardımcıları: schemaType normalizasyonu, okunaklı (pretty) JSON ve uzun metin kesme. */
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

	/** null/boş → AVRO; büyük harfe çevirir; AVRO/JSON/PROTOBUF dışı değerlerde Türkçe hata. */
	public static String normalizeType(String schemaType) {
		if (schemaType == null || schemaType.isBlank()) {
			return AVRO;
		}
		String t = schemaType.trim().toUpperCase(Locale.ROOT);
		if (!TYPES.contains(t)) {
			throw new ToolException("Geçersiz schemaType: " + schemaType + " (geçerli: " + TYPES + ")");
		}
		return t;
	}

	/** SR yanıtındaki schemaType: yoksa AVRO, varsa büyük harfli değer (bilinmeyen tipler de olduğu gibi kalır). */
	public static String typeOf(String schemaType) {
		return schemaType == null || schemaType.isBlank() ? AVRO : schemaType.trim().toUpperCase(Locale.ROOT);
	}

	/** AVRO/JSON şemalarını girintili JSON'a çevirir; PROTOBUF ya da parse edilemeyen metin olduğu gibi döner. */
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

	/** Metni en fazla maxChars karaktere keser (kesildiyse sonuna "…" eklenir). */
	public static String truncate(String text, int maxChars) {
		if (text == null || text.length() <= maxChars) {
			return text;
		}
		return text.substring(0, Math.max(0, maxChars)) + "…";
	}

}
