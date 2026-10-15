package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Kayıt/uyumluluk/lookup istekleri için şema gövdesi: şema metni, schemaType ve referanslar. */
public record SchemaInput(String schema, String schemaType, List<SchemaReference> references) {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	/**
	 * Araç argümanlarından ({@code schema}, {@code schemaType}, {@code references}) okur. {@code schema} JSON
	 * nesnesi olarak verilmişse metne çevrilir.
	 */
	public static SchemaInput fromCall(ToolCall call) {
		Object raw = call.args().get("schema");
		String schema;
		if (raw instanceof Map<?, ?> || raw instanceof Collection<?>) {
			try {
				schema = MAPPER.writeValueAsString(raw);
			}
			catch (JsonProcessingException e) {
				throw new ToolException("schema JSON'a çevrilemedi: " + e.getOriginalMessage());
			}
		}
		else {
			schema = call.str("schema");
		}
		return new SchemaInput(schema, SchemaTexts.normalizeType(call.str("schemaType", null)),
				SchemaReference.fromArg(call.args().get("references")));
	}

	/** SR istek gövdesi; AVRO için schemaType gönderilmez (eski SR sürümleriyle uyum). */
	public Map<String, Object> toBody() {
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("schema", schema);
		if (!SchemaTexts.AVRO.equals(schemaType)) {
			body.put("schemaType", schemaType);
		}
		if (!references.isEmpty()) {
			body.put("references", references.stream().map(SchemaReference::toMap).toList());
		}
		return body;
	}

}
