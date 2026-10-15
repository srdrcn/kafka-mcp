package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Schema body for register/compatibility/lookup requests: schema text, schemaType and references. */
public record SchemaInput(String schema, String schemaType, List<SchemaReference> references) {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	/**
	 * Reads the tool arguments ({@code schema}, {@code schemaType}, {@code references}). If {@code schema} is given
	 * as a JSON object, it is converted to text.
	 */
	public static SchemaInput fromCall(ToolCall call) {
		Object raw = call.args().get("schema");
		String schema;
		if (raw instanceof Map<?, ?> || raw instanceof Collection<?>) {
			try {
				schema = MAPPER.writeValueAsString(raw);
			}
			catch (JsonProcessingException e) {
				throw new ToolException("Could not serialize schema to JSON: " + e.getOriginalMessage());
			}
		}
		else {
			schema = call.str("schema");
		}
		return new SchemaInput(schema, SchemaTexts.normalizeType(call.str("schemaType", null)),
				SchemaReference.fromArg(call.args().get("references")));
	}

	/** SR request body; schemaType is not sent for AVRO (compatibility with older SR versions). */
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
