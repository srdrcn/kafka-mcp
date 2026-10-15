package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

/** A subject version (GET /subjects/{s}/versions/{v} response); AVRO if schemaType is absent. */
public record SchemaVersion(String subject, int version, int id, String schemaType, List<SchemaReference> references,
		String schema) {

	public static SchemaVersion fromJson(JsonNode n) {
		return new SchemaVersion(n.path("subject").asText(null), n.path("version").asInt(), n.path("id").asInt(),
				SchemaTexts.typeOf(n.path("schemaType").asText(null)),
				SchemaReference.fromJson(n.get("references")), n.path("schema").asText(""));
	}

	public boolean isAvro() {
		return SchemaTexts.AVRO.equals(schemaType);
	}

}
