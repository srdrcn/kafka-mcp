package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Schema Registry schema reference ({@code name, subject, version}); read from a tool argument or an SR response. */
public record SchemaReference(String name, String subject, int version) {

	/** Schema of the {@code references} parameter (shared by the tools). */
	public static Map<String, Object> param() {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("name", P.string("Reference name (full type name for Avro, file/URL name for JSON/PROTOBUF)"));
		item.put("subject", P.string("Referenced subject"));
		item.put("version", P.integer("Referenced version"));
		return P.objectArray("Schema references (optional)", item, List.of("name", "subject", "version"));
	}

	/** Reference list from the tool argument; empty list if absent. Error if a field is missing. */
	public static List<SchemaReference> fromArg(Object arg) {
		List<SchemaReference> out = new ArrayList<>();
		if (arg == null) {
			return out;
		}
		if (!(arg instanceof Collection<?> c)) {
			throw new ToolException("references must be an array: [{name, subject, version}]");
		}
		for (Object o : c) {
			if (!(o instanceof Map<?, ?> m)) {
				throw new ToolException("references items must be objects: {name, subject, version}");
			}
			Object name = m.get("name");
			Object subject = m.get("subject");
			Object version = m.get("version");
			if (name == null || subject == null || version == null || String.valueOf(name).isBlank()
					|| String.valueOf(subject).isBlank()) {
				throw new ToolException("Each reference requires name, subject and version");
			}
			int v;
			try {
				v = version instanceof Number n ? n.intValue() : Integer.parseInt(String.valueOf(version).trim());
			}
			catch (NumberFormatException e) {
				throw new ToolException("Reference version must be an integer: " + version);
			}
			out.add(new SchemaReference(String.valueOf(name).trim(), String.valueOf(subject).trim(), v));
		}
		return out;
	}

	/** The {@code references} array of an SR response; empty list if absent. */
	public static List<SchemaReference> fromJson(JsonNode node) {
		List<SchemaReference> out = new ArrayList<>();
		if (node != null && node.isArray()) {
			for (JsonNode r : node) {
				out.add(new SchemaReference(r.path("name").asText(), r.path("subject").asText(),
						r.path("version").asInt()));
			}
		}
		return out;
	}

	/** For the SR request body. */
	public Map<String, Object> toMap() {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("name", name);
		m.put("subject", subject);
		m.put("version", version);
		return m;
	}

}
