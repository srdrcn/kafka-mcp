package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Connect REST JSON helpers: JsonNode → Java value, short class name and stack trace summary. */
public final class ConnectJson {

	/** Maximum number of lines kept in traceSummary. */
	public static final int TRACE_LINES = 3;

	private static final int TRACE_LINE_MAX = 300;

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private ConnectJson() {
	}

	/** Converts a JsonNode to a Map/List/primitive; null/missing → null. */
	public static Object toJava(JsonNode node) {
		if (node == null || node.isNull() || node.isMissingNode()) {
			return null;
		}
		return MAPPER.convertValue(node, Object.class);
	}

	/** Text value of the field; null if absent or null. */
	public static String text(JsonNode node, String field) {
		JsonNode v = node == null ? null : node.get(field);
		return v == null || v.isNull() || v.isMissingNode() ? null : v.asText();
	}

	/** Converts a JSON object to a string map sorted by key (for configs). */
	public static Map<String, String> stringMap(JsonNode node) {
		Map<String, String> out = new TreeMap<>();
		if (node != null && node.isObject()) {
			node.properties().forEach(e -> out.put(e.getKey(), e.getValue().isNull() ? null : e.getValue().asText()));
		}
		return out;
	}

	/** {@code org.apache.kafka.connect.file.FileStreamSinkConnector} → {@code FileStreamSinkConnector}. */
	public static String shortClass(String className) {
		if (className == null) {
			return null;
		}
		return className.substring(className.lastIndexOf('.') + 1);
	}

	/**
	 * First {@value #TRACE_LINES} non-blank lines of the stack trace (each at most 300 characters, credentials removed
	 * with {@link ConnectSecrets#scrub(String)}); null if none.
	 */
	public static String traceSummary(String trace) {
		if (trace == null || trace.isBlank()) {
			return null;
		}
		List<String> lines = new ArrayList<>();
		for (String line : trace.split("\\R")) {
			String l = line.strip();
			if (l.isEmpty()) {
				continue;
			}
			// credentials are removed before the cut, so a cut never leaves part of one
			l = ConnectSecrets.scrub(l.length() > 4 * TRACE_LINE_MAX ? l.substring(0, 4 * TRACE_LINE_MAX) : l);
			lines.add(l.length() > TRACE_LINE_MAX ? l.substring(0, TRACE_LINE_MAX) + "…" : l);
			if (lines.size() == TRACE_LINES) {
				break;
			}
		}
		return String.join("\n", lines);
	}

}
