package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Connect REST JSON yardımcıları: JsonNode → Java değeri, kısa sınıf adı ve stack trace özeti. */
public final class ConnectJson {

	/** traceSummary'de tutulan en fazla satır. */
	public static final int TRACE_LINES = 3;

	private static final int TRACE_LINE_MAX = 300;

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private ConnectJson() {
	}

	/** JsonNode'u Map/List/primitive'e çevirir; null/eksik → null. */
	public static Object toJava(JsonNode node) {
		if (node == null || node.isNull() || node.isMissingNode()) {
			return null;
		}
		return MAPPER.convertValue(node, Object.class);
	}

	/** Alanın metin değeri; yoksa ya da null ise null. */
	public static String text(JsonNode node, String field) {
		JsonNode v = node == null ? null : node.get(field);
		return v == null || v.isNull() || v.isMissingNode() ? null : v.asText();
	}

	/** JSON nesnesini anahtara göre sıralı string map'e çevirir (config'ler için). */
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

	/** Stack trace'in ilk {@value #TRACE_LINES} dolu satırı (her biri en fazla 300 karakter); yoksa null. */
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
			lines.add(l.length() > TRACE_LINE_MAX ? l.substring(0, TRACE_LINE_MAX) + "…" : l);
			if (lines.size() == TRACE_LINES) {
				break;
			}
		}
		return String.join("\n", lines);
	}

}
