package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Örnek değerlerden yapı çıkarır: JSON yolları (iç içe alanlar nokta, dizi elemanları {@code []}), görülen
 * tipler ve sayıları, eksik/null oranları ve kısaltılmış örnek değer. Kök nesne değilse yol {@code $}'dır.
 */
public final class ShapeInference {

	public static final int DEFAULT_MAX_PATHS = 200;

	/** Dizi başına incelenen en fazla eleman. */
	public static final int MAX_ARRAY_ELEMENTS = 100;

	private static final int EXAMPLE_CHARS = 100;

	private static final int MAX_SKIPPED_TRACKED = 10_000;

	private final int maxPaths;

	private final Map<String, PathStat> paths = new LinkedHashMap<>();

	private final Set<String> skipped = new HashSet<>();

	private int records;

	private boolean arraysSampled;

	public ShapeInference(int maxPaths) {
		this.maxPaths = maxPaths;
	}

	public void add(JsonNode value) {
		records++;
		Set<String> seen = new HashSet<>();
		if (value.isObject()) {
			for (Iterator<Map.Entry<String, JsonNode>> it = value.fields(); it.hasNext();) {
				Map.Entry<String, JsonNode> e = it.next();
				walk(e.getKey(), e.getValue(), seen);
			}
		}
		else if (value.isArray()) {
			elements("", value, seen);
		}
		else {
			walk("$", value, seen);
		}
	}

	private void walk(String path, JsonNode node, Set<String> seen) {
		record(path, node, seen);
		if (node.isObject()) {
			for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext();) {
				Map.Entry<String, JsonNode> e = it.next();
				walk(path + "." + e.getKey(), e.getValue(), seen);
			}
		}
		else if (node.isArray()) {
			elements(path, node, seen);
		}
	}

	private void elements(String path, JsonNode array, Set<String> seen) {
		int i = 0;
		for (JsonNode e : array) {
			if (i++ >= MAX_ARRAY_ELEMENTS) {
				arraysSampled = true;
				break;
			}
			walk(path + "[]", e, seen);
		}
	}

	private void record(String path, JsonNode node, Set<String> seen) {
		PathStat s = paths.get(path);
		if (s == null) {
			if (paths.size() >= maxPaths) {
				if (skipped.size() < MAX_SKIPPED_TRACKED) {
					skipped.add(path);
				}
				return;
			}
			s = new PathStat();
			paths.put(path, s);
		}
		s.occurrences++;
		s.types.merge(type(node), 1, Integer::sum);
		if (node.isNull()) {
			s.nulls++;
		}
		if (seen.add(path)) {
			s.records++;
		}
		if (s.example == null && node.isValueNode() && !node.isNull()) {
			s.example = node.isTextual() ? TextNode.valueOf(RecordDecoder.cut(node.asText(), EXAMPLE_CHARS)) : node;
		}
	}

	static String type(JsonNode n) {
		if (n.isObject()) {
			return "object";
		}
		if (n.isArray()) {
			return "array";
		}
		if (n.isTextual()) {
			return "string";
		}
		if (n.isIntegralNumber()) {
			return "integer";
		}
		if (n.isNumber()) {
			return "number";
		}
		if (n.isBoolean()) {
			return "boolean";
		}
		if (n.isNull()) {
			return "null";
		}
		return n.getNodeType().name().toLowerCase(Locale.ROOT);
	}

	/** İncelenen kayıt sayısı. */
	public int records() {
		return records;
	}

	/** Sınır nedeniyle listelenmeyen farklı yol sayısı. */
	public int skippedPaths() {
		return skipped.size();
	}

	/** Bazı dizilerin sadece ilk {@link #MAX_ARRAY_ELEMENTS} elemanı incelendi mi? */
	public boolean arraysSampled() {
		return arraysSampled;
	}

	/** Yol başına özet: path, types, presentIn, missingRatio, nullRatio, example. */
	public List<Map<String, Object>> paths() {
		List<Map<String, Object>> out = new ArrayList<>();
		paths.forEach((path, s) -> out.add(Results.map("path", path, "types", s.types, "presentIn", s.records,
				"missingRatio", ratio(records - s.records, records), "nullRatio", ratio(s.nulls, s.occurrences),
				"example", s.example)));
		return out;
	}

	private static double ratio(long part, long whole) {
		return whole <= 0 ? 0 : Math.round(part * 1000.0 / whole) / 1000.0;
	}

	private static final class PathStat {

		private final Map<String, Integer> types = new LinkedHashMap<>();

		private int occurrences;

		private int records;

		private int nulls;

		private JsonNode example;

	}

}
