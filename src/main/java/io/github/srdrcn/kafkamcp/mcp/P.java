package io.github.srdrcn.kafkamcp.mcp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parametre şeması parçaları. Üretilen map'lerde null değer olmaz (Claude Code uyumluluğu).
 */
public final class P {

	private P() {
	}

	public static Map<String, Object> string(String description) {
		return base("string", description);
	}

	public static Map<String, Object> integer(String description) {
		return base("integer", description);
	}

	public static Map<String, Object> integer(String description, long min, long max) {
		Map<String, Object> m = base("integer", description);
		m.put("minimum", min);
		m.put("maximum", max);
		return m;
	}

	public static Map<String, Object> number(String description) {
		return base("number", description);
	}

	public static Map<String, Object> bool(String description) {
		return base("boolean", description);
	}

	public static Map<String, Object> enumOf(String description, String... values) {
		return enumOf(description, Arrays.asList(values));
	}

	public static Map<String, Object> enumOf(String description, List<String> values) {
		Map<String, Object> m = base("string", description);
		m.put("enum", new ArrayList<>(values));
		return m;
	}

	public static Map<String, Object> stringArray(String description) {
		Map<String, Object> m = base("array", description);
		m.put("items", Map.of("type", "string"));
		return m;
	}

	/** Serbest anahtar/değer map'i (değerler string). */
	public static Map<String, Object> stringMap(String description) {
		Map<String, Object> m = base("object", description);
		m.put("additionalProperties", Map.of("type", "string"));
		return m;
	}

	/** Serbest JSON nesnesi. */
	public static Map<String, Object> object(String description) {
		return base("object", description);
	}

	/** Verilen alt şemadan oluşan nesne dizisi. */
	public static Map<String, Object> objectArray(String description, Map<String, Object> itemProperties,
			List<String> itemRequired) {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("type", "object");
		item.put("properties", new LinkedHashMap<>(itemProperties));
		item.put("required", new ArrayList<>(itemRequired));
		Map<String, Object> m = base("array", description);
		m.put("items", item);
		return m;
	}

	private static Map<String, Object> base(String type, String description) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("type", type);
		if (description != null && !description.isBlank()) {
			m.put("description", description);
		}
		return m;
	}

}
