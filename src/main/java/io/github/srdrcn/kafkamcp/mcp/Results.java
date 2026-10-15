package io.github.srdrcn.kafkamcp.mcp;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Small helpers for tool results. */
public final class Results {

	private Results() {
	}

	/**
	 * Truncates the list to the limit: {@code {"total": N, "returned": M, "truncated": bool, "<key>": [...]}}.
	 */
	public static Map<String, Object> page(String key, Collection<?> items, int limit) {
		List<?> list = items instanceof List<?> l ? l : List.copyOf(items);
		int n = Math.min(limit, list.size());
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("total", list.size());
		m.put("returned", n);
		m.put("truncated", list.size() > n);
		m.put(key, list.subList(0, n));
		return m;
	}

	/** Ordered map; null values are skipped. {@code Results.map("a", 1, "b", null)} → {a:1} */
	public static Map<String, Object> map(Object... keyValues) {
		if (keyValues.length % 2 != 0) {
			throw new IllegalArgumentException("an even number of arguments is required");
		}
		Map<String, Object> m = new LinkedHashMap<>();
		for (int i = 0; i < keyValues.length; i += 2) {
			if (keyValues[i + 1] != null) {
				m.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
			}
		}
		return m;
	}

}
