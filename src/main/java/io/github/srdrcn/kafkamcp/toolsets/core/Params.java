package io.github.srdrcn.kafkamcp.toolsets.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Shared parameter and result helpers of the core tools. */
final class Params {

	private Params() {
	}

	/** Integer array schema. */
	static Map<String, Object> integerArray(String description) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("type", "array");
		m.put("description", description);
		m.put("items", Map.of("type", "integer"));
		return m;
	}

	/** Only the limit parameter (for list tools without a filter). */
	static Map<String, Object> limit(int defaultLimit) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("type", "integer");
		m.put("description", "Maximum number of items to return (default " + defaultLimit + ")");
		m.put("minimum", 1);
		m.put("maximum", ToolSpec.MAX_LIST_LIMIT);
		m.put("default", defaultLimit);
		return m;
	}

	/** Integer list parameter; an array or a comma-separated string. Empty list if absent. */
	static List<Integer> ints(ToolCall call, String name) {
		return intList(call.args().get(name), name);
	}

	/**
	 * Converts a JSON value to an integer list: an array, a single number, "2,3" or the JSON text "[2,3]". Empty
	 * list if null or empty; a non-integer element (null included) is an error, never silently skipped.
	 */
	static List<Integer> intList(Object v, String name) {
		List<Integer> out = new ArrayList<>();
		if (v instanceof Collection<?> c) {
			for (Object o : c) {
				out.add(toInt(o, name));
			}
		}
		else if (v instanceof Number) {
			out.add(toInt(v, name));
		}
		else if (v != null) {
			String text = String.valueOf(v).trim();
			if (text.startsWith("[") && text.endsWith("]")) {
				text = text.substring(1, text.length() - 1);
			}
			for (String s : text.split(",")) {
				if (!s.isBlank()) {
					out.add(toInt(s, name));
				}
			}
		}
		return out;
	}

	/** Converts a JSON value to an integer; throws a readable error otherwise. */
	static int toInt(Object v, String name) {
		if (v instanceof Number n && n.doubleValue() == Math.rint(n.doubleValue())) {
			return n.intValue();
		}
		try {
			return Integer.parseInt(String.valueOf(v).trim());
		}
		catch (NumberFormatException e) {
			throw new ToolException("Expected an integer: " + name + "=" + v);
		}
	}

	/** Result in the Results.page format for rows already cut to the limit. */
	static Map<String, Object> page(String key, List<?> rows, int total) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("total", total);
		m.put("returned", rows.size());
		m.put("truncated", total > rows.size());
		m.put(key, rows);
		return m;
	}

}
