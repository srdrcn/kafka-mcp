package io.github.srdrcn.kafkamcp.toolsets.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** core araçlarının ortak parametre/sonuç parçaları. */
final class Params {

	private Params() {
	}

	/** Tamsayı dizisi şeması. */
	static Map<String, Object> integerArray(String description) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("type", "array");
		m.put("description", description);
		m.put("items", Map.of("type", "integer"));
		return m;
	}

	/** Sadece limit parametresi (filter'sız liste araçları için). */
	static Map<String, Object> limit(int defaultLimit) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("type", "integer");
		m.put("description", "En fazla kaç kayıt dönsün (varsayılan " + defaultLimit + ")");
		m.put("minimum", 1);
		m.put("maximum", ToolSpec.MAX_LIST_LIMIT);
		m.put("default", defaultLimit);
		return m;
	}

	/** Tamsayı listesi parametresi; dizi ya da virgülle ayrılmış string. Yoksa boş liste. */
	static List<Integer> ints(ToolCall call, String name) {
		return intList(call.args().get(name), name);
	}

	/**
	 * JSON değerini tamsayı listesine çevirir: dizi, tek sayı, "2,3" ya da JSON metni "[2,3]". null ya da boş
	 * ise boş liste; tamsayı olmayan (null dahil) eleman Türkçe hata, sessizce atlanmaz.
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

	/** JSON değerini tamsayıya çevirir; olmazsa Türkçe hata. */
	static int toInt(Object v, String name) {
		if (v instanceof Number n && n.doubleValue() == Math.rint(n.doubleValue())) {
			return n.intValue();
		}
		try {
			return Integer.parseInt(String.valueOf(v).trim());
		}
		catch (NumberFormatException e) {
			throw new ToolException("Tamsayı bekleniyordu: " + name + "=" + v);
		}
	}

	/** Zaten limit'e göre kesilmiş satırlar için Results.page biçiminde sonuç. */
	static Map<String, Object> page(String key, List<?> rows, int total) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("total", total);
		m.put("returned", rows.size());
		m.put("truncated", total > rows.size());
		m.put(key, rows);
		return m;
	}

}
