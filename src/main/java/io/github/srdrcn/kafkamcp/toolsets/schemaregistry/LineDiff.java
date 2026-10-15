package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * İki metin arasında kompakt satır farkı: ortak baş/son atılır, kalan kısım LCS ile hizalanır ({@code "-12: ..."} eski
 * satır, {@code "+13: ..."} yeni satır). Çok büyük metinlerde sırasız çoklu-küme farkına düşer ({@code exact=false}).
 */
public final class LineDiff {

	/** LCS tablosu için üst sınır (hücre); üstünde çoklu-küme farkı kullanılır. */
	static final long MAX_LCS_CELLS = 4_000_000L;

	private static final int MAX_LINE_CHARS = 200;

	private LineDiff() {
	}

	/**
	 * @param added eklenen satır sayısı
	 * @param removed silinen satır sayısı
	 * @param exact false ise satırlar sırasız (çok büyük metin) karşılaştırıldı
	 * @param lines en fazla maxLines fark satırı
	 * @param truncated fark satırları kesildi mi
	 */
	public record Result(int added, int removed, boolean exact, List<String> lines, boolean truncated) {

		public boolean identical() {
			return added == 0 && removed == 0;
		}

		public Map<String, Object> toMap() {
			Map<String, Object> m = Results.map("addedLines", added, "removedLines", removed, "lines", lines);
			if (truncated) {
				m.put("truncated", true);
				m.put("omittedLines", added + removed - lines.size());
			}
			if (!exact) {
				m.put("exact", false);
				m.put("note", "Metin çok büyük; satırlar sırasız (küme farkı) karşılaştırıldı");
			}
			return m;
		}

	}

	public static Result diff(String from, String to, int maxLines) {
		String[] a = split(from);
		String[] b = split(to);
		int prefix = 0;
		while (prefix < a.length && prefix < b.length && a[prefix].equals(b[prefix])) {
			prefix++;
		}
		int suffix = 0;
		while (suffix < a.length - prefix && suffix < b.length - prefix
				&& a[a.length - 1 - suffix].equals(b[b.length - 1 - suffix])) {
			suffix++;
		}
		int n = a.length - prefix - suffix;
		int m = b.length - prefix - suffix;
		Collector out = new Collector(maxLines);
		if ((long) (n + 1) * (m + 1) <= MAX_LCS_CELLS) {
			lcs(a, b, prefix, n, m, out);
			return new Result(out.added, out.removed, true, out.lines, out.truncated);
		}
		multiset(a, b, prefix, n, m, out);
		return new Result(out.added, out.removed, false, out.lines, out.truncated);
	}

	private static void lcs(String[] a, String[] b, int off, int n, int m, Collector out) {
		int[][] len = new int[n + 1][m + 1];
		for (int i = n - 1; i >= 0; i--) {
			for (int j = m - 1; j >= 0; j--) {
				len[i][j] = a[off + i].equals(b[off + j]) ? len[i + 1][j + 1] + 1
						: Math.max(len[i + 1][j], len[i][j + 1]);
			}
		}
		int i = 0;
		int j = 0;
		while (i < n || j < m) {
			if (i < n && j < m && a[off + i].equals(b[off + j])) {
				i++;
				j++;
			}
			else if (i < n && (j == m || len[i + 1][j] >= len[i][j + 1])) {
				out.add('-', off + i + 1, a[off + i]);
				i++;
			}
			else {
				out.add('+', off + j + 1, b[off + j]);
				j++;
			}
		}
	}

	private static void multiset(String[] a, String[] b, int off, int n, int m, Collector out) {
		Map<String, Integer> counts = new HashMap<>();
		for (int j = 0; j < m; j++) {
			counts.merge(b[off + j], 1, Integer::sum);
		}
		for (int i = 0; i < n; i++) {
			String line = a[off + i];
			Integer c = counts.get(line);
			if (c == null || c == 0) {
				out.add('-', off + i + 1, line);
			}
			else {
				counts.put(line, c - 1);
			}
		}
		Map<String, Integer> remaining = new HashMap<>();
		for (int i = 0; i < n; i++) {
			remaining.merge(a[off + i], 1, Integer::sum);
		}
		for (int j = 0; j < m; j++) {
			String line = b[off + j];
			Integer c = remaining.get(line);
			if (c == null || c == 0) {
				out.add('+', off + j + 1, line);
			}
			else {
				remaining.put(line, c - 1);
			}
		}
	}

	private static String[] split(String text) {
		if (text == null || text.isEmpty()) {
			return new String[0];
		}
		return text.replace("\r\n", "\n").split("\n", -1);
	}

	private static final class Collector {

		private final int max;

		private final List<String> lines = new ArrayList<>();

		private int added;

		private int removed;

		private boolean truncated;

		Collector(int max) {
			this.max = max;
		}

		void add(char sign, int lineNo, String text) {
			if (sign == '+') {
				added++;
			}
			else {
				removed++;
			}
			if (lines.size() >= max) {
				truncated = true;
				return;
			}
			String t = text.length() > MAX_LINE_CHARS ? text.substring(0, MAX_LINE_CHARS) + "…" : text;
			lines.add(sign + "" + lineNo + ": " + t);
		}

	}

}
