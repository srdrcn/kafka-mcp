package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Compact line diff between two texts: the common head/tail is skipped and the rest is aligned with LCS
 * ({@code "-12: ..."} old line, {@code "+13: ..."} new line). Very large texts fall back to an unordered multiset diff
 * ({@code exact=false}).
 */
public final class LineDiff {

	/** Upper bound for the LCS table (cells); above it the multiset diff is used. */
	static final long MAX_LCS_CELLS = 4_000_000L;

	private static final int MAX_LINE_CHARS = 200;

	private LineDiff() {
	}

	/**
	 * @param added number of added lines
	 * @param removed number of removed lines
	 * @param exact if false, the lines were compared unordered (very large text)
	 * @param lines at most maxLines diff lines
	 * @param truncated whether the diff lines were truncated
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
				m.put("note", "Text too large; lines were compared unordered (multiset diff)");
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
