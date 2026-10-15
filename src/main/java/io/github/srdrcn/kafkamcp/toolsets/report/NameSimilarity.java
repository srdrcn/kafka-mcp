package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;

/**
 * Topic name similarity: normalization (lower case, -/_/. unified, environment tokens and trailing version
 * suffixes dropped) and a bounded (banded, early-exit) Levenshtein distance.
 */
public final class NameSimilarity {

	/** Environment tokens dropped during normalization. */
	public static final Set<String> ENV_TOKENS = Set.of("dev", "test", "uat", "prod", "stg");

	/** Maximum number of pairs collected by the similar pair search (memory bound). */
	public static final int MAX_PAIRS = 10_000;

	private static final Pattern SEPARATORS = Pattern.compile("[-_.]+");

	private static final Pattern VERSION = Pattern.compile("v\\d+");

	private static final Pattern DIGITS = Pattern.compile("\\d+");

	private NameSimilarity() {
	}

	/**
	 * Normalized name: lower case, -/_/. become a single separator (.), environment tokens (dev/test/uat/prod/stg)
	 * and trailing version tokens (v1, v2...) are dropped. If nothing is left, the lower-case name is returned.
	 */
	public static String normalize(String name) {
		String lower = name.toLowerCase(Locale.ROOT);
		List<String> kept = new ArrayList<>();
		for (String t : SEPARATORS.split(lower)) {
			if (!t.isEmpty() && !ENV_TOKENS.contains(t)) {
				kept.add(t);
			}
		}
		while (!kept.isEmpty() && VERSION.matcher(kept.get(kept.size() - 1)).matches()) {
			kept.remove(kept.size() - 1);
		}
		return kept.isEmpty() ? lower : String.join(".", kept);
	}

	/** Groups (of at least two) that share a normalized name: normalized name → names (sorted). */
	public static Map<String, List<String>> collisions(Collection<String> names) {
		Map<String, List<String>> all = new TreeMap<>();
		for (String n : names) {
			all.computeIfAbsent(normalize(n), k -> new ArrayList<>()).add(n);
		}
		Map<String, List<String>> out = new LinkedHashMap<>();
		all.forEach((k, v) -> {
			if (v.size() > 1) {
				v.sort(null);
				out.put(k, v);
			}
		});
		return out;
	}

	/** Levenshtein distance; {@code max + 1} if greater than {@code max} (the band only, with early exit). */
	public static int boundedDistance(String a, String b, int max) {
		int n = a.length();
		int m = b.length();
		if (Math.abs(n - m) > max) {
			return max + 1;
		}
		int inf = max + 1;
		int[] prev = new int[m + 1];
		int[] cur = new int[m + 1];
		for (int j = 0; j <= m; j++) {
			prev[j] = j <= max ? j : inf;
		}
		for (int i = 1; i <= n; i++) {
			int from = Math.max(1, i - max);
			int to = Math.min(m, i + max);
			cur[from - 1] = from - 1 == 0 ? i : inf;
			int rowMin = cur[from - 1];
			char ca = a.charAt(i - 1);
			for (int j = from; j <= to; j++) {
				int cost = ca == b.charAt(j - 1) ? 0 : 1;
				int v = Math.min(prev[j - 1] + cost, Math.min(prev[j] + 1, cur[j - 1] + 1));
				cur[j] = Math.min(v, inf);
				rowMin = Math.min(rowMin, cur[j]);
			}
			if (to + 1 <= m) {
				cur[to + 1] = inf;
			}
			if (rowMin > max) {
				return inf;
			}
			int[] t = prev;
			prev = cur;
			cur = t;
		}
		return Math.min(prev[m], inf);
	}

	/** Similar name pair. */
	public record Pair(String a, String b, int distance) {
	}

	/**
	 * Result of the similar pair scan: pairs (by distance, then name), pairs that differ only in digits (separate
	 * list, at most {@link #MAX_PAIRS}; {@code numericVariants} is the total count), the number of comparisons made,
	 * and whether it stopped early because of time ({@code partial}) or the pair limit ({@code capped}).
	 */
	public record Similar(List<Pair> pairs, List<Pair> numericPairs, int numericVariants, long comparisons,
			boolean partial, boolean capped) {
	}

	/**
	 * Pairs with distance at most {@code max} among names at least {@code minLength} long. Names are sorted by
	 * length and only those whose lengths differ by ≤ max are compared; names with the same normalized name (already
	 * in a collision group) are skipped, and pairs that differ only in digits (e.g. shard-01/shard-02,
	 * x.v1.y/x.v2.y) are returned in a separate list. If {@code stop} returns true (deadline), a partial result is
	 * returned.
	 */
	public static Similar similarPairs(Collection<String> names, int max, int minLength, BooleanSupplier stop) {
		List<String> sorted = names.stream()
			.filter(n -> n.length() >= minLength)
			.sorted(Comparator.comparingInt(String::length).thenComparing(Comparator.naturalOrder()))
			.toList();
		List<String> lower = sorted.stream().map(s -> s.toLowerCase(Locale.ROOT)).toList();
		List<String> normalized = sorted.stream().map(NameSimilarity::normalize).toList();
		List<String> skeleton = lower.stream().map(s -> DIGITS.matcher(s).replaceAll("#")).toList();
		List<Pair> pairs = new ArrayList<>();
		List<Pair> numericPairs = new ArrayList<>();
		long comparisons = 0;
		int numeric = 0;
		boolean partial = false;
		boolean capped = false;
		outer: for (int i = 0; i < sorted.size(); i++) {
			String a = sorted.get(i);
			for (int j = i + 1; j < sorted.size(); j++) {
				String b = sorted.get(j);
				if (b.length() - a.length() > max) {
					break;
				}
				if ((++comparisons & 0xFFF) == 0 && stop.getAsBoolean()) {
					partial = true;
					break outer;
				}
				if (normalized.get(i).equals(normalized.get(j))) {
					continue;
				}
				int d = boundedDistance(lower.get(i), lower.get(j), max);
				if (d > max) {
					continue;
				}
				if (skeleton.get(i).equals(skeleton.get(j))) {
					numeric++;
					if (numericPairs.size() < MAX_PAIRS) {
						numericPairs.add(new Pair(a, b, d));
					}
					continue;
				}
				pairs.add(new Pair(a, b, d));
				if (pairs.size() >= MAX_PAIRS) {
					capped = true;
					break outer;
				}
			}
		}
		Comparator<Pair> order = Comparator.comparingInt(Pair::distance).thenComparing(Pair::a).thenComparing(Pair::b);
		pairs.sort(order);
		numericPairs.sort(order);
		return new Similar(pairs, numericPairs, numeric, comparisons, partial, capped);
	}

}
