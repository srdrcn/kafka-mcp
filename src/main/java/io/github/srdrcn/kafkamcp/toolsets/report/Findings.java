package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Audit findings: one row per check (check, severity, message, total count) and at most {@code itemLimit}
 * sample items; the number of remaining items is given in {@code itemsOmitted} (no silent truncation).
 */
public final class Findings {

	/** Severity (lower case in the output). */
	public enum Severity {

		CRITICAL, HIGH, MEDIUM, LOW, INFO;

		public String label() {
			return name().toLowerCase(Locale.ROOT);
		}

	}

	private record Check(String check, Severity severity, String message, List<Map<String, Object>> items) {
	}

	private final int itemLimit;

	private final Map<String, Check> checks = new LinkedHashMap<>();

	public Findings(int itemLimit) {
		this.itemLimit = Math.max(1, itemLimit);
	}

	/** Adds a finding; the severity/message of the first call for a check wins. */
	public void add(String check, Severity severity, String message, Map<String, Object> item) {
		checks.computeIfAbsent(check, k -> new Check(check, severity, message, new ArrayList<>())).items().add(item);
	}

	/** Total number of findings (items). */
	public int total() {
		return checks.values().stream().mapToInt(c -> c.items().size()).sum();
	}

	/** check → item count. */
	public Map<String, Integer> counts() {
		Map<String, Integer> out = new LinkedHashMap<>();
		checks.forEach((k, c) -> out.put(k, c.items().size()));
		return out;
	}

	/** Items of a check (empty if none). */
	public List<Map<String, Object>> items(String check) {
		Check c = checks.get(check);
		return c == null ? List.of() : c.items();
	}

	/** Item counts by severity (critical..info, zero counts omitted). */
	public Map<String, Integer> bySeverity() {
		Map<String, Integer> out = new LinkedHashMap<>();
		for (Severity s : Severity.values()) {
			int n = checks.values().stream().filter(c -> c.severity() == s).mapToInt(c -> c.items().size()).sum();
			if (n > 0) {
				out.put(s.label(), n);
			}
		}
		return out;
	}

	/** Finding rows sorted by severity, then by count. */
	public List<Map<String, Object>> render() {
		List<Check> sorted = new ArrayList<>(checks.values());
		sorted.sort(Comparator.comparing(Check::severity)
			.thenComparing(c -> -c.items().size())
			.thenComparing(Check::check));
		List<Map<String, Object>> out = new ArrayList<>();
		for (Check c : sorted) {
			int n = c.items().size();
			List<Map<String, Object>> shown = c.items().subList(0, Math.min(itemLimit, n));
			out.add(Results.map("check", c.check(), "severity", c.severity().label(), "message", c.message(), "count", n,
					"items", shown, "itemsOmitted", n > shown.size() ? n - shown.size() : null));
		}
		return out;
	}

}
