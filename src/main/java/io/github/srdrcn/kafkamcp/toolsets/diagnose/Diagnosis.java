package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Thread-safe collector of a diagnosis result: findings, the partial-result flag and the standard output
 * {@code {status, summary, findings, facts, partial}}. status: the highest severity among the findings (critical,
 * warning) or ok; findings are sorted by severity (and by code within the same severity).
 */
public final class Diagnosis {

	public static final String OK = "ok";

	public static final String WARNING = "warning";

	public static final String CRITICAL = "critical";

	private static final Comparator<Finding> ORDER = Comparator.comparing(Finding::severity)
		.thenComparing(Finding::code);

	private final List<Finding> findings = new ArrayList<>();

	private boolean partial;

	public synchronized Diagnosis add(Finding f) {
		findings.add(f);
		return this;
	}

	public Diagnosis critical(String code, String message, Map<String, Object> evidence, String recommendation) {
		return add(new Finding(Severity.CRITICAL, code, message, evidence, recommendation));
	}

	public Diagnosis warning(String code, String message, Map<String, Object> evidence, String recommendation) {
		return add(new Finding(Severity.WARNING, code, message, evidence, recommendation));
	}

	public Diagnosis info(String code, String message, Map<String, Object> evidence, String recommendation) {
		return add(new Finding(Severity.INFO, code, message, evidence, recommendation));
	}

	/** A check was incomplete (error/timeout/sampling): {@code "partial": true} in the output. */
	public synchronized void markPartial() {
		partial = true;
	}

	public synchronized boolean partial() {
		return partial;
	}

	/** Copy sorted by severity. */
	public synchronized List<Finding> findings() {
		List<Finding> out = new ArrayList<>(findings);
		out.sort(ORDER);
		return out;
	}

	/** Is there a finding with this code? */
	public synchronized boolean has(String code) {
		return findings.stream().anyMatch(f -> f.code().equals(code));
	}

	/** ok | warning | critical (highest severity; info findings do not affect the status). */
	public synchronized String status() {
		if (findings.stream().anyMatch(f -> f.severity() == Severity.CRITICAL)) {
			return CRITICAL;
		}
		if (findings.stream().anyMatch(f -> f.severity() == Severity.WARNING)) {
			return WARNING;
		}
		return OK;
	}

	/** Short summary: "Findings: 1 critical, 2 warning, 1 info" or "No issues found". */
	public synchronized String summary() {
		long c = count(Severity.CRITICAL);
		long w = count(Severity.WARNING);
		long i = count(Severity.INFO);
		if (c == 0 && w == 0) {
			return i == 0 ? "No issues found" : "No issues found (" + i + (i == 1 ? " info note)" : " info notes)");
		}
		List<String> parts = new ArrayList<>();
		if (c > 0) {
			parts.add(c + " critical");
		}
		if (w > 0) {
			parts.add(w + " warning");
		}
		if (i > 0) {
			parts.add(i + " info");
		}
		return "Findings: " + String.join(", ", parts);
	}

	private long count(Severity s) {
		return findings.stream().filter(f -> f.severity() == s).count();
	}

	/**
	 * Standard output: head fields (e.g. topic, cluster), status, summary, findings, facts and partial if needed.
	 * extra (may be null) is added before facts.
	 */
	public Map<String, Object> result(Map<String, Object> head, Map<String, Object> extra, Map<String, Object> facts) {
		Map<String, Object> out = new LinkedHashMap<>();
		if (head != null) {
			out.putAll(head);
		}
		out.put("status", status());
		out.put("summary", summary());
		out.put("findings", findings().stream().map(Finding::toMap).toList());
		if (extra != null) {
			out.putAll(extra);
		}
		out.put("facts", facts == null ? Map.of() : facts);
		if (partial()) {
			out.put("partial", true);
		}
		return out;
	}

	/** {@link #result(Map, Map, Map)} without extra fields. */
	public Map<String, Object> result(Map<String, Object> head, Map<String, Object> facts) {
		return result(head, null, facts);
	}

	/** Shortcut for a small evidence object ({@link Results#map}; null values are omitted). */
	public static Map<String, Object> evidence(Object... keyValues) {
		return Results.map(keyValues);
	}

}
