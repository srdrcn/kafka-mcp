package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.Map;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Diagnosis finding: severity, fixed code (e.g. UNDER_MIN_ISR), message, small evidence object and an
 * actionable recommendation.
 */
public record Finding(Severity severity, String code, String message, Map<String, Object> evidence,
		String recommendation) {

	/** {severity, code, message, evidence, recommendation}; empty evidence and a null recommendation are omitted. */
	public Map<String, Object> toMap() {
		return Results.map("severity", severity.label(), "code", code, "message", message, "evidence",
				evidence == null || evidence.isEmpty() ? null : evidence, "recommendation", recommendation);
	}

}
