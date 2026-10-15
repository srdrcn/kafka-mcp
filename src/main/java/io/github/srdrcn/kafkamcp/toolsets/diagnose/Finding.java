package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.Map;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Teşhis bulgusu: önem, sabit kod (ör. UNDER_MIN_ISR), Türkçe mesaj, küçük kanıt nesnesi ve uygulanabilir
 * Türkçe öneri.
 */
public record Finding(Severity severity, String code, String message, Map<String, Object> evidence,
		String recommendation) {

	/** {severity, code, message, evidence, recommendation}; boş kanıt ve null öneri atlanır. */
	public Map<String, Object> toMap() {
		return Results.map("severity", severity.label(), "code", code, "message", message, "evidence",
				evidence == null || evidence.isEmpty() ? null : evidence, "recommendation", recommendation);
	}

}
