package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Teşhis sonucunu toplayan (thread-safe) yardımcı: bulgular, kısmi sonuç bayrağı ve standart çıktı
 * {@code {status, summary, findings, facts, partial}}. status: bulgulardaki en yüksek önem (critical,
 * warning) ya da ok; bulgular önem sırasına göre (aynı önemde koda göre) sıralanır.
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

	/** Bir kontrol eksik kaldı (hata/zaman aşımı/örnekleme): çıktıda {@code "partial": true}. */
	public synchronized void markPartial() {
		partial = true;
	}

	public synchronized boolean partial() {
		return partial;
	}

	/** Önem sırasına göre sıralı kopya. */
	public synchronized List<Finding> findings() {
		List<Finding> out = new ArrayList<>(findings);
		out.sort(ORDER);
		return out;
	}

	/** Bu kodla bir bulgu var mı? */
	public synchronized boolean has(String code) {
		return findings.stream().anyMatch(f -> f.code().equals(code));
	}

	/** ok | warning | critical (en yüksek önem; info bulguları durumu etkilemez). */
	public synchronized String status() {
		if (findings.stream().anyMatch(f -> f.severity() == Severity.CRITICAL)) {
			return CRITICAL;
		}
		if (findings.stream().anyMatch(f -> f.severity() == Severity.WARNING)) {
			return WARNING;
		}
		return OK;
	}

	/** Kısa Türkçe özet: "1 kritik, 2 uyarı, 1 bilgi bulgusu" ya da "Sorun bulunmadı". */
	public synchronized String summary() {
		long c = count(Severity.CRITICAL);
		long w = count(Severity.WARNING);
		long i = count(Severity.INFO);
		if (c == 0 && w == 0) {
			return i == 0 ? "Sorun bulunmadı" : "Sorun bulunmadı (" + i + " bilgi notu)";
		}
		List<String> parts = new ArrayList<>();
		if (c > 0) {
			parts.add(c + " kritik");
		}
		if (w > 0) {
			parts.add(w + " uyarı");
		}
		if (i > 0) {
			parts.add(i + " bilgi");
		}
		return String.join(", ", parts) + " bulgusu";
	}

	private long count(Severity s) {
		return findings.stream().filter(f -> f.severity() == s).count();
	}

	/**
	 * Standart çıktı: head alanları (ör. topic, cluster), status, summary, findings, facts ve gerekirse partial.
	 * extra (null olabilir) facts'ten önce eklenir.
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

	/** {@link #result(Map, Map, Map)}; ek alan olmadan. */
	public Map<String, Object> result(Map<String, Object> head, Map<String, Object> facts) {
		return result(head, null, facts);
	}

	/** Küçük kanıt nesnesi kısayolu ({@link Results#map}; null değerler atlanır). */
	public static Map<String, Object> evidence(Object... keyValues) {
		return Results.map(keyValues);
	}

}
