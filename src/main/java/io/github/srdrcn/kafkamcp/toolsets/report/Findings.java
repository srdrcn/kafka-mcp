package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Denetim bulguları: kontrol başına tek satır (check, severity, açıklama, toplam sayı) ve en fazla
 * {@code itemLimit} örnek; kalan örnek sayısı {@code itemsOmitted} ile belirtilir (sessiz kırpma yok).
 */
public final class Findings {

	/** Önem derecesi (çıktıda küçük harf). */
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

	/** Bulgu ekler; aynı check'in ilk çağrısındaki severity/açıklama geçerlidir. */
	public void add(String check, Severity severity, String message, Map<String, Object> item) {
		checks.computeIfAbsent(check, k -> new Check(check, severity, message, new ArrayList<>())).items().add(item);
	}

	/** Toplam bulgu (öğe) sayısı. */
	public int total() {
		return checks.values().stream().mapToInt(c -> c.items().size()).sum();
	}

	/** check → öğe sayısı. */
	public Map<String, Integer> counts() {
		Map<String, Integer> out = new LinkedHashMap<>();
		checks.forEach((k, c) -> out.put(k, c.items().size()));
		return out;
	}

	/** Bir check'in öğeleri (yoksa boş). */
	public List<Map<String, Object>> items(String check) {
		Check c = checks.get(check);
		return c == null ? List.of() : c.items();
	}

	/** Önem derecesine göre öğe sayıları (critical..info, sıfır olanlar hariç). */
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

	/** Önem derecesine, sonra sayıya göre sıralı bulgu satırları. */
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
