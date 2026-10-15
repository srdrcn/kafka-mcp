package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bir raporun kapsamı: ne incelendi ({@code examined}), ne atlandı ({@code skipped}, nedene göre sayı),
 * örnekleme yapıldı mı ({@code sampled}), süre yetmediği için eksik mi ({@code partial}), hatalar ve notlar.
 * Sessiz kırpma olmasın diye her rapor çıktısına {@link #addTo(Map)} ile eklenir.
 */
public final class Coverage {

	private final Map<String, Integer> examined = new LinkedHashMap<>();

	private final Map<String, Integer> skipped = new LinkedHashMap<>();

	private final Map<String, Object> errors = new LinkedHashMap<>();

	private final List<String> notes = new ArrayList<>();

	private boolean sampled;

	private boolean partial;

	/** İncelenen öğe sayısı (ör. topics, partitions, groups). */
	public Coverage examined(String what, int count) {
		examined.put(what, count);
		return this;
	}

	/** Atlanan öğe sayısı; 0 ise yazılmaz. Aynı anahtar tekrar verilirse toplanır. */
	public Coverage skipped(String why, int count) {
		if (count > 0) {
			skipped.merge(why, count, Integer::sum);
		}
		return this;
	}

	/** Bir adımın hatası (kısa mesaj ya da anahtar başına hata sayısı/map'i). */
	public Coverage error(String what, Object detail) {
		if (detail != null) {
			errors.put(what, detail);
		}
		return this;
	}

	/** Sınır nedeniyle örnekleme yapıldı (tüm öğeler incelenmedi). */
	public Coverage sampled() {
		sampled = true;
		return this;
	}

	/** Süre yetmediği/yanıt gelmediği için sonuç eksik. */
	public Coverage partial() {
		partial = true;
		return this;
	}

	public Coverage note(String note) {
		if (note != null && !notes.contains(note)) {
			notes.add(note);
		}
		return this;
	}

	public boolean isPartial() {
		return partial;
	}

	public boolean isSampled() {
		return sampled;
	}

	public Map<String, Integer> examinedCounts() {
		return examined;
	}

	public Map<String, Integer> skippedCounts() {
		return skipped;
	}

	/** examined, skipped, sampled, partial, errors, notes alanlarını (boş olanlar hariç) ekler. */
	public void addTo(Map<String, Object> out) {
		out.put("examined", new LinkedHashMap<>(examined));
		if (!skipped.isEmpty()) {
			out.put("skipped", new LinkedHashMap<>(skipped));
		}
		if (sampled) {
			out.put("sampled", true);
		}
		if (partial) {
			out.put("partial", true);
		}
		if (!errors.isEmpty()) {
			out.put("errors", new LinkedHashMap<>(errors));
		}
		if (!notes.isEmpty()) {
			out.put("notes", List.copyOf(notes));
		}
	}

}
