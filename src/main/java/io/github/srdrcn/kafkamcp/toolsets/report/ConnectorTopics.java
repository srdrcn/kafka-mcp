package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Connector config'inin hangi topic'lere referans verdiğini bulur: adında "topic" geçen anahtarlar; değer
 * virgülle ayrılmış topic listesi, {@code *regex}/{@code *pattern} ise regex, {@code *prefix} ise önek olarak
 * yorumlanır. Sadece config'e bakar (çalışan connector'ın aktif topic'leri ayrıca sorgulanmaz).
 */
public final class ConnectorTopics {

	/** Config'in topic referansları: literal adlar, regex'ler ve önekler (anahtar adlarıyla). */
	public record References(Set<String> literals, List<Pattern> regexes, Set<String> prefixes, Set<String> keys) {

		/** Topic bu config tarafından kapsanıyor mu? */
		public boolean covers(String topic) {
			if (literals.contains(topic)) {
				return true;
			}
			for (String p : prefixes) {
				if (topic.startsWith(p)) {
					return true;
				}
			}
			for (Pattern r : regexes) {
				if (r.matcher(topic).matches()) {
					return true;
				}
			}
			return false;
		}

		public boolean isEmpty() {
			return literals.isEmpty() && regexes.isEmpty() && prefixes.isEmpty();
		}

	}

	private ConnectorTopics() {
	}

	/** Config'teki topic referanslarını ayrıştırır (geçersiz regex'ler atlanır). */
	public static References parse(Map<String, String> config) {
		Set<String> literals = new TreeSet<>();
		List<Pattern> regexes = new ArrayList<>();
		Set<String> prefixes = new TreeSet<>();
		Set<String> keys = new LinkedHashSet<>();
		if (config == null) {
			return new References(literals, regexes, prefixes, keys);
		}
		for (Map.Entry<String, String> e : config.entrySet()) {
			String key = e.getKey().toLowerCase(Locale.ROOT);
			String value = e.getValue() == null ? "" : e.getValue().trim();
			if (!key.contains("topic") || value.isEmpty()) {
				continue;
			}
			if (key.endsWith("regex") || key.endsWith("pattern")) {
				try {
					regexes.add(Pattern.compile(value));
					keys.add(e.getKey());
				}
				catch (PatternSyntaxException ignored) {
					// geçersiz regex: referans sayılmaz
				}
			}
			else if (key.endsWith("prefix")) {
				prefixes.add(value);
				keys.add(e.getKey());
			}
			else if (!key.contains("replication") && !key.contains("partitions") && !key.contains("creation")) {
				for (String part : value.split(",")) {
					if (!part.isBlank()) {
						literals.add(part.trim());
					}
				}
				keys.add(e.getKey());
			}
		}
		return new References(literals, regexes, prefixes, keys);
	}

	/** Verilen topic'lerden config'in kapsadıkları (sıralı). */
	public static Set<String> covered(References refs, Collection<String> topics) {
		Set<String> out = new TreeSet<>();
		if (refs.isEmpty()) {
			return out;
		}
		for (String t : topics) {
			if (refs.covers(t)) {
				out.add(t);
			}
		}
		return out;
	}

}
