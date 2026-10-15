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
 * Finds which topics a connector config references: keys whose name contains "topic"; the value is read as a
 * comma-separated topic list, as a regex for {@code *regex}/{@code *pattern} keys and as a prefix for
 * {@code *prefix} keys. Only the config is inspected (the active topics of the running connector are not queried).
 */
public final class ConnectorTopics {

	/** Topic references of the config: literal names, regexes and prefixes (with the key names). */
	public record References(Set<String> literals, List<Pattern> regexes, Set<String> prefixes, Set<String> keys) {

		/** Is the topic covered by this config? */
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

	/** Parses the topic references in the config (invalid regexes are skipped). */
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
					// invalid regex: not counted as a reference
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

	/** The given topics that the config covers (sorted). */
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
