package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Naming reports: topics that break the naming rule and very similar (suspected duplicate) topic names. */
@Component
public class NamingTools {

	/** Maximum distance and minimum name length for Levenshtein pairs. */
	static final int MAX_DISTANCE = 2;

	static final int MIN_LENGTH = 8;

	private static final Pattern FIRST_TOKEN = Pattern.compile("[-_.]");

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("report_naming_check",
						"Topics that do not match a naming rule (full-match regex), with counts and the most common prefixes")
					.required("pattern", P.string("Regex the whole topic name must match (e.g. [a-z]+\\.[a-z0-9-]+\\.v[0-9]+)"))
					.param("includeInternal", ReportParams.includeInternal(false))
					.listParams()
					.handler(this::namingCheck),
				ToolSpec.read("report_duplicate_like_topics",
						"Suspected duplicate topic names: groups that collide after normalization and pairs with Levenshtein distance ≤ 2")
					.param("includeInternal", ReportParams.includeInternal(false))
					.listParams(50)
					.handler(this::duplicateLikeTopics));
	}

	/** listTopics: names matching the filter (without internal/system topics unless included); fills in the coverage. */
	static List<String> names(ToolCall call, boolean includeInternal, Coverage coverage) {
		Map<String, Boolean> listing = Topics.list(call, true);
		List<String> out = new ArrayList<>();
		int internal = 0;
		int filtered = 0;
		for (Map.Entry<String, Boolean> e : listing.entrySet()) {
			if (!includeInternal && Topics.protectedReason(e.getKey(), e.getValue()) != null) {
				internal++;
			}
			else if (!call.matches(e.getKey())) {
				filtered++;
			}
			else {
				out.add(e.getKey());
			}
		}
		coverage.examined("topics", out.size());
		coverage.skipped("internalOrSystemTopics", internal);
		coverage.skipped("notMatchingFilter", filtered);
		return out;
	}

	Object namingCheck(ToolCall call) {
		Pattern pattern = call.regex("pattern");
		if (pattern == null) {
			throw new io.github.srdrcn.kafkamcp.mcp.ToolException("Missing parameter: pattern");
		}
		Coverage coverage = new Coverage();
		List<String> names = names(call, call.bool("includeInternal", false), coverage);
		List<String> bad = new ArrayList<>();
		Map<String, Integer> byPrefix = new HashMap<>();
		for (String n : names) {
			if (!call.fullMatch(pattern, n)) {
				bad.add(n);
				byPrefix.merge(FIRST_TOKEN.split(n, 2)[0], 1, Integer::sum);
			}
		}
		List<Map.Entry<String, Integer>> prefixes = new ArrayList<>(byPrefix.entrySet());
		prefixes.sort(Map.Entry.<String, Integer>comparingByValue().reversed().thenComparing(Map.Entry.comparingByKey()));
		Map<String, Integer> topPrefixes = new LinkedHashMap<>();
		ReportParams.head(prefixes, 10).forEach(e -> topPrefixes.put(e.getKey(), e.getValue()));
		Map<String, Object> out = Results.map("pattern", pattern.pattern(), "match", "full name (matches)", "matching",
				names.size() - bad.size(), "nonMatching", bad.size(), "nonMatchingPct",
				names.isEmpty() ? 0.0 : Format.round(100.0 * bad.size() / names.size(), 1), "nonMatchingByFirstToken",
				topPrefixes.isEmpty() ? null : topPrefixes, "firstTokensOmitted",
				prefixes.size() > 10 ? prefixes.size() - 10 : null);
		coverage.addTo(out);
		out.putAll(ReportParams.page("topics", bad, call.limit()));
		return out;
	}

	Object duplicateLikeTopics(ToolCall call) {
		Coverage coverage = new Coverage();
		List<String> names = names(call, call.bool("includeInternal", false), coverage);
		int limit = call.limit(50);
		Map<String, List<String>> collisions = NameSimilarity.collisions(names);
		List<Map<String, Object>> groups = new ArrayList<>();
		collisions.forEach((k, v) -> groups.add(Results.map("normalized", k, "count", v.size(), "topics", v)));
		groups.sort(Comparator.comparingInt((Map<String, Object> m) -> (Integer) m.get("count"))
			.reversed()
			.thenComparing(m -> (String) m.get("normalized")));
		NameSimilarity.Similar similar = NameSimilarity.similarPairs(names, MAX_DISTANCE, MIN_LENGTH,
				() -> call.nearDeadline(1_500));
		List<Map<String, Object>> pairs = new ArrayList<>();
		for (NameSimilarity.Pair p : similar.pairs()) {
			pairs.add(Results.map("a", p.a(), "b", p.b(), "distance", p.distance()));
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("rules", Results.map("normalize",
				"lower case; - _ . are the same separator; environment tokens (" + String.join("|", new java.util.TreeSet<>(NameSimilarity.ENV_TOKENS))
						+ ") and trailing version suffixes (v1, v2...) are dropped",
				"similar", "Levenshtein ≤ " + MAX_DISTANCE + ", name ≥ " + MIN_LENGTH
						+ " characters; names in the same collision group are excluded; pairs that differ only in digits "
						+ "(e.g. shard-01/shard-02, x.v1.y/x.v2.y) are under numericVariants"));
		out.put("collisionGroups", ReportParams.page("groups", groups, limit));
		Map<String, Object> sim = ReportParams.page("pairs", pairs, limit);
		sim.put("comparisons", similar.comparisons());
		if (similar.numericVariants() > 0) {
			List<Map<String, Object>> numeric = new ArrayList<>();
			for (NameSimilarity.Pair p : ReportParams.head(similar.numericPairs(), limit)) {
				numeric.add(Results.map("a", p.a(), "b", p.b(), "distance", p.distance()));
			}
			sim.put("numericVariants", Results.map("total", similar.numericVariants(), "returned", numeric.size(),
					"truncated", similar.numericVariants() > numeric.size(), "pairs", numeric));
		}
		if (similar.capped()) {
			sim.put("capped", true);
			coverage.note("Similar pairs reached the limit of " + NameSimilarity.MAX_PAIRS + "; scan stopped");
			coverage.partial();
		}
		if (similar.partial()) {
			coverage.partial();
			coverage.note("Time ran out: the similar pair scan is incomplete");
		}
		out.put("similarPairs", sim);
		coverage.addTo(out);
		return out;
	}

}
