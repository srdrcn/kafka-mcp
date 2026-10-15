package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.github.srdrcn.kafkamcp.mcp.P;

/** Shared parameter schemas and result fragments of the report tools. */
final class ReportParams {

	static final int DEFAULT_OLDER_THAN_DAYS = 30;

	private ReportParams() {
	}

	/** Integer parameter with a default. */
	static Map<String, Object> integer(String description, int defaultValue, int min, int max) {
		Map<String, Object> m = P.integer(description, min, max);
		m.put("default", defaultValue);
		return m;
	}

	/** olderThanDays: default 30 days. */
	static Map<String, Object> olderThanDays() {
		return integer("Days since the last write after which a topic counts as idle (default 30)",
				DEFAULT_OLDER_THAN_DAYS, 1, 3650);
	}

	/** intervalSeconds: time between the two samples (1-10, default 5). */
	static Map<String, Object> intervalSeconds() {
		return integer("Sampling interval in seconds for the write rate (default 5, 1-10)", 5, 1, 10);
	}

	/** includeInternal parameter. */
	static Map<String, Object> includeInternal(boolean defaultValue) {
		Map<String, Object> m = P.bool("Include internal (__*) and system topics (Connect/Schema Registry/MirrorMaker) (default "
				+ defaultValue + ")");
		m.put("default", defaultValue);
		return m;
	}

	/** Returns up to limit items of an already sorted list as {total, returned, truncated, key}. */
	static Map<String, Object> page(String key, List<?> rows, int limit) {
		int n = Math.min(limit, rows.size());
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("total", rows.size());
		m.put("returned", n);
		m.put("truncated", rows.size() > n);
		m.put(key, rows.subList(0, n));
		return m;
	}

	/** The first n items of the list; if truncated, the count must be reported separately. */
	static <T> List<T> head(List<T> list, int n) {
		return list.subList(0, Math.min(n, list.size()));
	}

}
