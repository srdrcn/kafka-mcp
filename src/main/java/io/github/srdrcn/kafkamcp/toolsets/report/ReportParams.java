package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.github.srdrcn.kafkamcp.mcp.P;

/** report araçlarının ortak parametre şemaları ve sonuç parçaları. */
final class ReportParams {

	static final int DEFAULT_OLDER_THAN_DAYS = 30;

	private ReportParams() {
	}

	/** Varsayılanlı tamsayı parametresi. */
	static Map<String, Object> integer(String description, int defaultValue, int min, int max) {
		Map<String, Object> m = P.integer(description, min, max);
		m.put("default", defaultValue);
		return m;
	}

	/** olderThanDays: varsayılan 30 gün. */
	static Map<String, Object> olderThanDays() {
		return integer("Son yazma kaç günden eskiyse boşta sayılır (varsayılan 30)", DEFAULT_OLDER_THAN_DAYS, 1, 3650);
	}

	/** intervalSeconds: iki örnek arası (1-10, varsayılan 5). */
	static Map<String, Object> intervalSeconds() {
		return integer("Yazma hızı için iki örnek arası saniye (varsayılan 5, 1-10)", 5, 1, 10);
	}

	/** includeInternal parametresi. */
	static Map<String, Object> includeInternal(boolean defaultValue) {
		Map<String, Object> m = P.bool("İç (__*) ve sistem topic'lerini (Connect/Schema Registry/MirrorMaker) dahil et (varsayılan "
				+ defaultValue + ")");
		m.put("default", defaultValue);
		return m;
	}

	/** Zaten sıralanmış listeden limit kadarını {total, returned, truncated, key} biçiminde döner. */
	static Map<String, Object> page(String key, List<?> rows, int limit) {
		int n = Math.min(limit, rows.size());
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("total", rows.size());
		m.put("returned", n);
		m.put("truncated", rows.size() > n);
		m.put(key, rows.subList(0, n));
		return m;
	}

	/** Liste ilk n öğeyle; kırpıldıysa sayısı ayrıca verilmelidir. */
	static <T> List<T> head(List<T> list, int n) {
		return list.subList(0, Math.min(n, list.size()));
	}

}
