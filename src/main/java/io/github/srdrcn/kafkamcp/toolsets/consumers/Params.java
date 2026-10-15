package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** consumers araçlarının ortak parametre şemaları ve ayrıştırıcıları. */
final class Params {

	static final int DEFAULT_INTERVAL_SECONDS = 5;

	/** İkinci örnek (committed + end offset) için deadline'dan ayrılan pay. */
	static final long SECOND_SAMPLE_RESERVE_MS = 4_000;

	/**
	 * İki örnekli araçlarda ilk örneğin adımları için paylar (grup listesi, committed, end offset): yanıt
	 * vermeyen grup/partition ilk örnekte bütün süreyi yiyemez; bekleme + ikinci örnek için süre kalır.
	 */
	static final long FIRST_LIST_RESERVE_MS = 9_000;

	static final long FIRST_COMMITTED_RESERVE_MS = 7_000;

	static final long FIRST_END_RESERVE_MS = 5_000;

	/** İkinci örneğin adımları için paylar (committed, end offset, son describe). */
	static final long SECOND_COMMITTED_RESERVE_MS = 2_000;

	static final long SECOND_END_RESERVE_MS = 800;

	static final long FINAL_DESCRIBE_RESERVE_MS = 300;

	private static final long MIN_PAUSE_MS = 500;

	private Params() {
	}

	static Map<String, Object> group() {
		return P.string("Consumer group adı (groupId)");
	}

	static Map<String, Object> intervalSeconds() {
		Map<String, Object> m = P.integer("İki örnek arası bekleme, sn (varsayılan " + DEFAULT_INTERVAL_SECONDS + ")",
				1, 10);
		m.put("default", DEFAULT_INTERVAL_SECONDS);
		return m;
	}

	static Map<String, Object> intArray(String description) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("type", "array");
		m.put("description", description);
		m.put("items", Map.of("type", "integer"));
		return m;
	}

	static int intervalSeconds(ToolCall call) {
		return call.integer("intervalSeconds", DEFAULT_INTERVAL_SECONDS, 1, 10);
	}

	/** partitions parametresi: sıralı, tekil; yoksa boş liste. */
	static List<Integer> partitions(ToolCall call) {
		TreeSet<Integer> out = new TreeSet<>();
		for (String s : call.strings("partitions")) {
			try {
				int p = new BigDecimal(s).intValueExact();
				if (p < 0) {
					throw new NumberFormatException();
				}
				out.add(p);
			}
			catch (NumberFormatException | ArithmeticException e) {
				throw new ToolException("partitions negatif olmayan tamsayı listesi olmalı: " + s);
			}
		}
		return new ArrayList<>(out);
	}

	/**
	 * İki örnek arası bekler; ikinci örneğe süre kalsın diye gerekirse beklemeyi kısaltır. Gerçek bekleme
	 * süresini (ms) döner; ikinci örneğe süre kalmadıysa beklemeden -1 döner.
	 */
	static long pause(ToolCall call, int seconds) {
		long wanted = seconds * 1000L;
		long ms = Math.min(wanted, call.remainingMillis() - SECOND_SAMPLE_RESERVE_MS);
		if (ms < MIN_PAUSE_MS) {
			return -1;
		}
		call.sleep(ms);
		return ms;
	}

	static ToolException noTimeForSecondSample() {
		return new ToolException("İkinci örnek için süre kalmadı (araç zaman sınırı 15 sn); daha dar filtre deneyin");
	}

	/** Saniye başına oran, 2 ondalık. */
	static double rate(long delta, double seconds) {
		return seconds <= 0 ? 0 : Math.round(delta / seconds * 100) / 100.0;
	}

	static double seconds(long nanos) {
		return Math.round(nanos / 1_000_000.0) / 1000.0;
	}

}
