package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/** diagnose araçlarının ortak parametreleri ve küçük çıktı yardımcıları. */
final class DiagnoseParams {

	/** Teşhis araçlarında iki örnek arası varsayılan bekleme (sn): diğer alt kontrollere süre kalsın diye kısa. */
	static final int DEFAULT_INTERVAL_SECONDS = 3;

	/** Kanıt listelerinde gösterilen en fazla öğe (fazlası sayılır). */
	static final int MAX_EVIDENCE = 10;

	private DiagnoseParams() {
	}

	static Map<String, Object> interval() {
		Map<String, Object> m = P.integer("İki offset örneği arası bekleme, sn (varsayılan " + DEFAULT_INTERVAL_SECONDS
				+ ", 1-10; süre sınırına göre kısaltılabilir)", 1, 10);
		m.put("default", DEFAULT_INTERVAL_SECONDS);
		return m;
	}

	static long intervalMs(ToolCall call) {
		return call.integer("intervalSeconds", DEFAULT_INTERVAL_SECONDS, 1, 10) * 1000L;
	}

	/** İlk n öğe. */
	static <T> List<T> first(Collection<T> items, int n) {
		List<T> list = new ArrayList<>(items);
		return list.subList(0, Math.min(n, list.size()));
	}

	/** Partition numaraları (ilk MAX_EVIDENCE). */
	static List<Integer> partitionNumbers(Collection<TopicPartition> tps) {
		return first(tps.stream().map(TopicPartition::partition).sorted().toList(), MAX_EVIDENCE);
	}

	/** "topic-3" biçiminde (ilk MAX_EVIDENCE). */
	static List<String> partitionNames(Collection<TopicPartition> tps) {
		return first(tps.stream().map(TopicPartition::toString).sorted().toList(), MAX_EVIDENCE);
	}

	/** Liste MAX_EVIDENCE'tan uzunsa atlanan sayısı, değilse null. */
	static Integer omitted(Collection<?> items) {
		return items.size() > MAX_EVIDENCE ? items.size() - MAX_EVIDENCE : null;
	}

}
