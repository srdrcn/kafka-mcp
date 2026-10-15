package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/** Shared parameters and small output helpers of the diagnose tools. */
final class DiagnoseParams {

	/** Default wait between two samples (s; same intervalSeconds rule as the other tools). */
	static final int DEFAULT_INTERVAL_SECONDS = 5;

	/** Short sampling interval of diagnose_topic (s): there are many sub-checks per topic. */
	static final int TOPIC_INTERVAL_SECONDS = 3;

	/** Maximum number of items shown in evidence lists (the rest are counted). */
	static final int MAX_EVIDENCE = 10;

	private DiagnoseParams() {
	}

	static Map<String, Object> interval(int defaultSeconds) {
		Map<String, Object> m = P.integer("Wait between two offset samples in seconds (default " + defaultSeconds
				+ ", 1-10; may be shortened to fit the time limit)", 1, 10);
		m.put("default", defaultSeconds);
		return m;
	}

	static long intervalMs(ToolCall call, int defaultSeconds) {
		return call.integer("intervalSeconds", defaultSeconds, 1, 10) * 1000L;
	}

	/** First n items. */
	static <T> List<T> first(Collection<T> items, int n) {
		List<T> list = new ArrayList<>(items);
		return list.subList(0, Math.min(n, list.size()));
	}

	/** Partition numbers (first MAX_EVIDENCE). */
	static List<Integer> partitionNumbers(Collection<TopicPartition> tps) {
		return first(tps.stream().map(TopicPartition::partition).sorted().toList(), MAX_EVIDENCE);
	}

	/** In "topic-3" form (first MAX_EVIDENCE). */
	static List<String> partitionNames(Collection<TopicPartition> tps) {
		return first(tps.stream().map(TopicPartition::toString).sorted().toList(), MAX_EVIDENCE);
	}

	/** Number of omitted items if the list is longer than MAX_EVIDENCE, otherwise null. */
	static Integer omitted(Collection<?> items) {
		return omitted(items, MAX_EVIDENCE);
	}

	/** Number of omitted items if the list is longer than max, otherwise null. */
	static Integer omitted(Collection<?> items, int max) {
		return items.size() > max ? items.size() - max : null;
	}

}
