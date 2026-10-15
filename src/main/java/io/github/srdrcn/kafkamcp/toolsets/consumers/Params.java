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

/** Shared parameter schemas and parsers of the consumers tools. */
final class Params {

	static final int DEFAULT_INTERVAL_SECONDS = 5;

	/** Share of the deadline kept for the second sample (committed + end offsets). */
	static final long SECOND_SAMPLE_RESERVE_MS = 4_000;

	/**
	 * Budgets for the first-sample steps of two-sample tools (group list, committed, end offsets): a group or
	 * partition that does not respond cannot use up all the time in the first sample; time is left for the
	 * pause and the second sample.
	 */
	static final long FIRST_LIST_RESERVE_MS = 9_000;

	static final long FIRST_COMMITTED_RESERVE_MS = 7_000;

	static final long FIRST_END_RESERVE_MS = 5_000;

	/** Budgets for the second-sample steps (committed, end offsets, final describe). */
	static final long SECOND_COMMITTED_RESERVE_MS = 2_000;

	static final long SECOND_END_RESERVE_MS = 800;

	static final long FINAL_DESCRIBE_RESERVE_MS = 300;

	private static final long MIN_PAUSE_MS = 500;

	private Params() {
	}

	static Map<String, Object> group() {
		return P.string("Consumer group id");
	}

	static Map<String, Object> intervalSeconds() {
		Map<String, Object> m = P.integer(
				"Sampling interval in seconds (default " + DEFAULT_INTERVAL_SECONDS + ", 1-10)", 1, 10);
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

	/** partitions parameter: sorted and distinct; empty list if absent. */
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
				throw new ToolException("partitions must be a list of non-negative integers: " + s);
			}
		}
		return new ArrayList<>(out);
	}

	/**
	 * Waits between the two samples; shortens the wait if needed so that time is left for the second sample.
	 * Returns the actual wait (ms); returns -1 without waiting if no time is left for the second sample.
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
		return new ToolException("No time left for the second sample (tool time limit is 15 s); try a narrower filter");
	}

	/** Rate per second, 2 decimals. */
	static double rate(long delta, double seconds) {
		return seconds <= 0 ? 0 : Math.round(delta / seconds * 100) / 100.0;
	}

	static double seconds(long nanos) {
		return Math.round(nanos / 1_000_000.0) / 1000.0;
	}

}
