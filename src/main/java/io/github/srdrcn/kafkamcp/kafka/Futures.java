package io.github.srdrcn.kafkamcp.kafka;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.KafkaFuture;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Helper that awaits per-key KafkaFutures (partition, broker, group...) in one go and separates the successful
 * results from the errors, so a single partition/group error does not fail the whole tool.
 */
public final class Futures {

	private Futures() {
	}

	/** Successful values and error messages per key. */
	public record Each<K, V>(Map<K, V> values, Map<K, String> errors) {

		/** Throws the first error as a ToolException if there is no successful value; otherwise returns itself. */
		public Each<K, V> requireAny() {
			if (values.isEmpty() && !errors.isEmpty()) {
				throw new ToolException(errors.values().iterator().next());
			}
			return this;
		}

	}

	/** Waits for all futures until the deadline; never throws, collects the errors per key. */
	public static <K, V> Each<K, V> awaitEach(ToolCall call, Map<K, ? extends KafkaFuture<V>> futures) {
		return awaitEach(call, futures, Long.MAX_VALUE);
	}

	/**
	 * Like {@link #awaitEach(ToolCall, Map)}, but waits at most {@code maxMillis}; unfinished ones get a "timeout"
	 * error. So a stuck broker/coordinator does not use up the time of the following steps.
	 */
	public static <K, V> Each<K, V> awaitEach(ToolCall call, Map<K, ? extends KafkaFuture<V>> futures,
			long maxMillis) {
		Map<K, V> values = new LinkedHashMap<>();
		Map<K, String> errors = new LinkedHashMap<>();
		if (futures.isEmpty()) {
			return new Each<>(values, errors);
		}
		try {
			awaitAtMost(call, KafkaFuture.allOf(futures.values().toArray(KafkaFuture[]::new)), maxMillis);
		}
		catch (ToolException ignored) {
			// errors are collected per key below
		}
		for (Map.Entry<K, ? extends KafkaFuture<V>> e : futures.entrySet()) {
			KafkaFuture<V> f = e.getValue();
			if (!f.isDone()) {
				errors.put(e.getKey(), "timeout");
				continue;
			}
			try {
				values.put(e.getKey(), f.getNow(null));
			}
			catch (ExecutionException ex) {
				errors.put(e.getKey(), KafkaErrors.explain(ex.getCause()));
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw new ToolException("Tool was cancelled");
			}
			catch (RuntimeException ex) {
				errors.put(e.getKey(), KafkaErrors.explain(ex));
			}
		}
		return new Each<>(values, errors);
	}

	/** Waits on the future for at most min(remaining time, maxMillis); ToolException on error/timeout. */
	public static <T> T awaitAtMost(ToolCall call, KafkaFuture<T> future, long maxMillis) {
		long wait = Math.max(1, Math.min(call.waitMillis(), maxMillis));
		try {
			return future.get(wait, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Tool was cancelled");
		}
		catch (ExecutionException e) {
			throw new ToolException(KafkaErrors.explain(e.getCause()), e.getCause());
		}
		catch (TimeoutException e) {
			throw new ToolException("No response within " + wait + " ms (timeout)");
		}
	}

}
