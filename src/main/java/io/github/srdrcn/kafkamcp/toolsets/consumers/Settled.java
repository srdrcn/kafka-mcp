package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TimeoutException;

import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * Result of a Kafka future awaited within the deadline: a value or a (per-item) error. Used in batch admin
 * calls so that an error or a missing response for a single group/partition does not fail the tool:
 * {@link #within} waits while leaving a share for the following steps, and items without a response become
 * {@link #timedOut()}.
 */
public record Settled<T>(T value, Throwable error) {

	/** Explanation for items without a response (coordinator loading, partition without a leader, broker silent). */
	public static final String NO_RESPONSE = "No response in time (the group coordinator may be loading or "
			+ "unreachable, or the partition may have no leader)";

	/** Shortest timeoutMs given to admin requests. */
	static final int MIN_TIMEOUT_MS = 500;

	private static final long AWAIT_CUSHION_MS = 100;

	public boolean ok() {
		return error == null;
	}

	/** Did no response arrive in time (admin timeoutMs elapsed or the wait limit was exceeded)? */
	public boolean timedOut() {
		return error instanceof TimeoutException;
	}

	/**
	 * Explanation for an API/version an old broker lacks (e.g. OffsetDelete on a Kafka 2.x ZooKeeper broker);
	 * same as {@link KafkaErrors#UNSUPPORTED}.
	 */
	public static final String UNSUPPORTED = KafkaErrors.UNSUPPORTED;

	/** Short explanation of the error; null on success. */
	public String errorMessage() {
		if (error == null) {
			return null;
		}
		return timedOut() ? NO_RESPONSE : explain(error);
	}

	/**
	 * {@link KafkaErrors#explain}: requests an old (e.g. ZooKeeper 2.x) broker does not support are explained as
	 * {@link KafkaErrors#UNSUPPORTED}.
	 */
	public static String explain(Throwable error) {
		return KafkaErrors.explain(error);
	}

	/** Waits on the future with {@link ToolCall#await}; the future's own error is carried in the result, not thrown. */
	public static <T> Settled<T> of(ToolCall call, KafkaFuture<T> future) {
		CompletableFuture<Settled<T>> settled = new CompletableFuture<>();
		future.whenComplete((v, e) -> settled.complete(new Settled<>(v, e == null ? null : KafkaErrors.unwrap(e))));
		return call.await(settled);
	}

	/**
	 * Waits on the future for at most (remaining time - reserveMillis); if it does not finish, returns a
	 * {@link #timedOut()} result instead of failing the tool. This leaves time for the following steps (e.g. end
	 * offsets, output).
	 */
	public static <T> Settled<T> within(ToolCall call, KafkaFuture<T> future, long reserveMillis) {
		CompletableFuture<Settled<T>> settled = new CompletableFuture<>();
		future.whenComplete((v, e) -> settled.complete(new Settled<>(v, e == null ? null : KafkaErrors.unwrap(e))));
		settled.completeOnTimeout(noResponse(), waitMillis(call, reserveMillis), TimeUnit.MILLISECONDS);
		return call.await(settled);
	}

	/**
	 * Collects the per-key futures of the same admin request with a single bounded wait (see {@link #within});
	 * when the time is up, unfinished ones are not awaited in a loop but returned as {@link #timedOut()}.
	 */
	public static <K, T> Map<K, Settled<T>> all(ToolCall call, Map<K, KafkaFuture<T>> futures, long reserveMillis) {
		Map<K, Settled<T>> out = new LinkedHashMap<>();
		if (futures.isEmpty()) {
			return out;
		}
		within(call, KafkaFuture.allOf(futures.values().toArray(new KafkaFuture<?>[0])), reserveMillis);
		futures.forEach((k, f) -> out.put(k, now(f)));
		return out;
	}

	/** Result of a completed future (does not wait); {@link #timedOut()} if it has not completed. */
	public static <T> Settled<T> now(KafkaFuture<T> future) {
		if (!future.isDone()) {
			return noResponse();
		}
		try {
			return new Settled<>(future.getNow(null), null);
		}
		catch (ExecutionException | CancellationException e) {
			return new Settled<>(null, KafkaErrors.unwrap(e));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return noResponse();
		}
	}

	/** timeoutMs of an admin request: remaining time minus reserveMillis for the following steps (at least 500 ms). */
	public static int timeoutMs(ToolCall call, long reserveMillis) {
		return (int) Math.max(MIN_TIMEOUT_MS, Math.min(Integer.MAX_VALUE, call.waitMillis() - reserveMillis));
	}

	/** Bounded wait time; always ends a little before the own timeout of {@link ToolCall#await}. */
	private static long waitMillis(ToolCall call, long reserveMillis) {
		return Math.max(0, Math.min(call.waitMillis() - AWAIT_CUSHION_MS, timeoutMs(call, reserveMillis)));
	}

	private static <T> Settled<T> noResponse() {
		return new Settled<>(null, new TimeoutException(NO_RESPONSE));
	}

}
