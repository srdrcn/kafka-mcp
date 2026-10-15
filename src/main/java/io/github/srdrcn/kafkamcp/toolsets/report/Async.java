package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.errors.TimeoutException;

import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.toolsets.consumers.Settled;

/**
 * Starts a side task (REST call, independent admin chain) on a virtual thread and waits for its result while
 * keeping a margin before the deadline. If it does not finish in time, the task is cancelled and a
 * {@link Settled#timedOut()} result is returned; the tool does not fail.
 */
public final class Async<T> {

	private static final ExecutorService POOL = Executors.newVirtualThreadPerTaskExecutor();

	private final Future<T> future;

	private Async(Future<T> future) {
		this.future = future;
	}

	/** Starts the task immediately. */
	public static <T> Async<T> start(Callable<T> task) {
		return new Async<>(POOL.submit(task));
	}

	/** Waits at most (remaining time - reserveMillis); errors/timeouts are carried in the result. */
	public Settled<T> await(ToolCall call, long reserveMillis) {
		long wait = Math.max(1, call.waitMillis() - reserveMillis);
		try {
			return new Settled<>(future.get(wait, TimeUnit.MILLISECONDS), null);
		}
		catch (java.util.concurrent.TimeoutException e) {
			future.cancel(true);
			return new Settled<>(null, new TimeoutException(Settled.NO_RESPONSE));
		}
		catch (ExecutionException e) {
			Throwable cause = KafkaErrors.unwrap(e.getCause());
			return new Settled<>(null, cause == null ? e : cause);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Tool was cancelled");
		}
	}

	/** Error message: a ToolException's message as is, otherwise the Kafka error explanation. */
	public static String message(Settled<?> s) {
		if (s == null || s.ok()) {
			return null;
		}
		if (s.error() instanceof ToolException te) {
			return te.getMessage();
		}
		return s.errorMessage();
	}

}
