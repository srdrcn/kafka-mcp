package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Runs independent sub-checks concurrently on virtual threads. Each result is awaited until the tool deadline
 * (minus a margin): a failing check becomes a {@code CHECK_FAILED} finding, one that does not finish in time a
 * {@code CHECK_TIMEOUT} finding, and the result is marked partial; the tool does not fail. On close, unfinished
 * checks are cancelled (not awaited). No locks are used: waits do not pin the carrier thread of virtual threads.
 */
public final class Checks implements AutoCloseable {

	/** get(): default margin kept before the deadline to merge the results and return the response. */
	public static final long DEFAULT_RESERVE_MS = 1_200;

	public static final String CHECK_FAILED = "CHECK_FAILED";

	public static final String CHECK_TIMEOUT = "CHECK_TIMEOUT";

	/** Start of the {@link DependencyTimeout} message (shown as the reason of the CHECK_TIMEOUT finding). */
	private static final String DEPENDENCY_PREFIX = "dependency check ";

	private final ToolCall call;

	private final Diagnosis diagnosis;

	private final ExecutorService pool = Executors
		.newThreadPerTaskExecutor(Thread.ofVirtual().name("diagnose-", 0).factory());

	public Checks(ToolCall call, Diagnosis diagnosis) {
		this.call = call;
		this.diagnosis = diagnosis;
	}

	/**
	 * A check that another check depends on did not finish in time ({@link Check#require(long)}): the dependent
	 * check becomes a {@code CHECK_TIMEOUT} finding and the result is marked partial.
	 */
	public static final class DependencyTimeout extends ToolException {

		private final long waitedMillis;

		public DependencyTimeout(String dependency, long waitedMillis) {
			super(DEPENDENCY_PREFIX + "'" + dependency + "' did not finish within " + waitedMillis + " ms");
			this.waitedMillis = waitedMillis;
		}

		public long waitedMillis() {
			return waitedMillis;
		}

	}

	/** Starts the check immediately; get the result with {@link Check#get()}. name is a short name (shown in findings). */
	public <T> Check<T> start(String name, Callable<T> task) {
		return new Check<>(name, pool.submit(task));
	}

	/** A started sub-check. */
	public final class Check<T> {

		private final String name;

		private final Future<T> future;

		private final AtomicBoolean reported = new AtomicBoolean();

		private volatile String error;

		private Check(String name, Future<T> future) {
			this.name = name;
			this.future = future;
		}

		public String name() {
			return name;
		}

		/** Result; null on error or timeout (the finding is added once). */
		public T get() {
			return get(DEFAULT_RESERVE_MS);
		}

		/**
		 * Waits for the result for at most (remaining time - reserveMillis); returns null on error or timeout, adds the
		 * finding once and cancels the check on timeout. For the main flow.
		 */
		public T get(long reserveMillis) {
			long wait = Math.max(1, call.remainingMillis() - reserveMillis);
			try {
				return future.get(wait, TimeUnit.MILLISECONDS);
			}
			catch (TimeoutException e) {
				future.cancel(true);
				report(Checks.CHECK_TIMEOUT, "did not finish within the time limit", wait);
			}
			catch (CancellationException e) {
				report(Checks.CHECK_TIMEOUT, "was cancelled", wait);
			}
			catch (ExecutionException e) {
				Throwable cause = KafkaErrors.unwrap(e.getCause());
				if (cause instanceof DependencyTimeout dt) {
					report(Checks.CHECK_TIMEOUT, dt.getMessage(), dt.waitedMillis());
				}
				else {
					report(Checks.CHECK_FAILED,
							cause instanceof ToolException ? cause.getMessage() : KafkaErrors.explain(cause), wait);
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new ToolException("Tool was cancelled");
			}
			return null;
		}

		/**
		 * Waits as a dependency from inside another check: at most maxWaitMillis (and the remaining time); returns null
		 * if it is not ready or failed. Adds no finding and does not cancel (the main flow reports via get()).
		 */
		public T await(long maxWaitMillis) {
			long wait = Math.max(1, Math.min(maxWaitMillis, call.remainingMillis() - DEFAULT_RESERVE_MS));
			try {
				return future.get(wait, TimeUnit.MILLISECONDS);
			}
			catch (TimeoutException | ExecutionException | CancellationException e) {
				return null;
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new ToolException("Tool was cancelled");
			}
		}

		/**
		 * Waits as a REQUIRED dependency from inside another check (at most maxWaitMillis and the remaining time).
		 * Returns null if the dependency failed (the main flow reports its own finding); throws
		 * {@link DependencyTimeout} if it did not finish in time, so the dependent check is not silently empty: a
		 * CHECK_TIMEOUT finding and a partial result.
		 */
		public T require(long maxWaitMillis) {
			long wait = Math.max(1, Math.min(maxWaitMillis, call.remainingMillis() - DEFAULT_RESERVE_MS));
			try {
				return future.get(wait, TimeUnit.MILLISECONDS);
			}
			catch (TimeoutException e) {
				throw new DependencyTimeout(name, wait);
			}
			catch (ExecutionException | CancellationException e) {
				return null;
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new ToolException("Tool was cancelled");
			}
		}

		/** After get(): did it succeed? */
		public boolean ok() {
			return future.isDone() && !future.isCancelled() && error == null && future.state() == Future.State.SUCCESS;
		}

		/** After get(): error message or null. */
		public String error() {
			return error;
		}

		private void report(String code, String message, long waited) {
			error = message;
			if (!reported.compareAndSet(false, true)) {
				return;
			}
			if (CHECK_TIMEOUT.equals(code)) {
				diagnosis.warning(CHECK_TIMEOUT, "Sub-check did not finish within the time limit: " + name,
						Diagnosis.evidence("check", name, "waitedMs", waited, "reason",
								message.startsWith(DEPENDENCY_PREFIX) ? message : null),
						"Result is incomplete; run the tool again or use the tool dedicated to this topic");
			}
			else {
				diagnosis.warning(CHECK_FAILED, "Sub-check could not run: " + name,
						Diagnosis.evidence("check", name, "error", message),
						"The error may be caused by authorization (ACL), access or version; this check's result was not evaluated");
			}
			diagnosis.markPartial();
		}

	}

	@Override
	public void close() {
		pool.shutdownNow();
	}

}
