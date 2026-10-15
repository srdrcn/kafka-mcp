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
 * Yan işi (REST çağrısı, bağımsız admin zinciri) sanal thread'de başlatır; sonucu deadline'dan pay bırakarak
 * bekler. Yetişmezse iş iptal edilir ve {@link Settled#timedOut()} sonucu döner; araç düşmez.
 */
public final class Async<T> {

	private static final ExecutorService POOL = Executors.newVirtualThreadPerTaskExecutor();

	private final Future<T> future;

	private Async(Future<T> future) {
		this.future = future;
	}

	/** İşi hemen başlatır. */
	public static <T> Async<T> start(Callable<T> task) {
		return new Async<>(POOL.submit(task));
	}

	/** En fazla (kalan süre - reserveMillis) bekler; hata/zaman aşımı sonuçta taşınır. */
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
			throw new ToolException("Araç iptal edildi");
		}
	}

	/** Hata mesajı: ToolException ise olduğu gibi, değilse Kafka hata açıklaması. */
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
