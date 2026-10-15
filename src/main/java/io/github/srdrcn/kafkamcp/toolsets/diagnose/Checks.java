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
 * Birbirinden bağımsız alt kontrolleri sanal thread'lerde eşzamanlı çalıştırır. Her sonuç araç deadline'ına
 * (pay bırakılarak) kadar beklenir: hata veren kontrol {@code CHECK_FAILED}, yetişmeyen {@code CHECK_TIMEOUT}
 * bulgusu olur ve sonuç kısmi işaretlenir; araç düşmez. Kapatılınca bitmemiş kontroller iptal edilir
 * (beklenmez). Kilit kullanılmaz: beklemeler sanal thread'lerin taşıyıcı thread'ini bağlamaz (pinning).
 */
public final class Checks implements AutoCloseable {

	/** get(): sonuçları birleştirip yanıtı döndürmek için deadline'dan ayrılan varsayılan pay. */
	public static final long DEFAULT_RESERVE_MS = 1_200;

	public static final String CHECK_FAILED = "CHECK_FAILED";

	public static final String CHECK_TIMEOUT = "CHECK_TIMEOUT";

	private final ToolCall call;

	private final Diagnosis diagnosis;

	private final ExecutorService pool = Executors
		.newThreadPerTaskExecutor(Thread.ofVirtual().name("diagnose-", 0).factory());

	public Checks(ToolCall call, Diagnosis diagnosis) {
		this.call = call;
		this.diagnosis = diagnosis;
	}

	/** Kontrolü hemen başlatır; sonucu {@link Check#get()} ile alın. name kısa Türkçe ad (bulgularda görünür). */
	public <T> Check<T> start(String name, Callable<T> task) {
		return new Check<>(name, pool.submit(task));
	}

	/** Başlatılmış bir alt kontrol. */
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

		/** Sonuç; hata ya da zaman aşımında null (bulgu bir kez eklenir). */
		public T get() {
			return get(DEFAULT_RESERVE_MS);
		}

		/**
		 * Sonucu en geç (kalan süre - reserveMillis) bekler; hata ya da zaman aşımında null döner, bulgu bir kez
		 * eklenir ve zaman aşımında kontrol iptal edilir. Ana akış için.
		 */
		public T get(long reserveMillis) {
			long wait = Math.max(1, call.remainingMillis() - reserveMillis);
			try {
				return future.get(wait, TimeUnit.MILLISECONDS);
			}
			catch (TimeoutException e) {
				future.cancel(true);
				report(Checks.CHECK_TIMEOUT, "süre sınırında bitmedi", wait);
			}
			catch (CancellationException e) {
				report(Checks.CHECK_TIMEOUT, "iptal edildi", wait);
			}
			catch (ExecutionException e) {
				Throwable cause = KafkaErrors.unwrap(e.getCause());
				report(Checks.CHECK_FAILED, cause instanceof ToolException ? cause.getMessage() : KafkaErrors.explain(cause),
						wait);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new ToolException("Araç iptal edildi");
			}
			return null;
		}

		/**
		 * Başka bir kontrolün içinden bağımlılık olarak bekler: en fazla maxWaitMillis (ve kalan süre); hazır
		 * değilse ya da hata verdiyse null döner. Bulgu eklemez, iptal etmez (ana akış get() ile raporlar).
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
				throw new ToolException("Araç iptal edildi");
			}
		}

		/** get() sonrası: başarılı mı? */
		public boolean ok() {
			return future.isDone() && !future.isCancelled() && error == null && future.state() == Future.State.SUCCESS;
		}

		/** get() sonrası: hata mesajı ya da null. */
		public String error() {
			return error;
		}

		private void report(String code, String message, long waited) {
			error = message;
			if (!reported.compareAndSet(false, true)) {
				return;
			}
			if (CHECK_TIMEOUT.equals(code)) {
				diagnosis.warning(CHECK_TIMEOUT, "Alt kontrol süre sınırında bitmedi: " + name,
						Diagnosis.evidence("check", name, "waitedMs", waited),
						"Sonuç eksik; aracı tekrar çalıştırın ya da bu konuya özel aracı kullanın");
			}
			else {
				diagnosis.warning(CHECK_FAILED, "Alt kontrol yapılamadı: " + name,
						Diagnosis.evidence("check", name, "error", message),
						"Hata yetki (ACL), erişim ya da sürüm kaynaklı olabilir; bu kontrolün sonucu değerlendirilmedi");
			}
			diagnosis.markPartial();
		}

	}

	@Override
	public void close() {
		pool.shutdownNow();
	}

}
