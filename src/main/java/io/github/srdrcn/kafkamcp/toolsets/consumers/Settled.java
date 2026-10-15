package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * Deadline içinde beklenmiş bir Kafka future'ının sonucu: değer ya da (öğe bazında) hata. Toplu admin
 * çağrılarında tek bir grubun/partition'ın hatası ya da yanıtsız kalması aracı düşürmesin diye kullanılır:
 * {@link #within} sonraki adımlara pay bırakarak bekler, yanıt gelmeyenler {@link #timedOut()} olur.
 */
public record Settled<T>(T value, Throwable error) {

	/** Yanıt gelmeyen öğelerin (koordinatör yükleniyor, partition leader'ı yok, broker yanıtsız) açıklaması. */
	public static final String NO_RESPONSE = "Süre içinde yanıt gelmedi (grup koordinatörü yükleniyor/erişilemiyor "
			+ "ya da partition leader'ı yok olabilir)";

	/** Admin isteklerine verilen en kısa timeoutMs. */
	static final int MIN_TIMEOUT_MS = 500;

	private static final long AWAIT_CUSHION_MS = 100;

	public boolean ok() {
		return error == null;
	}

	/** Süre içinde yanıt gelmedi mi (admin timeoutMs doldu ya da bekleme sınırı aşıldı)? */
	public boolean timedOut() {
		return error instanceof TimeoutException;
	}

	/** Eski broker'da olmayan API/sürüm (ör. Kafka 2.x ZooKeeper broker'ında OffsetDelete) için açıklama. */
	public static final String UNSUPPORTED = "Broker bu işlemi desteklemiyor (sürüm)";

	/** Hatanın kısa Türkçe açıklaması; başarılıysa null. */
	public String errorMessage() {
		if (error == null) {
			return null;
		}
		return timedOut() ? NO_RESPONSE : explain(error);
	}

	/**
	 * {@link KafkaErrors#explain}; ancak kafka-clients 4.x'te UnsupportedVersionException,
	 * InvalidConfigurationException'ın alt sınıfı olduğundan ortak açıklama onu "Geçersiz istek/config" sanar.
	 * Eski (ör. ZooKeeper 2.x) broker'ın desteklemediği istekler burada doğru Türkçe notu alır.
	 */
	public static String explain(Throwable error) {
		Throwable root = KafkaErrors.unwrap(error);
		if (root instanceof UnsupportedVersionException) {
			String m = root.getMessage();
			return UNSUPPORTED + " (" + root.getClass().getSimpleName() + (m == null || m.isBlank() ? "" : ": " + m)
					+ ")";
		}
		return KafkaErrors.explain(error);
	}

	/** Future'ı {@link ToolCall#await} ile bekler; future'ın kendi hatası fırlatılmaz, sonuçta taşınır. */
	public static <T> Settled<T> of(ToolCall call, KafkaFuture<T> future) {
		CompletableFuture<Settled<T>> settled = new CompletableFuture<>();
		future.whenComplete((v, e) -> settled.complete(new Settled<>(v, e == null ? null : KafkaErrors.unwrap(e))));
		return call.await(settled);
	}

	/**
	 * Future'ı en fazla (kalan süre - reserveMillis) bekler; bitmezse {@link #timedOut()} sonucu döner, araç
	 * düşmez. Böylece sonraki adımlar (ör. end offset'ler, çıktı) için süre kalır.
	 */
	public static <T> Settled<T> within(ToolCall call, KafkaFuture<T> future, long reserveMillis) {
		CompletableFuture<Settled<T>> settled = new CompletableFuture<>();
		future.whenComplete((v, e) -> settled.complete(new Settled<>(v, e == null ? null : KafkaErrors.unwrap(e))));
		settled.completeOnTimeout(noResponse(), waitMillis(call, reserveMillis), TimeUnit.MILLISECONDS);
		return call.await(settled);
	}

	/**
	 * Aynı admin isteğinden gelen anahtar başına future'ları tek bir sınırlı beklemeyle toplar (bkz.
	 * {@link #within}); süre dolunca bitmemiş olanlar döngüde beklenmez, {@link #timedOut()} olarak döner.
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

	/** Bitmiş future'ın sonucu (beklemez); bitmemişse {@link #timedOut()}. */
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

	/** Admin isteğinin timeoutMs'i: kalan süreden sonraki adımlar için reserveMillis düşülür (en az 500 ms). */
	public static int timeoutMs(ToolCall call, long reserveMillis) {
		return (int) Math.max(MIN_TIMEOUT_MS, Math.min(Integer.MAX_VALUE, call.waitMillis() - reserveMillis));
	}

	/** Sınırlı bekleme süresi; {@link ToolCall#await}'in kendi zaman aşımından hep biraz önce biter. */
	private static long waitMillis(ToolCall call, long reserveMillis) {
		return Math.max(0, Math.min(call.waitMillis() - AWAIT_CUSHION_MS, timeoutMs(call, reserveMillis)));
	}

	private static <T> Settled<T> noResponse() {
		return new Settled<>(null, new TimeoutException(NO_RESPONSE));
	}

}
