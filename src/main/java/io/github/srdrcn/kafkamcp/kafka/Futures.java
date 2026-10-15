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
 * Anahtar başına KafkaFuture'ları (partition, broker, grup...) tek seferde bekleyip başarılı sonuçları ve
 * hataları ayıran yardımcı. Böylece tek bir partition/grup hatası tüm aracı düşürmez.
 */
public final class Futures {

	private Futures() {
	}

	/** Anahtar başına başarılı değerler ve Türkçe hata mesajları. */
	public record Each<K, V>(Map<K, V> values, Map<K, String> errors) {

		/** Hiç başarılı değer yoksa ilk hatayı ToolException olarak fırlatır; yoksa kendini döner. */
		public Each<K, V> requireAny() {
			if (values.isEmpty() && !errors.isEmpty()) {
				throw new ToolException(errors.values().iterator().next());
			}
			return this;
		}

	}

	/** Tüm future'ları deadline'a kadar bekler; istisna fırlatmaz, hataları anahtar bazında toplar. */
	public static <K, V> Each<K, V> awaitEach(ToolCall call, Map<K, ? extends KafkaFuture<V>> futures) {
		return awaitEach(call, futures, Long.MAX_VALUE);
	}

	/**
	 * {@link #awaitEach(ToolCall, Map)} gibi, ama en fazla {@code maxMillis} bekler; bitmeyenler "zaman aşımı"
	 * hatası olur. Takılan bir broker/coordinator sonraki adımların süresini tüketmesin diye.
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
			// hatalar aşağıda anahtar bazında toplanır
		}
		for (Map.Entry<K, ? extends KafkaFuture<V>> e : futures.entrySet()) {
			KafkaFuture<V> f = e.getValue();
			if (!f.isDone()) {
				errors.put(e.getKey(), "zaman aşımı");
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
				throw new ToolException("Araç iptal edildi");
			}
			catch (RuntimeException ex) {
				errors.put(e.getKey(), KafkaErrors.explain(ex));
			}
		}
		return new Each<>(values, errors);
	}

	/** Future'ı en fazla min(kalan süre, maxMillis) bekler; hata/zaman aşımında Türkçe ToolException. */
	public static <T> T awaitAtMost(ToolCall call, KafkaFuture<T> future, long maxMillis) {
		long wait = Math.max(1, Math.min(call.waitMillis(), maxMillis));
		try {
			return future.get(wait, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Araç iptal edildi");
		}
		catch (ExecutionException e) {
			throw new ToolException(KafkaErrors.explain(e.getCause()), e.getCause());
		}
		catch (TimeoutException e) {
			throw new ToolException("Yanıt " + wait + " ms içinde gelmedi (zaman aşımı)");
		}
	}

}
