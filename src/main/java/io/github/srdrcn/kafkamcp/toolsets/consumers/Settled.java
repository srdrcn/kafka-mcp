package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.KafkaFuture;

import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * Deadline içinde beklenmiş bir Kafka future'ının sonucu: değer ya da (öğe bazında) hata. Toplu admin
 * çağrılarında tek bir grubun/partition'ın hatası aracı düşürmesin diye kullanılır; zaman aşımı yine
 * {@link io.github.srdrcn.kafkamcp.mcp.ToolException} olur.
 */
public record Settled<T>(T value, Throwable error) {

	public boolean ok() {
		return error == null;
	}

	/** Hatanın kısa Türkçe açıklaması; başarılıysa null. */
	public String errorMessage() {
		return error == null ? null : KafkaErrors.explain(error);
	}

	/** Future'ı {@link ToolCall#await} ile bekler; future'ın kendi hatası fırlatılmaz, sonuçta taşınır. */
	public static <T> Settled<T> of(ToolCall call, KafkaFuture<T> future) {
		CompletableFuture<Settled<T>> settled = new CompletableFuture<>();
		future.whenComplete((v, e) -> settled.complete(new Settled<>(v, e == null ? null : KafkaErrors.unwrap(e))));
		return call.await(settled);
	}

	/** Anahtar başına future'ları sırayla bekler (hepsi aynı istekten geldiği için süre tek istek kadardır). */
	public static <K, T> Map<K, Settled<T>> all(ToolCall call, Map<K, KafkaFuture<T>> futures) {
		Map<K, Settled<T>> out = new LinkedHashMap<>();
		futures.forEach((k, f) -> out.put(k, of(call, f)));
		return out;
	}

}
