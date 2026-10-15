package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.time.Duration;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * {@link ToolCall#newConsumer()} sarmalayıcısı: try-with-resources kapanışı kalan süreyle sınırlıdır (en fazla
 * 1 sn). Varsayılan {@code close()} yavaş bir broker'ın fetch-session kapanışını request.timeout (15 sn) kadar
 * bekleyebilir ve deadline'dan önce durup hazırlanan partial sonucu zaman aşımına sokar.
 */
public final class BoundedConsumer implements AutoCloseable {

	static final long MAX_CLOSE_MS = 1_000;

	static final long CLOSE_MARGIN_MS = 500;

	private final ToolCall call;

	private final KafkaConsumer<byte[], byte[]> consumer;

	BoundedConsumer(ToolCall call, KafkaConsumer<byte[], byte[]> consumer) {
		this.call = call;
		this.consumer = consumer;
	}

	/** Seçilen cluster için yeni assign-only consumer açar. */
	public static BoundedConsumer open(ToolCall call) {
		return new BoundedConsumer(call, call.newConsumer());
	}

	public KafkaConsumer<byte[], byte[]> consumer() {
		return consumer;
	}

	@Override
	public void close() {
		consumer.close(CloseOptions.timeout(Duration.ofMillis(closeMillis(call.remainingMillis()))));
	}

	/** Kapanış için beklenecek süre: en fazla 1 sn, deadline'a 0,5 sn pay bırakarak. */
	static long closeMillis(long remainingMillis) {
		return Math.min(MAX_CLOSE_MS, Math.max(0, remainingMillis - CLOSE_MARGIN_MS));
	}

}
