package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.time.Duration;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * Wrapper around {@link ToolCall#newConsumer()}: the try-with-resources close is bounded by the remaining time (at
 * most 1 s). The default {@code close()} can wait up to request.timeout (15 s) for a slow broker's fetch-session
 * close, pushing the partial result prepared before the deadline into a timeout.
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

	/** Opens a new assign-only consumer for the selected cluster. */
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

	/** Time to wait for the close: at most 1 s, leaving a 0.5 s margin before the deadline. */
	static long closeMillis(long remainingMillis) {
		return Math.min(MAX_CLOSE_MS, Math.max(0, remainingMillis - CLOSE_MARGIN_MS));
	}

}
