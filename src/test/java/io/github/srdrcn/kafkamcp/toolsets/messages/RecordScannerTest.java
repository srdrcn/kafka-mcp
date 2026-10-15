package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.toolsets.messages.RecordScanner.Next;
import io.github.srdrcn.kafkamcp.toolsets.messages.RecordScanner.Window;

/** RecordScanner window bounds, stopping and deadline behaviour (mock consumer, independent of timing). */
class RecordScannerTest {

	private static final TopicPartition TP = new TopicPartition("t", 0);

	private static ToolCall call(long remainingMillis) {
		return new ToolCall("test", Map.of(), null, Instant.now().plusMillis(remainingMillis));
	}

	private static ConsumerRecords<byte[], byte[]> batch(long from, long to) {
		List<ConsumerRecord<byte[], byte[]>> list = new ArrayList<>();
		for (long o = from; o < to; o++) {
			list.add(new ConsumerRecord<>("t", 0, o, null, new byte[] { (byte) o }));
		}
		return new ConsumerRecords<>(Map.of(TP, list), Map.of());
	}

	@Test
	@SuppressWarnings("unchecked")
	void stopsBeforeDeadlineAndReportsPartial() {
		KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
		RecordScanner.Result r = RecordScanner.scan(call(500), consumer, List.of(new Window(TP, 3, 10)),
				rec -> Next.CONTINUE);
		assertThat(r.deadlineReached()).isTrue();
		assertThat(r.completed()).isFalse();
		assertThat(r.scanned()).isZero();
		assertThat(r.positions()).containsEntry(TP, 3L);
		verify(consumer, never()).poll(any(Duration.class));
	}

	@Test
	@SuppressWarnings("unchecked")
	void readsOnlyInsideWindow() {
		KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
		when(consumer.poll(any(Duration.class))).thenReturn(batch(1, 8));
		when(consumer.position(eq(TP), any(Duration.class))).thenReturn(8L);
		List<Long> seen = new ArrayList<>();
		RecordScanner.Result r = RecordScanner.scan(call(10_000), consumer, List.of(new Window(TP, 1, 4)), rec -> {
			seen.add(rec.offset());
			return Next.CONTINUE;
		});
		assertThat(seen).containsExactly(1L, 2L, 3L);
		assertThat(r.completed()).isTrue();
		assertThat(r.scanned()).isEqualTo(3);
		assertThat(r.positions()).containsEntry(TP, 4L);
		assertThat(r.reached(Map.of(TP, 4L))).isTrue();
		assertThat(r.reached(Map.of(TP, 8L))).isFalse();
		verify(consumer).seek(TP, 1L);
	}

	@Test
	@SuppressWarnings("unchecked")
	void visitorCanStopOrFinishPartition() {
		KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
		when(consumer.poll(any(Duration.class))).thenReturn(batch(0, 5));
		RecordScanner.Result stop = RecordScanner.scan(call(10_000), consumer, List.of(new Window(TP, 0, 5)),
				rec -> rec.offset() == 1 ? Next.STOP : Next.CONTINUE);
		assertThat(stop.stopped()).isTrue();
		assertThat(stop.scanned()).isEqualTo(2);
		assertThat(stop.positions()).containsEntry(TP, 2L);

		RecordScanner.Result done = RecordScanner.scan(call(10_000), consumer, List.of(new Window(TP, 0, 5)),
				rec -> rec.offset() == 2 ? Next.PARTITION_DONE : Next.CONTINUE);
		assertThat(done.completed()).isTrue();
		assertThat(done.scanned()).isEqualTo(3);
		assertThat(done.positions()).containsEntry(TP, 3L);

		RecordScanner.Result empty = RecordScanner.scan(call(10_000), consumer, List.of(new Window(TP, 7, 7)),
				rec -> Next.CONTINUE);
		assertThat(empty.completed()).isTrue();
		assertThat(empty.positions()).containsEntry(TP, 7L);
	}

}
