package io.github.srdrcn.kafkamcp.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/** Reads the last records of partitions with an assign-only consumer (no group.id, no commit). */
public final class LastRecords {

	/** How many records to step back from latest (the last offsets may be transaction markers). */
	private static final int BACKTRACK = 10;

	private LastRecords() {
	}

	/** Offset and timestamp of the last record read. */
	public record Last(long offset, long timestamp) {
	}

	/**
	 * Reads the last record of every non-empty partition; takes at most {@code budgetMs} (and the tool deadline).
	 * Partitions that could not be read (empty, only control records, out of time) are absent from the result.
	 */
	public static Map<TopicPartition, Last> read(ToolCall call, Map<TopicPartition, Offsets.Range> ranges,
			long budgetMs) {
		Map<TopicPartition, Offsets.Range> todo = new LinkedHashMap<>();
		ranges.forEach((tp, r) -> {
			if (r.count() > 0) {
				todo.put(tp, r);
			}
		});
		Map<TopicPartition, Last> out = new LinkedHashMap<>();
		if (todo.isEmpty()) {
			return out;
		}
		long end = System.currentTimeMillis() + Math.max(0, Math.min(budgetMs, call.remainingMillis() - 1_000));
		try (KafkaConsumer<byte[], byte[]> consumer = call.newConsumer()) {
			consumer.assign(todo.keySet());
			todo.forEach((tp, r) -> consumer.seek(tp, Math.max(r.earliest(), r.latest() - BACKTRACK)));
			Set<TopicPartition> pending = new HashSet<>(todo.keySet());
			while (!pending.isEmpty()) {
				long left = end - System.currentTimeMillis();
				if (left <= 0) {
					break;
				}
				for (ConsumerRecord<byte[], byte[]> rec : consumer.poll(Duration.ofMillis(Math.min(500, left)))) {
					TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
					Last prev = out.get(tp);
					if (prev == null || rec.offset() > prev.offset()) {
						out.put(tp, new Last(rec.offset(), rec.timestamp()));
					}
				}
				List<TopicPartition> done = new ArrayList<>();
				for (TopicPartition tp : pending) {
					if (consumer.position(tp, Duration.ofMillis(200)) >= todo.get(tp).latest()) {
						done.add(tp);
					}
				}
				done.forEach(pending::remove);
				consumer.pause(done);
			}
		}
		catch (KafkaException e) {
			// partial result: whatever could be read is returned (e.g. retention passed the offset, timeout)
		}
		return out;
	}

}
