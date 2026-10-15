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

/** Partition'ların son kayıtlarını assign-only consumer ile okur (group.id yok, commit yok). */
public final class LastRecords {

	/** latest'tan kaç kayıt geri gidilir (son offset'ler transaction marker olabilir). */
	private static final int BACKTRACK = 10;

	private LastRecords() {
	}

	/** Son okunan kaydın offset'i ve zaman damgası. */
	public record Last(long offset, long timestamp) {
	}

	/**
	 * Boş olmayan her partition'ın son kaydını okur; en fazla {@code budgetMs} (ve araç deadline'ı) kadar sürer.
	 * Okunamayan partition'lar (boş, sadece kontrol kaydı, süre yetmedi) sonuçta yoktur.
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
			// kısmi sonuç: okunabilenler döner (ör. retention offset'i geçti, zaman aşımı)
		}
		return out;
	}

}
