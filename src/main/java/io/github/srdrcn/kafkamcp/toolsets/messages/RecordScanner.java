package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * assign()-only consumer ile partition başına offset pencerelerini ({@code [start, end)}) okur. Yeni veri
 * beklemez; deadline'a yaklaşınca durur ve {@code deadlineReached} döner (çağıran partial işaretler).
 */
public final class RecordScanner {

	/** Döngüden çıkıp sonucu hazırlamak için deadline'dan önce bırakılan pay. */
	public static final long DEFAULT_RESERVE_MS = 2_000;

	private static final long MAX_POLL_MS = 500;

	private RecordScanner() {
	}

	/** Okunacak aralık: {@code [start, end)}. */
	public record Window(TopicPartition partition, long start, long end) {

		public long size() {
			return Math.max(0, end - start);
		}

	}

	/** Ziyaretçinin kararı: devam, bu partition bitti, tümüyle dur. */
	public enum Next {

		CONTINUE, PARTITION_DONE, STOP

	}

	@FunctionalInterface
	public interface Visitor {

		Next visit(ConsumerRecord<byte[], byte[]> record);

	}

	/**
	 * Tarama sonucu. {@code positions}: partition başına bir sonraki okunacak offset (pencere sonuna
	 * ulaşılmışsa en az {@code end}).
	 */
	public record Result(long scanned, boolean completed, boolean deadlineReached, boolean stopped,
			Map<TopicPartition, Long> positions) {

		/** Her pencere kendi sonuna (ya da verilen offset'lere) ulaştı mı? */
		public boolean reached(Map<TopicPartition, Long> ends) {
			for (Map.Entry<TopicPartition, Long> e : ends.entrySet()) {
				Long p = positions.get(e.getKey());
				if (p == null || p < e.getValue()) {
					return false;
				}
			}
			return true;
		}

	}

	public static Result scan(ToolCall call, KafkaConsumer<byte[], byte[]> consumer, Collection<Window> windows,
			Visitor visitor) {
		return scan(call, consumer, windows, DEFAULT_RESERVE_MS, visitor);
	}

	public static Result scan(ToolCall call, KafkaConsumer<byte[], byte[]> consumer, Collection<Window> windows,
			long reserveMs, Visitor visitor) {
		Map<TopicPartition, Window> active = new LinkedHashMap<>();
		Map<TopicPartition, Long> positions = new HashMap<>();
		for (Window w : windows) {
			positions.put(w.partition(), w.start());
			if (w.size() > 0) {
				active.put(w.partition(), w);
			}
			else {
				positions.put(w.partition(), Math.max(w.start(), w.end()));
			}
		}
		if (active.isEmpty()) {
			return new Result(0, true, false, false, positions);
		}
		consumer.assign(active.keySet());
		active.values().forEach(w -> consumer.seek(w.partition(), w.start()));
		// aynı consumer'la ikinci tarama: önceki taramada biten partition'lar pause'ta kalmış olabilir (assign()
		// aynı küme için durumu sıfırlamaz)
		consumer.resume(active.keySet());
		long scanned = 0;
		while (!active.isEmpty()) {
			if (call.nearDeadline(reserveMs)) {
				return new Result(scanned, false, true, false, positions);
			}
			long pollMs = Math.max(1, Math.min(MAX_POLL_MS, call.remainingMillis() - reserveMs - 300));
			ConsumerRecords<byte[], byte[]> records;
			try {
				records = consumer.poll(Duration.ofMillis(pollMs));
			}
			catch (OffsetOutOfRangeException e) {
				// retention başlangıcı ileri aldı: kalan en eski kayıttan devam
				consumer.seekToBeginning(e.partitions());
				continue;
			}
			Set<TopicPartition> done = new HashSet<>();
			for (TopicPartition tp : records.partitions()) {
				Window w = active.get(tp);
				if (w == null) {
					continue;
				}
				for (ConsumerRecord<byte[], byte[]> r : records.records(tp)) {
					if (r.offset() >= w.end()) {
						break;
					}
					scanned++;
					positions.put(tp, r.offset() + 1);
					Next next = visitor.visit(r);
					if (next == Next.STOP) {
						return new Result(scanned, false, false, true, positions);
					}
					if (next == Next.PARTITION_DONE) {
						done.add(tp);
						break;
					}
				}
			}
			for (Window w : active.values()) {
				if (done.contains(w.partition())) {
					continue;
				}
				long pos = consumer.position(w.partition(), Duration.ofMillis(call.waitMillis()));
				if (pos >= w.end()) {
					// consumer pencere sonrasını da çekmiş olabilir; işlenen kısım pencere sonunda biter
					positions.put(w.partition(), w.end());
					done.add(w.partition());
				}
			}
			if (!done.isEmpty()) {
				consumer.pause(done);
				done.forEach(active::remove);
			}
		}
		return new Result(scanned, true, false, false, positions);
	}

	/**
	 * Bütçeyi partition'lara adil dağıtır (water-filling): az kaydı olan partition ihtiyacı kadar alır, artan
	 * pay diğerlerine kalır. Toplam = min(bütçe, toplam talep). Sonuç deterministiktir.
	 */
	public static Map<TopicPartition, Long> fairShare(Map<TopicPartition, Long> demand, long budget) {
		List<Map.Entry<TopicPartition, Long>> entries = new ArrayList<>(demand.entrySet());
		entries.sort(Comparator.comparingLong((Map.Entry<TopicPartition, Long> e) -> Math.max(0, e.getValue()))
			.thenComparing(e -> e.getKey().partition()));
		Map<TopicPartition, Long> out = new HashMap<>();
		long remaining = Math.max(0, budget);
		int left = entries.size();
		for (Map.Entry<TopicPartition, Long> e : entries) {
			long share = remaining / left;
			long give = Math.min(Math.max(0, e.getValue()), share);
			out.put(e.getKey(), give);
			remaining -= give;
			left--;
		}
		return out;
	}

	public static long sum(Map<TopicPartition, Long> m) {
		long s = 0;
		for (long v : m.values()) {
			s += Math.max(0, v);
		}
		return s;
	}

}
