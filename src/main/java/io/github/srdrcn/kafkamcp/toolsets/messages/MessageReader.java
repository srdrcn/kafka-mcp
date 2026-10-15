package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.toolsets.messages.RecordScanner.Next;
import io.github.srdrcn.kafkamcp.toolsets.messages.RecordScanner.Window;

/**
 * Consumer group'suz okuma motoru: earliest/latest/offset/timestamp başlangıcı, opsiyonel filtre,
 * maxRecords/maxScan sınırları; yeni veri beklemez, deadline'a yaklaşınca okuduğunu partial döner.
 * <ul>
 * <li>latest: seçilen partition'ların son kayıtları (end offset'lerden hesaplanır), zamana göre en yeni
 * maxRecords kayıt. Okunan toplam pencere maxScan ile sınırlıdır.</li>
 * <li>earliest/offset/timestamp: başlangıçtan ileri; filtresizken maxRecords partition'lara adil bölünür,
 * filtreliyken maxScan kadar kayıt taranır ve ilk maxRecords eşleşme döner.</li>
 * </ul>
 */
public final class MessageReader {

	public static final int DEFAULT_MAX_SCAN = 10_000;

	public static final int MAX_SCAN_LIMIT = 100_000;

	private static final int MAX_LISTED_PARTITIONS = 20;

	/** latest sıralaması: zaman, partition, offset. */
	public static final Comparator<ConsumerRecord<byte[], byte[]>> BY_TIME = Comparator
		.comparingLong((ConsumerRecord<byte[], byte[]> r) -> r.timestamp())
		.thenComparingInt(ConsumerRecord::partition)
		.thenComparingLong(ConsumerRecord::offset);

	public static final Comparator<ConsumerRecord<byte[], byte[]>> BY_OFFSET = Comparator
		.comparingInt((ConsumerRecord<byte[], byte[]> r) -> r.partition())
		.thenComparingLong(ConsumerRecord::offset);

	private MessageReader() {
	}

	public enum From {

		EARLIEST, LATEST, OFFSET, TIMESTAMP;

		public String id() {
			return name().toLowerCase(Locale.ROOT);
		}

		public static From parse(String value, From defaultValue) {
			if (value == null || value.isBlank()) {
				return defaultValue;
			}
			for (From f : values()) {
				if (f.id().equals(value.trim().toLowerCase(Locale.ROOT))) {
					return f;
				}
			}
			throw new ToolException("Geçersiz from: " + value + " (geçerli: earliest|latest|offset|timestamp)");
		}

	}

	/**
	 * Okuma isteği. {@code offset} from=offset, {@code timestamp} (epoch ms) from=timestamp için;
	 * {@code filter} null ise filtresiz.
	 */
	public record Request(String topic, Integer partition, From from, Long offset, Long timestamp, int maxRecords,
			int maxScan, Predicate<ConsumerRecord<byte[], byte[]>> filter) {
	}

	/**
	 * Okuma sonucu. {@code records}: latest için zamana, diğerleri için partition/offset'e göre sıralı.
	 * {@code nextOffsets}: partition başına bir sonraki okunacak offset.
	 */
	public record Result(List<ConsumerRecord<byte[], byte[]>> records, List<TopicPartition> partitions, long scanned,
			long matched, boolean reachedEnd, boolean partial, boolean scanLimitReached, boolean approximate,
			Map<TopicPartition, Long> nextOffsets, List<String> notes) {
	}

	public static Result read(ToolCall call, Request req) {
		List<TopicPartition> tps = PartitionOffsets.partitions(call, req.topic(), req.partition());
		PartitionOffsets.Bounds b = PartitionOffsets.bounds(call, tps);
		List<String> notes = new ArrayList<>();
		if (req.from() == From.LATEST) {
			return latest(call, req, tps, b, notes);
		}
		return forward(call, req, tps, b, starts(call, req, tps, b, notes), notes);
	}

	private static Map<TopicPartition, Long> starts(ToolCall call, Request req, List<TopicPartition> tps,
			PartitionOffsets.Bounds b, List<String> notes) {
		Map<TopicPartition, Long> starts = new LinkedHashMap<>();
		switch (req.from()) {
			case EARLIEST -> tps.forEach(tp -> starts.put(tp, b.earliest(tp)));
			case OFFSET -> {
				long o = req.offset();
				List<Integer> below = new ArrayList<>();
				List<Integer> above = new ArrayList<>();
				for (TopicPartition tp : tps) {
					if (o < b.earliest(tp)) {
						below.add(tp.partition());
					}
					else if (o > b.latest(tp)) {
						above.add(tp.partition());
					}
					starts.put(tp, Math.max(b.earliest(tp), Math.min(o, b.latest(tp))));
				}
				if (!below.isEmpty()) {
					notes.add("offset " + o + " earliest'ten küçük, earliest'ten başlandı: partition " + list(below));
				}
				if (!above.isEmpty()) {
					notes.add("offset " + o + " latest'ten büyük, okunacak kayıt yok: partition " + list(above));
				}
			}
			case TIMESTAMP -> {
				Map<TopicPartition, PartitionOffsets.OffsetAt> at = PartitionOffsets.forTimestamp(call, tps,
						req.timestamp());
				List<Integer> none = new ArrayList<>();
				for (TopicPartition tp : tps) {
					PartitionOffsets.OffsetAt a = at.get(tp);
					if (a == null || !a.found()) {
						none.add(tp.partition());
						starts.put(tp, b.latest(tp));
					}
					else {
						starts.put(tp, Math.max(b.earliest(tp), a.offset()));
					}
				}
				if (!none.isEmpty()) {
					notes.add("Bu zamandan sonra kaydı olmayan partition: " + list(none));
				}
			}
			default -> throw new IllegalStateException();
		}
		return starts;
	}

	private static Result latest(ToolCall call, Request req, List<TopicPartition> tps, PartitionOffsets.Bounds b,
			List<String> notes) {
		boolean filtered = req.filter() != null;
		Map<TopicPartition, Long> demand = new HashMap<>();
		for (TopicPartition tp : tps) {
			demand.put(tp, filtered ? b.size(tp) : Math.min(b.size(tp), req.maxRecords()));
		}
		Map<TopicPartition, Long> size = RecordScanner.fairShare(demand, req.maxScan());
		boolean reduced = RecordScanner.sum(size) < RecordScanner.sum(demand);
		List<Window> windows = new ArrayList<>();
		for (TopicPartition tp : tps) {
			windows.add(new Window(tp, b.latest(tp) - size.get(tp), b.latest(tp)));
		}
		PriorityQueue<ConsumerRecord<byte[], byte[]>> heap = new PriorityQueue<>(BY_TIME);
		long[] matched = { 0 };
		RecordScanner.Result scan;
		try (KafkaConsumer<byte[], byte[]> consumer = call.newConsumer()) {
			scan = RecordScanner.scan(call, consumer, windows, r -> {
				if (filtered && !req.filter().test(r)) {
					return Next.CONTINUE;
				}
				matched[0]++;
				heap.add(r);
				if (heap.size() > req.maxRecords()) {
					heap.poll();
				}
				return Next.CONTINUE;
			});
		}
		List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>(heap);
		records.sort(BY_TIME);
		boolean approximate = !filtered && reduced;
		if (approximate) {
			notes.add("Partition başına okunan pencere maxScan (" + req.maxScan()
					+ ") ile sınırlandı; en yeni kayıt seçimi yaklaşık (daha kesin sonuç için maxScan'ı artırın)");
		}
		if (filtered && reduced) {
			notes.add("Sadece son " + RecordScanner.sum(size) + " kayıt tarandı (maxScan); daha eski eşleşmeler olabilir");
		}
		if (scan.deadlineReached()) {
			notes.add("Zaman sınırına yaklaşıldı; o ana kadar okunanlar döndü");
		}
		return new Result(records, tps, scan.scanned(), matched[0], scan.completed(), scan.deadlineReached(),
				filtered && reduced, approximate, scan.positions(), notes);
	}

	private static Result forward(ToolCall call, Request req, List<TopicPartition> tps, PartitionOffsets.Bounds b,
			Map<TopicPartition, Long> starts, List<String> notes) {
		boolean filtered = req.filter() != null;
		Map<TopicPartition, Long> avail = new HashMap<>();
		for (TopicPartition tp : tps) {
			avail.put(tp, Math.max(0, b.latest(tp) - starts.get(tp)));
		}
		List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
		List<Window> windows = new ArrayList<>();
		RecordScanner.Visitor visitor;
		boolean scanLimited = false;
		if (!filtered) {
			Map<TopicPartition, Long> quota = RecordScanner.fairShare(avail, req.maxRecords());
			Map<TopicPartition, Long> taken = new HashMap<>();
			for (TopicPartition tp : tps) {
				long start = starts.get(tp);
				windows.add(new Window(tp, start, quota.get(tp) > 0 ? b.latest(tp) : start));
			}
			visitor = r -> {
				records.add(r);
				TopicPartition tp = new TopicPartition(r.topic(), r.partition());
				long n = taken.merge(tp, 1L, Long::sum);
				return n >= quota.get(tp) ? Next.PARTITION_DONE : Next.CONTINUE;
			};
		}
		else {
			Map<TopicPartition, Long> size = RecordScanner.fairShare(avail, req.maxScan());
			scanLimited = RecordScanner.sum(size) < RecordScanner.sum(avail);
			for (TopicPartition tp : tps) {
				long start = starts.get(tp);
				windows.add(new Window(tp, start, start + size.get(tp)));
			}
			visitor = r -> {
				if (!req.filter().test(r)) {
					return Next.CONTINUE;
				}
				records.add(r);
				return records.size() >= req.maxRecords() ? Next.STOP : Next.CONTINUE;
			};
		}
		RecordScanner.Result scan;
		try (KafkaConsumer<byte[], byte[]> consumer = call.newConsumer()) {
			scan = RecordScanner.scan(call, consumer, windows, visitor);
		}
		records.sort(BY_OFFSET);
		Map<TopicPartition, Long> ends = new HashMap<>();
		tps.forEach(tp -> ends.put(tp, b.latest(tp)));
		boolean reachedEnd = scan.reached(ends);
		boolean scanLimitReached = filtered && scanLimited && scan.completed() && !reachedEnd;
		if (scanLimitReached) {
			notes.add("maxScan (" + req.maxScan() + ") kadar kayıt tarandı; devamı taranmadı");
		}
		if (scan.deadlineReached()) {
			notes.add("Zaman sınırına yaklaşıldı; o ana kadar okunanlar döndü");
		}
		return new Result(records, tps, scan.scanned(), records.size(), reachedEnd,
				scan.deadlineReached(), scanLimitReached, false, scan.positions(), notes);
	}

	private static String list(List<Integer> partitions) {
		if (partitions.size() <= MAX_LISTED_PARTITIONS) {
			return partitions.toString();
		}
		return partitions.subList(0, MAX_LISTED_PARTITIONS) + " (+" + (partitions.size() - MAX_LISTED_PARTITIONS)
				+ " partition daha)";
	}

}
