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
 * maxRecords kayıt. Okunan toplam pencere maxScan ile sınırlıdır. Transaction marker'ları/compaction
 * boşlukları yüzünden pencereden az kayıt çıkan partition'ların penceresi geriye genişletilir.</li>
 * <li>earliest/offset/timestamp: başlangıçtan ileri; filtresizken maxRecords partition'lara adil bölünür,
 * filtreliyken maxScan kadar kayıt taranır ve ilk maxRecords eşleşme döner.</li>
 * </ul>
 */
public final class MessageReader {

	public static final int DEFAULT_MAX_SCAN = 10_000;

	public static final int MAX_SCAN_LIMIT = 100_000;

	private static final int MAX_LISTED_PARTITIONS = 20;

	/** latest'te az kayıt çıkan partition pencerelerini geriye genişletme turu sınırı. */
	private static final int MAX_EXTEND_ROUNDS = 6;

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
	 * {@code partitions}: okunan (lideri olan) partition'lar; {@code offlinePartitions}: lideri olmadığı için
	 * atlananlar. {@code nextOffsets}: partition başına bir sonraki okunacak offset.
	 */
	public record Result(List<ConsumerRecord<byte[], byte[]>> records, List<TopicPartition> partitions,
			List<Integer> offlinePartitions, long scanned, long matched, boolean reachedEnd, boolean partial,
			boolean scanLimitReached, boolean approximate, Map<TopicPartition, Long> nextOffsets, List<String> notes) {
	}

	public static Result read(ToolCall call, Request req) {
		PartitionOffsets.Selection sel = PartitionOffsets.select(call, req.topic(), req.partition());
		List<TopicPartition> tps = sel.online();
		List<String> notes = new ArrayList<>();
		if (sel.note() != null) {
			notes.add(sel.note());
		}
		PartitionOffsets.Bounds b = PartitionOffsets.bounds(call, tps);
		if (req.from() == From.LATEST) {
			return latest(call, req, sel, b, notes);
		}
		return forward(call, req, sel, b, starts(call, req, tps, b, notes), notes);
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

	private static Result latest(ToolCall call, Request req, PartitionOffsets.Selection sel, PartitionOffsets.Bounds b,
			List<String> notes) {
		List<TopicPartition> tps = sel.online();
		boolean filtered = req.filter() != null;
		Map<TopicPartition, Long> demand = new HashMap<>();
		for (TopicPartition tp : tps) {
			demand.put(tp, filtered ? b.size(tp) : Math.min(b.size(tp), req.maxRecords()));
		}
		Map<TopicPartition, Long> size = RecordScanner.fairShare(demand, req.maxScan());
		boolean reduced = RecordScanner.sum(size) < RecordScanner.sum(demand);
		Map<TopicPartition, Long> starts = new HashMap<>();
		List<Window> windows = new ArrayList<>();
		for (TopicPartition tp : tps) {
			long start = b.latest(tp) - size.get(tp);
			starts.put(tp, start);
			windows.add(new Window(tp, start, b.latest(tp)));
		}
		PriorityQueue<ConsumerRecord<byte[], byte[]>> heap = new PriorityQueue<>(BY_TIME);
		Map<TopicPartition, Long> got = new HashMap<>();
		long[] matched = { 0 };
		RecordScanner.Visitor visitor = r -> {
			got.merge(new TopicPartition(r.topic(), r.partition()), 1L, Long::sum);
			if (filtered && !req.filter().test(r)) {
				return Next.CONTINUE;
			}
			matched[0]++;
			heap.add(r);
			if (heap.size() > req.maxRecords()) {
				heap.poll();
			}
			return Next.CONTINUE;
		};
		RecordScanner.Result scan;
		long scanned;
		boolean completed;
		boolean deadline;
		List<Integer> shortfall = List.of();
		try (BoundedConsumer c = BoundedConsumer.open(call)) {
			scan = RecordScanner.scan(call, c.consumer(), windows, visitor);
			scanned = scan.scanned();
			completed = scan.completed();
			deadline = scan.deadlineReached();
			if (!filtered) {
				// transaction marker'ları ve compaction boşlukları offset kaplar: pencereden az kayıt çıkan
				// partition'ların penceresi (maxScan bütçesiyle) geriye genişletilir
				long budget = req.maxScan() - RecordScanner.sum(size);
				for (int round = 0; round < MAX_EXTEND_ROUNDS && completed; round++) {
					Map<TopicPartition, Long> grant = RecordScanner.fairShare(extension(tps, b, size, starts, got),
							budget);
					if (RecordScanner.sum(grant) == 0) {
						break;
					}
					budget -= RecordScanner.sum(grant);
					List<Window> more = new ArrayList<>();
					for (TopicPartition tp : tps) {
						long g = grant.getOrDefault(tp, 0L);
						if (g > 0) {
							long start = starts.get(tp);
							more.add(new Window(tp, start - g, start));
							starts.put(tp, start - g);
						}
					}
					RecordScanner.Result next = RecordScanner.scan(call, c.consumer(), more, visitor);
					scanned += next.scanned();
					completed = next.completed();
					deadline = next.deadlineReached();
				}
				if (completed) {
					shortfall = shortfall(tps, b, size, starts, got);
				}
			}
		}
		List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>(heap);
		records.sort(BY_TIME);
		boolean approximate = !filtered && (reduced || !shortfall.isEmpty());
		if (!filtered && reduced) {
			notes.add("Partition başına okunan pencere maxScan (" + req.maxScan()
					+ ") ile sınırlandı; en yeni kayıt seçimi yaklaşık (daha kesin sonuç için maxScan'ı artırın)");
		}
		if (!shortfall.isEmpty()) {
			notes.add("Partition " + list(shortfall) + ": transaction marker/compaction boşlukları nedeniyle son "
					+ "offset'lerde istenenden az kayıt vardı; geriye genişletme sınırına (maxScan=" + req.maxScan()
					+ ") takıldı, bu partition'ların daha eski kayıtları eksik olabilir");
		}
		if (filtered && reduced) {
			notes.add("Sadece son " + RecordScanner.sum(size) + " kayıt tarandı (maxScan); daha eski eşleşmeler olabilir");
		}
		if (deadline) {
			notes.add("Zaman sınırına yaklaşıldı; o ana kadar okunanlar döndü");
		}
		return new Result(records, tps, sel.offline(), scanned, matched[0], completed, deadline, filtered && reduced,
				approximate, scan.positions(), notes);
	}

	/**
	 * Penceresinden hedefinden ({@code size}) az kayıt çıkan ve geriye yeri olan partition'lar için ek pencere
	 * boyu (offset): gözlenen offset/kayıt oranıyla eksik kayıt sayısına göre tahmin.
	 */
	private static Map<TopicPartition, Long> extension(List<TopicPartition> tps, PartitionOffsets.Bounds b,
			Map<TopicPartition, Long> size, Map<TopicPartition, Long> starts, Map<TopicPartition, Long> got) {
		Map<TopicPartition, Long> ext = new HashMap<>();
		for (TopicPartition tp : tps) {
			long have = got.getOrDefault(tp, 0L);
			long deficit = size.get(tp) - have;
			long room = starts.get(tp) - b.earliest(tp);
			if (deficit <= 0 || room <= 0) {
				continue;
			}
			long read = b.latest(tp) - starts.get(tp);
			long need = have == 0 ? 2 * read : Math.ceilDiv(deficit * read, have);
			ext.put(tp, Math.min(room, Math.max(deficit, need)));
		}
		return ext;
	}

	/** Hedefinden az kayıt çıkan ama geriye daha okunmamış offset'i kalan partition'lar. */
	private static List<Integer> shortfall(List<TopicPartition> tps, PartitionOffsets.Bounds b,
			Map<TopicPartition, Long> size, Map<TopicPartition, Long> starts, Map<TopicPartition, Long> got) {
		List<Integer> out = new ArrayList<>();
		for (TopicPartition tp : tps) {
			if (got.getOrDefault(tp, 0L) < size.get(tp) && starts.get(tp) > b.earliest(tp)) {
				out.add(tp.partition());
			}
		}
		return out;
	}

	private static Result forward(ToolCall call, Request req, PartitionOffsets.Selection sel,
			PartitionOffsets.Bounds b, Map<TopicPartition, Long> starts, List<String> notes) {
		List<TopicPartition> tps = sel.online();
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
		try (BoundedConsumer c = BoundedConsumer.open(call)) {
			scan = RecordScanner.scan(call, c.consumer(), windows, visitor);
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
		return new Result(records, tps, sel.offline(), scan.scanned(), records.size(), reachedEnd,
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
