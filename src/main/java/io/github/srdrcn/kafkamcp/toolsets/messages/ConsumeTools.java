package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.NullNode;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient;
import io.github.srdrcn.kafkamcp.toolsets.messages.MessageReader.From;

/** Mesaj okuma araçları: messages_consume, messages_get, messages_headers. */
@Component
public class ConsumeTools {

	private static final int MAX_HEADER_KEYS = 200;

	private static final int HEADER_SAMPLES = 3;

	private static final int HEADER_SAMPLE_CHARS = 200;

	private final SchemaRegistryClient registry;

	public ConsumeTools(SchemaRegistryClient registry) {
		this.registry = registry;
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("messages_consume",
					"Topic'ten mesaj okur (group'suz, commit'siz): latest/earliest/offset/timestamp, key/value/header filtresi, format çözme")
			.required("topic", MessageParams.topic())
			.param("partition", MessageParams.partition())
			.param("from",
					P.enumOf("Başlangıç (varsayılan latest = son maxRecords kayıt; yeni veri beklenmez)", "earliest",
							"latest", "offset", "timestamp"))
			.param("offset",
					MessageParams.nonNegative("from=offset için başlangıç offset'i (seçilen her partition'a uygulanır)"))
			.param("timestamp", MessageParams.timestamp("from=timestamp için başlangıç zamanı"))
			.param("keyRegex", P.string("Key regex filtresi (çözülmüş metinde kısmi eşleşme)"))
			.param("valueRegex", P.string("Value regex filtresi (çözülmüş metinde kısmi eşleşme)"))
			.param("headerKey", P.string("Sadece bu header'ı taşıyan kayıtlar"))
			.param("headerValueRegex", P.string("headerKey değeri için regex (headerKey ile birlikte)"))
			.param("format", MessageParams.valueFormat())
			.param("keyFormat", MessageParams.keyFormat())
			.param("maxValueChars", MessageParams.maxValueChars())
			.param("maxScan", MessageParams.maxScan())
			.maxRecordsParam()
			.handler(this::consume),
				ToolSpec.read("messages_get", "Tek bir mesajı (topic/partition/offset) tam value'suyla getirir")
					.required("topic", MessageParams.topic())
					.required("partition", MessageParams.nonNegative("Partition"))
					.required("offset", MessageParams.nonNegative("Offset"))
					.param("format", MessageParams.valueFormat())
					.param("keyFormat", MessageParams.keyFormat())
					.handler(this::get),
				ToolSpec
					.read("messages_headers",
							"Okunan mesajlardaki header istatistikleri: anahtar başına sayı, farklı değer sayısı, örnek değerler")
					.required("topic", MessageParams.topic())
					.param("partition", MessageParams.partition())
					.param("from", P.enumOf("Başlangıç (varsayılan latest = son kayıtlar)", "earliest", "latest"))
					.maxRecordsParam()
					.handler(this::headers));
	}

	Object consume(ToolCall call) {
		String topic = call.str("topic");
		MessageFormat valueFormat = MessageParams.valueFormat(call);
		MessageFormat keyFormat = MessageParams.keyFormat(call);
		RecordDecoder.requireSupported(call, keyFormat, valueFormat);
		int maxChars = MessageParams.maxValueChars(call);
		From from = from(call);
		Long offset = null;
		Long timestamp = null;
		if (from == From.OFFSET) {
			offset = call.optLong("offset");
			if (offset == null || offset < 0) {
				throw new ToolException("from=offset için offset parametresi (>= 0) gerekli");
			}
		}
		if (from == From.TIMESTAMP) {
			timestamp = Timestamps.optional(call, "timestamp");
			if (timestamp == null) {
				throw new ToolException("from=timestamp için timestamp parametresi gerekli (ISO-8601 ya da epoch ms)");
			}
		}
		Integer partition = call.optInt("partition");
		RecordDecoder decoder = new RecordDecoder(registry, call);
		RecordFilter filter = RecordFilter.fromParams(call, decoder, keyFormat, valueFormat);
		MessageReader.Result r = MessageReader.read(call, new MessageReader.Request(topic, partition, from, offset,
				timestamp, call.maxRecords(), MessageParams.maxScan(call), filter));
		List<Map<String, Object>> records = new ArrayList<>();
		for (ConsumerRecord<byte[], byte[]> rec : r.records()) {
			records.add(RecordJson.render(rec, decoder, keyFormat, valueFormat, maxChars));
		}
		// hiçbirinde zaman damgası yoksa (v0 mesaj formatı) latest de partition/offset sırasıyla listelenir
		boolean allUntimed = !r.records().isEmpty() && r.records().stream().allMatch(MessageReader::untimed);
		boolean byTime = from == From.LATEST && !allUntimed;
		Map<String, Object> out = Results.map("topic", topic, "from", from.id(), "partition", partition,
				"partitions", partition == null ? r.partitions().size() : null, "offset", offset, "timestamp",
				timestamp == null ? null : Timestamps.iso(timestamp), "order", byTime ? "timestamp" : "partition,offset",
				"returned", records.size());
		if (filter != null) {
			out.put("filters", filter.describe());
			out.put("scanned", r.scanned());
			out.put("matched", r.matched());
		}
		out.put("reachedEnd", r.reachedEnd());
		if (r.scanLimitReached()) {
			out.put("scanLimitReached", true);
		}
		if (partition != null && from != From.LATEST) {
			// partition okunamadıysa (skippedPartitions) devam offset'i yok: latest verilirse partition atlanırdı
			Long next = r.nextOffsets().get(new TopicPartition(topic, partition));
			out.put("nextOffset", next == null ? NullNode.getInstance() : next);
		}
		status(out, r, decoder);
		out.put("records", records);
		return out;
	}

	/**
	 * Okuma/çözme durumunu çıktıya ekler: offlinePartitions, skippedPartitions, approximate, partial (okuma ya da
	 * şema isteği zaman sınırına takıldıysa ya da okunmayan partition varsa), schemaLookupsSkipped ve notes.
	 * {@code decoder} null olabilir.
	 */
	static void status(Map<String, Object> out, MessageReader.Result r, RecordDecoder decoder) {
		if (!r.offlinePartitions().isEmpty()) {
			out.put("offlinePartitions", r.offlinePartitions());
		}
		if (!r.skippedPartitions().isEmpty()) {
			out.put("skippedPartitions", r.skippedPartitions());
		}
		if (r.approximate()) {
			out.put("approximate", true);
		}
		if (r.partial() || decoder != null && decoder.timeLimited()) {
			out.put("partial", true);
		}
		List<String> notes = new ArrayList<>(r.notes());
		if (decoder != null && decoder.skippedSchemaLookups() > 0) {
			out.put("schemaLookupsSkipped", decoder.skippedSchemaLookups());
			notes.add(decoder.skipNote());
		}
		if (!notes.isEmpty()) {
			out.put("notes", notes);
		}
	}

	private static From from(ToolCall call) {
		String f = call.str("from", null);
		From from;
		if (f == null) {
			from = call.has("offset") ? From.OFFSET : call.has("timestamp") ? From.TIMESTAMP : From.LATEST;
		}
		else {
			from = From.parse(f, From.LATEST);
		}
		if (from != From.OFFSET && call.has("offset")) {
			throw new ToolException("offset parametresi sadece from=offset ile kullanılır");
		}
		if (from != From.TIMESTAMP && call.has("timestamp")) {
			throw new ToolException("timestamp parametresi sadece from=timestamp ile kullanılır");
		}
		return from;
	}

	Object get(ToolCall call) {
		String topic = call.str("topic");
		int partition = Math.toIntExact(call.requiredLong("partition"));
		long offset = call.requiredLong("offset");
		MessageFormat valueFormat = MessageParams.valueFormat(call);
		MessageFormat keyFormat = MessageParams.keyFormat(call);
		RecordDecoder.requireSupported(call, keyFormat, valueFormat);
		List<TopicPartition> tps = PartitionOffsets.select(call, topic, partition).online();
		TopicPartition tp = tps.get(0);
		PartitionOffsets.Bounds b = PartitionOffsets.bounds(call, tps);
		long earliest = b.earliest(tp);
		long latest = b.latest(tp);
		if (earliest >= latest) {
			throw new ToolException("Partition " + partition + " boş: okunacak kayıt yok (earliest=latest=" + latest + ")");
		}
		if (offset < earliest || offset >= latest) {
			throw new ToolException("Offset " + offset + " aralık dışında: partition " + partition + " için geçerli offset'ler "
					+ earliest + ".." + (latest - 1) + " (earliest=" + earliest + ", latest=" + latest + ")");
		}
		List<ConsumerRecord<byte[], byte[]>> found = new ArrayList<>(1);
		RecordScanner.Result scan;
		try (BoundedConsumer c = BoundedConsumer.open(call)) {
			scan = RecordScanner.scan(call, c.consumer(), List.of(new RecordScanner.Window(tp, offset, latest)), r -> {
				found.add(r);
				return RecordScanner.Next.STOP;
			});
		}
		if (found.isEmpty()) {
			if (scan.deadlineReached()) {
				throw new ToolException("Kayıt zaman sınırı içinde okunamadı (partition " + partition + ", offset "
						+ offset + ")");
			}
			throw new ToolException("Offset " + offset + " ile latest (" + latest
					+ ") arasında kayıt yok (compaction ya da transaction marker olabilir)");
		}
		ConsumerRecord<byte[], byte[]> rec = found.get(0);
		RecordDecoder decoder = new RecordDecoder(registry, call);
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("topic", topic);
		out.putAll(RecordJson.render(rec, decoder, keyFormat, valueFormat, MessageParams.FULL_VALUE_CHARS));
		if (rec.offset() != offset) {
			out.put("requestedOffset", offset);
			out.put("note", "İstenen offset'te kayıt yok (compaction ya da transaction marker); sonraki kayıt döndü");
		}
		if (decoder.timeLimited()) {
			out.put("partial", true);
		}
		return out;
	}

	Object headers(ToolCall call) {
		String topic = call.str("topic");
		Integer partition = call.optInt("partition");
		From from = From.parse(call.str("from", null), From.LATEST);
		if (from != From.LATEST && from != From.EARLIEST) {
			throw new ToolException("from sadece earliest ya da latest olabilir");
		}
		MessageReader.Result r = MessageReader.read(call, new MessageReader.Request(topic, partition, from, null, null,
				call.maxRecords(), MessageReader.DEFAULT_MAX_SCAN, null));
		Map<String, HeaderStat> stats = new LinkedHashMap<>();
		int withHeaders = 0;
		for (ConsumerRecord<byte[], byte[]> rec : r.records()) {
			Set<String> seenInRecord = new HashSet<>();
			for (Header h : rec.headers()) {
				HeaderStat s = stats.computeIfAbsent(h.key(), HeaderStat::new);
				s.add(RecordJson.headerText(h.value()), seenInRecord.add(h.key()));
			}
			if (!seenInRecord.isEmpty()) {
				withHeaders++;
			}
		}
		List<HeaderStat> sorted = new ArrayList<>(stats.values());
		sorted.sort(Comparator.comparingInt((HeaderStat s) -> -s.records).thenComparing(s -> s.key));
		List<Map<String, Object>> rows = new ArrayList<>();
		for (HeaderStat s : sorted.subList(0, Math.min(MAX_HEADER_KEYS, sorted.size()))) {
			rows.add(s.toMap());
		}
		Map<String, Object> out = Results.map("topic", topic, "from", from.id(), "partition", partition,
				"recordsRead", r.records().size(), "withHeaders", withHeaders, "withoutHeaders",
				r.records().size() - withHeaders, "headerKeys", stats.size());
		if (sorted.size() > MAX_HEADER_KEYS) {
			out.put("headerKeysSkipped", sorted.size() - MAX_HEADER_KEYS);
		}
		status(out, r, null);
		out.put("headers", rows);
		return out;
	}

	private static final class HeaderStat {

		private final String key;

		private int count;

		private int records;

		private final Set<String> distinct = new HashSet<>();

		private final List<String> samples = new ArrayList<>();

		private boolean samplesTruncated;

		HeaderStat(String key) {
			this.key = key;
		}

		/** {@code value}: tam (kesilmemiş) değer; farklı değer sayımı bunun üzerinden yapılır. */
		void add(String value, boolean firstInRecord) {
			count++;
			if (firstInRecord) {
				records++;
			}
			distinct.add(value);
			String sample = RecordDecoder.cut(value, HEADER_SAMPLE_CHARS);
			if (samples.size() < HEADER_SAMPLES && !samples.contains(sample)) {
				samples.add(sample);
				samplesTruncated |= sample.length() < value.length();
			}
		}

		Map<String, Object> toMap() {
			return Results.map("key", key, "count", count, "records", records, "distinctValues", distinct.size(),
					"samples", samples, "samplesTruncatedAt", samplesTruncated ? HEADER_SAMPLE_CHARS : null);
		}

	}

}
