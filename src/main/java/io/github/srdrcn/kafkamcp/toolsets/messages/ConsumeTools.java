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

/** Message reading tools: messages_consume, messages_get, messages_headers. */
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
					"Read messages without a consumer group or commit: from latest/earliest/offset/timestamp, key/value/header filters, format decoding")
			.required("topic", MessageParams.topic())
			.param("partition", MessageParams.partition())
			.param("from",
					P.enumOf("Start position (default latest = the last maxRecords records; does not wait for new data)", "earliest",
							"latest", "offset", "timestamp"))
			.param("offset",
					MessageParams.nonNegative("Start offset for from=offset (applied to every selected partition)"))
			.param("timestamp", MessageParams.timestamp("Start time for from=timestamp"))
			.param("keyRegex", P.string("Key regex filter (partial match on the decoded text)"))
			.param("valueRegex", P.string("Value regex filter (partial match on the decoded text)"))
			.param("headerKey", P.string("Only records that carry this header"))
			.param("headerValueRegex", P.string("Regex for the headerKey value (requires headerKey)"))
			.param("format", MessageParams.valueFormat())
			.param("keyFormat", MessageParams.keyFormat())
			.param("maxValueChars", MessageParams.maxValueChars())
			.param("maxScan", MessageParams.maxScan())
			.maxRecordsParam()
			.handler(this::consume),
				ToolSpec.read("messages_get", "Get a single message (topic/partition/offset) with its full value")
					.required("topic", MessageParams.topic())
					.required("partition", MessageParams.nonNegative("Partition"))
					.required("offset", MessageParams.nonNegative("Offset"))
					.param("format", MessageParams.valueFormat())
					.param("keyFormat", MessageParams.keyFormat())
					.handler(this::get),
				ToolSpec
					.read("messages_headers",
							"Summarize headers of the latest (or earliest) messages: count per key, distinct value count, sample values")
					.required("topic", MessageParams.topic())
					.param("partition", MessageParams.partition())
					.param("from", P.enumOf("Start position (default latest = the most recent records)", "earliest", "latest"))
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
				throw new ToolException("from=offset requires the offset parameter (>= 0)");
			}
		}
		if (from == From.TIMESTAMP) {
			timestamp = Timestamps.optional(call, "timestamp");
			if (timestamp == null) {
				throw new ToolException("from=timestamp requires the timestamp parameter (ISO-8601 or epoch ms)");
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
		// if no record has a timestamp (v0 message format), latest is also listed in partition/offset order
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
			// if the partition could not be read (skippedPartitions) there is no next offset: returning latest would skip it
			Long next = r.nextOffsets().get(new TopicPartition(topic, partition));
			out.put("nextOffset", next == null ? NullNode.getInstance() : next);
		}
		status(out, r, decoder);
		out.put("records", records);
		return out;
	}

	/**
	 * Adds the read/decode status to the output: offlinePartitions, skippedPartitions, approximate, partial (if
	 * reading or a schema lookup hit the time limit, or a partition was not read), schemaLookupsSkipped and notes.
	 * {@code decoder} may be null.
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
			throw new ToolException("The offset parameter can only be used with from=offset");
		}
		if (from != From.TIMESTAMP && call.has("timestamp")) {
			throw new ToolException("The timestamp parameter can only be used with from=timestamp");
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
			throw new ToolException("Partition " + partition + " is empty: no records to read (earliest=latest=" + latest + ")");
		}
		if (offset < earliest || offset >= latest) {
			throw new ToolException("Offset " + offset + " is out of range: valid offsets for partition " + partition + " are "
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
				throw new ToolException("Could not read the record within the time limit (partition " + partition
						+ ", offset " + offset + ")");
			}
			throw new ToolException("No records between offset " + offset + " and latest (" + latest
					+ ") (possibly compaction or transaction markers)");
		}
		ConsumerRecord<byte[], byte[]> rec = found.get(0);
		RecordDecoder decoder = new RecordDecoder(registry, call);
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("topic", topic);
		out.putAll(RecordJson.render(rec, decoder, keyFormat, valueFormat, MessageParams.FULL_VALUE_CHARS));
		if (rec.offset() != offset) {
			out.put("requestedOffset", offset);
			out.put("note", "No record at the requested offset (compaction or transaction marker); returned the next record");
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
			throw new ToolException("from must be earliest or latest");
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

		/** {@code value}: the full (untruncated) value; distinct values are counted on it. */
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
