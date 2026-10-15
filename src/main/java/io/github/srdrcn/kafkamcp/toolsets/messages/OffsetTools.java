package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.NullNode;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.messages.PartitionOffsets.OffsetAt;

/** Offset calculation tools: messages_offset_for_timestamp, messages_count, messages_partition_for_key. */
@Component
public class OffsetTools {

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec
					.read("messages_offset_for_timestamp",
							"Find, per partition, the offset and time of the first message at or after the given time")
					.required("topic", MessageParams.topic())
					.required("timestamp", MessageParams.timestamp("Time"))
					.param("partition", MessageParams.partition())
					.handler(this::offsetForTimestamp),
				ToolSpec
					.read("messages_count",
							"Count messages in an offset or time range (offset difference per partition; approximate)")
					.required("topic", MessageParams.topic())
					.param("partition", MessageParams.partition())
					.param("fromOffset", MessageParams.nonNegative("Start offset (inclusive; default earliest)"))
					.param("toOffset", MessageParams.nonNegative("End offset (exclusive; default latest)"))
					.param("fromTimestamp", MessageParams.timestamp("Start time (inclusive)"))
					.param("toTimestamp", MessageParams.timestamp("End time (exclusive)"))
					.handler(this::count),
				ToolSpec
					.read("messages_partition_for_key",
							"Compute the partition a key maps to with Kafka's default partitioner (murmur2)")
					.required("topic", MessageParams.topic())
					.required("key", P.string("Key (text, hex or number depending on keyFormat); text is hashed as is, including whitespace"))
					.param("keyFormat",
							MessageParams.format("Key", List.of("string", "hex", "long"), "string"))
					.handler(this::partitionForKey));
	}

	Object offsetForTimestamp(ToolCall call) {
		String topic = call.str("topic");
		long ts = Timestamps.required(call, "timestamp");
		PartitionOffsets.Selection sel = PartitionOffsets.select(call, topic, call.optInt("partition"));
		List<TopicPartition> tps = sel.online();
		ListOffsetsResult byTime = PartitionOffsets.request(call, tps, OffsetSpec.forTimestamp(ts));
		ListOffsetsResult latest = PartitionOffsets.request(call, tps, OffsetSpec.latest());
		// old ZooKeeper brokers (v0 message format / old protocol) may reject the time lookup per partition: those
		// partitions get an error note, the others are returned
		PartitionOffsets.Lookup at = PartitionOffsets.awaitEach(call, tps, byTime);
		Map<TopicPartition, OffsetAt> ends = PartitionOffsets.await(call, latest);
		List<Map<String, Object>> rows = new ArrayList<>();
		int found = 0;
		for (TopicPartition tp : tps) {
			OffsetAt a = at.offsets().get(tp);
			String unsupported = at.unsupported().get(tp);
			Map<String, Object> row = Results.map("partition", tp.partition());
			if (a != null && a.found()) {
				found++;
				row.put("offset", a.offset());
				row.put("timestamp", Timestamps.iso(a.timestamp()));
			}
			else {
				row.put("offset", NullNode.getInstance());
				row.put("latestOffset", ends.get(tp).offset());
				if (unsupported != null) {
					row.put("error", unsupported);
				}
			}
			rows.add(row);
		}
		int failed = at.unsupported().size();
		int notFound = tps.size() - found - failed;
		Map<String, Object> out = Results.map("topic", topic, "timestamp", Timestamps.iso(ts), "found", found,
				"notFound", notFound, "unsupported", failed > 0 ? failed : null);
		if (notFound > 0) {
			out.put("note", "offset null: no records at or after this time in that partition (new records will start at latestOffset)");
		}
		if (failed > 0) {
			out.put("unsupportedNote", unsupportedNote(at));
		}
		offline(out, sel);
		out.put("partitions", rows);
		return out;
	}

	/** Note for the partitions that do not support time lookups (grouped by reason). */
	private static String unsupportedNote(PartitionOffsets.Lookup at) {
		List<String> parts = new ArrayList<>();
		at.unsupportedByReason().forEach((reason, ps) -> parts.add("partition " + ps + ": " + reason));
		return "Could not look up offsets by time (" + String.join("; ", parts) + ")";
	}

	Object count(ToolCall call) {
		String topic = call.str("topic");
		Long fromOffset = call.optLong("fromOffset");
		Long toOffset = call.optLong("toOffset");
		Long fromTs = Timestamps.optional(call, "fromTimestamp");
		Long toTs = Timestamps.optional(call, "toTimestamp");
		if ((fromOffset != null || toOffset != null) && (fromTs != null || toTs != null)) {
			throw new ToolException("Offset bounds (fromOffset/toOffset) and time bounds "
					+ "(fromTimestamp/toTimestamp) cannot be used together");
		}
		if (fromOffset != null && toOffset != null && toOffset < fromOffset) {
			throw new ToolException("toOffset cannot be less than fromOffset");
		}
		if (fromTs != null && toTs != null && toTs < fromTs) {
			throw new ToolException("toTimestamp cannot be before fromTimestamp");
		}
		PartitionOffsets.Selection sel = PartitionOffsets.select(call, topic, call.optInt("partition"));
		List<TopicPartition> tps = sel.online();
		ListOffsetsResult e = PartitionOffsets.request(call, tps, OffsetSpec.earliest());
		ListOffsetsResult l = PartitionOffsets.request(call, tps, OffsetSpec.latest());
		ListOffsetsResult f = fromTs == null ? null : PartitionOffsets.request(call, tps, OffsetSpec.forTimestamp(fromTs));
		ListOffsetsResult t = toTs == null ? null : PartitionOffsets.request(call, tps, OffsetSpec.forTimestamp(toTs));
		Map<TopicPartition, OffsetAt> earliest = PartitionOffsets.await(call, e);
		Map<TopicPartition, OffsetAt> latest = PartitionOffsets.await(call, l);
		// time bounds: partitions without time lookup support (old ZooKeeper brokers) are left out of the count with a note
		PartitionOffsets.Lookup fromLookup = f == null ? null : PartitionOffsets.awaitEach(call, tps, f);
		PartitionOffsets.Lookup toLookup = t == null ? null : PartitionOffsets.awaitEach(call, tps, t);
		Map<TopicPartition, OffsetAt> fromAt = fromLookup == null ? null : fromLookup.offsets();
		Map<TopicPartition, OffsetAt> toAt = toLookup == null ? null : toLookup.offsets();
		Map<TopicPartition, String> unsupported = new LinkedHashMap<>();
		if (fromLookup != null) {
			unsupported.putAll(fromLookup.unsupported());
		}
		if (toLookup != null) {
			toLookup.unsupported().forEach(unsupported::putIfAbsent);
		}
		List<Map<String, Object>> rows = new ArrayList<>();
		long total = 0;
		for (TopicPartition tp : tps) {
			String reason = unsupported.get(tp);
			if (reason != null) {
				rows.add(Results.map("partition", tp.partition(), "count", NullNode.getInstance(), "error", reason));
				continue;
			}
			long lo = earliest.get(tp).offset();
			long hi = latest.get(tp).offset();
			long start = lo;
			long end = hi;
			if (fromOffset != null) {
				start = fromOffset;
			}
			if (toOffset != null) {
				end = toOffset;
			}
			if (fromAt != null) {
				OffsetAt a = fromAt.get(tp);
				start = a != null && a.found() ? a.offset() : hi;
			}
			if (toAt != null) {
				OffsetAt a = toAt.get(tp);
				end = a != null && a.found() ? a.offset() : hi;
			}
			start = Math.max(lo, Math.min(start, hi));
			end = Math.max(start, Math.min(end, hi));
			long n = end - start;
			total += n;
			rows.add(Results.map("partition", tp.partition(), "start", start, "end", end, "count", n));
		}
		Map<String, Object> out = Results.map("topic", topic, "fromOffset", fromOffset, "toOffset", toOffset,
				"fromTimestamp", fromTs == null ? null : Timestamps.iso(fromTs), "toTimestamp",
				toTs == null ? null : Timestamps.iso(toTs), "total", total, "approximate", true, "note",
				"The count is an offset difference (end exclusive): compacted or transactional topics may hold fewer actual records");
		if (!unsupported.isEmpty()) {
			out.put("partial", true);
			out.put("unsupported", unsupported.size());
			out.put("unsupportedNote", unsupportedNote(new PartitionOffsets.Lookup(Map.of(), unsupported))
					+ "; total does not include these partitions");
		}
		offline(out, sel);
		out.put("partitions", rows);
		return out;
	}

	/** Adds the leaderless (skipped) partitions to the output. */
	private static void offline(Map<String, Object> out, PartitionOffsets.Selection sel) {
		if (!sel.offline().isEmpty()) {
			out.put("offlinePartitions", sel.offline());
			out.put("offlineNote", sel.note());
		}
	}

	Object partitionForKey(ToolCall call) {
		String topic = call.str("topic");
		// raw value: the producer hashes the key bytes as is; " a " and "a" can map to different partitions, and
		// the empty string ("") is a valid (non-null) key
		Object raw = call.args().get("key");
		if (raw == null) {
			throw new ToolException("Missing parameter: key");
		}
		String key = String.valueOf(raw);
		String format = call.str("keyFormat", "string").toLowerCase(Locale.ROOT);
		if (!"string".equals(format) && key.isBlank()) {
			throw new ToolException("Missing parameter: key (keyFormat=" + format + ")");
		}
		byte[] bytes = switch (format) {
			case "string" -> key.getBytes(StandardCharsets.UTF_8);
			case "hex" -> parseHex(key);
			case "long" -> {
				try {
					yield ByteBuffer.allocate(Long.BYTES).putLong(Long.parseLong(key.trim())).array();
				}
				catch (NumberFormatException ex) {
					throw new ToolException("key must be an integer for keyFormat=long: " + key);
				}
			}
			default -> throw new ToolException("Invalid keyFormat: " + format + " (valid: string|hex|long)");
		};
		int partitions = PartitionOffsets.partitionCount(call, topic);
		int partition = Utils.toPositive(Utils.murmur2(bytes)) % partitions;
		return Results.map("topic", topic, "key", key, "keyFormat", format, "keyHex", RecordDecoder.hex(bytes),
				"keyBytes", bytes.length, "partition", partition, "partitionCount", partitions, "note",
				"Applies to Kafka's default partitioner (murmur2); producers that use a custom partitioner or set the "
						+ "partition explicitly may differ. The result changes if the partition count changes.");
	}

	private static byte[] parseHex(String key) {
		String h = key.replaceAll("\\s+", "");
		if (h.startsWith("0x") || h.startsWith("0X")) {
			h = h.substring(2);
		}
		try {
			return HexFormat.of().parseHex(h);
		}
		catch (IllegalArgumentException ex) {
			throw new ToolException("Invalid hex for keyFormat=hex: " + key);
		}
	}

}
