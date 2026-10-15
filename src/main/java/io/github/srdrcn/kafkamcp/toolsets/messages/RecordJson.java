package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import com.fasterxml.jackson.databind.node.NullNode;

/**
 * Converts a {@link ConsumerRecord} into a compact JSON map: partition, offset, timestamp (ISO-8601 UTC) +
 * timestampType, decoded key/value (with truncation/error flags), headers (same character limit as the value;
 * truncated ones are listed in {@code headersTruncated}) and serialized sizes.
 */
public final class RecordJson {

	/** Prefix of non-UTF-8 header values. */
	public static final String HEX_PREFIX = "hex:";

	private RecordJson() {
	}

	public static Map<String, Object> render(ConsumerRecord<byte[], byte[]> r, RecordDecoder decoder,
			MessageFormat keyFormat, MessageFormat valueFormat, int maxChars) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("partition", r.partition());
		m.put("offset", r.offset());
		String ts = Timestamps.iso(r.timestamp());
		if (ts != null) {
			m.put("timestamp", ts);
			if (r.timestampType() != TimestampType.NO_TIMESTAMP_TYPE) {
				m.put("timestampType", r.timestampType().name);
			}
		}
		if (r.key() != null) {
			put(m, "key", decoder.decode(r.key(), keyFormat, maxChars));
		}
		if (r.value() == null) {
			m.put("value", NullNode.getInstance());
			m.put("tombstone", true);
		}
		else {
			put(m, "value", decoder.decode(r.value(), valueFormat, maxChars));
		}
		HeaderValues headers = headers(r.headers(), maxChars);
		if (!headers.values().isEmpty()) {
			m.put("headers", headers.values());
		}
		if (!headers.truncated().isEmpty()) {
			m.put("headersTruncated", headers.truncated());
		}
		if (r.serializedKeySize() >= 0) {
			m.put("keySize", r.serializedKeySize());
		}
		if (r.serializedValueSize() >= 0) {
			m.put("valueSize", r.serializedValueSize());
		}
		return m;
	}

	private static void put(Map<String, Object> m, String name, RecordDecoder.Decoded d) {
		m.put(name, d.value());
		if (d.truncated()) {
			m.put(name + "Truncated", true);
		}
		if (d.error() != null) {
			m.put(name + "Error", d.error());
		}
		if (d.schemaId() != null) {
			m.put(name + "SchemaId", d.schemaId());
		}
	}

	/**
	 * Headers: name → value (repeated names become a list; non-UTF-8 values get a "hex:" prefix) and the names of
	 * headers whose value was truncated at {@code maxChars}.
	 */
	public record HeaderValues(Map<String, Object> values, List<String> truncated) {
	}

	/** Decodes the headers; values longer than {@code maxChars} are truncated and their names added to {@code truncated}. */
	public static HeaderValues headers(Headers headers, int maxChars) {
		Map<String, Object> out = new LinkedHashMap<>();
		Set<String> truncated = new LinkedHashSet<>();
		if (headers == null) {
			return new HeaderValues(out, List.of());
		}
		for (Header h : headers) {
			String full = headerText(h.value());
			String v = RecordDecoder.cut(full, maxChars);
			if (v.length() < full.length()) {
				truncated.add(h.key());
			}
			Object prev = out.get(h.key());
			if (prev == null) {
				out.put(h.key(), v);
			}
			else if (prev instanceof List<?> l) {
				@SuppressWarnings("unchecked")
				List<Object> list = (List<Object>) l;
				list.add(v);
			}
			else {
				List<Object> list = new ArrayList<>();
				list.add(prev);
				list.add(v);
				out.put(h.key(), list);
			}
		}
		return new HeaderValues(out, new ArrayList<>(truncated));
	}

	/** The full (untruncated) text of a header value: the text if UTF-8, otherwise "hex:..."; empty text if null. */
	public static String headerText(byte[] value) {
		if (value == null) {
			return "";
		}
		String s = RecordDecoder.utf8(value);
		return s != null ? s : HEX_PREFIX + RecordDecoder.hex(value);
	}

}
