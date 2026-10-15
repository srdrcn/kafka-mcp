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
 * {@link ConsumerRecord}'u kompakt JSON map'ine çevirir: partition, offset, timestamp (ISO-8601 UTC) +
 * timestampType, çözülmüş key/value (kesilme/hata bayraklarıyla), header'lar (value ile aynı karakter sınırı;
 * kesilenler {@code headersTruncated} listesinde) ve serialized boyutlar.
 */
public final class RecordJson {

	/** UTF-8 olmayan header değerlerinin öneki. */
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
	 * Header'lar: ad → değer (tekrarlayan adlar liste olur; UTF-8 olmayanlar "hex:" önekli) ve değeri
	 * {@code maxChars}'ta kesilen header adları.
	 */
	public record HeaderValues(Map<String, Object> values, List<String> truncated) {
	}

	/** Header'ları çözer; {@code maxChars}'tan uzun değerler kesilir ve adları {@code truncated}'a eklenir. */
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

	/** Header değerinin tam (kesilmemiş) metni: UTF-8 ise metin, değilse "hex:..." ; null ise boş metin. */
	public static String headerText(byte[] value) {
		if (value == null) {
			return "";
		}
		String s = RecordDecoder.utf8(value);
		return s != null ? s : HEX_PREFIX + RecordDecoder.hex(value);
	}

}
