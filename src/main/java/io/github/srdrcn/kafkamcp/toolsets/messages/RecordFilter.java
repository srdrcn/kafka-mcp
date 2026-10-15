package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Kayıt filtresi: keyRegex/valueRegex (çözülmüş metinde kısmi eşleşme), headerKey (+ headerValueRegex). Tüm
 * koşullar VE ile birleşir; null key/value regex'e uymaz.
 */
public final class RecordFilter implements Predicate<ConsumerRecord<byte[], byte[]>> {

	private final Pattern key;

	private final Pattern value;

	private final String headerKey;

	private final Pattern headerValue;

	private final RecordDecoder decoder;

	private final MessageFormat keyFormat;

	private final MessageFormat valueFormat;

	public RecordFilter(Pattern key, Pattern value, String headerKey, Pattern headerValue, RecordDecoder decoder,
			MessageFormat keyFormat, MessageFormat valueFormat) {
		if (headerValue != null && headerKey == null) {
			throw new ToolException("headerValueRegex için headerKey de verilmeli");
		}
		this.key = key;
		this.value = value;
		this.headerKey = headerKey;
		this.headerValue = headerValue;
		this.decoder = decoder;
		this.keyFormat = keyFormat;
		this.valueFormat = valueFormat;
	}

	/** keyRegex, valueRegex, headerKey, headerValueRegex parametrelerinden filtre; hiçbiri yoksa null. */
	public static RecordFilter fromParams(ToolCall call, RecordDecoder decoder, MessageFormat keyFormat,
			MessageFormat valueFormat) {
		RecordFilter f = new RecordFilter(call.regex("keyRegex"), call.regex("valueRegex"), call.str("headerKey", null),
				call.regex("headerValueRegex"), decoder, keyFormat, valueFormat);
		return f.isEmpty() ? null : f;
	}

	public boolean isEmpty() {
		return key == null && value == null && headerKey == null;
	}

	/** Çıktı için filtre özeti. */
	public Map<String, Object> describe() {
		return Results.map("keyRegex", key == null ? null : key.pattern(), "valueRegex",
				value == null ? null : value.pattern(), "headerKey", headerKey, "headerValueRegex",
				headerValue == null ? null : headerValue.pattern());
	}

	@Override
	public boolean test(ConsumerRecord<byte[], byte[]> r) {
		if (headerKey != null && !headerMatches(r)) {
			return false;
		}
		if (key != null && !matches(key, r.key(), keyFormat)) {
			return false;
		}
		return value == null || matches(value, r.value(), valueFormat);
	}

	private boolean headerMatches(ConsumerRecord<byte[], byte[]> r) {
		for (Header h : r.headers().headers(headerKey)) {
			if (headerValue == null) {
				return true;
			}
			byte[] v = h.value();
			String text = v == null ? "" : RecordDecoder.utf8(v);
			if (text == null) {
				text = RecordJson.HEX_PREFIX + RecordDecoder.hex(v);
			}
			if (headerValue.matcher(text).find()) {
				return true;
			}
		}
		return false;
	}

	private boolean matches(Pattern p, byte[] bytes, MessageFormat format) {
		if (bytes == null) {
			return false;
		}
		String text = decoder.decode(bytes, format, Integer.MAX_VALUE).text();
		return text != null && p.matcher(text).find();
	}

}
