package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.time.Instant;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import io.github.srdrcn.kafkamcp.mcp.Regexes;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Record filter: keyRegex/valueRegex (partial match on the decoded text), headerKey (+ headerValueRegex). All
 * conditions are combined with AND; a null key/value never matches a regex. A regex searches at most the first
 * {@link #MAX_FILTER_CHARS} characters of a decoded key/value/header and stops with
 * {@link Regexes.RegexTimeoutException} when the call is cancelled or the deadline passes (see {@link RecordScanner}).
 */
public final class RecordFilter implements Predicate<ConsumerRecord<byte[], byte[]>> {

	/** Characters of a decoded key/value/header value that a regex searches (the start of longer values). */
	public static final int MAX_FILTER_CHARS = 262_144;

	private final Pattern key;

	private final Pattern value;

	private final String headerKey;

	private final Pattern headerValue;

	private final RecordDecoder decoder;

	private final MessageFormat keyFormat;

	private final MessageFormat valueFormat;

	/** Regex matches stop after this instant (null: only when the thread is interrupted). */
	private final Instant deadline;

	public RecordFilter(Pattern key, Pattern value, String headerKey, Pattern headerValue, RecordDecoder decoder,
			MessageFormat keyFormat, MessageFormat valueFormat) {
		this(key, value, headerKey, headerValue, decoder, keyFormat, valueFormat, null);
	}

	public RecordFilter(Pattern key, Pattern value, String headerKey, Pattern headerValue, RecordDecoder decoder,
			MessageFormat keyFormat, MessageFormat valueFormat, Instant deadline) {
		if (headerValue != null && headerKey == null) {
			throw new ToolException("headerValueRegex requires headerKey");
		}
		this.key = key;
		this.value = value;
		this.headerKey = headerKey;
		this.headerValue = headerValue;
		this.decoder = decoder;
		this.keyFormat = keyFormat;
		this.valueFormat = valueFormat;
		this.deadline = deadline;
	}

	/**
	 * Filter from the keyRegex, valueRegex, headerKey and headerValueRegex parameters; null if none is given. Values
	 * are not trimmed (e.g. "ERROR " matches with the trailing space). Regex matches stop when the scan would stop
	 * ({@link RecordScanner#DEFAULT_RESERVE_MS} before the deadline), so the scan can return a partial result.
	 */
	public static RecordFilter fromParams(ToolCall call, RecordDecoder decoder, MessageFormat keyFormat,
			MessageFormat valueFormat) {
		RecordFilter f = new RecordFilter(MessageParams.rawRegex(call, "keyRegex"),
				MessageParams.rawRegex(call, "valueRegex"), MessageParams.raw(call, "headerKey"),
				MessageParams.rawRegex(call, "headerValueRegex"), decoder, keyFormat, valueFormat,
				call.softDeadline(RecordScanner.DEFAULT_RESERVE_MS));
		return f.isEmpty() ? null : f;
	}

	public boolean isEmpty() {
		return key == null && value == null && headerKey == null;
	}

	/** Filter summary for the output. */
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
			if (Regexes.find(headerValue, RecordDecoder.cut(text, MAX_FILTER_CHARS), deadline)) {
				return true;
			}
		}
		return false;
	}

	private boolean matches(Pattern p, byte[] bytes, MessageFormat format) {
		if (bytes == null) {
			return false;
		}
		String text = decoder.decode(bytes, format, MAX_FILTER_CHARS).text();
		return text != null && Regexes.find(p, RecordDecoder.cut(text, MAX_FILTER_CHARS), deadline);
	}

}
