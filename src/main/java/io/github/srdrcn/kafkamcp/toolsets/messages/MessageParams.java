package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** messages araçlarının ortak parametre şemaları ve okuyucuları. */
final class MessageParams {

	static final int MAX_VALUE_CHARS_LIMIT = 100_000;

	static final int FULL_VALUE_CHARS = 1_000_000;

	private MessageParams() {
	}

	static Map<String, Object> topic() {
		return P.string("Topic adı");
	}

	static Map<String, Object> partition() {
		return nonNegative("Partition (boşsa tüm partition'lar)");
	}

	/** En az 0 olan tamsayı (üst sınır yok). */
	static Map<String, Object> nonNegative(String description) {
		Map<String, Object> m = P.integer(description);
		m.put("minimum", 0);
		return m;
	}

	static Map<String, Object> format(String what, List<String> values, String defaultValue) {
		Map<String, Object> m = P.enumOf(what + " formatı (varsayılan " + defaultValue + ")", values);
		m.put("default", defaultValue);
		return m;
	}

	static Map<String, Object> valueFormat() {
		return format("Value", MessageFormat.ids(), "string");
	}

	static Map<String, Object> keyFormat() {
		return format("Key", MessageFormat.ids(), "string");
	}

	static Map<String, Object> maxValueChars() {
		Map<String, Object> m = P.integer("Value/key için en fazla karakter; fazlası kesilir (varsayılan "
				+ RecordDecoder.DEFAULT_MAX_CHARS + ")", 16, MAX_VALUE_CHARS_LIMIT);
		m.put("default", RecordDecoder.DEFAULT_MAX_CHARS);
		return m;
	}

	static Map<String, Object> maxScan() {
		Map<String, Object> m = P.integer("Filtrede/latest'te taranacak en fazla kayıt (varsayılan "
				+ MessageReader.DEFAULT_MAX_SCAN + ")", 1, MessageReader.MAX_SCAN_LIMIT);
		m.put("default", MessageReader.DEFAULT_MAX_SCAN);
		return m;
	}

	static Map<String, Object> timestamp(String description) {
		return P.string(description + ": ISO-8601 (ör. 2025-01-31T10:00:00Z) ya da epoch ms");
	}

	static MessageFormat valueFormat(ToolCall call) {
		return MessageFormat.parse("format", call.str("format", null), MessageFormat.STRING);
	}

	static MessageFormat keyFormat(ToolCall call) {
		return MessageFormat.parse("keyFormat", call.str("keyFormat", null), MessageFormat.STRING);
	}

	static int maxValueChars(ToolCall call) {
		return call.integer("maxValueChars", RecordDecoder.DEFAULT_MAX_CHARS, 16, MAX_VALUE_CHARS_LIMIT);
	}

	static int maxScan(ToolCall call) {
		return call.integer("maxScan", MessageReader.DEFAULT_MAX_SCAN, 1, MessageReader.MAX_SCAN_LIMIT);
	}

	/**
	 * Ham string argüman (trim edilmez: key/regex/header adındaki boşluklar anlamlıdır); yoksa ya da boş string
	 * ise null.
	 */
	static String raw(ToolCall call, String name) {
		Object v = call.args().get(name);
		if (v == null) {
			return null;
		}
		String s = String.valueOf(v);
		return s.isEmpty() ? null : s;
	}

	/** Ham (trim edilmemiş) regex argümanını derler; yoksa null. */
	static Pattern rawRegex(ToolCall call, String name) {
		String s = raw(call, name);
		if (s == null) {
			return null;
		}
		try {
			return Pattern.compile(s);
		}
		catch (PatternSyntaxException e) {
			throw new ToolException("Geçersiz regex (" + name + "): " + e.getDescription());
		}
	}

}
