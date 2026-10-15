package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Masks secrets in connector configs, in two ways:
 * <ul>
 * <li>by key: the whole value is replaced when the key contains (case-insensitive) password/passwd/passphrase/
 * secret/token/credential/jaas/user.info/authorization/connection.string, when one of its segments (split on
 * {@code . _ -}) is pwd/sas/auth/private, when its last segment is header/headers, or when its last dotted segment
 * contains "key" (api.key, private.key, apikey; but not key.converter/key.serializer);</li>
 * <li>by value, for every other string ({@link #scrub(String)}): URL user info ({@code //user:pass@}), Oracle
 * {@code user/pass@}, {@code password=}/{@code pwd=}/{@code SharedAccessKey=}/{@code AccountKey=}/{@code sig=}/
 * {@code token=} pairs (JDBC URLs, connection strings) and {@code Bearer}/{@code Basic} credentials.</li>
 * </ul>
 */
public final class ConnectSecrets {

	public static final String MASK = "******";

	private static final List<String> SECRET_WORDS = List.of("password", "passwd", "passphrase", "secret", "token",
			"credential", "jaas", "user.info", "authorization", "connection.string", "connectionstring");

	private static final Set<String> SECRET_SEGMENTS = Set.of("pwd", "sas", "auth", "private");

	private static final Set<String> SECRET_LAST_SEGMENTS = Set.of("header", "headers");

	private static final Pattern SEGMENT_SEPARATOR = Pattern.compile("[._-]");

	/** scheme://user:pass@host → scheme://***@host (up to the last '@' before the path; bounded to stay linear). */
	private static final Pattern URL_USER_INFO = Pattern.compile("(?i)\\b([a-z][a-z0-9+.-]*://)[^/\\s]{1,256}@");

	/** jdbc:oracle:thin:user/pass@host → jdbc:oracle:thin:***@host */
	private static final Pattern ORACLE_USER_PASS = Pattern
		.compile("(?i)\\b(jdbc:oracle:[a-z0-9]+:)[^/@\\s]{1,256}/[^@\\s]{1,256}@");

	/** password=..., SharedAccessKey=... inside JDBC URLs and connection strings (up to ; & , or whitespace). */
	private static final Pattern SECRET_PAIR = Pattern.compile(
			"(?i)\\b(password|passwd|pwd|secret|sharedaccesskey|sharedaccesssignature|accountkey|sig|token|access_token|apikey|api_key)(\\s*=\\s*)[^;&,\\s]+");

	/** Authorization header values. */
	private static final Pattern AUTH_SCHEME = Pattern.compile("(?i)\\b(bearer|basic)(\\s+)[A-Za-z0-9._~+/=-]{8,}");

	private ConnectSecrets() {
	}

	/** Is the value of this config key secret? */
	public static boolean isSecret(String key) {
		if (key == null) {
			return false;
		}
		String k = key.toLowerCase(Locale.ROOT);
		for (String w : SECRET_WORDS) {
			if (k.contains(w)) {
				return true;
			}
		}
		String[] segments = SEGMENT_SEPARATOR.split(k);
		for (String s : segments) {
			if (SECRET_SEGMENTS.contains(s)) {
				return true;
			}
		}
		if (segments.length > 0 && SECRET_LAST_SEGMENTS.contains(segments[segments.length - 1])) {
			return true;
		}
		String last = k.substring(k.lastIndexOf('.') + 1);
		return last.contains("key");
	}

	/**
	 * Replaces the (non-null) value of a secret key with {@link #MASK}; scrubs credentials out of other string values
	 * ({@link #scrub(String)}); returns other values unchanged.
	 */
	public static Object mask(String key, Object value) {
		if (value == null) {
			return null;
		}
		if (isSecret(key)) {
			return MASK;
		}
		return value instanceof String s ? scrub(s) : value;
	}

	/** Masked copy of the config, sorted by key. */
	public static Map<String, Object> mask(Map<String, ?> config) {
		Map<String, Object> out = new TreeMap<>();
		if (config != null) {
			config.forEach((k, v) -> out.put(k, mask(k, v)));
		}
		return out;
	}

	/** Removes credentials embedded in a text (connection URLs/strings, error messages, stack traces); null-safe. */
	public static String scrub(String text) {
		if (text == null || text.isEmpty()) {
			return text;
		}
		String s = URL_USER_INFO.matcher(text).replaceAll("$1***@");
		s = ORACLE_USER_PASS.matcher(s).replaceAll("$1***@");
		s = SECRET_PAIR.matcher(s).replaceAll("$1$2" + Matcher.quoteReplacement(MASK));
		return AUTH_SCHEME.matcher(s).replaceAll("$1$2" + Matcher.quoteReplacement(MASK));
	}

}
