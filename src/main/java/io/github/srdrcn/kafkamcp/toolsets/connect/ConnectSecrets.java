package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Masks secret values in connector configs: a key is secret if it contains (case-insensitive)
 * password/secret/token/credential/jaas/user.info, or if its last dotted segment contains "key" (api.key,
 * private.key, apikey; but not key.converter/key.serializer).
 */
public final class ConnectSecrets {

	public static final String MASK = "******";

	private static final List<String> SECRET_WORDS = List.of("password", "secret", "token", "credential", "jaas",
			"user.info");

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
		String last = k.substring(k.lastIndexOf('.') + 1);
		return last.contains("key");
	}

	/** Replaces the (non-null) value of a secret key with {@link #MASK}; returns other values unchanged. */
	public static Object mask(String key, Object value) {
		return value != null && isSecret(key) ? MASK : value;
	}

	/** Masked copy of the config, sorted by key. */
	public static Map<String, Object> mask(Map<String, ?> config) {
		Map<String, Object> out = new TreeMap<>();
		if (config != null) {
			config.forEach((k, v) -> out.put(k, mask(k, v)));
		}
		return out;
	}

}
