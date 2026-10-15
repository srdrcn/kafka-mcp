package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Connector config'lerindeki gizli değerleri maskeler: anahtar (büyük/küçük harf duyarsız)
 * password/secret/token/credential/jaas/user.info içeriyorsa ya da son noktalı segmenti "key" içeriyorsa
 * (api.key, private.key, apikey; ama key.converter/key.serializer değil).
 */
public final class ConnectSecrets {

	public static final String MASK = "******";

	private static final List<String> SECRET_WORDS = List.of("password", "secret", "token", "credential", "jaas",
			"user.info");

	private ConnectSecrets() {
	}

	/** Bu config anahtarının değeri gizli mi? */
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

	/** Gizli anahtarın (null olmayan) değerini {@link #MASK} ile değiştirir; diğerlerini olduğu gibi döner. */
	public static Object mask(String key, Object value) {
		return value != null && isSecret(key) ? MASK : value;
	}

	/** Config'in maskelenmiş, anahtara göre sıralı kopyası. */
	public static Map<String, Object> mask(Map<String, ?> config) {
		Map<String, Object> out = new TreeMap<>();
		if (config != null) {
			config.forEach((k, v) -> out.put(k, mask(k, v)));
		}
		return out;
	}

}
