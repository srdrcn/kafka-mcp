package io.github.srdrcn.kafkamcp.kafka;

/** metadata.version feature seviyesini Kafka sürümüne eşler (server-common MetadataVersion tablosu). */
public final class KafkaVersions {

	/** İndeks = feature level. 0 seviyesi yok (3.0-IV0 KRaft'ta finalize edilemez). */
	private static final String[] METADATA_VERSIONS = { null, "3.0-IV1", "3.1-IV0", "3.2-IV0", "3.3-IV0", "3.3-IV1",
			"3.3-IV2", "3.3-IV3", "3.4-IV0", "3.5-IV0", "3.5-IV1", "3.5-IV2", "3.6-IV0", "3.6-IV1", "3.6-IV2",
			"3.7-IV0", "3.7-IV1", "3.7-IV2", "3.7-IV3", "3.7-IV4", "3.8-IV0", "3.9-IV0", "4.0-IV0", "4.0-IV1",
			"4.0-IV2", "4.0-IV3", "4.1-IV0", "4.1-IV1", "4.2-IV0", "4.2-IV1" };

	private KafkaVersions() {
	}

	/** metadata.version seviyesinin adı (ör. 27 → "4.1-IV1"); bilinmiyorsa null. */
	public static String metadataVersion(int level) {
		return level > 0 && level < METADATA_VERSIONS.length ? METADATA_VERSIONS[level] : null;
	}

	/** metadata.version seviyesinden sürüm (ör. "4.1"); tablodan yeniyse "&gt;4.2" gibi. */
	public static String release(int level) {
		String mv = metadataVersion(level);
		if (mv != null) {
			return releaseOf(mv);
		}
		return level >= METADATA_VERSIONS.length ? ">" + releaseOf(METADATA_VERSIONS[METADATA_VERSIONS.length - 1])
				: null;
	}

	/** "3.6-IV2" ya da "3.6" gibi IBP değerinden "3.6". */
	public static String releaseOf(String ibp) {
		if (ibp == null || ibp.isBlank()) {
			return null;
		}
		String v = ibp.trim();
		int dash = v.indexOf('-');
		v = dash > 0 ? v.substring(0, dash) : v;
		String[] parts = v.split("\\.");
		return parts.length >= 2 ? parts[0] + "." + parts[1] : v;
	}

}
