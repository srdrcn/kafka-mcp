package io.github.srdrcn.kafkamcp.kafka;

import java.time.Instant;
import java.util.Locale;

/** Çıktı biçimlendirme yardımcıları (boyut, zaman, yuvarlama). */
public final class Format {

	private static final String[] UNITS = { "B", "KiB", "MiB", "GiB", "TiB", "PiB" };

	private Format() {
	}

	/** Okunabilir boyut: 512 B, 1.2 GiB. */
	public static String bytes(long bytes) {
		if (bytes < 1024) {
			return bytes + " B";
		}
		double v = bytes;
		int u = 0;
		while (v >= 1024 && u < UNITS.length - 1) {
			v /= 1024;
			u++;
		}
		return String.format(Locale.ROOT, "%.1f %s", v, UNITS[u]);
	}

	/** Epoch ms → ISO-8601 (UTC); negatifse null. */
	public static String iso(long epochMs) {
		return epochMs < 0 ? null : Instant.ofEpochMilli(epochMs).toString();
	}

	/** Epoch ms'den bu yana geçen saniye (en az 0); negatifse null. */
	public static Long ageSeconds(long epochMs) {
		return epochMs < 0 ? null : Math.max(0, (System.currentTimeMillis() - epochMs) / 1000);
	}

	/** Ondalık yuvarlama. */
	public static double round(double value, int decimals) {
		double f = Math.pow(10, decimals);
		return Math.round(value * f) / f;
	}

}
