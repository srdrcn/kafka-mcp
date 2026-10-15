package io.github.srdrcn.kafkamcp.mcp;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;

/**
 * Tek bir araç çağrısının bağlamı: argümanlar, seçilen cluster, Kafka client'ları ve 15 sn'lik deadline.
 */
public final class ToolCall {

	/** Kafka beklemelerinde deadline'dan önce bırakılan pay; böylece hata mesajı araç zaman aşımından önce döner. */
	private static final long MARGIN_MS = 300;

	private final String toolName;

	private final Map<String, Object> args;

	private final KafkaClients clients;

	private final Instant deadline;

	private Pattern filter;

	private boolean filterParsed;

	public ToolCall(String toolName, Map<String, Object> args, KafkaClients clients, Instant deadline) {
		this.toolName = toolName;
		this.args = args == null ? Map.of() : args;
		this.clients = clients;
		this.deadline = deadline;
	}

	public String toolName() {
		return toolName;
	}

	public Map<String, Object> args() {
		return args;
	}

	public KafkaClients clients() {
		return clients;
	}

	public Instant deadline() {
		return deadline;
	}

	public long remainingMillis() {
		return Math.max(0, Duration.between(Instant.now(), deadline).toMillis());
	}

	/** Seçilen cluster adı (zorunlu). */
	public String cluster() {
		String c = str("cluster", null);
		if (c == null) {
			throw new ToolException("cluster parametresi zorunlu");
		}
		return c;
	}

	/** Seçilen cluster adı ya da null (cluster'ı opsiyonel araçlar için). */
	public String clusterOrNull() {
		return str("cluster", null);
	}

	public ClusterConfig clusterConfig() {
		return clients.cluster(cluster());
	}

	/** Seçilen cluster'ın paylaşılan AdminClient'ı (kapatmayın). */
	public Admin admin() {
		return clients.admin(cluster());
	}

	/** Seçilen cluster için yeni assign-only consumer (try-with-resources ile kapatın). */
	public KafkaConsumer<byte[], byte[]> newConsumer() {
		return clients.newConsumer(cluster(), null);
	}

	public boolean has(String name) {
		Object v = args.get(name);
		return v != null && !(v instanceof String s && s.isBlank());
	}

	/** Zorunlu string parametre. */
	public String str(String name) {
		String v = str(name, null);
		if (v == null) {
			throw new ToolException("Parametre eksik: " + name);
		}
		return v;
	}

	public String str(String name, String defaultValue) {
		Object v = args.get(name);
		if (v == null) {
			return defaultValue;
		}
		String s = String.valueOf(v).trim();
		return s.isEmpty() ? defaultValue : s;
	}

	public int integer(String name, int defaultValue) {
		Long v = optLong(name);
		return v == null ? defaultValue : Math.toIntExact(Math.max(Integer.MIN_VALUE, Math.min(Integer.MAX_VALUE, v)));
	}

	/** Değer [min, max] aralığına sıkıştırılır. */
	public int integer(String name, int defaultValue, int min, int max) {
		return Math.max(min, Math.min(max, integer(name, defaultValue)));
	}

	/** Zorunlu tamsayı parametre. */
	public long requiredLong(String name) {
		Long v = optLong(name);
		if (v == null) {
			throw new ToolException("Parametre eksik: " + name);
		}
		return v;
	}

	public long longValue(String name, long defaultValue) {
		Long v = optLong(name);
		return v == null ? defaultValue : v;
	}

	public Integer optInt(String name) {
		Long v = optLong(name);
		return v == null ? null : Math.toIntExact(v);
	}

	public Long optLong(String name) {
		Object v = args.get(name);
		if (v == null || v instanceof String s && s.isBlank()) {
			return null;
		}
		if (v instanceof Number n) {
			return n.longValue();
		}
		try {
			return Long.parseLong(String.valueOf(v).trim());
		}
		catch (NumberFormatException e) {
			throw new ToolException("Parametre tamsayı olmalı: " + name + "=" + v);
		}
	}

	public Double optDouble(String name) {
		Object v = args.get(name);
		if (v == null || v instanceof String s && s.isBlank()) {
			return null;
		}
		if (v instanceof Number n) {
			return n.doubleValue();
		}
		try {
			return Double.parseDouble(String.valueOf(v).trim());
		}
		catch (NumberFormatException e) {
			throw new ToolException("Parametre sayı olmalı: " + name + "=" + v);
		}
	}

	public boolean bool(String name, boolean defaultValue) {
		Object v = args.get(name);
		if (v == null) {
			return defaultValue;
		}
		if (v instanceof Boolean b) {
			return b;
		}
		String s = String.valueOf(v).trim().toLowerCase();
		return switch (s) {
			case "true", "1", "yes", "evet" -> true;
			case "false", "0", "no", "hayir", "hayır" -> false;
			default -> defaultValue;
		};
	}

	/** String listesi; dizi ya da virgülle ayrılmış string kabul eder. Yoksa boş liste. */
	public List<String> strings(String name) {
		Object v = args.get(name);
		List<String> out = new ArrayList<>();
		if (v instanceof Collection<?> c) {
			for (Object o : c) {
				if (o != null && !String.valueOf(o).isBlank()) {
					out.add(String.valueOf(o).trim());
				}
			}
		}
		else if (v != null) {
			for (String s : String.valueOf(v).split(",")) {
				if (!s.isBlank()) {
					out.add(s.trim());
				}
			}
		}
		return out;
	}

	/** JSON nesnesi parametresi; yoksa boş map. */
	@SuppressWarnings("unchecked")
	public Map<String, Object> map(String name) {
		Object v = args.get(name);
		if (v == null) {
			return new LinkedHashMap<>();
		}
		if (v instanceof Map<?, ?> m) {
			return new LinkedHashMap<>((Map<String, Object>) m);
		}
		throw new ToolException("Parametre nesne (JSON object) olmalı: " + name);
	}

	/** Değerleri string'e çevrilmiş map parametresi (config'ler için). */
	public Map<String, String> stringMap(String name) {
		Map<String, String> out = new LinkedHashMap<>();
		map(name).forEach((k, v) -> out.put(k, v == null ? null : String.valueOf(v)));
		return out;
	}

	/** filter parametresinden derlenmiş regex ya da null. */
	public Pattern filter() {
		if (!filterParsed) {
			filter = regex("filter");
			filterParsed = true;
		}
		return filter;
	}

	/** Verilen regex parametresini derler; yoksa null. */
	public Pattern regex(String name) {
		String f = str(name, null);
		if (f == null) {
			return null;
		}
		try {
			return Pattern.compile(f);
		}
		catch (PatternSyntaxException e) {
			throw new ToolException("Geçersiz regex (" + name + "): " + e.getDescription());
		}
	}

	/** filter yoksa true; varsa kısmi eşleşme (find). */
	public boolean matches(String value) {
		Pattern p = filter();
		return p == null || (value != null && p.matcher(value).find());
	}

	public int limit() {
		return limit(ToolSpec.DEFAULT_LIST_LIMIT);
	}

	public int limit(int defaultLimit) {
		return integer("limit", defaultLimit, 1, ToolSpec.MAX_LIST_LIMIT);
	}

	/** maxRecords: varsayılan 50, en fazla 500. */
	public int maxRecords() {
		return integer("maxRecords", ToolSpec.DEFAULT_MAX_RECORDS, 1, ToolSpec.MAX_RECORDS_LIMIT);
	}

	/** Kafka future'ını kalan süre kadar bekler; hatayı Türkçe {@link ToolException}'a çevirir. */
	public <T> T await(KafkaFuture<T> future) {
		try {
			return future.get(waitMillis(), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Araç iptal edildi");
		}
		catch (ExecutionException e) {
			throw new ToolException(KafkaErrors.explain(e.getCause()), e.getCause());
		}
		catch (TimeoutException e) {
			throw new ToolException("Kafka yanıtı " + KafkaClients.TIMEOUT_MS / 1000 + " sn içinde gelmedi (zaman aşımı)");
		}
	}

	public <T> T await(Future<T> future) {
		try {
			return future.get(waitMillis(), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Araç iptal edildi");
		}
		catch (ExecutionException e) {
			throw new ToolException(KafkaErrors.explain(e.getCause()), e.getCause());
		}
		catch (TimeoutException e) {
			future.cancel(true);
			throw new ToolException("Yanıt " + KafkaClients.TIMEOUT_MS / 1000 + " sn içinde gelmedi (zaman aşımı)");
		}
	}

	/** Kafka beklemeleri için kalan süre (pay düşülmüş), en az 1 ms. */
	public long waitMillis() {
		return Math.max(1, remainingMillis() - MARGIN_MS);
	}

	/** Kalan süre bitti mi (pay dahil)? Döngülerde erken çıkmak için. */
	public boolean nearDeadline(long reserveMillis) {
		return remainingMillis() <= MARGIN_MS + reserveMillis;
	}

	/** Belirtilen süre bekler (iki örnek arası). Deadline'ı aşacaksa ToolException. */
	public void sleep(long millis) {
		if (remainingMillis() - millis < MARGIN_MS + 1_000) {
			throw new ToolException("Bekleme süresi araç zaman sınırını (15 sn) aşıyor; daha kısa aralık verin");
		}
		try {
			Thread.sleep(millis);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Araç iptal edildi");
		}
	}

}
