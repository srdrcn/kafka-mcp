package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient.SchemaRegistryException;

/**
 * Schema Registry REST uç noktalarına tipli erişim (subject, versiyon, config, uyumluluk, kayıt/silme). Subject'ler
 * {@link SchemaRegistryClient#enc(String)} ile kodlanır; hatalar Türkçe {@link SchemaRegistryException}'dır.
 */
@Component
public class SchemaRegistryApi {

	public static final String LATEST = "latest";

	public static final List<String> LEVELS = List.of("BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD",
			"FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE", "NONE");

	/** Subject seviyesinde compatibility tanımlı değil. */
	private static final int SUBJECT_COMPAT_NOT_CONFIGURED = 40408;

	private static final int SUBJECT_NOT_FOUND = 40401;

	private static final int VERSION_NOT_FOUND = 40402;

	private static final int SCHEMA_NOT_FOUND = 40403;

	private final SchemaRegistryClient client;

	public SchemaRegistryApi(SchemaRegistryClient client) {
		this.client = client;
	}

	public SchemaRegistryClient client() {
		return client;
	}

	/** "latest" ya da -1 → null (son versiyon); pozitif tamsayı → değeri; diğerleri Türkçe hata. */
	public static Integer parseVersion(String raw, String param) {
		if (raw == null || raw.isBlank() || LATEST.equalsIgnoreCase(raw.trim()) || "-1".equals(raw.trim())) {
			return null;
		}
		String s = raw.trim();
		try {
			double d = Double.parseDouble(s);
			if (d >= 1 && d == Math.rint(d) && d <= Integer.MAX_VALUE) {
				return (int) d;
			}
		}
		catch (NumberFormatException ignored) {
			// aşağıda hata
		}
		throw new ToolException(param + " pozitif bir versiyon numarası ya da 'latest' olmalı: " + raw);
	}

	/** Versiyon yol parçası: null → latest. */
	public static String versionSegment(Integer version) {
		return version == null ? LATEST : String.valueOf(version);
	}

	/** Compatibility seviyesini doğrular ve büyük harfe çevirir. */
	public static String normalizeLevel(String level) {
		String l = level == null ? "" : level.trim().toUpperCase(Locale.ROOT);
		if (!LEVELS.contains(l)) {
			throw new ToolException("Geçersiz compatibility seviyesi: " + level + " (geçerli: " + LEVELS + ")");
		}
		return l;
	}

	/** GET /subjects (deleted=true ise soft-delete edilmişler dahil), sıralı. */
	public List<String> subjects(String cluster, boolean deleted) {
		JsonNode n = client.get(cluster, "/subjects" + (deleted ? "?deleted=true" : ""));
		List<String> out = new ArrayList<>();
		if (n != null && n.isArray()) {
			n.forEach(s -> out.add(s.asText()));
		}
		out.sort(null);
		return out;
	}

	/** GET /subjects/{s}/versions (deleted=true ise soft-delete edilmişler dahil), artan sırada. */
	public List<Integer> versions(String cluster, String subject, boolean deleted) {
		JsonNode n = client.get(cluster,
				"/subjects/" + SchemaRegistryClient.enc(subject) + "/versions" + (deleted ? "?deleted=true" : ""));
		List<Integer> out = new ArrayList<>();
		if (n != null && n.isArray()) {
			n.forEach(v -> out.add(v.asInt()));
		}
		out.sort(null);
		return out;
	}

	/** {@link #versions} ama subject yoksa (ya da tüm versiyonları soft-delete edilmişse) boş liste. */
	public List<Integer> versionsOrEmpty(String cluster, String subject, boolean deleted) {
		try {
			return versions(cluster, subject, deleted);
		}
		catch (SchemaRegistryException e) {
			if (e.status() == 404) {
				return new ArrayList<>();
			}
			throw e;
		}
	}

	/** GET /subjects/{s}/versions/{v}; version null ise latest. */
	public SchemaVersion version(String cluster, String subject, Integer version) {
		JsonNode n = client.get(cluster,
				"/subjects/" + SchemaRegistryClient.enc(subject) + "/versions/" + versionSegment(version));
		SchemaVersion v = SchemaVersion.fromJson(n);
		return v.subject() == null ? new SchemaVersion(subject, v.version(), v.id(), v.schemaType(), v.references(),
				v.schema()) : v;
	}

	/** Son versiyon; subject yoksa ya da aktif versiyonu yoksa null. */
	public SchemaVersion latestOrNull(String cluster, String subject) {
		try {
			return version(cluster, subject, null);
		}
		catch (SchemaRegistryException e) {
			if (e.status() == 404 && (e.errorCode() == SUBJECT_NOT_FOUND || e.errorCode() == VERSION_NOT_FOUND)) {
				return null;
			}
			throw e;
		}
	}

	/** POST /subjects/{s}: şema bu subject'te kayıtlıysa versiyonu, değilse (ya da subject yoksa) null. */
	public SchemaVersion lookup(String cluster, String subject, SchemaInput input) {
		try {
			JsonNode n = client.post(cluster, "/subjects/" + SchemaRegistryClient.enc(subject), input.toBody());
			SchemaVersion v = SchemaVersion.fromJson(n);
			return new SchemaVersion(subject, v.version(), v.id(), input.schemaType(), v.references(), v.schema());
		}
		catch (SchemaRegistryException e) {
			if (e.status() == 404 && (e.errorCode() == SUBJECT_NOT_FOUND || e.errorCode() == SCHEMA_NOT_FOUND)) {
				return null;
			}
			throw e;
		}
	}

	/** POST /subjects/{s}/versions → şema id'si. */
	public int register(String cluster, String subject, SchemaInput input) {
		JsonNode n = client.post(cluster, "/subjects/" + SchemaRegistryClient.enc(subject) + "/versions",
				input.toBody());
		if (n == null || !n.has("id")) {
			throw new ToolException("Schema Registry kayıt yanıtında id yok");
		}
		return n.get("id").asInt();
	}

	/** GET /config → global compatibility seviyesi. */
	public String globalCompatibility(String cluster) {
		return level(client.get(cluster, "/config"));
	}

	/** GET /config/{s} → subject seviyesinde tanımlı compatibility; tanımlı değilse null. */
	public String subjectCompatibility(String cluster, String subject) {
		try {
			return level(client.get(cluster, "/config/" + SchemaRegistryClient.enc(subject)));
		}
		catch (SchemaRegistryException e) {
			if (e.status() == 404
					&& (e.errorCode() == SUBJECT_COMPAT_NOT_CONFIGURED || e.errorCode() == SUBJECT_NOT_FOUND)) {
				return null;
			}
			throw e;
		}
	}

	/** GET /config/{s}?defaultToGlobal=true → subject için geçerli seviye (subject yoksa global). */
	public String effectiveCompatibility(String cluster, String subject) {
		if (subject == null) {
			return globalCompatibility(cluster);
		}
		return level(client.get(cluster, "/config/" + SchemaRegistryClient.enc(subject) + "?defaultToGlobal=true"));
	}

	/** PUT /config[/subject] → yeni seviye. */
	public String setCompatibility(String cluster, String subject, String level) {
		String path = subject == null ? "/config" : "/config/" + SchemaRegistryClient.enc(subject);
		JsonNode n = client.put(cluster, path, Map.of("compatibility", normalizeLevel(level)));
		String after = n == null ? null : n.path("compatibility").asText(null);
		return after == null ? level(n) : after;
	}

	/** Uyumluluk testi sonucu: SR'ın is_compatible değeri ve (verbose) mesajları. */
	public record CompatibilityCheck(boolean compatible, List<String> messages) {
	}

	/** POST /compatibility/subjects/{s}/versions/{v}?verbose=true; version null ise latest. */
	public CompatibilityCheck checkCompatibility(String cluster, String subject, Integer version, SchemaInput input) {
		JsonNode n = client.post(cluster, "/compatibility/subjects/" + SchemaRegistryClient.enc(subject) + "/versions/"
				+ versionSegment(version) + "?verbose=true", input.toBody());
		List<String> messages = new ArrayList<>();
		if (n != null && n.has("messages") && n.get("messages").isArray()) {
			n.get("messages").forEach(m -> messages.add(m.asText()));
		}
		return new CompatibilityCheck(n != null && n.path("is_compatible").asBoolean(false), messages);
	}

	/** DELETE /subjects/{s}/versions/{v}[?permanent=true] → silinen versiyon numarası. */
	public int deleteVersion(String cluster, String subject, int version, boolean permanent) {
		JsonNode n = client.delete(cluster, "/subjects/" + SchemaRegistryClient.enc(subject) + "/versions/" + version
				+ (permanent ? "?permanent=true" : ""));
		return n != null && n.canConvertToInt() ? n.asInt() : version;
	}

	private static String level(JsonNode n) {
		if (n == null) {
			return null;
		}
		String l = n.path("compatibilityLevel").asText(null);
		return l != null ? l : n.path("compatibility").asText(null);
	}

}
