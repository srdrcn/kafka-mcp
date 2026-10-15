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
 * Typed access to the Schema Registry REST endpoints (subject, version, config, compatibility, register/delete).
 * Subjects are encoded with {@link SchemaRegistryClient#enc(String)}; errors are readable
 * {@link SchemaRegistryException}s.
 */
@Component
public class SchemaRegistryApi {

	public static final String LATEST = "latest";

	public static final List<String> LEVELS = List.of("BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD",
			"FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE", "NONE");

	/** No subject-level compatibility configured. */
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

	/** "latest" or -1 → null (latest version); positive integer → its value; anything else is an error. */
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
			// error below
		}
		throw new ToolException(param + " must be a positive version number or 'latest': " + raw);
	}

	/** Version path segment: null → latest. */
	public static String versionSegment(Integer version) {
		return version == null ? LATEST : String.valueOf(version);
	}

	/** Validates the compatibility level and converts it to upper case. */
	public static String normalizeLevel(String level) {
		String l = level == null ? "" : level.trim().toUpperCase(Locale.ROOT);
		if (!LEVELS.contains(l)) {
			throw new ToolException("Invalid compatibility level: " + level + " (valid: " + LEVELS + ")");
		}
		return l;
	}

	/** GET /subjects (soft-deleted ones included if deleted=true), sorted. */
	public List<String> subjects(String cluster, boolean deleted) {
		JsonNode n = client.get(cluster, "/subjects" + (deleted ? "?deleted=true" : ""));
		List<String> out = new ArrayList<>();
		if (n != null && n.isArray()) {
			n.forEach(s -> out.add(s.asText()));
		}
		out.sort(null);
		return out;
	}

	/** GET /subjects/{s}/versions (soft-deleted ones included if deleted=true), in ascending order. */
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

	/**
	 * Like {@link #versions}, but an empty list if the subject does not exist (or all its versions are soft-deleted).
	 */
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

	/** GET /subjects/{s}/versions/{v}; latest if version is null. */
	public SchemaVersion version(String cluster, String subject, Integer version) {
		JsonNode n = client.get(cluster,
				"/subjects/" + SchemaRegistryClient.enc(subject) + "/versions/" + versionSegment(version));
		SchemaVersion v = SchemaVersion.fromJson(n);
		return v.subject() == null ? new SchemaVersion(subject, v.version(), v.id(), v.schemaType(), v.references(),
				v.schema()) : v;
	}

	/** Latest version; null if the subject does not exist or has no active versions. */
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

	/**
	 * POST /subjects/{s}: the version if the schema is registered under this subject, otherwise (or if the subject does
	 * not exist) null.
	 */
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

	/** POST /subjects/{s}/versions → schema id. */
	public int register(String cluster, String subject, SchemaInput input) {
		JsonNode n = client.post(cluster, "/subjects/" + SchemaRegistryClient.enc(subject) + "/versions",
				input.toBody());
		if (n == null || !n.has("id")) {
			throw new ToolException("Schema Registry register response has no id");
		}
		return n.get("id").asInt();
	}

	/** GET /config → global compatibility level. */
	public String globalCompatibility(String cluster) {
		return level(client.get(cluster, "/config"));
	}

	/** GET /config/{s} → compatibility configured at subject level; null if not configured. */
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

	/** GET /config/{s}?defaultToGlobal=true → effective level for the subject (global if subject is null). */
	public String effectiveCompatibility(String cluster, String subject) {
		if (subject == null) {
			return globalCompatibility(cluster);
		}
		return level(client.get(cluster, "/config/" + SchemaRegistryClient.enc(subject) + "?defaultToGlobal=true"));
	}

	/** PUT /config[/subject] → new level. */
	public String setCompatibility(String cluster, String subject, String level) {
		String path = subject == null ? "/config" : "/config/" + SchemaRegistryClient.enc(subject);
		JsonNode n = client.put(cluster, path, Map.of("compatibility", normalizeLevel(level)));
		String after = n == null ? null : n.path("compatibility").asText(null);
		return after == null ? level(n) : after;
	}

	/** Compatibility test result: the SR is_compatible value and the (verbose) messages. */
	public record CompatibilityCheck(boolean compatible, List<String> messages) {
	}

	/** POST /compatibility/subjects/{s}/versions/{v}?verbose=true; latest if version is null. */
	public CompatibilityCheck checkCompatibility(String cluster, String subject, Integer version, SchemaInput input) {
		JsonNode n = client.post(cluster, "/compatibility/subjects/" + SchemaRegistryClient.enc(subject) + "/versions/"
				+ versionSegment(version) + "?verbose=true", input.toBody());
		List<String> messages = new ArrayList<>();
		if (n != null && n.has("messages") && n.get("messages").isArray()) {
			n.get("messages").forEach(m -> messages.add(m.asText()));
		}
		return new CompatibilityCheck(n != null && n.path("is_compatible").asBoolean(false), messages);
	}

	/** DELETE /subjects/{s}/versions/{v}[?permanent=true] → deleted version number. */
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
