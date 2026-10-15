package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient.SchemaRegistryException;

/** schemas_get, schemas_diff, schemas_register, schemas_delete_version. */
@Component
public class SchemaTools {

	static final int DEFAULT_MAX_CHARS = 20_000;

	static final int MAX_DIFF_LINES = 300;

	/** Maximum number of reference schemas fetched when resolving Avro references. */
	static final int MAX_REFERENCES = 20;

	private static final int MAX_SR_MESSAGE = 1_500;

	/** SR error_code: invalid schema. */
	static final int INVALID_SCHEMA_CODE = 42201;

	/** Dump of the old schema in the SR 409 message (noise). */
	private static final Pattern OLD_SCHEMA = Pattern.compile(",?\\s*\\{oldSchema: '.*?'\\}", Pattern.DOTALL);

	/** Dump of the submitted schema at the start of the SR 422 message (noise). */
	private static final Pattern INVALID_SCHEMA_ECHO = Pattern
		.compile("^Invalid schema \\{.*\\} with refs \\[.*?\\] of type [A-Z]+, details: ", Pattern.DOTALL);

	private final SchemaRegistryApi api;

	public SchemaTools(SchemaRegistryApi api) {
		this.api = api;
	}

	static Map<String, Object> schemaTypeParam() {
		return P.enumOf("Schema type (default AVRO)", SchemaTexts.TYPES);
	}

	static Map<String, Object> schemaParam() {
		return P.string("Schema text (Avro or JSON Schema JSON, .proto content for PROTOBUF)");
	}

	public List<ToolSpec> tools() {
		Map<String, Object> maxChars = P.integer(
				"Maximum number of characters of the schema text (default " + DEFAULT_MAX_CHARS + "); the rest is truncated", 200,
				1_000_000);
		maxChars.put("default", DEFAULT_MAX_CHARS);
		return List.of(
				ToolSpec.read("schemas_get", "Get the schema of a subject version: id, version, schemaType, references and schema text (AVRO/JSON pretty-printed)")
					.schemaRegistry()
					.required("subject", P.string("Subject name"))
					.param("version", P.string("Version number or latest (default latest)"))
					.param("maxChars", maxChars)
					.handler(this::get),
				ToolSpec.read("schemas_diff", "Compare two schema versions: field-level diff and compatibility hint for AVRO, line diff for JSON/PROTOBUF")
					.schemaRegistry()
					.required("subject", P.string("Subject name"))
					.param("fromVersion", P.string("Old version (default: the version before toVersion)"))
					.param("toVersion", P.string("New version or latest (default latest)"))
					.handler(this::diff),
				ToolSpec.write("schemas_register", "Register a schema under a subject; returns its id and version, or the SR message for an incompatible (409) or invalid (422) schema")
					.schemaRegistry()
					.required("subject", P.string("Subject name (e.g. <topic>-value)"))
					.required("schema", schemaParam())
					.param("schemaType", schemaTypeParam())
					.param("references", SchemaReference.param())
					.handler(this::register),
				ToolSpec.destructive("schemas_delete_version", "Delete a schema version (soft); permanent=true deletes it permanently (soft-deleting it first if needed)")
					.schemaRegistry()
					.required("subject", P.string("Subject name"))
					.required("version", P.string("Version number to delete or latest"))
					.param("permanent", P.bool("Delete permanently, cannot be undone (default false: soft delete)"))
					.handler(this::deleteVersion));
	}

	Object get(ToolCall call) {
		String subject = call.str("subject");
		Integer version = SchemaRegistryApi.parseVersion(call.str("version", null), "version");
		int maxChars = call.integer("maxChars", DEFAULT_MAX_CHARS, 200, 1_000_000);
		SchemaVersion v = api.version(call.cluster(), subject, version);
		String text = SchemaTexts.pretty(v.schemaType(), v.schema());
		Map<String, Object> out = Results.map("subject", v.subject(), "version", v.version(), "id", v.id(),
				"schemaType", v.schemaType(), "references",
				v.references().isEmpty() ? null : v.references().stream().map(SchemaReference::toMap).toList());
		if (text.length() > maxChars) {
			out.put("truncated", true);
			out.put("schemaChars", text.length());
		}
		out.put("schema", SchemaTexts.truncate(text, maxChars));
		return out;
	}

	Object diff(ToolCall call) {
		String cluster = call.cluster();
		String subject = call.str("subject");
		Integer toArg = SchemaRegistryApi.parseVersion(call.str("toVersion", null), "toVersion");
		boolean fromGiven = call.has("fromVersion");
		Integer fromArg = fromGiven ? SchemaRegistryApi.parseVersion(call.str("fromVersion"), "fromVersion") : null;
		List<Integer> versions = api.versions(cluster, subject, false);
		if (versions.isEmpty()) {
			throw new ToolException("Subject '" + subject + "' has no active versions");
		}
		int latest = versions.get(versions.size() - 1);
		int to = toArg == null ? latest : toArg;
		int from;
		if (fromGiven) {
			from = fromArg == null ? latest : fromArg;
		}
		else {
			Integer prev = versions.stream().filter(x -> x < to).reduce((x, y) -> y).orElse(null);
			if (prev == null) {
				if (versions.size() == 1) {
					throw new ToolException("Subject '" + subject + "' has a single version (v" + latest
							+ "); there is no previous version to compare. Use schemas_get to see the schema.");
				}
				throw new ToolException("No active version before v" + to + "; pass fromVersion (available: "
						+ versions + ")");
			}
			from = prev;
		}
		if (from == to) {
			throw new ToolException("fromVersion and toVersion are the same (v" + from + "); pass two different versions");
		}
		SchemaVersion a = api.version(cluster, subject, from);
		SchemaVersion b = api.version(cluster, subject, to);
		Map<String, Object> out = Results.map("subject", subject, "fromVersion", from, "toVersion", to, "fromId",
				a.id(), "toId", b.id(), "schemaType", b.schemaType());
		if (!a.schemaType().equals(b.schemaType())) {
			out.put("schemaTypeChanged", a.schemaType() + " → " + b.schemaType());
		}
		if (a.id() == b.id()) {
			out.put("identical", true);
			out.put("message", "Both versions point to the same schema (id " + a.id() + "); no differences");
			return out;
		}
		if (a.isAvro() && b.isAvro()) {
			try {
				Schema sa = parseAvro(call, cluster, a);
				Schema sb = parseAvro(call, cluster, b);
				AvroSchemaDiff.Result r = AvroSchemaDiff.diff(sa, sb);
				out.put("mode", "avro-fields");
				out.putAll(r.toMap());
				if (r.totalChanges() == 0) {
					out.put("message", "No field-level differences (only doc, field order or formatting may differ)");
				}
				addLevel(cluster, subject, r, out);
				return out;
			}
			catch (RuntimeException e) {
				out.put("avroParseError", "Could not parse the Avro schema, returned a line diff: " + e.getMessage());
			}
		}
		LineDiff.Result r = LineDiff.diff(SchemaTexts.pretty(a.schemaType(), a.schema()),
				SchemaTexts.pretty(b.schemaType(), b.schema()), MAX_DIFF_LINES);
		out.put("mode", "lines");
		out.putAll(r.toMap());
		if (r.identical()) {
			out.put("message", "No line-level differences (only formatting may differ)");
		}
		return out;
	}

	private void addLevel(String cluster, String subject, AvroSchemaDiff.Result r, Map<String, Object> out) {
		String level;
		try {
			level = api.effectiveCompatibility(cluster, subject);
		}
		catch (ToolException e) {
			return;
		}
		if (level == null || !(out.get("compatibility") instanceof Map<?, ?> m)) {
			return;
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> compat = (Map<String, Object>) m;
		compat.put("subjectLevel", level);
		Boolean ok = r.satisfies(level);
		if (ok != null) {
			compat.put("satisfiesSubjectLevel", ok);
			if (level.endsWith("_TRANSITIVE")) {
				compat.put("levelNote", "Only these two versions were compared; " + level
						+ " checks all previous versions");
			}
		}
	}

	/** Parses an Avro schema together with its references (the references are loaded into the same parser first). */
	private Schema parseAvro(ToolCall call, String cluster, SchemaVersion v) {
		Schema.Parser parser = new Schema.Parser().setValidateDefaults(false);
		loadReferences(call, cluster, parser, v.references(), new HashSet<>(), 0);
		return parser.parse(v.schema());
	}

	private void loadReferences(ToolCall call, String cluster, Schema.Parser parser, List<SchemaReference> refs,
			Set<String> seen, int depth) {
		for (SchemaReference ref : refs) {
			if (!seen.add(ref.subject() + "@" + ref.version())) {
				continue;
			}
			if (depth > 5 || seen.size() > MAX_REFERENCES || call.nearDeadline(2_000)) {
				throw new ToolException("Reference chain too deep/large or out of time");
			}
			SchemaVersion rv = api.version(cluster, ref.subject(), ref.version());
			loadReferences(call, cluster, parser, rv.references(), seen, depth + 1);
			if (rv.isAvro()) {
				parser.parse(rv.schema());
			}
		}
	}

	Object register(ToolCall call) {
		String cluster = call.cluster();
		String subject = call.str("subject");
		SchemaInput in = SchemaInput.fromCall(call);
		SchemaVersion existing;
		int id;
		try {
			existing = api.lookup(cluster, subject, in);
			id = api.register(cluster, subject, in);
		}
		catch (SchemaRegistryException e) {
			throw registerError(cluster, subject, e);
		}
		SchemaVersion registered = existing != null ? existing : api.lookup(cluster, subject, in);
		Map<String, Object> out = Results.map("subject", subject, "id", id, "version",
				registered == null ? null : registered.version(), "schemaType", in.schemaType(), "created",
				existing == null);
		if (existing != null) {
			out.put("message",
					"Schema was already registered under this subject (v" + existing.version() + "); no new version created");
		}
		return out;
	}

	private ToolException registerError(String cluster, String subject, SchemaRegistryException e) {
		if (e.status() == 409) {
			String level;
			try {
				level = api.effectiveCompatibility(cluster, subject);
			}
			catch (ToolException ignored) {
				level = "?";
			}
			return new ToolException("Incompatible schema (409): the compatibility level of subject '" + subject + "' ("
					+ level + ") rejected the new schema. SR: " + cleanSrMessage(e.srMessage())
					+ " — use schemas_compatibility_check for details and schemas_diff for the diff.", e);
		}
		return invalidSchemaError(e);
	}

	/**
	 * Turns SR 42201 (invalid schema) into a plain message. Other 422 codes (42205 READONLY/IMPORT mode, 42207, 42208
	 * invalid subject, 42209 etc.) are not schema errors: they are returned unchanged, with the client's hint.
	 */
	static ToolException invalidSchemaError(SchemaRegistryException e) {
		if (e.status() == 422 && e.errorCode() == INVALID_SCHEMA_CODE) {
			return new ToolException("Schema rejected (422/" + INVALID_SCHEMA_CODE + "): " + cleanSrMessage(e.srMessage()),
					e);
		}
		return e;
	}

	/** Removes the old schema dump from an SR message and limits its length. */
	static String cleanSrMessage(String message) {
		if (message == null) {
			return "";
		}
		String m = OLD_SCHEMA.matcher(message).replaceAll("");
		m = INVALID_SCHEMA_ECHO.matcher(m).replaceFirst("Invalid schema: ");
		return SchemaTexts.truncate(m, MAX_SR_MESSAGE);
	}

	Object deleteVersion(ToolCall call) {
		String cluster = call.cluster();
		String subject = call.str("subject");
		Integer requested = SchemaRegistryApi.parseVersion(call.str("version"), "version");
		boolean permanent = call.bool("permanent", false);
		int version;
		if (requested != null) {
			version = requested;
		}
		else {
			SchemaVersion latest = api.latestOrNull(cluster, subject);
			if (latest == null) {
				throw new ToolException("Subject not found or has no active versions: " + subject
						+ ". To delete a soft-deleted version permanently, pass its version number "
						+ "(schemas_versions_list deleted=true)");
			}
			version = latest.version();
		}
		boolean softNow = false;
		if (!permanent) {
			api.deleteVersion(cluster, subject, version, false);
			softNow = true;
		}
		else {
			List<Integer> active = api.versionsOrEmpty(cluster, subject, false);
			if (active.contains(version)) {
				api.deleteVersion(cluster, subject, version, false);
				softNow = true;
			}
			else {
				List<Integer> all = api.versionsOrEmpty(cluster, subject, true);
				if (!all.contains(version)) {
					throw new ToolException("Version not found: " + subject + " v" + version
							+ (all.isEmpty() ? " (subject does not exist)" : " (available: " + all + ")"));
				}
			}
			api.deleteVersion(cluster, subject, version, true);
		}
		List<Integer> remaining = api.versionsOrEmpty(cluster, subject, false);
		String note = permanent ? "Version deleted permanently (cannot be undone)"
				: "Version soft-deleted; visible with deleted=true, can be deleted permanently with permanent=true";
		return Results.map("subject", subject, "deletedVersion", version, "permanent", permanent, "softDeletedNow",
				softNow, "remainingVersions", remaining, "note",
				remaining.isEmpty() ? note + ". The subject has no active versions left" : note);
	}

}
