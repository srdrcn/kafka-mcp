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

	/** Avro referans çözümlemesinde en fazla getirilecek referans şeması. */
	static final int MAX_REFERENCES = 20;

	private static final int MAX_SR_MESSAGE = 1_500;

	/** SR error_code: geçersiz şema. */
	static final int INVALID_SCHEMA_CODE = 42201;

	/** SR 409 mesajındaki eski şema dökümü (gürültü). */
	private static final Pattern OLD_SCHEMA = Pattern.compile(",?\\s*\\{oldSchema: '.*?'\\}", Pattern.DOTALL);

	/** SR 422 mesajının başındaki gönderilen şema dökümü (gürültü). */
	private static final Pattern INVALID_SCHEMA_ECHO = Pattern
		.compile("^Invalid schema \\{.*\\} with refs \\[.*?\\] of type [A-Z]+, details: ", Pattern.DOTALL);

	private final SchemaRegistryApi api;

	public SchemaTools(SchemaRegistryApi api) {
		this.api = api;
	}

	static Map<String, Object> schemaTypeParam() {
		return P.enumOf("Şema tipi (varsayılan AVRO)", SchemaTexts.TYPES);
	}

	static Map<String, Object> schemaParam() {
		return P.string("Şema metni (Avro ya da JSON Schema JSON'u, PROTOBUF için .proto içeriği)");
	}

	public List<ToolSpec> tools() {
		Map<String, Object> maxChars = P.integer(
				"Şema metni için en fazla karakter (varsayılan " + DEFAULT_MAX_CHARS + "); aşan kısım kesilir", 200,
				1_000_000);
		maxChars.put("default", DEFAULT_MAX_CHARS);
		return List.of(
				ToolSpec.read("schemas_get", "Subject versiyonundaki şemayı getirir: id, versiyon, schemaType, referanslar ve şema metni (AVRO/JSON girintili)")
					.schemaRegistry()
					.required("subject", P.string("Subject adı"))
					.param("version", P.string("Versiyon numarası ya da latest (varsayılan latest)"))
					.param("maxChars", maxChars)
					.handler(this::get),
				ToolSpec.read("schemas_diff", "İki şema versiyonunu karşılaştırır: AVRO'da alan bazlı fark + uyumluluk ipucu, JSON/PROTOBUF'ta satır farkı")
					.schemaRegistry()
					.required("subject", P.string("Subject adı"))
					.param("fromVersion", P.string("Eski versiyon (varsayılan: toVersion'dan önceki versiyon)"))
					.param("toVersion", P.string("Yeni versiyon ya da latest (varsayılan latest)"))
					.handler(this::diff),
				ToolSpec.write("schemas_register", "Subject'e şema kaydeder, id ve versiyonu döner; uyumsuz (409) ya da geçersiz (422) şemada SR mesajını verir")
					.schemaRegistry()
					.required("subject", P.string("Subject adı (ör. <topic>-value)"))
					.required("schema", schemaParam())
					.param("schemaType", schemaTypeParam())
					.param("references", SchemaReference.param())
					.handler(this::register),
				ToolSpec.destructive("schemas_delete_version", "Şema versiyonunu siler (soft); permanent=true kalıcı siler (gerekirse önce soft delete yapar)")
					.schemaRegistry()
					.required("subject", P.string("Subject adı"))
					.required("version", P.string("Silinecek versiyon numarası ya da latest"))
					.param("permanent", P.bool("Kalıcı sil, geri alınamaz (varsayılan false: soft delete)"))
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
			throw new ToolException("Subject '" + subject + "' için aktif versiyon yok");
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
					throw new ToolException("Subject '" + subject + "' tek versiyona sahip (v" + latest
							+ "); karşılaştırılacak önceki versiyon yok. Şemayı görmek için schemas_get kullanın.");
				}
				throw new ToolException("v" + to + " öncesinde aktif versiyon yok; fromVersion verin (mevcut: "
						+ versions + ")");
			}
			from = prev;
		}
		if (from == to) {
			throw new ToolException("fromVersion ve toVersion aynı (v" + from + "); farklı iki versiyon verin");
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
			out.put("message", "İki versiyon aynı şemayı (id " + a.id() + ") gösteriyor; fark yok");
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
					out.put("message", "Alan bazında fark yok (sadece doc, alan sırası ya da biçim farkı olabilir)");
				}
				addLevel(cluster, subject, r, out);
				return out;
			}
			catch (RuntimeException e) {
				out.put("avroParseError", "Avro şeması parse edilemedi, satır farkı verildi: " + e.getMessage());
			}
		}
		LineDiff.Result r = LineDiff.diff(SchemaTexts.pretty(a.schemaType(), a.schema()),
				SchemaTexts.pretty(b.schemaType(), b.schema()), MAX_DIFF_LINES);
		out.put("mode", "lines");
		out.putAll(r.toMap());
		if (r.identical()) {
			out.put("message", "Satır bazında fark yok (sadece biçim farkı olabilir)");
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
				compat.put("levelNote", "Sadece bu iki versiyon karşılaştırıldı; " + level
						+ " tüm eski versiyonlara bakar");
			}
		}
	}

	/** Avro şemasını referanslarıyla birlikte parse eder (referanslar önce aynı parser'a yüklenir). */
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
				throw new ToolException("Referans zinciri çok derin/büyük ya da süre doldu");
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
					"Şema bu subject'te zaten kayıtlıydı (v" + existing.version() + "); yeni versiyon oluşmadı");
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
			return new ToolException("Şema uyumsuz (409): '" + subject + "' subject'inin compatibility seviyesi ("
					+ level + ") yeni şemayı reddetti. SR: " + cleanSrMessage(e.srMessage())
					+ " — ayrıntı için schemas_compatibility_check, fark için schemas_diff kullanın.", e);
		}
		return invalidSchemaError(e);
	}

	/**
	 * SR 42201'i (geçersiz şema) sade mesaja çevirir. Diğer 422 kodları (42205 READONLY/IMPORT modu, 42207, 42208
	 * geçersiz subject, 42209 vb.) şema hatası değildir: istemcinin Türkçe ipucuyla aynen döner.
	 */
	static ToolException invalidSchemaError(SchemaRegistryException e) {
		if (e.status() == 422 && e.errorCode() == INVALID_SCHEMA_CODE) {
			return new ToolException("Geçersiz şema (422/" + INVALID_SCHEMA_CODE + "): " + cleanSrMessage(e.srMessage()),
					e);
		}
		return e;
	}

	/** SR mesajından eski şema dökümünü atar ve uzunluğu sınırlar. */
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
				throw new ToolException("Subject bulunamadı ya da aktif versiyonu yok: " + subject
						+ ". Soft-delete edilmiş versiyonu kalıcı silmek için versiyon numarası verin (schemas_versions_list deleted=true)");
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
					throw new ToolException("Versiyon bulunamadı: " + subject + " v" + version
							+ (all.isEmpty() ? " (subject yok)" : " (mevcut: " + all + ")"));
				}
			}
			api.deleteVersion(cluster, subject, version, true);
		}
		List<Integer> remaining = api.versionsOrEmpty(cluster, subject, false);
		String note = permanent ? "Versiyon kalıcı olarak silindi (geri alınamaz)"
				: "Versiyon soft-delete edildi; deleted=true ile görülür, permanent=true ile kalıcı silinebilir";
		return Results.map("subject", subject, "deletedVersion", version, "permanent", permanent, "softDeletedNow",
				softNow, "remainingVersions", remaining, "note",
				remaining.isEmpty() ? note + ". Subject'te aktif versiyon kalmadı" : note);
	}

}
