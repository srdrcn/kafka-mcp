package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient.SchemaRegistryException;

/** schemas_compatibility_get, schemas_compatibility_check, schemas_compatibility_set. */
@Component
public class CompatibilityTools {

	static final int MAX_MESSAGES = 20;

	private static final int MAX_MESSAGE_CHARS = 500;

	private final SchemaRegistryApi api;

	public CompatibilityTools(SchemaRegistryApi api) {
		this.api = api;
	}

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("schemas_compatibility_get", "Compatibility seviyesini verir: subject seviyesi, global seviye ve hangisinin geçerli olduğu")
					.schemaRegistry()
					.param("subject", P.string("Subject adı (verilmezse sadece global seviye)"))
					.handler(this::get),
				ToolSpec.read("schemas_compatibility_check", "Şemanın subject versiyonuyla uyumlu olup olmadığını kaydetmeden test eder; SR'ın ayrıntılı mesajlarını verir")
					.schemaRegistry()
					.required("subject", P.string("Subject adı"))
					.required("schema", SchemaTools.schemaParam())
					.param("schemaType", SchemaTools.schemaTypeParam())
					.param("version", P.string("Karşılaştırılacak versiyon ya da latest (varsayılan latest)"))
					.param("references", SchemaReference.param())
					.handler(this::check),
				ToolSpec.write("schemas_compatibility_set", "Subject'in (verilmezse global) compatibility seviyesini değiştirir; önceki ve yeni değeri döner")
					.schemaRegistry()
					.param("subject", P.string("Subject adı (verilmezse global seviye değişir)"))
					.required("level", P.enumOf("Yeni compatibility seviyesi", SchemaRegistryApi.LEVELS))
					.handler(this::set));
	}

	Object get(ToolCall call) {
		String cluster = call.cluster();
		String subject = call.str("subject", null);
		String global = api.globalCompatibility(cluster);
		if (subject == null) {
			return Results.map("globalLevel", global, "effective", global, "appliesFrom", "global");
		}
		String subjectLevel = api.subjectCompatibility(cluster, subject);
		boolean own = subjectLevel != null;
		// subject seviyesi yoksa geçerli değer SR'dan (defaultToGlobal=true; context varsayılanlarını da kapsar)
		String effective = own ? subjectLevel : api.effectiveCompatibility(cluster, subject);
		return Results.map("subject", subject, "subjectLevel", subjectLevel, "globalLevel", global, "effective",
				effective, "appliesFrom", own ? "subject" : "global", "note",
				own ? "Subject seviyesindeki tanım global seviyeyi geçersiz kılar"
						: "Subject seviyesinde tanım yok; global seviye geçerli");
	}

	Object check(ToolCall call) {
		String cluster = call.cluster();
		String subject = call.str("subject");
		SchemaInput in = SchemaInput.fromCall(call);
		Integer version = SchemaRegistryApi.parseVersion(call.str("version", null), "version");
		SchemaRegistryApi.CompatibilityCheck r;
		try {
			r = api.checkCompatibility(cluster, subject, version, in);
		}
		catch (SchemaRegistryException e) {
			if (e.status() == 422) {
				throw new ToolException("Geçersiz şema (422" + (e.errorCode() > 0 ? "/" + e.errorCode() : "") + "): "
						+ SchemaTools.cleanSrMessage(e.srMessage()), e);
			}
			throw e;
		}
		if (!r.compatible()) {
			// SR 7.x geçersiz şemada 422 yerine is_compatible=false + "Invalid schema ..." mesajı döner
			r.messages()
				.stream()
				.filter(m -> m.startsWith("Invalid schema"))
				.findFirst()
				.ifPresent(m -> {
					throw new ToolException("Geçersiz şema: " + SchemaTools.cleanSrMessage(m));
				});
		}
		String level;
		try {
			level = api.effectiveCompatibility(cluster, subject);
		}
		catch (ToolException e) {
			level = null;
		}
		List<String> messages = r.messages()
			.stream()
			.filter(m -> !m.startsWith("{oldSchema:"))
			.map(m -> SchemaTexts.truncate(m, MAX_MESSAGE_CHARS))
			.toList();
		Map<String, Object> out = Results.map("subject", subject, "againstVersion",
				version == null ? SchemaRegistryApi.LATEST : version, "schemaType", in.schemaType(), "compatible",
				r.compatible(), "compatibilityLevel", level);
		if (!messages.isEmpty()) {
			out.put("messages", messages.stream().limit(MAX_MESSAGES).toList());
			if (messages.size() > MAX_MESSAGES) {
				out.put("messagesOmitted", messages.size() - MAX_MESSAGES);
			}
		}
		else if (r.compatible() && api.versionsOrEmpty(cluster, subject, false).isEmpty()) {
			out.put("note", "Subject'te aktif versiyon yok; ilk şema her zaman uyumlu sayılır");
		}
		return out;
	}

	Object set(ToolCall call) {
		String cluster = call.cluster();
		String level = SchemaRegistryApi.normalizeLevel(call.str("level"));
		String subject = call.str("subject", null);
		if (subject == null) {
			String before = api.globalCompatibility(cluster);
			String after = api.setCompatibility(cluster, null, level);
			return Results.map("scope", "global", "before", before, "after", after, "changed",
					!Objects.equals(before, after));
		}
		String before = api.subjectCompatibility(cluster, subject);
		String inherited = before == null ? api.globalCompatibility(cluster) : null;
		String after = api.setCompatibility(cluster, subject, level);
		return Results.map("scope", "subject", "subject", subject, "before", before, "beforeInherited", inherited,
				"after", after, "changed", !Objects.equals(before != null ? before : inherited, after), "note",
				before == null ? "Önceden subject seviyesinde tanım yoktu (global " + inherited + " geçerliydi)" : null);
	}

}
