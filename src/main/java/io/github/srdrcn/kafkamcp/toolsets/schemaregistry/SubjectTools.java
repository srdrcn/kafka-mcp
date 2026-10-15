package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient.SchemaRegistryException;

/** subjects_list, subjects_for_topic, schemas_versions_list. */
@Component
public class SubjectTools {

	/** subjects_for_topic'te son versiyonu sorgulanacak en fazla subject. */
	static final int MAX_TOPIC_LOOKUPS = 20;

	/** TopicRecordNameStrategy kayıt adı: Avro/Protobuf tam adı (tire içermez). */
	private static final Pattern RECORD_NAME = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*$");

	private final SchemaRegistryApi api;

	public SubjectTools(SchemaRegistryApi api) {
		this.api = api;
	}

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("subjects_list", "Schema Registry subject adlarını listeler (sıralı); deleted=true soft-delete edilmişleri de getirir")
					.schemaRegistry()
					.listParams()
					.param("deleted", P.bool("Soft-delete edilmiş subject'leri de getir (varsayılan false)"))
					.handler(this::subjectsList),
				ToolSpec.read("subjects_for_topic", "Topic'in subject'lerini bulur (<topic>-key/-value ve <topic>-<KayıtAdı>); her birinin son versiyonu, id ve tipiyle")
					.schemaRegistry()
					.required("topic", P.string("Topic adı"))
					.handler(this::subjectsForTopic),
				ToolSpec.read("schemas_versions_list", "Subject'in versiyon numaralarını listeler; deleted=true ile soft-delete edilmiş versiyonlar da işaretlenir")
					.schemaRegistry()
					.required("subject", P.string("Subject adı"))
					.param("deleted", P.bool("Soft-delete edilmiş versiyonları da getir (varsayılan false)"))
					.handler(this::versionsList));
	}

	Object subjectsList(ToolCall call) {
		boolean deleted = call.bool("deleted", false);
		List<String> all = api.subjects(call.cluster(), deleted);
		List<String> matching = all.stream().filter(call::matches).toList();
		Map<String, Object> out = Results.page("subjects", matching, call.limit());
		if (deleted) {
			Set<String> active = new HashSet<>(api.subjects(call.cluster(), false));
			List<String> soft = matching.stream().filter(s -> !active.contains(s)).toList();
			out.put("softDeletedCount", soft.size());
			out.put("softDeleted", soft.stream().limit(call.limit()).toList());
		}
		return out;
	}

	Object subjectsForTopic(ToolCall call) {
		String topic = call.str("topic");
		String cluster = call.cluster();
		List<String[]> matches = new ArrayList<>();
		String prefix = topic + "-";
		for (String s : api.subjects(cluster, false)) {
			if (s.equals(topic + "-key")) {
				matches.add(new String[] { s, "TopicNameStrategy", "key" });
			}
			else if (s.equals(topic + "-value")) {
				matches.add(new String[] { s, "TopicNameStrategy", "value" });
			}
			else if (s.startsWith(prefix) && RECORD_NAME.matcher(s.substring(prefix.length())).matches()) {
				matches.add(new String[] { s, "TopicRecordNameStrategy", s.substring(prefix.length()) });
			}
		}
		List<Map<String, Object>> rows = new ArrayList<>();
		int lookups = 0;
		int skipped = 0;
		for (String[] m : matches) {
			boolean byTopic = "TopicNameStrategy".equals(m[1]);
			Map<String, Object> row = Results.map("subject", m[0], "strategy", m[1], "part", byTopic ? m[2] : null,
					"recordName", byTopic ? null : m[2]);
			rows.add(row);
			if (lookups >= MAX_TOPIC_LOOKUPS || call.nearDeadline(2_000)) {
				skipped++;
				continue;
			}
			lookups++;
			try {
				SchemaVersion v = api.latestOrNull(cluster, m[0]);
				if (v != null) {
					row.put("latestVersion", v.version());
					row.put("id", v.id());
					row.put("schemaType", v.schemaType());
				}
			}
			catch (SchemaRegistryException e) {
				row.put("error", e.getMessage());
			}
		}
		Map<String, Object> out = Results.map("topic", topic, "found", rows.size(), "subjects", rows);
		if (skipped > 0) {
			out.put("partial", true);
			out.put("lookupsSkipped", skipped);
			out.put("note", "İlk " + lookups + " subject'in son versiyonuna bakıldı; " + skipped
					+ " subject için versiyon bilgisi atlandı (schemas_get ile bakın)");
		}
		if (rows.isEmpty()) {
			out.put("message", "Bu topic için subject bulunamadı (" + topic + "-key, " + topic + "-value ya da " + topic
					+ "-<KayıtAdı>). RecordNameStrategy kullanılıyorsa subject adı topic'ten bağımsızdır (subjects_list ile arayın).");
		}
		return out;
	}

	Object versionsList(ToolCall call) {
		String subject = call.str("subject");
		String cluster = call.cluster();
		boolean deleted = call.bool("deleted", false);
		if (!deleted) {
			List<Integer> versions = api.versions(cluster, subject, false);
			return Results.map("subject", subject, "count", versions.size(), "latest",
					versions.isEmpty() ? null : versions.get(versions.size() - 1), "versions", versions);
		}
		List<Integer> all = api.versions(cluster, subject, true);
		Set<Integer> active = new HashSet<>(api.versionsOrEmpty(cluster, subject, false));
		List<Integer> soft = all.stream().filter(v -> !active.contains(v)).toList();
		List<Integer> activeSorted = all.stream().filter(active::contains).toList();
		return Results.map("subject", subject, "count", all.size(), "latest",
				activeSorted.isEmpty() ? null : activeSorted.get(activeSorted.size() - 1), "versions", all,
				"softDeletedVersions", soft, "note",
				activeSorted.isEmpty() ? "Tüm versiyonlar soft-delete edilmiş (subject soft-delete durumda)" : null);
	}

}
