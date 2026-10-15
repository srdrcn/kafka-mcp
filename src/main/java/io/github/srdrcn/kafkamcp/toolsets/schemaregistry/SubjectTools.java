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

	/** Maximum number of subjects whose latest version subjects_for_topic looks up. */
	static final int MAX_TOPIC_LOOKUPS = 20;

	/** Maximum number of possible (inexact) matches that subjects_for_topic lists by name. */
	static final int MAX_POSSIBLE_MATCHES = 20;

	/** TopicRecordNameStrategy record name: Avro/Protobuf full name (contains no hyphen). */
	private static final Pattern RECORD_NAME = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*$");

	private final SchemaRegistryApi api;

	public SubjectTools(SchemaRegistryApi api) {
		this.api = api;
	}

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("subjects_list", "List Schema Registry subject names (sorted); deleted=true also returns soft-deleted ones")
					.schemaRegistry()
					.listParams()
					.param("deleted", P.bool("Also return soft-deleted subjects (default false)"))
					.handler(this::subjectsList),
				ToolSpec.read("subjects_for_topic", "Find the subjects of a topic (<topic>-key/-value and <topic>-<RecordName>) with the latest version, id and type of each")
					.schemaRegistry()
					.required("topic", P.string("Topic name"))
					.handler(this::subjectsForTopic),
				ToolSpec.read("schemas_versions_list", "List the version numbers of a subject; with deleted=true soft-deleted versions are included and marked")
					.schemaRegistry()
					.required("subject", P.string("Subject name"))
					.param("deleted", P.bool("Also return soft-deleted versions (default false)"))
					.param("filter", P.string("Version filter (regex on the version number, partial match)"))
					.param("limit", versionLimit())
					.handler(this::versionsList));
	}

	private static Map<String, Object> versionLimit() {
		Map<String, Object> m = P.integer("Maximum number of versions to return; the newest are kept (default "
				+ ToolSpec.DEFAULT_LIST_LIMIT + ")", 1, ToolSpec.MAX_LIST_LIMIT);
		m.put("default", ToolSpec.DEFAULT_LIST_LIMIT);
		return m;
	}

	/** The versions matching the filter (input in ascending order). */
	private static List<Integer> matching(ToolCall call, List<Integer> versions) {
		return versions.stream().filter(v -> call.matches(String.valueOf(v))).toList();
	}

	/** The newest {@code limit} entries of an ascending list. */
	private static List<Integer> newest(List<Integer> versions, int limit) {
		return versions.subList(Math.max(0, versions.size() - limit), versions.size());
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
		// starts with '<topic>-' but the record name is not a full name (JSON Schema title or another topic): name only
		List<String> possible = new ArrayList<>();
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
			else if (s.startsWith(prefix)) {
				possible.add(s);
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
			out.put("note", "Looked up the latest version of the first " + lookups + " subjects; skipped version info for "
					+ skipped + " subjects (see schemas_get)");
		}
		if (!possible.isEmpty()) {
			out.put("possibleMatchesCount", possible.size());
			out.put("possibleMatches", possible.stream().limit(MAX_POSSIBLE_MATCHES).toList());
			out.put("possibleMatchesNote", "Starts with '" + prefix + "' but the record name is not an Avro/Protobuf full "
					+ "name: may be a TopicRecordNameStrategy subject derived from a JSON Schema title, or a subject of "
					+ "another topic whose name starts with '" + prefix + "'; versions not looked up (schemas_get)");
		}
		if (rows.isEmpty() && possible.isEmpty()) {
			out.put("message", "No subjects found for this topic (" + topic + "-key, " + topic + "-value or " + topic
					+ "-<RecordName>). With RecordNameStrategy the subject name does not depend on the topic "
					+ "(search with subjects_list).");
		}
		else if (rows.isEmpty()) {
			out.put("message", "No exact match; the subjects in possibleMatches may belong to this topic");
		}
		return out;
	}

	Object versionsList(ToolCall call) {
		String subject = call.str("subject");
		String cluster = call.cluster();
		boolean deleted = call.bool("deleted", false);
		int limit = call.limit();
		if (!deleted) {
			List<Integer> versions = api.versions(cluster, subject, false);
			List<Integer> matching = matching(call, versions);
			return Results.map("subject", subject, "count", versions.size(), "latest",
					versions.isEmpty() ? null : versions.get(versions.size() - 1), "versions", newest(matching, limit),
					"truncated", matching.size() > limit ? true : null);
		}
		List<Integer> all = api.versions(cluster, subject, true);
		Set<Integer> active = new HashSet<>(api.versionsOrEmpty(cluster, subject, false));
		List<Integer> activeSorted = all.stream().filter(active::contains).toList();
		List<Integer> matching = matching(call, all);
		List<Integer> shown = newest(matching, limit);
		return Results.map("subject", subject, "count", all.size(), "latest",
				activeSorted.isEmpty() ? null : activeSorted.get(activeSorted.size() - 1), "versions", shown,
				"truncated", matching.size() > limit ? true : null, "softDeletedVersions",
				shown.stream().filter(v -> !active.contains(v)).toList(), "note",
				activeSorted.isEmpty() ? "All versions are soft-deleted (the subject is soft-deleted)" : null);
	}

}
