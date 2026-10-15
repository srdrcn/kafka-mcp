package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.SchemaRegistryIntegrationTest;

/** subjects_list, schemas_versions_list, subjects_for_topic and subjects with special characters. */
class SubjectToolsTest extends SchemaRegistryIntegrationTest {

	private static final String TITLED_JSON = """
			{"title":"Order Created","type":"object","properties":{"id":{"type":"string"}}}""";

	private static List<String> texts(JsonNode array) {
		List<String> out = new ArrayList<>();
		array.forEach(n -> out.add(n.asText()));
		return out;
	}

	private static List<Integer> ints(JsonNode array) {
		List<Integer> out = new ArrayList<>();
		array.forEach(n -> out.add(n.asInt()));
		return out;
	}

	@Test
	void subjectsListFiltersSortsLimitsAndShowsSoftDeleted() {
		String base = unique("sl");
		String a = base + ".a-value";
		String b = base + ".b-value";
		String c = base + ".c-value";
		SrRest.register(c, SrRest.V1);
		SrRest.register(a, SrRest.V1);
		SrRest.register(b, SrRest.V1);
		String filter = "^" + Pattern.quote(base);

		JsonNode r = ok("subjects_list", args("filter", filter));
		assertThat(r.get("total").asInt()).isEqualTo(3);
		assertThat(texts(r.get("subjects"))).containsExactly(a, b, c);
		assertThat(r.has("softDeleted")).isFalse();

		JsonNode limited = ok("subjects_list", args("filter", filter, "limit", 2));
		assertThat(limited.get("returned").asInt()).isEqualTo(2);
		assertThat(limited.get("truncated").asBoolean()).isTrue();
		assertThat(texts(limited.get("subjects"))).containsExactly(a, b);

		SrRest.softDelete(c, 1);
		assertThat(texts(ok("subjects_list", args("filter", filter)).get("subjects"))).containsExactly(a, b);
		JsonNode withDeleted = ok("subjects_list", args("filter", filter, "deleted", true));
		assertThat(texts(withDeleted.get("subjects"))).containsExactly(a, b, c);
		assertThat(withDeleted.get("softDeletedCount").asInt()).isEqualTo(1);
		assertThat(texts(withDeleted.get("softDeleted"))).containsExactly(c);

		// read tools also work on a readOnly cluster
		JsonNode ro = ok("subjects_list", args("cluster", "test-ro", "filter", filter));
		assertThat(ro.get("total").asInt()).isEqualTo(2);
	}

	@Test
	void versionsListMarksSoftDeletedVersions() {
		String s = unique("vl") + "-value";
		SrRest.register(s, SrRest.V1);
		SrRest.register(s, SrRest.V2);

		JsonNode r = ok("schemas_versions_list", args("subject", s));
		assertThat(ints(r.get("versions"))).containsExactly(1, 2);
		assertThat(r.get("latest").asInt()).isEqualTo(2);
		assertThat(r.get("count").asInt()).isEqualTo(2);

		SrRest.softDelete(s, 1);
		assertThat(ints(ok("schemas_versions_list", args("subject", s)).get("versions"))).containsExactly(2);
		JsonNode all = ok("schemas_versions_list", args("subject", s, "deleted", true));
		assertThat(ints(all.get("versions"))).containsExactly(1, 2);
		assertThat(ints(all.get("softDeletedVersions"))).containsExactly(1);
		assertThat(all.get("latest").asInt()).isEqualTo(2);

		SrRest.softDelete(s, 2);
		JsonNode gone = ok("schemas_versions_list", args("subject", s, "deleted", true));
		assertThat(ints(gone.get("softDeletedVersions"))).containsExactly(1, 2);
		assertThat(gone.has("latest")).isFalse();
		assertThat(gone.get("note").asText()).contains("soft-delete");

		assertThat(error("schemas_versions_list", args("subject", unique("missing")))).contains("Subject not found");
	}

	@Test
	void subjectsForTopicFindsTopicAndRecordNameStrategies() {
		String topic = unique("orders");
		SrRest.register(topic + "-key", "\"string\"");
		SrRest.register(topic + "-value", SrRest.V1);
		SrRest.register(topic + "-value", SrRest.V2);
		SrRest.register(topic + "-com.example.Order", SrRest.V1);
		// a subject of another topic (<topic>-v2) must not count as an exact match; it is reported as a possible match
		SrRest.register(topic + "-v2-value", SrRest.V1);
		// JSON Schema TopicRecordNameStrategy: the record name comes from the title (may contain spaces/hyphens)
		SrRest.register(topic + "-Order Created", TITLED_JSON, "JSON");

		JsonNode r = ok("subjects_for_topic", args("topic", topic));
		assertThat(r.get("topic").asText()).isEqualTo(topic);
		assertThat(r.get("found").asInt()).isEqualTo(3);
		assertThat(r.has("message")).isFalse();
		List<String> subjects = new ArrayList<>();
		for (JsonNode row : r.get("subjects")) {
			subjects.add(row.get("subject").asText());
			assertThat(row.get("schemaType").asText()).isEqualTo("AVRO");
			assertThat(row.get("id").asInt()).isPositive();
			if (row.get("subject").asText().equals(topic + "-value")) {
				assertThat(row.get("strategy").asText()).isEqualTo("TopicNameStrategy");
				assertThat(row.get("part").asText()).isEqualTo("value");
				assertThat(row.get("latestVersion").asInt()).isEqualTo(2);
			}
			if (row.get("subject").asText().equals(topic + "-key")) {
				assertThat(row.get("part").asText()).isEqualTo("key");
				assertThat(row.get("latestVersion").asInt()).isEqualTo(1);
			}
			if (row.get("subject").asText().endsWith("Order")) {
				assertThat(row.get("strategy").asText()).isEqualTo("TopicRecordNameStrategy");
				assertThat(row.get("recordName").asText()).isEqualTo("com.example.Order");
			}
		}
		assertThat(subjects).containsExactlyInAnyOrder(topic + "-key", topic + "-value", topic + "-com.example.Order");
		assertThat(r.get("possibleMatchesCount").asInt()).isEqualTo(2);
		assertThat(texts(r.get("possibleMatches"))).containsExactly(topic + "-Order Created", topic + "-v2-value");
		assertThat(r.get("possibleMatchesNote").asText()).contains("JSON Schema title");

		// with only possible matches the message does not say "No subjects found"
		String jsonTopic = unique("events");
		SrRest.register(jsonTopic + "-order-created", TITLED_JSON, "JSON");
		JsonNode onlyPossible = ok("subjects_for_topic", args("topic", jsonTopic));
		assertThat(onlyPossible.get("found").asInt()).isZero();
		assertThat(texts(onlyPossible.get("possibleMatches"))).containsExactly(jsonTopic + "-order-created");
		assertThat(onlyPossible.get("message").asText()).contains("possibleMatches").doesNotContain("No subjects found");

		JsonNode none = ok("subjects_for_topic", args("topic", unique("nosubject")));
		assertThat(none.get("found").asInt()).isZero();
		assertThat(none.get("message").asText()).contains("No subjects found");
		assertThat(none.has("possibleMatches")).isFalse();
	}

	@Test
	void subjectsWithSpecialCharactersAreUrlEncoded() {
		String base = unique("sp");
		List<String> subjects = List.of(base + ".with.dots-value", base + "/with/slash", base + ":colon:x",
				base + " with space", base + ";semi=1-value");
		for (String s : subjects) {
			JsonNode reg = ok("schemas_register", args("subject", s, "schema", SrRest.V1));
			assertThat(reg.get("created").asBoolean()).as(s).isTrue();
			assertThat(reg.get("version").asInt()).as(s).isEqualTo(1);
			assertThat(SrRest.versions(s, false)).as(s).containsExactly(1);

			JsonNode got = ok("schemas_get", args("subject", s));
			assertThat(got.get("subject").asText()).isEqualTo(s);
			assertThat(got.get("schema").asText()).contains("Order");
			assertThat(ints(ok("schemas_versions_list", args("subject", s)).get("versions"))).containsExactly(1);
			assertThat(ok("schemas_compatibility_get", args("subject", s)).get("appliesFrom").asText())
				.isEqualTo("global");
		}
		JsonNode listed = ok("subjects_list", args("filter", "^" + Pattern.quote(base)));
		assertThat(texts(listed.get("subjects"))).containsExactlyInAnyOrderElementsOf(subjects);
	}

}
