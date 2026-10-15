package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient;
import io.github.srdrcn.kafkamcp.support.SchemaRegistryIntegrationTest;

/** schemas_register and schemas_delete_version; writes are rejected on a readOnly cluster. */
class SchemaWriteToolsTest extends SchemaRegistryIntegrationTest {

	private static List<Integer> ints(JsonNode array) {
		List<Integer> out = new ArrayList<>();
		array.forEach(n -> out.add(n.asInt()));
		return out;
	}

	@Test
	void registerCreatesVersionsAndIsIdempotent() throws Exception {
		String s = unique("reg") + "-value";
		JsonNode first = ok("schemas_register", args("subject", s, "schema", SrRest.V1));
		assertThat(first.get("created").asBoolean()).isTrue();
		assertThat(first.get("version").asInt()).isEqualTo(1);
		assertThat(first.get("id").asInt()).isPositive();
		assertThat(first.get("schemaType").asText()).isEqualTo("AVRO");
		assertThat(SrRest.versions(s, false)).containsExactly(1);

		JsonNode again = ok("schemas_register", args("subject", s, "schema", SrRest.V1));
		assertThat(again.get("created").asBoolean()).isFalse();
		assertThat(again.get("version").asInt()).isEqualTo(1);
		assertThat(again.get("id").asInt()).isEqualTo(first.get("id").asInt());
		assertThat(again.get("message").asText()).contains("already registered");

		// the schema can also be given as a JSON object
		Map<?, ?> asObject = new ObjectMapper().readValue(SrRest.V2, Map.class);
		JsonNode second = ok("schemas_register", args("subject", s, "schema", asObject));
		assertThat(second.get("created").asBoolean()).isTrue();
		assertThat(second.get("version").asInt()).isEqualTo(2);
		assertThat(SrRest.versions(s, false)).containsExactly(1, 2);
		assertThat(SrRest.ok("GET", SrRest.subjectPath(s) + "/versions/2", null).get("id").asInt())
			.isEqualTo(second.get("id").asInt());
	}

	@Test
	void registerJsonSchemaWithSchemaType() {
		String s = unique("regjson") + "-value";
		JsonNode r = ok("schemas_register", args("subject", s, "schema", SrRest.JSON_V1, "schemaType", "json"));
		assertThat(r.get("schemaType").asText()).isEqualTo("JSON");
		assertThat(r.get("version").asInt()).isEqualTo(1);
		JsonNode stored = SrRest.ok("GET", SrRest.subjectPath(s) + "/versions/latest", null);
		assertThat(stored.get("schemaType").asText()).isEqualTo("JSON");
	}

	@Test
	void registerRejectsIncompatibleAndInvalidSchemasWithSrMessage() {
		String s = unique("regbad") + "-value";
		SrRest.register(s, SrRest.V1);

		String incompatible = error("schemas_register", args("subject", s, "schema", SrRest.INCOMPATIBLE));
		assertThat(incompatible).contains("409").contains("Incompatible schema").contains(SrRest.globalLevel())
			.contains("TYPE_MISMATCH")
			.doesNotContain("oldSchema:");

		String invalid = error("schemas_register", args("subject", s, "schema", "{not json"));
		assertThat(invalid).contains("422").contains("Schema rejected (422/42201): Invalid schema").doesNotContain("with refs");

		assertThat(SrRest.versions(s, false)).containsExactly(1);
	}

	@Test
	void registerOnReadOnlyModeSubjectReportsModeNotInvalidSchema() {
		String s = unique("regmode") + "-value";
		SrRest.register(s, SrRest.V1);
		String modePath = "/mode/" + SchemaRegistryClient.enc(s);
		SrRest.ok("PUT", modePath, Map.of("mode", "READONLY"));
		try {
			String err = error("schemas_register", args("subject", s, "schema", SrRest.V2));
			assertThat(err).contains("422").contains("42205").contains("READONLY").doesNotContain("Schema rejected");
			assertThat(SrRest.versions(s, false)).containsExactly(1);
		}
		finally {
			SrRest.call("DELETE", modePath, null);
		}
	}

	@Test
	void deleteSoftThenPermanent() {
		String s = unique("del") + "-value";
		SrRest.register(s, SrRest.V1);
		SrRest.register(s, SrRest.V2);

		JsonNode soft = ok("schemas_delete_version", args("subject", s, "version", "1"));
		assertThat(soft.get("deletedVersion").asInt()).isEqualTo(1);
		assertThat(soft.get("permanent").asBoolean()).isFalse();
		assertThat(soft.get("softDeletedNow").asBoolean()).isTrue();
		assertThat(ints(soft.get("remainingVersions"))).containsExactly(2);
		assertThat(SrRest.versions(s, false)).containsExactly(2);
		assertThat(SrRest.versions(s, true)).containsExactly(1, 2);
		assertThat(ints(ok("schemas_versions_list", args("subject", s, "deleted", true)).get("softDeletedVersions")))
			.containsExactly(1);

		// an already soft-deleted version is deleted permanently (no second soft delete is attempted)
		JsonNode hard = ok("schemas_delete_version", args("subject", s, "version", 1, "permanent", true));
		assertThat(hard.get("permanent").asBoolean()).isTrue();
		assertThat(hard.get("softDeletedNow").asBoolean()).isFalse();
		assertThat(SrRest.versions(s, true)).containsExactly(2);

		// an active version with permanent=true: soft first, then permanent
		JsonNode latest = ok("schemas_delete_version", args("subject", s, "version", "latest", "permanent", true));
		assertThat(latest.get("deletedVersion").asInt()).isEqualTo(2);
		assertThat(latest.get("softDeletedNow").asBoolean()).isTrue();
		assertThat(latest.get("remainingVersions")).isEmpty();
		assertThat(latest.get("note").asText()).contains("no active versions left");
		assertThat(SrRest.versions(s, true)).isEmpty();
		assertThat(ok("subjects_list", args("filter", java.util.regex.Pattern.quote(s), "deleted", true))
			.get("total").asInt()).isZero();
	}

	@Test
	void deleteGuards() {
		String s = unique("delguard") + "-value";
		SrRest.register(s, SrRest.V1);
		SrRest.register(s, SrRest.V2);
		SrRest.softDelete(s, 1);

		assertThat(error("schemas_delete_version", args("subject", s, "version", 1))).contains("already soft-deleted");
		assertThat(error("schemas_delete_version", args("subject", s, "version", 7, "permanent", true)))
			.contains("Version not found")
			.contains("[1, 2]");
		assertThat(error("schemas_delete_version", args("subject", s, "version", "last"))).contains("latest");
		assertThat(error("schemas_delete_version", args("subject", unique("missing"), "version", "latest")))
			.contains("Subject not found");
		assertThat(SrRest.versions(s, false)).containsExactly(2);
		assertThat(SrRest.versions(s, true)).containsExactly(1, 2);
	}

	@Test
	void writeToolsAreRejectedOnReadOnlyCluster() {
		String s = unique("ro") + "-value";
		assertThat(error("schemas_register", args("cluster", "test-ro", "subject", s, "schema", SrRest.V1)))
			.contains("readOnly");
		assertThat(SrRest.versions(s, true)).isEmpty();

		SrRest.register(s, SrRest.V1);
		assertThat(error("schemas_delete_version", args("cluster", "test-ro", "subject", s, "version", 1)))
			.contains("readOnly");
		assertThat(error("schemas_compatibility_set", args("cluster", "test-ro", "subject", s, "level", "NONE")))
			.contains("readOnly");
		assertThat(SrRest.versions(s, false)).containsExactly(1);
		assertThat(SrRest.subjectLevel(s)).isNull();

		// read tools work on a readOnly cluster
		assertThat(ok("schemas_get", args("cluster", "test-ro", "subject", s)).get("version").asInt()).isEqualTo(1);
	}

}
