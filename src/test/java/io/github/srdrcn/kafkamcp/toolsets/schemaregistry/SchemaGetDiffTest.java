package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.SchemaRegistryIntegrationTest;

/** schemas_get and schemas_diff (Avro field diff, JSON line diff, single version). */
class SchemaGetDiffTest extends SchemaRegistryIntegrationTest {

	private static JsonNode change(JsonNode diff, String path, String kind) {
		for (JsonNode c : diff.get("changes")) {
			if (c.get("path").asText().equals(path) && c.get("kind").asText().equals(kind)) {
				return c;
			}
		}
		throw new AssertionError(path + " " + kind + " not found: " + diff.get("changes"));
	}

	@Test
	void getReturnsMetadataAndPrettySchema() {
		String s = unique("get") + "-value";
		int id1 = SrRest.register(s, SrRest.V1);
		int id2 = SrRest.register(s, SrRest.V2);

		JsonNode latest = ok("schemas_get", args("subject", s));
		assertThat(latest.get("subject").asText()).isEqualTo(s);
		assertThat(latest.get("version").asInt()).isEqualTo(2);
		assertThat(latest.get("id").asInt()).isEqualTo(id2);
		assertThat(latest.get("schemaType").asText()).isEqualTo("AVRO");
		assertThat(latest.has("references")).isFalse();
		assertThat(latest.has("truncated")).isFalse();
		String schema = latest.get("schema").asText();
		assertThat(schema).contains("\n  ").contains("\"name\": \"currency\"").contains("\"default\": \"TRY\"");

		JsonNode v1 = ok("schemas_get", args("subject", s, "version", 1));
		assertThat(v1.get("version").asInt()).isEqualTo(1);
		assertThat(v1.get("id").asInt()).isEqualTo(id1);
		assertThat(v1.get("schema").asText()).doesNotContain("currency");
		assertThat(ok("schemas_get", args("subject", s, "version", "latest")).get("version").asInt()).isEqualTo(2);

		JsonNode cut = ok("schemas_get", args("subject", s, "maxChars", 200));
		assertThat(cut.get("truncated").asBoolean()).isTrue();
		assertThat(cut.get("schemaChars").asInt()).isEqualTo(schema.length()).isGreaterThan(200);
		assertThat(cut.get("schema").asText()).hasSize(201).endsWith("…");

		assertThat(error("schemas_get", args("subject", s, "version", "abc"))).contains("latest");
		assertThat(error("schemas_get", args("subject", s, "version", 9))).contains("Version not found");
		assertThat(error("schemas_get", args("subject", unique("missing")))).contains("Subject not found");
	}

	@Test
	void getHandlesJsonAndProtobufSchemaTypes() {
		String js = unique("getjson") + "-value";
		SrRest.register(js, SrRest.JSON_V1, "JSON");
		JsonNode j = ok("schemas_get", args("subject", js));
		assertThat(j.get("schemaType").asText()).isEqualTo("JSON");
		assertThat(j.get("schema").asText()).contains("\"properties\": {").contains("\n");

		String ps = unique("getproto") + "-value";
		SrRest.register(ps, SrRest.PROTO, "PROTOBUF");
		JsonNode p = ok("schemas_get", args("subject", ps));
		assertThat(p.get("schemaType").asText()).isEqualTo("PROTOBUF");
		assertThat(p.get("schema").asText()).contains("message Ping");
	}

	@Test
	void avroDiffDetectsAddedFieldAndTypeChange() {
		String s = unique("diff") + "-value";
		SrRest.register(s, SrRest.V1);
		SrRest.register(s, SrRest.V2);

		JsonNode d = ok("schemas_diff", args("subject", s));
		assertThat(d.get("fromVersion").asInt()).isEqualTo(1);
		assertThat(d.get("toVersion").asInt()).isEqualTo(2);
		assertThat(d.get("mode").asText()).isEqualTo("avro-fields");
		assertThat(d.get("totalChanges").asInt()).isEqualTo(2);
		JsonNode added = change(d, "Order.currency", "FIELD_ADDED");
		assertThat(added.get("to").asText()).isEqualTo("string");
		assertThat(added.get("detail").asText()).contains("\"TRY\"");
		JsonNode type = change(d, "Order.amount", "TYPE_CHANGED");
		assertThat(type.get("from").asText()).isEqualTo("int");
		assertThat(type.get("to").asText()).isEqualTo("long");
		JsonNode compat = d.get("compatibility");
		assertThat(compat.get("backward").asBoolean()).isTrue();
		assertThat(compat.get("forward").asBoolean()).isFalse();
		assertThat(compat.get("hint").asText()).contains("BACKWARD");
		assertThat(compat.get("forwardIssues")).isNotEmpty();
		assertThat(compat.get("subjectLevel").asText()).isEqualTo(SrRest.globalLevel());

		JsonNode reverse = ok("schemas_diff", args("subject", s, "fromVersion", "2", "toVersion", "1"));
		change(reverse, "Order.currency", "FIELD_REMOVED");
		assertThat(change(reverse, "Order.amount", "TYPE_CHANGED").get("detail").asText()).contains("FORWARD");
		assertThat(reverse.get("compatibility").get("backward").asBoolean()).isFalse();
	}

	@Test
	void referencesAreReturnedAndResolvedForAvroDiff() {
		String addressSubject = unique("address") + "-value";
		SrRest.register(addressSubject, """
				{"type":"record","name":"Address","namespace":"com.example","fields":[{"name":"city","type":"string"}]}""");
		Map<String, Object> ref = new LinkedHashMap<>();
		ref.put("name", "com.example.Address");
		ref.put("subject", addressSubject);
		ref.put("version", 1);
		String s = unique("withref") + "-value";
		String v1 = """
				{"type":"record","name":"Customer","namespace":"com.example","fields":[
				{"name":"name","type":"string"},{"name":"address","type":"com.example.Address"}]}""";
		String v2 = """
				{"type":"record","name":"Customer","namespace":"com.example","fields":[
				{"name":"name","type":"string"},{"name":"address","type":"com.example.Address"},
				{"name":"vip","type":"boolean","default":false}]}""";
		assertThat(ok("schemas_register", args("subject", s, "schema", v1, "references", List.of(ref)))
			.get("version").asInt()).isEqualTo(1);
		assertThat(ok("schemas_compatibility_check", args("subject", s, "schema", v2, "references", List.of(ref)))
			.get("compatible").asBoolean()).isTrue();
		assertThat(ok("schemas_register", args("subject", s, "schema", v2, "references", List.of(ref)))
			.get("version").asInt()).isEqualTo(2);

		JsonNode got = ok("schemas_get", args("subject", s));
		JsonNode refs = got.get("references");
		assertThat(refs).hasSize(1);
		assertThat(refs.get(0).get("name").asText()).isEqualTo("com.example.Address");
		assertThat(refs.get(0).get("subject").asText()).isEqualTo(addressSubject);
		assertThat(refs.get(0).get("version").asInt()).isEqualTo(1);

		JsonNode d = ok("schemas_diff", args("subject", s));
		assertThat(d.has("avroParseError")).as("%s", d).isFalse();
		assertThat(d.get("mode").asText()).isEqualTo("avro-fields");
		change(d, "Customer.vip", "FIELD_ADDED");
		assertThat(d.get("compatibility").get("backward").asBoolean()).isTrue();

		assertThat(error("schemas_register",
				args("subject", s, "schema", v2, "references", List.of(Map.of("name", "x")))))
			.contains("name, subject and version");
	}

	@Test
	void diffNeedsTwoDifferentVersions() {
		String single = unique("single") + "-value";
		SrRest.register(single, SrRest.V1);
		assertThat(error("schemas_diff", args("subject", single))).contains("single version");

		String two = unique("two") + "-value";
		SrRest.register(two, SrRest.V1);
		SrRest.register(two, SrRest.V2);
		assertThat(error("schemas_diff", args("subject", two, "fromVersion", 2, "toVersion", "latest")))
			.contains("are the same");
		assertThat(error("schemas_diff", args("subject", two, "toVersion", 1))).contains("No active version before v1");
		assertThat(error("schemas_diff", args("subject", unique("missing")))).contains("Subject not found");
	}

	@Test
	void jsonSchemaDiffUsesLineDiff() {
		String s = unique("diffjson") + "-value";
		SrRest.register(s, SrRest.JSON_V1, "JSON");
		SrRest.setSubjectLevel(s, "NONE");
		SrRest.register(s, SrRest.JSON_V2, "JSON");

		JsonNode d = ok("schemas_diff", args("subject", s));
		assertThat(d.get("schemaType").asText()).isEqualTo("JSON");
		assertThat(d.get("mode").asText()).isEqualTo("lines");
		assertThat(d.get("addedLines").asInt()).isPositive();
		List<String> lines = new ArrayList<>();
		d.get("lines").forEach(l -> lines.add(l.asText()));
		assertThat(lines).anySatisfy(l -> assertThat(l).startsWith("+").contains("\"b\""));
		assertThat(lines).allSatisfy(l -> assertThat(l).matches("^[+-]\\d+: .*"));
	}

}
