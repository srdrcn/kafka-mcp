package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.SchemaRegistryIntegrationTest;

/** schemas_compatibility_get / _set / _check (subject and global level). */
class CompatibilityToolsTest extends SchemaRegistryIntegrationTest {

	@Test
	void getShowsSubjectAndGlobalLevelsAndWhichApplies() {
		String global = SrRest.globalLevel();
		JsonNode g = ok("schemas_compatibility_get", args());
		assertThat(g.get("globalLevel").asText()).isEqualTo(global);
		assertThat(g.get("effective").asText()).isEqualTo(global);
		assertThat(g.get("appliesFrom").asText()).isEqualTo("global");
		assertThat(g.has("subject")).isFalse();

		String s = unique("cget") + "-value";
		SrRest.register(s, SrRest.V1);
		JsonNode inherited = ok("schemas_compatibility_get", args("subject", s));
		assertThat(inherited.has("subjectLevel")).isFalse();
		assertThat(inherited.get("effective").asText()).isEqualTo(global);
		assertThat(inherited.get("appliesFrom").asText()).isEqualTo("global");
		assertThat(inherited.get("note").asText()).contains("global");

		SrRest.setSubjectLevel(s, "FULL_TRANSITIVE");
		JsonNode own = ok("schemas_compatibility_get", args("cluster", "test-ro", "subject", s));
		assertThat(own.get("subjectLevel").asText()).isEqualTo("FULL_TRANSITIVE");
		assertThat(own.get("globalLevel").asText()).isEqualTo(global);
		assertThat(own.get("effective").asText()).isEqualTo("FULL_TRANSITIVE");
		assertThat(own.get("appliesFrom").asText()).isEqualTo("subject");
	}

	@Test
	void setSubjectLevelReturnsBeforeAndAfter() {
		String s = unique("cset") + "-value";
		SrRest.register(s, SrRest.V1);
		String global = SrRest.globalLevel();

		JsonNode first = ok("schemas_compatibility_set", args("subject", s, "level", "forward_transitive"));
		assertThat(first.get("scope").asText()).isEqualTo("subject");
		assertThat(first.has("before")).isFalse();
		assertThat(first.get("beforeInherited").asText()).isEqualTo(global);
		assertThat(first.get("after").asText()).isEqualTo("FORWARD_TRANSITIVE");
		assertThat(first.get("changed").asBoolean()).isTrue();
		assertThat(SrRest.subjectLevel(s)).isEqualTo("FORWARD_TRANSITIVE");

		JsonNode second = ok("schemas_compatibility_set", args("subject", s, "level", "FULL"));
		assertThat(second.get("before").asText()).isEqualTo("FORWARD_TRANSITIVE");
		assertThat(second.get("after").asText()).isEqualTo("FULL");
		assertThat(SrRest.subjectLevel(s)).isEqualTo("FULL");
		assertThat(SrRest.globalLevel()).isEqualTo(global);

		assertThat(error("schemas_compatibility_set", args("subject", s, "level", "SIDEWAYS")))
			.contains("Invalid compatibility level");
	}

	@Test
	void setGlobalLevelAndRestore() {
		String original = SrRest.globalLevel();
		String target = "NONE".equals(original) ? "FULL" : "NONE";
		try {
			JsonNode r = ok("schemas_compatibility_set", args("level", target));
			assertThat(r.get("scope").asText()).isEqualTo("global");
			assertThat(r.get("before").asText()).isEqualTo(original);
			assertThat(r.get("after").asText()).isEqualTo(target);
			assertThat(r.get("changed").asBoolean()).isTrue();
			assertThat(SrRest.globalLevel()).isEqualTo(target);
			assertThat(ok("schemas_compatibility_get", args()).get("globalLevel").asText()).isEqualTo(target);
		}
		finally {
			SrRest.setGlobalLevel(original);
		}
		assertThat(SrRest.globalLevel()).isEqualTo(original);
	}

	@Test
	void checkReportsCompatibilityWithSrMessages() {
		String s = unique("ccheck") + "-value";
		SrRest.register(s, SrRest.V1);

		JsonNode okResult = ok("schemas_compatibility_check", args("subject", s, "schema", SrRest.V2));
		assertThat(okResult.get("compatible").asBoolean()).isTrue();
		assertThat(okResult.get("againstVersion").asText()).isEqualTo("latest");
		assertThat(okResult.get("schemaType").asText()).isEqualTo("AVRO");
		assertThat(okResult.get("compatibilityLevel").asText()).isEqualTo(SrRest.globalLevel());

		JsonNode bad = ok("schemas_compatibility_check",
				args("subject", s, "schema", SrRest.INCOMPATIBLE, "schemaType", "AVRO", "version", "1"));
		assertThat(bad.get("compatible").asBoolean()).isFalse();
		assertThat(bad.get("againstVersion").asInt()).isEqualTo(1);
		List<String> messages = new ArrayList<>();
		bad.get("messages").forEach(m -> messages.add(m.asText()));
		assertThat(messages).anySatisfy(m -> assertThat(m).contains("TYPE_MISMATCH"));
		assertThat(messages).anySatisfy(m -> assertThat(m).contains("READER_FIELD_MISSING_DEFAULT_VALUE"));
		assertThat(messages).noneSatisfy(m -> assertThat(m).startsWith("{oldSchema:"));
		// the check does not register the schema
		assertThat(SrRest.versions(s, false)).containsExactly(1);

		JsonNode fresh = ok("schemas_compatibility_check", args("subject", unique("fresh"), "schema", SrRest.V1));
		assertThat(fresh.get("compatible").asBoolean()).isTrue();
		assertThat(fresh.get("note").asText()).contains("no active versions");

		assertThat(error("schemas_compatibility_check", args("subject", s, "schema", "{not json")))
			.contains("Schema is invalid").contains("Invalid schema");
		assertThat(error("schemas_compatibility_check", args("subject", s, "schema", SrRest.V2, "schemaType", "XML")))
			.contains("schemaType");
	}

}
