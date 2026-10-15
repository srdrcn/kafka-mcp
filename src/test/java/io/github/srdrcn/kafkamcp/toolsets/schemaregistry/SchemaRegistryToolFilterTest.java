package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.RegisteredTool;
import io.github.srdrcn.kafkamcp.mcp.ToolFilter;
import io.github.srdrcn.kafkamcp.mcp.ToolKind;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient;
import io.github.srdrcn.kafkamcp.support.TestConfigs;

/** Araç adları/türleri ve ToolFilter seviyesinde schemaRegistryUrl şartı (ağa çıkmaz). */
class SchemaRegistryToolFilterTest {

	private static SchemaRegistryToolset toolset(KafkaMcpConfig c) {
		SchemaRegistryApi api = new SchemaRegistryApi(new SchemaRegistryClient(new KafkaClients(c)));
		return new SchemaRegistryToolset(new SubjectTools(api), new SchemaTools(api), new CompatibilityTools(api));
	}

	private static KafkaMcpConfig config() {
		KafkaMcpConfig c = TestConfigs.offline();
		c.setToolsets(List.of("schemaregistry"));
		return c;
	}

	private static Map<String, List<String>> select(KafkaMcpConfig c) {
		return ToolFilter.select(c, List.of(toolset(c)))
			.stream()
			.collect(Collectors.toMap(t -> t.spec().name(), RegisteredTool::clusters));
	}

	@Test
	void toolNamesAndKinds() {
		Map<String, ToolKind> kinds = toolset(config()).tools()
			.stream()
			.collect(Collectors.toMap(ToolSpec::name, ToolSpec::kind));
		assertThat(kinds).containsOnlyKeys("subjects_list", "subjects_for_topic", "schemas_versions_list", "schemas_get",
				"schemas_diff", "schemas_compatibility_get", "schemas_compatibility_check", "schemas_register",
				"schemas_compatibility_set", "schemas_delete_version");
		assertThat(kinds.get("schemas_register")).isEqualTo(ToolKind.WRITE);
		assertThat(kinds.get("schemas_compatibility_set")).isEqualTo(ToolKind.WRITE);
		assertThat(kinds.get("schemas_delete_version")).isEqualTo(ToolKind.DESTRUCTIVE);
		assertThat(kinds.entrySet().stream().filter(e -> e.getValue() == ToolKind.READ)).hasSize(7);
		assertThat(toolset(config()).tools()).allSatisfy(t -> assertThat(t.description()).hasSizeLessThan(300));
	}

	@Test
	void offeredOnlyForClustersWithSchemaRegistryUrl() {
		KafkaMcpConfig c = config();
		Map<String, List<String>> tools = select(c);
		assertThat(tools).hasSize(10);
		// alpha: SR var; beta: SR yok (ve readOnly)
		assertThat(tools.values()).allSatisfy(clusters -> assertThat(clusters).containsExactly("alpha"));
		ToolSpec any = toolset(c).tools().get(0);
		assertThat(ToolFilter.reasonUnavailable(c, any, c.cluster("beta"))).contains("schemaRegistryUrl");

		c.cluster("beta").setSchemaRegistryUrl("http://beta.invalid:8081");
		Map<String, List<String>> both = select(c);
		assertThat(both.get("subjects_list")).containsExactly("alpha", "beta");
		// beta readOnly: yazma araçları sadece alpha'da
		assertThat(both.get("schemas_register")).containsExactly("alpha");
		assertThat(both.get("schemas_delete_version")).containsExactly("alpha");

		c.setDisableDestructive(true);
		assertThat(select(c)).doesNotContainKey("schemas_delete_version").containsKey("schemas_register");

		KafkaMcpConfig none = config();
		none.cluster("alpha").setSchemaRegistryUrl(null);
		assertThat(select(none)).isEmpty();
	}

}
