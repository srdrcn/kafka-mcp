package io.github.srdrcn.kafkamcp.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.config.ConfigException;
import io.github.srdrcn.kafkamcp.config.ConfigLoader;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;

/** readOnly (global + cluster override), disableDestructive and toolset selection. */
class ToolFilterTest {

	private static final List<Toolset> TOOLSETS = List.of(new FakeToolset());

	private static KafkaMcpConfig config(String head) {
		return ConfigLoader.parse(head + """
				clusters:
				  - name: a
				    bootstrapServers: a:9092
				    connectUrl: http://a:8083
				    schemaRegistryUrl: http://a:8081
				  - name: b
				    bootstrapServers: b:9092
				""");
	}

	private static Map<String, List<String>> select(KafkaMcpConfig config) {
		return ToolFilter.select(config, TOOLSETS)
			.stream()
			.collect(Collectors.toMap(t -> t.spec().name(), RegisteredTool::clusters, (x, y) -> x,
					java.util.LinkedHashMap::new));
	}

	@Test
	void everythingOpen() {
		Map<String, List<String>> tools = select(config("toolsets: [fake]\n"));
		assertThat(tools).containsOnlyKeys("fake_list", "fake_update", "fake_delete", "fake_offsets_reset",
				"fake_connect_list", "fake_connect_pause", "fake_subjects_list", "fake_slow");
		assertThat(tools.get("fake_list")).containsExactly("a", "b");
		assertThat(tools.get("fake_connect_list")).containsExactly("a");
		assertThat(tools.get("fake_subjects_list")).containsExactly("a");
	}

	@Test
	void readOnlyHidesAllWriteTools() {
		Map<String, List<String>> tools = select(config("toolsets: [fake]\nreadOnly: true\ndisableDestructive: false\n"));
		assertThat(tools).containsOnlyKeys("fake_list", "fake_connect_list", "fake_subjects_list", "fake_slow");
	}

	@Test
	void clusterReadOnlyOverrideNarrowsEnum() {
		KafkaMcpConfig c = config("toolsets: [fake]\nreadOnly: true\n");
		c.cluster("b").setReadOnly(false);
		Map<String, List<String>> tools = select(c);
		assertThat(tools.get("fake_update")).containsExactly("b");
		assertThat(tools.get("fake_delete")).containsExactly("b");
		assertThat(tools.get("fake_list")).containsExactly("a", "b");
		// b has no connectUrl and a is readOnly: the connect write tool is available on no cluster
		assertThat(tools).doesNotContainKey("fake_connect_pause");

		KafkaMcpConfig c2 = config("toolsets: [fake]\nreadOnly: false\n");
		c2.cluster("a").setReadOnly(true);
		assertThat(select(c2).get("fake_update")).containsExactly("b");
	}

	@Test
	void disableDestructiveHidesOnlyDeleteAndReset() {
		Map<String, List<String>> tools = select(config("toolsets: [fake]\ndisableDestructive: true\n"));
		assertThat(tools).doesNotContainKeys("fake_delete", "fake_offsets_reset");
		assertThat(tools).containsKeys("fake_update", "fake_connect_pause", "fake_list");
	}

	@Test
	void toolsetSelection() {
		assertThat(ToolFilter.select(config("toolsets: []\n"), TOOLSETS)).isEmpty();
		assertThatThrownBy(() -> ToolFilter.select(config("toolsets: [fake, nope]\n"), TOOLSETS))
			.isInstanceOf(ConfigException.class)
			.hasMessageContaining("Unknown toolset: [nope]");
	}

	@Test
	void schemaHasClusterEnumAndRequired() {
		KafkaMcpConfig c = config("toolsets: [fake]\n");
		RegisteredTool t = ToolFilter.select(c, TOOLSETS)
			.stream()
			.filter(x -> x.spec().name().equals("fake_update"))
			.findFirst()
			.orElseThrow();
		Map<String, Object> schema = t.tool().inputSchema();
		assertThat(schema.get("type")).isEqualTo("object");
		assertThat(schema.get("required")).isEqualTo(List.of("cluster", "name"));
		@SuppressWarnings("unchecked")
		Map<String, Object> cluster = (Map<String, Object>) ((Map<String, Object>) schema.get("properties"))
			.get("cluster");
		assertThat(cluster.get("enum")).isEqualTo(List.of("a", "b"));
		assertThat(t.tool().description()).startsWith("WRITE:");
		assertThat(t.tool().annotations().readOnlyHint()).isFalse();
	}

	@Test
	void runtimeRejectsClusterOutsideEnum() {
		KafkaMcpConfig c = config("toolsets: [fake]\n");
		c.cluster("a").setReadOnly(true);
		ToolRegistry registry = new ToolRegistry(c, TOOLSETS, new KafkaClients(c));
		var r = registry.call("fake_update", Map.of("cluster", "a", "name", "x"));
		assertThat(r.isError()).isTrue();
		assertThat(((io.modelcontextprotocol.spec.McpSchema.TextContent) r.content().get(0)).text())
			.contains("readOnly");
		var unknown = registry.call("fake_list", Map.of("cluster", "zzz"));
		assertThat(unknown.isError()).isTrue();
		var missing = registry.call("fake_list", Map.of());
		assertThat(missing.isError()).isTrue();
		assertThat(registry.call("fake_update", Map.of("cluster", "b", "name", "x")).isError()).isFalse();
		registry.destroy();
	}

	@Test
	void toolTimesOutAfterFifteenSeconds() {
		KafkaMcpConfig c = config("toolsets: [fake]\n");
		ToolRegistry registry = new ToolRegistry(c, TOOLSETS, new KafkaClients(c));
		long start = System.nanoTime();
		var r = registry.call("fake_slow", Map.of("cluster", "a"));
		long ms = (System.nanoTime() - start) / 1_000_000;
		assertThat(r.isError()).isTrue();
		assertThat(((io.modelcontextprotocol.spec.McpSchema.TextContent) r.content().get(0)).text())
			.contains("timeout");
		assertThat(ms).isBetween(14_000L, 17_000L);
		registry.destroy();
	}

}
