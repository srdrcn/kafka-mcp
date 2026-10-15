package io.github.srdrcn.kafkamcp.toolsets.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.RegisteredTool;
import io.github.srdrcn.kafkamcp.mcp.ToolFilter;
import io.github.srdrcn.kafkamcp.mcp.ToolKind;
import io.github.srdrcn.kafkamcp.mcp.ToolRegistry;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.mcp.Toolset;
import io.github.srdrcn.kafkamcp.support.TestConfigs;
import io.modelcontextprotocol.spec.McpSchema;

/** connect araçları sadece connectUrl'si olan cluster'larda sunulur (ağsız). */
class ConnectToolAvailabilityTest {

	static final List<String> TOOLS = List.of("connect_clusters_list", "connect_plugins_list",
			"connect_plugins_validate", "connectors_list", "connectors_get", "connectors_status", "connectors_failed",
			"connectors_topics", "connectors_offsets", "connectors_task_distribution", "connectors_config_diff",
			"connectors_pause", "connectors_resume", "connectors_stop", "connectors_restart",
			"connectors_task_restart", "connectors_create_or_update", "connectors_delete",
			"connectors_offsets_reset");

	private static KafkaMcpConfig config() {
		// alpha: connect + yazılabilir; beta: readOnly, connectUrl yok
		KafkaMcpConfig c = TestConfigs.offline();
		c.setToolsets(List.of("connect"));
		return c;
	}

	private static Toolset toolset(KafkaMcpConfig c) {
		ConnectApi api = new ConnectApi(new ConnectClient(new KafkaClients(c)));
		return new ConnectToolset(new ConnectClusterTools(api), new ConnectorReadTools(api),
				new ConnectorAnalysisTools(api), new ConnectorWriteTools(api));
	}

	@Test
	void allToolsDeclaredAndConnectOnly() {
		KafkaMcpConfig c = config();
		Toolset t = toolset(c);
		assertThat(t.name()).isEqualTo("connect");
		assertThat(t.tools()).extracting(ToolSpec::name).containsExactlyInAnyOrderElementsOf(TOOLS);
		for (ToolSpec s : t.tools()) {
			assertThat(s.requirement()).as(s.name()).isEqualTo(io.github.srdrcn.kafkamcp.mcp.ClusterRequirement.CONNECT);
			assertThat(s.properties()).as(s.name()).doesNotContainKey("cluster");
		}
		assertThat(t.tools()).filteredOn(s -> s.kind() == ToolKind.DESTRUCTIVE)
			.extracting(ToolSpec::name)
			.containsExactlyInAnyOrder("connectors_delete", "connectors_offsets_reset");
		assertThat(t.tools()).filteredOn(ToolSpec::clusterOptional)
			.extracting(ToolSpec::name)
			.containsExactly("connect_clusters_list");
	}

	@Test
	void offeredOnlyForClustersWithConnectUrl() {
		KafkaMcpConfig c = config();
		Toolset t = toolset(c);
		List<RegisteredTool> tools = ToolFilter.select(c, List.of(t));
		assertThat(tools).hasSize(TOOLS.size());
		assertThat(tools).allSatisfy(r -> assertThat(r.clusters()).as(r.spec().name()).containsExactly("alpha"));

		c.cluster("alpha").setConnectUrl(null);
		assertThat(ToolFilter.select(c, List.of(t))).as("connectUrl'siz config'te connect aracı yok").isEmpty();
	}

	@Test
	void registryRejectsClusterWithoutConnectUrl() {
		KafkaMcpConfig c = config();
		ToolRegistry registry = new ToolRegistry(c, List.of(toolset(c)), new KafkaClients(c));
		try {
			McpSchema.CallToolResult r = registry.call("connectors_list", Map.of("cluster", "beta"));
			assertThat(r.isError()).isTrue();
			assertThat(((McpSchema.TextContent) r.content().get(0)).text()).contains("connectUrl");
			McpSchema.CallToolResult w = registry.call("connectors_pause", Map.of("cluster", "beta", "name", "x"));
			assertThat(w.isError()).isTrue();
		}
		finally {
			registry.destroy();
		}
	}

}
