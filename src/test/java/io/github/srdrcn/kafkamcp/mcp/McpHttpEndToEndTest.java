package io.github.srdrcn.kafkamcp.mcp;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;

import io.github.srdrcn.kafkamcp.KafkaMcpApplication;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.support.TestConfigs;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;

/** Resmi MCP Java istemcisiyle Streamable HTTP üzerinden uçtan uca: initialize, tools/list, tools/call. */
@SpringBootTest(classes = { KafkaMcpApplication.class, McpHttpEndToEndTest.Config.class },
		webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "kafka-mcp.transport=http")
class McpHttpEndToEndTest {

	@LocalServerPort
	int port;

	@TestConfiguration(proxyBeanMethods = false)
	static class Config {

		@Bean
		KafkaMcpConfig kafkaMcpConfig() {
			KafkaMcpConfig c = TestConfigs.kafka(null, null);
			c.setApiKey("e2e-key");
			return c;
		}

	}

	@Test
	void initializeListAndCall() {
		HttpClientStreamableHttpTransport transport = HttpClientStreamableHttpTransport
			.builder("http://localhost:" + port)
			.endpoint("/mcp")
			.httpRequestCustomizer((builder, method, uri, body, ctx) -> builder.header("X-Authorization", "e2e-key"))
			.build();
		try (McpSyncClient client = McpClient.sync(transport).requestTimeout(Duration.ofSeconds(30)).build()) {
			McpSchema.InitializeResult init = client.initialize();
			assertThat(init.serverInfo().name()).isEqualTo("kafka-mcp");
			assertThat(client.listTools().tools()).extracting(McpSchema.Tool::name).contains("clusters_list");
			McpSchema.CallToolResult r = client
				.callTool(new McpSchema.CallToolRequest("clusters_list", Map.of("cluster", TestConfigs.CLUSTER)));
			assertThat(r.isError()).isFalse();
			String text = ((McpSchema.TextContent) r.content().get(0)).text();
			assertThat(text).contains("\"status\":\"ok\"").contains("\"brokerCount\":1");
		}
	}

}
