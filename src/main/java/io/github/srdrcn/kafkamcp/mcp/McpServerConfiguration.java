package io.github.srdrcn.kafkamcp.mcp;

import java.util.List;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpStatelessServerFeatures;
import io.modelcontextprotocol.server.McpStatelessSyncServer;
import io.modelcontextprotocol.server.transport.HttpServletStatelessServerTransport;
import io.modelcontextprotocol.spec.McpSchema;

/**
 * MCP server. In HTTP mode, Streamable HTTP on {@code /mcp} (stateless: no session is kept, so the client
 * carries on even if the pod restarts). The stdio mode lives in {@link StdioServer}.
 */
@Configuration(proxyBeanMethods = false)
public class McpServerConfiguration {

	public static final String ENDPOINT = "/mcp";

	public static final String SERVER_NAME = "kafka-mcp";

	static final String INSTRUCTIONS = """
			MCP server for Kafka clusters. Every tool takes a `cluster` parameter (valid names are in the \
			schema enum; use clusters_list to see their status). Listing tools take `filter` (regex) and \
			`limit`. Tools that read messages take `maxRecords` (default 50, max 500), never create a \
			consumer group and never commit offsets. Tools whose description starts with "WRITE:" change \
			the cluster. Each tool runs for at most 15 s.""";

	@Bean
	McpJsonMapper mcpJsonMapper() {
		return new JacksonMcpJsonMapper(new ObjectMapper());
	}

	static String version(ObjectProvider<BuildProperties> build) {
		BuildProperties props = build.getIfAvailable();
		return props == null ? "dev" : props.getVersion();
	}

	@Bean
	@ConditionalOnProperty(name = "kafka-mcp.transport", havingValue = "http")
	HttpServletStatelessServerTransport mcpTransport(McpJsonMapper jsonMapper) {
		return HttpServletStatelessServerTransport.builder().jsonMapper(jsonMapper).messageEndpoint(ENDPOINT).build();
	}

	@Bean(destroyMethod = "close")
	@ConditionalOnProperty(name = "kafka-mcp.transport", havingValue = "http")
	McpStatelessSyncServer mcpServer(HttpServletStatelessServerTransport transport, ToolRegistry registry,
			McpJsonMapper jsonMapper, ObjectProvider<BuildProperties> build) {
		List<McpStatelessServerFeatures.SyncToolSpecification> specs = registry.tools()
			.stream()
			.map(t -> new McpStatelessServerFeatures.SyncToolSpecification(t.tool(),
					(ctx, request) -> registry.call(request.name(), request.arguments())))
			.toList();
		return McpServer.sync(transport)
			.serverInfo(SERVER_NAME, version(build))
			.instructions(INSTRUCTIONS)
			.jsonMapper(jsonMapper)
			.capabilities(McpSchema.ServerCapabilities.builder().tools(false).build())
			.tools(specs)
			.build();
	}

	@Bean
	@ConditionalOnProperty(name = "kafka-mcp.transport", havingValue = "http")
	ServletRegistrationBean<HttpServletStatelessServerTransport> mcpServlet(HttpServletStatelessServerTransport transport,
			McpStatelessSyncServer server) {
		ServletRegistrationBean<HttpServletStatelessServerTransport> reg = new ServletRegistrationBean<>(transport,
				ENDPOINT);
		reg.setName("mcp");
		reg.setAsyncSupported(true);
		reg.setLoadOnStartup(1);
		return reg;
	}

	@Bean
	@ConditionalOnProperty(name = "kafka-mcp.transport", havingValue = "http")
	FilterRegistrationBean<ApiKeyFilter> apiKeyFilter(KafkaMcpConfig config) {
		FilterRegistrationBean<ApiKeyFilter> reg = new FilterRegistrationBean<>(new ApiKeyFilter(config.getApiKey()));
		reg.addUrlPatterns(ENDPOINT, ENDPOINT + "/*");
		reg.setOrder(Ordered.HIGHEST_PRECEDENCE);
		reg.setName("apiKeyFilter");
		return reg;
	}

}
