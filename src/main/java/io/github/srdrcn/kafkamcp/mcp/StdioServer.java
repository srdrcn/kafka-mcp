package io.github.srdrcn.kafkamcp.mcp;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.info.BuildProperties;
import org.springframework.stereotype.Component;

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;

/**
 * stdio transport: reads JSON-RPC from stdin and writes to stdout (in this mode logs go to stderr). The
 * application stops when stdin is closed.
 */
@Component
@ConditionalOnProperty(name = "kafka-mcp.transport", havingValue = "stdio")
public class StdioServer implements ApplicationRunner {

	private static final Logger log = LoggerFactory.getLogger(StdioServer.class);

	private final ToolRegistry registry;

	private final McpJsonMapper jsonMapper;

	private final ObjectProvider<BuildProperties> build;

	public StdioServer(ToolRegistry registry, McpJsonMapper jsonMapper, ObjectProvider<BuildProperties> build) {
		this.registry = registry;
		this.jsonMapper = jsonMapper;
		this.build = build;
	}

	@Override
	public void run(ApplicationArguments args) throws InterruptedException {
		CountDownLatch eof = new CountDownLatch(1);
		InputStream in = new FilterInputStream(System.in) {
			@Override
			public int read() throws IOException {
				int r = super.read();
				if (r < 0) {
					eof.countDown();
				}
				return r;
			}

			@Override
			public int read(byte[] b, int off, int len) throws IOException {
				int r = super.read(b, off, len);
				if (r < 0) {
					eof.countDown();
				}
				return r;
			}
		};
		StdioServerTransportProvider transport = new StdioServerTransportProvider(jsonMapper, in, System.out);
		List<McpServerFeatures.SyncToolSpecification> specs = registry.tools()
			.stream()
			.map(t -> new McpServerFeatures.SyncToolSpecification(t.tool(),
					(exchange, request) -> registry.call(request.name(), request.arguments())))
			.toList();
		McpSyncServer server = McpServer.sync(transport)
			.serverInfo(McpServerConfiguration.SERVER_NAME, McpServerConfiguration.version(build))
			.instructions(McpServerConfiguration.INSTRUCTIONS)
			.jsonMapper(jsonMapper)
			.capabilities(McpSchema.ServerCapabilities.builder().tools(false).build())
			.tools(specs)
			.build();
		log.info("stdio transport ready ({} tools)", specs.size());
		eof.await();
		log.info("stdin closed, shutting down");
		server.closeGracefully();
	}

}
