package io.github.srdrcn.kafkamcp.mcp;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

/**
 * Holds the listed tools and runs calls: cluster validation, a 15 s time limit per tool, error
 * translation, rendering the result as JSON text and a single-line JSON log to stdout.
 */
@Component
public class ToolRegistry implements DisposableBean {

	private static final Logger log = LoggerFactory.getLogger(ToolRegistry.class);

	private static final ObjectMapper RESULT_MAPPER = JsonMapper.builder()
		.addModule(new JavaTimeModule())
		.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
		.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
		.serializationInclusion(JsonInclude.Include.NON_NULL)
		.build();

	private final KafkaMcpConfig config;

	private final KafkaClients clients;

	private final Map<String, RegisteredTool> tools = new LinkedHashMap<>();

	private final ExecutorService executor = Executors.newThreadPerTaskExecutor(
			Thread.ofVirtual().name("tool-", 0).factory());

	public ToolRegistry(KafkaMcpConfig config, List<Toolset> toolsets, KafkaClients clients) {
		this.config = config;
		this.clients = clients;
		for (RegisteredTool t : ToolFilter.select(config, toolsets)) {
			tools.put(t.spec().name(), t);
		}
		log.info("{} tools listed (toolsets={}, readOnly={}, disableDestructive={})", tools.size(),
				config.getToolsets(), config.isReadOnly(), config.isDisableDestructive());
	}

	public List<RegisteredTool> tools() {
		return List.copyOf(tools.values());
	}

	public List<McpSchema.Tool> mcpTools() {
		return tools.values().stream().map(RegisteredTool::tool).toList();
	}

	public static ObjectMapper resultMapper() {
		return RESULT_MAPPER;
	}

	public CallToolResult call(String name, Map<String, Object> arguments) {
		long start = System.nanoTime();
		Map<String, Object> args = arguments == null ? Map.of() : arguments;
		Object clusterArg = args.get("cluster");
		String cluster = clusterArg == null ? null : String.valueOf(clusterArg);
		String outcome = "ok";
		String error = null;
		try {
			RegisteredTool tool = tools.get(name);
			if (tool == null) {
				throw new ToolException("Unknown or disabled tool: " + name);
			}
			checkCluster(tool, cluster);
			ToolCall call = new ToolCall(name, args, clients, Instant.now().plusMillis(KafkaClients.TIMEOUT_MS));
			Object result = execute(tool, call);
			return CallToolResult.builder().addTextContent(render(result)).isError(false).build();
		}
		catch (ToolException e) {
			outcome = "error";
			error = e.getMessage();
		}
		catch (RuntimeException e) {
			outcome = "error";
			error = KafkaErrors.explain(e);
			log.warn("Tool failed with an unexpected error: tool={}", name, e);
		}
		finally {
			long ms = (System.nanoTime() - start) / 1_000_000;
			log.atInfo()
				.addKeyValue("tool", name)
				.addKeyValue("cluster", cluster)
				.addKeyValue("durationMs", ms)
				.addKeyValue("outcome", outcome)
				.addKeyValue("error", error)
				.log("tool call");
		}
		return CallToolResult.builder().addTextContent(error).isError(true).build();
	}

	private void checkCluster(RegisteredTool tool, String cluster) {
		if (cluster == null || cluster.isBlank()) {
			if (tool.spec().clusterOptional()) {
				return;
			}
			throw new ToolException("Missing parameter: cluster. Valid values: " + tool.clusters());
		}
		if (tool.clusters().contains(cluster)) {
			return;
		}
		ClusterConfig c = config.cluster(cluster);
		if (c == null) {
			throw new ToolException("Unknown cluster: " + cluster + ". Valid values: " + tool.clusters());
		}
		String reason = ToolFilter.reasonUnavailable(config, tool.spec(), c);
		throw new ToolException(tool.spec().name() + " is not available on this cluster: "
				+ (reason == null ? "not allowed" : reason) + ". Valid values: " + tool.clusters());
	}

	private Object execute(RegisteredTool tool, ToolCall call) {
		Future<Object> future = executor.submit(() -> tool.spec().handler().handle(call));
		try {
			return future.get(KafkaClients.TIMEOUT_MS, TimeUnit.MILLISECONDS);
		}
		catch (TimeoutException e) {
			future.cancel(true);
			throw new ToolException("Tool did not finish within " + KafkaClients.TIMEOUT_MS / 1000
					+ " s (timeout). Try a narrower filter/limit.");
		}
		catch (InterruptedException e) {
			future.cancel(true);
			Thread.currentThread().interrupt();
			throw new ToolException("Tool was cancelled");
		}
		catch (ExecutionException e) {
			Throwable cause = KafkaErrors.unwrap(e.getCause());
			if (cause instanceof ToolException te) {
				throw te;
			}
			log.debug("Tool error: tool={}", tool.spec().name(), cause);
			throw new ToolException(KafkaErrors.explain(cause), cause);
		}
	}

	static String render(Object result) {
		if (result == null) {
			return "{}";
		}
		if (result instanceof CharSequence s) {
			return s.toString();
		}
		try {
			return RESULT_MAPPER.writeValueAsString(result);
		}
		catch (JsonProcessingException e) {
			throw new ToolException("Could not serialize the result to JSON: " + e.getOriginalMessage());
		}
	}

	@Override
	public void destroy() {
		executor.shutdownNow();
	}

}
