package io.github.srdrcn.kafkamcp.mcp;

import java.util.List;

import io.modelcontextprotocol.spec.McpSchema;

/** A tool that passed the filters and gets listed: definition + clusters it can be used on + MCP schema. */
public record RegisteredTool(String toolset, ToolSpec spec, List<String> clusters, McpSchema.Tool tool) {
}
