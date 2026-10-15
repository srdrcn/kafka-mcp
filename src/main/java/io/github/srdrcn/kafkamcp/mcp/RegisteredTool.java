package io.github.srdrcn.kafkamcp.mcp;

import java.util.List;

import io.modelcontextprotocol.spec.McpSchema;

/** Filtrelerden geçmiş, listelenecek araç: tanım + kullanılabildiği cluster'lar + MCP şeması. */
public record RegisteredTool(String toolset, ToolSpec spec, List<String> clusters, McpSchema.Tool tool) {
}
