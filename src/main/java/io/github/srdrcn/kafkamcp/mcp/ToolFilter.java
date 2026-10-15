package io.github.srdrcn.kafkamcp.mcp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.ConfigException;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.modelcontextprotocol.spec.McpSchema;

/**
 * Decides which tools are listed, based on the config:
 * <ul>
 * <li>only the toolsets in the config's {@code toolsets} list,</li>
 * <li>write tools only on clusters with an effective {@code readOnly=false},</li>
 * <li>destructive tools additionally only while {@code disableDestructive=false},</li>
 * <li>connect/schemaregistry tools only on clusters that have the matching URL.</li>
 * </ul>
 * A tool with no eligible cluster left is not listed at all; for the others the {@code cluster} enum is
 * limited to the eligible clusters.
 */
public final class ToolFilter {

	private ToolFilter() {
	}

	public static List<RegisteredTool> select(KafkaMcpConfig config, Collection<? extends Toolset> toolsets) {
		Map<String, Toolset> byName = new LinkedHashMap<>();
		for (Toolset t : toolsets) {
			if (byName.put(t.name(), t) != null) {
				throw new IllegalStateException("Duplicate toolset name: " + t.name());
			}
		}
		List<String> unknown = config.getToolsets().stream().filter(n -> !byName.containsKey(n)).toList();
		if (!unknown.isEmpty()) {
			throw new ConfigException("Unknown toolset: " + unknown + " (valid: " + new TreeSet<>(byName.keySet()) + ")");
		}
		List<RegisteredTool> out = new ArrayList<>();
		Set<String> names = new LinkedHashSet<>();
		for (String toolsetName : config.getToolsets()) {
			for (ToolSpec spec : byName.get(toolsetName).tools()) {
				if (!names.add(spec.name())) {
					throw new IllegalStateException("Duplicate tool name: " + spec.name());
				}
				List<String> clusters = eligibleClusters(config, spec);
				if (clusters.isEmpty()) {
					continue;
				}
				out.add(new RegisteredTool(toolsetName, spec, clusters, toMcpTool(spec, clusters)));
			}
		}
		return out;
	}

	public static List<String> eligibleClusters(KafkaMcpConfig config, ToolSpec spec) {
		List<String> out = new ArrayList<>();
		for (ClusterConfig c : config.getClusters()) {
			if (reasonUnavailable(config, spec, c) == null) {
				out.add(c.getName());
			}
		}
		return out;
	}

	/** Why the tool is not available on this cluster; null if it is. */
	public static String reasonUnavailable(KafkaMcpConfig config, ToolSpec spec, ClusterConfig c) {
		if (!spec.requirement().supports(c)) {
			return switch (spec.requirement()) {
				case CONNECT -> "no connectUrl configured for cluster " + c.getName();
				case SCHEMA_REGISTRY -> "no schemaRegistryUrl configured for cluster " + c.getName();
				default -> "not supported";
			};
		}
		if (spec.kind().writes() && config.isReadOnly(c)) {
			return "cluster " + c.getName() + " is readOnly; write tools are disabled";
		}
		if (spec.kind() == ToolKind.DESTRUCTIVE && config.isDisableDestructive()) {
			return "disableDestructive is on; delete/reset tools are disabled";
		}
		return null;
	}

	static McpSchema.Tool toMcpTool(ToolSpec spec, List<String> clusters) {
		Map<String, Object> properties = new LinkedHashMap<>();
		Map<String, Object> cluster = new LinkedHashMap<>();
		cluster.put("type", "string");
		cluster.put("description", "Cluster name");
		cluster.put("enum", List.copyOf(clusters));
		properties.put("cluster", cluster);
		properties.putAll(spec.properties());
		List<String> required = new ArrayList<>();
		if (!spec.clusterOptional()) {
			required.add("cluster");
		}
		required.addAll(spec.required());
		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		schema.put("required", required);
		McpSchema.ToolAnnotations annotations = new McpSchema.ToolAnnotations(null, spec.kind() == ToolKind.READ,
				spec.kind() == ToolKind.DESTRUCTIVE, null, null, null);
		return McpSchema.Tool.builder(spec.name(), schema)
			.description(spec.description())
			.annotations(annotations)
			.build();
	}

}
