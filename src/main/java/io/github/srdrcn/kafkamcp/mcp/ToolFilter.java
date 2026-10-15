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
 * Config'e göre hangi araçların listeleneceğini belirler:
 * <ul>
 * <li>sadece config'teki {@code toolsets} listesindeki toolset'ler,</li>
 * <li>yazma araçları sadece efektif {@code readOnly=false} cluster'larda,</li>
 * <li>destructive araçlar ayrıca {@code disableDestructive=false} iken,</li>
 * <li>connect/schemaregistry araçları sadece ilgili URL'si olan cluster'larda.</li>
 * </ul>
 * Uygun cluster'ı kalmayan araç hiç listelenmez; kalanların {@code cluster} enum'u uygun cluster'larla
 * sınırlanır.
 */
public final class ToolFilter {

	private ToolFilter() {
	}

	public static List<RegisteredTool> select(KafkaMcpConfig config, Collection<? extends Toolset> toolsets) {
		Map<String, Toolset> byName = new LinkedHashMap<>();
		for (Toolset t : toolsets) {
			if (byName.put(t.name(), t) != null) {
				throw new IllegalStateException("Toolset adı tekrar ediyor: " + t.name());
			}
		}
		List<String> unknown = config.getToolsets().stream().filter(n -> !byName.containsKey(n)).toList();
		if (!unknown.isEmpty()) {
			throw new ConfigException("Bilinmeyen toolset: " + unknown + " (geçerli: " + new TreeSet<>(byName.keySet()) + ")");
		}
		List<RegisteredTool> out = new ArrayList<>();
		Set<String> names = new LinkedHashSet<>();
		for (String toolsetName : config.getToolsets()) {
			for (ToolSpec spec : byName.get(toolsetName).tools()) {
				if (!names.add(spec.name())) {
					throw new IllegalStateException("Araç adı tekrar ediyor: " + spec.name());
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

	/** Araç bu cluster'da neden kullanılamaz; kullanılabiliyorsa null. */
	public static String reasonUnavailable(KafkaMcpConfig config, ToolSpec spec, ClusterConfig c) {
		if (!spec.requirement().supports(c)) {
			return switch (spec.requirement()) {
				case CONNECT -> "cluster " + c.getName() + " için connectUrl tanımlı değil";
				case SCHEMA_REGISTRY -> "cluster " + c.getName() + " için schemaRegistryUrl tanımlı değil";
				default -> "desteklenmiyor";
			};
		}
		if (spec.kind().writes() && config.isReadOnly(c)) {
			return "cluster " + c.getName() + " readOnly; yazma araçları kapalı";
		}
		if (spec.kind() == ToolKind.DESTRUCTIVE && config.isDisableDestructive()) {
			return "disableDestructive açık; silme/sıfırlama araçları kapalı";
		}
		return null;
	}

	static McpSchema.Tool toMcpTool(ToolSpec spec, List<String> clusters) {
		Map<String, Object> properties = new LinkedHashMap<>();
		Map<String, Object> cluster = new LinkedHashMap<>();
		cluster.put("type", "string");
		cluster.put("description", "Cluster adı");
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
