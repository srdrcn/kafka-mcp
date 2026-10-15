package io.github.srdrcn.kafkamcp.toolsets.core;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.mcp.Toolset;

/** core: cluster, broker ve topic araçları (varsayılan açık). */
@Component
public class CoreToolset implements Toolset {

	private final ClusterTools clusterTools;

	public CoreToolset(ClusterTools clusterTools) {
		this.clusterTools = clusterTools;
	}

	@Override
	public String name() {
		return "core";
	}

	@Override
	public String description() {
		return "Cluster, broker ve topic araçları";
	}

	@Override
	public List<ToolSpec> tools() {
		List<ToolSpec> tools = new ArrayList<>();
		tools.addAll(clusterTools.tools());
		return tools;
	}

}
