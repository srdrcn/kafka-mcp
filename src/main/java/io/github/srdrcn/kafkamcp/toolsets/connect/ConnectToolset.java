package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.mcp.Toolset;

/** connect: Kafka Connect REST tools (listed only on clusters that have a connectUrl). */
@Component
public class ConnectToolset implements Toolset {

	private final ConnectClusterTools clusterTools;

	private final ConnectorReadTools readTools;

	private final ConnectorAnalysisTools analysisTools;

	private final ConnectorWriteTools writeTools;

	public ConnectToolset(ConnectClusterTools clusterTools, ConnectorReadTools readTools,
			ConnectorAnalysisTools analysisTools, ConnectorWriteTools writeTools) {
		this.clusterTools = clusterTools;
		this.readTools = readTools;
		this.analysisTools = analysisTools;
		this.writeTools = writeTools;
	}

	@Override
	public String name() {
		return "connect";
	}

	@Override
	public String description() {
		return "Kafka Connect connector, task and plugin tools";
	}

	@Override
	public List<ToolSpec> tools() {
		List<ToolSpec> tools = new ArrayList<>();
		tools.addAll(clusterTools.tools());
		tools.addAll(readTools.tools());
		tools.addAll(analysisTools.tools());
		tools.addAll(writeTools.tools());
		return tools;
	}

}
