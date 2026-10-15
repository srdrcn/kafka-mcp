package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.mcp.Toolset;

/** diagnose: tek çağrıda sağlık teşhisi araçları (topic, consumer group, connector, cluster, erişim, bağlantı). */
@Component
public class DiagnoseToolset implements Toolset {

	private final DiagnoseTopicTools topicTools;

	private final DiagnoseGroupTools groupTools;

	private final DiagnoseConnectorTools connectorTools;

	private final DiagnoseClusterTools clusterTools;

	private final DiagnoseAccessTools accessTools;

	private final DiagnoseConnectivityTools connectivityTools;

	public DiagnoseToolset(DiagnoseTopicTools topicTools, DiagnoseGroupTools groupTools,
			DiagnoseConnectorTools connectorTools, DiagnoseClusterTools clusterTools, DiagnoseAccessTools accessTools,
			DiagnoseConnectivityTools connectivityTools) {
		this.topicTools = topicTools;
		this.groupTools = groupTools;
		this.connectorTools = connectorTools;
		this.clusterTools = clusterTools;
		this.accessTools = accessTools;
		this.connectivityTools = connectivityTools;
	}

	@Override
	public String name() {
		return "diagnose";
	}

	@Override
	public String description() {
		return "Teşhis araçları: topic, consumer group, connector, cluster, erişim ve bağlantı sağlık özetleri";
	}

	@Override
	public List<ToolSpec> tools() {
		List<ToolSpec> tools = new ArrayList<>();
		tools.addAll(topicTools.tools());
		tools.addAll(groupTools.tools());
		tools.addAll(connectorTools.tools());
		tools.addAll(clusterTools.tools());
		tools.addAll(accessTools.tools());
		tools.addAll(connectivityTools.tools());
		return tools;
	}

}
