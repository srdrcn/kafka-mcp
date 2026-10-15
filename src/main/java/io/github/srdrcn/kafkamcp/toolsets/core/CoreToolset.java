package io.github.srdrcn.kafkamcp.toolsets.core;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.mcp.Toolset;

/** core: cluster, broker and topic tools (enabled by default). */
@Component
public class CoreToolset implements Toolset {

	private final ClusterTools clusterTools;

	private final BrokerTools brokerTools;

	private final BrokerWriteTools brokerWriteTools;

	private final ReassignmentTools reassignmentTools;

	private final TopicTools topicTools;

	private final TopicDataTools topicDataTools;

	private final TopicClientTools topicClientTools;

	private final TopicWriteTools topicWriteTools;

	public CoreToolset(ClusterTools clusterTools, BrokerTools brokerTools, BrokerWriteTools brokerWriteTools,
			ReassignmentTools reassignmentTools, TopicTools topicTools, TopicDataTools topicDataTools,
			TopicClientTools topicClientTools, TopicWriteTools topicWriteTools) {
		this.clusterTools = clusterTools;
		this.brokerTools = brokerTools;
		this.brokerWriteTools = brokerWriteTools;
		this.reassignmentTools = reassignmentTools;
		this.topicTools = topicTools;
		this.topicDataTools = topicDataTools;
		this.topicClientTools = topicClientTools;
		this.topicWriteTools = topicWriteTools;
	}

	@Override
	public String name() {
		return "core";
	}

	@Override
	public String description() {
		return "Cluster, broker and topic tools";
	}

	@Override
	public List<ToolSpec> tools() {
		List<ToolSpec> tools = new ArrayList<>();
		tools.addAll(clusterTools.tools());
		tools.addAll(brokerTools.tools());
		tools.addAll(brokerWriteTools.tools());
		tools.addAll(reassignmentTools.tools());
		tools.addAll(topicTools.tools());
		tools.addAll(topicDataTools.tools());
		tools.addAll(topicClientTools.tools());
		tools.addAll(topicWriteTools.tools());
		return tools;
	}

}
