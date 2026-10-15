package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.mcp.Toolset;

/** consumers: consumer group listing, lag analysis and offset management tools. */
@Component
public class ConsumersToolset implements Toolset {

	private final GroupTools groupTools;

	private final LagTools lagTools;

	private final OffsetTools offsetTools;

	public ConsumersToolset(GroupTools groupTools, LagTools lagTools, OffsetTools offsetTools) {
		this.groupTools = groupTools;
		this.lagTools = lagTools;
		this.offsetTools = offsetTools;
	}

	@Override
	public String name() {
		return "consumers";
	}

	@Override
	public String description() {
		return "Consumer group, lag and offset tools";
	}

	@Override
	public List<ToolSpec> tools() {
		List<ToolSpec> tools = new ArrayList<>();
		tools.addAll(groupTools.tools());
		tools.addAll(lagTools.tools());
		tools.addAll(offsetTools.tools());
		return tools;
	}

}
