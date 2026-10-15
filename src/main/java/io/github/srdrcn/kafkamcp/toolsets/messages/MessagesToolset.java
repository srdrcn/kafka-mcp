package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.mcp.Toolset;

/** messages: reading messages without a consumer group, offset calculations and format decoding (all read-only). */
@Component
public class MessagesToolset implements Toolset {

	private final ConsumeTools consumeTools;

	private final OffsetTools offsetTools;

	private final SchemaTools schemaTools;

	public MessagesToolset(ConsumeTools consumeTools, OffsetTools offsetTools, SchemaTools schemaTools) {
		this.consumeTools = consumeTools;
		this.offsetTools = offsetTools;
		this.schemaTools = schemaTools;
	}

	@Override
	public String name() {
		return "messages";
	}

	@Override
	public String description() {
		return "Message reading (no consumer group), offset/time calculations and format decoding";
	}

	@Override
	public List<ToolSpec> tools() {
		List<ToolSpec> tools = new ArrayList<>();
		tools.addAll(consumeTools.tools());
		tools.addAll(offsetTools.tools());
		tools.addAll(schemaTools.tools());
		return tools;
	}

}
