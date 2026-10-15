package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.mcp.Toolset;

/** schemaregistry: subject, schema, diff and compatibility tools (only on clusters with a schemaRegistryUrl). */
@Component
public class SchemaRegistryToolset implements Toolset {

	private final SubjectTools subjectTools;

	private final SchemaTools schemaTools;

	private final CompatibilityTools compatibilityTools;

	public SchemaRegistryToolset(SubjectTools subjectTools, SchemaTools schemaTools,
			CompatibilityTools compatibilityTools) {
		this.subjectTools = subjectTools;
		this.schemaTools = schemaTools;
		this.compatibilityTools = compatibilityTools;
	}

	@Override
	public String name() {
		return "schemaregistry";
	}

	@Override
	public String description() {
		return "Schema Registry subject, schema, diff and compatibility tools";
	}

	@Override
	public List<ToolSpec> tools() {
		List<ToolSpec> tools = new ArrayList<>();
		tools.addAll(subjectTools.tools());
		tools.addAll(schemaTools.tools());
		tools.addAll(compatibilityTools.tools());
		return tools;
	}

}
