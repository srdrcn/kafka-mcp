package io.github.srdrcn.kafkamcp.mcp;

import java.util.List;

/** Fake toolset (not a Spring bean) with read/write/destructive/connect/SR tools for the filter tests. */
class FakeToolset implements Toolset {

	@Override
	public String name() {
		return "fake";
	}

	@Override
	public String description() {
		return "test";
	}

	@Override
	public List<ToolSpec> tools() {
		return List.of(ToolSpec.read("fake_list", "Read").listParams().handler(c -> "ok"),
				ToolSpec.write("fake_update", "Write").required("name", P.string("Name")).handler(c -> "ok"),
				ToolSpec.destructive("fake_delete", "Delete").handler(c -> "ok"),
				ToolSpec.destructive("fake_offsets_reset", "Reset").handler(c -> "ok"),
				ToolSpec.read("fake_connect_list", "Connect read").connect().handler(c -> "ok"),
				ToolSpec.write("fake_connect_pause", "Connect write").connect().handler(c -> "ok"),
				ToolSpec.read("fake_subjects_list", "SR read").schemaRegistry().handler(c -> "ok"),
				ToolSpec.read("fake_slow", "Slow").handler(c -> {
					Thread.sleep(60_000);
					return "late";
				}));
	}

}
