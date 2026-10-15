package io.github.srdrcn.kafkamcp.mcp;

import java.util.List;

/** Filtre testleri için okuma/yazma/destructive/connect/SR araçları olan sahte toolset (Spring bean değil). */
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
		return List.of(ToolSpec.read("fake_list", "Okuma").listParams().handler(c -> "ok"),
				ToolSpec.write("fake_update", "Yazma").required("name", P.string("Ad")).handler(c -> "ok"),
				ToolSpec.destructive("fake_delete", "Silme").handler(c -> "ok"),
				ToolSpec.destructive("fake_offsets_reset", "Sıfırlama").handler(c -> "ok"),
				ToolSpec.read("fake_connect_list", "Connect okuma").connect().handler(c -> "ok"),
				ToolSpec.write("fake_connect_pause", "Connect yazma").connect().handler(c -> "ok"),
				ToolSpec.read("fake_subjects_list", "SR okuma").schemaRegistry().handler(c -> "ok"),
				ToolSpec.read("fake_slow", "Yavaş").handler(c -> {
					Thread.sleep(60_000);
					return "geç";
				}));
	}

}
