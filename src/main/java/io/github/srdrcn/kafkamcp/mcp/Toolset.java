package io.github.srdrcn.kafkamcp.mcp;

import java.util.List;

/**
 * A toolset: a group of tools switched on and off by the config's {@code toolsets} list. Each toolset is a
 * Spring {@code @Component} in the {@code toolsets/<name>} package.
 */
public interface Toolset {

	/** Name used in the config (e.g. core, messages). */
	String name();

	/** Short description. */
	String description();

	/** All tools of the toolset (unfiltered). */
	List<ToolSpec> tools();

}
