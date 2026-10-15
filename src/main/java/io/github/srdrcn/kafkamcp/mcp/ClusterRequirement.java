package io.github.srdrcn.kafkamcp.mcp;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;

/** Which clusters a tool can be used on (the cluster enum is narrowed accordingly). */
public enum ClusterRequirement {

	ANY, CONNECT, SCHEMA_REGISTRY;

	public boolean supports(ClusterConfig c) {
		return switch (this) {
			case ANY -> true;
			case CONNECT -> c.hasConnect();
			case SCHEMA_REGISTRY -> c.hasSchemaRegistry();
		};
	}

}
