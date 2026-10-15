package io.github.srdrcn.kafkamcp.mcp;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;

/** Aracın hangi cluster'larda kullanılabileceği (cluster enum'u buna göre daraltılır). */
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
