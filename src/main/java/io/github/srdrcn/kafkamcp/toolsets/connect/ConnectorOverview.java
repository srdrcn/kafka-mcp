package io.github.srdrcn.kafkamcp.toolsets.connect;

/**
 * A single connector in {@code GET /connectors?expand=status&expand=info}: status and info (info holds the raw
 * config; mask it before returning it).
 */
public record ConnectorOverview(String name, ConnectorStatus status, ConnectorInfo info) {

	/** source / sink (from the status, otherwise from the info). */
	public String type() {
		if (status.type() != null) {
			return status.type();
		}
		return info == null ? null : info.type();
	}

	/** Full connector.class (null without info). */
	public String connectorClass() {
		return info == null ? null : info.connectorClass();
	}

}
