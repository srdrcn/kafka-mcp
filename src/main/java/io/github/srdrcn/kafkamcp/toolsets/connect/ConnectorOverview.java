package io.github.srdrcn.kafkamcp.toolsets.connect;

/**
 * {@code GET /connectors?expand=status&expand=info} içindeki tek connector: durum ve info (info ham config
 * içerir; dışarı verirken maskeleyin).
 */
public record ConnectorOverview(String name, ConnectorStatus status, ConnectorInfo info) {

	/** source / sink (durumdan, yoksa info'dan). */
	public String type() {
		if (status.type() != null) {
			return status.type();
		}
		return info == null ? null : info.type();
	}

	/** Tam connector.class (info yoksa null). */
	public String connectorClass() {
		return info == null ? null : info.connectorClass();
	}

}
