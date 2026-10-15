package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * {@code GET /connectors/{name}} (or {@code expand=info}) response: type, raw (unmasked!) config and task ids. Mask
 * the config with {@link ConnectSecrets#mask(Map)} before returning it.
 */
public record ConnectorInfo(String name, String type, Map<String, String> config, List<Integer> taskIds) {

	public static final String CONNECTOR_CLASS = "connector.class";

	public static ConnectorInfo from(JsonNode n) {
		List<Integer> ids = new ArrayList<>();
		for (JsonNode t : n.path("tasks")) {
			ids.add(t.path("task").asInt());
		}
		ids.sort(null);
		return new ConnectorInfo(ConnectJson.text(n, "name"), ConnectJson.text(n, "type"),
				ConnectJson.stringMap(n.get("config")), List.copyOf(ids));
	}

	public String connectorClass() {
		return config.get(CONNECTOR_CLASS);
	}

}
