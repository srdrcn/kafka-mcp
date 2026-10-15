package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientResponseException;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Parsed Connect REST calls on top of {@link ConnectClient}: all connectors in a single call (expand=status+info),
 * the status/info/config of a single connector, plugin resolution and a bounded wait for state transitions.
 */
@Component
public class ConnectApi {

	/** Wait between status polls. */
	static final long POLL_MS = 300;

	/** Margin left before the tool deadline when polling stops (for the last REST call and the response). */
	static final long POLL_RESERVE_MS = 3_000;

	private final ConnectClient client;

	public ConnectApi(ConnectClient client) {
		this.client = client;
	}

	public ConnectClient client() {
		return client;
	}

	/** {@code /connectors/{encoded name}} */
	public static String path(String connector) {
		return "/connectors/" + ConnectClient.enc(connector);
	}

	/** All connectors sorted by name, in a single REST call (expand=status&expand=info). */
	public List<ConnectorOverview> overview(String cluster) {
		JsonNode r = client.get(cluster, "/connectors?expand=status&expand=info");
		if (!r.isObject()) {
			throw new ToolException("Connect REST does not support expand=status/info (Connect 2.3+ required)");
		}
		List<ConnectorOverview> out = new ArrayList<>();
		r.properties().forEach(e -> {
			JsonNode s = e.getValue().get("status");
			JsonNode i = e.getValue().get("info");
			ConnectorStatus status = s == null || s.isNull() ? new ConnectorStatus(e.getKey(), null, null, null, null,
					List.of()) : ConnectorStatus.from(s);
			out.add(new ConnectorOverview(e.getKey(), status, i == null || i.isNull() ? null : ConnectorInfo.from(i)));
		});
		out.sort(Comparator.comparing(ConnectorOverview::name));
		return out;
	}

	public ConnectorStatus status(String cluster, String connector) {
		return ConnectorStatus.from(client.get(cluster, path(connector) + "/status"));
	}

	/**
	 * {@link #status}; null if Connect returns an error response (e.g. the connector does not exist yet: 404).
	 * Connection errors are thrown.
	 */
	public ConnectorStatus statusOrNull(String cluster, String connector) {
		try {
			return status(cluster, connector);
		}
		catch (ToolException e) {
			if (e.getCause() instanceof RestClientResponseException) {
				return null;
			}
			throw e;
		}
	}

	public ConnectorInfo info(String cluster, String connector) {
		return ConnectorInfo.from(client.get(cluster, path(connector)));
	}

	/** Raw (unmasked) config of the connector. */
	public Map<String, String> config(String cluster, String connector) {
		return ConnectJson.stringMap(client.get(cluster, path(connector) + "/config"));
	}

	/** Task id → raw task config ({@code GET /connectors/{name}/tasks}). */
	public Map<Integer, Map<String, String>> taskConfigs(String cluster, String connector) {
		Map<Integer, Map<String, String>> out = new LinkedHashMap<>();
		for (JsonNode t : client.get(cluster, path(connector) + "/tasks")) {
			out.put(t.path("id").path("task").asInt(), ConnectJson.stringMap(t.get("config")));
		}
		return out;
	}

	/** Fully qualified class names of the installed connector plugins ({@code GET /connector-plugins}). */
	public List<String> connectorPlugins(String cluster) {
		List<String> classes = new ArrayList<>();
		for (JsonNode n : client.get(cluster, "/connector-plugins")) {
			String cls = ConnectJson.text(n, "class");
			if (cls != null) {
				classes.add(cls);
			}
		}
		return classes;
	}

	/**
	 * Resolves a plugin name (full class, short name or without the "Connector" suffix; case-insensitive) to one of
	 * the given plugin classes; {@link ToolException} if not found.
	 */
	public static String resolvePlugin(List<String> pluginClasses, String plugin) {
		String p = plugin.trim().toLowerCase(Locale.ROOT);
		for (String cls : pluginClasses) {
			String full = cls.toLowerCase(Locale.ROOT);
			String simple = ConnectJson.shortClass(cls).toLowerCase(Locale.ROOT);
			if (p.equals(full) || p.equals(simple)
					|| simple.endsWith("connector") && p.equals(simple.substring(0, simple.length() - 9))) {
				return cls;
			}
		}
		throw new ToolException("Connector plugin not found: " + plugin + " (installed: "
				+ pluginClasses.stream().map(ConnectJson::shortClass).toList() + ")");
	}

	/**
	 * Result of status polling: last status, whether the target was reached, last error (if the status could not
	 * be read).
	 */
	public record Awaited(ConnectorStatus status, boolean settled, String error) {
	}

	/**
	 * Polls the connector status until {@code until} holds, for at most {@code maxMillis} (and until
	 * {@value #POLL_RESERVE_MS} ms remain before the tool deadline). Keeps trying if the status cannot be read
	 * (e.g. still 404).
	 */
	public Awaited await(ToolCall call, String connector, long initialDelayMillis, long maxMillis,
			Predicate<ConnectorStatus> until) {
		long end = System.nanoTime() + maxMillis * 1_000_000L;
		if (initialDelayMillis > 0 && !call.nearDeadline(POLL_RESERVE_MS + initialDelayMillis)) {
			pause(initialDelayMillis);
		}
		ConnectorStatus last = null;
		String error = null;
		while (true) {
			try {
				last = status(call.cluster(), connector);
				error = null;
				if (until.test(last)) {
					return new Awaited(last, true, null);
				}
			}
			catch (ToolException e) {
				error = e.getMessage();
			}
			if (System.nanoTime() >= end || call.nearDeadline(POLL_RESERVE_MS + POLL_MS)) {
				return new Awaited(last, false, error);
			}
			pause(POLL_MS);
		}
	}

	private static void pause(long millis) {
		try {
			Thread.sleep(millis);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Tool was cancelled");
		}
	}

}
