package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Connect cluster'ı ve plugin araçları: connect_clusters_list, connect_plugins_list, connect_plugins_validate. */
@Component
public class ConnectClusterTools {

	/** Doğrulamada name verilmemişse kullanılan yer tutucu (Connect name'i zorunlu tutar). */
	static final String VALIDATE_NAME = "kafka-mcp-validate";

	private final ConnectApi api;

	public ConnectClusterTools(ConnectApi api) {
		this.api = api;
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("connect_clusters_list", "Connect URL'si olan cluster'larda Connect erişimi, sürümü ve connector sayısı")
			.connect()
			.clusterOptional()
			.handler(this::clustersList),
				ToolSpec.read("connect_plugins_list", "Yüklü Connect plugin'leri (sınıf, tip, sürüm)")
					.connect()
					.listParams()
					.param("connectorsOnly",
							P.bool("Sadece connector plugin'leri (varsayılan true; false: converter/transform vb. dahil)"))
					.handler(this::pluginsList),
				ToolSpec
					.read("connect_plugins_validate",
							"Connector config'ini plugin'e göre doğrular; sadece hatalı alanları döner")
					.connect()
					.required("plugin", P.string("Connector sınıfı (tam ya da kısa ad, ör. FileStreamSinkConnector)"))
					.required("config", P.stringMap("Doğrulanacak connector config'i (anahtar → string değer)"))
					.handler(this::pluginsValidate));
	}

	Object clustersList(ToolCall call) {
		String only = call.clusterOrNull();
		List<ClusterConfig> selected = call.clients()
			.config()
			.getClusters()
			.stream()
			.filter(ClusterConfig::hasConnect)
			.filter(c -> only == null || c.getName().equals(only))
			.toList();
		List<Map<String, Object>> rows = new ArrayList<>();
		ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
		try {
			List<Future<Map<String, Object>>> probes = new ArrayList<>();
			for (ClusterConfig c : selected) {
				probes.add(pool.submit(() -> probe(c.getName())));
			}
			for (int i = 0; i < selected.size(); i++) {
				ClusterConfig c = selected.get(i);
				Map<String, Object> row = Results.map("name", c.getName(), "connectUrl", safeUrl(c.getConnectUrl()));
				Future<Map<String, Object>> f = probes.get(i);
				try {
					row.putAll(f.get(call.waitMillis(), TimeUnit.MILLISECONDS));
				}
				catch (TimeoutException e) {
					f.cancel(true);
					row.put("reachable", false);
					row.put("error", "Connect yanıtı zaman sınırında gelmedi");
				}
				catch (java.util.concurrent.ExecutionException e) {
					row.put("reachable", false);
					row.put("error", KafkaErrors.explain(e.getCause()));
				}
				rows.add(row);
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Araç iptal edildi");
		}
		finally {
			pool.shutdownNow();
		}
		return Results.map("total", rows.size(), "clusters", rows);
	}

	private Map<String, Object> probe(String cluster) {
		try {
			ConnectClient client = api.client();
			JsonNode root = client.get(cluster, "/");
			JsonNode names = client.get(cluster, "/connectors");
			return Results.map("reachable", true, "version", ConnectJson.text(root, "version"), "commit",
					ConnectJson.text(root, "commit"), "kafkaClusterId", ConnectJson.text(root, "kafka_cluster_id"),
					"connectorCount", names.isArray() ? names.size() : null);
		}
		catch (RuntimeException e) {
			return Results.map("reachable", false, "error", KafkaErrors.explain(e));
		}
	}

	/** URL'deki kullanıcı bilgisini (user:pass@) gizler. */
	static String safeUrl(String url) {
		return url == null ? null : url.replaceAll("//[^/@]*@", "//***@");
	}

	Object pluginsList(ToolCall call) {
		boolean connectorsOnly = call.bool("connectorsOnly", true);
		JsonNode r = api.client()
			.get(call.cluster(), connectorsOnly ? "/connector-plugins" : "/connector-plugins?connectorsOnly=false");
		List<Map<String, Object>> rows = new ArrayList<>();
		for (JsonNode n : r) {
			String cls = ConnectJson.text(n, "class");
			if (call.matches(cls)) {
				rows.add(Results.map("class", cls, "type", ConnectJson.text(n, "type"), "version",
						ConnectJson.text(n, "version")));
			}
		}
		return Results.page("plugins", rows, call.limit());
	}

	Object pluginsValidate(ToolCall call) {
		String cluster = call.cluster();
		List<String> plugins = api.connectorPlugins(cluster);
		String plugin = ConnectApi.resolvePlugin(plugins, call.str("plugin"));
		Map<String, String> config = new LinkedHashMap<>(call.stringMap("config"));
		List<String> added = new ArrayList<>();
		String given = config.get(ConnectorInfo.CONNECTOR_CLASS);
		if (given == null || given.isBlank()) {
			added.add(ConnectorInfo.CONNECTOR_CLASS);
		}
		else if (!ConnectApi.resolvePlugin(plugins, given).equals(plugin)) {
			throw new ToolException("config'teki connector.class (" + given + ") plugin parametresiyle (" + plugin
					+ ") uyuşmuyor");
		}
		config.put(ConnectorInfo.CONNECTOR_CLASS, plugin);
		if (config.get("name") == null || config.get("name").isBlank()) {
			config.put("name", VALIDATE_NAME);
			added.add("name");
		}
		JsonNode r = api.client()
			.put(cluster, "/connector-plugins/" + ConnectClient.enc(plugin) + "/config/validate", config);
		List<Map<String, Object>> errors = new ArrayList<>();
		for (JsonNode c : r.path("configs")) {
			JsonNode v = c.path("value");
			JsonNode errs = v.path("errors");
			if (!errs.isArray() || errs.isEmpty()) {
				continue;
			}
			String key = ConnectJson.text(v, "name");
			List<String> messages = new ArrayList<>();
			errs.forEach(e -> messages.add(e.asText()));
			// parse hatalarında Connect value'yu null döner; gönderilen değeri göster
			String value = ConnectJson.text(v, "value");
			errors.add(Results.map("name", key, "value",
					ConnectSecrets.mask(key, value != null ? value : config.get(key)), "errors", messages));
		}
		return Results.map("plugin", plugin, "valid", errors.isEmpty() && r.path("error_count").asInt() == 0,
				"errorCount", r.path("error_count").asInt(), "addedKeys", added.isEmpty() ? null : added, "errors",
				errors);
	}

}
