package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectApi;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectJson;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorOverview;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorStatus;

/**
 * Bir topic'e referans veren Connect connector'ları: config'te adında "topic" geçen anahtarlar (liste, *regex,
 * *pattern, *prefix) ve connector'ın aktif topic'leri ({@code GET /connectors/{name}/topics}, sınırlı paralel).
 */
public final class ConnectorRefs {

	/** Aktif topic sorgusu yapılan en fazla connector (fazlası unchecked sayılır). */
	public static final int MAX_ACTIVE_CHECKS = 100;

	private static final int PARALLELISM = 8;

	private ConnectorRefs() {
	}

	/** Eşleşen connector. */
	public record Ref(String name, String type, String connectorClass, ConnectorStatus status, List<String> matchedBy) {

		public Map<String, Object> toMap() {
			return Results.map("name", name, "type", type, "class", ConnectJson.shortClass(connectorClass), "state",
					status.state(), "tasks", status.tasks().size(), "failedTasks",
					status.failedTaskIds().isEmpty() ? null : status.failedTaskIds().size(), "matchedBy", matchedBy);
		}

	}

	/** Tarama sonucu: taranan connector sayısı, eşleşenler, aktif topic'i kontrol edilemeyenler. */
	public record Result(int scanned, List<Ref> refs, int activeUnchecked) {
	}

	/** Topic'e referans veren connector'lar; aktif topic kontrolü en fazla maxMillis sürer. */
	public static Result find(ConnectApi api, ToolCall call, String topic, long maxMillis) {
		String cluster = call.cluster();
		List<ConnectorOverview> all = api.overview(cluster);
		Map<String, List<String>> matched = new LinkedHashMap<>();
		Map<String, ConnectorOverview> byName = new LinkedHashMap<>();
		for (ConnectorOverview c : all) {
			byName.put(c.name(), c);
			matched.put(c.name(), new ArrayList<>(c.info() == null ? List.of() : configMatches(c.info().config(), topic)));
		}
		List<String> toCheck = all.stream().map(ConnectorOverview::name).limit(MAX_ACTIVE_CHECKS).toList();
		int unchecked = all.size() - toCheck.size();
		long end = System.currentTimeMillis() + Math.max(0, Math.min(maxMillis, call.waitMillis() - 500));
		ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
		try {
			Semaphore permits = new Semaphore(PARALLELISM);
			Map<String, Future<JsonNode>> futures = new LinkedHashMap<>();
			for (String name : toCheck) {
				futures.put(name, pool.submit(() -> {
					permits.acquire();
					try {
						return api.client().get(cluster, ConnectApi.path(name) + "/topics");
					}
					finally {
						permits.release();
					}
				}));
			}
			for (Map.Entry<String, Future<JsonNode>> f : futures.entrySet()) {
				try {
					JsonNode r = f.getValue().get(Math.max(1, end - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
					JsonNode node = r.has(f.getKey()) ? r.get(f.getKey()) : null;
					if (node != null) {
						for (JsonNode t : node.path("topics")) {
							if (topic.equals(t.asText())) {
								matched.get(f.getKey()).add(0, "activeTopics");
							}
						}
					}
				}
				catch (TimeoutException e) {
					f.getValue().cancel(true);
					unchecked++;
				}
				catch (ExecutionException e) {
					unchecked++;
				}
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Araç iptal edildi");
		}
		finally {
			pool.shutdownNow();
		}
		List<Ref> refs = new ArrayList<>();
		matched.forEach((name, by) -> {
			if (!by.isEmpty()) {
				ConnectorOverview c = byName.get(name);
				refs.add(new Ref(name, c.type(), c.connectorClass(), c.status(), by));
			}
		});
		return new Result(all.size(), refs, unchecked);
	}

	/**
	 * Config'te topic'e referans veren anahtarlar: adında "topic" geçen ve değeri (virgülle ayrılmış) topic olan,
	 * *regex/*pattern ile eşleşen ya da *prefix ile başlayan.
	 */
	public static List<String> configMatches(Map<String, String> config, String topic) {
		List<String> out = new ArrayList<>();
		if (config == null) {
			return out;
		}
		config.forEach((k, v) -> {
			String key = k.toLowerCase();
			String value = v == null ? "" : v.trim();
			if (!key.contains("topic") || value.isEmpty()) {
				return;
			}
			boolean match = false;
			if (key.endsWith("regex") || key.endsWith("pattern")) {
				try {
					match = Pattern.compile(value).matcher(topic).matches();
				}
				catch (PatternSyntaxException ignored) {
					// geçersiz regex: eşleşme yok
				}
			}
			else if (key.endsWith("prefix")) {
				match = topic.startsWith(value);
			}
			else {
				for (String part : value.split(",")) {
					if (part.trim().equals(topic)) {
						match = true;
						break;
					}
				}
			}
			if (match) {
				out.add("config:" + k);
			}
		});
		return out;
	}

}
