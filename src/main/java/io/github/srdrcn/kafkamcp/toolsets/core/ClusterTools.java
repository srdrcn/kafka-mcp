package io.github.srdrcn.kafkamcp.toolsets.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

@Component
public class ClusterTools {

	private final KafkaClients clients;

	public ClusterTools(KafkaClients clients) {
		this.clients = clients;
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("clusters_list", "Config'teki cluster'ları ve erişilebilirliklerini listeler")
			.clusterOptional()
			.listParams()
			.param("check", P.bool("Bağlantıyı da test et (varsayılan true)"))
			.handler(this::clustersList));
	}

	Object clustersList(ToolCall call) throws Exception {
		KafkaMcpConfig config = clients.config();
		String only = call.clusterOrNull();
		List<ClusterConfig> selected = config.getClusters()
			.stream()
			.filter(c -> only == null || c.getName().equals(only))
			.filter(c -> call.matches(c.getName()))
			.limit(call.limit())
			.toList();
		boolean check = call.bool("check", true);
		List<Map<String, Object>> rows = new ArrayList<>();
		try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
			List<Future<Map<String, Object>>> checks = new ArrayList<>();
			for (ClusterConfig c : selected) {
				checks.add(check ? pool.submit(() -> probe(c, call)) : null);
			}
			for (int i = 0; i < selected.size(); i++) {
				ClusterConfig c = selected.get(i);
				Map<String, Object> row = Results.map("name", c.getName(), "bootstrapServers", c.getBootstrapServers(),
						"readOnly", config.isReadOnly(c), "connect", c.hasConnect(), "schemaRegistry",
						c.hasSchemaRegistry());
				Future<Map<String, Object>> f = checks.get(i);
				if (f != null) {
					try {
						row.putAll(f.get(Math.max(1, call.waitMillis()), TimeUnit.MILLISECONDS));
					}
					catch (java.util.concurrent.TimeoutException e) {
						f.cancel(true);
						row.put("status", "timeout");
						row.put("error", "Bağlantı testi zaman sınırında bitmedi");
					}
				}
				rows.add(row);
			}
			pool.shutdownNow();
		}
		return Results.page("clusters", rows, call.limit());
	}

	private Map<String, Object> probe(ClusterConfig c, ToolCall call) {
		try {
			DescribeClusterResult r = clients.admin(c.getName())
				.describeCluster(new DescribeClusterOptions().timeoutMs((int) Math.min(10_000, call.waitMillis())));
			String id = call.await(r.clusterId());
			Node controller = call.await(r.controller());
			int brokers = call.await(r.nodes()).size();
			return Results.map("status", "ok", "clusterId", id, "controllerId",
					controller == null || controller.isEmpty() ? null : controller.id(), "brokerCount", brokers);
		}
		catch (Exception e) {
			return Results.map("status", "error", "error", KafkaErrors.explain(e));
		}
	}

}
