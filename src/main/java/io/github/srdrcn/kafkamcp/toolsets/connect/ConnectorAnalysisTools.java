package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Connector analiz araçları: connectors_task_distribution, connectors_config_diff. */
@Component
public class ConnectorAnalysisTools {

	/** Worker başına örnek olarak listelenen RUNNING olmayan görev sayısı üst sınırı. */
	static final int NOT_RUNNING_SAMPLE = 20;

	private final ConnectApi api;

	public ConnectorAnalysisTools(ConnectApi api) {
		this.api = api;
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("connectors_task_distribution",
					"Worker başına connector/görev dağılımı, dengesizlik ve RUNNING olmayan görevler")
			.connect()
			.handler(this::taskDistribution),
				ToolSpec
					.read("connectors_config_diff",
							"Connector config'ini verilen config'le ya da başka bir connector'la karşılaştırır")
					.connect()
					.required("name", ConnectorReadTools.nameParam())
					.param("config", P.stringMap("Karşılaştırılacak hedef config (otherName ile birlikte verilmez)"))
					.param("otherName", P.string("Karşılaştırılacak diğer connector adı (config yerine)"))
					.handler(this::configDiff));
	}

	private static final class Worker {

		int connectors;

		int connectorsNotRunning;

		int tasks;

		int tasksNotRunning;

		final List<String> notRunningSample = new ArrayList<>();

	}

	Object taskDistribution(ToolCall call) {
		List<ConnectorOverview> all = api.overview(call.cluster());
		Map<String, Worker> workers = new TreeMap<>();
		int taskCount = 0;
		int unassignedConnectors = 0;
		int unassignedTasks = 0;
		for (ConnectorOverview c : all) {
			ConnectorStatus s = c.status();
			if (s.workerId() == null) {
				unassignedConnectors++;
			}
			else {
				Worker w = workers.computeIfAbsent(s.workerId(), k -> new Worker());
				w.connectors++;
				if (!ConnectorStatus.RUNNING.equals(s.state())) {
					w.connectorsNotRunning++;
				}
			}
			for (ConnectorStatus.Task t : s.tasks()) {
				taskCount++;
				if (t.workerId() == null) {
					unassignedTasks++;
					continue;
				}
				Worker w = workers.computeIfAbsent(t.workerId(), k -> new Worker());
				w.tasks++;
				if (!ConnectorStatus.RUNNING.equals(t.state())) {
					w.tasksNotRunning++;
					if (w.notRunningSample.size() < NOT_RUNNING_SAMPLE) {
						w.notRunningSample.add(c.name() + "/" + t.id() + " (" + t.state() + ")");
					}
				}
			}
		}
		List<Map<String, Object>> rows = new ArrayList<>();
		int min = Integer.MAX_VALUE;
		int max = 0;
		for (Map.Entry<String, Worker> e : workers.entrySet()) {
			Worker w = e.getValue();
			min = Math.min(min, w.tasks);
			max = Math.max(max, w.tasks);
			rows.add(Results.map("worker", e.getKey(), "connectors", w.connectors, "connectorsNotRunning",
					w.connectorsNotRunning, "tasks", w.tasks, "tasksNotRunning", w.tasksNotRunning, "notRunningTasks",
					w.notRunningSample.isEmpty() ? null : w.notRunningSample, "notRunningTasksOmitted",
					w.tasksNotRunning > w.notRunningSample.size() ? w.tasksNotRunning - w.notRunningSample.size()
							: null));
		}
		return Results.map("connectors", all.size(), "tasks", taskCount, "workerCount", workers.size(),
				"taskImbalance", workers.isEmpty() ? 0 : max - min, "maxTasksPerWorker", workers.isEmpty() ? 0 : max,
				"minTasksPerWorker", workers.isEmpty() ? 0 : min, "unassignedConnectors",
				unassignedConnectors == 0 ? null : unassignedConnectors, "unassignedTasks",
				unassignedTasks == 0 ? null : unassignedTasks, "workers", rows, "note",
				"Sadece connector/görev çalıştıran worker'lar görünür (boştaki worker'lar REST'te listelenmez)");
	}

	Object configDiff(ToolCall call) {
		String cluster = call.cluster();
		String name = call.str("name");
		boolean hasConfig = call.args().get("config") != null;
		String other = call.str("otherName", null);
		if (hasConfig == (other != null)) {
			throw new ToolException("config ya da otherName parametrelerinden tam olarak biri verilmeli");
		}
		Map<String, String> current = api.config(cluster, name);
		Map<String, String> target = other != null ? api.config(cluster, other) : call.stringMap("config");
		Map<String, Object> out = Results.map("name", name, "target", other != null ? "connector:" + other : "config");
		out.putAll(ConfigDiff.diff(current, target, Set.of("name")));
		return out;
	}

}
