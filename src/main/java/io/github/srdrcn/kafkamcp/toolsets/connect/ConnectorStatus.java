package io.github.srdrcn.kafkamcp.toolsets.connect;

import static io.github.srdrcn.kafkamcp.toolsets.connect.ConnectJson.text;
import static io.github.srdrcn.kafkamcp.toolsets.connect.ConnectJson.traceSummary;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * {@code GET /connectors/{name}/status} (ya da {@code expand=status}) yanıtının ayrıştırılmış hali.
 * state/workerId/trace connector'ın kendisine aittir; görevler id'ye göre sıralıdır.
 */
public record ConnectorStatus(String name, String type, String state, String workerId, String trace,
		List<Task> tasks) {

	public static final String RUNNING = "RUNNING";

	public static final String FAILED = "FAILED";

	public static final String PAUSED = "PAUSED";

	public static final String STOPPED = "STOPPED";

	public static final String UNASSIGNED = "UNASSIGNED";

	public static final String RESTARTING = "RESTARTING";

	/** Bir görevin durumu. */
	public record Task(int id, String state, String workerId, String trace) {

		public boolean failed() {
			return FAILED.equals(state);
		}

		/** {id, state, worker, traceSummary} */
		public Map<String, Object> toMap() {
			return Results.map("id", id, "state", state, "worker", workerId, "traceSummary", traceSummary(trace));
		}

	}

	public static ConnectorStatus from(JsonNode n) {
		JsonNode c = n.path("connector");
		List<Task> tasks = new ArrayList<>();
		for (JsonNode t : n.path("tasks")) {
			tasks.add(new Task(t.path("id").asInt(), text(t, "state"), text(t, "worker_id"), text(t, "trace")));
		}
		tasks.sort(Comparator.comparingInt(Task::id));
		return new ConnectorStatus(text(n, "name"), text(n, "type"), text(c, "state"), text(c, "worker_id"),
				text(c, "trace"), List.copyOf(tasks));
	}

	public List<Integer> failedTaskIds() {
		return tasks.stream().filter(Task::failed).map(Task::id).toList();
	}

	public long tasksInState(String s) {
		return tasks.stream().filter(t -> s.equals(t.state())).count();
	}

	/** Connector ya da görevlerinden biri FAILED mı? */
	public boolean hasFailure() {
		return FAILED.equals(state) || tasks.stream().anyMatch(Task::failed);
	}

	/** Connector ve tüm görevleri verilen durumda mı (görev yoksa sadece connector'a bakılır)? */
	public boolean allIn(String s) {
		return s.equals(state) && tasks.stream().allMatch(t -> s.equals(t.state()));
	}

	/** Connector ya da bir görev geçiş durumunda mı (RESTARTING/UNASSIGNED ya da durum henüz yok)? */
	public boolean inTransition() {
		if (state == null || RESTARTING.equals(state) || UNASSIGNED.equals(state)) {
			return true;
		}
		return tasks.stream().anyMatch(t -> RESTARTING.equals(t.state()) || UNASSIGNED.equals(t.state()));
	}

	/** Connector FAILED ise onun trace'i, değilse ilk FAILED görevin trace'i. */
	public String firstTrace() {
		if (FAILED.equals(state) && trace != null) {
			return trace;
		}
		return tasks.stream().filter(Task::failed).map(Task::trace).filter(t -> t != null).findFirst().orElse(null);
	}

	/** FAILED görevlerin çalıştığı worker'lar. */
	public Set<String> failedTaskWorkers() {
		Set<String> out = new LinkedHashSet<>();
		tasks.stream().filter(Task::failed).map(Task::workerId).filter(w -> w != null).forEach(out::add);
		return out;
	}

	/** Tam durum: ad, tip, connector state/worker/traceSummary ve her görev. */
	public Map<String, Object> toMap() {
		Map<String, Object> m = Results.map("name", name, "type", type);
		m.putAll(states());
		return m;
	}

	/** Ad/tip olmadan durum: connector state/worker/traceSummary ve her görev (başka çıktıya gömmek için). */
	public Map<String, Object> states() {
		return Results.map("state", state, "worker", workerId, "traceSummary", traceSummary(trace), "tasks",
				tasks.stream().map(Task::toMap).toList());
	}

	/** Kısa özet: durum, worker ve görev sayıları. */
	public Map<String, Object> summary() {
		return Results.map("state", state, "worker", workerId, "traceSummary", traceSummary(trace), "tasks",
				tasks.size(), "runningTasks", tasksInState(RUNNING), "failedTasks", tasksInState(FAILED));
	}

}
