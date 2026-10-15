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
 * Parsed {@code GET /connectors/{name}/status} (or {@code expand=status}) response. state/workerId/trace belong to
 * the connector itself; tasks are sorted by id.
 */
public record ConnectorStatus(String name, String type, String state, String workerId, String trace,
		List<Task> tasks) {

	public static final String RUNNING = "RUNNING";

	public static final String FAILED = "FAILED";

	public static final String PAUSED = "PAUSED";

	public static final String STOPPED = "STOPPED";

	public static final String UNASSIGNED = "UNASSIGNED";

	public static final String RESTARTING = "RESTARTING";

	/** Status of a task. */
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

	/** Is the connector or one of its tasks FAILED? */
	public boolean hasFailure() {
		return FAILED.equals(state) || tasks.stream().anyMatch(Task::failed);
	}

	/** Are the connector and all its tasks in the given state (only the connector is checked if there are no tasks)? */
	public boolean allIn(String s) {
		return s.equals(state) && tasks.stream().allMatch(t -> s.equals(t.state()));
	}

	/** Is the connector or a task in transition (RESTARTING/UNASSIGNED, or no state yet)? */
	public boolean inTransition() {
		if (state == null || RESTARTING.equals(state) || UNASSIGNED.equals(state)) {
			return true;
		}
		return tasks.stream().anyMatch(t -> RESTARTING.equals(t.state()) || UNASSIGNED.equals(t.state()));
	}

	/** The connector's trace if it is FAILED, otherwise the trace of the first FAILED task. */
	public String firstTrace() {
		if (FAILED.equals(state) && trace != null) {
			return trace;
		}
		return tasks.stream().filter(Task::failed).map(Task::trace).filter(t -> t != null).findFirst().orElse(null);
	}

	/** Workers running the FAILED tasks. */
	public Set<String> failedTaskWorkers() {
		Set<String> out = new LinkedHashSet<>();
		tasks.stream().filter(Task::failed).map(Task::workerId).filter(w -> w != null).forEach(out::add);
		return out;
	}

	/** Full status: name, type, connector state/worker/traceSummary and every task. */
	public Map<String, Object> toMap() {
		Map<String, Object> m = Results.map("name", name, "type", type);
		m.putAll(states());
		return m;
	}

	/** Status without name/type: connector state/worker/traceSummary and every task (for embedding in other output). */
	public Map<String, Object> states() {
		return Results.map("state", state, "worker", workerId, "traceSummary", traceSummary(trace), "tasks",
				tasks.stream().map(Task::toMap).toList());
	}

	/** Short summary: state, worker and task counts. */
	public Map<String, Object> summary() {
		return Results.map("state", state, "worker", workerId, "traceSummary", traceSummary(trace), "tasks",
				tasks.size(), "runningTasks", tasksInState(RUNNING), "failedTasks", tasksInState(FAILED));
	}

}
