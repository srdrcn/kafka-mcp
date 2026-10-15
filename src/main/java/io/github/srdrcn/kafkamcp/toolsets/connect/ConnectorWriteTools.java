package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/**
 * Connector write tools: pause/resume/stop/restart/task_restart/create_or_update and the destructive
 * connectors_delete, connectors_offsets_reset. Tools that change the state return the new state after polling it
 * for a short, bounded time.
 */
@Component
public class ConnectorWriteTools {

	/** Maximum wait for a state transition. */
	static final long STATE_WAIT_MS = 6_000;

	private final ConnectApi api;

	public ConnectorWriteTools(ConnectApi api) {
		this.api = api;
	}

	public List<ToolSpec> tools() {
		Map<String, Object> name = ConnectorReadTools.nameParam();
		return List.of(
				ToolSpec.write("connectors_pause", "Pause a connector and its tasks (PAUSED)")
					.connect()
					.required("name", name)
					.handler(this::pause),
				ToolSpec.write("connectors_resume", "Resume a paused/stopped connector (RUNNING)")
					.connect()
					.required("name", name)
					.handler(this::resume),
				ToolSpec.write("connectors_stop", "Stop a connector (STOPPED: its tasks shut down, the config is kept)")
					.connect()
					.required("name", name)
					.handler(this::stop),
				ToolSpec
					.write("connectors_restart",
							"Restart a connector (optionally with its tasks, or only the FAILED ones)")
					.connect()
					.required("name", name)
					.param("includeTasks", P.bool("Also restart the tasks (default false)"))
					.param("onlyFailed", P.bool("Restart only the FAILED connector/tasks (default false)"))
					.handler(this::restart),
				ToolSpec.write("connectors_task_restart", "Restart a single task of a connector")
					.connect()
					.required("name", name)
					.required("taskId", P.integer("Task id (starts at 0)"))
					.handler(this::taskRestart),
				ToolSpec.write("connectors_create_or_update", "Create a connector or replace its whole config")
					.connect()
					.required("name", name)
					.required("config", P.stringMap("Connector config (key → string value; including connector.class)"))
					.handler(this::createOrUpdate),
				ToolSpec.destructive("connectors_delete", "Delete a connector (irreversible)")
					.connect()
					.required("name", name)
					.handler(this::delete),
				ToolSpec
					.destructive("connectors_offsets_reset",
							"Reset the offsets of a STOPPED connector (irreversible)")
					.connect()
					.required("name", name)
					.handler(this::offsetsReset));
	}

	private Map<String, Object> stateChange(ToolCall call, String action, String endpoint, String target,
			Predicate<ConnectorStatus> until) {
		String name = call.str("name");
		ConnectClient.Response r = api.client().send(call.cluster(), "PUT", ConnectApi.path(name) + endpoint, null);
		return withState(Results.map("name", name, "action", action, "httpStatus", r.status()), target, null,
				api.await(call, name, 0, STATE_WAIT_MS, until));
	}

	/**
	 * Adds the polling result to the output; if it did not settle, adds a short note on why ({@code before}: the
	 * status before the call, if known).
	 */
	private static Map<String, Object> withState(Map<String, Object> out, String target, ConnectorStatus before,
			ConnectApi.Awaited a) {
		ConnectorStatus s = a.status();
		if (s != null) {
			out.put("status", s.states());
		}
		out.put("settled", a.settled());
		if (!a.settled()) {
			String note;
			if (s != null && s.equals(before)) {
				note = "Status is the same as before the call: the operation may not have been applied yet, or it may"
						+ " have finished too fast to observe and returned to the same state";
			}
			else if (target != null && s != null && target.equals(s.state())) {
				note = "Connector is " + target + " but its tasks have not settled yet";
			}
			else {
				note = target == null ? "Status has not settled yet" : "Status is not " + target + " yet";
			}
			out.put("note",
					note + "; check again with connectors_status" + (a.error() == null ? "" : " (" + a.error() + ")"));
		}
		return out;
	}

	Object pause(ToolCall call) {
		return stateChange(call, "pause", "/pause", ConnectorStatus.PAUSED, s -> s.allIn(ConnectorStatus.PAUSED));
	}

	Object resume(ToolCall call) {
		return stateChange(call, "resume", "/resume", ConnectorStatus.RUNNING,
				ConnectorSettle.resumed(System::nanoTime));
	}

	Object stop(ToolCall call) {
		return stateChange(call, "stop", "/stop", ConnectorStatus.STOPPED,
				s -> ConnectorStatus.STOPPED.equals(s.state()) && s.tasks().isEmpty());
	}

	Object restart(ToolCall call) {
		String name = call.str("name");
		boolean includeTasks = call.bool("includeTasks", false);
		boolean onlyFailed = call.bool("onlyFailed", false);
		ConnectorStatus before = api.statusOrNull(call.cluster(), name);
		ConnectClient.Response r = api.client()
			.send(call.cluster(), "POST", ConnectApi.path(name) + "/restart?includeTasks=" + includeTasks
					+ "&onlyFailed=" + onlyFailed, null);
		Map<String, Object> out = Results.map("name", name, "action", "restart", "includeTasks", includeTasks,
				"onlyFailed", onlyFailed, "httpStatus", r.status());
		// Items that must change (or be seen RESTARTING/UNASSIGNED) to count as settled. 202: the request is
		// written to the config topic and the workers apply the plan later → every item in the plan. 204: the
		// connector was already restarted, but if it was FAILED before, the status cache may still show the old
		// FAILED for a while.
		List<Integer> mustChange = new ArrayList<>();
		if (r.body().isObject()) {
			// 202: the plan in which Connect marks what it will restart as RESTARTING
			ConnectorStatus planned = ConnectorStatus.from(r.body());
			boolean connector = ConnectorStatus.RESTARTING.equals(planned.state());
			List<Integer> tasks = planned.tasks()
				.stream()
				.filter(t -> ConnectorStatus.RESTARTING.equals(t.state()))
				.map(ConnectorStatus.Task::id)
				.toList();
			out.put("restarting", Results.map("connector", connector, "tasks", tasks));
			if (connector) {
				mustChange.add(ConnectorSettle.CONNECTOR);
			}
			mustChange.addAll(tasks);
		}
		else if (before != null && ConnectorStatus.FAILED.equals(before.state())) {
			mustChange.add(ConnectorSettle.CONNECTOR);
		}
		Predicate<ConnectorStatus> changed = ConnectorSettle.changedFrom(before, mustChange);
		// On 202 poll right away so the transition (RESTARTING/UNASSIGNED) is not missed; on 204 wait for the new
		// status to be readable
		long initialDelay = r.body().isObject() ? 0 : 500;
		return withState(out, null, before,
				api.await(call, name, initialDelay, STATE_WAIT_MS, s -> changed.test(s) && !s.inTransition()));
	}

	Object taskRestart(ToolCall call) {
		String name = call.str("name");
		long taskId = call.requiredLong("taskId");
		if (taskId < 0) {
			throw new ToolException("taskId cannot be negative");
		}
		ConnectorStatus before = api.statusOrNull(call.cluster(), name);
		ConnectClient.Response r = api.client()
			.send(call.cluster(), "POST", ConnectApi.path(name) + "/tasks/" + taskId + "/restart", null);
		int id = (int) taskId;
		// 204: the task was restarted; but if it was FAILED before, the status cache may still show the old FAILED
		// for a while → it does not count as settled until the task changes (or is seen in transition).
		ConnectorStatus.Task old = before == null ? null : task(before, id);
		Predicate<ConnectorStatus> changed = ConnectorSettle.changedFrom(before,
				old != null && old.failed() ? List.of(id) : List.of());
		ConnectApi.Awaited a = api.await(call, name, 500, STATE_WAIT_MS, s -> changed.test(s) && s.tasks()
			.stream()
			.anyMatch(t -> t.id() == id && (ConnectorStatus.RUNNING.equals(t.state()) || t.failed())));
		Map<String, Object> out = Results.map("name", name, "action", "task_restart", "taskId", taskId, "httpStatus",
				r.status());
		ConnectorStatus.Task now = a.status() == null ? null : task(a.status(), id);
		if (now != null) {
			out.put("task", now.toMap());
		}
		out.put("settled", a.settled());
		if (!a.settled()) {
			out.put("note", (now != null && now.equals(old)
					? "Task status is the same as before the call: the restart may not be visible yet, or the task may"
							+ " have failed again with the same error"
					: "Task is not RUNNING/FAILED yet") + "; check again with connectors_status"
					+ (a.error() == null ? "" : " (" + a.error() + ")"));
		}
		return out;
	}

	private static ConnectorStatus.Task task(ConnectorStatus s, int id) {
		return s.tasks().stream().filter(t -> t.id() == id).findFirst().orElse(null);
	}

	Object createOrUpdate(ToolCall call) {
		String name = call.str("name");
		Map<String, String> config = call.stringMap("config");
		if (config.isEmpty()) {
			throw new ToolException("config cannot be empty (at least connector.class is required)");
		}
		config.forEach((k, v) -> {
			if (v == null) {
				throw new ToolException("config value cannot be null: " + k);
			}
		});
		ConnectorStatus before = api.statusOrNull(call.cluster(), name);
		ConnectClient.Response r = api.client().send(call.cluster(), "PUT", ConnectApi.path(name) + "/config", config);
		boolean created = r.status() == 201;
		JsonNode body = r.body();
		String connectorClass = ConnectJson.text(body.path("config"), ConnectorInfo.CONNECTOR_CLASS);
		Map<String, Object> out = Results.map("name", name, "result", created ? "created" : "updated",
				"httpStatus", r.status(), "type", ConnectJson.text(body, "type"), "class",
				connectorClass == null ? config.get(ConnectorInfo.CONNECTOR_CLASS) : connectorClass);
		// On update (200) Connect restarts the connector/tasks after responding: it does not count as settled until
		// the pre-call status changes (or is seen in transition).
		ConnectorStatus old = created ? null : before;
		return withState(out, null, old,
				api.await(call, name, 0, STATE_WAIT_MS, ConnectorSettle.applied(old, System::nanoTime)));
	}

	Object delete(ToolCall call) {
		String name = call.str("name");
		ConnectClient.Response r = api.client().send(call.cluster(), "DELETE", ConnectApi.path(name), null);
		return Results.map("name", name, "deleted", true, "httpStatus", r.status());
	}

	Object offsetsReset(ToolCall call) {
		String name = call.str("name");
		ConnectorStatus s = api.status(call.cluster(), name);
		if (!ConnectorStatus.STOPPED.equals(s.state())) {
			throw new ToolException("Connector '" + name + "' is " + s.state()
					+ "; offsets can only be reset in the STOPPED state. Stop it first with connectors_stop.");
		}
		ConnectClient.Response r = api.client()
			.send(call.cluster(), "DELETE", ConnectApi.path(name) + "/offsets", null);
		return Results.map("name", name, "reset", true, "httpStatus", r.status(), "message",
				ConnectJson.text(r.body(), "message"));
	}

}
