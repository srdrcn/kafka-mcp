package io.github.srdrcn.kafkamcp.toolsets.connect;

import static io.github.srdrcn.kafkamcp.toolsets.connect.ConnectJson.shortClass;
import static io.github.srdrcn.kafkamcp.toolsets.connect.ConnectJson.traceSummary;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/**
 * Connector read tools: connectors_list, connectors_get, connectors_status, connectors_failed,
 * connectors_topics, connectors_offsets.
 */
@Component
public class ConnectorReadTools {

	static final List<String> STATES = List.of(ConnectorStatus.RUNNING, ConnectorStatus.PAUSED,
			ConnectorStatus.STOPPED, ConnectorStatus.FAILED, ConnectorStatus.UNASSIGNED, ConnectorStatus.RESTARTING);

	private final ConnectApi api;

	public ConnectorReadTools(ConnectApi api) {
		this.api = api;
	}

	static Map<String, Object> nameParam() {
		return P.string("Connector name");
	}

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("connectors_list", "List connectors with state, type, class, worker and task counts")
					.connect()
					.listParams()
					.param("state", P.enumOf("Connector state filter", STATES))
					.handler(this::list),
				ToolSpec.read("connectors_get", "Connector details: type, class, masked config, tasks and status summary")
					.connect()
					.required("name", nameParam())
					.param("includeTaskConfigs", P.bool("Also return the task configs (masked; default false)"))
					.handler(this::get),
				ToolSpec
					.read("connectors_status",
							"Status, worker and error summary (first 3 trace lines) of a connector and its tasks")
					.connect()
					.required("name", nameParam())
					.handler(this::status),
				ToolSpec.read("connectors_failed", "FAILED connectors and connectors with FAILED tasks, with an error summary")
					.connect()
					.listParams()
					.handler(this::failed),
				ToolSpec.read("connectors_topics", "Topics actively used by a connector")
					.connect()
					.required("name", nameParam())
					.handler(this::topics),
				ToolSpec
					.read("connectors_offsets",
							"Connector offsets (source: partition/offset, sink: topic/partition/offset)")
					.connect()
					.required("name", nameParam())
					.listParams()
					.param("filter", P.string("Filter on the topic (sink) or the partition JSON (source) (regex, partial match)"))
					.handler(this::offsets));
	}

	Object list(ToolCall call) {
		String state = call.str("state", null);
		if (state != null && !STATES.contains(state.toUpperCase(java.util.Locale.ROOT))) {
			throw new ToolException("Invalid state: " + state + " (valid: " + STATES + ")");
		}
		List<Map<String, Object>> rows = new ArrayList<>();
		Map<String, Integer> states = new TreeMap<>();
		for (ConnectorOverview c : api.overview(call.cluster())) {
			ConnectorStatus s = c.status();
			if (!call.matches(c.name()) || state != null && !state.equalsIgnoreCase(s.state())) {
				continue;
			}
			states.merge(s.state() == null ? "UNKNOWN" : s.state(), 1, Integer::sum);
			rows.add(Results.map("name", c.name(), "type", c.type(), "class", shortClass(c.connectorClass()), "state",
					s.state(), "worker", s.workerId(), "tasks", s.tasks().size(), "failedTasks",
					s.failedTaskIds().size()));
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("states", states);
		out.putAll(Results.page("connectors", rows, call.limit()));
		return out;
	}

	Object get(ToolCall call) {
		String cluster = call.cluster();
		String name = call.str("name");
		ConnectorInfo info = api.info(cluster, name);
		ConnectorStatus status = null;
		String statusError = null;
		try {
			status = api.status(cluster, name);
		}
		catch (ToolException e) {
			statusError = e.getMessage();
		}
		Map<Integer, Map<String, String>> taskConfigs = call.bool("includeTaskConfigs", false)
				? api.taskConfigs(cluster, name) : Map.of();
		TreeSet<Integer> ids = new TreeSet<>(info.taskIds());
		Map<Integer, ConnectorStatus.Task> taskStatus = new LinkedHashMap<>();
		if (status != null) {
			status.tasks().forEach(t -> taskStatus.put(t.id(), t));
			ids.addAll(taskStatus.keySet());
		}
		ids.addAll(taskConfigs.keySet());
		List<Map<String, Object>> tasks = new ArrayList<>();
		for (Integer id : ids) {
			ConnectorStatus.Task t = taskStatus.get(id);
			Map<String, Object> row = t == null ? Results.map("id", id) : t.toMap();
			if (taskConfigs.containsKey(id)) {
				row.put("config", ConnectSecrets.mask(taskConfigs.get(id)));
			}
			tasks.add(row);
		}
		return Results.map("name", info.name(), "type", info.type() != null ? info.type()
				: status == null ? null : status.type(), "class", info.connectorClass(), "status",
				status == null ? null : status.summary(), "statusError", statusError, "config",
				ConnectSecrets.mask(info.config()), "tasks", tasks);
	}

	Object status(ToolCall call) {
		ConnectorStatus s = api.status(call.cluster(), call.str("name"));
		Map<String, Object> out = s.toMap();
		out.put("failedTasks", s.failedTaskIds().size());
		return out;
	}

	Object failed(ToolCall call) {
		List<ConnectorOverview> all = api.overview(call.cluster());
		List<Map<String, Object>> rows = new ArrayList<>();
		for (ConnectorOverview c : all) {
			ConnectorStatus s = c.status();
			if (!s.hasFailure() || !call.matches(c.name())) {
				continue;
			}
			rows.add(Results.map("name", c.name(), "type", c.type(), "class", shortClass(c.connectorClass()), "state",
					s.state(), "worker", s.workerId(), "tasks", s.tasks().size(), "failedTaskIds", s.failedTaskIds(),
					"failedTaskWorkers", s.failedTaskWorkers().isEmpty() ? null : s.failedTaskWorkers(),
					"traceSummary", traceSummary(s.firstTrace())));
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("checked", all.size());
		out.putAll(Results.page("connectors", rows, call.limit()));
		return out;
	}

	Object topics(ToolCall call) {
		String name = call.str("name");
		JsonNode r = api.client().get(call.cluster(), ConnectApi.path(name) + "/topics");
		JsonNode node = r.has(name) ? r.get(name) : r.properties().stream().findFirst().map(Map.Entry::getValue)
			.orElse(null);
		List<String> topics = new ArrayList<>();
		if (node != null) {
			node.path("topics").forEach(t -> topics.add(t.asText()));
		}
		topics.sort(null);
		return Results.map("name", name, "count", topics.size(), "topics", topics);
	}

	Object offsets(ToolCall call) {
		String name = call.str("name");
		JsonNode r = api.client().get(call.cluster(), ConnectApi.path(name) + "/offsets");
		List<Map<String, Object>> sink = new ArrayList<>();
		List<Map<String, Object>> source = new ArrayList<>();
		for (JsonNode o : r.path("offsets")) {
			JsonNode p = o.path("partition");
			JsonNode off = o.path("offset");
			if (p.has("kafka_topic") && p.has("kafka_partition")) {
				String topic = p.get("kafka_topic").asText();
				if (call.matches(topic)) {
					sink.add(Results.map("topic", topic, "partition", p.get("kafka_partition").asInt(), "offset",
							off.has("kafka_offset") ? off.get("kafka_offset").asLong() : ConnectJson.toJava(off)));
				}
			}
			else if (call.matches(p.toString())) {
				source.add(Results.map("partition", ConnectJson.toJava(p), "offset", ConnectJson.toJava(off)));
			}
		}
		sink.sort(Comparator.comparing((Map<String, Object> m) -> (String) m.get("topic"))
			.thenComparing(m -> (Integer) m.get("partition")));
		List<Map<String, Object>> rows = new ArrayList<>(sink);
		rows.addAll(source);
		String kind = sink.isEmpty() && source.isEmpty() ? null : source.isEmpty() ? "sink" : sink.isEmpty() ? "source"
				: "mixed";
		Map<String, Object> out = Results.map("name", name, "kind", kind);
		out.putAll(Results.page("offsets", rows, call.limit()));
		return out;
	}

}
