package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.MAX_EVIDENCE;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.first;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.omitted;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.Diagnosis.evidence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientResponseException;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectApi;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectJson;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectSecrets;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorInfo;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorOverview;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorStatus;
import io.github.srdrcn.kafkamcp.toolsets.diagnose.Checks.Check;

/** diagnose_connector: single-call Connect connector diagnosis (only on clusters with a connectUrl). */
@Component
public class DiagnoseConnectorTools {

	static final long HIGH_LAG = 10_000;

	static final long GROWING_LAG_MIN = 100;

	/** At most this many source topics are measured. */
	static final int MAX_SOURCE_TOPICS = 20;

	static final long SOURCE_IDLE_SECONDS = 3_600;

	private static final int CONFIG_VALUE_MAX = 200;

	/** Maximum time dependent sub-checks wait for the connector config / active topics. */
	private static final long DEPENDENCY_WAIT_MS = 5_000;

	/** Time left after the second sample for reading record ages / last write times. */
	private static final long WINDOW_RESERVE_MS = 3_500;

	/** Maximum number of topics shown in the facts.topics lists (the rest are counted). */
	static final int MAX_TOPICS_SHOWN = 20;

	/** Maximum number of workers shown in facts.workers.clusterTasksByWorker. */
	static final int MAX_WORKERS_SHOWN = 20;

	/** Plugin types: if the response has other types (converters etc.) besides connectors, converters can be checked. */
	private static final Set<String> CONNECTOR_TYPES = Set.of("source", "sink");

	private static final Set<String> SINGLE_TOPIC_KEYS = Set.of("topic", "topics", "kafka.topic", "topic.name");

	private final ConnectApi api;

	public DiagnoseConnectorTools(ConnectApi api) {
		this.api = api;
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("diagnose_connector",
					"Diagnose a connector: connector/task state and error summaries, config/plugin/converters, topics, sink lag, source writes, workers")
			.connect()
			.required("name", P.string("Connector name"))
			.param("intervalSeconds", DiagnoseParams.interval(DiagnoseParams.DEFAULT_INTERVAL_SECONDS))
			.handler(this::diagnose));
	}

	/**
	 * Plugins on the workers: connector classes, converter classes and whether the response really includes
	 * converters (Connect &lt; 3.2 ignores the connectorsOnly parameter and returns only connectors).
	 */
	record Plugins(Set<String> connectors, Set<String> converters, boolean includesConverters) {
	}

	record Validation(int errorCount, List<Map<String, Object>> fields) {
	}

	record TopicCheck(List<String> configured, Set<String> missing, String regex, Integer regexMatches,
			String dlq, boolean dlqExists) {
	}

	record Activity(String group, String dlq, OffsetWindow window, Set<String> sourceTopics,
			int sourceTopicsOmitted, Map<String, Long> lastWrite, StuckCheck stuck) {
	}

	Object diagnose(ToolCall call) {
		String cluster = call.cluster();
		String name = call.str("name");
		long intervalMs = DiagnoseParams.intervalMs(call, DiagnoseParams.DEFAULT_INTERVAL_SECONDS);
		ConnectorStatus status = status(cluster, name);
		Diagnosis dx = new Diagnosis();
		Map<String, Object> facts = new LinkedHashMap<>();
		try (Checks checks = new Checks(call, dx)) {
			Check<ConnectorInfo> info = checks.start("connector config", () -> api.info(cluster, name));
			Check<List<ConnectorOverview>> overview = checks.start("worker distribution", () -> api.overview(cluster));
			Check<Plugins> plugins = checks.start("plugins", () -> plugins(cluster));
			Check<List<String>> active = checks.start("active topics", () -> activeTopics(cluster, name));
			Check<Validation> validation = checks.start("config validation", () -> {
				ConnectorInfo i = info.require(DEPENDENCY_WAIT_MS);
				return i == null || i.connectorClass() == null ? null : validate(cluster, i);
			});
			Check<TopicCheck> topics = checks.start("topic existence", () -> {
				ConnectorInfo i = info.require(DEPENDENCY_WAIT_MS);
				return i == null ? null : topicCheck(call, i);
			});
			Check<Activity> activity = checks.start("sink lag / source writes", () -> {
				ConnectorInfo i = info.require(DEPENDENCY_WAIT_MS);
				List<String> a = sink(status, i) ? null : active.require(DEPENDENCY_WAIT_MS);
				return activity(call, name, status, i, a, intervalMs);
			});

			states(dx, facts, status, name);
			ConnectorInfo i = info.get();
			configFindings(dx, facts, i, status, plugins.get(), validation.get());
			topicFindings(dx, facts, topics.get(), active.get(), status, i);
			activityFindings(dx, facts, activity.get(), status);
			workers(dx, facts, overview.get(), status);
		}
		Map<String, Object> head = Results.map("name", name, "cluster", cluster);
		return dx.result(head, facts);
	}

	/** Connector status; an empty status if there is none yet (just created), an error if the connector does not exist. */
	private ConnectorStatus status(String cluster, String name) {
		try {
			return api.status(cluster, name);
		}
		catch (ToolException e) {
			if (!(e.getCause() instanceof RestClientResponseException r && r.getStatusCode().value() == 404)) {
				throw e;
			}
			ConnectorInfo info;
			try {
				info = api.info(cluster, name);
			}
			catch (ToolException notFound) {
				throw new ToolException("Connector not found: " + name + " (see connectors_list)", e);
			}
			return new ConnectorStatus(name, info.type(), null, null, null, List.of());
		}
	}

	// ---- sub-checks ----

	private Plugins plugins(String cluster) {
		JsonNode all;
		try {
			all = api.client().get(cluster, "/connector-plugins?connectorsOnly=false");
		}
		catch (ToolException e) {
			all = api.client().get(cluster, "/connector-plugins");
		}
		return plugins(all);
	}

	/**
	 * Plugins from the /connector-plugins response. Converters can only be checked if the response has non-connector
	 * types (converter, transformation...): Connect &lt; 3.2 ignores connectorsOnly=false and returns only connectors.
	 */
	static Plugins plugins(JsonNode all) {
		Set<String> connectors = new TreeSet<>();
		Set<String> converters = new TreeSet<>();
		boolean nonConnectorTypes = false;
		for (JsonNode n : all) {
			String cls = ConnectJson.text(n, "class");
			String type = ConnectJson.text(n, "type");
			if (cls == null) {
				continue;
			}
			String t = type == null ? "" : type.toLowerCase(Locale.ROOT);
			if (type == null || CONNECTOR_TYPES.contains(t)) {
				connectors.add(cls);
			}
			else {
				nonConnectorTypes = true;
				if (t.equals("converter")) {
					converters.add(cls);
				}
			}
		}
		return new Plugins(connectors, converters, nonConnectorTypes);
	}

	/**
	 * Is the value one of the installed classes? Connect's aliases are accepted too: full name, simple name or the
	 * simple name without the trailing {@code suffix} (Connector/Converter); case-insensitive.
	 */
	static boolean pluginPresent(Collection<String> classes, String value, String suffix) {
		if (value == null || value.isBlank()) {
			return false;
		}
		if ("Connector".equals(suffix)) {
			try {
				ConnectApi.resolvePlugin(new ArrayList<>(classes), value);
				return true;
			}
			catch (ToolException e) {
				return false;
			}
		}
		String v = value.trim().toLowerCase(Locale.ROOT);
		String sfx = suffix.toLowerCase(Locale.ROOT);
		for (String cls : classes) {
			String simple = ConnectJson.shortClass(cls).toLowerCase(Locale.ROOT);
			if (v.equals(cls.toLowerCase(Locale.ROOT)) || v.equals(simple)
					|| simple.endsWith(sfx) && v.equals(simple.substring(0, simple.length() - sfx.length()))) {
				return true;
			}
		}
		return false;
	}

	private static boolean sink(ConnectorStatus status, ConnectorInfo info) {
		String type = status.type() != null ? status.type() : info == null ? null : info.type();
		return "sink".equalsIgnoreCase(type);
	}

	private List<String> activeTopics(String cluster, String name) {
		JsonNode r = api.client().get(cluster, ConnectApi.path(name) + "/topics");
		JsonNode node = r.has(name) ? r.get(name) : null;
		List<String> out = new ArrayList<>();
		if (node != null) {
			node.path("topics").forEach(t -> out.add(t.asText()));
		}
		out.sort(null);
		return out;
	}

	private Validation validate(String cluster, ConnectorInfo info) {
		JsonNode r = api.client()
			.put(cluster, "/connector-plugins/" + ConnectClient.enc(info.connectorClass()) + "/config/validate",
					info.config());
		List<Map<String, Object>> fields = new ArrayList<>();
		for (JsonNode c : r.path("configs")) {
			JsonNode value = c.path("value");
			List<String> errors = new ArrayList<>();
			value.path("errors").forEach(e -> errors.add(e.asText()));
			if (errors.isEmpty()) {
				continue;
			}
			String field = ConnectJson.text(value, "name");
			fields.add(Results.map("name", field, "errors", ConnectSecrets.isSecret(field)
					? List.of("invalid value (secret field; value not shown)") : first(errors, 3)));
		}
		return new Validation(r.path("error_count").asInt(fields.size()), fields);
	}

	private static TopicCheck topicCheck(ToolCall call, ConnectorInfo info) {
		Map<String, String> cfg = info.config();
		Set<String> configured = new LinkedHashSet<>();
		cfg.forEach((k, v) -> {
			if (SINGLE_TOPIC_KEYS.contains(k.toLowerCase(Locale.ROOT)) && v != null) {
				for (String t : v.split(",")) {
					if (!t.isBlank()) {
						configured.add(t.trim());
					}
				}
			}
		});
		String regex = cfg.get("topics.regex");
		String dlq = cfg.get("errors.deadletterqueue.topic.name");
		Set<String> names = new LinkedHashSet<>(configured);
		if (dlq != null && !dlq.isBlank()) {
			names.add(dlq.trim());
		}
		Set<String> missing = new TreeSet<>();
		Map<String, Boolean> existing = null;
		if (!names.isEmpty() || regex != null && !regex.isBlank()) {
			existing = Topics.list(call, true);
			for (String n : names) {
				if (!existing.containsKey(n)) {
					missing.add(n);
				}
			}
		}
		Integer regexMatches = null;
		if (regex != null && !regex.isBlank() && existing != null) {
			try {
				Pattern p = Pattern.compile(regex.trim());
				regexMatches = (int) existing.keySet().stream().filter(t -> p.matcher(t).matches()).count();
			}
			catch (PatternSyntaxException e) {
				regexMatches = -1;
			}
		}
		boolean dlqExists = dlq != null && !dlq.isBlank() && !missing.contains(dlq.trim());
		missing.remove(dlq == null ? "" : dlq.trim());
		return new TopicCheck(List.copyOf(configured), missing, regex, regexMatches, dlq == null ? null : dlq.trim(),
				dlqExists);
	}

	private static Activity activity(ToolCall call, String name, ConnectorStatus status, ConnectorInfo info,
			List<String> active, long intervalMs) {
		Map<String, String> cfg = info == null ? Map.of() : info.config();
		boolean sink = sink(status, info);
		String group = sink ? cfg.getOrDefault("consumer.override.group.id", "connect-" + name) : null;
		Set<String> topics = new TreeSet<>();
		Set<String> sources = new TreeSet<>();
		int omittedTopics = 0;
		if (!sink && active != null) {
			List<String> all = new ArrayList<>(active);
			sources.addAll(first(all, MAX_SOURCE_TOPICS));
			topics.addAll(sources);
			omittedTopics = Math.max(0, all.size() - MAX_SOURCE_TOPICS);
		}
		String dlq = cfg.get("errors.deadletterqueue.topic.name");
		dlq = dlq == null || dlq.isBlank() ? null : dlq.trim();
		if (dlq != null) {
			topics.add(dlq);
		}
		List<TopicPartition> partitions = new ArrayList<>();
		if (!topics.isEmpty()) {
			Futures.Each<String, TopicDescription> described = Topics.describe(call, topics);
			partitions.addAll(Topics.onlinePartitions(described.values().values()));
		}
		ListOffsetsResult maxTs = partitions.isEmpty() || sink ? null
				: Offsets.submit(call, partitions, OffsetSpec.maxTimestamp());
		OffsetWindow w = OffsetWindow.measure(call, partitions, group == null ? List.of() : List.of(group), false,
				intervalMs, WINDOW_RESERVE_MS);
		StuckCheck stuck = StuckCheck.of(call, w, group == null ? List.of() : List.of(group),
				Math.min(StuckCheck.READ_BUDGET_MS, call.remainingMillis() - 2_000));
		Map<String, Long> lastWrite = new TreeMap<>();
		if (maxTs != null) {
			Map<TopicPartition, ListOffsetsResultInfo> max = Offsets.awaitEach(call, partitions, maxTs).values();
			max.forEach((tp, i) -> {
				if (i.timestamp() >= 0) {
					lastWrite.merge(tp.topic(), i.timestamp(), Math::max);
				}
			});
		}
		return new Activity(group, dlq, w, sources, omittedTopics, lastWrite, stuck);
	}

	// ---- evaluation ----

	private static void states(Diagnosis dx, Map<String, Object> facts, ConnectorStatus s, String name) {
		facts.put("type", s.type());
		facts.put("state", s.state());
		if (s.workerId() != null) {
			facts.put("worker", s.workerId());
		}
		facts.put("tasks", s.tasks().stream()
			.map(t -> Results.map("id", t.id(), "state", t.state(), "worker", t.workerId()))
			.toList());
		if (s.state() == null) {
			dx.warning("CONNECTOR_STATUS_PENDING",
					"Connector has no status yet (just created or not assigned to any worker)", null,
					"Check again in a few seconds; if it persists, check the Connect workers and the config topic");
		}
		String state = s.state() == null ? "" : s.state();
		switch (state) {
			case ConnectorStatus.FAILED -> dx.critical("CONNECTOR_FAILED", "Connector is FAILED",
					evidence("worker", s.workerId(), "traceSummary", ConnectJson.traceSummary(s.trace())),
					"Fix the error in the trace (config, access, plugin); then connectors_restart name=" + name);
			case ConnectorStatus.PAUSED -> dx.warning("CONNECTOR_PAUSED", "Connector is PAUSED: no data flows", null,
					"If this is not intended, resume it with connectors_resume");
			case ConnectorStatus.STOPPED -> dx.warning("CONNECTOR_STOPPED",
					"Connector is STOPPED: its tasks are shut down, no data flows", null,
					"If this is not intended, resume it with connectors_resume");
			case ConnectorStatus.UNASSIGNED, ConnectorStatus.RESTARTING -> dx.warning("CONNECTOR_UNASSIGNED",
					"Connector is " + state + " (not assigned to a worker or restarting)", null,
					"Check again with diagnose_connector in a few seconds; if it persists, check the workers");
			default -> {
				// RUNNING
			}
		}
		List<ConnectorStatus.Task> failed = s.tasks().stream().filter(ConnectorStatus.Task::failed).toList();
		if (!failed.isEmpty()) {
			List<Map<String, Object>> ev = new ArrayList<>();
			for (ConnectorStatus.Task t : first(failed, MAX_EVIDENCE)) {
				ev.add(Results.map("id", t.id(), "worker", t.workerId(), "traceSummary",
						ConnectJson.traceSummary(t.trace())));
			}
			dx.critical("TASKS_FAILED",
					failed.size() + "/" + s.tasks().size() + " task(s) FAILED: no data flows through them",
					evidence("tasks", ev, "omitted", omitted(failed)),
					"Restart the failed tasks with connectors_restart name=" + name + " includeTasks=true onlyFailed=true "
							+ "(single task: connectors_task_restart). If the same error recurs, fix the cause in the "
							+ "trace (config, file/access, authorization, data format)");
		}
		long unassigned = s.tasksInState(ConnectorStatus.UNASSIGNED);
		if (unassigned > 0) {
			dx.warning("TASKS_UNASSIGNED", unassigned + " task(s) UNASSIGNED (not assigned to a worker)", null,
					"Check that the workers are up and the rebalance has finished (connectors_task_distribution)");
		}
		if (ConnectorStatus.RUNNING.equals(s.state()) && s.tasks().isEmpty()) {
			dx.warning("NO_TASKS", "Connector is RUNNING but has no tasks: no data is processed", null,
					"Check the settings that produce tasks, such as tasks.max and the topic/table selection");
		}
	}

	private static void configFindings(Diagnosis dx, Map<String, Object> facts, ConnectorInfo info, ConnectorStatus s,
			Plugins plugins, Validation validation) {
		if (info == null) {
			return;
		}
		Map<String, String> cfg = info.config();
		String cls = info.connectorClass();
		facts.put("class", cls);
		Map<String, Object> masked = new TreeMap<>();
		ConnectSecrets.mask(cfg).forEach((k, v) -> {
			String text = v == null ? null : String.valueOf(v);
			masked.put(k, text != null && text.length() > CONFIG_VALUE_MAX ? text.substring(0, CONFIG_VALUE_MAX) + "…" : text);
		});
		facts.put("config", masked);
		boolean running = ConnectorStatus.RUNNING.equals(s.state()) || s.tasksInState(ConnectorStatus.RUNNING) > 0;
		if (plugins != null && cls != null && !running && !pluginPresent(plugins.connectors(), cls, "Connector")) {
			dx.critical("PLUGIN_MISSING", "connector.class is not installed on the workers: " + cls,
					evidence("class", cls), "Install the plugin in the plugin.path of every worker (check with connect_plugins_list)");
		}
		List<String> missingConverters = new ArrayList<>();
		List<String> defaulted = new ArrayList<>();
		for (String key : List.of("key.converter", "value.converter")) {
			String conv = cfg.get(key);
			if (conv == null || conv.isBlank()) {
				defaulted.add(key);
			}
			else if (plugins != null && plugins.includesConverters()
					&& !pluginPresent(plugins.converters(), conv, "Converter")) {
				missingConverters.add(key + "=" + conv.trim());
			}
		}
		if (plugins != null && !plugins.includesConverters() && defaulted.size() < 2) {
			facts.put("converterCheck",
					"not possible: the Connect REST API does not list converters (Connect < 3.2, connectorsOnly not supported)");
		}
		if (!missingConverters.isEmpty()) {
			dx.critical("CONVERTER_MISSING", "Converter class not found on the workers: " + String.join(", ", missingConverters),
					evidence("converters", missingConverters),
					"Install the converter (e.g. Avro/Protobuf converter) in the worker plugin.path or use an installed converter in the config");
		}
		if (!defaulted.isEmpty()) {
			dx.info("CONVERTER_DEFAULT", String.join(", ", defaulted)
					+ " not set on the connector: the worker's default converter is used",
					evidence("keys", defaulted), "If the data format differs from the worker default, set the converter explicitly in the connector config");
		}
		if (validation != null && validation.errorCount() > 0) {
			dx.warning("CONFIG_INVALID", "Connect config validation found " + validation.errorCount() + " error(s)",
					evidence("fields", first(validation.fields(), MAX_EVIDENCE), "omitted", omitted(validation.fields())),
					"Fix the invalid fields and update with connectors_create_or_update (try connect_plugins_validate first)");
		}
		boolean sink = "sink".equalsIgnoreCase(s.type() != null ? s.type() : info.type());
		String tolerance = cfg.get("errors.tolerance");
		String dlq = cfg.get("errors.deadletterqueue.topic.name");
		if (sink && "all".equalsIgnoreCase(tolerance) && (dlq == null || dlq.isBlank())) {
			dx.warning("ERRORS_TOLERATED_WITHOUT_DLQ",
					"errors.tolerance=all without a DLQ: records that cannot be converted are silently skipped", null,
					"Define a DLQ with errors.deadletterqueue.topic.name and set errors.log.enable=true");
		}
	}

	private static void topicFindings(Diagnosis dx, Map<String, Object> facts, TopicCheck tc, List<String> active,
			ConnectorStatus s, ConnectorInfo info) {
		boolean sink = "sink".equalsIgnoreCase(s.type() != null ? s.type() : info == null ? null : info.type());
		Map<String, Object> topics = new LinkedHashMap<>();
		if (active != null) {
			topics.put("active", first(active, MAX_TOPICS_SHOWN));
			if (active.size() > MAX_TOPICS_SHOWN) {
				topics.put("activeOmitted", active.size() - MAX_TOPICS_SHOWN);
			}
		}
		if (tc != null) {
			topics.putAll(Results.map("configured",
					tc.configured().isEmpty() ? null : first(tc.configured(), MAX_TOPICS_SHOWN), "configuredOmitted",
					omitted(tc.configured(), MAX_TOPICS_SHOWN), "regex", tc.regex(), "regexMatches", tc.regexMatches(),
					"missing", tc.missing().isEmpty() ? null : first(tc.missing(), MAX_TOPICS_SHOWN), "missingOmitted",
					omitted(tc.missing(), MAX_TOPICS_SHOWN), "deadLetterQueue", tc.dlq()));
			if (!tc.missing().isEmpty()) {
				if (sink) {
					dx.warning("TOPIC_MISSING",
							tc.missing().size() + " sink topic(s) do not exist: " + String.join(", ", first(tc.missing(), MAX_EVIDENCE)),
							evidence("topics", first(tc.missing(), MAX_EVIDENCE), "omitted", omitted(tc.missing())),
							"Fix the topic name or create the topic (topics_create)");
				}
				else {
					dx.info("TOPIC_MISSING",
							tc.missing().size() + " configured topic(s) do not exist yet: "
									+ String.join(", ", first(tc.missing(), MAX_EVIDENCE)),
							evidence("topics", first(tc.missing(), MAX_EVIDENCE), "omitted", omitted(tc.missing())),
							"If auto.create.topics.enable is off and the connector does not create topics, create them with topics_create");
				}
			}
			if (tc.regexMatches() != null && tc.regexMatches() == 0) {
				dx.warning("TOPICS_REGEX_NO_MATCH", "topics.regex matches no topic: " + tc.regex(), null,
						"Check the regex (a full match is required) and the topic names");
			}
			else if (tc.regexMatches() != null && tc.regexMatches() < 0) {
				dx.warning("TOPICS_REGEX_INVALID", "topics.regex is invalid: " + tc.regex(), null, "Fix the regex");
			}
			if (tc.dlq() != null && !tc.dlqExists()) {
				boolean running = s.tasksInState(ConnectorStatus.RUNNING) > 0;
				String msg = "DLQ topic does not exist: " + tc.dlq() + " (Connect creates it when the sink task starts)";
				String rec = "Check the worker's permission to create topics (TOPIC CREATE) and the "
						+ "errors.deadletterqueue.topic.replication.factor value, or create it with topics_create";
				if (running) {
					dx.warning("DLQ_TOPIC_MISSING", msg, null, rec);
				}
				else {
					dx.info("DLQ_TOPIC_MISSING", msg, null, rec);
				}
			}
		}
		if (!topics.isEmpty()) {
			facts.put("topics", topics);
		}
	}

	static void activityFindings(Diagnosis dx, Map<String, Object> facts, Activity a, ConnectorStatus s) {
		if (a == null) {
			return;
		}
		OffsetWindow w = a.window();
		boolean running = ConnectorStatus.RUNNING.equals(s.state()) && s.tasksInState(ConnectorStatus.RUNNING) > 0;
		if (a.group() != null) {
			OffsetWindow.GroupProgress p = w.progress(a.group());
			Map<String, Object> lag = Results.map("group", a.group());
			if (w.committed0().getOrDefault(a.group(), Map.of()).isEmpty()) {
				lag.put("note", "The group has no committed offsets");
				dx.info("SINK_GROUP_NO_OFFSETS",
						"Sink consumer group (" + a.group() + ") has no committed offsets: no data may have been processed yet",
						evidence("group", a.group(), "error", w.groupErrors().get(a.group())),
						"If the tasks are RUNNING, commits appear once data arrives on the topic; if consumer.override.group.id is used, check the name");
			}
			else {
				sinkFindings(dx, lag, a, p, running);
			}
			lag.putAll(w.describe());
			facts.put("sinkLag", lag);
		}
		Map<TopicPartition, Long> delta = w.endDelta();
		if (a.group() == null) {
			Map<String, long[]> byTopic = new TreeMap<>();
			delta.forEach((tp, d) -> {
				if (a.sourceTopics().contains(tp.topic())) {
					byTopic.computeIfAbsent(tp.topic(), k -> new long[1])[0] += d;
				}
			});
			long total = byTopic.values().stream().mapToLong(v -> v[0]).sum();
			List<Map<String, Object>> rows = new ArrayList<>();
			byTopic.forEach((t, v) -> rows.add(Results.map("topic", t, "msgsPerSec", w.rate(v[0]), "lastWriteTime",
					Format.iso(a.lastWrite().getOrDefault(t, -1L)), "lastWriteAgeSeconds",
					Format.ageSeconds(a.lastWrite().getOrDefault(t, -1L)))));
			Map<String, Object> src = Results.map("msgsPerSec", w.rate(total), "topics", rows, "topicsOmitted",
					a.sourceTopicsOmitted() > 0 ? a.sourceTopicsOmitted() : null);
			src.putAll(w.describe());
			facts.put("sourceActivity", src);
			long newest = a.lastWrite().values().stream().mapToLong(Long::longValue).max().orElse(-1);
			if (running && w.complete() && total == 0 && !a.sourceTopics().isEmpty()
					&& (newest < 0 || Format.ageSeconds(newest) > SOURCE_IDLE_SECONDS)) {
				dx.info("SOURCE_IDLE", "Source connector is running but does not write to its topics"
						+ (newest < 0 ? "" : " (last write " + Format.ageSeconds(newest) / 60 + " min ago)"),
						evidence("topics", first(a.sourceTopics(), MAX_EVIDENCE), "omitted", omitted(a.sourceTopics())),
						"Check whether the source system has new data and whether the connector offset advances (connectors_offsets)");
			}
		}
		if (a.dlq() != null) {
			String dlqTopic = a.dlq();
			long dlqDelta = delta.entrySet().stream().filter(e -> e.getKey().topic().equals(dlqTopic))
				.mapToLong(Map.Entry::getValue)
				.sum();
			if (dlqDelta > 0) {
				dx.warning("DLQ_RECEIVING", dlqDelta + " record(s) were written to the DLQ (" + dlqTopic + ") during the measurement",
						evidence("topic", dlqTopic, "records", dlqDelta),
						"Inspect the error cause in the headers of the DLQ records (messages_headers)");
			}
		}
		if (!w.groupErrors().isEmpty() || !w.endErrors().isEmpty()) {
			dx.markPartial();
		}
	}

	/**
	 * Lag/stuck findings of the sink group. Connect sink tasks commit offsets every offset.flush.interval.ms (default
	 * 60 s): seeing no commit in a short window alone does not mean stuck; the verdict is based on the age of the oldest
	 * pending record ({@link StuckCheck}).
	 */
	static void sinkFindings(Diagnosis dx, Map<String, Object> lag, Activity a, OffsetWindow.GroupProgress p,
			boolean running) {
		StuckCheck.Verdict v = a.stuck().verdict(a.group());
		lag.putAll(p.summary());
		if (p.complete()) {
			lag.put("stuckPartitions", v.stuck().size());
			if (!v.suspect().isEmpty()) {
				lag.put("stuckSuspectPartitions", v.suspect().size());
			}
		}
		Map<String, Object> window = v.window();
		boolean stalled = !v.stuck().isEmpty() || !v.suspect().isEmpty();
		if (!v.stuck().isEmpty()) {
			Map<String, Object> ev = evidence("partitions", v.evidence(StuckCheck.Level.STUCK, MAX_EVIDENCE), "omitted",
					omitted(v.stuck()), "group", a.group());
			ev.putAll(window);
			String age = ", the oldest pending record was written " + v.maxBehindSeconds(StuckCheck.Level.STUCK) / 60 + " min ago";
			if (running) {
				dx.critical("SINK_STUCK",
						v.stuck().size() + " partition(s) have a sink commit that does not advance while new messages arrive" + age, ev,
						"Check the task logs and the target system (write errors, slowness); try restarting the tasks "
								+ "with connectors_restart includeTasks=true onlyFailed=false");
			}
			else {
				dx.warning("SINK_STUCK",
						v.stuck().size() + " partition(s) are not progressing in the sink (tasks are not RUNNING)" + age, ev,
						"Fix the task states first (see the TASKS_FAILED / PAUSED findings)");
			}
		}
		if (!v.suspect().isEmpty()) {
			Map<String, Object> ev = evidence("partitions", v.evidence(StuckCheck.Level.SUSPECT, MAX_EVIDENCE),
					"omitted", omitted(v.suspect()), "group", a.group());
			ev.putAll(window);
			dx.warning("SINK_STUCK_SUSPECT",
					v.suspect().size() + " partition(s) did not advance the sink commit in the window and the oldest pending record "
							+ "was written about " + v.maxBehindSeconds(StuckCheck.Level.SUSPECT) + " s ago (sinks commit "
							+ "every ~60 s; the " + StuckCheck.COMMIT_ALLOWANCE_SECONDS + " s allowance was exceeded)",
					ev, "Check again with diagnose_connector in a few minutes; if it does not progress, check the task logs "
							+ "and the target system");
		}
		if (!v.periodic().isEmpty() && !stalled) {
			Map<String, Object> ev = evidence("partitions", v.evidence(StuckCheck.Level.PERIODIC, MAX_EVIDENCE),
					"omitted", omitted(v.periodic()), "group", a.group());
			ev.putAll(window);
			dx.info("SINK_COMMIT_NOT_ADVANCED",
					"The sink commit did not advance in the window but the pending records are recent (oldest "
							+ v.maxBehindSeconds(StuckCheck.Level.PERIODIC) + " s): sink tasks commit every "
							+ "offset.flush.interval.ms (default 60 s), no evidence of being stuck",
					ev, null);
		}
		if ("growing".equals(p.trend()) && p.lag() >= GROWING_LAG_MIN && !stalled && p.lagBeyondCommitAllowance()) {
			dx.warning("SINK_LAG_GROWING",
					"Sink lag is growing: consuming " + p.consumeRate() + " msg/s, producing " + p.produceRate() + " msg/s",
					evidence("lag", p.lag(), "lagAtStart", p.lag0(), "windowSeconds", window.get("windowSeconds")),
					"Increase tasks.max up to the partition count; check the write capacity of the target system and the batch settings");
		}
		else if (p.lag() >= HIGH_LAG && !stalled) {
			dx.info("SINK_LAG_HIGH", "Sink lag is " + p.lag(), evidence("lag", p.lag()), "Follow the trend");
		}
	}

	private static void workers(Diagnosis dx, Map<String, Object> facts, List<ConnectorOverview> all,
			ConnectorStatus s) {
		Map<String, Integer> mine = new TreeMap<>();
		s.tasks().forEach(t -> mine.merge(t.workerId() == null ? "(unassigned)" : t.workerId(), 1, Integer::sum));
		Map<String, Object> w = Results.map("tasksByWorker", mine);
		if (all != null) {
			Map<String, Integer> load = new TreeMap<>();
			for (ConnectorOverview c : all) {
				for (ConnectorStatus.Task t : c.status().tasks()) {
					if (t.workerId() != null) {
						load.merge(t.workerId(), 1, Integer::sum);
					}
				}
			}
			Map<String, Integer> shown = new LinkedHashMap<>();
			first(load.entrySet(), MAX_WORKERS_SHOWN).forEach(e -> shown.put(e.getKey(), e.getValue()));
			w.put("clusterTasksByWorker", shown);
			w.put("workers", load.size());
			if (load.size() > MAX_WORKERS_SHOWN) {
				w.put("clusterTasksByWorkerOmitted", load.size() - MAX_WORKERS_SHOWN);
			}
			if (load.size() > 1) {
				int max = load.values().stream().mapToInt(Integer::intValue).max().orElse(0);
				int min = load.values().stream().mapToInt(Integer::intValue).min().orElse(0);
				double avg = load.values().stream().mapToInt(Integer::intValue).average().orElse(0);
				if (max - min > 2 && max > 2 * avg) {
					dx.info("WORKER_IMBALANCE", "Tasks are unevenly distributed across workers (max " + max
							+ ", min " + min + ")", evidence("tasksByWorker", load),
							"Connect incremental cooperative rebalancing evens this out shortly; if it persists, check the workers");
				}
			}
		}
		Set<String> failedWorkers = s.failedTaskWorkers();
		if (!failedWorkers.isEmpty()) {
			w.put("failedTaskWorkers", List.copyOf(failedWorkers));
		}
		facts.put("workers", w);
	}

}
