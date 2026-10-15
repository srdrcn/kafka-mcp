package io.github.srdrcn.kafkamcp.toolsets.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.support.TestConfigs;

/**
 * Write tools: create_or_update, pause/resume/stop, restart, offsets_reset (refused when RUNNING, works when
 * STOPPED), delete; names containing ';', resume of a connector without tasks; write tools refused on test-ro.
 */
class ConnectorWriteToolsTest extends ConnectToolTestBase {

	@Test
	void createUpdatePauseResumeStopRestartDelete() throws Exception {
		String token = unique("wr");
		String topic = token + "-in";
		String name = "mcp write " + token;
		createTopic(topic);
		Map<String, Object> config = sinkConfig(topic, "/tmp/" + token + ".txt");

		JsonNode created = create(name, config);
		assertThat(created.get("result").asText()).isEqualTo("created");
		assertThat(created.get("httpStatus").asInt()).isEqualTo(201);
		assertThat(created.get("type").asText()).isEqualTo("sink");
		assertThat(created.get("class").asText()).isEqualTo(FILE_SINK);
		awaitTaskState(name, "RUNNING");

		config.put("tasks.max", "2");
		config.put("database.password", "pw-x");
		JsonNode updated = ok("connectors_create_or_update", args("name", name, "config", config));
		assertThat(updated.get("result").asText()).isEqualTo("updated");
		assertThat(updated.get("httpStatus").asInt()).isEqualTo(200);
		assertThat(updated.toString()).doesNotContain("pw-x");
		JsonNode restConfig = rest.get(TestConfigs.CLUSTER, ConnectApi.path(name) + "/config");
		assertThat(restConfig.get("tasks.max").asText()).isEqualTo("2");
		assertThat(restConfig.get("database.password").asText()).isEqualTo("pw-x");
		assertThat(error("connectors_create_or_update", args("name", name, "config", Map.of())))
			.contains("config cannot be empty");

		JsonNode paused = ok("connectors_pause", args("name", name));
		assertThat(paused.get("httpStatus").asInt()).isEqualTo(202);
		awaitConnectorState(name, "PAUSED");
		assertThat(ok("connectors_status", args("name", name)).get("state").asText()).isEqualTo("PAUSED");

		JsonNode resumed = ok("connectors_resume", args("name", name));
		assertThat(resumed.get("httpStatus").asInt()).isEqualTo(202);
		awaitConnectorState(name, "RUNNING");

		JsonNode restart = ok("connectors_restart", args("name", name));
		assertThat(restart.get("httpStatus").asInt()).isEqualTo(204);
		assertThat(restart.has("restarting")).isFalse();
		JsonNode restartAll = ok("connectors_restart", args("name", name, "includeTasks", true));
		assertThat(restartAll.get("httpStatus").asInt()).isEqualTo(202);
		assertThat(restartAll.at("/restarting/connector").asBoolean()).isTrue();
		awaitTaskState(name, "RUNNING");
		await("task 1 after tasks.max=2", () -> restStatus(name), s -> task(s, 1).isPresent());
		JsonNode taskRestart = ok("connectors_task_restart", args("name", name, "taskId", 1));
		assertThat(taskRestart.get("httpStatus").asInt()).isEqualTo(204);
		assertThat(taskRestart.at("/task/id").asInt()).isEqualTo(1);
		await("task 1 RUNNING", () -> restStatus(name),
				s -> task(s, 1).map(t -> "RUNNING".equals(t.path("state").asText())).orElse(false));

		JsonNode stopped = ok("connectors_stop", args("name", name));
		assertThat(stopped.get("httpStatus").asInt()).isBetween(202, 204);
		JsonNode stoppedStatus = awaitConnectorState(name, "STOPPED");
		assertThat(stoppedStatus.get("tasks")).isEmpty();

		JsonNode deleted = ok("connectors_delete", args("name", name));
		assertThat(deleted.get("deleted").asBoolean()).isTrue();
		assertThat(deleted.get("httpStatus").asInt()).isEqualTo(204);
		JsonNode names = rest.get(TestConfigs.CLUSTER, "/connectors");
		assertThat(texts(names)).doesNotContain(name);
		assertThat(error("connectors_delete", args("name", name))).contains("404");
	}

	@Test
	void offsetsResetRequiresStoppedConnector() throws Exception {
		String token = unique("off");
		String topic = token + "-out";
		String name = "mcp source " + token;
		createTopic(topic);
		create(name, sourceConfig(topic));
		JsonNode offsets = await("source offset", () -> ok("connectors_offsets", args("name", name)),
				o -> o.get("total").asInt() == 1);
		assertThat(offsets.get("kind").asText()).isEqualTo("source");
		assertThat(offsets.at("/offsets/0/partition/filename").asText()).isEqualTo("/etc/passwd");
		assertThat(offsets.at("/offsets/0/offset/position").asLong()).isPositive();
		JsonNode topics = await("source topic", () -> ok("connectors_topics", args("name", name)),
				t -> t.get("count").asInt() == 1);
		assertThat(texts(topics.get("topics"))).containsExactly(topic);

		String refused = error("connectors_offsets_reset", args("name", name));
		assertThat(refused).contains("RUNNING").contains("STOPPED").contains("connectors_stop");
		assertThat(ok("connectors_offsets", args("name", name)).get("total").asInt()).isEqualTo(1);

		JsonNode stopped = ok("connectors_stop", args("name", name));
		assertThat(stopped.get("httpStatus").asInt()).isBetween(202, 204);
		// stopping (config topic write + task shutdown + rebalance) can take longer than the tool's short wait on a
		// busy machine: wait (bounded) via REST
		await(name + " STOPPED, no tasks", () -> restStatus(name),
				st -> "STOPPED".equals(state(st)) && st.path("tasks").isEmpty());

		JsonNode reset = ok("connectors_offsets_reset", args("name", name));
		assertThat(reset.get("reset").asBoolean()).isTrue();
		assertThat(reset.get("httpStatus").asInt()).isEqualTo(200);
		assertThat(reset.get("message").asText()).containsIgnoringCase("reset successfully");
		JsonNode after = ok("connectors_offsets", args("name", name));
		assertThat(after.get("total").asInt()).isZero();
		assertThat(rest.get(TestConfigs.CLUSTER, ConnectApi.path(name) + "/offsets").get("offsets")).isEmpty();
	}

	@Test
	void namesWithReservedCharactersReachTheRightConnector() throws Exception {
		String token = unique("semi");
		String topic = token + "-in";
		createTopic(topic);
		String base = "mcp " + token;
		String other = base + ";v2=1&x";
		assertThat(create(base, sinkConfig(topic, "/tmp/" + token + "-a.txt")).get("result").asText())
			.isEqualTo("created");
		// an unencoded ';' is a matrix parameter for Connect: the name would be cut to "base" and base would be
		// updated
		JsonNode created = create(other, sinkConfig(topic, "/tmp/" + token + "-b.txt"));
		assertThat(created.get("result").asText()).isEqualTo("created");
		assertThat(texts(rest.get(TestConfigs.CLUSTER, "/connectors"))).contains(base, other);
		assertThat(ok("connectors_get", args("name", other)).get("name").asText()).isEqualTo(other);
		assertThat(ok("connectors_status", args("name", other)).get("name").asText()).isEqualTo(other);

		JsonNode deleted = ok("connectors_delete", args("name", other));
		assertThat(deleted.get("httpStatus").asInt()).isEqualTo(204);
		JsonNode names = rest.get(TestConfigs.CLUSTER, "/connectors");
		assertThat(texts(names)).contains(base).doesNotContain(other);
		assertThat(rest.get(TestConfigs.CLUSTER, ConnectApi.path(base) + "/config").get("file").asText())
			.isEqualTo("/tmp/" + token + "-a.txt");
	}

	@Test
	void resumeSettlesForConnectorWithoutTasks() {
		String name = "mcp no tasks " + unique("hb");
		Map<String, Object> config = new LinkedHashMap<>();
		config.put("connector.class", "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector");
		config.put("source.cluster.alias", "src");
		config.put("target.cluster.alias", "dst");
		config.put("source.cluster.bootstrap.servers", KafkaTestCluster.INTERNAL_BOOTSTRAP);
		config.put("target.cluster.bootstrap.servers", KafkaTestCluster.INTERNAL_BOOTSTRAP);
		// heartbeats off → the connector creates no tasks (taskConfigs is empty)
		config.put("emit.heartbeats.enabled", "false");
		// RF=5 cannot be created on a single broker: keeps the "heartbeats" topic from being created on the shared
		// cluster (the error is only logged)
		config.put("heartbeats.topic.replication.factor", "5");
		JsonNode created = create(name, config);
		assertThat(created.get("result").asText()).isEqualTo("created");
		JsonNode running = await(name + " RUNNING without tasks", () -> restStatus(name),
				st -> "RUNNING".equals(state(st)) && st.path("tasks").isEmpty());
		assertThat(running.get("tasks")).isEmpty();
		assertNoFalseNote(created);

		assertThat(ok("connectors_pause", args("name", name)).get("httpStatus").asInt()).isEqualTo(202);
		awaitConnectorState(name, "PAUSED");
		JsonNode resumed = ok("connectors_resume", args("name", name));
		assertThat(resumed.get("httpStatus").asInt()).isEqualTo(202);
		assertNoFalseNote(resumed);
		if (resumed.get("settled").asBoolean()) {
			assertThat(resumed.at("/status/state").asText()).isEqualTo("RUNNING");
			assertThat(resumed.at("/status/tasks")).isEmpty();
		}
		awaitConnectorState(name, "RUNNING");
	}

	/** For a task-less RUNNING connector the tool must not add a false "not RUNNING"/"not settled" note. */
	private static void assertNoFalseNote(JsonNode result) {
		if ("RUNNING".equals(result.at("/status/state").asText()) && result.at("/status/tasks").isEmpty()
				&& !result.get("settled").asBoolean()) {
			// on a busy machine the 6 s wait may not cover the stability window of the task-less state; the note
			// must be correct
			assertThat(result.get("note").asText()).as(result.toString())
				.doesNotContain("not RUNNING")
				.contains("connectors_status");
		}
	}

	@Test
	void writeToolsAreRejectedOnReadOnlyCluster() {
		String name = "mcp ro " + unique("ro");
		for (String tool : new String[] { "connectors_pause", "connectors_resume", "connectors_stop",
				"connectors_restart", "connectors_delete", "connectors_offsets_reset" }) {
			assertThat(error(tool, args("cluster", "test-ro", "name", name))).as(tool).contains("readOnly");
		}
		assertThat(error("connectors_task_restart", args("cluster", "test-ro", "name", name, "taskId", 0)))
			.contains("readOnly");
		assertThat(error("connectors_create_or_update",
				args("cluster", "test-ro", "name", name, "config", sinkConfig("t", "/tmp/x.txt"))))
			.contains("readOnly");
		JsonNode names = rest.get(TestConfigs.CLUSTER, "/connectors");
		assertThat(texts(names)).doesNotContain(name);
	}

}
