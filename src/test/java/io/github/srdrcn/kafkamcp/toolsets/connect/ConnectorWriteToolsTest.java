package io.github.srdrcn.kafkamcp.toolsets.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.TestConfigs;

/**
 * Yazma araçları: create_or_update, pause/resume/stop, restart, offsets_reset (RUNNING'de reddedilir,
 * STOPPED'da çalışır), delete; test-ro'da yazma araçlarının reddi.
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
			.contains("config boş olamaz");

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
		await("tasks.max=2 sonrası görev 1", () -> restStatus(name), s -> task(s, 1).isPresent());
		JsonNode taskRestart = ok("connectors_task_restart", args("name", name, "taskId", 1));
		assertThat(taskRestart.get("httpStatus").asInt()).isEqualTo(204);
		assertThat(taskRestart.at("/task/id").asInt()).isEqualTo(1);
		await("görev 1 RUNNING", () -> restStatus(name),
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
		assertThat(stopped.get("settled").asBoolean()).as(stopped.toString()).isTrue();
		assertThat(stopped.at("/status/state").asText()).isEqualTo("STOPPED");

		JsonNode reset = ok("connectors_offsets_reset", args("name", name));
		assertThat(reset.get("reset").asBoolean()).isTrue();
		assertThat(reset.get("httpStatus").asInt()).isEqualTo(200);
		assertThat(reset.get("message").asText()).containsIgnoringCase("reset successfully");
		JsonNode after = ok("connectors_offsets", args("name", name));
		assertThat(after.get("total").asInt()).isZero();
		assertThat(rest.get(TestConfigs.CLUSTER, ConnectApi.path(name) + "/offsets").get("offsets")).isEmpty();
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
