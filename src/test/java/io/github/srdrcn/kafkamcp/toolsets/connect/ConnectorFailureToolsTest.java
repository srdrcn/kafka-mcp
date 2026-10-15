package io.github.srdrcn.kafkamcp.toolsets.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Connector with a FAILED task (FileStreamSink writing to a file it cannot write): connectors_failed, the
 * connectors_status trace summary, connectors_restart (onlyFailed), connectors_task_restart, and create_or_update
 * not returning the old FAILED status as the result once the config is fixed.
 */
class ConnectorFailureToolsTest extends ConnectToolTestBase {

	@Test
	void failedConnectorIsReportedAndRestartable() throws Exception {
		String token = unique("fail");
		String topic = token + "-in";
		String name = "mcp failing " + token;
		createTopic(topic);
		create(name, sinkConfig(topic, "/proc/kafka-mcp-cannot-write/" + token + ".txt"));
		awaitTaskState(name, "FAILED");

		JsonNode failed = ok("connectors_failed", args("filter", token));
		assertThat(failed.get("checked").asInt()).isGreaterThanOrEqualTo(1);
		assertThat(failed.get("total").asInt()).isEqualTo(1);
		JsonNode row = failed.at("/connectors/0");
		assertThat(row.get("name").asText()).isEqualTo(name);
		assertThat(row.get("state").asText()).isEqualTo("RUNNING");
		assertThat(texts(row.get("failedTaskIds"))).containsExactly("0");
		assertThat(texts(row.get("failedTaskWorkers"))).containsExactly("connect:8083");
		assertThat(row.get("worker").asText()).isEqualTo("connect:8083");
		String trace = row.get("traceSummary").asText();
		assertThat(trace).contains("ConnectException").contains("Couldn't find or create file");
		assertThat(trace.split("\n")).hasSizeLessThanOrEqualTo(3);

		JsonNode status = ok("connectors_status", args("name", name));
		assertThat(status.get("failedTasks").asInt()).isEqualTo(1);
		assertThat(status.at("/tasks/0/state").asText()).isEqualTo("FAILED");
		assertThat(status.at("/tasks/0/traceSummary").asText().split("\n")).hasSize(3);
		assertThat(status.at("/tasks/0/traceSummary").asText()).startsWith(
				"org.apache.kafka.connect.errors.ConnectException");
		assertThat(status.toString()).doesNotContain("Caused by");

		JsonNode listRow = ok("connectors_list", args("filter", token)).at("/connectors/0");
		assertThat(listRow.get("failedTasks").asInt()).isEqualTo(1);

		JsonNode dist = ok("connectors_task_distribution", args());
		assertThat(dist.at("/workers/0/tasksNotRunning").asInt()).isGreaterThanOrEqualTo(1);
		assertThat(texts(dist.at("/workers/0/notRunningTasks"))).contains(name + "/0 (FAILED)");

		// restart includeTasks+onlyFailed: 202 and only the FAILED task is restarted
		JsonNode restart = ok("connectors_restart",
				args("name", name, "includeTasks", true, "onlyFailed", true));
		assertThat(restart.get("httpStatus").asInt()).isEqualTo(202);
		assertThat(restart.at("/restarting/connector").asBoolean()).isFalse();
		assertThat(texts(restart.at("/restarting/tasks"))).containsExactly("0");
		awaitTaskState(name, "FAILED");

		// single task restart: the file still cannot be written, the task becomes FAILED again
		JsonNode taskRestart = ok("connectors_task_restart", args("name", name, "taskId", 0));
		assertThat(taskRestart.get("httpStatus").asInt()).isEqualTo(204);
		assertThat(taskRestart.get("taskId").asInt()).isZero();
		awaitTaskState(name, "FAILED");
		assertThat(error("connectors_task_restart", args("name", name, "taskId", 7))).contains("404");

		// a healthy connector does not show up in connectors_failed
		String healthy = "mcp healthy " + token;
		create(healthy, sinkConfig(topic, "/tmp/" + token + ".txt"));
		awaitTaskState(healthy, "RUNNING");
		assertThat(ok("connectors_failed", args("filter", token)).get("connectors").findValuesAsText("name"))
			.containsExactly(name);
		assertThat(ok("connectors_failed", args("filter", "healthy " + token)).get("total").asInt()).isZero();
		assertThat(ok("connectors_failed", Map.of("cluster", "test-ro", "filter", token)).get("total").asInt())
			.isEqualTo(1);

		// the config is fixed: after PUT 200 Connect restarts the task later; the tool must not return the
		// pre-update FAILED status as "settled"
		JsonNode fixed = ok("connectors_create_or_update",
				args("name", name, "config", sinkConfig(topic, "/tmp/" + token + "-fixed.txt")));
		assertThat(fixed.get("result").asText()).isEqualTo("updated");
		if (fixed.get("settled").asBoolean()) {
			assertThat(fixed.at("/status/tasks/0/state").asText()).as(fixed.toString()).isEqualTo("RUNNING");
		}
		else {
			assertThat(fixed.get("note").asText()).as(fixed.toString()).contains("connectors_status");
		}
		awaitTaskState(name, "RUNNING");
		assertThat(ok("connectors_failed", args("filter", token)).get("total").asInt()).isZero();
	}

}
