package io.github.srdrcn.kafkamcp.toolsets.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.ConnectTestCluster;

/**
 * Read tools: connect_clusters_list, connectors_list/get/status/topics/offsets, connectors_task_distribution,
 * connectors_config_diff (with a sink that has spaces in its name and secret configs).
 */
class ConnectorReadToolsTest extends ConnectToolTestBase {

	@Test
	void connectClustersList() {
		JsonNode all = ok("connect_clusters_list", Map.of());
		assertThat(all.get("total").asInt()).isEqualTo(2);
		for (JsonNode c : all.get("clusters")) {
			assertThat(c.get("reachable").asBoolean()).as(c.toString()).isTrue();
			assertThat(c.get("version").asText()).isEqualTo("4.1.1");
			assertThat(c.get("commit").asText()).isNotBlank();
			assertThat(c.get("kafkaClusterId").asText()).isNotBlank();
			assertThat(c.get("connectorCount").asInt()).isGreaterThanOrEqualTo(0);
			assertThat(c.get("connectUrl").asText()).isEqualTo(ConnectTestCluster.url());
		}
		assertThat(all.get("clusters").findValuesAsText("name")).containsExactly("test", "test-ro");

		JsonNode one = ok("connect_clusters_list", args());
		assertThat(one.get("total").asInt()).isEqualTo(1);
		assertThat(one.at("/clusters/0/name").asText()).isEqualTo("test");
	}

	@Test
	void sinkConnectorReadTools() throws Exception {
		String token = unique("rd");
		String topic = token + "-in";
		String name = "mcp sink " + token;
		String other = "mcp other " + token;
		createTopic(topic);
		produce(topic, 5);
		Map<String, Object> config = sinkConfig(topic, "/tmp/" + token + ".txt");
		config.put("database.password", "s3cret-pw");
		config.put("api.key", "abc-key");
		config.put("key.converter.schemas.enable", "false");
		JsonNode created = create(name, config);
		assertThat(created.get("result").asText()).isEqualTo("created");
		awaitTaskState(name, "RUNNING");

		// connectors_list: filter + state filter + short class name
		JsonNode list = ok("connectors_list", args("filter", token));
		assertThat(list.get("total").asInt()).isEqualTo(1);
		JsonNode row = list.at("/connectors/0");
		assertThat(row.get("name").asText()).isEqualTo(name);
		assertThat(row.get("type").asText()).isEqualTo("sink");
		assertThat(row.get("class").asText()).isEqualTo("FileStreamSinkConnector");
		assertThat(row.get("state").asText()).isEqualTo("RUNNING");
		assertThat(row.get("worker").asText()).isEqualTo("connect:8083");
		assertThat(row.get("tasks").asInt()).isEqualTo(1);
		assertThat(row.get("failedTasks").asInt()).isZero();
		assertThat(list.at("/states/RUNNING").asInt()).isEqualTo(1);
		assertThat(ok("connectors_list", args("filter", token, "state", "PAUSED")).get("total").asInt()).isZero();
		assertThat(ok("connectors_list", args("filter", token, "state", "RUNNING")).get("total").asInt()).isEqualTo(1);
		assertThat(error("connectors_list", args("state", "BOGUS"))).contains("Invalid state");
		// read tools work on a readOnly cluster
		assertThat(ok("connectors_list", args("cluster", "test-ro", "filter", token)).get("total").asInt())
			.isEqualTo(1);

		// connectors_get: masked config, key.converter visible, tasks
		JsonNode get = ok("connectors_get", args("name", name));
		assertThat(get.get("name").asText()).isEqualTo(name);
		assertThat(get.get("type").asText()).isEqualTo("sink");
		assertThat(get.get("class").asText()).isEqualTo(FILE_SINK);
		assertThat(get.at("/config/database.password").asText()).isEqualTo(ConnectSecrets.MASK);
		assertThat(get.at("/config/api.key").asText()).isEqualTo(ConnectSecrets.MASK);
		assertThat(get.at("/config/key.converter").asText()).isEqualTo(STRING_CONVERTER);
		assertThat(get.at("/config/key.converter.schemas.enable").asText()).isEqualTo("false");
		assertThat(get.at("/config/topics").asText()).isEqualTo(topic);
		assertThat(get.toString()).doesNotContain("s3cret-pw").doesNotContain("abc-key");
		assertThat(get.at("/status/state").asText()).isEqualTo("RUNNING");
		assertThat(get.at("/status/runningTasks").asInt()).isEqualTo(1);
		assertThat(get.get("tasks")).hasSize(1);
		assertThat(get.at("/tasks/0/id").asInt()).isZero();
		assertThat(get.at("/tasks/0/state").asText()).isEqualTo("RUNNING");
		assertThat(get.at("/tasks/0").has("config")).isFalse();
		JsonNode withTasks = ok("connectors_get", args("name", name, "includeTaskConfigs", true));
		assertThat(withTasks.at("/tasks/0/config/task.class").asText()).endsWith("FileStreamSinkTask");
		assertThat(withTasks.at("/tasks/0/config/database.password").asText()).isEqualTo(ConnectSecrets.MASK);
		assertThat(withTasks.toString()).doesNotContain("s3cret-pw");

		// connectors_status
		JsonNode status = ok("connectors_status", args("name", name));
		assertThat(status.get("state").asText()).isEqualTo("RUNNING");
		assertThat(status.get("worker").asText()).isEqualTo("connect:8083");
		assertThat(status.at("/tasks/0/state").asText()).isEqualTo("RUNNING");
		assertThat(status.at("/tasks/0/worker").asText()).isEqualTo("connect:8083");
		assertThat(status.get("failedTasks").asInt()).isZero();

		// connectors_topics: active topic after the records are processed
		JsonNode topics = await("active topic", () -> ok("connectors_topics", args("name", name)),
				t -> t.get("count").asInt() == 1);
		assertThat(texts(topics.get("topics"))).containsExactly(topic);

		// connectors_offsets: sink topic/partition/offset
		JsonNode offsets = await("sink offset", () -> ok("connectors_offsets", args("name", name)),
				o -> o.get("total").asInt() == 1 && o.at("/offsets/0/offset").asLong() == 5);
		assertThat(offsets.get("kind").asText()).isEqualTo("sink");
		assertThat(offsets.at("/offsets/0/topic").asText()).isEqualTo(topic);
		assertThat(offsets.at("/offsets/0/partition").asInt()).isZero();
		assertThat(ok("connectors_offsets", args("name", name, "filter", "^nomatch$")).get("total").asInt()).isZero();

		// connectors_task_distribution
		JsonNode dist = ok("connectors_task_distribution", args());
		assertThat(dist.get("workerCount").asInt()).isEqualTo(1);
		assertThat(dist.at("/workers/0/worker").asText()).isEqualTo("connect:8083");
		assertThat(dist.at("/workers/0/tasks").asInt()).isGreaterThanOrEqualTo(1);
		assertThat(dist.at("/workers/0/connectors").asInt()).isGreaterThanOrEqualTo(1);
		assertThat(dist.get("taskImbalance").asInt()).isZero();
		assertThat(dist.get("tasks").asInt()).isEqualTo(dist.at("/workers/0/tasks").asInt());

		// connectors_config_diff: against a given config
		Map<String, Object> desired = sinkConfig(topic + "-v2", "/tmp/" + token + ".txt");
		desired.put("database.password", "changed-pw");
		desired.put("tasks.max", "1");
		desired.put("errors.tolerance", "all");
		JsonNode diff = ok("connectors_config_diff", args("name", name, "config", desired));
		assertThat(diff.get("target").asText()).isEqualTo("config");
		assertThat(diff.get("identical").asBoolean()).isFalse();
		assertThat(diff.at("/added/errors.tolerance").asText()).isEqualTo("all");
		assertThat(diff.at("/removed/api.key").asText()).isEqualTo(ConnectSecrets.MASK);
		assertThat(diff.at("/removed/key.converter.schemas.enable").asText()).isEqualTo("false");
		assertThat(diff.get("removed").has("name")).isFalse();
		assertThat(diff.get("changed")).hasSize(2);
		assertThat(diff.at("/changed/0/key").asText()).isEqualTo("database.password");
		assertThat(diff.at("/changed/0/current").asText()).isEqualTo(ConnectSecrets.MASK);
		assertThat(diff.at("/changed/1/key").asText()).isEqualTo("topics");
		assertThat(diff.at("/changed/1/target").asText()).isEqualTo(topic + "-v2");
		assertThat(diff.toString()).doesNotContain("changed-pw").doesNotContain("s3cret-pw");

		// connectors_config_diff: against another connector
		Map<String, Object> otherConfig = new java.util.LinkedHashMap<>(config);
		otherConfig.put("tasks.max", "2");
		create(other, otherConfig);
		JsonNode diff2 = ok("connectors_config_diff", args("name", name, "otherName", other));
		assertThat(diff2.get("target").asText()).isEqualTo("connector:" + other);
		assertThat(diff2.get("changedCount").asInt()).isEqualTo(1);
		assertThat(diff2.at("/changed/0/key").asText()).isEqualTo("tasks.max");
		assertThat(diff2.get("addedCount").asInt()).isZero();
		assertThat(diff2.get("removedCount").asInt()).isZero();
		assertThat(error("connectors_config_diff", args("name", name))).contains("Exactly one of");
		assertThat(error("connectors_config_diff", args("name", name, "otherName", other, "config", desired)))
			.contains("Exactly one of");
	}

	@Test
	void unknownConnectorIsReported() {
		String msg = error("connectors_status", args("name", "missing " + unique("x")));
		assertThat(msg).contains("404");
		assertThat(error("connectors_get", args("name", "missing " + unique("x")))).contains("404");
	}

}
