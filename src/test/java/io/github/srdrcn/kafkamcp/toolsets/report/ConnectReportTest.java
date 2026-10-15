package io.github.srdrcn.kafkamcp.toolsets.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.ConnectIntegrationTest;

/** Cluster with Connect: connector counts in the summary, connectors referencing the topics in the team inventory. */
class ConnectReportTest extends ConnectIntegrationTest {

	static final String FILE_SINK = "org.apache.kafka.connect.file.FileStreamSinkConnector";

	@Test
	void summaryAndTeamInventoryIncludeConnectors() throws Exception {
		String token = unique("rep-conn");
		String prefix = token + ".";
		String topic = prefix + "orders";
		testAdmin().createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
		String healthy = token + "-sink";
		String failing = token + "-failing";
		String unrelated = token + "-other";
		try {
			putConnector(healthy, Map.of("connector.class", FILE_SINK, "tasks.max", "1", "topics", topic, "file",
					"/tmp/" + token + ".txt"));
			putConnector(failing, Map.of("connector.class", FILE_SINK, "tasks.max", "1", "topics.regex",
					Pattern.quote(prefix) + ".*", "file", "/proc/kafka-mcp-cannot-write/" + token + ".txt"));
			putConnector(unrelated, Map.of("connector.class", FILE_SINK, "tasks.max", "1", "topics",
					token + "-elsewhere", "file", "/tmp/" + token + "-other.txt"));
			awaitTaskState(failing, "FAILED");

			JsonNode s = ok("report_cluster_summary", args());
			assertThat(s.at("/connect/connectors").asInt()).isGreaterThanOrEqualTo(3);
			assertThat(s.at("/connect/failed").asInt()).isGreaterThanOrEqualTo(1);
			assertThat(texts(s.at("/connect/failedConnectors"))).contains(failing);

			JsonNode t = ok("report_team_inventory", args("prefix", prefix));
			JsonNode connectors = t.at("/connectors/connectors");
			assertThat(t.at("/connectors/connectorsScanned").asInt()).isGreaterThanOrEqualTo(3);
			assertThat(texts(connectors, "name")).containsExactlyInAnyOrder(healthy, failing);
			JsonNode h = row(connectors, healthy);
			assertThat(texts(h.get("topics"))).containsExactly(topic);
			assertThat(texts(h.get("configKeys"))).containsExactly("topics");
			assertThat(h.get("type").asText()).isEqualTo("sink");
			JsonNode f = row(connectors, failing);
			assertThat(f.get("failed").asBoolean()).isTrue();
			assertThat(texts(f.get("configKeys"))).containsExactly("topics.regex");
		}
		finally {
			deleteConnectorQuietly(healthy);
			deleteConnectorQuietly(failing);
			deleteConnectorQuietly(unrelated);
		}
	}

	private static void awaitTaskState(String name, String state) throws Exception {
		long deadline = System.currentTimeMillis() + 90_000;
		while (true) {
			try {
				JsonNode s = connectRest("GET", "/connectors/" + name + "/status", null);
				if (state.equals(s.at("/tasks/0/state").asText())) {
					return;
				}
			}
			catch (IllegalStateException e) {
				// not there yet
			}
			if (System.currentTimeMillis() > deadline) {
				throw new IllegalStateException("task did not reach " + state + ": " + name);
			}
			Thread.sleep(300);
		}
	}

	private static JsonNode row(JsonNode array, String name) {
		for (JsonNode n : array) {
			if (name.equals(n.path("name").asText())) {
				return n;
			}
		}
		throw new AssertionError("no row: " + name);
	}

	private static List<String> texts(JsonNode array) {
		List<String> out = new java.util.ArrayList<>();
		if (array != null) {
			array.forEach(n -> out.add(n.isObject() ? n.path("name").asText() : n.asText()));
		}
		return out;
	}

	private static List<String> texts(JsonNode array, String field) {
		List<String> out = new java.util.ArrayList<>();
		array.forEach(n -> out.add(n.path(field).asText()));
		return out;
	}

}
