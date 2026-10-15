package io.github.srdrcn.kafkamcp.toolsets.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

/** connect_plugins_list and connect_plugins_validate. */
class ConnectPluginToolsTest extends ConnectToolTestBase {

	@Test
	void pluginsList() {
		JsonNode r = ok("connect_plugins_list", args());
		assertThat(r.get("plugins").findValuesAsText("class")).contains(FILE_SINK, FILE_SOURCE,
				"org.apache.kafka.connect.mirror.MirrorSourceConnector");
		assertThat(r.get("plugins").findValuesAsText("type")).containsOnly("sink", "source");
		JsonNode sink = ok("connect_plugins_list", args("filter", "FileStreamSink"));
		assertThat(sink.get("total").asInt()).isEqualTo(1);
		assertThat(sink.at("/plugins/0/type").asText()).isEqualTo("sink");
		assertThat(sink.at("/plugins/0/version").asText()).isEqualTo("4.1.1");

		JsonNode all = ok("connect_plugins_list", args("connectorsOnly", false));
		assertThat(all.get("total").asInt()).isGreaterThan(r.get("total").asInt());
		assertThat(all.get("plugins").findValuesAsText("type")).contains("transformation", "converter");
		JsonNode limited = ok("connect_plugins_list", args("connectorsOnly", false, "limit", 2));
		assertThat(limited.get("returned").asInt()).isEqualTo(2);
		assertThat(limited.get("truncated").asBoolean()).isTrue();
	}

	@Test
	void validateReportsOnlyErroneousConfigs() {
		// topics missing: topics and topics.regex are invalid; connector.class and name are added automatically
		JsonNode bad = ok("connect_plugins_validate", args("plugin", "FileStreamSinkConnector", "config",
				Map.of("file", "/tmp/x.txt", "database.password", "pw-123", "tasks.max", "1")));
		assertThat(bad.get("plugin").asText()).isEqualTo(FILE_SINK);
		assertThat(bad.get("valid").asBoolean()).isFalse();
		assertThat(bad.get("errorCount").asInt()).isGreaterThanOrEqualTo(1);
		assertThat(bad.get("errors").findValuesAsText("name")).contains("topics").doesNotContain("file", "name");
		for (JsonNode e : bad.get("errors")) {
			assertThat(e.get("errors")).isNotEmpty();
		}
		assertThat(texts(bad.get("addedKeys"))).containsExactlyInAnyOrder("connector.class", "name");
		assertThat(bad.toString()).doesNotContain("pw-123");

		// invalid tasks.max: value + error message; the value of the secret key is masked
		JsonNode badValue = ok("connect_plugins_validate",
				args("plugin", "filestreamsink", "config", Map.of("connector.class", FILE_SINK, "topics", "t",
						"tasks.max", "abc", "name", "v1")));
		JsonNode tasksMax = null;
		for (JsonNode e : badValue.get("errors")) {
			if ("tasks.max".equals(e.get("name").asText())) {
				tasksMax = e;
			}
		}
		assertThat(tasksMax).as(badValue.toString()).isNotNull();
		assertThat(tasksMax.get("value").asText()).isEqualTo("abc");
		assertThat(badValue.has("addedKeys")).isFalse();

		JsonNode good = ok("connect_plugins_validate",
				args("plugin", FILE_SINK, "config", Map.of("topics", "t", "file", "/tmp/y.txt")));
		assertThat(good.get("valid").asBoolean()).as(good.toString()).isTrue();
		assertThat(good.get("errorCount").asInt()).isZero();
		assertThat(good.get("errors")).isEmpty();

		assertThat(error("connect_plugins_validate", args("plugin", "NoSuchConnector", "config", Map.of())))
			.contains("not found");
		assertThat(error("connect_plugins_validate",
				args("plugin", "FileStreamSinkConnector", "config", Map.of("connector.class", FILE_SOURCE))))
			.contains("does not match");
	}

}
