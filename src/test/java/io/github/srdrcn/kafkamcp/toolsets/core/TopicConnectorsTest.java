package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.support.ConnectIntegrationTest;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;

class TopicConnectorsTest extends ConnectIntegrationTest {

	private static final String SINK = "org.apache.kafka.connect.file.FileStreamSinkConnector";

	private static final String SOURCE = "org.apache.kafka.connect.file.FileStreamSourceConnector";

	@Test
	void findsConnectorsByConfigAndActiveTopics() throws Exception {
		String topic = unique("conn");
		TestData.createTopic(testAdmin(), topic, 1, Map.of());
		String direct = unique("sink-direct");
		String regex = unique("sink-regex");
		String unrelated = unique("source-other");
		try {
			putConnector(direct, Map.of("connector.class", SINK, "tasks.max", "1", "topics", "x-unrelated," + topic,
					"file", "/tmp/" + direct + ".txt", "errors.tolerance", "all"));
			putConnector(regex, Map.of("connector.class", SINK, "tasks.max", "1", "topics.regex", "conn-.*", "file",
					"/tmp/" + regex + ".txt"));
			putConnector(unrelated, Map.of("connector.class", SOURCE, "tasks.max", "1", "topic", unique("elsewhere"),
					"file", "/tmp/" + unrelated + ".txt"));
			try (KafkaProducer<byte[], byte[]> p = KafkaTestCluster.producer(null)) {
				p.send(new ProducerRecord<>(topic, "\"hello\"".getBytes(StandardCharsets.UTF_8))).get();
			}
			awaitActiveTopic(direct, topic);

			JsonNode r = ok("topics_connectors", args("topic", topic));
			assertThat(r.get("topic").asText()).isEqualTo(topic);
			assertThat(r.get("connectorsScanned").asInt()).isGreaterThanOrEqualTo(3);
			Map<String, JsonNode> byName = new java.util.HashMap<>();
			r.get("connectors").forEach(c -> byName.put(c.get("name").asText(), c));
			assertThat(byName).containsKeys(direct, regex).doesNotContainKey(unrelated);
			JsonNode d = byName.get(direct);
			assertThat(d.get("type").asText()).isEqualTo("sink");
			assertThat(d.get("state").asText()).isNotBlank();
			assertThat(strings(d.get("matchedBy"))).contains("config:topics", "activeTopics");
			assertThat(strings(byName.get(regex).get("matchedBy"))).contains("config:topics.regex");

			JsonNode none = ok("topics_connectors", args("topic", unique("unused")));
			assertThat(none.get("connectors")).isEmpty();
		}
		finally {
			deleteConnectorQuietly(direct);
			deleteConnectorQuietly(regex);
			deleteConnectorQuietly(unrelated);
		}
	}

	@Test
	void configMatchingHandlesListsRegexAndPrefix() throws Exception {
		ObjectMapper m = new ObjectMapper();
		JsonNode cfg = m.readTree("""
				{"topics":"a, orders ,b","topics.regex":"ord.*","topic.prefix":"or",
				 "errors.deadletterqueue.topic.name":"orders","name":"orders","topic.creation.default.partitions":"3"}
				""");
		assertThat(TopicClientTools.configMatches(cfg, "orders")).containsExactly("config:topics",
				"config:topics.regex", "config:topic.prefix", "config:errors.deadletterqueue.topic.name");
		assertThat(TopicClientTools.configMatches(cfg, "payments")).isEmpty();
	}

	private static void awaitActiveTopic(String connector, String topic) throws Exception {
		long end = System.currentTimeMillis() + 60_000;
		while (System.currentTimeMillis() < end) {
			try {
				JsonNode active = connectRest("GET", "/connectors/" + connector + "/topics", null);
				if (strings(active.path(connector).path("topics")).contains(topic)) {
					return;
				}
			}
			catch (IllegalStateException ignored) {
				// connector not ready yet
			}
			Thread.sleep(500);
		}
		throw new AssertionError("Connector did not report the active topic: " + connector);
	}

	private static List<String> strings(JsonNode array) {
		List<String> out = new ArrayList<>();
		array.forEach(n -> out.add(n.asText()));
		return out;
	}

}
