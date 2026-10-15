package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.support.ConnectIntegrationTest;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.support.TestConfigs;

/** Shared helpers for the connect tool tests: topic/connector setup, REST checks, bounded waiting. */
abstract class ConnectToolTestBase extends ConnectIntegrationTest {

	static final Duration WAIT = Duration.ofSeconds(90);

	static final String FILE_SINK = "org.apache.kafka.connect.file.FileStreamSinkConnector";

	static final String FILE_SOURCE = "org.apache.kafka.connect.file.FileStreamSourceConnector";

	static final String STRING_CONVERTER = "org.apache.kafka.connect.storage.StringConverter";

	@Autowired
	protected ConnectClient rest;

	private final List<String> created = new ArrayList<>();

	@AfterEach
	void deleteCreatedConnectors() {
		for (String name : created) {
			try {
				rest.delete(TestConfigs.CLUSTER, ConnectApi.path(name));
			}
			catch (RuntimeException ignored) {
				// the test may have deleted it already
			}
		}
		created.clear();
	}

	/** Registers the name as a connector to delete at the end of the test. */
	protected String track(String name) {
		created.add(name);
		return name;
	}

	protected static void createTopic(String topic) throws Exception {
		testAdmin().createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
	}

	protected static void produce(String topic, int count) throws Exception {
		try (KafkaProducer<byte[], byte[]> p = KafkaTestCluster.producer(null)) {
			for (int i = 0; i < count; i++) {
				p.send(new ProducerRecord<>(topic, ("{\"n\":" + i + "}").getBytes(StandardCharsets.UTF_8)));
			}
			p.flush();
		}
	}

	/** FileStreamSink config that reads the given topic and writes it to a file in the connect container. */
	protected static Map<String, Object> sinkConfig(String topic, String file) {
		Map<String, Object> c = new LinkedHashMap<>();
		c.put("connector.class", FILE_SINK);
		c.put("tasks.max", "1");
		c.put("topics", topic);
		c.put("file", file);
		c.put("key.converter", STRING_CONVERTER);
		c.put("value.converter", STRING_CONVERTER);
		return c;
	}

	/** FileStreamSource config that writes the container's /etc/passwd to the topic (its offset appears right away). */
	protected static Map<String, Object> sourceConfig(String topic) {
		Map<String, Object> c = new LinkedHashMap<>();
		c.put("connector.class", FILE_SOURCE);
		c.put("tasks.max", "1");
		c.put("topic", topic);
		c.put("file", "/etc/passwd");
		return c;
	}

	/** Creates the connector with the connectors_create_or_update tool and registers it for cleanup. */
	protected JsonNode create(String name, Map<String, Object> config) {
		track(name);
		return ok("connectors_create_or_update", args("name", name, "config", config));
	}

	/** Connector status via REST ({@code GET /connectors/{name}/status}). */
	protected JsonNode restStatus(String name) {
		return rest.get(TestConfigs.CLUSTER, ConnectApi.path(name) + "/status");
	}

	protected static String state(JsonNode status) {
		return status.path("connector").path("state").asText();
	}

	protected static Optional<JsonNode> task(JsonNode status, int id) {
		for (JsonNode t : status.path("tasks")) {
			if (t.path("id").asInt() == id) {
				return Optional.of(t);
			}
		}
		return Optional.empty();
	}

	/** Waits until task 0 of the connector is in the given state. */
	protected JsonNode awaitTaskState(String name, String taskState) {
		return await(name + " task 0 " + taskState, () -> restStatus(name),
				s -> task(s, 0).map(t -> taskState.equals(t.path("state").asText())).orElse(false));
	}

	protected JsonNode awaitConnectorState(String name, String connectorState) {
		return await(name + " " + connectorState, () -> restStatus(name), s -> connectorState.equals(state(s)));
	}

	/** Polls until the condition holds (at most {@link #WAIT}); errors/AssertionErrors are retried. */
	protected static <T> T await(String what, Supplier<T> supplier, Predicate<T> done) {
		long end = System.nanoTime() + WAIT.toNanos();
		T last = null;
		Throwable lastError = null;
		while (System.nanoTime() < end) {
			try {
				last = supplier.get();
				lastError = null;
				if (done.test(last)) {
					return last;
				}
			}
			catch (RuntimeException | AssertionError e) {
				lastError = e;
			}
			try {
				Thread.sleep(500);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new AssertionError("interrupted", e);
			}
		}
		throw new AssertionError("Expected state not reached: " + what + " (last: " + last + ")", lastError);
	}

	protected static List<String> texts(JsonNode array) {
		List<String> out = new ArrayList<>();
		array.forEach(n -> out.add(n.asText()));
		return out;
	}

}
