package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.ConnectIntegrationTest;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;

/**
 * diagnose_connector (ve connector'ları gören diagnose_topic / diagnose_cluster): görevi FAILED olan sink
 * (yazılamayan /proc dosyası), sağlıklı sink'in consumer group lag'i, eksik topic ve kaynak connector yazma aktivitesi.
 */
class DiagnoseConnectorTest extends ConnectIntegrationTest {

	private static final String FILE_SINK = "org.apache.kafka.connect.file.FileStreamSinkConnector";

	private static final String FILE_SOURCE = "org.apache.kafka.connect.file.FileStreamSourceConnector";

	private static final String STRING_CONVERTER = "org.apache.kafka.connect.storage.StringConverter";

	private final List<String> connectors = new ArrayList<>();

	@AfterEach
	void deleteConnectors() {
		connectors.forEach(ConnectIntegrationTest::deleteConnectorQuietly);
		connectors.clear();
	}

	private void create(String name, Map<String, String> config) throws Exception {
		connectors.add(name);
		putConnector(name, config);
	}

	private static Map<String, String> sink(String topic, String file) {
		Map<String, String> c = new LinkedHashMap<>();
		c.put("connector.class", FILE_SINK);
		c.put("tasks.max", "1");
		c.put("topics", topic);
		c.put("file", file);
		c.put("key.converter", STRING_CONVERTER);
		c.put("value.converter", STRING_CONVERTER);
		return c;
	}

	private static void createTopic(String topic) throws Exception {
		testAdmin().createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
	}

	private static void produce(String topic, int count) throws Exception {
		try (KafkaProducer<byte[], byte[]> p = KafkaTestCluster.producer(null)) {
			for (int i = 0; i < count; i++) {
				p.send(new ProducerRecord<>(topic, ("line-" + i).getBytes(StandardCharsets.UTF_8)));
			}
			p.flush();
		}
	}

	private static <T> T await(String what, Supplier<T> supplier, Predicate<T> done) throws InterruptedException {
		long end = System.currentTimeMillis() + 90_000;
		T last = null;
		while (System.currentTimeMillis() < end) {
			try {
				last = supplier.get();
				if (done.test(last)) {
					return last;
				}
			}
			catch (RuntimeException e) {
				// tekrar dene
			}
			Thread.sleep(500);
		}
		throw new AssertionError("Beklenen durum oluşmadı: " + what + " (son: " + last + ")");
	}

	private static JsonNode status(String name) {
		try {
			return connectRest("GET", "/connectors/" + name + "/status", null);
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static String taskState(JsonNode status) {
		return status.path("tasks").path(0).path("state").asText();
	}

	@Test
	void failedSinkTaskIsCriticalWithTraceAndRestartHint() throws Exception {
		String token = unique("diagconn");
		String topic = token + "-in";
		String name = "diag-failing-" + token;
		createTopic(topic);
		produce(topic, 3);
		create(name, sink(topic, "/proc/kafka-mcp-cannot-write/" + token + ".txt"));
		await(name + " FAILED", () -> status(name), s -> "FAILED".equals(taskState(s)));

		JsonNode r = ok("diagnose_connector", args("name", name, "intervalSeconds", 1));
		assertThat(r.get("status").asText()).isEqualTo("critical");
		JsonNode failed = DiagnoseTestBase.finding(r, "TASKS_FAILED");
		assertThat(failed.get("message").asText()).contains("1/1");
		assertThat(failed.at("/evidence/tasks/0/traceSummary").asText()).contains("Couldn't find or create file");
		assertThat(failed.at("/evidence/tasks/0/traceSummary").asText().split("\n")).hasSizeLessThanOrEqualTo(3);
		assertThat(failed.at("/evidence/tasks/0/worker").asText()).isEqualTo("connect:8083");
		assertThat(failed.get("recommendation").asText()).contains("connectors_restart", "includeTasks=true",
				"onlyFailed=true", "connectors_task_restart");
		JsonNode facts = r.get("facts");
		assertThat(facts.get("type").asText()).isEqualTo("sink");
		assertThat(facts.get("class").asText()).isEqualTo(FILE_SINK);
		assertThat(facts.at("/config/topics").asText()).isEqualTo(topic);
		assertThat(facts.at("/topics/configured/0").asText()).isEqualTo(topic);
		assertThat(facts.at("/workers/tasksByWorker/connect:8083").asInt()).isEqualTo(1);
		assertThat(facts.at("/workers/failedTaskWorkers/0").asText()).isEqualTo("connect:8083");
		assertThat(facts.at("/sinkLag/group").asText()).isEqualTo("connect-" + name);
		assertThat(DiagnoseTestBase.codes(r)).doesNotContain("PLUGIN_MISSING", "CONVERTER_MISSING", "CONVERTER_DEFAULT",
				"TOPIC_MISSING");

		// topic ve cluster teşhisi de bu connector'ı görür
		JsonNode t = ok("diagnose_topic", args("topic", topic, "intervalSeconds", 1));
		assertThat(t.at("/facts/connectors/referencing/0/name").asText()).isEqualTo(name);
		assertThat(DiagnoseTestBase.finding(t, "CONNECTOR_FAILED").at("/evidence/connectors/0/name").asText())
			.isEqualTo(name);
		JsonNode c = ok("diagnose_cluster", args());
		assertThat(DiagnoseTestBase.finding(c, "CONNECTORS_FAILED").at("/evidence/connectors").findValuesAsText("name"))
			.contains(name);
	}

	@Test
	void healthySinkLagMissingTopicAndSourceActivity() throws Exception {
		String token = unique("diagconn");
		String topic = token + "-ok";
		String name = "diag-healthy-" + token;
		createTopic(topic);
		produce(topic, 5);
		Map<String, String> config = sink(topic, "/tmp/" + token + ".txt");
		config.remove("key.converter");
		create(name, config);
		await(name + " RUNNING", () -> status(name), s -> "RUNNING".equals(taskState(s)));
		await(name + " commit", () -> {
			try {
				return testAdmin().listConsumerGroupOffsets("connect-" + name)
					.partitionsToOffsetAndMetadata()
					.get(30, TimeUnit.SECONDS);
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}, m -> m.values().stream().anyMatch(o -> o != null && o.offset() == 5));

		JsonNode r = ok("diagnose_connector", args("name", name, "intervalSeconds", 1));
		assertThat(r.get("status").asText()).as(r.toString()).isNotEqualTo("critical");
		assertThat(r.at("/facts/state").asText()).isEqualTo("RUNNING");
		assertThat(r.at("/facts/sinkLag/group").asText()).isEqualTo("connect-" + name);
		assertThat(r.at("/facts/sinkLag/lag").asLong()).isZero();
		assertThat(DiagnoseTestBase.finding(r, "CONVERTER_DEFAULT").at("/evidence/keys/0").asText())
			.isEqualTo("key.converter");
		assertThat(DiagnoseTestBase.codes(r)).doesNotContain("TASKS_FAILED", "SINK_STUCK", "CONFIG_INVALID");

		// regex hiçbir topic'le eşleşmiyor (regex aboneliği topic oluşturmaz); DLQ'yu görev başlarken Connect oluşturur
		String regexName = "diag-regex-" + token;
		Map<String, String> regex = sink(topic, "/tmp/" + token + "-r.txt");
		regex.remove("topics");
		regex.put("topics.regex", token + "-nomatch-.*");
		regex.put("errors.tolerance", "all");
		regex.put("errors.deadletterqueue.topic.name", token + "-dlq");
		regex.put("errors.deadletterqueue.topic.replication.factor", "1");
		create(regexName, regex);
		await(regexName + " RUNNING", () -> status(regexName), st -> "RUNNING".equals(taskState(st)));
		JsonNode m = ok("diagnose_connector", args("name", regexName, "intervalSeconds", 1));
		assertThat(DiagnoseTestBase.finding(m, "TOPICS_REGEX_NO_MATCH").get("severity").asText()).isEqualTo("warning");
		assertThat(DiagnoseTestBase.codes(m)).doesNotContain("ERRORS_TOLERATED_WITHOUT_DLQ", "DLQ_TOPIC_MISSING");
		assertThat(m.at("/facts/topics/deadLetterQueue").asText()).isEqualTo(token + "-dlq");
		assertThat(m.at("/facts/topics/regex").asText()).isEqualTo(token + "-nomatch-.*");
		assertThat(m.at("/facts/topics/regexMatches").asInt()).isZero();

		String tolerantName = "diag-tolerant-" + token;
		Map<String, String> tolerant = sink(topic, "/tmp/" + token + "-t.txt");
		tolerant.put("errors.tolerance", "all");
		create(tolerantName, tolerant);
		await(tolerantName + " RUNNING", () -> status(tolerantName), st -> "RUNNING".equals(taskState(st)));
		JsonNode tol = ok("diagnose_connector", args("name", tolerantName, "intervalSeconds", 1));
		assertThat(DiagnoseTestBase.finding(tol, "ERRORS_TOLERATED_WITHOUT_DLQ").get("recommendation").asText())
			.contains("errors.deadletterqueue.topic.name");

		String sourceTopic = token + "-src";
		String source = "diag-source-" + token;
		Map<String, String> src = new LinkedHashMap<>();
		src.put("connector.class", FILE_SOURCE);
		src.put("tasks.max", "1");
		src.put("topic", sourceTopic);
		src.put("file", "/etc/passwd");
		create(source, src);
		await(source + " RUNNING", () -> status(source), st -> "RUNNING".equals(taskState(st)));
		await(source + " aktif topic", () -> {
			try {
				return connectRest("GET", "/connectors/" + source + "/topics", null);
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}, n -> n.path(source).path("topics").size() > 0);
		JsonNode s = ok("diagnose_connector", args("name", source, "intervalSeconds", 1));
		assertThat(s.at("/facts/type").asText()).isEqualTo("source");
		assertThat(s.at("/facts/sourceActivity/topics/0/topic").asText()).isEqualTo(sourceTopic);
		assertThat(s.at("/facts/sourceActivity/topics/0/lastWriteTime").asText()).isNotBlank();
		assertThat(s.at("/facts/topics/active/0").asText()).isEqualTo(sourceTopic);
		assertThat(s.at("/facts/sinkLag").isMissingNode()).isTrue();
	}

	/**
	 * connector.class ve converter Connect takma adlarıyla (FileStreamSink, StringConverter) verilse de plugin/converter
	 * eksik denmez; connector RUNNING değilken (PAUSED) de takma ad çözülür.
	 */
	@Test
	void aliasedConnectorClassAndConverterAreRecognized() throws Exception {
		String token = unique("diagconn");
		String topic = token + "-alias";
		String name = "diag-alias-" + token;
		createTopic(topic);
		Map<String, String> config = sink(topic, "/tmp/" + token + "-a.txt");
		config.put("connector.class", "FileStreamSink");
		config.put("value.converter", "StringConverter");
		create(name, config);
		await(name + " RUNNING", () -> status(name), s -> "RUNNING".equals(taskState(s)));

		JsonNode r = ok("diagnose_connector", args("name", name, "intervalSeconds", 1));
		assertThat(r.at("/facts/class").asText()).isEqualTo("FileStreamSink");
		assertThat(DiagnoseTestBase.codes(r)).as(r.toString()).doesNotContain("PLUGIN_MISSING", "CONVERTER_MISSING");

		connectRest("PUT", "/connectors/" + name + "/pause", null);
		await(name + " PAUSED", () -> status(name), s -> "PAUSED".equals(s.path("connector").path("state").asText()));
		JsonNode paused = ok("diagnose_connector", args("name", name, "intervalSeconds", 1));
		assertThat(DiagnoseTestBase.codes(paused)).as(paused.toString()).contains("CONNECTOR_PAUSED")
			.doesNotContain("PLUGIN_MISSING", "CONVERTER_MISSING");
	}

	@Test
	void unknownConnectorIsAnError() {
		assertThat(error("diagnose_connector", args("name", unique("diag-nope")))).contains("Connector bulunamadı");
	}

}
