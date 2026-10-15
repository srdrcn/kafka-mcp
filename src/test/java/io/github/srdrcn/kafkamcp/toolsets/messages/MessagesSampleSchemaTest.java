package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;

/** messages_sample_schema (json) and serdes_list (without Schema Registry). */
class MessagesSampleSchemaTest extends KafkaIntegrationTest {

	@Test
	void infersJsonStructureFromLatestRecords() throws Exception {
		String topic = unique("msg-schema");
		TestData.createTopic(testAdmin(), topic, 2);
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		// old records (must not be among the last maxRecords)
		for (int i = 0; i < 5; i++) {
			records.add(TestData.record(topic, i % 2, TestData.BASE_TS + i, null, "{\"legacy\":true}"));
		}
		for (int i = 0; i < 8; i++) {
			String email = i % 4 == 0 ? "null" : "\"u" + i + "@example.com\"";
			String extra = i % 2 == 0 ? ",\"items\":[{\"sku\":\"s" + i + "\",\"qty\":" + i + "}]" : "";
			records.add(TestData.record(topic, i % 2, TestData.BASE_TS + 1_000 + i, null,
					"{\"id\":" + i + ",\"user\":{\"email\":" + email + "}" + extra + "}"));
		}
		records.add(TestData.record(topic, 0, TestData.BASE_TS + 2_000, null, "broken {json"));
		records.add(TestData.record(topic, 1, TestData.BASE_TS + 2_001, "k", null));
		TestData.send(records);

		JsonNode r = ok("messages_sample_schema", args("topic", topic, "maxRecords", 10));
		assertThat(r.get("format").asText()).isEqualTo("json");
		assertThat(r.get("recordsRead").asInt()).isEqualTo(10);
		assertThat(r.get("parsed").asInt()).isEqualTo(8);
		assertThat(r.get("failed").asInt()).isEqualTo(1);
		assertThat(r.get("tombstones").asInt()).isEqualTo(1);
		assertThat(r.get("errors").get(0).get("error").asText()).startsWith("Not JSON");
		Map<String, JsonNode> paths = new HashMap<>();
		r.get("paths").forEach(p -> paths.put(p.get("path").asText(), p));
		assertThat(paths).containsKeys("id", "user", "user.email", "items", "items[]", "items[].sku", "items[].qty")
			.doesNotContainKey("legacy");
		assertThat(paths.get("id").get("types").get("integer").asInt()).isEqualTo(8);
		assertThat(paths.get("user.email").get("types").get("null").asInt()).isEqualTo(2);
		assertThat(paths.get("user.email").get("nullRatio").asDouble()).isEqualTo(0.25);
		assertThat(paths.get("user.email").get("example").asText()).endsWith("@example.com");
		assertThat(paths.get("items").get("missingRatio").asDouble()).isEqualTo(0.5);

		assertThat(error("messages_sample_schema", args("topic", topic, "format", "avro"))).contains("schemaRegistryUrl");
		assertThat(error("messages_sample_schema", args("topic", topic, "format", "hex"))).contains("json or avro");
	}

	@Test
	void serdesListShowsFormatsAndAvroAvailability() {
		JsonNode r = ok("serdes_list", Map.of());
		List<String> names = new ArrayList<>();
		r.get("formats").forEach(f -> names.add(f.get("name").asText()));
		assertThat(names).containsExactly("string", "json", "hex", "long", "avro");
		assertThat(r.get("formats").get(4).get("requiresSchemaRegistry").asBoolean()).isTrue();
		assertThat(r.get("clusters")).hasSize(2);
		assertThat(r.get("clusters").get(0).get("avro").asBoolean()).isFalse();

		JsonNode one = ok("serdes_list", args());
		assertThat(one.get("clusters")).hasSize(1);
		assertThat(one.get("clusters").get(0).get("name").asText()).isEqualTo("test");
	}

}
