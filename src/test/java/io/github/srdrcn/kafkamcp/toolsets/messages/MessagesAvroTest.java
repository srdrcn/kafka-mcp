package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.support.SchemaRegistryIntegrationTest;
import io.github.srdrcn.kafkamcp.support.SchemaRegistryTestCluster;

/** format=avro: Confluent wire format records are decoded with the writer schema from Schema Registry. */
class MessagesAvroTest extends SchemaRegistryIntegrationTest {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static final String SCHEMA = """
			{"type":"record","name":"Order","namespace":"kafkamcp.test","fields":[
			  {"name":"id","type":"long"},
			  {"name":"customer","type":"string"},
			  {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
			  {"name":"note","type":["null","string"],"default":null},
			  {"name":"createdAt","type":{"type":"long","logicalType":"timestamp-millis"}},
			  {"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW","PAID"]}},
			  {"name":"tags","type":{"type":"array","items":"string"}}
			]}""";

	private static String topic;

	private static int schemaId;

	@BeforeAll
	static void produce() throws Exception {
		topic = unique("msg-avro");
		TestData.createTopic(testAdmin(), topic, 1);
		schemaId = register(topic + "-value", SCHEMA);
		Schema schema = new Schema.Parser().parse(SCHEMA);
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			GenericRecord r = new GenericData.Record(schema);
			r.put("id", (long) i);
			r.put("customer", "customer-€" + i);
			r.put("amount", ByteBuffer.wrap(new BigDecimal("12.3" + i).unscaledValue().toByteArray()));
			r.put("note", i % 2 == 0 ? null : "note-" + i);
			r.put("createdAt", 1_700_000_000_000L + i);
			r.put("status", new GenericData.EnumSymbol(schema.getField("status").schema(), i < 2 ? "NEW" : "PAID"));
			r.put("tags", List.of("t" + i));
			records.add(new ProducerRecord<>(topic, 0, null, TestData.utf8("order-" + i), wire(schemaId, schema, r)));
		}
		records.add(new ProducerRecord<>(topic, 0, null, null, TestData.utf8("plain text")));
		records.add(new ProducerRecord<>(topic, 0, null, null, new byte[] { 0, 0, 0x0f, 0x42, 0x3f, 2 }));
		TestData.send(records);
	}

	private static int register(String subject, String schema) throws Exception {
		String body = MAPPER.writeValueAsString(Map.of("schema", schema));
		HttpRequest req = HttpRequest
			.newBuilder(URI.create(SchemaRegistryTestCluster.url() + "/subjects/" + subject + "/versions"))
			.header("Content-Type", "application/vnd.schemaregistry.v1+json")
			.POST(HttpRequest.BodyPublishers.ofString(body))
			.build();
		HttpResponse<String> resp = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
		assertThat(resp.statusCode()).as(resp.body()).isEqualTo(200);
		return MAPPER.readTree(resp.body()).get("id").asInt();
	}

	private static byte[] wire(int id, Schema schema, GenericRecord record) throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(0);
		out.write(ByteBuffer.allocate(4).putInt(id).array());
		BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
		new GenericDatumWriter<GenericRecord>(schema).write(record, enc);
		enc.flush();
		return out.toByteArray();
	}

	@Test
	void consumeDecodesAvroWithRegistrySchema() {
		JsonNode r = ok("messages_consume", args("topic", topic, "from", "earliest", "format", "avro"));
		assertThat(r.get("returned").asInt()).isEqualTo(6);
		JsonNode first = r.get("records").get(0);
		assertThat(first.get("key").asText()).isEqualTo("order-0");
		assertThat(first.get("valueSchemaId").asInt()).isEqualTo(schemaId);
		JsonNode v = first.get("value");
		assertThat(v.get("id").asLong()).isZero();
		assertThat(v.get("customer").asText()).isEqualTo("customer-€0");
		assertThat(v.get("amount").decimalValue()).isEqualByComparingTo("12.30");
		assertThat(v.has("note")).isTrue();
		assertThat(v.get("note").isNull()).isTrue();
		assertThat(v.get("createdAt").asText()).isEqualTo(Instant.ofEpochMilli(1_700_000_000_000L).toString());
		assertThat(v.get("status").asText()).isEqualTo("NEW");
		assertThat(v.get("tags").get(0).asText()).isEqualTo("t0");
		assertThat(r.get("records").get(1).get("value").get("note").asText()).isEqualTo("note-1");

		JsonNode plain = r.get("records").get(4);
		assertThat(plain.get("valueError").asText()).contains("wire format");
		assertThat(plain.get("value").asText()).isEqualTo(RecordDecoder.hex(TestData.utf8("plain text")));
		JsonNode unknown = r.get("records").get(5);
		assertThat(unknown.get("valueSchemaId").asInt()).isEqualTo(999_999);
		assertThat(unknown.get("valueError").asText()).contains("999999").contains("Could not fetch");

		JsonNode filtered = ok("messages_consume", args("topic", topic, "format", "avro", "valueRegex", "\"PAID\""));
		assertThat(filtered.get("matched").asInt()).isEqualTo(2);

		JsonNode one = ok("messages_get", args("topic", topic, "partition", 0, "offset", 3, "format", "avro"));
		assertThat(one.get("value").get("status").asText()).isEqualTo("PAID");
	}

	@Test
	void sampleSchemaReadsAvroFields() {
		JsonNode r = ok("messages_sample_schema", args("topic", topic, "format", "avro"));
		assertThat(r.get("parsed").asInt()).isEqualTo(4);
		assertThat(r.get("failed").asInt()).isEqualTo(2);
		assertThat(r.get("schemaIds").get(String.valueOf(schemaId)).asInt()).isEqualTo(4);
		Map<String, JsonNode> paths = new HashMap<>();
		r.get("paths").forEach(p -> paths.put(p.get("path").asText(), p));
		assertThat(paths).containsKeys("id", "customer", "amount", "note", "createdAt", "status", "tags", "tags[]");
		assertThat(paths.get("note").get("nullRatio").asDouble()).isEqualTo(0.5);
		assertThat(paths.get("id").get("types").get("integer").asInt()).isEqualTo(4);
	}

	@Test
	void serdesListShowsAvroAvailable() {
		JsonNode r = ok("serdes_list", args());
		assertThat(r.get("clusters").get(0).get("avro").asBoolean()).isTrue();
	}

}
