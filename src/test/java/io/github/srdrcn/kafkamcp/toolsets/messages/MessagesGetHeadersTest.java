package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;

/** messages_get (single record, full value, out-of-range error) and messages_headers (header statistics). */
class MessagesGetHeadersTest extends KafkaIntegrationTest {

	private static String topic;

	private static final String BIG = "b".repeat(10_000);

	@BeforeAll
	static void produce() throws Exception {
		topic = unique("msg-get");
		TestData.createTopic(testAdmin(), topic, 2);
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (int i = 0; i < 6; i++) {
			ProducerRecord<byte[], byte[]> r = TestData.record(topic, 0, null, "key-" + i, i == 2 ? BIG : "value-" + i);
			r.headers().add(new RecordHeader("source", ("svc-" + (i % 2)).getBytes(StandardCharsets.UTF_8)));
			if (i < 3) {
				r.headers().add(new RecordHeader("bin", new byte[] { (byte) 0xff, 0x01 }));
			}
			if (i == 0) {
				r.headers().add(new RecordHeader("source", "dup".getBytes(StandardCharsets.UTF_8)));
			}
			records.add(r);
		}
		records.add(TestData.record(topic, 1, null, null, "no-headers"));
		TestData.send(records);
	}

	@Test
	void getsSingleRecordWithFullValue() {
		JsonNode r = ok("messages_get", args("topic", topic, "partition", 0, "offset", 2));
		assertThat(r.get("topic").asText()).isEqualTo(topic);
		assertThat(r.get("offset").asLong()).isEqualTo(2);
		assertThat(r.get("key").asText()).isEqualTo("key-2");
		assertThat(r.get("value").asText()).isEqualTo(BIG);
		assertThat(r.has("valueTruncated")).isFalse();
		assertThat(r.get("headers").get("bin").asText()).isEqualTo("hex:ff01");

		JsonNode first = ok("messages_get", args("topic", topic, "partition", 0, "offset", 0, "format", "hex"));
		assertThat(first.get("value").asText()).isEqualTo(RecordDecoder.hex("value-0".getBytes(StandardCharsets.UTF_8)));
		assertThat(first.get("headers").get("source")).hasSize(2);
	}

	@Test
	void getRejectsOffsetOutOfRange() {
		String e = error("messages_get", args("topic", topic, "partition", 0, "offset", 99));
		assertThat(e).contains("out of range").contains("earliest=0").contains("latest=6");
		assertThat(error("messages_get", args("topic", topic, "partition", 5, "offset", 0)))
			.contains("Partition 5 does not exist");
		assertThat(error("messages_get", args("topic", topic, "partition", 0))).contains("offset");
	}

	@Test
	void headerStatistics() {
		JsonNode r = ok("messages_headers", args("topic", topic, "from", "earliest"));
		assertThat(r.get("recordsRead").asInt()).isEqualTo(7);
		assertThat(r.get("withHeaders").asInt()).isEqualTo(6);
		assertThat(r.get("withoutHeaders").asInt()).isEqualTo(1);
		assertThat(r.get("headerKeys").asInt()).isEqualTo(2);
		Map<String, JsonNode> byKey = new HashMap<>();
		r.get("headers").forEach(h -> byKey.put(h.get("key").asText(), h));
		JsonNode source = byKey.get("source");
		assertThat(source.get("count").asInt()).isEqualTo(7);
		assertThat(source.get("records").asInt()).isEqualTo(6);
		assertThat(source.get("distinctValues").asInt()).isEqualTo(3);
		assertThat(source.get("samples")).hasSizeLessThanOrEqualTo(3);
		assertThat(byKey.get("bin").get("records").asInt()).isEqualTo(3);
		assertThat(byKey.get("bin").get("samples").get(0).asText()).isEqualTo("hex:ff01");
		assertThat(r.get("headers").get(0).get("key").asText()).isEqualTo("source");

		JsonNode latest = ok("messages_headers", args("topic", topic, "partition", 0, "maxRecords", 2));
		assertThat(latest.get("recordsRead").asInt()).isEqualTo(2);
		assertThat(latest.get("headerKeys").asInt()).isEqualTo(1);
		assertThat(error("messages_headers", args("topic", topic, "from", "offset"))).contains("earliest");
	}

	@Test
	void longHeadersAreFlaggedWhenCutAndFullInGet() throws Exception {
		String t = unique("msg-long-headers");
		TestData.createTopic(testAdmin(), t, 1);
		String trace = "java.lang.IllegalStateException: boom\n" + "\tat com.example.Handler.handle(Handler.java:42)\n".repeat(200);
		byte[] binary = new byte[600];
		for (int i = 0; i < binary.length; i++) {
			binary[i] = (byte) (0x80 | i);
		}
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (int i = 0; i < 2; i++) {
			ProducerRecord<byte[], byte[]> r = TestData.record(t, 0, null, null, "dlt-" + i);
			r.headers().add(new RecordHeader("kafka_dlt-exception-stacktrace", (trace + i).getBytes(StandardCharsets.UTF_8)));
			r.headers().add(new RecordHeader("bin", binary));
			records.add(r);
		}
		TestData.send(records);
		String hex = RecordJson.HEX_PREFIX + RecordDecoder.hex(binary);

		JsonNode full = ok("messages_get", args("topic", t, "partition", 0, "offset", 0));
		assertThat(full.get("headers").get("kafka_dlt-exception-stacktrace").asText()).isEqualTo(trace + 0);
		assertThat(full.get("headers").get("bin").asText()).isEqualTo(hex);
		assertThat(full.has("headersTruncated")).isFalse();

		// default maxValueChars (4096): the long stacktrace is truncated and flagged, the 1204-character hex is not
		JsonNode consumed = ok("messages_consume", args("topic", t, "from", "earliest", "maxRecords", 1));
		JsonNode rec = consumed.get("records").get(0);
		assertThat(rec.get("headers").get("kafka_dlt-exception-stacktrace").asText())
			.hasSize(RecordDecoder.DEFAULT_MAX_CHARS);
		assertThat(rec.get("headers").get("bin").asText()).isEqualTo(hex);
		assertThat(rec.get("headersTruncated")).hasSize(1);
		assertThat(rec.get("headersTruncated").get(0).asText()).isEqualTo("kafka_dlt-exception-stacktrace");

		JsonNode small = ok("messages_consume", args("topic", t, "from", "earliest", "maxRecords", 1, "maxValueChars", 100));
		JsonNode smallRec = small.get("records").get(0);
		assertThat(smallRec.get("headers").get("bin").asText()).hasSize(100);
		assertThat(smallRec.get("headersTruncated").toString()).contains("kafka_dlt-exception-stacktrace").contains("bin");

		// distinct values are counted on the untruncated values: the two stacktraces differ only in the last character
		JsonNode stats = ok("messages_headers", args("topic", t, "from", "earliest"));
		Map<String, JsonNode> byKey = new HashMap<>();
		stats.get("headers").forEach(h -> byKey.put(h.get("key").asText(), h));
		JsonNode st = byKey.get("kafka_dlt-exception-stacktrace");
		assertThat(st.get("distinctValues").asInt()).isEqualTo(2);
		assertThat(st.get("samplesTruncatedAt").asInt()).isEqualTo(200);
		assertThat(byKey.get("bin").get("distinctValues").asInt()).isEqualTo(1);
	}

}
