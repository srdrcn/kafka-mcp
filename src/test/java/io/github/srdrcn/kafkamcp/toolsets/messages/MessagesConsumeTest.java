package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.support.TestConfigs;

/** messages_consume: start modes, ordering, filters, formats; no group or commit is created. */
class MessagesConsumeTest extends KafkaIntegrationTest {

	/** 3 partitions, 30 records: i → partition i%3, time BASE+i s, key k{i%5}, value v{i}. */
	private static String topic;

	@BeforeAll
	static void produce() throws Exception {
		topic = unique("msg-consume");
		TestData.createTopic(testAdmin(), topic, 3);
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (int i = 0; i < 30; i++) {
			ProducerRecord<byte[], byte[]> r = TestData.record(topic, i % 3, TestData.BASE_TS + i * 1000L, "k" + (i % 5),
					"v" + i + (i % 4 == 0 ? " ERROR disk full" : " ok"));
			if (i % 2 == 0) {
				r.headers().add(new RecordHeader("trace", ("t-" + i).getBytes(StandardCharsets.UTF_8)));
			}
			records.add(r);
		}
		TestData.send(records);
	}

	private static List<String> values(JsonNode r) {
		List<String> out = new ArrayList<>();
		r.get("records").forEach(n -> out.add(n.get("value").asText().split(" ")[0]));
		return out;
	}

	@Test
	void latestReturnsMostRecentByTimestampWithoutGroups() throws Exception {
		Set<String> groupsBefore = TestData.groupIds(testAdmin());
		JsonNode r = ok("messages_consume", args("topic", topic, "maxRecords", 5));
		assertThat(r.get("from").asText()).isEqualTo("latest");
		assertThat(r.get("order").asText()).isEqualTo("timestamp");
		assertThat(r.get("partitions").asInt()).isEqualTo(3);
		assertThat(r.get("returned").asInt()).isEqualTo(5);
		assertThat(values(r)).containsExactly("v25", "v26", "v27", "v28", "v29");
		assertThat(r.get("reachedEnd").asBoolean()).isTrue();
		assertThat(r.has("partial")).isFalse();
		JsonNode last = r.get("records").get(4);
		assertThat(last.get("partition").asInt()).isEqualTo(29 % 3);
		assertThat(last.get("key").asText()).isEqualTo("k4");
		assertThat(last.get("timestamp").asText()).isEqualTo(Timestamps.iso(TestData.BASE_TS + 29_000L));
		assertThat(last.get("timestampType").asText()).isEqualTo("CreateTime");
		assertThat(last.get("valueSize").asInt()).isEqualTo("v29 ok".length());
		assertThat(r.get("records").get(3).get("headers").get("trace").asText()).isEqualTo("t-28");
		assertThat(TestData.groupIds(testAdmin())).isEqualTo(groupsBefore);
	}

	@Test
	void earliestSplitsMaxRecordsFairlyAcrossPartitions() {
		JsonNode r = ok("messages_consume", args("topic", topic, "from", "earliest", "maxRecords", 6));
		assertThat(r.get("order").asText()).isEqualTo("partition,offset");
		assertThat(r.get("returned").asInt()).isEqualTo(6);
		assertThat(values(r)).containsExactly("v0", "v3", "v1", "v4", "v2", "v5");
		assertThat(r.get("reachedEnd").asBoolean()).isFalse();

		JsonNode all = ok("messages_consume", args("topic", topic, "from", "earliest", "maxRecords", 500));
		assertThat(all.get("returned").asInt()).isEqualTo(30);
		assertThat(all.get("reachedEnd").asBoolean()).isTrue();
	}

	@Test
	void offsetModeReadsFromGivenOffsetAndReportsNextOffset() {
		JsonNode r = ok("messages_consume", args("topic", topic, "partition", 1, "from", "offset", "offset", 3,
				"maxRecords", 2));
		assertThat(r.get("records")).hasSize(2);
		assertThat(r.get("records").get(0).get("offset").asLong()).isEqualTo(3);
		assertThat(values(r)).containsExactly("v10", "v13");
		assertThat(r.get("nextOffset").asLong()).isEqualTo(5);

		// without from it is inferred from offset; an offset beyond latest leaves no records to read
		JsonNode beyond = ok("messages_consume", args("topic", topic, "partition", 1, "offset", 1000));
		assertThat(beyond.get("from").asText()).isEqualTo("offset");
		assertThat(beyond.get("returned").asInt()).isZero();
		assertThat(beyond.get("notes").get(0).asText()).contains("latest");

		assertThat(error("messages_consume", args("topic", topic, "from", "offset"))).contains("offset");
		assertThat(error("messages_consume", args("topic", topic, "from", "latest", "offset", 3)))
			.contains("from=offset");
	}

	@Test
	void timestampModeStartsAtFirstRecordAtOrAfterTime() {
		JsonNode r = ok("messages_consume", args("topic", topic, "from", "timestamp", "timestamp",
				Timestamps.iso(TestData.BASE_TS + 24_500L), "maxRecords", 50));
		assertThat(values(r)).containsExactlyInAnyOrder("v25", "v26", "v27", "v28", "v29");
		assertThat(r.get("reachedEnd").asBoolean()).isTrue();

		JsonNode epoch = ok("messages_consume", args("topic", topic, "partition", 0, "timestamp",
				String.valueOf(TestData.BASE_TS + 27_000L)));
		assertThat(values(epoch)).containsExactly("v27");
	}

	@Test
	void filtersScanAndReportCounts() {
		JsonNode r = ok("messages_consume", args("topic", topic, "from", "earliest", "valueRegex", "ERROR",
				"keyRegex", "^k0$", "maxRecords", 50));
		// value ERROR: i%4==0, key k0: i%5==0 -> i=0,20
		assertThat(values(r)).containsExactlyInAnyOrder("v0", "v20");
		assertThat(r.get("scanned").asInt()).isEqualTo(30);
		assertThat(r.get("matched").asInt()).isEqualTo(2);
		assertThat(r.get("reachedEnd").asBoolean()).isTrue();
		assertThat(r.get("filters").get("valueRegex").asText()).isEqualTo("ERROR");

		JsonNode h = ok("messages_consume", args("topic", topic, "headerKey", "trace", "headerValueRegex", "^t-1\\d$",
				"maxRecords", 3));
		// latest + filter: the newest 3 of the matches (even numbers 10..18)
		assertThat(values(h)).containsExactly("v14", "v16", "v18");
		assertThat(h.get("matched").asInt()).isEqualTo(5);

		JsonNode limited = ok("messages_consume", args("topic", topic, "from", "earliest", "valueRegex", "nomatch",
				"maxScan", 6));
		assertThat(limited.get("scanned").asInt()).isEqualTo(6);
		assertThat(limited.get("scanLimitReached").asBoolean()).isTrue();
		assertThat(limited.get("reachedEnd").asBoolean()).isFalse();

		assertThat(error("messages_consume", args("topic", topic, "headerValueRegex", "x"))).contains("headerKey");
		assertThat(error("messages_consume", args("topic", topic, "valueRegex", "(unclosed"))).contains("regex");
	}

	@Test
	void regexFiltersKeepSignificantWhitespace() {
		// "v1 " finds only v1; trimmed, "v1" would also find v10..v19
		JsonNode r = ok("messages_consume", args("topic", topic, "from", "earliest", "valueRegex", "v1 "));
		assertThat(values(r)).containsExactly("v1");
		assertThat(r.get("filters").get("valueRegex").asText()).isEqualTo("v1 ");
		JsonNode both = ok("messages_consume", args("topic", topic, "from", "earliest", "valueRegex", " ok$",
				"keyRegex", "^k1"));
		// key k1: i%5==1, " ok": i%4!=0 -> 1, 6, 11, 21, 26
		assertThat(values(both)).containsExactlyInAnyOrder("v1", "v6", "v11", "v21", "v26");
	}

	@Test
	void latestFillsMaxRecordsDespiteTransactionMarkers() throws Exception {
		String t = unique("msg-tx");
		TestData.createTopic(testAdmin(), t, 1);
		// 1 record per transaction: the commit marker takes an offset too (60 records, 120 offsets)
		try (KafkaProducer<byte[], byte[]> producer = KafkaTestCluster
			.producer(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, unique("tx")))) {
			producer.initTransactions();
			for (int i = 0; i < 60; i++) {
				producer.beginTransaction();
				producer.send(TestData.record(t, 0, null, null, "v" + i));
				producer.commitTransaction();
			}
		}
		JsonNode r = ok("messages_consume", args("topic", t, "maxRecords", 50));
		assertThat(r.get("returned").asInt()).isEqualTo(50);
		List<String> expected = new ArrayList<>();
		for (int i = 10; i < 60; i++) {
			expected.add("v" + i);
		}
		assertThat(values(r)).containsExactlyElementsOf(expected);
		assertThat(r.has("approximate")).isFalse();
		assertThat(r.get("reachedEnd").asBoolean()).isTrue();

		// if maxScan cuts the backward extension short, the shortfall is not silent
		JsonNode capped = ok("messages_consume", args("topic", t, "maxRecords", 50, "maxScan", 60));
		assertThat(capped.get("returned").asInt()).isLessThan(50).isGreaterThan(25);
		assertThat(capped.get("approximate").asBoolean()).isTrue();
		assertThat(capped.get("notes").toString()).contains("transaction marker");

		JsonNode headers = ok("messages_headers", args("topic", t, "maxRecords", 40));
		assertThat(headers.get("recordsRead").asInt()).isEqualTo(40);
	}

	@Test
	void decodesFormatsAndFlagsProblems() throws Exception {
		String t = unique("msg-formats");
		TestData.createTopic(testAdmin(), t, 1);
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		records.add(new ProducerRecord<>(t, 0, null, TestData.longBytes(7L),
				TestData.utf8("{\"id\":7,\"name\":\"Ωmega €\",\"opt\":null,\"tags\":[\"a\"]}")));
		records.add(new ProducerRecord<>(t, 0, null, TestData.longBytes(8L), TestData.utf8("not json")));
		records.add(new ProducerRecord<>(t, 0, null, new byte[] { 1, 2 }, new byte[] { (byte) 0xc3, (byte) 0x28 }));
		records.add(new ProducerRecord<>(t, 0, null, null, TestData.utf8("x".repeat(100))));
		records.add(new ProducerRecord<>(t, 0, TestData.utf8("tomb"), null));
		TestData.send(records);

		JsonNode json = ok("messages_consume", args("topic", t, "from", "earliest", "format", "json", "keyFormat", "long"));
		JsonNode r0 = json.get("records").get(0);
		assertThat(r0.get("key").asLong()).isEqualTo(7L);
		assertThat(r0.get("value").get("name").asText()).isEqualTo("Ωmega €");
		assertThat(r0.get("value").has("opt")).as("a null field inside the JSON is kept").isTrue();
		assertThat(r0.get("value").get("tags").get(0).asText()).isEqualTo("a");
		JsonNode r1 = json.get("records").get(1);
		assertThat(r1.get("value").asText()).isEqualTo("not json");
		assertThat(r1.get("valueError").asText()).startsWith("Not JSON");
		JsonNode r2 = json.get("records").get(2);
		assertThat(r2.get("keyError").asText()).contains("8 bytes");
		assertThat(r2.get("key").asText()).isEqualTo("0102");
		assertThat(r2.get("value").asText()).isEqualTo("c328");
		assertThat(r2.get("valueError").asText()).contains("UTF-8");
		JsonNode r3 = json.get("records").get(3);
		assertThat(r3.has("key")).isFalse();
		JsonNode r4 = json.get("records").get(4);
		assertThat(r4.get("tombstone").asBoolean()).isTrue();
		assertThat(r4.get("value").isNull()).isTrue();

		JsonNode cut = ok("messages_consume", args("topic", t, "partition", 0, "offset", 3, "maxRecords", 1,
				"maxValueChars", 20));
		assertThat(cut.get("records").get(0).get("value").asText()).hasSize(20);
		assertThat(cut.get("records").get(0).get("valueTruncated").asBoolean()).isTrue();

		JsonNode hex = ok("messages_consume", args("topic", t, "partition", 0, "offset", 1, "maxRecords", 1,
				"format", "hex"));
		assertThat(hex.get("records").get(0).get("value").asText()).isEqualTo("6e6f74206a736f6e");
	}

	@Test
	void readOnlyClusterCanReadAndAvroNeedsSchemaRegistry() {
		JsonNode r = ok("messages_consume", args("cluster", TestConfigs.READ_ONLY_CLUSTER, "topic", topic,
				"maxRecords", 2));
		assertThat(values(r)).containsExactly("v28", "v29");

		assertThat(error("messages_consume", args("topic", topic, "format", "avro"))).contains("schemaRegistryUrl");
		assertThat(error("messages_consume", args("topic", "missing-" + unique("x")))).contains("not found");
		assertThat(error("messages_consume", args("topic", topic, "partition", 7))).contains("Partition 7 does not exist");
		assertThat(error("messages_consume", args("topic", topic, "format", "xml"))).contains("Invalid format");
	}

}
