package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;

/**
 * Large records: the raw records held per call are limited to {@link MessageReader#MAX_HELD_BYTES} and the rendered
 * response of messages_consume to {@link ConsumeTools#MAX_RESPONSE_CHARS}; both return a partial result with a note.
 */
class MessagesSizeLimitsTest extends KafkaIntegrationTest {

	private static final int COUNT = 40;

	private static final int VALUE_BYTES = 900_000;

	/** One partition, 40 records of ~900 KB (36 MB in total): value "v{i}:xxx...". */
	private static String topic;

	@BeforeAll
	static void produce() throws Exception {
		topic = unique("msg-size");
		TestData.createTopic(testAdmin(), topic, 1);
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (int i = 0; i < COUNT; i++) {
			String value = "v" + i + ":" + "x".repeat(VALUE_BYTES);
			records.add(TestData.record(topic, 0, TestData.BASE_TS + i * 1000L, "k" + i, value));
		}
		TestData.send(records);
	}

	private static List<Integer> indexes(JsonNode r) {
		List<Integer> out = new ArrayList<>();
		r.get("records").forEach(n -> out.add(Integer.parseInt(n.get("value").asText().split(":")[0].substring(1))));
		return out;
	}

	private static String notes(JsonNode r) {
		return r.path("notes").toString();
	}

	@Test
	void latestKeepsTheNewestRecordsWithinTheMemoryBudget() {
		JsonNode r = ok("messages_consume", args("topic", topic, "maxRecords", COUNT));
		int expected = (int) (MessageReader.MAX_HELD_BYTES / (VALUE_BYTES + 6));
		assertThat(r.get("returned").asInt()).isBetween(expected - 1, expected).isLessThan(COUNT);
		List<Integer> idx = indexes(r);
		assertThat(idx.get(idx.size() - 1)).isEqualTo(COUNT - 1);
		assertThat(idx).isSorted();
		assertThat(r.get("partial").asBoolean()).isTrue();
		assertThat(notes(r)).contains("memory budget");
	}

	@Test
	void forwardReadStopsAtTheMemoryBudget() {
		JsonNode r = ok("messages_consume",
				args("topic", topic, "partition", 0, "from", "earliest", "maxRecords", COUNT));
		int n = r.get("returned").asInt();
		assertThat(n).isLessThan(COUNT).isGreaterThan(30);
		assertThat(indexes(r)).startsWith(0, 1, 2);
		assertThat(r.get("nextOffset").asLong()).isEqualTo(n);
		assertThat(r.get("reachedEnd").asBoolean()).isFalse();
		assertThat(r.get("partial").asBoolean()).isTrue();
		assertThat(notes(r)).contains("memory budget");
	}

	@Test
	void responseSizeIsCapped() {
		JsonNode latest = ok("messages_consume",
				args("topic", topic, "maxRecords", COUNT, "maxValueChars", 100_000));
		int n = latest.get("returned").asInt();
		assertThat(n).isLessThan(ConsumeTools.MAX_RESPONSE_CHARS / 100_000 + 1).isPositive();
		assertThat(latest.get("truncated").asBoolean()).isTrue();
		List<Integer> idx = indexes(latest);
		assertThat(idx.get(idx.size() - 1)).as("the newest records are kept").isEqualTo(COUNT - 1);
		assertThat(notes(latest)).contains("Response size limit");
		assertThat(latest.toString().length()).isLessThan(ConsumeTools.MAX_RESPONSE_CHARS + 100_000);

		JsonNode forward = ok("messages_consume", args("topic", topic, "partition", 0, "from", "offset", "offset", 0,
				"maxRecords", 20, "maxValueChars", 100_000));
		int m = forward.get("returned").asInt();
		assertThat(m).isLessThan(20);
		assertThat(indexes(forward)).startsWith(0, 1);
		// paging continues at the first record that was left out
		assertThat(forward.get("nextOffset").asLong()).isEqualTo(m);
		assertThat(forward.get("reachedEnd").asBoolean()).isFalse();
		assertThat(forward.get("truncated").asBoolean()).isTrue();
	}

}
