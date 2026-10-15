package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.kafka.LastRecords;
import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;

class TopicDataToolsTest extends KafkaIntegrationTest {

	/** p0'a yazılan mesajların zamanı (p1'den yeni). */
	private static final long TS0 = Instant.parse("2026-01-02T03:04:05Z").toEpochMilli();

	private static final long TS1 = TS0 - 3_600_000;

	private static String prefix;

	private static String topic;

	private static String empty;

	@Autowired
	private KafkaClients clients;

	@BeforeAll
	static void setUp() throws Exception {
		prefix = unique("td");
		topic = prefix + "-data";
		empty = prefix + "-empty";
		// sabit (eski) zaman damgaları retention'a takılmasın
		TestData.createTopic(testAdmin(), topic, 3, Map.of("retention.ms", "-1"));
		TestData.createTopic(testAdmin(), empty, 1, Map.of());
		TestData.produce(topic, 0, 6, TS0);
		TestData.produce(topic, 1, 4, TS1);
	}

	@Test
	void topicsOffsetsReportsPerPartitionAndTotals() {
		JsonNode r = ok("topics_offsets", args("topic", topic));
		assertThat(r.at("/totals/count").asLong()).isEqualTo(10);
		assertThat(r.at("/totals/latest").asLong()).isEqualTo(10);
		assertThat(r.at("/totals/earliest").asLong()).isZero();
		JsonNode p0 = r.get("partitions").get(0);
		assertThat(p0.get("partition").asInt()).isZero();
		assertThat(p0.get("earliest").asLong()).isZero();
		assertThat(p0.get("latest").asLong()).isEqualTo(6);
		assertThat(p0.get("count").asLong()).isEqualTo(6);
		assertThat(r.at("/partitions/2/count").asLong()).isZero();
		assertThat(error("topics_offsets", args("topic", prefix + "-yok"))).contains("Topic bulunamadı");
	}

	@Test
	void topicsSizeShowsPartitionsOrLargestTopics() {
		JsonNode r = ok("topics_size", args("topic", topic));
		assertThat(r.get("partitionCount").asInt()).isEqualTo(3);
		assertThat(r.get("sizeBytes").asLong()).isPositive();
		assertThat(r.get("totalSizeBytes").asLong()).isEqualTo(r.get("sizeBytes").asLong());
		assertThat(r.get("size").asText()).endsWith("B");
		JsonNode biggest = r.get("partitions").get(0);
		assertThat(biggest.get("partition").asInt()).isZero();
		assertThat(biggest.get("sizeBytes").asLong()).isGreaterThan(r.at("/partitions/1/sizeBytes").asLong());
		long sum = 0;
		for (JsonNode p : r.get("partitions")) {
			sum += p.get("sizeBytes").asLong();
		}
		assertThat(sum).isEqualTo(r.get("sizeBytes").asLong());

		JsonNode top = ok("topics_size", args("filter", "^" + prefix));
		assertThat(top.get("topicsMatched").asInt()).isEqualTo(2);
		assertThat(top.at("/topics/0/topic").asText()).isEqualTo(topic);
		assertThat(top.at("/topics/0/sizeBytes").asLong()).isEqualTo(r.get("sizeBytes").asLong());
		assertThat(top.at("/topics/0/partitions").asInt()).isEqualTo(3);
		assertThat(top.at("/topics/1/topic").asText()).isEqualTo(empty);
		assertThat(top.at("/topics/1/sizeBytes").asLong()).isZero();
		assertThat(top.get("sizeBytes").asLong()).isEqualTo(r.get("sizeBytes").asLong());
	}

	@Test
	void topicsLastWriteTimeUsesMaxTimestamp() {
		JsonNode r = ok("topics_last_write_time", args("topic", topic));
		assertThat(r.get("lastWriteTime").asText()).isEqualTo("2026-01-02T03:04:05Z");
		assertThat(r.get("partition").asInt()).isZero();
		assertThat(r.get("ageSeconds").asLong()).isPositive();
		assertThat(r.get("method").asText()).isEqualTo("maxTimestamp");
		assertThat(r.get("emptyPartitions").asInt()).isEqualTo(1);
		JsonNode p1 = r.get("partitions").get(1);
		assertThat(p1.get("lastWriteTime").asText()).isEqualTo(Instant.ofEpochMilli(TS1).toString());
		assertThat(p1.get("offset").asLong()).isBetween(0L, 3L);
		assertThat(r.at("/partitions/2/empty").asBoolean()).isTrue();

		JsonNode none = ok("topics_last_write_time", args("topic", empty));
		assertThat(none.has("lastWriteTime")).isFalse();
		assertThat(none.get("emptyPartitions").asInt()).isEqualTo(1);
	}

	@Test
	void lastRecordsReadsLastRecordWithAssignOnlyConsumer() {
		ToolCall call = new ToolCall("test", Map.of("cluster", "test"), clients, Instant.now().plusSeconds(15));
		List<TopicPartition> tps = List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1),
				new TopicPartition(topic, 2));
		Map<TopicPartition, LastRecords.Last> last = LastRecords.read(call, Offsets.ranges(call, tps), 5_000);
		assertThat(last).containsOnlyKeys(tps.get(0), tps.get(1));
		assertThat(last.get(tps.get(0))).isEqualTo(new LastRecords.Last(5, TS0));
		assertThat(last.get(tps.get(1))).isEqualTo(new LastRecords.Last(3, TS1));
	}

	@Test
	void topicsWriteRateMeasuresMessagesPerSecond() throws Exception {
		String busy = unique("tdbusy");
		TestData.createTopic(testAdmin(), busy, 2, Map.of());
		AtomicBoolean stop = new AtomicBoolean();
		Thread writer = Thread.ofVirtual().start(() -> {
			try (KafkaProducer<byte[], byte[]> p = KafkaTestCluster.producer(null)) {
				int i = 0;
				while (!stop.get()) {
					p.send(new ProducerRecord<>(busy, i++ % 2, null, new byte[] { 1 }));
					Thread.sleep(10);
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		try {
			Thread.sleep(500);
			JsonNode r = ok("topics_write_rate", args("topic", busy, "intervalSeconds", 2));
			assertThat(r.get("topic").asText()).isEqualTo(busy);
			assertThat(r.get("intervalSeconds").asDouble()).isBetween(1.9, 4.0);
			assertThat(r.get("totalMsgsPerSec").asDouble()).isPositive();
			assertThat(r.get("totalMessages").asLong()).isPositive();
			assertThat(r.get("partitions")).hasSize(2);
			assertThat(r.at("/partitions/0/msgsPerSec").asDouble()).isPositive();

			JsonNode top = ok("topics_write_rate",
					args("filter", "^(" + busy + "|" + empty + ")$", "intervalSeconds", 1));
			assertThat(top.get("topicsScanned").asInt()).isEqualTo(2);
			assertThat(top.get("idleTopics").asInt()).isEqualTo(1);
			assertThat(top.get("total").asInt()).isEqualTo(1);
			assertThat(top.at("/activeTopics/0/topic").asText()).isEqualTo(busy);
			assertThat(top.at("/activeTopics/0/msgsPerSec").asDouble()).isPositive();
		}
		finally {
			stop.set(true);
			writer.join(10_000);
		}
	}

}
