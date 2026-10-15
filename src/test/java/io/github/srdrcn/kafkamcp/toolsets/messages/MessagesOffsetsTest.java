package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;

/** messages_offset_for_timestamp, messages_count, messages_partition_for_key. */
class MessagesOffsetsTest extends KafkaIntegrationTest {

	/** 2 partition; p0: 10 kayıt (BASE+i sn), p1: 4 kayıt (BASE+100+i sn). */
	private static String topic;

	@BeforeAll
	static void produce() throws Exception {
		topic = unique("msg-offsets");
		TestData.createTopic(testAdmin(), topic, 2);
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			records.add(TestData.record(topic, 0, TestData.BASE_TS + i * 1000L, null, "p0-" + i));
		}
		for (int i = 0; i < 4; i++) {
			records.add(TestData.record(topic, 1, TestData.BASE_TS + (100 + i) * 1000L, null, "p1-" + i));
		}
		TestData.send(records);
	}

	@Test
	void offsetForTimestamp() {
		JsonNode r = ok("messages_offset_for_timestamp",
				args("topic", topic, "timestamp", Timestamps.iso(TestData.BASE_TS + 5_500L)));
		assertThat(r.get("found").asInt()).isEqualTo(2);
		JsonNode p0 = r.get("partitions").get(0);
		assertThat(p0.get("partition").asInt()).isZero();
		assertThat(p0.get("offset").asLong()).isEqualTo(6);
		assertThat(p0.get("timestamp").asText()).isEqualTo(Timestamps.iso(TestData.BASE_TS + 6_000L));
		assertThat(r.get("partitions").get(1).get("offset").asLong()).isZero();

		JsonNode late = ok("messages_offset_for_timestamp",
				args("topic", topic, "timestamp", String.valueOf(TestData.BASE_TS + 50_000L), "partition", 0));
		assertThat(late.get("notFound").asInt()).isEqualTo(1);
		JsonNode row = late.get("partitions").get(0);
		assertThat(row.get("offset").isNull()).isTrue();
		assertThat(row.get("latestOffset").asLong()).isEqualTo(10);
		assertThat(error("messages_offset_for_timestamp", args("topic", topic, "timestamp", "geçen hafta")))
			.contains("Geçersiz zaman");
	}

	@Test
	void countsByOffsetsAndTimestamps() {
		JsonNode all = ok("messages_count", args("topic", topic));
		assertThat(all.get("total").asLong()).isEqualTo(14);
		assertThat(all.get("approximate").asBoolean()).isTrue();
		assertThat(all.get("note").asText()).contains("compacted");
		assertThat(all.get("partitions").get(1).get("count").asLong()).isEqualTo(4);

		JsonNode offsets = ok("messages_count", args("topic", topic, "partition", 0, "fromOffset", 2, "toOffset", 7));
		assertThat(offsets.get("total").asLong()).isEqualTo(5);
		JsonNode clamped = ok("messages_count", args("topic", topic, "partition", 0, "fromOffset", 8, "toOffset", 500));
		assertThat(clamped.get("total").asLong()).isEqualTo(2);

		JsonNode times = ok("messages_count", args("topic", topic, "fromTimestamp", Timestamps.iso(TestData.BASE_TS + 3_000L),
				"toTimestamp", Timestamps.iso(TestData.BASE_TS + 102_000L)));
		// p0: offset 3..9 (7 kayıt), p1: 100 ve 101 sn (2 kayıt)
		assertThat(times.get("partitions").get(0).get("count").asLong()).isEqualTo(7);
		assertThat(times.get("partitions").get(1).get("count").asLong()).isEqualTo(2);
		assertThat(times.get("total").asLong()).isEqualTo(9);

		assertThat(error("messages_count", args("topic", topic, "fromOffset", 1, "toTimestamp", "2024-01-01")))
			.contains("birlikte kullanılamaz");
	}

	@Test
	void partitionForKeyMatchesDefaultPartitioner() throws Exception {
		String t = unique("msg-keys");
		TestData.createTopic(testAdmin(), t, 7);
		List<String> keys = List.of("order-1", "order-2", "müşteri-42", "x", "a-much-longer-key-value-123456");
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (String k : keys) {
			records.add(new ProducerRecord<>(t, TestData.utf8(k), TestData.utf8("v")));
		}
		records.add(new ProducerRecord<>(t, TestData.longBytes(123456789L), TestData.utf8("v")));
		List<RecordMetadata> sent = TestData.send(records);
		for (int i = 0; i < keys.size(); i++) {
			JsonNode r = ok("messages_partition_for_key", args("topic", t, "key", keys.get(i)));
			assertThat(r.get("partition").asInt()).as("key %s", keys.get(i)).isEqualTo(sent.get(i).partition());
			assertThat(r.get("partitionCount").asInt()).isEqualTo(7);
			assertThat(r.get("note").asText()).contains("partitioner");
		}
		JsonNode hex = ok("messages_partition_for_key",
				args("topic", t, "key", RecordDecoder.hex(TestData.utf8("order-1")), "keyFormat", "hex"));
		assertThat(hex.get("partition").asInt()).isEqualTo(sent.get(0).partition());
		JsonNode longKey = ok("messages_partition_for_key", args("topic", t, "key", "123456789", "keyFormat", "long"));
		assertThat(longKey.get("partition").asInt()).isEqualTo(sent.get(keys.size()).partition());
		assertThat(longKey.get("keyHex").asText()).isEqualTo("00000000075bcd15");
		assertThat(error("messages_partition_for_key", args("topic", t, "key", "zz", "keyFormat", "hex")))
			.contains("hex");
	}

	@Test
	void partitionForKeyHashesKeyBytesExactlyWithoutTrimming() throws Exception {
		String t = unique("msg-keys-ws");
		TestData.createTopic(testAdmin(), t, 11);
		// sabit genişlikli/boşluklu key'ler ve boş string (null olmayan geçerli key) olduğu gibi hash'lenir
		List<String> keys = List.of("ACC123   ", "ACC123", " order-1", "order-1", "", "   ");
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (String k : keys) {
			records.add(new ProducerRecord<>(t, TestData.utf8(k), TestData.utf8("v")));
		}
		List<RecordMetadata> sent = TestData.send(records);
		for (int i = 0; i < keys.size(); i++) {
			JsonNode r = ok("messages_partition_for_key", args("topic", t, "key", keys.get(i)));
			assertThat(r.get("partition").asInt()).as("key '%s'", keys.get(i)).isEqualTo(sent.get(i).partition());
			assertThat(r.get("key").asText()).isEqualTo(keys.get(i));
			assertThat(r.get("keyBytes").asInt()).isEqualTo(keys.get(i).length());
		}
		// trim edilseydi aynı partition'a düşerlerdi: en az bir çift gerçekten farklı partition'da olmalı
		assertThat(sent.get(0).partition() != sent.get(1).partition() || sent.get(2).partition() != sent.get(3).partition()
				|| sent.get(4).partition() != sent.get(5).partition())
			.as("test verisi trim farkını göstermeli")
			.isTrue();
		assertThat(error("messages_partition_for_key", args("topic", t))).contains("key");
		assertThat(error("messages_partition_for_key", args("topic", t, "key", "  ", "keyFormat", "long")))
			.contains("key");
	}

}
