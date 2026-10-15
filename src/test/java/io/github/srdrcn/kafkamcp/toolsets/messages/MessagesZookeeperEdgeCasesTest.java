package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolRegistry;
import io.github.srdrcn.kafkamcp.support.MixedClusterIntegrationTest;
import io.github.srdrcn.kafkamcp.support.ZkKafkaTestCluster;

/**
 * Realistic data situations on the ZooKeeper-mode cluster ({@code zk}): transaction markers, a log start moved
 * forward with deleteRecords, a LogAppendTime + zstd topic; and a simulated old ZooKeeper broker that does not support
 * time lookups (v0 message format / old protocol): the tool does not fail, it gives a note per partition.
 */
class MessagesZookeeperEdgeCasesTest extends MixedClusterIntegrationTest {

	@Autowired
	private KafkaClients clients;

	@Autowired
	private OffsetTools offsetTools;

	@Autowired
	private ConsumeTools consumeTools;

	private static void createZkTopic(NewTopic topic) throws Exception {
		zkAdmin().createTopics(List.of(topic)).all().get(60, TimeUnit.SECONDS);
	}

	private static List<String> values(JsonNode r) {
		List<String> out = new ArrayList<>();
		r.get("records").forEach(n -> out.add(n.get("value").asText()));
		return out;
	}

	@Test
	void transactionMarkersOnZookeeperCluster() throws Exception {
		String topic = unique("mx-tx");
		createZkTopic(new NewTopic(topic, 1, (short) 1));
		// offsets: c0..c4 = 0..4, commit marker 5, a0..a2 = 6..8 (aborted), abort marker 9, c5..c6 = 10..11,
		// commit marker 12; latest = 13
		try (KafkaProducer<byte[], byte[]> p = ZkKafkaTestCluster
			.producer(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, unique("mx-txid")))) {
			p.initTransactions();
			p.beginTransaction();
			for (int i = 0; i < 5; i++) {
				p.send(TestData.record(topic, 0, null, null, "c" + i));
			}
			p.commitTransaction();
			p.beginTransaction();
			for (int i = 0; i < 3; i++) {
				p.send(TestData.record(topic, 0, null, null, "a" + i));
			}
			p.flush();
			p.abortTransaction();
			p.beginTransaction();
			for (int i = 5; i < 7; i++) {
				p.send(TestData.record(topic, 0, null, null, "c" + i));
			}
			p.commitTransaction();
		}

		JsonNode count = ok("messages_count", zkArgs("topic", topic));
		assertThat(count.get("total").asLong()).isEqualTo(13);
		assertThat(count.get("approximate").asBoolean()).isTrue();
		assertThat(count.get("note").asText()).contains("transactional");

		JsonNode marker = ok("messages_get", zkArgs("topic", topic, "partition", 0, "offset", 5));
		assertThat(marker.get("offset").asLong()).isEqualTo(6);
		assertThat(marker.get("requestedOffset").asLong()).isEqualTo(5);
		assertThat(marker.get("value").asText()).isEqualTo("a0");
		assertThat(marker.get("note").asText()).contains("transaction marker");
		assertThat(error("messages_get", zkArgs("topic", topic, "partition", 0, "offset", 12)))
			.contains("transaction marker");

		// reads are read_uncommitted: aborted records are visible too, markers are not
		JsonNode all = ok("messages_consume", zkArgs("topic", topic, "from", "earliest", "maxRecords", 50));
		assertThat(values(all)).containsExactly("c0", "c1", "c2", "c3", "c4", "a0", "a1", "a2", "c5", "c6");
		assertThat(all.get("reachedEnd").asBoolean()).isTrue();

		// latest: markers take up offsets, so the window is extended backwards and exactly maxRecords records come back
		JsonNode last = ok("messages_consume", zkArgs("topic", topic, "maxRecords", 3));
		assertThat(values(last)).containsExactly("a2", "c5", "c6");
		assertThat(last.has("approximate")).isFalse();
		JsonNode ten = ok("messages_consume", zkArgs("topic", topic, "maxRecords", 10));
		assertThat(ten.get("returned").asInt()).isEqualTo(10);
		assertThat(ten.has("approximate")).isFalse();
	}

	@Test
	void deletedRecordsMoveEarliestOnZookeeperCluster() throws Exception {
		String topic = unique("mx-del");
		createZkTopic(new NewTopic(topic, 1, (short) 1));
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			records.add(TestData.record(topic, 0, TestData.BASE_TS + i * 1000L, null, "d" + i));
		}
		try (KafkaProducer<byte[], byte[]> p = ZkKafkaTestCluster.producer(null)) {
			MessagesMixedClusterTest.send(p, records);
		}
		zkAdmin().deleteRecords(Map.of(new TopicPartition(topic, 0), RecordsToDelete.beforeOffset(4)))
			.all()
			.get(30, TimeUnit.SECONDS);

		JsonNode count = ok("messages_count", zkArgs("topic", topic));
		assertThat(count.get("total").asLong()).isEqualTo(6);
		assertThat(count.get("partitions").get(0).get("start").asLong()).isEqualTo(4);
		assertThat(error("messages_get", zkArgs("topic", topic, "partition", 0, "offset", 2)))
			.contains("out of range")
			.contains("earliest=4");
		JsonNode got = ok("messages_get", zkArgs("topic", topic, "partition", 0, "offset", 4));
		assertThat(got.get("value").asText()).isEqualTo("d4");

		JsonNode fromZero = ok("messages_consume",
				zkArgs("topic", topic, "partition", 0, "offset", 0, "maxRecords", 3));
		assertThat(values(fromZero)).containsExactly("d4", "d5", "d6");
		assertThat(fromZero.get("notes").get(0).asText()).contains("below earliest");
		assertThat(fromZero.get("nextOffset").asLong()).isEqualTo(7);

		JsonNode at = ok("messages_offset_for_timestamp",
				zkArgs("topic", topic, "timestamp", Timestamps.iso(TestData.BASE_TS)));
		assertThat(at.get("partitions").get(0).get("offset").asLong()).isEqualTo(4);
		assertThat(at.get("partitions").get(0).get("timestamp").asText())
			.isEqualTo(Timestamps.iso(TestData.BASE_TS + 4_000L));
	}

	@Test
	void logAppendTimeAndCompressionOnZookeeperCluster() throws Exception {
		String topic = unique("mx-lat");
		createZkTopic(new NewTopic(topic, 1, (short) 1).configs(Map.of("message.timestamp.type", "LogAppendTime")));
		Instant before = Instant.now().minus(10, ChronoUnit.MINUTES);
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			// the producer's CreateTime (2 hours ago) is overwritten by the broker's LogAppendTime
			records.add(TestData.record(topic, 0, TestData.BASE_TS + i, "k" + i, "{\"n\":" + i + "}"));
		}
		try (KafkaProducer<byte[], byte[]> p = ZkKafkaTestCluster
			.producer(Map.of(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd"))) {
			MessagesMixedClusterTest.send(p, records);
		}

		JsonNode r = ok("messages_consume", zkArgs("topic", topic, "from", "earliest", "format", "json"));
		assertThat(r.get("returned").asInt()).isEqualTo(3);
		for (JsonNode rec : r.get("records")) {
			assertThat(rec.get("timestampType").asText()).isEqualTo("LogAppendTime");
			assertThat(Instant.parse(rec.get("timestamp").asText())).isAfter(before);
		}
		assertThat(r.get("records").get(2).get("value").get("n").asInt()).isEqualTo(2);

		// looked up by the broker's append time, not by CreateTime (2 hours ago)
		JsonNode at = ok("messages_offset_for_timestamp",
				zkArgs("topic", topic, "timestamp", Timestamps.iso(TestData.BASE_TS + 3_600_000L)));
		assertThat(at.get("found").asInt()).isEqualTo(1);
		assertThat(at.get("partitions").get(0).get("offset").asLong()).isZero();
	}

	/**
	 * Proxy that wraps the zk Admin and, like an old broker, rejects time-based listOffsets
	 * (OffsetSpec.forTimestamp) on the given partitions: a topic with the v0 message format
	 * (UNSUPPORTED_FOR_MESSAGE_FORMAT) or a protocol version that does not support the request
	 * (UnsupportedVersionException). All other calls go to the real Admin.
	 */
	@SuppressWarnings("unchecked")
	private static Admin legacyZookeeperBroker(Admin real, Map<Integer, RuntimeException> failures) {
		return (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] { Admin.class },
				(proxy, method, a) -> {
					if (method.getName().equals("listOffsets") && a != null && a.length >= 1
							&& a[0] instanceof Map<?, ?> request) {
						Map<TopicPartition, OffsetSpec> pass = new HashMap<>();
						Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new HashMap<>();
						((Map<TopicPartition, OffsetSpec>) request).forEach((tp, spec) -> {
							RuntimeException e = spec instanceof OffsetSpec.TimestampSpec ? failures.get(tp.partition())
									: null;
							if (e == null) {
								pass.put(tp, spec);
							}
							else {
								KafkaFutureImpl<ListOffsetsResultInfo> f = new KafkaFutureImpl<>();
								f.completeExceptionally(e);
								futures.put(tp, f);
							}
						});
						ListOffsetsResult r = a.length > 1 ? real.listOffsets(pass, (ListOffsetsOptions) a[1])
								: real.listOffsets(pass);
						pass.keySet().forEach(tp -> futures.put(tp, r.partitionResult(tp)));
						return new ListOffsetsResult(futures);
					}
					try {
						return method.invoke(real, a);
					}
					catch (InvocationTargetException e) {
						throw e.getCause();
					}
				});
	}

	/** Calls the tool with the zk Admin simulating an old broker; it must finish before the deadline. */
	private JsonNode onLegacyBroker(Function<ToolCall, Object> handler, Map<String, Object> args) {
		Admin legacy = legacyZookeeperBroker(zkAdmin(),
				Map.of(1, new UnsupportedForMessageFormatException("Cannot search offsets based on timestamp because "
						+ "message format version for partition is 0.9.0 which is earlier than the minimum required "
						+ "version 0.10.0"), 2, new UnsupportedVersionException("simulated: old broker")));
		KafkaClients legacyClients = new KafkaClients(clients.config()) {
			@Override
			public Admin admin(String cluster) {
				return legacy;
			}
		};
		Instant deadline = Instant.now().plusMillis(KafkaClients.TIMEOUT_MS);
		Object result = handler.apply(new ToolCall("test", args, legacyClients, deadline));
		assertThat(Instant.now()).isBefore(deadline);
		return ToolRegistry.resultMapper().valueToTree(result);
	}

	@Test
	void timestampLookupUnsupportedByOldBrokerBecomesPerPartitionNote() throws Exception {
		String topic = unique("mx-legacy");
		createZkTopic(new NewTopic(topic, 3, (short) 1));
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (int p = 0; p < 3; p++) {
			for (int j = 0; j < 4; j++) {
				records.add(TestData.record(topic, p, TestData.BASE_TS + j * 1000L, null, "p" + p + "-" + j));
			}
		}
		try (KafkaProducer<byte[], byte[]> p = ZkKafkaTestCluster.producer(null)) {
			MessagesMixedClusterTest.send(p, records);
		}

		JsonNode at = onLegacyBroker(offsetTools::offsetForTimestamp,
				zkArgs("topic", topic, "timestamp", Timestamps.iso(TestData.BASE_TS + 1_000L)));
		assertThat(at.get("found").asInt()).isEqualTo(1);
		assertThat(at.get("notFound").asInt()).isZero();
		assertThat(at.get("unsupported").asInt()).isEqualTo(2);
		assertThat(at.get("unsupportedNote").asText()).contains("partition [1]").contains("partition [2]");
		assertThat(at.at("/partitions/0/offset").asLong()).isEqualTo(1);
		assertThat(at.at("/partitions/1/offset").isNull()).isTrue();
		assertThat(at.at("/partitions/1/latestOffset").asLong()).isEqualTo(4);
		assertThat(at.at("/partitions/1/error").asText()).contains("does not support").contains("message format");
		assertThat(at.at("/partitions/2/error").asText()).isEqualTo(KafkaErrors.UNSUPPORTED);

		JsonNode window = onLegacyBroker(offsetTools::count,
				zkArgs("topic", topic, "fromTimestamp", Timestamps.iso(TestData.BASE_TS + 1_000L)));
		assertThat(window.get("total").asLong()).isEqualTo(3);
		assertThat(window.get("partial").asBoolean()).isTrue();
		assertThat(window.get("unsupported").asInt()).isEqualTo(2);
		assertThat(window.at("/partitions/0/count").asLong()).isEqualTo(3);
		assertThat(window.at("/partitions/2/count").isNull()).isTrue();
		assertThat(window.at("/partitions/2/error").asText()).contains("version");
		// without time bounds (earliest/latest) the result is complete on the old broker too
		JsonNode plain = onLegacyBroker(offsetTools::count, zkArgs("topic", topic));
		assertThat(plain.get("total").asLong()).isEqualTo(12);
		assertThat(plain.has("partial")).isFalse();

		JsonNode consumed = onLegacyBroker(consumeTools::consume, zkArgs("topic", topic, "from", "timestamp",
				"timestamp", Timestamps.iso(TestData.BASE_TS + 1_000L)));
		assertThat(values(consumed)).containsExactly("p0-1", "p0-2", "p0-3");
		assertThat(consumed.get("partial").asBoolean()).isTrue();
		assertThat(consumed.get("notes").toString()).contains("partition [1]")
			.contains("partition [2]")
			.contains("does not support");
		// unread partitions do not count as read/finished: reachedEnd is false, partitions counts only those read
		assertThat(consumed.get("reachedEnd").asBoolean()).isFalse();
		assertThat(consumed.get("partitions").asInt()).isEqualTo(1);
		assertThat(consumed.get("skippedPartitions").toString()).isEqualTo("[1,2]");
		// a single requested partition that could not be read gets nextOffset null (no jump to latest that would miss it)
		JsonNode skipped = onLegacyBroker(consumeTools::consume, zkArgs("topic", topic, "partition", 1, "from",
				"timestamp", "timestamp", Timestamps.iso(TestData.BASE_TS + 1_000L)));
		assertThat(skipped.get("returned").asInt()).isZero();
		assertThat(skipped.get("reachedEnd").asBoolean()).isFalse();
		assertThat(skipped.has("nextOffset")).isTrue();
		assertThat(skipped.get("nextOffset").isNull()).isTrue();
		assertThat(skipped.get("skippedPartitions").toString()).isEqualTo("[1]");
		assertThat(skipped.get("partial").asBoolean()).isTrue();
		JsonNode readable = onLegacyBroker(consumeTools::consume, zkArgs("topic", topic, "partition", 0, "from",
				"timestamp", "timestamp", Timestamps.iso(TestData.BASE_TS + 1_000L)));
		assertThat(values(readable)).containsExactly("p0-1", "p0-2", "p0-3");
		assertThat(readable.get("reachedEnd").asBoolean()).isTrue();
		assertThat(readable.get("nextOffset").asLong()).isEqualTo(4);
		assertThat(readable.has("skippedPartitions")).isFalse();
		assertThat(readable.has("partial")).isFalse();
		JsonNode latest = onLegacyBroker(consumeTools::consume, zkArgs("topic", topic, "maxRecords", 50));
		assertThat(latest.get("returned").asInt()).isEqualTo(12);
		assertThat(latest.has("partial")).isFalse();
	}


	@Test
	void negativeTimestampsAreRejectedInsteadOfBecomingListOffsetsSentinels() throws Exception {
		String topic = unique("mx-neg");
		createZkTopic(new NewTopic(topic, 1, (short) 1));
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			records.add(TestData.record(topic, 0, TestData.BASE_TS + i * 1000L, null, "n" + i));
		}
		try (KafkaProducer<byte[], byte[]> p = ZkKafkaTestCluster.producer(null)) {
			MessagesMixedClusterTest.send(p, records);
		}
		// -1/-2/-3 mean LATEST/EARLIEST/MAX_TIMESTAMP in listOffsets: they must not be sent as times
		for (String ts : List.of("-1", "-2", "-3", "1969-12-31T23:59:59.997Z")) {
			assertThat(error("messages_offset_for_timestamp", zkArgs("topic", topic, "timestamp", ts)))
				.contains("negative")
				.contains("1970");
		}
		assertThat(error("messages_count", zkArgs("topic", topic, "fromTimestamp", "-1"))).contains("negative");
		assertThat(error("messages_count", zkArgs("topic", topic, "toTimestamp", "-2"))).contains("negative");
		assertThat(error("messages_consume", zkArgs("topic", topic, "timestamp", "-3"))).contains("negative");
		// 0 (the epoch start) is a valid time: the first record
		JsonNode zero = ok("messages_offset_for_timestamp", zkArgs("topic", topic, "timestamp", "0"));
		assertThat(zero.at("/partitions/0/offset").asLong()).isZero();
		assertThat(zero.at("/partitions/0/timestamp").asText()).isEqualTo(Timestamps.iso(TestData.BASE_TS));
		JsonNode count = ok("messages_count", zkArgs("topic", topic, "fromTimestamp", "1970-01-01T00:00:00Z"));
		assertThat(count.get("total").asLong()).isEqualTo(3);
	}

	/**
	 * Record without a timestamp: records in the old (v0) message format reach the consumer with timestamp = -1.
	 * Since 3.x+ brokers do not write v0, the same situation is set up with a record carrying timestamp = -1
	 * (RecordBatch.NO_TIMESTAMP); ProducerRecord rejects negative times, so the field is set directly (the broker
	 * accepts NO_TIMESTAMP).
	 */
	private static ProducerRecord<byte[], byte[]> untimed(String topic, int partition, String value) throws Exception {
		ProducerRecord<byte[], byte[]> r = TestData.record(topic, partition, null, null, value);
		Field f = ProducerRecord.class.getDeclaredField("timestamp");
		f.setAccessible(true);
		f.set(r, RecordBatch.NO_TIMESTAMP);
		r.headers().add(new RecordHeader("p", String.valueOf(partition).getBytes()));
		return r;
	}

	@Test
	void latestPicksFairlyFromPartitionTailsWhenRecordsHaveNoTimestamp() throws Exception {
		String topic = unique("mx-notime");
		createZkTopic(new NewTopic(topic, 3, (short) 1));
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (int p = 0; p < 3; p++) {
			for (int j = 0; j < 10; j++) {
				records.add(untimed(topic, p, "{\"p" + p + "\":" + j + "}"));
			}
		}
		try (KafkaProducer<byte[], byte[]> p = ZkKafkaTestCluster.producer(null)) {
			MessagesMixedClusterTest.send(p, records);
		}

		JsonNode r = ok("messages_consume", zkArgs("topic", topic, "maxRecords", 6));
		assertThat(r.at("/records/0").has("timestamp")).isFalse();
		// not the last 6 records of the highest-numbered partition, but the last 2 records of each partition
		assertThat(values(r)).containsExactly("{\"p0\":8}", "{\"p0\":9}", "{\"p1\":8}", "{\"p1\":9}",
				"{\"p2\":8}", "{\"p2\":9}");
		assertThat(r.get("order").asText()).isEqualTo("partition,offset");
		assertThat(r.get("approximate").asBoolean()).isTrue();
		assertThat(r.get("notes").toString()).contains("no timestamp");

		JsonNode filtered = ok("messages_consume",
				zkArgs("topic", topic, "maxRecords", 3, "valueRegex", ":[0-4]}$"));
		assertThat(values(filtered)).containsExactly("{\"p0\":4}", "{\"p1\":4}", "{\"p2\":4}");
		assertThat(filtered.get("notes").toString()).contains("no timestamp");

		JsonNode headers = ok("messages_headers", zkArgs("topic", topic, "maxRecords", 3));
		assertThat(headers.at("/headers/0/key").asText()).isEqualTo("p");
		assertThat(headers.at("/headers/0/distinctValues").asInt()).isEqualTo(3);

		JsonNode shape = ok("messages_sample_schema", zkArgs("topic", topic, "maxRecords", 3));
		List<String> paths = new ArrayList<>();
		shape.get("paths").forEach(n -> paths.add(n.get("path").asText()));
		assertThat(paths).containsExactlyInAnyOrder("p0", "p1", "p2");

		// a timed record counts as newer than the untimed ones (left over from the old format)
		try (KafkaProducer<byte[], byte[]> p = ZkKafkaTestCluster.producer(null)) {
			MessagesMixedClusterTest.send(p, List.of(TestData.record(topic, 0, TestData.BASE_TS, null, "timed")));
		}
		JsonNode mixed = ok("messages_consume", zkArgs("topic", topic, "maxRecords", 4));
		assertThat(mixed.get("returned").asInt()).isEqualTo(4);
		assertThat(mixed.at("/records/3/value").asText()).isEqualTo("timed");
		assertThat(mixed.at("/records/3/timestamp").asText()).isEqualTo(Timestamps.iso(TestData.BASE_TS));
		assertThat(mixed.at("/records/0").has("timestamp")).isFalse();
		assertThat(mixed.get("order").asText()).isEqualTo("timestamp");
		assertThat(mixed.get("approximate").asBoolean()).isTrue();
	}

}
