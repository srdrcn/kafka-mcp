package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.support.MixedClusterIntegrationTest;
import io.github.srdrcn.kafkamcp.support.ZkKafkaTestCluster;

/**
 * The messages tools on the ZooKeeper-mode cluster ({@code zk}, Kafka 3.9) and the KRaft cluster ({@code test},
 * Kafka 4.1). The data of the zk tests exists only on zk (or differs from KRaft): the test fails if a call goes to
 * KRaft by mistake. Since the behaviour does not depend on the mode, identical data on both clusters
 * ({@link #same}) gives identical answers.
 */
class MessagesMixedClusterTest extends MixedClusterIntegrationTest {

	/**
	 * Only on zk, 3 partitions. i = 0..29: partition i%3, time BASE+i s, key k{i%5}, JSON value
	 * {id, level (i%4==0 ? ERROR : INFO), user.name, tags (even i)}; header trace=t-i (even i), source=svc-a
	 * (i%3==0). Plus a tombstone on p0 at BASE+30 s (key k-del). Offsets: p0 0..10, p1 0..9, p2 0..9.
	 */
	private static String topic;

	/**
	 * Records sent with a key and no partition (default partitioner). The same name on both clusters with different
	 * partition counts: 4 on zk, 6 on KRaft.
	 */
	private static String keyed;

	/** The partition the producer picked per key on zk. */
	private static Map<String, Integer> zkKeyPartitions;

	/** The partition the producer picked per key on KRaft. */
	private static Map<String, Integer> kraftKeyPartitions;

	/** Identical data on both clusters (same structure as {@link #topic}). */
	private static String same;

	/** Only on KRaft. */
	private static String kraftOnly;

	@BeforeAll
	static void prepare() throws Exception {
		topic = unique("mx-msg");
		keyed = unique("mx-keyed");
		same = unique("mx-same");
		kraftOnly = unique("mx-kraft");
		zkAdmin()
			.createTopics(List.of(new NewTopic(topic, 3, (short) 1), new NewTopic(keyed, 4, (short) 1),
					new NewTopic(same, 3, (short) 1)))
			.all()
			.get(60, TimeUnit.SECONDS);
		testAdmin()
			.createTopics(List.of(new NewTopic(same, 3, (short) 1), new NewTopic(keyed, 6, (short) 1),
					new NewTopic(kraftOnly, 1, (short) 1)))
			.all()
			.get(60, TimeUnit.SECONDS);
		try (KafkaProducer<byte[], byte[]> p = ZkKafkaTestCluster.producer(null)) {
			send(p, records(topic));
			send(p, records(same));
			zkKeyPartitions = sendKeyed(p);
		}
		try (KafkaProducer<byte[], byte[]> p = KafkaTestCluster.producer(null)) {
			send(p, records(same));
			kraftKeyPartitions = sendKeyed(p);
			send(p, List.of(TestData.record(kraftOnly, 0, null, "k", "v")));
		}
		assertThat(kraftKeyPartitions).as("different partition counts: some keys must map to different partitions")
			.isNotEqualTo(zkKeyPartitions);
	}

	static String json(int i) {
		return "{\"id\":" + i + ",\"level\":\"" + (i % 4 == 0 ? "ERROR" : "INFO") + "\",\"user\":{\"name\":\"u"
				+ (i % 4) + "\"}" + (i % 2 == 0 ? ",\"tags\":[\"a\",\"b" + i + "\"]" : "") + "}";
	}

	private static List<ProducerRecord<byte[], byte[]>> records(String topic) {
		List<ProducerRecord<byte[], byte[]>> out = new ArrayList<>();
		for (int i = 0; i < 30; i++) {
			ProducerRecord<byte[], byte[]> r = TestData.record(topic, i % 3, TestData.BASE_TS + i * 1000L, "k" + (i % 5),
					json(i));
			if (i % 2 == 0) {
				r.headers().add(new RecordHeader("trace", ("t-" + i).getBytes(StandardCharsets.UTF_8)));
			}
			if (i % 3 == 0) {
				r.headers().add(new RecordHeader("source", "svc-a".getBytes(StandardCharsets.UTF_8)));
			}
			out.add(r);
		}
		out.add(TestData.record(topic, 0, TestData.BASE_TS + 30_000L, "k-del", null));
		return out;
	}

	static List<RecordMetadata> send(KafkaProducer<byte[], byte[]> producer, List<ProducerRecord<byte[], byte[]>> records)
			throws Exception {
		List<Future<RecordMetadata>> futures = new ArrayList<>();
		for (ProducerRecord<byte[], byte[]> r : records) {
			futures.add(producer.send(r));
		}
		producer.flush();
		List<RecordMetadata> out = new ArrayList<>();
		for (Future<RecordMetadata> f : futures) {
			out.add(f.get(60, TimeUnit.SECONDS));
		}
		return out;
	}

	private static Map<String, Integer> sendKeyed(KafkaProducer<byte[], byte[]> producer) throws Exception {
		List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
		for (int i = 0; i < 12; i++) {
			records.add(new ProducerRecord<>(keyed, TestData.utf8("user-" + i), TestData.utf8("v" + i)));
		}
		List<RecordMetadata> md = send(producer, records);
		Map<String, Integer> out = new LinkedHashMap<>();
		for (int i = 0; i < md.size(); i++) {
			out.put("user-" + i, md.get(i).partition());
		}
		return out;
	}

	static Set<String> groupIds(Admin admin) throws Exception {
		return admin.listGroups().all().get(30, TimeUnit.SECONDS).stream().map(GroupListing::groupId)
			.collect(Collectors.toSet());
	}

	private static List<Integer> ids(JsonNode r) {
		List<Integer> out = new ArrayList<>();
		r.get("records").forEach(n -> out.add(n.get("value").isNull() ? -1 : n.get("value").get("id").asInt()));
		return out;
	}

	@Test
	void consumeLatestAndEarliestOnZookeeperClusterWithoutGroups() throws Exception {
		Set<String> groupsBefore = groupIds(zkAdmin());
		JsonNode r = ok("messages_consume", zkArgs("topic", topic, "maxRecords", 5, "format", "json"));
		assertThat(r.get("from").asText()).isEqualTo("latest");
		assertThat(r.get("order").asText()).isEqualTo("timestamp");
		assertThat(r.get("partitions").asInt()).isEqualTo(3);
		assertThat(r.get("returned").asInt()).isEqualTo(5);
		assertThat(ids(r)).containsExactly(26, 27, 28, 29, -1);
		assertThat(r.get("reachedEnd").asBoolean()).isTrue();
		assertThat(r.has("partial")).isFalse();
		assertThat(r.has("approximate")).isFalse();
		JsonNode i27 = r.get("records").get(1);
		assertThat(i27.get("partition").asInt()).isZero();
		assertThat(i27.get("offset").asLong()).isEqualTo(9);
		assertThat(i27.get("key").asText()).isEqualTo("k2");
		assertThat(i27.get("timestamp").asText()).isEqualTo(Timestamps.iso(TestData.BASE_TS + 27_000L));
		assertThat(i27.get("timestampType").asText()).isEqualTo("CreateTime");
		assertThat(i27.get("value").get("user").get("name").asText()).isEqualTo("u3");
		assertThat(i27.get("headers").get("source").asText()).isEqualTo("svc-a");
		assertThat(i27.get("valueSize").asInt()).isEqualTo(json(27).length());
		assertThat(r.get("records").get(2).get("headers").get("trace").asText()).isEqualTo("t-28");
		JsonNode tombstone = r.get("records").get(4);
		assertThat(tombstone.get("tombstone").asBoolean()).isTrue();
		assertThat(tombstone.get("key").asText()).isEqualTo("k-del");
		assertThat(tombstone.get("offset").asLong()).isEqualTo(10);

		JsonNode first = ok("messages_consume",
				zkArgs("topic", topic, "from", "earliest", "maxRecords", 6, "format", "json"));
		assertThat(first.get("order").asText()).isEqualTo("partition,offset");
		assertThat(ids(first)).containsExactly(0, 3, 1, 4, 2, 5);
		assertThat(first.get("reachedEnd").asBoolean()).isFalse();
		JsonNode all = ok("messages_consume", zkArgs("topic", topic, "from", "earliest", "maxRecords", 500));
		assertThat(all.get("returned").asInt()).isEqualTo(31);
		assertThat(all.get("reachedEnd").asBoolean()).isTrue();
		// reading without a group or commit: no group is created on the ZooKeeper cluster either
		assertThat(groupIds(zkAdmin())).isSubsetOf(groupsBefore);
	}

	@Test
	void consumeOffsetTimestampFiltersAndFormatsOnZookeeperCluster() {
		JsonNode byOffset = ok("messages_consume", zkArgs("topic", topic, "partition", 1, "from", "offset", "offset", 3,
				"maxRecords", 2, "format", "json"));
		assertThat(ids(byOffset)).containsExactly(10, 13);
		assertThat(byOffset.get("records").get(0).get("offset").asLong()).isEqualTo(3);
		assertThat(byOffset.get("nextOffset").asLong()).isEqualTo(5);
		JsonNode beyond = ok("messages_consume", zkArgs("topic", topic, "partition", 1, "offset", 1000));
		assertThat(beyond.get("from").asText()).isEqualTo("offset");
		assertThat(beyond.get("returned").asInt()).isZero();
		assertThat(beyond.get("notes").get(0).asText()).contains("latest");

		JsonNode byTime = ok("messages_consume", zkArgs("topic", topic, "from", "timestamp", "timestamp",
				Timestamps.iso(TestData.BASE_TS + 24_500L), "format", "json"));
		assertThat(ids(byTime)).containsExactlyInAnyOrder(25, 26, 27, 28, 29, -1);
		assertThat(byTime.get("reachedEnd").asBoolean()).isTrue();
		assertThat(byTime.has("partial")).isFalse();

		JsonNode errors = ok("messages_consume", zkArgs("topic", topic, "from", "earliest", "valueRegex",
				"\"level\":\"ERROR\"", "format", "json", "maxRecords", 100));
		assertThat(ids(errors)).containsExactlyInAnyOrder(0, 4, 8, 12, 16, 20, 24, 28);
		assertThat(errors.get("scanned").asInt()).isEqualTo(31);
		assertThat(errors.get("matched").asInt()).isEqualTo(8);
		assertThat(errors.get("filters").get("valueRegex").asText()).contains("ERROR");

		JsonNode byKey = ok("messages_consume",
				zkArgs("topic", topic, "from", "earliest", "keyRegex", "^k1$", "format", "json"));
		assertThat(ids(byKey)).containsExactlyInAnyOrder(1, 6, 11, 16, 21, 26);
		JsonNode byHeader = ok("messages_consume", zkArgs("topic", topic, "from", "earliest", "headerKey", "trace",
				"headerValueRegex", "^t-1\\d$", "format", "json"));
		assertThat(ids(byHeader)).containsExactlyInAnyOrder(10, 12, 14, 16, 18);

		JsonNode hex = ok("messages_consume", zkArgs("topic", topic, "partition", 0, "from", "earliest", "maxRecords",
				1, "keyFormat", "hex", "format", "hex", "maxValueChars", 16));
		JsonNode rec = hex.get("records").get(0);
		assertThat(rec.get("key").asText()).isEqualTo("6b30");
		assertThat(rec.get("value").asText()).isEqualTo(RecordDecoder.hex(TestData.utf8(json(0))).substring(0, 16));
		assertThat(rec.get("valueTruncated").asBoolean()).isTrue();

		assertThat(error("messages_consume", zkArgs("topic", unique("mx-missing")))).contains("not found");
		assertThat(error("messages_consume", zkArgs("topic", topic, "partition", 7))).contains("Partition 7 does not exist");
	}

	@Test
	void getAndHeadersOnZookeeperCluster() {
		JsonNode one = ok("messages_get", zkArgs("topic", topic, "partition", 2, "offset", 4, "format", "json"));
		assertThat(one.get("topic").asText()).isEqualTo(topic);
		assertThat(one.get("offset").asLong()).isEqualTo(4);
		assertThat(one.get("value").get("id").asInt()).isEqualTo(14);
		assertThat(one.get("value").get("tags").get(1).asText()).isEqualTo("b14");
		assertThat(one.get("key").asText()).isEqualTo("k4");
		assertThat(one.get("headers").get("trace").asText()).isEqualTo("t-14");
		assertThat(one.has("note")).isFalse();
		JsonNode tombstone = ok("messages_get", zkArgs("topic", topic, "partition", 0, "offset", 10));
		assertThat(tombstone.get("tombstone").asBoolean()).isTrue();
		assertThat(error("messages_get", zkArgs("topic", topic, "partition", 0, "offset", 11)))
			.contains("out of range")
			.contains("0..10");

		JsonNode h = ok("messages_headers", zkArgs("topic", topic, "from", "earliest", "maxRecords", 500));
		assertThat(h.get("recordsRead").asInt()).isEqualTo(31);
		// even i (15) ∪ multiples of 3 (10) = 20 records
		assertThat(h.get("withHeaders").asInt()).isEqualTo(20);
		assertThat(h.get("withoutHeaders").asInt()).isEqualTo(11);
		assertThat(h.get("headerKeys").asInt()).isEqualTo(2);
		JsonNode trace = h.get("headers").get(0);
		assertThat(trace.get("key").asText()).isEqualTo("trace");
		assertThat(trace.get("records").asInt()).isEqualTo(15);
		assertThat(trace.get("distinctValues").asInt()).isEqualTo(15);
		JsonNode source = h.get("headers").get(1);
		assertThat(source.get("key").asText()).isEqualTo("source");
		assertThat(source.get("records").asInt()).isEqualTo(10);
		assertThat(source.get("distinctValues").asInt()).isEqualTo(1);
		assertThat(source.get("samples").get(0).asText()).isEqualTo("svc-a");
	}

	@Test
	void offsetsCountsAndKeyPartitionsOnZookeeperCluster() {
		JsonNode at = ok("messages_offset_for_timestamp",
				zkArgs("topic", topic, "timestamp", Timestamps.iso(TestData.BASE_TS + 5_500L)));
		assertThat(at.get("found").asInt()).isEqualTo(3);
		assertThat(at.has("unsupported")).isFalse();
		for (int p = 0; p < 3; p++) {
			JsonNode row = at.get("partitions").get(p);
			assertThat(row.get("partition").asInt()).isEqualTo(p);
			// the first record in p after 5.5 s is i = 6 + p, offset 2
			assertThat(row.get("offset").asLong()).isEqualTo(2);
			assertThat(row.get("timestamp").asText()).isEqualTo(Timestamps.iso(TestData.BASE_TS + (6 + p) * 1000L));
		}
		JsonNode late = ok("messages_offset_for_timestamp",
				zkArgs("topic", topic, "timestamp", String.valueOf(TestData.BASE_TS + 31_000L)));
		assertThat(late.get("notFound").asInt()).isEqualTo(3);
		assertThat(late.get("note").asText()).contains("latestOffset");
		assertThat(late.get("partitions").get(0).get("offset").isNull()).isTrue();
		assertThat(late.get("partitions").get(0).get("latestOffset").asLong()).isEqualTo(11);
		assertThat(late.get("partitions").get(1).get("latestOffset").asLong()).isEqualTo(10);

		JsonNode all = ok("messages_count", zkArgs("topic", topic));
		assertThat(all.get("total").asLong()).isEqualTo(31);
		assertThat(all.get("approximate").asBoolean()).isTrue();
		assertThat(all.get("partitions").get(0).get("count").asLong()).isEqualTo(11);
		JsonNode window = ok("messages_count", zkArgs("topic", topic, "fromTimestamp",
				Timestamps.iso(TestData.BASE_TS + 10_000L), "toTimestamp", Timestamps.iso(TestData.BASE_TS + 20_000L)));
		// i 10..19: p0 {12,15,18}, p1 {10,13,16,19}, p2 {11,14,17}
		assertThat(window.get("total").asLong()).isEqualTo(10);
		assertThat(window.get("partitions").get(1).get("count").asLong()).isEqualTo(4);
		assertThat(window.has("partial")).isFalse();
		JsonNode offsets = ok("messages_count", zkArgs("topic", topic, "partition", 1, "fromOffset", 2, "toOffset", 5));
		assertThat(offsets.get("total").asLong()).isEqualTo(3);

		Map<String, Integer> computed = new HashMap<>();
		for (String key : zkKeyPartitions.keySet()) {
			JsonNode r = ok("messages_partition_for_key", zkArgs("topic", keyed, "key", key));
			assertThat(r.get("partitionCount").asInt()).isEqualTo(4);
			computed.put(key, r.get("partition").asInt());
		}
		assertThat(computed).as("the tool must find the partition the producer actually picked").isEqualTo(zkKeyPartitions);
		// the same name has 6 partitions on KRaft: the partition count is read from each cluster itself
		Map<String, Integer> kraft = new HashMap<>();
		for (String key : kraftKeyPartitions.keySet()) {
			JsonNode r = ok("messages_partition_for_key", args("topic", keyed, "key", key));
			assertThat(r.get("partitionCount").asInt()).isEqualTo(6);
			kraft.put(key, r.get("partition").asInt());
		}
		assertThat(kraft).isEqualTo(kraftKeyPartitions);
	}

	@Test
	void sampleSchemaAndSerdesOnZookeeperCluster() {
		JsonNode s = ok("messages_sample_schema", zkArgs("topic", topic, "maxRecords", 100));
		assertThat(s.get("format").asText()).isEqualTo("json");
		assertThat(s.get("recordsRead").asInt()).isEqualTo(31);
		assertThat(s.get("parsed").asInt()).isEqualTo(30);
		assertThat(s.get("failed").asInt()).isZero();
		assertThat(s.get("tombstones").asInt()).isEqualTo(1);
		Map<String, JsonNode> paths = new HashMap<>();
		s.get("paths").forEach(p -> paths.put(p.get("path").asText(), p));
		assertThat(paths).containsKeys("id", "level", "user", "user.name", "tags", "tags[]");
		assertThat(paths.get("id").get("types").get("integer").asInt()).isEqualTo(30);
		assertThat(paths.get("tags").get("missingRatio").asDouble()).isEqualTo(0.5);
		// the zk cluster has no schemaRegistryUrl: avro is rejected with a clear error
		assertThat(error("messages_sample_schema", zkArgs("topic", topic, "format", "avro")))
			.contains("schemaRegistryUrl")
			.contains("zk");
		assertThat(error("messages_consume", zkArgs("topic", topic, "format", "avro"))).contains("schemaRegistryUrl");

		JsonNode one = ok("serdes_list", zkArgs());
		assertThat(one.get("clusters")).hasSize(1);
		assertThat(one.get("clusters").get(0).get("name").asText()).isEqualTo(ZK_CLUSTER);
		assertThat(one.get("clusters").get(0).get("avro").asBoolean()).isFalse();
		JsonNode every = ok("serdes_list", Map.of());
		List<String> names = new ArrayList<>();
		every.get("clusters").forEach(c -> names.add(c.get("name").asText()));
		assertThat(names).contains("test", "test-ro", ZK_CLUSTER);
	}

	@Test
	void sameDataGivesSameAnswersOnKraftAndZookeeper() {
		// the calls really go to separate clusters: a topic on one does not exist on the other
		assertThat(error("messages_consume", args("topic", topic))).contains("not found");
		assertThat(error("messages_consume", zkArgs("topic", kraftOnly))).contains("not found");
		assertThat(ok("messages_consume", args("topic", kraftOnly)).get("returned").asInt()).isEqualTo(1);
		List<Object[]> calls = List.of(
				new Object[] { "messages_consume", "topic", same, "maxRecords", 7, "format", "json" },
				new Object[] { "messages_consume", "topic", same, "from", "earliest", "maxRecords", 9, "keyFormat",
						"hex" },
				new Object[] { "messages_consume", "topic", same, "partition", 2, "offset", 4, "maxRecords", 3 },
				new Object[] { "messages_consume", "topic", same, "timestamp",
						Timestamps.iso(TestData.BASE_TS + 12_000L), "maxRecords", 4, "format", "json" },
				new Object[] { "messages_consume", "topic", same, "from", "earliest", "valueRegex", "ERROR",
						"headerKey", "trace" },
				new Object[] { "messages_get", "topic", same, "partition", 1, "offset", 6, "format", "json" },
				new Object[] { "messages_headers", "topic", same, "from", "earliest" },
				new Object[] { "messages_offset_for_timestamp", "topic", same, "timestamp",
						Timestamps.iso(TestData.BASE_TS + 15_500L) },
				new Object[] { "messages_count", "topic", same, "fromTimestamp",
						Timestamps.iso(TestData.BASE_TS + 3_000L), "toTimestamp",
						Timestamps.iso(TestData.BASE_TS + 23_000L) },
				new Object[] { "messages_partition_for_key", "topic", same, "key", "user-7" },
				new Object[] { "messages_sample_schema", "topic", same, "maxRecords", 20 });
		for (Object[] c : calls) {
			String tool = (String) c[0];
			Object[] kv = Arrays.copyOfRange(c, 1, c.length);
			JsonNode kraft = ok(tool, args(kv));
			JsonNode zk = ok(tool, zkArgs(kv));
			assertThat(zk).as("%s %s", tool, Arrays.toString(kv)).isEqualTo(kraft);
		}
	}

}
