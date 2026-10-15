package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolRegistry;
import io.github.srdrcn.kafkamcp.support.MixedClusterIntegrationTest;
import io.github.srdrcn.kafkamcp.support.ZkKafkaTestCluster;

/**
 * ZooKeeper modlu cluster'da ({@code zk}) gerçekçi veri durumları: transaction marker'ları, deleteRecords ile
 * ileri alınmış log başlangıcı, LogAppendTime + zstd topic'i; ve zamana göre aramayı desteklemeyen eski
 * ZooKeeper broker'ı (v0 mesaj formatı / eski protokol) simülasyonu: araç düşmez, partition başına not verir.
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
		// offset'ler: c0..c4 = 0..4, commit marker 5, a0..a2 = 6..8 (abort), abort marker 9, c5..c6 = 10..11,
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

		// okuma read_uncommitted: abort edilen kayıtlar da görünür, marker'lar görünmez
		JsonNode all = ok("messages_consume", zkArgs("topic", topic, "from", "earliest", "maxRecords", 50));
		assertThat(values(all)).containsExactly("c0", "c1", "c2", "c3", "c4", "a0", "a1", "a2", "c5", "c6");
		assertThat(all.get("reachedEnd").asBoolean()).isTrue();

		// latest: marker'lar offset kapladığı için pencere geriye genişletilir, yine tam maxRecords kayıt döner
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
			.contains("aralık dışında")
			.contains("earliest=4");
		JsonNode got = ok("messages_get", zkArgs("topic", topic, "partition", 0, "offset", 4));
		assertThat(got.get("value").asText()).isEqualTo("d4");

		JsonNode fromZero = ok("messages_consume",
				zkArgs("topic", topic, "partition", 0, "offset", 0, "maxRecords", 3));
		assertThat(values(fromZero)).containsExactly("d4", "d5", "d6");
		assertThat(fromZero.get("notes").get(0).asText()).contains("earliest'ten küçük");
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
			// producer'ın CreateTime'ı (2 saat önce) broker'da LogAppendTime ile ezilir
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

		// CreateTime'a göre (2 saat önce) değil, broker'ın ekleme zamanına göre aranır
		JsonNode at = ok("messages_offset_for_timestamp",
				zkArgs("topic", topic, "timestamp", Timestamps.iso(TestData.BASE_TS + 3_600_000L)));
		assertThat(at.get("found").asInt()).isEqualTo(1);
		assertThat(at.get("partitions").get(0).get("offset").asLong()).isZero();
	}

	/**
	 * zk Admin'ini saran ve zamana göre listOffsets'i (OffsetSpec.forTimestamp) verilen partition'larda eski
	 * broker gibi reddeden proxy: v0 mesaj formatlı topic (UNSUPPORTED_FOR_MESSAGE_FORMAT) ya da isteği
	 * desteklemeyen protokol sürümü (UnsupportedVersionException). Diğer çağrılar gerçek Admin'e gider.
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

	/** Aracı eski broker simülasyonlu zk Admin'iyle çağırır; deadline'dan önce bitmeli. */
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
		assertThat(at.at("/partitions/1/error").asText()).contains("desteklemiyor").contains("mesaj formatı");
		assertThat(at.at("/partitions/2/error").asText()).isEqualTo("broker bu işlemi desteklemiyor (sürüm)");

		JsonNode window = onLegacyBroker(offsetTools::count,
				zkArgs("topic", topic, "fromTimestamp", Timestamps.iso(TestData.BASE_TS + 1_000L)));
		assertThat(window.get("total").asLong()).isEqualTo(3);
		assertThat(window.get("partial").asBoolean()).isTrue();
		assertThat(window.get("unsupported").asInt()).isEqualTo(2);
		assertThat(window.at("/partitions/0/count").asLong()).isEqualTo(3);
		assertThat(window.at("/partitions/2/count").isNull()).isTrue();
		assertThat(window.at("/partitions/2/error").asText()).contains("sürüm");
		// zaman sınırı yoksa (earliest/latest) eski broker'da da tam sonuç
		JsonNode plain = onLegacyBroker(offsetTools::count, zkArgs("topic", topic));
		assertThat(plain.get("total").asLong()).isEqualTo(12);
		assertThat(plain.has("partial")).isFalse();

		JsonNode consumed = onLegacyBroker(consumeTools::consume, zkArgs("topic", topic, "from", "timestamp",
				"timestamp", Timestamps.iso(TestData.BASE_TS + 1_000L)));
		assertThat(values(consumed)).containsExactly("p0-1", "p0-2", "p0-3");
		assertThat(consumed.get("partial").asBoolean()).isTrue();
		assertThat(consumed.get("notes").toString()).contains("partition [1]")
			.contains("partition [2]")
			.contains("desteklemiyor");
		JsonNode latest = onLegacyBroker(consumeTools::consume, zkArgs("topic", topic, "maxRecords", 50));
		assertThat(latest.get("returned").asInt()).isEqualTo(12);
		assertThat(latest.has("partial")).isFalse();
	}

}
