package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterEach;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.support.MixedClusterIntegrationTest;
import io.github.srdrcn.kafkamcp.support.TestConfigs;
import io.github.srdrcn.kafkamcp.support.ZkKafkaTestCluster;

/**
 * diagnose testleri için ortak hazırlık: her iki cluster tipinde (KRaft "test", ZooKeeper "zk") topic, veri,
 * offset commit, gerçek consumer ve arka planda üretim; bulgu arama yardımcıları.
 */
abstract class DiagnoseTestBase extends MixedClusterIntegrationTest {

	/** Testlerin koştuğu iki cluster: KRaft ve ZooKeeper. */
	static final List<String> CLUSTERS = List.of(TestConfigs.CLUSTER, ZK_CLUSTER);

	private final List<AutoCloseable> closeables = new ArrayList<>();

	@AfterEach
	void closeResources() {
		for (AutoCloseable c : closeables) {
			try {
				c.close();
			}
			catch (Exception ignored) {
				// test bitti
			}
		}
		closeables.clear();
	}

	static boolean zk(String cluster) {
		return ZK_CLUSTER.equals(cluster);
	}

	static Admin admin(String cluster) {
		return zk(cluster) ? zkAdmin() : testAdmin();
	}

	static Map<String, Object> clientProperties(String cluster, String user) {
		return zk(cluster) ? ZkKafkaTestCluster.clientProperties(user) : KafkaTestCluster.clientProperties(user);
	}

	/** cluster parametresiyle argümanlar. */
	static Map<String, Object> on(String cluster, Object... keyValues) {
		Map<String, Object> m = args(keyValues);
		m.put("cluster", cluster);
		return m;
	}

	static void createTopic(String cluster, String topic, int partitions, Map<String, String> configs)
			throws Exception {
		admin(cluster).createTopics(List.of(new NewTopic(topic, partitions, (short) 1).configs(configs)))
			.all()
			.get(60, TimeUnit.SECONDS);
		waitUntil(() -> {
			try {
				TopicDescription d = admin(cluster).describeTopics(List.of(topic))
					.allTopicNames()
					.get(30, TimeUnit.SECONDS)
					.get(topic);
				return d.partitions().size() == partitions
						&& d.partitions().stream().allMatch(p -> p.leader() != null && !p.leader().isEmpty());
			}
			catch (Exception e) {
				return false;
			}
		}, "topic hazır olmadı: " + topic);
	}

	static KafkaProducer<byte[], byte[]> producer(String cluster) {
		Map<String, Object> o = Map.of(ProducerConfig.ACKS_CONFIG, "1", ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
				false);
		return zk(cluster) ? ZkKafkaTestCluster.producer(o) : KafkaTestCluster.producer(o);
	}

	static void produce(String cluster, String topic, int partition, int count) throws Exception {
		try (KafkaProducer<byte[], byte[]> p = producer(cluster)) {
			for (int i = 0; i < count; i++) {
				p.send(new ProducerRecord<>(topic, partition, null, ("v" + i).getBytes()));
			}
			p.flush();
		}
	}

	/**
	 * Zaman damgası ageMs önce olan kayıtlar yazar (çoktan beri bekleyen backlog: takılma kanıtı için kayıt yaşı).
	 */
	static void produceAged(String cluster, String topic, int partition, int count, long ageMs) throws Exception {
		long ts = System.currentTimeMillis() - ageMs;
		try (KafkaProducer<byte[], byte[]> p = producer(cluster)) {
			for (int i = 0; i < count; i++) {
				p.send(new ProducerRecord<>(topic, partition, ts + i, null, ("old" + i).getBytes()));
			}
			p.flush();
		}
	}

	/** Admin ile commit (üyesiz grup: Empty kalır). */
	static void commit(String cluster, String group, String topic, Map<Integer, Long> offsets) throws Exception {
		Map<TopicPartition, OffsetAndMetadata> m = new LinkedHashMap<>();
		offsets.forEach((p, o) -> m.put(new TopicPartition(topic, p), new OffsetAndMetadata(o)));
		admin(cluster).alterConsumerGroupOffsets(group, m).all().get(30, TimeUnit.SECONDS);
	}

	/** Arka planda partition'a sürekli yazar (test sonunda durur). */
	void produceInBackground(String cluster, String topic, int partition) {
		KafkaProducer<byte[], byte[]> p = producer(cluster);
		Thread t = Thread.ofPlatform().daemon().start(() -> {
			while (!Thread.currentThread().isInterrupted()) {
				try {
					p.send(new ProducerRecord<>(topic, partition, null, "bg".getBytes())).get(30, TimeUnit.SECONDS);
					Thread.sleep(50);
				}
				catch (InterruptedException e) {
					return;
				}
				catch (Exception e) {
					// geçici hata: devam
				}
			}
		});
		closeables.add(() -> {
			t.interrupt();
			t.join(10_000);
			p.close(Duration.ofSeconds(5));
		});
	}

	/**
	 * Gruba katılan, ataması gelince offset'i commit eden ve sonra poll etmeyen consumer (heartbeat thread'i
	 * üyeliği korur: grup Stable, commit ilerlemez).
	 */
	KafkaConsumer<byte[], byte[]> joinAndCommit(String cluster, String group, String topic, Map<Integer, Long> offsets) {
		Map<String, Object> p = clientProperties(cluster, "admin");
		p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		p.put(ConsumerConfig.CLIENT_ID_CONFIG, group + "-client");
		p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
		p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p);
		closeables.add(0, c);
		c.subscribe(List.of(topic));
		long deadline = System.currentTimeMillis() + 90_000;
		while (c.assignment().isEmpty()) {
			if (System.currentTimeMillis() > deadline) {
				throw new IllegalStateException("Consumer gruba katılamadı: " + group);
			}
			c.poll(Duration.ofMillis(200));
		}
		Map<TopicPartition, OffsetAndMetadata> m = new LinkedHashMap<>();
		offsets.forEach((part, o) -> m.put(new TopicPartition(topic, part), new OffsetAndMetadata(o)));
		c.commitSync(m, Duration.ofSeconds(60));
		return c;
	}

	/**
	 * Sağlıklı ama seyrek commit eden consumer (Streams/Connect sink gibi): gruba katılır, konumunu bir kez commit
	 * eder, sonra durmadan tüketir ama ölçüm boyunca bir daha commit etmez. Hazır olunca döner.
	 */
	void consumeWithRareCommits(String cluster, String group, String topic) throws InterruptedException {
		Map<String, Object> p = clientProperties(cluster, "admin");
		p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		p.put(ConsumerConfig.CLIENT_ID_CONFIG, group + "-client");
		p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p);
		CountDownLatch committed = new CountDownLatch(1);
		AtomicBoolean stop = new AtomicBoolean();
		Thread t = Thread.ofPlatform().daemon().start(() -> {
			try {
				c.subscribe(List.of(topic));
				while (!stop.get()) {
					c.poll(Duration.ofMillis(200));
					if (committed.getCount() > 0 && !c.assignment().isEmpty()) {
						// konum bilinene kadar bekleyip açıkça commit (ilk poll'da konum henüz yoksa commitSync() boş geçer)
						Map<TopicPartition, OffsetAndMetadata> m = new LinkedHashMap<>();
						for (TopicPartition tp : c.assignment()) {
							m.put(tp, new OffsetAndMetadata(c.position(tp, Duration.ofSeconds(60))));
						}
						c.commitSync(m, Duration.ofSeconds(60));
						committed.countDown();
					}
				}
			}
			catch (WakeupException e) {
				// durduruldu
			}
			finally {
				c.close(Duration.ofSeconds(5));
			}
		});
		closeables.add(0, () -> {
			stop.set(true);
			c.wakeup();
			t.join(15_000);
		});
		if (!committed.await(90, TimeUnit.SECONDS)) {
			throw new IllegalStateException("Consumer gruba katılıp commit edemedi: " + group);
		}
	}

	static void waitUntil(BooleanSupplier condition, String message) {
		long deadline = System.currentTimeMillis() + 60_000;
		while (!condition.getAsBoolean()) {
			if (System.currentTimeMillis() > deadline) {
				throw new AssertionError(message);
			}
			try {
				Thread.sleep(200);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new AssertionError(message);
			}
		}
	}

	/** Verilen koddaki bulgular. */
	static List<JsonNode> findings(JsonNode r, String code) {
		List<JsonNode> out = new ArrayList<>();
		r.path("findings").forEach(f -> {
			if (code.equals(f.path("code").asText())) {
				out.add(f);
			}
		});
		return out;
	}

	/** Verilen koddaki tek bulgu (yoksa hata). */
	static JsonNode finding(JsonNode r, String code) {
		List<JsonNode> f = findings(r, code);
		if (f.isEmpty()) {
			throw new AssertionError(code + " bulgusu yok: " + r.path("findings"));
		}
		return f.get(0);
	}

	static List<String> codes(JsonNode r) {
		List<String> out = new ArrayList<>();
		r.path("findings").forEach(f -> out.add(f.path("code").asText()));
		return out;
	}

	static List<String> texts(JsonNode array) {
		List<String> out = new ArrayList<>();
		if (array != null) {
			array.forEach(n -> out.add(n.isValueNode() ? n.asText() : n.toString()));
		}
		return out;
	}

	/** Bulguların önem sırası: critical, warning, info. */
	static boolean sortedBySeverity(JsonNode r) {
		List<String> order = List.of("critical", "warning", "info");
		int last = 0;
		for (JsonNode f : r.path("findings")) {
			int i = order.indexOf(f.path("severity").asText());
			if (i < last) {
				return false;
			}
			last = i;
		}
		return true;
	}

}
