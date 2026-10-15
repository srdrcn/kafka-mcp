package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;

import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.support.MixedClusterIntegrationTest;
import io.github.srdrcn.kafkamcp.support.TestConfigs;
import io.github.srdrcn.kafkamcp.support.ZkKafkaTestCluster;

/**
 * consumers karışık ortam testleri için cluster'a göre ({@link #ZK} ya da {@link #KRAFT}) hazırlık: topic,
 * veri, gerçek consumer group'lar (classic ya da KIP-848 consumer protokolü), commit ve doğrulama okumaları.
 */
abstract class ConsumersMixedTestBase extends MixedClusterIntegrationTest {

	/** ZooKeeper modlu cluster (Kafka 3.9, AclAuthorizer). */
	static final String ZK = ZK_CLUSTER;

	/** KRaft cluster (Kafka 4.1, StandardAuthorizer). */
	static final String KRAFT = TestConfigs.CLUSTER;

	static final String CLASSIC = "classic";

	/** KIP-848 consumer rebalance protokolü (sadece KRaft). */
	static final String CONSUMER = "consumer";

	private static final Map<String, KafkaProducer<byte[], byte[]>> PRODUCERS = new HashMap<>();

	private final List<KafkaConsumer<byte[], byte[]>> consumers = new ArrayList<>();

	/** Aracın argümanları; cluster açıkça verilir. */
	static Map<String, Object> on(String cluster, Object... keyValues) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("cluster", cluster);
		for (int i = 0; i < keyValues.length; i += 2) {
			m.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
		}
		return m;
	}

	static Admin admin(String cluster) {
		return ZK.equals(cluster) ? zkAdmin() : testAdmin();
	}

	static Map<String, Object> clientProperties(String cluster) {
		return ZK.equals(cluster) ? ZkKafkaTestCluster.clientProperties("admin")
				: KafkaTestCluster.clientProperties("admin");
	}

	static synchronized KafkaProducer<byte[], byte[]> producer(String cluster) {
		return PRODUCERS.computeIfAbsent(cluster, c -> {
			// Yeni topic'te ilk batch NOT_LEADER alınca idempotent producer'ın OUT_OF_ORDER_SEQUENCE
			// döngüsüne girmemesi için idempotence kapalı, tek in-flight istek.
			Map<String, Object> overrides = Map.of(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false,
					ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1, ProducerConfig.ACKS_CONFIG, "all");
			return ZK.equals(c) ? ZkKafkaTestCluster.producer(overrides) : KafkaTestCluster.producer(overrides);
		});
	}

	@AfterAll
	static synchronized void closeProducers() {
		PRODUCERS.values().forEach(KafkaProducer::close);
		PRODUCERS.clear();
	}

	@AfterEach
	void closeConsumers() {
		for (KafkaConsumer<byte[], byte[]> c : consumers) {
			try {
				c.close(Duration.ofSeconds(10));
			}
			catch (RuntimeException ignored) {
				// test bitti
			}
		}
		consumers.clear();
	}

	static void createTopic(String cluster, String topic, int partitions) throws Exception {
		admin(cluster).createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
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
		}, "topic hazır olmadı: " + cluster + "/" + topic);
	}

	/** Partition'a count mesaj yazar (timestamp: baseTs + i*1000, baseTs < 0 ise broker zamanı). */
	static void produce(String cluster, String topic, int partition, int count, long baseTs) throws Exception {
		KafkaProducer<byte[], byte[]> p = producer(cluster);
		List<Future<RecordMetadata>> sent = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			Long ts = baseTs < 0 ? null : baseTs + i * 1000L;
			sent.add(p.send(new ProducerRecord<>(topic, partition, ts, ("k" + i).getBytes(), ("v" + i).getBytes())));
		}
		p.flush();
		for (Future<RecordMetadata> f : sent) {
			f.get(60, TimeUnit.SECONDS);
		}
	}

	static void produce(String cluster, String topic, int partition, int count) throws Exception {
		produce(cluster, topic, partition, count, -1);
	}

	/**
	 * count consumer'ı aynı gruba katar (protocol: classic ya da consumer); hepsi atama alıp grup Stable olana
	 * kadar sırayla poll eder (test sonunda kapanır). clientId'ler clientIdPrefix-0, -1, ...
	 */
	List<KafkaConsumer<byte[], byte[]>> joinAll(String cluster, String protocol, String group, String clientIdPrefix,
			int count, List<String> topics) {
		List<KafkaConsumer<byte[], byte[]>> members = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			KafkaConsumer<byte[], byte[]> c = newGroupConsumer(cluster, protocol, group, clientIdPrefix + "-" + i);
			c.subscribe(topics);
			members.add(c);
		}
		long deadline = System.currentTimeMillis() + 120_000;
		while (members.stream().anyMatch(c -> c.assignment().isEmpty())
				|| state(cluster, group) != GroupState.STABLE) {
			if (System.currentTimeMillis() > deadline) {
				throw new IllegalStateException("Consumer'lar gruba katılamadı: " + cluster + "/" + group);
			}
			members.forEach(c -> c.poll(Duration.ofMillis(100)));
		}
		return members;
	}

	KafkaConsumer<byte[], byte[]> join(String cluster, String protocol, String group, String clientId, String topic) {
		return joinAll(cluster, protocol, group, clientId, 1, List.of(topic)).get(0);
	}

	/** Gruba katılacak (henüz subscribe olmamış) consumer; test sonunda kapanır. */
	KafkaConsumer<byte[], byte[]> newGroupConsumer(String cluster, String protocol, String group, String clientId) {
		Map<String, Object> p = clientProperties(cluster);
		p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		p.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
		p.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol);
		p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p);
		consumers.add(c);
		return c;
	}

	/** Consumer'ı kapatır (gruptan ayrılır) ve listeden çıkarır. */
	void leave(KafkaConsumer<byte[], byte[]> c) {
		c.close(Duration.ofSeconds(30));
		consumers.remove(c);
	}

	static void commit(KafkaConsumer<byte[], byte[]> c, String topic, Map<Integer, Long> offsets) {
		Map<TopicPartition, OffsetAndMetadata> m = new HashMap<>();
		offsets.forEach((p, o) -> m.put(new TopicPartition(topic, p), new OffsetAndMetadata(o)));
		c.commitSync(m, Duration.ofSeconds(60));
	}

	/** Gerçek consumer ile katılıp commit eder ve kapanır: grup Empty kalır (classic ya da consumer tipi). */
	void emptyGroup(String cluster, String protocol, String group, String topic, Map<Integer, Long> offsets) {
		KafkaConsumer<byte[], byte[]> c = join(cluster, protocol, group, group + "-client", topic);
		commit(c, topic, offsets);
		leave(c);
		waitUntil(() -> state(cluster, group) == GroupState.EMPTY, "grup Empty olmadı: " + cluster + "/" + group);
	}

	static ConsumerGroupDescription describe(String cluster, String group) throws Exception {
		return admin(cluster).describeConsumerGroups(List.of(group))
			.describedGroups()
			.get(group)
			.get(30, TimeUnit.SECONDS);
	}

	static GroupState state(String cluster, String group) {
		try {
			return describe(cluster, group).groupState();
		}
		catch (Exception e) {
			return null;
		}
	}

	/** partition → committed offset (cluster'ın admin'iyle). */
	static Map<Integer, Long> committed(String cluster, String group, String topic) throws Exception {
		Map<TopicPartition, OffsetAndMetadata> m = admin(cluster).listConsumerGroupOffsets(group)
			.partitionsToOffsetAndMetadata()
			.get(30, TimeUnit.SECONDS);
		Map<Integer, Long> out = new TreeMap<>();
		m.forEach((tp, om) -> {
			if (tp.topic().equals(topic) && om != null) {
				out.put(tp.partition(), om.offset());
			}
		});
		return out;
	}

	/** Admin ile offset commit'i (üyesi olmayan "simple" gruplar için). */
	static void alter(String cluster, String group, String topic, Map<Integer, Long> offsets) throws Exception {
		Map<TopicPartition, OffsetAndMetadata> m = new HashMap<>();
		offsets.forEach((p, o) -> m.put(new TopicPartition(topic, p), new OffsetAndMetadata(o)));
		admin(cluster).alterConsumerGroupOffsets(group, m).all().get(30, TimeUnit.SECONDS);
	}

	/**
	 * Taze cluster'da __consumer_offsets ilk group isteğiyle oluşturulur; o sırada koordinatör bulunamaz ve
	 * aracın admin'i (retries=3) hemen vazgeçer. Test admin'i (sınırsız retry) ile koordinatörün hazır olması
	 * beklenir.
	 */
	static void awaitGroupCoordinator(String cluster) throws Exception {
		admin(cluster).listConsumerGroupOffsets(unique("warmup"))
			.partitionsToOffsetAndMetadata()
			.get(60, TimeUnit.SECONDS);
	}

	static List<String> groupIds(String cluster) throws Exception {
		return admin(cluster).listGroups(ListGroupsOptions.forConsumerGroups())
			.all()
			.get(30, TimeUnit.SECONDS)
			.stream()
			.map(GroupListing::groupId)
			.toList();
	}

	static void waitUntil(BooleanSupplier condition, String message) {
		long deadline = System.currentTimeMillis() + 90_000;
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

	/** Arka planda iş çalıştırır (ör. araç çağrısı sürerken üretim); close() ile durur. */
	static final class Background implements AutoCloseable {

		private final Thread thread;

		private volatile boolean running = true;

		private volatile Throwable failure;

		Background(ThrowingRunnable step, long pauseMillis) {
			thread = Thread.ofPlatform().daemon().start(() -> {
				while (running) {
					try {
						step.run();
						Thread.sleep(pauseMillis);
					}
					catch (InterruptedException e) {
						return;
					}
					catch (Throwable t) {
						failure = t;
						return;
					}
				}
			});
		}

		@Override
		public void close() throws Exception {
			running = false;
			thread.join(30_000);
			if (failure != null) {
				throw new AssertionError("Arka plan işi hata verdi", failure);
			}
		}

	}

	@FunctionalInterface
	interface ThrowingRunnable {

		void run() throws Exception;

	}

}
