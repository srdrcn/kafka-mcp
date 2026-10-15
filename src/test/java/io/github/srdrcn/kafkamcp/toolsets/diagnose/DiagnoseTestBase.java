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
 * Shared setup for the diagnose tests: topics, data, offset commits, real consumers and background production on
 * both cluster types (KRaft "test", ZooKeeper "zk"); helpers for looking up findings.
 */
abstract class DiagnoseTestBase extends MixedClusterIntegrationTest {

	/** The two clusters the tests run on: KRaft and ZooKeeper. */
	static final List<String> CLUSTERS = List.of(TestConfigs.CLUSTER, ZK_CLUSTER);

	private final List<AutoCloseable> closeables = new ArrayList<>();

	@AfterEach
	void closeResources() {
		for (AutoCloseable c : closeables) {
			try {
				c.close();
			}
			catch (Exception ignored) {
				// test finished
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

	/** Arguments with the cluster parameter. */
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
		}, "topic not ready: " + topic);
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
	 * Writes records with a timestamp ageMs in the past (a long-pending backlog: record age as evidence of being stuck).
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

	/** Commit via Admin (group without members: stays Empty). */
	static void commit(String cluster, String group, String topic, Map<Integer, Long> offsets) throws Exception {
		Map<TopicPartition, OffsetAndMetadata> m = new LinkedHashMap<>();
		offsets.forEach((p, o) -> m.put(new TopicPartition(topic, p), new OffsetAndMetadata(o)));
		admin(cluster).alterConsumerGroupOffsets(group, m).all().get(30, TimeUnit.SECONDS);
	}

	/** Keeps writing to the partition in the background (stops at the end of the test). */
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
					// transient error: continue
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
	 * Consumer that joins the group, commits the offset once it gets an assignment and then stops polling (the
	 * heartbeat thread keeps the membership: the group is Stable, the commit does not advance).
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
				throw new IllegalStateException("Consumer could not join the group: " + group);
			}
			c.poll(Duration.ofMillis(200));
		}
		Map<TopicPartition, OffsetAndMetadata> m = new LinkedHashMap<>();
		offsets.forEach((part, o) -> m.put(new TopicPartition(topic, part), new OffsetAndMetadata(o)));
		c.commitSync(m, Duration.ofSeconds(60));
		return c;
	}

	/**
	 * Healthy consumer that commits rarely (like Streams/Connect sink): joins the group, commits its position once,
	 * then keeps consuming but does not commit again during the measurement. Returns when ready.
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
						// wait until the position is known and commit explicitly (commitSync() is a no-op if there is no position yet on the first poll)
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
				// stopped
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
			throw new IllegalStateException("Consumer could not join the group and commit: " + group);
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

	/** Findings with the given code. */
	static List<JsonNode> findings(JsonNode r, String code) {
		List<JsonNode> out = new ArrayList<>();
		r.path("findings").forEach(f -> {
			if (code.equals(f.path("code").asText())) {
				out.add(f);
			}
		});
		return out;
	}

	/** The single finding with the given code (error if absent). */
	static JsonNode finding(JsonNode r, String code) {
		List<JsonNode> f = findings(r, code);
		if (f.isEmpty()) {
			throw new AssertionError("No " + code + " finding: " + r.path("findings"));
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

	/** Severity order of the findings: critical, warning, info. */
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
