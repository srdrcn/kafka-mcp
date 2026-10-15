package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
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
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;

/** Shared setup for the consumers tests: topics, data, real consumer groups (subscribe + commit). */
abstract class ConsumersTestBase extends KafkaIntegrationTest {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static KafkaProducer<byte[], byte[]> producer;

	private final List<KafkaConsumer<byte[], byte[]>> consumers = new ArrayList<>();

	@Autowired
	protected KafkaClients clients;

	@Autowired
	protected GroupTools groupTools;

	@Autowired
	protected LagTools lagTools;

	/** ToolCall for calling a handler directly (test cluster, 15 s deadline). */
	protected ToolCall call(String tool, Map<String, Object> args) {
		return new ToolCall(tool, args, clients, Instant.now().plusMillis(KafkaClients.TIMEOUT_MS));
	}

	/** Converts a handler result (Map/List) to a JSON tree. */
	protected static JsonNode json(Object result) {
		return MAPPER.valueToTree(result);
	}

	protected static synchronized KafkaProducer<byte[], byte[]> producer() {
		if (producer == null) {
			// Idempotence off and a single in-flight request, so that the idempotent producer does not loop on
			// OUT_OF_ORDER_SEQUENCE when the first batch on a new topic gets NOT_LEADER.
			producer = KafkaTestCluster.producer(Map.of(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false,
					ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1, ProducerConfig.ACKS_CONFIG, "all"));
		}
		return producer;
	}

	@AfterAll
	static synchronized void closeProducer() {
		if (producer != null) {
			producer.close();
			producer = null;
		}
	}

	@AfterEach
	void closeConsumers() {
		for (KafkaConsumer<byte[], byte[]> c : consumers) {
			try {
				c.close();
			}
			catch (RuntimeException ignored) {
				// test is over
			}
		}
		consumers.clear();
	}

	protected static void createTopic(String topic, int partitions) throws Exception {
		testAdmin().createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get(60, TimeUnit.SECONDS);
		waitUntil(() -> {
			try {
				TopicDescription d = testAdmin().describeTopics(List.of(topic))
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

	/** Writes count messages to the partition (timestamp: baseTs + i*1000, broker time if baseTs < 0). */
	protected static void produce(String topic, int partition, int count, long baseTs) throws Exception {
		List<Future<RecordMetadata>> sent = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			Long ts = baseTs < 0 ? null : baseTs + i * 1000L;
			sent.add(producer().send(new ProducerRecord<>(topic, partition, ts, ("k" + i).getBytes(), ("v" + i).getBytes())));
		}
		producer().flush();
		for (Future<RecordMetadata> f : sent) {
			f.get(60, TimeUnit.SECONDS);
		}
	}

	protected static void produce(String topic, int partition, int count) throws Exception {
		produce(topic, partition, count, -1);
	}

	/** Active consumer that joins the group (polls until it gets an assignment; closed at the end of the test). */
	protected KafkaConsumer<byte[], byte[]> join(String group, String clientId, String topic) {
		KafkaConsumer<byte[], byte[]> c = newGroupConsumer(group, clientId);
		c.subscribe(List.of(topic));
		long deadline = System.currentTimeMillis() + 90_000;
		while (c.assignment().isEmpty()) {
			if (System.currentTimeMillis() > deadline) {
				throw new IllegalStateException("Consumer could not join the group: " + group);
			}
			c.poll(Duration.ofMillis(200));
		}
		return c;
	}

	/**
	 * Joins count consumers subscribed to topics to the same group; polls them in turn until all have an
	 * assignment so that the rebalance completes (closed at the end of the test). clientIds are clientIdPrefix-0,
	 * -1, ...
	 */
	protected List<KafkaConsumer<byte[], byte[]>> joinAll(String group, String clientIdPrefix, int count,
			List<String> topics) {
		List<KafkaConsumer<byte[], byte[]>> members = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			KafkaConsumer<byte[], byte[]> c = newGroupConsumer(group, clientIdPrefix + "-" + i);
			c.subscribe(topics);
			members.add(c);
		}
		long deadline = System.currentTimeMillis() + 90_000;
		while (members.stream().anyMatch(c -> c.assignment().isEmpty()) || state(group) != GroupState.STABLE) {
			if (System.currentTimeMillis() > deadline) {
				throw new IllegalStateException("Consumers could not join the group: " + group);
			}
			members.forEach(c -> c.poll(Duration.ofMillis(100)));
		}
		return members;
	}

	private KafkaConsumer<byte[], byte[]> newGroupConsumer(String group, String clientId) {
		Map<String, Object> p = KafkaTestCluster.clientProperties("admin");
		p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		p.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
		p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p);
		consumers.add(c);
		return c;
	}

	protected static void commit(KafkaConsumer<byte[], byte[]> c, String topic, Map<Integer, Long> offsets) {
		Map<TopicPartition, OffsetAndMetadata> m = new java.util.HashMap<>();
		offsets.forEach((p, o) -> m.put(new TopicPartition(topic, p), new OffsetAndMetadata(o)));
		c.commitSync(m, Duration.ofSeconds(60));
	}

	/** Joins with a real consumer, commits and closes it: the group stays Empty. */
	protected void emptyGroup(String group, String topic, Map<Integer, Long> offsets) {
		KafkaConsumer<byte[], byte[]> c = join(group, group + "-client", topic);
		commit(c, topic, offsets);
		c.close();
		consumers.remove(c);
		waitUntil(() -> state(group) == GroupState.EMPTY, "group did not become Empty: " + group);
	}

	protected static GroupState state(String group) {
		try {
			ConsumerGroupDescription d = testAdmin().describeConsumerGroups(List.of(group))
				.describedGroups()
				.get(group)
				.get(30, TimeUnit.SECONDS);
			return d.groupState();
		}
		catch (Exception e) {
			return null;
		}
	}

	/** partition → committed offset (via testAdmin). */
	protected static Map<Integer, Long> committed(String group, String topic) throws Exception {
		Map<TopicPartition, OffsetAndMetadata> m = testAdmin().listConsumerGroupOffsets(group)
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

	/** Offset commit via admin (for groups without members). */
	protected static void alter(String group, String topic, Map<Integer, Long> offsets) throws Exception {
		Map<TopicPartition, OffsetAndMetadata> m = new java.util.HashMap<>();
		offsets.forEach((p, o) -> m.put(new TopicPartition(topic, p), new OffsetAndMetadata(o)));
		testAdmin().alterConsumerGroupOffsets(group, m).all().get(30, TimeUnit.SECONDS);
	}

	protected static void waitUntil(BooleanSupplier condition, String message) {
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

	/** Runs a task in the background (e.g. producing while a tool call runs); stops on close(). */
	protected static final class Background implements AutoCloseable {

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
				throw new AssertionError("Background task failed", failure);
			}
		}

	}

	@FunctionalInterface
	protected interface ThrowingRunnable {

		void run() throws Exception;

	}

}
