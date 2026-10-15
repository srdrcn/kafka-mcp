package io.github.srdrcn.kafkamcp.toolsets.core;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.support.ZkKafkaTestCluster;

/** Helpers for the core tests that create topics and produce data. */
final class TestData {

	private TestData() {
	}

	/** Creates the topic and waits until every partition has a visible leader. */
	static void createTopic(Admin admin, String topic, int partitions, Map<String, String> configs) throws Exception {
		admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1).configs(configs))).all().get();
		awaitTopic(admin, topic, partitions);
	}

	/** Waits until the topic metadata is visible (with the given partition count and leaders). */
	static void awaitTopic(Admin admin, String topic, int partitions) throws Exception {
		long end = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
		while (true) {
			try {
				TopicDescription d = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
				if (d.partitions().size() == partitions && d.partitions().stream().allMatch(p -> p.leader() != null)) {
					return;
				}
			}
			catch (Exception e) {
				if (System.currentTimeMillis() > end) {
					throw e;
				}
			}
			if (System.currentTimeMillis() > end) {
				throw new IllegalStateException("topic not ready: " + topic);
			}
			Thread.sleep(100);
		}
	}

	/**
	 * Retries for at most 20 s while the topic is not yet in the broker metadata (UnknownTopicOrPartition); there
	 * is a short window between the CreateTopics response and the broker applying the metadata.
	 */
	static <T> T retryUnknownTopic(Callable<T> action) throws Exception {
		long end = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
		while (true) {
			try {
				return action.call();
			}
			catch (ExecutionException e) {
				if (!(e.getCause() instanceof UnknownTopicOrPartitionException) || System.currentTimeMillis() > end) {
					throw e;
				}
			}
			Thread.sleep(100);
		}
	}

	/** Writes n messages to the given partition with the given timestamp (null = now). */
	static void produce(String topic, int partition, int count, Long timestamp) throws Exception {
		try (KafkaProducer<byte[], byte[]> p = KafkaTestCluster.producer(null)) {
			produce(p, topic, partition, count, timestamp);
		}
	}

	/** {@link #produce(String, int, int, Long)} against the ZooKeeper-mode test cluster. */
	static void produceZk(String topic, int partition, int count, Long timestamp) throws Exception {
		try (KafkaProducer<byte[], byte[]> p = ZkKafkaTestCluster.producer(null)) {
			produce(p, topic, partition, count, timestamp);
		}
	}

	/** Writes n messages with the given producer (null timestamp = now). */
	static void produce(KafkaProducer<byte[], byte[]> p, String topic, int partition, int count, Long timestamp)
			throws Exception {
		for (int i = 0; i < count; i++) {
			p.send(new ProducerRecord<>(topic, partition, timestamp, ("k" + i).getBytes(StandardCharsets.UTF_8),
					("v" + i).getBytes(StandardCharsets.UTF_8)))
				.get();
		}
	}

}
