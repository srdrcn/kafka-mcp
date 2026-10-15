package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.support.MixedClusterIntegrationTest;
import io.github.srdrcn.kafkamcp.support.ZkKafkaTestCluster;

/**
 * report testleri için ortak hazırlık: KRaft ({@code test}) ve ZooKeeper ({@code zk}) cluster'larında topic,
 * eski/yeni zaman damgalı veri ve commit'li consumer group'lar.
 */
abstract class ReportTestBase extends MixedClusterIntegrationTest {

	static final long DAY_MS = 86_400_000L;

	private static KafkaProducer<byte[], byte[]> kraftProducer;

	private static KafkaProducer<byte[], byte[]> zkProducer;

	/** Hazırlık için cluster'a göre admin (test ya da zk). */
	static Admin admin(boolean zk) {
		return zk ? zkAdmin() : testAdmin();
	}

	/** Araç argümanları; zk true ise cluster=zk. */
	static Map<String, Object> on(boolean zk, Object... keyValues) {
		return zk ? zkArgs(keyValues) : args(keyValues);
	}

	static synchronized KafkaProducer<byte[], byte[]> producer(boolean zk) {
		Map<String, Object> overrides = Map.of(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false,
				ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1, ProducerConfig.ACKS_CONFIG, "all");
		if (zk) {
			if (zkProducer == null) {
				zkProducer = ZkKafkaTestCluster.producer(overrides);
			}
			return zkProducer;
		}
		if (kraftProducer == null) {
			kraftProducer = KafkaTestCluster.producer(overrides);
		}
		return kraftProducer;
	}

	@AfterAll
	static synchronized void closeProducers() {
		if (kraftProducer != null) {
			kraftProducer.close();
			kraftProducer = null;
		}
		if (zkProducer != null) {
			zkProducer.close();
			zkProducer = null;
		}
	}

	/** Topic oluşturur (RF=1) ve tüm partition'ların lideri gelene kadar bekler. */
	static void createTopic(boolean zk, String name, int partitions, Map<String, String> configs) throws Exception {
		createTopics(zk, List.of(new NewTopic(name, partitions, (short) 1).configs(configs)));
	}

	static void createTopics(boolean zk, List<NewTopic> topics) throws Exception {
		Admin admin = admin(zk);
		admin.createTopics(topics).all().get(60, TimeUnit.SECONDS);
		List<String> names = topics.stream().map(NewTopic::name).toList();
		long deadline = System.currentTimeMillis() + 60_000;
		while (true) {
			try {
				Map<String, TopicDescription> d = admin.describeTopics(names).allTopicNames().get(30, TimeUnit.SECONDS);
				boolean ready = d.values()
					.stream()
					.allMatch(t -> t.partitions().stream().allMatch(p -> p.leader() != null && !p.leader().isEmpty()));
				if (ready) {
					return;
				}
			}
			catch (Exception e) {
				// henüz görünmüyor
			}
			if (System.currentTimeMillis() > deadline) {
				throw new IllegalStateException("topic'ler hazır olmadı: " + names);
			}
			Thread.sleep(200);
		}
	}

	/** Partition'a count kayıt yazar; timestamp null ise broker/producer zamanı. */
	static void produce(boolean zk, String topic, int partition, int count, Long timestamp) throws Exception {
		produce(zk, topic, partition, count, timestamp, 10);
	}

	/** Partition'a count kayıt yazar (her biri valueSize bayt). */
	static void produce(boolean zk, String topic, int partition, int count, Long timestamp, int valueSize)
			throws Exception {
		KafkaProducer<byte[], byte[]> p = producer(zk);
		List<Future<RecordMetadata>> sent = new ArrayList<>();
		byte[] value = new byte[valueSize];
		for (int i = 0; i < count; i++) {
			sent.add(p.send(new ProducerRecord<>(topic, partition, timestamp, ("k" + i).getBytes(), value)));
		}
		p.flush();
		for (Future<RecordMetadata> f : sent) {
			f.get(60, TimeUnit.SECONDS);
		}
	}

	/** Grubun topic/partition'daki offset'ini commit eder (grup yoksa Empty durumda oluşur). */
	static void commit(boolean zk, String group, String topic, int partition, long offset) throws Exception {
		admin(zk).alterConsumerGroupOffsets(group, Map.of(new TopicPartition(topic, partition),
				new OffsetAndMetadata(offset))).all().get(30, TimeUnit.SECONDS);
	}

	/** Topic'leri sessizce siler (temizlik). */
	static void deleteTopicsQuietly(boolean zk, List<String> topics) {
		try {
			admin(zk).deleteTopics(topics).all().get(60, TimeUnit.SECONDS);
		}
		catch (Exception ignored) {
			// temizlik
		}
	}

	/** Dizideki öğelerin bir alanının metin değerleri. */
	static List<String> texts(JsonNode array, String field) {
		List<String> out = new ArrayList<>();
		if (array != null) {
			array.forEach(n -> out.add(field == null ? n.asText() : n.path(field).asText()));
		}
		return out;
	}

	/** Dizide alanı verilen değere eşit ilk öğe; yoksa MissingNode. */
	static JsonNode find(JsonNode array, String field, String value) {
		if (array != null) {
			for (JsonNode n : array) {
				if (value.equals(n.path(field).asText())) {
					return n;
				}
			}
		}
		return com.fasterxml.jackson.databind.node.MissingNode.getInstance();
	}

}
