package io.github.srdrcn.kafkamcp.toolsets.core;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;

/** core testleri için topic oluşturma ve veri üretme yardımcıları. */
final class TestData {

	private TestData() {
	}

	/** Topic'i oluşturur ve tüm partition'ların lideri görünene kadar bekler. */
	static void createTopic(Admin admin, String topic, int partitions, Map<String, String> configs) throws Exception {
		admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1).configs(configs))).all().get();
		awaitTopic(admin, topic, partitions);
	}

	/** Topic'in metadata'sı (verilen partition sayısıyla, liderli) görünene kadar bekler. */
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
				throw new IllegalStateException("topic hazır olmadı: " + topic);
			}
			Thread.sleep(100);
		}
	}

	/** Belirli partition'a, verilen zaman damgasıyla (null = şimdi) n mesaj yazar. */
	static void produce(String topic, int partition, int count, Long timestamp) throws Exception {
		try (KafkaProducer<byte[], byte[]> p = KafkaTestCluster.producer(null)) {
			for (int i = 0; i < count; i++) {
				p.send(new ProducerRecord<>(topic, partition, timestamp, ("k" + i).getBytes(StandardCharsets.UTF_8),
						("v" + i).getBytes(StandardCharsets.UTF_8)))
					.get();
			}
		}
	}

}
