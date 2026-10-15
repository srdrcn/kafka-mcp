package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
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

import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;

/** messages testleri için topic oluşturma ve veri üretme yardımcıları. */
final class TestData {

	/** Geçmişte sabit bir başlangıç zamanı (broker'ın gelecek zaman sınırına takılmasın). */
	static final long BASE_TS = Instant.now().minus(2, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS).toEpochMilli();

	private TestData() {
	}

	static void createTopic(Admin admin, String topic, int partitions) throws Exception {
		admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get(30, TimeUnit.SECONDS);
	}

	static byte[] utf8(String s) {
		return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
	}

	static byte[] longBytes(long v) {
		return ByteBuffer.allocate(Long.BYTES).putLong(v).array();
	}

	static ProducerRecord<byte[], byte[]> record(String topic, Integer partition, Long timestamp, String key,
			String value) {
		return new ProducerRecord<>(topic, partition, timestamp, utf8(key), utf8(value));
	}

	/** Kayıtları sırayla gönderir ve onaylarını bekler. */
	static List<RecordMetadata> send(List<ProducerRecord<byte[], byte[]>> records) throws Exception {
		try (KafkaProducer<byte[], byte[]> producer = KafkaTestCluster.producer(null)) {
			List<Future<RecordMetadata>> futures = new ArrayList<>();
			for (ProducerRecord<byte[], byte[]> r : records) {
				futures.add(producer.send(r));
			}
			producer.flush();
			List<RecordMetadata> out = new ArrayList<>();
			for (Future<RecordMetadata> f : futures) {
				out.add(f.get(30, TimeUnit.SECONDS));
			}
			return out;
		}
	}

	static Set<String> groupIds(Admin admin) throws Exception {
		return admin.listGroups()
			.all()
			.get(30, TimeUnit.SECONDS)
			.stream()
			.map(GroupListing::groupId)
			.collect(Collectors.toSet());
	}

}
