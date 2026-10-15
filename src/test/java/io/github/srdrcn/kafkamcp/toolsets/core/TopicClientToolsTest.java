package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;

class TopicClientToolsTest extends KafkaIntegrationTest {

	@Test
	void topicsConsumersListsGroupsWithLagOnTopic() throws Exception {
		String topic = unique("tc");
		String other = unique("tc-other");
		TestData.createTopic(testAdmin(), topic, 2, Map.of());
		TestData.createTopic(testAdmin(), other, 1, Map.of());
		TestData.produce(topic, 0, 6, null);
		TestData.produce(topic, 1, 4, null);
		String behind = unique("g-behind");
		String done = unique("g-done");
		String unrelated = unique("g-other");
		testAdmin().alterConsumerGroupOffsets(behind, Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(2),
				new TopicPartition(topic, 1), new OffsetAndMetadata(1)))
			.all()
			.get();
		testAdmin().alterConsumerGroupOffsets(done, Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(6)))
			.all()
			.get();
		testAdmin().alterConsumerGroupOffsets(unrelated, Map.of(new TopicPartition(other, 0), new OffsetAndMetadata(0)))
			.all()
			.get();

		JsonNode r = ok("topics_consumers", args("topic", topic));
		assertThat(r.get("topic").asText()).isEqualTo(topic);
		assertThat(r.get("groupsScanned").asInt()).isGreaterThanOrEqualTo(3);
		assertThat(r.get("total").asInt()).isEqualTo(2);
		assertThat(r.get("totalLag").asLong()).isEqualTo(7);
		JsonNode g0 = r.get("groups").get(0);
		assertThat(g0.get("groupId").asText()).isEqualTo(behind);
		assertThat(g0.get("lag").asLong()).isEqualTo(7);
		assertThat(g0.get("partitions").asInt()).isEqualTo(2);
		assertThat(g0.get("state").asText()).isEqualToIgnoringCase("empty");
		assertThat(g0.get("members").asInt()).isZero();
		JsonNode g1 = r.get("groups").get(1);
		assertThat(g1.get("groupId").asText()).isEqualTo(done);
		assertThat(g1.get("lag").asLong()).isZero();

		JsonNode filtered = ok("topics_consumers", args("topic", topic, "filter", "^g-done"));
		assertThat(filtered.get("total").asInt()).isEqualTo(1);

		JsonNode none = ok("topics_consumers", args("topic", other, "filter", "^g-behind"));
		assertThat(none.get("total").asInt()).isZero();
		assertThat(error("topics_consumers", args("topic", unique("missing")))).contains("Topic not found");
	}

	@Test
	void topicsProducersAndTransactionsShowOpenTransaction() throws Exception {
		String topic = unique("tx");
		TestData.createTopic(testAdmin(), topic, 2, Map.of());
		String txId = unique("txid");
		try (KafkaProducer<byte[], byte[]> idempotent = KafkaTestCluster
			.producer(Map.of(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true));
				KafkaProducer<byte[], byte[]> tx = KafkaTestCluster
					.producer(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId))) {
			idempotent.send(new ProducerRecord<>(topic, 1, null, new byte[] { 1 })).get();
			tx.initTransactions();
			tx.beginTransaction();
			tx.send(new ProducerRecord<>(topic, 0, null, new byte[] { 2 })).get();
			tx.flush();

			JsonNode p = ok("topics_producers", args("topic", topic));
			assertThat(p.get("partitionsScanned").asInt()).isEqualTo(2);
			assertThat(p.get("producerCount").asInt()).isEqualTo(2);
			assertThat(p.get("partitionsWithOpenTransaction").asInt()).isEqualTo(1);
			assertThat(p.get("total").asInt()).isEqualTo(2);
			JsonNode open = null;
			for (JsonNode row : p.get("producers")) {
				assertThat(row.get("producerId").asLong()).isNotNegative();
				assertThat(row.get("lastSequence").asInt()).isZero();
				assertThat(row.get("lastTimestamp").asText()).endsWith("Z");
				if (row.has("currentTransactionStartOffset")) {
					open = row;
				}
			}
			assertThat(open).isNotNull();
			assertThat(open.get("partition").asInt()).isZero();
			assertThat(open.get("currentTransactionStartOffset").asLong()).isZero();
			assertThat(ok("topics_producers", args("topic", topic, "limit", 1)).get("producers")).hasSize(1);

			JsonNode t = ok("topics_transactions", args("topic", topic));
			assertThat(t.get("openTransactionsInCluster").asInt()).isPositive();
			JsonNode txn = t.get("openTransactions").get(0);
			assertThat(txn.get("transactionalId").asText()).isEqualTo(txId);
			assertThat(txn.get("state").asText()).isEqualToIgnoringCase("ongoing");
			assertThat(txn.get("partitions").get(0).asInt()).isZero();
			assertThat(txn.get("timeoutMs").asLong()).isPositive();
			assertThat(txn.get("startTime").asText()).endsWith("Z");
			JsonNode part = t.get("partitionsWithOpenTransaction").get(0);
			assertThat(part.get("partition").asInt()).isZero();
			assertThat(part.get("producerId").asLong()).isEqualTo(txn.get("producerId").asLong());
			assertThat(part.get("startOffset").asLong()).isZero();
			assertThat(part.has("hangingSuspect")).isFalse();
			assertThat(t.get("hangingSuspects").asInt()).isZero();

			tx.abortTransaction();
		}
		// after an abort the coordinator can stay in PrepareAbort for a short while
		JsonNode after = ok("topics_transactions", args("topic", topic));
		for (int i = 0; i < 50 && !after.get("openTransactions").isEmpty(); i++) {
			Thread.sleep(200);
			after = ok("topics_transactions", args("topic", topic));
		}
		assertThat(after.get("openTransactions")).isEmpty();
		assertThat(after.get("partitionsWithOpenTransaction")).isEmpty();
	}

	@Test
	void topicsProducersCountsPartitionsWithOpenTransactionNotProducerStates() throws Exception {
		String topic = unique("tx2");
		TestData.createTopic(testAdmin(), topic, 2, Map.of());
		try (KafkaProducer<byte[], byte[]> a = KafkaTestCluster
			.producer(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, unique("txa")));
				KafkaProducer<byte[], byte[]> b = KafkaTestCluster
					.producer(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, unique("txb")))) {
			a.initTransactions();
			b.initTransactions();
			a.beginTransaction();
			b.beginTransaction();
			a.send(new ProducerRecord<>(topic, 0, null, new byte[] { 1 })).get();
			b.send(new ProducerRecord<>(topic, 0, null, new byte[] { 2 })).get();

			JsonNode p = ok("topics_producers", args("topic", topic));
			assertThat(p.get("producerCount").asInt()).isEqualTo(2);
			assertThat(p.get("partitionsWithOpenTransaction").asInt()).isEqualTo(1);

			// two fresh open transactions: no hanging suspects
			JsonNode t = ok("topics_transactions", args("topic", topic));
			assertThat(t.get("partitionsWithOpenTransaction")).hasSize(2);
			assertThat(t.get("hangingSuspects").asInt()).isZero();
			assertThat(t.has("hint")).isFalse();

			a.abortTransaction();
			b.abortTransaction();
		}
	}

}
