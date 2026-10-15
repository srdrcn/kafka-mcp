package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;

class TopicWriteToolsTest extends KafkaIntegrationTest {

	@Test
	void topicsCreateCreatesTopicWithConfigs() throws Exception {
		String topic = unique("create");
		JsonNode r = ok("topics_create", args("topic", topic, "partitions", 3, "replicationFactor", 1, "configs",
				Map.of("retention.ms", "60000")));
		assertThat(r.get("topic").asText()).isEqualTo(topic);
		assertThat(r.get("partitions").asInt()).isEqualTo(3);
		assertThat(r.get("replicationFactor").asInt()).isEqualTo(1);
		assertThat(r.get("topicId").asText()).isNotBlank();
		assertThat(r.at("/configs/retention.ms").asText()).isEqualTo("60000");
		TopicDescription d = describe(topic);
		assertThat(d.partitions()).hasSize(3);
		assertThat(topicConfig(topic, "retention.ms").value()).isEqualTo("60000");

		String defaults = unique("create-default");
		JsonNode def = ok("topics_create", args("topic", defaults));
		assertThat(def.get("partitions").asInt()).isEqualTo(1);
		assertThat(def.has("configs")).isFalse();
		assertThat(describe(defaults).partitions()).hasSize(1);

		assertThat(error("topics_create", args("topic", topic))).contains("Topic zaten var");
		assertThat(error("topics_create", args("topic", unique("rf"), "replicationFactor", 3)))
			.contains("broker sayısından (1) büyük olamaz");
		assertThat(error("topics_create", args("cluster", "test-ro", "topic", unique("ro")))).contains("readOnly");
	}

	@Test
	void topicsCloneCopiesStructureAndTopicConfigs() throws Exception {
		String source = unique("clone-src");
		TestData.createTopic(testAdmin(), source, 4,
				Map.of("retention.ms", "120000", "cleanup.policy", "compact,delete"));
		TestData.produce(source, 0, 2, null);
		String target = unique("clone-dst");
		JsonNode r = ok("topics_clone",
				args("source", source, "target", target, "configs", Map.of("retention.ms", "240000")));
		assertThat(r.get("topic").asText()).isEqualTo(target);
		assertThat(r.get("source").asText()).isEqualTo(source);
		assertThat(r.get("partitions").asInt()).isEqualTo(4);
		assertThat(r.get("replicationFactor").asInt()).isEqualTo(1);
		assertThat(r.at("/copiedConfigs/retention.ms").asText()).isEqualTo("120000");
		assertThat(r.at("/copiedConfigs/cleanup.policy").asText()).isEqualTo("compact,delete");
		assertThat(r.at("/overriddenConfigs/retention.ms").asText()).isEqualTo("240000");
		assertThat(r.get("note").asText()).contains("kopyalanmadı");
		assertThat(describe(target).partitions()).hasSize(4);
		assertThat(topicConfig(target, "retention.ms").value()).isEqualTo("240000");
		assertThat(topicConfig(target, "cleanup.policy").value()).isEqualTo("compact,delete");
		long messages = testAdmin()
			.listOffsets(Map.of(new org.apache.kafka.common.TopicPartition(target, 0),
					org.apache.kafka.clients.admin.OffsetSpec.latest()))
			.all()
			.get()
			.values()
			.iterator()
			.next()
			.offset();
		assertThat(messages).isZero();

		JsonNode wider = ok("topics_clone", args("source", source, "target", unique("clone-wide"), "partitions", 6));
		assertThat(wider.get("partitions").asInt()).isEqualTo(6);
		assertThat(error("topics_clone", args("source", unique("yok"), "target", unique("x"))))
			.contains("Topic bulunamadı");
	}

	@Test
	void topicsConfigUpdateReturnsBeforeAndAfter() throws Exception {
		String topic = unique("cfg");
		TestData.createTopic(testAdmin(), topic, 1, Map.of("retention.ms", "100000"));
		JsonNode r = ok("topics_config_update",
				args("topic", topic, "set", Map.of("retention.ms", "200000", "max.message.bytes", "2000000")));
		assertThat(r.get("topic").asText()).isEqualTo(topic);
		JsonNode retention = ClusterBrokerToolsTest.find(r.get("changes"), "retention.ms");
		assertThat(retention.get("before").asText()).isEqualTo("100000");
		assertThat(retention.get("after").asText()).isEqualTo("200000");
		assertThat(retention.get("afterSource").asText()).isEqualTo("DYNAMIC_TOPIC_CONFIG");
		JsonNode maxBytes = ClusterBrokerToolsTest.find(r.get("changes"), "max.message.bytes");
		assertThat(maxBytes.get("beforeSource").asText()).isNotEqualTo("DYNAMIC_TOPIC_CONFIG");
		assertThat(maxBytes.get("after").asText()).isEqualTo("2000000");
		assertThat(topicConfig(topic, "retention.ms").value()).isEqualTo("200000");
		assertThat(topicConfig(topic, "max.message.bytes").value()).isEqualTo("2000000");

		JsonNode del = ok("topics_config_update", args("topic", topic, "delete", List.of("retention.ms")));
		assertThat(del.at("/changes/0/op").asText()).isEqualTo("delete");
		assertThat(del.at("/changes/0/before").asText()).isEqualTo("200000");
		assertThat(del.at("/changes/0/after").asText()).isEqualTo("604800000");
		assertThat(topicConfig(topic, "retention.ms").source()).isNotEqualTo(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);

		assertThat(error("topics_config_update", args("topic", topic))).contains("set ya da delete");
		assertThat(error("topics_config_update", args("topic", unique("yok"), "set", Map.of("retention.ms", "1"))))
			.contains("Topic bulunamadı");
		assertThat(error("topics_config_update",
				args("cluster", "test-ro", "topic", topic, "set", Map.of("retention.ms", "1"))))
			.contains("readOnly");
		assertThat(topicConfig(topic, "retention.ms").value()).isEqualTo("604800000");
	}

	@Test
	void topicsPartitionsIncreaseAddsPartitions() throws Exception {
		String topic = unique("grow");
		TestData.createTopic(testAdmin(), topic, 2, Map.of());
		JsonNode r = ok("topics_partitions_increase", args("topic", topic, "totalCount", 5));
		assertThat(r.get("before").asInt()).isEqualTo(2);
		assertThat(r.get("after").asInt()).isEqualTo(5);
		assertThat(r.get("warning").asText()).contains("key");
		TestData.awaitTopic(testAdmin(), topic, 5);
		assertThat(describe(topic).partitions()).hasSize(5);
		assertThat(error("topics_partitions_increase", args("topic", topic, "totalCount", 5)))
			.contains("büyük olmalı");
		assertThat(error("topics_partitions_increase", args("cluster", "test-ro", "topic", topic, "totalCount", 9)))
			.contains("readOnly");
	}

	@Test
	void topicsDeleteDeletesAndRefusesSystemTopics() throws Exception {
		String topic = unique("del");
		TestData.createTopic(testAdmin(), topic, 1, Map.of());
		JsonNode r = ok("topics_delete", args("topic", topic));
		assertThat(r.get("deleted").asText()).isEqualTo(topic);
		assertThat(r.get("partitions").asInt()).isEqualTo(1);
		assertThat(testAdmin().listTopics().names().get()).doesNotContain(topic);

		String connectConfigs = unique("my") + "-connect-configs";
		TestData.createTopic(testAdmin(), connectConfigs, 1, Map.of());
		assertThat(error("topics_delete", args("topic", connectConfigs))).contains("reddedildi");
		assertThat(testAdmin().listTopics().names().get()).contains(connectConfigs);
		String confluent = "_confluent-" + unique("metrics");
		TestData.createTopic(testAdmin(), confluent, 1, Map.of());
		assertThat(error("topics_delete", args("topic", confluent))).contains("reddedildi");
		String internal = "__" + unique("internal");
		TestData.createTopic(testAdmin(), internal, 1, Map.of());
		assertThat(error("topics_delete", args("topic", internal))).contains("iç topic");

		assertThat(error("topics_delete", args("topic", unique("yok")))).contains("Topic bulunamadı");
		assertThat(error("topics_delete", args("cluster", "test-ro", "topic", connectConfigs))).contains("readOnly");
	}

	private static TopicDescription describe(String topic) throws Exception {
		return testAdmin().describeTopics(List.of(topic)).allTopicNames().get().get(topic);
	}

	private static ConfigEntry topicConfig(String topic, String key) throws Exception {
		ConfigResource r = new ConfigResource(ConfigResource.Type.TOPIC, topic);
		return testAdmin().describeConfigs(List.of(r)).all().get().get(r).get(key);
	}

}
