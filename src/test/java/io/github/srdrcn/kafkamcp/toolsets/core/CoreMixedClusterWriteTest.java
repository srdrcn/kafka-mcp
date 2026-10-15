package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.MixedClusterIntegrationTest;

/**
 * core yazma araçları ZooKeeper modlu cluster'da ({@code zk}): her değişiklik test cluster'ının kendi
 * AdminClient'ıyla ({@link #zkAdmin()}) doğrulanır. ZooKeeper'da metadata/config değişiklikleri controller
 * ve ZooKeeper bildirimleriyle yayıldığı için doğrulamalar kısa süre tekrar okur.
 */
class CoreMixedClusterWriteTest extends MixedClusterIntegrationTest {

	private static final String KEY = "log.cleaner.backoff.ms";

	@Test
	void brokersConfigUpdateOnZooKeeperPerBrokerAndClusterDefault() throws Exception {
		ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "1");
		JsonNode set = ok("brokers_config_update", zkArgs("brokerId", 1, "set", Map.of(KEY, "16000")));
		assertThat(set.get("scope").asText()).isEqualTo("broker");
		assertThat(set.at("/changes/0/before").asText()).isEqualTo("15000");
		assertThat(set.at("/changes/0/after").asText()).isEqualTo("16000");
		assertThat(set.at("/changes/0/afterSource").asText()).isEqualTo("DYNAMIC_BROKER_CONFIG");
		awaitConfig(broker, KEY, e -> e != null && "16000".equals(e.value())
				&& e.source() == ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);

		JsonNode del = ok("brokers_config_update", zkArgs("brokerId", 1, "delete", List.of(KEY)));
		assertThat(del.at("/changes/0/before").asText()).isEqualTo("16000");
		assertThat(del.at("/changes/0/after").asText()).isEqualTo("15000");
		awaitConfig(broker, KEY, e -> e.source() != ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);

		ConfigResource clusterDefault = new ConfigResource(ConfigResource.Type.BROKER, "");
		JsonNode def = ok("brokers_config_update", zkArgs("set", Map.of(KEY, "17000")));
		assertThat(def.get("scope").asText()).isEqualTo("cluster-default");
		assertThat(def.at("/changes/0/after").asText()).isEqualTo("17000");
		assertThat(def.at("/changes/0/afterSource").asText()).isEqualTo("DYNAMIC_DEFAULT_BROKER_CONFIG");
		awaitConfig(clusterDefault, KEY, e -> e != null && "17000".equals(e.value()));
		awaitConfig(broker, KEY, e -> "17000".equals(e.value())
				&& e.source() == ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG);

		JsonNode defDel = ok("brokers_config_update", zkArgs("delete", List.of(KEY)));
		assertThat(defDel.at("/changes/0/before").asText()).isEqualTo("17000");
		assertThat(defDel.at("/changes/0").has("after")).isFalse();
		awaitConfig(clusterDefault, KEY, e -> e == null);
		awaitConfig(broker, KEY, e -> "15000".equals(e.value()));

		assertThat(error("brokers_config_update", zkArgs("brokerId", 1, "set", Map.of(KEY, "abc"))))
			.contains("Geçersiz istek/config");
		assertThat(error("brokers_config_update", zkArgs("brokerId", 7, "set", Map.of(KEY, "1"))))
			.contains("Broker bulunamadı: 7");
	}

	@Test
	void electPreferredLeadersOnZooKeeper() throws Exception {
		String topic = unique("zkelect");
		TestData.createTopic(zkAdmin(), topic, 2, Map.of());
		JsonNode r = ok("brokers_elect_preferred_leaders", zkArgs("topic", topic));
		assertThat(r.get("requested").asInt()).isEqualTo(2);
		assertThat(r.get("failed").asInt()).isZero();
		// tek broker: lider zaten tercih edilen replika
		assertThat(r.get("notNeeded").asInt() + r.get("elected").asInt()).isEqualTo(2);
		assertThat(r.at("/results/0/topic").asText()).isEqualTo(topic);
		describe(topic).partitions()
			.forEach(p -> assertThat(p.leader().id()).isEqualTo(p.replicas().get(0).id()));

		JsonNode all = ok("brokers_elect_preferred_leaders", zkArgs());
		assertThat(all.get("requested").asInt()).isZero();
		assertThat(all.get("message").asText()).contains("tercih edilen");
		assertThat(error("brokers_elect_preferred_leaders", zkArgs("topic", topic, "partitions", List.of(5))))
			.contains("geçersiz partition: 5");
	}

	@Test
	void partitionReassignmentsOnZooKeeper() throws Exception {
		String topic = unique("zkreassign");
		TestData.createTopic(zkAdmin(), topic, 1, Map.of());
		JsonNode r = ok("partition_reassignments_alter",
				zkArgs("reassignments", List.of(Map.of("topic", topic, "partition", 0, "replicas", List.of(1)))));
		assertThat(r.get("requested").asInt()).isEqualTo(1);
		assertThat(r.get("ok").asInt()).isEqualTo(1);
		assertThat(r.at("/results/0/action").asText()).isEqualTo("reassign");
		assertThat(r.at("/results/0/result").asText()).isEqualTo("ok");
		assertThat(describe(topic).partitions().get(0).replicas()).extracting(Node::id).containsExactly(1);

		// ZooKeeper controller'ı aynı replika kümesine reassignment'ı hemen tamamlar
		JsonNode list = ok("partition_reassignments_list", zkArgs("filter", "^" + topic + "$"));
		for (int i = 0; i < 50 && list.get("total").asInt() > 0; i++) {
			Thread.sleep(100);
			list = ok("partition_reassignments_list", zkArgs("filter", "^" + topic + "$"));
		}
		assertThat(list.get("total").asInt()).isZero();
		assertThat(list.get("reassignments").isArray()).isTrue();
		assertThat(zkAdmin().listPartitionReassignments().reassignments().get().keySet())
			.doesNotContain(new TopicPartition(topic, 0));

		JsonNode cancel = ok("partition_reassignments_alter",
				zkArgs("reassignments", List.of(Map.of("topic", topic, "partition", 0))));
		assertThat(cancel.at("/results/0/action").asText()).isEqualTo("cancel");
		assertThat(cancel.at("/results/0/result").asText()).isEqualTo("error");
		assertThat(cancel.at("/results/0/error").asText()).contains("devam eden reassignment yok");
		assertThat(cancel.get("failed").asInt()).isEqualTo(1);

		assertThat(error("partition_reassignments_alter",
				zkArgs("reassignments", List.of(Map.of("topic", topic, "partition", 0, "replicas", List.of(1, 99))))))
			.contains("Broker bulunamadı: 99");
	}

	@Test
	void topicLifecycleOnZooKeeper() throws Exception {
		String topic = unique("zkcreate");
		JsonNode created = ok("topics_create", zkArgs("topic", topic, "partitions", 3, "replicationFactor", 1,
				"configs", Map.of("retention.ms", "3600000", "cleanup.policy", "compact")));
		assertThat(created.get("topic").asText()).isEqualTo(topic);
		assertThat(created.get("topicId").asText()).isNotBlank();
		assertThat(created.get("partitions").asInt()).isEqualTo(3);
		assertThat(created.get("replicationFactor").asInt()).isEqualTo(1);
		TestData.awaitTopic(zkAdmin(), topic, 3);
		assertThat(describe(topic).topicId().toString()).isEqualTo(created.get("topicId").asText());
		assertThat(topicConfig(topic, "retention.ms").value()).isEqualTo("3600000");
		assertThat(topicConfig(topic, "cleanup.policy").value()).isEqualTo("compact");
		assertThat(error("topics_create", zkArgs("topic", topic))).contains("Topic zaten var");
		assertThat(error("topics_create", zkArgs("topic", unique("zkrf"), "replicationFactor", 3)))
			.contains("broker sayısından");

		String clone = unique("zkclone");
		JsonNode cloned = ok("topics_clone",
				zkArgs("source", topic, "target", clone, "configs", Map.of("retention.ms", "7200000")));
		assertThat(cloned.get("partitions").asInt()).isEqualTo(3);
		assertThat(cloned.at("/copiedConfigs/cleanup.policy").asText()).isEqualTo("compact");
		TestData.awaitTopic(zkAdmin(), clone, 3);
		assertThat(topicConfig(clone, "cleanup.policy").value()).isEqualTo("compact");
		assertThat(topicConfig(clone, "retention.ms").value()).isEqualTo("7200000");

		JsonNode upd = ok("topics_config_update", zkArgs("topic", topic, "set", Map.of("retention.ms", "1800000"),
				"delete", List.of("cleanup.policy")));
		assertThat(upd.get("changes")).hasSize(2);
		JsonNode setRow = upd.get("changes").get(0);
		assertThat(setRow.get("name").asText()).isEqualTo("retention.ms");
		assertThat(setRow.get("before").asText()).isEqualTo("3600000");
		assertThat(setRow.get("after").asText()).isEqualTo("1800000");
		JsonNode delRow = upd.get("changes").get(1);
		assertThat(delRow.get("before").asText()).isEqualTo("compact");
		assertThat(delRow.get("after").asText()).isEqualTo("delete");
		assertThat(delRow.get("afterSource").asText()).isNotEqualTo("DYNAMIC_TOPIC_CONFIG");
		assertThat(topicConfig(topic, "retention.ms").value()).isEqualTo("1800000");
		assertThat(topicConfig(topic, "cleanup.policy").source())
			.isNotEqualTo(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);

		JsonNode inc = ok("topics_partitions_increase", zkArgs("topic", topic, "totalCount", 5));
		assertThat(inc.get("before").asInt()).isEqualTo(3);
		assertThat(inc.get("after").asInt()).isEqualTo(5);
		TestData.awaitTopic(zkAdmin(), topic, 5);
		assertThat(error("topics_partitions_increase", zkArgs("topic", topic, "totalCount", 4)))
			.contains("azaltılamaz");

		JsonNode deleted = ok("topics_delete", zkArgs("topic", clone));
		assertThat(deleted.get("deleted").asText()).isEqualTo(clone);
		assertThat(deleted.get("partitions").asInt()).isEqualTo(3);
		// ZooKeeper'da silme asenkron (controller topic'i önce silinmek üzere işaretler)
		awaitGone(clone);
		ok("topics_delete", zkArgs("topic", topic));
		awaitGone(topic);

		String internal = "__" + unique("zkinternal");
		TestData.createTopic(zkAdmin(), internal, 1, Map.of());
		assertThat(error("topics_delete", zkArgs("topic", internal))).contains("iç topic");
		assertThat(zkAdmin().listTopics().names().get()).contains(internal);
		assertThat(error("topics_delete", zkArgs("topic", "__consumer_offsets"))).satisfiesAnyOf(
				m -> assertThat(m).contains("iç topic"), m -> assertThat(m).contains("Topic bulunamadı"));
		assertThat(error("topics_delete", zkArgs("topic", unique("yok")))).contains("Topic bulunamadı");
	}

	private static TopicDescription describe(String topic) throws Exception {
		return TestData.retryUnknownTopic(
				() -> zkAdmin().describeTopics(List.of(topic)).allTopicNames().get().get(topic));
	}

	private static ConfigEntry topicConfig(String topic, String key) throws Exception {
		ConfigResource r = new ConfigResource(ConfigResource.Type.TOPIC, topic);
		return TestData.retryUnknownTopic(() -> zkAdmin().describeConfigs(List.of(r)).all().get().get(r).get(key));
	}

	/** Config, ZooKeeper bildirimiyle broker'a yansıyana kadar (en çok 20 sn) tekrar okur. */
	private static void awaitConfig(ConfigResource r, String key, Predicate<ConfigEntry> condition) throws Exception {
		long end = System.currentTimeMillis() + 20_000;
		ConfigEntry e = zkAdmin().describeConfigs(List.of(r)).all().get().get(r).get(key);
		while (!condition.test(e)) {
			assertThat(System.currentTimeMillis()).as("%s %s beklenen değere ulaşmadı: %s", r, key, e).isLessThan(end);
			Thread.sleep(100);
			e = zkAdmin().describeConfigs(List.of(r)).all().get().get(r).get(key);
		}
	}

	private static void awaitGone(String topic) throws Exception {
		long end = System.currentTimeMillis() + 30_000;
		while (zkAdmin().listTopics().names().get().contains(topic)) {
			assertThat(System.currentTimeMillis()).as("topic silinmedi: %s", topic).isLessThan(end);
			Thread.sleep(100);
		}
	}

}
