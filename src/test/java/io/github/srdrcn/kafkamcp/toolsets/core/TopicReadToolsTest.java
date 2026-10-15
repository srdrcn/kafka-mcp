package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;

class TopicReadToolsTest extends KafkaIntegrationTest {

	private static String prefix;

	private static String retention;

	private static String compacted;

	@BeforeAll
	static void setUp() throws Exception {
		prefix = unique("tr");
		retention = prefix + "-a";
		compacted = prefix + "-b";
		TestData.createTopic(testAdmin(), retention, 3, Map.of("retention.ms", "3600000"));
		TestData.createTopic(testAdmin(), compacted, 1, Map.of("cleanup.policy", "compact"));
		TestData.produce(retention, 0, 3, null);
		TestData.produce(retention, 2, 2, null);
	}

	@Test
	void topicsListReturnsPartitionsAndReplicationFactor() {
		JsonNode r = ok("topics_list", args("filter", "^" + prefix));
		assertThat(r.get("total").asInt()).isEqualTo(2);
		assertThat(r.get("truncated").asBoolean()).isFalse();
		JsonNode first = r.get("topics").get(0);
		assertThat(first.get("name").asText()).isEqualTo(retention);
		assertThat(first.get("partitions").asInt()).isEqualTo(3);
		assertThat(first.get("replicationFactor").asInt()).isEqualTo(1);
		assertThat(first.has("internal")).isFalse();
		assertThat(r.at("/topics/1/name").asText()).isEqualTo(compacted);

		JsonNode limited = ok("topics_list", args("filter", "^" + prefix, "limit", 1));
		assertThat(limited.get("total").asInt()).isEqualTo(2);
		assertThat(limited.get("returned").asInt()).isEqualTo(1);
		assertThat(limited.get("truncated").asBoolean()).isTrue();

		assertThat(error("topics_list", args("filter", "(")).toLowerCase()).contains("regex");
	}

	@Test
	void topicsGetShowsPartitionsConfigsSizeAndOffsets() {
		JsonNode r = ok("topics_get", args("topic", retention));
		assertThat(r.get("topic").asText()).isEqualTo(retention);
		assertThat(r.get("topicId").asText()).isNotBlank();
		assertThat(r.get("partitionCount").asInt()).isEqualTo(3);
		assertThat(r.get("replicationFactor").asInt()).isEqualTo(1);
		assertThat(r.get("underReplicatedPartitions").asInt()).isZero();
		assertThat(r.get("offlinePartitions").asInt()).isZero();
		JsonNode p0 = r.get("partitions").get(0);
		assertThat(p0.get("partition").asInt()).isZero();
		assertThat(p0.get("leader").asInt()).isEqualTo(1);
		assertThat(p0.get("replicas").get(0).asInt()).isEqualTo(1);
		assertThat(p0.get("isr").get(0).asInt()).isEqualTo(1);
		assertThat(p0.has("urp")).isFalse();
		JsonNode cfg = ClusterBrokerToolsTest.find(r.get("nonDefaultConfigs"), "retention.ms");
		assertThat(cfg.get("value").asText()).isEqualTo("3600000");
		assertThat(cfg.get("source").asText()).isEqualTo("DYNAMIC_TOPIC_CONFIG");
		r.get("nonDefaultConfigs").forEach(c -> assertThat(c.get("source").asText()).isNotEqualTo("DEFAULT_CONFIG"));
		assertThat(r.at("/offsets/approxMessages").asLong()).isEqualTo(5);
		assertThat(r.at("/offsets/latestSum").asLong()).isEqualTo(5);
		assertThat(r.get("sizeBytes").asLong()).isPositive();
		assertThat(r.get("totalSizeBytes").asLong()).isEqualTo(r.get("sizeBytes").asLong());
		assertThat(r.get("size").asText()).endsWith("B");

		assertThat(error("topics_get", args("topic", prefix + "-missing"))).contains("Topic not found");
	}

	@Test
	void topicsConfigGetListsAllOrOnlyOverrides() {
		JsonNode all = ok("topics_config_get", args("topic", retention));
		assertThat(all.get("count").asInt()).isGreaterThan(20);
		JsonNode ret = ClusterBrokerToolsTest.find(all.get("configs"), "retention.ms");
		assertThat(ret.get("value").asText()).isEqualTo("3600000");
		assertThat(ret.get("isDefault").asBoolean()).isFalse();
		assertThat(ret.get("source").asText()).isEqualTo("DYNAMIC_TOPIC_CONFIG");
		assertThat(ret.get("readOnly").asBoolean()).isFalse();
		JsonNode seg = ClusterBrokerToolsTest.find(all.get("configs"), "segment.bytes");
		assertThat(seg.get("isDefault").asBoolean()).isTrue();

		JsonNode overrides = ok("topics_config_get", args("topic", retention, "includeDefaults", false));
		overrides.get("configs").forEach(c -> assertThat(c.get("isDefault").asBoolean()).isFalse());
		assertThat(names(overrides.get("configs"))).contains("retention.ms").doesNotContain("segment.bytes");

		JsonNode filtered = ok("topics_config_get", args("topic", retention, "filter", "^retention\\."));
		assertThat(names(filtered.get("configs"))).isNotEmpty().allMatch(n -> n.startsWith("retention."));

		assertThat(error("topics_config_get", args("topic", prefix + "-missing"))).contains("Topic not found");
	}

	@Test
	void topicsFindByConfigComparesNumbersStringsAndRegex() {
		JsonNode lt = ok("topics_find_by_config",
				args("key", "retention.ms", "operator", "lt", "value", "7200000", "filter", "^" + prefix));
		assertThat(lt.get("topicsChecked").asInt()).isEqualTo(2);
		assertThat(topics(lt)).containsExactly(retention);
		assertThat(lt.at("/topics/0/value").asText()).isEqualTo("3600000");
		assertThat(lt.at("/topics/0/source").asText()).isEqualTo("DYNAMIC_TOPIC_CONFIG");

		JsonNode gte = ok("topics_find_by_config",
				args("key", "retention.ms", "operator", "gte", "value", "3600000", "filter", "^" + prefix));
		assertThat(topics(gte)).containsExactly(retention, compacted);

		JsonNode eq = ok("topics_find_by_config",
				args("key", "cleanup.policy", "operator", "eq", "value", "compact", "filter", "^" + prefix));
		assertThat(topics(eq)).containsExactly(compacted);

		JsonNode ne = ok("topics_find_by_config",
				args("key", "cleanup.policy", "operator", "ne", "value", "compact", "filter", "^" + prefix));
		assertThat(topics(ne)).containsExactly(retention);

		JsonNode regex = ok("topics_find_by_config",
				args("key", "cleanup.policy", "operator", "regex", "value", "^comp", "filter", "^" + prefix));
		assertThat(topics(regex)).containsExactly(compacted);

		JsonNode contains = ok("topics_find_by_config",
				args("key", "cleanup.policy", "operator", "contains", "value", "del", "filter", "^" + prefix));
		assertThat(topics(contains)).containsExactly(retention);

		JsonNode exists = ok("topics_find_by_config", args("key", "retention.ms", "operator", "exists",
				"onlyNonDefault", true, "filter", "^" + prefix));
		assertThat(topics(exists)).containsExactly(retention);

		JsonNode unknown = ok("topics_find_by_config",
				args("key", "no.such.key", "operator", "exists", "filter", "^" + prefix));
		assertThat(unknown.get("total").asInt()).isZero();
		assertThat(unknown.get("warning").asText()).contains("no.such.key");

		assertThat(error("topics_find_by_config", args("key", "retention.ms", "operator", "gt", "value", "abc")))
			.contains("must be a number");
		assertThat(error("topics_find_by_config", args("key", "retention.ms", "operator", "eq")))
			.contains("Missing parameter: value");
	}

	private static List<String> topics(JsonNode r) {
		List<String> out = new ArrayList<>();
		r.get("topics").forEach(t -> out.add(t.get("topic").asText()));
		return out;
	}

	private static List<String> names(JsonNode configs) {
		List<String> out = new ArrayList<>();
		configs.forEach(c -> out.add(c.get("name").asText()));
		return out;
	}

}
