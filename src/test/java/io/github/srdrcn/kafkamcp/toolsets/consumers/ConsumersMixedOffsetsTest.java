package io.github.srdrcn.kafkamcp.toolsets.consumers;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.GroupState;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Writing/irreversible consumers tools on a ZooKeeper-mode cluster (Kafka 3.9, classic group coordinator):
 * offsets_reset (all targets), offsets_delete, delete; the effects are verified with zkAdmin.
 */
class ConsumersMixedOffsetsTest extends ConsumersMixedTestBase {

	private static final long BASE_TS = Instant.parse("2024-05-01T10:00:00Z").toEpochMilli();

	/** On ZooKeeper: 2 partitions with 10 messages each (timestamp BASE_TS + i s); group Empty, commit {0:5, 1:5}. */
	private String[] zkEmptyGroupWithData(String prefix) throws Exception {
		String topic = prefix + "-t";
		createTopic(ZK, topic, 2);
		produce(ZK, topic, 0, 10, BASE_TS);
		produce(ZK, topic, 1, 10, BASE_TS);
		String group = prefix + "-g";
		emptyGroup(ZK, CLASSIC, group, topic, Map.of(0, 5L, 1, 5L));
		return new String[] { group, topic };
	}

	@Test
	void resetToEveryTargetOnZooKeeper() throws Exception {
		String[] gt = zkEmptyGroupWithData(unique("zkrst"));
		String group = gt[0];
		String topic = gt[1];

		JsonNode earliest = ok("consumer_groups_offsets_reset", on(ZK, "group", group, "topic", topic, "to", "earliest"));
		assertThat(earliest.get("groupState").asText()).isEqualTo("Empty");
		assertThat(earliest.get("changedPartitions").asInt()).isEqualTo(2);
		assertThat(earliest.at("/partitions/0/before").asLong()).isEqualTo(5);
		assertThat(earliest.at("/partitions/0/after").asLong()).isZero();
		assertThat(committed(ZK, group, topic)).isEqualTo(Map.of(0, 0L, 1, 0L));

		JsonNode latest = ok("consumer_groups_offsets_reset",
				on(ZK, "group", group, "topic", topic, "to", "latest", "partitions", List.of(1)));
		assertThat(latest.get("changedPartitions").asInt()).isEqualTo(1);
		assertThat(latest.at("/partitions/0/after").asLong()).isEqualTo(10);
		assertThat(committed(ZK, group, topic)).isEqualTo(Map.of(0, 0L, 1, 10L));

		JsonNode clamped = ok("consumer_groups_offsets_reset",
				on(ZK, "group", group, "topic", topic, "to", "offset", "offset", 100, "partitions", List.of(0)));
		assertThat(clamped.at("/partitions/0/after").asLong()).isEqualTo(10);
		assertThat(clamped.at("/partitions/0/clamped").asBoolean()).isTrue();
		assertThat(committed(ZK, group, topic)).isEqualTo(Map.of(0, 10L, 1, 10L));

		ok("consumer_groups_offsets_reset", on(ZK, "group", group, "topic", topic, "to", "shift", "shiftBy", -3));
		assertThat(committed(ZK, group, topic)).isEqualTo(Map.of(0, 7L, 1, 7L));

		JsonNode byTime = ok("consumer_groups_offsets_reset",
				on(ZK, "group", group, "topic", topic, "to", "timestamp", "timestamp", "2024-05-01T13:00:04+03:00"));
		assertThat(byTime.get("timestamp").asText()).isEqualTo("2024-05-01T10:00:04Z");
		assertThat(committed(ZK, group, topic)).isEqualTo(Map.of(0, 4L, 1, 4L));

		ok("consumer_groups_offsets_reset",
				on(ZK, "group", group, "topic", topic, "to", "timestamp", "timestamp", "2030-01-01T00:00:00Z"));
		assertThat(committed(ZK, group, topic)).isEqualTo(Map.of(0, 10L, 1, 10L));
		assertThat(state(ZK, group)).isEqualTo(GroupState.EMPTY);
	}

	@Test
	void resetRefusesActiveGroupAndCreatesNewGroupOnZooKeeper() throws Exception {
		String prefix = unique("zkrsa");
		String topic = prefix + "-t";
		createTopic(ZK, topic, 1);
		produce(ZK, topic, 0, 10);
		String group = prefix + "-g";
		KafkaConsumer<byte[], byte[]> consumer = join(ZK, CLASSIC, group, prefix + "-c", topic);
		commit(consumer, topic, Map.of(0, 6L));
		assertThat(error("consumer_groups_offsets_reset", on(ZK, "group", group, "topic", topic, "to", "earliest")))
			.contains("active member")
			.contains("must be Empty");
		assertThat(committed(ZK, group, topic)).isEqualTo(Map.of(0, 6L));

		// The ZooKeeper broker describes a missing group as Dead instead of failing: it must be treated as a new group.
		String fresh = prefix + "-new";
		JsonNode r = ok("consumer_groups_offsets_reset", on(ZK, "group", fresh, "topic", topic, "to", "offset", "offset", 3));
		assertThat(r.has("groupState")).isFalse();
		assertThat(r.get("note").asText()).contains("new group");
		assertThat(r.at("/partitions/0").has("before")).isFalse();
		assertThat(r.at("/partitions/0/after").asLong()).isEqualTo(3);
		assertThat(committed(ZK, fresh, topic)).isEqualTo(Map.of(0, 3L));
		assertThat(groupIds(ZK)).contains(fresh);
	}

	@Test
	void offsetsDeleteOnZooKeeper() throws Exception {
		String[] gt = zkEmptyGroupWithData(unique("zkodl"));
		String group = gt[0];
		String topic = gt[1];
		String other = topic + "-other";
		createTopic(ZK, other, 1);
		alter(ZK, group, other, Map.of(0, 0L));

		JsonNode one = ok("consumer_groups_offsets_delete", on(ZK, "group", group, "topic", topic, "partitions", List.of(0)));
		assertThat(one.get("deletedPartitions").asInt()).isEqualTo(1);
		assertThat(one.get("failedPartitions").asInt()).isZero();
		assertThat(one.at("/partitions/0/committedBefore").asLong()).isEqualTo(5);
		assertThat(one.at("/partitions/0/deleted").asBoolean()).isTrue();
		assertThat(committed(ZK, group, topic)).isEqualTo(Map.of(1, 5L));

		JsonNode rest = ok("consumer_groups_offsets_delete", on(ZK, "group", group, "topic", topic, "partitions",
				List.of(0, 1)));
		assertThat(rest.get("deletedPartitions").asInt()).isEqualTo(1);
		assertThat(rest.get("partitionsWithoutCommit").asInt()).isEqualTo(1);
		assertThat(committed(ZK, group, topic)).isEmpty();
		assertThat(committed(ZK, group, other)).isEqualTo(Map.of(0, 0L));

		assertThat(error("consumer_groups_offsets_delete", on(ZK, "group", group, "topic", topic)))
			.contains("has no committed offsets");
		// Missing group: ZooKeeper does not fail the offset query (returns empty); the message must match KRaft's.
		for (String cluster : List.of(ZK, KRAFT)) {
			awaitGroupCoordinator(cluster);
			assertThat(error("consumer_groups_offsets_delete", on(cluster, "group", unique("zk-missing"), "topic", topic)))
				.as(cluster)
				.contains("Consumer group not found");
		}
	}

	@Test
	void offsetsDeleteRefusedWhileSubscribedOnZooKeeper() throws Exception {
		String prefix = unique("zkods");
		String topic = prefix + "-t";
		createTopic(ZK, topic, 1);
		produce(ZK, topic, 0, 5);
		String group = prefix + "-g";
		KafkaConsumer<byte[], byte[]> consumer = join(ZK, CLASSIC, group, prefix + "-c", topic);
		commit(consumer, topic, Map.of(0, 2L));
		assertThat(error("consumer_groups_offsets_delete", on(ZK, "group", group, "topic", topic))).contains("still subscribed");
		assertThat(committed(ZK, group, topic)).isEqualTo(Map.of(0, 2L));
	}

	@Test
	void deleteGroupOnZooKeeper() throws Exception {
		String[] gt = zkEmptyGroupWithData(unique("zkdel"));
		String group = gt[0];
		String topic = gt[1];

		String activeGroup = group + "-active";
		join(ZK, CLASSIC, activeGroup, activeGroup + "-c", topic);
		assertThat(error("consumer_groups_delete", on(ZK, "group", activeGroup)))
			.contains("not empty")
			.contains("state=Stable")
			.contains("members=1");
		assertThat(groupIds(ZK)).contains(activeGroup);

		JsonNode r = ok("consumer_groups_delete", on(ZK, "group", group));
		assertThat(r.get("deleted").asBoolean()).isTrue();
		assertThat(r.at("/deletedOffsets/topics/0").asText()).isEqualTo(topic);
		assertThat(r.at("/deletedOffsets/partitions").asInt()).isEqualTo(2);
		assertThat(groupIds(ZK)).doesNotContain(group);
		assertThat(committed(ZK, group, topic)).isEmpty();

		assertThat(error("consumer_groups_delete", on(ZK, "group", group))).contains("not found");
	}

}
