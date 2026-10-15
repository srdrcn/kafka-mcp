package io.github.srdrcn.kafkamcp.toolsets.consumers;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

/** consumer_groups_offsets_reset, _offsets_delete, _delete and the safety rules. */
class ConsumerGroupsOffsetsTest extends ConsumersTestBase {

	private static final long BASE_TS = Instant.parse("2024-05-01T10:00:00Z").toEpochMilli();

	/** 2 partitions with 10 messages each (timestamp BASE_TS + i s); group Empty, commit {0:5, 1:5}. */
	private String[] emptyGroupWithData(String prefix) throws Exception {
		String topic = prefix + "-t";
		createTopic(topic, 2);
		produce(topic, 0, 10, BASE_TS);
		produce(topic, 1, 10, BASE_TS);
		String group = prefix + "-g";
		emptyGroup(group, topic, Map.of(0, 5L, 1, 5L));
		return new String[] { group, topic };
	}

	private static List<String> groupIds() throws Exception {
		return testAdmin().listGroups(ListGroupsOptions.forConsumerGroups())
			.all()
			.get(30, TimeUnit.SECONDS)
			.stream()
			.map(GroupListing::groupId)
			.toList();
	}

	@Test
	void resetToEarliestLatestOffsetShiftAndTimestamp() throws Exception {
		String[] gt = emptyGroupWithData(unique("rst"));
		String group = gt[0];
		String topic = gt[1];

		JsonNode earliest = ok("consumer_groups_offsets_reset", args("group", group, "topic", topic, "to", "earliest"));
		assertThat(earliest.get("groupState").asText()).isEqualTo("Empty");
		assertThat(earliest.get("changedPartitions").asInt()).isEqualTo(2);
		JsonNode p0 = earliest.at("/partitions/0");
		assertThat(p0.get("partition").asInt()).isZero();
		assertThat(p0.get("before").asLong()).isEqualTo(5);
		assertThat(p0.get("after").asLong()).isZero();
		assertThat(committed(group, topic)).isEqualTo(Map.of(0, 0L, 1, 0L));

		JsonNode latest = ok("consumer_groups_offsets_reset",
				args("group", group, "topic", topic, "to", "latest", "partitions", List.of(1)));
		assertThat(latest.get("changedPartitions").asInt()).isEqualTo(1);
		assertThat(latest.at("/partitions/0/partition").asInt()).isEqualTo(1);
		assertThat(latest.at("/partitions/0/after").asLong()).isEqualTo(10);
		assertThat(committed(group, topic)).isEqualTo(Map.of(0, 0L, 1, 10L));

		JsonNode clamped = ok("consumer_groups_offsets_reset",
				args("group", group, "topic", topic, "to", "offset", "offset", 100, "partitions", List.of(0)));
		assertThat(clamped.at("/partitions/0/after").asLong()).isEqualTo(10);
		assertThat(clamped.at("/partitions/0/clamped").asBoolean()).isTrue();

		JsonNode shift = ok("consumer_groups_offsets_reset",
				args("group", group, "topic", topic, "to", "shift", "shiftBy", -3));
		assertThat(committed(group, topic)).isEqualTo(Map.of(0, 7L, 1, 7L));
		assertThat(shift.at("/partitions/1/before").asLong()).isEqualTo(10);

		ok("consumer_groups_offsets_reset", args("group", group, "topic", topic, "to", "shift", "shiftBy", -50));
		assertThat(committed(group, topic)).isEqualTo(Map.of(0, 0L, 1, 0L));

		JsonNode byTime = ok("consumer_groups_offsets_reset",
				args("group", group, "topic", topic, "to", "timestamp", "timestamp", "2024-05-01T13:00:04+03:00"));
		assertThat(byTime.get("timestamp").asText()).isEqualTo("2024-05-01T10:00:04Z");
		assertThat(committed(group, topic)).isEqualTo(Map.of(0, 4L, 1, 4L));

		ok("consumer_groups_offsets_reset",
				args("group", group, "topic", topic, "to", "timestamp", "timestamp", String.valueOf(BASE_TS + 2000)));
		assertThat(committed(group, topic)).isEqualTo(Map.of(0, 2L, 1, 2L));

		ok("consumer_groups_offsets_reset",
				args("group", group, "topic", topic, "to", "timestamp", "timestamp", "2030-01-01T00:00:00Z"));
		assertThat(committed(group, topic)).isEqualTo(Map.of(0, 10L, 1, 10L));
	}

	@Test
	void resetValidatesParameters() throws Exception {
		String[] gt = emptyGroupWithData(unique("rsv"));
		String group = gt[0];
		String topic = gt[1];
		assertThat(error("consumer_groups_offsets_reset", args("group", group, "topic", topic, "to", "offset")))
			.contains("offset parameter is required");
		assertThat(error("consumer_groups_offsets_reset", args("group", group, "topic", topic, "to", "shift")))
			.contains("shiftBy parameter is required");
		assertThat(error("consumer_groups_offsets_reset",
				args("group", group, "topic", topic, "to", "timestamp", "timestamp", "yesterday")))
			.contains("Invalid timestamp");
		assertThat(error("consumer_groups_offsets_reset",
				args("group", group, "topic", topic, "to", "earliest", "partitions", List.of(0, 7))))
			.contains("Partitions not in topic")
			.contains("[7]");
		assertThat(error("consumer_groups_offsets_reset", args("group", group, "topic", topic, "to", "middle")))
			.contains("Invalid value for to");
		assertThat(error("consumer_groups_offsets_reset",
				args("group", group, "topic", unique("no-topic"), "to", "earliest")))
			.contains("not found");
		assertThat(committed(group, topic)).isEqualTo(Map.of(0, 5L, 1, 5L));
	}

	@Test
	void resetRefusesGroupWithActiveMembers() throws Exception {
		String prefix = unique("rsa");
		String topic = prefix + "-t";
		createTopic(topic, 1);
		produce(topic, 0, 10);
		String group = prefix + "-g";
		var consumer = join(group, prefix + "-c", topic);
		commit(consumer, topic, Map.of(0, 6L));
		String err = error("consumer_groups_offsets_reset", args("group", group, "topic", topic, "to", "earliest"));
		assertThat(err).contains("active member").contains("must be Empty");
		assertThat(committed(group, topic)).isEqualTo(Map.of(0, 6L));
	}

	@Test
	void resetCreatesOffsetsForNewGroup() throws Exception {
		String prefix = unique("rsn");
		String topic = prefix + "-t";
		createTopic(topic, 1);
		produce(topic, 0, 10);
		String group = prefix + "-new";
		JsonNode r = ok("consumer_groups_offsets_reset",
				args("group", group, "topic", topic, "to", "offset", "offset", 3));
		assertThat(r.has("groupState")).isFalse();
		assertThat(r.get("note").asText()).contains("new group");
		assertThat(r.at("/partitions/0").has("before")).isFalse();
		assertThat(r.at("/partitions/0/after").asLong()).isEqualTo(3);
		assertThat(committed(group, topic)).isEqualTo(Map.of(0, 3L));
	}

	@Test
	void offsetsDeleteRemovesSelectedOrAllPartitions() throws Exception {
		String[] gt = emptyGroupWithData(unique("odl"));
		String group = gt[0];
		String topic = gt[1];
		String other = topic + "-other";
		createTopic(other, 1);
		alter(group, other, Map.of(0, 0L));

		JsonNode one = ok("consumer_groups_offsets_delete", args("group", group, "topic", topic, "partitions", List.of(0)));
		assertThat(one.get("deletedPartitions").asInt()).isEqualTo(1);
		assertThat(one.at("/partitions/0/committedBefore").asLong()).isEqualTo(5);
		assertThat(one.at("/partitions/0/deleted").asBoolean()).isTrue();
		assertThat(committed(group, topic)).isEqualTo(Map.of(1, 5L));

		JsonNode rest = ok("consumer_groups_offsets_delete", args("group", group, "topic", topic));
		assertThat(rest.get("deletedPartitions").asInt()).isEqualTo(1);
		assertThat(committed(group, topic)).isEmpty();
		assertThat(committed(group, other)).isEqualTo(Map.of(0, 0L));

		assertThat(error("consumer_groups_offsets_delete", args("group", group, "topic", topic)))
			.contains("has no committed offsets");
	}

	@Test
	void offsetsDeleteRefusedWhileSubscribed() throws Exception {
		String prefix = unique("ods");
		String topic = prefix + "-t";
		createTopic(topic, 1);
		produce(topic, 0, 5);
		String group = prefix + "-g";
		var consumer = join(group, prefix + "-c", topic);
		commit(consumer, topic, Map.of(0, 2L));
		String err = error("consumer_groups_offsets_delete", args("group", group, "topic", topic));
		assertThat(err).contains("still subscribed");
		assertThat(committed(group, topic)).isEqualTo(Map.of(0, 2L));
	}

	@Test
	void offsetsDeleteDoesNotReportPartitionsWithoutCommitAsDeleted() throws Exception {
		String prefix = unique("odn");
		String topic = prefix + "-t";
		createTopic(topic, 3);
		produce(topic, 0, 5);
		String group = prefix + "-g";
		alter(group, topic, Map.of(0, 3L));

		assertThat(error("consumer_groups_offsets_delete", args("group", group, "topic", topic, "partitions", List.of(1, 2))))
			.contains("has no committed offsets")
			.contains("[1, 2]");
		assertThat(committed(group, topic)).isEqualTo(Map.of(0, 3L));

		JsonNode r = ok("consumer_groups_offsets_delete",
				args("group", group, "topic", topic, "partitions", List.of(0, 1, 99)));
		assertThat(r.get("deletedPartitions").asInt()).isEqualTo(1);
		assertThat(r.get("failedPartitions").asInt()).isZero();
		assertThat(r.get("partitionsWithoutCommit").asInt()).isEqualTo(2);
		assertThat(r.at("/partitions/0/partition").asInt()).isZero();
		assertThat(r.at("/partitions/0/deleted").asBoolean()).isTrue();
		assertThat(r.at("/partitions/0/committedBefore").asLong()).isEqualTo(3);
		assertThat(r.at("/partitions/1/partition").asInt()).isEqualTo(1);
		assertThat(r.at("/partitions/1/deleted").asBoolean()).isFalse();
		assertThat(r.at("/partitions/1/note").asText()).contains("No committed offset");
		assertThat(r.at("/partitions/2/partition").asInt()).isEqualTo(99);
		assertThat(r.at("/partitions/2/deleted").asBoolean()).isFalse();
		assertThat(committed(group, topic)).isEmpty();
	}

	@Test
	void deleteGroupOnlyWhenEmpty() throws Exception {
		String[] gt = emptyGroupWithData(unique("gdl"));
		String group = gt[0];
		JsonNode r = ok("consumer_groups_delete", args("group", group));
		assertThat(r.get("deleted").asBoolean()).isTrue();
		assertThat(r.at("/deletedOffsets/partitions").asInt()).isEqualTo(2);
		assertThat(r.at("/deletedOffsets/topics/0").asText()).isEqualTo(gt[1]);
		assertThat(groupIds()).doesNotContain(group);

		assertThat(error("consumer_groups_delete", args("group", group))).contains("not found");

		String prefix = unique("gda");
		String topic = prefix + "-t";
		createTopic(topic, 1);
		String active = prefix + "-g";
		join(active, prefix + "-c", topic);
		String err = error("consumer_groups_delete", args("group", active));
		assertThat(err).contains("not empty").contains("members=1");
		assertThat(groupIds()).contains(active);
	}

	@Test
	void writeToolsAreRejectedOnReadOnlyCluster() throws Exception {
		String[] gt = emptyGroupWithData(unique("ro"));
		assertThat(error("consumer_groups_delete", args("cluster", "test-ro", "group", gt[0]))).contains("readOnly");
		assertThat(error("consumer_groups_offsets_reset",
				args("cluster", "test-ro", "group", gt[0], "topic", gt[1], "to", "earliest")))
			.contains("readOnly");
		assertThat(error("consumer_groups_offsets_delete", args("cluster", "test-ro", "group", gt[0], "topic", gt[1])))
			.contains("readOnly");
		assertThat(groupIds()).contains(gt[0]);
		assertThat(committed(gt[0], gt[1])).isEqualTo(Map.of(0, 5L, 1, 5L));
	}

}
