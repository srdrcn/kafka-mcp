package io.github.srdrcn.kafkamcp.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Unit tests of the pure helpers in the kafka package. */
class KafkaHelpersTest {

	@Test
	void protectedTopics() {
		assertThat(Topics.protectedReason("__consumer_offsets", true)).contains("internal topic");
		assertThat(Topics.protectedReason("__custom", false)).contains("internal topic");
		assertThat(Topics.protectedReason("_schemas", false)).contains("system");
		assertThat(Topics.protectedReason("_confluent-command", false)).contains("system");
		assertThat(Topics.protectedReason("connect-configs", false)).contains("system");
		assertThat(Topics.protectedReason("_connect-offsets", false)).contains("system");
		assertThat(Topics.protectedReason("connect-cluster-status", false)).contains("system");
		assertThat(Topics.protectedReason("mm2-offset-syncs.b.internal", false)).contains("system");
		assertThat(Topics.protectedReason("my_connect_statuses", false)).contains("system");
		assertThat(Topics.protectedReason("orders", false)).isNull();
		assertThat(Topics.protectedReason("orders-status", false)).isNull();
		assertThat(Topics.protectedReason("app-changelog", false)).isNull();
		// ordinary user topics that merely contain "connect" are not protected
		assertThat(Topics.protectedReason("iot-connected-devices-status", false)).isNull();
		assertThat(Topics.protectedReason("salesforce-connector-offsets", false)).isNull();
		assertThat(Topics.protectedReason("connections-configs", false)).isNull();
	}

	@Test
	void underReplicationIgnoresAddingReplicasAndOfflinePartitions() {
		Node b1 = new Node(1, "h1", 9092);
		Node b2 = new Node(2, "h2", 9092);
		Node b3 = new Node(3, "h3", 9092);
		Node b4 = new Node(4, "h4", 9092);
		// reassignment: 4 is being added, not in the ISR yet
		TopicPartitionInfo moving = new TopicPartitionInfo(0, b1, List.of(b1, b2, b3, b4), List.of(b1, b2, b3));
		TopicPartitionInfo urp = new TopicPartitionInfo(1, b1, List.of(b1, b2, b3), List.of(b1, b2));
		TopicPartitionInfo offline = new TopicPartitionInfo(2, null, List.of(b2, b3), List.of(b2));
		TopicDescription d = new TopicDescription("t", false, List.of(moving, urp, offline));
		Topics.Reassignments r = new Topics.Reassignments(
				Map.of(new TopicPartition("t", 0), new PartitionReassignment(List.of(1, 2, 3, 4), List.of(4), List.of())),
				null);

		assertThat(Topics.underReplicated(moving, r.adding(new TopicPartition("t", 0)))).isFalse();
		assertThat(Topics.underReplicated(moving, List.of())).isTrue();
		assertThat(Topics.underReplicated(urp, r.adding(new TopicPartition("t", 1)))).isTrue();
		assertThat(Topics.underReplicated(offline, List.of())).isFalse();
		assertThat(Topics.offline(offline)).isTrue();
		assertThat(Topics.replicationFactor(d, r)).isEqualTo(3);
		assertThat(Topics.replicationFactor(d, Topics.Reassignments.NONE)).isEqualTo(4);
	}

	@Test
	void hangingSuspectRequiresAgeOverMaxTimeoutAndNoCoordinatorTransaction() {
		long now = System.currentTimeMillis();
		long max = Producers.DEFAULT_MAX_TRANSACTION_TIMEOUT_MS;
		ProducerState fresh = state(7, now - 1_000, true);
		ProducerState old = state(7, now - max - 60_000, true);
		ProducerState idle = state(7, now - max - 60_000, false);
		// not in the coordinator list but just started: not hanging (race with listTransactions)
		assertThat(Producers.hangingSuspect(fresh, Set.of(), max, now)).isFalse();
		assertThat(Producers.hangingSuspect(old, Set.of(), max, now)).isTrue();
		assertThat(Producers.hangingSuspect(old, Set.of(7L), max, now)).isFalse();
		assertThat(Producers.hangingSuspect(idle, Set.of(), max, now)).isFalse();
	}

	@Test
	void boundedWaitsReturnPartialResultsWithoutConsumingTheDeadline() {
		ToolCall call = new ToolCall("t", Map.of(), null, Instant.now().plusMillis(KafkaClients.TIMEOUT_MS));
		KafkaFutureImpl<Integer> never = new KafkaFutureImpl<>();
		Map<String, KafkaFuture<Integer>> futures = new LinkedHashMap<>();
		futures.put("slow", never);
		futures.put("fast", KafkaFuture.completedFuture(1));
		long start = System.nanoTime();
		Futures.Each<String, Integer> each = Futures.awaitEach(call, futures, 300);
		assertThat(each.values()).containsExactly(Map.entry("fast", 1));
		assertThat(each.errors()).containsEntry("slow", "No response (timeout)");
		assertThatThrownBy(() -> Futures.awaitAtMost(call, never, 200)).isInstanceOf(ToolException.class)
			.hasMessageContaining("timeout");
		assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(3_000);
	}

	@Test
	void metadataVersionMapping() {
		assertThat(KafkaVersions.metadataVersion(27)).isEqualTo("4.1-IV1");
		assertThat(KafkaVersions.release(27)).isEqualTo("4.1");
		assertThat(KafkaVersions.release(21)).isEqualTo("3.9");
		assertThat(KafkaVersions.release(7)).isEqualTo("3.3");
		assertThat(KafkaVersions.release(1)).isEqualTo("3.0");
		assertThat(KafkaVersions.release(99)).isEqualTo(">4.2");
		assertThat(KafkaVersions.release(0)).isNull();
		assertThat(KafkaVersions.releaseOf("3.6-IV2")).isEqualTo("3.6");
		assertThat(KafkaVersions.releaseOf("2.8")).isEqualTo("2.8");
		assertThat(KafkaVersions.releaseOf("3.7.1")).isEqualTo("3.7");
	}

	@Test
	void formatHelpers() {
		assertThat(Format.bytes(512)).isEqualTo("512 B");
		assertThat(Format.bytes(1536)).isEqualTo("1.5 KiB");
		assertThat(Format.bytes(1288490189L)).isEqualTo("1.2 GiB");
		assertThat(Format.iso(0)).isEqualTo("1970-01-01T00:00:00Z");
		assertThat(Format.iso(-1)).isNull();
		assertThat(Format.ageSeconds(-1)).isNull();
		assertThat(Format.ageSeconds(System.currentTimeMillis() - 5_000)).isBetween(4L, 6L);
		assertThat(Format.round(1.2345, 2)).isEqualTo(1.23);
		assertThat(ConsumerGroups.lag(10, 7)).isZero();
		assertThat(ConsumerGroups.lag(3, 7)).isEqualTo(4);
	}

	private static ProducerState state(long producerId, long lastTimestamp, boolean openTransaction) {
		return new ProducerState(producerId, 0, 3, lastTimestamp, OptionalInt.empty(),
				openTransaction ? OptionalLong.of(10) : OptionalLong.empty());
	}

}
