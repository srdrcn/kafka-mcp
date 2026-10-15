package io.github.srdrcn.kafkamcp.toolsets.consumers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolHandler;

/**
 * Old ZooKeeper brokers (Kafka 2.x, see {@link OldBrokerAdmin}): even without state/type in the listing, groups
 * are listed with the correct state, type and simple flag (Empty groups are not missed); on a broker without
 * OffsetDelete the tool fails fast with a readable "does not support" error instead of hanging.
 */
class ConsumersOldBrokerTest extends ConsumersMixedTestBase {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	@Autowired
	private KafkaClients clients;

	@Autowired
	private GroupTools groupTools;

	@Autowired
	private OffsetTools offsetTools;

	/** Calls the handler with an admin that mimics a Kafka 2.3 broker (ZooKeeper cluster, 15 s deadline). */
	private JsonNode runOnOldBroker(ToolHandler handler, Map<String, Object> args) throws Exception {
		return run(OldBrokerAdmin.wrap(zkAdmin()), handler, args);
	}

	/** Calls the handler with the given admin (15 s deadline). */
	private JsonNode run(Admin old, ToolHandler handler, Map<String, Object> args) throws Exception {
		KafkaClients oldClients = new KafkaClients(clients.config()) {
			@Override
			public Admin admin(String cluster) {
				return old;
			}
		};
		Instant deadline = Instant.now().plusMillis(KafkaClients.TIMEOUT_MS);
		Object result = handler.handle(new ToolCall("test", args, oldClients, deadline));
		assertThat(Instant.now()).isBefore(deadline);
		return MAPPER.valueToTree(result);
	}

	@Test
	void groupsWithoutStateOrTypeInListingAreCompletedFromDescribe() throws Exception {
		String prefix = unique("zkold");
		String topic = prefix + "-t";
		createTopic(ZK, topic, 1);
		produce(ZK, topic, 0, 10);
		String active = prefix + "-a-active";
		join(ZK, CLASSIC, active, prefix + "-c", topic);
		String empty = prefix + "-b-empty";
		emptyGroup(ZK, CLASSIC, empty, topic, Map.of(0, 4L));
		String simple = prefix + "-c-simple";
		alter(ZK, simple, topic, Map.of(0, 10L));

		JsonNode list = runOnOldBroker(groupTools::list, on(ZK, "filter", Pattern.quote(prefix)));
		assertThat(list.get("total").asInt()).isEqualTo(3);
		assertThat(list.at("/byState/Stable").asInt()).isEqualTo(1);
		assertThat(list.at("/byState/Empty").asInt()).isEqualTo(2);
		assertThat(list.has("byState") && list.get("byState").has("Unknown")).isFalse();
		assertThat(list.at("/groups/0/state").asText()).isEqualTo("Stable");
		assertThat(list.at("/groups/0/type").asText()).isEqualTo("classic");
		assertThat(list.at("/groups/0/protocol").asText()).isEqualTo("consumer");
		assertThat(list.at("/groups/1/state").asText()).isEqualTo("Empty");
		assertThat(list.at("/groups/2/groupId").asText()).isEqualTo(simple);
		assertThat(list.at("/groups/2/state").asText()).isEqualTo("Empty");
		// the simple group is marked even on a broker that does not report the type.
		assertThat(list.at("/groups/2/simple").asBoolean()).isTrue();

		JsonNode emptyOnly = runOnOldBroker(groupTools::list,
				on(ZK, "filter", Pattern.quote(prefix), "state", "Empty"));
		assertThat(emptyOnly.get("total").asInt()).isEqualTo(2);

		JsonNode emptyGroups = runOnOldBroker(groupTools::empty, on(ZK, "filter", Pattern.quote(prefix)));
		assertThat(emptyGroups.get("total").asInt()).isEqualTo(2);
		assertThat(emptyGroups.at("/groups/0/groupId").asText()).isEqualTo(empty);
		assertThat(emptyGroups.at("/groups/0/totalLag").asLong()).isEqualTo(6);
		assertThat(emptyGroups.at("/groups/1/groupId").asText()).isEqualTo(simple);
		assertThat(emptyGroups.at("/groups/1/simple").asBoolean()).isTrue();

		JsonNode found = runOnOldBroker(groupTools::findByMember, on(ZK, "clientId", Pattern.quote(prefix + "-c")));
		assertThat(found.get("matchedMembers").asInt()).isEqualTo(1);
		assertThat(found.at("/groups/0/groupId").asText()).isEqualTo(active);
	}

	/**
	 * Kafka 2.6-3.7 (ListGroups v4): state but no type in the listing. Groups on ZooKeeper are always classic: the
	 * type is completed without describe (with a single describeFeatures) and matches the type shown by get.
	 */
	@Test
	void zooKeeperGroupsWithStateButWithoutTypeAreClassicWithoutDescribe() throws Exception {
		String prefix = unique("zk37");
		String topic = prefix + "-t";
		createTopic(ZK, topic, 1);
		produce(ZK, topic, 0, 10);
		String active = prefix + "-a-active";
		join(ZK, CLASSIC, active, prefix + "-c", topic);
		String empty = prefix + "-b-empty";
		emptyGroup(ZK, CLASSIC, empty, topic, Map.of(0, 4L));
		String simple = prefix + "-c-simple";
		alter(ZK, simple, topic, Map.of(0, 10L));

		OldBrokerAdmin old = OldBrokerAdmin.of(zkAdmin(), OldBrokerAdmin.Era.KAFKA_3_7);
		JsonNode list = run(old.admin(), groupTools::list, on(ZK, "filter", Pattern.quote(prefix)));
		assertThat(list.get("total").asInt()).isEqualTo(3);
		assertThat(list.has("partial")).isFalse();
		for (int i = 0; i < 3; i++) {
			assertThat(list.at("/groups/" + i + "/type").asText()).as("groups/" + i).isEqualTo("classic");
		}
		assertThat(list.at("/groups/0/state").asText()).isEqualTo("Stable");
		assertThat(list.at("/groups/1/state").asText()).isEqualTo("Empty");
		assertThat(list.at("/groups/2/simple").asBoolean()).isTrue();
		// the state is in the listing and consumer-type groups are impossible on ZooKeeper: no group was described.
		assertThat(old.describedGroups()).isEmpty();

		JsonNode emptyGroups = run(old.admin(), groupTools::empty, on(ZK, "filter", Pattern.quote(prefix)));
		assertThat(emptyGroups.get("total").asInt()).isEqualTo(2);
		assertThat(emptyGroups.at("/groups/0/groupId").asText()).isEqualTo(empty);
		assertThat(emptyGroups.at("/groups/0/type").asText()).isEqualTo("classic");
		assertThat(emptyGroups.at("/groups/1/type").asText()).isEqualTo("classic");

		JsonNode get = run(old.admin(), groupTools::get, on(ZK, "group", empty));
		assertThat(get.get("type").asText()).isEqualTo(list.at("/groups/1/type").asText());
	}

	/**
	 * KRaft 3.7 (KIP-848 early access, ListGroups v4): consumer-type groups are possible; groups whose type cannot
	 * be inferred from the state are described (except simple groups).
	 */
	@Test
	void kraftGroupsWithoutTypeAreDescribedToTellConsumerFromClassic() throws Exception {
		awaitGroupCoordinator(KRAFT);
		String prefix = unique("kr37");
		String topic = prefix + "-t";
		createTopic(KRAFT, topic, 1);
		produce(KRAFT, topic, 0, 10);
		String active = prefix + "-a-active";
		join(KRAFT, CONSUMER, active, prefix + "-c", topic);
		String empty = prefix + "-b-empty";
		emptyGroup(KRAFT, CLASSIC, empty, topic, Map.of(0, 4L));
		String simple = prefix + "-c-simple";
		alter(KRAFT, simple, topic, Map.of(0, 10L));

		OldBrokerAdmin old = OldBrokerAdmin.of(testAdmin(), OldBrokerAdmin.Era.KAFKA_3_7);
		JsonNode list = run(old.admin(), groupTools::list, on(KRAFT, "filter", Pattern.quote(prefix)));
		assertThat(list.get("total").asInt()).isEqualTo(3);
		assertThat(list.at("/groups/0/groupId").asText()).isEqualTo(active);
		assertThat(list.at("/groups/0/type").asText()).isEqualTo("consumer");
		assertThat(list.at("/groups/1/type").asText()).isEqualTo("classic");
		assertThat(list.at("/groups/2/type").asText()).isEqualTo("classic");
		assertThat(list.has("partial")).isFalse();
		assertThat(old.describedGroups()).containsExactly(active, empty);
	}

	/**
	 * Kafka 2.3: a group whose state cannot be fetched with describe either is not silently lost; the list is marked
	 * partial. find_by_member does not describe again the groups list() has already described.
	 */
	@Test
	void groupsWhoseStateCannotBeCompletedMarkResultPartialAndAreNotDescribedTwice() throws Exception {
		String prefix = unique("zkold");
		String topic = prefix + "-t";
		createTopic(ZK, topic, 1);
		produce(ZK, topic, 0, 10);
		String active = prefix + "-a-active";
		join(ZK, CLASSIC, active, prefix + "-c", topic);
		String empty = prefix + "-b-empty";
		emptyGroup(ZK, CLASSIC, empty, topic, Map.of(0, 4L));
		String hung = prefix + "-c-hung";
		emptyGroup(ZK, CLASSIC, hung, topic, Map.of(0, 2L));

		OldBrokerAdmin counting = OldBrokerAdmin.of(zkAdmin(), OldBrokerAdmin.Era.KAFKA_2_3);
		JsonNode found = run(counting.admin(), groupTools::findByMember,
				on(ZK, "clientId", Pattern.quote(prefix + "-c")));
		assertThat(found.get("matchedMembers").asInt()).isEqualTo(1);
		assertThat(found.at("/groups/0/groupId").asText()).isEqualTo(active);
		assertThat(counting.describedGroups()).filteredOn(g -> g.startsWith(prefix))
			.containsExactlyInAnyOrder(active, empty, hung);

		Admin faulty = OldBrokerAdmin.wrap(FaultyAdmin.wrap(zkAdmin(), Set.of(hung), Set.of()));
		JsonNode list = run(faulty, groupTools::list, on(ZK, "filter", Pattern.quote(prefix)));
		assertThat(list.get("total").asInt()).isEqualTo(3);
		assertThat(list.at("/byState/Unknown").asInt()).isEqualTo(1);
		assertThat(list.get("partial").asBoolean()).isTrue();
		assertThat(list.at("/listErrors/0").asText()).contains("Groups whose state could not be fetched: 1");

		JsonNode emptyGroups = run(faulty, groupTools::empty, on(ZK, "filter", Pattern.quote(prefix)));
		assertThat(emptyGroups.get("total").asInt()).isEqualTo(1);
		assertThat(emptyGroups.at("/groups/0/groupId").asText()).isEqualTo(empty);
		assertThat(emptyGroups.get("partial").asBoolean()).isTrue();
		assertThat(emptyGroups.at("/listErrors/0").asText()).contains("Groups whose state could not be fetched: 1");
	}

	@Test
	void offsetsDeleteOnBrokerWithoutOffsetDeleteFailsWithUnsupportedMessage() throws Exception {
		String prefix = unique("zkold");
		String topic = prefix + "-t";
		createTopic(ZK, topic, 1);
		produce(ZK, topic, 0, 5);
		String group = prefix + "-g";
		alter(ZK, group, topic, Map.of(0, 3L));

		assertThatThrownBy(() -> runOnOldBroker(offsetTools::deleteOffsets, on(ZK, "group", group, "topic", topic)))
			.isInstanceOf(ToolException.class)
			.hasMessageContaining("Could not delete the offsets")
			.hasMessageContaining(KafkaErrors.UNSUPPORTED);
		assertThat(committed(ZK, group, topic)).isEqualTo(Map.of(0, 3L));
	}

}
