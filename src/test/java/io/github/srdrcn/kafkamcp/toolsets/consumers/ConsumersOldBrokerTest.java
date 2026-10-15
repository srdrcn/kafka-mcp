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
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolHandler;

/**
 * Eski ZooKeeper broker'ları (Kafka 2.x, bkz. {@link OldBrokerAdmin}): listede state/type gelmese de gruplar
 * doğru state, type ve simple bilgisiyle listelenir (Empty gruplar kaçmaz); OffsetDelete'i olmayan broker'da
 * araç asılmadan Türkçe "desteklemiyor" hatası verir.
 */
class ConsumersOldBrokerTest extends ConsumersMixedTestBase {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	@Autowired
	private KafkaClients clients;

	@Autowired
	private GroupTools groupTools;

	@Autowired
	private OffsetTools offsetTools;

	/** Handler'ı Kafka 2.3 broker'ını taklit eden admin'le (ZooKeeper cluster'ı, 15 sn deadline) çağırır. */
	private JsonNode runOnOldBroker(ToolHandler handler, Map<String, Object> args) throws Exception {
		return run(OldBrokerAdmin.wrap(zkAdmin()), handler, args);
	}

	/** Handler'ı verilen admin'le (15 sn deadline) çağırır. */
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
		// type'ı bildirmeyen broker'da da simple grup işaretlenir.
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
	 * Kafka 2.6-3.7 (ListGroups v4): listede state var, type yok. ZooKeeper'daki gruplar hep classic: type
	 * describe etmeden (tek describeFeatures ile) tamamlanır; get ile aynı type'ı gösterir.
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
		// state listede var, consumer tipi grup ZooKeeper'da olamaz: hiçbir grup describe edilmedi.
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
	 * KRaft 3.7 (KIP-848 early access, ListGroups v4): consumer tipi grup olabilir; type'ı state'ten
	 * çıkarılamayan gruplar describe edilir (simple gruplar hariç).
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
	 * Kafka 2.3: state'i describe ile de alınamayan grup sessizce kaybolmaz; liste partial işaretlenir.
	 * find_by_member list()'in zaten describe ettiği grupları tekrar describe etmez.
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
		assertThat(list.at("/listErrors/0").asText()).contains("State'i alınamayan grup: 1");

		JsonNode emptyGroups = run(faulty, groupTools::empty, on(ZK, "filter", Pattern.quote(prefix)));
		assertThat(emptyGroups.get("total").asInt()).isEqualTo(1);
		assertThat(emptyGroups.at("/groups/0/groupId").asText()).isEqualTo(empty);
		assertThat(emptyGroups.get("partial").asBoolean()).isTrue();
		assertThat(emptyGroups.at("/listErrors/0").asText()).contains("State'i alınamayan grup: 1");
	}

	@Test
	void offsetsDeleteOnBrokerWithoutOffsetDeleteFailsWithTurkishMessage() throws Exception {
		String prefix = unique("zkold");
		String topic = prefix + "-t";
		createTopic(ZK, topic, 1);
		produce(ZK, topic, 0, 5);
		String group = prefix + "-g";
		alter(ZK, group, topic, Map.of(0, 3L));

		assertThatThrownBy(() -> runOnOldBroker(offsetTools::deleteOffsets, on(ZK, "group", group, "topic", topic)))
			.isInstanceOf(ToolException.class)
			.hasMessageContaining("Offset'ler silinemedi")
			.hasMessageContaining("desteklemiyor (sürüm)");
		assertThat(committed(ZK, group, topic)).isEqualTo(Map.of(0, 3L));
	}

}
