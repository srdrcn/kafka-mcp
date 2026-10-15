package io.github.srdrcn.kafkamcp.toolsets.consumers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
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

	/** Handler'ı eski broker'ı taklit eden admin'le (ZooKeeper cluster'ı, 15 sn deadline) çağırır. */
	private JsonNode runOnOldBroker(ToolHandler handler, Map<String, Object> args) throws Exception {
		Admin old = OldBrokerAdmin.wrap(zkAdmin());
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
