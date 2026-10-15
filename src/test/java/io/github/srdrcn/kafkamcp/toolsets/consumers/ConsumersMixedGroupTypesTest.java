package io.github.srdrcn.kafkamcp.toolsets.consumers;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Moda göre farklı çıktı: KRaft (Kafka 4.1) cluster'ında KIP-848 {@code consumer} tipi group'lar (sunucu
 * tarafı assignor, group/member epoch'ları) ve classic group'lar bir arada; ZooKeeper cluster'ında group'lar
 * hep {@code classic}. Aynı araçlar her iki tipte doğru alanları raporlamalı.
 */
class ConsumersMixedGroupTypesTest extends ConsumersMixedTestBase {

	private static void commitByOwner(List<KafkaConsumer<byte[], byte[]>> members, String topic,
			Map<Integer, Long> offsets) {
		for (KafkaConsumer<byte[], byte[]> c : members) {
			Map<Integer, Long> owned = new HashMap<>();
			offsets.forEach((p, o) -> {
				if (c.assignment().contains(new TopicPartition(topic, p))) {
					owned.put(p, o);
				}
			});
			if (!owned.isEmpty()) {
				commit(c, topic, owned);
			}
		}
	}

	@Test
	void consumerTypeGroupsOnKraftAndClassicGroupsOnZooKeeper() throws Exception {
		String prefix = unique("gty");
		String topic = prefix + "-t";
		String group = prefix + "-a-active";
		String classicOnKraft = prefix + "-b-classic";
		Map<String, List<KafkaConsumer<byte[], byte[]>>> members = new HashMap<>();
		for (String cluster : List.of(KRAFT, ZK)) {
			createTopic(cluster, topic, 2);
			produce(cluster, topic, 0, 8);
			produce(cluster, topic, 1, 4);
			// ZooKeeper broker'ları KIP-848'i desteklemez: orada aynı grup classic protokolle kurulur.
			String protocol = KRAFT.equals(cluster) ? CONSUMER : CLASSIC;
			List<KafkaConsumer<byte[], byte[]>> m = joinAll(cluster, protocol, group, prefix + "-cli", 2,
					List.of(topic));
			commitByOwner(m, topic, Map.of(0, 3L, 1, 1L));
			members.put(cluster, m);
		}
		emptyGroup(KRAFT, CLASSIC, classicOnKraft, topic, Map.of(0, 8L));

		JsonNode kraftList = ok("consumer_groups_list", on(KRAFT, "filter", Pattern.quote(prefix)));
		assertThat(kraftList.get("total").asInt()).isEqualTo(2);
		assertThat(kraftList.at("/groups/0/groupId").asText()).isEqualTo(group);
		assertThat(kraftList.at("/groups/0/type").asText()).isEqualTo("consumer");
		assertThat(kraftList.at("/groups/0/state").asText()).isEqualTo("Stable");
		assertThat(kraftList.at("/groups/0/protocol").asText()).isEqualTo("consumer");
		assertThat(kraftList.at("/groups/1/groupId").asText()).isEqualTo(classicOnKraft);
		assertThat(kraftList.at("/groups/1/type").asText()).isEqualTo("classic");
		assertThat(kraftList.at("/groups/1/state").asText()).isEqualTo("Empty");
		JsonNode zkList = ok("consumer_groups_list", on(ZK, "filter", Pattern.quote(prefix)));
		assertThat(zkList.get("total").asInt()).isEqualTo(1);
		assertThat(zkList.at("/groups/0/type").asText()).isEqualTo("classic");
		assertThat(zkList.at("/groups/0/state").asText()).isEqualTo("Stable");

		JsonNode kraft = ok("consumer_groups_get", on(KRAFT, "group", group));
		assertThat(kraft.get("type").asText()).isEqualTo("consumer");
		assertThat(kraft.get("state").asText()).isEqualTo("Stable");
		// KIP-848: atamayı broker'daki (sunucu tarafı) assignor yapar.
		assertThat(kraft.get("partitionAssignor").asText()).isEqualTo("uniform");
		assertThat(kraft.get("groupEpoch").asInt()).isPositive();
		assertThat(kraft.get("targetAssignmentEpoch").asInt()).isEqualTo(kraft.get("groupEpoch").asInt());
		assertThat(kraft.get("memberCount").asInt()).isEqualTo(2);
		List<String> clientIds = new ArrayList<>();
		for (JsonNode m : kraft.get("members")) {
			clientIds.add(m.get("clientId").asText());
			assertThat(m.get("memberEpoch").asInt()).isPositive();
			assertThat(m.get("assignment").get(topic)).hasSize(1);
			// Stable: mevcut atama hedef atamayla aynı; alınacak/bırakılacak partition yok.
			assertThat(m.has("pendingPartitions")).isFalse();
			assertThat(m.has("classicMember")).isFalse();
		}
		assertThat(clientIds).containsExactly(prefix + "-cli-0", prefix + "-cli-1");
		assertThat(kraft.at("/totals/lag").asLong()).isEqualTo(8);
		assertThat(kraft.at("/offsets/0/partitions/0/lag").asLong()).isEqualTo(5);
		assertThat(kraft.at("/offsets/0/partitions/1/lag").asLong()).isEqualTo(3);
		assertThat(kraft.at("/offsets/0/partitions/0/owner").asText()).isNotBlank();

		JsonNode zk = ok("consumer_groups_get", on(ZK, "group", group));
		assertThat(zk.get("type").asText()).isEqualTo("classic");
		assertThat(zk.get("state").asText()).isEqualTo("Stable");
		assertThat(zk.get("partitionAssignor").asText()).isEqualTo("range");
		assertThat(zk.has("groupEpoch")).isFalse();
		assertThat(zk.at("/members/0").has("memberEpoch")).isFalse();
		assertThat(zk.at("/totals/lag").asLong()).isEqualTo(8);

		JsonNode classicGet = ok("consumer_groups_get", on(KRAFT, "group", classicOnKraft));
		assertThat(classicGet.get("type").asText()).isEqualTo("classic");
		assertThat(classicGet.has("groupEpoch")).isFalse();

		for (String cluster : List.of(KRAFT, ZK)) {
			JsonNode lag = ok("consumer_groups_lag", on(cluster, "group", group));
			assertThat(lag.get("totalLag").asLong()).as(cluster).isEqualTo(8);
			assertThat(lag.get("members").asInt()).isEqualTo(2);
			assertThat(lag.at("/topics/0/partitions/0/clientId").asText()).startsWith(prefix + "-cli-");
			JsonNode found = ok("consumer_groups_find_by_member",
					on(cluster, "clientId", "^" + Pattern.quote(prefix + "-cli-0") + "$"));
			assertThat(found.get("matchedMembers").asInt()).as(cluster).isEqualTo(1);
			assertThat(found.at("/groups/0/groupId").asText()).isEqualTo(group);
			assertThat(found.at("/groups/0/members/0/topics/0").asText()).isEqualTo(topic);
			JsonNode withLag = ok("consumer_groups_with_lag",
					on(cluster, "filter", Pattern.quote(group), "threshold", 1));
			assertThat(withLag.at("/groups/0/totalLag").asLong()).as(cluster).isEqualTo(8);
		}

		// Aktif üyeli consumer tipi grubun offset'i değiştirilmez/silinemez.
		assertThat(error("consumer_groups_offsets_reset", on(KRAFT, "group", group, "topic", topic, "to", "earliest")))
			.contains("aktif üye");
		assertThat(error("consumer_groups_offsets_delete", on(KRAFT, "group", group, "topic", topic)))
			.contains("abone");
		assertThat(error("consumer_groups_delete", on(KRAFT, "group", group))).contains("boş değil");
		assertThat(committed(KRAFT, group, topic)).isEqualTo(Map.of(0, 3L, 1, 1L));

		// Üyeler ayrılınca consumer tipi grup Empty olur; reset/delete araçları çalışır.
		for (KafkaConsumer<byte[], byte[]> c : members.get(KRAFT)) {
			leave(c);
		}
		waitUntil(() -> state(KRAFT, group) == GroupState.EMPTY, "consumer tipi grup Empty olmadı");
		JsonNode emptyGroups = ok("consumer_groups_empty", on(KRAFT, "filter", Pattern.quote(group)));
		assertThat(emptyGroups.at("/groups/0/type").asText()).isEqualTo("consumer");
		assertThat(emptyGroups.at("/groups/0/totalLag").asLong()).isEqualTo(8);

		JsonNode reset = ok("consumer_groups_offsets_reset", on(KRAFT, "group", group, "topic", topic, "to", "earliest"));
		assertThat(reset.get("groupState").asText()).isEqualTo("Empty");
		assertThat(reset.get("changedPartitions").asInt()).isEqualTo(2);
		assertThat(committed(KRAFT, group, topic)).isEqualTo(Map.of(0, 0L, 1, 0L));

		JsonNode deleted = ok("consumer_groups_offsets_delete",
				on(KRAFT, "group", group, "topic", topic, "partitions", List.of(1)));
		assertThat(deleted.get("deletedPartitions").asInt()).isEqualTo(1);
		assertThat(committed(KRAFT, group, topic)).isEqualTo(Map.of(0, 0L));

		JsonNode del = ok("consumer_groups_delete", on(KRAFT, "group", group));
		assertThat(del.get("deleted").asBoolean()).isTrue();
		assertThat(groupIds(KRAFT)).doesNotContain(group);
		// ZooKeeper'daki aynı adlı classic grup etkilenmez.
		assertThat(groupIds(ZK)).contains(group);
		assertThat(committed(ZK, group, topic)).isEqualTo(Map.of(0, 3L, 1, 1L));
	}

	/**
	 * KIP-848 revoke sırasında (sadece KRaft): ConsumerGroupDescribe'daki atama broker'ın mevcut ∩ hedef
	 * kümesidir, revoke edilmekte olan partition yanıtta yoktur. Bırakan üyede uydurma bir "revoking" sayısı
	 * çıkmaz; sahipsiz görünen partition hedef üyesiyle (reassigningTo) işaretlenir, bekleyen üyede
	 * pendingPartitions görünür.
	 */
	@Test
	void revokingMemberOfConsumerTypeGroupOnKraft() throws Exception {
		awaitGroupCoordinator(KRAFT);
		String prefix = unique("rvk");
		String topic = prefix + "-t";
		String group = prefix + "-g";
		createTopic(KRAFT, topic, 2);
		produce(KRAFT, topic, 0, 6);
		produce(KRAFT, topic, 1, 4);
		// A'nın revoke callback'i uygulama thread'inde (poll içinde) çalışır: A poll edilmedikçe revoke bitmez.
		KafkaConsumer<byte[], byte[]> a = newGroupConsumer(KRAFT, CONSUMER, group, prefix + "-a");
		a.subscribe(List.of(topic), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			}

		});
		long deadline = System.currentTimeMillis() + 120_000;
		while (a.assignment().size() < 2 || state(KRAFT, group) != GroupState.STABLE) {
			assertThat(System.currentTimeMillis()).as("A gruba katılamadı").isLessThan(deadline);
			a.poll(Duration.ofMillis(100));
		}
		commit(a, topic, Map.of(0, 1L, 1, 1L));

		KafkaConsumer<byte[], byte[]> b = newGroupConsumer(KRAFT, CONSUMER, group, prefix + "-b");
		b.subscribe(List.of(topic));
		// Sadece B poll edilir: hedef atamada A bir partition'ı B'ye bırakmalı, ama callback'i çalışmadığı için
		// A revoke'ta (UNREVOKED_PARTITIONS) kalır, B o partition'ı bekler (Reconciling).
		ConsumerGroupDescription d = null;
		while (d == null) {
			assertThat(System.currentTimeMillis()).as("grup revoke aşamasına gelmedi").isLessThan(deadline);
			b.poll(Duration.ofMillis(200));
			ConsumerGroupDescription now = describe(KRAFT, group);
			MemberDescription ma = member(now, prefix + "-a");
			MemberDescription mb = member(now, prefix + "-b");
			if (ma != null && mb != null && ma.assignment().topicPartitions().size() == 1
					&& ma.targetAssignment().map(t -> t.topicPartitions().size()).orElse(0) == 1
					&& mb.assignment().topicPartitions().isEmpty()
					&& mb.targetAssignment().map(t -> t.topicPartitions().size()).orElse(0) == 1) {
				d = now;
			}
		}
		MemberDescription ma = member(d, prefix + "-a");
		MemberDescription mb = member(d, prefix + "-b");
		int kept = ma.assignment().topicPartitions().iterator().next().partition();
		int given = 1 - kept;
		assertThat(mb.targetAssignment().get().topicPartitions()).containsExactly(new TopicPartition(topic, given));

		JsonNode get = ok("consumer_groups_get", on(KRAFT, "group", group));
		assertThat(get.get("state").asText()).isEqualTo("Reconciling");
		assertThat(get.get("memberCount").asInt()).isEqualTo(2);
		JsonNode memberA = get.at("/members/0");
		JsonNode memberB = get.at("/members/1");
		assertThat(memberA.get("clientId").asText()).isEqualTo(prefix + "-a");
		assertThat(memberA.at("/assignment/" + topic)).hasSize(1);
		assertThat(memberA.has("revokingPartitions")).isFalse();
		assertThat(memberA.has("pendingPartitions")).isFalse();
		assertThat(memberB.get("clientId").asText()).isEqualTo(prefix + "-b");
		assertThat(memberB.get("pendingPartitions").asInt()).isEqualTo(1);
		JsonNode keptRow = get.at("/offsets/0/partitions/" + kept);
		JsonNode givenRow = get.at("/offsets/0/partitions/" + given);
		assertThat(keptRow.get("partition").asInt()).isEqualTo(kept);
		assertThat(keptRow.get("owner").asText()).isEqualTo(ma.consumerId());
		assertThat(keptRow.has("reassigningTo")).isFalse();
		assertThat(givenRow.get("partition").asInt()).isEqualTo(given);
		assertThat(givenRow.has("owner")).isFalse();
		assertThat(givenRow.get("reassigningTo").asText()).isEqualTo(mb.consumerId());
		assertThat(givenRow.get("committed").asLong()).isEqualTo(1);
		assertThat(get.get("note").asText()).contains("reassigningTo");

		JsonNode lag = ok("consumer_groups_lag", on(KRAFT, "group", group));
		assertThat(lag.get("totalLag").asLong()).isEqualTo(8);
		for (JsonNode row : lag.at("/topics/0/partitions")) {
			if (row.get("partition").asInt() == kept) {
				assertThat(row.get("clientId").asText()).isEqualTo(prefix + "-a");
				assertThat(row.has("reassigningTo")).isFalse();
			}
			else {
				assertThat(row.has("clientId")).isFalse();
				assertThat(row.get("reassigningTo").asText()).isEqualTo(prefix + "-b");
			}
		}
		assertThat(lag.get("note").asText()).contains("reassigningTo");

		// A poll edince revoke biter: partition B'ye geçer, grup Stable olur ve reassigningTo kalkar.
		long settle = System.currentTimeMillis() + 120_000;
		while (b.assignment().isEmpty() || state(KRAFT, group) != GroupState.STABLE) {
			assertThat(System.currentTimeMillis()).as("revoke tamamlanmadı").isLessThan(settle);
			a.poll(Duration.ofMillis(100));
			b.poll(Duration.ofMillis(100));
		}
		JsonNode settled = ok("consumer_groups_get", on(KRAFT, "group", group));
		assertThat(settled.at("/offsets/0/partitions/" + given + "/owner").asText()).isEqualTo(mb.consumerId());
		assertThat(settled.at("/offsets/0/partitions/" + given).has("reassigningTo")).isFalse();
		assertThat(settled.has("note")).isFalse();
	}

	private static MemberDescription member(ConsumerGroupDescription d, String clientId) {
		return d.members().stream().filter(m -> clientId.equals(m.clientId())).findFirst().orElse(null);
	}

}
