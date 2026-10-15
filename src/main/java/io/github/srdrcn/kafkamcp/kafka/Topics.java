package io.github.srdrcn.kafkamcp.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Topic listeleme/açıklama yardımcıları (tek listTopics, tek describeTopics). */
public final class Topics {

	/**
	 * Silinmesi reddedilen sistem topic'leri: Connect iç topic'leri (*connect-configs/-offsets/-status,
	 * Strimzi'nin *connect-cluster-* adları, Debezium örneklerindeki *connect_* adları), Schema Registry,
	 * MirrorMaker 2.
	 */
	private static final Pattern SYSTEM_TOPIC = Pattern.compile(
			"^(_schemas|_confluent.*|.*connect(-cluster)?[-_](configs|offsets|status|statuses)|.*\\.internal)$");

	/** listPartitionReassignments için en fazla bekleme; controller yanıt vermese de araç devam eder. */
	static final long REASSIGNMENTS_WAIT_MS = 3_000;

	private Topics() {
	}

	/** Topic adı → internal bayrağı (ada göre sıralı); includeInternal false ise internal'lar hariç. */
	public static Map<String, Boolean> list(ToolCall call, boolean includeInternal) {
		Collection<TopicListing> listings = call
			.await(call.admin().listTopics(new ListTopicsOptions().listInternal(includeInternal)).listings());
		Map<String, Boolean> out = new TreeMap<>();
		for (TopicListing l : listings) {
			out.put(l.name(), l.isInternal());
		}
		return out;
	}

	/** Tek describeTopics çağrısı; arada silinmiş/erişilemeyen topic'ler hatalarda döner. */
	public static Futures.Each<String, TopicDescription> describe(ToolCall call, Collection<String> names) {
		if (names.isEmpty()) {
			return new Futures.Each<>(new LinkedHashMap<>(), new LinkedHashMap<>());
		}
		return Futures.awaitEach(call, call.admin().describeTopics(names).topicNameValues());
	}

	/** listTopics + tek describeTopics: tüm topic'lerin açıklaması (ada göre sıralı). */
	public static Map<String, TopicDescription> describeAll(ToolCall call, boolean includeInternal) {
		return new TreeMap<>(describe(call, list(call, includeInternal).keySet()).requireAny().values());
	}

	/** Tek topic; yoksa "Topic bulunamadı" hatası. */
	public static TopicDescription describeOne(ToolCall call, String topic) {
		try {
			return call.await(call.admin().describeTopics(List.of(topic)).allTopicNames()).get(topic);
		}
		catch (ToolException e) {
			throw notFound(e, topic);
		}
	}

	/** UnknownTopicOrPartition hatasını "Topic bulunamadı: x" mesajına çevirir; diğerlerini olduğu gibi döner. */
	public static ToolException notFound(ToolException e, String topic) {
		if (KafkaErrors.unwrap(e.getCause()) instanceof UnknownTopicOrPartitionException) {
			return new ToolException("Topic bulunamadı: " + topic + " (topics_list ile bakın)", e.getCause());
		}
		return e;
	}

	/** Topic'in tüm partition'ları. */
	public static List<TopicPartition> partitions(TopicDescription d) {
		List<TopicPartition> out = new ArrayList<>(d.partitions().size());
		for (TopicPartitionInfo p : d.partitions()) {
			out.add(new TopicPartition(d.name(), p.partition()));
		}
		return out;
	}

	/** Lideri olan (offline olmayan) partition'lar; offset/producer sorguları için. */
	public static List<TopicPartition> onlinePartitions(Collection<TopicDescription> topics) {
		List<TopicPartition> out = new ArrayList<>();
		for (TopicDescription d : topics) {
			for (TopicPartitionInfo p : d.partitions()) {
				if (!offline(p)) {
					out.add(new TopicPartition(d.name(), p.partition()));
				}
			}
		}
		return out;
	}

	/** Partition'ın lideri yok mu? */
	public static boolean offline(TopicPartitionInfo p) {
		return p.leader() == null || p.leader().isEmpty() || p.leader().id() < 0;
	}

	/**
	 * URP mu? Kafka'nın UnderReplicatedPartitions metriği gibi: lideri olan ve ISR'ı, reassignment'ta eklenmekte
	 * olan replikalar hariç replika sayısından az olan partition. Offline partition'lar ayrıca sayılır.
	 */
	public static boolean underReplicated(TopicPartitionInfo p, Collection<Integer> adding) {
		return !offline(p) && p.isr().size() < replicaCount(p, adding);
	}

	/** Reassignment'ta eklenmekte olan replikalar hariç replika sayısı. */
	public static int replicaCount(TopicPartitionInfo p, Collection<Integer> adding) {
		if (adding.isEmpty()) {
			return p.replicas().size();
		}
		return (int) p.replicas().stream().filter(n -> !adding.contains(n.id())).count();
	}

	/** Lider id'si ya da null. */
	public static Integer leader(TopicPartitionInfo p) {
		return offline(p) ? null : p.leader().id();
	}

	/** Tercih edilen lider (ilk replika) id'si ya da null. */
	public static Integer preferredLeader(TopicPartitionInfo p) {
		return p.replicas().isEmpty() ? null : p.replicas().get(0).id();
	}

	/** Lider tercih edilen replika değil mi (offline hariç)? */
	public static boolean nonPreferredLeader(TopicPartitionInfo p) {
		Integer leader = leader(p);
		return leader != null && !leader.equals(preferredLeader(p));
	}

	/** Replication factor: partition'lardaki en büyük replika sayısı (reassignment'ta eklenenler hariç). */
	public static int replicationFactor(TopicDescription d, Reassignments reassignments) {
		return d.partitions()
			.stream()
			.mapToInt(p -> replicaCount(p, reassignments.adding(new TopicPartition(d.name(), p.partition()))))
			.max()
			.orElse(0);
	}

	/**
	 * Devam eden partition reassignment'ları. {@code error} doluysa (ör. Cluster:Describe yetkisi yok) liste
	 * boştur; URP/RF hesapları eklenen replikaları ayıklayamaz.
	 */
	public record Reassignments(Map<TopicPartition, PartitionReassignment> byPartition, String error) {

		/** Reassignment bilgisi olmadan hesap için boş değer. */
		public static final Reassignments NONE = new Reassignments(Map.of(), null);

		/** Partition'a eklenmekte olan replika id'leri (yoksa boş). */
		public List<Integer> adding(TopicPartition tp) {
			PartitionReassignment r = byPartition.get(tp);
			return r == null ? List.of() : r.addingReplicas();
		}

	}

	/** listPartitionReassignments isteğini gönderir (beklemez); sonucu {@link #reassignments} ile alın. */
	public static KafkaFuture<Map<TopicPartition, PartitionReassignment>> submitReassignments(ToolCall call) {
		return call.admin().listPartitionReassignments().reassignments();
	}

	/** Gönderilmiş listPartitionReassignments'ı en fazla birkaç sn bekler; hata/zaman aşımında boş + hata mesajı. */
	public static Reassignments reassignments(ToolCall call,
			KafkaFuture<Map<TopicPartition, PartitionReassignment>> pending) {
		try {
			return new Reassignments(Futures.awaitAtMost(call, pending, REASSIGNMENTS_WAIT_MS), null);
		}
		catch (ToolException e) {
			return new Reassignments(Map.of(), e.getMessage());
		}
	}

	/**
	 * Eligible leader replicas (KIP-966) id'leri; sadece KRaft'ta (ELR açıkken) dolu olabilir. ZooKeeper
	 * broker'ları bu alanı döndürmez (client null verir): null döner, NPE yok.
	 */
	public static List<Integer> elr(TopicPartitionInfo p) {
		return p.elr() == null ? null : ids(p.elr());
	}

	/** Son bilinen ELR id'leri (KRaft); ZooKeeper'da null. */
	public static List<Integer> lastKnownElr(TopicPartitionInfo p) {
		return p.lastKnownElr() == null ? null : ids(p.lastKnownElr());
	}

	/** Topic id'si; bilinmiyorsa (eski broker'lar sıfır UUID döndürür) null. */
	public static String topicId(TopicDescription d) {
		Uuid id = d.topicId();
		return id == null || Uuid.ZERO_UUID.equals(id) ? null : id.toString();
	}

	/** Node listesini id listesine çevirir. */
	public static List<Integer> ids(List<Node> nodes) {
		return nodes.stream().map(Node::id).toList();
	}

	/** Partition → lider id (offline olanlar hariç). */
	public static Map<TopicPartition, Integer> leaders(Collection<TopicDescription> topics) {
		Map<TopicPartition, Integer> out = new LinkedHashMap<>();
		for (TopicDescription d : topics) {
			for (TopicPartitionInfo p : d.partitions()) {
				Integer l = leader(p);
				if (l != null) {
					out.put(new TopicPartition(d.name(), p.partition()), l);
				}
			}
		}
		return out;
	}

	/** Silinmemesi/değiştirilmemesi gereken iç/sistem topic'i ise nedeni, değilse null. */
	public static String protectedReason(String name, boolean internal) {
		if (internal || name.startsWith("__")) {
			return "Kafka iç topic'i";
		}
		if (SYSTEM_TOPIC.matcher(name).matches()) {
			return "sistem topic'i (Connect/Schema Registry/MirrorMaker)";
		}
		return null;
	}

}
