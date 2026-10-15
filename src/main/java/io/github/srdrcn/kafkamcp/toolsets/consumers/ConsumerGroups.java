package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Consumer group listeleme ve toplu describe yardımcıları (state/type adları, üyeler, partition sahipleri).
 * Tüm çağrılar tek admin isteğiyle yapılır; tek bir grubun hatası diğerlerini etkilemez.
 */
public final class ConsumerGroups {

	/** Group listesindeki bir satır. state/type broker bildirmezse null. */
	public record GroupSummary(String groupId, String state, String type, String protocol, boolean simple) {

		public boolean isEmptyState() {
			return GroupState.EMPTY.toString().equals(state);
		}

		/** Üyesi olabilecek durumda mı (Empty/Dead değil). */
		public boolean mayHaveMembers() {
			return !GroupState.EMPTY.toString().equals(state) && !GroupState.DEAD.toString().equals(state);
		}

		public Map<String, Object> toMap() {
			return Results.map("groupId", groupId, "state", state, "type", type, "protocol", protocol, "simple",
					simple ? Boolean.TRUE : null);
		}

	}

	/** listGroups sonucu: başarıyla listelenen gruplar (groupId'ye göre sıralı) ve broker hataları. */
	public record GroupList(List<GroupSummary> groups, List<String> errors) {

		public Map<String, GroupSummary> byId() {
			Map<String, GroupSummary> m = new LinkedHashMap<>();
			groups.forEach(g -> m.put(g.groupId(), g));
			return m;
		}

	}

	private ConsumerGroups() {
	}

	/** Tüm consumer group'lar (classic + consumer protokolü) tek listGroups çağrısıyla. */
	public static GroupList list(ToolCall call) {
		ListGroupsResult r = call.admin().listGroups(ListGroupsOptions.forConsumerGroups());
		Settled<Collection<GroupListing>> valid = Settled.of(call, r.valid());
		Settled<Collection<Throwable>> errors = Settled.of(call, r.errors());
		if (!valid.ok()) {
			throw new ToolException(valid.errorMessage(), valid.error());
		}
		List<String> errorMessages = new ArrayList<>();
		if (errors.ok() && errors.value() != null) {
			errors.value().forEach(e -> errorMessages.add(KafkaErrors.explain(e)));
		}
		if (valid.value().isEmpty() && !errorMessages.isEmpty()) {
			throw new ToolException(errorMessages.get(0));
		}
		List<GroupSummary> groups = new ArrayList<>();
		for (GroupListing g : valid.value()) {
			String protocol = g.protocol() == null || g.protocol().isBlank() ? null : g.protocol();
			groups.add(new GroupSummary(g.groupId(), g.groupState().map(ConsumerGroups::state).orElse(null),
					g.type().map(ConsumerGroups::type).orElse(null), protocol, g.isSimpleConsumerGroup()));
		}
		groups.sort(Comparator.comparing(GroupSummary::groupId));
		return new GroupList(groups, errorMessages);
	}

	/** Grupları tek describeConsumerGroups çağrısıyla describe eder; kaybolan gruplar hata olarak döner. */
	public static Map<String, Settled<ConsumerGroupDescription>> describe(ToolCall call, Collection<String> groups) {
		Map<String, Settled<ConsumerGroupDescription>> out = new LinkedHashMap<>();
		if (groups.isEmpty()) {
			return out;
		}
		List<String> ids = new ArrayList<>(new LinkedHashSet<>(groups));
		Map<String, Settled<ConsumerGroupDescription>> settled = Settled.all(call,
				call.admin().describeConsumerGroups(ids).describedGroups());
		for (String id : ids) {
			out.put(id, settled.get(id));
		}
		return out;
	}

	/** Tek grubu describe eder; grup yoksa Türkçe {@link ToolException}. */
	public static ConsumerGroupDescription describeExisting(ToolCall call, String group) {
		Settled<ConsumerGroupDescription> s = describe(call, List.of(group)).get(group);
		if (s == null || !s.ok()) {
			if (s != null && notFound(s.error())) {
				throw notFoundError(group);
			}
			throw new ToolException(s == null ? "Grup describe edilemedi: " + group : s.errorMessage(),
					s == null ? null : s.error());
		}
		return s.value();
	}

	public static ToolException notFoundError(String group) {
		return new ToolException("Consumer group bulunamadı: " + group + " (consumer_groups_list ile bakın)");
	}

	public static boolean notFound(Throwable e) {
		return KafkaErrors.unwrap(e) instanceof GroupIdNotFoundException;
	}

	/** Eski broker'lar olmayan grup için Dead + üyesiz döner. */
	public static boolean deadOrUnknown(ConsumerGroupDescription d) {
		GroupState s = d.groupState();
		return (s == null || s == GroupState.DEAD || s == GroupState.UNKNOWN) && d.members().isEmpty();
	}

	/** "Stable", "Empty", ... (Kafka'nın kendi adları). */
	public static String state(GroupState s) {
		return s == null ? null : s.toString();
	}

	/** "classic" ya da "consumer" (KIP-848). */
	public static String type(GroupType t) {
		return t == null ? null : t.toString().toLowerCase(Locale.ROOT);
	}

	/** Koordinatör broker'ı {id, host, port}; bilinmiyorsa null. */
	public static Map<String, Object> node(Node n) {
		if (n == null || n.isEmpty()) {
			return null;
		}
		return Results.map("id", n.id(), "host", n.host(), "port", n.port());
	}

	/** Partition listesini topic → sıralı partition numaraları olarak gruplar (topic adına göre sıralı). */
	public static Map<String, List<Integer>> byTopic(Collection<TopicPartition> partitions) {
		Map<String, List<Integer>> m = new TreeMap<>();
		for (TopicPartition tp : partitions) {
			m.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp.partition());
		}
		m.values().forEach(l -> l.sort(Comparator.naturalOrder()));
		return m;
	}

	/** Atanmış partition → sahibi üye. */
	public static Map<TopicPartition, MemberDescription> owners(ConsumerGroupDescription d) {
		Map<TopicPartition, MemberDescription> m = new HashMap<>();
		if (d == null) {
			return m;
		}
		for (MemberDescription member : d.members()) {
			if (member.assignment() != null) {
				member.assignment().topicPartitions().forEach(tp -> m.put(tp, member));
			}
		}
		return m;
	}

	/** Üyeler clientId, memberId sırasıyla. */
	public static List<MemberDescription> sortedMembers(ConsumerGroupDescription d) {
		List<MemberDescription> members = new ArrayList<>(d.members());
		members.sort(Comparator.comparing((MemberDescription m) -> String.valueOf(m.clientId()))
			.thenComparing(m -> String.valueOf(m.consumerId())));
		return members;
	}

	/** Üye satırı: memberId, groupInstanceId, clientId, host ve topic → partition ataması. */
	public static Map<String, Object> memberMap(MemberDescription m) {
		Map<String, List<Integer>> assignment = m.assignment() == null ? Map.of()
				: byTopic(m.assignment().topicPartitions());
		return Results.map("memberId", m.consumerId(), "groupInstanceId", m.groupInstanceId().orElse(null),
				"clientId", m.clientId(), "host", m.host(), "assignment", assignment);
	}

}
