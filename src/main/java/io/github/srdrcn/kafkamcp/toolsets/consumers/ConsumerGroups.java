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
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
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
 * Tüm çağrılar tek admin isteğiyle yapılır; tek bir grubun hatası ya da yanıtsız kalması diğerlerini etkilemez.
 */
public final class ConsumerGroups {

	/** list(): ardından gelen committed + end offset sorguları için deadline'dan ayrılan varsayılan pay. */
	public static final long LIST_RESERVE_MS = 6_000;

	/** describe(): çıktı için deadline'dan ayrılan varsayılan pay. */
	public static final long DESCRIBE_RESERVE_MS = 1_000;

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

	/**
	 * listGroups sonucu: başarıyla listelenen gruplar (groupId'ye göre sıralı) ve broker hataları (hata varsa
	 * liste eksik olabilir).
	 */
	public record GroupList(List<GroupSummary> groups, List<String> errors) {

		/** Yanıt vermeyen/hata veren broker'lar yüzünden liste eksik olabilir mi? */
		public boolean partial() {
			return !errors.isEmpty();
		}

		public Map<String, GroupSummary> byId() {
			Map<String, GroupSummary> m = new LinkedHashMap<>();
			groups.forEach(g -> m.put(g.groupId(), g));
			return m;
		}

	}

	private ConsumerGroups() {
	}

	/**
	 * Tüm consumer group'lar (classic + consumer protokolü) tek listGroups çağrısıyla; deadline'dan
	 * {@link #LIST_RESERVE_MS} pay bırakır.
	 */
	public static GroupList list(ToolCall call) {
		return list(call, LIST_RESERVE_MS);
	}

	/**
	 * Tüm consumer group'lar; istek en geç (kalan süre - reserveMillis) içinde biter. Yanıt vermeyen broker'lar
	 * {@link GroupList#errors()}'a düşer.
	 */
	public static GroupList list(ToolCall call, long reserveMillis) {
		ListGroupsResult r = call.admin()
			.listGroups(ListGroupsOptions.forConsumerGroups().timeoutMs(Settled.timeoutMs(call, reserveMillis)));
		Settled<Collection<GroupListing>> valid = Settled.within(call, r.valid(), reserveMillis);
		Settled<Collection<Throwable>> errors = Settled.within(call, r.errors(), reserveMillis);
		if (!valid.ok()) {
			if (valid.timedOut()) {
				throw new ToolException("Consumer group listesi süre içinde alınamadı (broker'lar yanıt vermedi)",
						valid.error());
			}
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

	/**
	 * Grupları tek describeConsumerGroups çağrısıyla describe eder; kaybolan gruplar hata, yanıt gelmeyenler
	 * {@link Settled#timedOut()} olarak döner. {@link #DESCRIBE_RESERVE_MS} pay bırakır.
	 */
	public static Map<String, Settled<ConsumerGroupDescription>> describe(ToolCall call, Collection<String> groups) {
		return describe(call, groups, DESCRIBE_RESERVE_MS);
	}

	/** {@link #describe(ToolCall, Collection)}; istek en geç (kalan süre - reserveMillis) içinde biter. */
	public static Map<String, Settled<ConsumerGroupDescription>> describe(ToolCall call, Collection<String> groups,
			long reserveMillis) {
		Map<String, Settled<ConsumerGroupDescription>> out = new LinkedHashMap<>();
		if (groups.isEmpty()) {
			return out;
		}
		List<String> ids = new ArrayList<>(new LinkedHashSet<>(groups));
		DescribeConsumerGroupsOptions options = new DescribeConsumerGroupsOptions()
			.timeoutMs(Settled.timeoutMs(call, reserveMillis));
		Map<String, Settled<ConsumerGroupDescription>> settled = Settled.all(call,
				call.admin().describeConsumerGroups(ids, options).describedGroups(), reserveMillis);
		for (String id : ids) {
			out.put(id, settled.get(id));
		}
		return out;
	}

	/** Tek grubu describe eder; grup yoksa Türkçe {@link ToolException}. */
	public static ConsumerGroupDescription describeExisting(ToolCall call, String group) {
		return describeExisting(call, group, DESCRIBE_RESERVE_MS);
	}

	/** {@link #describeExisting(ToolCall, String)}; deadline'dan reserveMillis pay bırakır. */
	public static ConsumerGroupDescription describeExisting(ToolCall call, String group, long reserveMillis) {
		Settled<ConsumerGroupDescription> s = describe(call, List.of(group), reserveMillis).get(group);
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
		return memberMap(m, Integer.MAX_VALUE);
	}

	/** Üyeye atanmış partition sayısı. */
	public static int assignedCount(MemberDescription m) {
		return m.assignment() == null ? 0 : m.assignment().topicPartitions().size();
	}

	/**
	 * Üye satırı; atamada en fazla maxPartitions partition numarası gösterilir (topic adına göre sırayla, son
	 * topic yarım kalabilir). Kesilirse assignedPartitions ve assignmentPartitionsOmitted sayıları eklenir.
	 */
	public static Map<String, Object> memberMap(MemberDescription m, int maxPartitions) {
		Map<String, List<Integer>> full = m.assignment() == null ? Map.of() : byTopic(m.assignment().topicPartitions());
		int total = assignedCount(m);
		Map<String, List<Integer>> assignment = full;
		if (total > maxPartitions) {
			assignment = new LinkedHashMap<>();
			int budget = Math.max(0, maxPartitions);
			for (Map.Entry<String, List<Integer>> e : full.entrySet()) {
				if (budget <= 0) {
					break;
				}
				List<Integer> shown = e.getValue().subList(0, Math.min(budget, e.getValue().size()));
				assignment.put(e.getKey(), shown);
				budget -= shown.size();
			}
		}
		int omitted = total - assignment.values().stream().mapToInt(List::size).sum();
		return Results.map("memberId", m.consumerId(), "groupInstanceId", m.groupInstanceId().orElse(null),
				"clientId", m.clientId(), "host", m.host(), "assignment",
				assignment.isEmpty() && omitted > 0 ? null : assignment, "assignedPartitions",
				omitted > 0 ? total : null, "assignmentPartitionsOmitted", omitted > 0 ? omitted : null);
	}

}
