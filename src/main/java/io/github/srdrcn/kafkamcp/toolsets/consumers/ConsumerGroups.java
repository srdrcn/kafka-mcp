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
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.SupportedVersionRange;
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

	/** get/lag çıktısında reassigningTo alanının açıklaması. */
	public static final String REASSIGNING_NOTE = "reassigningTo: sahibi bilinmiyor; partition bu üyeye aktarılıyor "
			+ "(KIP-848 reconciling). Önceki sahibi revoke'u bitirene kadar describe'da görünmez, tüketim sürüyor olabilir";

	private static final String METADATA_VERSION = "metadata.version";

	/** metadata.version 3.7-IV0: KIP-848 consumer tipi grupların (early access) geldiği ilk sürüm. */
	private static final short CONSUMER_GROUPS_MIN_METADATA_LEVEL = 15;

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
	 * listGroups sonucu: başarıyla listelenen gruplar (groupId'ye göre sıralı), broker hataları (hata varsa
	 * liste eksik olabilir; eski broker'da state'i tamamlanamayan gruplar da burada sayılır) ve listeyi
	 * tamamlarken (eski broker) zaten describe edilmiş grupların açıklamaları (tekrar describe etmemek için).
	 */
	public record GroupList(List<GroupSummary> groups, List<String> errors,
			Map<String, ConsumerGroupDescription> descriptions) {

		public GroupList(List<GroupSummary> groups, List<String> errors) {
			this(groups, errors, Map.of());
		}

		/** Yanıt vermeyen/hata veren broker'lar ya da state'i alınamayan gruplar yüzünden liste eksik olabilir mi? */
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
		return list(call, reserveMillis, null);
	}

	/**
	 * {@link #list(ToolCall, long)}; groupFilter null değilse sadece uyan gruplar döner (eski broker'da eksik
	 * state/type'ı tamamlamak için sadece onlar describe edilir).
	 */
	public static GroupList list(ToolCall call, long reserveMillis, Predicate<String> groupFilter) {
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
			errors.value().forEach(e -> errorMessages.add(Settled.explain(e)));
		}
		if (valid.value().isEmpty() && !errorMessages.isEmpty()) {
			throw new ToolException(errorMessages.get(0));
		}
		List<GroupSummary> groups = new ArrayList<>();
		for (GroupListing g : valid.value()) {
			if (groupFilter != null && !groupFilter.test(g.groupId())) {
				continue;
			}
			String protocol = g.protocol() == null || g.protocol().isBlank() ? null : g.protocol();
			// GroupListing.isSimpleConsumerGroup() broker'ın type bildirmesini (ListGroups v5+, Kafka 3.8+)
			// şart koşar; eski (ZooKeeper) broker'larda simple grup hiç işaretlenmezdi. Boş protocol type'ı
			// sadece classic gruplarda olur: type yoksa da simple sayılır.
			boolean simple = protocol == null && g.type().map(t -> t == GroupType.CLASSIC).orElse(true);
			groups.add(new GroupSummary(g.groupId(), g.groupState().map(ConsumerGroups::state).orElse(null),
					g.type().map(ConsumerGroups::type).orElse(null), protocol, simple));
		}
		groups.sort(Comparator.comparing(GroupSummary::groupId));
		return complete(call, groups, errorMessages, reserveMillis);
	}

	/**
	 * Listede eksik kalan state/type alanlarını tamamlar:
	 * <ul>
	 * <li>Kafka 2.6 öncesi broker'lar (ListGroups v4 öncesi) state döndürmez: Empty/Stable filtreleri ve
	 * consumer_groups_empty bu grupları sessizce kaçırmasın diye state'i bilinmeyen gruplar describe edilir.
	 * Yanıt gelmeyen/hata verenler state'siz kalır ve {@link GroupList#errors()}'a sayılır (partial).</li>
	 * <li>Kafka 3.8 öncesi broker'lar (ListGroups v5 öncesi; 2.6-3.7 ZooKeeper filolarının çoğu) type
	 * döndürmez. Simple grup (boş protocol) ve classic'e/consumer'a özgü state'ler type'ı belirler; kalanlar
	 * için tek describeFeatures: ZooKeeper'da (finalize edilmiş metadata.version yok) ve 3.7 öncesi KRaft'ta
	 * consumer tipi grup olamaz, hepsi classic. Sadece 3.7 KRaft'ta (KIP-848 early access) describe edilir.</li>
	 * </ul>
	 */
	private static GroupList complete(ToolCall call, List<GroupSummary> groups, List<String> errors,
			long reserveMillis) {
		List<GroupSummary> typed = new ArrayList<>(groups.size());
		for (GroupSummary g : groups) {
			String type = g.type() != null ? g.type() : inferType(g);
			typed.add(new GroupSummary(g.groupId(), g.state(), type, g.protocol(), g.simple()));
		}
		Set<String> toDescribe = new LinkedHashSet<>();
		typed.stream().filter(g -> g.state() == null).forEach(g -> toDescribe.add(g.groupId()));
		boolean typeless = typed.stream().anyMatch(g -> g.type() == null);
		Boolean consumerTypePossible = typeless ? consumerTypeGroupsPossible(call, reserveMillis) : null;
		if (Boolean.TRUE.equals(consumerTypePossible)) {
			typed.stream().filter(g -> g.type() == null).forEach(g -> toDescribe.add(g.groupId()));
		}
		Map<String, Settled<ConsumerGroupDescription>> described = describe(call, toDescribe, reserveMillis);
		Map<String, ConsumerGroupDescription> descriptions = new LinkedHashMap<>();
		List<GroupSummary> out = new ArrayList<>(typed.size());
		List<String> outErrors = new ArrayList<>(errors);
		int stateUnknown = 0;
		String firstError = null;
		for (GroupSummary g : typed) {
			// Broker type bildirmiyor ve consumer tipi grup olamaz: classic.
			String type = g.type() == null && Boolean.FALSE.equals(consumerTypePossible) ? type(GroupType.CLASSIC)
					: g.type();
			String state = g.state();
			Settled<ConsumerGroupDescription> s = described.get(g.groupId());
			if (s != null && s.ok()) {
				ConsumerGroupDescription d = s.value();
				descriptions.put(g.groupId(), d);
				state = state != null ? state : state(d.groupState());
				type = type != null ? type : type(d.type());
			}
			else if (s != null && state == null) {
				if (notFound(s.error())) {
					// listeden sonra silinmiş: eski broker'ların olmayan grup için döndürdüğü gibi Dead
					state = state(GroupState.DEAD);
				}
				else {
					stateUnknown++;
					firstError = firstError != null ? firstError : s.errorMessage();
				}
			}
			out.add(new GroupSummary(g.groupId(), state, type, g.protocol(), g.simple()));
		}
		if (stateUnknown > 0) {
			outErrors.add("State'i alınamayan grup: " + stateUnknown + " (broker listede state bildirmiyor; describe: "
					+ firstError + ")");
		}
		return new GroupList(out, outErrors, descriptions);
	}

	/** Listede type yoksa (Kafka &lt; 3.8) describe etmeden belirlenebilen type; belirlenemezse null. */
	private static String inferType(GroupSummary g) {
		if (g.simple()) {
			// boş protocol type'ı sadece classic gruplarda olur
			return type(GroupType.CLASSIC);
		}
		if (g.state() == null) {
			return null;
		}
		GroupState s = GroupState.parse(g.state());
		if (s == GroupState.PREPARING_REBALANCE || s == GroupState.COMPLETING_REBALANCE) {
			return type(GroupType.CLASSIC);
		}
		if (s == GroupState.ASSIGNING || s == GroupState.RECONCILING) {
			return type(GroupType.CONSUMER);
		}
		return null;
	}

	/**
	 * Type bildirmeyen (Kafka &lt; 3.8) cluster'da KIP-848 consumer tipi grup olabilir mi? Sadece 3.7 KRaft'ta
	 * (early access) olabilir: ZooKeeper'da finalize edilmiş metadata.version yoktur (describeFeatures boş),
	 * KRaft'ta broker'ın desteklediği en yüksek metadata.version 3.7-IV0'dan küçükse olamaz. Sorgu süre
	 * içinde yanıtlanmazsa null (type bilinmiyor kalır; ağır describe yapılmaz).
	 */
	private static Boolean consumerTypeGroupsPossible(ToolCall call, long reserveMillis) {
		DescribeFeaturesOptions options = new DescribeFeaturesOptions().timeoutMs(Settled.timeoutMs(call, reserveMillis));
		Settled<FeatureMetadata> s = Settled.within(call, call.admin().describeFeatures(options).featureMetadata(),
				reserveMillis);
		if (!s.ok() || s.value() == null) {
			return null;
		}
		if (!s.value().finalizedFeatures().containsKey(METADATA_VERSION)) {
			return false;
		}
		SupportedVersionRange supported = s.value().supportedFeatures().get(METADATA_VERSION);
		return supported == null || supported.maxVersion() >= CONSUMER_GROUPS_MIN_METADATA_LEVEL;
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
		Map<String, Object> out = Results.map("memberId", m.consumerId(), "groupInstanceId",
				m.groupInstanceId().orElse(null), "clientId", m.clientId(), "host", m.host(), "assignment",
				assignment.isEmpty() && omitted > 0 ? null : assignment, "assignedPartitions",
				omitted > 0 ? total : null, "assignmentPartitionsOmitted", omitted > 0 ? omitted : null);
		out.putAll(consumerProtocolFields(m));
		return out;
	}

	/**
	 * KIP-848 (consumer tipi, sadece KRaft) üye alanları: memberEpoch; hedef atamasında olup üyeye henüz
	 * verilmemiş (başka üye bırakana kadar bekleyen) partition sayısı pendingPartitions; consumer tipi gruba
	 * hâlâ classic protokolle bağlı üye (online geçiş) için classicMember. Classic gruplarda (ZooKeeper'da hep)
	 * bu alanlar yoktur; boş map döner.
	 * <p>
	 * Bırakılacak partition'lar raporlanmaz: ConsumerGroupDescribe'daki atama broker'ın assignedPartitions'ıdır
	 * (mevcut ∩ hedef); revoke edilmekte olanlar (partitionsPendingRevocation) yanıtta yoktur. assignment \
	 * hedef farkı sadece yeni hedef epoch'u ile üyenin sonraki heartbeat'i arasındaki kısa anda dolar; gerçek
	 * revoke sırasında boştur.
	 */
	public static Map<String, Object> consumerProtocolFields(MemberDescription m) {
		Map<String, Object> out = new LinkedHashMap<>();
		m.memberEpoch().ifPresent(e -> out.put("memberEpoch", e));
		if (m.upgraded().isPresent() && !m.upgraded().get()) {
			out.put("classicMember", Boolean.TRUE);
		}
		long pending = incoming(m).size();
		if (pending > 0) {
			out.put("pendingPartitions", pending);
		}
		return out;
	}

	/** Üyenin hedef atamasında olup mevcut atamasında olmayan partition'lar (KIP-848); classic'te boş. */
	private static Set<TopicPartition> incoming(MemberDescription m) {
		if (m.targetAssignment().isEmpty()) {
			return Set.of();
		}
		Set<TopicPartition> current = m.assignment() == null ? Set.of() : m.assignment().topicPartitions();
		Set<TopicPartition> out = new LinkedHashSet<>(m.targetAssignment().get().topicPartitions());
		out.removeAll(current);
		return out;
	}

	/**
	 * KIP-848 consumer tipi grupta (sadece KRaft) hedef atamada bir üyeye verilmiş ama henüz o üyeye geçmemiş
	 * partition → hedef üye. Bu partition'ların mevcut sahibi (revoke etmekte olan üye) describe yanıtında
	 * görünmez: {@link #owners} onları sahipsiz bırakır, ama tüketim sürüyor olabilir. Classic gruplarda boş.
	 */
	public static Map<TopicPartition, MemberDescription> incomingOwners(ConsumerGroupDescription d) {
		Map<TopicPartition, MemberDescription> m = new HashMap<>();
		if (d == null) {
			return m;
		}
		for (MemberDescription member : d.members()) {
			incoming(member).forEach(tp -> m.put(tp, member));
		}
		return m;
	}

	/** KIP-848 grup epoch'ları (groupEpoch, targetAssignmentEpoch); classic gruplarda boş map. */
	public static Map<String, Object> consumerProtocolFields(ConsumerGroupDescription d) {
		Map<String, Object> out = new LinkedHashMap<>();
		d.groupEpoch().ifPresent(e -> out.put("groupEpoch", e));
		d.targetAssignmentEpoch().ifPresent(e -> out.put("targetAssignmentEpoch", e));
		return out;
	}

}
