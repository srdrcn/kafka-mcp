package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.MAX_EVIDENCE;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.first;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.omitted;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.partitionNames;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.Diagnosis.evidence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.consumers.ConsumerGroups;
import io.github.srdrcn.kafkamcp.toolsets.consumers.Settled;
import io.github.srdrcn.kafkamcp.toolsets.diagnose.Checks.Check;

/** diagnose_consumer_group: tek çağrıda consumer group sağlık teşhisi. */
@Component
public class DiagnoseGroupTools {

	static final long HIGH_LAG = 10_000;

	static final long GROWING_LAG_MIN = 100;

	static final int MAX_MEMBERS = 20;

	static final int MAX_TOPICS = 20;

	/** ACL kontrolünde en fazla bu kadar topic değerlendirilir. */
	static final int MAX_ACL_TOPICS = 20;

	static final Set<GroupState> REBALANCING = Set.of(GroupState.PREPARING_REBALANCE,
			GroupState.COMPLETING_REBALANCE, GroupState.ASSIGNING, GroupState.RECONCILING);

	private static final long DESCRIBE_MAX_MS = 3_000;

	private static final long COMMITTED_MAX_MS = 3_000;

	private static final long WINDOW_RESERVE_MS = 2_500;

	private static final long FINAL_DESCRIBE_RESERVE_MS = 1_500;

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("diagnose_consumer_group",
					"Consumer group teşhisi: durum/tip, üyeler ve atama, lag ve eğilimi, takılı partition'lar, eksik topic'ler, coordinator, ACL")
			.required("group", P.string("Consumer group adı (groupId)"))
			.param("intervalSeconds", DiagnoseParams.interval())
			.handler(this::diagnose));
	}

	record Window(OffsetWindow window, Settled<ConsumerGroupDescription> after) {
	}

	record TopicScan(Map<TopicPartition, Long> committed, Map<String, TopicDescription> topics, Set<String> missing,
			Map<String, String> topicErrors, Map<TopicPartition, Long> earliest) {
	}

	Object diagnose(ToolCall call) {
		String group = call.str("group");
		long intervalMs = DiagnoseParams.intervalMs(call);
		Settled<ConsumerGroupDescription> s0 = ConsumerGroups
			.describe(call, List.of(group), Math.max(0, call.remainingMillis() - DESCRIBE_MAX_MS))
			.get(group);
		if (!s0.ok() && ConsumerGroups.notFound(s0.error())) {
			throw ConsumerGroups.notFoundError(group);
		}
		ConsumerGroupDescription d0 = s0.ok() ? s0.value() : null;
		Set<TopicPartition> assigned = new LinkedHashSet<>(ConsumerGroups.owners(d0).keySet());
		Diagnosis dx = new Diagnosis();
		Map<String, Object> facts = new LinkedHashMap<>();
		String mode;
		try (Checks checks = new Checks(call, dx)) {
			Check<ClusterMeta.Info> meta = checks.start("cluster metadata", () -> ClusterMeta.describe(call, true));
			Check<TopicScan> topics = checks.start("topic'ler ve offset aralıkları",
					() -> topicScan(call, group, assigned));
			Check<Window> window = checks.start("lag ve eğilim", () -> {
				OffsetWindow w = OffsetWindow.measure(call, assigned, List.of(group), false, intervalMs,
						WINDOW_RESERVE_MS);
				Settled<ConsumerGroupDescription> after = ConsumerGroups
					.describe(call, List.of(group), FINAL_DESCRIBE_RESERVE_MS)
					.get(group);
				return new Window(w, after);
			});
			Check<AclView> acl = checks.start("ACL", () -> {
				TopicScan ts = topics.await(6_000);
				List<AclView.Resource> resources = new ArrayList<>();
				resources.add(new AclView.Resource(ResourceType.GROUP, group));
				if (ts != null) {
					first(ts.topics().keySet(), MAX_ACL_TOPICS)
						.forEach(t -> resources.add(new AclView.Resource(ResourceType.TOPIC, t)));
				}
				return AclView.load(call, resources, null);
			});

			if (d0 == null) {
				dx.warning("GROUP_DESCRIBE_FAILED", "Grup describe edilemedi: " + s0.errorMessage(),
						evidence("group", group), "Grup koordinatörüne erişimi ve GROUP DESCRIBE yetkisini kontrol edin");
				dx.markPartial();
			}
			ClusterMeta.Info info = meta.get();
			mode = info == null ? null : info.mode();
			TopicScan ts = topics.get();
			Window win = window.get();
			ConsumerGroupDescription d1 = win != null && win.after() != null && win.after().ok() ? win.after().value()
					: null;
			if (d0 != null && ConsumerGroups.deadOrUnknown(d0) && ts != null && ts.committed().isEmpty()) {
				throw ConsumerGroups.notFoundError(group);
			}
			describeFacts(dx, facts, d0, d1, info);
			assignment(dx, facts, d0, ts);
			lag(dx, facts, group, d0, d1, win == null ? null : win.window(), ts, assigned);
			topicsFindings(dx, facts, ts, win == null ? null : win.window(), d0);
			aclFindings(dx, facts, acl.get(), group, ts);
		}
		Map<String, Object> head = Results.map("groupId", group, "cluster", call.cluster(), "mode", mode);
		return dx.result(head, facts);
	}

	// ---- alt kontroller ----

	private static TopicScan topicScan(ToolCall call, String group, Set<TopicPartition> assigned) {
		Futures.Each<String, Map<TopicPartition, Long>> c = io.github.srdrcn.kafkamcp.kafka.ConsumerGroups
			.committed(call, List.of(group), List.of(), COMMITTED_MAX_MS);
		if (!c.errors().isEmpty() && c.values().isEmpty()) {
			throw new ToolException(c.errors().values().iterator().next());
		}
		Map<TopicPartition, Long> committed = c.values().getOrDefault(group, Map.of());
		Set<String> names = new TreeSet<>();
		committed.keySet().forEach(tp -> names.add(tp.topic()));
		assigned.forEach(tp -> names.add(tp.topic()));
		Futures.Each<String, TopicDescription> described = Topics.describe(call, names);
		Set<String> missing = new TreeSet<>();
		Map<String, String> errors = new TreeMap<>();
		described.errors().forEach((t, e) -> {
			if (e.contains(UnknownTopicOrPartitionException.class.getSimpleName()) || e.contains("bulunamadı")) {
				missing.add(t);
			}
			else {
				errors.put(t, e);
			}
		});
		List<TopicPartition> existing = committed.keySet()
			.stream()
			.filter(tp -> described.values().containsKey(tp.topic()))
			.toList();
		Map<TopicPartition, Long> earliest = new LinkedHashMap<>();
		if (!existing.isEmpty()) {
			ListOffsetsResult r = Offsets.submit(call, existing, OffsetSpec.earliest());
			Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new LinkedHashMap<>();
			existing.forEach(tp -> futures.put(tp, r.partitionResult(tp)));
			Futures.awaitEach(call, futures, 3_000).values().forEach((tp, i) -> earliest.put(tp, i.offset()));
		}
		return new TopicScan(committed, new TreeMap<>(described.values()), missing, errors, earliest);
	}

	// ---- değerlendirme ----

	private static void describeFacts(Diagnosis dx, Map<String, Object> facts, ConsumerGroupDescription d0,
			ConsumerGroupDescription d1, ClusterMeta.Info info) {
		if (d0 == null) {
			return;
		}
		ConsumerGroupDescription last = d1 != null ? d1 : d0;
		facts.put("state", ConsumerGroups.state(last.groupState()));
		facts.put("type", ConsumerGroups.type(d0.type()));
		if (d0.isSimpleConsumerGroup()) {
			facts.put("simple", true);
		}
		if (d0.partitionAssignor() != null && !d0.partitionAssignor().isBlank()) {
			facts.put("partitionAssignor", d0.partitionAssignor());
		}
		d0.groupEpoch().ifPresent(e -> facts.put("groupEpoch", e));
		d0.targetAssignmentEpoch().ifPresent(e -> facts.put("targetAssignmentEpoch", e));
		Node coordinator = d0.coordinator();
		Map<String, Object> coord = ConsumerGroups.node(coordinator);
		if (coord != null) {
			facts.put("coordinator", coord);
		}
		if (coordinator == null || coordinator.isEmpty()) {
			dx.critical("COORDINATOR_UNAVAILABLE", "Grubun coordinator broker'ı belirlenemedi",
					null, "__consumer_offsets partition'larının sağlığını kontrol edin (diagnose_topic topic=__consumer_offsets)");
		}
		else if (info != null && !info.brokerIds().contains(coordinator.id())) {
			dx.critical("COORDINATOR_UNAVAILABLE",
					"Coordinator broker " + coordinator.id() + " canlı broker listesinde yok",
					evidence("coordinator", coordinator.id(), "liveBrokers", info.brokerIds()),
					"Broker'ı ayağa kaldırın; __consumer_offsets liderliği başka broker'a geçmeli (diagnose_cluster)");
		}
		GroupState s0 = d0.groupState();
		GroupState s1 = d1 == null ? null : d1.groupState();
		if (REBALANCING.contains(s0) || s1 != null && REBALANCING.contains(s1)) {
			dx.warning("REBALANCING",
					"Grup rebalance/uzlaşma durumunda (" + ConsumerGroups.state(s0)
							+ (s1 != null && s1 != s0 ? " → " + ConsumerGroups.state(s1) : "") + "): tüketim duraklayabilir",
					evidence("stateAtStart", ConsumerGroups.state(s0), "stateAtEnd", ConsumerGroups.state(s1)),
					"Sık tekrarlıyorsa üyelerin session.timeout.ms/max.poll.interval.ms aşımlarını ve pod yeniden "
							+ "başlatmalarını kontrol edin; static membership (group.instance.id) ya da consumer protokolü "
							+ "(KIP-848) rebalance etkisini azaltır");
		}
		if (d1 != null) {
			Set<String> m0 = memberIds(d0);
			Set<String> m1 = memberIds(d1);
			if (!m0.equals(m1)) {
				Set<String> joined = new TreeSet<>(m1);
				joined.removeAll(m0);
				Set<String> left = new TreeSet<>(m0);
				left.removeAll(m1);
				dx.info("MEMBERSHIP_CHANGED", "Ölçüm sırasında üyeler değişti (katılan " + joined.size() + ", ayrılan "
						+ left.size() + ")", evidence("joined", first(joined, MAX_EVIDENCE), "left", first(left, MAX_EVIDENCE)),
						"Sürekli değişiyorsa consumer'lar yeniden başlıyor ya da gruptan düşüyor olabilir");
			}
		}
	}

	private static Set<String> memberIds(ConsumerGroupDescription d) {
		Set<String> out = new TreeSet<>();
		d.members().forEach(m -> out.add(m.consumerId()));
		return out;
	}

	private static void assignment(Diagnosis dx, Map<String, Object> facts, ConsumerGroupDescription d,
			TopicScan ts) {
		if (d == null) {
			return;
		}
		List<Map<String, Object>> members = new ArrayList<>();
		List<String> idle = new ArrayList<>();
		int reconciling = 0;
		for (MemberDescription m : ConsumerGroups.sortedMembers(d)) {
			int n = ConsumerGroups.assignedCount(m);
			if (n == 0) {
				idle.add(m.clientId() + "@" + m.host());
			}
			Integer target = m.targetAssignment().map(t -> t.topicPartitions().size()).orElse(null);
			boolean pending = m.targetAssignment().isPresent()
					&& !Objects.equals(m.targetAssignment().get().topicPartitions(),
							m.assignment() == null ? Set.of() : m.assignment().topicPartitions());
			reconciling += pending ? 1 : 0;
			if (members.size() < MAX_MEMBERS) {
				members.add(Results.map("clientId", m.clientId(), "host", m.host(), "memberId", m.consumerId(),
						"groupInstanceId", m.groupInstanceId().orElse(null), "assignedPartitions", n,
						"targetPartitions", pending ? target : null, "memberEpoch", m.memberEpoch().orElse(null),
						"topics", m.assignment() == null ? null
								: List.copyOf(ConsumerGroups.byTopic(m.assignment().topicPartitions()).keySet())));
			}
		}
		facts.put("memberCount", d.members().size());
		facts.put("members", members);
		if (d.members().size() > MAX_MEMBERS) {
			facts.put("membersOmitted", d.members().size() - MAX_MEMBERS);
		}
		boolean stable = d.groupState() == GroupState.STABLE;
		if (stable && !idle.isEmpty()) {
			dx.info("IDLE_MEMBERS",
					idle.size() + " üyeye partition atanmamış (üye sayısı partition sayısından fazla olabilir)",
					evidence("members", first(idle, MAX_EVIDENCE), "omitted", omitted(idle)),
					"Fazla consumer boşta bekler; ölçek için partition sayısını artırın (topics_partitions_increase) ya da üye sayısını azaltın");
		}
		if (reconciling > 0) {
			dx.info("ASSIGNMENT_RECONCILING", reconciling + " üyenin mevcut ataması hedef atamadan farklı (consumer protokolü uzlaşması sürüyor)",
					null, "Kısa sürede kapanmalı; uzun sürüyorsa üyelerin loglarını kontrol edin");
		}
		if (!stable || ts == null) {
			return;
		}
		Map<TopicPartition, MemberDescription> owners = ConsumerGroups.owners(d);
		Set<String> subscribed = new TreeSet<>();
		owners.keySet().forEach(tp -> subscribed.add(tp.topic()));
		List<TopicPartition> unassigned = new ArrayList<>();
		for (String t : subscribed) {
			TopicDescription td = ts.topics().get(t);
			if (td == null) {
				continue;
			}
			for (TopicPartition tp : Topics.partitions(td)) {
				if (!owners.containsKey(tp)) {
					unassigned.add(tp);
				}
			}
		}
		if (!unassigned.isEmpty()) {
			dx.warning("UNASSIGNED_PARTITIONS",
					unassigned.size() + " partition grup Stable olduğu halde hiçbir üyeye atanmamış: bu partition'lar tüketilmiyor",
					evidence("partitions", partitionNames(unassigned), "omitted", omitted(unassigned)),
					"Partition sayısı yeni artırılmışsa metadata yenilenince atanır (metadata.max.age.ms); değilse "
							+ "assignor/abonelik (regex) ayarlarını ve consumer loglarını kontrol edin");
		}
	}

	private static void lag(Diagnosis dx, Map<String, Object> facts, String group, ConsumerGroupDescription d0,
			ConsumerGroupDescription d1, OffsetWindow w, TopicScan ts, Set<TopicPartition> assigned) {
		if (w == null) {
			return;
		}
		OffsetWindow.GroupProgress p = w.progress(group);
		Map<String, Object> lag = new LinkedHashMap<>(p.summary());
		lag.putAll(w.describe());
		facts.put("lag", lag);
		List<Map<String, Object>> byTopic = new ArrayList<>();
		for (OffsetWindow.TopicProgress t : first(p.byTopic(), MAX_TOPICS)) {
			byTopic.add(Results.map("topic", t.topic(), "lag", t.lag1(), "partitions", t.partitions(), "consumeRate",
					w.rate(t.consumed()), "produceRate", w.rate(t.produced()), "trend",
					w.complete() ? t.lag1() > t.lag0() ? "growing" : t.lag1() < t.lag0() ? "shrinking" : "stable" : null,
					"stuckPartitions", t.stuck() > 0 ? t.stuck() : null));
		}
		facts.put("lagByTopic", byTopic);
		if (p.byTopic().size() > MAX_TOPICS) {
			facts.put("lagByTopicOmitted", p.byTopic().size() - MAX_TOPICS);
		}
		if (!w.groupErrors().isEmpty()) {
			dx.warning("OFFSETS_UNAVAILABLE", "Grubun commit'li offset'leri alınamadı",
					evidence("errors", w.groupErrors()), "GROUP DESCRIBE yetkisini ve coordinator erişimini kontrol edin");
			dx.markPartial();
		}
		if (!w.endErrors().isEmpty()) {
			dx.markPartial();
		}
		ConsumerGroupDescription d = d1 != null ? d1 : d0;
		int members = d == null ? 0 : d.members().size();
		boolean empty = d != null && d.groupState() == GroupState.EMPTY;
		int partitions = ts == null ? -1
				: ts.topics().values().stream().filter(td -> p.byTopic().stream().anyMatch(t -> t.topic().equals(td.name())))
					.mapToInt(td -> td.partitions().size())
					.sum();
		List<OffsetWindow.PartitionProgress> stuck = p.stuck();
		if (!stuck.isEmpty()) {
			Map<String, Object> ev = evidence("partitions",
					first(stuck.stream().map(OffsetWindow.PartitionProgress::toMap).toList(), MAX_EVIDENCE), "omitted",
					omitted(stuck), "members", members);
			if (members > 0) {
				dx.critical("STUCK_PARTITIONS",
						stuck.size() + " partition'da commit ilerlemiyor ama yeni mesaj geliyor (üyeler var): tüketim takılmış",
						ev, "Bu partition'ların sahibi üyelerin loglarını inceleyin (poison pill, uzun işleme, "
								+ "max.poll.interval.ms aşımı, commit hatası); gerekirse ilgili consumer'ı yeniden başlatın");
			}
			else {
				dx.warning("STUCK_PARTITIONS",
						stuck.size() + " partition'da commit ilerlemiyor ve yeni mesaj geliyor: grupta aktif üye yok"
								+ (empty ? " (EMPTY), lag " + p.lag() : ""),
						ev, "Consumer uygulamasını başlatın. Backlog atlanacaksa grup EMPTY iken consumer_groups_offsets_reset "
								+ "(to=latest ya da timestamp); grup terk edildiyse consumer_groups_delete");
			}
		}
		if (empty && p.lag() > 0 && stuck.isEmpty()) {
			dx.warning("NO_ACTIVE_MEMBERS", "Grup EMPTY (aktif consumer yok) ve " + p.lag() + " mesaj lag var",
					evidence("lag", p.lag()),
					"Consumer'ları başlatın. Backlog'u atlamak gerekiyorsa grup EMPTY iken consumer_groups_offsets_reset "
							+ "(to=latest ya da timestamp) kullanılabilir; grup terk edildiyse consumer_groups_delete");
		}
		if ("growing".equals(p.trend()) && p.lag() >= GROWING_LAG_MIN && stuck.isEmpty()) {
			String scale = partitions > 0 && members < partitions
					? "Üye sayısını (" + members + ") partition sayısına (" + partitions + ") kadar artırın"
					: "Üye sayısı partition sayısına ulaşmış: partition sayısını artırın (topics_partitions_increase) ya da "
							+ "işleme hızını iyileştirin (batch, async işlem)";
			dx.warning("LAG_GROWING",
					"Lag büyüyor: tüketim " + p.consumeRate() + " msg/sn, üretim " + p.produceRate() + " msg/sn",
					evidence("lagAtStart", p.lag0(), "lag", p.lag(), "members", members, "partitions",
							partitions > 0 ? partitions : null),
					scale + (empty ? "" : ". Offset reset için önce consumer'ları durdurun (grup EMPTY olmalı)"));
		}
		else if (p.lag() >= HIGH_LAG && stuck.isEmpty() && !"growing".equals(p.trend())) {
			dx.info("HIGH_LAG", "Toplam lag " + p.lag() + (p.etaSeconds() != null
					? " (azalıyor, tahmini yetişme " + p.etaSeconds() + " sn)" : ""), evidence("lag", p.lag()),
					"consumer_groups_lag_trend ile izleyin");
		}
		if (p.resetDetected()) {
			dx.info("OFFSET_RESET_DETECTED", "Ölçüm sırasında commit geri gitti (offset reset ya da yeniden işleme)", null,
					null);
		}
		Set<TopicPartition> committed = ts == null ? Set.of() : ts.committed().keySet();
		List<TopicPartition> noCommit = assigned.stream().filter(tp -> !committed.contains(tp)).toList();
		if (ts != null && !noCommit.isEmpty()) {
			dx.info("PARTITIONS_WITHOUT_COMMIT",
					noCommit.size() + " atanmış partition'da commit yok (lag hesaplanamaz; auto.offset.reset uygulanır)",
					evidence("partitions", partitionNames(noCommit), "omitted", omitted(noCommit)),
					"Consumer commit ediyor mu kontrol edin (enable.auto.commit ya da commitSync/commitAsync)");
		}
	}

	private static void topicsFindings(Diagnosis dx, Map<String, Object> facts, TopicScan ts, OffsetWindow w,
			ConsumerGroupDescription d) {
		if (ts == null) {
			return;
		}
		facts.put("topics", first(ts.topics().keySet(), MAX_TOPICS));
		if (!ts.missing().isEmpty()) {
			List<String> missing = List.copyOf(ts.missing());
			dx.warning("MISSING_TOPICS",
					missing.size() + " silinmiş topic'te hâlâ commit'li offset var",
					evidence("topics", first(missing, MAX_EVIDENCE), "omitted", omitted(missing)),
					"Gereksizse consumer_groups_offsets_delete ile bu topic'lerin offset'lerini silin (lag/izleme gürültüsünü azaltır)");
		}
		if (!ts.topicErrors().isEmpty()) {
			dx.markPartial();
		}
		List<String> beforeStart = new ArrayList<>();
		List<String> afterEnd = new ArrayList<>();
		ts.committed().forEach((tp, c) -> {
			Long e = ts.earliest().get(tp);
			Long end = w == null ? null : w.end0().get(tp);
			if (e != null && c < e) {
				beforeStart.add(tp.toString());
			}
			else if (end != null && c > end) {
				afterEnd.add(tp.toString());
			}
		});
		boolean empty = d != null && d.groupState() == GroupState.EMPTY;
		if (!beforeStart.isEmpty()) {
			dx.warning("COMMITTED_BEFORE_LOG_START",
					beforeStart.size() + " partition'da commit log başlangıcından eski: okunmadan silinmiş mesajlar var",
					evidence("partitions", first(new TreeSet<>(beforeStart), MAX_EVIDENCE), "omitted", omitted(beforeStart)),
					"Consumer çalışınca auto.offset.reset uygulanır" + (empty
							? "; şimdi (grup EMPTY) consumer_groups_offsets_reset to=earliest ile düzeltilebilir"
							: "; retention.ms'i artırmayı ya da tüketimi hızlandırmayı değerlendirin"));
		}
		if (!afterEnd.isEmpty()) {
			dx.warning("COMMITTED_AFTER_LOG_END",
					afterEnd.size() + " partition'da commit log sonundan ileride (topic yeniden oluşturulmuş ya da kesilmiş olabilir)",
					evidence("partitions", first(new TreeSet<>(afterEnd), MAX_EVIDENCE), "omitted", omitted(afterEnd)),
					empty ? "Grup EMPTY: consumer_groups_offsets_reset ile geçerli bir offset'e alın"
							: "Consumer'ları durdurup (grup EMPTY) consumer_groups_offsets_reset ile düzeltin");
		}
	}

	private static void aclFindings(Diagnosis dx, Map<String, Object> facts, AclView v, String group, TopicScan ts) {
		if (v == null) {
			return;
		}
		if (!v.authorizerEnabled()) {
			facts.put("access", Results.map("authorizer", false));
			return;
		}
		List<String> groupReaders = v.allowedPrincipals(AclOperation.READ, ResourceType.GROUP, group);
		Collection<String> topics = ts == null ? List.of() : first(ts.topics().keySet(), MAX_ACL_TOPICS);
		List<Map<String, Object>> missing = new ArrayList<>();
		for (String p : groupReaders) {
			if (v.superUsers().contains(p)) {
				continue;
			}
			List<String> cannotRead = new ArrayList<>();
			for (String t : topics) {
				boolean ok = false;
				for (String host : v.hosts(p)) {
					if (v.decide(p, AclOperation.READ, ResourceType.TOPIC, t, host).allowed()) {
						ok = true;
						break;
					}
				}
				if (!ok) {
					cannotRead.add(t);
				}
			}
			if (!cannotRead.isEmpty()) {
				missing.add(Results.map("principal", p, "topics", first(cannotRead, MAX_EVIDENCE)));
			}
		}
		Set<String> uniq = new HashSet<>(groupReaders);
		facts.put("access", Results.map("authorizer", true, "groupReaders", first(uniq.stream().sorted().toList(), 20),
				"note", "Kafka üyelerin principal'ını göstermez; değerlendirme bu grupta GROUP READ ACL'i olan principal'lar için"));
		if (groupReaders.isEmpty()) {
			dx.info("NO_GROUP_ACLS",
					"Bu gruba GROUP READ veren ACL yok: grup sadece süper kullanıcılarca"
							+ (v.allowEveryone().value() ? " (ya da allow.everyone.if.no.acl.found=true ile herkesçe)" : "")
							+ " kullanılabilir",
					null, "Uygulama principal'ı için acls_create preset=consumer (topic, group) kullanın");
		}
		if (!missing.isEmpty()) {
			dx.warning("ACL_TOPIC_READ_MISSING",
					"Grubu kullanabilen principal'lardan bazıları tükettiği topic'lerde TOPIC READ yetkisine sahip değil",
					evidence("principals", first(missing, MAX_EVIDENCE)),
					"Eksik izni acls_create ile verin (resourceType=TOPIC, operation=READ) ya da diagnose_access intent=consume ile doğrulayın");
		}
	}

}
