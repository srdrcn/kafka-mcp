package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.MAX_EVIDENCE;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.first;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.omitted;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.Diagnosis.evidence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.Configs;
import io.github.srdrcn.kafkamcp.kafka.ConsumerGroups;
import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.kafka.KafkaVersions;
import io.github.srdrcn.kafkamcp.kafka.LogDirs;
import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectApi;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectJson;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorOverview;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorStatus;
import io.github.srdrcn.kafkamcp.toolsets.diagnose.Checks.Check;

/** diagnose_cluster: tek çağrıda cluster sağlık teşhisi (KRaft ve ZooKeeper). */
@Component
public class DiagnoseClusterTools {

	/** Broker'a özgü ya da moda özgü olduğu için config farkı sayılmayan anahtarlar. */
	public static final List<String> PER_BROKER_KEYS = List.of("broker.id", "node.id", "broker.rack", "listeners",
			"advertised.listeners", "advertised.host.name", "advertised.port", "host.name", "port", "log.dir",
			"log.dirs", "metadata.log.dir", "process.roles", "controller.quorum.voters",
			"controller.quorum.bootstrap.servers", "controller.listener.names", "early.start.listeners",
			"zookeeper.connect", "broker.id.generation.enable", "reserved.broker.max.id");

	private static final Pattern PER_BROKER_PATTERN = Pattern
		.compile(".*(ssl\\.(keystore|truststore)\\.(location|type)|\\.keytab|\\.principal|server\\.cnxn)$");

	static final long LARGE_LAG = 10_000;

	/** Lag taramasında en fazla bu kadar grup (aktifler önce); fazlası atlanır ve raporlanır. */
	static final int MAX_GROUPS = 500;

	static final int TOP_GROUPS = 5;

	static final long VOTER_LAG_RECORDS = 1_000;

	static final long VOTER_CAUGHT_UP_MS = 30_000;

	static final double SKEW_PCT = 20;

	static final double PREFERRED_IMBALANCE_PCT = 10;

	static final double DISK_WARN_PCT = 85;

	static final double DISK_CRITICAL_PCT = 95;

	private static final long GROUP_STEP_MAX_MS = 4_000;

	private final ConnectApi connect;

	public DiagnoseClusterTools(ConnectApi connect) {
		this.connect = connect;
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("diagnose_cluster",
					"Cluster teşhisi: mod/sürüm, controller/quorum, broker'lar, URP/offline/min ISR, disk, dağılım, config farkı, lag, connector")
			.handler(this::diagnose));
	}

	record GroupLagScan(int total, int scanned, List<Map<String, Object>> top, int failed, List<String> listErrors) {
	}

	record ConfigScan(Map<String, Integer> minIsr, List<String> uncleanTopics, int failed) {
	}

	Object diagnose(ToolCall call) {
		Diagnosis dx = new Diagnosis();
		Map<String, Object> facts = new LinkedHashMap<>();
		String mode;
		try (Checks checks = new Checks(call, dx)) {
			Check<ClusterMeta.Info> meta = checks.start("cluster metadata", () -> ClusterMeta.describe(call, true));
			Check<FeatureMetadata> features = checks.start("feature'lar",
					() -> Futures.awaitAtMost(call, call.admin().describeFeatures().featureMetadata(), 4_000));
			Check<Map<String, TopicDescription>> topics = checks.start("topic'ler",
					() -> Topics.describeAll(call, true));
			Check<Topics.Reassignments> reassign = checks.start("reassignment'lar",
					() -> Topics.reassignments(call, Topics.submitReassignments(call)));
			Check<ConfigScan> topicConfigs = checks.start("topic config'leri", () -> {
				Map<String, TopicDescription> t = topics.await(8_000);
				return t == null ? null : topicConfigs(call, t.keySet());
			});
			Check<Futures.Each<ConfigResource, Config>> brokerConfigs = checks.start("broker config'leri", () -> {
				ClusterMeta.Info i = meta.await(6_000);
				return i == null ? null : Configs.describe(call, i.brokerIds().stream().map(Configs::broker).toList());
			});
			Check<Futures.Each<Integer, Map<String, LogDirDescription>>> dirs = checks.start("log dizinleri", () -> {
				ClusterMeta.Info i = meta.await(6_000);
				return i == null ? null : LogDirs.describe(call, i.brokerIds(), i.brokerIds());
			});
			Check<GroupLagScan> groups = checks.start("consumer group lag'i", () -> groupLag(call));
			Check<List<ConnectorOverview>> connectors = call.clusterConfig().hasConnect()
					? checks.start("connector'lar", () -> connect.overview(call.cluster())) : null;

			ClusterMeta.Info info = meta.get();
			FeatureMetadata fm = features.get();
			mode = info == null ? null : info.mode();
			if (mode == null && fm != null && fm.finalizedFeatures().containsKey("metadata.version")) {
				mode = ClusterMeta.KRAFT;
			}
			Futures.Each<ConfigResource, Config> bc = brokerConfigs.get();
			controller(dx, facts, info, fm, mode, bc);
			Map<String, TopicDescription> all = topics.get();
			Topics.Reassignments r = reassign.get();
			ConfigScan cs = topicConfigs.get();
			partitions(dx, facts, all, r, cs, info, mode);
			disks(dx, facts, dirs.get());
			skew(dx, facts, all, info);
			drift(dx, facts, bc);
			groupFindings(dx, facts, groups.get());
			if (connectors != null) {
				connectorFindings(dx, facts, connectors.get());
			}
		}
		return dx.result(Results.map("cluster", call.cluster(), "mode", mode), facts);
	}

	// ---- alt kontroller ----

	private static ConfigScan topicConfigs(ToolCall call, Collection<String> names) {
		List<ConfigResource> resources = names.stream().map(Configs::topic).toList();
		Futures.Each<ConfigResource, Config> each = Configs.describe(call, resources);
		Map<String, Integer> minIsr = new HashMap<>();
		List<String> unclean = new ArrayList<>();
		each.values().forEach((res, cfg) -> {
			ConfigEntry m = cfg.get("min.insync.replicas");
			if (m != null && m.value() != null) {
				try {
					minIsr.put(res.name(), Integer.parseInt(m.value().trim()));
				}
				catch (NumberFormatException ignored) {
					// geçersiz değer: atlanır
				}
			}
			ConfigEntry u = cfg.get("unclean.leader.election.enable");
			if (u != null && "true".equalsIgnoreCase(u.value()) && !res.name().startsWith("__")) {
				unclean.add(res.name());
			}
		});
		unclean.sort(null);
		return new ConfigScan(minIsr, unclean, each.errors().size());
	}

	private static GroupLagScan groupLag(ToolCall call) {
		Groups.Listing listing = Groups.list(call, 3_000);
		List<GroupListing> ordered = new ArrayList<>(listing.groups().values());
		// aktif gruplar (Empty olmayanlar) önce taranır
		ordered.sort(Comparator
			.comparingInt((GroupListing g) -> g.groupState().orElse(GroupState.UNKNOWN) == GroupState.EMPTY ? 1 : 0)
			.thenComparing(GroupListing::groupId));
		List<GroupListing> sample = ordered.subList(0, Math.min(MAX_GROUPS, ordered.size()));
		Map<String, GroupListing> byId = new LinkedHashMap<>();
		sample.forEach(g -> byId.put(g.groupId(), g));
		Futures.Each<String, Map<TopicPartition, Long>> committed = ConsumerGroups.committed(call, byId.keySet(),
				List.of(), GROUP_STEP_MAX_MS);
		Set<TopicPartition> tps = new LinkedHashSet<>();
		committed.values().values().forEach(m -> tps.addAll(m.keySet()));
		Map<TopicPartition, Long> latest = new HashMap<>();
		if (!tps.isEmpty()) {
			ListOffsetsResult r = Offsets.submit(call, tps, OffsetSpec.latest());
			Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new LinkedHashMap<>();
			tps.forEach(tp -> futures.put(tp, r.partitionResult(tp)));
			Futures.awaitEach(call, futures, GROUP_STEP_MAX_MS).values().forEach((tp, i) -> latest.put(tp, i.offset()));
		}
		record Lag(String groupId, String state, long lag, int topics) {
		}
		List<Lag> lags = new ArrayList<>();
		committed.values().forEach((g, offsets) -> {
			if (offsets.isEmpty()) {
				return;
			}
			long total = ConsumerGroups.lagByPartition(offsets, latest).values().stream().mapToLong(Long::longValue).sum();
			Set<String> topics = new LinkedHashSet<>();
			offsets.keySet().forEach(tp -> topics.add(tp.topic()));
			lags.add(new Lag(g, byId.get(g).groupState().map(String::valueOf).orElse(null), total, topics.size()));
		});
		lags.sort(Comparator.comparingLong(Lag::lag).reversed().thenComparing(Lag::groupId));
		List<Map<String, Object>> top = new ArrayList<>();
		for (Lag l : lags.subList(0, Math.min(TOP_GROUPS, lags.size()))) {
			if (l.lag() > 0) {
				top.add(Results.map("groupId", l.groupId(), "state", l.state(), "lag", l.lag(), "topics", l.topics()));
			}
		}
		return new GroupLagScan(listing.groups().size(), sample.size(), top, committed.errors().size(),
				listing.errors());
	}

	// ---- değerlendirme ----

	private static void controller(Diagnosis dx, Map<String, Object> facts, ClusterMeta.Info info, FeatureMetadata fm,
			String mode, Futures.Each<ConfigResource, Config> brokerConfigs) {
		if (info == null) {
			return;
		}
		facts.put("clusterId", info.clusterId());
		facts.put("controllerId", info.controllerId());
		FinalizedVersionRange mv = fm == null ? null : fm.finalizedFeatures().get("metadata.version");
		if (mv != null) {
			int level = mv.maxVersionLevel();
			facts.put("kafkaVersion", KafkaVersions.release(level));
			facts.put("versionSource", "metadata.version=" + level);
		}
		else if (brokerConfigs != null) {
			for (Config c : brokerConfigs.values().values()) {
				ConfigEntry ibp = c.get("inter.broker.protocol.version");
				if (ibp != null && ibp.value() != null) {
					facts.put("kafkaVersion", KafkaVersions.releaseOf(ibp.value()));
					facts.put("versionSource", "inter.broker.protocol.version=" + ibp.value());
					break;
				}
			}
		}
		List<Map<String, Object>> brokers = new ArrayList<>();
		info.brokers().stream().limit(50).forEach(n -> brokers.add(ClusterMeta.node(n)));
		facts.put("brokerCount", info.brokers().size());
		facts.put("brokers", brokers);
		if (info.brokers().size() == 1) {
			dx.info("SINGLE_BROKER", "Cluster'da tek broker var: yedeklilik yok", null, null);
		}
		if (ClusterMeta.KRAFT.equals(mode)) {
			QuorumInfo q = info.quorum();
			if (q == null) {
				dx.warning("QUORUM_UNKNOWN", "Metadata quorum'u sorgulanamadı" + (info.quorumError() == null ? ""
						: ": " + info.quorumError()), null,
						"CLUSTER DESCRIBE yetkisini ve controller'ların erişilebilirliğini kontrol edin");
				dx.markPartial();
				return;
			}
			quorum(dx, facts, q);
		}
		else if (ClusterMeta.ZOOKEEPER.equals(mode)) {
			if (info.controllerId() == null) {
				dx.critical("NO_ACTIVE_CONTROLLER", "ZooKeeper modunda aktif controller yok", null,
						"ZooKeeper bağlantısını ve broker loglarını kontrol edin; controller seçimi yapılamıyor olabilir");
			}
			dx.info("ZOOKEEPER_MODE", "Cluster ZooKeeper modunda: ZooKeeper Kafka 4.0'da kaldırıldı", null,
					"KRaft'a geçişi planlayın (3.9 köprü sürümü üzerinden migration)");
		}
		else if (info.quorumError() != null) {
			dx.warning("QUORUM_UNKNOWN", "Mod/quorum belirlenemedi: " + info.quorumError(), null,
					"CLUSTER DESCRIBE yetkisini kontrol edin");
			dx.markPartial();
		}
	}

	private static void quorum(Diagnosis dx, Map<String, Object> facts, QuorumInfo q) {
		long now = System.currentTimeMillis();
		List<Map<String, Object>> voters = new ArrayList<>();
		List<Map<String, Object>> laggingVoters = new ArrayList<>();
		for (QuorumInfo.ReplicaState r : q.voters()) {
			Long lag = r.logEndOffset() < 0 ? null : Math.max(0, q.highWatermark() - r.logEndOffset());
			Long caughtUpAgo = r.lastCaughtUpTimestamp().isPresent() && r.lastCaughtUpTimestamp().getAsLong() > 0
					? Math.max(0, now - r.lastCaughtUpTimestamp().getAsLong()) : null;
			Map<String, Object> row = Results.map("id", r.replicaId(), "leader", r.replicaId() == q.leaderId() ? true : null,
					"lag", lag, "caughtUpAgoMs", caughtUpAgo);
			voters.add(row);
			if (r.replicaId() != q.leaderId()
					&& (lag != null && lag > VOTER_LAG_RECORDS || caughtUpAgo != null && caughtUpAgo > VOTER_CAUGHT_UP_MS)) {
				laggingVoters.add(row);
			}
		}
		List<Map<String, Object>> laggingObservers = new ArrayList<>();
		for (QuorumInfo.ReplicaState r : q.observers()) {
			Long lag = r.logEndOffset() < 0 ? null : Math.max(0, q.highWatermark() - r.logEndOffset());
			if (lag != null && lag > VOTER_LAG_RECORDS) {
				laggingObservers.add(Results.map("id", r.replicaId(), "lag", lag));
			}
		}
		facts.put("quorum", Results.map("leaderId", q.leaderId(), "leaderEpoch", q.leaderEpoch(), "highWatermark",
				q.highWatermark(), "voters", voters, "observers", q.observers().size(), "laggingObservers",
				laggingObservers.isEmpty() ? null : first(laggingObservers, MAX_EVIDENCE)));
		if (q.leaderId() < 0) {
			dx.critical("NO_ACTIVE_CONTROLLER", "KRaft metadata quorum'unun lideri yok: metadata değişiklikleri yapılamaz",
					evidence("voters", q.voters().size()),
					"Controller düğümlerinin çoğunluğunun ayakta ve birbirine erişebilir olduğunu kontrol edin");
		}
		if (!laggingVoters.isEmpty()) {
			dx.warning("QUORUM_VOTER_LAGGING", laggingVoters.size() + " controller voter'ı quorum liderinin gerisinde",
					evidence("voters", laggingVoters),
					"Geride kalan controller'ın disk/ağ durumunu ve loglarını kontrol edin; çoğunluk kaybı metadata yazmalarını durdurur");
		}
		if (!laggingObservers.isEmpty()) {
			dx.warning("QUORUM_OBSERVER_LAGGING",
					laggingObservers.size() + " broker (observer) metadata log'unda " + VOTER_LAG_RECORDS + "+ kayıt geride",
					evidence("brokers", first(laggingObservers, MAX_EVIDENCE)),
					"Bu broker'lar güncel olmayan metadata ile çalışıyor olabilir; ağ ve broker loglarını kontrol edin");
		}
		if (q.voters().size() == 1) {
			dx.info("SINGLE_CONTROLLER", "Metadata quorum'unda tek voter var: controller yedekliliği yok", null,
					"Üretim için 3 (ya da 5) controller önerilir");
		}
		else if (q.voters().size() % 2 == 0) {
			dx.info("EVEN_VOTERS", "Voter sayısı çift (" + q.voters().size() + "): ek voter dayanıklılığı artırmaz", null,
					"Tek sayıda (3 ya da 5) voter kullanın");
		}
	}

	private static void partitions(Diagnosis dx, Map<String, Object> facts, Map<String, TopicDescription> topics,
			Topics.Reassignments r, ConfigScan cs, ClusterMeta.Info info, String mode) {
		if (topics == null) {
			return;
		}
		PartitionHealth h = PartitionHealth.of(topics.values(), r, cs == null ? Map.of() : cs.minIsr(),
				info == null ? null : info.brokerIds());
		long internal = topics.values().stream().filter(TopicDescription::isInternal).count();
		Map<String, Object> pf = Results.map("topics", topics.size() - internal, "internalTopics", internal,
				"partitions", h.partitions(), "offline", h.offline().size(), "underReplicated",
				h.underReplicated().size(), "underMinIsr", h.underMinIsr().size(), "nonPreferredLeader",
				h.nonPreferredLeader().size(), "reassigning",
				r == null || r.byPartition().isEmpty() ? null : r.byPartition().size(), "elrPartitions",
				ClusterMeta.KRAFT.equals(mode) ? h.withElr().size() : null, "minIsrUnknownTopics",
				cs == null ? null : cs.failed() > 0 ? cs.failed() : null);
		facts.put("partitionHealth", pf);
		if (!h.missingBrokers().isEmpty()) {
			dx.critical("BROKERS_DOWN",
					"Replika listelerinde olup canlı olmayan broker'lar: " + h.missingBrokers(),
					evidence("missingBrokers", List.copyOf(h.missingBrokers()), "liveBrokers",
							info == null ? null : info.brokerIds()),
					"Bu broker'ları ayağa kaldırın; kalıcı olarak kaldırıldılarsa partition'larını partition_reassignments_alter ile taşıyın");
		}
		if (!h.offline().isEmpty()) {
			dx.critical("OFFLINE_PARTITIONS", h.offline().size() + " partition offline (lider yok)",
					evidence("byTopic", byTopic(h.offline())),
					"Replika broker'larını ayağa kaldırın; topic bazında diagnose_topic ile inceleyin");
		}
		if (!h.underMinIsr().isEmpty()) {
			dx.critical("UNDER_MIN_ISR",
					h.underMinIsr().size() + " partition'da ISR min.insync.replicas altında: acks=all yazmalar reddedilir",
					evidence("byTopic", byTopic(h.underMinIsr())),
					"Geride kalan replikaların broker'larını kontrol edin (brokers_logdirs, brokers_list)");
		}
		List<TopicPartition> urpOnly = h.underReplicated().stream().filter(tp -> !h.underMinIsr().contains(tp)).toList();
		if (!urpOnly.isEmpty()) {
			dx.warning("UNDER_REPLICATED", urpOnly.size() + " partition under-replicated",
					evidence("byTopic", byTopic(urpOnly)),
					"Broker sağlığını, disk/ağ durumunu ve süren reassignment'ları (partition_reassignments_list) kontrol edin");
		}
		Set<String> minIsrTopics = new java.util.TreeSet<>();
		h.minIsrAboveReplicas().forEach(tp -> minIsrTopics.add(tp.topic()));
		if (!minIsrTopics.isEmpty()) {
			dx.critical("MIN_ISR_EXCEEDS_RF",
					minIsrTopics.size() + " topic'te min.insync.replicas replication factor'dan büyük: acks=all yazmalar hiç kabul edilmez",
					evidence("topics", first(minIsrTopics, MAX_EVIDENCE), "omitted", omitted(minIsrTopics)),
					"topics_config_update ile min.insync.replicas'ı RF-1'e düşürün ya da RF'yi artırın");
		}
		if (cs != null && !cs.uncleanTopics().isEmpty()) {
			dx.warning("UNCLEAN_LEADER_ELECTION",
					cs.uncleanTopics().size() + " topic'te unclean.leader.election.enable=true (veri kaybı riski)",
					evidence("topics", first(cs.uncleanTopics(), MAX_EVIDENCE), "omitted", omitted(cs.uncleanTopics())),
					"Veri kaybı kabul edilemiyorsa topics_config_update ile false yapın");
		}
		int online = h.partitions() - h.offline().size();
		if (online > 0 && !h.nonPreferredLeader().isEmpty()) {
			double pct = 100.0 * h.nonPreferredLeader().size() / online;
			String msg = h.nonPreferredLeader().size() + " partition'ın lideri tercih edilen replika değil (%"
					+ Format.round(pct, 1) + ")";
			if (pct > PREFERRED_IMBALANCE_PCT) {
				dx.warning("PREFERRED_LEADER_IMBALANCE", msg, evidence("byTopic", byTopic(h.nonPreferredLeader())),
						"brokers_elect_preferred_leaders ile tercih edilen liderlere dönün");
			}
			else {
				dx.info("PREFERRED_LEADER_IMBALANCE", msg, null, "brokers_elect_preferred_leaders");
			}
		}
	}

	/** topic → partition sayısı (en çok etkilenen ilk 10 topic). */
	private static Map<String, Object> byTopic(Collection<TopicPartition> tps) {
		Map<String, Integer> counts = new HashMap<>();
		tps.forEach(tp -> counts.merge(tp.topic(), 1, Integer::sum));
		List<Map.Entry<String, Integer>> sorted = new ArrayList<>(counts.entrySet());
		sorted.sort(Map.Entry.<String, Integer>comparingByValue().reversed().thenComparing(Map.Entry.comparingByKey()));
		Map<String, Object> out = new LinkedHashMap<>();
		sorted.stream().limit(MAX_EVIDENCE).forEach(e -> out.put(e.getKey(), e.getValue()));
		if (sorted.size() > MAX_EVIDENCE) {
			out.put("(diğer topic'ler)", sorted.size() - MAX_EVIDENCE);
		}
		return out;
	}

	private static void disks(Diagnosis dx, Map<String, Object> facts,
			Futures.Each<Integer, Map<String, LogDirDescription>> dirs) {
		if (dirs == null) {
			return;
		}
		List<Map<String, Object>> rows = new ArrayList<>();
		List<Map<String, Object>> errors = new ArrayList<>();
		List<Map<String, Object>> high = new ArrayList<>();
		List<Map<String, Object>> full = new ArrayList<>();
		for (Map.Entry<Integer, Map<String, LogDirDescription>> b : new TreeMap<>(dirs.values()).entrySet()) {
			for (Map.Entry<String, LogDirDescription> d : new TreeMap<>(b.getValue()).entrySet()) {
				LogDirDescription desc = d.getValue();
				long size = desc.replicaInfos().values().stream().mapToLong(i -> i.size()).sum();
				Map<String, Object> row = Results.map("brokerId", b.getKey(), "path", d.getKey(), "sizeBytes", size,
						"size", Format.bytes(size));
				if (desc.error() != null) {
					String err = KafkaErrors.explain(desc.error());
					row.put("error", err);
					errors.add(Results.map("brokerId", b.getKey(), "path", d.getKey(), "error", err));
				}
				if (desc.totalBytes().isPresent() && desc.usableBytes().isPresent() && desc.totalBytes().getAsLong() > 0) {
					long total = desc.totalBytes().getAsLong();
					long usable = desc.usableBytes().getAsLong();
					double pct = Format.round(100.0 * (total - usable) / total, 1);
					row.put("usedPct", pct);
					row.put("disk", Format.bytes(usable) + " boş / " + Format.bytes(total));
					Map<String, Object> ev = Results.map("brokerId", b.getKey(), "path", d.getKey(), "usedPct", pct);
					if (pct >= DISK_CRITICAL_PCT) {
						full.add(ev);
					}
					else if (pct >= DISK_WARN_PCT) {
						high.add(ev);
					}
				}
				rows.add(row);
			}
		}
		Map<String, Object> disk = Results.map("logDirs", first(rows, 50), "logDirsOmitted", omitted50(rows),
				"failedBrokers", dirs.errors().isEmpty() ? null : dirs.errors());
		facts.put("disk", disk);
		if (!errors.isEmpty()) {
			dx.critical("LOG_DIR_ERROR", errors.size() + " log dizini hata durumunda (offline dizin)",
					evidence("dirs", first(errors, MAX_EVIDENCE)),
					"Broker loglarında disk/IO hatasını inceleyin; dizindeki replikalar offline olabilir");
		}
		if (!full.isEmpty()) {
			dx.critical("DISK_FULL", full.size() + " log dizini %" + (int) DISK_CRITICAL_PCT + "+ dolu",
					evidence("dirs", first(full, MAX_EVIDENCE)),
					"Hemen yer açın: retention'ı düşürün (topics_config_update), büyük topic'leri inceleyin (topics_size) ya da disk ekleyin");
		}
		if (!high.isEmpty()) {
			dx.warning("DISK_HIGH", high.size() + " log dizini %" + (int) DISK_WARN_PCT + "+ dolu",
					evidence("dirs", first(high, MAX_EVIDENCE)),
					"En büyük topic'leri (topics_size, brokers_logdirs) ve retention ayarlarını gözden geçirin");
		}
		if (!dirs.errors().isEmpty()) {
			dx.markPartial();
		}
	}

	private static Integer omitted50(List<?> rows) {
		return rows.size() > 50 ? rows.size() - 50 : null;
	}

	private static void skew(Diagnosis dx, Map<String, Object> facts, Map<String, TopicDescription> topics,
			ClusterMeta.Info info) {
		if (topics == null || info == null || info.brokers().size() < 2) {
			return;
		}
		Map<Integer, int[]> counts = new TreeMap<>();
		info.brokerIds().forEach(id -> counts.put(id, new int[2]));
		long leaders = 0;
		long replicas = 0;
		for (TopicDescription d : topics.values()) {
			for (TopicPartitionInfo p : d.partitions()) {
				Integer leader = Topics.leader(p);
				if (leader != null && counts.containsKey(leader)) {
					counts.get(leader)[0]++;
					leaders++;
				}
				for (Node n : p.replicas()) {
					if (counts.containsKey(n.id())) {
						counts.get(n.id())[1]++;
						replicas++;
					}
				}
			}
		}
		double avgLeaders = (double) leaders / counts.size();
		double avgReplicas = (double) replicas / counts.size();
		double maxLeaderDev = 0;
		double maxReplicaDev = 0;
		List<Map<String, Object>> rows = new ArrayList<>();
		for (Map.Entry<Integer, int[]> e : counts.entrySet()) {
			double ld = avgLeaders == 0 ? 0 : (e.getValue()[0] - avgLeaders) / avgLeaders * 100;
			double rd = avgReplicas == 0 ? 0 : (e.getValue()[1] - avgReplicas) / avgReplicas * 100;
			maxLeaderDev = Math.max(maxLeaderDev, Math.abs(ld));
			maxReplicaDev = Math.max(maxReplicaDev, Math.abs(rd));
			rows.add(Results.map("brokerId", e.getKey(), "leaders", e.getValue()[0], "replicas", e.getValue()[1],
					"leaderDeviationPct", Format.round(ld, 1), "replicaDeviationPct", Format.round(rd, 1)));
		}
		facts.put("distribution", Results.map("maxLeaderDeviationPct", Format.round(maxLeaderDev, 1),
				"maxReplicaDeviationPct", Format.round(maxReplicaDev, 1), "brokers", first(rows, 50)));
		if (maxReplicaDev > SKEW_PCT && replicas >= 10L * counts.size()) {
			dx.warning("REPLICA_SKEW", "Replika dağılımı dengesiz: en büyük sapma %" + Format.round(maxReplicaDev, 1),
					evidence("brokers", first(rows, MAX_EVIDENCE)),
					"partition_reassignments_alter ile replikaları dengeleyin (yeni eklenen broker boş kalmış olabilir)");
		}
		if (maxLeaderDev > SKEW_PCT && leaders >= 10L * counts.size()) {
			dx.warning("LEADER_SKEW", "Lider dağılımı dengesiz: en büyük sapma %" + Format.round(maxLeaderDev, 1),
					evidence("brokers", first(rows, MAX_EVIDENCE)),
					"brokers_elect_preferred_leaders; tercih edilen liderler de dengesizse partition_reassignments_alter");
		}
	}

	private static void drift(Diagnosis dx, Map<String, Object> facts, Futures.Each<ConfigResource, Config> configs) {
		if (configs == null) {
			return;
		}
		Map<Integer, Config> byBroker = new TreeMap<>();
		configs.values().forEach((r, c) -> byBroker.put(Integer.parseInt(r.name()), c));
		if (byBroker.size() < 2) {
			facts.put("configDrift", Results.map("brokersCompared", byBroker.size(), "note",
					"Karşılaştırma için en az 2 broker gerekli"));
			return;
		}
		Map<String, Map<Integer, String>> values = new TreeMap<>();
		byBroker.forEach((id, c) -> {
			for (ConfigEntry e : c.entries()) {
				if (e.isSensitive() || PER_BROKER_KEYS.contains(e.name()) || PER_BROKER_PATTERN.matcher(e.name()).matches()) {
					continue;
				}
				values.computeIfAbsent(e.name(), k -> new TreeMap<>()).put(id, e.value());
			}
		});
		List<Map<String, Object>> diffs = new ArrayList<>();
		values.forEach((key, perBroker) -> {
			Set<String> distinct = new LinkedHashSet<>();
			for (Integer id : byBroker.keySet()) {
				distinct.add(Objects.toString(perBroker.get(id), "(yok)"));
			}
			if (distinct.size() > 1) {
				Map<String, List<Integer>> groups = new LinkedHashMap<>();
				for (Integer id : byBroker.keySet()) {
					groups.computeIfAbsent(Objects.toString(perBroker.get(id), "(yok)"), k -> new ArrayList<>()).add(id);
				}
				diffs.add(Results.map("name", key, "values", groups));
			}
		});
		facts.put("configDrift", Results.map("brokersCompared", byBroker.size(), "differingKeys", diffs.size(),
				"failedBrokers", configs.errors().isEmpty() ? null : configs.errors().size()));
		if (!diffs.isEmpty()) {
			dx.warning("CONFIG_DRIFT", diffs.size() + " broker config'i broker'lar arasında farklı",
					evidence("keys", first(diffs, MAX_EVIDENCE), "omitted", omitted(diffs)),
					"Bilinçli değilse eşitleyin (brokers_config_update ya da deployment); ayrıntı için brokers_config_drift");
		}
	}

	private static void groupFindings(Diagnosis dx, Map<String, Object> facts, GroupLagScan g) {
		if (g == null) {
			return;
		}
		facts.put("consumerGroups", Results.map("total", g.total(), "scanned", g.scanned(), "skipped",
				g.total() > g.scanned() ? g.total() - g.scanned() : null, "sampling",
				g.total() > g.scanned() ? "aktif gruplar önce, ilk " + MAX_GROUPS + " grup tarandı" : null,
				"failedGroups", g.failed() > 0 ? g.failed() : null, "topLag", g.top(), "listErrors",
				g.listErrors().isEmpty() ? null : g.listErrors()));
		if (g.failed() > 0 || !g.listErrors().isEmpty() || g.total() > g.scanned()) {
			dx.markPartial();
		}
		List<Map<String, Object>> active = new ArrayList<>();
		List<Map<String, Object>> idle = new ArrayList<>();
		for (Map<String, Object> row : g.top()) {
			if (((Long) row.get("lag")) < LARGE_LAG) {
				continue;
			}
			(GroupState.EMPTY.toString().equals(row.get("state")) ? idle : active).add(row);
		}
		if (!active.isEmpty()) {
			dx.warning("LARGE_CONSUMER_LAG", active.size() + " aktif consumer group'un lag'i " + LARGE_LAG + "+",
					evidence("groups", active),
					"diagnose_consumer_group ile eğilimi ve takılı partition'ları inceleyin");
		}
		if (!idle.isEmpty()) {
			dx.info("LARGE_LAG_EMPTY_GROUPS", idle.size() + " üyesiz (EMPTY) grubun lag'i " + LARGE_LAG + "+",
					evidence("groups", idle),
					"Terk edilmiş gruplarsa consumer_groups_delete ile temizleyin; değilse consumer'ları başlatın");
		}
	}

	private static void connectorFindings(Diagnosis dx, Map<String, Object> facts, List<ConnectorOverview> all) {
		if (all == null) {
			return;
		}
		List<Map<String, Object>> failed = new ArrayList<>();
		int failedTasks = 0;
		for (ConnectorOverview c : all) {
			ConnectorStatus s = c.status();
			if (s.hasFailure()) {
				failedTasks += s.failedTaskIds().size();
				failed.add(Results.map("name", c.name(), "state", s.state(), "failedTasks", s.failedTaskIds().size(),
						"traceSummary", ConnectJson.traceSummary(s.firstTrace())));
			}
		}
		facts.put("connectors", Results.map("total", all.size(), "failed", failed.size(), "failedTasks", failedTasks));
		if (!failed.isEmpty()) {
			dx.warning("CONNECTORS_FAILED", failed.size() + " connector'da (ya da görevinde) hata var",
					evidence("connectors", first(failed, MAX_EVIDENCE), "omitted", omitted(failed)),
					"connectors_restart includeTasks=true onlyFailed=true; tekrar düşerse diagnose_connector ile nedenini inceleyin");
		}
	}

}
