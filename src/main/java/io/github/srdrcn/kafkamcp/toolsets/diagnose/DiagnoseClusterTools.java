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

/** diagnose_cluster: single-call cluster health diagnosis (KRaft and ZooKeeper). */
@Component
public class DiagnoseClusterTools {

	/** Keys that are broker-specific or mode-specific and therefore do not count as config drift. */
	public static final List<String> PER_BROKER_KEYS = List.of("broker.id", "node.id", "broker.rack", "listeners",
			"advertised.listeners", "advertised.host.name", "advertised.port", "host.name", "port", "log.dir",
			"log.dirs", "metadata.log.dir", "process.roles", "controller.quorum.voters",
			"controller.quorum.bootstrap.servers", "controller.listener.names", "early.start.listeners",
			"zookeeper.connect", "broker.id.generation.enable", "reserved.broker.max.id");

	private static final Pattern PER_BROKER_PATTERN = Pattern
		.compile(".*(ssl\\.(keystore|truststore)\\.(location|type)|\\.keytab|\\.principal|server\\.cnxn)$");

	static final long LARGE_LAG = 10_000;

	/** At most this many groups in the lag scan (active ones first); the rest are skipped and reported. */
	static final int MAX_GROUPS = 500;

	static final int TOP_GROUPS = 5;

	static final long VOTER_LAG_RECORDS = 1_000;

	static final long VOTER_CAUGHT_UP_MS = 30_000;

	static final double SKEW_PCT = 20;

	static final double PREFERRED_IMBALANCE_PCT = 10;

	static final double DISK_WARN_PCT = 85;

	static final double DISK_CRITICAL_PCT = 95;

	private static final long GROUP_STEP_MAX_MS = 4_000;

	/** Time kept before the deadline for the dependent checks' own work (config, log dirs). */
	private static final long DEPENDENT_WORK_MS = 3_500;

	private final ConnectApi connect;

	public DiagnoseClusterTools(ConnectApi connect) {
		this.connect = connect;
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("diagnose_cluster",
					"Diagnose the cluster: mode/version, controller/quorum, brokers, URP/offline/min ISR, disks, balance, config drift, lag, connectors")
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
			Check<FeatureMetadata> features = checks.start("features",
					() -> Futures.awaitAtMost(call, call.admin().describeFeatures().featureMetadata(), 4_000));
			Check<Map<String, TopicDescription>> topics = checks.start("topics",
					() -> Topics.describeAll(call, true));
			Check<Topics.Reassignments> reassign = checks.start("reassignments",
					() -> Topics.reassignments(call, Topics.submitReassignments(call)));
			Check<ConfigScan> topicConfigs = checks.start("topic configs", () -> {
				Map<String, TopicDescription> t = topics.require(call.remainingMillis() - DEPENDENT_WORK_MS);
				return t == null ? null : topicConfigs(call, t.keySet());
			});
			Check<Futures.Each<ConfigResource, Config>> brokerConfigs = checks.start("broker configs", () -> {
				ClusterMeta.Info i = meta.require(call.remainingMillis() - DEPENDENT_WORK_MS);
				return i == null ? null : Configs.describe(call, i.brokerIds().stream().map(Configs::broker).toList());
			});
			Check<Futures.Each<Integer, Map<String, LogDirDescription>>> dirs = checks.start("log dirs", () -> {
				ClusterMeta.Info i = meta.require(call.remainingMillis() - DEPENDENT_WORK_MS);
				return i == null ? null : LogDirs.describe(call, i.brokerIds(), i.brokerIds());
			});
			Check<GroupLagScan> groups = checks.start("consumer group lag", () -> groupLag(call));
			Check<List<ConnectorOverview>> connectors = call.clusterConfig().hasConnect()
					? checks.start("connectors", () -> connect.overview(call.cluster())) : null;

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

	// ---- sub-checks ----

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
					// invalid value: skipped
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
		// active groups (not Empty) are scanned first
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

	// ---- evaluation ----

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
		if (info.brokers().size() > 50) {
			facts.put("brokersOmitted", info.brokers().size() - 50);
		}
		if (info.brokers().size() == 1) {
			dx.info("SINGLE_BROKER", "Cluster has a single broker: no redundancy", null, null);
		}
		if (ClusterMeta.KRAFT.equals(mode)) {
			QuorumInfo q = info.quorum();
			if (q == null) {
				dx.warning("QUORUM_UNKNOWN", "Could not query the metadata quorum" + (info.quorumError() == null ? ""
						: ": " + info.quorumError()), null,
						"Check the CLUSTER DESCRIBE permission and that the controllers are reachable");
				dx.markPartial();
				return;
			}
			quorum(dx, facts, q);
		}
		else if (ClusterMeta.ZOOKEEPER.equals(mode)) {
			if (info.controllerId() == null) {
				dx.critical("NO_ACTIVE_CONTROLLER", "No active controller in ZooKeeper mode", null,
						"Check the ZooKeeper connection and the broker logs; controller election may be failing");
			}
			dx.info("ZOOKEEPER_MODE", "Cluster runs in ZooKeeper mode: ZooKeeper was removed in Kafka 4.0", null,
					"Plan the move to KRaft (migration via the 3.9 bridge release)");
		}
		else if (info.quorumError() != null) {
			dx.warning("QUORUM_UNKNOWN", "Could not determine the mode/quorum: " + info.quorumError(), null,
					"Check the CLUSTER DESCRIBE permission");
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
				laggingObservers.isEmpty() ? null : first(laggingObservers, MAX_EVIDENCE), "laggingObserversOmitted",
				omitted(laggingObservers)));
		if (q.leaderId() < 0) {
			dx.critical("NO_ACTIVE_CONTROLLER", "The KRaft metadata quorum has no leader: metadata changes are not possible",
					evidence("voters", q.voters().size()),
					"Check that a majority of the controller nodes are up and can reach each other");
		}
		if (!laggingVoters.isEmpty()) {
			dx.warning("QUORUM_VOTER_LAGGING", laggingVoters.size() + " controller voter(s) are behind the quorum leader",
					evidence("voters", laggingVoters),
					"Check the disk/network and logs of the lagging controller; losing the majority stops metadata writes");
		}
		if (!laggingObservers.isEmpty()) {
			dx.warning("QUORUM_OBSERVER_LAGGING",
					laggingObservers.size() + " broker(s) (observers) are " + VOTER_LAG_RECORDS + "+ records behind in the metadata log",
					evidence("brokers", first(laggingObservers, MAX_EVIDENCE), "omitted", omitted(laggingObservers)),
					"These brokers may be running with stale metadata; check the network and the broker logs");
		}
		if (q.voters().size() == 1) {
			dx.info("SINGLE_CONTROLLER", "The metadata quorum has a single voter: no controller redundancy", null,
					"3 (or 5) controllers are recommended for production");
		}
		else if (q.voters().size() % 2 == 0) {
			dx.info("EVEN_VOTERS", "Even number of voters (" + q.voters().size() + "): the extra voter does not improve fault tolerance", null,
					"Use an odd number of voters (3 or 5)");
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
					"Brokers in replica lists that are not alive: " + h.missingBrokers(),
					evidence("missingBrokers", List.copyOf(h.missingBrokers()), "liveBrokers",
							info == null ? null : info.brokerIds()),
					"Bring these brokers up; if they were removed permanently, move their partitions with partition_reassignments_alter");
		}
		if (!h.offline().isEmpty()) {
			dx.critical("OFFLINE_PARTITIONS", h.offline().size() + " partition(s) offline (no leader)",
					evidence("byTopic", byTopic(h.offline())),
					"Bring up the replica brokers; inspect per topic with diagnose_topic");
		}
		if (!h.underMinIsr().isEmpty()) {
			dx.critical("UNDER_MIN_ISR",
					h.underMinIsr().size() + " partition(s) have ISR below min.insync.replicas: acks=all writes are rejected",
					evidence("byTopic", byTopic(h.underMinIsr())),
					"Check the brokers of the lagging replicas (brokers_logdirs, brokers_list)");
		}
		List<TopicPartition> urpOnly = h.underReplicated().stream().filter(tp -> !h.underMinIsr().contains(tp)).toList();
		if (!urpOnly.isEmpty()) {
			dx.warning("UNDER_REPLICATED", urpOnly.size() + " partition(s) under-replicated",
					evidence("byTopic", byTopic(urpOnly)),
					"Check broker health, disks/network and ongoing reassignments (partition_reassignments_list)");
		}
		Set<String> minIsrTopics = new java.util.TreeSet<>();
		h.minIsrAboveReplicas().forEach(tp -> minIsrTopics.add(tp.topic()));
		if (!minIsrTopics.isEmpty()) {
			dx.critical("MIN_ISR_EXCEEDS_RF",
					minIsrTopics.size() + " topic(s) have min.insync.replicas greater than the replication factor: no acks=all write is ever accepted",
					evidence("topics", first(minIsrTopics, MAX_EVIDENCE), "omitted", omitted(minIsrTopics)),
					"Lower min.insync.replicas to RF-1 with topics_config_update or increase the RF");
		}
		if (cs != null && !cs.uncleanTopics().isEmpty()) {
			dx.warning("UNCLEAN_LEADER_ELECTION",
					cs.uncleanTopics().size() + " topic(s) have unclean.leader.election.enable=true (risk of data loss)",
					evidence("topics", first(cs.uncleanTopics(), MAX_EVIDENCE), "omitted", omitted(cs.uncleanTopics())),
					"If data loss is unacceptable, set it to false with topics_config_update");
		}
		int online = h.partitions() - h.offline().size();
		if (online > 0 && !h.nonPreferredLeader().isEmpty()) {
			double pct = 100.0 * h.nonPreferredLeader().size() / online;
			String msg = h.nonPreferredLeader().size() + " partition(s) are not led by their preferred replica ("
					+ Format.round(pct, 1) + "%)";
			if (pct > PREFERRED_IMBALANCE_PCT) {
				dx.warning("PREFERRED_LEADER_IMBALANCE", msg, evidence("byTopic", byTopic(h.nonPreferredLeader())),
						"Move back to the preferred leaders with brokers_elect_preferred_leaders");
			}
			else {
				dx.info("PREFERRED_LEADER_IMBALANCE", msg, null, "brokers_elect_preferred_leaders");
			}
		}
	}

	/** topic → partition count (the 10 most affected topics). */
	private static Map<String, Object> byTopic(Collection<TopicPartition> tps) {
		Map<String, Integer> counts = new HashMap<>();
		tps.forEach(tp -> counts.merge(tp.topic(), 1, Integer::sum));
		List<Map.Entry<String, Integer>> sorted = new ArrayList<>(counts.entrySet());
		sorted.sort(Map.Entry.<String, Integer>comparingByValue().reversed().thenComparing(Map.Entry.comparingByKey()));
		Map<String, Object> out = new LinkedHashMap<>();
		sorted.stream().limit(MAX_EVIDENCE).forEach(e -> out.put(e.getKey(), e.getValue()));
		if (sorted.size() > MAX_EVIDENCE) {
			out.put("(other topics)", sorted.size() - MAX_EVIDENCE);
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
					row.put("disk", Format.bytes(usable) + " free / " + Format.bytes(total));
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
			dx.critical("LOG_DIR_ERROR", errors.size() + " log dir(s) in an error state (offline dir)",
					evidence("dirs", first(errors, MAX_EVIDENCE)),
					"Look for disk/IO errors in the broker logs; the replicas in the dir may be offline");
		}
		if (!full.isEmpty()) {
			dx.critical("DISK_FULL", full.size() + " log dir(s) are " + (int) DISK_CRITICAL_PCT + "%+ full",
					evidence("dirs", first(full, MAX_EVIDENCE)),
					"Free up space now: lower retention (topics_config_update), inspect large topics (topics_size) or add disk");
		}
		if (!high.isEmpty()) {
			dx.warning("DISK_HIGH", high.size() + " log dir(s) are " + (int) DISK_WARN_PCT + "%+ full",
					evidence("dirs", first(high, MAX_EVIDENCE)),
					"Review the largest topics (topics_size, brokers_logdirs) and the retention settings");
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
				"maxReplicaDeviationPct", Format.round(maxReplicaDev, 1), "brokers", first(rows, 50), "brokersOmitted",
				omitted50(rows)));
		if (maxReplicaDev > SKEW_PCT && replicas >= 10L * counts.size()) {
			dx.warning("REPLICA_SKEW", "Replica distribution is unbalanced: max deviation " + Format.round(maxReplicaDev, 1) + "%",
					evidence("brokers", first(rows, MAX_EVIDENCE)),
					"Rebalance the replicas with partition_reassignments_alter (a newly added broker may have been left empty)");
		}
		if (maxLeaderDev > SKEW_PCT && leaders >= 10L * counts.size()) {
			dx.warning("LEADER_SKEW", "Leader distribution is unbalanced: max deviation " + Format.round(maxLeaderDev, 1) + "%",
					evidence("brokers", first(rows, MAX_EVIDENCE)),
					"brokers_elect_preferred_leaders; if the preferred leaders are unbalanced too, partition_reassignments_alter");
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
					"At least 2 brokers are needed for a comparison"));
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
				distinct.add(Objects.toString(perBroker.get(id), "(none)"));
			}
			if (distinct.size() > 1) {
				Map<String, List<Integer>> groups = new LinkedHashMap<>();
				for (Integer id : byBroker.keySet()) {
					groups.computeIfAbsent(Objects.toString(perBroker.get(id), "(none)"), k -> new ArrayList<>()).add(id);
				}
				diffs.add(Results.map("name", key, "values", groups));
			}
		});
		facts.put("configDrift", Results.map("brokersCompared", byBroker.size(), "differingKeys", diffs.size(),
				"failedBrokers", configs.errors().isEmpty() ? null : configs.errors().size()));
		if (!diffs.isEmpty()) {
			dx.warning("CONFIG_DRIFT", diffs.size() + " broker config(s) differ between brokers",
					evidence("keys", first(diffs, MAX_EVIDENCE), "omitted", omitted(diffs)),
					"If not intended, align them (brokers_config_update or the deployment); see brokers_config_drift for details");
		}
	}

	private static void groupFindings(Diagnosis dx, Map<String, Object> facts, GroupLagScan g) {
		if (g == null) {
			return;
		}
		facts.put("consumerGroups", Results.map("total", g.total(), "scanned", g.scanned(), "skipped",
				g.total() > g.scanned() ? g.total() - g.scanned() : null, "sampling",
				g.total() > g.scanned() ? "active groups first, the first " + MAX_GROUPS + " groups were scanned" : null,
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
			dx.warning("LARGE_CONSUMER_LAG", active.size() + " active consumer group(s) have a lag of " + LARGE_LAG + "+",
					evidence("groups", active),
					"Inspect the trend and stuck partitions with diagnose_consumer_group");
		}
		if (!idle.isEmpty()) {
			dx.info("LARGE_LAG_EMPTY_GROUPS", idle.size() + " group(s) without members (EMPTY) have a lag of " + LARGE_LAG + "+",
					evidence("groups", idle),
					"If the groups are abandoned, clean them up with consumer_groups_delete; otherwise start the consumers");
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
			dx.warning("CONNECTORS_FAILED", failed.size() + " connector(s) (or their tasks) have failed",
					evidence("connectors", first(failed, MAX_EVIDENCE), "omitted", omitted(failed)),
					"connectors_restart includeTasks=true onlyFailed=true; if it fails again, find the cause with diagnose_connector");
		}
	}

}
