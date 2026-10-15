package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static io.github.srdrcn.kafkamcp.toolsets.diagnose.Diagnosis.evidence;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.MAX_EVIDENCE;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.first;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.omitted;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.partitionNumbers;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.Configs;
import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.LastRecords;
import io.github.srdrcn.kafkamcp.kafka.LogDirs;
import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.kafka.Producers;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectApi;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectJson;
import io.github.srdrcn.kafkamcp.toolsets.consumers.ConsumerGroups;
import io.github.srdrcn.kafkamcp.toolsets.consumers.Settled;
import io.github.srdrcn.kafkamcp.toolsets.diagnose.Checks.Check;

/** diagnose_topic: single-call topic health diagnosis. */
@Component
public class DiagnoseTopicTools {

	/** Lag above this is an info finding (even if it is not growing or stuck). */
	static final long HIGH_LAG = 10_000;

	/** Minimum lag for the growing-lag warning. */
	static final long GROWING_LAG_MIN = 100;

	/** The topic counts as idle if the last write is older than this. */
	static final long IDLE_SECONDS = 7L * 24 * 3600;

	static final long SKEW_MIN_BYTES = 64L * 1024 * 1024;

	static final long SKEW_MIN_MESSAGES = 100_000;

	static final long SHORT_RETENTION_MS = 3_600_000;

	private static final long LIST_GROUPS_MAX_MS = 3_000;

	/** Time left after the second sample for reading record ages and describing groups. */
	private static final long WINDOW_RESERVE_MS = 4_000;

	private static final long DESCRIBE_GROUPS_RESERVE_MS = 1_500;

	private static final long CONNECTOR_CHECK_MS = 5_000;

	private static final long LAST_RECORD_BUDGET_MS = 2_000;

	/** Maximum number of settings shown in facts.config.nonDefault (the rest are counted). */
	static final int MAX_NON_DEFAULT = 15;

	/** Maximum number of principals shown in the facts.access lists (the rest are counted). */
	static final int MAX_PRINCIPALS = 20;

	private final ConnectApi connect;

	public DiagnoseTopicTools(ConnectApi connect) {
		this.connect = connect;
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("diagnose_topic",
					"Diagnose topic health: partitions/ISR, config, size/skew, writes, consumer lag/stuck, transactions, connectors, ACLs")
			.required("topic", P.string("Topic name"))
			.param("intervalSeconds", DiagnoseParams.interval(DiagnoseParams.TOPIC_INTERVAL_SECONDS))
			.handler(this::diagnose));
	}

	record Sizes(Map<TopicPartition, LogDirs.PartitionSize> sizes, Map<Integer, String> failedBrokers) {
	}

	record OffsetsInfo(Map<TopicPartition, Offsets.Range> ranges, Map<TopicPartition, Long> lastWrite) {
	}

	record GroupScan(int listed, List<String> listErrors, OffsetWindow window,
			Map<String, Settled<ConsumerGroupDescription>> described, StuckCheck stuck) {
	}

	record ProducerScan(int activeProducers, int partitionsWithOpenTxn, int openTransactions,
			List<Map<String, Object>> hanging, List<Map<String, Object>> longRunning, int failedPartitions,
			Long hangingThresholdMs) {
	}

	Object diagnose(ToolCall call) {
		String topic = call.str("topic");
		long intervalMs = DiagnoseParams.intervalMs(call, DiagnoseParams.TOPIC_INTERVAL_SECONDS);
		TopicDescription d = Topics.describeOne(call, topic);
		List<TopicPartition> online = Topics.onlinePartitions(List.of(d));
		Diagnosis dx = new Diagnosis();
		Map<String, Object> facts = new LinkedHashMap<>();
		String mode;
		try (Checks checks = new Checks(call, dx)) {
			Check<ClusterMeta.Info> meta = checks.start("cluster metadata", () -> ClusterMeta.describe(call, true));
			Check<FeatureMetadata> features = checks.start("features",
					() -> Futures.awaitAtMost(call, call.admin().describeFeatures().featureMetadata(), 4_000));
			Check<Config> config = checks.start("topic config", () -> Configs.describeOne(call, Configs.topic(topic)));
			Check<Topics.Reassignments> reassign = checks.start("reassignments",
					() -> Topics.reassignments(call, Topics.submitReassignments(call)));
			Check<Sizes> sizes = checks.start("size (log dirs)", () -> {
				Futures.Each<Integer, Map<String, LogDirDescription>> dirs = LogDirs.describe(call,
						LogDirs.brokersHosting(List.of(d)));
				return new Sizes(LogDirs.partitionSizes(dirs.values(), Map.of(topic, d)), dirs.errors());
			});
			Check<OffsetsInfo> offsets = checks.start("offsets and last write", () -> offsetsInfo(call, online));
			Check<GroupScan> groups = checks.start("consumer groups and write rate",
					() -> groupScan(call, online, intervalMs));
			Check<ProducerScan> producers = checks.start("producer/transaction", () -> producers(call, online, topic));
			Check<ConnectorRefs.Result> connectors = call.clusterConfig().hasConnect()
					? checks.start("connectors", () -> ConnectorRefs.find(connect, call, topic, CONNECTOR_CHECK_MS))
					: null;
			Check<AclView> access = checks.start("ACL",
					() -> AclView.load(call, List.of(new AclView.Resource(ResourceType.TOPIC, topic)), null));

			ClusterMeta.Info info = meta.get();
			mode = info == null ? null : info.mode();
			FeatureMetadata fm = features.get();
			if (mode == null && fm != null && fm.finalizedFeatures().containsKey("metadata.version")) {
				mode = ClusterMeta.KRAFT;
			}
			Config cfg = config.get();
			Topics.Reassignments reassignments = reassign.get();
			int rf = Topics.replicationFactor(d, reassignments == null ? Topics.Reassignments.NONE : reassignments);
			Integer minIsr = intValue(cfg, "min.insync.replicas");
			partitionHealth(dx, facts, d, reassignments, minIsr, info, mode, fm);
			configSanity(dx, facts, cfg, rf, minIsr, info, d);
			OffsetsInfo oi = offsets.get();
			sizes(dx, facts, sizes.get(), oi, d);
			activity(dx, facts, oi, d);
			GroupScan gs = groups.get();
			consumers(dx, facts, gs, oi, d);
			producerFindings(dx, facts, producers.get());
			if (connectors != null) {
				connectorFindings(dx, facts, connectors.get());
			}
			accessFindings(dx, facts, access.get(), topic);
		}
		Map<String, Object> head = Results.map("topic", topic, "cluster", call.cluster(), "mode", mode);
		return dx.result(head, facts);
	}

	// ---- sub-checks (on virtual threads) ----

	private static OffsetsInfo offsetsInfo(ToolCall call, List<TopicPartition> online) {
		ListOffsetsResult maxTs = Offsets.submit(call, online, OffsetSpec.maxTimestamp());
		Map<TopicPartition, Offsets.Range> ranges = Offsets.ranges(call, online);
		Map<TopicPartition, ListOffsetsResultInfo> max = Offsets.awaitEach(call, online, maxTs).values();
		Map<TopicPartition, Long> last = new LinkedHashMap<>();
		Map<TopicPartition, Offsets.Range> fallback = new LinkedHashMap<>();
		ranges.forEach((tp, r) -> {
			ListOffsetsResultInfo i = max.get(tp);
			if (r.count() == 0) {
				return;
			}
			if (i != null && i.timestamp() >= 0) {
				last.put(tp, i.timestamp());
			}
			else {
				fallback.put(tp, r);
			}
		});
		if (!fallback.isEmpty()) {
			LastRecords.read(call, fallback, LAST_RECORD_BUDGET_MS).forEach((tp, l) -> {
				if (l.timestamp() >= 0) {
					last.put(tp, l.timestamp());
				}
			});
		}
		return new OffsetsInfo(ranges, last);
	}

	private static GroupScan groupScan(ToolCall call, List<TopicPartition> online, long intervalMs) {
		List<String> ids = List.of();
		List<String> listErrors = new ArrayList<>();
		int listed = 0;
		try {
			Groups.Listing listing = Groups.list(call, LIST_GROUPS_MAX_MS);
			ids = listing.ids();
			listed = ids.size();
			listErrors.addAll(listing.errors());
		}
		catch (ToolException e) {
			listErrors.add(e.getMessage());
		}
		OffsetWindow w = OffsetWindow.measure(call, online, ids, true, intervalMs, WINDOW_RESERVE_MS);
		List<String> withCommits = w.groupsWithCommits();
		StuckCheck stuck = StuckCheck.of(call, w, withCommits,
				Math.min(StuckCheck.READ_BUDGET_MS, call.remainingMillis() - DESCRIBE_GROUPS_RESERVE_MS - 1_000));
		Map<String, Settled<ConsumerGroupDescription>> described = withCommits.isEmpty() ? Map.of()
				: ConsumerGroups.describe(call, withCommits, DESCRIBE_GROUPS_RESERVE_MS);
		return new GroupScan(listed, listErrors, w, described, stuck);
	}

	private static ProducerScan producers(ToolCall call, List<TopicPartition> online, String topic) {
		Futures.Each<TopicPartition, List<ProducerState>> each = Producers.describe(call, online);
		Set<Long> ids = new HashSet<>();
		List<Map.Entry<TopicPartition, ProducerState>> open = new ArrayList<>();
		each.values().forEach((tp, list) -> list.forEach(s -> {
			ids.add(s.producerId());
			if (s.currentTransactionStartOffset().isPresent()) {
				open.add(Map.entry(tp, s));
			}
		}));
		Set<TopicPartition> openPartitions = new HashSet<>();
		open.forEach(e -> openPartitions.add(e.getKey()));
		List<Map<String, Object>> hanging = new ArrayList<>();
		List<Map<String, Object>> longRunning = new ArrayList<>();
		int openTransactions = 0;
		Long threshold = null;
		if (!open.isEmpty()) {
			Producers.OpenTransactions txns = Producers.listOpen(call);
			Set<Long> openIds = new HashSet<>();
			List<String> touching = new ArrayList<>();
			Set<Long> topicTxnProducers = new HashSet<>();
			open.forEach(e -> topicTxnProducers.add(e.getValue().producerId()));
			for (TransactionListing l : txns.listings()) {
				openIds.add(l.producerId());
				if (topicTxnProducers.contains(l.producerId())) {
					touching.add(l.transactionalId());
				}
			}
			boolean candidates = open.stream().anyMatch(e -> !openIds.contains(e.getValue().producerId()));
			threshold = candidates ? Producers.maxTransactionTimeoutMs(call) : null;
			long now = System.currentTimeMillis();
			for (Map.Entry<TopicPartition, ProducerState> e : open) {
				ProducerState s = e.getValue();
				if (threshold != null && Producers.hangingSuspect(s, openIds, threshold, now)) {
					hanging.add(Results.map("partition", e.getKey().partition(), "producerId", s.producerId(),
							"startOffset", s.currentTransactionStartOffset().getAsLong(), "lastTimestamp",
							Format.iso(s.lastTimestamp()), "ageSeconds", Format.ageSeconds(s.lastTimestamp())));
				}
			}
			Futures.Each<String, TransactionDescription> described = Producers.describeTransactions(call, touching);
			for (Map.Entry<String, TransactionDescription> t : described.values().entrySet()) {
				openTransactions++;
				long start = t.getValue().transactionStartTimeMs().orElse(-1);
				Long age = Format.ageSeconds(start);
				if (age != null && age * 1000 > t.getValue().transactionTimeoutMs()) {
					longRunning.add(Results.map("transactionalId", t.getKey(), "state",
							String.valueOf(t.getValue().state()), "ageSeconds", age, "timeoutMs",
							t.getValue().transactionTimeoutMs()));
				}
			}
		}
		return new ProducerScan(ids.size(), openPartitions.size(), openTransactions, hanging, longRunning,
				each.errors().size(), threshold);
	}

	// ---- evaluation (main thread) ----

	private static void partitionHealth(Diagnosis dx, Map<String, Object> facts, TopicDescription d,
			Topics.Reassignments reassignments, Integer minIsr, ClusterMeta.Info info, String mode,
			FeatureMetadata fm) {
		PartitionHealth h = PartitionHealth.of(List.of(d), reassignments,
				minIsr == null ? Map.of() : Map.of(d.name(), minIsr), info == null ? null : info.brokerIds());
		boolean kraft = ClusterMeta.KRAFT.equals(mode);
		if (!h.offline().isEmpty()) {
			String elrNote = kraft && !h.offlineWithLastKnownElr().isEmpty()
					? " KRaft: partitions with a non-empty lastKnownElr (" + partitionNumbers(h.offlineWithLastKnownElr())
							+ ") can elect a leader without data loss once those replicas return."
					: "";
			dx.critical("OFFLINE_PARTITIONS",
					h.offline().size() + " partition(s) have no leader (offline): they cannot be read or written",
					evidence("partitions", partitionNumbers(h.offline()), "omitted", omitted(h.offline()),
							"missingBrokers", h.missingBrokers().isEmpty() ? null : List.copyOf(h.missingBrokers())),
					"Bring up the brokers hosting the replicas (brokers_list, diagnose_cluster). On permanent loss "
							+ "unclean.leader.election.enable is the last resort (data loss)." + elrNote);
		}
		if (!h.underMinIsr().isEmpty()) {
			dx.critical("UNDER_MIN_ISR",
					h.underMinIsr().size() + " partition(s) have ISR below min.insync.replicas (" + minIsr
							+ "): acks=all producers get NotEnoughReplicas errors",
					evidence("partitions", partitionNumbers(h.underMinIsr()), "omitted", omitted(h.underMinIsr()),
							"minInsyncReplicas", minIsr),
					"Check the brokers of the lagging replicas (brokers_list, brokers_logdirs); lowering min.insync.replicas "
							+ "(topics_config_update) is a temporary fix and reduces durability");
		}
		List<TopicPartition> urpOnly = h.underReplicated().stream().filter(tp -> !h.underMinIsr().contains(tp)).toList();
		if (!urpOnly.isEmpty()) {
			dx.warning("UNDER_REPLICATED",
					urpOnly.size() + " partition(s) have fewer ISR members than replicas (under-replicated)",
					evidence("partitions", partitionNumbers(urpOnly), "omitted", omitted(urpOnly), "missingBrokers",
							h.missingBrokers().isEmpty() ? null : List.copyOf(h.missingBrokers())),
					"Check the health, disks and network of the replica brokers; if a reassignment is in progress, "
							+ "monitor it with partition_reassignments_list");
		}
		if (!h.nonPreferredLeader().isEmpty()) {
			dx.warning("LEADER_NOT_PREFERRED",
					h.nonPreferredLeader().size() + " partition(s) are not led by their preferred (first) replica: load imbalance",
					evidence("partitions", partitionNumbers(h.nonPreferredLeader()), "omitted",
							omitted(h.nonPreferredLeader())),
					"Move back to the preferred leaders with brokers_elect_preferred_leaders (auto.leader.rebalance.enable may be off)");
		}
		Boolean elrEnabled = null;
		if (fm != null) {
			FinalizedVersionRange elr = fm.finalizedFeatures().get("eligible.leader.replicas.version");
			elrEnabled = elr != null && elr.maxVersionLevel() > 0;
		}
		if (kraft && !h.withElr().isEmpty()) {
			dx.info("ELR_ACTIVE",
					h.withElr().size() + " partition(s) have a shrunk ISR; replicas in the ELR (eligible leader replicas) can "
							+ "become leader without data loss",
					evidence("partitions", partitionNumbers(h.withElr()), "omitted", omitted(h.withElr())),
					"Check the brokers of the replicas that dropped out of the ISR");
		}
		Map<String, Object> health = Results.map("partitions", h.partitions(), "offline", h.offline().size(),
				"underReplicated", h.underReplicated().size(), "underMinIsr", h.underMinIsr().size(),
				"nonPreferredLeader", h.nonPreferredLeader().size());
		if (kraft) {
			health.put("elrPartitions", h.withElr().size());
			if (elrEnabled != null) {
				health.put("elrEnabled", elrEnabled);
			}
		}
		else if (ClusterMeta.ZOOKEEPER.equals(mode)) {
			health.put("elr", "No ELR in ZooKeeper mode");
		}
		if (reassignments != null && !reassignments.byPartition().isEmpty()) {
			long reassigning = reassignments.byPartition().keySet().stream()
				.filter(tp -> tp.topic().equals(d.name()))
				.count();
			if (reassigning > 0) {
				health.put("reassigningPartitions", reassigning);
			}
		}
		facts.put("health", health);
	}

	private static void configSanity(Diagnosis dx, Map<String, Object> facts, Config cfg, int rf, Integer minIsr,
			ClusterMeta.Info info, TopicDescription d) {
		Map<String, Object> config = Results.map("replicationFactor", rf, "partitions", d.partitions().size(),
				"internal", d.isInternal() ? true : null);
		if (cfg == null) {
			facts.put("config", config);
			return;
		}
		String unclean = value(cfg, "unclean.leader.election.enable");
		String cleanup = value(cfg, "cleanup.policy");
		Long retentionMs = longValue(cfg, "retention.ms");
		Long retentionBytes = longValue(cfg, "retention.bytes");
		config.putAll(Results.map("min.insync.replicas", minIsr, "unclean.leader.election.enable", unclean,
				"cleanup.policy", cleanup, "retention.ms", retentionMs, "retention.bytes", retentionBytes));
		List<String> nonDefault = new ArrayList<>();
		for (ConfigEntry e : Configs.sorted(cfg)) {
			if (!Configs.isDefault(e)) {
				nonDefault.add(e.name() + "=" + Configs.value(e));
			}
		}
		if (!nonDefault.isEmpty()) {
			config.putAll(Results.map("nonDefault", first(nonDefault, MAX_NON_DEFAULT), "nonDefaultOmitted",
					omitted(nonDefault, MAX_NON_DEFAULT)));
		}
		facts.put("config", config);
		int brokers = info == null ? -1 : info.brokers().size();
		if (minIsr != null && minIsr > rf) {
			dx.critical("MIN_ISR_EXCEEDS_RF",
					"min.insync.replicas (" + minIsr + ") is greater than the replication factor (" + rf
							+ "): no acks=all write is accepted (NotEnoughReplicas)",
					evidence("minInsyncReplicas", minIsr, "replicationFactor", rf, "source",
							Configs.source(cfg.get("min.insync.replicas"))),
					"Set min.insync.replicas=" + Math.max(1, rf - 1)
							+ " with topics_config_update or increase the replication factor (partition_reassignments_alter)");
		}
		else if (minIsr != null && rf > 1 && minIsr == rf) {
			dx.warning("MIN_ISR_EQUALS_RF",
					"min.insync.replicas equals the replication factor (" + rf
							+ "): acks=all writes stop when a single replica/broker is lost",
					evidence("minInsyncReplicas", minIsr, "replicationFactor", rf),
					"min.insync.replicas = RF-1 (" + (rf - 1) + ") is recommended (topics_config_update)");
		}
		if (rf == 1 && !d.isInternal()) {
			if (brokers > 1) {
				dx.warning("NO_REDUNDANCY",
						"replication factor 1: this topic's data becomes unavailable or is lost when a broker is lost",
						evidence("replicationFactor", rf, "brokers", brokers),
						"RF=3 and min.insync.replicas=2 are recommended for production topics (partition_reassignments_alter)");
			}
			else {
				dx.info("NO_REDUNDANCY", "replication factor 1 (the cluster has a single broker): no redundancy",
						evidence("replicationFactor", rf, "brokers", brokers), null);
			}
		}
		if ("true".equalsIgnoreCase(unclean)) {
			dx.warning("UNCLEAN_LEADER_ELECTION",
					"unclean.leader.election.enable=true: an out-of-sync replica can become leader and committed data can be lost",
					evidence("source", Configs.source(cfg.get("unclean.leader.election.enable"))),
					"If data loss is unacceptable, set unclean.leader.election.enable=false with topics_config_update");
		}
		boolean delete = cleanup != null && cleanup.toLowerCase(Locale.ROOT).contains("delete");
		if (delete && retentionMs != null && retentionMs < 0 && retentionBytes != null && retentionBytes < 0) {
			dx.info("RETENTION_UNLIMITED",
					"cleanup.policy=" + cleanup + " but retention.ms and retention.bytes are unlimited (-1): disk usage grows without bound",
					evidence("retention.ms", retentionMs, "retention.bytes", retentionBytes),
					"If this is not intended, set retention.ms or retention.bytes with topics_config_update");
		}
		if (delete && retentionMs != null && retentionMs > 0 && retentionMs < SHORT_RETENTION_MS) {
			dx.info("RETENTION_SHORT",
					"retention.ms is short (" + retentionMs / 1000 + " s): data may be deleted before lagging consumers read it",
					evidence("retention.ms", retentionMs), "Evaluate together with the consumer lag");
		}
	}

	private static void sizes(Diagnosis dx, Map<String, Object> facts, Sizes s, OffsetsInfo oi, TopicDescription d) {
		Map<String, Object> size = new LinkedHashMap<>();
		if (s != null) {
			long leader = 0;
			long total = 0;
			long max = -1;
			TopicPartition largest = null;
			for (Map.Entry<TopicPartition, LogDirs.PartitionSize> e : s.sizes().entrySet()) {
				leader += e.getValue().leaderBytes();
				total += e.getValue().totalBytes();
				if (e.getValue().leaderBytes() > max) {
					max = e.getValue().leaderBytes();
					largest = e.getKey();
				}
			}
			size.put("sizeBytes", leader);
			size.put("size", Format.bytes(leader));
			size.put("totalSizeBytes", total);
			if (!s.failedBrokers().isEmpty()) {
				size.put("failedBrokers", s.failedBrokers());
			}
			int n = s.sizes().size();
			if (largest != null && n > 1) {
				double avg = (double) leader / n;
				double ratio = avg == 0 ? 0 : max / avg;
				size.put("largestPartition", Results.map("partition", largest.partition(), "sizeBytes", max));
				size.put("maxToAvgRatio", Format.round(ratio, 2));
				if (ratio >= 2 && max >= SKEW_MIN_BYTES) {
					dx.warning("PARTITION_SKEW",
							"Partition sizes are skewed: the largest partition (" + largest.partition() + ", "
									+ Format.bytes(max) + ") is " + Format.round(ratio, 1) + "x the average",
							evidence("basis", "bytes", "largestPartition", largest.partition(), "maxBytes", max,
									"avgBytes", Math.round(avg)),
							"Check the key distribution (hot keys) and any custom partitioner; change the key design if needed");
				}
			}
		}
		if (oi != null && !dx.has("PARTITION_SKEW") && oi.ranges().size() > 1) {
			long totalCount = 0;
			long maxCount = -1;
			TopicPartition largest = null;
			for (Map.Entry<TopicPartition, Offsets.Range> e : oi.ranges().entrySet()) {
				totalCount += e.getValue().count();
				if (e.getValue().count() > maxCount) {
					maxCount = e.getValue().count();
					largest = e.getKey();
				}
			}
			double avg = (double) totalCount / oi.ranges().size();
			if (avg > 0 && maxCount / avg >= 2 && maxCount >= SKEW_MIN_MESSAGES) {
				dx.warning("PARTITION_SKEW",
						"Partition message counts are skewed: partition " + largest.partition() + " holds "
								+ Format.round(maxCount / avg, 1) + "x the average number of messages",
						evidence("basis", "messages", "largestPartition", largest.partition(), "maxMessages", maxCount,
								"avgMessages", Math.round(avg)),
						"Check the key distribution (hot keys) and any custom partitioner");
			}
		}
		if (!size.isEmpty()) {
			facts.put("size", size);
		}
	}

	private static void activity(Diagnosis dx, Map<String, Object> facts, OffsetsInfo oi, TopicDescription d) {
		if (oi == null) {
			return;
		}
		long messages = 0;
		int empty = 0;
		for (Offsets.Range r : oi.ranges().values()) {
			messages += r.count();
			empty += r.count() == 0 ? 1 : 0;
		}
		long lastWrite = oi.lastWrite().values().stream().mapToLong(Long::longValue).max().orElse(-1);
		int noOffsets = d.partitions().size() - oi.ranges().size();
		facts.put("data", Results.map("approxMessages", messages, "emptyPartitions", empty, "partitionsWithoutOffsets",
				noOffsets > 0 ? noOffsets : null, "lastWriteTime", Format.iso(lastWrite), "lastWriteAgeSeconds",
				Format.ageSeconds(lastWrite)));
		if (messages == 0 && noOffsets == 0) {
			dx.info("TOPIC_EMPTY", "Topic has no messages (empty, or everything was deleted by retention)", null, null);
		}
		else if (lastWrite >= 0 && Format.ageSeconds(lastWrite) > IDLE_SECONDS) {
			dx.info("TOPIC_IDLE", "Topic has not been written to for " + Format.ageSeconds(lastWrite) / 86_400 + " days",
					evidence("lastWriteTime", Format.iso(lastWrite)),
					"If unused, consider deleting it after checking with its owner; did the producers move to another topic?");
		}
	}

	static void consumers(Diagnosis dx, Map<String, Object> facts, GroupScan gs, OffsetsInfo oi,
			TopicDescription d) {
		if (gs == null) {
			return;
		}
		OffsetWindow w = gs.window();
		Map<TopicPartition, Long> delta = w.endDelta();
		long produced = delta.values().stream().mapToLong(Long::longValue).sum();
		Map<String, Object> rate = Results.map("msgsPerSec", w.rate(produced), "messages", w.complete() ? produced : null);
		rate.putAll(w.describe());
		facts.put("writeRate", rate);

		record Row(String groupId, String state, Integer members, OffsetWindow.GroupProgress p, StuckCheck.Verdict v) {

			int stalled() {
				return v.stuck().size() + v.suspect().size();
			}

		}
		List<Row> rows = new ArrayList<>();
		for (String g : w.groupsWithCommits()) {
			Settled<ConsumerGroupDescription> s = gs.described().get(g);
			ConsumerGroupDescription desc = s != null && s.ok() ? s.value() : null;
			rows.add(new Row(g, desc == null ? null : ConsumerGroups.state(desc.groupState()),
					desc == null ? null : desc.members().size(), w.progress(g), gs.stuck().verdict(g)));
		}
		rows.sort(Comparator.comparingInt((Row r) -> -r.v().stuck().size())
			.thenComparingInt(r -> -r.stalled())
			.thenComparing(Comparator.comparingLong((Row r) -> r.p().lag()).reversed())
			.thenComparing(Row::groupId));
		List<Map<String, Object>> stuckActive = new ArrayList<>();
		List<Map<String, Object>> stuckIdle = new ArrayList<>();
		List<Map<String, Object>> suspect = new ArrayList<>();
		List<Map<String, Object>> periodic = new ArrayList<>();
		List<Map<String, Object>> growing = new ArrayList<>();
		List<Map<String, Object>> high = new ArrayList<>();
		List<Map<String, Object>> beforeStart = new ArrayList<>();
		long totalLag = 0;
		for (Row r : rows) {
			OffsetWindow.GroupProgress p = r.p();
			StuckCheck.Verdict v = r.v();
			totalLag += p.lag();
			Map<String, Object> ev = Results.map("groupId", r.groupId(), "state", r.state(), "members", r.members(),
					"lag", p.lag());
			boolean active = r.members() != null && r.members() > 0;
			if (!v.stuck().isEmpty()) {
				ev.put("stuckPartitions", partitionNumbers(v.stuck().stream().map(StuckCheck.Assessed::partition).toList()));
				ev.put("oldestPendingAgeSeconds", v.maxBehindSeconds(StuckCheck.Level.STUCK));
				(active ? stuckActive : stuckIdle).add(ev);
			}
			else if (!v.suspect().isEmpty()) {
				ev.put("stuckSuspectPartitions",
						partitionNumbers(v.suspect().stream().map(StuckCheck.Assessed::partition).toList()));
				ev.put("oldestPendingAgeSeconds", v.maxBehindSeconds(StuckCheck.Level.SUSPECT));
				(active ? suspect : stuckIdle).add(ev);
			}
			else if ("growing".equals(p.trend()) && p.lag() >= GROWING_LAG_MIN && p.lagBeyondCommitAllowance()) {
				ev.put("consumeRate", p.consumeRate());
				ev.put("produceRate", p.produceRate());
				growing.add(ev);
			}
			else if (p.lag() >= HIGH_LAG) {
				high.add(ev);
			}
			if (v.stuck().isEmpty() && v.suspect().isEmpty() && !v.periodic().isEmpty()) {
				periodic.add(Results.map("groupId", r.groupId(), "partitions",
						partitionNumbers(v.periodic().stream().map(StuckCheck.Assessed::partition).toList()),
						"oldestPendingAgeSeconds", v.maxBehindSeconds(StuckCheck.Level.PERIODIC)));
			}
			if (oi != null) {
				List<Integer> lost = new ArrayList<>();
				for (OffsetWindow.PartitionProgress pp : p.partitions()) {
					Offsets.Range range = oi.ranges().get(pp.partition());
					if (range != null && pp.committed0() != null && pp.committed0() < range.earliest()) {
						lost.add(pp.partition().partition());
					}
				}
				if (!lost.isEmpty()) {
					beforeStart.add(Results.map("groupId", r.groupId(), "state", r.state(), "partitions",
							first(lost, MAX_EVIDENCE), "partitionsOmitted", omitted(lost)));
				}
			}
		}
		Map<String, Object> window = Results.map("windowSeconds", Format.round(w.elapsedSeconds(), 1),
				"commitAllowanceSeconds", StuckCheck.COMMIT_ALLOWANCE_SECONDS, "stuckAfterSeconds",
				StuckCheck.STUCK_SECONDS);
		if (!stuckActive.isEmpty()) {
			Map<String, Object> ev = evidence("groups", first(stuckActive, MAX_EVIDENCE), "omitted", omitted(stuckActive));
			ev.putAll(window);
			dx.critical("CONSUMER_STUCK",
					stuckActive.size() + " active consumer group(s) are not progressing on this topic: the commit is unchanged, new "
							+ "messages arrive and the oldest pending record was written " + StuckCheck.STUCK_SECONDS / 60
							+ "+ min ago",
					ev, "Inspect the consumer logs of these groups (poison pill, long processing, max.poll.interval.ms, commit "
							+ "errors); use diagnose_consumer_group for details");
		}
		if (!stuckIdle.isEmpty()) {
			Map<String, Object> ev = evidence("groups", first(stuckIdle, MAX_EVIDENCE), "omitted", omitted(stuckIdle));
			ev.putAll(window);
			dx.warning("CONSUMER_STUCK",
					stuckIdle.size() + " consumer group(s) have no members and are falling behind: the commit does not advance, new messages arrive",
					ev, "Start the consumer application; if the group is no longer used, consumer_groups_delete or (while the group "
							+ "is EMPTY) consumer_groups_offsets_reset");
		}
		if (!suspect.isEmpty()) {
			Map<String, Object> ev = evidence("groups", first(suspect, MAX_EVIDENCE), "omitted", omitted(suspect));
			ev.putAll(window);
			dx.warning("CONSUMER_STUCK_SUSPECT",
					suspect.size() + " active consumer group(s) did not advance their commit in the window and the oldest pending "
							+ "record is older than the periodic commit allowance (" + StuckCheck.COMMIT_ALLOWANCE_SECONDS
							+ " s)",
					ev, "Check again with diagnose_consumer_group in a few minutes; if it does not progress, inspect the consumer "
							+ "logs");
		}
		if (!periodic.isEmpty()) {
			Map<String, Object> ev = evidence("groups", first(periodic, MAX_EVIDENCE), "omitted", omitted(periodic));
			ev.putAll(window);
			dx.info("COMMIT_NOT_ADVANCED",
					periodic.size() + " group(s) did not advance their commit in the window but the pending records are recent: "
							+ "periodic commit (auto commit 5 s, Streams 30 s, Connect 60 s) likely, no evidence of being stuck",
					ev, null);
		}
		if (!growing.isEmpty()) {
			dx.warning("CONSUMER_LAG_GROWING",
					growing.size() + " consumer group(s) have a growing lag: consumption is slower than production",
					evidence("groups", first(growing, MAX_EVIDENCE), "omitted", omitted(growing)),
					"Increase the consumers up to the partition count (" + d.partitions().size()
							+ ") or speed up processing; inspect with diagnose_consumer_group");
		}
		if (!high.isEmpty()) {
			dx.info("CONSUMER_LAG_HIGH", high.size() + " consumer group(s) have a lag of " + HIGH_LAG + "+ (not growing)",
					evidence("groups", first(high, MAX_EVIDENCE), "omitted", omitted(high)),
					"Monitor the trend with consumer_groups_lag_trend");
		}
		if (!beforeStart.isEmpty()) {
			dx.warning("COMMITTED_BEFORE_LOG_START",
					beforeStart.size()
							+ " group(s) have a commit older than the log start: messages were deleted before being read (data loss)",
					evidence("groups", first(beforeStart, MAX_EVIDENCE), "omitted", omitted(beforeStart)),
					"auto.offset.reset applies when the group runs; consider increasing retention.ms or speeding up "
							+ "consumption");
		}
		if (!w.groupErrors().isEmpty()) {
			List<String> failed = List.copyOf(w.groupErrors().keySet());
			dx.warning("GROUP_OFFSETS_UNAVAILABLE",
					"Could not read the committed offsets of " + failed.size() + " group(s): groups consuming this topic may be missing",
					evidence("groups", first(failed, MAX_EVIDENCE), "omitted", omitted(failed), "firstError",
							w.groupErrors().values().iterator().next()),
					"Check the health of the coordinators (__consumer_offsets) and the GROUP DESCRIBE permission; "
							+ "run the tool again");
		}
		long messages = oi == null ? -1 : oi.ranges().values().stream().mapToLong(Offsets.Range::count).sum();
		if (rows.isEmpty() && messages != 0 && gs.listErrors().isEmpty() && w.groupErrors().isEmpty()) {
			dx.info("NO_CONSUMER_GROUPS",
					"No consumer group commits on this topic (clients reading with assign() or not committing are not visible)",
					evidence("groupsScanned", gs.listed()), null);
		}
		if (!gs.listErrors().isEmpty() || !w.groupErrors().isEmpty()) {
			dx.markPartial();
		}
		List<Map<String, Object>> top = new ArrayList<>();
		for (Row r : rows.subList(0, Math.min(MAX_EVIDENCE, rows.size()))) {
			Map<String, Object> m = Results.map("groupId", r.groupId(), "state", r.state(), "members", r.members());
			m.putAll(r.p().summary());
			if (r.p().complete()) {
				m.put("stuckPartitions", r.v().stuck().size());
				if (!r.v().suspect().isEmpty()) {
					m.put("stuckSuspectPartitions", r.v().suspect().size());
				}
			}
			top.add(m);
		}
		facts.put("consumers", Results.map("groupsScanned", gs.listed(), "groupsOnTopic", rows.size(), "totalLag",
				totalLag, "groups", top, "groupsOmitted", omitted(rows), "listErrors",
				gs.listErrors().isEmpty() ? null : gs.listErrors(), "groupErrors",
				w.groupErrors().isEmpty() ? null : w.groupErrors().size()));
	}

	private static void producerFindings(Diagnosis dx, Map<String, Object> facts, ProducerScan p) {
		if (p == null) {
			return;
		}
		facts.put("producers", Results.map("activeProducers", p.activeProducers(), "partitionsWithOpenTransaction",
				p.partitionsWithOpenTxn(), "openTransactions", p.openTransactions(), "hangingSuspects",
				p.hanging().size(), "failedPartitions", p.failedPartitions() > 0 ? p.failedPartitions() : null));
		if (!p.hanging().isEmpty()) {
			dx.critical("HANGING_TRANSACTION",
					p.hanging().size() + " partition(s) have a suspected hanging transaction: the LSO does not advance, "
							+ "read_committed consumers get stuck",
					evidence("partitions", first(p.hanging(), MAX_EVIDENCE), "omitted", omitted(p.hanging()), "thresholdMs",
							p.hangingThresholdMs()),
					"Verify with topics_transactions; close it with kafka-transactions.sh find-hanging / abort");
		}
		if (!p.longRunning().isEmpty()) {
			dx.warning("LONG_RUNNING_TRANSACTION",
					p.longRunning().size() + " open transaction(s) have been open longer than their own timeout",
					evidence("transactions", first(p.longRunning(), MAX_EVIDENCE), "omitted", omitted(p.longRunning())),
					"Check the producer application (commit/abort may not be called); topics_transactions");
		}
	}

	private static void connectorFindings(Diagnosis dx, Map<String, Object> facts, ConnectorRefs.Result r) {
		if (r == null) {
			return;
		}
		facts.put("connectors", Results.map("scanned", r.scanned(), "referencing",
				r.refs().stream().map(ConnectorRefs.Ref::toMap).toList(), "activeTopicsUnchecked",
				r.activeUnchecked() > 0 ? r.activeUnchecked() : null));
		List<Map<String, Object>> failed = new ArrayList<>();
		for (ConnectorRefs.Ref ref : r.refs()) {
			if (ref.status().hasFailure()) {
				failed.add(Results.map("name", ref.name(), "state", ref.status().state(), "failedTasks",
						ref.status().failedTaskIds(), "traceSummary", ConnectJson.traceSummary(ref.status().firstTrace())));
			}
		}
		if (!failed.isEmpty()) {
			dx.warning("CONNECTOR_FAILED", failed.size() + " connector(s) using this topic are in a failed state",
					evidence("connectors", first(failed, MAX_EVIDENCE), "omitted", omitted(failed)),
					"Restart with connectors_restart (includeTasks=true, onlyFailed=true); if it fails again, "
							+ "look for config/authorization problems with diagnose_connector");
		}
	}

	private static void accessFindings(Diagnosis dx, Map<String, Object> facts, AclView v, String topic) {
		if (v == null) {
			return;
		}
		if (!v.authorizerEnabled()) {
			facts.put("access", Results.map("authorizer", false));
			dx.info("NO_AUTHORIZER", "No authorizer on the cluster: every authenticated client can read and write this topic",
					null, "If access must be restricted, set authorizer.class.name on the brokers");
			return;
		}
		List<String> readers = v.allowedPrincipals(AclOperation.READ, ResourceType.TOPIC, topic);
		List<String> writers = v.allowedPrincipals(AclOperation.WRITE, ResourceType.TOPIC, topic);
		List<String> deniedRead = v.deniedPrincipals(AclOperation.READ, ResourceType.TOPIC, topic);
		List<String> deniedWrite = v.deniedPrincipals(AclOperation.WRITE, ResourceType.TOPIC, topic);
		int acls = v.resourceAclCount(ResourceType.TOPIC, topic);
		facts.put("access", Results.map("authorizer", true, "authorizerClass", v.authorizerClass(), "aclCount", acls,
				"readers", first(readers, MAX_PRINCIPALS), "readersOmitted", omitted(readers, MAX_PRINCIPALS), "writers",
				first(writers, MAX_PRINCIPALS), "writersOmitted", omitted(writers, MAX_PRINCIPALS), "deniedRead",
				deniedRead.isEmpty() ? null : first(deniedRead, MAX_PRINCIPALS), "deniedReadOmitted",
				omitted(deniedRead, MAX_PRINCIPALS), "deniedWrite",
				deniedWrite.isEmpty() ? null : first(deniedWrite, MAX_PRINCIPALS), "deniedWriteOmitted",
				omitted(deniedWrite, MAX_PRINCIPALS), "superUsers",
				v.superUsers().known() ? List.copyOf(v.superUsers().users()) : "unknown (hidden by the broker)",
				"allowEveryoneIfNoAclFound", v.allowEveryone().value(), "note",
				"Super users have access regardless of ACLs; readers/writers are the principals with ACLs"));
		if (acls == 0) {
			dx.info("NO_TOPIC_ACLS",
					v.allowEveryone().value() ? "Topic has no ACLs and allow.everyone.if.no.acl.found=true: everyone has access"
							: "Topic has no ACLs: only super users have access",
					null, "Use acls_create (preset producer/consumer) for the application principals");
		}
		if (readers.contains("User:*") || writers.contains("User:*")) {
			dx.info("PUBLIC_TOPIC_ACCESS", "User:* (everyone) has access to this topic",
					evidence("read", readers.contains("User:*"), "write", writers.contains("User:*")),
					"If this is not intended, remove the User:* binding with acls_delete");
		}
	}

	private static String value(Config c, String key) {
		ConfigEntry e = c == null ? null : c.get(key);
		return e == null ? null : Configs.value(e);
	}

	private static Integer intValue(Config c, String key) {
		Long v = longValue(c, key);
		return v == null ? null : v.intValue();
	}

	private static Long longValue(Config c, String key) {
		String v = value(c, key);
		if (v == null) {
			return null;
		}
		try {
			return Long.parseLong(v.trim());
		}
		catch (NumberFormatException e) {
			return null;
		}
	}

}
