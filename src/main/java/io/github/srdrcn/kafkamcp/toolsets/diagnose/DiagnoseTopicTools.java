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

/** diagnose_topic: tek çağrıda topic sağlık teşhisi. */
@Component
public class DiagnoseTopicTools {

	/** Bu lag'in üstü (büyümüyor/takılı değilse de) bilgi bulgusu. */
	static final long HIGH_LAG = 10_000;

	/** Lag büyüyor uyarısı için en az lag. */
	static final long GROWING_LAG_MIN = 100;

	/** Son yazma bundan eskiyse topic boşta sayılır. */
	static final long IDLE_SECONDS = 7L * 24 * 3600;

	static final long SKEW_MIN_BYTES = 64L * 1024 * 1024;

	static final long SKEW_MIN_MESSAGES = 100_000;

	static final long SHORT_RETENTION_MS = 3_600_000;

	private static final long LIST_GROUPS_MAX_MS = 3_000;

	private static final long WINDOW_RESERVE_MS = 2_500;

	private static final long DESCRIBE_GROUPS_RESERVE_MS = 1_500;

	private static final long CONNECTOR_CHECK_MS = 5_000;

	private static final long LAST_RECORD_BUDGET_MS = 2_000;

	private final ConnectApi connect;

	public DiagnoseTopicTools(ConnectApi connect) {
		this.connect = connect;
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("diagnose_topic",
					"Topic sağlık teşhisi: partition/ISR, config, boyut/dağılım, yazma, consumer lag/takılma, transaction, connector, ACL")
			.required("topic", P.string("Topic adı"))
			.param("intervalSeconds", DiagnoseParams.interval())
			.handler(this::diagnose));
	}

	record Sizes(Map<TopicPartition, LogDirs.PartitionSize> sizes, Map<Integer, String> failedBrokers) {
	}

	record OffsetsInfo(Map<TopicPartition, Offsets.Range> ranges, Map<TopicPartition, Long> lastWrite) {
	}

	record GroupScan(int listed, List<String> listErrors, OffsetWindow window,
			Map<String, Settled<ConsumerGroupDescription>> described) {
	}

	record ProducerScan(int activeProducers, int partitionsWithOpenTxn, int openTransactions,
			List<Map<String, Object>> hanging, List<Map<String, Object>> longRunning, int failedPartitions,
			Long hangingThresholdMs) {
	}

	Object diagnose(ToolCall call) {
		String topic = call.str("topic");
		long intervalMs = DiagnoseParams.intervalMs(call);
		TopicDescription d = Topics.describeOne(call, topic);
		List<TopicPartition> online = Topics.onlinePartitions(List.of(d));
		Diagnosis dx = new Diagnosis();
		Map<String, Object> facts = new LinkedHashMap<>();
		String mode;
		try (Checks checks = new Checks(call, dx)) {
			Check<ClusterMeta.Info> meta = checks.start("cluster metadata", () -> ClusterMeta.describe(call, true));
			Check<FeatureMetadata> features = checks.start("feature'lar",
					() -> Futures.awaitAtMost(call, call.admin().describeFeatures().featureMetadata(), 4_000));
			Check<Config> config = checks.start("topic config", () -> Configs.describeOne(call, Configs.topic(topic)));
			Check<Topics.Reassignments> reassign = checks.start("reassignment'lar",
					() -> Topics.reassignments(call, Topics.submitReassignments(call)));
			Check<Sizes> sizes = checks.start("boyut (log dir)", () -> {
				Futures.Each<Integer, Map<String, LogDirDescription>> dirs = LogDirs.describe(call,
						LogDirs.brokersHosting(List.of(d)));
				return new Sizes(LogDirs.partitionSizes(dirs.values(), Map.of(topic, d)), dirs.errors());
			});
			Check<OffsetsInfo> offsets = checks.start("offset'ler ve son yazma", () -> offsetsInfo(call, online));
			Check<GroupScan> groups = checks.start("consumer group'lar ve yazma hızı",
					() -> groupScan(call, online, intervalMs));
			Check<ProducerScan> producers = checks.start("producer/transaction", () -> producers(call, online, topic));
			Check<ConnectorRefs.Result> connectors = call.clusterConfig().hasConnect()
					? checks.start("connector'lar", () -> ConnectorRefs.find(connect, call, topic, CONNECTOR_CHECK_MS))
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

	// ---- alt kontroller (sanal thread'lerde) ----

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
		Map<String, Settled<ConsumerGroupDescription>> described = withCommits.isEmpty() ? Map.of()
				: ConsumerGroups.describe(call, withCommits, DESCRIBE_GROUPS_RESERVE_MS);
		return new GroupScan(listed, listErrors, w, described);
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

	// ---- değerlendirme (ana thread) ----

	private static void partitionHealth(Diagnosis dx, Map<String, Object> facts, TopicDescription d,
			Topics.Reassignments reassignments, Integer minIsr, ClusterMeta.Info info, String mode,
			FeatureMetadata fm) {
		PartitionHealth h = PartitionHealth.of(List.of(d), reassignments,
				minIsr == null ? Map.of() : Map.of(d.name(), minIsr), info == null ? null : info.brokerIds());
		boolean kraft = ClusterMeta.KRAFT.equals(mode);
		if (!h.offline().isEmpty()) {
			String elrNote = kraft && !h.offlineWithLastKnownElr().isEmpty()
					? " KRaft: lastKnownElr'si dolu partition'lar (" + partitionNumbers(h.offlineWithLastKnownElr())
							+ ") o replikalar dönünce veri kaybı olmadan lider seçebilir."
					: "";
			dx.critical("OFFLINE_PARTITIONS",
					h.offline().size() + " partition'ın lideri yok (offline): bu partition'lara okuma/yazma yapılamaz",
					evidence("partitions", partitionNumbers(h.offline()), "omitted", omitted(h.offline()),
							"missingBrokers", h.missingBrokers().isEmpty() ? null : List.copyOf(h.missingBrokers())),
					"Replikaları barındıran broker'ları ayağa kaldırın (brokers_list, diagnose_cluster). Kalıcı kayıpta "
							+ "unclean.leader.election.enable son çaredir (veri kaybı)." + elrNote);
		}
		if (!h.underMinIsr().isEmpty()) {
			dx.critical("UNDER_MIN_ISR",
					h.underMinIsr().size() + " partition'da ISR min.insync.replicas (" + minIsr
							+ ") altında: acks=all producer'lar NotEnoughReplicas hatası alır",
					evidence("partitions", partitionNumbers(h.underMinIsr()), "omitted", omitted(h.underMinIsr()),
							"minInsyncReplicas", minIsr),
					"Geride kalan replikaların broker'larını kontrol edin (brokers_list, brokers_logdirs); min.insync.replicas'ı "
							+ "düşürmek (topics_config_update) geçici çözümdür ve dayanıklılığı azaltır");
		}
		List<TopicPartition> urpOnly = h.underReplicated().stream().filter(tp -> !h.underMinIsr().contains(tp)).toList();
		if (!urpOnly.isEmpty()) {
			dx.warning("UNDER_REPLICATED",
					urpOnly.size() + " partition'da ISR replika sayısından az (under-replicated)",
					evidence("partitions", partitionNumbers(urpOnly), "omitted", omitted(urpOnly), "missingBrokers",
							h.missingBrokers().isEmpty() ? null : List.copyOf(h.missingBrokers())),
					"Replika broker'larının sağlığını, disk ve ağ durumunu kontrol edin; reassignment sürüyorsa "
							+ "partition_reassignments_list ile izleyin");
		}
		if (!h.nonPreferredLeader().isEmpty()) {
			dx.warning("LEADER_NOT_PREFERRED",
					h.nonPreferredLeader().size() + " partition'ın lideri tercih edilen (ilk) replika değil: yük dengesizliği",
					evidence("partitions", partitionNumbers(h.nonPreferredLeader()), "omitted",
							omitted(h.nonPreferredLeader())),
					"brokers_elect_preferred_leaders ile tercih edilen liderlere dönün (auto.leader.rebalance.enable kapalı olabilir)");
		}
		Boolean elrEnabled = null;
		if (fm != null) {
			FinalizedVersionRange elr = fm.finalizedFeatures().get("eligible.leader.replicas.version");
			elrEnabled = elr != null && elr.maxVersionLevel() > 0;
		}
		if (kraft && !h.withElr().isEmpty()) {
			dx.info("ELR_ACTIVE",
					h.withElr().size() + " partition'da ISR daralmış; ELR'deki (eligible leader replicas) replikalar veri "
							+ "kaybı olmadan lider olabilir",
					evidence("partitions", partitionNumbers(h.withElr()), "omitted", omitted(h.withElr())),
					"ISR dışına düşen replikaların broker'larını kontrol edin");
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
			health.put("elr", "ZooKeeper modunda ELR yok");
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
			config.put("nonDefault", first(nonDefault, 15));
		}
		facts.put("config", config);
		int brokers = info == null ? -1 : info.brokers().size();
		if (minIsr != null && minIsr > rf) {
			dx.critical("MIN_ISR_EXCEEDS_RF",
					"min.insync.replicas (" + minIsr + ") replication factor'dan (" + rf
							+ ") büyük: acks=all ile hiçbir yazma kabul edilmez (NotEnoughReplicas)",
					evidence("minInsyncReplicas", minIsr, "replicationFactor", rf, "source",
							Configs.source(cfg.get("min.insync.replicas"))),
					"topics_config_update ile min.insync.replicas=" + Math.max(1, rf - 1)
							+ " yapın ya da replication factor'ı artırın (partition_reassignments_alter)");
		}
		else if (minIsr != null && rf > 1 && minIsr == rf) {
			dx.warning("MIN_ISR_EQUALS_RF",
					"min.insync.replicas replication factor'a eşit (" + rf
							+ "): tek replika/broker kaybında acks=all yazmalar durur",
					evidence("minInsyncReplicas", minIsr, "replicationFactor", rf),
					"min.insync.replicas = RF-1 (" + (rf - 1) + ") önerilir (topics_config_update)");
		}
		if (rf == 1 && !d.isInternal()) {
			if (brokers > 1) {
				dx.warning("NO_REDUNDANCY",
						"replication factor 1: broker kaybında bu topic'in verisi erişilemez ya da kaybolur",
						evidence("replicationFactor", rf, "brokers", brokers),
						"Üretim topic'leri için RF=3 ve min.insync.replicas=2 önerilir (partition_reassignments_alter)");
			}
			else {
				dx.info("NO_REDUNDANCY", "replication factor 1 (cluster'da tek broker var): yedeklilik yok",
						evidence("replicationFactor", rf, "brokers", brokers), null);
			}
		}
		if ("true".equalsIgnoreCase(unclean)) {
			dx.warning("UNCLEAN_LEADER_ELECTION",
					"unclean.leader.election.enable=true: ISR dışı replika lider olabilir, commit edilmiş veri kaybolabilir",
					evidence("source", Configs.source(cfg.get("unclean.leader.election.enable"))),
					"Veri kaybı kabul edilemiyorsa topics_config_update ile unclean.leader.election.enable=false yapın");
		}
		boolean delete = cleanup != null && cleanup.toLowerCase(Locale.ROOT).contains("delete");
		if (delete && retentionMs != null && retentionMs < 0 && retentionBytes != null && retentionBytes < 0) {
			dx.info("RETENTION_UNLIMITED",
					"cleanup.policy=" + cleanup + " ama retention.ms ve retention.bytes sınırsız (-1): disk sınırsız büyür",
					evidence("retention.ms", retentionMs, "retention.bytes", retentionBytes),
					"Bilinçli değilse topics_config_update ile retention.ms ya da retention.bytes verin");
		}
		if (delete && retentionMs != null && retentionMs > 0 && retentionMs < SHORT_RETENTION_MS) {
			dx.info("RETENTION_SHORT",
					"retention.ms kısa (" + retentionMs / 1000 + " sn): geride kalan consumer'lar okumadan veri silinebilir",
					evidence("retention.ms", retentionMs), "Consumer lag'i ile birlikte değerlendirin");
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
							"Partition boyutları dengesiz: en büyük partition (" + largest.partition() + ", "
									+ Format.bytes(max) + ") ortalamanın " + Format.round(ratio, 1) + " katı",
							evidence("basis", "bytes", "largestPartition", largest.partition(), "maxBytes", max,
									"avgBytes", Math.round(avg)),
							"Anahtar dağılımını (hot key) ve özel partitioner'ı kontrol edin; gerekirse anahtar tasarımını değiştirin");
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
						"Partition mesaj sayıları dengesiz: partition " + largest.partition() + " ortalamanın "
								+ Format.round(maxCount / avg, 1) + " katı mesaj içeriyor",
						evidence("basis", "messages", "largestPartition", largest.partition(), "maxMessages", maxCount,
								"avgMessages", Math.round(avg)),
						"Anahtar dağılımını (hot key) ve özel partitioner'ı kontrol edin");
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
			dx.info("TOPIC_EMPTY", "Topic'te mesaj yok (boş ya da tümü retention ile silinmiş)", null, null);
		}
		else if (lastWrite >= 0 && Format.ageSeconds(lastWrite) > IDLE_SECONDS) {
			dx.info("TOPIC_IDLE", "Topic'e " + Format.ageSeconds(lastWrite) / 86_400 + " gündür yazılmıyor",
					evidence("lastWriteTime", Format.iso(lastWrite)),
					"Kullanılmıyorsa sahibine danışıp silmeyi değerlendirin; producer'lar başka topic'e mi geçti?");
		}
	}

	private static void consumers(Diagnosis dx, Map<String, Object> facts, GroupScan gs, OffsetsInfo oi,
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

		record Row(String groupId, String state, Integer members, OffsetWindow.GroupProgress p) {
		}
		List<Row> rows = new ArrayList<>();
		for (String g : w.groupsWithCommits()) {
			Settled<ConsumerGroupDescription> s = gs.described().get(g);
			ConsumerGroupDescription desc = s != null && s.ok() ? s.value() : null;
			rows.add(new Row(g, desc == null ? null : ConsumerGroups.state(desc.groupState()),
					desc == null ? null : desc.members().size(), w.progress(g)));
		}
		rows.sort(Comparator.comparingInt((Row r) -> -r.p().stuck().size())
			.thenComparing(Comparator.comparingLong((Row r) -> r.p().lag()).reversed())
			.thenComparing(Row::groupId));
		List<Map<String, Object>> stuckActive = new ArrayList<>();
		List<Map<String, Object>> stuckIdle = new ArrayList<>();
		List<Map<String, Object>> growing = new ArrayList<>();
		List<Map<String, Object>> high = new ArrayList<>();
		List<Map<String, Object>> beforeStart = new ArrayList<>();
		long totalLag = 0;
		for (Row r : rows) {
			OffsetWindow.GroupProgress p = r.p();
			totalLag += p.lag();
			Map<String, Object> ev = Results.map("groupId", r.groupId(), "state", r.state(), "members", r.members(),
					"lag", p.lag(), "stuckPartitions", p.stuck().isEmpty() ? null : partitionNumbers(
							p.stuck().stream().map(OffsetWindow.PartitionProgress::partition).toList()));
			if (!p.stuck().isEmpty()) {
				(r.members() != null && r.members() > 0 ? stuckActive : stuckIdle).add(ev);
			}
			else if ("growing".equals(p.trend()) && p.lag() >= GROWING_LAG_MIN) {
				ev.put("consumeRate", p.consumeRate());
				ev.put("produceRate", p.produceRate());
				growing.add(ev);
			}
			else if (p.lag() >= HIGH_LAG) {
				high.add(ev);
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
							first(lost, MAX_EVIDENCE)));
				}
			}
		}
		if (!stuckActive.isEmpty()) {
			dx.critical("CONSUMER_STUCK",
					stuckActive.size() + " aktif consumer group bu topic'te ilerlemiyor: commit sabit, yeni mesaj geliyor",
					evidence("groups", first(stuckActive, MAX_EVIDENCE), "omitted", omitted(stuckActive)),
					"Bu grupların consumer loglarını inceleyin (poison pill, uzun işleme, max.poll.interval.ms, commit "
							+ "hatası); detay için diagnose_consumer_group");
		}
		if (!stuckIdle.isEmpty()) {
			dx.warning("CONSUMER_STUCK",
					stuckIdle.size() + " consumer group üyesiz ve geride kalıyor: commit ilerlemiyor, yeni mesaj geliyor",
					evidence("groups", first(stuckIdle, MAX_EVIDENCE), "omitted", omitted(stuckIdle)),
					"Consumer uygulamasını başlatın; grup artık kullanılmıyorsa consumer_groups_delete ya da (grup EMPTY "
							+ "iken) consumer_groups_offsets_reset");
		}
		if (!growing.isEmpty()) {
			dx.warning("CONSUMER_LAG_GROWING",
					growing.size() + " consumer group'un lag'i büyüyor: tüketim hızı üretimin altında",
					evidence("groups", first(growing, MAX_EVIDENCE), "omitted", omitted(growing)),
					"Consumer sayısını partition sayısına (" + d.partitions().size()
							+ ") kadar artırın ya da işleme hızını iyileştirin; diagnose_consumer_group ile inceleyin");
		}
		if (!high.isEmpty()) {
			dx.info("CONSUMER_LAG_HIGH", high.size() + " consumer group'un lag'i " + HIGH_LAG + "+ (büyümüyor)",
					evidence("groups", first(high, MAX_EVIDENCE), "omitted", omitted(high)),
					"consumer_groups_lag_trend ile eğilimi izleyin");
		}
		if (!beforeStart.isEmpty()) {
			dx.warning("COMMITTED_BEFORE_LOG_START",
					beforeStart.size()
							+ " group'un commit'i log başlangıcından eski: okunmadan silinen mesajlar var (veri kaybı)",
					evidence("groups", first(beforeStart, MAX_EVIDENCE), "omitted", omitted(beforeStart)),
					"Grup çalıştığında auto.offset.reset devreye girer; retention.ms'i artırmayı ya da tüketimi "
							+ "hızlandırmayı değerlendirin");
		}
		long messages = oi == null ? -1 : oi.ranges().values().stream().mapToLong(Offsets.Range::count).sum();
		if (rows.isEmpty() && messages != 0 && gs.listErrors().isEmpty()) {
			dx.info("NO_CONSUMER_GROUPS",
					"Bu topic'te commit eden consumer group yok (assign() ile okuyan ya da commit etmeyen istemciler görünmez)",
					evidence("groupsScanned", gs.listed()), null);
		}
		if (!gs.listErrors().isEmpty() || !w.groupErrors().isEmpty()) {
			dx.markPartial();
		}
		List<Map<String, Object>> top = new ArrayList<>();
		for (Row r : rows.subList(0, Math.min(MAX_EVIDENCE, rows.size()))) {
			Map<String, Object> m = Results.map("groupId", r.groupId(), "state", r.state(), "members", r.members());
			m.putAll(r.p().summary());
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
					p.hanging().size() + " partition'da askıda (hanging) transaction şüphesi: LSO ilerlemez, "
							+ "read_committed consumer'lar takılır",
					evidence("partitions", first(p.hanging(), MAX_EVIDENCE), "thresholdMs", p.hangingThresholdMs()),
					"topics_transactions ile doğrulayın; kafka-transactions.sh find-hanging / abort ile kapatın");
		}
		if (!p.longRunning().isEmpty()) {
			dx.warning("LONG_RUNNING_TRANSACTION",
					p.longRunning().size() + " açık transaction kendi timeout'undan uzun süredir açık",
					evidence("transactions", first(p.longRunning(), MAX_EVIDENCE)),
					"Producer uygulamasını kontrol edin (commit/abort çağrılmıyor olabilir); topics_transactions");
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
			dx.warning("CONNECTOR_FAILED", failed.size() + " connector bu topic'i kullanıyor ve hata durumunda",
					evidence("connectors", first(failed, MAX_EVIDENCE)),
					"connectors_restart (includeTasks=true, onlyFailed=true) ile yeniden başlatın; tekrar düşerse "
							+ "diagnose_connector ile config/yetki sorununa bakın");
		}
	}

	private static void accessFindings(Diagnosis dx, Map<String, Object> facts, AclView v, String topic) {
		if (v == null) {
			return;
		}
		if (!v.authorizerEnabled()) {
			facts.put("access", Results.map("authorizer", false));
			dx.info("NO_AUTHORIZER", "Cluster'da authorizer yok: kimliği doğrulanan her istemci bu topic'i okuyup yazabilir",
					null, "Erişim kısıtlanmalıysa broker'da authorizer.class.name tanımlayın");
			return;
		}
		List<String> readers = v.allowedPrincipals(AclOperation.READ, ResourceType.TOPIC, topic);
		List<String> writers = v.allowedPrincipals(AclOperation.WRITE, ResourceType.TOPIC, topic);
		List<String> deniedRead = v.deniedPrincipals(AclOperation.READ, ResourceType.TOPIC, topic);
		List<String> deniedWrite = v.deniedPrincipals(AclOperation.WRITE, ResourceType.TOPIC, topic);
		int acls = v.resourceAclCount(ResourceType.TOPIC, topic);
		facts.put("access", Results.map("authorizer", true, "authorizerClass", v.authorizerClass(), "aclCount", acls,
				"readers", first(readers, 20), "writers", first(writers, 20), "deniedRead",
				deniedRead.isEmpty() ? null : first(deniedRead, 20), "deniedWrite",
				deniedWrite.isEmpty() ? null : first(deniedWrite, 20), "superUsers",
				v.superUsers().known() ? List.copyOf(v.superUsers().users()) : "bilinmiyor (broker gizliyor)",
				"allowEveryoneIfNoAclFound", v.allowEveryone().value(), "note",
				"Süper kullanıcılar ACL'den bağımsız erişir; readers/writers ACL'i olan principal'lardır"));
		if (acls == 0) {
			dx.info("NO_TOPIC_ACLS",
					v.allowEveryone().value() ? "Topic'e ACL yok ve allow.everyone.if.no.acl.found=true: herkes erişebilir"
							: "Topic'e ACL yok: sadece süper kullanıcılar erişebilir",
					null, "Uygulama principal'ları için acls_create (preset producer/consumer) kullanın");
		}
		if (readers.contains("User:*") || writers.contains("User:*")) {
			dx.info("PUBLIC_TOPIC_ACCESS", "User:* (herkes) bu topic'e erişebiliyor",
					evidence("read", readers.contains("User:*"), "write", writers.contains("User:*")),
					"Bilinçli değilse User:* binding'ini acls_delete ile kaldırın");
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
