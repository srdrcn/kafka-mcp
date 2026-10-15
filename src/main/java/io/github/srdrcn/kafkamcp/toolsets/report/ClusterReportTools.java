package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.KafkaVersions;
import io.github.srdrcn.kafkamcp.kafka.LogDirs;
import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectApi;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorOverview;
import io.github.srdrcn.kafkamcp.toolsets.consumers.ConsumerGroups;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag;
import io.github.srdrcn.kafkamcp.toolsets.consumers.Settled;
import io.github.srdrcn.kafkamcp.toolsets.schemaregistry.SchemaRegistryApi;

/** Cluster summary and topic rankings (size, partitions, consumers, write rate). */
@Component
public class ClusterReportTools {

	/** Maximum number of groups examined for the lag counts in the summary. */
	static final int SUMMARY_LAG_GROUPS = 500;

	static final List<String> TOP_BY = List.of("size", "partitions", "consumers", "write_rate");

	private final ConnectApi connectApi;

	private final SchemaRegistryApi schemaRegistryApi;

	public ClusterReportTools(ConnectApi connectApi, SchemaRegistryApi schemaRegistryApi) {
		this.connectApi = connectApi;
		this.schemaRegistryApi = schemaRegistryApi;
	}

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("report_cluster_summary",
						"Cluster summary: mode/version, brokers, topics/partitions, size, URP/offline, group states and lag, largest topics, connectors/subjects")
					.handler(this::clusterSummary),
				ToolSpec.read("report_top_topics",
						"Top N topics by size, partition count, consumer group count or write rate")
					.required("by", P.enumOf("Ranking metric", TOP_BY))
					.listParams(20)
					.param("intervalSeconds", ReportParams.intervalSeconds())
					.handler(this::topTopics));
	}

	Object clusterSummary(ToolCall call) {
		String cluster = call.cluster();
		ClusterConfig cc = call.clusterConfig();
		Async<List<ConnectorOverview>> connectors = cc.hasConnect() ? Async.start(() -> connectApi.overview(cluster))
				: null;
		Async<List<String>> subjects = cc.hasSchemaRegistry()
				? Async.start(() -> schemaRegistryApi.subjects(cluster, false)) : null;
		Async<ClusterMeta.Info> meta = Async.start(() -> ClusterMeta.describe(call, true));
		KafkaFuture<FeatureMetadata> features = call.admin().describeFeatures().featureMetadata();
		KafkaFuture<Map<TopicPartition, PartitionReassignment>> pendingReassignments = Topics
			.submitReassignments(call);
		FleetScan scan = FleetScan.of(call)
			.names(n -> true)
			.includeInternal(true)
			.ranges()
			.sizes()
			.configs()
			.brokerConfig()
			.groups(SUMMARY_LAG_GROUPS)
			.reserve(3_000)
			.run();
		Coverage coverage = scan.coverage();
		Settled<ClusterMeta.Info> info = meta.await(call, 2_500);
		Settled<FeatureMetadata> fm = Settled.within(call, features, 2_500);
		FinalizedVersionRange metadataVersion = fm.ok() ? fm.value().finalizedFeatures().get("metadata.version") : null;
		Topics.Reassignments reassignments = Topics.reassignments(call, pendingReassignments);
		if (reassignments.error() != null) {
			coverage.error("listPartitionReassignments", reassignments.error());
			coverage.note("Could not list reassignments: the URP count may include replicas being added by a reassignment");
		}

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("cluster", cluster);
		String mode = info.ok() ? info.value().mode() : null;
		if (mode == null && metadataVersion != null) {
			mode = ClusterMeta.KRAFT;
		}
		List<Node> brokers = info.ok() ? info.value().brokers() : scan.brokers();
		if (info.ok()) {
			out.put("clusterId", info.value().clusterId());
		}
		else {
			coverage.error("describeCluster", Async.message(info));
		}
		out.put("mode", mode);

		// URP / offline
		int partitions = 0;
		int urp = 0;
		int offline = 0;
		long internal = 0;
		for (TopicDescription d : scan.topics().values()) {
			internal += d.isInternal() ? 1 : 0;
			for (TopicPartitionInfo p : d.partitions()) {
				partitions++;
				if (Topics.offline(p)) {
					offline++;
				}
				else if (Topics.underReplicated(p, reassignments.adding(new TopicPartition(d.name(), p.partition())))) {
					urp++;
				}
			}
		}
		// version (KRaft: metadata.version; ZooKeeper: inter.broker.protocol.version)
		if (metadataVersion != null) {
			int level = metadataVersion.maxVersionLevel();
			String name = KafkaVersions.metadataVersion(level);
			out.put("kafkaVersion", KafkaVersions.release(level));
			out.put("versionSource", "metadata.version=" + level + (name == null ? "" : " (" + name + ")"));
		}
		else {
			String ibp = ConfigValues.text(scan.brokerConfig(), "inter.broker.protocol.version");
			if (ibp != null) {
				out.put("kafkaVersion", KafkaVersions.releaseOf(ibp));
				out.put("versionSource", "inter.broker.protocol.version=" + ibp);
			}
		}
		out.put("controllerId", info.ok() ? info.value().controllerId() : null);
		out.put("brokerCount", brokers.size());
		Set<String> racks = new TreeSet<>();
		brokers.forEach(n -> {
			if (n.hasRack()) {
				racks.add(n.rack());
			}
		});
		if (!racks.isEmpty()) {
			out.put("racks", racks);
		}
		out.put("topics", Results.map("user", scan.topics().size() - internal, "internal", internal));
		out.put("partitions", partitions);
		out.put("size", size(scan));
		ReplicationAudit.MinIsrCounts minIsr = ReplicationAudit.minIsrCounts(scan.topics().values(), scan.configs(),
				reassignments);
		out.put("replication", Results.map("underReplicatedPartitions", urp, "offlinePartitions", offline,
				"underMinIsrPartitions", minIsr.underMinIsrPartitions(), "underMinIsrUnknownTopics",
				minIsr.unknownTopics() > 0 ? minIsr.unknownTopics() : null, "minIsrAboveRfTopics",
				scan.configs().isEmpty() && !scan.topics().isEmpty() ? null : minIsr.aboveRfTopics(),
				"reassigningPartitions", reassignments.byPartition().isEmpty() ? null : reassignments.byPartition().size()));
		if (minIsr.aboveRfTopics() > 0) {
			coverage.note("minIsrAboveRfTopics: min.insync.replicas > RF (details: report_replication_audit); "
					+ "underMinIsrPartitions uses the effective min ISR: min(min.insync.replicas, replica count)");
		}
		int empty = 0;
		for (String t : scan.topics().keySet()) {
			empty += Boolean.TRUE.equals(scan.activity(t).empty()) ? 1 : 0;
		}
		out.put("emptyTopics", empty);
		out.put("consumerGroups", groups(scan));
		out.put("largestTopics", largest(scan, 5));
		if (connectors != null) {
			out.put("connect", connect(connectors.await(call, 500)));
		}
		if (subjects != null) {
			Settled<List<String>> s = subjects.await(call, 500);
			out.put("schemaRegistry", s.ok() ? Results.map("subjects", s.value().size())
					: Results.map("error", Async.message(s)));
		}
		coverage.addTo(out);
		return out;
	}

	private static Map<String, Object> size(FleetScan scan) {
		long leader = 0;
		long total = 0;
		for (LogDirs.PartitionSize s : scan.sizes().values()) {
			leader += s.leaderBytes();
			total += s.totalBytes();
		}
		return Results.map("logicalBytes", leader, "logical", Format.bytes(leader), "allReplicasBytes", total,
				"allReplicas", Format.bytes(total), "complete", scan.sizesComplete() ? null : false);
	}

	/** Group counts by state and the number of groups with lag (may be sampled). */
	private static Map<String, Object> groups(FleetScan scan) {
		GroupScan g = scan.groups();
		if (g == null) {
			return Results.map("error", scan.groupsError());
		}
		Map<String, Integer> byState = new TreeMap<>();
		for (ConsumerGroups.GroupSummary s : g.listed()) {
			byState.merge(s.state() == null ? "Unknown" : s.state(), 1, Integer::sum);
		}
		Map<TopicPartition, Offsets.Range> ranges = scan.ranges();
		int withLag = 0;
		int lagUnknown = 0;
		for (Map.Entry<String, GroupLag.Committed> e : g.committed().entrySet()) {
			GroupLag.Committed c = e.getValue();
			if (c.error() != null) {
				continue;
			}
			long lag = 0;
			boolean known = false;
			for (Map.Entry<TopicPartition, Long> o : c.offsets().entrySet()) {
				Offsets.Range r = ranges.get(o.getKey());
				if (r != null) {
					known = true;
					lag += GroupLag.lag(r.latest(), o.getValue());
				}
			}
			if (lag > 0) {
				withLag++;
			}
			else if (!known && !c.offsets().isEmpty()) {
				lagUnknown++;
			}
		}
		return Results.map("total", g.listed().size(), "byState", byState, "withLag", withLag, "lagExamined",
				g.committed().size(), "lagSampled", g.skipped() > 0 ? true : null, "lagSkipped",
				g.skipped() > 0 ? g.skipped() : null, "lagUnknown", lagUnknown > 0 ? lagUnknown : null, "note",
				"withLag: examined groups whose committed offsets are behind latest (difference > 0)");
	}

	private static List<Map<String, Object>> largest(FleetScan scan, int n) {
		List<String> names = new ArrayList<>(scan.topics().keySet());
		names.sort(Comparator.comparing((String t) -> scan.sizeBytes(t) == null ? 0L : scan.sizeBytes(t))
			.reversed()
			.thenComparing(Comparator.naturalOrder()));
		List<Map<String, Object>> out = new ArrayList<>();
		for (String t : ReportParams.head(names, n)) {
			Map<String, Object> row = scan.topicRow(t);
			Long total = scan.totalSizeBytes(t);
			if (total != null) {
				row.put("allReplicasBytes", total);
			}
			if (scan.topics().get(t).isInternal()) {
				row.put("internal", true);
			}
			out.add(row);
		}
		return out;
	}

	private static Map<String, Object> connect(Settled<List<ConnectorOverview>> s) {
		if (!s.ok()) {
			return Results.map("error", Async.message(s));
		}
		List<String> failed = new ArrayList<>();
		for (ConnectorOverview c : s.value()) {
			if (c.status().hasFailure()) {
				failed.add(c.name());
			}
		}
		return Results.map("connectors", s.value().size(), "failed", failed.size(), "failedConnectors",
				failed.isEmpty() ? null : ReportParams.head(failed, 20), "failedOmitted",
				failed.size() > 20 ? failed.size() - 20 : null);
	}

	Object topTopics(ToolCall call) {
		String by = call.str("by");
		if (!TOP_BY.contains(by)) {
			throw new ToolException("Invalid by: " + by + " (valid: " + String.join(", ", TOP_BY) + ")");
		}
		int limit = call.limit(20);
		if ("write_rate".equals(by)) {
			return writeRate(call, limit);
		}
		FleetScan.Builder b = FleetScan.of(call).sizes();
		if ("consumers".equals(by)) {
			b.groups(GroupScan.MAX_GROUPS);
		}
		FleetScan scan = b.run();
		Map<String, Long> metric = new HashMap<>();
		Map<String, Set<String>> consumers = Map.of();
		if ("consumers".equals(by)) {
			consumers = scan.requireGroups().groupsByTopic();
		}
		for (String t : scan.topics().keySet()) {
			metric.put(t, switch (by) {
				case "size" -> scan.sizeBytes(t);
				case "partitions" -> (long) scan.partitions(t);
				default -> (long) consumers.getOrDefault(t, Set.of()).size();
			});
		}
		List<String> ranked = rank(metric);
		List<Map<String, Object>> rows = new ArrayList<>();
		for (String t : ReportParams.head(ranked, limit)) {
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("rank", rows.size() + 1);
			row.putAll(scan.topicRow(t));
			if ("consumers".equals(by)) {
				List<String> gs = new ArrayList<>(consumers.getOrDefault(t, Set.of()));
				row.put("consumerGroups", gs.size());
				row.put("sampleGroups", gs.isEmpty() ? null : ReportParams.head(gs, 3));
			}
			rows.add(row);
		}
		Map<String, Object> out = Results.map("by", by);
		if ("size".equals(by) && !scan.sizesComplete()) {
			scan.coverage().note("Size ranking is based on incomplete log dir data");
		}
		if ("consumers".equals(by)) {
			scan.coverage()
				.note("consumers: number of groups with committed offsets on the topic; readers that do not commit are not visible");
		}
		scan.coverage().addTo(out);
		out.putAll(ReportParams.page("topics", rows, limit));
		out.put("total", ranked.size());
		out.put("truncated", ranked.size() > rows.size());
		return out;
	}

	/** Topic names sorted by metric (descending), ties broken by name. */
	private static List<String> rank(Map<String, Long> metric) {
		List<String> names = new ArrayList<>(metric.keySet());
		names.sort(Comparator.comparing((String t) -> metric.get(t) == null ? -1L : metric.get(t))
			.reversed()
			.thenComparing(Comparator.naturalOrder()));
		return names;
	}

	private Object writeRate(ToolCall call, int limit) {
		int intervalMs = call.integer("intervalSeconds", 5, 1, 10) * 1000;
		// sizes (context) complete in parallel with the samples; they are not awaited before sampling
		FleetScan scan = FleetScan.of(call).deferSizes().run();
		Coverage coverage = scan.coverage();
		List<TopicPartition> online = Topics.onlinePartitions(scan.topics().values());
		Map<TopicPartition, Long> first = latest(call, online, intervalMs + 2_500L);
		long t1 = System.nanoTime();
		long available = call.remainingMillis() - 2_500;
		long sleepMs = Math.min(intervalMs, available);
		if (sleepMs < 500) {
			throw new ToolException("Not enough time left for two samples; try a narrower filter");
		}
		call.sleep(sleepMs);
		Map<TopicPartition, Long> second = latest(call, online, 1_000);
		double elapsed = (System.nanoTime() - t1) / 1e9;
		scan.awaitSizes(700);
		Map<String, Long> delta = new HashMap<>();
		scan.topics().keySet().forEach(t -> delta.put(t, 0L));
		int missing = 0;
		for (TopicPartition tp : online) {
			Long a = first.get(tp);
			Long z = second.get(tp);
			if (a == null || z == null) {
				missing++;
				continue;
			}
			delta.merge(tp.topic(), Math.max(0, z - a), Long::sum);
		}
		if (missing > 0) {
			coverage.skipped("partitionsWithoutSamples", missing);
			coverage.partial();
		}
		List<String> ranked = rank(delta);
		int active = (int) delta.values().stream().filter(v -> v > 0).count();
		List<Map<String, Object>> rows = new ArrayList<>();
		for (String t : ReportParams.head(ranked, limit)) {
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("rank", rows.size() + 1);
			row.put("topic", t);
			row.put("msgsPerSec", Format.round(delta.get(t) / elapsed, 2));
			row.put("messages", delta.get(t));
			Map<String, Object> ctx = scan.topicRow(t);
			ctx.remove("topic");
			row.putAll(ctx);
			rows.add(row);
		}
		Map<String, Object> out = Results.map("by", "write_rate", "intervalSeconds", Format.round(elapsed, 2),
				"intervalReduced", sleepMs < intervalMs ? true : null, "activeTopics", active, "idleTopics",
				delta.size() - active);
		coverage.addTo(out);
		out.putAll(ReportParams.page("topics", rows, limit));
		out.put("total", ranked.size());
		out.put("truncated", ranked.size() > rows.size());
		return out;
	}

	/** Latest offsets of all partitions (one listOffsets); keeps reserve ms before the deadline. */
	static Map<TopicPartition, Long> latest(ToolCall call, List<TopicPartition> tps, long reserve) {
		Map<TopicPartition, Long> out = new HashMap<>();
		if (tps.isEmpty()) {
			return out;
		}
		var r = Offsets.submit(call, tps, OffsetSpec.latest());
		Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new LinkedHashMap<>();
		tps.forEach(tp -> futures.put(tp, r.partitionResult(tp)));
		Settled.all(call, futures, reserve).forEach((tp, s) -> {
			if (s.ok()) {
				out.put(tp, s.value().offset());
			}
		});
		return out;
	}

}
