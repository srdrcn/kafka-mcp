package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Denetim raporları: replikasyon, retention/segment config'leri ve partition aykırılıkları. */
@Component
public class AuditTools {

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("report_replication_audit",
						"Replikasyon denetimi: RF=1, min.insync.replicas ≥ RF, URP/offline, unclean election, varsayılan altı RF, tek rack")
					.param("includeInternal", ReportParams.includeInternal(true))
					.listParams(50)
					.handler(this::replicationAudit),
				ToolSpec.read("report_retention_audit",
						"Retention denetimi: sınırsız/çok kısa retention, retention.bytes, compact ayarları, segment aykırılıkları (değer + kaynak)")
					.param("includeInternal", ReportParams.includeInternal(false))
					.param("largeSizeMb", ReportParams.integer(
							"Sınırsız retention'da bu boyuttan (MB) büyük topic yüksek önemli (varsayılan 10240)", 10_240, 0,
							100_000_000))
					.param("shortRetentionMinutes", ReportParams.integer(
							"Bundan kısa retention/delete.retention/segment.ms aykırı sayılır (dakika, varsayılan 60)", 60, 1,
							10_080))
					.listParams(50)
					.handler(this::retentionAudit),
				ToolSpec.read("report_partition_outliers",
						"Partition aykırılıkları: medyanın çok üstünde partition sayısı, topic içi boyut skew'i, broker lider dengesizliği")
					.param("partitionFactor", ReportParams.integer("Medyanın kaç katı aykırı (varsayılan 4)", 4, 2, 1000))
					.param("minPartitions", ReportParams.integer("Aykırı sayılmak için en az partition (varsayılan 50)", 50, 1,
							100_000))
					.param("skewFactor", ReportParams.integer("Topic ortalamasının kaç katı skew (varsayılan 3)", 3, 2, 1000))
					.param("minSizeMb", ReportParams.integer("Skew için partition en az bu boyutta olmalı (MB, varsayılan 1)", 1,
							0, 10_000_000))
					.param("leaderImbalancePct", ReportParams.integer(
							"Broker lider sayısı ortalamadan bu yüzdeden fazla saparsa dengesiz (varsayılan 25)", 25, 1, 1000))
					.listParams(20)
					.handler(this::partitionOutliers));
	}

	Object replicationAudit(ToolCall call) {
		KafkaFuture<Map<TopicPartition, PartitionReassignment>> pending = Topics.submitReassignments(call);
		FleetScan scan = FleetScan.of(call)
			.includeInternal(call.bool("includeInternal", true))
			.configs()
			.brokerConfig()
			.reserve(2_500)
			.run();
		Topics.Reassignments reassignments = Topics.reassignments(call, pending);
		Map<Integer, String> racks = new TreeMap<>();
		for (Node n : scan.brokers()) {
			if (n.hasRack()) {
				racks.put(n.id(), n.rack());
			}
		}
		Long defaultRf = ConfigValues.longValue(scan.brokerConfig(), "default.replication.factor");
		Findings f = new Findings(call.limit(50));
		int withoutConfig = ReplicationAudit.evaluate(new ReplicationAudit.Input(scan.topics(), scan.configs(),
				defaultRf == null ? null : defaultRf.intValue(), racks, reassignments), f);
		Coverage coverage = scan.coverage();
		coverage.examined("brokers", scan.brokers().size());
		if (scan.brokerConfig() == null) {
			coverage.note("Broker config'i okunamadı: varsayılan RF kontrolü atlandı");
		}
		if (withoutConfig > 0) {
			coverage.note("Config'i okunamayan " + withoutConfig + " topic'te min ISR/unclean kontrolleri atlandı");
		}
		if (reassignments.error() != null) {
			coverage.error("listPartitionReassignments", reassignments.error());
		}
		Map<String, Object> out = Results.map("brokers", scan.brokers().size(), "racks",
				racks.isEmpty() ? null : new java.util.TreeSet<>(racks.values()), "rackCheck",
				new java.util.HashSet<>(racks.values()).size() >= 2 ? "enabled" : "skipped (rack'li broker < 2)",
				"defaultReplicationFactor", defaultRf, "reassigningPartitions",
				reassignments.byPartition().isEmpty() ? null : reassignments.byPartition().size());
		out.put("findingsTotal", f.total());
		out.put("bySeverity", f.bySeverity());
		out.put("findings", f.render());
		coverage.addTo(out);
		return out;
	}

	Object retentionAudit(ToolCall call) {
		long largeBytes = call.integer("largeSizeMb", 10_240, 0, 100_000_000) * 1024L * 1024L;
		long shortMs = call.integer("shortRetentionMinutes", 60, 1, 10_080) * 60_000L;
		FleetScan scan = FleetScan.of(call)
			.includeInternal(call.bool("includeInternal", false))
			.configs()
			.sizes()
			.run();
		Findings f = new Findings(call.limit(50));
		RetentionAudit.Thresholds th = new RetentionAudit.Thresholds(largeBytes, shortMs);
		int withoutConfig = 0;
		for (String t : scan.topics().keySet()) {
			Config c = scan.configs().get(t);
			if (c == null) {
				withoutConfig++;
				continue;
			}
			RetentionAudit.evaluate(t, c, scan.sizeBytes(t), scan.partitions(t), th, f);
		}
		Map<String, Object> out = Results.map("thresholds", Results.map("largeSize", Format.bytes(largeBytes),
				"shortMinutes", shortMs / 60_000));
		out.put("findingsTotal", f.total());
		out.put("bySeverity", f.bySeverity());
		out.put("findings", f.render());
		scan.coverage().note("source: topic = topic override'ı, broker = broker config'i, default = Kafka varsayılanı");
		if (withoutConfig > 0) {
			scan.coverage().note("Config'i okunamayan " + withoutConfig + " topic denetlenemedi");
		}
		scan.coverage().addTo(out);
		return out;
	}

	Object partitionOutliers(ToolCall call) {
		PartitionOutliers.Thresholds th = new PartitionOutliers.Thresholds(
				call.integer("partitionFactor", 4, 2, 1000), call.integer("minPartitions", 50, 1, 100_000),
				call.integer("skewFactor", 3, 2, 1000), call.integer("minSizeMb", 1, 0, 10_000_000) * 1024L * 1024L,
				call.integer("leaderImbalancePct", 25, 1, 1000));
		int limit = call.limit(20);
		FleetScan scan = FleetScan.of(call).names(n -> true).includeInternal(true).sizes().run();
		Map<String, Integer> userCounts = new TreeMap<>();
		List<TopicDescription> reported = new ArrayList<>();
		for (TopicDescription d : scan.topics().values()) {
			if (Topics.protectedReason(d.name(), d.isInternal()) != null) {
				continue;
			}
			userCounts.put(d.name(), d.partitions().size());
			if (call.matches(d.name())) {
				reported.add(d);
			}
		}
		double median = PartitionOutliers.median(userCounts.values());
		Map<String, Integer> reportedCounts = new LinkedHashMap<>();
		reported.forEach(d -> reportedCounts.put(d.name(), d.partitions().size()));
		List<PartitionOutliers.CountOutlier> counts = PartitionOutliers.countOutliers(reportedCounts, median, th);
		List<PartitionOutliers.SkewedTopic> skew = PartitionOutliers.skew(reported, scan.sizes(), th);
		List<PartitionOutliers.BrokerLoad> loads = PartitionOutliers.leaderBalance(scan.topics().values(),
				scan.brokers().stream().map(Node::id).toList(), th.leaderImbalancePct());

		Map<String, Object> out = Results.map("thresholds", Results.map("partitionFactor", th.partitionFactor(),
				"minPartitions", th.minPartitions(), "skewFactor", th.skewFactor(), "minSize",
				Format.bytes(th.minSizeBytes()), "leaderImbalancePct", th.leaderImbalancePct()), "medianPartitions",
				median, "medianOver", userCounts.size() + " kullanıcı topic'i (filtreden bağımsız)", "reportedTopics",
				reported.size());
		List<Map<String, Object>> countRows = new ArrayList<>();
		counts.forEach(c -> countRows.add(Results.map("topic", c.topic(), "partitions", c.partitions(), "timesMedian",
				c.timesMedian())));
		out.put("partitionCountOutliers", ReportParams.page("topics", countRows, limit));
		List<Map<String, Object>> skewRows = new ArrayList<>();
		for (PartitionOutliers.SkewedTopic s : skew) {
			List<Map<String, Object>> parts = new ArrayList<>();
			for (PartitionOutliers.SkewedPartition p : ReportParams.head(s.skewed(), 5)) {
				parts.add(Results.map("partition", p.partition(), "sizeBytes", p.sizeBytes(), "size",
						Format.bytes(p.sizeBytes()), "timesAverage", p.timesAverage()));
			}
			skewRows.add(Results.map("topic", s.topic(), "partitions", s.partitions(), "averagePartitionBytes",
					s.averageBytes(), "averagePartitionSize", Format.bytes(s.averageBytes()), "skewedCount",
					s.skewed().size(), "skewedPartitions", parts));
		}
		out.put("skewedTopics", ReportParams.page("topics", skewRows, limit));
		List<Map<String, Object>> brokerRows = new ArrayList<>();
		List<Integer> imbalanced = new ArrayList<>();
		int nonPreferred = 0;
		for (PartitionOutliers.BrokerLoad b : loads) {
			nonPreferred += b.nonPreferredLeaders();
			if (b.imbalanced()) {
				imbalanced.add(b.id());
			}
			brokerRows.add(Results.map("id", b.id(), "leaders", b.leaders(), "replicas", b.replicas(),
					"deviationPct", b.deviationPct(), "nonPreferredLeaders", b.nonPreferredLeaders(), "imbalanced",
					b.imbalanced() ? true : null));
		}
		out.put("leaderBalance", Results.map("scope", "tüm topic'ler (iç topic'ler dahil)", "imbalancedBrokers",
				imbalanced, "nonPreferredLeaders", nonPreferred, "brokers", ReportParams.head(brokerRows, 500),
				"brokersOmitted", brokerRows.size() > 500 ? brokerRows.size() - 500 : null));
		if (!scan.sizesComplete()) {
			scan.coverage().note("Boyutlar eksik: skew sonuçları eksik olabilir");
		}
		scan.coverage().addTo(out);
		return out;
	}

}
