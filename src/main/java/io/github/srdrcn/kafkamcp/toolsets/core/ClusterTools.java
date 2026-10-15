package io.github.srdrcn.kafkamcp.toolsets.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.Configs;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.kafka.KafkaVersions;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

@Component
public class ClusterTools {

	private final KafkaClients clients;

	public ClusterTools(KafkaClients clients) {
		this.clients = clients;
	}

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("clusters_list", "Config'teki cluster'ları ve erişilebilirliklerini listeler")
					.clusterOptional()
					.listParams()
					.param("check", P.bool("Bağlantıyı da test et (varsayılan true)"))
					.handler(this::clustersList),
				ToolSpec.read("clusters_get",
						"Cluster özeti: id, KRaft/ZooKeeper, controller/quorum, Kafka sürümü, broker/topic/partition sayıları, URP ve offline partition'lar")
					.handler(this::clustersGet));
	}

	Object clustersList(ToolCall call) throws Exception {
		KafkaMcpConfig config = clients.config();
		String only = call.clusterOrNull();
		List<ClusterConfig> selected = config.getClusters()
			.stream()
			.filter(c -> only == null || c.getName().equals(only))
			.filter(c -> call.matches(c.getName()))
			.limit(call.limit())
			.toList();
		boolean check = call.bool("check", true);
		List<Map<String, Object>> rows = new ArrayList<>();
		try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
			List<Future<Map<String, Object>>> checks = new ArrayList<>();
			for (ClusterConfig c : selected) {
				checks.add(check ? pool.submit(() -> probe(c, call)) : null);
			}
			for (int i = 0; i < selected.size(); i++) {
				ClusterConfig c = selected.get(i);
				Map<String, Object> row = Results.map("name", c.getName(), "bootstrapServers", c.getBootstrapServers(),
						"readOnly", config.isReadOnly(c), "connect", c.hasConnect(), "schemaRegistry",
						c.hasSchemaRegistry());
				Future<Map<String, Object>> f = checks.get(i);
				if (f != null) {
					try {
						row.putAll(f.get(Math.max(1, call.waitMillis()), TimeUnit.MILLISECONDS));
					}
					catch (java.util.concurrent.TimeoutException e) {
						f.cancel(true);
						row.put("status", "timeout");
						row.put("error", "Bağlantı testi zaman sınırında bitmedi");
					}
				}
				rows.add(row);
			}
			pool.shutdownNow();
		}
		return Results.page("clusters", rows, call.limit());
	}

	Object clustersGet(ToolCall call) {
		KafkaFuture<FeatureMetadata> features = call.admin().describeFeatures().featureMetadata();
		KafkaFuture<Map<TopicPartition, PartitionReassignment>> pendingReassignments = Topics
			.submitReassignments(call);
		ClusterMeta.Info info = ClusterMeta.describe(call, true);
		Map<String, Boolean> listing = Topics.list(call, true);
		Futures.Each<String, TopicDescription> topics = Topics.describe(call, listing.keySet());
		Topics.Reassignments reassignments = Topics.reassignments(call, pendingReassignments);
		int partitions = 0;
		int urp = 0;
		int offline = 0;
		Set<String> urpTopics = new TreeSet<>();
		for (TopicDescription d : topics.values().values()) {
			for (TopicPartitionInfo p : d.partitions()) {
				partitions++;
				if (Topics.offline(p)) {
					offline++;
				}
				if (Topics.underReplicated(p, reassignments.adding(new TopicPartition(d.name(), p.partition())))) {
					urp++;
					urpTopics.add(d.name());
				}
			}
		}
		long internal = listing.values().stream().filter(Boolean::booleanValue).count();
		FinalizedVersionRange metadataVersion = metadataVersion(call, features);
		String mode = info.mode();
		if (mode == null && metadataVersion != null) {
			// quorum sorgusu başarısız ama metadata.version finalize edilmiş: sadece KRaft'ta olur
			mode = ClusterMeta.KRAFT;
		}
		Map<String, Object> out = Results.map("cluster", call.cluster(), "clusterId", info.clusterId(), "mode", mode,
				"controllerId", info.controllerId());
		if (info.controllerId() == null && info.quorumError() != null) {
			out.put("controllerNote", "Aktif controller belirlenemedi (metadata quorum sorgusu başarısız)");
		}
		out.putAll(version(call, metadataVersion, info));
		out.put("brokerCount", info.brokers().size());
		out.put("topicCount", listing.size() - internal);
		out.put("internalTopicCount", internal);
		out.put("partitionCount", partitions);
		out.put("underReplicatedPartitions", urp);
		out.put("offlinePartitions", offline);
		if (!reassignments.byPartition().isEmpty()) {
			out.put("reassigningPartitions", reassignments.byPartition().size());
		}
		if (reassignments.error() != null) {
			out.put("reassignmentsError", reassignments.error());
		}
		out.putAll(underMinIsr(call, topics.values(), urpTopics));
		if (info.quorum() != null) {
			out.put("quorum", quorum(info.quorum()));
		}
		if (info.quorumError() != null) {
			out.put("quorumError", info.quorumError());
		}
		if (!topics.errors().isEmpty()) {
			out.put("topicsNotDescribed", topics.errors().size());
		}
		return out;
	}

	/** Finalize edilmiş metadata.version (sadece KRaft) ya da null (ZooKeeper, eski broker ya da hata). */
	private static FinalizedVersionRange metadataVersion(ToolCall call, KafkaFuture<FeatureMetadata> features) {
		try {
			return call.await(features).finalizedFeatures().get("metadata.version");
		}
		catch (ToolException e) {
			return null; // eski broker: describeFeatures yok
		}
	}

	/** metadata.version (KRaft) ya da inter.broker.protocol.version (ZooKeeper) üzerinden en iyi tahmin. */
	private static Map<String, Object> version(ToolCall call, FinalizedVersionRange metadataVersion,
			ClusterMeta.Info info) {
		if (metadataVersion != null) {
			int level = metadataVersion.maxVersionLevel();
			String name = KafkaVersions.metadataVersion(level);
			return Results.map("kafkaVersion", KafkaVersions.release(level), "versionSource",
					"metadata.version=" + level + (name == null ? "" : " (" + name + ")"));
		}
		if (info.brokers().isEmpty()) {
			return Map.of();
		}
		try {
			Config c = Configs.describeOne(call, Configs.broker(info.defaultBrokerId()));
			ConfigEntry ibp = c.get("inter.broker.protocol.version");
			if (ibp != null && ibp.value() != null) {
				return Results.map("kafkaVersion", KafkaVersions.releaseOf(ibp.value()), "versionSource",
						"inter.broker.protocol.version=" + ibp.value());
			}
		}
		catch (ToolException e) {
			return Results.map("versionError", e.getMessage());
		}
		return Map.of();
	}

	/** URP olan topic'lerde ISR, min.insync.replicas'ın altında olan partition sayısı (tek describeConfigs). */
	private static Map<String, Object> underMinIsr(ToolCall call, Map<String, TopicDescription> topics,
			Set<String> urpTopics) {
		if (urpTopics.isEmpty()) {
			return Results.map("underMinIsrPartitions", 0);
		}
		List<ConfigResource> resources = urpTopics.stream().map(Configs::topic).toList();
		Futures.Each<ConfigResource, Config> configs = Configs.describe(call, resources);
		int count = 0;
		for (Map.Entry<ConfigResource, Config> e : configs.values().entrySet()) {
			ConfigEntry minIsr = e.getValue().get("min.insync.replicas");
			if (minIsr == null || minIsr.value() == null) {
				continue;
			}
			int min = Integer.parseInt(minIsr.value());
			for (TopicPartitionInfo p : topics.get(e.getKey().name()).partitions()) {
				if (p.isr().size() < min) {
					count++;
				}
			}
		}
		return Results.map("underMinIsrPartitions", count, "underMinIsrUnknownTopics",
				configs.errors().isEmpty() ? null : configs.errors().size());
	}

	private static Map<String, Object> quorum(QuorumInfo q) {
		List<Map<String, Object>> voters = new ArrayList<>();
		for (QuorumInfo.ReplicaState r : q.voters()) {
			Long lag = quorumLag(q.highWatermark(), r.logEndOffset());
			voters.add(Results.map("id", r.replicaId(), "lag", lag, "status", lag == null ? "unknown" : null));
		}
		List<Map<String, Object>> lagging = new ArrayList<>();
		for (QuorumInfo.ReplicaState r : q.observers()) {
			Long lag = quorumLag(q.highWatermark(), r.logEndOffset());
			if ((lag == null || lag > 0) && lagging.size() < 20) {
				lagging.add(Results.map("id", r.replicaId(), "lag", lag, "status", lag == null ? "unknown" : null));
			}
		}
		return Results.map("leaderId", q.leaderId(), "leaderEpoch", q.leaderEpoch(), "highWatermark",
				q.highWatermark(), "voters", voters, "observerCount", q.observers().size(), "laggingObservers",
				lagging.isEmpty() ? null : lagging);
	}

	/** Quorum replika gecikmesi; lider replikanın log end offset'ini bilmiyorsa (-1) null. */
	static Long quorumLag(long highWatermark, long logEndOffset) {
		return logEndOffset < 0 ? null : Math.max(0, highWatermark - logEndOffset);
	}

	private Map<String, Object> probe(ClusterConfig c, ToolCall call) {
		try {
			DescribeClusterResult r = clients.admin(c.getName())
				.describeCluster(new DescribeClusterOptions().timeoutMs((int) Math.min(10_000, call.waitMillis())));
			String id = call.await(r.clusterId());
			Node controller = call.await(r.controller());
			int brokers = call.await(r.nodes()).size();
			return Results.map("status", "ok", "clusterId", id, "controllerId",
					controller == null || controller.isEmpty() ? null : controller.id(), "brokerCount", brokers);
		}
		catch (Exception e) {
			return Results.map("status", "error", "error", KafkaErrors.explain(e));
		}
	}

}
