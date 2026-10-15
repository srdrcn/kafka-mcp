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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.FeatureMetadata;
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
				ToolSpec.read("clusters_list",
						"List configured clusters with reachability and mode (KRaft/ZooKeeper)")
					.clusterOptional()
					.listParams()
					.param("check", P.bool("Also test the connection (default true)"))
					.handler(this::clustersList),
				ToolSpec.read("clusters_get",
						"Cluster summary: id, mode (KRaft/ZooKeeper, migration), controller/quorum, min Kafka version, authorizer, counts, URP/offline partitions")
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
						row.put("error", "Connection test did not finish within the time limit");
					}
				}
				rows.add(row);
			}
			pool.shutdownNow();
		}
		return Results.page("clusters", rows, call.limit());
	}

	Object clustersGet(ToolCall call) {
		KafkaFuture<Map<TopicPartition, PartitionReassignment>> pendingReassignments = Topics
			.submitReassignments(call);
		ClusterMeta.Info info = ClusterMeta.describe(call, true);
		// config of the default broker: authorizer, plus IBP and zookeeper.connect on ZooKeeper
		ConfigResource brokerResource = info.brokers().isEmpty() ? null : Configs.broker(info.defaultBrokerId());
		KafkaFuture<Config> pendingBrokerConfig = brokerResource == null ? null
				: Configs.submitOne(call, brokerResource);
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
		ClusterMeta.Mode mode = info.clusterMode();
		Config brokerConfig = null;
		String brokerConfigError = null;
		if (pendingBrokerConfig != null) {
			try {
				brokerConfig = Configs.awaitOne(call, brokerResource, pendingBrokerConfig);
			}
			catch (ToolException e) {
				brokerConfigError = e.getMessage();
			}
		}
		if (!mode.isKnown() && brokerConfig != null) {
			// no answer to the quorum and feature queries: use the broker's config (process.roles / zookeeper.connect)
			mode = BrokerTools.brokerMode(brokerConfig);
		}
		// ZooKeeper→KRaft migration: the describeCluster controller of ZooKeeper brokers is unreliable, and the
		// mode may depend on the responding broker
		boolean migration = info.migration() || ClusterMeta.migrationEnabled(brokerConfig);
		Integer controllerId = migration && !mode.isKraft() ? null : info.controllerId();
		Map<String, Object> out = Results.map("cluster", call.cluster(), "clusterId", info.clusterId(), "mode",
				mode.label(), "controllerId", controllerId);
		String controllerNote = controllerId == null && migration ? MIGRATION_CONTROLLER_NOTE : controllerNote(info);
		if (controllerNote != null) {
			out.put("controllerNote", controllerNote);
		}
		if (migration) {
			out.put("zkMigration", true);
			out.put("modeNote", MIGRATION_MODE_NOTE);
		}
		out.putAll(version(info, mode, brokerConfig, brokerConfigError));
		if (mode.isKraft() && info.finalizedFeatures() != null && !info.finalizedFeatures().isEmpty()) {
			out.put("features", info.finalizedFeatures());
		}
		if (brokerConfig != null) {
			Map<String, Object> zk = zookeeper(brokerConfig, mode);
			if (zk != null) {
				out.put("zookeeper", zk);
			}
			String authorizer = value(brokerConfig, "authorizer.class.name");
			out.put("authorizer", authorizer == null || authorizer.isBlank() ? "none (ACLs disabled)" : authorizer);
		}
		else if (brokerConfigError != null) {
			out.put("brokerConfigError", brokerConfigError);
		}
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

	/** Note for when the controller is not reported during a ZooKeeper→KRaft migration. */
	static final String MIGRATION_CONTROLLER_NOTE = "ZooKeeper→KRaft migration: the active controller may be the KRaft quorum leader; ZooKeeper brokers return a random broker in describeCluster";

	/** Note about the mode during a ZooKeeper→KRaft migration. */
	static final String MIGRATION_MODE_NOTE = "ZooKeeper→KRaft migration is enabled (zookeeper.metadata.migration.enable=true): mode and controller may depend on the responding broker; see brokers_config_drift for per-broker modes";

	/**
	 * Note about the controller: on KRaft the controller is unknown if the quorum query fails (the random broker
	 * returned by describeCluster is not reported); during a ZooKeeper→KRaft migration the describeCluster
	 * controller is unreliable too; a quorum leader that is not a broker is a separate controller node.
	 */
	static String controllerNote(ClusterMeta.Info info) {
		if (info.controllerId() == null && info.migration()) {
			return MIGRATION_CONTROLLER_NOTE;
		}
		if (info.controllerId() == null) {
			return info.clusterMode().isZooKeeper() ? "No active controller (an election may be in progress)"
					: "Active controller could not be determined (metadata quorum query failed)";
		}
		if (info.clusterMode().isKraft() && !info.controllerIsBroker()) {
			return "Active controller is a separate KRaft controller node (not a broker)";
		}
		return null;
	}

	/** Note explaining the version field (the protocol level can lag behind the software version). */
	static final String VERSION_NOTE = "Protocol level: the broker software may be newer (IBP/metadata.version may not be upgraded yet, or may be pinned)";

	/**
	 * Minimum Kafka version: the finalized metadata.version on KRaft, inter.broker.protocol.version on ZooKeeper
	 * (or when the mode is unknown). Both are protocol levels; the broker software may be newer (metadata.version
	 * goes up with "kafka-features upgrade" after an upgrade; the IBP can be pinned by hand). An IBP at its default
	 * value is the broker software version; an explicitly set one is reported as {@code versionPinned}.
	 */
	private static Map<String, Object> version(ClusterMeta.Info info, ClusterMeta.Mode mode, Config brokerConfig,
			String configError) {
		Short level = info.metadataVersionLevel();
		if (level != null) {
			String name = KafkaVersions.metadataVersion(level);
			return Results.map("minKafkaVersion", KafkaVersions.release(level), "versionSource",
					"metadata.version=" + level + (name == null ? "" : " (" + name + ")"), "versionNote",
					VERSION_NOTE);
		}
		if (mode.isKraft()) {
			return Map.of();
		}
		if (brokerConfig == null) {
			return configError == null ? Map.of() : Results.map("versionError", configError);
		}
		ConfigEntry ibp = brokerConfig.get("inter.broker.protocol.version");
		if (ibp != null && ibp.value() != null && !ibp.value().isBlank()) {
			boolean pinned = !Configs.isDefault(ibp);
			return Results.map("minKafkaVersion", KafkaVersions.releaseOf(ibp.value()), "versionSource",
					"inter.broker.protocol.version=" + ibp.value(), "versionPinned", pinned ? true : null,
					"versionNote", pinned ? VERSION_NOTE : null);
		}
		return Map.of();
	}

	/** Connection and ZooKeeper→KRaft migration info of a ZooKeeper cluster; null on KRaft. */
	static Map<String, Object> zookeeper(Config brokerConfig, ClusterMeta.Mode mode) {
		if (mode.isKraft()) {
			return null;
		}
		String connect = value(brokerConfig, "zookeeper.connect");
		if (connect == null || connect.isBlank()) {
			return null;
		}
		boolean migration = ClusterMeta.migrationEnabled(brokerConfig);
		return Results.map("connect", connect, "migrationEnabled", migration ? true : null);
	}

	/** Config value (null if sensitive). */
	private static String value(Config c, String key) {
		ConfigEntry e = c.get(key);
		return e == null || e.isSensitive() ? null : e.value();
	}

	/** Number of partitions in URP topics whose ISR is below min.insync.replicas (one describeConfigs). */
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

	/** Quorum replica lag; null if the leader does not know the replica's log end offset (-1). */
	static Long quorumLag(long highWatermark, long logEndOffset) {
		return logEndOffset < 0 ? null : Math.max(0, highWatermark - logEndOffset);
	}

	/**
	 * Connection test: describeCluster + describeFeatures (cheap; for the mode). controllerId is only reported on
	 * ZooKeeper: KRaft brokers return a random broker as the controller in describeCluster (use clusters_get for
	 * the active controller). On ZooKeeper the controller broker's migration flag is checked too (one
	 * describeConfigs): during a ZooKeeper→KRaft migration the controller is not reported.
	 */
	private Map<String, Object> probe(ClusterConfig c, ToolCall call) {
		try {
			Admin admin = clients.admin(c.getName());
			int timeout = (int) Math.min(10_000, call.waitMillis());
			DescribeClusterResult r = admin.describeCluster(new DescribeClusterOptions().timeoutMs(timeout));
			KafkaFuture<FeatureMetadata> features = admin
				.describeFeatures(new DescribeFeaturesOptions().timeoutMs(timeout))
				.featureMetadata();
			String id = call.await(r.clusterId());
			Node controller = call.await(r.controller());
			int brokers = call.await(r.nodes()).size();
			ClusterMeta.Mode mode;
			try {
				mode = ClusterMeta.modeOf(call.await(features));
			}
			catch (ToolException e) {
				mode = ClusterMeta.Mode.UNKNOWN;
			}
			Integer controllerId = null;
			Boolean migration = null;
			if (mode.isZooKeeper() && controller != null && !controller.isEmpty()) {
				migration = ClusterMeta.zkMigration(call, admin, controller.id(), Math.min(timeout, 4_000));
				controllerId = Boolean.TRUE.equals(migration) ? null : controller.id();
			}
			return Results.map("status", "ok", "clusterId", id, "mode", mode.label(), "controllerId", controllerId,
					"zkMigration", Boolean.TRUE.equals(migration) ? true : null, "brokerCount", brokers);
		}
		catch (Exception e) {
			return Results.map("status", "error", "error", KafkaErrors.explain(e));
		}
	}

}
