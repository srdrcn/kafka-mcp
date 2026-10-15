package io.github.srdrcn.kafkamcp.kafka;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Cluster metadata: brokers, active controller, cluster mode (KRaft/ZooKeeper) and (on KRaft) the metadata
 * quorum.
 *
 * <p>
 * Mode detection: KRaft if describeMetadataQuorum succeeds. Otherwise (not authorized, timeout, not supported)
 * describeFeatures is checked: a finalized {@code metadata.version} means KRaft, no finalized features means
 * ZooKeeper (ZooKeeper brokers do not finalize metadata.version). describeFeatures goes through ApiVersions
 * and needs no authorization; if neither answers, the mode is {@link Mode#UNKNOWN}.
 *
 * <p>
 * Active controller: on KRaft the quorum leader (brokers return a random live broker as the "controller" in
 * describeCluster; clients cannot reach the controllers), on ZooKeeper the describeCluster controller.
 * During a ZooKeeper→KRaft migration ({@value #MIGRATION_ENABLE}=true), once KRaft has taken over the
 * controller, ZooKeeper brokers also return a random broker; so in ZooKeeper mode this flag is read from the
 * controller broker and no controller is reported while the migration is on ({@link Info#zkMigration()}).
 * During the migration (some brokers on KRaft) the mode can depend on the broker that answers the request.
 */
public final class ClusterMeta {

	/** {@link Info#mode()} value: KRaft. */
	public static final String KRAFT = "KRaft";

	/** {@link Info#mode()} value: ZooKeeper (describeMetadataQuorum not supported). */
	public static final String ZOOKEEPER = "ZooKeeper";

	/** Value used in the output when the mode cannot be determined ({@link Mode#UNKNOWN}). */
	public static final String UNKNOWN = "unknown";

	/** Metadata version feature finalized by KRaft; absent on ZooKeeper clusters. */
	public static final String METADATA_VERSION_FEATURE = "metadata.version";

	/**
	 * Maximum wait for describeMetadataQuorum. Without a quorum leader the request hangs until the deadline;
	 * brokers can answer topic/config queries from their cache, so the remaining time is left for those.
	 */
	static final long QUORUM_WAIT_MS = 4_000;

	/** Maximum wait for describeFeatures (ApiVersions). */
	static final long FEATURES_WAIT_MS = 4_000;

	/** ZooKeeper→KRaft migration flag (broker config); true on ZooKeeper brokers and on migrating KRaft brokers. */
	public static final String MIGRATION_ENABLE = "zookeeper.metadata.migration.enable";

	/** Maximum wait for the controller broker's config when reading the migration flag in ZooKeeper mode. */
	static final long MIGRATION_WAIT_MS = 4_000;

	private ClusterMeta() {
	}

	/** Cluster mode. */
	public enum Mode {

		/** KRaft (has a metadata quorum or a finalized metadata.version). */
		KRAFT(ClusterMeta.KRAFT),

		/** ZooKeeper (DescribeQuorum not supported / no finalized features). */
		ZOOKEEPER(ClusterMeta.ZOOKEEPER),

		/** Could not be determined (quorum and feature queries failed). */
		UNKNOWN(ClusterMeta.UNKNOWN);

		private final String label;

		Mode(String label) {
			this.label = label;
		}

		/** Name used in the output: "KRaft", "ZooKeeper" or "unknown". */
		public String label() {
			return label;
		}

		public boolean isKraft() {
			return this == KRAFT;
		}

		public boolean isZooKeeper() {
			return this == ZOOKEEPER;
		}

		public boolean isKnown() {
			return this != UNKNOWN;
		}

		/** From an {@link Info#mode()} value (null = unknown). */
		public static Mode of(String label) {
			if (ClusterMeta.KRAFT.equals(label)) {
				return KRAFT;
			}
			if (ClusterMeta.ZOOKEEPER.equals(label)) {
				return ZOOKEEPER;
			}
			return UNKNOWN;
		}

		@Override
		public String toString() {
			return label;
		}

	}

	/**
	 * describeCluster result. {@code controllerId} is the quorum leader on KRaft and the describeCluster
	 * controller on ZooKeeper; null if the quorum was not queried, if the quorum query failed on KRaft or if a
	 * ZooKeeper→KRaft migration is on (in these cases brokers return a random broker as the controller in
	 * describeCluster). {@code mode} is null if the quorum was not queried or the mode could not be determined.
	 * {@code finalizedFeatures} are the finalized features (name → max level; empty on ZooKeeper), null if not
	 * queried or if the query failed. {@code zkMigration} is the controller broker's {@value #MIGRATION_ENABLE}
	 * value in ZooKeeper mode; null on KRaft, if not queried or if the config could not be read.
	 */
	public record Info(String clusterId, List<Node> brokers, Integer controllerId, String mode, QuorumInfo quorum,
			String quorumError, Map<String, Short> finalizedFeatures, Boolean zkMigration) {

		/** Without migration info (backward compatibility). */
		public Info(String clusterId, List<Node> brokers, Integer controllerId, String mode, QuorumInfo quorum,
				String quorumError, Map<String, Short> finalizedFeatures) {
			this(clusterId, brokers, controllerId, mode, quorum, quorumError, finalizedFeatures, null);
		}

		/** Without feature info (backward compatibility). */
		public Info(String clusterId, List<Node> brokers, Integer controllerId, String mode, QuorumInfo quorum,
				String quorumError) {
			this(clusterId, brokers, controllerId, mode, quorum, quorumError, null, null);
		}

		/**
		 * Is a ZooKeeper→KRaft migration on? If true, the describeCluster controller is unreliable (once KRaft has
		 * taken over the controller, ZooKeeper brokers return a random broker) and {@link #controllerId()} is null.
		 */
		public boolean migration() {
			return Boolean.TRUE.equals(zkMigration);
		}

		/** Broker ids (sorted). */
		public List<Integer> brokerIds() {
			return brokers.stream().map(Node::id).toList();
		}

		/** Default broker: the controller if it is a broker, otherwise the lowest id. */
		public int defaultBrokerId() {
			if (brokers.isEmpty()) {
				throw new ToolException("No reachable broker in the cluster");
			}
			if (controllerIsBroker()) {
				return controllerId;
			}
			return brokers.get(0).id();
		}

		/** Finds a broker by id; throws a readable error if it is missing. */
		public Node broker(int id) {
			return brokers.stream()
				.filter(n -> n.id() == id)
				.findFirst()
				.orElseThrow(() -> new ToolException("Broker not found: " + id + " (available: " + brokerIds() + ")"));
		}

		/** Mode; {@link Mode#UNKNOWN} if {@link #mode()} is null. */
		public Mode clusterMode() {
			return Mode.of(mode);
		}

		/** Is the active controller also a reachable broker (ZooKeeper or KRaft combined mode)? */
		public boolean controllerIsBroker() {
			return controllerId != null && brokerIds().contains(controllerId);
		}

		/** Finalized metadata.version level (KRaft only) or null. */
		public Short metadataVersionLevel() {
			return finalizedFeatures == null ? null : finalizedFeatures.get(METADATA_VERSION_FEATURE);
		}

	}

	/**
	 * Sends describeCluster (+ describeMetadataQuorum and describeFeatures if withQuorum) in parallel.
	 * Quorum and features are awaited for at most a few seconds; without a response {@link Info#quorumError()}
	 * is set and the tool carries on. If withQuorum is false, only describeCluster (mode and controller null).
	 */
	public static Info describe(ToolCall call, boolean withQuorum) {
		Admin admin = call.admin();
		DescribeClusterResult dc = admin.describeCluster();
		KafkaFuture<QuorumInfo> qf = withQuorum ? admin.describeMetadataQuorum().quorumInfo() : null;
		KafkaFuture<FeatureMetadata> ff = withQuorum ? admin.describeFeatures().featureMetadata() : null;
		// both requests were sent together: waits count from sending (at most ~4 s in total)
		long quorumEnd = System.currentTimeMillis() + QUORUM_WAIT_MS;
		long featuresEnd = System.currentTimeMillis() + FEATURES_WAIT_MS;
		String clusterId = call.await(dc.clusterId());
		List<Node> nodes = new ArrayList<>(call.await(dc.nodes()));
		nodes.sort(Comparator.comparingInt(Node::id));
		Node controller = call.await(dc.controller());
		if (!withQuorum) {
			return new Info(clusterId, List.copyOf(nodes), null, null, null, null, null, null);
		}
		QuorumInfo quorum = null;
		String quorumError = null;
		boolean quorumUnsupported = false;
		try {
			quorum = Futures.awaitAtMost(call, qf, untilMillis(quorumEnd));
		}
		catch (ToolException e) {
			quorumUnsupported = KafkaErrors.isUnsupported(e);
			quorumError = e.getMessage();
		}
		Map<String, Short> features = finalized(call, ff, untilMillis(featuresEnd));
		Mode mode = resolveMode(quorum != null, quorumUnsupported, features);
		if (mode == Mode.ZOOKEEPER) {
			// ZooKeeper has no DescribeQuorum anyway: not an error
			quorumError = null;
		}
		Integer controllerId = null;
		Boolean migration = null;
		if (quorum != null) {
			controllerId = quorum.leaderId() >= 0 ? quorum.leaderId() : null;
		}
		else if (mode == Mode.ZOOKEEPER && controller != null && !controller.isEmpty()) {
			// in a migration KRaft may own the controller and this is a random broker: skip it if the flag is on
			migration = zkMigration(call, admin, controller.id(), MIGRATION_WAIT_MS);
			controllerId = Boolean.TRUE.equals(migration) ? null : controller.id();
		}
		return new Info(clusterId, List.copyOf(nodes), controllerId, mode.isKnown() ? mode.label() : null, quorum,
				quorumError, features, migration);
	}

	/**
	 * Is a ZooKeeper→KRaft migration on in the broker config ({@value #MIGRATION_ENABLE}=true)? During a migration
	 * it is on for both ZooKeeper brokers and brokers already moved to KRaft (removed once the migration is finalized).
	 */
	public static boolean migrationEnabled(Config brokerConfig) {
		ConfigEntry e = brokerConfig == null ? null : brokerConfig.get(MIGRATION_ENABLE);
		return e != null && !e.isSensitive() && "true".equalsIgnoreCase(e.value());
	}

	/**
	 * The broker's {@value #MIGRATION_ENABLE} value (a single describeConfigs, at most {@code maxMillis}); null if
	 * the config cannot be read (not authorized, timeout). Never throws.
	 */
	public static Boolean zkMigration(ToolCall call, Admin admin, int brokerId, long maxMillis) {
		try {
			ConfigResource r = Configs.broker(brokerId);
			int timeout = (int) Math.max(1, Math.min(maxMillis, call.waitMillis()));
			KafkaFuture<Config> f = admin.describeConfigs(List.of(r), new DescribeConfigsOptions().timeoutMs(timeout))
				.values()
				.get(r);
			return f == null ? null : migrationEnabled(Futures.awaitAtMost(call, f, maxMillis));
		}
		catch (RuntimeException e) {
			return null;
		}
	}

	/**
	 * Mode of the selected cluster; tools (report/diagnose/acl...) call this to adapt to the mode. Tries
	 * describeFeatures first (needs no authorization, the broker answers from its cache), then
	 * describeMetadataQuorum if that fails. Each request is awaited for at most a few seconds; if neither
	 * answers, {@link Mode#UNKNOWN}. Never throws. During a ZooKeeper→KRaft migration (some brokers on KRaft)
	 * the result is the mode of the answering broker.
	 */
	public static Mode mode(ToolCall call) {
		Admin admin;
		try {
			admin = call.admin();
		}
		catch (ToolException e) {
			return Mode.UNKNOWN;
		}
		long end = System.currentTimeMillis() + QUORUM_WAIT_MS;
		Map<String, Short> features = finalized(call, admin.describeFeatures().featureMetadata(), FEATURES_WAIT_MS);
		if (features != null) {
			return modeOf(features);
		}
		// feature query failed: try the quorum in the remaining time (after a timeout: UNKNOWN without waiting)
		try {
			Futures.awaitAtMost(call, admin.describeMetadataQuorum().quorumInfo(), untilMillis(end));
			return Mode.KRAFT;
		}
		catch (ToolException e) {
			return KafkaErrors.isUnsupported(e) ? Mode.ZOOKEEPER : Mode.UNKNOWN;
		}
	}

	/**
	 * Mode from the finalized features: KRaft if metadata.version is present, otherwise ZooKeeper;
	 * {@link Mode#UNKNOWN} if null. The value type does not matter (FinalizedVersionRange or level).
	 */
	public static Mode modeOf(Map<String, ?> finalizedFeatures) {
		if (finalizedFeatures == null) {
			return Mode.UNKNOWN;
		}
		return finalizedFeatures.containsKey(METADATA_VERSION_FEATURE) ? Mode.KRAFT : Mode.ZOOKEEPER;
	}

	/** Mode from a describeFeatures result (see {@link #modeOf(Map)}). */
	public static Mode modeOf(FeatureMetadata features) {
		return modeOf(features == null ? null : features.finalizedFeatures());
	}

	/** Finalized features: name → max level (sorted by name). */
	public static Map<String, Short> levels(FeatureMetadata features) {
		Map<String, Short> out = new TreeMap<>();
		if (features != null) {
			for (Map.Entry<String, FinalizedVersionRange> e : features.finalizedFeatures().entrySet()) {
				out.put(e.getKey(), e.getValue().maxVersionLevel());
			}
		}
		return out;
	}

	/**
	 * Mode from the results of the quorum and feature queries. A quorum response means KRaft; otherwise the
	 * features decide (metadata.version → KRaft; older KRaft brokers finalize metadata.version even if they do
	 * not support DescribeQuorum). If neither is available, a "not supported" error means ZooKeeper.
	 */
	static Mode resolveMode(boolean quorumOk, boolean quorumUnsupported, Map<String, ?> finalizedFeatures) {
		if (quorumOk) {
			return Mode.KRAFT;
		}
		if (finalizedFeatures != null) {
			return modeOf(finalizedFeatures);
		}
		return quorumUnsupported ? Mode.ZOOKEEPER : Mode.UNKNOWN;
	}

	/** Waits at most {@code maxMillis} for a pending describeFeatures; null on error/timeout. */
	private static Map<String, Short> finalized(ToolCall call, KafkaFuture<FeatureMetadata> pending, long maxMillis) {
		try {
			return levels(Futures.awaitAtMost(call, pending, maxMillis));
		}
		catch (ToolException e) {
			return null;
		}
	}

	/** Time left until the given instant (at least 1 ms). */
	private static long untilMillis(long endEpochMillis) {
		return Math.max(1, endEpochMillis - System.currentTimeMillis());
	}

	/** Compact broker representation: id, host, port, rack. */
	public static Map<String, Object> node(Node n) {
		return Results.map("id", n.id(), "host", n.host(), "port", n.port(), "rack", n.hasRack() ? n.rack() : null);
	}

}
