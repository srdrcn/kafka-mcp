package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import io.github.srdrcn.kafkamcp.kafka.Configs;
import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.LogDirs;
import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.toolsets.consumers.Settled;

/**
 * Batched cluster scan for the report tools: one listTopics, one describeTopics and one admin request for each
 * requested extra (earliest/latest/maxTimestamp listOffsets, describeLogDirs, describeConfigs, consumer group
 * offsets). Requests are sent in parallel and awaited with a margin before the deadline; parts that do not finish
 * in time or fail are reported in {@link #coverage()} (skipped/partial/errors) and the tool does not fail.
 *
 * <pre>
 * FleetScan scan = FleetScan.of(call).ranges().maxTimestamps().sizes().run();
 * TopicActivity a = scan.activity("orders");
 * </pre>
 */
public final class FleetScan {

	/** Default margin kept before the deadline for computation and output. */
	public static final long OUTPUT_RESERVE_MS = 1_500;

	/** Last-write reason when the maxTimestamp query is not supported (broker &lt; 3.0). */
	public static final String MAX_TIMESTAMP_UNSUPPORTED = "Broker does not support the maxTimestamp query (Kafka < 3.0)";

	private final ToolCall call;

	private final Coverage coverage = new Coverage();

	private final long nowMs = System.currentTimeMillis();

	private Map<String, Boolean> listing = Map.of();

	private final Map<String, TopicDescription> topics = new TreeMap<>();

	private List<Node> brokers = List.of();

	private Map<TopicPartition, Offsets.Range> ranges;

	private Map<TopicPartition, Long> maxTimestamps;

	private Map<TopicPartition, String> maxTimestampErrors;

	private boolean maxTimestampUnsupported;

	private Map<TopicPartition, LogDirs.PartitionSize> sizes;

	private boolean sizesComplete = true;

	private Set<Integer> sizeErrorBrokers = Set.of();

	private Map<String, long[]> topicSizes;

	private Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> pendingDirs;

	private Map<Integer, String> pendingDirErrors;

	private Map<String, Config> configs;

	private Config brokerConfig;

	private Integer brokerConfigId;

	private GroupScan groups;

	private String groupsError;

	private final Map<String, TopicActivity> activities = new HashMap<>();

	private FleetScan(ToolCall call) {
		this.call = call;
	}

	/** Scan definition; runs with {@link Builder#run()}. The default topic filter is the {@code filter} parameter. */
	public static Builder of(ToolCall call) {
		return new Builder(call);
	}

	/** What to scan. */
	public static final class Builder {

		private final ToolCall call;

		private Predicate<String> names;

		private String notMatchingLabel = "notMatchingFilter";

		private boolean includeInternal;

		private boolean ranges;

		private boolean maxTimestamps;

		private boolean sizes;

		private boolean deferSizes;

		private boolean configs;

		private boolean brokerConfig;

		private int maxGroups;

		private long reserveMillis = OUTPUT_RESERVE_MS;

		private Builder(ToolCall call) {
			this.call = call;
			this.names = call::matches;
		}

		/** Topic name filter (default: the filter parameter). */
		public Builder names(Predicate<String> names) {
			this.names = names;
			return this;
		}

		/** Topic name filter; non-matching topics are counted under skipped with the {@code label} key. */
		public Builder names(Predicate<String> names, String label) {
			this.names = names;
			this.notMatchingLabel = label;
			return this;
		}

		/** Also scan internal (__*) and system topics (Connect/Schema Registry/MirrorMaker) (default false). */
		public Builder includeInternal(boolean includeInternal) {
			this.includeInternal = includeInternal;
			return this;
		}

		/** earliest + latest offsets (emptiness, message count). */
		public Builder ranges() {
			this.ranges = true;
			return this;
		}

		/** Largest timestamp per partition (OffsetSpec.maxTimestamp; Kafka 3.0+). */
		public Builder maxTimestamps() {
			this.maxTimestamps = true;
			return this;
		}

		/** Partition sizes via describeLogDirs. */
		public Builder sizes() {
			this.sizes = true;
			return this;
		}

		/**
		 * The describeLogDirs request is sent but not awaited; sizes are filled in when
		 * {@link FleetScan#awaitSizes(long)} is called (e.g. so it completes in parallel between the write-rate samples).
		 */
		public Builder deferSizes() {
			this.sizes = true;
			this.deferSizes = true;
			return this;
		}

		/** Configs of the scanned topics (one describeConfigs). */
		public Builder configs() {
			this.configs = true;
			return this;
		}

		/** One broker's config (e.g. default.replication.factor); in the same request as configs. */
		public Builder brokerConfig() {
			this.brokerConfig = true;
			return this;
		}

		/** Consumer groups and the committed offsets of at most maxGroups groups. */
		public Builder groups(int maxGroups) {
			this.maxGroups = maxGroups;
			return this;
		}

		/** Time left to the tool after waiting (default {@link #OUTPUT_RESERVE_MS}). */
		public Builder reserve(long reserveMillis) {
			this.reserveMillis = reserveMillis;
			return this;
		}

		public FleetScan run() {
			FleetScan s = new FleetScan(call);
			s.collect(this);
			return s;
		}

	}

	private void collect(Builder b) {
		long reserve = b.reserveMillis;
		Async<GroupScan> groupTask = b.maxGroups > 0
				? Async.start(() -> GroupScan.run(call, b.maxGroups, reserve + 500)) : null;
		KafkaFuture<Collection<Node>> nodes = call.admin().describeCluster().nodes();
		listing = Topics.list(call, true);
		List<String> selected = new ArrayList<>();
		int internal = 0;
		int filtered = 0;
		for (Map.Entry<String, Boolean> e : listing.entrySet()) {
			if (!b.includeInternal && Topics.protectedReason(e.getKey(), e.getValue()) != null) {
				internal++;
			}
			else if (!b.names.test(e.getKey())) {
				filtered++;
			}
			else {
				selected.add(e.getKey());
			}
		}
		Futures.Each<String, TopicDescription> described = Topics.describe(call, selected);
		topics.putAll(described.values());
		int partitions = topics.values().stream().mapToInt(d -> d.partitions().size()).sum();
		coverage.examined("topics", topics.size()).examined("partitions", partitions);
		coverage.skipped("internalOrSystemTopics", internal);
		coverage.skipped(b.notMatchingLabel, filtered);
		if (!described.errors().isEmpty()) {
			coverage.skipped("topicsNotDescribed", described.errors().size());
			coverage.error("describeTopics", firstErrors(described.errors()));
		}
		List<TopicPartition> online = Topics.onlinePartitions(topics.values());
		boolean needOffsets = b.ranges || b.maxTimestamps;
		if (needOffsets) {
			coverage.skipped("offlinePartitions", partitions - online.size());
		}
		Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> earliest = b.ranges
				? offsets(online, OffsetSpec.earliest()) : Map.of();
		Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> latest = b.ranges
				? offsets(online, OffsetSpec.latest()) : Map.of();
		Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> maxTs = b.maxTimestamps
				? offsets(online, OffsetSpec.maxTimestamp()) : Map.of();
		Settled<Collection<Node>> live = Settled.within(call, nodes, reserve);
		if (live.ok()) {
			brokers = new ArrayList<>(live.value());
			brokers.sort(java.util.Comparator.comparingInt(Node::id));
		}
		else {
			coverage.error("describeCluster", live.errorMessage());
		}
		Map<ConfigResource, KafkaFuture<Config>> configFutures = new LinkedHashMap<>();
		if (b.configs || b.brokerConfig) {
			List<ConfigResource> resources = new ArrayList<>();
			if (b.configs) {
				topics.keySet().forEach(t -> resources.add(Configs.topic(t)));
			}
			if (b.brokerConfig && !brokers.isEmpty()) {
				brokerConfigId = brokers.get(0).id();
				resources.add(Configs.broker(brokerConfigId));
			}
			if (!resources.isEmpty()) {
				configFutures.putAll(call.admin().describeConfigs(resources).values());
			}
		}
		Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> dirFutures = new LinkedHashMap<>();
		Map<Integer, String> dirErrors = new TreeMap<>();
		if (b.sizes) {
			Set<Integer> liveIds = new LinkedHashSet<>(brokers.stream().map(Node::id).toList());
			Set<Integer> query = new LinkedHashSet<>();
			for (Integer id : LogDirs.brokersHosting(topics.values())) {
				if (liveIds.contains(id) || !live.ok()) {
					query.add(id);
				}
				else {
					dirErrors.put(id, LogDirs.OFFLINE);
				}
			}
			if (!query.isEmpty()) {
				dirFutures.putAll(call.admin().describeLogDirs(query).descriptions());
			}
		}
		if (b.ranges) {
			collectRanges(earliest, latest, reserve);
		}
		if (b.maxTimestamps) {
			collectMaxTimestamps(maxTs, reserve);
		}
		if (b.sizes) {
			pendingDirs = dirFutures;
			pendingDirErrors = dirErrors;
			if (!b.deferSizes) {
				awaitSizes(reserve);
			}
		}
		if (!configFutures.isEmpty() || b.configs) {
			collectConfigs(configFutures, reserve);
		}
		if (groupTask != null) {
			Settled<GroupScan> g = groupTask.await(call, reserve);
			if (g.ok()) {
				groups = g.value();
				groups.describe(coverage);
			}
			else {
				groupsError = Async.message(g);
				coverage.error("consumerGroups", groupsError);
				coverage.partial();
			}
		}
	}

	private Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> offsets(List<TopicPartition> tps, OffsetSpec spec) {
		Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> out = new LinkedHashMap<>();
		if (tps.isEmpty()) {
			return out;
		}
		ListOffsetsResult r = Offsets.submit(call, tps, spec);
		for (TopicPartition tp : tps) {
			out.put(tp, r.partitionResult(tp));
		}
		return out;
	}

	private void collectRanges(Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> earliest,
			Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> latest, long reserve) {
		Map<TopicPartition, Settled<ListOffsetsResultInfo>> e = Settled.all(call, earliest, reserve);
		Map<TopicPartition, Settled<ListOffsetsResultInfo>> l = Settled.all(call, latest, reserve);
		ranges = new HashMap<>();
		Map<String, String> errors = new TreeMap<>();
		int missing = 0;
		int timedOut = 0;
		for (TopicPartition tp : earliest.keySet()) {
			Settled<ListOffsetsResultInfo> a = e.get(tp);
			Settled<ListOffsetsResultInfo> z = l.get(tp);
			if (a.ok() && z.ok()) {
				ranges.put(tp, new Offsets.Range(a.value().offset(), z.value().offset()));
				continue;
			}
			missing++;
			Settled<ListOffsetsResultInfo> bad = a.ok() ? z : a;
			timedOut += bad.timedOut() ? 1 : 0;
			if (errors.size() < 5) {
				errors.putIfAbsent(tp.toString(), bad.errorMessage());
			}
		}
		if (missing > 0) {
			coverage.skipped("partitionsWithoutOffsets", missing);
			coverage.error("listOffsets", errors);
			if (timedOut > 0) {
				coverage.partial();
			}
		}
	}

	private void collectMaxTimestamps(Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures,
			long reserve) {
		Map<TopicPartition, Settled<ListOffsetsResultInfo>> settled = Settled.all(call, futures, reserve);
		maxTimestamps = new HashMap<>();
		maxTimestampErrors = new HashMap<>();
		int timedOut = 0;
		Map<String, String> sample = new TreeMap<>();
		for (Map.Entry<TopicPartition, Settled<ListOffsetsResultInfo>> en : settled.entrySet()) {
			Settled<ListOffsetsResultInfo> s = en.getValue();
			if (s.ok()) {
				maxTimestamps.put(en.getKey(), s.value().timestamp());
				continue;
			}
			if (s.error() instanceof UnsupportedVersionException) {
				maxTimestampUnsupported = true;
				maxTimestampErrors.put(en.getKey(), MAX_TIMESTAMP_UNSUPPORTED);
				continue;
			}
			timedOut += s.timedOut() ? 1 : 0;
			maxTimestampErrors.put(en.getKey(), s.errorMessage());
			if (sample.size() < 5) {
				sample.put(en.getKey().toString(), s.errorMessage());
			}
		}
		if (maxTimestampUnsupported) {
			coverage.note("Last write time unknown: " + MAX_TIMESTAMP_UNSUPPORTED);
		}
		if (!sample.isEmpty()) {
			coverage.error("maxTimestamp", sample);
		}
		if (timedOut > 0) {
			coverage.partial();
		}
	}

	/**
	 * Waits at most (remaining time - reserve) for the pending describeLogDirs and fills in the sizes; does nothing
	 * if sizes() already waited or sizes were not requested.
	 */
	public void awaitSizes(long reserve) {
		if (pendingDirs == null) {
			return;
		}
		Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> futures = pendingDirs;
		Map<Integer, String> errors = pendingDirErrors;
		pendingDirs = null;
		pendingDirErrors = null;
		topicSizes = null;
		Map<Integer, Map<String, LogDirDescription>> ok = new LinkedHashMap<>();
		Settled.all(call, futures, reserve).forEach((id, s) -> {
			if (s.ok()) {
				ok.put(id, s.value());
			}
			else {
				errors.put(id, s.errorMessage());
				if (s.timedOut()) {
					coverage.partial();
				}
			}
		});
		sizes = LogDirs.partitionSizes(ok, topics);
		if (!errors.isEmpty()) {
			sizesComplete = false;
			sizeErrorBrokers = Set.copyOf(errors.keySet());
			coverage.error("logDirs", errors);
			coverage.note("Could not read the log dirs of some brokers: sizes are lower bounds");
		}
	}

	private void collectConfigs(Map<ConfigResource, KafkaFuture<Config>> futures, long reserve) {
		configs = new TreeMap<>();
		Map<String, String> errors = new TreeMap<>();
		int failed = 0;
		for (Map.Entry<ConfigResource, Settled<Config>> e : Settled.all(call, futures, reserve).entrySet()) {
			Settled<Config> s = e.getValue();
			ConfigResource r = e.getKey();
			if (!s.ok()) {
				failed += r.type() == ConfigResource.Type.TOPIC ? 1 : 0;
				if (errors.size() < 5) {
					errors.put(r.type() + ":" + r.name(), s.errorMessage());
				}
				if (s.timedOut()) {
					coverage.partial();
				}
				continue;
			}
			if (r.type() == ConfigResource.Type.TOPIC) {
				configs.put(r.name(), s.value());
			}
			else {
				brokerConfig = s.value();
			}
		}
		coverage.skipped("topicsWithoutConfigs", failed);
		if (!errors.isEmpty()) {
			coverage.error("describeConfigs", errors);
		}
	}

	private static Map<String, String> firstErrors(Map<String, String> errors) {
		Map<String, String> out = new TreeMap<>();
		for (Map.Entry<String, String> e : errors.entrySet()) {
			if (out.size() >= 5) {
				break;
			}
			out.put(e.getKey(), e.getValue());
		}
		return out;
	}

	public ToolCall call() {
		return call;
	}

	public Coverage coverage() {
		return coverage;
	}

	/** When the scan started (epoch ms); ages are computed from it. */
	public long nowMs() {
		return nowMs;
	}

	/** All topics in the cluster (including internal ones) → internal flag. */
	public Map<String, Boolean> listing() {
		return listing;
	}

	/** Scanned (matching and described) topics, sorted by name. */
	public Map<String, TopicDescription> topics() {
		return topics;
	}

	/** Live brokers (by id; empty if describeCluster failed). */
	public List<Node> brokers() {
		return brokers;
	}

	/** Partition → earliest/latest (if ranges() was requested; missing ones are absent). */
	public Map<TopicPartition, Offsets.Range> ranges() {
		return ranges == null ? Map.of() : ranges;
	}

	/** Partition sizes (if sizes() was requested). */
	public Map<TopicPartition, LogDirs.PartitionSize> sizes() {
		return sizes == null ? Map.of() : sizes;
	}

	/** Were the log dirs of all brokers read (are the sizes complete)? */
	public boolean sizesComplete() {
		return sizes != null && sizesComplete;
	}

	/**
	 * Is the topic's size complete: sizes were read and none of the topic's replicas is on a broker whose log dirs
	 * could not be read. If false, {@link #sizeBytes} is null or a lower bound.
	 */
	public boolean sizeComplete(String topic) {
		TopicDescription d = topics.get(topic);
		if (sizes == null || d == null) {
			return false;
		}
		if (sizesComplete) {
			return true;
		}
		for (TopicPartitionInfo p : d.partitions()) {
			for (Node n : p.replicas()) {
				if (sizeErrorBrokers.contains(n.id())) {
					return false;
				}
			}
		}
		return true;
	}

	/** Topic → config (if configs() was requested; unreadable ones are absent). */
	public Map<String, Config> configs() {
		return configs == null ? Map.of() : configs;
	}

	/** The broker config read for brokerConfig(), or null. */
	public Config brokerConfig() {
		return brokerConfig;
	}

	/** Id of the broker whose config was read, or null. */
	public Integer brokerConfigId() {
		return brokerConfigId;
	}

	/** Group scan; null if not requested or failed (reason in {@link #groupsError()}). */
	public GroupScan groups() {
		return groups;
	}

	public String groupsError() {
		return groupsError;
	}

	/** For tools that need the group scan: throws an error if it failed. */
	public GroupScan requireGroups() {
		if (groups == null) {
			throw new ToolException("Could not read consumer groups: " + (groupsError == null ? "unknown error" : groupsError));
		}
		return groups;
	}

	/** Was maxTimestamp unsupported on some partitions? */
	public boolean maxTimestampUnsupported() {
		return maxTimestampUnsupported;
	}

	public int partitions(String topic) {
		TopicDescription d = topics.get(topic);
		return d == null ? 0 : d.partitions().size();
	}

	public int replicationFactor(String topic) {
		TopicDescription d = topics.get(topic);
		return d == null ? 0 : Topics.replicationFactor(d, Topics.Reassignments.NONE);
	}

	/**
	 * Logical size of the topic (leader replicas). Null if sizes were not requested or no replica size of the topic
	 * could be read (broker error); a lower bound if some brokers could not be read.
	 */
	public Long sizeBytes(String topic) {
		return topicSize(topic, 0);
	}

	/** Total size of all replicas; null if unknown (see {@link #sizeBytes}). */
	public Long totalSizeBytes(String topic) {
		return topicSize(topic, 1);
	}

	/** index 0: leader, 1: all replicas (a ternary mixing long/null would NPE on unboxing; explicit if). */
	private Long topicSize(String topic, int index) {
		if (sizes == null) {
			return null;
		}
		long[] a = topicSizes().get(topic);
		if (a != null) {
			return a[index];
		}
		return sizesComplete ? Long.valueOf(0) : null;
	}

	/** Size fields for the output: sizeBytes and a human-readable size. */
	public Map<String, Object> sizeFields(String topic) {
		Long bytes = sizeBytes(topic);
		return bytes == null ? new LinkedHashMap<>() : Results.map("sizeBytes", bytes, "size", Format.bytes(bytes));
	}

	private Map<String, long[]> topicSizes() {
		if (topicSizes == null) {
			topicSizes = new HashMap<>();
			sizes().forEach((tp, s) -> {
				long[] a = topicSizes.computeIfAbsent(tp.topic(), k -> new long[2]);
				a[0] += s.leaderBytes();
				a[1] += s.totalBytes();
			});
		}
		return topicSizes;
	}

	/** Emptiness/last-write summary of the topic (from the ranges and maxTimestamps scans). */
	public TopicActivity activity(String topic) {
		return activities.computeIfAbsent(topic, this::computeActivity);
	}

	private TopicActivity computeActivity(String topic) {
		TopicDescription d = topics.get(topic);
		if (d == null) {
			return new TopicActivity(topic, null, false, null, null, false, "Could not describe the topic");
		}
		long messages = 0;
		boolean allKnown = true;
		boolean anyData = false;
		boolean allLatestZero = true;
		Long lastWrite = null;
		boolean lastComplete = true;
		String reason = null;
		for (TopicPartition tp : Topics.partitions(d)) {
			Offsets.Range r = ranges == null ? null : ranges.get(tp);
			if (r == null) {
				allKnown = false;
			}
			else {
				messages += r.count();
				anyData |= r.count() > 0;
				allLatestZero &= r.latest() == 0;
			}
			if (maxTimestamps == null) {
				continue;
			}
			Long ts = maxTimestamps.get(tp);
			if (ts != null && ts >= 0) {
				lastWrite = lastWrite == null ? ts : Math.max(lastWrite, ts);
			}
			else if (r == null || r.count() > 0) {
				lastComplete = false;
				if (reason == null) {
					String err = maxTimestampErrors.get(tp);
					reason = err != null ? err : r == null ? "Could not get offsets" : "Records have no timestamp";
				}
			}
		}
		Boolean empty = anyData ? Boolean.FALSE : allKnown && ranges != null ? Boolean.TRUE : null;
		boolean neverWritten = Boolean.TRUE.equals(empty) && allLatestZero;
		if (maxTimestamps == null) {
			lastComplete = false;
		}
		return new TopicActivity(topic, empty, neverWritten, allKnown && ranges != null ? messages : null, lastWrite,
				lastComplete, lastComplete ? null : reason);
	}

	/** Short topic row: partitions, replicationFactor and size (if known). */
	public Map<String, Object> topicRow(String topic) {
		Map<String, Object> m = Results.map("topic", topic, "partitions", partitions(topic), "replicationFactor",
				replicationFactor(topic));
		m.putAll(sizeFields(topic));
		return m;
	}

}
