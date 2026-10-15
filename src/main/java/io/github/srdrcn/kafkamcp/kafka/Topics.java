package io.github.srdrcn.kafkamcp.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Topic listing/description helpers (a single listTopics, a single describeTopics). */
public final class Topics {

	/**
	 * System topics whose deletion is refused: Connect internal topics (*connect-configs/-offsets/-status,
	 * Strimzi's *connect-cluster-* names, the *connect_* names from the Debezium examples), Schema Registry,
	 * MirrorMaker 2.
	 */
	private static final Pattern SYSTEM_TOPIC = Pattern.compile(
			"^(_schemas|_confluent.*|.*connect(-cluster)?[-_](configs|offsets|status|statuses)|.*\\.internal)$");

	/** Maximum wait for listPartitionReassignments; the tool carries on even if the controller does not respond. */
	static final long REASSIGNMENTS_WAIT_MS = 3_000;

	private Topics() {
	}

	/** Topic name → internal flag (sorted by name); internal topics are excluded if includeInternal is false. */
	public static Map<String, Boolean> list(ToolCall call, boolean includeInternal) {
		Collection<TopicListing> listings = call
			.await(call.admin().listTopics(new ListTopicsOptions().listInternal(includeInternal)).listings());
		Map<String, Boolean> out = new TreeMap<>();
		for (TopicListing l : listings) {
			out.put(l.name(), l.isInternal());
		}
		return out;
	}

	/** A single describeTopics call; topics deleted in the meantime or unreachable are returned in the errors. */
	public static Futures.Each<String, TopicDescription> describe(ToolCall call, Collection<String> names) {
		if (names.isEmpty()) {
			return new Futures.Each<>(new LinkedHashMap<>(), new LinkedHashMap<>());
		}
		return Futures.awaitEach(call, call.admin().describeTopics(names).topicNameValues());
	}

	/** listTopics + a single describeTopics: descriptions of all topics (sorted by name). */
	public static Map<String, TopicDescription> describeAll(ToolCall call, boolean includeInternal) {
		return new TreeMap<>(describe(call, list(call, includeInternal).keySet()).requireAny().values());
	}

	/** Single topic; "Topic not found" error if it does not exist. */
	public static TopicDescription describeOne(ToolCall call, String topic) {
		try {
			return call.await(call.admin().describeTopics(List.of(topic)).allTopicNames()).get(topic);
		}
		catch (ToolException e) {
			throw notFound(e, topic);
		}
	}

	/** Turns an UnknownTopicOrPartition error into a "Topic not found: x" message; returns any other error as is. */
	public static ToolException notFound(ToolException e, String topic) {
		if (KafkaErrors.unwrap(e.getCause()) instanceof UnknownTopicOrPartitionException) {
			return new ToolException("Topic not found: " + topic + " (see topics_list)", e.getCause());
		}
		return e;
	}

	/** All partitions of the topic. */
	public static List<TopicPartition> partitions(TopicDescription d) {
		List<TopicPartition> out = new ArrayList<>(d.partitions().size());
		for (TopicPartitionInfo p : d.partitions()) {
			out.add(new TopicPartition(d.name(), p.partition()));
		}
		return out;
	}

	/** Partitions that have a leader (not offline); for offset/producer queries. */
	public static List<TopicPartition> onlinePartitions(Collection<TopicDescription> topics) {
		List<TopicPartition> out = new ArrayList<>();
		for (TopicDescription d : topics) {
			for (TopicPartitionInfo p : d.partitions()) {
				if (!offline(p)) {
					out.add(new TopicPartition(d.name(), p.partition()));
				}
			}
		}
		return out;
	}

	/** Does the partition have no leader? */
	public static boolean offline(TopicPartitionInfo p) {
		return p.leader() == null || p.leader().isEmpty() || p.leader().id() < 0;
	}

	/**
	 * Is it a URP? Like Kafka's UnderReplicatedPartitions metric: a partition with a leader whose ISR is smaller
	 * than its replica count, excluding replicas being added by a reassignment. Offline partitions are counted
	 * separately.
	 */
	public static boolean underReplicated(TopicPartitionInfo p, Collection<Integer> adding) {
		return !offline(p) && p.isr().size() < replicaCount(p, adding);
	}

	/** Replica count excluding replicas being added by a reassignment. */
	public static int replicaCount(TopicPartitionInfo p, Collection<Integer> adding) {
		if (adding.isEmpty()) {
			return p.replicas().size();
		}
		return (int) p.replicas().stream().filter(n -> !adding.contains(n.id())).count();
	}

	/** Leader id or null. */
	public static Integer leader(TopicPartitionInfo p) {
		return offline(p) ? null : p.leader().id();
	}

	/** Preferred leader (first replica) id or null. */
	public static Integer preferredLeader(TopicPartitionInfo p) {
		return p.replicas().isEmpty() ? null : p.replicas().get(0).id();
	}

	/** Is the leader not the preferred replica (offline excluded)? */
	public static boolean nonPreferredLeader(TopicPartitionInfo p) {
		Integer leader = leader(p);
		return leader != null && !leader.equals(preferredLeader(p));
	}

	/** Replication factor: the largest replica count of the partitions (excluding replicas a reassignment is adding). */
	public static int replicationFactor(TopicDescription d, Reassignments reassignments) {
		return d.partitions()
			.stream()
			.mapToInt(p -> replicaCount(p, reassignments.adding(new TopicPartition(d.name(), p.partition()))))
			.max()
			.orElse(0);
	}

	/**
	 * Ongoing partition reassignments. If {@code error} is set (e.g. no Cluster:Describe permission) the list is
	 * empty; URP/RF calculations then cannot exclude the replicas being added.
	 */
	public record Reassignments(Map<TopicPartition, PartitionReassignment> byPartition, String error) {

		/** Empty value for calculations without reassignment info. */
		public static final Reassignments NONE = new Reassignments(Map.of(), null);

		/** Ids of the replicas being added to the partition (empty if none). */
		public List<Integer> adding(TopicPartition tp) {
			PartitionReassignment r = byPartition.get(tp);
			return r == null ? List.of() : r.addingReplicas();
		}

	}

	/** Sends the listPartitionReassignments request (does not wait); get the result with {@link #reassignments}. */
	public static KafkaFuture<Map<TopicPartition, PartitionReassignment>> submitReassignments(ToolCall call) {
		return call.admin().listPartitionReassignments().reassignments();
	}

	/** Waits at most a few seconds for a pending listPartitionReassignments; empty + error message on error/timeout. */
	public static Reassignments reassignments(ToolCall call,
			KafkaFuture<Map<TopicPartition, PartitionReassignment>> pending) {
		try {
			return new Reassignments(Futures.awaitAtMost(call, pending, REASSIGNMENTS_WAIT_MS), null);
		}
		catch (ToolException e) {
			return new Reassignments(Map.of(), e.getMessage());
		}
	}

	/**
	 * Eligible leader replicas (KIP-966) ids; can only be non-empty on KRaft (with ELR enabled). ZooKeeper
	 * brokers do not return this field (the client gives null): returns null, no NPE.
	 */
	public static List<Integer> elr(TopicPartitionInfo p) {
		return p.elr() == null ? null : ids(p.elr());
	}

	/** Last known ELR ids (KRaft); null on ZooKeeper. */
	public static List<Integer> lastKnownElr(TopicPartitionInfo p) {
		return p.lastKnownElr() == null ? null : ids(p.lastKnownElr());
	}

	/** Topic id; null if unknown (older brokers return the zero UUID). */
	public static String topicId(TopicDescription d) {
		Uuid id = d.topicId();
		return id == null || Uuid.ZERO_UUID.equals(id) ? null : id.toString();
	}

	/** Converts a node list to a list of ids. */
	public static List<Integer> ids(List<Node> nodes) {
		return nodes.stream().map(Node::id).toList();
	}

	/** Partition → leader id (offline ones excluded). */
	public static Map<TopicPartition, Integer> leaders(Collection<TopicDescription> topics) {
		Map<TopicPartition, Integer> out = new LinkedHashMap<>();
		for (TopicDescription d : topics) {
			for (TopicPartitionInfo p : d.partitions()) {
				Integer l = leader(p);
				if (l != null) {
					out.put(new TopicPartition(d.name(), p.partition()), l);
				}
			}
		}
		return out;
	}

	/** The reason if this is an internal/system topic that must not be deleted/changed, otherwise null. */
	public static String protectedReason(String name, boolean internal) {
		if (internal || name.startsWith("__")) {
			return "Kafka internal topic";
		}
		if (SYSTEM_TOPIC.matcher(name).matches()) {
			return "system topic (Connect/Schema Registry/MirrorMaker)";
		}
		return null;
	}

}
