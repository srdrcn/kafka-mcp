package io.github.srdrcn.kafkamcp.kafka;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/** Disk usage: broker/directory/replica sizes with a single describeLogDirs. */
public final class LogDirs {

	private LogDirs() {
	}

	/** Partition size: the leader replica (logical) and the sum of all replicas. */
	public record PartitionSize(long leaderBytes, long totalBytes, int replicas) {
	}

	/** Error message for a broker missing from the cluster metadata (down/fenced). */
	public static final String OFFLINE = "broker unreachable (not in the cluster metadata)";

	/**
	 * Broker → (directory path → description); a single describeLogDirs, per-broker errors separately. Live
	 * brokers come from describeCluster; brokers still in replica lists but down are not queried (the admin
	 * client would hold the request until the timeout) and are returned with the {@link #OFFLINE} error.
	 */
	public static Futures.Each<Integer, Map<String, LogDirDescription>> describe(ToolCall call,
			Collection<Integer> brokerIds) {
		if (brokerIds.isEmpty()) {
			return new Futures.Each<>(new LinkedHashMap<>(), new LinkedHashMap<>());
		}
		List<Integer> live = call.await(call.admin().describeCluster().nodes()).stream().map(Node::id).toList();
		return describe(call, brokerIds, live);
	}

	/** {@link #describe(ToolCall, Collection)} when the live broker ids are already known. */
	public static Futures.Each<Integer, Map<String, LogDirDescription>> describe(ToolCall call,
			Collection<Integer> brokerIds, Collection<Integer> liveBrokerIds) {
		Set<Integer> query = new LinkedHashSet<>();
		Map<Integer, String> offline = new LinkedHashMap<>();
		for (Integer id : brokerIds) {
			if (liveBrokerIds.contains(id)) {
				query.add(id);
			}
			else {
				offline.put(id, OFFLINE);
			}
		}
		Futures.Each<Integer, Map<String, LogDirDescription>> each = query.isEmpty()
				? new Futures.Each<>(new LinkedHashMap<>(), new LinkedHashMap<>())
				: Futures.awaitEach(call, call.admin().describeLogDirs(query).descriptions());
		each.errors().putAll(offline);
		return each;
	}

	/** Ids of the brokers hosting replicas of the given topics (including brokers that are down). */
	public static Set<Integer> brokersHosting(Collection<TopicDescription> topics) {
		Set<Integer> out = new LinkedHashSet<>();
		for (TopicDescription d : topics) {
			for (TopicPartitionInfo p : d.partitions()) {
				for (Node n : p.replicas()) {
					out.add(n.id());
				}
			}
		}
		return out;
	}

	/**
	 * Size per partition (future replicas excluded). If the leader's size could not be read (offline or broker
	 * error), the largest replica counts as the leader size. Only partitions in {@code topics} are returned.
	 */
	public static Map<TopicPartition, PartitionSize> partitionSizes(Map<Integer, Map<String, LogDirDescription>> dirs,
			Map<String, TopicDescription> topics) {
		Map<TopicPartition, Integer> leaders = Topics.leaders(topics.values());
		Map<TopicPartition, long[]> acc = new HashMap<>();
		for (Map.Entry<Integer, Map<String, LogDirDescription>> b : dirs.entrySet()) {
			int broker = b.getKey();
			for (LogDirDescription dir : b.getValue().values()) {
				for (Map.Entry<TopicPartition, ReplicaInfo> r : dir.replicaInfos().entrySet()) {
					TopicPartition tp = r.getKey();
					if (r.getValue().isFuture() || !topics.containsKey(tp.topic())) {
						continue;
					}
					long size = r.getValue().size();
					long[] a = acc.computeIfAbsent(tp, k -> new long[5]);
					a[1] += size;
					a[2]++;
					a[3] = Math.max(a[3], size);
					if (Integer.valueOf(broker).equals(leaders.get(tp))) {
						a[0] = size;
						a[4] = 1;
					}
				}
			}
		}
		Map<TopicPartition, PartitionSize> out = new LinkedHashMap<>();
		acc.forEach((tp, a) -> out.put(tp, new PartitionSize(a[4] == 1 ? a[0] : a[3], a[1], (int) a[2])));
		return out;
	}

}
