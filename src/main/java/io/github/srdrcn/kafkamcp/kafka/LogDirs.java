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

/** Disk kullanımı: tek describeLogDirs ile broker/dizin/replika boyutları. */
public final class LogDirs {

	private LogDirs() {
	}

	/** Partition boyutu: lider replika (mantıksal) ve tüm replikaların toplamı. */
	public record PartitionSize(long leaderBytes, long totalBytes, int replicas) {
	}

	/** Cluster metadata'sında olmayan (kapalı/fenced) broker için hata mesajı. */
	public static final String OFFLINE = "broker erişilemiyor (cluster metadata'sında yok)";

	/**
	 * Broker → (dizin yolu → açıklama); tek describeLogDirs, broker bazında hatalar ayrı. Canlı broker'lar
	 * describeCluster ile alınır; replika listesinde kalmış ama kapalı broker'lar sorgulanmaz (admin client
	 * onlar için isteği zaman aşımına kadar bekletir), {@link #OFFLINE} hatasıyla döner.
	 */
	public static Futures.Each<Integer, Map<String, LogDirDescription>> describe(ToolCall call,
			Collection<Integer> brokerIds) {
		if (brokerIds.isEmpty()) {
			return new Futures.Each<>(new LinkedHashMap<>(), new LinkedHashMap<>());
		}
		List<Integer> live = call.await(call.admin().describeCluster().nodes()).stream().map(Node::id).toList();
		return describe(call, brokerIds, live);
	}

	/** {@link #describe(ToolCall, Collection)}; canlı broker id'leri zaten biliniyorsa. */
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

	/** Verilen topic'lerin replikalarını barındıran broker id'leri (kapalı olanlar dahil). */
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
	 * Partition başına boyut (future replikalar hariç). Liderin boyutu okunamadıysa (offline ya da broker
	 * hatası) en büyük replika lider boyutu sayılır. Sadece {@code topics} içindeki partition'lar döner.
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
