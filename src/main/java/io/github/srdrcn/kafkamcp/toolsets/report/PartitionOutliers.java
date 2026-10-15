package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.LogDirs;
import io.github.srdrcn.kafkamcp.kafka.Topics;

/**
 * Partition aykırılıkları (saf hesap, test edilebilir): cluster medyanının çok üstünde partition sayısı,
 * topic içinde ortalamanın çok üstünde boyutlu partition'lar (skew) ve broker başına lider dengesizliği.
 */
public final class PartitionOutliers {

	private PartitionOutliers() {
	}

	/** Eşikler: medyan katı ve en az partition, skew katı ve en küçük boyut, lider sapma yüzdesi. */
	public record Thresholds(int partitionFactor, int minPartitions, int skewFactor, long minSizeBytes,
			int leaderImbalancePct) {
	}

	/** Partition sayısı aykırı topic. */
	public record CountOutlier(String topic, int partitions, double timesMedian) {
	}

	/** Ortalamanın çok üstündeki partition. */
	public record SkewedPartition(int partition, long sizeBytes, double timesAverage) {
	}

	/** Skew'li topic: ortalama partition boyutu ve aykırı partition'lar (boyuta göre azalan). */
	public record SkewedTopic(String topic, int partitions, long averageBytes, List<SkewedPartition> skewed) {
	}

	/** Broker yükü: lider/replika sayısı, tercih edilmeyen lider sayısı, ortalamadan sapma (%). */
	public record BrokerLoad(int id, int leaders, int replicas, int nonPreferredLeaders, double deviationPct,
			boolean imbalanced) {
	}

	/** Medyan (boşsa 0). */
	public static double median(Collection<Integer> values) {
		if (values.isEmpty()) {
			return 0;
		}
		List<Integer> sorted = new ArrayList<>(values);
		sorted.sort(null);
		int n = sorted.size();
		return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
	}

	/** partitions &gt; factor × medyan ve &gt; minPartitions olan topic'ler (partition sayısına göre azalan). */
	public static List<CountOutlier> countOutliers(Map<String, Integer> partitionsByTopic, double median,
			Thresholds th) {
		List<CountOutlier> out = new ArrayList<>();
		partitionsByTopic.forEach((t, n) -> {
			if (n > th.minPartitions() && n > th.partitionFactor() * median) {
				out.add(new CountOutlier(t, n, median > 0 ? Format.round(n / median, 1) : 0));
			}
		});
		out.sort(Comparator.comparingInt(CountOutlier::partitions).reversed().thenComparing(CountOutlier::topic));
		return out;
	}

	/**
	 * Boyutu topic ortalamasının skewFactor katından büyük ve en az minSizeBytes olan partition'lar. Boyutu
	 * bilinen en az iki partition'ı olan topic'ler incelenir.
	 */
	public static List<SkewedTopic> skew(Collection<TopicDescription> topics,
			Map<TopicPartition, LogDirs.PartitionSize> sizes, Thresholds th) {
		List<SkewedTopic> out = new ArrayList<>();
		for (TopicDescription d : topics) {
			Map<Integer, Long> known = new TreeMap<>();
			for (TopicPartitionInfo p : d.partitions()) {
				LogDirs.PartitionSize s = sizes.get(new TopicPartition(d.name(), p.partition()));
				if (s != null) {
					known.put(p.partition(), s.leaderBytes());
				}
			}
			if (known.size() < 2) {
				continue;
			}
			double avg = known.values().stream().mapToLong(Long::longValue).sum() / (double) known.size();
			List<SkewedPartition> skewed = new ArrayList<>();
			known.forEach((p, size) -> {
				if (size >= th.minSizeBytes() && size > th.skewFactor() * avg && avg > 0) {
					skewed.add(new SkewedPartition(p, size, Format.round(size / avg, 1)));
				}
			});
			if (!skewed.isEmpty()) {
				skewed.sort(Comparator.comparingLong(SkewedPartition::sizeBytes).reversed());
				out.add(new SkewedTopic(d.name(), d.partitions().size(), Math.round(avg), skewed));
			}
		}
		out.sort(Comparator.comparingLong((SkewedTopic s) -> s.skewed().get(0).sizeBytes())
			.reversed()
			.thenComparing(SkewedTopic::topic));
		return out;
	}

	/**
	 * Broker başına lider/replika sayıları ve ortalamadan sapma. Broker kümesi: verilen canlı broker'lar ve
	 * replika listelerinde geçenler (lideri hiç olmayan broker -100% sapma görünür).
	 */
	public static List<BrokerLoad> leaderBalance(Collection<TopicDescription> topics, Collection<Integer> brokerIds,
			int imbalancePct) {
		Set<Integer> ids = new TreeSet<>(brokerIds);
		Map<Integer, int[]> counts = new TreeMap<>();
		int totalLeaders = 0;
		for (TopicDescription d : topics) {
			for (TopicPartitionInfo p : d.partitions()) {
				for (Node r : p.replicas()) {
					ids.add(r.id());
					counts.computeIfAbsent(r.id(), k -> new int[3])[1]++;
				}
				Integer leader = Topics.leader(p);
				if (leader != null) {
					ids.add(leader);
					int[] c = counts.computeIfAbsent(leader, k -> new int[3]);
					c[0]++;
					totalLeaders++;
					if (Topics.nonPreferredLeader(p)) {
						c[2]++;
					}
				}
			}
		}
		double avg = ids.isEmpty() ? 0 : totalLeaders / (double) ids.size();
		List<BrokerLoad> out = new ArrayList<>();
		for (Integer id : ids) {
			int[] c = counts.getOrDefault(id, new int[3]);
			double dev = avg > 0 ? Format.round((c[0] - avg) / avg * 100, 1) : 0;
			out.add(new BrokerLoad(id, c[0], c[1], c[2], dev, Math.abs(dev) > imbalancePct));
		}
		return out;
	}

}
