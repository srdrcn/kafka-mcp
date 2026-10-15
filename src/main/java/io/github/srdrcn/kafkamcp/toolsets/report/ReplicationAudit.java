package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.toolsets.report.Findings.Severity;

/**
 * Replication audit rules (pure computation, testable): offline/URP/under-min-ISR partitions, RF=1,
 * min.insync.replicas ≥ RF, unclean.leader.election.enable=true, RF below the broker default.replication.factor
 * and, when racks are set, replicas piled onto a single rack.
 */
public final class ReplicationAudit {

	/** Maximum number of partition ids shown per item. */
	static final int MAX_PARTITIONS_SHOWN = 10;

	private ReplicationAudit() {
	}

	/** Audit input: topics, their configs (may be incomplete), the broker default RF and broker → rack. */
	public record Input(Map<String, TopicDescription> topics, Map<String, Config> configs,
			Integer defaultReplicationFactor, Map<Integer, String> racks, Topics.Reassignments reassignments) {
	}

	/**
	 * Applies the rules; returns the number of topics whose config could not be read (config rules are skipped for
	 * them).
	 */
	public static int evaluate(Input in, Findings f) {
		Set<String> distinctRacks = new HashSet<>(in.racks().values());
		boolean rackAware = distinctRacks.size() >= 2;
		int withoutConfig = 0;
		for (TopicDescription d : in.topics().values()) {
			String t = d.name();
			int rf = Topics.replicationFactor(d, in.reassignments());
			List<Integer> offline = new ArrayList<>();
			List<Integer> urp = new ArrayList<>();
			List<Integer> singleRack = new ArrayList<>();
			String rack = null;
			for (TopicPartitionInfo p : d.partitions()) {
				List<Integer> adding = in.reassignments().adding(new TopicPartition(t, p.partition()));
				if (Topics.offline(p)) {
					offline.add(p.partition());
				}
				else if (Topics.underReplicated(p, adding)) {
					urp.add(p.partition());
				}
				if (rackAware && p.replicas().size() > 1) {
					String r = sameRack(p.replicas(), in.racks());
					if (r != null) {
						singleRack.add(p.partition());
						rack = r;
					}
				}
			}
			if (!offline.isEmpty()) {
				f.add("offline_partitions", Severity.CRITICAL, "Partitions without a leader: no reads or writes possible",
						partitions(t, offline));
			}
			if (!urp.isEmpty()) {
				f.add("under_replicated", Severity.HIGH, "ISR smaller than the replica count (URP)", partitions(t, urp));
			}
			if (rf == 1) {
				f.add("replication_factor_1", Severity.HIGH,
						"RF=1: losing a broker/disk means data loss and downtime", Results.map("topic", t, "partitions",
								d.partitions().size(), "internal", d.isInternal() ? true : null));
			}
			if (!singleRack.isEmpty()) {
				Map<String, Object> item = partitions(t, singleRack);
				item.put("rack", rack);
				f.add("single_rack_replicas", Severity.MEDIUM,
						"Partitions with all replicas in one rack (unavailable if the rack is lost)", item);
			}
			if (in.defaultReplicationFactor() != null && rf > 0 && rf < in.defaultReplicationFactor()) {
				f.add("rf_below_default", Severity.LOW, "RF below the broker default.replication.factor",
						Results.map("topic", t, "replicationFactor", rf, "defaultReplicationFactor",
								in.defaultReplicationFactor()));
			}
			Config c = in.configs().get(t);
			if (c == null) {
				withoutConfig++;
				continue;
			}
			Long minIsr = ConfigValues.longValue(c, "min.insync.replicas");
			if (minIsr != null && rf > 0) {
				ConfigEntry e = c.get("min.insync.replicas");
				if (minIsr > rf) {
					f.add("min_isr_above_rf", Severity.CRITICAL,
							"min.insync.replicas > RF: older brokers always reject acks=all writes; newer versions cap min ISR at RF (no expected durability)",
							minIsrItem(t, rf, minIsr, e));
				}
				else if (minIsr == rf && rf > 1) {
					f.add("min_isr_equals_rf", Severity.MEDIUM,
							"min.insync.replicas = RF: acks=all producers stop when a single replica goes down",
							minIsrItem(t, rf, minIsr, e));
				}
				List<Integer> underMin = new ArrayList<>();
				for (TopicPartitionInfo p : d.partitions()) {
					if (underMinIsr(p, minIsr)) {
						underMin.add(p.partition());
					}
				}
				if (!underMin.isEmpty()) {
					Map<String, Object> item = partitions(t, underMin);
					item.put("minInsyncReplicas", minIsr);
					f.add("under_min_isr", Severity.CRITICAL,
							"ISR below min.insync.replicas: acks=all writes are being rejected now", item);
				}
			}
			if (ConfigValues.isTrue(c, "unclean.leader.election.enable")) {
				f.add("unclean_leader_election", Severity.MEDIUM,
						"unclean.leader.election.enable=true: leader changes may lose data",
						ConfigValues.item(t, c, "unclean.leader.election.enable"));
			}
		}
		return withoutConfig;
	}

	/**
	 * Is the partition below the effective min ISR: ISR &lt; min(min.insync.replicas, replica count). Brokers
	 * (verified on 3.9 and 4.1) cap min ISR at the replica count for acks=all writes; offline partitions are excluded.
	 */
	public static boolean underMinIsr(TopicPartitionInfo p, long minIsr) {
		return !Topics.offline(p) && p.isr().size() < Math.min(minIsr, p.replicas().size());
	}

	/**
	 * Min ISR counts for the cluster summary: partitions below the effective min ISR, topics with
	 * min.insync.replicas &gt; RF, and topics whose ISR shortfall cannot be evaluated because their config could not
	 * be read.
	 */
	public record MinIsrCounts(int underMinIsrPartitions, int aboveRfTopics, int unknownTopics) {
	}

	/** Computes {@link MinIsrCounts} with the same rules as {@link #evaluate} (topics without a config are skipped). */
	public static MinIsrCounts minIsrCounts(Collection<TopicDescription> topics, Map<String, Config> configs,
			Topics.Reassignments reassignments) {
		int under = 0;
		int above = 0;
		int unknown = 0;
		for (TopicDescription d : topics) {
			Long minIsr = ConfigValues.longValue(configs.get(d.name()), "min.insync.replicas");
			if (minIsr == null) {
				boolean isrShort = d.partitions()
					.stream()
					.anyMatch(p -> !Topics.offline(p) && p.isr().size() < p.replicas().size());
				unknown += isrShort ? 1 : 0;
				continue;
			}
			int rf = Topics.replicationFactor(d, reassignments);
			above += rf > 0 && minIsr > rf ? 1 : 0;
			for (TopicPartitionInfo p : d.partitions()) {
				under += underMinIsr(p, minIsr) ? 1 : 0;
			}
		}
		return new MinIsrCounts(under, above, unknown);
	}

	/** The rack if all replicas are in the same (known) rack, otherwise null. */
	static String sameRack(List<Node> replicas, Map<Integer, String> racks) {
		String first = null;
		for (Node n : replicas) {
			String r = racks.get(n.id());
			if (r == null) {
				return null;
			}
			if (first == null) {
				first = r;
			}
			else if (!first.equals(r)) {
				return null;
			}
		}
		return first;
	}

	private static Map<String, Object> minIsrItem(String topic, int rf, long minIsr, ConfigEntry e) {
		return Results.map("topic", topic, "replicationFactor", rf, "minInsyncReplicas", minIsr, "source",
				ConfigValues.source(e));
	}

	/** {topic, count, partitions (at most 10)}. */
	static Map<String, Object> partitions(String topic, List<Integer> ids) {
		return Results.map("topic", topic, "count", ids.size(), "partitions",
				ids.subList(0, Math.min(MAX_PARTITIONS_SHOWN, ids.size())), "partitionsOmitted",
				ids.size() > MAX_PARTITIONS_SHOWN ? ids.size() - MAX_PARTITIONS_SHOWN : null);
	}

}
