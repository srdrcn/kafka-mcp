package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import io.github.srdrcn.kafkamcp.kafka.Topics;

/**
 * Partition health counts (from a single describeTopics result): offline, URP (excluding replicas being added by a
 * reassignment), below min.insync.replicas due to replica loss, min ISR > replica count even with a full ISR,
 * non-preferred leader, partitions with a non-empty ELR (KRaft) and brokers listed in replica lists that are not
 * alive.
 */
public final class PartitionHealth {

	private final List<TopicPartition> offline = new ArrayList<>();

	private final List<TopicPartition> underReplicated = new ArrayList<>();

	private final List<TopicPartition> underMinIsr = new ArrayList<>();

	private final List<TopicPartition> minIsrAboveReplicas = new ArrayList<>();

	private final List<TopicPartition> nonPreferredLeader = new ArrayList<>();

	private final List<TopicPartition> withElr = new ArrayList<>();

	private final List<TopicPartition> offlineWithLastKnownElr = new ArrayList<>();

	private final Set<Integer> referencedBrokers = new TreeSet<>();

	private final Set<Integer> missingBrokers = new TreeSet<>();

	private int partitions;

	private PartitionHealth() {
	}

	/**
	 * @param minIsrByTopic topic → min.insync.replicas (unknown topics are skipped by the min ISR check)
	 * @param liveBrokers live broker ids (null: not checked)
	 */
	public static PartitionHealth of(Collection<TopicDescription> topics, Topics.Reassignments reassignments,
			Map<String, Integer> minIsrByTopic, Collection<Integer> liveBrokers) {
		PartitionHealth h = new PartitionHealth();
		Topics.Reassignments r = reassignments == null ? Topics.Reassignments.NONE : reassignments;
		for (TopicDescription d : topics) {
			Integer minIsr = minIsrByTopic == null ? null : minIsrByTopic.get(d.name());
			for (TopicPartitionInfo p : d.partitions()) {
				h.partitions++;
				TopicPartition tp = new TopicPartition(d.name(), p.partition());
				List<Integer> adding = r.adding(tp);
				int replicas = Topics.replicaCount(p, adding);
				boolean offline = Topics.offline(p);
				if (offline) {
					h.offline.add(tp);
					if (p.lastKnownElr() != null && !p.lastKnownElr().isEmpty()) {
						h.offlineWithLastKnownElr.add(tp);
					}
				}
				if (Topics.underReplicated(p, adding)) {
					h.underReplicated.add(tp);
				}
				if (!offline && minIsr != null && p.isr().size() < minIsr) {
					if (p.isr().size() < replicas) {
						h.underMinIsr.add(tp);
					}
					else {
						h.minIsrAboveReplicas.add(tp);
					}
				}
				if (Topics.nonPreferredLeader(p)) {
					h.nonPreferredLeader.add(tp);
				}
				if (p.elr() != null && !p.elr().isEmpty()) {
					h.withElr.add(tp);
				}
				for (Node n : p.replicas()) {
					h.referencedBrokers.add(n.id());
					if (liveBrokers != null && !liveBrokers.contains(n.id())) {
						h.missingBrokers.add(n.id());
					}
				}
			}
		}
		return h;
	}

	public int partitions() {
		return partitions;
	}

	public List<TopicPartition> offline() {
		return offline;
	}

	public List<TopicPartition> underReplicated() {
		return underReplicated;
	}

	/** ISR is below min.insync.replicas due to replica loss (acks=all writes are rejected). */
	public List<TopicPartition> underMinIsr() {
		return underMinIsr;
	}

	/** ISR is full but min.insync.replicas is greater than the replica count (config error; acks=all can never write). */
	public List<TopicPartition> minIsrAboveReplicas() {
		return minIsrAboveReplicas;
	}

	public List<TopicPartition> nonPreferredLeader() {
		return nonPreferredLeader;
	}

	/** Partitions with a non-empty ELR (KRaft, KIP-966). */
	public List<TopicPartition> withElr() {
		return withElr;
	}

	/** Offline partitions with a non-empty lastKnownElr (a clean recovery candidate exists). */
	public List<TopicPartition> offlineWithLastKnownElr() {
		return offlineWithLastKnownElr;
	}

	/** Brokers that appear in replica lists. */
	public Set<Integer> referencedBrokers() {
		return referencedBrokers;
	}

	/** Brokers in replica lists that are not alive. */
	public Set<Integer> missingBrokers() {
		return missingBrokers;
	}

}
