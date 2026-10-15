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
 * Partition sağlığı sayımı (tek describeTopics sonucundan): offline, URP (reassignment'ta eklenen replikalar
 * hariç), replika kaybı yüzünden min.insync.replicas altı, ISR'ı tam olsa da min ISR > replika sayısı olanlar,
 * tercih edilmeyen lider, ELR'si dolu (KRaft) partition'lar ve replika listelerinde olup canlı olmayan broker'lar.
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
	 * @param minIsrByTopic topic → min.insync.replicas (bilinmeyen topic'ler min ISR kontrolüne girmez)
	 * @param liveBrokers canlı broker id'leri (null: kontrol edilmez)
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

	/** ISR, replika kaybı yüzünden min.insync.replicas altında (acks=all yazmalar reddedilir). */
	public List<TopicPartition> underMinIsr() {
		return underMinIsr;
	}

	/** ISR tam ama min.insync.replicas replika sayısından büyük (config hatası; acks=all hiç yazamaz). */
	public List<TopicPartition> minIsrAboveReplicas() {
		return minIsrAboveReplicas;
	}

	public List<TopicPartition> nonPreferredLeader() {
		return nonPreferredLeader;
	}

	/** ELR'si dolu partition'lar (KRaft, KIP-966). */
	public List<TopicPartition> withElr() {
		return withElr;
	}

	/** Offline olup lastKnownElr'si dolu partition'lar (temiz kurtarma adayı var). */
	public List<TopicPartition> offlineWithLastKnownElr() {
		return offlineWithLastKnownElr;
	}

	/** Replika listelerinde geçen broker'lar. */
	public Set<Integer> referencedBrokers() {
		return referencedBrokers;
	}

	/** Replika listelerinde olup canlı olmayan broker'lar. */
	public Set<Integer> missingBrokers() {
		return missingBrokers;
	}

}
