package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
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
 * Replikasyon denetimi kuralları (saf hesap, test edilebilir): offline/URP/min ISR altı partition'lar, RF=1,
 * min.insync.replicas ≥ RF, unclean.leader.election.enable=true, broker default.replication.factor altı RF ve
 * rack'ler varken tek rack'e yığılmış replikalar.
 */
public final class ReplicationAudit {

	/** Öğe başına gösterilen en fazla partition numarası. */
	static final int MAX_PARTITIONS_SHOWN = 10;

	private ReplicationAudit() {
	}

	/** Denetim girdisi: topic'ler, config'leri (eksik olabilir), broker varsayılan RF'i ve broker → rack. */
	public record Input(Map<String, TopicDescription> topics, Map<String, Config> configs,
			Integer defaultReplicationFactor, Map<Integer, String> racks, Topics.Reassignments reassignments) {
	}

	/** Kuralları uygular; config'i okunamayan topic sayısını döner (config kuralları onlarda atlanır). */
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
				f.add("offline_partitions", Severity.CRITICAL, "Lideri olmayan partition'lar: okuma/yazma yapılamaz",
						partitions(t, offline));
			}
			if (!urp.isEmpty()) {
				f.add("under_replicated", Severity.HIGH, "ISR, replika sayısından az (URP)", partitions(t, urp));
			}
			if (rf == 1) {
				f.add("replication_factor_1", Severity.HIGH,
						"RF=1: broker/disk kaybında veri kaybı ve kesinti", Results.map("topic", t, "partitions",
								d.partitions().size(), "internal", d.isInternal() ? true : null));
			}
			if (!singleRack.isEmpty()) {
				Map<String, Object> item = partitions(t, singleRack);
				item.put("rack", rack);
				f.add("single_rack_replicas", Severity.MEDIUM,
						"Tüm replikaları tek rack'te olan partition'lar (rack kaybında erişilemez)", item);
			}
			if (in.defaultReplicationFactor() != null && rf > 0 && rf < in.defaultReplicationFactor()) {
				f.add("rf_below_default", Severity.LOW, "RF, broker default.replication.factor değerinin altında",
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
							"min.insync.replicas > RF: acks=all producer'lar hiç yazamaz",
							minIsrItem(t, rf, minIsr, e));
				}
				else if (minIsr == rf && rf > 1) {
					f.add("min_isr_equals_rf", Severity.MEDIUM,
							"min.insync.replicas = RF: tek replika düşünce acks=all producer'lar durur",
							minIsrItem(t, rf, minIsr, e));
				}
				List<Integer> underMin = new ArrayList<>();
				for (TopicPartitionInfo p : d.partitions()) {
					if (!Topics.offline(p) && p.isr().size() < minIsr && minIsr <= rf) {
						underMin.add(p.partition());
					}
				}
				if (!underMin.isEmpty()) {
					Map<String, Object> item = partitions(t, underMin);
					item.put("minInsyncReplicas", minIsr);
					f.add("under_min_isr", Severity.CRITICAL,
							"ISR, min.insync.replicas altında: acks=all yazmalar şu an reddediliyor", item);
				}
			}
			if (ConfigValues.isTrue(c, "unclean.leader.election.enable")) {
				f.add("unclean_leader_election", Severity.MEDIUM,
						"unclean.leader.election.enable=true: lider değişiminde veri kaybı olabilir",
						ConfigValues.item(t, c, "unclean.leader.election.enable"));
			}
		}
		return withoutConfig;
	}

	/** Replikaların hepsi aynı (bilinen) rack'teyse o rack, değilse null. */
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

	/** {topic, count, partitions (en fazla 10)}. */
	static Map<String, Object> partitions(String topic, List<Integer> ids) {
		return Results.map("topic", topic, "count", ids.size(), "partitions",
				ids.subList(0, Math.min(MAX_PARTITIONS_SHOWN, ids.size())), "partitionsOmitted",
				ids.size() > MAX_PARTITIONS_SHOWN ? ids.size() - MAX_PARTITIONS_SHOWN : null);
	}

}
