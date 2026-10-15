package io.github.srdrcn.kafkamcp.toolsets.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Partition reassignment araçları: devam edenleri listeleme, başlatma/iptal. */
@Component
public class ReassignmentTools {

	public List<ToolSpec> tools() {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("topic", P.string("Topic adı"));
		item.put("partition", P.integer("Partition numarası"));
		item.put("replicas", Params.integerArray("Hedef replika broker id'leri (ilk = tercih edilen lider); boşsa iptal"));
		return List.of(
				ToolSpec.read("partition_reassignments_list",
						"Devam eden partition reassignment'ları listeler: replikalar, eklenen ve çıkarılanlar")
					.listParams()
					.handler(this::list),
				ToolSpec.write("partition_reassignments_alter",
						"Partition reassignment başlatır ya da iptal eder (replicas boşsa iptal); partition bazında sonuç döner")
					.required("reassignments",
							P.objectArray("Reassignment listesi", item, List.of("topic", "partition")))
					.handler(this::alter));
	}

	Object list(ToolCall call) {
		Map<TopicPartition, PartitionReassignment> all = call
			.await(call.admin().listPartitionReassignments().reassignments());
		List<TopicPartition> tps = new ArrayList<>(all.keySet());
		tps.sort(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
		List<Map<String, Object>> rows = new ArrayList<>();
		for (TopicPartition tp : tps) {
			if (!call.matches(tp.topic())) {
				continue;
			}
			PartitionReassignment r = all.get(tp);
			rows.add(Results.map("topic", tp.topic(), "partition", tp.partition(), "replicas", r.replicas(), "adding",
					r.addingReplicas(), "removing", r.removingReplicas()));
		}
		return Results.page("reassignments", rows, call.limit());
	}

	Object alter(ToolCall call) {
		Object raw = call.args().get("reassignments");
		if (!(raw instanceof Collection<?> items) || items.isEmpty()) {
			throw new ToolException("reassignments boş olmayan bir dizi olmalı");
		}
		Map<TopicPartition, List<Integer>> requested = new LinkedHashMap<>();
		for (Object o : items) {
			if (!(o instanceof Map<?, ?> m)) {
				throw new ToolException("reassignments elemanları {topic, partition, replicas} nesnesi olmalı");
			}
			Object topic = m.get("topic");
			Object partition = m.get("partition");
			if (topic == null || String.valueOf(topic).isBlank() || partition == null) {
				throw new ToolException("Her reassignment için topic ve partition gerekli");
			}
			TopicPartition tp = new TopicPartition(String.valueOf(topic).trim(), Params.toInt(partition, "partition"));
			// sadece eksik/null/boş replicas iptal demek; dizi dışı değerler ("2,3", "[2,3]", 2) de ayrıştırılır
			List<Integer> replicas = Params.intList(m.get("replicas"), "replicas");
			if (new HashSet<>(replicas).size() != replicas.size()) {
				throw new ToolException("replicas tekrar eden broker id içeriyor: " + tp + " " + replicas);
			}
			if (requested.put(tp, replicas) != null) {
				throw new ToolException("Aynı partition birden fazla kez verilmiş: " + tp);
			}
		}
		validate(call, requested);
		Map<TopicPartition, Optional<NewPartitionReassignment>> req = new LinkedHashMap<>();
		requested.forEach((tp, replicas) -> req.put(tp,
				replicas.isEmpty() ? Optional.empty() : Optional.of(new NewPartitionReassignment(replicas))));
		Futures.Each<TopicPartition, Void> result = Futures.awaitEach(call,
				call.admin().alterPartitionReassignments(req).values());
		List<Map<String, Object>> rows = new ArrayList<>();
		int ok = 0;
		for (Map.Entry<TopicPartition, List<Integer>> e : requested.entrySet()) {
			TopicPartition tp = e.getKey();
			boolean success = result.values().containsKey(tp);
			if (success) {
				ok++;
			}
			rows.add(Results.map("topic", tp.topic(), "partition", tp.partition(), "action",
					e.getValue().isEmpty() ? "cancel" : "reassign", "replicas",
					e.getValue().isEmpty() ? null : e.getValue(), "result", success ? "ok" : "error", "error",
					result.errors().get(tp)));
		}
		return Results.map("requested", requested.size(), "ok", ok, "failed", requested.size() - ok, "results", rows,
				"hint", ok > 0 ? "İlerlemeyi partition_reassignments_list ile izleyin" : null);
	}

	/** Topic/partition'ların ve broker id'lerinin var olduğunu doğrular. */
	private static void validate(ToolCall call, Map<TopicPartition, List<Integer>> requested) {
		Set<String> topics = new LinkedHashSet<>();
		requested.keySet().forEach(tp -> topics.add(tp.topic()));
		Futures.Each<String, TopicDescription> described = Topics.describe(call, topics);
		for (String t : topics) {
			if (!described.values().containsKey(t)) {
				throw new ToolException("Topic bulunamadı: " + t + " (" + described.errors().get(t) + ")");
			}
		}
		for (TopicPartition tp : requested.keySet()) {
			int count = described.values().get(tp.topic()).partitions().size();
			if (tp.partition() < 0 || tp.partition() >= count) {
				throw new ToolException("Geçersiz partition: " + tp + " (topic'te " + count + " partition var)");
			}
		}
		List<Integer> brokers = ClusterMeta.describe(call, false).brokerIds();
		for (Map.Entry<TopicPartition, List<Integer>> e : requested.entrySet()) {
			for (Integer b : e.getValue()) {
				if (!brokers.contains(b)) {
					throw new ToolException("Broker bulunamadı: " + b + " (" + e.getKey() + "); mevcut broker'lar: " + brokers);
				}
			}
		}
	}

}
