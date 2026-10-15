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

/** Partition reassignment tools: list ongoing ones, start/cancel. */
@Component
public class ReassignmentTools {

	public List<ToolSpec> tools() {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("topic", P.string("Topic name"));
		item.put("partition", P.integer("Partition number"));
		item.put("replicas", Params.integerArray("Target replica broker ids (first = preferred leader); empty cancels"));
		return List.of(
				ToolSpec.read("partition_reassignments_list",
						"List ongoing partition reassignments: current, adding and removing replicas")
					.listParams()
					.handler(this::list),
				ToolSpec.write("partition_reassignments_alter",
						"Start or cancel partition reassignments (empty replicas cancels); returns a result per partition")
					.required("reassignments",
							P.objectArray("List of reassignments", item, List.of("topic", "partition")))
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
			throw new ToolException("reassignments must be a non-empty array");
		}
		Map<TopicPartition, List<Integer>> requested = new LinkedHashMap<>();
		for (Object o : items) {
			if (!(o instanceof Map<?, ?> m)) {
				throw new ToolException("reassignments items must be {topic, partition, replicas} objects");
			}
			Object topic = m.get("topic");
			Object partition = m.get("partition");
			if (topic == null || String.valueOf(topic).isBlank() || partition == null) {
				throw new ToolException("Each reassignment needs topic and partition");
			}
			TopicPartition tp = new TopicPartition(String.valueOf(topic).trim(), Params.toInt(partition, "partition"));
			// only missing/null/empty replicas means cancel; non-array values ("2,3", "[2,3]", 2) are parsed too
			List<Integer> replicas = Params.intList(m.get("replicas"), "replicas");
			if (new HashSet<>(replicas).size() != replicas.size()) {
				throw new ToolException("replicas contains duplicate broker ids: " + tp + " " + replicas);
			}
			if (requested.put(tp, replicas) != null) {
				throw new ToolException("Same partition given more than once: " + tp);
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
				"hint", ok > 0 ? "Track progress with partition_reassignments_list" : null);
	}

	/** Checks that the topics/partitions and broker ids exist. */
	private static void validate(ToolCall call, Map<TopicPartition, List<Integer>> requested) {
		Set<String> topics = new LinkedHashSet<>();
		requested.keySet().forEach(tp -> topics.add(tp.topic()));
		Futures.Each<String, TopicDescription> described = Topics.describe(call, topics);
		for (String t : topics) {
			if (!described.values().containsKey(t)) {
				throw new ToolException("Topic not found: " + t + " (" + described.errors().get(t) + ")");
			}
		}
		for (TopicPartition tp : requested.keySet()) {
			int count = described.values().get(tp.topic()).partitions().size();
			if (tp.partition() < 0 || tp.partition() >= count) {
				throw new ToolException("Invalid partition " + tp.partition() + " for topic " + tp.topic() + " (valid: 0.."
						+ (count - 1) + ")");
			}
		}
		List<Integer> brokers = ClusterMeta.describe(call, false).brokerIds();
		for (Map.Entry<TopicPartition, List<Integer>> e : requested.entrySet()) {
			for (Integer b : e.getValue()) {
				if (!brokers.contains(b)) {
					throw new ToolException("Broker not found: " + b + " (" + e.getKey() + "); available brokers: " + brokers);
				}
			}
		}
	}

}
