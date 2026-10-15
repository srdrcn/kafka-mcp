package io.github.srdrcn.kafkamcp.toolsets.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.Configs;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Topic write tools: create, clone, config update, partition increase, delete. */
@Component
public class TopicWriteTools {

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.write("topics_create", "Create a topic (partitions and RF default to the broker defaults)")
					.required("topic", P.string("Topic name"))
					.param("partitions", P.integer("Partition count (default: broker default)", 1, 100_000))
					.param("replicationFactor", P.integer("Replication factor (default: broker default)", 1, 32))
					.param("configs", P.stringMap("Topic configs (key → value)"))
					.handler(this::topicsCreate),
				ToolSpec.write("topics_clone",
						"Create a topic with the partition count, RF and topic-level configs of a source topic (data is not copied)")
					.required("source", P.string("Source topic"))
					.required("target", P.string("Topic to create"))
					.param("partitions", P.integer("Partition count (default: same as the source)", 1, 100_000))
					.param("replicationFactor", P.integer("Replication factor (default: same as the source)", 1, 32))
					.param("configs", P.stringMap("Configs that override the source configs (key → value)"))
					.handler(this::topicsClone),
				ToolSpec.write("topics_config_update", "Change topic configs (set/delete); returns before/after values")
					.required("topic", P.string("Topic name"))
					.param("set", P.stringMap("Configs to set (key → value)"))
					.param("delete", P.stringArray("Config names to delete (revert to default)"))
					.handler(this::topicsConfigUpdate),
				ToolSpec.write("topics_partitions_increase",
						"Increase the partition count of a topic (cannot be decreased; changes the key→partition mapping of keyed data)")
					.required("topic", P.string("Topic name"))
					.required("totalCount", P.integer("New total partition count (greater than the current one)", 2, 100_000))
					.handler(this::topicsPartitionsIncrease),
				ToolSpec.destructive("topics_delete", "Delete a topic and all its data (internal and system topics are refused)")
					.required("topic", P.string("Name of the topic to delete (a single topic)"))
					.handler(this::topicsDelete));
	}

	Object topicsCreate(ToolCall call) {
		return create(call, call.str("topic"), call.optInt("partitions"), call.optInt("replicationFactor"),
				configs(call));
	}

	Object topicsClone(ToolCall call) {
		String source = call.str("source");
		String target = call.str("target");
		KafkaFuture<Map<TopicPartition, PartitionReassignment>> pendingReassignments = Topics
			.submitReassignments(call);
		TopicDescription d = Topics.describeOne(call, source);
		Map<String, String> copied = new TreeMap<>();
		for (ConfigEntry e : Configs.describeOne(call, Configs.topic(source)).entries()) {
			if (e.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG && !e.isSensitive() && e.value() != null) {
				copied.put(e.name(), e.value());
			}
		}
		Map<String, String> overrides = configs(call);
		Map<String, String> merged = new LinkedHashMap<>(copied);
		merged.putAll(overrides);
		Integer partitions = call.optInt("partitions");
		Integer rf = call.optInt("replicationFactor");
		if (rf == null) {
			// replicas added by an ongoing reassignment must not inflate the RF
			rf = Topics.replicationFactor(d, Topics.reassignments(call, pendingReassignments));
		}
		Map<String, Object> out = create(call, target, partitions == null ? d.partitions().size() : partitions, rf,
				merged);
		out.put("source", source);
		out.put("copiedConfigs", copied);
		if (!overrides.isEmpty()) {
			out.put("overriddenConfigs", overrides);
		}
		out.put("note", "Only the structure was copied; messages were not copied");
		return out;
	}

	Object topicsConfigUpdate(ToolCall call) {
		String topic = call.str("topic");
		List<Map<String, Object>> changes = Configs.alter(call, Configs.topic(topic), call.stringMap("set"),
				call.strings("delete"));
		return Results.map("topic", topic, "changes", changes);
	}

	Object topicsPartitionsIncrease(ToolCall call) {
		String topic = call.str("topic");
		int total = Math.toIntExact(call.requiredLong("totalCount"));
		TopicDescription d = Topics.describeOne(call, topic);
		int current = d.partitions().size();
		if (total <= current) {
			throw new ToolException("totalCount (" + total + ") must be greater than the current partition count ("
					+ current + "); the partition count cannot be decreased");
		}
		call.await(call.admin().createPartitions(Map.of(topic, NewPartitions.increaseTo(total))).all());
		return Results.map("topic", topic, "before", current, "after", total, "warning",
				"The key → partition mapping changed for keyed messages: new messages with the same key may go to "
						+ "a different partition (affects per-key ordering and compaction)");
	}

	Object topicsDelete(ToolCall call) {
		String topic = call.str("topic");
		TopicDescription d = Topics.describeOne(call, topic);
		String reason = Topics.protectedReason(topic, d.isInternal());
		if (reason != null) {
			throw new ToolException("Delete refused: " + topic + " is a " + reason);
		}
		call.await(call.admin().deleteTopics(List.of(topic)).all());
		return Results.map("deleted", topic, "partitions", d.partitions().size());
	}

	private Map<String, Object> create(ToolCall call, String topic, Integer partitions, Integer rf,
			Map<String, String> configs) {
		if (partitions != null && partitions < 1) {
			throw new ToolException("partitions must be at least 1");
		}
		if (rf != null) {
			if (rf < 1) {
				throw new ToolException("replicationFactor must be at least 1");
			}
			int brokers = ClusterMeta.describe(call, false).brokers().size();
			if (rf > brokers) {
				throw new ToolException("replicationFactor (" + rf + ") cannot be greater than the broker count ("
						+ brokers + ")");
			}
		}
		NewTopic nt = new NewTopic(topic, Optional.ofNullable(partitions),
				Optional.ofNullable(rf == null ? null : rf.shortValue()))
			.configs(configs);
		CreateTopicsResult r = call.admin().createTopics(List.of(nt));
		call.await(r.all());
		Map<String, Object> out = Results.map("topic", topic);
		try {
			// older brokers (before CreateTopics v5/v7) do not return the topic id and partitions/RF: zero UUID / -1
			Uuid id = call.await(r.topicId(topic));
			if (id != null && !Uuid.ZERO_UUID.equals(id)) {
				out.put("topicId", id.toString());
			}
			int created = call.await(r.numPartitions(topic));
			int createdRf = call.await(r.replicationFactor(topic));
			out.putAll(Results.map("partitions", created > 0 ? Integer.valueOf(created) : partitions,
					"replicationFactor", createdRf > 0 ? Integer.valueOf(createdRf) : rf));
		}
		catch (ToolException e) {
			// the topic was created but its details could not be read (e.g. no DESCRIBE_CONFIGS permission, old broker)
			out.putAll(Results.map("partitions", partitions, "replicationFactor", rf));
			out.put("note", "Topic created; could not read its details (" + e.getMessage() + ")");
		}
		if (!configs.isEmpty()) {
			out.put("configs", configs);
		}
		return out;
	}

	private static Map<String, String> configs(ToolCall call) {
		Map<String, String> configs = call.stringMap("configs");
		configs.forEach((k, v) -> {
			if (v == null) {
				throw new ToolException("configs value cannot be empty: " + k);
			}
		});
		return configs;
	}

}
