package io.github.srdrcn.kafkamcp.toolsets.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.Configs;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Broker write tools: dynamic config update, preferred leader election. */
@Component
public class BrokerWriteTools {

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.write("brokers_config_update",
						"Change dynamic broker configs (cluster-wide default if brokerId is omitted); returns before/after values")
					.param("brokerId", P.integer("Broker id (default: the cluster-wide default config)"))
					.param("set", P.stringMap("Configs to set (key → value)"))
					.param("delete", P.stringArray("Config names to delete (revert to default)"))
					.handler(this::brokersConfigUpdate),
				ToolSpec.write("brokers_elect_preferred_leaders",
						"Run preferred leader election; without a topic, on every partition whose leader is not the preferred replica")
					.param("topic", P.string("Topic name (default: all eligible partitions)"))
					.param("partitions", Params.integerArray("Partition numbers (requires topic; default: all partitions of the topic)"))
					.handler(this::electPreferredLeaders));
	}

	Object brokersConfigUpdate(ToolCall call) {
		ConfigResource resource;
		if (call.has("brokerId")) {
			resource = Configs.broker(ClusterMeta.describe(call, false).broker(call.optInt("brokerId")).id());
		}
		else {
			resource = Configs.brokerDefault();
		}
		List<Map<String, Object>> changes = Configs.alter(call, resource, call.stringMap("set"), call.strings("delete"));
		return Results.map("scope", resource.isDefault() ? "cluster-default" : "broker", "brokerId",
				resource.isDefault() ? null : Integer.valueOf(resource.name()), "changes", changes);
	}

	Object electPreferredLeaders(ToolCall call) {
		String topic = call.str("topic", null);
		List<Integer> partitions = Params.ints(call, "partitions");
		if (topic == null && !partitions.isEmpty()) {
			throw new ToolException("topic is required when partitions is given");
		}
		Set<TopicPartition> tps = new LinkedHashSet<>();
		if (topic != null) {
			TopicDescription d = Topics.describeOne(call, topic);
			int count = d.partitions().size();
			if (partitions.isEmpty()) {
				tps.addAll(Topics.partitions(d));
			}
			for (int p : partitions) {
				if (p < 0 || p >= count) {
					throw new ToolException("Invalid partition " + p + " for topic " + topic + " (valid: 0.." + (count - 1) + ")");
				}
				tps.add(new TopicPartition(topic, p));
			}
		}
		else {
			for (TopicDescription d : Topics.describeAll(call, true).values()) {
				for (TopicPartitionInfo p : d.partitions()) {
					if (Topics.nonPreferredLeader(p)) {
						tps.add(new TopicPartition(d.name(), p.partition()));
					}
				}
			}
			if (tps.isEmpty()) {
				return Results.map("requested", 0, "message",
						"Every partition is already led by its preferred replica; no election needed");
			}
		}
		Map<TopicPartition, Optional<Throwable>> result = call
			.await(call.admin().electLeaders(ElectionType.PREFERRED, tps).partitions());
		List<Map<String, Object>> rows = new ArrayList<>();
		int elected = 0;
		int notNeeded = 0;
		int failed = 0;
		for (TopicPartition tp : tps) {
			Optional<Throwable> err = result.getOrDefault(tp, Optional.empty());
			String status;
			String error = null;
			if (err.isEmpty()) {
				status = "elected";
				elected++;
			}
			else if (KafkaErrors.unwrap(err.get()) instanceof ElectionNotNeededException) {
				status = "notNeeded";
				notNeeded++;
			}
			else {
				status = "error";
				error = KafkaErrors.explain(err.get());
				failed++;
			}
			rows.add(Results.map("topic", tp.topic(), "partition", tp.partition(), "result", status, "error", error));
		}
		rows.sort(Comparator.comparing((Map<String, Object> m) -> !"error".equals(m.get("result"))));
		Map<String, Object> out = Results.map("requested", tps.size(), "elected", elected, "notNeeded", notNeeded,
				"failed", failed);
		out.putAll(Results.page("results", rows, ToolSpec.DEFAULT_LIST_LIMIT));
		return out;
	}

}
