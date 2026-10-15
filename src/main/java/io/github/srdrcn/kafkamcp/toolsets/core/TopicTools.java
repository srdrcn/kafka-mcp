package io.github.srdrcn.kafkamcp.toolsets.core;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.Configs;
import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.LogDirs;
import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Topic read tools: list, details, configs, search by config. */
@Component
public class TopicTools {

	static final List<String> OPERATORS = List.of("eq", "ne", "gt", "gte", "lt", "lte", "contains", "regex", "exists");

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("topics_list", "List topics with partition count and replication factor")
					.param("includeInternal", P.bool("Also list internal topics (default false)"))
					.listParams()
					.handler(this::topicsList),
				ToolSpec.read("topics_get",
						"Topic details: partitions (leader, replicas, ISR, ELR on KRaft, URP), non-default configs, size, offsets")
					.required("topic", P.string("Topic name"))
					.handler(this::topicsGet),
				ToolSpec.read("topics_config_get", "Topic configs: value, source, default and read-only flags")
					.required("topic", P.string("Topic name"))
					.param("includeDefaults", P.bool("Also return default values (default true)"))
					.param("filter", P.string("Config name filter (regex, partial match)"))
					.param("limit", Params.limit(ToolSpec.DEFAULT_LIST_LIMIT))
					.handler(this::topicsConfigGet),
				ToolSpec.read("topics_find_by_config",
						"Find topics by config value (e.g. retention.ms gt 604800000, cleanup.policy eq compact)")
					.required("key", P.string("Config name (e.g. retention.ms)"))
					.required("operator", P.enumOf("Comparison operator (gt/gte/lt/lte are numeric)", OPERATORS))
					.param("value", P.string("Value to compare with (not needed for exists)"))
					.param("onlyNonDefault", P.bool("Only non-default values (default false)"))
					.listParams()
					.handler(this::topicsFindByConfig));
	}

	Object topicsList(ToolCall call) {
		KafkaFuture<Map<TopicPartition, PartitionReassignment>> pendingReassignments = Topics
			.submitReassignments(call);
		Map<String, Boolean> listing = Topics.list(call, call.bool("includeInternal", false));
		List<String> names = listing.keySet().stream().filter(call::matches).toList();
		List<String> page = names.subList(0, Math.min(call.limit(), names.size()));
		Futures.Each<String, TopicDescription> described = Topics.describe(call, page);
		Topics.Reassignments reassignments = Topics.reassignments(call, pendingReassignments);
		List<Map<String, Object>> rows = new ArrayList<>();
		for (String name : page) {
			TopicDescription d = described.values().get(name);
			if (d == null) {
				rows.add(Results.map("name", name, "error", described.errors().get(name)));
				continue;
			}
			rows.add(Results.map("name", name, "partitions", d.partitions().size(), "replicationFactor",
					Topics.replicationFactor(d, reassignments), "internal", listing.get(name) ? true : null));
		}
		return Params.page("topics", rows, names.size());
	}

	Object topicsGet(ToolCall call) {
		String topic = call.str("topic");
		KafkaFuture<Map<TopicPartition, PartitionReassignment>> pendingReassignments = Topics
			.submitReassignments(call);
		TopicDescription d = Topics.describeOne(call, topic);
		// offset, config and size requests run in parallel so that one that hangs does not use up the others' time
		ConfigResource resource = Configs.topic(topic);
		Offsets.PendingRanges pendingOffsets = Offsets.submitRanges(call, Topics.onlinePartitions(List.of(d)));
		KafkaFuture<Config> pendingConfigs = Configs.submitOne(call, resource);
		Futures.Each<Integer, Map<String, LogDirDescription>> dirs = LogDirs.describe(call,
				LogDirs.brokersHosting(List.of(d)));
		Topics.Reassignments reassignments = Topics.reassignments(call, pendingReassignments);

		List<Map<String, Object>> partitions = new ArrayList<>();
		int urp = 0;
		int offline = 0;
		for (TopicPartitionInfo p : d.partitions()) {
			List<Integer> adding = reassignments.adding(new TopicPartition(topic, p.partition()));
			boolean isUrp = Topics.underReplicated(p, adding);
			boolean isOffline = Topics.offline(p);
			urp += isUrp ? 1 : 0;
			offline += isOffline ? 1 : 0;
			// ELR (KIP-966) exists only on KRaft; it is null on ZooKeeper and omitted when empty
			List<Integer> elr = Topics.elr(p);
			List<Integer> lastKnownElr = Topics.lastKnownElr(p);
			partitions.add(Results.map("partition", p.partition(), "leader", Topics.leader(p), "replicas",
					Topics.ids(p.replicas()), "isr", Topics.ids(p.isr()), "elr",
					elr == null || elr.isEmpty() ? null : elr, "lastKnownElr",
					lastKnownElr == null || lastKnownElr.isEmpty() ? null : lastKnownElr, "addingReplicas",
					adding.isEmpty() ? null : adding, "urp", isUrp ? true : null, "offline", isOffline ? true : null));
		}
		Map<String, Object> out = Results.map("topic", topic, "topicId", Topics.topicId(d), "internal",
				d.isInternal() ? true : null, "partitionCount", d.partitions().size(), "replicationFactor",
				Topics.replicationFactor(d, reassignments), "underReplicatedPartitions", urp, "offlinePartitions",
				offline, "reassignmentsError", reassignments.error());

		long leaderBytes = 0;
		long totalBytes = 0;
		for (LogDirs.PartitionSize s : LogDirs.partitionSizes(dirs.values(), Map.of(topic, d)).values()) {
			leaderBytes += s.leaderBytes();
			totalBytes += s.totalBytes();
		}
		out.put("sizeBytes", leaderBytes);
		out.put("size", Format.bytes(leaderBytes));
		out.put("totalSizeBytes", totalBytes);
		out.put("totalSize", Format.bytes(totalBytes));
		if (!dirs.errors().isEmpty()) {
			out.put("sizeFailedBrokers", dirs.errors());
		}

		try {
			Map<TopicPartition, Offsets.Range> ranges = pendingOffsets.await(call);
			long earliest = 0;
			long latest = 0;
			long messages = 0;
			for (Offsets.Range r : ranges.values()) {
				earliest += r.earliest();
				latest += r.latest();
				messages += r.count();
			}
			int missing = d.partitions().size() - ranges.size();
			out.put("offsets", Results.map("earliestSum", earliest, "latestSum", latest, "approxMessages", messages,
					"partitionsWithoutOffsets", missing > 0 ? missing : null));
		}
		catch (ToolException e) {
			out.put("offsetsError", e.getMessage());
		}
		try {
			List<Map<String, Object>> configs = new ArrayList<>();
			for (ConfigEntry e : Configs.sorted(Configs.awaitOne(call, resource, pendingConfigs))) {
				if (!Configs.isDefault(e)) {
					configs.add(Results.map("name", e.name(), "value", Configs.value(e), "source", Configs.source(e)));
				}
			}
			out.put("nonDefaultConfigs", configs);
		}
		catch (ToolException e) {
			out.put("configsError", e.getMessage());
		}
		out.put("partitions", partitions);
		return out;
	}

	Object topicsConfigGet(ToolCall call) {
		String topic = call.str("topic");
		boolean includeDefaults = call.bool("includeDefaults", true);
		Config config = Configs.describeOne(call, Configs.topic(topic));
		List<Map<String, Object>> rows = new ArrayList<>();
		for (ConfigEntry e : Configs.sorted(config)) {
			if ((includeDefaults || !Configs.isDefault(e)) && call.matches(e.name())) {
				rows.add(Configs.row(e, true));
			}
		}
		int limit = call.limit();
		return Results.map("topic", topic, "count", rows.size(), "truncated", rows.size() > limit ? true : null,
				"configs", rows.subList(0, Math.min(limit, rows.size())));
	}

	Object topicsFindByConfig(ToolCall call) {
		String key = call.str("key");
		String op = call.str("operator").toLowerCase();
		if (!OPERATORS.contains(op)) {
			throw new ToolException("Invalid operator: " + op + " (valid: " + OPERATORS + ")");
		}
		String value = call.str("value", null);
		if (value == null && !"exists".equals(op)) {
			throw new ToolException("Missing parameter: value (required unless operator=exists)");
		}
		Pattern regex = "regex".equals(op) ? call.regex("value") : null;
		BigDecimal number = null;
		if (List.of("gt", "gte", "lt", "lte").contains(op)) {
			number = decimal(value);
			if (number == null) {
				throw new ToolException("value must be a number for " + op + ": " + value);
			}
		}
		boolean onlyNonDefault = call.bool("onlyNonDefault", false);
		List<String> names = Topics.list(call, false).keySet().stream().filter(call::matches).toList();
		List<ConfigResource> resources = names.stream().map(Configs::topic).toList();
		Futures.Each<ConfigResource, Config> configs = Configs.describe(call, resources);
		List<Map<String, Object>> rows = new ArrayList<>();
		boolean keyFound = false;
		int nonNumeric = 0;
		for (ConfigResource r : resources) {
			Config c = configs.values().get(r);
			ConfigEntry e = c == null ? null : c.get(key);
			if (e == null) {
				continue;
			}
			keyFound = true;
			if (onlyNonDefault && Configs.isDefault(e)) {
				continue;
			}
			String v = e.isSensitive() ? null : e.value();
			boolean match;
			if (number != null) {
				BigDecimal n = decimal(v);
				if (n == null) {
					nonNumeric++;
					continue;
				}
				int cmp = n.compareTo(number);
				match = switch (op) {
					case "gt" -> cmp > 0;
					case "gte" -> cmp >= 0;
					case "lt" -> cmp < 0;
					default -> cmp <= 0;
				};
			}
			else {
				match = switch (op) {
					case "exists" -> v != null;
					case "eq" -> Objects.equals(v, value);
					case "ne" -> !Objects.equals(v, value);
					case "contains" -> v != null && v.contains(value);
					default -> v != null && call.find(regex, v);
				};
			}
			if (match) {
				rows.add(Results.map("topic", r.name(), "value", Configs.value(e), "source", Configs.source(e)));
			}
		}
		Map<String, Object> out = Results.map("key", key, "operator", op, "value", value, "topicsChecked",
				names.size());
		if (!keyFound && !names.isEmpty() && configs.errors().size() < names.size()) {
			out.put("warning", "Key not found in topic configs: " + key);
		}
		if (nonNumeric > 0) {
			out.put("nonNumericValues", nonNumeric);
		}
		if (!configs.errors().isEmpty()) {
			out.put("topicsFailed", configs.errors().size());
		}
		out.putAll(Results.page("topics", rows, call.limit()));
		return out;
	}

	private static BigDecimal decimal(String s) {
		if (s == null) {
			return null;
		}
		try {
			return new BigDecimal(s.trim());
		}
		catch (NumberFormatException e) {
			return null;
		}
	}

}
