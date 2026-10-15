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

/** Topic okuma araçları: liste, detay, config, config'e göre arama. */
@Component
public class TopicTools {

	static final List<String> OPERATORS = List.of("eq", "ne", "gt", "gte", "lt", "lte", "contains", "regex", "exists");

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("topics_list", "Topic'leri partition sayısı ve replication factor ile listeler")
					.param("includeInternal", P.bool("İç topic'leri de listele (varsayılan false)"))
					.listParams()
					.handler(this::topicsList),
				ToolSpec.read("topics_get",
						"Topic detayı: partition'lar (lider, replika, ISR, KRaft'ta ELR, URP), varsayılandan farklı config'ler, boyut, offset'ler")
					.required("topic", P.string("Topic adı"))
					.handler(this::topicsGet),
				ToolSpec.read("topics_config_get", "Topic config'leri: değer, kaynak, varsayılan mı, salt okunur mu")
					.required("topic", P.string("Topic adı"))
					.param("includeDefaults", P.bool("Varsayılan değerleri de getir (varsayılan true)"))
					.param("filter", P.string("Config adı filtresi (regex, kısmi eşleşme)"))
					.handler(this::topicsConfigGet),
				ToolSpec.read("topics_find_by_config",
						"Config değerine göre topic arar (ör. retention.ms gt 604800000, cleanup.policy eq compact)")
					.required("key", P.string("Config adı (ör. retention.ms)"))
					.required("operator", P.enumOf("Karşılaştırma (gt/gte/lt/lte sayısal)", OPERATORS))
					.param("value", P.string("Karşılaştırılacak değer (exists için gerekmez)"))
					.param("onlyNonDefault", P.bool("Sadece varsayılandan farklı değerler (varsayılan false)"))
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
		// offset, config ve boyut istekleri paralel gider: biri takılırsa diğerlerinin süresini tüketmesin
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
			// ELR (KIP-966) sadece KRaft'ta; ZooKeeper'da null, boşsa da gösterilmez
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
		return Results.map("topic", topic, "count", rows.size(), "configs", rows);
	}

	Object topicsFindByConfig(ToolCall call) {
		String key = call.str("key");
		String op = call.str("operator").toLowerCase();
		if (!OPERATORS.contains(op)) {
			throw new ToolException("Geçersiz operator: " + op + " (geçerli: " + OPERATORS + ")");
		}
		String value = call.str("value", null);
		if (value == null && !"exists".equals(op)) {
			throw new ToolException("value parametresi gerekli (operator=exists hariç)");
		}
		Pattern regex = "regex".equals(op) ? call.regex("value") : null;
		BigDecimal number = null;
		if (List.of("gt", "gte", "lt", "lte").contains(op)) {
			number = decimal(value);
			if (number == null) {
				throw new ToolException(op + " için value sayı olmalı: " + value);
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
					default -> v != null && regex.matcher(v).find();
				};
			}
			if (match) {
				rows.add(Results.map("topic", r.name(), "value", Configs.value(e), "source", Configs.source(e)));
			}
		}
		Map<String, Object> out = Results.map("key", key, "operator", op, "value", value, "topicsChecked",
				names.size());
		if (!keyFound && !names.isEmpty() && configs.errors().size() < names.size()) {
			out.put("warning", "Bu anahtar topic config'lerinde yok: " + key);
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
