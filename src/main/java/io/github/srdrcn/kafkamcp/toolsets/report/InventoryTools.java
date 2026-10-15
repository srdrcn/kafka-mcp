package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.acl.Acls;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectApi;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorOverview;
import io.github.srdrcn.kafkamcp.toolsets.consumers.Settled;

/** Inventory reports: team (prefix) inventory and cleanup candidates. */
@Component
public class InventoryTools {

	private final ConnectApi connectApi;

	public InventoryTools(ConnectApi connectApi) {
		this.connectApi = connectApi;
	}

	public List<ToolSpec> tools() {
		Map<String, Object> limit = ReportParams.integer("Maximum number of items per list (default 100)", 100, 1,
				ToolSpec.MAX_LIST_LIMIT);
		return List.of(
				ToolSpec.read("report_team_inventory",
						"Team inventory by topic prefix: topics, size, empty/idle flags, consumer groups and lag, connectors, ACL principals")
					.required("prefix", P.string("Topic name prefix (e.g. payments.)"))
					.param("olderThanDays", ReportParams.olderThanDays())
					.param("limit", limit)
					.handler(this::teamInventory),
				ToolSpec.read("report_cleanup_candidates",
						"Cleanup candidates (deletes nothing): empty/idle/unconsumed topics, Empty groups, orphan LITERAL ACLs, with reasons and risk")
					.param("olderThanDays", ReportParams.olderThanDays())
					.listParams()
					.handler(this::cleanupCandidates));
	}

	/** ACLs of one resource type such as TOPIC or GROUP (one describeAcls). */
	private static KafkaFuture<Collection<AclBinding>> submitAcls(ToolCall call, ResourceType type) {
		return call.admin()
			.describeAcls(new AclBindingFilter(new ResourcePatternFilter(type, null, PatternType.ANY),
					AccessControlEntryFilter.ANY))
			.values();
	}

	Object teamInventory(ToolCall call) {
		String prefix = call.str("prefix");
		int days = call.integer("olderThanDays", ReportParams.DEFAULT_OLDER_THAN_DAYS, 1, 3650);
		int limit = call.integer("limit", 100, 1, ToolSpec.MAX_LIST_LIMIT);
		String cluster = call.cluster();
		Async<List<ConnectorOverview>> connectors = call.clusterConfig().hasConnect()
				? Async.start(() -> connectApi.overview(cluster)) : null;
		KafkaFuture<Collection<AclBinding>> acls = submitAcls(call, ResourceType.TOPIC);
		FleetScan scan = FleetScan.of(call)
			.names(n -> n.startsWith(prefix), "notMatchingPrefix")
			.includeInternal(true)
			.ranges()
			.maxTimestamps()
			.sizes()
			.groups(GroupScan.MAX_GROUPS)
			.reserve(2_000)
			.run();
		Coverage coverage = scan.coverage();
		long now = scan.nowMs();
		long threshold = TopicActivity.threshold(now, days);
		Set<String> teamTopics = scan.topics().keySet();

		List<Map<String, Object>> topicRows = new ArrayList<>();
		long size = 0;
		long totalSize = 0;
		int partitions = 0;
		int empty = 0;
		int idle = 0;
		for (String t : teamTopics) {
			TopicActivity a = scan.activity(t);
			Map<String, Object> row = scan.topicRow(t);
			partitions += scan.partitions(t);
			size += scan.sizeBytes(t) == null ? 0 : scan.sizeBytes(t);
			totalSize += scan.totalSizeBytes(t) == null ? 0 : scan.totalSizeBytes(t);
			if (Boolean.TRUE.equals(a.empty())) {
				empty++;
				row.put("empty", true);
			}
			else {
				row.put("lastWriteAgeDays", a.ageDays(now));
				if (Boolean.TRUE.equals(a.idleSince(threshold))) {
					idle++;
					row.put("idle", true);
				}
			}
			topicRows.add(row);
		}
		Map<String, Object> topics = Results.map("count", teamTopics.size(), "partitions", partitions, "sizeBytes",
				size, "size", Format.bytes(size), "allReplicasSize", Format.bytes(totalSize), "emptyCount", empty,
				"idleCount", idle, "idleOlderThanDays", days);
		topics.putAll(ReportParams.page("list", topicRows, limit));

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("prefix", prefix);
		out.put("topics", topics);
		out.put("consumerGroups", teamGroups(scan, prefix, limit));
		if (connectors != null) {
			out.put("connectors", teamConnectors(connectors.await(call, 800), prefix, teamTopics, limit));
		}
		Settled<Collection<AclBinding>> aclResult = Settled.within(call, acls, 500);
		if (aclResult.ok()) {
			List<Map<String, Object>> principals = PrefixAccess.principals(aclResult.value(), prefix, teamTopics);
			Map<String, Object> a = Results.map("principalCount", principals.size());
			a.putAll(ReportParams.page("principals", principals, limit));
			out.put("acls", a);
		}
		else if (Acls.isSecurityDisabled(aclResult.error())) {
			out.put("acls", Acls.noAuthorizer());
		}
		else {
			out.put("acls", Results.map("error", aclResult.errorMessage()));
			if (aclResult.timedOut()) {
				coverage.partial();
			}
		}
		coverage.note("Connectors are matched by config (topics/topic, *.regex, *.prefix); ACLs: LITERAL, PREFIXED and '*' TOPIC bindings");
		coverage.addTo(out);
		return out;
	}

	/** Groups with committed offsets on the team topics (by lag) and other groups named with the prefix. */
	private static Map<String, Object> teamGroups(FleetScan scan, String prefix, int limit) {
		GroupScan groups = scan.groups();
		if (groups == null) {
			return Results.map("error", scan.groupsError());
		}
		Map<String, String> states = groups.states();
		Map<TopicPartition, Offsets.Range> ranges = scan.ranges();
		List<Map<String, Object>> rows = new ArrayList<>();
		long totalLag = 0;
		Set<String> consuming = new HashSet<>();
		for (Map.Entry<String, Map<TopicPartition, Long>> e : groups.offsets().entrySet()) {
			Set<String> topics = new TreeSet<>();
			long lag = 0;
			int unknown = 0;
			for (Map.Entry<TopicPartition, Long> o : e.getValue().entrySet()) {
				if (!scan.topics().containsKey(o.getKey().topic())) {
					continue;
				}
				topics.add(o.getKey().topic());
				Offsets.Range r = ranges.get(o.getKey());
				if (r == null) {
					unknown++;
				}
				else {
					lag += Math.max(0, r.latest() - o.getValue());
				}
			}
			if (topics.isEmpty()) {
				continue;
			}
			consuming.add(e.getKey());
			totalLag += lag;
			rows.add(Results.map("groupId", e.getKey(), "state", states.get(e.getKey()), "topics", topics.size(),
					"totalLag", lag, "lagUnknownPartitions", unknown > 0 ? unknown : null));
		}
		rows.sort(Comparator.comparingLong((Map<String, Object> m) -> (Long) m.get("totalLag"))
			.reversed()
			.thenComparing(m -> (String) m.get("groupId")));
		List<String> named = new ArrayList<>();
		for (String g : states.keySet()) {
			if (g.startsWith(prefix) && !consuming.contains(g)) {
				named.add(g);
			}
		}
		Map<String, Object> out = Results.map("count", rows.size(), "totalLag", totalLag, "groupsListed",
				groups.listed().size(), "otherGroupsWithPrefixCount", named.isEmpty() ? null : named.size(),
				"otherGroupsWithPrefix", named.isEmpty() ? null : ReportParams.head(named, limit),
				"otherGroupsWithPrefixOmitted", named.size() > limit ? named.size() - limit : null);
		out.putAll(ReportParams.page("groups", rows, limit));
		return out;
	}

	/** Connectors whose config references the team topics (or names under the prefix). */
	private static Map<String, Object> teamConnectors(Settled<List<ConnectorOverview>> s, String prefix,
			Set<String> teamTopics, int limit) {
		if (!s.ok()) {
			return Results.map("error", Async.message(s));
		}
		List<Map<String, Object>> rows = new ArrayList<>();
		int noInfo = 0;
		for (ConnectorOverview c : s.value()) {
			if (c.info() == null) {
				noInfo++;
				continue;
			}
			ConnectorTopics.References refs = ConnectorTopics.parse(c.info().config());
			Set<String> covered = ConnectorTopics.covered(refs, teamTopics);
			List<String> missing = refs.literals()
				.stream()
				.filter(t -> t.startsWith(prefix) && !teamTopics.contains(t))
				.toList();
			List<String> prefixes = refs.prefixes().stream().filter(p -> p.startsWith(prefix)).toList();
			if (covered.isEmpty() && missing.isEmpty() && prefixes.isEmpty()) {
				continue;
			}
			List<String> shown = ReportParams.head(new ArrayList<>(covered), 10);
			rows.add(Results.map("name", c.name(), "type", c.type(), "state", c.status().state(), "failed",
					c.status().hasFailure() ? true : null, "topicCount", covered.size(), "topics",
					shown.isEmpty() ? null : shown, "missingTopics", missing.isEmpty() ? null : missing, "topicPrefixes",
					prefixes.isEmpty() ? null : prefixes, "configKeys", new ArrayList<>(refs.keys())));
		}
		Map<String, Object> out = Results.map("connectorsScanned", s.value().size(), "withoutInfo",
				noInfo > 0 ? noInfo : null);
		out.putAll(ReportParams.page("connectors", rows, limit));
		return out;
	}

	Object cleanupCandidates(ToolCall call) {
		int days = call.integer("olderThanDays", ReportParams.DEFAULT_OLDER_THAN_DAYS, 1, 3650);
		KafkaFuture<Collection<AclBinding>> acls = call.admin().describeAcls(AclBindingFilter.ANY).values();
		KafkaFuture<Collection<GroupListing>> allGroups = call.admin().listGroups(new ListGroupsOptions()).all();
		FleetScan scan = FleetScan.of(call)
			.names(n -> true)
			.includeInternal(true)
			.ranges()
			.maxTimestamps()
			.sizes()
			.groups(GroupScan.MAX_GROUPS)
			.reserve(2_000)
			.run();
		// if the group scan fails (error/partial recorded by FleetScan), topic and ACL candidates are still produced
		GroupScan groups = scan.groups();
		Coverage coverage = scan.coverage();
		long now = scan.nowMs();
		long threshold = TopicActivity.threshold(now, days);
		Map<String, Set<String>> consumersByTopic = groups == null ? null : groups.groupsByTopic();
		List<CleanupRules.Candidate> candidates = new ArrayList<>();
		int protectedSkipped = 0;
		int topicsNotMatching = 0;
		for (String t : scan.topics().keySet()) {
			if (Topics.protectedReason(t, scan.listing().getOrDefault(t, false)) != null) {
				protectedSkipped++;
				continue;
			}
			if (!call.matches(t)) {
				topicsNotMatching++;
				continue;
			}
			TopicActivity a = scan.activity(t);
			Map<String, Object> details = Results.map("partitions", scan.partitions(t));
			details.putAll(scan.sizeFields(t));
			if (!Boolean.TRUE.equals(a.empty())) {
				details.putAll(a.lastWriteFields(now));
			}
			CleanupRules.Candidate c = CleanupRules.topic(t, a, threshold,
					consumersByTopic == null ? null : consumersByTopic.getOrDefault(t, Set.of()).size(), details);
			if (c != null) {
				candidates.add(c);
			}
		}
		coverage.skipped("protectedTopics", protectedSkipped);
		coverage.skipped("topicsNotMatchingFilter", topicsNotMatching);
		Set<String> existing = scan.listing().keySet();
		if (groups != null) {
			Map<String, String> states = groups.states();
			int groupsNotMatching = 0;
			for (Map.Entry<String, Map<TopicPartition, Long>> e : groups.offsets().entrySet()) {
				if (!call.matches(e.getKey())) {
					groupsNotMatching++;
					continue;
				}
				CleanupRules.Candidate c = CleanupRules.group(e.getKey(), states.get(e.getKey()), e.getValue(),
						existing, scan.ranges(), t -> {
							TopicActivity a = scan.activity(t);
							return Boolean.TRUE.equals(a.empty()) || Boolean.TRUE.equals(a.idleSince(threshold));
						});
				if (c != null) {
					candidates.add(c);
				}
			}
			coverage.skipped("groupsNotMatchingFilter", groupsNotMatching);
		}
		else {
			coverage.note("Could not read consumer groups: group candidates and the no_consumers reason are skipped; "
					+ "consumers of topic candidates are unknown (consumersUnknown)");
		}
		Settled<Collection<AclBinding>> aclResult = Settled.within(call, acls, 700);
		Settled<Collection<GroupListing>> groupIds = Settled.within(call, allGroups, 600);
		Map<String, Object> aclInfo = null;
		if (aclResult.ok()) {
			Set<String> ids = new HashSet<>();
			boolean groupsKnown = groupIds.ok();
			if (groupsKnown) {
				groupIds.value().forEach(g -> ids.add(g.groupId()));
			}
			else {
				coverage.note("Could not list groups (all types): orphan GROUP ACL check skipped");
			}
			int aclsNotMatching = 0;
			for (CleanupRules.Candidate c : CleanupRules.orphanAcls(aclResult.value(), existing, ids, groupsKnown)) {
				if (call.matches(c.name())) {
					candidates.add(c);
				}
				else {
					aclsNotMatching++;
				}
			}
			coverage.examined("aclBindings", aclResult.value().size());
			coverage.skipped("orphanAclResourcesNotMatchingFilter", aclsNotMatching);
		}
		else if (Acls.isSecurityDisabled(aclResult.error())) {
			aclInfo = Acls.noAuthorizer();
		}
		else {
			coverage.error("describeAcls", aclResult.errorMessage());
			if (aclResult.timedOut()) {
				coverage.partial();
			}
		}
		candidates.sort(Comparator.comparingInt((CleanupRules.Candidate c) -> riskOrder(c.risk()))
			.thenComparing(c -> typeOrder(c.type()))
			.thenComparing(CleanupRules.Candidate::name));
		Map<String, Integer> byType = new TreeMap<>();
		Map<String, Integer> byRisk = new LinkedHashMap<>();
		for (String r : List.of(CleanupRules.LOW, CleanupRules.MEDIUM, CleanupRules.HIGH)) {
			byRisk.put(r, 0);
		}
		candidates.forEach(c -> {
			byType.merge(c.type(), 1, Integer::sum);
			byRisk.merge(c.risk(), 1, Integer::sum);
		});
		List<Map<String, Object>> rows = candidates.stream().map(CleanupRules.Candidate::toMap).toList();
		Map<String, Object> out = Results.map("olderThanDays", days, "threshold", Format.iso(threshold), "byType",
				byType, "byRisk", byRisk, "acls", aclInfo);
		if (groups != null && !groups.complete()) {
			coverage.note("Group scan is incomplete: no_consumers reasons are not certain");
		}
		coverage.note("Read-only report: nothing is deleted. Risk: low = no data/consumer traces, medium = verify first, "
				+ "high = active writes");
		coverage.note(TopicUsageTools.INVISIBLE_READERS);
		coverage.addTo(out);
		out.putAll(ReportParams.page("candidates", rows, call.limit()));
		return out;
	}

	private static int riskOrder(String risk) {
		return switch (risk) {
			case CleanupRules.LOW -> 0;
			case CleanupRules.MEDIUM -> 1;
			default -> 2;
		};
	}

	private static int typeOrder(String type) {
		return switch (type) {
			case "topic" -> 0;
			case "group" -> 1;
			default -> 2;
		};
	}

}
