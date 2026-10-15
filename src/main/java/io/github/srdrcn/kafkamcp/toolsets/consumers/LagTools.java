package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.consumers.ConsumerGroups.GroupList;
import io.github.srdrcn.kafkamcp.toolsets.consumers.ConsumerGroups.GroupSummary;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.Committed;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.GroupLagInfo;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.OffsetLookup;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.PartitionLag;

/** Lag tools: lag, with_lag, lag_trend, stuck. */
@Component
public class LagTools {

	static final long DEFAULT_THRESHOLD = 1000;

	static final int TOP_TOPICS = 5;

	static final int MAX_STUCK_ROWS_PER_GROUP = 20;

	private static final String STUCK_NOTE = "Groups that commit periodically (e.g. auto commit every 5 s) can be "
			+ "false positives with a short interval; members=0 means there is no consumer";

	public List<ToolSpec> tools() {
		Map<String, Object> threshold = P.integer("Minimum total lag (default " + DEFAULT_THRESHOLD + ")");
		threshold.put("minimum", 0);
		threshold.put("default", DEFAULT_THRESHOLD);
		return List.of(
				ToolSpec
					.read("consumer_groups_lag",
							"Group lag: per-topic totals and per-partition committed, end, lag, owner clientId/host (by lag, descending)")
					.required("group", Params.group())
					.param("topic", P.string("Only this topic (exact name)"))
					.handler(this::lag),
				ToolSpec
					.read("consumer_groups_with_lag",
							"Groups whose total lag is above the threshold (descending), with state and the 5 topics with the most lag")
					.param("threshold", threshold)
					.listParams()
					.param("topic", P.string("Only topics matching this regex are counted"))
					.handler(this::withLag),
				ToolSpec
					.read("consumer_groups_lag_trend",
							"Lag trend from two samples: per-topic lag, consume/produce rate (msg/s), trend and estimated catch-up time")
					.required("group", Params.group())
					.param("intervalSeconds", Params.intervalSeconds())
					.handler(this::lagTrend),
				ToolSpec
					.read("consumer_groups_stuck",
							"Stuck groups: partitions with lag whose commit did not advance between two samples while the end offset grew")
					.listParams()
					.param("intervalSeconds", Params.intervalSeconds())
					.handler(this::stuck));
	}

	Object lag(ToolCall call) {
		return lag(call, GroupTools.MAX_PARTITION_ROWS);
	}

	/** maxRows: maximum partition rows shown; once the budget is used up, the remaining topics are only counted. */
	Object lag(ToolCall call, int maxRows) {
		String group = call.str("group");
		String topic = call.str("topic", null);
		GroupDetail g = GroupDetail.load(call, group, topic == null ? null : topic::equals);
		if (topic != null && g.lag().partitions().isEmpty()) {
			throw new ToolException("Group '" + group + "' has no commits or assignments on topic '" + topic + "'");
		}
		Map<String, List<PartitionLag>> byTopic = new LinkedHashMap<>();
		Map<String, Long> lagByTopic = g.lag().lagByTopic();
		lagByTopic.keySet().forEach(t -> byTopic.put(t, new ArrayList<>()));
		for (PartitionLag p : g.lag().partitions()) {
			byTopic.get(p.topic()).add(p);
		}
		Map<TopicPartition, MemberDescription> incoming = ConsumerGroups.incomingOwners(g.description());
		int reassigning = 0;
		List<Map<String, Object>> topics = new ArrayList<>();
		int budget = maxRows;
		int omitted = 0;
		int topicsOmitted = 0;
		long omittedTopicsLag = 0;
		for (Map.Entry<String, List<PartitionLag>> e : byTopic.entrySet()) {
			if (budget <= 0) {
				topicsOmitted++;
				omitted += e.getValue().size();
				omittedTopicsLag += lagByTopic.get(e.getKey());
				continue;
			}
			List<PartitionLag> partitions = new ArrayList<>(e.getValue());
			partitions.sort(PartitionLag.BY_LAG_DESC);
			List<Map<String, Object>> rows = new ArrayList<>();
			int topicOmitted = 0;
			for (PartitionLag p : partitions) {
				if (budget <= 0) {
					topicOmitted++;
					continue;
				}
				budget--;
				MemberDescription owner = g.owners().get(p.topicPartition());
				MemberDescription target = owner == null ? incoming.get(p.topicPartition()) : null;
				reassigning += target == null ? 0 : 1;
				rows.add(Results.map("partition", p.partition(), "committed", p.committed(), "end", p.end(), "lag",
						p.lag(), "clientId", owner == null ? null : owner.clientId(), "host",
						owner == null ? null : owner.host(), "reassigningTo", target == null ? null : target.clientId(),
						"noCommit", p.committed() == null ? Boolean.TRUE : null, "error", p.error()));
			}
			omitted += topicOmitted;
			topics.add(Results.map("topic", e.getKey(), "lag", lagByTopic.get(e.getKey()), "partitions", rows,
					"partitionsOmitted", topicOmitted > 0 ? topicOmitted : null));
		}
		int withoutCommit = g.lag().withoutCommit();
		int withoutEnd = (int) g.lag().partitions().stream().filter(p -> p.end() == null).count();
		return Results.map("groupId", group, "state", g.state(), "members", g.description().members().size(),
				"totalLag", g.lag().totalLag(), "partitionsWithoutCommit", withoutCommit > 0 ? withoutCommit : null,
				"partitionsWithoutEndOffset", withoutEnd > 0 ? withoutEnd : null, "partial",
				withoutEnd > 0 ? Boolean.TRUE : null, "topics", topics, "partitionRowsOmitted",
				omitted > 0 ? omitted : null, "topicsOmitted", topicsOmitted > 0 ? topicsOmitted : null,
				"omittedTopicsLag", topicsOmitted > 0 ? omittedTopicsLag : null, "note",
				topics.isEmpty() ? "The group has no committed offsets or partition assignments"
						: reassigning > 0 ? ConsumerGroups.REASSIGNING_NOTE : null);
	}

	Object withLag(ToolCall call) {
		long threshold = Math.max(0, call.longValue("threshold", DEFAULT_THRESHOLD));
		Pattern topicRe = call.regex("topic");
		Predicate<String> topicFilter = topicRe == null ? null : t -> topicRe.matcher(t).find();
		GroupList list = ConsumerGroups.list(call, ConsumerGroups.LIST_RESERVE_MS, call::matches);
		List<GroupSummary> groups = list.groups();
		GroupLag.Snapshot snapshot = GroupLag.lagSnapshot(call, groups.stream().map(GroupSummary::groupId).toList(),
				topicFilter);
		Map<String, GroupLagInfo> lags = snapshot.groups();
		record Row(long total, Map<String, Object> map) {
		}
		List<Row> rows = new ArrayList<>();
		int disappeared = 0;
		ScanIssues issues = new ScanIssues();
		issues.endErrors(snapshot.end());
		for (GroupSummary g : groups) {
			GroupLagInfo info = lags.get(g.groupId());
			if (info == null) {
				continue;
			}
			if (info.error() != null) {
				if (info.notFound()) {
					disappeared++;
				}
				else {
					issues.groupError(info);
				}
				continue;
			}
			issues.unknownLag(info);
			long total = info.totalLag();
			if (info.partitions().isEmpty() || total < threshold) {
				continue;
			}
			List<Map<String, Object>> top = new ArrayList<>();
			Map<String, Long> byTopic = info.lagByTopic();
			byTopic.entrySet()
				.stream()
				.limit(TOP_TOPICS)
				.forEach(e -> top.add(Results.map("topic", e.getKey(), "lag", e.getValue())));
			int withoutCommit = info.withoutCommit();
			int withoutEnd = info.withoutEnd();
			rows.add(new Row(total, Results.map("groupId", g.groupId(), "state", g.state(), "totalLag", total,
					"topicCount", byTopic.size(), "topTopics", top, "partitionsWithoutCommit",
					withoutCommit > 0 ? withoutCommit : null, "partitionsWithoutEndOffset",
					withoutEnd > 0 ? withoutEnd : null)));
		}
		rows.sort(Comparator.comparingLong(Row::total).reversed());
		List<Map<String, Object>> sorted = rows.stream().map(Row::map).toList();
		Map<String, Object> out = Results.map("threshold", threshold, "scannedGroups", groups.size(),
				"skippedGroups", disappeared > 0 ? disappeared : null);
		out.putAll(issues.toMap(list));
		out.putAll(Results.page("groups", sorted, call.limit()));
		return out;
	}

	Object lagTrend(ToolCall call) {
		String group = call.str("group");
		int interval = Params.intervalSeconds(call);
		Settled<ConsumerGroupDescription> described = ConsumerGroups
			.describe(call, List.of(group), Params.FIRST_LIST_RESERVE_MS)
			.get(group);
		long t0 = System.nanoTime();
		Committed c0 = GroupLag.committed(call, List.of(group), null, Params.FIRST_COMMITTED_RESERVE_MS).get(group);
		if (c0.error() != null) {
			throw c0.notFound() ? ConsumerGroups.notFoundError(group) : new ToolException(c0.error());
		}
		if (c0.offsets().isEmpty()) {
			if (!described.ok() && ConsumerGroups.notFound(described.error())
					|| described.ok() && ConsumerGroups.deadOrUnknown(described.value())) {
				throw ConsumerGroups.notFoundError(group);
			}
			throw new ToolException("Group '" + group + "' has no committed offsets; cannot compute the lag trend");
		}
		Set<TopicPartition> tps = new LinkedHashSet<>(c0.offsets().keySet());
		OffsetLookup e0 = GroupLag.endOffsets(call, tps, Params.FIRST_END_RESERVE_MS);
		long t0End = System.nanoTime();
		long slept = Params.pause(call, interval);
		if (slept < 0) {
			throw Params.noTimeForSecondSample();
		}
		long t1 = System.nanoTime();
		Committed c1 = GroupLag.committed(call, List.of(group), null, Params.SECOND_COMMITTED_RESERVE_MS).get(group);
		if (c1.error() != null) {
			throw c1.notFound() ? new ToolException("Group was deleted during the measurement: " + group)
					: new ToolException(c1.error());
		}
		// Partitions whose end offset could not be fetched in the first sample are skipped; they are not asked again.
		OffsetLookup e1 = GroupLag.endOffsets(call, e0.offsets().keySet(), Params.SECOND_END_RESERVE_MS);
		long t1End = System.nanoTime();
		double elapsed = Params.seconds((t1 + t1End) / 2 - (t0 + t0End) / 2);

		Map<String, long[]> agg = new TreeMap<>();
		int skipped = 0;
		int withoutEnd = 0;
		for (TopicPartition tp : tps) {
			Long a = c0.offsets().get(tp);
			Long b = c1.offsets().get(tp);
			Long ea = e0.offsets().get(tp);
			Long eb = e1.offsets().get(tp);
			if (a == null || b == null || ea == null || eb == null) {
				skipped++;
				withoutEnd += ea == null || eb == null ? 1 : 0;
				continue;
			}
			long[] s = agg.computeIfAbsent(tp.topic(), k -> new long[5]);
			s[0] += GroupLag.lag(ea, a);
			s[1] += GroupLag.lag(eb, b);
			s[2] += b - a;
			s[3] += eb - ea;
			s[4]++;
		}
		long[] total = new long[5];
		List<Map<String, Object>> topics = new ArrayList<>();
		List<Map.Entry<String, long[]>> entries = new ArrayList<>(agg.entrySet());
		entries.sort(Comparator.comparing((Map.Entry<String, long[]> e) -> e.getValue()[1], Comparator.reverseOrder())
			.thenComparing(Map.Entry::getKey));
		for (Map.Entry<String, long[]> e : entries) {
			long[] s = e.getValue();
			for (int i = 0; i < total.length; i++) {
				total[i] += s[i];
			}
			Map<String, Object> row = Results.map("topic", e.getKey(), "partitions", s[4]);
			row.putAll(trend(s, elapsed));
			topics.add(row);
		}
		Map<String, String> endErrors = new TreeMap<>(e0.errorsByTopic());
		e1.errorsByTopic().forEach(endErrors::putIfAbsent);
		Map<String, Object> totals = trend(total, elapsed);
		return Results.map("groupId", group, "state",
				described.ok() ? ConsumerGroups.state(described.value().groupState()) : null, "members",
				described.ok() ? described.value().members().size() : null, "intervalSeconds", interval,
				"elapsedSeconds", elapsed, "shortened", slept < interval * 1000L ? Boolean.TRUE : null, "total",
				totals, "topics", topics, "partitionsSkipped", skipped > 0 ? skipped : null,
				"partitionsWithoutEndOffset", withoutEnd > 0 ? withoutEnd : null, "endOffsetErrors",
				endErrors.isEmpty() ? null : GroupTools.first(endErrors, ScanIssues.MAX_ERROR_SAMPLES), "partial",
				withoutEnd > 0 ? Boolean.TRUE : null, "partitionsWithoutCommit",
				c0.withoutOffset().isEmpty() ? null : c0.withoutOffset().size(), "note",
				"consumeRate: committed offset increase/s, produceRate: end offset increase/s");
	}

	/** s: [lagT0, lagT1, committedDelta, endDelta, partitions] → lag, rates, trend, estimated catch-up time. */
	private static Map<String, Object> trend(long[] s, double elapsed) {
		String trend = s[1] > s[0] ? "growing" : s[1] < s[0] ? "shrinking" : "stable";
		Long eta = null;
		if (s[1] < s[0] && elapsed > 0) {
			double shrinkPerSecond = (s[0] - s[1]) / elapsed;
			eta = Math.round(s[1] / shrinkPerSecond);
		}
		return Results.map("lagT0", s[0], "lagT1", s[1], "consumeRate", Params.rate(s[2], elapsed), "produceRate",
				Params.rate(s[3], elapsed), "trend", trend, "etaSeconds", eta, "resetDetected",
				s[2] < 0 ? Boolean.TRUE : null);
	}

	Object stuck(ToolCall call) {
		int interval = Params.intervalSeconds(call);
		GroupList list = ConsumerGroups.list(call, Params.FIRST_LIST_RESERVE_MS, call::matches);
		Map<String, GroupSummary> summaries = new LinkedHashMap<>();
		list.groups().forEach(g -> summaries.put(g.groupId(), g));
		long t0 = System.nanoTime();
		Map<String, Committed> c0 = GroupLag.committed(call, summaries.keySet(), null,
				Params.FIRST_COMMITTED_RESERVE_MS);
		ScanIssues issues = new ScanIssues();
		int disappeared = 0;
		List<String> withOffsets = new ArrayList<>();
		for (Map.Entry<String, Committed> e : c0.entrySet()) {
			Committed c = e.getValue();
			if (c.error() != null) {
				if (c.notFound()) {
					disappeared++;
				}
				else {
					issues.groupError(e.getKey(), c.error(), c.timedOut());
				}
			}
			else if (!c.offsets().isEmpty()) {
				withOffsets.add(e.getKey());
			}
		}
		Set<TopicPartition> tps = new LinkedHashSet<>();
		withOffsets.forEach(g -> tps.addAll(c0.get(g).offsets().keySet()));
		OffsetLookup e0 = GroupLag.endOffsets(call, tps, Params.FIRST_END_RESERVE_MS);
		issues.endErrors(e0);
		long t0End = System.nanoTime();
		long slept = Params.pause(call, interval);
		if (slept < 0) {
			return firstSampleOnly(call, list, summaries, c0, withOffsets, e0, issues, disappeared, interval);
		}
		long t1 = System.nanoTime();
		Map<String, Committed> c1 = GroupLag.committed(call, withOffsets, null, Params.SECOND_COMMITTED_RESERVE_MS);
		// Partitions whose end offset could not be fetched in the first sample cannot be checked (unchecked); they are
		// not asked again in the second sample.
		OffsetLookup e1 = GroupLag.endOffsets(call, e0.offsets().keySet(), Params.SECOND_END_RESERVE_MS);
		issues.endErrors(e1);
		long t1End = System.nanoTime();
		double elapsed = Params.seconds((t1 + t1End) / 2 - (t0 + t0End) / 2);

		Map<String, List<Map<String, Object>>> stuckRows = new LinkedHashMap<>();
		Map<String, long[]> stats = new LinkedHashMap<>();
		int checkedGroups = 0;
		for (String g : withOffsets) {
			Committed before = c0.get(g);
			Committed after = c1.get(g);
			if (after == null || after.error() != null) {
				if (after == null || after.notFound()) {
					disappeared++;
				}
				else {
					issues.groupError(g, after.error(), after.timedOut());
				}
				continue;
			}
			checkedGroups++;
			List<PartitionLag> stuck = new ArrayList<>();
			Map<TopicPartition, Long> endDelta = new LinkedHashMap<>();
			List<TopicPartition> unchecked = new ArrayList<>();
			long advanced = 0;
			for (Map.Entry<TopicPartition, Long> e : before.offsets().entrySet()) {
				TopicPartition tp = e.getKey();
				Long a = e.getValue();
				Long b = after.offsets().get(tp);
				Long ea = e0.offsets().get(tp);
				Long eb = e1.offsets().get(tp);
				if (ea == null || eb == null) {
					unchecked.add(tp);
					continue;
				}
				if (b == null) {
					// commit deleted between the samples (offsets_delete / reset): not counted as stuck
					continue;
				}
				if (b > a) {
					advanced++;
				}
				else if (b.equals(a) && eb > ea && GroupLag.lag(eb, b) > 0) {
					stuck.add(new PartitionLag(tp.topic(), tp.partition(), b, eb, GroupLag.lag(eb, b), null));
					endDelta.put(tp, eb - ea);
				}
			}
			issues.unknownLag(unchecked);
			if (stuck.isEmpty()) {
				continue;
			}
			stuck.sort(PartitionLag.BY_LAG_DESC);
			long stuckLag = stuck.stream().mapToLong(PartitionLag::lag).sum();
			List<Map<String, Object>> rows = new ArrayList<>();
			stuck.stream()
				.limit(MAX_STUCK_ROWS_PER_GROUP)
				.forEach(p -> rows.add(Results.map("topic", p.topic(), "partition", p.partition(), "committed",
						p.committed(), "end", p.end(), "lag", p.lag(), "endDelta",
						endDelta.get(p.topicPartition()))));
			stuckRows.put(g, rows);
			stats.put(g, new long[] { stuck.size(), stuckLag, advanced, unchecked.size() });
		}
		Map<String, Settled<ConsumerGroupDescription>> described = ConsumerGroups.describe(call, stuckRows.keySet(),
				Params.FINAL_DESCRIBE_RESERVE_MS);
		List<String> order = new ArrayList<>(stuckRows.keySet());
		order.sort(Comparator.comparing((String g) -> stats.get(g)[1], Comparator.reverseOrder())
			.thenComparing(Comparator.naturalOrder()));
		List<Map<String, Object>> groups = new ArrayList<>();
		for (String g : order) {
			long[] s = stats.get(g);
			Settled<ConsumerGroupDescription> d = described.get(g);
			String state = d != null && d.ok() ? ConsumerGroups.state(d.value().groupState())
					: summaries.get(g).state();
			Integer members = d != null && d.ok() ? d.value().members().size() : null;
			groups.add(Results.map("groupId", g, "state", state, "members", members, "stuckPartitions", s[0],
					"stuckLag", s[1], "advancedPartitions", s[2], "fullyStuck", s[2] == 0 && s[3] == 0,
					"uncheckedPartitions", s[3] > 0 ? s[3] : null, "partitions", stuckRows.get(g),
					"partitionsOmitted", s[0] > MAX_STUCK_ROWS_PER_GROUP ? s[0] - MAX_STUCK_ROWS_PER_GROUP : null));
		}
		Map<String, Object> out = Results.map("scannedGroups", withOffsets.size(), "checkedGroups",
				checkedGroups < withOffsets.size() ? checkedGroups : null, "groupsWithoutOffsets",
				summaries.size() - withOffsets.size(), "skippedGroups", disappeared > 0 ? disappeared : null,
				"intervalSeconds", interval, "elapsedSeconds", elapsed, "shortened",
				slept < interval * 1000L ? Boolean.TRUE : null);
		out.putAll(issues.toMap(list));
		out.put("note",
				STUCK_NOTE + "; uncheckedPartitions: could not be checked because the end offset was not available");
		out.putAll(Results.page("groups", groups, call.limit()));
		return out;
	}

	/**
	 * When no time is left for the second sample: stuck groups cannot be detected; the groups with lag in the first
	 * sample (descending, at most limit) are returned with {@code "partial": true}.
	 */
	private static Map<String, Object> firstSampleOnly(ToolCall call, GroupList list,
			Map<String, GroupSummary> summaries, Map<String, Committed> c0, List<String> withOffsets,
			OffsetLookup e0, ScanIssues issues, int disappeared, int interval) {
		Map<String, Committed> sampled = new LinkedHashMap<>();
		withOffsets.forEach(g -> sampled.put(g, c0.get(g)));
		record Lagging(String groupId, long lag) {
		}
		List<Lagging> lagging = new ArrayList<>();
		for (GroupLagInfo info : GroupLag.compute(sampled, e0, Map.of()).values()) {
			issues.unknownLag(info);
			if (info.totalLag() > 0) {
				lagging.add(new Lagging(info.groupId(), info.totalLag()));
			}
		}
		lagging.sort(Comparator.comparingLong(Lagging::lag).reversed().thenComparing(Lagging::groupId));
		List<Map<String, Object>> rows = new ArrayList<>();
		lagging.stream()
			.limit(call.limit())
			.forEach(l -> rows.add(Results.map("groupId", l.groupId(), "state", summaries.get(l.groupId()).state(),
					"lag", l.lag())));
		Map<String, Object> out = Results.map("scannedGroups", withOffsets.size(), "groupsWithoutOffsets",
				summaries.size() - withOffsets.size(), "skippedGroups", disappeared > 0 ? disappeared : null,
				"intervalSeconds", interval, "secondSample", false);
		out.putAll(issues.toMap(list));
		out.put("partial", true);
		out.put("note", "No time left for the second sample (the first sample took too long); stuck groups "
				+ "could not be detected. laggingAtFirstSample: groups with lag in the first sample");
		out.put("laggingAtFirstSample", rows);
		if (lagging.size() > rows.size()) {
			out.put("laggingOmitted", lagging.size() - rows.size());
		}
		return out;
	}

}
