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

/** Lag araçları: lag, with_lag, lag_trend, stuck. */
@Component
public class LagTools {

	static final long DEFAULT_THRESHOLD = 1000;

	static final int TOP_TOPICS = 5;

	static final int MAX_STUCK_ROWS_PER_GROUP = 20;

	private static final String STUCK_NOTE = "Periyodik commit eden (ör. auto commit 5 sn) gruplar kısa aralıkta "
			+ "yanlış pozitif olabilir; members=0 ise tüketici yok";

	public List<ToolSpec> tools() {
		Map<String, Object> threshold = P.integer("En az toplam lag (varsayılan " + DEFAULT_THRESHOLD + ")");
		threshold.put("minimum", 0);
		threshold.put("default", DEFAULT_THRESHOLD);
		return List.of(
				ToolSpec
					.read("consumer_groups_lag",
							"Grubun lag'i: topic toplamları ve partition bazında committed, end, lag, sahip clientId/host (lag'e göre azalan)")
					.required("group", Params.group())
					.param("topic", P.string("Sadece bu topic (tam ad)"))
					.handler(this::lag),
				ToolSpec
					.read("consumer_groups_with_lag",
							"Toplam lag'i eşiğin üstündeki gruplar (azalan): state ve en yüksek lag'li 5 topic ile")
					.param("threshold", threshold)
					.listParams()
					.param("topic", P.string("Sadece bu regex'e uyan topic'ler sayılır"))
					.handler(this::withLag),
				ToolSpec
					.read("consumer_groups_lag_trend",
							"Lag eğilimi: iki örnekle topic bazında lag, tüketim/üretim hızı (mesaj/sn), trend ve yetişme süresi tahmini")
					.required("group", Params.group())
					.param("intervalSeconds", Params.intervalSeconds())
					.handler(this::lagTrend),
				ToolSpec
					.read("consumer_groups_stuck",
							"Takılı gruplar: iki örnek arasında commit'i ilerlemeyen, end offset'i artan ve lag'i olan partition'lar")
					.listParams()
					.param("intervalSeconds", Params.intervalSeconds())
					.handler(this::stuck));
	}

	Object lag(ToolCall call) {
		String group = call.str("group");
		String topic = call.str("topic", null);
		GroupDetail g = GroupDetail.load(call, group, topic == null ? null : topic::equals);
		if (topic != null && g.lag().partitions().isEmpty()) {
			throw new ToolException("'" + group + "' grubunun '" + topic + "' topic'inde commit'i ya da ataması yok");
		}
		Map<String, List<PartitionLag>> byTopic = new LinkedHashMap<>();
		Map<String, Long> lagByTopic = g.lag().lagByTopic();
		lagByTopic.keySet().forEach(t -> byTopic.put(t, new ArrayList<>()));
		for (PartitionLag p : g.lag().partitions()) {
			byTopic.get(p.topic()).add(p);
		}
		List<Map<String, Object>> topics = new ArrayList<>();
		int budget = GroupTools.MAX_PARTITION_ROWS;
		int omitted = 0;
		for (Map.Entry<String, List<PartitionLag>> e : byTopic.entrySet()) {
			List<PartitionLag> partitions = new ArrayList<>(e.getValue());
			partitions.sort(PartitionLag.BY_LAG_DESC);
			List<Map<String, Object>> rows = new ArrayList<>();
			for (PartitionLag p : partitions) {
				if (budget <= 0) {
					omitted++;
					continue;
				}
				budget--;
				MemberDescription owner = g.owners().get(p.topicPartition());
				rows.add(Results.map("partition", p.partition(), "committed", p.committed(), "end", p.end(), "lag",
						p.lag(), "clientId", owner == null ? null : owner.clientId(), "host",
						owner == null ? null : owner.host(), "noCommit", p.committed() == null ? Boolean.TRUE : null,
						"error", p.error()));
			}
			topics.add(Results.map("topic", e.getKey(), "lag", lagByTopic.get(e.getKey()), "partitions", rows));
		}
		int withoutCommit = g.lag().withoutCommit();
		return Results.map("groupId", group, "state", g.state(), "members", g.description().members().size(),
				"totalLag", g.lag().totalLag(), "partitionsWithoutCommit", withoutCommit > 0 ? withoutCommit : null,
				"topics", topics, "partitionRowsOmitted", omitted > 0 ? omitted : null, "note",
				topics.isEmpty() ? "Grubun commit edilmiş offset'i ya da partition ataması yok" : null);
	}

	Object withLag(ToolCall call) {
		long threshold = Math.max(0, call.longValue("threshold", DEFAULT_THRESHOLD));
		Pattern topicRe = call.regex("topic");
		Predicate<String> topicFilter = topicRe == null ? null : t -> topicRe.matcher(t).find();
		GroupList list = ConsumerGroups.list(call);
		List<GroupSummary> groups = list.groups().stream().filter(g -> call.matches(g.groupId())).toList();
		Map<String, GroupLagInfo> lags = GroupLag.snapshot(call, groups.stream().map(GroupSummary::groupId).toList(),
				topicFilter);
		record Row(long total, Map<String, Object> map) {
		}
		List<Row> rows = new ArrayList<>();
		int disappeared = 0;
		List<String> errors = new ArrayList<>();
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
					errors.add(g.groupId() + ": " + info.error());
				}
				continue;
			}
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
				"skippedGroups", disappeared > 0 ? disappeared : null, "groupErrors",
				errors.isEmpty() ? null : errors.size(), "errors",
				errors.isEmpty() ? null : errors.subList(0, Math.min(3, errors.size())), "listErrors",
				list.errors().isEmpty() ? null : list.errors());
		out.putAll(Results.page("groups", sorted, call.limit()));
		return out;
	}

	Object lagTrend(ToolCall call) {
		String group = call.str("group");
		int interval = Params.intervalSeconds(call);
		Settled<ConsumerGroupDescription> described = ConsumerGroups.describe(call, List.of(group)).get(group);
		long t0 = System.nanoTime();
		Committed c0 = GroupLag.committed(call, List.of(group), null).get(group);
		if (c0.error() != null) {
			throw c0.notFound() ? ConsumerGroups.notFoundError(group) : new ToolException(c0.error());
		}
		if (c0.offsets().isEmpty()) {
			if (!described.ok() && ConsumerGroups.notFound(described.error())
					|| described.ok() && ConsumerGroups.deadOrUnknown(described.value())) {
				throw ConsumerGroups.notFoundError(group);
			}
			throw new ToolException("'" + group + "' grubunun commit edilmiş offset'i yok; lag eğilimi hesaplanamaz");
		}
		Set<TopicPartition> tps = new LinkedHashSet<>(c0.offsets().keySet());
		OffsetLookup e0 = GroupLag.endOffsets(call, tps);
		long t0End = System.nanoTime();
		long slept = Params.pause(call, interval);
		long t1 = System.nanoTime();
		Committed c1 = GroupLag.committed(call, List.of(group), null).get(group);
		if (c1.error() != null) {
			throw c1.notFound() ? new ToolException("Grup ölçüm sırasında silindi: " + group)
					: new ToolException(c1.error());
		}
		OffsetLookup e1 = GroupLag.endOffsets(call, tps);
		long t1End = System.nanoTime();
		double elapsed = Params.seconds((t1 + t1End) / 2 - (t0 + t0End) / 2);

		Map<String, long[]> agg = new TreeMap<>();
		int skipped = 0;
		for (TopicPartition tp : tps) {
			Long a = c0.offsets().get(tp);
			Long b = c1.offsets().get(tp);
			Long ea = e0.offsets().get(tp);
			Long eb = e1.offsets().get(tp);
			if (a == null || b == null || ea == null || eb == null) {
				skipped++;
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
		Map<String, Object> totals = trend(total, elapsed);
		return Results.map("groupId", group, "state",
				described.ok() ? ConsumerGroups.state(described.value().groupState()) : null, "members",
				described.ok() ? described.value().members().size() : null, "intervalSeconds", interval,
				"elapsedSeconds", elapsed, "shortened", slept < interval * 1000L ? Boolean.TRUE : null, "total",
				totals, "topics", topics, "partitionsSkipped", skipped > 0 ? skipped : null,
				"partitionsWithoutCommit", c0.withoutOffset().isEmpty() ? null : c0.withoutOffset().size(), "note",
				"consumeRate: commit edilen offset artışı/sn, produceRate: end offset artışı/sn");
	}

	/** s: [lagT0, lagT1, committedDelta, endDelta, partitions] → lag, hızlar, trend, tahmini yetişme süresi. */
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
		GroupList list = ConsumerGroups.list(call);
		Map<String, GroupSummary> summaries = new LinkedHashMap<>();
		list.groups().stream().filter(g -> call.matches(g.groupId())).forEach(g -> summaries.put(g.groupId(), g));
		long t0 = System.nanoTime();
		Map<String, Committed> c0 = GroupLag.committed(call, summaries.keySet(), null);
		List<String> withOffsets = c0.entrySet()
			.stream()
			.filter(e -> e.getValue().error() == null && !e.getValue().offsets().isEmpty())
			.map(Map.Entry::getKey)
			.toList();
		Set<TopicPartition> tps = new LinkedHashSet<>();
		withOffsets.forEach(g -> tps.addAll(c0.get(g).offsets().keySet()));
		OffsetLookup e0 = GroupLag.endOffsets(call, tps);
		long t0End = System.nanoTime();
		long slept = Params.pause(call, interval);
		long t1 = System.nanoTime();
		Map<String, Committed> c1 = GroupLag.committed(call, withOffsets, null);
		OffsetLookup e1 = GroupLag.endOffsets(call, tps);
		long t1End = System.nanoTime();
		double elapsed = Params.seconds((t1 + t1End) / 2 - (t0 + t0End) / 2);

		Map<String, List<Map<String, Object>>> stuckRows = new LinkedHashMap<>();
		Map<String, long[]> stats = new LinkedHashMap<>();
		int disappeared = 0;
		for (String g : withOffsets) {
			Committed before = c0.get(g);
			Committed after = c1.get(g);
			if (after == null || after.error() != null) {
				disappeared++;
				continue;
			}
			List<PartitionLag> stuck = new ArrayList<>();
			Map<TopicPartition, Long> endDelta = new LinkedHashMap<>();
			long advanced = 0;
			for (Map.Entry<TopicPartition, Long> e : before.offsets().entrySet()) {
				TopicPartition tp = e.getKey();
				Long a = e.getValue();
				Long b = after.offsets().get(tp);
				Long ea = e0.offsets().get(tp);
				Long eb = e1.offsets().get(tp);
				if (b == null || ea == null || eb == null) {
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
			stats.put(g, new long[] { stuck.size(), stuckLag, advanced });
		}
		Map<String, Settled<ConsumerGroupDescription>> described = ConsumerGroups.describe(call, stuckRows.keySet());
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
					"stuckLag", s[1], "advancedPartitions", s[2], "fullyStuck", s[2] == 0, "partitions",
					stuckRows.get(g), "partitionsOmitted",
					s[0] > MAX_STUCK_ROWS_PER_GROUP ? s[0] - MAX_STUCK_ROWS_PER_GROUP : null));
		}
		Map<String, Object> out = Results.map("scannedGroups", withOffsets.size(), "groupsWithoutOffsets",
				summaries.size() - withOffsets.size(), "skippedGroups", disappeared > 0 ? disappeared : null,
				"intervalSeconds", interval, "elapsedSeconds", elapsed, "shortened",
				slept < interval * 1000L ? Boolean.TRUE : null, "listErrors",
				list.errors().isEmpty() ? null : list.errors(), "note",
				STUCK_NOTE);
		out.putAll(Results.page("groups", groups, call.limit()));
		return out;
	}

}
