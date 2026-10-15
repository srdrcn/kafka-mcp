package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Topic usage reports: empty topics, idle (not written) topics, topics without consumers. */
@Component
public class TopicUsageTools {

	/** Maximum number of names shown in the unknown/extra lists. */
	static final int SAMPLE = 20;

	/** Note about readers that do not commit. */
	static final String INVISIBLE_READERS = "Only consumer groups with committed offsets are visible: clients that read with "
			+ "assign() without committing, Connect source/internal readers and system readers such as Schema Registry are not shown";

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("report_empty_topics",
						"Topics with no messages (earliest == latest on every partition); never-written topics are flagged")
					.param("includeInternal", ReportParams.includeInternal(false))
					.listParams()
					.handler(this::emptyTopics),
				ToolSpec.read("report_idle_topics",
						"Topics whose last write (maxTimestamp) is older than N days, oldest first; empty topics are listed separately")
					.param("olderThanDays", ReportParams.olderThanDays())
					.listParams()
					.handler(this::idleTopics),
				ToolSpec.read("report_topics_without_consumers",
						"Non-internal topics without committed offsets from any consumer group, with size and last write age")
					.listParams()
					.handler(this::topicsWithoutConsumers));
	}

	Object emptyTopics(ToolCall call) {
		FleetScan scan = FleetScan.of(call).includeInternal(call.bool("includeInternal", false)).ranges().run();
		List<Map<String, Object>> rows = new ArrayList<>();
		List<String> unknown = new ArrayList<>();
		int neverWritten = 0;
		for (String t : scan.topics().keySet()) {
			TopicActivity a = scan.activity(t);
			if (a.empty() == null) {
				unknown.add(t);
			}
			else if (a.empty()) {
				neverWritten += a.neverWritten() ? 1 : 0;
				rows.add(Results.map("topic", t, "partitions", scan.partitions(t), "neverWritten", a.neverWritten()));
			}
		}
		Map<String, Object> out = Results.map("emptyCount", rows.size(), "neverWrittenCount", neverWritten,
				"unknownCount", unknown.isEmpty() ? null : unknown.size(), "unknownTopics",
				unknown.isEmpty() ? null : ReportParams.head(unknown, SAMPLE));
		scan.coverage()
			.note("neverWritten=false: the topic was written to but all data was removed by retention (latest > 0)");
		scan.coverage().addTo(out);
		out.putAll(ReportParams.page("topics", rows, call.limit()));
		return out;
	}

	Object idleTopics(ToolCall call) {
		int days = call.integer("olderThanDays", ReportParams.DEFAULT_OLDER_THAN_DAYS, 1, 3650);
		FleetScan scan = FleetScan.of(call).ranges().maxTimestamps().sizes().run();
		long now = scan.nowMs();
		long threshold = TopicActivity.threshold(now, days);
		List<TopicActivity> idle = new ArrayList<>();
		List<String> empty = new ArrayList<>();
		Map<String, List<String>> unknown = new TreeMap<>();
		for (String t : scan.topics().keySet()) {
			TopicActivity a = scan.activity(t);
			if (Boolean.TRUE.equals(a.empty())) {
				empty.add(t);
				continue;
			}
			Boolean isIdle = a.idleSince(threshold);
			if (Boolean.TRUE.equals(isIdle)) {
				idle.add(a);
			}
			else if (isIdle == null) {
				unknown.computeIfAbsent(a.unknownReason() == null ? "unknown" : a.unknownReason(),
						k -> new ArrayList<>()).add(t);
			}
		}
		idle.sort(Comparator.comparingLong(TopicActivity::lastWriteMs).thenComparing(TopicActivity::topic));
		List<Map<String, Object>> rows = new ArrayList<>();
		for (TopicActivity a : idle) {
			Map<String, Object> row = Results.map("topic", a.topic());
			row.putAll(a.lastWriteFields(now));
			row.put("partitions", scan.partitions(a.topic()));
			row.putAll(scan.sizeFields(a.topic()));
			rows.add(row);
		}
		Map<String, Object> out = Results.map("olderThanDays", days, "threshold", Format.iso(threshold), "idleCount",
				idle.size());
		out.put("empty", Results.map("count", empty.size(), "topics", ReportParams.head(empty, call.limit()),
				"omitted", empty.size() > call.limit() ? empty.size() - call.limit() : null));
		if (!unknown.isEmpty()) {
			out.put("unknown", unknown(unknown));
		}
		scan.coverage().note("Last write: the largest record timestamp across partitions (with CreateTime, the time set by the producer)");
		scan.coverage().addTo(out);
		out.putAll(ReportParams.page("topics", rows, call.limit()));
		return out;
	}

	/** Unknown topics by reason: {count, byReason: {reason: {count, topics}}}. */
	static Map<String, Object> unknown(Map<String, List<String>> byReason) {
		Map<String, Object> reasons = new LinkedHashMap<>();
		int total = 0;
		for (Map.Entry<String, List<String>> e : byReason.entrySet()) {
			total += e.getValue().size();
			reasons.put(e.getKey(), Results.map("count", e.getValue().size(), "topics",
					ReportParams.head(e.getValue(), SAMPLE)));
		}
		return Results.map("count", total, "byReason", reasons);
	}

	Object topicsWithoutConsumers(ToolCall call) {
		FleetScan scan = FleetScan.of(call).ranges().maxTimestamps().sizes().groups(GroupScan.MAX_GROUPS).run();
		GroupScan groups = scan.requireGroups();
		Map<String, Set<String>> byTopic = groups.groupsByTopic();
		long now = scan.nowMs();
		List<String> names = new ArrayList<>();
		for (String t : scan.topics().keySet()) {
			if (!byTopic.containsKey(t)) {
				names.add(t);
			}
		}
		names.sort(Comparator.comparing((String t) -> scan.sizeBytes(t) == null ? 0L : scan.sizeBytes(t))
			.reversed()
			.thenComparing(Comparator.naturalOrder()));
		List<Map<String, Object>> rows = new ArrayList<>();
		for (String t : names) {
			TopicActivity a = scan.activity(t);
			Map<String, Object> row = Results.map("topic", t, "partitions", scan.partitions(t));
			row.putAll(scan.sizeFields(t));
			if (Boolean.TRUE.equals(a.empty())) {
				row.put("empty", true);
			}
			else {
				row.putAll(a.lastWriteFields(now));
			}
			rows.add(row);
		}
		Map<String, Object> out = Results.map("count", rows.size(), "topicsWithConsumers",
				scan.topics().size() - rows.size(), "groupsListed", groups.listed().size());
		if (!groups.complete()) {
			out.put("uncertain", true);
			scan.coverage().note("Offsets of some groups could not be read or were not examined: some listed topics may have consumers");
		}
		scan.coverage().note(INVISIBLE_READERS);
		scan.coverage().addTo(out);
		out.putAll(ReportParams.page("topics", rows, call.limit()));
		return out;
	}

}
