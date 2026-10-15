package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.toolsets.consumers.ConsumerGroups;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag;

/**
 * Batched scan of consumer groups: one listGroups and one listConsumerGroupOffsets for at most
 * {@code maxGroups} groups (all partitions of each group). Over the limit, active (Stable/rebalancing) groups are
 * picked first, then Empty ones; the rest are counted ({@link #skipped()}).
 */
public final class GroupScan {

	/** Maximum number of groups whose committed offsets are read (one batched request). */
	public static final int MAX_GROUPS = 1000;

	private final List<ConsumerGroups.GroupSummary> listed;

	private final List<String> listErrors;

	private final Map<String, GroupLag.Committed> committed;

	private final int skipped;

	GroupScan(List<ConsumerGroups.GroupSummary> listed, List<String> listErrors,
			Map<String, GroupLag.Committed> committed, int skipped) {
		this.listed = listed;
		this.listErrors = listErrors;
		this.committed = committed;
		this.skipped = skipped;
	}

	/**
	 * Lists the groups and reads the committed offsets of the selected ones. Both steps keep {@code reserveMillis}
	 * before the deadline; groups that do not respond become {@link GroupLag.Committed#timedOut()}.
	 */
	public static GroupScan run(ToolCall call, int maxGroups, long reserveMillis) {
		ConsumerGroups.GroupList list = ConsumerGroups.list(call, reserveMillis + 1_000);
		List<ConsumerGroups.GroupSummary> ordered = new ArrayList<>(list.groups());
		ordered.sort(Comparator.comparingInt((ConsumerGroups.GroupSummary g) -> priority(g.state()))
			.thenComparing(ConsumerGroups.GroupSummary::groupId));
		List<String> selected = new ArrayList<>();
		for (ConsumerGroups.GroupSummary g : ordered) {
			if (selected.size() >= maxGroups) {
				break;
			}
			selected.add(g.groupId());
		}
		Map<String, GroupLag.Committed> committed = GroupLag.committed(call, selected, null, reserveMillis);
		return new GroupScan(list.groups(), list.errors(), committed, ordered.size() - selected.size());
	}

	/** Sampling priority: active groups first, then Empty, Dead/unknown last. */
	static int priority(String state) {
		if (state == null) {
			return 3;
		}
		return switch (state) {
			case "Stable" -> 0;
			case "PreparingRebalance", "CompletingRebalance", "Assigning", "Reconciling" -> 1;
			case "Empty" -> 2;
			default -> 3;
		};
	}

	/** All listed groups (by groupId). */
	public List<ConsumerGroups.GroupSummary> listed() {
		return listed;
	}

	/** Messages of the brokers that failed listGroups (if any, the list may be incomplete). */
	public List<String> listErrors() {
		return listErrors;
	}

	/** Groups whose offsets were requested and their results (including per-group error/timedOut). */
	public Map<String, GroupLag.Committed> committed() {
		return committed;
	}

	/** Number of groups whose offsets were not read because of the limit. */
	public int skipped() {
		return skipped;
	}

	/** Group id → state (Stable, Empty, ...; null value if unknown). */
	public Map<String, String> states() {
		Map<String, String> out = new LinkedHashMap<>();
		listed.forEach(g -> out.put(g.groupId(), g.state()));
		return out;
	}

	/** Number of groups whose offset request got no response. */
	public int timedOut() {
		return (int) committed.values().stream().filter(GroupLag.Committed::timedOut).count();
	}

	/** Number of groups whose offsets failed (other than no response or deleted in the meantime). */
	public int failed() {
		return (int) committed.values()
			.stream()
			.filter(c -> c.error() != null && !c.timedOut() && !c.notFound())
			.count();
	}

	/** Were all groups listed and all their offsets read? */
	public boolean complete() {
		return skipped == 0 && listErrors.isEmpty() && timedOut() == 0 && failed() == 0;
	}

	/** Group → committed offsets (successful reads only; partitions without offsets excluded). */
	public Map<String, Map<TopicPartition, Long>> offsets() {
		Map<String, Map<TopicPartition, Long>> out = new TreeMap<>();
		committed.forEach((g, c) -> {
			if (c.error() == null) {
				out.put(g, c.offsets());
			}
		});
		return out;
	}

	/** Topic → groups with committed offsets on that topic (sorted). */
	public Map<String, Set<String>> groupsByTopic() {
		Map<String, Set<String>> out = new TreeMap<>();
		offsets().forEach((g, offsets) -> {
			for (TopicPartition tp : offsets.keySet()) {
				out.computeIfAbsent(tp.topic(), k -> new TreeSet<>()).add(g);
			}
		});
		return out;
	}

	/** All partitions touched by committed offsets. */
	public Set<TopicPartition> committedPartitions() {
		Set<TopicPartition> out = new java.util.HashSet<>();
		offsets().values().forEach(m -> out.addAll(m.keySet()));
		return out;
	}

	/** Adds the coverage info: examined/skipped group counts, sampling and errors. */
	public void describe(Coverage coverage) {
		coverage.examined("groups", committed.size());
		coverage.skipped("groupsOverLimit", skipped);
		if (skipped > 0) {
			coverage.sampled();
			coverage.note("Group count " + listed.size() + " > " + committed.size()
					+ ": only priority groups (active, then Empty) were examined");
		}
		int timedOut = timedOut();
		if (timedOut > 0) {
			coverage.partial();
			coverage.skipped("groupsNoResponse", timedOut);
		}
		coverage.skipped("groupsFailed", failed());
		if (!listErrors.isEmpty()) {
			coverage.partial();
			coverage.error("listGroups", listErrors);
		}
	}

}
