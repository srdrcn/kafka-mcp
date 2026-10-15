package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.MAX_EVIDENCE;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.first;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.omitted;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.partitionNames;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.Diagnosis.evidence;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.consumers.ConsumerGroups;
import io.github.srdrcn.kafkamcp.toolsets.consumers.Settled;
import io.github.srdrcn.kafkamcp.toolsets.diagnose.Checks.Check;

/** diagnose_consumer_group: single-call consumer group health diagnosis. */
@Component
public class DiagnoseGroupTools {

	static final long HIGH_LAG = 10_000;

	static final long GROWING_LAG_MIN = 100;

	static final int MAX_MEMBERS = 20;

	static final int MAX_TOPICS = 20;

	/** At most this many topics are evaluated by the ACL check (the rest are counted and reported). */
	static final int MAX_ACL_TOPICS = 20;

	static final int MAX_GROUP_READERS = 20;

	static final Set<GroupState> REBALANCING = Set.of(GroupState.PREPARING_REBALANCE,
			GroupState.COMPLETING_REBALANCE, GroupState.ASSIGNING, GroupState.RECONCILING);

	private static final long DESCRIBE_MAX_MS = 3_000;

	private static final long COMMITTED_MAX_MS = 3_000;

	/** Time left after the second sample for reading record ages and the final describe. */
	private static final long WINDOW_RESERVE_MS = 4_000;

	private static final long FINAL_DESCRIBE_RESERVE_MS = 1_500;

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("diagnose_consumer_group",
					"Diagnose a consumer group: state/type, members and assignment, lag and trend, stuck partitions, missing topics, coordinator, ACLs")
			.required("group", P.string("Consumer group id"))
			.param("intervalSeconds", DiagnoseParams.interval(DiagnoseParams.DEFAULT_INTERVAL_SECONDS))
			.handler(this::diagnose));
	}

	record Window(OffsetWindow window, Settled<ConsumerGroupDescription> after, StuckCheck stuck) {
	}

	/** ACL view and the topics whose TOPIC ACLs were actually read (only these are evaluated). */
	record GroupAcl(AclView view, List<String> topics, boolean topicScanReady) {
	}

	record TopicScan(Map<TopicPartition, Long> committed, Map<String, TopicDescription> topics, Set<String> missing,
			Map<String, String> topicErrors, Map<TopicPartition, Long> earliest) {
	}

	Object diagnose(ToolCall call) {
		String group = call.str("group");
		long intervalMs = DiagnoseParams.intervalMs(call, DiagnoseParams.DEFAULT_INTERVAL_SECONDS);
		Settled<ConsumerGroupDescription> s0 = ConsumerGroups
			.describe(call, List.of(group), Math.max(0, call.remainingMillis() - DESCRIBE_MAX_MS))
			.get(group);
		if (!s0.ok() && ConsumerGroups.notFound(s0.error())) {
			throw ConsumerGroups.notFoundError(group);
		}
		ConsumerGroupDescription d0 = s0.ok() ? s0.value() : null;
		Set<TopicPartition> assigned = new LinkedHashSet<>(ConsumerGroups.owners(d0).keySet());
		Diagnosis dx = new Diagnosis();
		Map<String, Object> facts = new LinkedHashMap<>();
		String mode;
		try (Checks checks = new Checks(call, dx)) {
			Check<ClusterMeta.Info> meta = checks.start("cluster metadata", () -> ClusterMeta.describe(call, true));
			Check<TopicScan> topics = checks.start("topics and offset ranges",
					() -> topicScan(call, group, assigned));
			Check<Window> window = checks.start("lag and trend", () -> {
				OffsetWindow w = OffsetWindow.measure(call, assigned, List.of(group), false, intervalMs,
						WINDOW_RESERVE_MS);
				StuckCheck stuck = StuckCheck.of(call, w, List.of(group),
						Math.min(StuckCheck.READ_BUDGET_MS, call.remainingMillis() - FINAL_DESCRIBE_RESERVE_MS - 1_000));
				Settled<ConsumerGroupDescription> after = ConsumerGroups
					.describe(call, List.of(group), FINAL_DESCRIBE_RESERVE_MS)
					.get(group);
				return new Window(w, after, stuck);
			});
			Check<GroupAcl> acl = checks.start("ACL", () -> {
				TopicScan ts = topics.await(6_000);
				List<AclView.Resource> resources = new ArrayList<>();
				resources.add(new AclView.Resource(ResourceType.GROUP, group));
				List<String> aclTopics = ts == null ? List.of() : first(ts.topics().keySet(), MAX_ACL_TOPICS);
				aclTopics.forEach(t -> resources.add(new AclView.Resource(ResourceType.TOPIC, t)));
				return new GroupAcl(AclView.load(call, resources, null), aclTopics, ts != null);
			});

			if (d0 == null) {
				dx.warning("GROUP_DESCRIBE_FAILED", "Could not describe the group: " + s0.errorMessage(),
						evidence("group", group), "Check access to the group coordinator and the GROUP DESCRIBE permission");
				dx.markPartial();
			}
			ClusterMeta.Info info = meta.get();
			mode = info == null ? null : info.mode();
			TopicScan ts = topics.get();
			Window win = window.get();
			ConsumerGroupDescription d1 = win != null && win.after() != null && win.after().ok() ? win.after().value()
					: null;
			if (d0 != null && ConsumerGroups.deadOrUnknown(d0) && ts != null && ts.committed().isEmpty()) {
				throw ConsumerGroups.notFoundError(group);
			}
			describeFacts(dx, facts, d0, d1, info);
			assignment(dx, facts, d0, ts);
			lag(dx, facts, group, d0, d1, win, ts, assigned);
			topicsFindings(dx, facts, ts, win == null ? null : win.window(), d0);
			aclFindings(dx, facts, acl.get(), group, ts);
		}
		Map<String, Object> head = Results.map("groupId", group, "cluster", call.cluster(), "mode", mode);
		return dx.result(head, facts);
	}

	// ---- sub-checks ----

	private static TopicScan topicScan(ToolCall call, String group, Set<TopicPartition> assigned) {
		Futures.Each<String, Map<TopicPartition, Long>> c = io.github.srdrcn.kafkamcp.kafka.ConsumerGroups
			.committed(call, List.of(group), List.of(), COMMITTED_MAX_MS);
		if (!c.errors().isEmpty() && c.values().isEmpty()) {
			throw new ToolException(c.errors().values().iterator().next());
		}
		Map<TopicPartition, Long> committed = c.values().getOrDefault(group, Map.of());
		Set<String> names = new TreeSet<>();
		committed.keySet().forEach(tp -> names.add(tp.topic()));
		assigned.forEach(tp -> names.add(tp.topic()));
		Futures.Each<String, TopicDescription> described = Topics.describe(call, names);
		Set<String> missing = new TreeSet<>();
		Map<String, String> errors = new TreeMap<>();
		described.errors().forEach((t, e) -> {
			if (e.contains(UnknownTopicOrPartitionException.class.getSimpleName()) || e.contains("not found")) {
				missing.add(t);
			}
			else {
				errors.put(t, e);
			}
		});
		List<TopicPartition> existing = committed.keySet()
			.stream()
			.filter(tp -> described.values().containsKey(tp.topic()))
			.toList();
		Map<TopicPartition, Long> earliest = new LinkedHashMap<>();
		if (!existing.isEmpty()) {
			ListOffsetsResult r = Offsets.submit(call, existing, OffsetSpec.earliest());
			Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new LinkedHashMap<>();
			existing.forEach(tp -> futures.put(tp, r.partitionResult(tp)));
			Futures.awaitEach(call, futures, 3_000).values().forEach((tp, i) -> earliest.put(tp, i.offset()));
		}
		return new TopicScan(committed, new TreeMap<>(described.values()), missing, errors, earliest);
	}

	// ---- evaluation ----

	private static void describeFacts(Diagnosis dx, Map<String, Object> facts, ConsumerGroupDescription d0,
			ConsumerGroupDescription d1, ClusterMeta.Info info) {
		if (d0 == null) {
			return;
		}
		ConsumerGroupDescription last = d1 != null ? d1 : d0;
		facts.put("state", ConsumerGroups.state(last.groupState()));
		facts.put("type", ConsumerGroups.type(d0.type()));
		if (d0.isSimpleConsumerGroup()) {
			facts.put("simple", true);
		}
		if (d0.partitionAssignor() != null && !d0.partitionAssignor().isBlank()) {
			facts.put("partitionAssignor", d0.partitionAssignor());
		}
		d0.groupEpoch().ifPresent(e -> facts.put("groupEpoch", e));
		d0.targetAssignmentEpoch().ifPresent(e -> facts.put("targetAssignmentEpoch", e));
		Node coordinator = d0.coordinator();
		Map<String, Object> coord = ConsumerGroups.node(coordinator);
		if (coord != null) {
			facts.put("coordinator", coord);
		}
		if (coordinator == null || coordinator.isEmpty()) {
			dx.critical("COORDINATOR_UNAVAILABLE", "Could not determine the group's coordinator broker",
					null, "Check the health of the __consumer_offsets partitions (diagnose_topic topic=__consumer_offsets)");
		}
		else if (info != null && !info.brokerIds().contains(coordinator.id())) {
			dx.critical("COORDINATOR_UNAVAILABLE",
					"Coordinator broker " + coordinator.id() + " is not in the live broker list",
					evidence("coordinator", coordinator.id(), "liveBrokers", info.brokerIds()),
					"Bring the broker up; __consumer_offsets leadership should move to another broker (diagnose_cluster)");
		}
		GroupState s0 = d0.groupState();
		GroupState s1 = d1 == null ? null : d1.groupState();
		if (REBALANCING.contains(s0) || s1 != null && REBALANCING.contains(s1)) {
			dx.warning("REBALANCING",
					"Group is rebalancing/reconciling (" + ConsumerGroups.state(s0)
							+ (s1 != null && s1 != s0 ? " → " + ConsumerGroups.state(s1) : "") + "): consumption may pause",
					evidence("stateAtStart", ConsumerGroups.state(s0), "stateAtEnd", ConsumerGroups.state(s1)),
					"If it happens often, check the members for session.timeout.ms/max.poll.interval.ms overruns and pod "
							+ "restarts; static membership (group.instance.id) or the consumer protocol (KIP-848) reduces "
							+ "the impact of rebalances");
		}
		if (d1 != null) {
			Set<String> m0 = memberIds(d0);
			Set<String> m1 = memberIds(d1);
			if (!m0.equals(m1)) {
				Set<String> joined = new TreeSet<>(m1);
				joined.removeAll(m0);
				Set<String> left = new TreeSet<>(m0);
				left.removeAll(m1);
				dx.info("MEMBERSHIP_CHANGED", "Members changed during the measurement (joined " + joined.size() + ", left "
						+ left.size() + ")", evidence("joined", first(joined, MAX_EVIDENCE), "joinedOmitted",
								omitted(joined), "left", first(left, MAX_EVIDENCE), "leftOmitted", omitted(left)),
						"If it keeps changing, consumers may be restarting or dropping out of the group");
			}
		}
	}

	private static Set<String> memberIds(ConsumerGroupDescription d) {
		Set<String> out = new TreeSet<>();
		d.members().forEach(m -> out.add(m.consumerId()));
		return out;
	}

	private static void assignment(Diagnosis dx, Map<String, Object> facts, ConsumerGroupDescription d,
			TopicScan ts) {
		if (d == null) {
			return;
		}
		List<Map<String, Object>> members = new ArrayList<>();
		List<String> idle = new ArrayList<>();
		int reconciling = 0;
		for (MemberDescription m : ConsumerGroups.sortedMembers(d)) {
			int n = ConsumerGroups.assignedCount(m);
			if (n == 0) {
				idle.add(m.clientId() + "@" + m.host());
			}
			Integer target = m.targetAssignment().map(t -> t.topicPartitions().size()).orElse(null);
			boolean pending = m.targetAssignment().isPresent()
					&& !Objects.equals(m.targetAssignment().get().topicPartitions(),
							m.assignment() == null ? Set.of() : m.assignment().topicPartitions());
			reconciling += pending ? 1 : 0;
			if (members.size() < MAX_MEMBERS) {
				members.add(Results.map("clientId", m.clientId(), "host", m.host(), "memberId", m.consumerId(),
						"groupInstanceId", m.groupInstanceId().orElse(null), "assignedPartitions", n,
						"targetPartitions", pending ? target : null, "memberEpoch", m.memberEpoch().orElse(null),
						"topics", m.assignment() == null ? null
								: List.copyOf(ConsumerGroups.byTopic(m.assignment().topicPartitions()).keySet())));
			}
		}
		facts.put("memberCount", d.members().size());
		facts.put("members", members);
		if (d.members().size() > MAX_MEMBERS) {
			facts.put("membersOmitted", d.members().size() - MAX_MEMBERS);
		}
		boolean stable = d.groupState() == GroupState.STABLE;
		if (stable && !idle.isEmpty()) {
			dx.info("IDLE_MEMBERS",
					idle.size() + " member(s) have no partitions assigned (there may be more members than partitions)",
					evidence("members", first(idle, MAX_EVIDENCE), "omitted", omitted(idle)),
					"Extra consumers stay idle; to scale, increase the partition count (topics_partitions_increase) or reduce the members");
		}
		if (reconciling > 0) {
			dx.info("ASSIGNMENT_RECONCILING", reconciling + " member(s) have a current assignment that differs from the target (consumer protocol reconciliation in progress)",
					null, "This should settle shortly; if it takes long, check the members' logs");
		}
		if (!stable || ts == null) {
			return;
		}
		Map<TopicPartition, MemberDescription> owners = ConsumerGroups.owners(d);
		Set<String> subscribed = new TreeSet<>();
		owners.keySet().forEach(tp -> subscribed.add(tp.topic()));
		List<TopicPartition> unassigned = new ArrayList<>();
		for (String t : subscribed) {
			TopicDescription td = ts.topics().get(t);
			if (td == null) {
				continue;
			}
			for (TopicPartition tp : Topics.partitions(td)) {
				if (!owners.containsKey(tp)) {
					unassigned.add(tp);
				}
			}
		}
		if (!unassigned.isEmpty()) {
			dx.warning("UNASSIGNED_PARTITIONS",
					unassigned.size() + " partition(s) are not assigned to any member although the group is Stable: they are not consumed",
					evidence("partitions", partitionNames(unassigned), "omitted", omitted(unassigned)),
					"If the partition count was just increased, they are assigned after a metadata refresh (metadata.max.age.ms); "
							+ "otherwise check the assignor/subscription (regex) settings and the consumer logs");
		}
	}

	private static void lag(Diagnosis dx, Map<String, Object> facts, String group, ConsumerGroupDescription d0,
			ConsumerGroupDescription d1, Window win, TopicScan ts, Set<TopicPartition> assigned) {
		if (win == null) {
			return;
		}
		OffsetWindow w = win.window();
		OffsetWindow.GroupProgress p = w.progress(group);
		StuckCheck.Verdict v = win.stuck().verdict(group);
		List<StuckCheck.Assessed> stuck = v.stuck();
		List<StuckCheck.Assessed> suspect = v.suspect();
		List<StuckCheck.Assessed> periodic = v.periodic();
		Map<String, Object> lag = new LinkedHashMap<>(p.summary());
		if (w.complete()) {
			lag.put("stuckPartitions", stuck.size());
			if (!suspect.isEmpty()) {
				lag.put("stuckSuspectPartitions", suspect.size());
			}
		}
		lag.putAll(w.describe());
		facts.put("lag", lag);
		List<Map<String, Object>> byTopic = new ArrayList<>();
		for (OffsetWindow.TopicProgress t : first(p.byTopic(), MAX_TOPICS)) {
			byTopic.add(Results.map("topic", t.topic(), "lag", t.lag1(), "partitions", t.partitions(), "consumeRate",
					w.rate(t.consumed()), "produceRate", w.rate(t.produced()), "trend",
					w.complete() ? t.lag1() > t.lag0() ? "growing" : t.lag1() < t.lag0() ? "shrinking" : "stable" : null,
					"commitNotAdvancedPartitions", t.stuck() > 0 ? t.stuck() : null));
		}
		facts.put("lagByTopic", byTopic);
		if (p.byTopic().size() > MAX_TOPICS) {
			facts.put("lagByTopicOmitted", p.byTopic().size() - MAX_TOPICS);
		}
		if (!w.groupErrors().isEmpty()) {
			dx.warning("OFFSETS_UNAVAILABLE", "Could not read the group's committed offsets",
					evidence("errors", w.groupErrors()), "Check the GROUP DESCRIBE permission and access to the coordinator");
			dx.markPartial();
		}
		if (!w.endErrors().isEmpty()) {
			dx.markPartial();
		}
		ConsumerGroupDescription d = d1 != null ? d1 : d0;
		int members = d == null ? 0 : d.members().size();
		boolean empty = d != null && d.groupState() == GroupState.EMPTY;
		int partitions = ts == null ? -1
				: ts.topics().values().stream().filter(td -> p.byTopic().stream().anyMatch(t -> t.topic().equals(td.name())))
					.mapToInt(td -> td.partitions().size())
					.sum();
		Map<String, Object> window = v.window();
		if (!stuck.isEmpty()) {
			Map<String, Object> ev = evidence("partitions", v.evidence(StuckCheck.Level.STUCK, MAX_EVIDENCE), "omitted",
					omitted(stuck), "members", members);
			ev.putAll(window);
			String age = " and the oldest pending record was written " + v.maxBehindSeconds(StuckCheck.Level.STUCK) / 60 + " min ago";
			if (members > 0) {
				dx.critical("STUCK_PARTITIONS",
						stuck.size() + " partition(s) have a commit that does not advance while new messages arrive" + age
								+ " (members present): consumption is stuck",
						ev, "Inspect the logs of the members owning these partitions (poison pill, long processing, "
								+ "max.poll.interval.ms overrun, commit errors); restart the consumer if needed");
			}
			else {
				dx.warning("STUCK_PARTITIONS",
						stuck.size() + " partition(s) have a commit that does not advance while new messages arrive" + age
								+ ": the group has no active members" + (empty ? " (EMPTY), lag " + p.lag() : ""),
						ev, "Start the consumer application. To skip the backlog, use consumer_groups_offsets_reset while the group "
								+ "is EMPTY (to=latest or timestamp); if the group is abandoned, consumer_groups_delete");
			}
		}
		if (!suspect.isEmpty()) {
			Map<String, Object> ev = evidence("partitions", v.evidence(StuckCheck.Level.SUSPECT, MAX_EVIDENCE),
					"omitted", omitted(suspect), "members", members);
			ev.putAll(window);
			dx.warning("STUCK_SUSPECT",
					suspect.size() + " partition(s) did not advance their commit in a " + Math.round(v.windowSeconds())
							+ " s window and the oldest pending record was written about "
							+ v.maxBehindSeconds(StuckCheck.Level.SUSPECT) + " s ago (periodic commit allowance of "
							+ StuckCheck.COMMIT_ALLOWANCE_SECONDS + " s exceeded)"
							+ (members == 0 ? "; the group has no active members" : ""),
					ev, "Check again in a few minutes with diagnose_consumer_group or consumer_groups_lag_trend; if it does not "
							+ "progress, inspect the logs of the owning members (poison pill, long processing, "
							+ "max.poll.interval.ms, commit errors)");
		}
		if (!periodic.isEmpty()) {
			Map<String, Object> ev = evidence("partitions", v.evidence(StuckCheck.Level.PERIODIC, MAX_EVIDENCE),
					"omitted", omitted(periodic));
			ev.putAll(window);
			dx.info("COMMIT_NOT_ADVANCED",
					periodic.size() + " partition(s) did not advance their commit in a " + Math.round(v.windowSeconds())
							+ " s window but the oldest pending record was written about "
							+ v.maxBehindSeconds(StuckCheck.Level.PERIODIC)
							+ " s ago: periodic commit (auto commit 5 s, Streams 30 s, Connect 60 s) likely, "
							+ "no evidence of being stuck",
					ev, "To be sure, run again with intervalSeconds=10 or in a few minutes");
		}
		boolean stalled = !stuck.isEmpty() || !suspect.isEmpty();
		if (empty && p.lag() > 0 && !stalled) {
			dx.warning("NO_ACTIVE_MEMBERS", "Group is EMPTY (no active consumers) with a lag of " + p.lag() + " messages",
					evidence("lag", p.lag()),
					"Start the consumers. To skip the backlog, use consumer_groups_offsets_reset while the group is EMPTY "
							+ "(to=latest or timestamp); if the group is abandoned, consumer_groups_delete");
		}
		boolean growing = "growing".equals(p.trend()) && p.lag() >= GROWING_LAG_MIN && !stalled
				&& p.lagBeyondCommitAllowance();
		if (growing) {
			String scale = partitions > 0 && members < partitions
					? "Increase the members (" + members + ") up to the partition count (" + partitions + ")"
					: "Members already match the partition count: increase the partitions (topics_partitions_increase) or "
							+ "speed up processing (batching, async processing)";
			dx.warning("LAG_GROWING",
					"Lag is growing: consuming " + p.consumeRate() + " msg/s, producing " + p.produceRate() + " msg/s; lag exceeds "
							+ StuckCheck.COMMIT_ALLOWANCE_SECONDS + " s of production",
					evidence("lagAtStart", p.lag0(), "lag", p.lag(), "members", members, "partitions",
							partitions > 0 ? partitions : null, "windowSeconds", window.get("windowSeconds")),
					scale + (empty ? "" : ". To reset offsets, stop the consumers first (the group must be EMPTY)"));
		}
		else if (p.lag() >= HIGH_LAG && !stalled) {
			dx.info("HIGH_LAG", "Total lag " + p.lag() + (p.etaSeconds() != null
					? " (shrinking, estimated catch-up in " + p.etaSeconds() + " s)" : ""), evidence("lag", p.lag()),
					"Follow it with consumer_groups_lag_trend");
		}
		if (p.resetDetected()) {
			dx.info("OFFSET_RESET_DETECTED", "The commit went backwards during the measurement (offset reset or reprocessing)", null,
					null);
		}
		Set<TopicPartition> committed = ts == null ? Set.of() : ts.committed().keySet();
		List<TopicPartition> noCommit = assigned.stream().filter(tp -> !committed.contains(tp)).toList();
		if (ts != null && !noCommit.isEmpty()) {
			dx.info("PARTITIONS_WITHOUT_COMMIT",
					noCommit.size() + " assigned partition(s) have no commit (lag cannot be computed; auto.offset.reset applies)",
					evidence("partitions", partitionNames(noCommit), "omitted", omitted(noCommit)),
					"Check that the consumer commits (enable.auto.commit or commitSync/commitAsync)");
		}
	}

	private static void topicsFindings(Diagnosis dx, Map<String, Object> facts, TopicScan ts, OffsetWindow w,
			ConsumerGroupDescription d) {
		if (ts == null) {
			return;
		}
		facts.put("topics", first(ts.topics().keySet(), MAX_TOPICS));
		if (ts.topics().size() > MAX_TOPICS) {
			facts.put("topicsOmitted", ts.topics().size() - MAX_TOPICS);
		}
		if (!ts.missing().isEmpty()) {
			List<String> missing = List.copyOf(ts.missing());
			dx.warning("MISSING_TOPICS",
					missing.size() + " deleted topic(s) still have committed offsets",
					evidence("topics", first(missing, MAX_EVIDENCE), "omitted", omitted(missing)),
					"If not needed, delete the offsets of these topics with consumer_groups_offsets_delete (reduces lag/monitoring noise)");
		}
		if (!ts.topicErrors().isEmpty()) {
			dx.markPartial();
		}
		List<String> beforeStart = new ArrayList<>();
		List<String> afterEnd = new ArrayList<>();
		ts.committed().forEach((tp, c) -> {
			Long e = ts.earliest().get(tp);
			Long end = w == null ? null : w.end0().get(tp);
			if (e != null && c < e) {
				beforeStart.add(tp.toString());
			}
			else if (end != null && c > end) {
				afterEnd.add(tp.toString());
			}
		});
		boolean empty = d != null && d.groupState() == GroupState.EMPTY;
		if (!beforeStart.isEmpty()) {
			dx.warning("COMMITTED_BEFORE_LOG_START",
					beforeStart.size() + " partition(s) have a commit older than the log start: messages were deleted before being read",
					evidence("partitions", first(new TreeSet<>(beforeStart), MAX_EVIDENCE), "omitted", omitted(beforeStart)),
					"auto.offset.reset applies when the consumer runs" + (empty
							? "; it can be fixed now (group is EMPTY) with consumer_groups_offsets_reset to=earliest"
							: "; consider increasing retention.ms or speeding up consumption"));
		}
		if (!afterEnd.isEmpty()) {
			dx.warning("COMMITTED_AFTER_LOG_END",
					afterEnd.size() + " partition(s) have a commit beyond the log end (the topic may have been recreated or truncated)",
					evidence("partitions", first(new TreeSet<>(afterEnd), MAX_EVIDENCE), "omitted", omitted(afterEnd)),
					empty ? "Group is EMPTY: move it to a valid offset with consumer_groups_offsets_reset"
							: "Stop the consumers (group EMPTY) and fix it with consumer_groups_offsets_reset");
		}
	}

	private static void aclFindings(Diagnosis dx, Map<String, Object> facts, GroupAcl acl, String group,
			TopicScan ts) {
		if (acl == null) {
			return;
		}
		AclView v = acl.view();
		if (!v.authorizerEnabled()) {
			facts.put("access", Results.map("authorizer", false));
			return;
		}
		List<String> groupReaders = v.allowedPrincipals(AclOperation.READ, ResourceType.GROUP, group);
		// only topics whose TOPIC ACLs were actually read are evaluated (unread ones do not count as "not authorized")
		List<String> topics = acl.topics();
		List<String> unchecked = ts == null ? List.of()
				: ts.topics().keySet().stream().filter(t -> !topics.contains(t)).toList();
		List<Map<String, Object>> missing = new ArrayList<>();
		for (String p : groupReaders) {
			if (v.superUsers().contains(p)) {
				continue;
			}
			List<String> cannotRead = new ArrayList<>();
			for (String t : topics) {
				boolean ok = false;
				for (String host : v.hosts(p)) {
					if (v.decide(p, AclOperation.READ, ResourceType.TOPIC, t, host).allowed()) {
						ok = true;
						break;
					}
				}
				if (!ok) {
					cannotRead.add(t);
				}
			}
			if (!cannotRead.isEmpty()) {
				missing.add(Results.map("principal", p, "topics", first(cannotRead, MAX_EVIDENCE), "topicsOmitted",
						omitted(cannotRead)));
			}
		}
		List<String> readers = new TreeSet<>(groupReaders).stream().toList();
		facts.put("access", Results.map("authorizer", true, "groupReaders", first(readers, MAX_GROUP_READERS),
				"groupReadersOmitted", omitted(readers, MAX_GROUP_READERS), "topicsEvaluated", topics.size(),
				"topicsNotEvaluated", unchecked.isEmpty() ? null : unchecked.size(), "note",
				"Kafka does not expose the members' principals; the evaluation covers principals with GROUP READ ACLs on this group"));
		if (!unchecked.isEmpty()) {
			boolean capped = acl.topicScanReady();
			dx.info("ACL_TOPICS_NOT_EVALUATED",
					unchecked.size() + " topic(s) were not evaluated for TOPIC READ ACLs"
							+ (capped ? " (at most " + MAX_ACL_TOPICS + " topics are checked)"
									: " (the topic scan did not finish in time for the ACL check)"),
					evidence("topics", first(unchecked, MAX_EVIDENCE), "omitted", omitted(unchecked)),
					"Check these topics with diagnose_access intent=consume or acls_list");
			if (!capped) {
				dx.markPartial();
			}
		}
		if (groupReaders.isEmpty()) {
			dx.info("NO_GROUP_ACLS",
					"No ACL grants GROUP READ on this group: it can only be used by super users"
							+ (v.allowEveryone().value() ? " (or by everyone, since allow.everyone.if.no.acl.found=true)" : ""),
					null, "Use acls_create preset=consumer (topic, group) for the application principal");
		}
		if (!missing.isEmpty()) {
			dx.warning("ACL_TOPIC_READ_MISSING",
					missing.size() + " principal(s) can use the group but lack TOPIC READ on the topics it consumes",
					evidence("principals", first(missing, MAX_EVIDENCE), "omitted", omitted(missing)),
					"Grant the missing permission with acls_create (resourceType=TOPIC, operation=READ) or verify with diagnose_access intent=consume");
		}
	}

}
