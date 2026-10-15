package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.consumers.ConsumerGroups.GroupList;
import io.github.srdrcn.kafkamcp.toolsets.consumers.ConsumerGroups.GroupSummary;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.GroupLagInfo;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.PartitionLag;

/** Consumer group listeleme/detay araçları: list, get, find_by_member, empty, acls. */
@Component
public class GroupTools {

	/** consumer_groups_get'te en fazla gösterilen partition satırı (fazlası sayılarak atlanır). */
	static final int MAX_PARTITION_ROWS = 1000;

	/** consumer_groups_empty'de grup başına gösterilen en fazla topic. */
	static final int MAX_TOPICS_PER_GROUP = 20;

	private static final String NO_ACL_NOTE = "Bu gruba uyan ACL yok "
			+ "(allow.everyone.if.no.acl.found=false ise sadece süper kullanıcılar erişir)";

	static final List<String> STATES = List.of("Stable", "Empty", "PreparingRebalance", "CompletingRebalance", "Dead",
			"Assigning", "Reconciling", "Unknown");

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("consumer_groups_list", "Consumer group'ları listeler (groupId, state, type, protocol)")
					.listParams()
					.param("state", P.enumOf("Sadece bu durumdaki gruplar", STATES))
					.handler(this::list),
				ToolSpec
					.read("consumer_groups_get",
							"Group detayı: state, coordinator, assignor, üyeler ve atamaları, partition bazında committed/end offset, lag ve sahip üye")
					.required("group", Params.group())
					.handler(this::get),
				ToolSpec
					.read("consumer_groups_find_by_member",
							"Üye host'u ya da clientId'si (regex) eşleşen grupları ve üyelerini bulur (en az biri gerekli)")
					.param("host", P.string("Üye host regex'i (ör. 10\\.1\\.2\\.)"))
					.param("clientId", P.string("Üye clientId regex'i"))
					.handler(this::findByMember),
				ToolSpec
					.read("consumer_groups_empty",
							"Empty durumdaki gruplar, hâlâ commit'li offset'i olan topic'leri ve toplam lag'leri (eski grup adayları)")
					.listParams()
					.handler(this::empty),
				ToolSpec
					.read("consumer_groups_acls",
							"Gruba uygulanan ACL'ler (literal, prefixed, wildcard): principal, host, operation, permission")
					.required("group", Params.group())
					.handler(this::acls));
	}

	Object list(ToolCall call) {
		String state = call.str("state", null);
		if (state != null && STATES.stream().noneMatch(state::equalsIgnoreCase)) {
			throw new ToolException("Geçersiz state: " + state + " (geçerli: " + STATES + ")");
		}
		GroupList list = ConsumerGroups.list(call);
		Map<String, Integer> byState = new TreeMap<>();
		List<Map<String, Object>> rows = new ArrayList<>();
		for (GroupSummary g : list.groups()) {
			if (!call.matches(g.groupId())) {
				continue;
			}
			byState.merge(g.state() == null ? "Unknown" : g.state(), 1, Integer::sum);
			if (state == null || state.equalsIgnoreCase(g.state() == null ? "Unknown" : g.state())) {
				rows.add(g.toMap());
			}
		}
		Map<String, Object> out = Results.map("byState", byState, "listErrors",
				list.errors().isEmpty() ? null : list.errors());
		out.putAll(Results.page("groups", rows, call.limit()));
		return out;
	}

	Object get(ToolCall call) {
		String group = call.str("group");
		GroupDetail g = GroupDetail.load(call, group, null);
		ConsumerGroupDescription d = g.description();
		List<Map<String, Object>> members = new ArrayList<>();
		for (MemberDescription m : ConsumerGroups.sortedMembers(d)) {
			members.add(ConsumerGroups.memberMap(m));
		}
		Map<String, List<PartitionLag>> byTopic = new TreeMap<>();
		for (PartitionLag p : g.lag().partitions()) {
			byTopic.computeIfAbsent(p.topic(), k -> new ArrayList<>()).add(p);
		}
		Map<String, Long> lagByTopic = g.lag().lagByTopic();
		List<Map<String, Object>> topics = new ArrayList<>();
		int budget = MAX_PARTITION_ROWS;
		int omitted = 0;
		for (Map.Entry<String, List<PartitionLag>> e : byTopic.entrySet()) {
			List<Map<String, Object>> rows = new ArrayList<>();
			for (PartitionLag p : e.getValue()) {
				if (budget <= 0) {
					omitted++;
					continue;
				}
				budget--;
				MemberDescription owner = g.owners().get(p.topicPartition());
				rows.add(Results.map("partition", p.partition(), "committed", p.committed(), "end", p.end(), "lag",
						p.lag(), "owner", owner == null ? null : owner.consumerId(), "noCommit",
						p.committed() == null ? Boolean.TRUE : null, "error", p.error()));
			}
			topics.add(Results.map("topic", e.getKey(), "lag", lagByTopic.get(e.getKey()), "partitions", rows));
		}
		int withoutCommit = g.lag().withoutCommit();
		Map<String, Object> totals = Results.map("lag", g.lag().totalLag(), "lagByTopic", lagByTopic, "topics",
				byTopic.size(), "partitions", g.lag().partitions().size(), "partitionsWithoutCommit",
				withoutCommit > 0 ? withoutCommit : null);
		String assignor = d.partitionAssignor() == null || d.partitionAssignor().isBlank() ? null
				: d.partitionAssignor();
		return Results.map("groupId", d.groupId(), "state", g.state(), "type", ConsumerGroups.type(d.type()),
				"simple", d.isSimpleConsumerGroup() ? Boolean.TRUE : null, "coordinator",
				ConsumerGroups.node(d.coordinator()), "partitionAssignor", assignor, "memberCount", members.size(),
				"members", members, "offsets", topics, "totals", totals, "partitionRowsOmitted",
				omitted > 0 ? omitted : null, "note",
				withoutCommit > 0 ? "noCommit: atanmış ama commit edilmiş offset'i olmayan partition (lag hesaplanmadı)"
						: null);
	}

	Object findByMember(ToolCall call) {
		Pattern host = call.regex("host");
		Pattern clientId = call.regex("clientId");
		if (host == null && clientId == null) {
			throw new ToolException("host ya da clientId parametrelerinden en az biri gerekli");
		}
		GroupList list = ConsumerGroups.list(call);
		List<GroupSummary> candidates = list.groups().stream().filter(GroupSummary::mayHaveMembers).toList();
		Map<String, Settled<ConsumerGroupDescription>> described = ConsumerGroups.describe(call,
				candidates.stream().map(GroupSummary::groupId).toList());
		List<Map<String, Object>> groups = new ArrayList<>();
		int matchedMembers = 0;
		int skipped = 0;
		for (Map.Entry<String, Settled<ConsumerGroupDescription>> e : described.entrySet()) {
			if (!e.getValue().ok()) {
				skipped++;
				continue;
			}
			ConsumerGroupDescription d = e.getValue().value();
			List<Map<String, Object>> members = new ArrayList<>();
			for (MemberDescription m : ConsumerGroups.sortedMembers(d)) {
				if (host != null && (m.host() == null || !host.matcher(m.host()).find())) {
					continue;
				}
				if (clientId != null && (m.clientId() == null || !clientId.matcher(m.clientId()).find())) {
					continue;
				}
				Collection<String> topics = m.assignment() == null ? List.of()
						: ConsumerGroups.byTopic(m.assignment().topicPartitions()).keySet();
				members.add(Results.map("memberId", m.consumerId(), "groupInstanceId",
						m.groupInstanceId().orElse(null), "clientId", m.clientId(), "host", m.host(), "topics",
						new ArrayList<>(topics)));
			}
			if (!members.isEmpty()) {
				matchedMembers += members.size();
				groups.add(Results.map("groupId", d.groupId(), "state", ConsumerGroups.state(d.groupState()),
						"members", members));
			}
		}
		return Results.map("scannedGroups", candidates.size(), "skippedGroups", skipped > 0 ? skipped : null,
				"matchedGroups", groups.size(), "matchedMembers", matchedMembers, "listErrors",
				list.errors().isEmpty() ? null : list.errors(), "groups", groups);
	}

	Object empty(ToolCall call) {
		GroupList list = ConsumerGroups.list(call);
		List<GroupSummary> empty = list.groups()
			.stream()
			.filter(GroupSummary::isEmptyState)
			.filter(g -> call.matches(g.groupId()))
			.toList();
		Map<String, GroupLagInfo> lags = GroupLag.snapshot(call, empty.stream().map(GroupSummary::groupId).toList(),
				null);
		List<Map<String, Object>> rows = new ArrayList<>();
		int disappeared = 0;
		for (GroupSummary g : empty) {
			GroupLagInfo info = lags.get(g.groupId());
			if (info == null) {
				continue;
			}
			if (info.error() != null) {
				disappeared++;
				continue;
			}
			Map<String, Long> byTopic = new TreeMap<>(info.lagByTopic());
			List<Map<String, Object>> topics = new ArrayList<>();
			byTopic.entrySet()
				.stream()
				.limit(MAX_TOPICS_PER_GROUP)
				.forEach(e -> topics.add(Results.map("topic", e.getKey(), "lag", e.getValue())));
			int withoutEnd = info.withoutEnd();
			rows.add(Results.map("groupId", g.groupId(), "type", g.type(), "simple", g.simple() ? Boolean.TRUE : null,
					"totalLag", info.totalLag(), "topicCount", byTopic.size(), "partitions", info.partitions().size(),
					"topics", topics, "topicsOmitted",
					byTopic.size() > MAX_TOPICS_PER_GROUP ? byTopic.size() - MAX_TOPICS_PER_GROUP : null,
					"partitionsWithoutEndOffset", withoutEnd > 0 ? withoutEnd : null));
		}
		Map<String, Object> out = Results.map("skippedGroups", disappeared > 0 ? disappeared : null, "listErrors",
				list.errors().isEmpty() ? null : list.errors());
		out.putAll(Results.page("groups", rows, call.limit()));
		return out;
	}

	Object acls(ToolCall call) {
		String group = call.str("group");
		AclBindingFilter filter = new AclBindingFilter(
				new ResourcePatternFilter(ResourceType.GROUP, group, PatternType.MATCH), AccessControlEntryFilter.ANY);
		Settled<Collection<AclBinding>> s = Settled.of(call, call.admin().describeAcls(filter).values());
		if (!s.ok()) {
			if (s.error() instanceof SecurityDisabledException) {
				return Results.map("group", group, "authorizer", false, "acls", List.of(), "note",
						"Cluster'da authorizer kapalı: ACL tanımlanamaz ve uygulanmaz (gruba erişim kısıtlanmıyor)");
			}
			throw new ToolException(s.errorMessage(), s.error());
		}
		List<AclBinding> bindings = new ArrayList<>(s.value());
		bindings.sort(Comparator.comparing((AclBinding b) -> b.entry().principal())
			.thenComparing(b -> b.pattern().patternType().toString())
			.thenComparing(b -> b.pattern().name())
			.thenComparing(b -> b.entry().operation().toString())
			.thenComparing(b -> b.entry().permissionType().toString()));
		List<Map<String, Object>> rows = new ArrayList<>();
		Map<String, Integer> byPatternType = new LinkedHashMap<>();
		TreeSet<String> principals = new TreeSet<>();
		for (AclBinding b : bindings) {
			rows.add(Results.map("principal", b.entry().principal(), "host", b.entry().host(), "operation",
					b.entry().operation().toString(), "permission", b.entry().permissionType().toString(),
					"patternType", b.pattern().patternType().toString(), "resourceName", b.pattern().name()));
			byPatternType.merge(b.pattern().patternType().toString(), 1, Integer::sum);
			principals.add(b.entry().principal());
		}
		return Results.map("group", group, "count", rows.size(), "principals", new ArrayList<>(principals),
				"byPatternType", byPatternType, "acls", rows, "note",
				rows.isEmpty() ? NO_ACL_NOTE : null);
	}

}
