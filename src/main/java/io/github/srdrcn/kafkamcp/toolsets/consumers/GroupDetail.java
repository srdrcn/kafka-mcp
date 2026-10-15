package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.Committed;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.GroupLagInfo;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.OffsetLookup;

/**
 * Tek grubun tam görünümü: describe + committed offset'ler + end offset'ler (3 admin isteği). Üyelere
 * atanmış ama commit'i olmayan partition'lar da {@code committed == null} satırı olarak dahildir.
 */
public record GroupDetail(String groupId, ConsumerGroupDescription description, GroupLagInfo lag,
		Map<TopicPartition, MemberDescription> owners) {

	/** Grubu yükler; grup yoksa Türkçe {@link ToolException}. topicFilter null değilse sadece uyan topic'ler. */
	public static GroupDetail load(ToolCall call, String group, Predicate<String> topicFilter) {
		ConsumerGroupDescription d = ConsumerGroups.describeExisting(call, group);
		Committed c = GroupLag.committed(call, List.of(group), topicFilter).get(group);
		if (c.error() != null) {
			if (c.notFound()) {
				throw ConsumerGroups.notFoundError(group);
			}
			throw new ToolException(c.error());
		}
		if (ConsumerGroups.deadOrUnknown(d) && c.isEmpty()) {
			throw ConsumerGroups.notFoundError(group);
		}
		Map<TopicPartition, MemberDescription> owners = ConsumerGroups.owners(d);
		Set<TopicPartition> assigned = new LinkedHashSet<>();
		for (TopicPartition tp : owners.keySet()) {
			if (topicFilter == null || topicFilter.test(tp.topic())) {
				assigned.add(tp);
			}
		}
		Set<TopicPartition> all = new LinkedHashSet<>(c.offsets().keySet());
		all.addAll(c.withoutOffset());
		all.addAll(assigned);
		OffsetLookup end = GroupLag.endOffsets(call, all);
		GroupLagInfo info = GroupLag.compute(Map.of(group, c), end, Map.of(group, assigned)).get(group);
		return new GroupDetail(group, d, info, owners);
	}

	public String state() {
		return ConsumerGroups.state(description.groupState());
	}

}
