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
 * Full view of a single group: describe + committed offsets + end offsets (3 admin requests). Partitions assigned
 * to members but without a commit are included as {@code committed == null} rows.
 */
public record GroupDetail(String groupId, ConsumerGroupDescription description, GroupLagInfo lag,
		Map<TopicPartition, MemberDescription> owners) {

	/** Share of the deadline kept in the describe step for the committed + end offset queries. */
	static final long DESCRIBE_RESERVE_MS = 5_000;

	/**
	 * Loads the group; {@link ToolException} if the group does not exist. If topicFilter is not null, only matching
	 * topics. Partitions whose end offset cannot be fetched in time (e.g. no leader) are returned with an error in
	 * their row instead of failing the tool.
	 */
	public static GroupDetail load(ToolCall call, String group, Predicate<String> topicFilter) {
		ConsumerGroupDescription d = ConsumerGroups.describeExisting(call, group, DESCRIBE_RESERVE_MS);
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
		// KIP-848 reconciling: partitions being moved to a target member (whose previous owner is not visible in
		// describe) also count as assigned, so their rows are not lost when they have no commit.
		Set<TopicPartition> targeted = new LinkedHashSet<>(owners.keySet());
		targeted.addAll(ConsumerGroups.incomingOwners(d).keySet());
		for (TopicPartition tp : targeted) {
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
