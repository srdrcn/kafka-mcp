package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.toolsets.consumers.ConsumerGroups.GroupList;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.GroupLagInfo;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.OffsetLookup;

/**
 * Counters of what a bulk lag scan could not cover: groups that did not respond or failed, and partitions whose
 * end offset could not be fetched (lag unknown). Added to the output so that nothing is dropped silently; if any
 * of them is present, the result is marked {@code "partial": true}.
 */
public final class ScanIssues {

	/** Maximum number of errors (groups/topics) shown as samples in the output. */
	public static final int MAX_ERROR_SAMPLES = 5;

	private int noResponse;

	private final List<String> errors = new ArrayList<>();

	private final Set<TopicPartition> withoutEnd = new HashSet<>();

	private final Map<String, String> endErrors = new TreeMap<>();

	private int groupsWithUnknownLag;

	/** The group's offsets could not be fetched: no response in time, or an error. */
	public void groupError(String groupId, String error, boolean timedOut) {
		if (timedOut) {
			noResponse++;
		}
		else {
			errors.add(groupId + ": " + error);
		}
	}

	public void groupError(GroupLagInfo info) {
		groupError(info.groupId(), info.error(), info.timedOut());
	}

	/** Committed partitions of a group whose end offset could not be fetched (lag unknown). */
	public void unknownLag(Collection<TopicPartition> missing) {
		if (!missing.isEmpty()) {
			groupsWithUnknownLag++;
			withoutEnd.addAll(missing);
		}
	}

	public void unknownLag(GroupLagInfo info) {
		unknownLag(info.withoutEndPartitions());
	}

	/** Collects the errors of the end offset query (one sample per topic). */
	public void endErrors(OffsetLookup lookup) {
		lookup.errorsByTopic().forEach(endErrors::putIfAbsent);
	}

	/** Is the result incomplete (group without response or with error, unknown lag, or partial group list)? */
	public boolean incomplete(GroupList list) {
		return noResponse > 0 || !errors.isEmpty() || !withoutEnd.isEmpty() || list.partial();
	}

	/** partial, groupsWithoutResponse, groupErrors/errors, groupsWithUnknownLag, partitionsWithoutEndOffset, ... */
	public Map<String, Object> toMap(GroupList list) {
		Map<String, String> endErrorSample = new LinkedHashMap<>();
		endErrors.entrySet()
			.stream()
			.limit(MAX_ERROR_SAMPLES)
			.forEach(e -> endErrorSample.put(e.getKey(), e.getValue()));
		return Results.map("partial", incomplete(list) ? Boolean.TRUE : null, "groupsWithoutResponse",
				noResponse > 0 ? noResponse : null, "groupErrors", errors.isEmpty() ? null : errors.size(), "errors",
				errors.isEmpty() ? null : errors.subList(0, Math.min(MAX_ERROR_SAMPLES, errors.size())),
				"groupsWithUnknownLag", groupsWithUnknownLag > 0 ? groupsWithUnknownLag : null,
				"partitionsWithoutEndOffset", withoutEnd.isEmpty() ? null : withoutEnd.size(), "endOffsetErrors",
				withoutEnd.isEmpty() || endErrorSample.isEmpty() ? null : endErrorSample, "listErrors",
				list.errors().isEmpty() ? null : list.errors());
	}

}
