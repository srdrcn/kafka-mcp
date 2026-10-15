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
 * Toplu lag taramasında eksik kalanların sayaçları: yanıt vermeyen / hata veren gruplar ve end offset'i
 * alınamayan (lag'i bilinmeyen) partition'lar. Hiçbir şey sessizce düşmesin diye çıktıya eklenir; herhangi
 * biri varsa sonuç {@code "partial": true} işaretlenir.
 */
public final class ScanIssues {

	/** Çıktıda örnek olarak gösterilen en fazla hata (grup/topic). */
	public static final int MAX_ERROR_SAMPLES = 5;

	private int noResponse;

	private final List<String> errors = new ArrayList<>();

	private final Set<TopicPartition> withoutEnd = new HashSet<>();

	private final Map<String, String> endErrors = new TreeMap<>();

	private int groupsWithUnknownLag;

	/** Grubun offset'leri alınamadı: süre içinde yanıt gelmedi ya da hata. */
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

	/** Bir grubun end offset'i alınamayan commit'li partition'ları (lag'i bilinmiyor). */
	public void unknownLag(Collection<TopicPartition> missing) {
		if (!missing.isEmpty()) {
			groupsWithUnknownLag++;
			withoutEnd.addAll(missing);
		}
	}

	public void unknownLag(GroupLagInfo info) {
		unknownLag(info.withoutEndPartitions());
	}

	/** End offset sorgusunun hatalarını (topic bazında örnek) toplar. */
	public void endErrors(OffsetLookup lookup) {
		lookup.errorsByTopic().forEach(endErrors::putIfAbsent);
	}

	/** Sonuç eksik mi (yanıtsız/hatalı grup, lag'i bilinmeyen partition ya da eksik grup listesi)? */
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
