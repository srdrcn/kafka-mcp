package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;

import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.toolsets.acl.Acls;

/**
 * Temizlik adayı kuralları (saf hesap, test edilebilir): topic (boş/boşta/consumer'sız), Empty consumer group
 * (silinmiş topic'lerde offset, offset'siz ya da lag'siz ve topic'leri boşta) ve sahipsiz LITERAL ACL'ler.
 * Her aday nedenler ve risk ipucuyla (low/medium/high) döner; hiçbir şey silinmez.
 */
public final class CleanupRules {

	public static final String LOW = "low";

	public static final String MEDIUM = "medium";

	public static final String HIGH = "high";

	private static final int MAX_NAMES = 10;

	private CleanupRules() {
	}

	/** Temizlik adayı: tip (topic/group/acl), ad, nedenler, risk ve kısa ayrıntılar. */
	public record Candidate(String type, String name, List<String> reasons, String risk, Map<String, Object> details) {

		public Map<String, Object> toMap() {
			Map<String, Object> m = Results.map("type", type, "name", name, "reasons", reasons, "risk", risk);
			m.putAll(details);
			return m;
		}

	}

	/**
	 * Topic adayı: boş (hiç yazılmamış), boşta (son yazma eşikten eski) ya da consumer'sız. Risk: veri yok ve
	 * consumer yoksa low; veri yok ama consumer varsa medium; sadece consumer'sız ve yakın zamanda yazılmışsa
	 * high (aktif producer var). Aday değilse null.
	 */
	public static Candidate topic(String name, TopicActivity a, long thresholdMs, int consumerGroups,
			Map<String, Object> details) {
		boolean empty = Boolean.TRUE.equals(a.empty());
		Boolean idle = a.idleSince(thresholdMs);
		boolean noConsumers = consumerGroups == 0;
		List<String> reasons = new ArrayList<>();
		if (empty) {
			reasons.add(a.neverWritten() ? "never_written" : "empty");
		}
		if (Boolean.TRUE.equals(idle)) {
			reasons.add("idle");
		}
		if (noConsumers) {
			reasons.add("no_consumers");
		}
		if (reasons.isEmpty()) {
			return null;
		}
		boolean inactive = empty || Boolean.TRUE.equals(idle);
		String risk;
		if (inactive) {
			risk = noConsumers ? LOW : MEDIUM;
		}
		else {
			risk = Boolean.FALSE.equals(idle) ? HIGH : MEDIUM;
		}
		Map<String, Object> d = new LinkedHashMap<>(details);
		d.put("consumerGroups", consumerGroups);
		return new Candidate("topic", name, reasons, risk, d);
	}

	/**
	 * Empty durumdaki consumer group adayı. Nedenler: offset'i yok; silinmiş topic'lerde offset'i var (hepsi
	 * silinmişse low, bir kısmıysa medium); ya da lag'i 0 ve commit'li topic'lerinin hepsi boşta (medium).
	 * Aday değilse null.
	 */
	public static Candidate group(String groupId, String state, Map<TopicPartition, Long> committed,
			Set<String> existingTopics, Map<TopicPartition, Offsets.Range> ranges, Predicate<String> topicInactive) {
		if (!"Empty".equals(state)) {
			return null;
		}
		Set<String> topics = new TreeSet<>();
		committed.keySet().forEach(tp -> topics.add(tp.topic()));
		if (topics.isEmpty()) {
			return new Candidate("group", groupId, List.of("empty_group", "no_offsets"), LOW, Map.of());
		}
		List<String> deleted = topics.stream().filter(t -> !existingTopics.contains(t)).toList();
		if (!deleted.isEmpty()) {
			return new Candidate("group", groupId, List.of("empty_group", "offsets_on_deleted_topics"),
					deleted.size() == topics.size() ? LOW : MEDIUM,
					Results.map("committedTopics", topics.size(), "deletedTopics", head(deleted), "deletedTopicCount",
							deleted.size()));
		}
		long lag = 0;
		for (Map.Entry<TopicPartition, Long> e : committed.entrySet()) {
			Offsets.Range r = ranges.get(e.getKey());
			if (r == null) {
				return null;
			}
			lag += Math.max(0, r.latest() - e.getValue());
		}
		if (lag == 0 && topics.stream().allMatch(topicInactive)) {
			return new Candidate("group", groupId, List.of("empty_group", "no_lag", "topics_idle"), MEDIUM,
					Results.map("committedTopics", topics.size(), "topics", head(new ArrayList<>(topics))));
		}
		return null;
	}

	/**
	 * Sahipsiz LITERAL ACL'ler: var olmayan topic'e ya da (grup listesi tamsa) var olmayan gruba verilmiş
	 * binding'ler, kaynak başına tek aday. Topic için risk low, grup için medium (grup sadece offset/üyesi varken
	 * listelenir; duran bir uygulamanın ACL'i olabilir).
	 */
	public static List<Candidate> orphanAcls(Collection<AclBinding> bindings, Set<String> topics, Set<String> groups,
			boolean groupsKnown) {
		Map<String, List<AclBinding>> byResource = new TreeMap<>();
		for (AclBinding b : bindings) {
			if (b.pattern().patternType() != PatternType.LITERAL || Acls.WILDCARD.equals(b.pattern().name())) {
				continue;
			}
			ResourceType type = b.pattern().resourceType();
			String name = b.pattern().name();
			boolean orphan = type == ResourceType.TOPIC && !topics.contains(name)
					|| type == ResourceType.GROUP && groupsKnown && !groups.contains(name);
			if (orphan) {
				byResource.computeIfAbsent(type + "\u0000" + name, k -> new ArrayList<>()).add(b);
			}
		}
		List<Candidate> out = new ArrayList<>();
		byResource.forEach((k, list) -> {
			AclBinding first = list.get(0);
			boolean topic = first.pattern().resourceType() == ResourceType.TOPIC;
			Set<String> principals = new TreeSet<>();
			Set<String> ops = new TreeSet<>();
			list.forEach(b -> {
				principals.add(b.entry().principal());
				ops.add(b.entry().permissionType() + ":" + b.entry().operation());
			});
			out.add(new Candidate("acl", first.pattern().name(),
					List.of(topic ? "orphan_topic_acl" : "orphan_group_acl"), topic ? LOW : MEDIUM,
					Results.map("resourceType", first.pattern().resourceType().name(), "patternType", "LITERAL",
							"bindings", list.size(), "principals", head(new ArrayList<>(principals)), "operations",
							new ArrayList<>(ops))));
		});
		return out;
	}

	private static List<String> head(List<String> names) {
		return names.subList(0, Math.min(MAX_NAMES, names.size()));
	}

}
