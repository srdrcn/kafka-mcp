package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.toolsets.acl.Acls;

/**
 * Bir topic önekine (takım ad alanına) TOPIC ACL'i olan principal'lar: LITERAL (önekli topic adı), PREFIXED
 * (daha geniş ya da daha dar önek) ve LITERAL "*" binding'leri; principal + izin başına işlemler ve kapsanan
 * topic sayısı.
 */
public final class PrefixAccess {

	private static final int MAX_PATTERNS = 10;

	private PrefixAccess() {
	}

	/** Binding önek ad alanıyla ilgili mi; ilgiliyse kapsadığı topic'ler (yoksa null). */
	static Set<String> covered(ResourcePattern p, String prefix, Collection<String> topics) {
		if (p.resourceType() != ResourceType.TOPIC) {
			return null;
		}
		Set<String> out = new TreeSet<>();
		if (p.patternType() == PatternType.LITERAL) {
			if (Acls.WILDCARD.equals(p.name())) {
				out.addAll(topics);
				return out;
			}
			if (topics.contains(p.name())) {
				out.add(p.name());
				return out;
			}
			return p.name().startsWith(prefix) ? out : null;
		}
		if (p.patternType() == PatternType.PREFIXED) {
			if (prefix.startsWith(p.name())) {
				out.addAll(topics);
				return out;
			}
			if (p.name().startsWith(prefix)) {
				topics.stream().filter(t -> t.startsWith(p.name())).forEach(out::add);
				return out;
			}
		}
		return null;
	}

	/** Principal + izin başına satırlar: işlemler, kapsanan topic sayısı, desenler (sıralı). */
	public static List<Map<String, Object>> principals(Collection<AclBinding> bindings, String prefix,
			Collection<String> topics) {
		record Key(String principal, String permission) {
		}
		Map<Key, Set<String>> ops = new TreeMap<>(java.util.Comparator.comparing(Key::principal)
			.thenComparing(Key::permission));
		Map<Key, Set<String>> covered = new java.util.HashMap<>();
		Map<Key, Set<String>> patterns = new java.util.HashMap<>();
		for (AclBinding b : bindings) {
			Set<String> c = covered(b.pattern(), prefix, topics);
			if (c == null) {
				continue;
			}
			Key k = new Key(b.entry().principal(), b.entry().permissionType().name());
			ops.computeIfAbsent(k, x -> new TreeSet<>()).add(b.entry().operation().name());
			covered.computeIfAbsent(k, x -> new TreeSet<>()).addAll(c);
			String via = Acls.WILDCARD.equals(b.pattern().name()) && b.pattern().patternType() == PatternType.LITERAL
					? "LITERAL:* (wildcard)" : b.pattern().patternType() + ":" + b.pattern().name();
			patterns.computeIfAbsent(k, x -> new LinkedHashSet<>()).add(via);
		}
		List<Map<String, Object>> out = new ArrayList<>();
		ops.forEach((k, o) -> {
			List<String> via = new ArrayList<>(patterns.get(k));
			int n = covered.get(k).size();
			out.add(Results.map("principal", k.principal(), "permission", k.permission(), "operations",
					new ArrayList<>(o), "topicsCovered", n, "coversAllTopics",
					!topics.isEmpty() && n == topics.size() ? true : null, "patterns",
					via.subList(0, Math.min(MAX_PATTERNS, via.size())), "patternsOmitted",
					via.size() > MAX_PATTERNS ? via.size() - MAX_PATTERNS : null));
		});
		return out;
	}

}
