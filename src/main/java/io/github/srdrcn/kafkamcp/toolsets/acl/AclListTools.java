package io.github.srdrcn.kafkamcp.toolsets.acl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** acls_list ve acls_audit. */
@Component
public class AclListTools {

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("acls_list", "ACL'leri listeler; principal, kaynak tipi/adı, desen, işlem, izin ve host'a göre filtreler")
			.param("principal", P.string("Principal (ör. User:alice; sadece ad verilirse User: eklenir)"))
			.param("resourceType", Acls.resourceTypeParam("Kaynak tipi (varsayılan ANY)", true))
			.param("resourceName", P.string("Kaynak adı (patternType'a göre eşleşir)"))
			.param("patternType", P.enumOf(
					"Desen tipi (varsayılan ANY; MATCH: bu kaynağa uygulanan literal, prefixed ve * binding'leri)",
					Acls.PATTERN_TYPES))
			.param("operation", Acls.operationParam("İşlem (varsayılan ANY)", true))
			.param("permission", P.enumOf("İzin (varsayılan ANY)", Acls.PERMISSIONS))
			.param("host", P.string("Host (varsayılan tümü)"))
			.listParams()
			.param("filter", P.string("Principal ya da kaynak adı filtresi (regex, kısmi eşleşme)"))
			.handler(this::list),
				ToolSpec
					.read("acls_audit",
							"Riskli ve sahipsiz ACL'leri bulur: User:*, wildcard kaynak, ALL, CLUSTER ALLOW, olmayan topic/group, boşa prefix")
					.listParams(20)
					.param("filter", P.string("Kapsam: principal ya da kaynak adı filtresi (regex, kısmi eşleşme)"))
					.param("limit", limitParam("Kategori başına en fazla örnek (varsayılan 20)", 20))
					.handler(this::audit));
	}

	/** Açıklaması özelleştirilmiş limit parametresi (listParams ile aynı sınırlar). */
	static Map<String, Object> limitParam(String description, int defaultValue) {
		Map<String, Object> m = P.integer(description, 1, ToolSpec.MAX_LIST_LIMIT);
		m.put("default", defaultValue);
		return m;
	}

	Object list(ToolCall call) {
		ResourceType type = Acls.resourceType(call, "resourceType", ResourceType.ANY);
		AclBindingFilter filter = new AclBindingFilter(
				new ResourcePatternFilter(type, call.str("resourceName", null),
						Acls.patternType(call, "patternType", PatternType.ANY)),
				new AccessControlEntryFilter(Acls.normalizePrincipal(call.str("principal", null)),
						call.str("host", null), Acls.operation(call, "operation", AclOperation.ANY),
						Acls.permission(call, "permission", AclPermissionType.ANY)));
		List<AclBinding> acls = Acls.describe(call, filter);
		if (acls == null) {
			return Acls.noAuthorizer();
		}
		List<Map<String, Object>> rows = acls.stream()
			.filter(b -> call.matches(b.entry().principal()) || call.matches(b.pattern().name()))
			.sorted(Acls.ORDER)
			.map(Acls::render)
			.toList();
		return Results.page("acls", rows, call.limit());
	}

	/** Bir denetim kategorisi: açıklama ve eşleşen binding'ler. */
	private record Category(String description, List<AclBinding> bindings) {

		Category(String description) {
			this(description, new ArrayList<>());
		}

	}

	Object audit(ToolCall call) {
		Admin admin = call.admin();
		KafkaFuture<Collection<AclBinding>> aclsFuture = admin.describeAcls(AclBindingFilter.ANY).values();
		KafkaFuture<Set<String>> topicsFuture = admin.listTopics(new ListTopicsOptions().listInternal(true)).names();
		KafkaFuture<Collection<GroupListing>> groupsFuture = admin.listGroups().all();
		BrokerSecurity broker = BrokerSecurity.tryLoad(call);
		List<AclBinding> acls = Acls.await(call, aclsFuture);
		if (acls == null) {
			return Acls.noAuthorizer();
		}
		List<String> notes = new ArrayList<>();
		NavigableSet<String> topics = null;
		try {
			topics = new TreeSet<>(call.await(topicsFuture));
		}
		catch (ToolException e) {
			notes.add("Topic listesi alınamadı, topic kontrolleri atlandı: " + e.getMessage());
		}
		Set<String> groups = null;
		try {
			groups = new TreeSet<>();
			for (GroupListing g : call.await(groupsFuture)) {
				groups.add(g.groupId());
			}
		}
		catch (ToolException e) {
			groups = null;
			notes.add("Group listesi alınamadı, group kontrolü atlandı: " + e.getMessage());
		}
		BrokerSecurity.SuperUsers superUsers = broker.superUsers(null);
		if (!superUsers.known()) {
			notes.add("super.users bilinmiyor: allowOnCluster kategorisinde süper kullanıcılar da listelenebilir");
		}

		Map<String, Category> categories = new LinkedHashMap<>();
		categories.put("allowAllPrincipals", new Category("Herkese (User:*) ALLOW veren ACL'ler"));
		categories.put("allowWildcardResource", new Category("Tüm kaynaklara (LITERAL \"*\") ALLOW veren ACL'ler"));
		categories.put("allowOperationAll", new Category("İşlemi ALL olan ALLOW ACL'ler (en geniş yetki)"));
		categories.put("allowOnCluster", new Category("CLUSTER kaynağında ALLOW (süper kullanıcı olmayan principal'lar)"));
		categories.put("orphanTopicAcls", new Category("Var olmayan topic'e LITERAL ACL (artık kullanılmıyor olabilir)"));
		categories.put("orphanGroupAcls",
				new Category("Var olmayan group'a LITERAL ACL (group ilk kullanımda oluşur; henüz başlamamış uygulama olabilir)"));
		categories.put("unusedPrefixedTopicAcls", new Category("Hiçbir topic'le eşleşmeyen PREFIXED topic ACL'leri"));

		int scanned = 0;
		Set<AclBinding> flagged = new HashSet<>();
		for (AclBinding b : acls) {
			if (!call.matches(b.entry().principal()) && !call.matches(b.pattern().name())) {
				continue;
			}
			scanned++;
			boolean allow = b.entry().permissionType() == AclPermissionType.ALLOW;
			ResourceType type = b.pattern().resourceType();
			PatternType pattern = b.pattern().patternType();
			String name = b.pattern().name();
			boolean literal = pattern == PatternType.LITERAL;
			boolean wildcardName = literal && Acls.WILDCARD.equals(name);
			if (allow && Acls.WILDCARD_PRINCIPAL.equals(b.entry().principal())) {
				add(categories, "allowAllPrincipals", b, flagged);
			}
			if (allow && wildcardName) {
				add(categories, "allowWildcardResource", b, flagged);
			}
			if (allow && b.entry().operation() == AclOperation.ALL) {
				add(categories, "allowOperationAll", b, flagged);
			}
			if (allow && type == ResourceType.CLUSTER && !superUsers.contains(b.entry().principal())) {
				add(categories, "allowOnCluster", b, flagged);
			}
			if (type == ResourceType.TOPIC && literal && !wildcardName && topics != null && !topics.contains(name)) {
				add(categories, "orphanTopicAcls", b, flagged);
			}
			if (type == ResourceType.GROUP && literal && !wildcardName && groups != null && !groups.contains(name)) {
				add(categories, "orphanGroupAcls", b, flagged);
			}
			if (type == ResourceType.TOPIC && pattern == PatternType.PREFIXED && topics != null
					&& !anyStartsWith(topics, name)) {
				add(categories, "unusedPrefixedTopicAcls", b, flagged);
			}
		}

		int limit = call.limit(20);
		Map<String, Object> out = new LinkedHashMap<>();
		for (Map.Entry<String, Category> e : categories.entrySet()) {
			List<AclBinding> list = e.getValue().bindings();
			List<Map<String, Object>> samples = Acls.render(list);
			out.put(e.getKey(), Results.map("description", e.getValue().description(), "count", list.size(),
					"truncated", list.size() > limit ? true : null, "samples",
					list.isEmpty() ? null : samples.subList(0, Math.min(limit, samples.size()))));
		}
		return Results.map("totalAcls", acls.size(), "scannedAcls", scanned, "flaggedAcls", flagged.size(),
				"topicCount", topics == null ? null : topics.size(), "groupCount", groups == null ? null : groups.size(),
				"samplesPerCategory", limit, "categories", out, "notes", notes.isEmpty() ? null : notes);
	}

	private static void add(Map<String, Category> categories, String key, AclBinding b, Set<AclBinding> flagged) {
		categories.get(key).bindings().add(b);
		flagged.add(b);
	}

	/** Sıralı kümede önekle başlayan eleman var mı (ceiling ile O(log n)). */
	static boolean anyStartsWith(NavigableSet<String> sorted, String prefix) {
		String c = sorted.ceiling(prefix);
		return c != null && c.startsWith(prefix);
	}

}
