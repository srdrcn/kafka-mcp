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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
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
		categories.put("unverifiedTopicAcls", new Category(
				"Topic'i bu sunucunun Kafka kullanıcısının göremediği (DESCRIBE yetkisi yok) LITERAL ACL'ler; topic var olabilir"));
		categories.put("orphanGroupAcls",
				new Category("Var olmayan group'a LITERAL ACL (group ilk kullanımda oluşur; henüz başlamamış uygulama olabilir)"));
		categories.put("unusedPrefixedTopicAcls", new Category("Hiçbir topic'le eşleşmeyen PREFIXED topic ACL'leri"));

		int scanned = 0;
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
				categories.get("allowAllPrincipals").bindings().add(b);
			}
			if (allow && wildcardName) {
				categories.get("allowWildcardResource").bindings().add(b);
			}
			if (allow && b.entry().operation() == AclOperation.ALL) {
				categories.get("allowOperationAll").bindings().add(b);
			}
			if (allow && type == ResourceType.CLUSTER && !superUsers.contains(b.entry().principal())) {
				categories.get("allowOnCluster").bindings().add(b);
			}
			if (type == ResourceType.TOPIC && literal && !wildcardName && topics != null && !topics.contains(name)) {
				categories.get("orphanTopicAcls").bindings().add(b);
			}
			if (type == ResourceType.GROUP && literal && !wildcardName && groups != null && !groups.contains(name)) {
				categories.get("orphanGroupAcls").bindings().add(b);
			}
			if (type == ResourceType.TOPIC && pattern == PatternType.PREFIXED && topics != null
					&& !anyStartsWith(topics, name)) {
				categories.get("unusedPrefixedTopicAcls").bindings().add(b);
			}
		}

		List<AclBinding> orphanTopics = categories.get("orphanTopicAcls").bindings();
		if (!orphanTopics.isEmpty()) {
			verifyOrphanTopics(call, admin, orphanTopics, categories.get("unverifiedTopicAcls").bindings(), notes);
		}
		if (!categories.get("orphanGroupAcls").bindings().isEmpty()
				|| !categories.get("unusedPrefixedTopicAcls").bindings().isEmpty()) {
			notes.add("Topic/group listesi bu sunucunun Kafka kullanıcısının DESCRIBE yetkisiyle sınırlı: görülemeyen "
					+ "group/topic'ler sahipsiz ya da eşleşmeyen görünebilir; silmeden önce doğrulayın");
		}

		Set<AclBinding> flagged = new HashSet<>();
		categories.values().forEach(c -> flagged.addAll(c.bindings()));
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

	/**
	 * listTopics sadece DESCRIBE yetkisi olan topic'leri döner. Sahipsiz adaylar tek describeTopics ile
	 * doğrulanır: yok/geçersiz → sahipsiz kalır; var → çıkarılır; yetki ya da başka hata → doğrulanamadı.
	 */
	private static void verifyOrphanTopics(ToolCall call, Admin admin, List<AclBinding> orphans,
			List<AclBinding> unverified, List<String> notes) {
		if (call.nearDeadline(2_000)) {
			notes.add("Süre azaldı: sahipsiz topic adayları describeTopics ile doğrulanmadı");
			return;
		}
		Set<String> names = new TreeSet<>();
		orphans.forEach(b -> names.add(b.pattern().name()));
		Map<String, KafkaFuture<TopicDescription>> futures = admin.describeTopics(names).topicNameValues();
		Set<String> existing = new HashSet<>();
		Set<String> unknown = new HashSet<>();
		String otherError = null;
		for (String name : names) {
			KafkaFuture<TopicDescription> f = futures.get(name);
			if (f == null) {
				unknown.add(name);
				continue;
			}
			try {
				call.await(f);
				existing.add(name);
			}
			catch (ToolException e) {
				if (Acls.causedBy(e, UnknownTopicOrPartitionException.class)
						|| Acls.causedBy(e, InvalidTopicException.class)) {
					continue;
				}
				unknown.add(name);
				if (otherError == null && !Acls.causedBy(e, TopicAuthorizationException.class)) {
					otherError = e.getMessage();
				}
			}
		}
		orphans.stream().filter(b -> unknown.contains(b.pattern().name())).forEach(unverified::add);
		orphans.removeIf(b -> existing.contains(b.pattern().name()) || unknown.contains(b.pattern().name()));
		if (otherError != null) {
			notes.add("Bazı sahipsiz topic adayları doğrulanamadı: " + otherError);
		}
	}

	/** Sıralı kümede önekle başlayan eleman var mı (ceiling ile O(log n)). */
	static boolean anyStartsWith(NavigableSet<String> sorted, String prefix) {
		String c = sorted.ceiling(prefix);
		return c != null && c.startsWith(prefix);
	}

}
