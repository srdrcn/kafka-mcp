package io.github.srdrcn.kafkamcp.toolsets.acl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** acls_explain, acls_who_can_access_topic, acls_principal_permissions. */
@Component
public class AclAccessTools {

	private static final String SUPER_USERS_DESC = "Bilinen süper kullanıcılar (ör. User:admin); broker super.users'ı "
			+ "gizli döndürdüğünde kullanılır";

	private static final String ALLOW_EVERYONE_PARAM = "allowEveryoneIfNoAclFound";

	private static final String ALLOW_EVERYONE_DESC = "Broker'ın allow.everyone.if.no.acl.found değeri; broker "
			+ "gizli döndürdüğünde kullanılır (verilmezse false varsayılır)";

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("acls_explain",
					"Principal'ın kaynakta bir işlemi yapıp yapamayacağını Kafka authorizer kurallarıyla (KRaft StandardAuthorizer, ZooKeeper AclAuthorizer) açıklar (ALLOW/DENY ve nedeni)")
			.required("principal", P.string("Principal (ör. User:alice; sadece ad verilirse User: eklenir)"))
			.required("operation", Acls.operationParam("İşlem", false))
			.required("resourceType", Acls.resourceTypeParam("Kaynak tipi", false))
			.required("resourceName", P.string("Kaynak adı (CLUSTER için kafka-cluster)"))
			.param("host", P.string("İstemci IP'si (varsayılan *: sadece host'u * olan ACL'ler eşleşir)"))
			.param("superUsers", P.stringArray(SUPER_USERS_DESC))
			.param(ALLOW_EVERYONE_PARAM, P.bool(ALLOW_EVERYONE_DESC))
			.handler(this::explain),
				ToolSpec
					.read("acls_who_can_access_topic",
							"Topic'e kimlerin erişebildiğini (ALLOW), kimlerin açıkça reddedildiğini (DENY) ve süper kullanıcıları gösterir")
					.required("topic", P.string("Topic adı"))
					.param("operation", Acls.operationParam("İşlem (varsayılan READ ve WRITE)", false))
					.param("superUsers", P.stringArray(SUPER_USERS_DESC))
					.param(ALLOW_EVERYONE_PARAM, P.bool(ALLOW_EVERYONE_DESC))
					.handler(this::whoCanAccessTopic),
				ToolSpec
					.read("acls_principal_permissions",
							"Principal'a uygulanan ACL'leri kaynağa göre gruplar; etkin ALLOW/DENY işlemlerini özetler")
					.required("principal", P.string("Principal (ör. User:alice; sadece ad verilirse User: eklenir)"))
					.param("includeWildcardPrincipal", P.bool("User:* binding'lerini de dahil et (varsayılan true)"))
					.listParams()
					.param("filter", P.string("Kaynak adı filtresi (regex, kısmi eşleşme)"))
					.param("limit", AclListTools.limitParam("En fazla kaç kaynak dönsün (varsayılan 100)", 100))
					.handler(this::principalPermissions));
	}

	Object explain(ToolCall call) {
		String principal = Acls.principal(call, "principal");
		AclOperation operation = Acls.parse("operation", call.str("operation"), AclOperation.class,
				Acls.withoutAny(Acls.OPERATIONS));
		ResourceType type = Acls.parse("resourceType", call.str("resourceType"), ResourceType.class,
				Acls.withoutAny(Acls.RESOURCE_TYPES));
		String name = call.str("resourceName");
		String host = call.str("host", Acls.WILDCARD);
		KafkaFuture<Collection<AclBinding>> future = call.admin()
			.describeAcls(new AclBindingFilter(new ResourcePatternFilter(type, name, PatternType.MATCH),
					AccessControlEntryFilter.ANY))
			.values();
		BrokerSecurity broker = BrokerSecurity.tryLoad(call);
		List<AclBinding> bindings = Acls.await(call, future);
		if (bindings == null) {
			return Results.map("decision", "ALLOW", "reason", Acls.NO_AUTHORIZER_NOTE, "authorizerEnabled", false);
		}
		BrokerSecurity.SuperUsers superUsers = broker.superUsers(call.strings("superUsers"));
		BrokerSecurity.AllowEveryone allowEveryone = broker.allowEveryone(allowEveryoneParam(call));
		AclEvaluator.Decision d = new AclEvaluator(bindings, superUsers.users(), allowEveryone.value())
			.authorize(principal, operation, type, name, host);
		String reason = d.reason();
		if (allowEveryone.assumed() && d.noAclOnResource() && !d.superUser()) {
			reason = "Kaynakta hiç ACL yok; allow.everyone.if.no.acl.found bilinmiyor, false varsayıldı → DENY "
					+ "(broker'da true ise ALLOW)";
		}

		List<String> notes = new ArrayList<>();
		if (superUsers.note() != null && !d.superUser()) {
			notes.add(superUsers.note());
		}
		if (allowEveryone.note() != null && d.noAclOnResource()) {
			notes.add(allowEveryone.note());
		}
		if (Acls.WILDCARD.equals(host) && bindings.stream()
			.anyMatch(b -> !Acls.WILDCARD.equals(b.entry().host())
					&& AclEvaluator.principalMatches(b.entry().principal(), principal))) {
			notes.add("Bu principal için host'a özel ACL'ler var; host=* ile sadece host'u * olanlar değerlendirildi. "
					+ "İstemci IP'sini host parametresiyle verin.");
		}
		if (type == ResourceType.CLUSTER && !Acls.CLUSTER_NAME.equals(name)) {
			notes.add("CLUSTER kaynağının adı her zaman " + Acls.CLUSTER_NAME + " olmalı");
		}
		if (operation == AclOperation.IDEMPOTENT_WRITE && type == ResourceType.CLUSTER && !d.allowed()
				&& !Boolean.TRUE.equals(broker.idempotentWriteRequired())) {
			notes.add("Kafka 2.8+ broker'larda (KRaft dahil) idempotent producer IDEMPOTENT_WRITE olmadan da çalışır: "
					+ "herhangi bir topic'te WRITE yetkisi yeterli (KIP-679); bu karar sadece IDEMPOTENT_WRITE ACL'ini "
					+ "değerlendirir");
		}
		if (broker.customAuthorizerNote() != null) {
			notes.add(broker.customAuthorizerNote());
		}
		return Results.map("decision", d.decision(), "reason", reason, "principal", principal, "operation",
				operation.name(), "resourceType", type.name(), "resourceName", name, "host", host, "authorizer",
				broker.authorizer(), "superUser", d.superUser(), "allowEveryoneIfNoAclFound", allowEveryone.value(),
				"resourceAclCount", d.resourceAclCount(), "matchedAllow", Acls.render(d.matchedAllow()),
				"matchedDeny", Acls.render(d.matchedDeny()), "notes", notes.isEmpty() ? null : notes);
	}

	Object whoCanAccessTopic(ToolCall call) {
		String topic = call.str("topic");
		List<AclOperation> operations = call.has("operation")
				? List.of(Acls.parse("operation", call.str("operation"), AclOperation.class,
						Acls.withoutAny(Acls.OPERATIONS)))
				: List.of(AclOperation.READ, AclOperation.WRITE);
		Admin admin = call.admin();
		KafkaFuture<Collection<AclBinding>> future = admin
			.describeAcls(new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, topic, PatternType.MATCH),
					AccessControlEntryFilter.ANY))
			.values();
		KafkaFuture<Map<String, TopicDescription>> topicFuture = admin.describeTopics(List.of(topic)).allTopicNames();
		BrokerSecurity broker = BrokerSecurity.tryLoad(call);
		List<AclBinding> bindings = Acls.await(call, future);
		if (bindings == null) {
			return Acls.noAuthorizer();
		}
		Boolean exists;
		try {
			call.await(topicFuture);
			exists = true;
		}
		catch (ToolException e) {
			exists = Acls.causedBy(e, UnknownTopicOrPartitionException.class) ? false : null;
		}
		BrokerSecurity.SuperUsers superUsers = broker.superUsers(call.strings("superUsers"));
		BrokerSecurity.AllowEveryone allowEveryone = broker.allowEveryone(allowEveryoneParam(call));
		// ACL'ler süper kullanıcılar olmadan değerlendirilir; süper kullanıcılar ayrıca işaretlenir.
		AclEvaluator evaluator = new AclEvaluator(bindings, Set.of(), allowEveryone.value());
		Set<String> principals = new TreeSet<>();
		bindings.forEach(b -> principals.add(b.entry().principal()));

		Map<String, Object> byOperation = new LinkedHashMap<>();
		for (AclOperation op : operations) {
			List<Map<String, Object>> allowed = new ArrayList<>();
			List<Map<String, Object>> denied = new ArrayList<>();
			for (String p : principals) {
				Set<String> hosts = new LinkedHashSet<>();
				hosts.add(Acls.WILDCARD);
				for (AclBinding b : bindings) {
					if (AclEvaluator.principalMatches(b.entry().principal(), p)) {
						hosts.add(b.entry().host());
					}
				}
				List<String> allowedHosts = new ArrayList<>();
				List<String> deniedHosts = new ArrayList<>();
				Set<AclBinding> granting = new LinkedHashSet<>();
				Set<AclBinding> denying = new LinkedHashSet<>();
				for (String h : hosts) {
					AclEvaluator.Decision d = evaluator.authorize(p, op, ResourceType.TOPIC, topic, h);
					if (d.allowed()) {
						allowedHosts.add(h);
						granting.addAll(d.matchedAllow());
					}
					else if (!d.matchedDeny().isEmpty()) {
						deniedHosts.add(h);
						denying.addAll(d.matchedDeny());
					}
				}
				boolean superUser = superUsers.contains(p);
				// Süper kullanıcıya DENY uygulanmaz; diğerlerinde host'a özel DENY'ler de gösterilir.
				boolean hasDeny = !superUser && !denying.isEmpty();
				if (!allowedHosts.isEmpty() || superUser) {
					boolean everywhere = !hasDeny
							&& (allowedHosts.isEmpty() || allowedHosts.equals(List.of(Acls.WILDCARD)));
					allowed.add(Results.map("principal", p, "superUser", superUser ? true : null, "hosts",
							everywhere ? null : allowedHosts, "deniedHosts", hasDeny ? deniedHosts : null, "via",
							Acls.render(granting), "deniedVia", hasDeny ? Acls.render(denying) : null));
				}
				if (hasDeny) {
					denied.add(Results.map("principal", p, "hosts", allowedHosts.isEmpty() ? null : deniedHosts,
							"via", Acls.render(denying)));
				}
			}
			byOperation.put(op.name(), Results.map("allowed", allowed, "denied", denied));
		}

		List<String> notes = new ArrayList<>();
		if (bindings.isEmpty()) {
			notes.add(allowEveryone.assumed()
					? "Topic'e uygulanan hiç ACL yok; allow.everyone.if.no.acl.found bilinmiyor (false varsayıldı): "
							+ "false ise süper kullanıcılar dışında kimse erişemez, true ise herkes erişebilir"
					: allowEveryone.value()
							? "Topic'e uygulanan hiç ACL yok ve allow.everyone.if.no.acl.found=true: herkes erişebilir"
							: "Topic'e uygulanan hiç ACL yok: süper kullanıcılar dışında kimse erişemez (allow.everyone.if.no.acl.found=false)");
			if (allowEveryone.note() != null) {
				notes.add(allowEveryone.note());
			}
		}
		if (principals.contains(Acls.WILDCARD_PRINCIPAL)) {
			notes.add("User:* herkesi temsil eder: ALLOW ise açıkça DENY edilmeyen her kullanıcı erişebilir");
		}
		if (superUsers.note() != null) {
			notes.add(superUsers.note());
		}
		if (broker.customAuthorizerNote() != null) {
			notes.add(broker.customAuthorizerNote());
		}
		return Results.map("topic", topic, "topicExists", exists, "aclCount", bindings.size(), "superUsers",
				superUsers.known() ? List.copyOf(superUsers.users()) : null, "allowEveryoneIfNoAclFound",
				allowEveryone.value(), "operations", byOperation, "notes", notes.isEmpty() ? null : notes);
	}

	Object principalPermissions(ToolCall call) {
		String principal = Acls.principal(call, "principal");
		boolean wildcard = call.bool("includeWildcardPrincipal", true) && !Acls.WILDCARD_PRINCIPAL.equals(principal);
		Admin admin = call.admin();
		KafkaFuture<Collection<AclBinding>> own = admin.describeAcls(new AclBindingFilter(ResourcePatternFilter.ANY,
				new AccessControlEntryFilter(principal, null, AclOperation.ANY, AclPermissionType.ANY))).values();
		KafkaFuture<Collection<AclBinding>> everyone = wildcard ? admin
			.describeAcls(new AclBindingFilter(ResourcePatternFilter.ANY, new AccessControlEntryFilter(
					Acls.WILDCARD_PRINCIPAL, null, AclOperation.ANY, AclPermissionType.ANY)))
			.values() : null;
		BrokerSecurity broker = BrokerSecurity.tryLoad(call);
		List<AclBinding> bindings = Acls.await(call, own);
		if (bindings == null) {
			return Acls.noAuthorizer();
		}
		if (everyone != null) {
			bindings.addAll(Acls.await(call, everyone));
		}

		Map<String, List<AclBinding>> groups = new TreeMap<>();
		for (AclBinding b : bindings) {
			if (!call.matches(b.pattern().name())) {
				continue;
			}
			String key = b.pattern().resourceType() + "\u0000" + b.pattern().patternType() + "\u0000" + b.pattern().name();
			groups.computeIfAbsent(key, k -> new ArrayList<>()).add(b);
		}
		List<Map<String, Object>> rows = new ArrayList<>();
		Map<String, Integer> byType = new TreeMap<>();
		int allowCount = 0;
		int denyCount = 0;
		int wildcardCount = 0;
		for (List<AclBinding> list : groups.values()) {
			ResourcePattern pattern = list.get(0).pattern();
			ResourceType type = pattern.resourceType();
			Set<AclOperation> allow = EnumSet.noneOf(AclOperation.class);
			Set<AclOperation> deny = EnumSet.noneOf(AclOperation.class);
			for (AclBinding b : list) {
				if (b.entry().permissionType() == AclPermissionType.ALLOW) {
					allow.add(b.entry().operation());
					allowCount++;
				}
				else if (b.entry().permissionType() == AclPermissionType.DENY) {
					deny.add(b.entry().operation());
					denyCount++;
				}
				if (Acls.WILDCARD_PRINCIPAL.equals(b.entry().principal())) {
					wildcardCount++;
				}
			}
			// Bu deseni kapsayan diğer desenlerin (LITERAL "*", PREFIXED önek) ALLOW/DENY'leri de etkilidir.
			Set<AclOperation> effectiveAllow = EnumSet.noneOf(AclOperation.class);
			effectiveAllow.addAll(allow);
			Set<AclOperation> effectiveDeny = EnumSet.noneOf(AclOperation.class);
			effectiveDeny.addAll(deny);
			Set<String> alsoAllowedBy = new TreeSet<>();
			List<AclBinding> deniedBy = new ArrayList<>();
			for (AclBinding b : bindings) {
				if (b.pattern().equals(pattern) || !AclEvaluator.covers(b.pattern(), pattern)) {
					continue;
				}
				if (b.entry().permissionType() == AclPermissionType.DENY) {
					effectiveDeny.add(b.entry().operation());
					deniedBy.add(b);
				}
				else if (b.entry().permissionType() == AclPermissionType.ALLOW) {
					effectiveAllow.add(b.entry().operation());
					alsoAllowedBy.add(b.pattern().patternType() + ":" + b.pattern().name());
				}
			}
			byType.merge(type.name(), 1, Integer::sum);
			rows.add(Results.map("resourceType", type.name(), "patternType", pattern.patternType().name(),
					"resourceName", pattern.name(), "allow", names(allow), "deny", names(deny), "effectiveAllow",
					AclEvaluator.effectiveAllow(type, effectiveAllow, effectiveDeny), "alsoAllowedBy",
					alsoAllowedBy.isEmpty() ? null : List.copyOf(alsoAllowedBy), "deniedBy",
					deniedBy.isEmpty() ? null : Acls.render(deniedBy), "bindings",
					list.stream().sorted(Acls.ORDER).map(Acls::renderEntry).toList()));
		}

		BrokerSecurity.SuperUsers superUsers = broker.superUsers(null);
		boolean superUser = superUsers.contains(principal);
		List<String> notes = new ArrayList<>();
		if (superUser) {
			notes.add("Principal süper kullanıcı: ACL'lerden bağımsız olarak tüm işlemlere yetkili");
		}
		else if (!superUsers.known()) {
			notes.add("super.users bilinmiyor (broker gizli döndürdü); principal süper kullanıcıysa tüm işlemlere yetkilidir");
		}
		if (bindings.isEmpty()) {
			notes.add("Bu principal'a uygulanan ACL yok");
		}
		Map<String, Object> out = Results.map("principal", principal, "superUser", superUser,
				"includeWildcardPrincipal", wildcard, "summary",
				Results.map("bindings", allowCount + denyCount, "allow", allowCount, "deny", denyCount,
						"viaWildcardPrincipal", wildcardCount, "resources", rows.size(), "byResourceType", byType),
				"notes", notes.isEmpty() ? null : notes);
		out.putAll(Results.page("resources", rows, call.limit()));
		return out;
	}

	/** allowEveryoneIfNoAclFound parametresi; verilmediyse null. */
	private static Boolean allowEveryoneParam(ToolCall call) {
		return call.has(ALLOW_EVERYONE_PARAM) ? call.bool(ALLOW_EVERYONE_PARAM, false) : null;
	}

	private static List<String> names(Set<AclOperation> ops) {
		return ops.stream().map(AclOperation::name).toList();
	}

}
