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

	private static final String SUPER_USERS_DESC = "Known super users (e.g. User:admin); used when the broker "
			+ "returns super.users as hidden";

	private static final String ALLOW_EVERYONE_PARAM = "allowEveryoneIfNoAclFound";

	private static final String ALLOW_EVERYONE_DESC = "The broker's allow.everyone.if.no.acl.found value; used when the "
			+ "broker returns it as hidden (assumed false if omitted)";

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("acls_explain",
					"Explain whether a principal may perform an operation on a resource under the built-in Kafka authorizer rules (ALLOW/DENY and why)")
			.required("principal", P.string("Principal (e.g. User:alice; a bare name gets the User: prefix)"))
			.required("operation", Acls.operationParam("Operation", false))
			.required("resourceType", Acls.resourceTypeParam("Resource type", false))
			.required("resourceName", P.string("Resource name (kafka-cluster for CLUSTER)"))
			.param("host", P.string("Client IP (default *: only ACLs whose host is * match)"))
			.param("superUsers", P.stringArray(SUPER_USERS_DESC))
			.param(ALLOW_EVERYONE_PARAM, P.bool(ALLOW_EVERYONE_DESC))
			.handler(this::explain),
				ToolSpec
					.read("acls_who_can_access_topic",
							"Show who can access a topic (ALLOW), who is explicitly denied (DENY), and the super users")
					.required("topic", P.string("Topic name"))
					.param("operation", Acls.operationParam("Operation (default READ and WRITE)", false))
					.param("superUsers", P.stringArray(SUPER_USERS_DESC))
					.param(ALLOW_EVERYONE_PARAM, P.bool(ALLOW_EVERYONE_DESC))
					.handler(this::whoCanAccessTopic),
				ToolSpec
					.read("acls_principal_permissions",
							"Group the ACLs that apply to a principal by resource and summarize the effective ALLOW/DENY operations")
					.required("principal", P.string("Principal (e.g. User:alice; a bare name gets the User: prefix)"))
					.param("includeWildcardPrincipal", P.bool("Also include User:* bindings (default true)"))
					.listParams()
					.param("filter", P.string("Resource name filter (regex, partial match)"))
					.param("limit", AclListTools.limitParam("Maximum number of resources to return (default 100)", 100))
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
			reason = "No ACLs on the resource; allow.everyone.if.no.acl.found unknown, assumed false → DENY "
					+ "(ALLOW if it is true on the broker)";
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
			notes.add("This principal has host-specific ACLs; with host=* only ACLs whose host is * were evaluated. "
					+ "Pass the client IP in the host parameter.");
		}
		if (type == ResourceType.CLUSTER && !Acls.CLUSTER_NAME.equals(name)) {
			notes.add("The CLUSTER resource name must always be " + Acls.CLUSTER_NAME);
		}
		if (operation == AclOperation.IDEMPOTENT_WRITE && type == ResourceType.CLUSTER && !d.allowed()
				&& !Boolean.TRUE.equals(broker.idempotentWriteRequired())) {
			notes.add("On Kafka 2.8+ brokers (KRaft included) the idempotent producer also works without IDEMPOTENT_WRITE: "
					+ "WRITE on any topic is enough (KIP-679); this decision only evaluates the IDEMPOTENT_WRITE "
					+ "ACL");
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
		// ACLs are evaluated without super users; super users are flagged separately.
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
				// DENY does not apply to a super user; for others host-specific DENYs are shown too.
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
					? "No ACLs apply to the topic; allow.everyone.if.no.acl.found unknown (assumed false): "
							+ "if false only super users can access it, if true everyone can"
					: allowEveryone.value()
							? "No ACLs apply to the topic and allow.everyone.if.no.acl.found=true: everyone can access it"
							: "No ACLs apply to the topic: only super users can access it (allow.everyone.if.no.acl.found=false)");
			if (allowEveryone.note() != null) {
				notes.add(allowEveryone.note());
			}
		}
		if (principals.contains(Acls.WILDCARD_PRINCIPAL)) {
			notes.add("User:* means everyone: with ALLOW, every user that is not explicitly denied can access it");
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
			// ALLOW/DENYs of other patterns that cover this pattern (LITERAL "*", PREFIXED prefix) also take effect.
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
			notes.add("Principal is a super user: authorized for every operation regardless of ACLs");
		}
		else if (!superUsers.known()) {
			notes.add("super.users unknown (the broker returned it as hidden); if the principal is a super user it is authorized for every operation");
		}
		if (bindings.isEmpty()) {
			notes.add("No ACLs apply to this principal");
		}
		Map<String, Object> out = Results.map("principal", principal, "superUser", superUser,
				"includeWildcardPrincipal", wildcard, "summary",
				Results.map("bindings", allowCount + denyCount, "allow", allowCount, "deny", denyCount,
						"viaWildcardPrincipal", wildcardCount, "resources", rows.size(), "byResourceType", byType),
				"notes", notes.isEmpty() ? null : notes);
		out.putAll(Results.page("resources", rows, call.limit()));
		return out;
	}

	/** allowEveryoneIfNoAclFound parameter; null if not given. */
	private static Boolean allowEveryoneParam(ToolCall call) {
		return call.has(ALLOW_EVERYONE_PARAM) ? call.bool(ALLOW_EVERYONE_PARAM, false) : null;
	}

	private static List<String> names(Set<AclOperation> ops) {
		return ops.stream().map(AclOperation::name).toList();
	}

}
