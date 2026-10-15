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

/** acls_list and acls_audit. */
@Component
public class AclListTools {

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("acls_list", "List ACLs, filtered by principal, resource type/name, pattern, operation, permission and host")
			.param("principal", P.string("Principal (e.g. User:alice; a bare name gets the User: prefix)"))
			.param("resourceType", Acls.resourceTypeParam("Resource type (default ANY)", true))
			.param("resourceName", P.string("Resource name (matched according to patternType)"))
			.param("patternType", P.enumOf(
					"Pattern type (default ANY; MATCH: literal, prefixed and * bindings that apply to this resource)",
					Acls.PATTERN_TYPES))
			.param("operation", Acls.operationParam("Operation (default ANY)", true))
			.param("permission", P.enumOf("Permission (default ANY)", Acls.PERMISSIONS))
			.param("host", P.string("Host (default all)"))
			.listParams()
			.param("filter", P.string("Principal or resource name filter (regex, partial match)"))
			.handler(this::list),
				ToolSpec
					.read("acls_audit",
							"Find risky and orphaned ACLs: User:*, wildcard resource, ALL, CLUSTER ALLOW, missing topic/group, unused prefix")
					.listParams(20)
					.param("filter", P.string("Scope: principal or resource name filter (regex, partial match)"))
					.param("limit", limitParam("Maximum samples per category (default 20)", 20))
					.handler(this::audit));
	}

	/** limit parameter with a custom description (same bounds as listParams). */
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

	/** An audit category: description and matching bindings. */
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
			notes.add("Could not list topics; topic checks skipped: " + e.getMessage());
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
			notes.add("Could not list groups; group check skipped: " + e.getMessage());
		}
		BrokerSecurity.SuperUsers superUsers = broker.superUsers(null);
		if (!superUsers.known()) {
			notes.add("super.users unknown: the allowOnCluster category may also list super users");
		}

		Map<String, Category> categories = new LinkedHashMap<>();
		categories.put("allowAllPrincipals", new Category("ACLs that ALLOW everyone (User:*)"));
		categories.put("allowWildcardResource", new Category("ACLs that ALLOW on all resources (LITERAL \"*\")"));
		categories.put("allowOperationAll", new Category("ALLOW ACLs with operation ALL (broadest permission)"));
		categories.put("allowOnCluster", new Category("ALLOW on the CLUSTER resource (principals that are not super users)"));
		categories.put("orphanTopicAcls", new Category("LITERAL ACLs on topics that do not exist (may be unused)"));
		categories.put("unverifiedTopicAcls", new Category(
				"LITERAL ACLs on topics this server's Kafka user cannot see (no DESCRIBE permission); the topic may exist"));
		categories.put("orphanGroupAcls",
				new Category("LITERAL ACLs on groups that do not exist (a group is created on first use; the application may not have started yet)"));
		categories.put("unusedPrefixedTopicAcls", new Category("PREFIXED topic ACLs that match no topic"));

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
			notes.add("Topic/group lists are limited by the DESCRIBE permission of this server's Kafka user: groups/topics it "
					+ "cannot see may look orphaned or unmatched; verify before deleting");
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
	 * listTopics only returns topics with DESCRIBE permission. Orphan candidates are verified with a single
	 * describeTopics: missing/invalid → stays orphaned; exists → removed; authorization or other error → unverified.
	 */
	private static void verifyOrphanTopics(ToolCall call, Admin admin, List<AclBinding> orphans,
			List<AclBinding> unverified, List<String> notes) {
		if (call.nearDeadline(2_000)) {
			notes.add("Running out of time: orphan topic candidates were not verified with describeTopics");
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
			notes.add("Some orphan topic candidates could not be verified: " + otherError);
		}
	}

	/** Does the sorted set contain an element starting with the prefix (O(log n) via ceiling)? */
	static boolean anyStartsWith(NavigableSet<String> sorted, String prefix) {
		String c = sorted.ceiling(prefix);
		return c != null && c.startsWith(prefix);
	}

}
