package io.github.srdrcn.kafkamcp.toolsets.acl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;

import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** ACL helpers: JSON view of a binding, Kafka enum parameters, principal normalization, describeAcls. */
public final class Acls {

	/** The only valid Kafka name of the CLUSTER resource. */
	public static final String CLUSTER_NAME = "kafka-cluster";

	/** Principal that matches every user. */
	public static final String WILDCARD_PRINCIPAL = "User:*";

	/** LITERAL wildcard resource name and any host. */
	public static final String WILDCARD = "*";

	public static final List<String> RESOURCE_TYPES = List.of("TOPIC", "GROUP", "CLUSTER", "TRANSACTIONAL_ID",
			"DELEGATION_TOKEN", "USER", "ANY");

	public static final List<String> PATTERN_TYPES = List.of("LITERAL", "PREFIXED", "MATCH", "ANY");

	public static final List<String> OPERATIONS = List.of("ALL", "READ", "WRITE", "CREATE", "DELETE", "ALTER",
			"DESCRIBE", "CLUSTER_ACTION", "DESCRIBE_CONFIGS", "ALTER_CONFIGS", "IDEMPOTENT_WRITE", "CREATE_TOKENS",
			"DESCRIBE_TOKENS", "ANY");

	public static final List<String> PERMISSIONS = List.of("ALLOW", "DENY", "ANY");

	/** Note for clusters without an authorizer. */
	public static final String NO_AUTHORIZER_NOTE = "No authorizer configured on the cluster (authorizer.class.name is empty): "
			+ "ACLs are not enforced; every authenticated client can perform every operation.";

	/** Ordered by resource type, resource name, principal, operation, permission. */
	public static final Comparator<AclBinding> ORDER = Comparator
		.comparing((AclBinding b) -> b.pattern().resourceType().name())
		.thenComparing(b -> b.pattern().name())
		.thenComparing(b -> b.pattern().patternType().name())
		.thenComparing(b -> b.entry().principal())
		.thenComparing(b -> b.entry().operation().name())
		.thenComparing(b -> b.entry().permissionType().name())
		.thenComparing(b -> b.entry().host());

	private Acls() {
	}

	/** {principal, host, operation, permission, resourceType, resourceName, patternType} */
	public static Map<String, Object> render(AclBinding b) {
		return Results.map("principal", b.entry().principal(), "host", b.entry().host(), "operation",
				b.entry().operation().name(), "permission", b.entry().permissionType().name(), "resourceType",
				b.pattern().resourceType().name(), "resourceName", b.pattern().name(), "patternType",
				b.pattern().patternType().name());
	}

	public static List<Map<String, Object>> render(Collection<AclBinding> bindings) {
		return bindings.stream().sorted(ORDER).map(Acls::render).toList();
	}

	/** Without the resource fields (for output grouped by resource). */
	public static Map<String, Object> renderEntry(AclBinding b) {
		return Results.map("principal", b.entry().principal(), "host", b.entry().host(), "operation",
				b.entry().operation().name(), "permission", b.entry().permissionType().name());
	}

	/** "alice" → "User:alice"; "User:alice" is kept as is. Null if blank. */
	public static String normalizePrincipal(String principal) {
		if (principal == null || principal.isBlank()) {
			return null;
		}
		String p = principal.trim();
		int i = p.indexOf(':');
		if (i < 0) {
			return "User:" + p;
		}
		if (i == 0 || i == p.length() - 1) {
			throw new ToolException("Invalid principal: " + p + " (format: User:alice)");
		}
		return p;
	}

	/** Required principal parameter (normalized). */
	public static String principal(ToolCall call, String name) {
		return normalizePrincipal(call.str(name));
	}

	public static ResourceType resourceType(ToolCall call, String name, ResourceType defaultValue) {
		return parse(call, name, defaultValue, ResourceType.class, RESOURCE_TYPES);
	}

	public static PatternType patternType(ToolCall call, String name, PatternType defaultValue) {
		return parse(call, name, defaultValue, PatternType.class, PATTERN_TYPES);
	}

	public static AclOperation operation(ToolCall call, String name, AclOperation defaultValue) {
		return parse(call, name, defaultValue, AclOperation.class, OPERATIONS);
	}

	public static AclPermissionType permission(ToolCall call, String name, AclPermissionType defaultValue) {
		return parse(call, name, defaultValue, AclPermissionType.class, PERMISSIONS);
	}

	/** Converts a single value to the enum; throws a readable error if it is invalid. */
	public static <E extends Enum<E>> E parse(String param, String value, Class<E> type, List<String> allowed) {
		String v = value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
		if (!allowed.contains(v)) {
			throw new ToolException("Invalid " + param + ": " + value + " (valid: " + String.join(", ", allowed) + ")");
		}
		return Enum.valueOf(type, v);
	}

	private static <E extends Enum<E>> E parse(ToolCall call, String name, E defaultValue, Class<E> type,
			List<String> allowed) {
		String v = call.str(name, null);
		return v == null ? defaultValue : parse(name, v, type, allowed);
	}

	/** Values without ANY (for create/decision tools). */
	public static List<String> withoutAny(List<String> values) {
		List<String> out = new ArrayList<>(values);
		out.remove("ANY");
		return out;
	}

	public static Map<String, Object> resourceTypeParam(String description, boolean includeAny) {
		return P.enumOf(description, includeAny ? RESOURCE_TYPES : withoutAny(RESOURCE_TYPES));
	}

	public static Map<String, Object> operationParam(String description, boolean includeAny) {
		return P.enumOf(description, includeAny ? OPERATIONS : withoutAny(OPERATIONS));
	}

	/**
	 * Single describeAcls call. Returns null if the cluster has no authorizer (SecurityDisabledException); the
	 * caller adds {@link #NO_AUTHORIZER_NOTE} as a note.
	 */
	public static List<AclBinding> describe(ToolCall call, AclBindingFilter filter) {
		return await(call, call.admin().describeAcls(filter).values());
	}

	/** Waits for an already started describeAcls result; null if the authorizer is disabled. */
	public static List<AclBinding> await(ToolCall call, KafkaFuture<Collection<AclBinding>> future) {
		try {
			return new ArrayList<>(call.await(future));
		}
		catch (ToolException e) {
			if (isSecurityDisabled(e)) {
				return null;
			}
			throw e;
		}
	}

	/**
	 * After createAcls/deleteAcls, waits briefly until the broker ACL cache is updated (ACLs reach the other
	 * brokers a little later: via the metadata log in KRaft, via ZooKeeper change notifications in ZooKeeper;
	 * describeAcls may go to any broker). {@code present=true}: until all are visible; false: until none
	 * matching the filter is left. True if this happens in time.
	 */
	public static boolean awaitVisible(ToolCall call, AclBindingFilter filter, Collection<AclBinding> expected,
			boolean present) {
		long until = System.currentTimeMillis() + 3_000;
		while (true) {
			List<AclBinding> now = describe(call, filter);
			if (now == null) {
				return true;
			}
			boolean done = present ? now.containsAll(expected) : now.stream().noneMatch(expected::contains);
			if (done) {
				return true;
			}
			if (System.currentTimeMillis() > until || call.nearDeadline(2_000)) {
				return false;
			}
			call.sleep(100);
		}
	}

	/**
	 * Short note if an older broker does not support the API (UnsupportedVersionException), otherwise null.
	 * @param requirement description of the required version (e.g. "listing SCRAM users requires Kafka 2.7+")
	 */
	public static String unsupportedNote(Throwable error, String requirement) {
		if (!causedBy(error, UnsupportedVersionException.class)) {
			return null;
		}
		return KafkaErrors.UNSUPPORTED + ": " + requirement;
	}

	/** Standard result for an unsupported operation: {supported: false, note}. */
	public static Map<String, Object> unsupported(String note) {
		return Results.map("supported", false, "note", note);
	}

	/** Is there a SecurityDisabledException (no authorizer) in the error chain? */
	public static boolean isSecurityDisabled(Throwable error) {
		return causedBy(error, SecurityDisabledException.class);
	}

	/** Is there an exception of the given type in the error chain? */
	public static boolean causedBy(Throwable error, Class<? extends Throwable> type) {
		Throwable t = error;
		for (int i = 0; t != null && i < 10; i++) {
			if (type.isInstance(KafkaErrors.unwrap(t))) {
				return true;
			}
			t = t.getCause();
		}
		return false;
	}

	/** Standard result when the authorizer is disabled. */
	public static Map<String, Object> noAuthorizer() {
		return Results.map("authorizerEnabled", false, "note", NO_AUTHORIZER_NOTE);
	}

}
