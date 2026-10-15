package io.github.srdrcn.kafkamcp.toolsets.acl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Makes authorization decisions with Kafka's built-in authorizer rules (KRaft StandardAuthorizer and ZooKeeper
 * AclAuthorizer/SimpleAclAuthorizer apply the same rules): super user → ALLOW; a matching DENY always wins;
 * otherwise a matching ALLOW; if the resource has no ACLs at all, allow.everyone.if.no.acl.found; otherwise
 * DENY. Matching: same resource type + (LITERAL exact name | LITERAL "*" | PREFIXED prefix), principal exact
 * or User:*, host exact or *, operation implication (ALL covers everything; for ALLOW, READ/WRITE/DELETE/ALTER
 * → DESCRIBE, ALTER_CONFIGS → DESCRIBE_CONFIGS).
 */
public final class AclEvaluator {

	/** Operations that also grant DESCRIBE in ALLOW bindings (same as Kafka). */
	public static final Set<AclOperation> IMPLIES_DESCRIBE = EnumSet.of(AclOperation.DESCRIBE, AclOperation.READ,
			AclOperation.WRITE, AclOperation.DELETE, AclOperation.ALTER);

	/** Operations that also grant DESCRIBE_CONFIGS in ALLOW bindings. */
	public static final Set<AclOperation> IMPLIES_DESCRIBE_CONFIGS = EnumSet.of(AclOperation.DESCRIBE_CONFIGS,
			AclOperation.ALTER_CONFIGS);

	private final List<AclBinding> bindings;

	private final Set<String> superUsers;

	private final boolean allowEveryoneIfNoAclFound;

	/**
	 * @param bindings ACLs to evaluate (those that do not match the resource are ignored)
	 * @param superUsers super user principals (e.g. User:admin); empty if unknown
	 * @param allowEveryoneIfNoAclFound the broker's allow.everyone.if.no.acl.found value
	 */
	public AclEvaluator(Collection<AclBinding> bindings, Collection<String> superUsers,
			boolean allowEveryoneIfNoAclFound) {
		this.bindings = List.copyOf(bindings);
		this.superUsers = superUsers == null ? Set.of() : Set.copyOf(superUsers);
		this.allowEveryoneIfNoAclFound = allowEveryoneIfNoAclFound;
	}

	/** Decision and its reason. */
	public record Decision(boolean allowed, String reason, boolean superUser, boolean noAclOnResource,
			int resourceAclCount, List<AclBinding> matchedAllow, List<AclBinding> matchedDeny) {

		public String decision() {
			return allowed ? "ALLOW" : "DENY";
		}

		/** JSON-friendly summary (bindings rendered). */
		public Map<String, Object> render() {
			return Results.map("decision", decision(), "reason", reason, "superUser", superUser,
					"resourceAclCount", resourceAclCount, "matchedAllow", Acls.render(matchedAllow), "matchedDeny",
					Acls.render(matchedDeny));
		}

	}

	/**
	 * Whether the principal may perform the operation on the resource from the host.
	 * @param host client IP; with "*" only bindings whose host is "*" match
	 */
	public Decision authorize(String principal, AclOperation operation, ResourceType type, String name,
			String host) {
		List<AclBinding> onResource = resourceBindings(type, name);
		List<AclBinding> allow = new ArrayList<>();
		List<AclBinding> deny = new ArrayList<>();
		for (AclBinding b : onResource) {
			if (!principalMatches(b.entry().principal(), principal) || !hostMatches(b.entry().host(), host)) {
				continue;
			}
			AclPermissionType perm = b.entry().permissionType();
			if (!operationMatches(b.entry().operation(), perm, operation)) {
				continue;
			}
			if (perm == AclPermissionType.DENY) {
				deny.add(b);
			}
			else if (perm == AclPermissionType.ALLOW) {
				allow.add(b);
			}
		}
		boolean superUser = superUsers.contains(principal);
		int count = onResource.size();
		if (superUser) {
			return new Decision(true, "Principal is a super user (super.users): ALLOW regardless of ACLs", true,
					count == 0, count, allow, deny);
		}
		if (!deny.isEmpty()) {
			return new Decision(false, "A matching DENY binding exists; DENY always overrides ALLOW", false, false, count,
					allow, deny);
		}
		if (!allow.isEmpty()) {
			return new Decision(true, "A matching ALLOW binding exists and no DENY" + implication(allow, operation), false,
					false, count, allow, deny);
		}
		if (count == 0) {
			return allowEveryoneIfNoAclFound
					? new Decision(true, "No ACLs on the resource and allow.everyone.if.no.acl.found=true → ALLOW", false,
							true, 0, allow, deny)
					: new Decision(false, "No ACLs on the resource and allow.everyone.if.no.acl.found=false → DENY", false,
							true, 0, allow, deny);
		}
		return new Decision(false, "The resource has " + count + " ACL(s) but no matching ALLOW for this principal/host/operation → DENY",
				false, false, count, allow, deny);
	}

	private static String implication(List<AclBinding> allow, AclOperation requested) {
		Set<String> implied = new LinkedHashSet<>();
		boolean all = false;
		for (AclBinding b : allow) {
			AclOperation op = b.entry().operation();
			if (op == requested) {
				return "";
			}
			all |= op == AclOperation.ALL;
			implied.add(op.name());
		}
		if (all) {
			return " (an ALL binding covers every operation)";
		}
		return " (" + requested.name() + " is implied by the " + String.join("/", implied) + " binding)";
	}

	/** All bindings that apply to the resource (regardless of principal/operation/host). */
	public List<AclBinding> resourceBindings(ResourceType type, String name) {
		List<AclBinding> out = new ArrayList<>();
		for (AclBinding b : bindings) {
			if (resourceMatches(b.pattern(), type, name)) {
				out.add(b);
			}
		}
		return out;
	}

	/** Does the binding's resource pattern apply to this resource (LITERAL exact name or "*", PREFIXED prefix)? */
	public static boolean resourceMatches(ResourcePattern pattern, ResourceType type, String name) {
		if (pattern.resourceType() != type) {
			return false;
		}
		if (pattern.patternType() == PatternType.LITERAL) {
			return pattern.name().equals(name) || pattern.name().equals(Acls.WILDCARD);
		}
		if (pattern.patternType() == PatternType.PREFIXED) {
			return name.startsWith(pattern.name());
		}
		return false;
	}

	/**
	 * Does the {@code outer} pattern also apply to every resource the {@code inner} pattern applies to? Same type;
	 * an equal pattern, LITERAL "*" covers everything, PREFIXED p covers LITERAL names and PREFIXED prefixes
	 * starting with p.
	 */
	public static boolean covers(ResourcePattern outer, ResourcePattern inner) {
		if (outer.resourceType() != inner.resourceType()) {
			return false;
		}
		if (outer.equals(inner)) {
			return true;
		}
		if (outer.patternType() == PatternType.LITERAL) {
			return outer.name().equals(Acls.WILDCARD);
		}
		if (outer.patternType() != PatternType.PREFIXED) {
			return false;
		}
		if (inner.patternType() == PatternType.LITERAL) {
			return !inner.name().equals(Acls.WILDCARD) && inner.name().startsWith(outer.name());
		}
		return inner.patternType() == PatternType.PREFIXED && inner.name().startsWith(outer.name());
	}

	/** Is the binding principal exactly the same, or User:*? */
	public static boolean principalMatches(String bindingPrincipal, String principal) {
		return bindingPrincipal.equals(principal) || bindingPrincipal.equals(Acls.WILDCARD_PRINCIPAL);
	}

	/** Is the binding host "*" or exactly the same? */
	public static boolean hostMatches(String bindingHost, String host) {
		return bindingHost.equals(Acls.WILDCARD) || bindingHost.equals(host);
	}

	/**
	 * Kafka rule: ALL covers every operation. In ALLOW bindings DESCRIBE ← READ/WRITE/DELETE/ALTER and
	 * DESCRIBE_CONFIGS ← ALTER_CONFIGS are implied; DENY only denies the same operation (or ALL).
	 */
	public static boolean operationMatches(AclOperation bindingOp, AclPermissionType permission,
			AclOperation requested) {
		if (bindingOp == AclOperation.ALL || bindingOp == requested) {
			return true;
		}
		if (permission != AclPermissionType.ALLOW) {
			return false;
		}
		if (requested == AclOperation.DESCRIBE) {
			return IMPLIES_DESCRIBE.contains(bindingOp);
		}
		if (requested == AclOperation.DESCRIBE_CONFIGS) {
			return IMPLIES_DESCRIBE_CONFIGS.contains(bindingOp);
		}
		return false;
	}

	/** Operations that make sense for the resource type (for expanding ALL). */
	public static List<AclOperation> operationsFor(ResourceType type) {
		return switch (type) {
			case TOPIC -> List.of(AclOperation.READ, AclOperation.WRITE, AclOperation.CREATE, AclOperation.DELETE,
					AclOperation.ALTER, AclOperation.DESCRIBE, AclOperation.DESCRIBE_CONFIGS,
					AclOperation.ALTER_CONFIGS);
			case GROUP -> List.of(AclOperation.READ, AclOperation.DESCRIBE, AclOperation.DELETE);
			case CLUSTER -> List.of(AclOperation.CREATE, AclOperation.CLUSTER_ACTION, AclOperation.DESCRIBE_CONFIGS,
					AclOperation.ALTER_CONFIGS, AclOperation.IDEMPOTENT_WRITE, AclOperation.ALTER,
					AclOperation.DESCRIBE);
			case TRANSACTIONAL_ID -> List.of(AclOperation.DESCRIBE, AclOperation.WRITE);
			case DELEGATION_TOKEN -> List.of(AclOperation.DESCRIBE);
			case USER -> List.of(AclOperation.CREATE_TOKENS, AclOperation.DESCRIBE_TOKENS);
			default -> List.of();
		};
	}

	/**
	 * Effective allowed operations from the ALLOW/DENY operations on the same resource pattern: implied
	 * operations are added, DENYs are removed. Just ["ALL"] for ALLOW ALL without DENY.
	 */
	public static List<String> effectiveAllow(ResourceType type, Collection<AclOperation> allowOps,
			Collection<AclOperation> denyOps) {
		if (denyOps.contains(AclOperation.ALL)) {
			return List.of();
		}
		if (allowOps.contains(AclOperation.ALL) && denyOps.isEmpty()) {
			return List.of("ALL");
		}
		Set<AclOperation> ops = new LinkedHashSet<>();
		if (allowOps.contains(AclOperation.ALL)) {
			ops.addAll(operationsFor(type));
		}
		ops.addAll(allowOps);
		ops.remove(AclOperation.ALL);
		for (AclOperation op : allowOps) {
			if (IMPLIES_DESCRIBE.contains(op)) {
				ops.add(AclOperation.DESCRIBE);
			}
			if (IMPLIES_DESCRIBE_CONFIGS.contains(op)) {
				ops.add(AclOperation.DESCRIBE_CONFIGS);
			}
		}
		ops.removeAll(denyOps);
		return ops.stream().sorted().map(AclOperation::name).toList();
	}

}
