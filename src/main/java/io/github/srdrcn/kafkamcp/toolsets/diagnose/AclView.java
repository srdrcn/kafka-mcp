package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.toolsets.acl.AclEvaluator;
import io.github.srdrcn.kafkamcp.toolsets.acl.Acls;
import io.github.srdrcn.kafkamcp.toolsets.acl.BrokerSecurity;

/**
 * ACLs that apply to the selected resources (one describeAcls per resource, MATCH: literal + "*" + prefixed) and
 * authorization decisions through {@link AclEvaluator} using the broker's super.users /
 * allow.everyone.if.no.acl.found values. Without an authorizer {@link #authorizerEnabled()} is false (everything is
 * allowed).
 */
public final class AclView {

	/** Resource type + name. */
	public record Resource(ResourceType type, String name) {
	}

	private final List<AclBinding> bindings;

	private final BrokerSecurity broker;

	private final BrokerSecurity.SuperUsers superUsers;

	private final BrokerSecurity.AllowEveryone allowEveryone;

	private AclView(List<AclBinding> bindings, BrokerSecurity broker, BrokerSecurity.SuperUsers superUsers,
			BrokerSecurity.AllowEveryone allowEveryone) {
		this.bindings = bindings;
		this.broker = broker;
		this.superUsers = superUsers;
		this.allowEveryone = allowEveryone;
	}

	/**
	 * Reads the ACLs of the resources (parallel describeAcls) and the broker security configs.
	 * @param knownSuperUsers list to use if the broker hides super.users (may be null)
	 */
	public static AclView load(ToolCall call, Collection<Resource> resources, Collection<String> knownSuperUsers) {
		Map<Resource, KafkaFuture<Collection<AclBinding>>> futures = new LinkedHashMap<>();
		for (Resource r : new LinkedHashSet<>(resources)) {
			futures.put(r, call.admin()
				.describeAcls(new AclBindingFilter(new ResourcePatternFilter(r.type(), r.name(), PatternType.MATCH),
						AccessControlEntryFilter.ANY))
				.values());
		}
		BrokerSecurity broker = BrokerSecurity.tryLoad(call);
		Set<AclBinding> all = new LinkedHashSet<>();
		boolean disabled = false;
		for (KafkaFuture<Collection<AclBinding>> f : futures.values()) {
			List<AclBinding> b = Acls.await(call, f);
			if (b == null) {
				disabled = true;
				break;
			}
			all.addAll(b);
		}
		return new AclView(disabled ? null : List.copyOf(all), broker, broker.superUsers(knownSuperUsers),
				broker.allowEveryone());
	}

	/** Is an authorizer configured (describeAcls did not return SecurityDisabled)? */
	public boolean authorizerEnabled() {
		return bindings != null;
	}

	/** Bindings read (empty without an authorizer). */
	public List<AclBinding> bindings() {
		return bindings == null ? List.of() : bindings;
	}

	public BrokerSecurity broker() {
		return broker;
	}

	public BrokerSecurity.SuperUsers superUsers() {
		return superUsers;
	}

	public BrokerSecurity.AllowEveryone allowEveryone() {
		return allowEveryone;
	}

	/** authorizer.class.name (null if unreadable or not set). */
	public String authorizerClass() {
		return broker.value(BrokerSecurity.AUTHORIZER);
	}

	/** Decision by Kafka's rules (super users included). Do not call without an authorizer. */
	public AclEvaluator.Decision decide(String principal, AclOperation op, ResourceType type, String name,
			String host) {
		return new AclEvaluator(bindings(), superUsers.users(), allowEveryone.value()).authorize(principal, op, type,
				name, host);
	}

	/** Decision if the DENY bindings were ignored (would it be allowed once the DENY is removed?). */
	public AclEvaluator.Decision decideIgnoringDeny(String principal, AclOperation op, ResourceType type, String name,
			String host) {
		List<AclBinding> allowOnly = bindings().stream()
			.filter(b -> b.entry().permissionType() != AclPermissionType.DENY)
			.toList();
		return new AclEvaluator(allowOnly, superUsers.users(), allowEveryone.value()).authorize(principal, op, type,
				name, host);
	}

	/** Number of bindings that apply to the resource (literal, "*" and prefixed). */
	public int resourceAclCount(ResourceType type, String name) {
		return new AclEvaluator(bindings(), Set.of(), false).resourceBindings(type, name).size();
	}

	/** Principals in the bindings that apply to the resource (sorted). */
	public Set<String> principals(ResourceType type, String name) {
		Set<String> out = new TreeSet<>();
		new AclEvaluator(bindings(), Set.of(), false).resourceBindings(type, name)
			.forEach(b -> out.add(b.entry().principal()));
		return out;
	}

	/**
	 * Principals that can perform the operation on the resource from at least one host (those in the bindings; super
	 * users are evaluated separately and not counted here).
	 */
	public List<String> allowedPrincipals(AclOperation op, ResourceType type, String name) {
		AclEvaluator e = new AclEvaluator(bindings(), Set.of(), allowEveryone.value());
		List<String> out = new ArrayList<>();
		for (String p : principals(type, name)) {
			for (String host : hosts(p)) {
				if (e.authorize(p, op, type, name, host).allowed()) {
					out.add(p);
					break;
				}
			}
		}
		return out;
	}

	/** Principals with a matching DENY binding for the operation on the resource. */
	public List<String> deniedPrincipals(AclOperation op, ResourceType type, String name) {
		AclEvaluator e = new AclEvaluator(bindings(), Set.of(), allowEveryone.value());
		List<String> out = new ArrayList<>();
		for (String p : principals(type, name)) {
			for (String host : hosts(p)) {
				if (!e.authorize(p, op, type, name, host).matchedDeny().isEmpty()) {
					out.add(p);
					break;
				}
			}
		}
		return out;
	}

	/** Hosts in the principal's bindings ("*" included). */
	public Set<String> hosts(String principal) {
		Set<String> out = new LinkedHashSet<>();
		out.add(Acls.WILDCARD);
		for (AclBinding b : bindings()) {
			if (AclEvaluator.principalMatches(b.entry().principal(), principal)) {
				out.add(b.entry().host());
			}
		}
		return out;
	}

	/** Does the principal have a binding whose host is not "*" (the host parameter matters)? */
	public boolean hasHostSpecific(String principal) {
		return bindings().stream()
			.anyMatch(b -> !Acls.WILDCARD.equals(b.entry().host())
					&& AclEvaluator.principalMatches(b.entry().principal(), principal));
	}

}
