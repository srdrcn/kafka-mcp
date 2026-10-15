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
 * Seçili kaynaklara uygulanan ACL'ler (kaynak başına bir describeAcls, MATCH: literal + "*" + prefixed) ve
 * broker'ın super.users / allow.everyone.if.no.acl.found değerleriyle {@link AclEvaluator} üzerinden yetki
 * kararları. Authorizer yoksa {@link #authorizerEnabled()} false olur (her şeye izin var).
 */
public final class AclView {

	/** Kaynak tipi + adı. */
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
	 * Kaynakların ACL'lerini (paralel describeAcls) ve broker güvenlik config'lerini okur.
	 * @param knownSuperUsers broker super.users'ı gizlerse kullanılacak liste (null olabilir)
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

	/** Authorizer tanımlı mı (describeAcls SecurityDisabled dönmedi)? */
	public boolean authorizerEnabled() {
		return bindings != null;
	}

	/** Okunan binding'ler (authorizer yoksa boş). */
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

	/** authorizer.class.name (okunamadıysa ya da tanımsızsa null). */
	public String authorizerClass() {
		return broker.value(BrokerSecurity.AUTHORIZER);
	}

	/** Kafka kurallarıyla karar (süper kullanıcılar dahil). Authorizer yoksa çağırmayın. */
	public AclEvaluator.Decision decide(String principal, AclOperation op, ResourceType type, String name,
			String host) {
		return new AclEvaluator(bindings(), superUsers.users(), allowEveryone.value()).authorize(principal, op, type,
				name, host);
	}

	/** DENY binding'leri yok sayılsaydı karar (DENY kaldırılınca izin olur mu?). */
	public AclEvaluator.Decision decideIgnoringDeny(String principal, AclOperation op, ResourceType type, String name,
			String host) {
		List<AclBinding> allowOnly = bindings().stream()
			.filter(b -> b.entry().permissionType() != AclPermissionType.DENY)
			.toList();
		return new AclEvaluator(allowOnly, superUsers.users(), allowEveryone.value()).authorize(principal, op, type,
				name, host);
	}

	/** Kaynağa uygulanan binding sayısı (literal, "*" ve prefixed). */
	public int resourceAclCount(ResourceType type, String name) {
		return new AclEvaluator(bindings(), Set.of(), false).resourceBindings(type, name).size();
	}

	/** Kaynağa uygulanan binding'lerdeki principal'lar (sıralı). */
	public Set<String> principals(ResourceType type, String name) {
		Set<String> out = new TreeSet<>();
		new AclEvaluator(bindings(), Set.of(), false).resourceBindings(type, name)
			.forEach(b -> out.add(b.entry().principal()));
		return out;
	}

	/**
	 * Kaynakta işlemi en az bir host'tan yapabilen principal'lar (binding'lerde geçenler; süper kullanıcılar
	 * ayrıca değerlendirilir, burada sayılmaz).
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

	/** Kaynakta işlem için eşleşen DENY binding'i olan principal'lar. */
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

	/** Principal'ın binding'lerinde geçen host'lar ("*" dahil). */
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

	/** Principal için host'u "*" olmayan binding var mı (host parametresi önemli)? */
	public boolean hasHostSpecific(String principal) {
		return bindings().stream()
			.anyMatch(b -> !Acls.WILDCARD.equals(b.entry().host())
					&& AclEvaluator.principalMatches(b.entry().principal(), principal));
	}

}
