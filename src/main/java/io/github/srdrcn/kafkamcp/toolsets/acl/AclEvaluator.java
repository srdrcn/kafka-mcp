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
 * Kafka StandardAuthorizer kurallarıyla yetki kararı verir: süper kullanıcı → ALLOW; eşleşen DENY her
 * zaman kazanır; yoksa eşleşen ALLOW; kaynakta hiç ACL yoksa allow.everyone.if.no.acl.found; aksi halde
 * DENY. Eşleşme: aynı kaynak tipi + (LITERAL tam ad | LITERAL "*" | PREFIXED önek), principal birebir ya
 * da User:*, host birebir ya da *, işlem çıkarımı (ALL her şeyi; ALLOW için READ/WRITE/DELETE/ALTER →
 * DESCRIBE, ALTER_CONFIGS → DESCRIBE_CONFIGS).
 */
public final class AclEvaluator {

	/** ALLOW binding'lerinde DESCRIBE'ı da veren işlemler (Kafka ile aynı). */
	public static final Set<AclOperation> IMPLIES_DESCRIBE = EnumSet.of(AclOperation.DESCRIBE, AclOperation.READ,
			AclOperation.WRITE, AclOperation.DELETE, AclOperation.ALTER);

	/** ALLOW binding'lerinde DESCRIBE_CONFIGS'i da veren işlemler. */
	public static final Set<AclOperation> IMPLIES_DESCRIBE_CONFIGS = EnumSet.of(AclOperation.DESCRIBE_CONFIGS,
			AclOperation.ALTER_CONFIGS);

	private final List<AclBinding> bindings;

	private final Set<String> superUsers;

	private final boolean allowEveryoneIfNoAclFound;

	/**
	 * @param bindings değerlendirilecek ACL'ler (kaynağa uymayanlar yok sayılır)
	 * @param superUsers süper kullanıcı principal'ları (ör. User:admin); bilinmiyorsa boş
	 * @param allowEveryoneIfNoAclFound broker'ın allow.everyone.if.no.acl.found değeri
	 */
	public AclEvaluator(Collection<AclBinding> bindings, Collection<String> superUsers,
			boolean allowEveryoneIfNoAclFound) {
		this.bindings = List.copyOf(bindings);
		this.superUsers = superUsers == null ? Set.of() : Set.copyOf(superUsers);
		this.allowEveryoneIfNoAclFound = allowEveryoneIfNoAclFound;
	}

	/** Karar ve gerekçesi. */
	public record Decision(boolean allowed, String reason, boolean superUser, boolean noAclOnResource,
			int resourceAclCount, List<AclBinding> matchedAllow, List<AclBinding> matchedDeny) {

		public String decision() {
			return allowed ? "ALLOW" : "DENY";
		}

		/** JSON'a uygun özet (binding'ler render edilmiş). */
		public Map<String, Object> render() {
			return Results.map("decision", decision(), "reason", reason, "superUser", superUser,
					"resourceAclCount", resourceAclCount, "matchedAllow", Acls.render(matchedAllow), "matchedDeny",
					Acls.render(matchedDeny));
		}

	}

	/**
	 * principal'ın host'tan kaynak üzerinde işlemi yapıp yapamayacağı.
	 * @param host istemci IP'si; "*" verilirse sadece host'u "*" olan binding'ler eşleşir
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
			return new Decision(true, "Principal süper kullanıcı (super.users): ACL'lere bakılmadan ALLOW", true,
					count == 0, count, allow, deny);
		}
		if (!deny.isEmpty()) {
			return new Decision(false, "Eşleşen DENY binding'i var; DENY her zaman ALLOW'u ezer", false, false, count,
					allow, deny);
		}
		if (!allow.isEmpty()) {
			return new Decision(true, "Eşleşen ALLOW binding'i var, DENY yok" + implication(allow, operation), false,
					false, count, allow, deny);
		}
		if (count == 0) {
			return allowEveryoneIfNoAclFound
					? new Decision(true, "Kaynakta hiç ACL yok ve allow.everyone.if.no.acl.found=true → ALLOW", false,
							true, 0, allow, deny)
					: new Decision(false, "Kaynakta hiç ACL yok ve allow.everyone.if.no.acl.found=false → DENY", false,
							true, 0, allow, deny);
		}
		return new Decision(false, "Kaynakta " + count + " ACL var ama bu principal/host/işlem için eşleşen ALLOW yok → DENY",
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
			return " (ALL binding'i tüm işlemleri kapsar)";
		}
		return " (" + requested.name() + " işlemi " + String.join("/", implied) + " binding'inden çıkarıldı)";
	}

	/** Kaynağa uygulanan tüm binding'ler (principal/işlem/host'tan bağımsız). */
	public List<AclBinding> resourceBindings(ResourceType type, String name) {
		List<AclBinding> out = new ArrayList<>();
		for (AclBinding b : bindings) {
			if (resourceMatches(b.pattern(), type, name)) {
				out.add(b);
			}
		}
		return out;
	}

	/** Binding'in kaynak deseni bu kaynağa uygulanır mı (LITERAL tam ad ya da "*", PREFIXED önek)? */
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
	 * {@code outer} deseni {@code inner} desenin uygulandığı her kaynağa da uygulanır mı? Aynı tip; eşit desen,
	 * LITERAL "*" her şeyi, PREFIXED p ise p ile başlayan LITERAL adları ve PREFIXED önekleri kapsar.
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

	/** Binding principal'ı birebir aynı ya da User:* mı? */
	public static boolean principalMatches(String bindingPrincipal, String principal) {
		return bindingPrincipal.equals(principal) || bindingPrincipal.equals(Acls.WILDCARD_PRINCIPAL);
	}

	/** Binding host'u "*" ya da birebir aynı mı? */
	public static boolean hostMatches(String bindingHost, String host) {
		return bindingHost.equals(Acls.WILDCARD) || bindingHost.equals(host);
	}

	/**
	 * Kafka kuralı: ALL her işlemi kapsar. ALLOW binding'lerinde DESCRIBE ← READ/WRITE/DELETE/ALTER ve
	 * DESCRIBE_CONFIGS ← ALTER_CONFIGS çıkarımı yapılır; DENY sadece aynı işlemi (ya da ALL) reddeder.
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

	/** Kaynak tipinde anlamlı işlemler (ALL açılımı için). */
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
	 * Aynı kaynak desenindeki ALLOW/DENY işlemlerinden etkin izinli işlemler: çıkarımlar eklenir, DENY'ler
	 * düşülür. ALLOW ALL ve DENY yoksa sadece ["ALL"].
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
