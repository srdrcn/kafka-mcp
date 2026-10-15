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
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;

import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** ACL yardımcıları: binding'in JSON gösterimi, Kafka enum parametreleri, principal normalizasyonu, describeAcls. */
public final class Acls {

	/** CLUSTER kaynağının Kafka'daki tek geçerli adı. */
	public static final String CLUSTER_NAME = "kafka-cluster";

	/** Her kullanıcıyı kapsayan principal. */
	public static final String WILDCARD_PRINCIPAL = "User:*";

	/** LITERAL wildcard kaynak adı ve her host. */
	public static final String WILDCARD = "*";

	public static final List<String> RESOURCE_TYPES = List.of("TOPIC", "GROUP", "CLUSTER", "TRANSACTIONAL_ID",
			"DELEGATION_TOKEN", "USER", "ANY");

	public static final List<String> PATTERN_TYPES = List.of("LITERAL", "PREFIXED", "MATCH", "ANY");

	public static final List<String> OPERATIONS = List.of("ALL", "READ", "WRITE", "CREATE", "DELETE", "ALTER",
			"DESCRIBE", "CLUSTER_ACTION", "DESCRIBE_CONFIGS", "ALTER_CONFIGS", "IDEMPOTENT_WRITE", "CREATE_TOKENS",
			"DESCRIBE_TOKENS", "ANY");

	public static final List<String> PERMISSIONS = List.of("ALLOW", "DENY", "ANY");

	/** Authorizer kapalı cluster'lar için not. */
	public static final String NO_AUTHORIZER_NOTE = "Cluster'da authorizer tanımlı değil (authorizer.class.name boş): "
			+ "ACL'ler uygulanmıyor, kimliği doğrulanan her istemci her işlemi yapabilir.";

	/** Kaynak tipi, kaynak adı, principal, işlem, izin sırası. */
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

	/** Kaynak alanları olmadan (kaynağa göre gruplanmış çıktılar için). */
	public static Map<String, Object> renderEntry(AclBinding b) {
		return Results.map("principal", b.entry().principal(), "host", b.entry().host(), "operation",
				b.entry().operation().name(), "permission", b.entry().permissionType().name());
	}

	/** "alice" → "User:alice"; "User:alice" olduğu gibi. Boşsa null. */
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
			throw new ToolException("Geçersiz principal: " + p + " (biçim: User:alice)");
		}
		return p;
	}

	/** Zorunlu principal parametresi (normalize edilmiş). */
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

	/** Tek bir değeri enum'a çevirir; geçersizse Türkçe hata. */
	public static <E extends Enum<E>> E parse(String param, String value, Class<E> type, List<String> allowed) {
		String v = value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
		if (!allowed.contains(v)) {
			throw new ToolException("Geçersiz " + param + ": " + value + " (geçerli: " + String.join(", ", allowed) + ")");
		}
		return Enum.valueOf(type, v);
	}

	private static <E extends Enum<E>> E parse(ToolCall call, String name, E defaultValue, Class<E> type,
			List<String> allowed) {
		String v = call.str(name, null);
		return v == null ? defaultValue : parse(name, v, type, allowed);
	}

	/** ANY olmayan değerler (oluşturma/karar araçları için). */
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
	 * Tek describeAcls çağrısı. Cluster'da authorizer yoksa (SecurityDisabledException) null döner; çağıran
	 * {@link #NO_AUTHORIZER_NOTE} ile not düşer.
	 */
	public static List<AclBinding> describe(ToolCall call, AclBindingFilter filter) {
		return await(call, call.admin().describeAcls(filter).values());
	}

	/** Önceden başlatılmış describeAcls sonucunu bekler; authorizer kapalıysa null. */
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
	 * createAcls/deleteAcls sonrası broker'ın ACL önbelleği (KRaft'ta controller'dan biraz geç gelir)
	 * güncellenene kadar kısa süre bekler. {@code present=true}: hepsi görünene; false: filtreye uyan
	 * hiçbiri kalmayana kadar. Zamanında görünürse true.
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

	/** Hata zincirinde SecurityDisabledException (authorizer yok) var mı? */
	public static boolean isSecurityDisabled(Throwable error) {
		return causedBy(error, SecurityDisabledException.class);
	}

	/** Hata zincirinde verilen tipte bir istisna var mı? */
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

	/** Authorizer kapalıyken dönen standart sonuç. */
	public static Map<String, Object> noAuthorizer() {
		return Results.map("authorizerEnabled", false, "note", NO_AUTHORIZER_NOTE);
	}

}
