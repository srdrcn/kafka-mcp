package io.github.srdrcn.kafkamcp.toolsets.acl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
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

/** acls_create (tek binding ya da producer/consumer/streams preset'i) ve acls_delete. */
@Component
public class AclWriteTools {

	public static final List<String> PRESETS = List.of("producer", "consumer", "streams");

	private static final String PROPAGATION_NOTE = "İşlem tamamlandı ama broker ACL önbelleği henüz güncellenmedi; "
			+ "birkaç saniye içinde acls_list ile görünür";

	private static final List<String> SINGLE_PARAMS = List.of("resourceType", "resourceName", "patternType",
			"operation");

	private static final List<String> PRESET_PARAMS = List.of("topic", "group", "transactionalId", "applicationId",
			"outputTopic");

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.write("acls_create", "ACL oluşturur: tek binding ya da preset (producer, consumer, streams); oluşan binding'leri döner")
			.required("principal", P.string("Principal (ör. User:alice; sadece ad verilirse User: eklenir)"))
			.param("preset", P.enumOf("Hazır set (verilirse topic zorunlu; resourceType/resourceName/operation verilmez)",
					PRESETS))
			.param("resourceType", Acls.resourceTypeParam("Kaynak tipi (preset yoksa zorunlu)", false))
			.param("resourceName", P.string("Kaynak adı (preset yoksa zorunlu; CLUSTER için kafka-cluster)"))
			.param("patternType", P.enumOf("Desen tipi (varsayılan LITERAL)", "LITERAL", "PREFIXED"))
			.param("operation", Acls.operationParam("İşlem (preset yoksa zorunlu)", false))
			.param("permission", P.enumOf("İzin (varsayılan ALLOW; preset'ler sadece ALLOW)", "ALLOW", "DENY"))
			.param("host", P.string("Host (varsayılan *)"))
			.param("topic", P.string("Preset topic'i (streams için input topic)"))
			.param("group", P.string("consumer preset'i: consumer group (zorunlu)"))
			.param("transactionalId", P.string("producer preset'i: transactional.id (opsiyonel)"))
			.param("applicationId", P.string("streams preset'i: application.id (zorunlu)"))
			.param("outputTopic", P.string("streams preset'i: çıktı topic'i (opsiyonel; WRITE, DESCRIBE)"))
			.handler(this::create),
				ToolSpec
					.destructive("acls_delete",
							"Filtreye uyan ACL'leri siler (principal, resourceType, resourceName zorunlu); silinen binding'leri döner")
					.required("principal", P.string("Principal (ör. User:alice; sadece ad verilirse User: eklenir)"))
					.required("resourceType", Acls.resourceTypeParam("Kaynak tipi", false))
					.required("resourceName", P.string("Kaynak adı (CLUSTER için kafka-cluster)"))
					.param("patternType", P.enumOf("Desen tipi (varsayılan LITERAL)", Acls.PATTERN_TYPES))
					.param("operation", Acls.operationParam("İşlem (varsayılan ANY)", true))
					.param("permission", P.enumOf("İzin (varsayılan ANY)", Acls.PERMISSIONS))
					.param("host", P.string("Host (varsayılan tümü)"))
					.handler(this::delete));
	}

	Object create(ToolCall call) {
		String principal = Acls.principal(call, "principal");
		String host = call.str("host", Acls.WILDCARD);
		String preset = call.str("preset", null);
		List<AclBinding> bindings = preset == null ? List.of(single(call, principal, host))
				: preset(call, preset, principal, host);
		Collection<AclBinding> created;
		try {
			call.await(call.admin().createAcls(bindings).all());
			created = bindings;
		}
		catch (ToolException e) {
			if (Acls.isSecurityDisabled(e)) {
				throw new ToolException(Acls.NO_AUTHORIZER_NOTE + " ACL oluşturulamaz.");
			}
			throw e;
		}
		boolean visible = Acls.awaitVisible(call, new AclBindingFilter(ResourcePatternFilter.ANY,
				new AccessControlEntryFilter(principal, null, AclOperation.ANY, AclPermissionType.ANY)), created, true);
		return Results.map("created", created.size(), "preset", preset, "bindings", Acls.render(created), "note",
				visible ? null : PROPAGATION_NOTE);
	}

	private AclBinding single(ToolCall call, String principal, String host) {
		List<String> extra = PRESET_PARAMS.stream().filter(call::has).toList();
		if (!extra.isEmpty()) {
			throw new ToolException(String.join(", ", extra) + " sadece preset ile kullanılır");
		}
		List<String> missing = List.of("resourceType", "operation").stream().filter(p -> !call.has(p)).toList();
		if (!missing.isEmpty()) {
			throw new ToolException("Eksik parametre: " + String.join(", ", missing)
					+ " (tek binding için resourceType, resourceName, operation; ya da preset verin)");
		}
		ResourceType type = Acls.parse("resourceType", call.str("resourceType"), ResourceType.class,
				Acls.withoutAny(Acls.RESOURCE_TYPES));
		String name = call.str("resourceName", type == ResourceType.CLUSTER ? Acls.CLUSTER_NAME : null);
		if (name == null) {
			throw new ToolException("Eksik parametre: resourceName");
		}
		PatternType pattern = Acls.parse("patternType", call.str("patternType", "LITERAL"), PatternType.class,
				List.of("LITERAL", "PREFIXED"));
		if (type == ResourceType.CLUSTER && (!Acls.CLUSTER_NAME.equals(name) || pattern != PatternType.LITERAL)) {
			throw new ToolException("CLUSTER kaynağı sadece LITERAL " + Acls.CLUSTER_NAME + " olabilir");
		}
		AclOperation op = Acls.parse("operation", call.str("operation"), AclOperation.class,
				Acls.withoutAny(Acls.OPERATIONS));
		AclPermissionType permission = Acls.parse("permission", call.str("permission", "ALLOW"),
				AclPermissionType.class, List.of("ALLOW", "DENY"));
		return binding(type, name, pattern, principal, host, op, permission);
	}

	private List<AclBinding> preset(ToolCall call, String presetValue, String principal, String host) {
		String preset = presetValue.toLowerCase(Locale.ROOT);
		if (!PRESETS.contains(preset)) {
			throw new ToolException("Geçersiz preset: " + presetValue + " (geçerli: " + String.join(", ", PRESETS) + ")");
		}
		List<String> single = SINGLE_PARAMS.stream().filter(call::has).toList();
		if (!single.isEmpty()) {
			throw new ToolException(String.join(", ", single) + " preset ile birlikte verilmez (preset binding'leri kendisi üretir)");
		}
		if (!"ALLOW".equalsIgnoreCase(call.str("permission", "ALLOW"))) {
			throw new ToolException("Preset'ler sadece ALLOW binding üretir; DENY için tek binding kullanın");
		}
		String topic = call.str("topic", null);
		if (topic == null) {
			throw new ToolException("preset=" + preset + " için topic zorunlu");
		}
		requireOnly(call, preset, "group", "consumer");
		requireOnly(call, preset, "transactionalId", "producer");
		requireOnly(call, preset, "applicationId", "streams");
		requireOnly(call, preset, "outputTopic", "streams");
		List<AclBinding> out = new ArrayList<>();
		switch (preset) {
			case "producer" -> {
				add(out, ResourceType.TOPIC, topic, PatternType.LITERAL, principal, host, AclOperation.WRITE,
						AclOperation.DESCRIBE);
				String txId = call.str("transactionalId", null);
				if (txId != null) {
					add(out, ResourceType.TRANSACTIONAL_ID, txId, PatternType.LITERAL, principal, host,
							AclOperation.WRITE, AclOperation.DESCRIBE);
				}
			}
			case "consumer" -> {
				String group = call.str("group", null);
				if (group == null) {
					throw new ToolException("preset=consumer için group zorunlu");
				}
				add(out, ResourceType.TOPIC, topic, PatternType.LITERAL, principal, host, AclOperation.READ,
						AclOperation.DESCRIBE);
				add(out, ResourceType.GROUP, group, PatternType.LITERAL, principal, host, AclOperation.READ);
			}
			default -> {
				String appId = call.str("applicationId", null);
				if (appId == null) {
					throw new ToolException("preset=streams için applicationId zorunlu");
				}
				add(out, ResourceType.TOPIC, topic, PatternType.LITERAL, principal, host, AclOperation.READ,
						AclOperation.DESCRIBE);
				add(out, ResourceType.TOPIC, appId, PatternType.PREFIXED, principal, host, AclOperation.ALL);
				add(out, ResourceType.GROUP, appId, PatternType.PREFIXED, principal, host, AclOperation.READ);
				add(out, ResourceType.TRANSACTIONAL_ID, appId, PatternType.PREFIXED, principal, host,
						AclOperation.ALL);
				String output = call.str("outputTopic", null);
				if (output != null) {
					add(out, ResourceType.TOPIC, output, PatternType.LITERAL, principal, host, AclOperation.WRITE,
							AclOperation.DESCRIBE);
				}
			}
		}
		return out;
	}

	private static void requireOnly(ToolCall call, String preset, String param, String allowedPreset) {
		if (call.has(param) && !allowedPreset.equals(preset)) {
			throw new ToolException(param + " sadece preset=" + allowedPreset + " ile kullanılır");
		}
	}

	private static void add(List<AclBinding> out, ResourceType type, String name, PatternType pattern,
			String principal, String host, AclOperation... ops) {
		for (AclOperation op : ops) {
			out.add(binding(type, name, pattern, principal, host, op, AclPermissionType.ALLOW));
		}
	}

	private static AclBinding binding(ResourceType type, String name, PatternType pattern, String principal,
			String host, AclOperation op, AclPermissionType permission) {
		return new AclBinding(new ResourcePattern(type, name, pattern),
				new AccessControlEntry(principal, host, op, permission));
	}

	Object delete(ToolCall call) {
		String principal = Acls.principal(call, "principal");
		String typeValue = call.str("resourceType");
		if ("ANY".equalsIgnoreCase(typeValue)) {
			throw new ToolException("resourceType ANY olamaz: geniş silme reddedildi, kaynak tipini belirtin");
		}
		ResourceType type = Acls.parse("resourceType", typeValue, ResourceType.class,
				Acls.withoutAny(Acls.RESOURCE_TYPES));
		String name = call.str("resourceName");
		PatternType pattern = Acls.patternType(call, "patternType", PatternType.LITERAL);
		AclOperation op = Acls.operation(call, "operation", AclOperation.ANY);
		AclPermissionType permission = Acls.permission(call, "permission", AclPermissionType.ANY);
		String host = call.str("host", null);
		AclBindingFilter filter = new AclBindingFilter(new ResourcePatternFilter(type, name, pattern),
				new AccessControlEntryFilter(principal, host, op, permission));
		Collection<AclBinding> deleted;
		try {
			deleted = call.await(call.admin().deleteAcls(List.of(filter)).all());
		}
		catch (ToolException e) {
			if (Acls.isSecurityDisabled(e)) {
				throw new ToolException(Acls.NO_AUTHORIZER_NOTE + " Silinecek ACL yok.");
			}
			throw e;
		}
		boolean gone = deleted.isEmpty() || Acls.awaitVisible(call, filter, deleted, false);
		Map<String, Object> filterView = Results.map("principal", principal, "resourceType", type.name(),
				"resourceName", name, "patternType", pattern.name(), "operation", op.name(), "permission",
				permission.name(), "host", host == null ? "ANY" : host);
		return Results.map("deleted", deleted.size(), "filter", filterView, "bindings",
				Acls.render(new ArrayList<>(deleted)), "note",
				deleted.isEmpty() ? "Filtreye uyan ACL yok; hiçbir şey silinmedi" : gone ? null : PROPAGATION_NOTE);
	}

}
