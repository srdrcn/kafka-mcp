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

/** acls_create (single binding or a producer/consumer/streams preset) and acls_delete. */
@Component
public class AclWriteTools {

	public static final List<String> PRESETS = List.of("producer", "consumer", "streams");

	private static final String PROPAGATION_NOTE = "Operation completed but the broker ACL cache is not updated yet; "
			+ "it shows up in acls_list within a few seconds";

	/** acls_delete patterns: no MATCH (it would also delete wildcard/prefix ACLs covering the resource); ANY only this name. */
	private static final List<String> DELETE_PATTERN_TYPES = List.of("LITERAL", "PREFIXED", "ANY");

	private static final List<String> SINGLE_PARAMS = List.of("resourceType", "resourceName", "patternType",
			"operation");

	private static final List<String> PRESET_PARAMS = List.of("topic", "group", "transactionalId", "applicationId",
			"outputTopic");

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.write("acls_create", "Create ACLs: a single binding or a preset (producer, consumer, streams); returns the created bindings")
			.required("principal", P.string("Principal (e.g. User:alice; a bare name gets the User: prefix)"))
			.param("preset", P.enumOf("Preset (requires topic; do not pass resourceType/resourceName/operation)",
					PRESETS))
			.param("resourceType", Acls.resourceTypeParam("Resource type (required without preset)", false))
			.param("resourceName", P.string("Resource name (required without preset; kafka-cluster for CLUSTER)"))
			.param("patternType", P.enumOf("Pattern type (default LITERAL)", "LITERAL", "PREFIXED"))
			.param("operation", Acls.operationParam("Operation (required without preset)", false))
			.param("permission", P.enumOf("Permission (default ALLOW; presets are ALLOW only)", "ALLOW", "DENY"))
			.param("host", P.string("Host (default *)"))
			.param("topic", P.string("Preset topic (input topic for streams)"))
			.param("group", P.string("consumer preset: consumer group (required)"))
			.param("transactionalId", P.string("producer preset: transactional.id (optional)"))
			.param("applicationId", P.string("streams preset: application.id (required)"))
			.param("outputTopic", P.string("streams preset: output topic (optional; WRITE, DESCRIBE)"))
			.handler(this::create),
				ToolSpec
					.destructive("acls_delete",
							"Delete ACLs matching the filter (principal, resourceType, resourceName required); returns the deleted bindings")
					.required("principal", P.string("Principal (e.g. User:alice; a bare name gets the User: prefix)"))
					.required("resourceType", Acls.resourceTypeParam("Resource type", false))
					.required("resourceName", P.string("Resource name (kafka-cluster for CLUSTER)"))
					.param("patternType", P.enumOf("Pattern type (default LITERAL; ANY: all patterns with this name)",
							DELETE_PATTERN_TYPES))
					.param("operation", Acls.operationParam("Operation (default ANY)", true))
					.param("permission", P.enumOf("Permission (default ANY)", Acls.PERMISSIONS))
					.param("host", P.string("Host (default all)"))
					.handler(this::delete));
	}

	Object create(ToolCall call) {
		String principal = Acls.principal(call, "principal");
		String host = call.str("host", Acls.WILDCARD);
		String preset = call.str("preset", null);
		List<String> notes = new ArrayList<>();
		List<AclBinding> bindings = preset == null ? List.of(single(call, principal, host))
				: preset(call, preset, principal, host, notes);
		Collection<AclBinding> created;
		try {
			call.await(call.admin().createAcls(bindings).all());
			created = bindings;
		}
		catch (ToolException e) {
			if (Acls.isSecurityDisabled(e)) {
				throw new ToolException(Acls.NO_AUTHORIZER_NOTE + " Cannot create ACLs.");
			}
			throw e;
		}
		boolean visible = Acls.awaitVisible(call, new AclBindingFilter(ResourcePatternFilter.ANY,
				new AccessControlEntryFilter(principal, null, AclOperation.ANY, AclPermissionType.ANY)), created, true);
		return Results.map("created", created.size(), "preset", preset, "bindings", Acls.render(created), "note",
				visible ? null : PROPAGATION_NOTE, "notes", notes.isEmpty() ? null : notes);
	}

	private AclBinding single(ToolCall call, String principal, String host) {
		List<String> extra = PRESET_PARAMS.stream().filter(call::has).toList();
		if (!extra.isEmpty()) {
			throw new ToolException(String.join(", ", extra) + " can only be used with preset");
		}
		List<String> missing = List.of("resourceType", "operation").stream().filter(p -> !call.has(p)).toList();
		if (!missing.isEmpty()) {
			throw new ToolException("Missing parameter: " + String.join(", ", missing)
					+ " (pass resourceType, resourceName, operation for a single binding, or a preset)");
		}
		ResourceType type = Acls.parse("resourceType", call.str("resourceType"), ResourceType.class,
				Acls.withoutAny(Acls.RESOURCE_TYPES));
		String name = call.str("resourceName", type == ResourceType.CLUSTER ? Acls.CLUSTER_NAME : null);
		if (name == null) {
			throw new ToolException("Missing parameter: resourceName");
		}
		PatternType pattern = Acls.parse("patternType", call.str("patternType", "LITERAL"), PatternType.class,
				List.of("LITERAL", "PREFIXED"));
		if (type == ResourceType.CLUSTER && (!Acls.CLUSTER_NAME.equals(name) || pattern != PatternType.LITERAL)) {
			throw new ToolException("The CLUSTER resource can only be LITERAL " + Acls.CLUSTER_NAME);
		}
		AclOperation op = Acls.parse("operation", call.str("operation"), AclOperation.class,
				Acls.withoutAny(Acls.OPERATIONS));
		AclPermissionType permission = Acls.parse("permission", call.str("permission", "ALLOW"),
				AclPermissionType.class, List.of("ALLOW", "DENY"));
		return binding(type, name, pattern, principal, host, op, permission);
	}

	private List<AclBinding> preset(ToolCall call, String presetValue, String principal, String host,
			List<String> notes) {
		String preset = presetValue.toLowerCase(Locale.ROOT);
		if (!PRESETS.contains(preset)) {
			throw new ToolException("Invalid preset: " + presetValue + " (valid: " + String.join(", ", PRESETS) + ")");
		}
		List<String> single = SINGLE_PARAMS.stream().filter(call::has).toList();
		if (!single.isEmpty()) {
			throw new ToolException(String.join(", ", single) + " cannot be used with preset (the preset generates the bindings itself)");
		}
		if (!"ALLOW".equalsIgnoreCase(call.str("permission", "ALLOW"))) {
			throw new ToolException("Presets only generate ALLOW bindings; use a single binding for DENY");
		}
		String topic = call.str("topic", null);
		if (topic == null) {
			throw new ToolException("topic is required for preset=" + preset);
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
					throw new ToolException("group is required for preset=consumer");
				}
				add(out, ResourceType.TOPIC, topic, PatternType.LITERAL, principal, host, AclOperation.READ,
						AclOperation.DESCRIBE);
				add(out, ResourceType.GROUP, group, PatternType.LITERAL, principal, host, AclOperation.READ);
			}
			default -> {
				String appId = call.str("applicationId", null);
				if (appId == null) {
					throw new ToolException("applicationId is required for preset=streams");
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
		// A transactional producer is authorized via TRANSACTIONAL_ID WRITE; the others (streams at_least_once
		// included) are idempotent by default on 3.0+ clients.
		if ("streams".equals(preset) || ("producer".equals(preset) && !call.has("transactionalId"))) {
			idempotentWrite(call, out, principal, host, notes);
		}
		return out;
	}

	/**
	 * Pre-2.8 (ZooKeeper) brokers require CLUSTER IDEMPOTENT_WRITE from the idempotent producer; 2.8+
	 * (KIP-679, KRaft included) accept WRITE on any topic. With an old/unknown IBP the binding is added (on 2.8+
	 * WRITE already grants this, so it is no extra privilege); if the mode cannot be read, only a note is added.
	 */
	private static void idempotentWrite(ToolCall call, List<AclBinding> out, String principal, String host,
			List<String> notes) {
		BrokerSecurity broker = BrokerSecurity.tryLoad(call);
		Boolean required = broker.idempotentWriteRequired();
		if (Boolean.TRUE.equals(required)) {
			add(out, ResourceType.CLUSTER, Acls.CLUSTER_NAME, PatternType.LITERAL, principal, host,
					AclOperation.IDEMPOTENT_WRITE);
			String ibp = broker.value(BrokerSecurity.INTER_BROKER_PROTOCOL_VERSION);
			notes.add("The ZooKeeper broker may be older than Kafka 2.8 (inter.broker.protocol.version="
					+ (ibp == null ? "unreadable" : ibp) + "): CLUSTER " + Acls.CLUSTER_NAME
					+ " IDEMPOTENT_WRITE was also added for the idempotent producer (3.0+ client default)");
		}
		else if (required == null) {
			notes.add("Could not read the broker mode/version"
					+ (broker.error() == null ? "" : " (" + broker.error() + ")")
					+ ": on ZooKeeper brokers older than Kafka 2.8 the idempotent producer needs CLUSTER " + Acls.CLUSTER_NAME
					+ " IDEMPOTENT_WRITE; add it if needed, or use enable.idempotence=false on the producer");
		}
	}

	private static void requireOnly(ToolCall call, String preset, String param, String allowedPreset) {
		if (call.has(param) && !allowedPreset.equals(preset)) {
			throw new ToolException(param + " can only be used with preset=" + allowedPreset);
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
			throw new ToolException("resourceType cannot be ANY: broad delete rejected, specify the resource type");
		}
		ResourceType type = Acls.parse("resourceType", typeValue, ResourceType.class,
				Acls.withoutAny(Acls.RESOURCE_TYPES));
		String name = call.str("resourceName");
		String patternValue = call.str("patternType", "LITERAL");
		if ("MATCH".equalsIgnoreCase(patternValue.trim())) {
			throw new ToolException("patternType=MATCH cannot be used for delete: it would also delete the \"*\" and PREFIXED "
					+ "ACLs covering this resource. Use LITERAL, PREFIXED or ANY");
		}
		PatternType pattern = Acls.parse("patternType", patternValue, PatternType.class, DELETE_PATTERN_TYPES);
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
				throw new ToolException(Acls.NO_AUTHORIZER_NOTE + " No ACLs to delete.");
			}
			throw e;
		}
		boolean gone = deleted.isEmpty() || Acls.awaitVisible(call, filter, deleted, false);
		Map<String, Object> filterView = Results.map("principal", principal, "resourceType", type.name(),
				"resourceName", name, "patternType", pattern.name(), "operation", op.name(), "permission",
				permission.name(), "host", host == null ? "ANY" : host);
		return Results.map("deleted", deleted.size(), "filter", filterView, "bindings",
				Acls.render(new ArrayList<>(deleted)), "note",
				deleted.isEmpty() ? "No ACLs match the filter; nothing was deleted" : gone ? null : PROPAGATION_NOTE);
	}

}
