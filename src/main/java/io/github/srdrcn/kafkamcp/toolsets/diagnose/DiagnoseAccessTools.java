package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static io.github.srdrcn.kafkamcp.toolsets.diagnose.Diagnosis.evidence;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.KafkaVersions;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.acl.AclEvaluator;
import io.github.srdrcn.kafkamcp.toolsets.acl.Acls;
import io.github.srdrcn.kafkamcp.toolsets.diagnose.Checks.Check;

/**
 * diagnose_access: evaluates with {@link AclEvaluator} whether a principal can perform every operation an intent
 * (produce, consume, transactional-produce, admin-topic) needs; for the missing ones it suggests binding objects that
 * can be passed directly to acls_create and a matching preset.
 */
@Component
public class DiagnoseAccessTools {

	public static final List<String> INTENTS = List.of("produce", "consume", "transactional-produce", "admin-topic");

	/** Built-in authorizers (same rules as AclEvaluator). */
	public static final List<String> BUILT_IN_AUTHORIZERS = List.of(
			"org.apache.kafka.metadata.authorizer.StandardAuthorizer", "kafka.security.authorizer.AclAuthorizer",
			"kafka.security.auth.SimpleAclAuthorizer");

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("diagnose_access",
					"Check if a principal can produce/consume/transactional-produce/admin-topic on a topic; suggests missing ACLs for acls_create")
			.required("principal", P.string("Principal (e.g. User:alice; a bare name gets the User: prefix)"))
			.required("intent", P.enumOf("Intent", INTENTS))
			.required("topic", P.string("Topic name"))
			.param("group", P.string("Consumer group (required for consume; for offset commits with transactional-produce)"))
			.param("transactionalId", P.string("transactional.id (required for transactional-produce)"))
			.param("host", P.string("Client IP (default *: only ACLs with host * match)"))
			.handler(this::diagnose));
	}

	/** A single operation the intent requires. */
	public record Requirement(ResourceType type, String name, AclOperation operation, String purpose) {
	}

	/**
	 * Operations the intent requires. idempotentWrite: with a broker &lt; 2.8, produce needs CLUSTER IDEMPOTENT_WRITE
	 * (before KIP-679).
	 */
	public static List<Requirement> requirements(String intent, String topic, String group, String transactionalId,
			boolean idempotentWrite) {
		List<Requirement> out = new ArrayList<>();
		switch (intent) {
			case "produce" -> {
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.WRITE, "Produce request"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.DESCRIBE, "Metadata (see the topic)"));
				if (idempotentWrite) {
					out.add(new Requirement(ResourceType.CLUSTER, Acls.CLUSTER_NAME, AclOperation.IDEMPOTENT_WRITE,
							"Idempotent producer (InitProducerId), broker < 2.8"));
				}
			}
			case "consume" -> {
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.READ, "Fetch (read messages)"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.DESCRIBE, "Metadata/ListOffsets"));
				out.add(new Requirement(ResourceType.GROUP, group, AclOperation.READ,
						"JoinGroup/OffsetCommit/OffsetFetch"));
			}
			case "transactional-produce" -> {
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.WRITE, "Produce request"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.DESCRIBE, "Metadata (see the topic)"));
				out.add(new Requirement(ResourceType.TRANSACTIONAL_ID, transactionalId, AclOperation.WRITE,
						"InitProducerId/AddPartitionsToTxn/EndTxn"));
				out.add(new Requirement(ResourceType.TRANSACTIONAL_ID, transactionalId, AclOperation.DESCRIBE,
						"FindCoordinator (transaction coordinator)"));
				if (group != null) {
					out.add(new Requirement(ResourceType.GROUP, group, AclOperation.READ,
							"sendOffsetsToTransaction (TxnOffsetCommit)"));
				}
			}
			case "admin-topic" -> {
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.CREATE,
						"CreateTopics (CLUSTER CREATE is enough too)"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.ALTER, "CreatePartitions"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.DELETE, "DeleteTopics/DeleteRecords"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.DESCRIBE_CONFIGS, "DescribeConfigs"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.ALTER_CONFIGS,
						"AlterConfigs/IncrementalAlterConfigs"));
			}
			default -> throw new ToolException("Invalid intent: " + intent + " (valid: " + String.join(", ", INTENTS) + ")");
		}
		return out;
	}

	/** ALLOW binding in the acls_create parameter format. */
	public static Map<String, Object> binding(String principal, Requirement r, String host) {
		return Results.map("principal", principal, "resourceType", r.type().name(), "resourceName", r.name(),
				"patternType", "LITERAL", "operation", r.operation().name(), "permission", "ALLOW", "host", host);
	}

	/** Is a version such as "3.9-IV0" older than 2.8? null if it cannot be parsed. */
	public static Boolean olderThan28(String version) {
		String v = KafkaVersions.releaseOf(version);
		if (v == null) {
			return null;
		}
		String[] parts = v.split("\\.");
		try {
			int major = Integer.parseInt(parts[0]);
			int minor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
			return major < 2 || major == 2 && minor < 8;
		}
		catch (NumberFormatException e) {
			return null;
		}
	}

	Object diagnose(ToolCall call) {
		String principal = Acls.principal(call, "principal");
		String intent = call.str("intent").toLowerCase(Locale.ROOT);
		if (!INTENTS.contains(intent)) {
			throw new ToolException("Invalid intent: " + intent + " (valid: " + String.join(", ", INTENTS) + ")");
		}
		String topic = call.str("topic");
		String group = call.str("group", null);
		String txId = call.str("transactionalId", null);
		String host = call.str("host", Acls.WILDCARD);
		if ("consume".equals(intent) && group == null) {
			throw new ToolException("group is required for intent=consume");
		}
		if ("transactional-produce".equals(intent) && txId == null) {
			throw new ToolException("transactionalId is required for intent=transactional-produce");
		}
		Diagnosis dx = new Diagnosis();
		Map<String, Object> facts = new LinkedHashMap<>();
		Map<String, Object> extra = new LinkedHashMap<>();
		String mode;
		try (Checks checks = new Checks(call, dx)) {
			Check<ClusterMeta.Info> meta = checks.start("cluster metadata", () -> ClusterMeta.describe(call, true));
			Check<Boolean> exists = checks.start("topic", () -> topicExists(call, topic));
			List<AclView.Resource> resources = new ArrayList<>();
			resources.add(new AclView.Resource(ResourceType.TOPIC, topic));
			resources.add(new AclView.Resource(ResourceType.CLUSTER, Acls.CLUSTER_NAME));
			if (group != null) {
				resources.add(new AclView.Resource(ResourceType.GROUP, group));
			}
			if (txId != null) {
				resources.add(new AclView.Resource(ResourceType.TRANSACTIONAL_ID, txId));
			}
			Check<AclView> acl = checks.start("ACL", () -> AclView.load(call, resources, null));

			ClusterMeta.Info info = meta.get();
			mode = info == null ? null : info.mode();
			AclView view = acl.get();
			Boolean topicExists = exists.get();

			Boolean idempotentRequired = null;
			String brokerVersion = null;
			if (ClusterMeta.KRAFT.equals(mode)) {
				idempotentRequired = false;
			}
			else if (view != null) {
				brokerVersion = view.broker().value("inter.broker.protocol.version");
				idempotentRequired = brokerVersion == null ? null : olderThan28(brokerVersion);
			}
			boolean needIdempotent = "produce".equals(intent) && Boolean.TRUE.equals(idempotentRequired);
			List<Requirement> reqs = requirements(intent, topic, group, txId, needIdempotent);
			facts.putAll(Results.map("brokerVersion", brokerVersion == null ? null : KafkaVersions.releaseOf(brokerVersion),
					"topicExists", topicExists));
			if ("produce".equals(intent)) {
				facts.put("idempotentWrite", Boolean.TRUE.equals(idempotentRequired)
						? "Broker < 2.8: the idempotent producer needs CLUSTER IDEMPOTENT_WRITE"
						: idempotentRequired == null
								? "Could not determine the broker version: with a broker < 2.8 CLUSTER IDEMPOTENT_WRITE is needed too"
								: "Broker ≥ 2.8 (KIP-679): the idempotent producer does not need CLUSTER IDEMPOTENT_WRITE, topic WRITE is enough");
			}
			if (Boolean.FALSE.equals(topicExists)) {
				if ("admin-topic".equals(intent)) {
					dx.info("TOPIC_NOT_FOUND", "Topic does not exist yet: " + topic + " (creating it needs CREATE)", null, null);
				}
				else {
					dx.warning("TOPIC_NOT_FOUND", "Topic does not exist: " + topic
							+ "; even when authorized, the client gets UnknownTopicOrPartition (if auto.create.topics.enable is off)",
							null, "Create the topic with topics_create or fix the name");
				}
			}
			if (view == null) {
				extra.put("explanation", "No decision because the ACLs could not be read (allowed could not be determined)");
			}
			else {
				evaluate(dx, facts, extra, view, principal, intent, topic, group, txId, host, reqs);
			}
		}
		Map<String, Object> head = Results.map("principal", principal, "intent", intent, "topic", topic, "group", group,
				"transactionalId", txId, "host", host, "cluster", call.cluster(), "mode", mode);
		return dx.result(head, extra, facts);
	}

	private static Boolean topicExists(ToolCall call, String topic) {
		try {
			Topics.describeOne(call, topic);
			return true;
		}
		catch (ToolException e) {
			if (e.getMessage() != null && e.getMessage().startsWith("Topic not found")) {
				return false;
			}
			throw e;
		}
	}

	private static void evaluate(Diagnosis dx, Map<String, Object> facts, Map<String, Object> extra, AclView view,
			String principal, String intent, String topic, String group, String txId, String host,
			List<Requirement> reqs) {
		facts.put("authorizer", view.authorizerEnabled());
		if (view.authorizerClass() != null) {
			facts.put("authorizerClass", view.authorizerClass());
		}
		if (!view.authorizerEnabled()) {
			List<Map<String, Object>> rows = new ArrayList<>();
			reqs.forEach(r -> rows.add(Results.map("resourceType", r.type().name(), "resourceName", r.name(),
					"operation", r.operation().name(), "purpose", r.purpose(), "allowed", true, "reason",
					"No authorizer")));
			extra.put("allowed", true);
			extra.put("explanation", principal + " can do this: no authorizer is configured on the cluster, ACLs are not enforced");
			extra.put("checks", rows);
			extra.put("missingAcls", List.of());
			dx.info("NO_AUTHORIZER", Acls.NO_AUTHORIZER_NOTE, null,
					"If access must be restricted, set authorizer.class.name on the brokers");
			return;
		}
		String authorizer = view.authorizerClass();
		if (authorizer != null && !BUILT_IN_AUTHORIZERS.contains(authorizer)) {
			dx.warning("CUSTOM_AUTHORIZER", "authorizer.class.name=" + authorizer
					+ " is not built in: its extra rules (RBAC, Ranger etc.) were not evaluated", null,
					"Verify the result with the authorizer's own tooling");
		}
		boolean superUser = view.superUsers().contains(principal);
		facts.putAll(Results.map("superUser", superUser, "superUsersKnown", view.superUsers().known(),
				"allowEveryoneIfNoAclFound", view.allowEveryone().value()));
		List<Map<String, Object>> rows = new ArrayList<>();
		List<Map<String, Object>> missing = new ArrayList<>();
		Set<AclBinding> blocking = new LinkedHashSet<>();
		List<String> deniedOps = new ArrayList<>();
		List<String> allowedOps = new ArrayList<>();
		for (Requirement r : reqs) {
			AclEvaluator.Decision d = view.decide(principal, r.operation(), r.type(), r.name(), host);
			boolean allowed = d.allowed();
			String via = null;
			if (!allowed && r.type() == ResourceType.TOPIC && r.operation() == AclOperation.CREATE) {
				AclEvaluator.Decision c = view.decide(principal, AclOperation.CREATE, ResourceType.CLUSTER,
						Acls.CLUSTER_NAME, host);
				if (c.allowed()) {
					allowed = true;
					via = "CLUSTER CREATE (can create all topics)";
					d = c;
				}
			}
			String label = r.type().name() + " '" + r.name() + "' " + r.operation().name();
			rows.add(Results.map("resourceType", r.type().name(), "resourceName", r.name(), "operation",
					r.operation().name(), "purpose", r.purpose(), "allowed", allowed, "reason", d.reason(), "via", via,
					"matchedAllow", d.matchedAllow().isEmpty() ? null : Acls.render(d.matchedAllow().stream().limit(3).toList()),
					"matchedDeny", d.matchedDeny().isEmpty() ? null : Acls.render(d.matchedDeny())));
			if (allowed) {
				allowedOps.add(label);
				continue;
			}
			deniedOps.add(label);
			Map<String, Object> toAdd = null;
			if (!view.decideIgnoringDeny(principal, r.operation(), r.type(), r.name(), host).allowed()) {
				toAdd = binding(principal, r, host);
				missing.add(toAdd);
			}
			if (!d.matchedDeny().isEmpty()) {
				blocking.addAll(d.matchedDeny());
				dx.critical("DENIED_BY_ACL", label + " is explicitly DENIED for " + principal + " (" + r.purpose() + ")",
						evidence("deny", Acls.render(d.matchedDeny()), "allowToAdd", toAdd),
						"DENY always overrides ALLOW: remove this DENY binding with acls_delete"
								+ (toAdd == null ? "" : ", then add the ALLOW binding with acls_create"));
			}
			else {
				dx.critical("MISSING_PERMISSION", principal + " lacks " + label + " (" + r.purpose() + "): "
						+ d.reason(), evidence("binding", toAdd),
						"Add it with acls_create: " + describe(toAdd));
			}
		}
		boolean allowed = deniedOps.isEmpty();
		extra.put("allowed", allowed);
		extra.put("explanation", explanation(principal, intent, topic, allowed, superUser, allowedOps, deniedOps));
		extra.put("checks", rows);
		extra.put("missingAcls", missing);
		if (!blocking.isEmpty()) {
			extra.put("blockingDenies", Acls.render(blocking));
		}
		Map<String, Object> preset = preset(principal, intent, topic, group, txId, host, missing);
		if (preset != null) {
			extra.put("preset", preset);
		}
		if (superUser) {
			dx.info("SUPER_USER", principal + " is a super user (super.users): authorized for every operation regardless of ACLs",
					null, null);
		}
		else if (!view.superUsers().known() && !allowed) {
			dx.info("SUPER_USERS_HIDDEN", "The broker hides the super.users value (Kafka 4.x returns it as sensitive): if "
					+ principal + " is a super user, it is authorized regardless of these checks", null,
					"Verify super.users in the broker config (server.properties/Helm values)");
		}
		if (view.allowEveryone().note() != null) {
			dx.info("ALLOW_EVERYONE_UNKNOWN", view.allowEveryone().note(), null, null);
		}
		if (Acls.WILDCARD.equals(host) && view.hasHostSpecific(principal)) {
			dx.info("HOST_SPECIFIC_ACLS", principal + " has host-specific ACLs; with host=* only ACLs with host * were "
					+ "evaluated", null, "Pass the client IP with the host parameter");
		}
	}

	private static String describe(Map<String, Object> binding) {
		if (binding == null) {
			return "(the existing ALLOW is enough once the DENY is removed)";
		}
		List<String> parts = new ArrayList<>();
		binding.forEach((k, v) -> parts.add(k + "=" + v));
		return String.join(", ", parts);
	}

	private static String explanation(String principal, String intent, String topic, boolean allowed,
			boolean superUser, List<String> allowedOps, List<String> deniedOps) {
		String what = switch (intent) {
			case "produce" -> "produce to topic '" + topic + "'";
			case "consume" -> "consume from topic '" + topic + "'";
			case "transactional-produce" -> "produce transactionally to topic '" + topic + "'";
			default -> "manage topic '" + topic + "' (create/alter/delete)";
		};
		if (superUser) {
			return principal + " can " + what + ": super user";
		}
		if (allowed) {
			return principal + " can " + what + ": all required operations (" + String.join(", ", allowedOps)
					+ ") are allowed by ACLs";
		}
		return principal + " cannot " + what + ": missing/blocked operations: " + String.join(", ", deniedOps)
				+ (allowedOps.isEmpty() ? "" : " (allowed: " + String.join(", ", allowedOps) + ")");
	}

	/** acls_create preset covering all or some of the missing bindings; null if there is no suitable preset. */
	private static Map<String, Object> preset(String principal, String intent, String topic, String group,
			String txId, String host, List<Map<String, Object>> missing) {
		if (missing.isEmpty()) {
			return null;
		}
		boolean idempotent = missing.stream().anyMatch(m -> "IDEMPOTENT_WRITE".equals(m.get("operation")));
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("principal", principal);
		String covers;
		switch (intent) {
			case "produce" -> {
				args.put("preset", "producer");
				args.put("topic", topic);
				covers = "TOPIC WRITE, DESCRIBE";
			}
			case "transactional-produce" -> {
				args.put("preset", "producer");
				args.put("topic", topic);
				args.put("transactionalId", txId);
				covers = "TOPIC WRITE, DESCRIBE + TRANSACTIONAL_ID WRITE, DESCRIBE";
			}
			case "consume" -> {
				args.put("preset", "consumer");
				args.put("topic", topic);
				args.put("group", group);
				covers = "TOPIC READ, DESCRIBE + GROUP READ";
			}
			default -> {
				return Results.map("tool", "acls_create", "note",
						"No preset for admin-topic; create the bindings in missingAcls one by one with acls_create");
			}
		}
		if (!Acls.WILDCARD.equals(host)) {
			args.put("host", host);
		}
		String note = "The preset creates these bindings: " + covers + " (existing ones are rewritten too, which is harmless)";
		if (idempotent) {
			note += "; CLUSTER IDEMPOTENT_WRITE is not part of the preset, create it separately";
		}
		if ("transactional-produce".equals(intent) && group != null) {
			note += "; GROUP READ (offset commit) is not part of the preset, create it separately";
		}
		return Results.map("tool", "acls_create", "args", args, "note", note);
	}

}
