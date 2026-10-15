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
 * diagnose_access: principal'ın bir amaç (produce, consume, transactional-produce, admin-topic) için gereken
 * her işlemi yapıp yapamayacağını {@link AclEvaluator} ile değerlendirir; eksikler için acls_create'e doğrudan
 * verilebilecek binding nesneleri ve uygun preset'i önerir.
 */
@Component
public class DiagnoseAccessTools {

	public static final List<String> INTENTS = List.of("produce", "consume", "transactional-produce", "admin-topic");

	/** Yerleşik authorizer'lar (kuralları AclEvaluator ile aynı). */
	public static final List<String> BUILT_IN_AUTHORIZERS = List.of(
			"org.apache.kafka.metadata.authorizer.StandardAuthorizer", "kafka.security.authorizer.AclAuthorizer",
			"kafka.security.auth.SimpleAclAuthorizer");

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("diagnose_access",
					"Principal bir amaç (produce/consume/transactional-produce/admin-topic) için yetkili mi; eksik ACL'leri acls_create biçiminde önerir")
			.required("principal", P.string("Principal (ör. User:alice; sadece ad verilirse User: eklenir)"))
			.required("intent", P.enumOf("Amaç", INTENTS))
			.required("topic", P.string("Topic adı"))
			.param("group", P.string("Consumer group (consume için zorunlu; transactional-produce'ta offset commit için)"))
			.param("transactionalId", P.string("transactional.id (transactional-produce için zorunlu)"))
			.param("host", P.string("İstemci IP'si (varsayılan *: sadece host'u * olan ACL'ler eşleşir)"))
			.handler(this::diagnose));
	}

	/** Amacın gerektirdiği tek işlem. */
	public record Requirement(ResourceType type, String name, AclOperation operation, String purpose) {
	}

	/**
	 * Amacın gerektirdiği işlemler. idempotentWrite: broker &lt; 2.8 ise produce için CLUSTER IDEMPOTENT_WRITE
	 * gerekir (KIP-679 öncesi).
	 */
	public static List<Requirement> requirements(String intent, String topic, String group, String transactionalId,
			boolean idempotentWrite) {
		List<Requirement> out = new ArrayList<>();
		switch (intent) {
			case "produce" -> {
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.WRITE, "Produce isteği"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.DESCRIBE, "Metadata (topic'i görmek)"));
				if (idempotentWrite) {
					out.add(new Requirement(ResourceType.CLUSTER, Acls.CLUSTER_NAME, AclOperation.IDEMPOTENT_WRITE,
							"Idempotent producer (InitProducerId), broker < 2.8"));
				}
			}
			case "consume" -> {
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.READ, "Fetch (mesaj okuma)"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.DESCRIBE, "Metadata/ListOffsets"));
				out.add(new Requirement(ResourceType.GROUP, group, AclOperation.READ,
						"JoinGroup/OffsetCommit/OffsetFetch"));
			}
			case "transactional-produce" -> {
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.WRITE, "Produce isteği"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.DESCRIBE, "Metadata (topic'i görmek)"));
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
						"CreateTopics (CLUSTER CREATE da yeterli)"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.ALTER, "CreatePartitions"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.DELETE, "DeleteTopics/DeleteRecords"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.DESCRIBE_CONFIGS, "DescribeConfigs"));
				out.add(new Requirement(ResourceType.TOPIC, topic, AclOperation.ALTER_CONFIGS,
						"AlterConfigs/IncrementalAlterConfigs"));
			}
			default -> throw new ToolException("Geçersiz intent: " + intent + " (geçerli: " + String.join(", ", INTENTS) + ")");
		}
		return out;
	}

	/** acls_create parametre biçiminde ALLOW binding'i. */
	public static Map<String, Object> binding(String principal, Requirement r, String host) {
		return Results.map("principal", principal, "resourceType", r.type().name(), "resourceName", r.name(),
				"patternType", "LITERAL", "operation", r.operation().name(), "permission", "ALLOW", "host", host);
	}

	/** "3.9-IV0" gibi sürüm 2.8'den eski mi? Ayrıştırılamazsa null. */
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
			throw new ToolException("Geçersiz intent: " + intent + " (geçerli: " + String.join(", ", INTENTS) + ")");
		}
		String topic = call.str("topic");
		String group = call.str("group", null);
		String txId = call.str("transactionalId", null);
		String host = call.str("host", Acls.WILDCARD);
		if ("consume".equals(intent) && group == null) {
			throw new ToolException("intent=consume için group zorunlu");
		}
		if ("transactional-produce".equals(intent) && txId == null) {
			throw new ToolException("intent=transactional-produce için transactionalId zorunlu");
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
						? "Broker < 2.8: idempotent producer için CLUSTER IDEMPOTENT_WRITE gerekir"
						: idempotentRequired == null
								? "Broker sürümü belirlenemedi: broker < 2.8 ise CLUSTER IDEMPOTENT_WRITE de gerekir"
								: "Broker ≥ 2.8 (KIP-679): idempotent producer için CLUSTER IDEMPOTENT_WRITE gerekmez, topic WRITE yeterli");
			}
			if (Boolean.FALSE.equals(topicExists)) {
				if ("admin-topic".equals(intent)) {
					dx.info("TOPIC_NOT_FOUND", "Topic henüz yok: " + topic + " (oluşturma için CREATE gerekir)", null, null);
				}
				else {
					dx.warning("TOPIC_NOT_FOUND", "Topic yok: " + topic
							+ "; yetki olsa da istemci UnknownTopicOrPartition alır (auto.create.topics.enable kapalıysa)",
							null, "Topic'i topics_create ile oluşturun ya da adı düzeltin");
				}
			}
			if (view == null) {
				extra.put("explanation", "ACL'ler okunamadığı için karar verilemedi (allowed belirlenemedi)");
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
			if (e.getMessage() != null && e.getMessage().startsWith("Topic bulunamadı")) {
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
					"Authorizer yok")));
			extra.put("allowed", true);
			extra.put("explanation", principal + " bu işlemi yapabilir: cluster'da authorizer tanımlı değil, ACL'ler uygulanmıyor");
			extra.put("checks", rows);
			extra.put("missingAcls", List.of());
			dx.info("NO_AUTHORIZER", Acls.NO_AUTHORIZER_NOTE, null,
					"Erişim kısıtlanmalıysa broker'da authorizer.class.name tanımlayın");
			return;
		}
		String authorizer = view.authorizerClass();
		if (authorizer != null && !BUILT_IN_AUTHORIZERS.contains(authorizer)) {
			dx.warning("CUSTOM_AUTHORIZER", "authorizer.class.name=" + authorizer
					+ " yerleşik değil: ek kuralları (RBAC, Ranger vb.) değerlendirilmedi", null,
					"Sonucu authorizer'ın kendi aracıyla doğrulayın");
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
					via = "CLUSTER CREATE (tüm topic'leri oluşturabilir)";
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
				dx.critical("DENIED_BY_ACL", principal + " için " + label + " açıkça DENY edilmiş (" + r.purpose() + ")",
						evidence("deny", Acls.render(d.matchedDeny()), "allowToAdd", toAdd),
						"DENY her zaman ALLOW'u ezer: acls_delete ile bu DENY binding'ini kaldırın"
								+ (toAdd == null ? "" : ", ardından acls_create ile ALLOW binding'ini ekleyin"));
			}
			else {
				dx.critical("MISSING_PERMISSION", principal + " için " + label + " izni yok (" + r.purpose() + "): "
						+ d.reason(), evidence("binding", toAdd),
						"acls_create ile ekleyin: " + describe(toAdd));
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
			dx.info("SUPER_USER", principal + " süper kullanıcı (super.users): ACL'lerden bağımsız her işleme yetkili",
					null, null);
		}
		else if (!view.superUsers().known() && !allowed) {
			dx.info("SUPER_USERS_HIDDEN", "Broker super.users değerini gizliyor (Kafka 4.x sensitive döndürür): "
					+ principal + " süper kullanıcıysa bu kontrollerden bağımsız olarak yetkilidir", null,
					"super.users'ı broker config'inden (server.properties/Helm values) doğrulayın");
		}
		if (view.allowEveryone().note() != null) {
			dx.info("ALLOW_EVERYONE_UNKNOWN", view.allowEveryone().note(), null, null);
		}
		if (Acls.WILDCARD.equals(host) && view.hasHostSpecific(principal)) {
			dx.info("HOST_SPECIFIC_ACLS", principal + " için host'a özel ACL'ler var; host=* ile sadece host'u * olanlar "
					+ "değerlendirildi", null, "İstemci IP'sini host parametresiyle verin");
		}
	}

	private static String describe(Map<String, Object> binding) {
		if (binding == null) {
			return "(DENY kaldırılınca mevcut ALLOW yeterli)";
		}
		List<String> parts = new ArrayList<>();
		binding.forEach((k, v) -> parts.add(k + "=" + v));
		return String.join(", ", parts);
	}

	private static String explanation(String principal, String intent, String topic, boolean allowed,
			boolean superUser, List<String> allowedOps, List<String> deniedOps) {
		String what = switch (intent) {
			case "produce" -> "'" + topic + "' topic'ine üretim (produce)";
			case "consume" -> "'" + topic + "' topic'inden tüketim (consume)";
			case "transactional-produce" -> "'" + topic + "' topic'ine transaction'lı üretim";
			default -> "'" + topic + "' topic'ini yönetme (oluşturma/değiştirme/silme)";
		};
		if (superUser) {
			return principal + " " + what + " yapabilir: süper kullanıcı";
		}
		if (allowed) {
			return principal + " " + what + " yapabilir: gereken tüm işlemler (" + String.join(", ", allowedOps)
					+ ") ACL'lerle izinli";
		}
		return principal + " " + what + " yapamaz: eksik/engellenen işlemler: " + String.join(", ", deniedOps)
				+ (allowedOps.isEmpty() ? "" : " (izinli olanlar: " + String.join(", ", allowedOps) + ")");
	}

	/** Eksik binding'lerin hepsini ya da bir kısmını kapsayan acls_create preset'i; uygun preset yoksa null. */
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
						"admin-topic için preset yok; missingAcls'teki binding'leri tek tek acls_create ile oluşturun");
			}
		}
		if (!Acls.WILDCARD.equals(host)) {
			args.put("host", host);
		}
		String note = "Preset bu binding'leri oluşturur: " + covers + " (eksik olmayanlar da yeniden yazılır, zararsız)";
		if (idempotent) {
			note += "; CLUSTER IDEMPOTENT_WRITE preset'e dahil değil, ayrıca oluşturun";
		}
		if ("transactional-produce".equals(intent) && group != null) {
			note += "; GROUP READ (offset commit) preset'e dahil değil, ayrıca oluşturun";
		}
		return Results.map("tool", "acls_create", "args", args, "note", note);
	}

}
