package io.github.srdrcn.kafkamcp.toolsets.acl;

import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.allowTopic;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.binding;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.create;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.createTopic;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.delete;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.denyTopic;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.field;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.find;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;

/** acls_explain, acls_who_can_access_topic, acls_principal_permissions. */
class AclExplainTest extends KafkaIntegrationTest {

	private JsonNode explain(String principal, String op, String type, String name, Object... extra) {
		Object[] kv = new Object[8 + extra.length];
		Object[] base = { "principal", principal, "operation", op, "resourceType", type, "resourceName", name };
		System.arraycopy(base, 0, kv, 0, 8);
		System.arraycopy(extra, 0, kv, 8, extra.length);
		return ok("acls_explain", args(kv));
	}

	@Test
	void literalAllowDenyWinsAndImplication() throws Exception {
		String p = "User:" + unique("ex");
		String topic = unique("ex-topic");
		create(testAdmin(), allowTopic(topic, p, AclOperation.READ), allowTopic(topic, p, AclOperation.WRITE));

		JsonNode read = explain(p, "READ", "TOPIC", topic);
		assertThat(read.get("decision").asText()).isEqualTo("ALLOW");
		assertThat(read.get("superUser").asBoolean()).isFalse();
		assertThat(read.get("matchedAllow")).hasSize(1);
		assertThat(read.at("/matchedAllow/0/operation").asText()).isEqualTo("READ");
		assertThat(read.at("/matchedAllow/0/principal").asText()).isEqualTo(p);
		assertThat(read.get("resourceAclCount").asInt()).isEqualTo(2);

		// WRITE → DESCRIBE çıkarımı; CREATE izni yok
		JsonNode describe = explain(p, "DESCRIBE", "TOPIC", topic);
		assertThat(describe.get("decision").asText()).isEqualTo("ALLOW");
		assertThat(describe.get("reason").asText()).contains("çıkarıldı");
		JsonNode createOp = explain(p, "CREATE", "TOPIC", topic);
		assertThat(createOp.get("decision").asText()).isEqualTo("DENY");
		assertThat(createOp.get("reason").asText()).contains("eşleşen ALLOW yok");

		// DENY her zaman kazanır
		create(testAdmin(), denyTopic(topic, p, AclOperation.READ));
		JsonNode denied = explain(p, "READ", "TOPIC", topic);
		assertThat(denied.get("decision").asText()).isEqualTo("DENY");
		assertThat(denied.get("matchedDeny")).hasSize(1);
		assertThat(denied.get("matchedAllow")).hasSize(1);
		assertThat(denied.get("reason").asText()).contains("DENY");
		// principal sadece adla da verilebilir
		assertThat(explain(p.substring("User:".length()), "WRITE", "TOPIC", topic).get("decision").asText())
			.isEqualTo("ALLOW");
	}

	@Test
	void prefixedWildcardResourceAndWildcardPrincipal() throws Exception {
		String p = "User:" + unique("wc");
		String prefix = unique("pfx") + "-";
		String shared = unique("shared");
		AclBinding wildcardResource = binding(ResourceType.TOPIC, "*", PatternType.LITERAL, p,
				AclOperation.DESCRIBE_CONFIGS, AclPermissionType.ALLOW);
		create(testAdmin(),
				binding(ResourceType.TOPIC, prefix, PatternType.PREFIXED, p, AclOperation.READ, AclPermissionType.ALLOW),
				allowTopic(shared, "User:*", AclOperation.READ), wildcardResource);
		try {
			JsonNode prefixed = explain(p, "READ", "TOPIC", prefix + "orders");
			assertThat(prefixed.get("decision").asText()).isEqualTo("ALLOW");
			assertThat(prefixed.at("/matchedAllow/0/patternType").asText()).isEqualTo("PREFIXED");
			assertThat(explain(p, "READ", "TOPIC", "x" + prefix).get("decision").asText()).isEqualTo("DENY");

			JsonNode wildcard = explain(p, "DESCRIBE_CONFIGS", "TOPIC", unique("any"));
			assertThat(wildcard.get("decision").asText()).isEqualTo("ALLOW");
			assertThat(wildcard.at("/matchedAllow/0/resourceName").asText()).isEqualTo("*");

			JsonNode everyone = explain("User:" + unique("nobody"), "READ", "TOPIC", shared);
			assertThat(everyone.get("decision").asText()).isEqualTo("ALLOW");
			assertThat(everyone.at("/matchedAllow/0/principal").asText()).isEqualTo("User:*");
			assertThat(explain("User:" + unique("nobody"), "WRITE", "TOPIC", shared).get("decision").asText())
				.isEqualTo("DENY");
		}
		finally {
			delete(testAdmin(), wildcardResource);
		}
	}

	@Test
	void superUserNoAclAndHost() throws Exception {
		String topic = unique("su-topic");
		// Kafka 4.x super.users'ı sensitive döndürür: bilinmiyorsa not düşülür
		JsonNode admin = explain("User:admin", "WRITE", "TOPIC", topic);
		assertThat(admin.get("decision").asText()).isEqualTo("DENY");
		assertThat(admin.get("reason").asText()).contains("allow.everyone.if.no.acl.found=false");
		assertThat(admin.get("allowEveryoneIfNoAclFound").asBoolean()).isFalse();
		assertThat(admin.get("resourceAclCount").asInt()).isZero();
		assertThat(admin.get("notes").toString()).contains("super.users").contains("allow.everyone.if.no.acl.found");

		JsonNode su = explain("User:admin", "WRITE", "TOPIC", topic, "superUsers", List.of("User:admin"));
		assertThat(su.get("decision").asText()).isEqualTo("ALLOW");
		assertThat(su.get("superUser").asBoolean()).isTrue();
		assertThat(su.get("reason").asText()).contains("süper kullanıcı");

		String p = "User:" + unique("host");
		create(testAdmin(), binding(ResourceType.TOPIC, topic, PatternType.LITERAL, p, "10.1.2.3", AclOperation.READ,
				AclPermissionType.ALLOW));
		JsonNode anyHost = explain(p, "READ", "TOPIC", topic);
		assertThat(anyHost.get("decision").asText()).isEqualTo("DENY");
		assertThat(anyHost.get("notes").toString()).contains("host");
		assertThat(explain(p, "READ", "TOPIC", topic, "host", "10.1.2.3").get("decision").asText())
			.isEqualTo("ALLOW");

		assertThat(error("acls_explain", args("principal", p, "operation", "ANY", "resourceType", "TOPIC",
				"resourceName", topic)))
			.contains("Geçersiz operation");
		assertThat(error("acls_explain", args("principal", p, "operation", "READ", "resourceType", "TOPIC")))
			.contains("resourceName");
	}

	@Test
	void whoCanAccessTopic() throws Exception {
		String topic = unique("who");
		createTopic(testAdmin(), topic);
		String reader = "User:" + unique("reader");
		String writer = "User:" + unique("writer");
		String blocked = "User:" + unique("blocked");
		String noWrite = "User:" + unique("nowrite");
		create(testAdmin(), allowTopic(topic, reader, AclOperation.READ),
				binding(ResourceType.TOPIC, topic.substring(0, 5), PatternType.PREFIXED, writer, AclOperation.WRITE,
						AclPermissionType.ALLOW),
				allowTopic(topic, blocked, AclOperation.READ), denyTopic(topic, blocked, AclOperation.READ),
				denyTopic(topic, noWrite, AclOperation.WRITE));

		JsonNode r = ok("acls_who_can_access_topic", args("topic", topic));
		assertThat(r.get("topicExists").asBoolean()).isTrue();
		assertThat(field(r.at("/operations/READ/allowed"), "principal")).contains(reader).doesNotContain(blocked, writer);
		assertThat(field(r.at("/operations/READ/denied"), "principal")).contains(blocked);
		assertThat(field(r.at("/operations/WRITE/allowed"), "principal")).contains(writer).doesNotContain(reader);
		assertThat(field(r.at("/operations/WRITE/denied"), "principal")).contains(noWrite);
		JsonNode readerEntry = find(r.at("/operations/READ/allowed"), "principal", reader);
		assertThat(readerEntry.at("/via/0/operation").asText()).isEqualTo("READ");

		// DESCRIBE: READ/WRITE'tan çıkarılır; DENY READ, DESCRIBE'ı engellemez
		JsonNode describe = ok("acls_who_can_access_topic", args("topic", topic, "operation", "DESCRIBE",
				"superUsers", List.of("User:admin")));
		assertThat(describe.get("operations").has("READ")).isFalse();
		assertThat(field(describe.at("/operations/DESCRIBE/allowed"), "principal")).contains(reader, writer, blocked);
		assertThat(describe.get("superUsers").toString()).contains("User:admin");

		// host'a özel DENY, host * ALLOW'u olan principal'da da görünür
		String hostDenied = "User:" + unique("hostdeny");
		create(testAdmin(), allowTopic(topic, hostDenied, AclOperation.READ), binding(ResourceType.TOPIC, topic,
				PatternType.LITERAL, hostDenied, "10.0.0.1", AclOperation.READ, AclPermissionType.DENY));
		JsonNode hostRead = ok("acls_who_can_access_topic", args("topic", topic, "operation", "READ"));
		JsonNode partly = find(hostRead.at("/operations/READ/allowed"), "principal", hostDenied);
		assertThat(partly.get("hosts").toString()).isEqualTo("[\"*\"]");
		assertThat(partly.get("deniedHosts").toString()).isEqualTo("[\"10.0.0.1\"]");
		assertThat(partly.at("/deniedVia/0/permission").asText()).isEqualTo("DENY");
		JsonNode deniedEntry = find(hostRead.at("/operations/READ/denied"), "principal", hostDenied);
		assertThat(deniedEntry.get("hosts").toString()).isEqualTo("[\"10.0.0.1\"]");
		assertThat(find(hostRead.at("/operations/READ/allowed"), "principal", reader).has("deniedHosts")).isFalse();

		JsonNode missing = ok("acls_who_can_access_topic", args("topic", unique("no-such-topic")));
		assertThat(missing.get("topicExists").asBoolean()).isFalse();
		assertThat(missing.get("aclCount").asInt()).isZero();
		assertThat(missing.get("notes").toString()).contains("hiç ACL yok");
	}

	@Test
	void principalPermissionsGroupsByResource() throws Exception {
		String p = "User:" + unique("perm");
		String topic = unique("perm-topic");
		String group = unique("perm-group");
		String prefix = unique("perm-app") + "-";
		String everyoneTopic = unique("perm-everyone");
		create(testAdmin(), allowTopic(topic, p, AclOperation.READ), denyTopic(topic, p, AclOperation.WRITE),
				binding(ResourceType.GROUP, group, PatternType.LITERAL, p, AclOperation.READ, AclPermissionType.ALLOW),
				binding(ResourceType.TOPIC, prefix, PatternType.PREFIXED, p, AclOperation.ALL, AclPermissionType.ALLOW),
				allowTopic(everyoneTopic, "User:*", AclOperation.READ));

		JsonNode own = ok("acls_principal_permissions", args("principal", p, "includeWildcardPrincipal", false));
		assertThat(own.get("total").asInt()).isEqualTo(3);
		assertThat(own.at("/summary/bindings").asInt()).isEqualTo(4);
		assertThat(own.at("/summary/deny").asInt()).isEqualTo(1);
		assertThat(own.at("/summary/byResourceType/TOPIC").asInt()).isEqualTo(2);
		JsonNode topicRow = find(own.get("resources"), "resourceName", topic);
		assertThat(topicRow.get("effectiveAllow").toString()).isEqualTo("[\"READ\",\"DESCRIBE\"]");
		assertThat(topicRow.get("deny").toString()).isEqualTo("[\"WRITE\"]");
		assertThat(topicRow.get("bindings")).hasSize(2);
		JsonNode prefixRow = find(own.get("resources"), "resourceName", prefix);
		assertThat(prefixRow.get("patternType").asText()).isEqualTo("PREFIXED");
		assertThat(prefixRow.get("effectiveAllow").toString()).isEqualTo("[\"ALL\"]");

		JsonNode withWildcard = ok("acls_principal_permissions", args("principal", p, "filter", everyoneTopic));
		assertThat(withWildcard.get("includeWildcardPrincipal").asBoolean()).isTrue();
		assertThat(withWildcard.get("total").asInt()).isEqualTo(1);
		assertThat(withWildcard.at("/resources/0/bindings/0/principal").asText()).isEqualTo("User:*");

		// Başka desenden gelen DENY/ALLOW (PREFIXED) LITERAL satırın etkin izinlerine yansır; acls_explain ile aynı
		String p2 = "User:" + unique("perm2");
		String pfx = unique("perm-pay");
		String payments = pfx + "ments";
		create(testAdmin(), allowTopic(payments, p2, AclOperation.READ),
				binding(ResourceType.TOPIC, pfx, PatternType.PREFIXED, p2, AclOperation.READ, AclPermissionType.DENY),
				binding(ResourceType.TOPIC, pfx, PatternType.PREFIXED, p2, AclOperation.WRITE,
						AclPermissionType.ALLOW));
		JsonNode cross = ok("acls_principal_permissions", args("principal", p2, "includeWildcardPrincipal", false));
		JsonNode paymentsRow = find(cross.get("resources"), "resourceName", payments);
		assertThat(paymentsRow.get("allow").toString()).isEqualTo("[\"READ\"]");
		assertThat(paymentsRow.get("effectiveAllow").toString()).isEqualTo("[\"WRITE\",\"DESCRIBE\"]");
		assertThat(paymentsRow.at("/deniedBy/0/patternType").asText()).isEqualTo("PREFIXED");
		assertThat(paymentsRow.at("/deniedBy/0/operation").asText()).isEqualTo("READ");
		assertThat(paymentsRow.get("alsoAllowedBy").toString()).contains("PREFIXED:" + pfx);
		assertThat(explain(p2, "READ", "TOPIC", payments).get("decision").asText()).isEqualTo("DENY");
		assertThat(explain(p2, "WRITE", "TOPIC", payments).get("decision").asText()).isEqualTo("ALLOW");
		assertThat(explain(p2, "DESCRIBE", "TOPIC", payments).get("decision").asText()).isEqualTo("ALLOW");
		JsonNode pfxRow = find(cross.get("resources"), "resourceName", pfx);
		assertThat(pfxRow.has("deniedBy")).isFalse();
		assertThat(pfxRow.get("effectiveAllow").toString()).isEqualTo("[\"WRITE\",\"DESCRIBE\"]");

		JsonNode none = ok("acls_principal_permissions",
				args("principal", unique("ghost"), "includeWildcardPrincipal", false));
		assertThat(none.get("total").asInt()).isZero();
		assertThat(none.get("notes").toString()).contains("ACL yok");
	}

}
