package io.github.srdrcn.kafkamcp.toolsets.acl;

import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.allowTopic;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.binding;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.create;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.createTopic;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.delete;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.deleteAllFor;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.denyTopic;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.field;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolRegistry;
import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;

/** acls_list ve acls_audit. */
class AclListAuditTest extends KafkaIntegrationTest {

	@Test
	void listFiltersByAllFields() throws Exception {
		String p = "User:" + unique("lst");
		String topic = unique("lst-topic");
		String group = unique("lst-group");
		create(testAdmin(), allowTopic(topic, p, AclOperation.READ), denyTopic(topic, p, AclOperation.WRITE),
				binding(ResourceType.GROUP, group, PatternType.LITERAL, p, AclOperation.READ, AclPermissionType.ALLOW),
				binding(ResourceType.TOPIC, topic + "-", PatternType.PREFIXED, p, "10.0.0.7", AclOperation.DESCRIBE,
						AclPermissionType.ALLOW));

		JsonNode all = ok("acls_list", args("principal", p));
		assertThat(all.get("total").asInt()).isEqualTo(4);
		assertThat(all.get("truncated").asBoolean()).isFalse();
		JsonNode first = all.get("acls").get(0);
		assertThat(first.get("principal").asText()).isEqualTo(p);
		assertThat(first.has("host")).isTrue();
		assertThat(first.has("patternType")).isTrue();
		assertThat(first.get("resourceType").asText()).isEqualTo("GROUP");

		assertThat(ok("acls_list", args("principal", p, "resourceType", "GROUP")).get("total").asInt()).isEqualTo(1);
		assertThat(ok("acls_list", args("principal", p, "permission", "DENY")).at("/acls/0/operation").asText())
			.isEqualTo("WRITE");
		assertThat(ok("acls_list", args("principal", p, "patternType", "PREFIXED")).at("/acls/0/host").asText())
			.isEqualTo("10.0.0.7");
		assertThat(ok("acls_list", args("principal", p, "host", "10.0.0.7")).get("total").asInt()).isEqualTo(1);
		assertThat(ok("acls_list", args("principal", p, "operation", "READ")).get("total").asInt()).isEqualTo(2);
		assertThat(ok("acls_list", args("resourceType", "TOPIC", "resourceName", topic)).get("total").asInt())
			.isEqualTo(2);
		// MATCH: topic adı prefix'le de eşleşir
		assertThat(ok("acls_list", args("resourceType", "TOPIC", "resourceName", topic + "-x", "patternType", "MATCH"))
			.get("acls")
			.findValuesAsText("patternType")).contains("PREFIXED");
		// filter: principal ya da kaynak adı üzerinde regex
		JsonNode filtered = ok("acls_list", args("filter", "^" + group + "$", "limit", 5));
		assertThat(filtered.get("total").asInt()).isEqualTo(1);
		JsonNode limited = ok("acls_list", args("principal", p, "limit", 1));
		assertThat(limited.get("returned").asInt()).isEqualTo(1);
		assertThat(limited.get("truncated").asBoolean()).isTrue();

		assertThat(error("acls_list", args("resourceType", "FOO"))).contains("Geçersiz resourceType");
		assertThat(error("acls_list", args("filter", "(["))).contains("Geçersiz regex");
	}

	@Test
	void auditFindsRiskyAndOrphanAcls() throws Exception {
		String tag = unique("aud");
		String user = "User:" + tag + "-u";
		String existing = tag + "-existing";
		createTopic(testAdmin(), existing);
		AclBinding everyone = allowTopic(tag + "-open", "User:*", AclOperation.READ);
		AclBinding[] mine = {
				binding(ResourceType.TOPIC, "*", PatternType.LITERAL, user, AclOperation.READ, AclPermissionType.ALLOW),
				allowTopic(existing, user, AclOperation.ALL),
				binding(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL, user, AclOperation.DESCRIBE,
						AclPermissionType.ALLOW),
				allowTopic(tag + "-missing", user, AclOperation.READ),
				binding(ResourceType.GROUP, tag + "-nogroup", PatternType.LITERAL, user, AclOperation.READ,
						AclPermissionType.ALLOW),
				binding(ResourceType.TOPIC, tag + "-nomatch-", PatternType.PREFIXED, user, AclOperation.READ,
						AclPermissionType.ALLOW),
				binding(ResourceType.TOPIC, tag + "-exist", PatternType.PREFIXED, user, AclOperation.WRITE,
						AclPermissionType.ALLOW) };
		create(testAdmin(), everyone);
		create(testAdmin(), mine);
		try {
			JsonNode r = ok("acls_audit", args("filter", tag));
			assertThat(r.get("scannedAcls").asInt()).isEqualTo(8);
			assertThat(r.get("totalAcls").asInt()).isGreaterThanOrEqualTo(8);
			assertThat(r.get("topicCount").asInt()).isPositive();
			JsonNode c = r.get("categories");
			assertThat(c.at("/allowAllPrincipals/count").asInt()).isEqualTo(1);
			assertThat(c.at("/allowAllPrincipals/samples/0/principal").asText()).isEqualTo("User:*");
			assertThat(c.at("/allowWildcardResource/count").asInt()).isEqualTo(1);
			assertThat(c.at("/allowOperationAll/count").asInt()).isEqualTo(1);
			assertThat(c.at("/allowOperationAll/samples/0/resourceName").asText()).isEqualTo(existing);
			assertThat(c.at("/allowOnCluster/count").asInt()).isEqualTo(1);
			assertThat(field(c.at("/orphanTopicAcls/samples"), "resourceName")).containsExactlyInAnyOrder(
					tag + "-open", tag + "-missing");
			assertThat(c.at("/orphanGroupAcls/samples/0/resourceName").asText()).isEqualTo(tag + "-nogroup");
			assertThat(field(c.at("/unusedPrefixedTopicAcls/samples"), "resourceName"))
				.containsExactly(tag + "-nomatch-");
			assertThat(c.at("/orphanTopicAcls/description").asText()).isNotBlank();
			// super.users gizli → not düşülür
			assertThat(r.get("notes").toString()).contains("super.users");

			assertThat(c.at("/unverifiedTopicAcls/count").asInt()).isZero();
			assertThat(r.get("notes").toString()).contains("DESCRIBE yetkisiyle sınırlı");

			JsonNode limited = ok("acls_audit", args("filter", tag, "limit", 1));
			assertThat(limited.at("/categories/orphanTopicAcls/count").asInt()).isEqualTo(2);
			assertThat(limited.at("/categories/orphanTopicAcls/samples")).hasSize(1);
			assertThat(limited.at("/categories/orphanTopicAcls/truncated").asBoolean()).isTrue();
		}
		finally {
			deleteAllFor(testAdmin(), user);
			delete(testAdmin(), everyone);
		}
	}

	/**
	 * Sunucunun Kafka kullanıcısı (bob) sadece bazı topic'leri görebiliyor: göremediği var olan topic'in ACL'i
	 * sahipsiz sayılmaz (doğrulanamadı), görebildiği ama olmayan topic'inki sahipsiz kalır.
	 */
	@Test
	void auditDoesNotReportTopicsInvisibleToServerAsOrphans() throws Exception {
		String tag = unique("audlp");
		String user = "User:" + tag + "-u";
		String hidden = tag + "-hidden";
		String visibleGone = tag + "-vis-gone";
		createTopic(testAdmin(), hidden);
		AclBinding bobCluster = binding(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL, "User:bob",
				AclOperation.DESCRIBE, AclPermissionType.ALLOW);
		AclBinding bobTopics = binding(ResourceType.TOPIC, tag + "-vis-", PatternType.PREFIXED, "User:bob",
				AclOperation.DESCRIBE, AclPermissionType.ALLOW);
		AclBinding onHidden = allowTopic(hidden, user, AclOperation.READ);
		AclBinding onGone = allowTopic(visibleGone, user, AclOperation.READ);
		create(testAdmin(), bobCluster, bobTopics, onHidden, onGone);
		KafkaMcpConfig config = new KafkaMcpConfig();
		ClusterConfig cluster = new ClusterConfig();
		cluster.setName("as-bob");
		cluster.setBootstrapServers(KafkaTestCluster.bootstrap());
		cluster.setProperties(KafkaTestCluster.saslProperties("bob"));
		config.setClusters(List.of(cluster));
		KafkaClients clients = new KafkaClients(config);
		try {
			ToolCall call = new ToolCall("acls_audit", Map.of("cluster", "as-bob", "filter", tag), clients,
					Instant.now().plusMillis(KafkaClients.TIMEOUT_MS));
			JsonNode r = ToolRegistry.resultMapper().valueToTree(new AclListTools().audit(call));
			JsonNode c = r.get("categories");
			assertThat(field(c.at("/orphanTopicAcls/samples"), "resourceName")).containsExactly(visibleGone);
			assertThat(field(c.at("/unverifiedTopicAcls/samples"), "resourceName")).containsExactly(hidden);
			assertThat(c.at("/unverifiedTopicAcls/description").asText()).contains("DESCRIBE");
		}
		finally {
			clients.destroy();
			delete(testAdmin(), bobCluster, bobTopics);
			deleteAllFor(testAdmin(), user);
		}
	}

}
