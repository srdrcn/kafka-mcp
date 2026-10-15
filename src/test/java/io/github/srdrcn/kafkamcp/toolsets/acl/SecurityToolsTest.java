package io.github.srdrcn.kafkamcp.toolsets.acl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;
import io.github.srdrcn.kafkamcp.support.TestConfigs;

/** scram_users_list, delegation_tokens_list, security_summary, kerberos_self_check. */
class SecurityToolsTest extends KafkaIntegrationTest {

	@Test
	void scramUsersAreListedWithoutSecrets() throws Exception {
		String user = unique("scram");
		// aynı kullanıcı tek istekte iki kez değiştirilemez: mekanizma başına ayrı istek
		testAdmin()
			.alterUserScramCredentials(List.of(new UserScramCredentialUpsertion(user,
					new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), "very-secret-pw")))
			.all()
			.get(15, TimeUnit.SECONDS);
		testAdmin()
			.alterUserScramCredentials(List.of(new UserScramCredentialUpsertion(user,
					new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 8192), "very-secret-pw")))
			.all()
			.get(15, TimeUnit.SECONDS);
		await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
			JsonNode r = ok("scram_users_list", args("filter", "^" + user + "$"));
			assertThat(r.get("total").asInt()).isEqualTo(1);
			JsonNode u = r.get("users").get(0);
			assertThat(u.get("user").asText()).isEqualTo(user);
			assertThat(u.get("mechanisms")).hasSize(2);
			assertThat(u.at("/mechanisms/0/mechanism").asText()).isEqualTo("SCRAM-SHA-256");
			assertThat(u.at("/mechanisms/0/iterations").asInt()).isEqualTo(4096);
			assertThat(u.at("/mechanisms/1/iterations").asInt()).isEqualTo(8192);
			assertThat(r.toString()).doesNotContain("very-secret-pw").doesNotContainIgnoringCase("salt");
		});
	}

	@Test
	void delegationTokensAreListedWithoutHmac() throws Exception {
		DelegationToken token = testAdmin().createDelegationToken().delegationToken().get(15, TimeUnit.SECONDS);
		String id = token.tokenInfo().tokenId();
		await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
			JsonNode r = ok("delegation_tokens_list", args("filter", id));
			assertThat(r.get("enabled").asBoolean()).isTrue();
			assertThat(r.get("total").asInt()).isEqualTo(1);
			JsonNode t = r.get("tokens").get(0);
			assertThat(t.get("tokenId").asText()).isEqualTo(id);
			assertThat(t.get("owner").asText()).isEqualTo("User:admin");
			assertThat(t.get("tokenRequester").asText()).isEqualTo("User:admin");
			assertThat(t.get("expired").asBoolean()).isFalse();
			assertThat(t.get("expiryTimestamp").asText()).endsWith("Z");
			assertThat(t.get("maxTimestamp").asText()).isNotBlank();
			assertThat(t.has("hmac")).isFalse();
			assertThat(r.toString()).doesNotContain(token.hmacAsBase64String());
		});
	}

	@Test
	void securitySummaryShowsBrokerAndClientSecurity() {
		JsonNode r = ok("security_summary", args());
		assertThat(r.at("/broker/authorizer.class.name").asText()).contains("StandardAuthorizer");
		assertThat(r.at("/broker/listeners").toString()).contains("SASL", "INTERNAL").doesNotContain("9092");
		assertThat(r.at("/broker/sasl.enabled.mechanisms").asText()).isEqualTo("PLAIN");
		assertThat(r.at("/broker/listener.security.protocol.map").asText()).contains("SASL:SASL_PLAINTEXT");
		assertThat(r.at("/broker/super.users").asText()).contains("gizli");
		assertThat(r.at("/acls/total").isInt()).isTrue();
		assertThat(r.at("/scram/users").isInt()).isTrue();
		assertThat(r.at("/quotas/entities").isInt()).isTrue();
		assertThat(r.at("/delegationTokens/count").isInt()).isTrue();
		assertThat(r.at("/client/securityProtocol").asText()).isEqualTo("SASL_PLAINTEXT");
		assertThat(r.at("/client/saslMechanism").asText()).isEqualTo("PLAIN");
		assertThat(r.get("notes").toString()).contains("super.users");
		assertThat(r.toString()).doesNotContain("admin-secret");
	}

	@Test
	void kerberosSelfCheckReportsNotConfigured() {
		JsonNode r = ok("kerberos_self_check", args());
		assertThat(r.get("kerberosConfigured").asBoolean()).isFalse();
		assertThat(r.get("note").asText()).contains("Kerberos yapılandırılmamış", "SASL_PLAINTEXT", "PLAIN");
		assertThat(r.at("/client/saslJaasConfig").asText()).contains("password=\"****\"").contains("username=\"admin\"");
		assertThat(r.toString()).doesNotContain("admin-secret");
		assertThat(r.has("login")).isFalse();
		assertThat(r.at("/kafka/status").asText()).isEqualTo("ok");
		assertThat(r.at("/kafka/clusterId").asText()).isNotBlank();
		// readOnly cluster'da da çalışır, cluster zorunlu
		assertThat(ok("kerberos_self_check", args("cluster", TestConfigs.READ_ONLY_CLUSTER)).get("cluster").asText())
			.isEqualTo(TestConfigs.READ_ONLY_CLUSTER);
		assertThat(error("kerberos_self_check", Map.of())).contains("cluster");
	}

}
