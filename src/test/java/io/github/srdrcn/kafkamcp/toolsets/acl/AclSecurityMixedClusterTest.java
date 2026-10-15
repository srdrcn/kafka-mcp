package io.github.srdrcn.kafkamcp.toolsets.acl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeDelegationTokenResult;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.support.MixedClusterIntegrationTest;
import io.github.srdrcn.kafkamcp.support.TestConfigs;

/**
 * acl security/quota tools on a ZooKeeper (Kafka 3.9) cluster: quotas_list, quotas_update, scram_users_list,
 * delegation_tokens_list, kerberos_self_check, security_summary. security_summary reports the mode-specific
 * information (KRaft/ZooKeeper, authorizer, controller/ZooKeeper keys) correctly in both modes; APIs an old
 * broker does not support do not make the tool fail.
 */
class AclSecurityMixedClusterTest extends MixedClusterIntegrationTest {

	private static final ObjectMapper JSON = new ObjectMapper();

	@Autowired
	private KafkaClients clients;

	@Autowired
	private SecurityTools securityTools;

	@Autowired
	private QuotaTools quotaTools;

	private static Map<String, Double> zkQuotasOf(ClientQuotaEntity entity) throws Exception {
		java.util.List<ClientQuotaFilterComponent> components = new java.util.ArrayList<>();
		entity.entries()
			.forEach((type, name) -> components.add(name == null ? ClientQuotaFilterComponent.ofDefaultEntity(type)
					: ClientQuotaFilterComponent.ofEntity(type, name)));
		return zkAdmin().describeClientQuotas(ClientQuotaFilter.containsOnly(components))
			.entities()
			.get(15, TimeUnit.SECONDS)
			.getOrDefault(entity, Map.of());
	}

	@Test
	void quotasUpdateAndListOnZooKeeper() throws Exception {
		String user = unique("mxq");
		ClientQuotaEntity userEntity = new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, user));
		JsonNode set = ok("quotas_update",
				zkArgs("user", user, "set", Map.of("producer_byte_rate", 1048576, "request_percentage", 25.5)));
		assertThat(set.get("before").isEmpty()).isTrue();
		assertThat(set.at("/after/producer_byte_rate").asLong()).isEqualTo(1048576L);
		assertThat(set.at("/after/request_percentage").asDouble()).isEqualTo(25.5);
		assertThat(set.has("note")).isFalse();
		assertThat(zkQuotasOf(userEntity)).containsEntry("producer_byte_rate", 1048576.0)
			.containsEntry("request_percentage", 25.5);

		JsonNode clientDefault = ok("quotas_update",
				zkArgs("user", user, "clientId", "<default>", "set", Map.of("consumer_byte_rate", 2048)));
		assertThat(clientDefault.at("/entity/clientId").asText()).isEqualTo("<default>");
		java.util.Map<String, String> both = new java.util.HashMap<>();
		both.put(ClientQuotaEntity.USER, user);
		both.put(ClientQuotaEntity.CLIENT_ID, null);
		assertThat(zkQuotasOf(new ClientQuotaEntity(both))).containsEntry("consumer_byte_rate", 2048.0);

		String ip = "10.254." + (System.nanoTime() % 200) + ".9";
		JsonNode ipSet = ok("quotas_update", zkArgs("ip", ip, "set", Map.of("connection_creation_rate", 7)));
		assertThat(ipSet.at("/after/connection_creation_rate").asLong()).isEqualTo(7L);
		assertThat(zkQuotasOf(new ClientQuotaEntity(Map.of(ClientQuotaEntity.IP, ip))))
			.containsEntry("connection_creation_rate", 7.0);

		JsonNode list = ok("quotas_list", zkArgs("filter", "user=" + user));
		assertThat(list.get("total").asInt()).isEqualTo(2);
		assertThat(list.at("/quotas/0/entity/user").asText()).isEqualTo(user);
		assertThat(list.at("/quotas/0/quotas/producer_byte_rate").asLong()).isEqualTo(1048576L);
		assertThat(list.at("/quotas/1/entity/clientId").asText()).isEqualTo("<default>");
		assertThat(ok("quotas_list", zkArgs("filter", "ip=" + ip.replace(".", "\\.") + "$")).get("total").asInt())
			.isEqualTo(1);
		// the same entity does not exist on the KRaft cluster
		assertThat(ok("quotas_list", args("filter", "user=" + user)).get("total").asInt()).isZero();

		JsonNode removed = ok("quotas_update",
				zkArgs("user", user, "remove", List.of("producer_byte_rate", "request_percentage")));
		assertThat(removed.at("/before/producer_byte_rate").asLong()).isEqualTo(1048576L);
		assertThat(removed.get("after").isEmpty()).isTrue();
		assertThat(zkQuotasOf(userEntity)).isEmpty();
		ok("quotas_update", zkArgs("user", user, "clientId", "<default>", "remove", List.of("consumer_byte_rate")));
		ok("quotas_update", zkArgs("ip", ip, "remove", List.of("connection_creation_rate")));
		assertThat(ok("quotas_list", zkArgs("filter", "user=" + user)).get("total").asInt()).isZero();
	}

	/**
	 * On ZooKeeper SCRAM credentials and quotas share the /config/users znode: a SCRAM user must not show up as
	 * an empty entity in the quota list, and the SCRAM credentials must remain after the quota is removed.
	 */
	@Test
	void scramUsersOnZooKeeperAndQuotaSeparation() throws Exception {
		String user = unique("mxscram");
		zkAdmin()
			.alterUserScramCredentials(List.of(new UserScramCredentialUpsertion(user,
					new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), "zk-secret-pw")))
			.all()
			.get(15, TimeUnit.SECONDS);
		zkAdmin()
			.alterUserScramCredentials(List.of(new UserScramCredentialUpsertion(user,
					new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 8192), "zk-secret-pw")))
			.all()
			.get(15, TimeUnit.SECONDS);
		try {
			await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
				JsonNode r = ok("scram_users_list", zkArgs("filter", "^" + user + "$"));
				assertThat(r.get("total").asInt()).isEqualTo(1);
				JsonNode u = r.get("users").get(0);
				assertThat(u.get("user").asText()).isEqualTo(user);
				assertThat(u.at("/mechanisms/0/mechanism").asText()).isEqualTo("SCRAM-SHA-256");
				assertThat(u.at("/mechanisms/0/iterations").asInt()).isEqualTo(4096);
				assertThat(u.at("/mechanisms/1/mechanism").asText()).isEqualTo("SCRAM-SHA-512");
				assertThat(u.at("/mechanisms/1/iterations").asInt()).isEqualTo(8192);
				assertThat(r.toString()).doesNotContain("zk-secret-pw").doesNotContainIgnoringCase("salt");
			});
			assertThat(ok("scram_users_list", args("filter", "^" + user + "$")).get("total").asInt()).isZero();
			assertThat(ok("quotas_list", zkArgs("filter", "user=" + user + "$")).get("total").asInt()).isZero();

			ok("quotas_update", zkArgs("user", user, "set", Map.of("consumer_byte_rate", 4096)));
			assertThat(ok("quotas_list", zkArgs("filter", "user=" + user + "$")).at("/quotas/0/quotas/consumer_byte_rate")
				.asLong()).isEqualTo(4096L);
			ok("quotas_update", zkArgs("user", user, "remove", List.of("consumer_byte_rate")));
			assertThat(ok("quotas_list", zkArgs("filter", "user=" + user + "$")).get("total").asInt()).isZero();
			assertThat(ok("scram_users_list", zkArgs("filter", "^" + user + "$")).get("total").asInt()).isEqualTo(1);
			JsonNode summary = ok("security_summary", zkArgs());
			assertThat(summary.at("/scram/users").asInt()).isPositive();
		}
		finally {
			zkAdmin()
				.alterUserScramCredentials(List.of(new UserScramCredentialDeletion(user, ScramMechanism.SCRAM_SHA_256),
						new UserScramCredentialDeletion(user, ScramMechanism.SCRAM_SHA_512)))
				.all()
				.get(15, TimeUnit.SECONDS);
		}
	}

	@Test
	void delegationTokensOnZooKeeper() throws Exception {
		DelegationToken token = zkAdmin().createDelegationToken().delegationToken().get(15, TimeUnit.SECONDS);
		String id = token.tokenInfo().tokenId();
		await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
			JsonNode r = ok("delegation_tokens_list", zkArgs("filter", id));
			assertThat(r.get("enabled").asBoolean()).isTrue();
			assertThat(r.get("total").asInt()).isEqualTo(1);
			JsonNode t = r.get("tokens").get(0);
			assertThat(t.get("tokenId").asText()).isEqualTo(id);
			assertThat(t.get("owner").asText()).isEqualTo("User:admin");
			assertThat(t.get("expired").asBoolean()).isFalse();
			assertThat(t.get("expiryTimestamp").asText()).endsWith("Z");
			assertThat(r.toString()).doesNotContain(token.hmacAsBase64String());
		});
		// the token belongs to the ZooKeeper cluster: not visible on the KRaft cluster
		assertThat(ok("delegation_tokens_list", args("filter", id)).get("total").asInt()).isZero();
		assertThat(ok("security_summary", zkArgs()).at("/delegationTokens/count").asInt()).isPositive();
		zkAdmin().expireDelegationToken(token.hmac()).expiryTimestamp().get(15, TimeUnit.SECONDS);
	}

	@Test
	void kerberosSelfCheckConnectsToZooKeeperCluster() throws Exception {
		String clusterId = zkAdmin().describeCluster().clusterId().get(15, TimeUnit.SECONDS);
		JsonNode r = ok("kerberos_self_check", zkArgs());
		assertThat(r.get("cluster").asText()).isEqualTo(ZK_CLUSTER);
		assertThat(r.get("kerberosConfigured").asBoolean()).isFalse();
		assertThat(r.at("/client/saslMechanism").asText()).isEqualTo("PLAIN");
		assertThat(r.at("/client/saslJaasConfig").asText()).contains("****").doesNotContain("admin-secret");
		assertThat(r.at("/kafka/status").asText()).isEqualTo("ok");
		assertThat(r.at("/kafka/clusterId").asText()).isEqualTo(clusterId);
		assertThat(ok("kerberos_self_check", args()).at("/kafka/clusterId").asText()).isNotEqualTo(clusterId);
	}

	/** security_summary reports the mode-specific information correctly in both modes. */
	@Test
	void securitySummaryReportsModeSpecificInformation() {
		JsonNode zk = ok("security_summary", zkArgs());
		assertThat(zk.get("mode").asText()).isEqualTo("ZooKeeper");
		JsonNode zb = zk.get("broker");
		assertThat(zb.get("authorizer.class.name").asText()).isEqualTo("kafka.security.authorizer.AclAuthorizer");
		assertThat(zb.get("zookeeper.set.acl").asText()).isEqualTo("false");
		assertThat(zb.get("zookeeper.ssl.client.enable").asText()).isEqualTo("false");
		// ZooKeeper 3.9 returns these with defaults but they are KRaft-specific: must not be shown
		assertThat(zb.has("process.roles")).isFalse();
		assertThat(zb.has("controller.listener.names")).isFalse();
		assertThat(zb.has("sasl.mechanism.controller.protocol")).isFalse();
		assertThat(zb.get("listeners").toString()).contains("SASL", "INTERNAL").doesNotContain("CONTROLLER");
		// INTERNAL listener is PLAINTEXT: the default sasl.mechanism.inter.broker.protocol=GSSAPI is not shown
		assertThat(zb.get("interBrokerSecurityProtocol").asText()).isEqualTo("PLAINTEXT");
		assertThat(zb.has("sasl.mechanism.inter.broker.protocol")).isFalse();
		assertThat(zb.has("controllerListenerSecurityProtocol")).isFalse();
		assertThat(zb.get("super.users").asText()).contains("hidden");
		assertThat(zb.get("allow.everyone.if.no.acl.found").asText()).contains("hidden");
		assertThat(zk.at("/acls/total").isInt()).isTrue();
		assertThat(zk.at("/scram/users").isInt()).isTrue();
		assertThat(zk.at("/quotas/entities").isInt()).isTrue();
		assertThat(zk.at("/delegationTokens/count").isInt()).isTrue();
		assertThat(zk.get("notes").toString()).contains("zookeeper.set.acl=false")
			.contains("super.users")
			.doesNotContain("4.x")
			.doesNotContain("is not built in")
			.doesNotContain("controller listener");

		JsonNode kraft = ok("security_summary", args());
		assertThat(kraft.get("mode").asText()).isEqualTo("KRaft");
		JsonNode kb = kraft.get("broker");
		assertThat(kb.get("authorizer.class.name").asText())
			.isEqualTo("org.apache.kafka.metadata.authorizer.StandardAuthorizer");
		assertThat(kb.get("process.roles").asText()).isEqualTo("broker,controller");
		assertThat(kb.get("controller.listener.names").asText()).isEqualTo("CONTROLLER");
		// CONTROLLER:PLAINTEXT — the default GSSAPI returned by the broker is not shown as if Kerberos were used
		assertThat(kb.get("controllerListenerSecurityProtocol").asText()).isEqualTo("PLAINTEXT");
		assertThat(kb.has("sasl.mechanism.controller.protocol")).isFalse();
		assertThat(kb.get("interBrokerSecurityProtocol").asText()).isEqualTo("PLAINTEXT");
		assertThat(kb.has("sasl.mechanism.inter.broker.protocol")).isFalse();
		assertThat(kb.has("zookeeper.set.acl")).isFalse();
		assertThat(kb.has("zookeeper.ssl.client.enable")).isFalse();
		assertThat(kb.get("listeners").toString()).contains("CONTROLLER");
		assertThat(kraft.get("notes").toString()).doesNotContain("zookeeper")
			.contains("super.users")
			.contains("the controller listener (CONTROLLER) is PLAINTEXT");
		// the readOnly cluster is the same KRaft cluster
		assertThat(ok("security_summary", args("cluster", TestConfigs.READ_ONLY_CLUSTER)).get("mode").asText())
			.isEqualTo("KRaft");
	}

	/**
	 * Old broker (e.g. 2.x ZooKeeper): describeUserScramCredentials (2.7+), describeClientQuotas (2.6+) and
	 * describeDelegationToken fail with UnsupportedVersionException. The tools do not fail; they return a note.
	 */
	@Test
	void unsupportedApisOnOldBrokersBecomeNotes() throws Exception {
		Admin old = unsupportedAdmin(zkAdmin(), Set.of("describeUserScramCredentials", "describeClientQuotas",
				"describeDelegationToken"));
		KafkaClients oldClients = new KafkaClients(clients.config()) {
			@Override
			public Admin admin(String cluster) {
				return old;
			}
		};
		Instant deadline = Instant.now().plusSeconds(15);

		JsonNode scram = JSON.valueToTree(securityTools.scramUsers(call(oldClients, deadline)));
		assertThat(scram.get("supported").asBoolean()).isFalse();
		assertThat(scram.get("note").asText()).contains(KafkaErrors.UNSUPPORTED)
			.contains("Kafka 2.7+ on ZooKeeper")
			.contains("3.5+ on KRaft");

		JsonNode quotas = JSON.valueToTree(quotaTools.list(call(oldClients, deadline)));
		assertThat(quotas.get("supported").asBoolean()).isFalse();
		assertThat(quotas.get("note").asText()).contains(KafkaErrors.UNSUPPORTED).contains("2.6");

		JsonNode tokens = JSON.valueToTree(securityTools.delegationTokens(call(oldClients, deadline)));
		assertThat(tokens.get("enabled").asBoolean()).isFalse();
		assertThat(tokens.get("note").asText()).contains("does not support the delegation token API (version)");

		JsonNode summary = JSON.valueToTree(securityTools.securitySummary(call(oldClients, deadline)));
		assertThat(summary.get("mode").asText()).isEqualTo("ZooKeeper");
		assertThat(summary.at("/scram/supported").asBoolean(true)).isFalse();
		assertThat(summary.at("/scram/note").asText()).contains(KafkaErrors.UNSUPPORTED);
		assertThat(summary.at("/quotas/supported").asBoolean(true)).isFalse();
		assertThat(summary.at("/delegationTokens/enabled").asBoolean(true)).isFalse();
		assertThat(summary.at("/acls/total").isInt()).isTrue();
		assertThat(Instant.now()).isBefore(deadline);
	}

	private static ToolCall call(KafkaClients clients, Instant deadline) {
		return new ToolCall("test", zkArgs(), clients, deadline);
	}

	/** Proxy that fails the given Admin methods with UnsupportedVersionException, like an old broker. */
	private static Admin unsupportedAdmin(Admin real, Set<String> unsupported) {
		return (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] { Admin.class },
				(proxy, method, methodArgs) -> {
					if (unsupported.contains(method.getName())) {
						return failedResult(method.getReturnType(), new UnsupportedVersionException(
								"The node does not support " + method.getName()));
					}
					try {
						return method.invoke(real, methodArgs);
					}
					catch (InvocationTargetException e) {
						throw e.getCause();
					}
				});
	}

	private static Object failedResult(Class<?> type, Exception error) throws Exception {
		KafkaFutureImpl<Object> f = new KafkaFutureImpl<>();
		f.completeExceptionally(error);
		Class<?> param = KafkaFuture.class;
		if (type == DescribeUserScramCredentialsResult.class || type == DescribeDelegationTokenResult.class
				|| type == DescribeClientQuotasResult.class) {
			Constructor<?> c = type.getDeclaredConstructor(param);
			c.setAccessible(true);
			return c.newInstance(f);
		}
		throw new IllegalArgumentException("unsupported result type: " + type);
	}

}
