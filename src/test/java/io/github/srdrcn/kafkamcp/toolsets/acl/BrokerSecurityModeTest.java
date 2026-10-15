package io.github.srdrcn.kafkamcp.toolsets.acl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.errors.DelegationTokenDisabledException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * BrokerSecurity'nin mod çıkarımı ve moda özgü anahtarları (Kafka gerekmez): tek düğümlü testlerin
 * gösteremediği KRaft 3.x (zookeeper.* varsayılanlarıyla), ZooKeeper 2.x (process.roles yok), ZK→KRaft
 * geçişi ve yerleşik olmayan authorizer durumları.
 */
class BrokerSecurityModeTest {

	private static ConfigEntry entry(String name, String value) {
		return new ConfigEntry(name, value);
	}

	private static ConfigEntry hidden(String name) {
		return new ConfigEntry(name, null, ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG, true, false, List.of(),
				ConfigEntry.ConfigType.UNKNOWN, null);
	}

	private static List<ConfigEntry> common(String authorizer) {
		List<ConfigEntry> l = new ArrayList<>();
		l.add(entry("listeners", "SASL://0.0.0.0:9092"));
		l.add(entry("authorizer.class.name", authorizer));
		l.add(hidden("super.users"));
		l.add(hidden("allow.everyone.if.no.acl.found"));
		return l;
	}

	@Test
	void kraft3xIgnoresZooKeeperDefaults() {
		List<ConfigEntry> l = common("org.apache.kafka.metadata.authorizer.StandardAuthorizer");
		l.add(entry("process.roles", "broker"));
		l.add(entry("zookeeper.connect", null));
		l.add(entry("zookeeper.set.acl", "false"));
		l.add(entry("zookeeper.ssl.client.enable", "false"));
		l.add(entry("listener.security.protocol.map", "SASL:SASL_SSL,REPLICATION:SASL_SSL,CONTROLLER:SASL_SSL"));
		l.add(entry("inter.broker.listener.name", "REPLICATION"));
		l.add(entry("sasl.mechanism.inter.broker.protocol", "SCRAM-SHA-512"));
		l.add(entry("controller.listener.names", "CONTROLLER"));
		l.add(entry("sasl.mechanism.controller.protocol", "SCRAM-SHA-512"));
		BrokerSecurity b = new BrokerSecurity(3, l);
		assertThat(b.mode()).isEqualTo("KRaft");
		Map<String, Object> r = b.render();
		// SASL_SSL controller/broker listener'larında mekanizma anlamlı: gösterilir
		assertThat(r).containsEntry("process.roles", "broker")
			.containsEntry("controller.listener.names", "CONTROLLER")
			.containsEntry("controllerListenerSecurityProtocol", "SASL_SSL")
			.containsEntry("sasl.mechanism.controller.protocol", "SCRAM-SHA-512")
			.containsEntry("interBrokerSecurityProtocol", "SASL_SSL")
			.containsEntry("sasl.mechanism.inter.broker.protocol", "SCRAM-SHA-512")
			.doesNotContainKeys("zookeeper.set.acl", "zookeeper.ssl.client.enable");
		assertThat(b.customAuthorizerNote()).isNull();
		assertThat(b.superUsers()).isNull();
	}

	/**
	 * Controller/broker listener'ı SASL değilse broker sasl.mechanism.*.protocol'ü varsayılanla (GSSAPI)
	 * döndürür: mekanizma gösterilmez, listener'ın protokolü gösterilir (Strimzi gibi mTLS controller da).
	 */
	@Test
	void kraftNonSaslListenersHideDefaultMechanisms() {
		List<ConfigEntry> l = common("org.apache.kafka.metadata.authorizer.StandardAuthorizer");
		l.add(entry("process.roles", "broker"));
		l.add(entry("listener.security.protocol.map",
				"SASL:SASL_PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,CONTROLPLANE-9090:SSL"));
		l.add(entry("inter.broker.listener.name", "INTERNAL"));
		l.add(entry("sasl.mechanism.inter.broker.protocol", "GSSAPI"));
		l.add(entry("controller.listener.names", "CONTROLLER"));
		l.add(entry("sasl.mechanism.controller.protocol", "GSSAPI"));
		BrokerSecurity plain = new BrokerSecurity(1, l);
		assertThat(plain.controllerListener()).isEqualTo("CONTROLLER");
		assertThat(plain.controllerSecurityProtocol()).isEqualTo("PLAINTEXT");
		assertThat(plain.render()).containsEntry("controllerListenerSecurityProtocol", "PLAINTEXT")
			.containsEntry("interBrokerSecurityProtocol", "PLAINTEXT")
			.doesNotContainKeys("sasl.mechanism.controller.protocol", "sasl.mechanism.inter.broker.protocol");

		l.removeIf(e -> e.name().equals("controller.listener.names"));
		l.add(entry("controller.listener.names", "CONTROLPLANE-9090"));
		BrokerSecurity mtls = new BrokerSecurity(1, l);
		assertThat(mtls.render()).containsEntry("controllerListenerSecurityProtocol", "SSL")
			.doesNotContainKey("sasl.mechanism.controller.protocol");
	}

	/** inter.broker.listener.name yoksa security.inter.broker.protocol; listener adı protokol adı olabilir. */
	@Test
	void interBrokerProtocolWithoutListenerName() {
		List<ConfigEntry> l = common("kafka.security.authorizer.AclAuthorizer");
		l.add(entry("zookeeper.connect", "zk:2181"));
		l.add(entry("security.inter.broker.protocol", "SASL_SSL"));
		l.add(entry("sasl.mechanism.inter.broker.protocol", "SCRAM-SHA-256"));
		BrokerSecurity b = new BrokerSecurity(1, l);
		assertThat(b.interBrokerSecurityProtocol()).isEqualTo("SASL_SSL");
		assertThat(b.securityProtocolOf("sasl_plaintext")).isEqualTo("SASL_PLAINTEXT");
		assertThat(b.securityProtocolOf("UNKNOWN")).isNull();
		assertThat(b.controllerListener()).isNull();
		assertThat(b.render()).containsEntry("sasl.mechanism.inter.broker.protocol", "SCRAM-SHA-256")
			.doesNotContainKey("controllerListenerSecurityProtocol");
	}

	/** KIP-679: 2.8 öncesi ZooKeeper broker'larında idempotent producer CLUSTER IDEMPOTENT_WRITE ister. */
	@Test
	void idempotentWriteRequirementByModeAndIbp() {
		assertThat(zkWithIbp("2.7-IV2").idempotentWriteRequired()).isTrue();
		assertThat(zkWithIbp("2.1").idempotentWriteRequired()).isTrue();
		assertThat(zkWithIbp(null).idempotentWriteRequired()).isTrue();
		assertThat(zkWithIbp("2.8-IV1").idempotentWriteRequired()).isFalse();
		assertThat(zkWithIbp("3.9-IV0").idempotentWriteRequired()).isFalse();
		assertThat(zkWithIbp("10.0-IV0").idempotentWriteRequired()).isFalse();
		List<ConfigEntry> kraft = common("org.apache.kafka.metadata.authorizer.StandardAuthorizer");
		kraft.add(entry("process.roles", "broker"));
		assertThat(new BrokerSecurity(1, kraft).idempotentWriteRequired()).isFalse();
		assertThat(BrokerSecurity.unavailable("yetki yok").idempotentWriteRequired()).isNull();
	}

	private static BrokerSecurity zkWithIbp(String ibp) {
		List<ConfigEntry> l = common("kafka.security.authorizer.AclAuthorizer");
		l.add(entry("zookeeper.connect", "zk:2181"));
		if (ibp != null) {
			l.add(entry("inter.broker.protocol.version", ibp));
		}
		return new BrokerSecurity(1, l);
	}

	/** allow.everyone.if.no.acl.found gizliyse parametre kullanılır; o da yoksa false varsayılır (assumed). */
	@Test
	void allowEveryoneFromParameterWhenHidden() {
		BrokerSecurity hidden = new BrokerSecurity(1, common("kafka.security.authorizer.AclAuthorizer"));
		BrokerSecurity.AllowEveryone assumed = hidden.allowEveryone(null);
		assertThat(assumed.value()).isFalse();
		assertThat(assumed.assumed()).isTrue();
		assertThat(assumed.note()).contains("false kabul edildi").contains("allowEveryoneIfNoAclFound");
		assertThat(hidden.allowEveryone()).isEqualTo(assumed);
		BrokerSecurity.AllowEveryone provided = hidden.allowEveryone(true);
		assertThat(provided.value()).isTrue();
		assertThat(provided.assumed()).isFalse();
		assertThat(provided.note()).contains("parametresindeki değer (true)");

		List<ConfigEntry> l = new ArrayList<>();
		l.add(entry("allow.everyone.if.no.acl.found", "false"));
		BrokerSecurity visible = new BrokerSecurity(1, l);
		BrokerSecurity.AllowEveryone fromBroker = visible.allowEveryone(true);
		assertThat(fromBroker.value()).isFalse();
		assertThat(fromBroker.assumed()).isFalse();
		assertThat(fromBroker.note()).contains("yok sayıldı");
		assertThat(visible.allowEveryone(false).note()).isNull();
	}

	@Test
	void zooKeeper3xIgnoresKraftDefaults() {
		List<ConfigEntry> l = common("kafka.security.authorizer.AclAuthorizer");
		l.add(entry("process.roles", ""));
		l.add(entry("zookeeper.connect", "zk1:2181,zk2:2181/kafka"));
		l.add(entry("zookeeper.set.acl", "true"));
		l.add(entry("controller.listener.names", null));
		l.add(entry("sasl.mechanism.controller.protocol", "GSSAPI"));
		l.add(entry("controller.quorum.voters", ""));
		BrokerSecurity b = new BrokerSecurity(1, l);
		assertThat(b.mode()).isEqualTo("ZooKeeper");
		assertThat(b.render()).containsEntry("zookeeper.set.acl", "true")
			.doesNotContainKeys("process.roles", "controller.listener.names", "sasl.mechanism.controller.protocol",
					"zookeeper.connect");
		assertThat(b.customAuthorizerNote()).isNull();
	}

	@Test
	void zooKeeper2xWithoutProcessRolesAndLegacyAuthorizer() {
		List<ConfigEntry> l = common("kafka.security.auth.SimpleAclAuthorizer");
		l.add(entry("zookeeper.connect", "zk:2181"));
		BrokerSecurity b = new BrokerSecurity(0, l);
		assertThat(b.mode()).isEqualTo("ZooKeeper");
		assertThat(b.customAuthorizerNote()).isNull();
		assertThat(b.authorizer()).isEqualTo("kafka.security.auth.SimpleAclAuthorizer");
	}

	@Test
	void migratingKraftBrokerWithZookeeperConnectIsKraft() {
		List<ConfigEntry> l = common("org.apache.kafka.metadata.authorizer.StandardAuthorizer");
		l.add(entry("process.roles", "broker"));
		l.add(entry("zookeeper.connect", "zk:2181"));
		l.add(entry("zookeeper.metadata.migration.enable", "true"));
		assertThat(new BrokerSecurity(4, l).mode()).isEqualTo("KRaft");
	}

	@Test
	void unknownModeAndCustomAuthorizer() {
		BrokerSecurity custom = new BrokerSecurity(1,
				common("io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer"));
		assertThat(custom.mode()).isNull();
		assertThat(custom.modeKeys()).isEmpty();
		assertThat(custom.customAuthorizerNote()).contains("ConfluentServerAuthorizer").contains("yerleşik değil");

		BrokerSecurity unavailable = BrokerSecurity.unavailable("Cluster yetkisi yok (ACL)");
		assertThat(unavailable.mode()).isNull();
		assertThat(unavailable.authorizer()).isNull();
		assertThat(unavailable.customAuthorizerNote()).isNull();
		assertThat(unavailable.render()).containsOnlyKeys("error");
	}

	@Test
	void unsupportedVersionNotes() {
		ToolException wrapped = new ToolException("Broker bu işlemi desteklemiyor (sürüm)",
				new UnsupportedVersionException("The node does not support DESCRIBE_USER_SCRAM_CREDENTIALS"));
		assertThat(Acls.unsupportedNote(wrapped, "X 2.7+ gerektirir"))
			.isEqualTo("Broker bu işlemi desteklemiyor (sürüm): X 2.7+ gerektirir");
		assertThat(Acls.unsupportedNote(new ToolException("başka hata"), "X")).isNull();
		assertThat(SecurityTools.tokenNote(wrapped)).contains("desteklemiyor (sürüm)").contains("KRaft'ta 3.6+");
		// SCRAM: KRaft'ta KIP-900 (3.5) gerekir, 2.7 değil
		assertThat(Acls.unsupportedNote(wrapped, SecurityTools.SCRAM_REQUIREMENT)).contains("ZooKeeper'da Kafka 2.7+")
			.contains("KRaft'ta 3.5+");
		// 3.0 öncesi ZooKeeper broker'ları delegation.token.secret.key'i tanımaz
		assertThat(SecurityTools.tokenNote(new ToolException("Delegation token kapalı",
				new DelegationTokenDisabledException("Delegation Token feature is not enabled"))))
			.contains("delegation.token.secret.key")
			.contains("delegation.token.master.key");
	}

}
