package io.github.srdrcn.kafkamcp.toolsets.acl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.ConfigEntry;
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
		l.add(entry("controller.listener.names", "CONTROLLER"));
		l.add(entry("sasl.mechanism.controller.protocol", "SCRAM-SHA-512"));
		BrokerSecurity b = new BrokerSecurity(3, l);
		assertThat(b.mode()).isEqualTo("KRaft");
		Map<String, Object> r = b.render();
		assertThat(r).containsEntry("process.roles", "broker")
			.containsEntry("controller.listener.names", "CONTROLLER")
			.containsEntry("sasl.mechanism.controller.protocol", "SCRAM-SHA-512")
			.doesNotContainKeys("zookeeper.set.acl", "zookeeper.ssl.client.enable");
		assertThat(b.customAuthorizerNote()).isNull();
		assertThat(b.superUsers()).isNull();
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
	}

}
