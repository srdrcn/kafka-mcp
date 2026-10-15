package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;

/** brokers_config_drift comparison logic (multi-broker case; the test cluster has a single broker). */
class BrokerConfigDriftTest {

	@Test
	void reportsOnlyDifferingKeysGroupedByValue() {
		Map<Integer, Config> byBroker = new TreeMap<>();
		byBroker.put(1, config(entry("num.io.threads", "8", ConfigSource.DEFAULT_CONFIG),
				entry("log.retention.hours", "168", ConfigSource.STATIC_BROKER_CONFIG),
				entry("broker.id", "1", ConfigSource.STATIC_BROKER_CONFIG),
				entry("ssl.keystore.password", null, ConfigSource.STATIC_BROKER_CONFIG, true)));
		byBroker.put(2, config(entry("num.io.threads", "16", ConfigSource.DYNAMIC_BROKER_CONFIG),
				entry("log.retention.hours", "168", ConfigSource.STATIC_BROKER_CONFIG),
				entry("broker.id", "2", ConfigSource.STATIC_BROKER_CONFIG),
				entry("ssl.keystore.password", null, ConfigSource.STATIC_BROKER_CONFIG, true)));
		byBroker.put(3, config(entry("num.io.threads", "8", ConfigSource.DEFAULT_CONFIG),
				entry("broker.id", "3", ConfigSource.STATIC_BROKER_CONFIG),
				entry("ssl.keystore.password", null, ConfigSource.STATIC_BROKER_CONFIG, true)));

		List<Map<String, Object>> diffs = BrokerTools.differences(byBroker,
				name -> !BrokerTools.PER_BROKER_KEYS.contains(name));

		assertThat(diffs).extracting(d -> d.get("name")).containsExactly("log.retention.hours", "num.io.threads");
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> io = (List<Map<String, Object>>) diffs.get(1).get("values");
		assertThat(io).hasSize(2);
		assertThat(io.get(0)).containsEntry("value", "8")
			.containsEntry("source", "DEFAULT_CONFIG")
			.containsEntry("brokers", List.of(1, 3));
		assertThat(io.get(1)).containsEntry("value", "16").containsEntry("brokers", List.of(2));
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> retention = (List<Map<String, Object>>) diffs.get(0).get("values");
		assertThat(retention.get(1)).containsEntry("value", "(missing)").containsEntry("brokers", List.of(3));
	}

	@Test
	void mixedZooKeeperAndKraftBrokersSkipModeSpecificKeys() {
		// ZooKeeper→KRaft migration: broker 1 is still ZooKeeper, broker 2 is a KRaft broker
		Map<Integer, Config> byBroker = new TreeMap<>();
		byBroker.put(1, config(entry("zookeeper.connect", "zk:2181", ConfigSource.STATIC_BROKER_CONFIG),
				entry("process.roles", "", ConfigSource.DEFAULT_CONFIG),
				entry("controller.quorum.voters", "", ConfigSource.DEFAULT_CONFIG),
				entry("inter.broker.protocol.version", "3.9-IV0", ConfigSource.STATIC_BROKER_CONFIG),
				entry("num.io.threads", "8", ConfigSource.DEFAULT_CONFIG)));
		byBroker.put(2, config(entry("zookeeper.connect", null, ConfigSource.DEFAULT_CONFIG),
				entry("process.roles", "broker", ConfigSource.STATIC_BROKER_CONFIG),
				entry("controller.quorum.voters", "3000@c:9093", ConfigSource.STATIC_BROKER_CONFIG),
				entry("inter.broker.protocol.version", "3.9-IV0", ConfigSource.DEFAULT_CONFIG),
				entry("num.io.threads", "16", ConfigSource.STATIC_BROKER_CONFIG)));

		assertThat(BrokerTools.brokerMode(byBroker.get(1))).isEqualTo(ClusterMeta.Mode.ZOOKEEPER);
		assertThat(BrokerTools.brokerMode(byBroker.get(2))).isEqualTo(ClusterMeta.Mode.KRAFT);
		Map<String, List<Integer>> modes = BrokerTools.brokerModes(byBroker);
		assertThat(modes).containsEntry("ZooKeeper", List.of(1)).containsEntry("KRaft", List.of(2));

		List<Map<String, Object>> mixed = BrokerTools.differences(byBroker,
				name -> !BrokerTools.PER_BROKER_KEYS.contains(name) && !BrokerTools.modeSpecific(name));
		assertThat(mixed).extracting(d -> d.get("name")).containsExactly("num.io.threads");
		// in the same mode, a zookeeper.connect / controller.quorum.voters difference would be real drift
		List<Map<String, Object>> all = BrokerTools.differences(byBroker,
				name -> !BrokerTools.PER_BROKER_KEYS.contains(name));
		assertThat(all).extracting(d -> d.get("name"))
			.contains("controller.quorum.voters", "zookeeper.connect", "num.io.threads")
			.doesNotContain("process.roles");
		assertThat(BrokerTools.modeSpecific("zookeeper.session.timeout.ms")).isTrue();
		assertThat(BrokerTools.modeSpecific("controller.quorum.bootstrap.servers")).isTrue();
		assertThat(BrokerTools.modeSpecific("controller.socket.timeout.ms")).isFalse();
	}

	@Test
	void mixedModesSkipAuthorizerOnlyWhenEachBrokerUsesItsModesAuthorizer() {
		// migration guide: a broker moved to KRaft switches to StandardAuthorizer, ZooKeeper brokers keep AclAuthorizer
		Map<Integer, Config> expected = new TreeMap<>();
		expected.put(1, zkBroker("kafka.security.authorizer.AclAuthorizer"));
		expected.put(2, zkBroker("kafka.security.auth.SimpleAclAuthorizer"));
		expected.put(3, kraftBroker("org.apache.kafka.metadata.authorizer.StandardAuthorizer"));
		assertThat(BrokerTools.authorizerMatchesModes(expected)).isTrue();
		assertThat(BrokerTools.differences(expected, BrokerTools.compared(expected, name -> true)))
			.extracting(d -> d.get("name"))
			.containsExactly("num.io.threads");

		// no authorizer on the KRaft broker (ACLs disabled): a real security difference, reported
		Map<Integer, Config> noAcl = new TreeMap<>();
		noAcl.put(1, zkBroker("kafka.security.authorizer.AclAuthorizer"));
		noAcl.put(2, kraftBroker(""));
		assertThat(BrokerTools.authorizerMatchesModes(noAcl)).isFalse();
		assertThat(BrokerTools.differences(noAcl, BrokerTools.compared(noAcl, name -> true)))
			.extracting(d -> d.get("name"))
			.contains(BrokerTools.AUTHORIZER);

		// custom authorizer on the ZooKeeper broker: not an expected combination, reported
		Map<Integer, Config> custom = new TreeMap<>();
		custom.put(1, zkBroker("com.example.CustomAuthorizer"));
		custom.put(2, kraftBroker("org.apache.kafka.metadata.authorizer.StandardAuthorizer"));
		assertThat(BrokerTools.differences(custom, BrokerTools.compared(custom, name -> true)))
			.extracting(d -> d.get("name"))
			.contains(BrokerTools.AUTHORIZER);

		// a different authorizer in the same mode is always drift
		Map<Integer, Config> sameMode = new TreeMap<>();
		sameMode.put(1, zkBroker("kafka.security.authorizer.AclAuthorizer"));
		sameMode.put(2, zkBroker("kafka.security.auth.SimpleAclAuthorizer"));
		assertThat(BrokerTools.differences(sameMode, BrokerTools.compared(sameMode, name -> true)))
			.extracting(d -> d.get("name"))
			.containsExactly(BrokerTools.AUTHORIZER);
	}

	private static Config zkBroker(String authorizer) {
		return config(entry("zookeeper.connect", "zk:2181", ConfigSource.STATIC_BROKER_CONFIG),
				entry("process.roles", "", ConfigSource.DEFAULT_CONFIG),
				entry(BrokerTools.AUTHORIZER, authorizer, ConfigSource.STATIC_BROKER_CONFIG),
				entry("num.io.threads", "8", ConfigSource.DEFAULT_CONFIG));
	}

	private static Config kraftBroker(String authorizer) {
		return config(entry("process.roles", "broker", ConfigSource.STATIC_BROKER_CONFIG),
				entry("controller.quorum.voters", "3000@c:9093", ConfigSource.STATIC_BROKER_CONFIG),
				entry(BrokerTools.AUTHORIZER, authorizer,
						authorizer.isEmpty() ? ConfigSource.DEFAULT_CONFIG : ConfigSource.STATIC_BROKER_CONFIG),
				entry("num.io.threads", "16", ConfigSource.STATIC_BROKER_CONFIG));
	}

	@Test
	void keysMissingOnSomeBrokersWithOnlyDefaultsAreNotDrift() {
		// key present on a 3.9 broker but absent on 4.x (at its default value): a version difference, not drift
		Map<Integer, Config> byBroker = new TreeMap<>();
		byBroker.put(1, config(entry("log.message.format.version", "3.0-IV1", ConfigSource.DEFAULT_CONFIG),
				entry("zookeeper.ssl.client.enable", "false", ConfigSource.DEFAULT_CONFIG),
				entry("num.io.threads", "8", ConfigSource.DEFAULT_CONFIG)));
		byBroker.put(2, config(entry("num.io.threads", "8", ConfigSource.DEFAULT_CONFIG)));
		BrokerTools.Drift d = BrokerTools.drift(byBroker, name -> true);
		assertThat(d.differences()).isEmpty();
		assertThat(d.presenceOnly()).containsExactly("log.message.format.version", "zookeeper.ssl.client.enable");
	}

	private static Config config(ConfigEntry... entries) {
		return new Config(List.of(entries));
	}

	private static ConfigEntry entry(String name, String value, ConfigSource source) {
		return entry(name, value, source, false);
	}

	private static ConfigEntry entry(String name, String value, ConfigSource source, boolean sensitive) {
		return new ConfigEntry(name, value, source, sensitive, false, List.of(), ConfigEntry.ConfigType.STRING, null);
	}

}
