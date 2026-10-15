package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;
import org.junit.jupiter.api.Test;

/** brokers_config_drift karşılaştırma mantığı (çok broker'lı durum; test cluster'ı tek broker). */
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
		assertThat(retention.get(1)).containsEntry("value", "(yok)").containsEntry("brokers", List.of(3));
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
