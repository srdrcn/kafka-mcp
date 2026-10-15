package io.github.srdrcn.kafkamcp.support;

import java.util.List;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;

public final class TestConfigs {

	/** Yazılabilir test cluster'ı (tüm araçlar açık). */
	public static final String CLUSTER = "test";

	/** Aynı Kafka, readOnly: true override'ı ile. */
	public static final String READ_ONLY_CLUSTER = "test-ro";

	private TestConfigs() {
	}

	/** Testcontainers Kafka'sına bağlanan, tüm toolset'leri açık config. */
	public static KafkaMcpConfig kafka(String connectUrl, String schemaRegistryUrl) {
		KafkaMcpConfig c = new KafkaMcpConfig();
		c.setReadOnly(false);
		c.setDisableDestructive(false);
		c.setToolsets(TestToolsets.all());
		ClusterConfig rw = new ClusterConfig();
		rw.setName(CLUSTER);
		rw.setBootstrapServers(KafkaTestCluster.bootstrap());
		rw.setProperties(KafkaTestCluster.saslProperties("admin"));
		rw.setConnectUrl(connectUrl);
		rw.setSchemaRegistryUrl(schemaRegistryUrl);
		ClusterConfig ro = new ClusterConfig();
		ro.setName(READ_ONLY_CLUSTER);
		ro.setBootstrapServers(KafkaTestCluster.bootstrap());
		ro.setProperties(KafkaTestCluster.saslProperties("admin"));
		ro.setReadOnly(true);
		ro.setConnectUrl(connectUrl);
		ro.setSchemaRegistryUrl(schemaRegistryUrl);
		c.setClusters(List.of(rw, ro));
		return c;
	}

	/**
	 * Ağa hiç çıkmayan sabit config (şema/filtre testleri için): alpha (connect+SR, yazılabilir), beta
	 * (readOnly, connect/SR yok).
	 */
	public static KafkaMcpConfig offline() {
		KafkaMcpConfig c = new KafkaMcpConfig();
		c.setReadOnly(false);
		c.setDisableDestructive(false);
		c.setToolsets(TestToolsets.all());
		ClusterConfig a = new ClusterConfig();
		a.setName("alpha");
		a.setBootstrapServers("alpha.invalid:9092");
		a.setConnectUrl("http://alpha.invalid:8083");
		a.setSchemaRegistryUrl("http://alpha.invalid:8081");
		ClusterConfig b = new ClusterConfig();
		b.setName("beta");
		b.setBootstrapServers("beta.invalid:9092");
		b.setReadOnly(true);
		c.setClusters(List.of(a, b));
		return c;
	}

}
