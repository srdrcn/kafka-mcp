package io.github.srdrcn.kafkamcp.support;

import java.util.List;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;

public final class TestConfigs {

	/** Writable test cluster (all tools enabled). */
	public static final String CLUSTER = "test";

	/** Same Kafka, with a readOnly: true override. */
	public static final String READ_ONLY_CLUSTER = "test-ro";

	private TestConfigs() {
	}

	/** Config connected to the Testcontainers Kafka with all toolsets enabled. */
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
	 * Fixed config that never touches the network (for schema/filter tests): alpha (connect+SR, writable), beta
	 * (readOnly, no connect/SR).
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
