package io.github.srdrcn.kafkamcp.support;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import io.github.srdrcn.kafkamcp.KafkaMcpApplication;
import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;

/**
 * Karışık ortam: aynı sunucuda KRaft ({@code test}, {@code test-ro}; Kafka 4.1) ve ZooKeeper
 * ({@code zk}; Kafka 3.9 / CP 7.9, AclAuthorizer) cluster'ları. Her araç her iki tipte de çalışmalı;
 * ZooKeeper cluster'ı için hazırlık {@link #zkAdmin()} ve {@link ZkKafkaTestCluster#producer}.
 */
@SpringBootTest(classes = { KafkaMcpApplication.class, MixedClusterIntegrationTest.MixedConfig.class },
		webEnvironment = SpringBootTest.WebEnvironment.NONE)
public abstract class MixedClusterIntegrationTest extends KafkaIntegrationTest {

	/** ZooKeeper modlu, yazılabilir cluster. */
	public static final String ZK_CLUSTER = "zk";

	private static Admin zkAdmin;

	@TestConfiguration(proxyBeanMethods = false)
	public static class MixedConfig {

		@Bean
		KafkaMcpConfig kafkaMcpConfig() {
			KafkaMcpConfig c = TestConfigs.kafka(null, null);
			ClusterConfig zk = new ClusterConfig();
			zk.setName(ZK_CLUSTER);
			zk.setBootstrapServers(ZkKafkaTestCluster.bootstrap());
			zk.setProperties(KafkaTestCluster.saslProperties("admin"));
			List<ClusterConfig> clusters = new ArrayList<>(c.getClusters());
			clusters.add(zk);
			c.setClusters(clusters);
			return c;
		}

	}

	/** {@code args(...)} gibi ama cluster=zk. */
	protected static java.util.Map<String, Object> zkArgs(Object... keyValues) {
		java.util.Map<String, Object> m = args(keyValues);
		m.put("cluster", ZK_CLUSTER);
		return m;
	}

	protected static synchronized Admin zkAdmin() {
		if (zkAdmin == null) {
			zkAdmin = ZkKafkaTestCluster.admin();
		}
		return zkAdmin;
	}

	@AfterAll
	static synchronized void closeZkAdmin() {
		if (zkAdmin != null) {
			zkAdmin.close();
			zkAdmin = null;
		}
	}

}
