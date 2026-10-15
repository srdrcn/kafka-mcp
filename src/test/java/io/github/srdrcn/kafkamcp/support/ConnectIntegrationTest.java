package io.github.srdrcn.kafkamcp.support;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import io.github.srdrcn.kafkamcp.KafkaMcpApplication;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;

/**
 * {@link KafkaIntegrationTest} + Testcontainers Kafka Connect: {@code test} ve {@code test-ro}
 * cluster'larında connectUrl tanımlı.
 */
@SpringBootTest(classes = { KafkaMcpApplication.class, ConnectIntegrationTest.ConnectConfig.class },
		webEnvironment = SpringBootTest.WebEnvironment.NONE)
public abstract class ConnectIntegrationTest extends KafkaIntegrationTest {

	@TestConfiguration(proxyBeanMethods = false)
	public static class ConnectConfig {

		@Bean
		KafkaMcpConfig kafkaMcpConfig() {
			return TestConfigs.kafka(ConnectTestCluster.url(), null);
		}

	}

}
