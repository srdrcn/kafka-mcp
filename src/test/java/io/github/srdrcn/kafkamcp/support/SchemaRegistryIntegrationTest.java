package io.github.srdrcn.kafkamcp.support;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import io.github.srdrcn.kafkamcp.KafkaMcpApplication;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;

/**
 * {@link KafkaIntegrationTest} + Testcontainers Schema Registry: the {@code test} and {@code test-ro}
 * clusters have a schemaRegistryUrl.
 */
@SpringBootTest(classes = { KafkaMcpApplication.class, SchemaRegistryIntegrationTest.SrConfig.class },
		webEnvironment = SpringBootTest.WebEnvironment.NONE)
public abstract class SchemaRegistryIntegrationTest extends KafkaIntegrationTest {

	@TestConfiguration(proxyBeanMethods = false)
	public static class SrConfig {

		@Bean
		KafkaMcpConfig kafkaMcpConfig() {
			return TestConfigs.kafka(null, SchemaRegistryTestCluster.url());
		}

	}

}
