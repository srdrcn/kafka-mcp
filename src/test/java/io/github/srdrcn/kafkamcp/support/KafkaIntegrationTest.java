package io.github.srdrcn.kafkamcp.support;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.KafkaMcpApplication;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.mcp.ToolRegistry;

/**
 * Application context connected to the Testcontainers Kafka (SASL/PLAIN) with all toolsets enabled. Tools
 * are called directly through {@link ToolRegistry}. Clusters: {@code test} (writable) and
 * {@code test-ro} (readOnly).
 */
@SpringBootTest(classes = { KafkaMcpApplication.class, KafkaIntegrationTest.Config.class },
		webEnvironment = SpringBootTest.WebEnvironment.NONE)
public abstract class KafkaIntegrationTest {

	private static final AtomicInteger SEQ = new AtomicInteger();

	private static Admin testAdmin;

	@Autowired
	protected ToolRegistry registry;

	@TestConfiguration(proxyBeanMethods = false)
	public static class Config {

		@Bean
		KafkaMcpConfig kafkaMcpConfig() {
			return TestConfigs.kafka(null, null);
		}

	}

	protected JsonNode ok(String tool, Map<String, Object> args) {
		return ToolAssertions.ok(registry, tool, args);
	}

	protected String error(String tool, Map<String, Object> args) {
		return ToolAssertions.error(registry, tool, args);
	}

	protected static Map<String, Object> args(Object... keyValues) {
		return ToolAssertions.args(keyValues);
	}

	/** Shared AdminClient with the admin user, for test setup. */
	protected static synchronized Admin testAdmin() {
		if (testAdmin == null) {
			testAdmin = KafkaTestCluster.admin();
		}
		return testAdmin;
	}

	/** Topic/group name that does not clash between tests. */
	protected static String unique(String prefix) {
		return prefix + "-" + System.nanoTime() % 1_000_000 + "-" + SEQ.incrementAndGet();
	}

	@AfterAll
	static synchronized void closeTestAdmin() {
		if (testAdmin != null) {
			testAdmin.close();
			testAdmin = null;
		}
	}

}
