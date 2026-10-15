package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.KafkaMcpApplication;
import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.support.TestConfigs;

/**
 * diagnose_connectivity failure paths (separate test config): unresolvable host, closed port, wrong SASL password,
 * unreachable Connect/Schema Registry. Each comes back with an explanation and a recommendation; the tool does not
 * fail.
 */
@SpringBootTest(classes = { KafkaMcpApplication.class, DiagnoseConnectivityFailureTest.FailureConfig.class },
		webEnvironment = SpringBootTest.WebEnvironment.NONE)
class DiagnoseConnectivityFailureTest extends KafkaIntegrationTest {

	@TestConfiguration(proxyBeanMethods = false)
	static class FailureConfig {

		@Bean
		KafkaMcpConfig kafkaMcpConfig() {
			KafkaMcpConfig c = TestConfigs.kafka(null, null);
			List<ClusterConfig> clusters = new ArrayList<>(c.getClusters());
			clusters.add(cluster("dns-fail", "nonexistent.invalid:9092", Map.of()));
			clusters.add(cluster("port-closed", "127.0.0.1:1", Map.of()));
			Map<String, String> wrong = KafkaTestCluster.saslProperties("alice");
			wrong.put("sasl.jaas.config", wrong.get("sasl.jaas.config").replace("alice-secret", "wrong-secret"));
			clusters.add(cluster("bad-auth", KafkaTestCluster.bootstrap(), wrong));
			ClusterConfig rest = cluster("rest-down", KafkaTestCluster.bootstrap(), KafkaTestCluster.saslProperties("admin"));
			rest.setConnectUrl("http://127.0.0.1:1");
			rest.setSchemaRegistryUrl("http://127.0.0.1:1");
			clusters.add(rest);
			c.setClusters(clusters);
			return c;
		}

		private static ClusterConfig cluster(String name, String bootstrap, Map<String, String> props) {
			ClusterConfig cc = new ClusterConfig();
			cc.setName(name);
			cc.setBootstrapServers(bootstrap);
			cc.setProperties(props);
			return cc;
		}

	}

	private static JsonNode step(JsonNode r, String name) {
		for (JsonNode s : r.get("steps")) {
			if (name.equals(s.get("step").asText())) {
				return s;
			}
		}
		throw new AssertionError("No " + name + " step: " + r.get("steps"));
	}

	@Test
	void unresolvableHostIsExplained() {
		long start = System.nanoTime();
		JsonNode r = ok("diagnose_connectivity", args("cluster", "dns-fail"));
		assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(15_000);
		assertThat(r.get("status").asText()).isEqualTo("critical");
		JsonNode f = DiagnoseTestBase.finding(r, "DNS_UNRESOLVED");
		assertThat(f.get("severity").asText()).isEqualTo("critical");
		assertThat(f.get("message").asText()).contains("DNS", "nonexistent.invalid");
		assertThat(f.get("recommendation").asText()).contains("OpenShift");
		assertThat(step(r, "dns").get("status").asText()).isEqualTo("error");
		JsonNode kafka = step(r, "kafka");
		assertThat(kafka.get("status").asText()).isEqualTo("skipped");
		assertThat(kafka.get("reason").asText()).contains("TCP");
	}

	@Test
	void closedPortIsExplained() {
		JsonNode r = ok("diagnose_connectivity", args("cluster", "port-closed"));
		assertThat(r.get("status").asText()).isEqualTo("critical");
		JsonNode f = DiagnoseTestBase.finding(r, "CONNECTION_REFUSED");
		assertThat(f.get("message").asText()).contains("Connection refused", "127.0.0.1:1");
		assertThat(f.get("recommendation").asText()).contains("listeners");
		assertThat(step(r, "dns").get("status").asText()).isEqualTo("ok");
		assertThat(step(r, "tcp").get("status").asText()).isEqualTo("error");
		assertThat(step(r, "kafka").get("status").asText()).isEqualTo("skipped");
	}

	@Test
	void wrongSaslPasswordIsExplained() {
		JsonNode r = ok("diagnose_connectivity", args("cluster", "bad-auth"));
		assertThat(r.get("status").asText()).isEqualTo("critical");
		JsonNode f = DiagnoseTestBase.finding(r, "SASL_AUTH_FAILED");
		assertThat(f.get("message").asText()).contains("SASL authentication failed");
		assertThat(f.get("recommendation").asText()).contains("sasl.jaas.config");
		assertThat(step(r, "tcp").get("status").asText()).isEqualTo("ok");
		assertThat(step(r, "kafka").get("status").asText()).isEqualTo("error");
		assertThat(r.toString()).doesNotContain("wrong-secret");
	}

	@Test
	void unreachableConnectAndSchemaRegistryAreWarnings() {
		JsonNode r = ok("diagnose_connectivity", args("cluster", "rest-down"));
		assertThat(r.get("status").asText()).isEqualTo("warning");
		assertThat(step(r, "kafka").get("status").asText()).isEqualTo("ok");
		assertThat(step(r, "connect").get("status").asText()).isEqualTo("error");
		assertThat(step(r, "schemaRegistry").get("status").asText()).isEqualTo("error");
		assertThat(DiagnoseTestBase.finding(r, "CONNECT_UNREACHABLE").get("message").asText())
			.contains("Connection refused");
		assertThat(DiagnoseTestBase.finding(r, "SCHEMA_REGISTRY_UNREACHABLE").get("severity").asText())
			.isEqualTo("warning");
	}

}
