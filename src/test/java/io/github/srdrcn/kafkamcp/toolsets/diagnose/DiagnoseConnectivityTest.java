package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

/** diagnose_connectivity success path: all steps ok on KRaft and ZooKeeper clusters. */
class DiagnoseConnectivityTest extends DiagnoseTestBase {

	private static List<String> steps(JsonNode r) {
		List<String> out = new ArrayList<>();
		r.get("steps").forEach(s -> out.add(s.get("step").asText() + ":" + s.get("status").asText()));
		return out;
	}

	@Test
	void allStepsSucceedOnBothClusters() throws Exception {
		for (String cluster : CLUSTERS) {
			JsonNode r = ok("diagnose_connectivity", on(cluster));
			assertThat(r.get("status").asText()).as(r.toString()).isEqualTo("ok");
			assertThat(codes(r)).doesNotContain("DNS_UNRESOLVED", "CONNECTION_REFUSED", "KAFKA_TIMEOUT",
					"ADVERTISED_UNREACHABLE", "SASL_AUTH_FAILED");
			assertThat(steps(r)).containsExactly("dns:ok", "tcp:ok", "clientSecurity:ok", "kafka:ok", "advertised:ok");
			for (JsonNode s : r.get("steps")) {
				if (!"clientSecurity".equals(s.get("step").asText())) {
					assertThat(s.get("durationMs").asLong()).as(s.toString()).isGreaterThanOrEqualTo(0);
				}
			}
			assertThat(r.get("mode").asText()).isEqualTo(zk(cluster) ? "ZooKeeper" : "KRaft");
			JsonNode kafka = r.at("/steps/3");
			assertThat(kafka.get("client").asText()).contains("new AdminClient");
			String clusterId = admin(cluster).describeCluster().clusterId().get(30, TimeUnit.SECONDS);
			assertThat(kafka.get("clusterId").asText()).isEqualTo(clusterId);
			assertThat(r.at("/facts/clusterId").asText()).isEqualTo(clusterId);
			assertThat(r.at("/facts/brokers/0/reachable").asBoolean()).isTrue();
			assertThat(r.at("/facts/client/securityProtocol").asText()).isEqualTo("SASL_PLAINTEXT");
			assertThat(r.at("/facts/client/saslMechanism").asText()).isEqualTo("PLAIN");
			assertThat(r.toString()).doesNotContain("admin-secret").contains("****");
		}
	}

}
