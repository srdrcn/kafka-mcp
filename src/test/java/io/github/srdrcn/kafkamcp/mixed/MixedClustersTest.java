package io.github.srdrcn.kafkamcp.mixed;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.MixedClusterIntegrationTest;

/** KRaft and ZooKeeper clusters on the same server: both are reachable and queried with the same tool. */
class MixedClustersTest extends MixedClusterIntegrationTest {

	@Test
	void bothClusterTypesAreReachable() {
		JsonNode r = ok("clusters_list", Map.of());
		assertThat(r.get("total").asInt()).isEqualTo(3);
		for (JsonNode c : r.get("clusters")) {
			assertThat(c.get("status").asText()).as(c.get("name").asText()).isEqualTo("ok");
			assertThat(c.get("brokerCount").asInt()).isEqualTo(1);
		}
		JsonNode zk = ok("clusters_list", zkArgs());
		assertThat(zk.get("clusters").get(0).get("name").asText()).isEqualTo("zk");
	}

}
