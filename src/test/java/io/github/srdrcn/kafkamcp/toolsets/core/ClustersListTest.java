package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;

class ClustersListTest extends KafkaIntegrationTest {

	@Test
	void listsAllClustersWithStatus() {
		JsonNode r = ok("clusters_list", args());
		assertThat(r.get("total").asInt()).isEqualTo(1);
		JsonNode c = r.get("clusters").get(0);
		assertThat(c.get("name").asText()).isEqualTo("test");
		assertThat(c.get("status").asText()).isEqualTo("ok");
		assertThat(c.get("clusterId").asText()).isNotBlank();
		assertThat(c.get("brokerCount").asInt()).isEqualTo(1);
		assertThat(c.get("readOnly").asBoolean()).isFalse();

		JsonNode all = ok("clusters_list", java.util.Map.of("check", false));
		assertThat(all.get("total").asInt()).isEqualTo(2);
		assertThat(all.get("clusters").get(1).get("readOnly").asBoolean()).isTrue();
		assertThat(all.get("clusters").get(1).has("status")).isFalse();

		JsonNode filtered = ok("clusters_list", java.util.Map.of("filter", "-ro$", "check", false));
		assertThat(filtered.get("total").asInt()).isEqualTo(1);
	}

}
