package io.github.srdrcn.kafkamcp.toolsets.acl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;
import io.github.srdrcn.kafkamcp.support.TestConfigs;

/** quotas_update ve quotas_list. */
class QuotaToolsTest extends KafkaIntegrationTest {

	private static Map<String, Double> quotasOfUser(String user) throws Exception {
		Map<ClientQuotaEntity, Map<String, Double>> m = testAdmin()
			.describeClientQuotas(
					ClientQuotaFilter.containsOnly(List.of(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, user))))
			.entities()
			.get(15, TimeUnit.SECONDS);
		return m.getOrDefault(new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, user)), Map.of());
	}

	@Test
	void setListAndRemoveUserQuota() throws Exception {
		String user = unique("quser");
		JsonNode set = ok("quotas_update",
				args("user", user, "set", Map.of("producer_byte_rate", 1048576, "consumer_byte_rate", "2097152")));
		assertThat(set.at("/entity/user").asText()).isEqualTo(user);
		assertThat(set.get("before").isEmpty()).isTrue();
		assertThat(set.at("/after/producer_byte_rate").asLong()).isEqualTo(1048576L);
		assertThat(set.at("/after/consumer_byte_rate").asLong()).isEqualTo(2097152L);
		assertThat(set.at("/after/producer_byte_rate").isIntegralNumber()).isTrue();
		assertThat(quotasOfUser(user)).containsEntry("producer_byte_rate", 1048576.0);

		JsonNode list = ok("quotas_list", args("filter", "user=" + user + "$"));
		assertThat(list.get("total").asInt()).isEqualTo(1);
		assertThat(list.at("/quotas/0/entity/user").asText()).isEqualTo(user);
		assertThat(list.at("/quotas/0/quotas/consumer_byte_rate").asLong()).isEqualTo(2097152L);

		JsonNode changed = ok("quotas_update", args("user", user, "set", Map.of("request_percentage", 50.5), "remove",
				List.of("consumer_byte_rate")));
		assertThat(changed.at("/before/consumer_byte_rate").asLong()).isEqualTo(2097152L);
		assertThat(changed.get("after").has("consumer_byte_rate")).isFalse();
		assertThat(changed.at("/after/request_percentage").asDouble()).isEqualTo(50.5);
		assertThat(changed.get("removed").toString()).contains("consumer_byte_rate");
		assertThat(quotasOfUser(user)).doesNotContainKey("consumer_byte_rate").containsKey("request_percentage");

		ok("quotas_update", args("user", user, "remove", List.of("producer_byte_rate", "request_percentage")));
		assertThat(quotasOfUser(user)).isEmpty();
		assertThat(ok("quotas_list", args("filter", "user=" + user + "$")).get("total").asInt()).isZero();
	}

	@Test
	void defaultClientIdAndIpEntities() {
		String user = unique("quser");
		JsonNode r = ok("quotas_update",
				args("user", user, "clientId", "<default>", "set", Map.of("controller_mutation_rate", 10)));
		assertThat(r.at("/entity/clientId").asText()).isEqualTo("<default>");
		assertThat(r.at("/after/controller_mutation_rate").asLong()).isEqualTo(10L);
		JsonNode list = ok("quotas_list", args("filter", "user=" + user + ",client-id=<default>"));
		assertThat(list.get("total").asInt()).isEqualTo(1);
		assertThat(list.at("/quotas/0/entity/clientId").asText()).isEqualTo("<default>");

		String ip = "10.255." + (System.nanoTime() % 200) + ".7";
		JsonNode ipSet = ok("quotas_update", args("ip", ip, "set", Map.of("connection_creation_rate", 5)));
		assertThat(ipSet.at("/entity/ip").asText()).isEqualTo(ip);
		assertThat(ipSet.at("/after/connection_creation_rate").asLong()).isEqualTo(5L);
		assertThat(ok("quotas_list", args("filter", "ip=" + ip.replace(".", "\\.") + "$")).get("total").asInt())
			.isEqualTo(1);
		JsonNode ipRemoved = ok("quotas_update", args("ip", ip, "remove", List.of("connection_creation_rate")));
		assertThat(ipRemoved.get("after").isEmpty()).isTrue();
		ok("quotas_update", args("user", user, "clientId", "<default>", "remove", List.of("controller_mutation_rate")));
	}

	@Test
	void validationAndReadOnly() {
		assertThat(error("quotas_update", args("set", Map.of("producer_byte_rate", 1)))).contains("En az bir entity");
		assertThat(error("quotas_update", args("user", "u", "ip", "1.2.3.4", "set", Map.of("producer_byte_rate", 1))))
			.contains("birlikte kullanılamaz");
		assertThat(error("quotas_update", args("user", "u", "set", Map.of("foo_rate", 1))))
			.contains("Geçersiz kota anahtarı");
		assertThat(error("quotas_update", args("ip", "1.2.3.4", "set", Map.of("producer_byte_rate", 1))))
			.contains("connection_creation_rate");
		assertThat(error("quotas_update", args("user", "u"))).contains("set ya da remove");
		assertThat(error("quotas_update", args("user", "u", "set", Map.of("producer_byte_rate", "hızlı"))))
			.contains("sayı olmalı");
		assertThat(error("quotas_update", args("user", "u", "set", Map.of("producer_byte_rate", 1), "remove",
				List.of("producer_byte_rate")))).contains("hem set hem remove");
		assertThat(error("quotas_update", args("cluster", TestConfigs.READ_ONLY_CLUSTER, "user", "u", "set",
				Map.of("producer_byte_rate", 1)))).contains("readOnly");
		assertThat(ok("quotas_list", args("cluster", TestConfigs.READ_ONLY_CLUSTER)).has("quotas")).isTrue();
	}

}
