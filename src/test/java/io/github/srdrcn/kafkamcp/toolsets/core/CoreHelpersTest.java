package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.kafka.Configs;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Unit tests of the pure helpers in the core package (no Kafka needed). */
class CoreHelpersTest {

	@Test
	void quorumLagIsUnknownWhenLogEndOffsetIsUnknown() {
		assertThat(ClusterTools.quorumLag(1_000, 990)).isEqualTo(10);
		assertThat(ClusterTools.quorumLag(1_000, 1_005)).isZero();
		assertThat(ClusterTools.quorumLag(12_345_678, -1)).isNull();
	}

	@Test
	void failedBrokersAreKeyedByBrokerId() {
		Map<ConfigResource, String> errors = new LinkedHashMap<>();
		errors.put(Configs.broker(3), "No response (timeout)");
		errors.put(Configs.broker(1), "Not authorized (ACL)");
		assertThat(BrokerTools.byBrokerId(errors)).containsExactly(Map.entry(1, "Not authorized (ACL)"),
				Map.entry(3, "No response (timeout)"));
	}

	@Test
	void integerListsAcceptArraysNumbersAndStringsButNeverSilentlyDrop() {
		assertThat(Params.intList(List.of(2, 3), "replicas")).containsExactly(2, 3);
		assertThat(Params.intList(2, "replicas")).containsExactly(2);
		assertThat(Params.intList("2,3", "replicas")).containsExactly(2, 3);
		assertThat(Params.intList("[2, 3]", "replicas")).containsExactly(2, 3);
		assertThat(Params.intList(null, "replicas")).isEmpty();
		assertThat(Params.intList(List.of(), "replicas")).isEmpty();
		assertThat(Params.intList("[]", "replicas")).isEmpty();
		assertThatThrownBy(() -> Params.intList("abc", "replicas")).isInstanceOf(ToolException.class)
			.hasMessageContaining("Expected an integer");
		assertThatThrownBy(() -> Params.intList(Arrays.asList(1, null), "replicas")).isInstanceOf(ToolException.class);
		assertThatThrownBy(() -> Params.intList(Map.of("a", 1), "replicas")).isInstanceOf(ToolException.class);
	}

}
