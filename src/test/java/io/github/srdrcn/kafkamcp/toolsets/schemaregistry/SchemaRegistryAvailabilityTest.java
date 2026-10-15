package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;

/** On clusters without a schemaRegistryUrl the schemaregistry tools are neither listed nor callable. */
class SchemaRegistryAvailabilityTest extends KafkaIntegrationTest {

	@Test
	void toolsAreNotOfferedWithoutSchemaRegistryUrl() {
		assertThat(registry.tools()).noneMatch(t -> t.toolset().equals("schemaregistry"));
		assertThat(error("subjects_list", args())).contains("Unknown or disabled tool");
		assertThat(error("schemas_register", args("subject", "x-value", "schema", SrRest.V1)))
			.contains("Unknown or disabled tool");
	}

}
