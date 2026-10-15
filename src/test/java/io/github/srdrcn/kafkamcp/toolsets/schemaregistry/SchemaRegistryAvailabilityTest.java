package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.support.KafkaIntegrationTest;

/** schemaRegistryUrl tanımlı olmayan cluster'larda schemaregistry araçları listelenmez ve çağrılamaz. */
class SchemaRegistryAvailabilityTest extends KafkaIntegrationTest {

	@Test
	void toolsAreNotOfferedWithoutSchemaRegistryUrl() {
		assertThat(registry.tools()).noneMatch(t -> t.toolset().equals("schemaregistry"));
		assertThat(error("subjects_list", args())).contains("Bilinmeyen ya da kapalı araç");
		assertThat(error("schemas_register", args("subject", "x-value", "schema", SrRest.V1)))
			.contains("Bilinmeyen ya da kapalı araç");
	}

}
