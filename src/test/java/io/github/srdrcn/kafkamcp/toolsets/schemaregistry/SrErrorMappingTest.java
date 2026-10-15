package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient.SchemaRegistryException;

/** schemas_register / schemas_compatibility_check 422 eşlemesi: sadece 42201 "Geçersiz şema" sayılır. */
class SrErrorMappingTest {

	private static SchemaRegistryException sr(int status, int code, String srMessage, String message) {
		return new SchemaRegistryException(status, code, srMessage, message, null);
	}

	@Test
	void onlyInvalidSchemaCodeIsReportedAsInvalidSchema() {
		ToolException invalid = SchemaTools.invalidSchemaError(sr(422, 42201,
				"Invalid schema {\"type\":\"record\"} with refs [] of type AVRO, details: No type", "x"));
		assertThat(invalid).isNotInstanceOf(SchemaRegistryException.class);
		assertThat(invalid.getMessage()).startsWith("Geçersiz şema (422/42201): ")
			.contains("Invalid schema: No type")
			.doesNotContain("with refs");

		// READONLY/IMPORT modu, id uyuşmazlığı, geçersiz subject, çok büyük şema: istemcinin ipucu korunur
		for (int code : new int[] { 42205, 42207, 42208, 42209, -1 }) {
			SchemaRegistryException e = sr(422, code, "Subject x in read-only mode",
					"Schema Registry hatası 422 (POST /subjects/x/versions): İşleme izin verilmiyor (READONLY/IMPORT)");
			ToolException mapped = SchemaTools.invalidSchemaError(e);
			assertThat(mapped).as("code %d", code).isSameAs(e);
			assertThat(mapped.getMessage()).doesNotContain("Geçersiz şema");
		}

		SchemaRegistryException notFound = sr(404, 40401, "Subject 'x' not found.", "Subject bulunamadı");
		assertThat(SchemaTools.invalidSchemaError(notFound)).isSameAs(notFound);
	}

}
