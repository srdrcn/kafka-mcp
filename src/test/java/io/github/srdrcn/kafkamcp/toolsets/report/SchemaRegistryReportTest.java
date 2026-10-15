package io.github.srdrcn.kafkamcp.toolsets.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.SchemaRegistryIntegrationTest;
import io.github.srdrcn.kafkamcp.support.SchemaRegistryTestCluster;

/** Cluster with Schema Registry: subject count in the summary. */
class SchemaRegistryReportTest extends SchemaRegistryIntegrationTest {

	@Test
	void summaryCountsSchemaRegistrySubjects() throws Exception {
		String subject = unique("rep-sr") + "-value";
		HttpRequest req = HttpRequest.newBuilder(URI.create(SchemaRegistryTestCluster.url() + "/subjects/" + subject + "/versions"))
			.timeout(Duration.ofSeconds(30))
			.header("Content-Type", "application/vnd.schemaregistry.v1+json")
			.POST(HttpRequest.BodyPublishers.ofString("{\"schema\":\"\\\"string\\\"\"}"))
			.build();
		HttpResponse<String> resp = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
		assertThat(resp.statusCode()).as(resp.body()).isEqualTo(200);

		JsonNode s = ok("report_cluster_summary", args());
		assertThat(s.at("/schemaRegistry/subjects").asInt()).isGreaterThanOrEqualTo(1);
		assertThat(s.has("connect")).isFalse();
		assertThat(s.get("mode").asText()).isEqualTo("KRaft");
	}

}
