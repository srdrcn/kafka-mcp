package io.github.srdrcn.kafkamcp.support;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.KafkaMcpApplication;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;

/**
 * {@link KafkaIntegrationTest} + paylaşılan Kafka Connect worker'ı ({@link ConnectTestCluster}); her iki
 * cluster'ın da connectUrl'i tanımlıdır. Connector'ları hazırlamak için küçük REST yardımcıları içerir.
 */
@SpringBootTest(classes = { KafkaMcpApplication.class, ConnectIntegrationTest.Config.class },
		webEnvironment = SpringBootTest.WebEnvironment.NONE)
public abstract class ConnectIntegrationTest extends KafkaIntegrationTest {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static final HttpClient HTTP = HttpClient.newBuilder()
		.version(HttpClient.Version.HTTP_1_1)
		.connectTimeout(Duration.ofSeconds(5))
		.build();

	@TestConfiguration(proxyBeanMethods = false)
	public static class Config {

		@Bean
		KafkaMcpConfig kafkaMcpConfig() {
			return TestConfigs.kafka(ConnectTestCluster.url(), null);
		}

	}

	/** Connect REST çağrısı (test hazırlığı); gövde JSON'a çevrilir, yanıt JSON döner. */
	protected static JsonNode connectRest(String method, String path, Object body) throws Exception {
		HttpRequest.BodyPublisher publisher = body == null ? HttpRequest.BodyPublishers.noBody()
				: HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body));
		HttpRequest req = HttpRequest.newBuilder(URI.create(ConnectTestCluster.url() + path))
			.timeout(Duration.ofSeconds(30))
			.header("Content-Type", "application/json")
			.header("Accept", "application/json")
			.method(method, publisher)
			.build();
		HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
		if (resp.statusCode() >= 300) {
			throw new IllegalStateException("Connect " + method + " " + path + " -> " + resp.statusCode() + ": "
					+ resp.body());
		}
		return resp.body() == null || resp.body().isBlank() ? MAPPER.nullNode() : MAPPER.readTree(resp.body());
	}

	/** Connector oluşturur (PUT /connectors/{name}/config; varsa günceller). */
	protected static void putConnector(String name, Map<String, String> config) throws Exception {
		connectRest("PUT", "/connectors/" + name + "/config", config);
	}

	/** Connector'ı siler; hata yutulur (temizlik). */
	protected static void deleteConnectorQuietly(String name) {
		try {
			connectRest("DELETE", "/connectors/" + name, null);
		}
		catch (Exception ignored) {
			// temizlik
		}
	}

}
