package io.github.srdrcn.kafkamcp.schemaregistry;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Schema Registry REST client'ı (cluster başına bir {@link RestClient}). Hatalar Türkçe
 * {@link ToolException}'a çevrilir.
 */
@Component
public class SchemaRegistryClient {

	public static final MediaType SR_JSON = MediaType.parseMediaType("application/vnd.schemaregistry.v1+json");

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private final KafkaClients clients;

	private final Map<String, RestClient> rest = new ConcurrentHashMap<>();

	public SchemaRegistryClient(KafkaClients clients) {
		this.clients = clients;
	}

	public String baseUrl(String cluster) {
		ClusterConfig c = clients.cluster(cluster);
		if (!c.hasSchemaRegistry()) {
			throw new ToolException("cluster " + cluster + " için schemaRegistryUrl tanımlı değil");
		}
		return c.getSchemaRegistryUrl().replaceAll("/+$", "");
	}

	public JsonNode get(String cluster, String path) {
		return exchange(cluster, HttpMethod.GET, path, null);
	}

	public JsonNode post(String cluster, String path, Object body) {
		return exchange(cluster, HttpMethod.POST, path, body);
	}

	public JsonNode put(String cluster, String path, Object body) {
		return exchange(cluster, HttpMethod.PUT, path, body);
	}

	public JsonNode delete(String cluster, String path) {
		return exchange(cluster, HttpMethod.DELETE, path, null);
	}

	/** path: {@code /subjects/{subject}/versions} gibi; parçaları {@link #enc(String)} ile kodlayın. */
	public JsonNode exchange(String cluster, HttpMethod method, String path, Object body) {
		String base = baseUrl(cluster);
		RestClient client = rest.computeIfAbsent(cluster, c -> build());
		try {
			RestClient.RequestBodySpec spec = client.method(method)
				.uri(java.net.URI.create(base + path))
				.accept(SR_JSON, MediaType.APPLICATION_JSON);
			if (body != null) {
				spec.contentType(SR_JSON).body(body);
			}
			String text = spec.retrieve().body(String.class);
			return text == null || text.isBlank() ? MAPPER.nullNode() : MAPPER.readTree(text);
		}
		catch (RestClientResponseException e) {
			throw new ToolException("Schema Registry hatası " + e.getStatusCode().value() + " (" + method + " " + path
					+ "): " + errorMessage(e.getResponseBodyAsString()), e);
		}
		catch (ResourceAccessException e) {
			throw new ToolException(
					"Schema Registry'ye ulaşılamadı (" + base + "): " + e.getMostSpecificCause().getMessage(), e);
		}
		catch (java.io.IOException e) {
			throw new ToolException("Schema Registry yanıtı JSON değil (" + method + " " + path + ")", e);
		}
	}

	static String errorMessage(String body) {
		try {
			JsonNode n = MAPPER.readTree(body);
			if (n != null && n.hasNonNull("message")) {
				return (n.hasNonNull("error_code") ? n.get("error_code").asText() + " " : "") + n.get("message").asText();
			}
		}
		catch (Exception ignored) {
			// gövde JSON değil
		}
		return body;
	}

	public static String enc(String segment) {
		return org.springframework.web.util.UriUtils.encodePathSegment(segment, java.nio.charset.StandardCharsets.UTF_8);
	}

	static RestClient build() {
		HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
		JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(http);
		factory.setReadTimeout(Duration.ofMillis(KafkaClients.TIMEOUT_MS - 1_000));
		return RestClient.builder().requestFactory(factory).build();
	}

}
