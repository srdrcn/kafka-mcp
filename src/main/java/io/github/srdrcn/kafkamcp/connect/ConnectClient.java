package io.github.srdrcn.kafkamcp.connect;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.http.HttpStatusCode;
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
 * Kafka Connect REST client'ı (cluster başına bir {@link RestClient}). Hatalar Türkçe
 * {@link ToolException}'a çevrilir.
 */
@Component
public class ConnectClient {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private final KafkaClients clients;

	private final Map<String, RestClient> rest = new ConcurrentHashMap<>();

	public ConnectClient(KafkaClients clients) {
		this.clients = clients;
	}

	public String baseUrl(String cluster) {
		ClusterConfig c = clients.cluster(cluster);
		if (!c.hasConnect()) {
			throw new ToolException("cluster " + cluster + " için connectUrl tanımlı değil");
		}
		return c.getConnectUrl().replaceAll("/+$", "");
	}

	public JsonNode get(String cluster, String path) {
		return exchange(cluster, "GET", path, null);
	}

	public JsonNode post(String cluster, String path, Object body) {
		return exchange(cluster, "POST", path, body);
	}

	public JsonNode put(String cluster, String path, Object body) {
		return exchange(cluster, "PUT", path, body);
	}

	public JsonNode patch(String cluster, String path, Object body) {
		return exchange(cluster, "PATCH", path, body);
	}

	public JsonNode delete(String cluster, String path) {
		return exchange(cluster, "DELETE", path, null);
	}

	/**
	 * path: {@code /connectors/{name}} gibi, başında "/" ile. Path parçalarını
	 * {@link #enc(String)} ile kodlayın. Boş gövde → {@code NullNode}.
	 */
	public JsonNode exchange(String cluster, String method, String path, Object body) {
		return send(cluster, method, path, body).body();
	}

	/** HTTP durum kodu ve gövde (boş gövde → {@code NullNode}). */
	public record Response(int status, JsonNode body) {
	}

	/** {@link #exchange} gibi, ama HTTP durum kodunu da döner (ör. 201 oluşturuldu / 200 güncellendi). */
	public Response send(String cluster, String method, String path, Object body) {
		String base = baseUrl(cluster);
		RestClient client = rest.computeIfAbsent(cluster, c -> build(base));
		try {
			RestClient.RequestBodySpec spec = client.method(org.springframework.http.HttpMethod.valueOf(method))
				.uri(java.net.URI.create(base + path))
				.accept(MediaType.APPLICATION_JSON);
			if (body != null) {
				spec.contentType(MediaType.APPLICATION_JSON).body(body);
			}
			org.springframework.http.ResponseEntity<String> entity = spec.retrieve().toEntity(String.class);
			String text = entity.getBody();
			return new Response(entity.getStatusCode().value(),
					text == null || text.isBlank() ? MAPPER.nullNode() : MAPPER.readTree(text));
		}
		catch (RestClientResponseException e) {
			throw new ToolException("Connect REST hatası " + e.getStatusCode().value() + " (" + method + " " + path + "): "
					+ errorMessage(e.getResponseBodyAsString(), e.getStatusCode()), e);
		}
		catch (ResourceAccessException e) {
			throw new ToolException("Connect'e ulaşılamadı (" + base + "): " + e.getMostSpecificCause().getMessage(), e);
		}
		catch (java.io.IOException e) {
			throw new ToolException("Connect yanıtı JSON değil (" + method + " " + path + ")", e);
		}
	}

	static String errorMessage(String body, HttpStatusCode status) {
		try {
			JsonNode n = MAPPER.readTree(body);
			if (n != null && n.hasNonNull("message")) {
				return n.get("message").asText();
			}
		}
		catch (Exception ignored) {
			// gövde JSON değil
		}
		return body == null || body.isBlank() ? status.toString() : body;
	}

	/**
	 * URL path parçası kodlaması (connector adlarında boşluk/özel karakter olabilir). Unreserved dışındaki her
	 * karakter kodlanır: ';' gibi sub-delim'ler açık kalırsa Connect (Jetty) ';' sonrasını matrix parametresi
	 * sayıp atar ve araç başka bir connector'a gider.
	 */
	public static String enc(String segment) {
		return org.springframework.web.util.UriUtils.encode(segment, java.nio.charset.StandardCharsets.UTF_8);
	}

	static RestClient build(String base) {
		HttpClient http = HttpClient.newBuilder()
			.version(HttpClient.Version.HTTP_1_1)
			.connectTimeout(Duration.ofSeconds(5))
			.build();
		JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(http);
		factory.setReadTimeout(Duration.ofMillis(KafkaClients.TIMEOUT_MS - 1_000));
		return RestClient.builder().requestFactory(factory).build();
	}

}
