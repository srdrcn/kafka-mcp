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
 * Kafka Connect REST client (one {@link RestClient} per cluster). Errors are turned into readable
 * {@link ToolException}s.
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
			throw new ToolException("No connectUrl configured for cluster " + cluster);
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
	 * path: like {@code /connectors/{name}}, starting with "/". Encode path segments with
	 * {@link #enc(String)}. Empty body → {@code NullNode}.
	 */
	public JsonNode exchange(String cluster, String method, String path, Object body) {
		return send(cluster, method, path, body).body();
	}

	/** HTTP status code and body (empty body → {@code NullNode}). */
	public record Response(int status, JsonNode body) {
	}

	/** Like {@link #exchange}, but also returns the HTTP status code (e.g. 201 created / 200 updated). */
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
			throw new ToolException("Connect REST error " + e.getStatusCode().value() + " (" + method + " " + path + "): "
					+ errorMessage(e.getResponseBodyAsString(), e.getStatusCode()), e);
		}
		catch (ResourceAccessException e) {
			throw new ToolException("Cannot reach Connect (" + base + "): " + e.getMostSpecificCause().getMessage(), e);
		}
		catch (java.io.IOException e) {
			throw new ToolException("Connect response is not JSON (" + method + " " + path + ")", e);
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
			// body is not JSON
		}
		return body == null || body.isBlank() ? status.toString() : body;
	}

	/**
	 * URL path segment encoding (connector names may contain spaces/special characters). Every character outside
	 * the unreserved set is encoded: if sub-delims such as ';' stayed as is, Connect (Jetty) would treat the part
	 * after ';' as a matrix parameter and drop it, and the tool would hit another connector.
	 */
	public static String enc(String segment) {
		// "." and ".." stay as they are when encoded (unreserved) and the server normalizes them away, so the request
		// would hit another resource (e.g. /config/. becomes /config, the global level)
		if (segment == null || segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
			throw new ToolException("Invalid name: '" + segment + "' (empty, '.' and '..' are not allowed in a "
					+ "Connect path)");
		}
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
