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
 * Schema Registry REST client (one {@link RestClient} per cluster). Errors are turned into readable
 * {@link ToolException}s.
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
			throw new ToolException("No schemaRegistryUrl configured for cluster " + cluster);
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

	/** path: like {@code /subjects/{subject}/versions}; encode the segments with {@link #enc(String)}. */
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
			int status = e.getStatusCode().value();
			String responseBody = e.getResponseBodyAsString();
			int code = errorCode(responseBody);
			String message = srMessage(responseBody);
			throw new SchemaRegistryException(status, code, message,
					"Schema Registry error " + status + " (" + method + " " + path + "): " + describe(status, code, message),
					e);
		}
		catch (ResourceAccessException e) {
			throw new UnreachableException(
					"Cannot reach Schema Registry (" + base + "): " + e.getMostSpecificCause().getMessage(), e);
		}
		catch (java.io.IOException e) {
			throw new ToolException("Schema Registry response is not JSON (" + method + " " + path + ")", e);
		}
	}

	/** error_code from the SR error body; -1 if absent. */
	static int errorCode(String body) {
		try {
			JsonNode n = MAPPER.readTree(body);
			if (n != null && n.has("error_code") && n.get("error_code").canConvertToInt()) {
				return n.get("error_code").asInt();
			}
		}
		catch (Exception ignored) {
			// body is not JSON
		}
		return -1;
	}

	/** Hint + the SR error_code and message: {@code "Subject not found (40401 Subject 'x' not found.)"}. */
	static String describe(int status, int code, String message) {
		String detail = ((code > 0 ? code + " " : "") + (message == null ? "" : message)).trim();
		String hint = hint(status, code);
		if (hint == null) {
			return detail.isEmpty() ? String.valueOf(status) : detail;
		}
		return detail.isEmpty() ? hint : hint + " (" + detail + ")";
	}

	/** Short explanation of known SR error_codes (otherwise of the HTTP status); null if unknown. */
	static String hint(int status, int code) {
		String byCode = switch (code) {
			case 40401 -> "Subject not found";
			case 40402 -> "Version not found";
			case 40403 -> "Schema not found";
			case 40404 -> "Subject is soft-deleted";
			case 40405 -> "Subject is not soft-deleted (soft delete it before deleting permanently)";
			case 40406 -> "Schema version is already soft-deleted (use permanent=true to delete it permanently)";
			case 40407 -> "Schema version is not soft-deleted (soft delete it before deleting permanently)";
			case 40408 -> "No subject-level compatibility configured";
			case 40409 -> "No subject-level mode configured";
			case 409 -> "Incompatible schema: the compatibility rule rejected the new schema";
			case 42201 -> "Invalid schema";
			case 42202 -> "Invalid version";
			case 42203 -> "Invalid compatibility level";
			case 42204 -> "Invalid mode";
			case 42205 -> "Operation not permitted (the registry or subject may be in READONLY/IMPORT mode)";
			case 42206 -> "Schema is referenced by other schemas; delete the referencing schemas first";
			case 42208 -> "Invalid subject name";
			case 42209 -> "Schema too large";
			case 50001, 50002, 50003, 50004 -> "Schema Registry internal error (store/leader/forward)";
			default -> null;
		};
		if (byCode != null) {
			return byCode;
		}
		return switch (status) {
			case 401 -> "Authentication required (401)";
			case 403 -> "Not authorized (403)";
			case 404 -> "Not found";
			case 409 -> "Conflict (409)";
			case 422 -> "Invalid request (422)";
			default -> null;
		};
	}

	/** The message field of the SR error body (without error_code); the body itself if it is not JSON. */
	static String srMessage(String body) {
		try {
			JsonNode n = MAPPER.readTree(body);
			if (n != null && n.hasNonNull("message")) {
				return n.get("message").asText();
			}
		}
		catch (Exception ignored) {
			// body is not JSON (e.g. an HTML error page from a proxy)
		}
		return body == null || body.length() <= 500 ? body : body.substring(0, 500) + "…";
	}

	/**
	 * HTTP error returned by Schema Registry: status code, SR {@code error_code} (-1 if absent) and SR message. Its
	 * message is the hint + the SR message.
	 */
	public static class SchemaRegistryException extends ToolException {

		private final int status;

		private final int errorCode;

		private final String srMessage;

		public SchemaRegistryException(int status, int errorCode, String srMessage, String message, Throwable cause) {
			super(message, cause);
			this.status = status;
			this.errorCode = errorCode;
			this.srMessage = srMessage;
		}

		public int status() {
			return status;
		}

		public int errorCode() {
			return errorCode;
		}

		public String srMessage() {
			return srMessage;
		}

	}

	/**
	 * Schema Registry could not be reached (connection refused, DNS, I/O timeout); no HTTP response came back. Callers
	 * can check the type instead of the message, e.g. to stop further lookups while the registry is down.
	 */
	public static class UnreachableException extends ToolException {

		public UnreachableException(String message, Throwable cause) {
			super(message, cause);
		}

	}

	/**
	 * URL path segment encoding: every character outside the unreserved set is encoded. If sub-delims such as ';'
	 * stayed as is, Schema Registry (Jetty) would treat the part after ';' as a matrix parameter, drop it and hit
	 * another subject.
	 */
	public static String enc(String segment) {
		// "." and ".." stay as they are when encoded (unreserved) and the server normalizes them away, so the request
		// would hit another resource (e.g. /config/. becomes /config, the global level)
		if (segment == null || segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
			throw new ToolException("Invalid name: '" + segment + "' (empty, '.' and '..' are not allowed in a "
					+ "Schema Registry path)");
		}
		return org.springframework.web.util.UriUtils.encode(segment, java.nio.charset.StandardCharsets.UTF_8);
	}

	static RestClient build() {
		HttpClient http = HttpClient.newBuilder()
			.version(HttpClient.Version.HTTP_1_1)
			.connectTimeout(Duration.ofSeconds(5))
			.build();
		JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(http);
		factory.setReadTimeout(Duration.ofMillis(KafkaClients.TIMEOUT_MS - 1_000));
		return RestClient.builder().requestFactory(factory).build();
	}

}
