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
			int status = e.getStatusCode().value();
			String responseBody = e.getResponseBodyAsString();
			int code = errorCode(responseBody);
			String message = srMessage(responseBody);
			throw new SchemaRegistryException(status, code, message,
					"Schema Registry hatası " + status + " (" + method + " " + path + "): " + describe(status, code, message),
					e);
		}
		catch (ResourceAccessException e) {
			throw new ToolException(
					"Schema Registry'ye ulaşılamadı (" + base + "): " + e.getMostSpecificCause().getMessage(), e);
		}
		catch (java.io.IOException e) {
			throw new ToolException("Schema Registry yanıtı JSON değil (" + method + " " + path + ")", e);
		}
	}

	/** SR hata gövdesindeki error_code; yoksa -1. */
	static int errorCode(String body) {
		try {
			JsonNode n = MAPPER.readTree(body);
			if (n != null && n.has("error_code") && n.get("error_code").canConvertToInt()) {
				return n.get("error_code").asInt();
			}
		}
		catch (Exception ignored) {
			// gövde JSON değil
		}
		return -1;
	}

	/** Türkçe ipucu + SR'ın error_code ve mesajı: {@code "Subject bulunamadı (40401 Subject 'x' not found.)"}. */
	static String describe(int status, int code, String message) {
		String detail = ((code > 0 ? code + " " : "") + (message == null ? "" : message)).trim();
		String hint = hint(status, code);
		if (hint == null) {
			return detail.isEmpty() ? String.valueOf(status) : detail;
		}
		return detail.isEmpty() ? hint : hint + " (" + detail + ")";
	}

	/** Bilinen SR error_code'larının (yoksa HTTP durumunun) kısa Türkçe açıklaması; bilinmiyorsa null. */
	static String hint(int status, int code) {
		String byCode = switch (code) {
			case 40401 -> "Subject bulunamadı";
			case 40402 -> "Versiyon bulunamadı";
			case 40403 -> "Şema bulunamadı";
			case 40404 -> "Subject soft-delete edilmiş";
			case 40405 -> "Subject soft-delete edilmemiş (kalıcı silmeden önce soft delete gerekir)";
			case 40406 -> "Şema versiyonu zaten soft-delete edilmiş (kalıcı silmek için permanent=true)";
			case 40407 -> "Şema versiyonu soft-delete edilmemiş (kalıcı silmeden önce soft delete gerekir)";
			case 40408 -> "Subject seviyesinde compatibility tanımlı değil";
			case 40409 -> "Subject seviyesinde mode tanımlı değil";
			case 409 -> "Şema uyumsuz: compatibility kuralı yeni şemayı reddetti";
			case 42201 -> "Geçersiz şema";
			case 42202 -> "Geçersiz versiyon";
			case 42203 -> "Geçersiz compatibility seviyesi";
			case 42204 -> "Geçersiz mode";
			case 42205 -> "İşleme izin verilmiyor (registry ya da subject READONLY/IMPORT modunda olabilir)";
			case 42206 -> "Şema başka şemalarca referans ediliyor; önce referans verenleri silin";
			case 42208 -> "Geçersiz subject adı";
			case 42209 -> "Şema çok büyük";
			case 50001, 50002, 50003, 50004 -> "Schema Registry iç hatası (store/lider/forward)";
			default -> null;
		};
		if (byCode != null) {
			return byCode;
		}
		return switch (status) {
			case 401 -> "Kimlik doğrulama gerekli (401)";
			case 403 -> "Yetki yok (403)";
			case 404 -> "Bulunamadı";
			case 409 -> "Çakışma (409)";
			case 422 -> "Geçersiz istek (422)";
			default -> null;
		};
	}

	/** SR hata gövdesindeki message alanı (error_code olmadan); gövde JSON değilse gövdenin kendisi. */
	static String srMessage(String body) {
		try {
			JsonNode n = MAPPER.readTree(body);
			if (n != null && n.hasNonNull("message")) {
				return n.get("message").asText();
			}
		}
		catch (Exception ignored) {
			// gövde JSON değil (ör. proxy'nin HTML hata sayfası)
		}
		return body == null || body.length() <= 500 ? body : body.substring(0, 500) + "…";
	}

	/**
	 * Schema Registry'nin döndürdüğü HTTP hatası: durum kodu, SR {@code error_code} (yoksa -1) ve SR mesajı. Mesajı
	 * Türkçe ipucu + SR mesajıdır.
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
