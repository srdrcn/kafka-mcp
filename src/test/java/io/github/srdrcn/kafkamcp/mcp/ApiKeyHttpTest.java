package io.github.srdrcn.kafkamcp.mcp;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;

import io.github.srdrcn.kafkamcp.KafkaMcpApplication;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.support.TestConfigs;

/** API key: correct → 200, wrong/missing → 401, empty apiKey → no auth. Health is always open. */
class ApiKeyHttpTest {

	static final String INIT = """
			{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}""";

	static HttpResponse<String> post(int port, String key) throws Exception {
		HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/mcp"))
			.header("Content-Type", "application/json")
			.header("Accept", "application/json, text/event-stream")
			.POST(HttpRequest.BodyPublishers.ofString(INIT));
		if (key != null) {
			b.header("X-Authorization", key);
		}
		return HttpClient.newHttpClient().send(b.build(), HttpResponse.BodyHandlers.ofString());
	}

	static HttpResponse<String> get(int port, String path) throws Exception {
		return HttpClient.newHttpClient()
			.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build(),
					HttpResponse.BodyHandlers.ofString());
	}

	@Nested
	@SpringBootTest(classes = { KafkaMcpApplication.class, WithKey.Config.class },
			webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "kafka-mcp.transport=http")
	class WithKey {

		@LocalServerPort
		int port;

		@TestConfiguration(proxyBeanMethods = false)
		static class Config {

			@Bean
			KafkaMcpConfig kafkaMcpConfig() {
				KafkaMcpConfig c = TestConfigs.offline();
				c.setApiKey("s3cr3t-key");
				return c;
			}

		}

		@Test
		void correctKeyPasses() throws Exception {
			HttpResponse<String> r = post(port, "s3cr3t-key");
			assertThat(r.statusCode()).isEqualTo(200);
			assertThat(r.body()).contains("\"serverInfo\"").contains("kafka-mcp");
		}

		@Test
		void wrongKeyIs401() throws Exception {
			assertThat(post(port, "wrong").statusCode()).isEqualTo(401);
			assertThat(post(port, "S3CR3T-KEY").statusCode()).isEqualTo(401);
			assertThat(post(port, "s3cr3t-key-extra").statusCode()).isEqualTo(401);
			assertThat(post(port, "").statusCode()).isEqualTo(401);
		}

		@Test
		void missingKeyIs401() throws Exception {
			HttpResponse<String> r = post(port, null);
			assertThat(r.statusCode()).isEqualTo(401);
			assertThat(r.body()).contains("unauthorized");
		}

		@Test
		void probesNeedNoKey() throws Exception {
			assertThat(get(port, "/actuator/health/liveness").statusCode()).isEqualTo(200);
			assertThat(get(port, "/actuator/health/readiness").statusCode()).isEqualTo(200);
		}

	}

	@Nested
	@SpringBootTest(classes = { KafkaMcpApplication.class, EmptyKey.Config.class },
			webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "kafka-mcp.transport=http")
	class EmptyKey {

		@LocalServerPort
		int port;

		@TestConfiguration(proxyBeanMethods = false)
		static class Config {

			@Bean
			KafkaMcpConfig kafkaMcpConfig() {
				KafkaMcpConfig c = TestConfigs.offline();
				c.setApiKey("");
				return c;
			}

		}

		@Test
		void noAuthWhenApiKeyEmpty() throws Exception {
			assertThat(post(port, null).statusCode()).isEqualTo(200);
			assertThat(post(port, "anything").statusCode()).isEqualTo(200);
		}

	}

}
