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

/**
 * API key: correct → 200, wrong/missing → 401 (also on /mcp/ and /mcp;params), empty apiKey → no auth. Health is
 * always open and the only exposed actuator endpoint. Requests with an Origin header (browsers) get 403.
 */
class ApiKeyHttpTest {

	static final String INIT = """
			{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}""";

	static HttpResponse<String> post(int port, String key) throws Exception {
		return post(port, "/mcp", key, null);
	}

	static HttpResponse<String> post(int port, String path, String key, String origin) throws Exception {
		HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
			.header("Content-Type", "application/json")
			.header("Accept", "application/json, text/event-stream")
			.POST(HttpRequest.BodyPublishers.ofString(INIT));
		if (key != null) {
			b.header("X-Authorization", key);
		}
		if (origin != null) {
			b.header("Origin", origin);
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

		@Test
		void pathVariantsNeedTheKeyToo() throws Exception {
			for (String path : new String[] { "/mcp/", "/mcp;x=y", "/mcp/anything" }) {
				assertThat(post(port, path, null, null).statusCode()).as(path).isEqualTo(401);
				assertThat(post(port, path, "wrong", null).statusCode()).as(path).isEqualTo(401);
			}
		}

		@Test
		void onlyHealthIsExposed() throws Exception {
			for (String path : new String[] { "/actuator/env", "/actuator/configprops", "/actuator/heapdump",
					"/actuator/beans", "/actuator/loggers", "/actuator/threaddump" }) {
				assertThat(get(port, path).statusCode()).as(path).isEqualTo(404);
			}
		}

		@Test
		void browserOriginIsRejectedEvenWithTheKey() throws Exception {
			assertThat(post(port, "/mcp", "s3cr3t-key", "https://evil.example.com").statusCode()).isEqualTo(403);
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

		@Test
		void browserOriginIsRejected() throws Exception {
			// a web page (CSRF or DNS rebinding) cannot drive the tools of a server without an API key
			assertThat(post(port, "/mcp", null, "http://evil.example.com").statusCode()).isEqualTo(403);
			assertThat(post(port, "/mcp", null, "http://localhost:" + port).statusCode()).isEqualTo(403);
			assertThat(post(port, "/mcp", null, null).statusCode()).isEqualTo(200);
		}

	}

}
