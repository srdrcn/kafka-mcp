package io.github.srdrcn.kafkamcp.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.KafkaMcpApplication;

/**
 * Runs the real jar entry point with {@code --transport stdio} in a child JVM: stdout must carry nothing but
 * JSON-RPC messages (a banner, a log line or any other print would break stdio clients); logs go to stderr.
 */
class StdioTransportTest {

	private static final ObjectMapper JSON = new ObjectMapper();

	@Test
	void stdoutCarriesOnlyJsonRpc(@TempDir Path dir) throws Exception {
		Path config = Files.writeString(dir.resolve("kafka-mcp.yaml"), """
				toolsets: [core]
				clusters:
				  - name: offline
				    bootstrapServers: localhost:1
				""");
		String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
		Path java = Path.of(System.getProperty("java.home"), "bin", "java");
		Process p = new ProcessBuilder(java.toString(), "-cp", classpath, KafkaMcpApplication.class.getName(),
				"--config", config.toString(), "--transport", "stdio")
			.redirectError(dir.resolve("stderr.log").toFile())
			.start();
		BlockingQueue<String> lines = new LinkedBlockingQueue<>();
		Thread reader = Thread.ofPlatform().daemon().start(() -> {
			try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
				String line;
				while ((line = r.readLine()) != null) {
					lines.add(line);
				}
			}
			catch (Exception e) {
				// process ended
			}
		});
		List<String> stdout = new ArrayList<>();
		try {
			OutputStream in = p.getOutputStream();
			send(in, """
					{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}""");
			stdout.add(await(lines, p, dir));
			send(in, """
					{"jsonrpc":"2.0","method":"notifications/initialized"}""");
			send(in, """
					{"jsonrpc":"2.0","id":2,"method":"tools/list"}""");
			stdout.add(await(lines, p, dir));
			in.close();
			assertThat(p.waitFor(30, TimeUnit.SECONDS)).as("process exits when stdin closes").isTrue();
			reader.join(5_000);
			lines.drainTo(stdout);
		}
		finally {
			p.destroyForcibly();
		}
		for (String line : stdout) {
			JsonNode n = JSON.readTree(line);
			assertThat(n.path("jsonrpc").asText()).as("stdout line: %s", line).isEqualTo("2.0");
		}
		JsonNode init = JSON.readTree(stdout.get(0));
		assertThat(init.path("id").asInt()).isEqualTo(1);
		assertThat(init.at("/result/serverInfo/name").asText()).isEqualTo("kafka-mcp");
		JsonNode list = JSON.readTree(stdout.get(1));
		assertThat(list.path("id").asInt()).isEqualTo(2);
		assertThat(list.at("/result/tools").size()).isPositive();
		String stderr = Files.readString(dir.resolve("stderr.log"));
		assertThat(stderr).contains("stdio transport ready");
	}

	private static void send(OutputStream in, String message) throws Exception {
		in.write((message + "\n").getBytes(StandardCharsets.UTF_8));
		in.flush();
	}

	private static String await(BlockingQueue<String> lines, Process p, Path dir) throws Exception {
		String line = lines.poll(60, TimeUnit.SECONDS);
		if (line == null) {
			fail("No response on stdout (alive=" + p.isAlive() + "); stderr:\n"
					+ Files.readString(dir.resolve("stderr.log")));
		}
		return line;
	}

}
