package io.github.srdrcn.kafkamcp.schemaregistry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.ServerSocket;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Unit tests of the Schema Registry client's error mapping (no running registry). */
class SchemaRegistryClientTest {

	@Test
	void unreachableRegistryThrowsTypedException() throws Exception {
		int port;
		try (ServerSocket s = new ServerSocket(0)) {
			port = s.getLocalPort();
		}
		ClusterConfig c = new ClusterConfig();
		c.setName("sr");
		c.setBootstrapServers("127.0.0.1:9092");
		c.setSchemaRegistryUrl("http://127.0.0.1:" + port + "/");
		KafkaMcpConfig config = new KafkaMcpConfig();
		config.setClusters(List.of(c));
		SchemaRegistryClient client = new SchemaRegistryClient(new KafkaClients(config));
		// callers (e.g. the avro decoder) rely on the type, not the message, to detect a registry that is down
		assertThatThrownBy(() -> client.get("sr", "/schemas/ids/1"))
			.isInstanceOf(SchemaRegistryClient.UnreachableException.class)
			.hasMessageStartingWith("Cannot reach Schema Registry (http://127.0.0.1:" + port + ")");
	}

	@Test
	void pathSegmentsRejectDotSegments() {
		// "/config/." would be normalized to "/config" (the global level), "/connectors/../x" to "/x"
		for (String bad : List.of(".", "..", "")) {
			assertThatThrownBy(() -> SchemaRegistryClient.enc(bad)).isInstanceOf(ToolException.class)
				.hasMessageContaining("not allowed");
			assertThatThrownBy(() -> ConnectClient.enc(bad)).isInstanceOf(ToolException.class)
				.hasMessageContaining("not allowed");
		}
		assertThat(SchemaRegistryClient.enc("a.b;c/d")).isEqualTo("a.b%3Bc%2Fd");
		assertThat(SchemaRegistryClient.enc("...")).isEqualTo("...");
		assertThat(ConnectClient.enc("my connector")).isEqualTo("my%20connector");
	}

}
