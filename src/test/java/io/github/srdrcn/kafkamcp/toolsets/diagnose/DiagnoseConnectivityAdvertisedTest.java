package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;

/**
 * The most common OpenShift failure for diagnose_connectivity: the bootstrap address is reachable but the broker's
 * advertised address cannot be resolved from this client. A separate, short-lived broker advertises its external
 * listener with an unresolvable name; describeCluster times out, the advertised addresses are read from metadata
 * fetched over the bootstrap connection only, probed and reported as ADVERTISED_UNREACHABLE.
 */
class DiagnoseConnectivityAdvertisedTest {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static final String ADVERTISED_HOST = "kafka-0.kafka-headless.nonexistent.invalid";

	@Test
	void unresolvableAdvertisedListenerIsListedAndProbed() throws Exception {
		int external = freePort();
		int good = freePort();
		try (GenericContainer<?> kafka = broker(external, good)) {
			kafka.start();
			String topic = "diag-advertised";
			try (Admin admin = Admin.create(Map.of("bootstrap.servers", "localhost:" + good))) {
				admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(60, TimeUnit.SECONDS);
				DiagnoseTestBase.waitUntil(() -> {
					try {
						TopicDescription d = admin.describeTopics(List.of(topic))
							.allTopicNames()
							.get(30, TimeUnit.SECONDS)
							.get(topic);
						return d.partitions().get(0).leader() != null && !d.partitions().get(0).leader().isEmpty();
					}
					catch (Exception e) {
						return false;
					}
				}, "no topic leader elected");
			}

			KafkaMcpConfig config = new KafkaMcpConfig();
			ClusterConfig cluster = new ClusterConfig();
			cluster.setName("advertised");
			cluster.setBootstrapServers("localhost:" + external);
			config.setClusters(List.of(cluster));
			KafkaClients clients = new KafkaClients(config) {

				@Override
				public Admin admin(String name) {
					throw new AssertionError("diagnose_connectivity must not use the cached AdminClient");
				}

			};
			try {
				DiagnoseConnectivityTools tools = new DiagnoseConnectivityTools(new ConnectClient(clients),
						new SchemaRegistryClient(clients));
				long start = System.nanoTime();
				ToolCall call = new ToolCall("diagnose_connectivity", Map.of("cluster", "advertised"), clients,
						Instant.now().plusMillis(KafkaClients.TIMEOUT_MS));
				JsonNode r = MAPPER.valueToTree(tools.diagnose(call));
				assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(KafkaClients.TIMEOUT_MS);

				assertThat(step(r, "tcp").get("status").asText()).isEqualTo("ok");
				assertThat(step(r, "kafka").get("status").asText()).as(r.toString()).isIn("timeout", "error");
				JsonNode md = step(r, "bootstrapMetadata");
				assertThat(md.get("status").asText()).as(r.toString()).isEqualTo("ok");
				assertThat(md.get("brokers").asInt()).isEqualTo(1);
				JsonNode adv = step(r, "advertised");
				assertThat(adv.get("target").asText()).isEqualTo(ADVERTISED_HOST + ":" + external);
				assertThat(adv.get("status").asText()).isEqualTo("error");

				JsonNode f = DiagnoseTestBase.finding(r, "ADVERTISED_UNREACHABLE");
				assertThat(f.get("severity").asText()).isEqualTo("critical");
				assertThat(f.at("/evidence/brokers/0/advertised").asText()).isEqualTo(ADVERTISED_HOST + ":" + external);
				assertThat(f.at("/evidence/brokers/0/error").asText()).contains("DNS resolution failed");
				assertThat(f.get("recommendation").asText()).contains("advertised.listeners", "OpenShift");
				assertThat(r.at("/facts/brokers/0/reachable").asBoolean()).isFalse();
				assertThat(r.get("status").asText()).isEqualTo("critical");
			}
			finally {
				clients.destroy();
			}
		}
	}

	private static JsonNode step(JsonNode r, String name) {
		for (JsonNode s : r.get("steps")) {
			if (name.equals(s.get("step").asText())) {
				return s;
			}
		}
		throw new AssertionError("No " + name + " step: " + r.get("steps"));
	}

	/** Single node that advertises its external listener (EXT) with an unresolvable name and is also reachable via localhost for setup. */
	@SuppressWarnings("resource")
	private static GenericContainer<?> broker(int external, int good) {
		return new GenericContainer<>(DockerImageName.parse(KafkaTestCluster.IMAGE))
			.withEnv("KAFKA_NODE_ID", "1")
			.withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
			.withEnv("KAFKA_LISTENERS",
					"EXT://0.0.0.0:9092,GOOD://0.0.0.0:9095,INTERNAL://0.0.0.0:19092,CONTROLLER://0.0.0.0:9093")
			.withEnv("KAFKA_ADVERTISED_LISTENERS", "EXT://" + ADVERTISED_HOST + ":" + external + ",GOOD://localhost:"
					+ good + ",INTERNAL://localhost:19092")
			.withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
					"EXT:PLAINTEXT,GOOD:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
			.withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
			.withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
			.withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
			.withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
			.withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
			.withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
			.withEnv("KAFKA_HEAP_OPTS", "-Xmx256m -Xms256m")
			.withCreateContainerCmdModifier(cmd -> {
				cmd.withExposedPorts(ExposedPort.tcp(9092), ExposedPort.tcp(9095));
				cmd.getHostConfig()
					.withPortBindings(new PortBinding(Ports.Binding.bindPort(external), ExposedPort.tcp(9092)),
							new PortBinding(Ports.Binding.bindPort(good), ExposedPort.tcp(9095)));
			})
			.waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1).withStartupTimeout(Duration.ofMinutes(3)));
	}

	private static int freePort() {
		try (ServerSocket s = new ServerSocket(0)) {
			s.setReuseAddress(true);
			return s.getLocalPort();
		}
		catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

}
