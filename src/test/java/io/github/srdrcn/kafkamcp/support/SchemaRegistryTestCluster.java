package io.github.srdrcn.kafkamcp.support;

import java.time.Duration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared Confluent Schema Registry (cp-schema-registry). Connects to Kafka over PLAINTEXT inside the Docker
 * network ({@code _schemas} topic).
 */
public final class SchemaRegistryTestCluster {

	public static final String IMAGE = "confluentinc/cp-schema-registry:7.9.1";

	private static GenericContainer<?> registry;

	private SchemaRegistryTestCluster() {
	}

	@SuppressWarnings("resource")
	public static synchronized void start() {
		if (registry != null) {
			return;
		}
		KafkaTestCluster.start();
		GenericContainer<?> c = new GenericContainer<>(DockerImageName.parse(IMAGE))
			.withNetwork(KafkaTestCluster.NETWORK)
			.withNetworkAliases("schema-registry")
			.withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
			.withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
			.withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + KafkaTestCluster.INTERNAL_BOOTSTRAP)
			.withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
			.withExposedPorts(8081)
			.waitingFor(Wait.forHttp("/subjects").forPort(8081).withStartupTimeout(Duration.ofMinutes(3)));
		c.start();
		registry = c;
	}

	public static String url() {
		start();
		return "http://" + registry.getHost() + ":" + registry.getMappedPort(8081);
	}

}
