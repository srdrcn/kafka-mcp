package io.github.srdrcn.kafkamcp.support;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

/**
 * Single-node KRaft Kafka (apache/kafka) shared for the whole test JVM. External listener SASL_PLAINTEXT +
 * PLAIN (users: admin, alice, bob), StandardAuthorizer enabled (admin is a super user). Reachable inside the
 * Docker network at {@code kafka:19092} (PLAINTEXT) (for Connect/Schema Registry).
 */
public final class KafkaTestCluster {

	public static final String IMAGE = "apache/kafka:4.1.1";

	public static final Network NETWORK = Network.newNetwork();

	public static final String INTERNAL_BOOTSTRAP = "kafka:19092";

	public static final Map<String, String> USERS = Map.of("admin", "admin-secret", "alice", "alice-secret", "bob",
			"bob-secret");

	private static GenericContainer<?> kafka;

	private static int hostPort;

	private KafkaTestCluster() {
	}

	@SuppressWarnings("resource")
	public static synchronized void start() {
		if (kafka != null) {
			return;
		}
		hostPort = freePort();
		StringBuilder jaas = new StringBuilder(
				"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\"");
		USERS.forEach((u, p) -> jaas.append(" user_").append(u).append("=\"").append(p).append('"'));
		jaas.append(';');
		GenericContainer<?> c = new GenericContainer<>(DockerImageName.parse(IMAGE)).withNetwork(NETWORK)
			.withNetworkAliases("kafka")
			.withEnv("KAFKA_NODE_ID", "1")
			.withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
			.withEnv("KAFKA_LISTENERS", "SASL://0.0.0.0:9092,INTERNAL://0.0.0.0:19092,CONTROLLER://0.0.0.0:9093")
			.withEnv("KAFKA_ADVERTISED_LISTENERS", "SASL://localhost:" + hostPort + ",INTERNAL://kafka:19092")
			.withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "SASL:SASL_PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
			.withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
			.withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
			.withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
			.withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
			.withEnv("KAFKA_LISTENER_NAME_SASL_PLAIN_SASL_JAAS_CONFIG", jaas.toString())
			.withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
			.withEnv("KAFKA_SUPER_USERS", "User:admin;User:ANONYMOUS")
			.withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false")
			.withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
			.withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
			.withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
			.withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
			.withEnv("KAFKA_NUM_PARTITIONS", "1")
			.withEnv("KAFKA_DELEGATION_TOKEN_SECRET_KEY", "kafka-mcp-test-token-secret")
			.withCreateContainerCmdModifier(cmd -> {
				cmd.withExposedPorts(ExposedPort.tcp(9092));
				cmd.getHostConfig()
					.withPortBindings(new PortBinding(Ports.Binding.bindPort(hostPort), ExposedPort.tcp(9092)));
			})
			.waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1).withStartupTimeout(Duration.ofMinutes(3)));
		c.start();
		kafka = c;
	}

	public static String bootstrap() {
		start();
		return "localhost:" + hostPort;
	}

	/** SASL/PLAIN client properties (for the {@code properties:} block of a cluster config). */
	public static Map<String, String> saslProperties(String user) {
		Map<String, String> p = new LinkedHashMap<>();
		p.put("security.protocol", "SASL_PLAINTEXT");
		p.put("sasl.mechanism", "PLAIN");
		p.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + user
				+ "\" password=\"" + USERS.get(user) + "\";");
		return p;
	}

	public static Map<String, Object> clientProperties(String user) {
		Map<String, Object> p = new LinkedHashMap<>(saslProperties(user));
		p.put("bootstrap.servers", bootstrap());
		return p;
	}

	/** New AdminClient with the admin user, for test setup (the caller closes it). */
	public static Admin admin() {
		return Admin.create(clientProperties("admin"));
	}

	/** Producer with the admin user, for producing test data (the caller closes it). */
	public static KafkaProducer<byte[], byte[]> producer(Map<String, Object> overrides) {
		Map<String, Object> p = clientProperties("admin");
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		p.put(ProducerConfig.LINGER_MS_CONFIG, 0);
		if (overrides != null) {
			p.putAll(overrides);
		}
		return new KafkaProducer<>(p);
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
