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
 * ZooKeeper-mode Kafka (Confluent Platform 7.9 = Kafka 3.9, ZooKeeper + a single broker) shared for the whole
 * test JVM. External listener SASL_PLAINTEXT + PLAIN (admin/alice/bob, the same users as
 * {@link KafkaTestCluster}), {@code kafka.security.authorizer.AclAuthorizer} enabled, admin is a super user.
 * For "mixed environment" tests together with the KRaft cluster.
 */
public final class ZkKafkaTestCluster {

	public static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.9.1";

	public static final String ZOOKEEPER_IMAGE = "confluentinc/cp-zookeeper:7.9.1";

	private static final Network NETWORK = Network.newNetwork();

	private static GenericContainer<?> zookeeper;

	private static GenericContainer<?> kafka;

	private static int hostPort;

	private ZkKafkaTestCluster() {
	}

	@SuppressWarnings("resource")
	public static synchronized void start() {
		if (kafka != null) {
			return;
		}
		GenericContainer<?> zk = new GenericContainer<>(DockerImageName.parse(ZOOKEEPER_IMAGE)).withNetwork(NETWORK)
			.withNetworkAliases("zookeeper")
			.withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
			.withEnv("ZOOKEEPER_TICK_TIME", "2000")
			.waitingFor(Wait.forLogMessage(".*binding to port.*", 1).withStartupTimeout(Duration.ofMinutes(3)));
		zk.start();
		zookeeper = zk;

		hostPort = freePort();
		StringBuilder jaas = new StringBuilder(
				"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\"");
		KafkaTestCluster.USERS.forEach((u, p) -> jaas.append(" user_").append(u).append("=\"").append(p).append('"'));
		jaas.append(';');
		GenericContainer<?> c = new GenericContainer<>(DockerImageName.parse(KAFKA_IMAGE)).withNetwork(NETWORK)
			.withNetworkAliases("zkkafka")
			.withEnv("KAFKA_BROKER_ID", "1")
			.withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
			.withEnv("KAFKA_LISTENERS", "SASL://0.0.0.0:9092,INTERNAL://0.0.0.0:19092")
			.withEnv("KAFKA_ADVERTISED_LISTENERS", "SASL://localhost:" + hostPort + ",INTERNAL://zkkafka:19092")
			.withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "SASL:SASL_PLAINTEXT,INTERNAL:PLAINTEXT")
			.withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
			.withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
			.withEnv("KAFKA_LISTENER_NAME_SASL_PLAIN_SASL_JAAS_CONFIG", jaas.toString())
			.withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer")
			.withEnv("KAFKA_SUPER_USERS", "User:admin;User:ANONYMOUS")
			.withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false")
			.withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
			.withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
			.withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
			.withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
			.withEnv("KAFKA_NUM_PARTITIONS", "1")
			.withEnv("KAFKA_DELEGATION_TOKEN_SECRET_KEY", "kafka-mcp-test-token-secret")
			.withEnv("KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
			.withCreateContainerCmdModifier(cmd -> {
				cmd.withExposedPorts(ExposedPort.tcp(9092));
				cmd.getHostConfig()
					.withPortBindings(new PortBinding(Ports.Binding.bindPort(hostPort), ExposedPort.tcp(9092)));
			})
			.waitingFor(Wait.forLogMessage(".*started \\(kafka.server.KafkaServer\\).*", 1)
				.withStartupTimeout(Duration.ofMinutes(3)));
		c.start();
		kafka = c;
	}

	public static String bootstrap() {
		start();
		return "localhost:" + hostPort;
	}

	public static Map<String, Object> clientProperties(String user) {
		Map<String, Object> p = new LinkedHashMap<>(KafkaTestCluster.saslProperties(user));
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
