package io.github.srdrcn.kafkamcp.support;

import java.time.Duration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared Kafka Connect worker (connect-distributed.sh from the apache/kafka image). Connects to Kafka over
 * PLAINTEXT inside the Docker network. The FileStream and MirrorMaker connectors are installed.
 */
public final class ConnectTestCluster {

	private static GenericContainer<?> connect;

	private ConnectTestCluster() {
	}

	@SuppressWarnings("resource")
	public static synchronized void start() {
		if (connect != null) {
			return;
		}
		KafkaTestCluster.start();
		String props = """
				bootstrap.servers=%s
				group.id=kafka-mcp-test-connect
				key.converter=org.apache.kafka.connect.json.JsonConverter
				value.converter=org.apache.kafka.connect.json.JsonConverter
				key.converter.schemas.enable=false
				value.converter.schemas.enable=false
				config.storage.topic=_connect-configs
				offset.storage.topic=_connect-offsets
				status.storage.topic=_connect-status
				config.storage.replication.factor=1
				offset.storage.replication.factor=1
				status.storage.replication.factor=1
				offset.flush.interval.ms=1000
				listeners=http://0.0.0.0:8083
				rest.advertised.host.name=connect
				plugin.path=/opt/kafka/libs/connect-file-4.1.1.jar
				""".formatted(KafkaTestCluster.INTERNAL_BOOTSTRAP);
		GenericContainer<?> c = new GenericContainer<>(DockerImageName.parse(KafkaTestCluster.IMAGE))
			.withNetwork(KafkaTestCluster.NETWORK)
			.withNetworkAliases("connect")
			.withCopyToContainer(Transferable.of(props), "/tmp/connect.properties")
			.withCommand("/opt/kafka/bin/connect-distributed.sh", "/tmp/connect.properties")
			.withExposedPorts(8083)
			.waitingFor(Wait.forHttp("/connectors").forPort(8083).withStartupTimeout(Duration.ofMinutes(3)));
		c.start();
		connect = c;
	}

	public static String url() {
		start();
		return "http://" + connect.getHost() + ":" + connect.getMappedPort(8083);
	}

}
