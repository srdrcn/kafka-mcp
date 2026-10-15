package io.github.srdrcn.kafkamcp.kafka;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.config.KerberosConfig;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Keeps a single {@link Admin} per cluster (created on first use, then reused). Each cluster's client is
 * created separately, so if one cluster's Kerberos login or connection breaks, the other clusters are not
 * affected. {@link KafkaConsumer}s are created anew for every call (no group.id, no commit; the caller reads
 * with {@code assign()} and closes it).
 */
@Component
public class KafkaClients implements DisposableBean {

	/** Time limit per tool; the Kafka client timeouts are set accordingly. */
	public static final int TIMEOUT_MS = 15_000;

	private static final Logger log = LoggerFactory.getLogger(KafkaClients.class);

	private final KafkaMcpConfig config;

	private final Map<String, AdminHolder> admins = new ConcurrentHashMap<>();

	/** Concurrent consumers must not share the same client.id (JMX AppInfo conflict). */
	private static final java.util.concurrent.atomic.AtomicLong READERS = new java.util.concurrent.atomic.AtomicLong();

	public KafkaClients(KafkaMcpConfig config) {
		this.config = config;
		KerberosConfig k = config.getKerberos();
		if (k != null && k.getKrb5Conf() != null && !k.getKrb5Conf().isBlank()
				&& System.getProperty("java.security.krb5.conf") == null) {
			System.setProperty("java.security.krb5.conf", k.getKrb5Conf());
		}
	}

	public KafkaMcpConfig config() {
		return config;
	}

	public ClusterConfig cluster(String name) {
		ClusterConfig c = config.cluster(name);
		if (c == null) {
			throw new ToolException("Unknown cluster: " + name + " (see clusters_list)");
		}
		return c;
	}

	/** Shared AdminClient of the cluster. Do not close it. */
	public Admin admin(String cluster) {
		ClusterConfig c = cluster(cluster);
		return admins.computeIfAbsent(c.getName(), n -> new AdminHolder()).get(c);
	}

	/** Closes and drops the cached AdminClient; the next call creates a new one. */
	public void reset(String cluster) {
		AdminHolder holder = admins.remove(cluster);
		if (holder != null) {
			holder.close();
		}
	}

	/**
	 * A new consumer for reading messages: no group.id, auto commit off, byte[] key/value. The caller uses it
	 * with {@code assign()} and closes it (try-with-resources).
	 */
	public KafkaConsumer<byte[], byte[]> newConsumer(String cluster, Map<String, Object> overrides) {
		ClusterConfig c = cluster(cluster);
		Map<String, Object> props = clientProperties(c);
		props.remove(ConsumerConfig.GROUP_ID_CONFIG);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
		props.put(CommonClientConfigs.CLIENT_ID_CONFIG, "kafka-mcp-" + c.getName() + "-reader-" + READERS.incrementAndGet());
		props.keySet().removeIf(k -> k.startsWith("admin."));
		if (overrides != null) {
			props.putAll(overrides);
		}
		try {
			return new KafkaConsumer<>(props);
		}
		catch (RuntimeException e) {
			throw new ToolException("Could not create consumer (" + c.getName() + "): " + KafkaErrors.explain(e), e);
		}
	}

	/**
	 * Kafka client properties for a cluster: bootstrap, the SASL/GSSAPI settings derived from the kerberos
	 * block, timeouts and, last, the {@code properties:} overrides under the cluster.
	 */
	public Map<String, Object> clientProperties(ClusterConfig c) {
		Map<String, Object> props = new LinkedHashMap<>();
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, c.getBootstrapServers());
		props.put(CommonClientConfigs.CLIENT_ID_CONFIG, "kafka-mcp-" + c.getName());
		props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
		props.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
		props.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 5_000);
		props.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, 10_000);
		props.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, 5_000);
		props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 60_000);
		KerberosConfig k = config.getKerberos();
		if (k != null) {
			props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
			props.put(SaslConfigs.SASL_MECHANISM, "GSSAPI");
			props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, k.getServiceName());
			props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig(k));
		}
		props.putAll(c.getProperties());
		return props;
	}

	public static String jaasConfig(KerberosConfig k) {
		return "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true "
				+ "useTicketCache=false keyTab=\"" + escape(k.getKeytab()) + "\" principal=\""
				+ escape(k.getPrincipal()) + "\";";
	}

	private static String escape(String s) {
		return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
	}

	@Override
	public void destroy() {
		admins.values().forEach(AdminHolder::close);
		admins.clear();
	}

	private final class AdminHolder {

		private volatile Admin admin;

		Admin get(ClusterConfig c) {
			Admin a = admin;
			if (a != null) {
				return a;
			}
			synchronized (this) {
				if (admin == null) {
					Map<String, Object> props = new HashMap<>(clientProperties(c));
					props.put(CommonClientConfigs.CLIENT_ID_CONFIG, "kafka-mcp-" + c.getName() + "-admin");
					props.put(AdminClientConfig.RETRIES_CONFIG, 3);
					try {
						admin = Admin.create(props);
						log.info("AdminClient created: cluster={}", c.getName());
					}
					catch (RuntimeException e) {
						log.warn("Could not create AdminClient: cluster={} error={}", c.getName(), KafkaErrors.explain(e));
						throw new ToolException(
								"Could not create Kafka client (" + c.getName() + "): " + KafkaErrors.explain(e), e);
					}
				}
				return admin;
			}
		}

		synchronized void close() {
			if (admin != null) {
				try {
					admin.close(java.time.Duration.ofSeconds(2));
				}
				catch (RuntimeException e) {
					log.debug("Error while closing AdminClient", e);
				}
				admin = null;
			}
		}

	}

}
