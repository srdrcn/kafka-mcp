package io.github.srdrcn.kafkamcp.kafka;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.CloseOptions;
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
 * created separately, on a platform thread with a bounded wait, so if one cluster's Kerberos login or connection
 * breaks or hangs, the other clusters are not affected. {@link KafkaConsumer}s are created anew for every call (no
 * group.id, no commit, no topic auto-creation; the caller reads with {@code assign()} and closes it).
 */
@Component
public class KafkaClients implements DisposableBean {

	/** Time limit per tool; the Kafka client timeouts are set accordingly. */
	public static final int TIMEOUT_MS = 15_000;

	private static final Logger log = LoggerFactory.getLogger(KafkaClients.class);

	private final KafkaMcpConfig config;

	private final Map<String, AdminHolder> admins = new ConcurrentHashMap<>();

	/** Concurrent consumers must not share the same client.id (JMX AppInfo conflict). */
	private static final AtomicLong READERS = new AtomicLong();

	/** Default fetch.max.bytes of the message consumers (Kafka's default is 50 MiB); overridable per cluster. */
	static final int CONSUMER_FETCH_MAX_BYTES = 8 * 1024 * 1024;

	/** Platform threads that create Kafka clients (see {@link #create}). */
	private final ExecutorService creator = Executors
		.newCachedThreadPool(Thread.ofPlatform().daemon().name("kafka-client-create-", 0).factory());

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
	 * A new consumer for reading messages: no group.id, auto commit off, no topic auto-creation, byte[] key/value.
	 * The caller uses it with {@code assign()} and closes it (try-with-resources). {@code overrides} (internal callers
	 * only) are applied after the cluster's properties; the safety settings are applied last and always win.
	 */
	public KafkaConsumer<byte[], byte[]> newConsumer(String cluster, Map<String, Object> overrides) {
		ClusterConfig c = cluster(cluster);
		Map<String, Object> props = consumerProperties(c, overrides);
		return create(c, "consumer", () -> new KafkaConsumer<byte[], byte[]>(props),
				consumer -> consumer.close(CloseOptions.timeout(Duration.ofSeconds(1))));
	}

	/**
	 * Consumer properties: consumer defaults (overridable), then {@link #clientProperties} (defaults, kerberos, the
	 * cluster's properties), then {@code overrides}, then the forced read-only/no-group settings.
	 */
	Map<String, Object> consumerProperties(ClusterConfig c, Map<String, Object> overrides) {
		Map<String, Object> props = new LinkedHashMap<>();
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
		props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, CONSUMER_FETCH_MAX_BYTES);
		props.putAll(clientProperties(c));
		// concurrent consumers must not share a client.id (JMX AppInfo conflict): a unique suffix is appended
		props.put(CommonClientConfigs.CLIENT_ID_CONFIG,
				props.get(CommonClientConfigs.CLIENT_ID_CONFIG) + "-reader-" + READERS.incrementAndGet());
		props.keySet().removeIf(k -> k.startsWith("admin."));
		if (overrides != null) {
			props.putAll(overrides);
		}
		props.remove(ConsumerConfig.GROUP_ID_CONFIG);
		props.remove(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
		// the offset windows rely on these: an out-of-range position must fail instead of jumping, and the end
		// offsets (listOffsets) are read_uncommitted
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		return props;
	}

	/** AdminClient properties: retries=3 and a client.id ending in "-admin" unless the cluster's properties set them. */
	Map<String, Object> adminProperties(ClusterConfig c) {
		Map<String, Object> props = new HashMap<>();
		props.put(AdminClientConfig.RETRIES_CONFIG, 3);
		props.putAll(clientProperties(c));
		if (!c.getProperties().containsKey(CommonClientConfigs.CLIENT_ID_CONFIG)) {
			props.put(CommonClientConfigs.CLIENT_ID_CONFIG, "kafka-mcp-" + c.getName() + "-admin");
		}
		return props;
	}

	/**
	 * Creates a Kafka client on a platform thread and waits for it at most {@link #TIMEOUT_MS}. Creating a client
	 * with SASL/GSSAPI performs the Kerberos login (KDC network I/O) inside {@code synchronized} blocks of the Kafka
	 * client (LoginManager); on JDK 21 a virtual thread blocked there pins its carrier, so a slow KDC would stall the
	 * tool calls of every cluster. A client that is created after the caller gave up is closed.
	 */
	<T> T create(ClusterConfig c, String what, Supplier<T> factory, Consumer<T> closer) {
		CompletableFuture<T> f = CompletableFuture.supplyAsync(factory, creator);
		try {
			return f.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
		}
		catch (TimeoutException e) {
			f.thenAccept(client -> closeQuietly(client, closer));
			throw new ToolException("Creating the Kafka " + what + " for cluster " + c.getName() + " took longer than "
					+ TIMEOUT_MS / 1000 + " s (slow Kerberos login or KDC?); see kerberos_self_check");
		}
		catch (InterruptedException e) {
			f.thenAccept(client -> closeQuietly(client, closer));
			Thread.currentThread().interrupt();
			throw new ToolException("Tool was cancelled");
		}
		catch (ExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof ToolException te) {
				throw te;
			}
			throw new ToolException(
					"Could not create the Kafka " + what + " (" + c.getName() + "): " + KafkaErrors.explain(cause), cause);
		}
	}

	private static <T> void closeQuietly(T client, Consumer<T> closer) {
		try {
			closer.accept(client);
		}
		catch (RuntimeException e) {
			log.debug("Error while closing a Kafka client", e);
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
		creator.shutdownNow();
	}

	/**
	 * The cached AdminClient of one cluster. Concurrent first calls share one creation (one Kerberos login); a failed
	 * creation is retried by the next call. Uses a {@link ReentrantLock} (not {@code synchronized}) and never blocks
	 * while holding it, so waiting virtual threads unmount.
	 */
	private final class AdminHolder {

		private final ReentrantLock lock = new ReentrantLock();

		private Admin admin;

		private CompletableFuture<Admin> pending;

		private boolean closed;

		Admin get(ClusterConfig c) {
			CompletableFuture<Admin> f;
			lock.lock();
			try {
				if (admin != null) {
					return admin;
				}
				if (closed) {
					throw new ToolException("Kafka client of cluster " + c.getName() + " is closed");
				}
				if (pending == null) {
					CompletableFuture<Admin> created = CompletableFuture.supplyAsync(() -> createAdmin(c), creator);
					pending = created;
					created.whenComplete((a, e) -> created(created, a));
				}
				f = pending;
			}
			finally {
				lock.unlock();
			}
			try {
				return f.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
			}
			catch (TimeoutException e) {
				throw new ToolException("Creating the Kafka client for cluster " + c.getName() + " took longer than "
						+ TIMEOUT_MS / 1000 + " s (slow Kerberos login or KDC?); it continues in the background, try "
						+ "again shortly or see kerberos_self_check");
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new ToolException("Tool was cancelled");
			}
			catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof ToolException te) {
					throw te;
				}
				throw new ToolException(
						"Could not create Kafka client (" + c.getName() + "): " + KafkaErrors.explain(cause), cause);
			}
		}

		private Admin createAdmin(ClusterConfig c) {
			try {
				Admin a = Admin.create(adminProperties(c));
				log.info("AdminClient created: cluster={}", c.getName());
				return a;
			}
			catch (RuntimeException e) {
				log.warn("Could not create AdminClient: cluster={} error={}", c.getName(), KafkaErrors.explain(e));
				throw new ToolException(
						"Could not create Kafka client (" + c.getName() + "): " + KafkaErrors.explain(e), e);
			}
		}

		/** Completion of a creation: keeps the client (or closes it if the holder was closed meanwhile). */
		private void created(CompletableFuture<Admin> f, Admin a) {
			Admin orphan = null;
			lock.lock();
			try {
				if (pending == f) {
					pending = null;
				}
				if (a != null) {
					if (closed) {
						orphan = a;
					}
					else {
						admin = a;
					}
				}
			}
			finally {
				lock.unlock();
			}
			if (orphan != null) {
				closeQuietly(orphan, x -> x.close(Duration.ofSeconds(2)));
			}
		}

		void close() {
			Admin a;
			lock.lock();
			try {
				closed = true;
				a = admin;
				admin = null;
			}
			finally {
				lock.unlock();
			}
			if (a != null) {
				closeQuietly(a, x -> x.close(Duration.ofSeconds(2)));
			}
		}

	}

}
