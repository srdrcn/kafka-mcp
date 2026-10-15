package io.github.srdrcn.kafkamcp.toolsets.acl;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.config.KerberosConfig;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolRegistry;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;

/**
 * kerberos_self_check with a kerberos block configured: the KDC is unreachable (login returns an explained
 * error), while the Kafka connection still succeeds because the cluster uses PLAIN via properties. No Spring
 * context is used; the JVM-wide java.security.krb5.conf is restored at the end of the test.
 */
class KerberosSelfCheckToolTest {

	@TempDir
	Path dir;

	@Test
	@SuppressWarnings("unchecked")
	void configuredKerberosBlockIsCheckedWhileClusterUsesPlain() throws Exception {
		String previous = System.getProperty(KerberosCheck.KRB5_CONF_PROPERTY);
		Path conf = KerberosTestFiles.unreachableKdcKrb5Conf(dir, "EXAMPLE.TEST");
		Path keytab = dir.resolve("svc.keytab");
		KerberosTestFiles.writeKeytab(keytab, "kafka-mcp", "EXAMPLE.TEST");
		System.setProperty(KerberosCheck.KRB5_CONF_PROPERTY, conf.toString());
		KafkaMcpConfig config = new KafkaMcpConfig();
		KerberosConfig kerberos = new KerberosConfig();
		kerberos.setKrb5Conf(conf.toString());
		kerberos.setKeytab(keytab.toString());
		kerberos.setPrincipal("kafka-mcp@EXAMPLE.TEST");
		config.setKerberos(kerberos);
		ClusterConfig cluster = new ClusterConfig();
		cluster.setName("krb");
		cluster.setBootstrapServers(KafkaTestCluster.bootstrap());
		cluster.setProperties(KafkaTestCluster.saslProperties("admin"));
		config.setClusters(List.of(cluster));
		KafkaClients clients = new KafkaClients(config);
		try {
			ToolCall call = new ToolCall("kerberos_self_check", Map.of("cluster", "krb"), clients,
					Instant.now().plusMillis(KafkaClients.TIMEOUT_MS));
			Map<String, Object> r = (Map<String, Object>) new SecurityTools().kerberosSelfCheck(call);
			assertThat(r.get("kerberosConfigured")).isEqualTo(true);
			assertThat(r.get("source")).isEqualTo("kerberos");
			assertThat(r.get("serviceName")).isEqualTo("kafka");
			Map<String, Object> login = (Map<String, Object>) r.get("login");
			assertThat(login.get("status")).isIn("error", "timeout");
			assertThat((String) login.get("error")).contains("Cannot reach the KDC");
			assertThat(((Map<String, Object>) r.get("keytab")).get("principalInKeytab")).isEqualTo(true);
			assertThat(((Map<String, Object>) r.get("krb5Conf")).get("defaultRealm")).isEqualTo("EXAMPLE.TEST");
			assertThat(r.get("ok")).isEqualTo(false);
			Map<String, Object> kafka = (Map<String, Object>) r.get("kafka");
			assertThat(kafka.get("status")).isEqualTo("ok");
			assertThat(r.get("notes").toString()).contains("sasl.mechanism=PLAIN");
			String json = ToolRegistry.resultMapper().writeValueAsString(r);
			assertThat(json).doesNotContain("admin-secret").contains("****");
		}
		finally {
			clients.destroy();
			if (previous == null) {
				System.clearProperty(KerberosCheck.KRB5_CONF_PROPERTY);
			}
			else {
				System.setProperty(KerberosCheck.KRB5_CONF_PROPERTY, previous);
			}
		}
	}

	/**
	 * The cluster uses GSSAPI with the kerberos block and the login fails: since Admin.create could block on the
	 * same login, the Kafka check is skipped; the diagnostic results are still returned.
	 */
	@Test
	@SuppressWarnings("unchecked")
	void kafkaCheckIsSkippedWhenGssapiLoginFails() throws Exception {
		String previous = System.getProperty(KerberosCheck.KRB5_CONF_PROPERTY);
		Path conf = KerberosTestFiles.unreachableKdcKrb5Conf(dir, "EXAMPLE.TEST");
		Path keytab = dir.resolve("svc.keytab");
		KerberosTestFiles.writeKeytab(keytab, "kafka-mcp", "EXAMPLE.TEST");
		System.setProperty(KerberosCheck.KRB5_CONF_PROPERTY, conf.toString());
		KafkaMcpConfig config = new KafkaMcpConfig();
		KerberosConfig kerberos = new KerberosConfig();
		kerberos.setKrb5Conf(conf.toString());
		kerberos.setKeytab(keytab.toString());
		kerberos.setPrincipal("kafka-mcp@EXAMPLE.TEST");
		config.setKerberos(kerberos);
		ClusterConfig cluster = new ClusterConfig();
		cluster.setName("krb-gssapi");
		cluster.setBootstrapServers("127.0.0.1:1");
		config.setClusters(List.of(cluster));
		KafkaClients clients = new KafkaClients(config) {

			@Override
			public Admin admin(String name) {
				throw new AssertionError("The Kafka client must not be created when the Kerberos login fails");
			}

		};
		try {
			ToolCall call = new ToolCall("kerberos_self_check", Map.of("cluster", "krb-gssapi"), clients,
					Instant.now().plusMillis(KafkaClients.TIMEOUT_MS));
			Map<String, Object> r = (Map<String, Object>) new SecurityTools().kerberosSelfCheck(call);
			assertThat(((Map<String, Object>) r.get("client")).get("saslMechanism")).isEqualTo("GSSAPI");
			Map<String, Object> login = (Map<String, Object>) r.get("login");
			assertThat(login.get("status")).isIn("error", "timeout");
			assertThat(((Map<String, Object>) r.get("krb5Conf")).get("defaultRealm")).isEqualTo("EXAMPLE.TEST");
			Map<String, Object> kafka = (Map<String, Object>) r.get("kafka");
			assertThat(kafka.get("status")).isEqualTo("skipped");
			assertThat((String) kafka.get("reason")).contains("Kerberos login failed", "not attempted");
		}
		finally {
			clients.destroy();
			if (previous == null) {
				System.clearProperty(KerberosCheck.KRB5_CONF_PROPERTY);
			}
			else {
				System.setProperty(KerberosCheck.KRB5_CONF_PROPERTY, previous);
			}
		}
	}

	/** Even if client creation blocks (e.g. KDC packets are dropped), the Kafka check finishes within the time limit. */
	@Test
	@SuppressWarnings("unchecked")
	void connectivityIsBoundedWhenClientCreationBlocks() {
		CountDownLatch release = new CountDownLatch(1);
		KafkaMcpConfig config = new KafkaMcpConfig();
		ClusterConfig cluster = new ClusterConfig();
		cluster.setName("slow");
		cluster.setBootstrapServers("127.0.0.1:1");
		config.setClusters(List.of(cluster));
		KafkaClients clients = new KafkaClients(config) {

			@Override
			public Admin admin(String name) {
				try {
					release.await(60, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				throw new ToolException("blocked");
			}

		};
		try {
			long start = System.nanoTime();
			ToolCall call = new ToolCall("kerberos_self_check", Map.of("cluster", "slow"), clients,
					Instant.now().plusMillis(3_000));
			Map<String, Object> r = (Map<String, Object>) new SecurityTools().kerberosSelfCheck(call);
			long elapsedMs = (System.nanoTime() - start) / 1_000_000;
			assertThat(elapsedMs).isLessThan(KafkaClients.TIMEOUT_MS);
			assertThat(r.get("kerberosConfigured")).isEqualTo(false);
			assertThat((String) r.get("note")).contains("Kerberos is not configured");
			Map<String, Object> kafka = (Map<String, Object>) r.get("kafka");
			assertThat(kafka.get("status")).isEqualTo("timeout");
			assertThat((String) kafka.get("error")).contains("was not created");
		}
		finally {
			release.countDown();
			clients.destroy();
		}
	}

}
