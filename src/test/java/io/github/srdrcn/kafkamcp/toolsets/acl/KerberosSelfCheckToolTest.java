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
 * kerberos_self_check'in kerberos bloğu tanımlı yolu: KDC erişilemez (login Türkçe hata), cluster ise
 * properties ile PLAIN kullandığı için Kafka bağlantısı yine başarılı. Spring bağlamı kullanılmaz;
 * JVM genelindeki java.security.krb5.conf test sonunda geri alınır.
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
			assertThat((String) login.get("error")).contains("KDC'ye ulaşılamadı");
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
	 * Cluster kerberos bloğuyla GSSAPI kullanıyor ve login başarısız: Admin.create aynı login'de
	 * bloklanabileceği için Kafka kontrolü atlanır, teşhis sonuçları yine döner.
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
				throw new AssertionError("Kerberos login başarısızken Kafka client'ı oluşturulmamalı");
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
			assertThat((String) kafka.get("reason")).contains("Kerberos login başarısız", "denenmedi");
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

	/** Client oluşturma bloklansa da (ör. KDC paketleri düşürülüyor) Kafka kontrolü süre sınırında biter. */
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
				throw new ToolException("bloklandı");
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
			assertThat((String) r.get("note")).contains("Kerberos yapılandırılmamış");
			Map<String, Object> kafka = (Map<String, Object>) r.get("kafka");
			assertThat(kafka.get("status")).isEqualTo("timeout");
			assertThat((String) kafka.get("error")).contains("oluşturulamadı");
		}
		finally {
			release.countDown();
			clients.destroy();
		}
	}

}
