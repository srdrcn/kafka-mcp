package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.config.KerberosConfig;
import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.toolsets.acl.KerberosCheck;

/**
 * diagnose_connectivity Kerberos failure paths (no Spring context): the kerberos block shows a missing keytab or an
 * unreachable KDC; the Kafka step is skipped (GSSAPI cannot log in), and the cached AdminClient is never used. The
 * JVM-wide java.security.krb5.conf is restored at the end of the test.
 */
class DiagnoseConnectivityKerberosTest {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	@TempDir
	Path dir;

	private String previous;

	@BeforeEach
	void rememberKrb5() {
		previous = System.getProperty(KerberosCheck.KRB5_CONF_PROPERTY);
	}

	@AfterEach
	void restoreKrb5() {
		if (previous == null) {
			System.clearProperty(KerberosCheck.KRB5_CONF_PROPERTY);
		}
		else {
			System.setProperty(KerberosCheck.KRB5_CONF_PROPERTY, previous);
		}
	}

	private JsonNode run(Path keytab) throws Exception {
		Path conf = krb5Conf(dir, "EXAMPLE.TEST");
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
		config.setClusters(List.of(cluster));
		return run(config);
	}

	private JsonNode run(KafkaMcpConfig config) throws Exception {
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
			ToolCall call = new ToolCall("diagnose_connectivity", Map.of("cluster", "krb"), clients,
					Instant.now().plusMillis(KafkaClients.TIMEOUT_MS));
			JsonNode r = MAPPER.valueToTree(tools.diagnose(call));
			assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(KafkaClients.TIMEOUT_MS);
			return r;
		}
		finally {
			clients.destroy();
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

	@Test
	void missingKeytabIsExplainedAndKafkaStepSkipped() throws Exception {
		Path keytab = dir.resolve("missing.keytab");
		JsonNode r = run(keytab);
		assertThat(r.get("status").asText()).isEqualTo("critical");
		JsonNode f = DiagnoseTestBase.finding(r, "KEYTAB_NOT_FOUND");
		assertThat(f.get("message").asText()).contains("Keytab not found", keytab.toString());
		assertThat(f.get("recommendation").asText()).contains("Secret");
		assertThat(r.at("/facts/client/saslMechanism").asText()).isEqualTo("GSSAPI");
		assertThat(step(r, "tcp").get("status").asText()).isEqualTo("ok");
		assertThat(step(r, "kerberos").get("status").asText()).isEqualTo("error");
		JsonNode kafka = step(r, "kafka");
		assertThat(kafka.get("status").asText()).isEqualTo("skipped");
		assertThat(kafka.get("reason").asText()).contains("Kerberos");
	}

	@Test
	void unreachableKdcIsExplained() throws Exception {
		Path keytab = dir.resolve("svc.keytab");
		writeKeytab(keytab, "kafka-mcp", "EXAMPLE.TEST");
		JsonNode r = run(keytab);
		assertThat(r.get("status").asText()).isEqualTo("critical");
		JsonNode f = DiagnoseTestBase.finding(r, "KERBEROS_LOGIN_FAILED");
		assertThat(f.get("message").asText()).contains("Cannot reach the KDC");
		assertThat(f.get("recommendation").asText()).contains("88");
		assertThat(DiagnoseTestBase.findings(r, "KEYTAB_NOT_FOUND")).isEmpty();
		assertThat(step(r, "kafka").get("status").asText()).isEqualTo("skipped");
	}

	/**
	 * GSSAPI with a ticket cache (kinit sidecar, useTicketCache=true): an unset keytab/principal is not critical; since
	 * the login is not attempted ("skipped"), the Kafka step is not skipped but tried with a new AdminClient.
	 */
	@Test
	void ticketCacheSetupStillTriesKafka() throws Exception {
		Path conf = krb5Conf(dir, "EXAMPLE.TEST");
		System.setProperty(KerberosCheck.KRB5_CONF_PROPERTY, conf.toString());
		KafkaMcpConfig config = new KafkaMcpConfig();
		ClusterConfig cluster = new ClusterConfig();
		cluster.setName("krb");
		cluster.setBootstrapServers(KafkaTestCluster.bootstrap());
		cluster.setProperties(Map.of("security.protocol", "SASL_PLAINTEXT", "sasl.mechanism", "GSSAPI",
				"sasl.kerberos.service.name", "kafka", "sasl.jaas.config",
				"com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true;"));
		config.setClusters(List.of(cluster));
		JsonNode r = run(config);
		assertThat(step(r, "kerberos").get("status").asText()).isEqualTo("skipped");
		assertThat(step(r, "kerberos").get("ticketCache").asBoolean()).isTrue();
		JsonNode kafka = step(r, "kafka");
		assertThat(kafka.get("status").asText()).as(r.toString()).isNotEqualTo("skipped");
		List<JsonNode> notes = DiagnoseTestBase.findings(r, "KERBEROS_TICKET_CACHE");
		assertThat(notes).isNotEmpty();
		assertThat(notes).allSatisfy(n -> assertThat(n.get("severity").asText()).isEqualTo("info"));
		// the problem text comes from KerberosCheck (acl toolset); check its leading token and our explanation only
		assertThat(notes).anySatisfy(n -> assertThat(n.get("message").asText()).startsWith("keytab")
			.contains("useTicketCache=true"));
		assertThat(DiagnoseTestBase.findings(r, "KERBEROS_PROBLEM")).isEmpty();
	}

	/** krb5.conf whose KDC is unreachable (127.0.0.1:1, short timeout, single attempt). */
	static Path krb5Conf(Path dir, String realm) throws IOException {
		Path conf = dir.resolve("krb5.conf");
		Files.writeString(conf, """
				[libdefaults]
				  default_realm = %s
				  dns_lookup_kdc = false
				  dns_lookup_realm = false
				  rdns = false
				  kdc_timeout = 300
				  max_retries = 1
				  udp_preference_limit = 65535

				[realms]
				  %s = {
				    kdc = 127.0.0.1:1
				  }
				""".formatted(realm, realm), StandardCharsets.UTF_8);
		return conf;
	}

	/** Fake single-entry keytab in MIT keytab format (v0x502) (aes128-cts-hmac-sha1-96). */
	static void writeKeytab(Path file, String name, String realm) throws IOException {
		ByteArrayOutputStream entry = new ByteArrayOutputStream();
		DataOutputStream e = new DataOutputStream(entry);
		e.writeShort(1);
		writeString(e, realm);
		writeString(e, name);
		e.writeInt(1);
		e.writeInt((int) (System.currentTimeMillis() / 1000));
		e.writeByte(1);
		e.writeShort(17);
		byte[] key = new byte[16];
		for (int i = 0; i < key.length; i++) {
			key[i] = (byte) (i + 1);
		}
		e.writeShort(key.length);
		e.write(key);
		e.writeInt(1);
		e.flush();
		try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(file))) {
			out.writeShort(0x0502);
			out.writeInt(entry.size());
			out.write(entry.toByteArray());
		}
	}

	private static void writeString(DataOutputStream out, String s) throws IOException {
		byte[] b = s.getBytes(StandardCharsets.UTF_8);
		out.writeShort(b.length);
		out.write(b);
	}

}
