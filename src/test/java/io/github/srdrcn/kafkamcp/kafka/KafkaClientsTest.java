package io.github.srdrcn.kafkamcp.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.config.ConfigLoader;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

class KafkaClientsTest {

	@Test
	void kerberosBlockProducesGssapiJaasAndClusterPropertiesOverride() {
		KafkaMcpConfig config = ConfigLoader.parse("""
				kerberos:
				  keytab: /etc/kafka-mcp/keytabs/svc.keytab
				  principal: svc@EXAMPLE.COM
				clusters:
				  - name: a
				    bootstrapServers: a:9092
				  - name: b
				    bootstrapServers: b:9092
				    properties:
				      sasl.kerberos.service.name: kafka2
				      request.timeout.ms: "20000"
				""");
		KafkaClients clients = new KafkaClients(config);
		Map<String, Object> a = clients.clientProperties(config.cluster("a"));
		assertThat(a).containsEntry("bootstrap.servers", "a:9092")
			.containsEntry("security.protocol", "SASL_PLAINTEXT")
			.containsEntry("sasl.mechanism", "GSSAPI")
			.containsEntry("sasl.kerberos.service.name", "kafka")
			.containsEntry("request.timeout.ms", 15000);
		assertThat((String) a.get("sasl.jaas.config")).isEqualTo(
				"com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true useTicketCache=false "
						+ "keyTab=\"/etc/kafka-mcp/keytabs/svc.keytab\" principal=\"svc@EXAMPLE.COM\";");
		Map<String, Object> b = clients.clientProperties(config.cluster("b"));
		assertThat(b).containsEntry("sasl.kerberos.service.name", "kafka2").containsEntry("request.timeout.ms", "20000");
		clients.destroy();
	}

	@Test
	void withoutKerberosNoSaslDefaults() {
		KafkaMcpConfig config = ConfigLoader.parse("clusters: [{name: a, bootstrapServers: 'a:9092'}]");
		Map<String, Object> a = new KafkaClients(config).clientProperties(config.cluster("a"));
		assertThat(a).doesNotContainKeys("security.protocol", "sasl.mechanism", "sasl.jaas.config");
	}

	@Test
	void unknownClusterIsToolError() {
		KafkaMcpConfig config = ConfigLoader.parse("clusters: [{name: a, bootstrapServers: 'a:9092'}]");
		assertThatThrownBy(() -> new KafkaClients(config).admin("nope")).isInstanceOf(ToolException.class)
			.hasMessageContaining("Unknown cluster: nope");
	}

	@Test
	void brokenClusterDoesNotAffectOthers() {
		KafkaMcpConfig config = ConfigLoader.parse("""
				clusters:
				  - name: broken
				    bootstrapServers: a:9092
				    properties:
				      security.protocol: SASL_PLAINTEXT
				      sasl.mechanism: GSSAPI
				      sasl.jaas.config: com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab="/nonexistent.keytab" principal="x@EXAMPLE.COM";
				  - name: fine
				    bootstrapServers: localhost:1
				""");
		KafkaClients clients = new KafkaClients(config);
		assertThatThrownBy(() -> clients.admin("broken")).isInstanceOf(ToolException.class)
			.hasMessageContaining("broken");
		assertThat(clients.admin("fine")).isNotNull();
		assertThat(clients.admin("fine")).isSameAs(clients.admin("fine"));
		clients.destroy();
	}

	@Test
	void kerberosMessagesAreTranslated() {
		assertThat(KafkaErrors.kerberos("Receive timed out")).contains("Cannot reach the KDC");
		assertThat(KafkaErrors.kerberos("Clock skew too great (37)")).contains("clock skew");
		assertThat(KafkaErrors.kerberos("Unable to obtain password from user")).contains("Keytab not found");
		assertThat(KafkaErrors.kerberos("Server not found in Kerberos database (7)")).contains("rdns = false");
		assertThat(KafkaErrors.kerberos("something else")).isNull();
	}

	@Test
	void consumerSafetySettingsWinOverClusterProperties() {
		KafkaMcpConfig config = ConfigLoader.parse("""
				clusters:
				  - name: a
				    bootstrapServers: a:9092
				    properties:
				      group.id: someone-elses-group
				      group.instance.id: static-member
				      enable.auto.commit: "true"
				      allow.auto.create.topics: "true"
				      auto.offset.reset: earliest
				      isolation.level: read_committed
				      key.deserializer: org.apache.kafka.common.serialization.StringDeserializer
				      max.poll.records: "100"
				      fetch.max.bytes: "1048576"
				      client.id: team-x
				""");
		KafkaClients clients = new KafkaClients(config);
		Map<String, Object> p = clients.consumerProperties(config.cluster("a"), null);
		assertThat(p).doesNotContainKeys("group.id", "group.instance.id")
			.containsEntry("enable.auto.commit", false)
			.containsEntry("allow.auto.create.topics", false)
			.containsEntry("auto.offset.reset", "none")
			.containsEntry("isolation.level", "read_uncommitted")
			.containsEntry("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
			// plain defaults are overridable
			.containsEntry("max.poll.records", "100")
			.containsEntry("fetch.max.bytes", "1048576");
		assertThat((String) p.get("client.id")).startsWith("team-x-reader-");
		assertThat(clients.consumerProperties(config.cluster("a"), null).get("client.id"))
			.isNotEqualTo(p.get("client.id"));
		Map<String, Object> overridden = clients.consumerProperties(config.cluster("a"),
				Map.of("allow.auto.create.topics", true, "max.poll.records", 7));
		assertThat(overridden).containsEntry("allow.auto.create.topics", false).containsEntry("max.poll.records", 7);
		clients.destroy();
	}

	@Test
	void consumerDefaultsWithoutClusterProperties() {
		KafkaMcpConfig config = ConfigLoader.parse("clusters: [{name: a, bootstrapServers: 'a:9092'}]");
		Map<String, Object> p = new KafkaClients(config).consumerProperties(config.cluster("a"), null);
		assertThat(p).containsEntry("max.poll.records", 500)
			.containsEntry("fetch.max.bytes", KafkaClients.CONSUMER_FETCH_MAX_BYTES)
			.containsEntry("allow.auto.create.topics", false);
		assertThat((String) p.get("client.id")).startsWith("kafka-mcp-a-reader-");
	}

	@Test
	void adminDefaultsAreOverridableByClusterProperties() {
		KafkaMcpConfig config = ConfigLoader.parse("""
				clusters:
				  - name: a
				    bootstrapServers: a:9092
				  - name: b
				    bootstrapServers: b:9092
				    properties:
				      retries: "7"
				      client.id: team-x
				""");
		KafkaClients clients = new KafkaClients(config);
		assertThat(clients.adminProperties(config.cluster("a"))).containsEntry("retries", 3)
			.containsEntry("client.id", "kafka-mcp-a-admin");
		assertThat(clients.adminProperties(config.cluster("b"))).containsEntry("retries", "7")
			.containsEntry("client.id", "team-x");
		clients.destroy();
	}

	@Test
	void clientsAreCreatedOnPlatformThreads() throws Exception {
		KafkaMcpConfig config = ConfigLoader.parse("clusters: [{name: a, bootstrapServers: 'a:9092'}]");
		KafkaClients clients = new KafkaClients(config);
		try (ExecutorService virtual = Executors.newVirtualThreadPerTaskExecutor()) {
			Boolean createdOnVirtual = virtual
				.submit(() -> clients.create(config.cluster("a"), "test", () -> Thread.currentThread().isVirtual(),
						x -> {
						}))
				.get();
			assertThat(createdOnVirtual).isFalse();
		}
		clients.destroy();
	}

	@Test
	void concurrentFirstCallsShareOneAdminClient() throws Exception {
		KafkaMcpConfig config = ConfigLoader.parse("clusters: [{name: fine, bootstrapServers: 'localhost:1'}]");
		KafkaClients clients = new KafkaClients(config);
		List<Future<Admin>> calls = new ArrayList<>();
		try (ExecutorService virtual = Executors.newVirtualThreadPerTaskExecutor()) {
			for (int i = 0; i < 16; i++) {
				calls.add(virtual.submit(() -> clients.admin("fine")));
			}
			Admin first = calls.get(0).get();
			for (Future<Admin> f : calls) {
				assertThat(f.get()).isSameAs(first);
			}
		}
		clients.reset("fine");
		assertThat(clients.admin("fine")).isNotNull();
		clients.destroy();
	}

}
