package io.github.srdrcn.kafkamcp.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

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

}
