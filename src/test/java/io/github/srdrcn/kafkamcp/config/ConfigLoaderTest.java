package io.github.srdrcn.kafkamcp.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigLoaderTest {

	private static final String MAIN = """
			port: 8080
			apiKey: 0123456701234567
			readOnly: true
			disableDestructive: true
			toolsets: [core, messages]
			kerberos:
			  krb5Conf: /etc/kafka-mcp/krb5/krb5.conf
			  keytab: /etc/kafka-mcp/keytabs/svc.keytab
			  principal: svc@EXAMPLE.COM
			clusters:
			  - name: c1
			    bootstrapServers: c1:9092
			    connectUrl: http://c1:8083
			  - name: c2
			    bootstrapServers: c2:9092
			    readOnly: false
			    properties:
			      client.dns.lookup: use_all_dns_ips
			      ssl.truststore.password: 012345
			      retries: 0x1F
			      linger.ms: 1_000
			""";

	@Test
	void loadsMainConfig(@TempDir Path dir) throws Exception {
		Path file = Files.writeString(dir.resolve("kafka-mcp.yaml"), MAIN);
		KafkaMcpConfig c = ConfigLoader.load(file, null);
		assertThat(c.getPort()).isEqualTo(8080);
		assertThat(c.isApiKeyEnabled()).isTrue();
		assertThat(c.isReadOnly()).isTrue();
		assertThat(c.isDisableDestructive()).isTrue();
		assertThat(c.getToolsets()).containsExactly("core", "messages");
		assertThat(c.getKerberos().getServiceName()).isEqualTo("kafka");
		assertThat(c.getClusters()).extracting(ClusterConfig::getName).containsExactly("c1", "c2");
		assertThat(c.isReadOnly(c.cluster("c1"))).isTrue();
		assertThat(c.isReadOnly(c.cluster("c2"))).isFalse();
		assertThat(c.cluster("c1").hasConnect()).isTrue();
		assertThat(c.cluster("c2").getProperties()).containsEntry("client.dns.lookup", "use_all_dns_ips");
	}

	@Test
	void numbersKeepTheirTextForStringValues(@TempDir Path dir) throws Exception {
		// YAML 1.1 would read these as octal 5349 / hex 31 / 1000; the strings must stay as written
		KafkaMcpConfig c = ConfigLoader.load(Files.writeString(dir.resolve("kafka-mcp.yaml"), MAIN), null);
		assertThat(c.getApiKey()).isEqualTo("0123456701234567");
		assertThat(c.cluster("c2").getProperties()).containsEntry("ssl.truststore.password", "012345")
			.containsEntry("retries", "0x1F")
			.containsEntry("linger.ms", "1_000");
		assertThat(c.getPort()).isEqualTo(8080);
		assertThat(ConfigLoader.parse("port: 9090\nclusters: [{name: 123, bootstrapServers: 'x:9092'}]").getPort())
			.isEqualTo(9090);
		assertThatThrownBy(() -> ConfigLoader.parse("port: 0x1F90\nclusters: [{name: x, bootstrapServers: 'x:9092'}]"))
			.isInstanceOf(ConfigException.class)
			.hasMessageContaining("port");
	}

	@Test
	void validatesApiKey() {
		String clusters = "\nclusters: [{name: x, bootstrapServers: 'x:9092'}]";
		assertThat(ConfigLoader.parse("apiKey: ''" + clusters).isApiKeyEnabled()).isFalse();
		assertThat(ConfigLoader.parse("apiKey:" + clusters).isApiKeyEnabled()).isFalse();
		assertThat(ConfigLoader.parse("apiKey: 3f9a1c2b7d4e8f60a1b2" + clusters).getApiKey())
			.isEqualTo("3f9a1c2b7d4e8f60a1b2");
		assertThatThrownBy(() -> ConfigLoader.parse("apiKey: <api-key>" + clusters)).hasMessageContaining("placeholder");
		assertThatThrownBy(() -> ConfigLoader.parse("apiKey: '${KAFKA_MCP_KEY}'" + clusters))
			.hasMessageContaining("placeholder");
		assertThatThrownBy(() -> ConfigLoader.parse("apiKey: '   '" + clusters)).hasMessageContaining("blank");
		assertThatThrownBy(() -> ConfigLoader.parse("apiKey: ' 3f9a1c2b7d4e8f60a1b2'" + clusters))
			.hasMessageContaining("whitespace");
		assertThatThrownBy(() -> ConfigLoader.parse("apiKey: |\n  3f9a1c2b7d4e8f60a1b2" + clusters))
			.hasMessageContaining("whitespace");
		// short keys are accepted (only a startup warning is logged)
		assertThat(ConfigLoader.parse("apiKey: short-key" + clusters).getApiKey()).isEqualTo("short-key");
	}

	@Test
	void rejectsCredentialsInRestUrls() {
		assertThatThrownBy(() -> ConfigLoader.parse("""
				clusters:
				  - {name: x, bootstrapServers: 'x:9092', connectUrl: 'http://user:secret@connect.example.com:8083'}
				  - {name: y, bootstrapServers: 'y:9092', schemaRegistryUrl: 'https://u@sr.example.com'}
				""")).hasMessageContaining("cluster x: connectUrl must not contain credentials")
			.hasMessageContaining("cluster y: schemaRegistryUrl must not contain credentials")
			.hasMessageNotContaining("secret");
		assertThat(ConfigLoader.parse("""
				clusters:
				  - {name: x, bootstrapServers: 'x:9092', connectUrl: 'http://connect_1.example.com:8083/'}
				""").cluster("x").hasConnect()).isTrue();
	}

	@Test
	void dropInsMergeInFileNameOrderAndClustersByName(@TempDir Path dir) throws Exception {
		Path file = Files.writeString(dir.resolve("kafka-mcp.yaml"), MAIN);
		Path d = Files.createDirectory(dir.resolve("conf.d"));
		Files.writeString(d.resolve("20-later.yaml"), """
				readOnly: false
				clusters:
				  - name: c1
				    readOnly: true
				""");
		Files.writeString(d.resolve("10-first.yml"), """
				readOnly: true
				toolsets: [core]
				kerberos:
				  principal: other@EXAMPLE.COM
				clusters:
				  - name: c1
				    schemaRegistryUrl: http://c1:8081
				  - name: c3
				    bootstrapServers: c3:9092
				""");
		Files.writeString(d.resolve("ignored.txt"), "readOnly: [broken");
		KafkaMcpConfig c = ConfigLoader.load(file, d);
		assertThat(c.isReadOnly()).isFalse();
		assertThat(c.getToolsets()).containsExactly("core");
		assertThat(c.getKerberos().getPrincipal()).isEqualTo("other@EXAMPLE.COM");
		assertThat(c.getKerberos().getKeytab()).isEqualTo("/etc/kafka-mcp/keytabs/svc.keytab");
		assertThat(c.getClusters()).extracting(ClusterConfig::getName).containsExactly("c1", "c2", "c3");
		ClusterConfig c1 = c.cluster("c1");
		assertThat(c1.getBootstrapServers()).isEqualTo("c1:9092");
		assertThat(c1.getConnectUrl()).isEqualTo("http://c1:8083");
		assertThat(c1.getSchemaRegistryUrl()).isEqualTo("http://c1:8081");
		assertThat(c1.getReadOnly()).isTrue();
	}

	@Test
	void configDirAlone(@TempDir Path dir) throws Exception {
		Files.writeString(dir.resolve("a.yaml"), "clusters: [{name: x, bootstrapServers: 'x:9092'}]");
		KafkaMcpConfig c = ConfigLoader.load(null, dir);
		assertThat(c.getClusters()).hasSize(1);
		assertThat(c.getToolsets()).containsExactly("core");
		assertThat(c.isReadOnly()).isFalse();
		assertThat(c.isApiKeyEnabled()).isFalse();
	}

	@Test
	void rejectsUnknownFields() {
		assertThatThrownBy(() -> ConfigLoader.parse("""
				readonly: true
				clusters: [{name: x, bootstrapServers: 'x:9092'}]
				""")).isInstanceOf(ConfigException.class).hasMessageContaining("readonly");
	}

	@Test
	void validatesClusters() {
		assertThatThrownBy(() -> ConfigLoader.parse("readOnly: true")).hasMessageContaining("at least one cluster");
		assertThatThrownBy(() -> ConfigLoader.parse("""
				clusters:
				  - {name: x, bootstrapServers: 'x:9092'}
				  - {name: x, bootstrapServers: 'y:9092'}
				  - {name: 'bad name'}
				  - {name: z, bootstrapServers: 'z:9092', connectUrl: 'z:8083'}
				""")).hasMessageContaining("duplicate cluster name: x")
			.hasMessageContaining("invalid cluster name")
			.hasMessageContaining("connectUrl must start with http:// or https://");
	}

	@Test
	void kerberosRequiresPrincipalAndKeytab() {
		assertThatThrownBy(() -> ConfigLoader.parse("""
				kerberos: {krb5Conf: /x}
				clusters: [{name: x, bootstrapServers: 'x:9092'}]
				""")).hasMessageContaining("kerberos.principal").hasMessageContaining("kerberos.keytab");
	}

}
