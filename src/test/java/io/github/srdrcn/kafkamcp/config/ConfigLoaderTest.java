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
			apiKey: <api-key>
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
		assertThatThrownBy(() -> ConfigLoader.parse("readOnly: true")).hasMessageContaining("en az bir cluster");
		assertThatThrownBy(() -> ConfigLoader.parse("""
				clusters:
				  - {name: x, bootstrapServers: 'x:9092'}
				  - {name: x, bootstrapServers: 'y:9092'}
				  - {name: 'bad name'}
				  - {name: z, bootstrapServers: 'z:9092', connectUrl: 'z:8083'}
				""")).hasMessageContaining("tekrar ediyor: x")
			.hasMessageContaining("geçersiz cluster adı")
			.hasMessageContaining("connectUrl http:// veya https://");
	}

	@Test
	void kerberosRequiresPrincipalAndKeytab() {
		assertThatThrownBy(() -> ConfigLoader.parse("""
				kerberos: {krb5Conf: /x}
				clusters: [{name: x, bootstrapServers: 'x:9092'}]
				""")).hasMessageContaining("kerberos.principal").hasMessageContaining("kerberos.keytab");
	}

}
