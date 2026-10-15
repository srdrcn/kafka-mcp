package io.github.srdrcn.kafkamcp.toolsets.connect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Maskeleme, config farkı, trace özeti ve plugin çözümleme (ağsız birim testleri). */
class ConnectSecretsTest {

	@Test
	void masksSecretKeys() {
		for (String k : List.of("database.password", "DB.PASSWORD", "ssl.key.password", "client.secret",
				"sasl.jaas.config", "basic.auth.user.info", "schema.registry.basic.auth.user.info", "auth.token",
				"aws.credentials.provider", "api.key", "aws.access.key", "aws.secret.access.key", "private.key",
				"apikey", "connection.ApiKey", "my.apiKeys")) {
			assertThat(ConnectSecrets.isSecret(k)).as(k).isTrue();
		}
		for (String k : List.of("key.converter", "key.converter.schemas.enable", "key.serializer", "value.converter",
				"connector.class", "topics", "tasks.max", "file", "ssl.keystore.location", "primary.key.mode",
				"pk.fields", "key.ignore", "name")) {
			assertThat(ConnectSecrets.isSecret(k)).as(k).isFalse();
		}
		Map<String, String> config = new LinkedHashMap<>();
		config.put("topics", "a");
		config.put("database.password", "s3cret");
		config.put("api.key", "abc");
		config.put("key.converter", "x.StringConverter");
		config.put("token.unset", null);
		Map<String, Object> masked = ConnectSecrets.mask(config);
		assertThat(masked).containsEntry("database.password", ConnectSecrets.MASK)
			.containsEntry("api.key", ConnectSecrets.MASK)
			.containsEntry("key.converter", "x.StringConverter")
			.containsEntry("topics", "a");
		assertThat(masked.get("token.unset")).isNull();
		assertThat(masked.keySet()).containsExactly("api.key", "database.password", "key.converter", "token.unset",
				"topics");
	}

	@Test
	void configDiffMasksValuesAndIgnoresName() {
		Map<String, String> current = Map.of("name", "a", "topics", "t1", "database.password", "old", "file", "/x",
				"tasks.max", "1");
		Map<String, String> target = Map.of("name", "b", "topics", "t2", "database.password", "new", "tasks.max", "1",
				"api.key", "k");
		Map<String, Object> d = ConfigDiff.diff(current, target, Set.of("name"));
		assertThat(d.get("identical")).isEqualTo(false);
		assertThat(d.get("added")).isEqualTo(Map.of("api.key", ConnectSecrets.MASK));
		assertThat(d.get("removed")).isEqualTo(Map.of("file", "/x"));
		assertThat(d.get("changed")).isEqualTo(List.of(
				Map.of("key", "database.password", "current", ConnectSecrets.MASK, "target", ConnectSecrets.MASK),
				Map.of("key", "topics", "current", "t1", "target", "t2")));
		Map<String, Object> same = ConfigDiff.diff(current, current, Set.of());
		assertThat(same.get("identical")).isEqualTo(true);
		assertThat(same).doesNotContainKeys("added", "removed", "changed").containsEntry("changedCount", 0);
	}

	@Test
	void traceSummaryKeepsFirstThreeLines() {
		String trace = "org.x.ConnectException: boom\n\tat a.B.c(B.java:1)\n\n\tat d.E.f(E.java:2)\n\tat g.H.i(H.java:3)\n";
		assertThat(ConnectJson.traceSummary(trace))
			.isEqualTo("org.x.ConnectException: boom\nat a.B.c(B.java:1)\nat d.E.f(E.java:2)");
		assertThat(ConnectJson.traceSummary(null)).isNull();
		assertThat(ConnectJson.traceSummary("x".repeat(1000))).hasSize(301);
	}

	@Test
	void resolvesPluginAliases() {
		List<String> plugins = List.of("org.apache.kafka.connect.file.FileStreamSinkConnector",
				"org.apache.kafka.connect.mirror.MirrorSourceConnector");
		assertThat(ConnectApi.resolvePlugin(plugins, "FileStreamSinkConnector")).isEqualTo(plugins.get(0));
		assertThat(ConnectApi.resolvePlugin(plugins, "filestreamsink")).isEqualTo(plugins.get(0));
		assertThat(ConnectApi.resolvePlugin(plugins, plugins.get(1))).isEqualTo(plugins.get(1));
		assertThatThrownBy(() -> ConnectApi.resolvePlugin(plugins, "Nope")).isInstanceOf(ToolException.class)
			.hasMessageContaining("bulunamadı")
			.hasMessageContaining("MirrorSourceConnector");
	}

	@Test
	void hidesUrlCredentials() {
		assertThat(ConnectClusterTools.safeUrl("http://user:pw@connect:8083")).isEqualTo("http://***@connect:8083");
		assertThat(ConnectClusterTools.safeUrl("http://connect:8083")).isEqualTo("http://connect:8083");
	}

}
