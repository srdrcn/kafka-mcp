package io.github.srdrcn.kafkamcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanCreationException;

import io.github.srdrcn.kafkamcp.config.ConfigException;

/** Command line parsing and the mapping of startup config errors to exit code 2. */
class KafkaMcpApplicationTest {

	@Test
	void parsesOptions() {
		KafkaMcpApplication.Cli cli = KafkaMcpApplication.Cli
			.parse(new String[] { "--config=a.yaml", "--port", "9090", "--transport", "http", "--logging.level.root=WARN" });
		assertThat(cli.config.toString()).isEqualTo("a.yaml");
		assertThat(cli.port).isEqualTo(9090);
		assertThat(cli.transport).isEqualTo("http");
		assertThat(cli.passthrough).containsExactly("--logging.level.root=WARN");
	}

	@Test
	void rejectsPortsOutOfRange() {
		for (String port : new String[] { "0", "-1", "65536", "70000" }) {
			assertThatThrownBy(() -> KafkaMcpApplication.Cli.parse(new String[] { "--port", port }))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("between 1 and 65535");
		}
		assertThat(KafkaMcpApplication.Cli.parse(new String[] { "--port=65535" }).port).isEqualTo(65535);
		assertThatThrownBy(() -> KafkaMcpApplication.Cli.parse(new String[] { "--port", "http" }))
			.hasMessageContaining("must be a number");
	}

	@Test
	void findsConfigErrorsRaisedDuringStartup() {
		ConfigException unknownToolset = new ConfigException("Unknown toolset: [nope]");
		RuntimeException wrapped = new BeanCreationException("toolRegistry", "Failed to instantiate",
				new IllegalStateException(unknownToolset));
		assertThat(KafkaMcpApplication.configCause(wrapped)).isSameAs(unknownToolset);
		assertThat(KafkaMcpApplication.configCause(new IllegalStateException("port in use"))).isNull();
	}

}
