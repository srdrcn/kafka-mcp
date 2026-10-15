package io.github.srdrcn.kafkamcp;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.FullyQualifiedAnnotationBeanNameGenerator;

import io.github.srdrcn.kafkamcp.config.ConfigException;
import io.github.srdrcn.kafkamcp.config.ConfigLoader;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;

// Toolsets may contain classes with the same name (e.g. messages.SchemaTools / schemaregistry.SchemaTools):
// bean names are generated including the package.
@SpringBootApplication(nameGenerator = FullyQualifiedAnnotationBeanNameGenerator.class)
public class KafkaMcpApplication {

	static final String USAGE = """
			Usage: java -jar kafka-mcp.jar --config kafka-mcp.yaml [options]

			  --config <file>         Main config file (YAML)
			  --config-dir <dir>      Drop-in .yaml/.yml files (merged over the main config in name order)
			  --port <port>           HTTP port (overrides the port in the config)
			  --transport <http|stdio>
			                          Default: http if a port is set (Streamable HTTP, /mcp), otherwise stdio
			  --help                  Show this help
			""";

	public static void main(String[] args) {
		Cli cli;
		KafkaMcpConfig config;
		try {
			cli = Cli.parse(args);
			if (cli.help) {
				System.err.print(USAGE);
				return;
			}
			if (cli.config == null && cli.configDir == null) {
				throw new ConfigException("--config or --config-dir is required\n\n" + USAGE);
			}
			config = ConfigLoader.load(cli.config, cli.configDir);
			if (cli.port != null) {
				config.setPort(cli.port);
			}
		}
		catch (ConfigException | IllegalArgumentException e) {
			System.err.println("kafka-mcp: " + e.getMessage());
			System.exit(2);
			return;
		}
		String transport = cli.transport != null ? cli.transport
				: (config.getPort() != null && config.getPort() > 0 ? "http" : "stdio");
		boolean http = "http".equals(transport);
		if (http && (config.getPort() == null || config.getPort() <= 0)) {
			config.setPort(8080);
		}

		Map<String, Object> props = new HashMap<>();
		props.put("kafka-mcp.transport", transport);
		props.put("kafka-mcp.log-target", http ? "System.out" : "System.err");
		if (http) {
			props.put("server.port", config.getPort());
		}
		final KafkaMcpConfig loaded = config;
		ConfigurableApplicationContext ctx = new SpringApplicationBuilder(KafkaMcpApplication.class)
			.web(http ? WebApplicationType.SERVLET : WebApplicationType.NONE)
			.properties(props)
			.initializers(c -> c.getBeanFactory().registerSingleton("kafkaMcpConfig", loaded))
			.run(cli.passthrough.toArray(String[]::new));
		if (!http) {
			System.exit(SpringApplication.exit(ctx));
		}
	}

	static final class Cli {

		Path config;

		Path configDir;

		Integer port;

		String transport;

		boolean help;

		final List<String> passthrough = new ArrayList<>();

		static Cli parse(String[] args) {
			Cli cli = new Cli();
			for (int i = 0; i < args.length; i++) {
				String a = args[i];
				String name = a;
				String value = null;
				int eq = a.indexOf('=');
				if (a.startsWith("--") && eq > 0) {
					name = a.substring(0, eq);
					value = a.substring(eq + 1);
				}
				switch (name) {
					case "--help", "-h" -> cli.help = true;
					case "--config", "--config-dir", "--port", "--transport" -> {
						if (value == null) {
							if (i + 1 >= args.length) {
								throw new IllegalArgumentException("Missing value for " + name);
							}
							value = args[++i];
						}
						switch (name) {
							case "--config" -> cli.config = Path.of(value);
							case "--config-dir" -> cli.configDir = Path.of(value);
							case "--port" -> {
								try {
									cli.port = Integer.parseInt(value);
								}
								catch (NumberFormatException e) {
									throw new IllegalArgumentException("--port must be a number: " + value);
								}
							}
							default -> {
								if (!value.equals("http") && !value.equals("stdio")) {
									throw new IllegalArgumentException("--transport must be http or stdio: " + value);
								}
								cli.transport = value;
							}
						}
					}
					default -> cli.passthrough.add(a);
				}
			}
			return cli;
		}

	}

}
