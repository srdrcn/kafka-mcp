package io.github.srdrcn.kafkamcp.mcp;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;

import io.github.srdrcn.kafkamcp.KafkaMcpApplication;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.support.TestConfigs;
import io.github.srdrcn.kafkamcp.support.TestToolsets;
import io.modelcontextprotocol.json.McpJsonMapper;

/**
 * Snapshot test of the tool schemas and the Claude Code compatibility rules. Snapshots live in
 * {@code src/test/resources/snapshots/<toolset>.json}; to update them run
 * {@code ./mvnw test -Dtest=ToolSchemaSnapshotTest -Dsnapshot.update=true}.
 */
@SpringBootTest(classes = { KafkaMcpApplication.class, ToolSchemaSnapshotTest.Config.class },
		webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ToolSchemaSnapshotTest {

	private static final Path SNAPSHOTS = Path.of("src", "test", "resources", "snapshots");

	private static final Pattern NAME = Pattern.compile("^[a-z][a-z0-9]*(_[a-z0-9]+)+$");

	/** Tools turned off by disableDestructive: names ending in *_delete, *_reset or *_delete_*. */
	private static final Pattern DESTRUCTIVE_NAME = Pattern.compile("(_delete|_reset)(_[a-z0-9]+)?$");

	private static final ObjectMapper PRETTY = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	@Autowired
	private ToolRegistry registry;

	@Autowired
	private List<Toolset> toolsets;

	@Autowired
	private McpJsonMapper wire;

	@TestConfiguration(proxyBeanMethods = false)
	static class Config {

		@Bean
		KafkaMcpConfig kafkaMcpConfig() {
			return TestConfigs.offline();
		}

	}

	private JsonNode onTheWire(RegisteredTool t) throws Exception {
		return PRETTY.readTree(wire.writeValueAsString(t.tool()));
	}

	@Test
	void toolsetNamesMatchPackages() {
		for (Toolset t : toolsets) {
			String pkg = t.getClass().getPackageName();
			assertThat(pkg).as("toolset package").endsWith(".toolsets." + t.name());
		}
		assertThat(toolsets).extracting(Toolset::name).containsExactlyInAnyOrderElementsOf(TestToolsets.all());
	}

	@Test
	void everyToolIsListedWithFullConfig() {
		int declared = toolsets.stream().mapToInt(t -> t.tools().size()).sum();
		assertThat(registry.tools()).hasSize(declared);
	}

	@Test
	void schemasAreClaudeCodeCompatible() throws Exception {
		for (RegisteredTool t : registry.tools()) {
			String name = t.spec().name();
			JsonNode json = onTheWire(t);
			assertThat(name).as("tool name").matches(NAME).hasSizeLessThanOrEqualTo(64);
			assertThat(json.path("description").asText()).as("%s description", name).isNotBlank().hasSizeLessThan(400);
			JsonNode schema = json.get("inputSchema");
			assertThat(schema.path("type").asText()).as("%s type", name).isEqualTo("object");
			assertThat(schema.get("properties")).as("%s properties", name).isNotNull();
			assertThat(schema.get("properties").isObject()).isTrue();
			assertThat(schema.get("required")).as("%s required", name).isNotNull();
			assertThat(schema.get("required").isArray()).isTrue();
			for (JsonNode r : schema.get("required")) {
				assertThat(schema.get("properties").has(r.asText())).as("%s required %s in properties", name, r)
					.isTrue();
			}
			Iterator<Map.Entry<String, JsonNode>> props = schema.get("properties").fields();
			while (props.hasNext()) {
				Map.Entry<String, JsonNode> p = props.next();
				assertThat(p.getValue().path("type").asText()).as("%s.%s type", name, p.getKey()).isNotBlank();
				assertThat(p.getValue().path("description").asText()).as("%s.%s description", name, p.getKey())
					.isNotBlank();
				if (p.getValue().has("enum")) {
					assertThat(p.getValue().get("enum").size()).as("%s.%s enum", name, p.getKey()).isPositive();
				}
			}
			assertThat(schema.at("/properties/cluster/enum").isArray()).as("%s cluster enum", name).isTrue();
			assertNoNulls(name, json);
		}
	}

	@Test
	void listToolsTakeFilterAndLimit() throws Exception {
		for (RegisteredTool t : registry.tools()) {
			if (!t.spec().name().endsWith("_list")) {
				continue;
			}
			JsonNode props = onTheWire(t).at("/inputSchema/properties");
			assertThat(props.path("filter").path("type").asText()).as("%s filter", t.spec().name()).isEqualTo("string");
			assertThat(props.path("limit").path("type").asText()).as("%s limit", t.spec().name()).isEqualTo("integer");
		}
	}

	@Test
	void writeToolsStartWithWritePrefix() {
		for (RegisteredTool t : registry.tools()) {
			boolean write = t.spec().description().startsWith("WRITE:");
			assertThat(write).as("%s: write tools (and only they) start with 'WRITE:'", t.spec().name())
				.isEqualTo(t.spec().kind().writes());
		}
	}

	@Test
	void destructiveToolsAreExactlyDeleteAndReset() {
		for (RegisteredTool t : registry.tools()) {
			boolean byName = DESTRUCTIVE_NAME.matcher(t.spec().name()).find();
			assertThat(t.spec().kind() == ToolKind.DESTRUCTIVE)
				.as("%s: DESTRUCTIVE iff name is *_delete/*_reset", t.spec().name())
				.isEqualTo(byName);
		}
	}

	@Test
	void snapshotsMatch() throws Exception {
		boolean update = "true".equals(System.getProperty("snapshot.update"));
		Map<String, ArrayNode> byToolset = new LinkedHashMap<>();
		for (RegisteredTool t : registry.tools()) {
			byToolset.computeIfAbsent(t.toolset(), k -> PRETTY.createArrayNode()).add(onTheWire(t));
		}
		List<String> mismatches = new ArrayList<>();
		for (Map.Entry<String, ArrayNode> e : byToolset.entrySet()) {
			Path file = SNAPSHOTS.resolve(e.getKey() + ".json");
			String actual = PRETTY.writeValueAsString(e.getValue()) + "\n";
			if (update) {
				Files.createDirectories(SNAPSHOTS);
				Files.writeString(file, actual, StandardCharsets.UTF_8);
				continue;
			}
			if (!Files.exists(file)) {
				mismatches.add(file + " missing");
				continue;
			}
			String expected = Files.readString(file, StandardCharsets.UTF_8);
			if (!PRETTY.readTree(expected).equals(PRETTY.readTree(actual))) {
				mismatches.add(file + " differs");
			}
		}
		assertThat(mismatches)
			.as("Tool schemas differ from the snapshot. If the change is intended: ./mvnw test -Dtest=ToolSchemaSnapshotTest -Dsnapshot.update=true")
			.isEmpty();
	}

	private static void assertNoNulls(String tool, JsonNode node) {
		if (node.isNull()) {
			throw new AssertionError(tool + ": schema contains a null value");
		}
		if (node.isContainerNode()) {
			for (JsonNode child : node) {
				assertNoNulls(tool, child);
			}
		}
	}

}
