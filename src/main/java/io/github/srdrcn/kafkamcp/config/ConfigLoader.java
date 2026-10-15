package io.github.srdrcn.kafkamcp.config;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Reads and merges kafka-mcp.yaml and the drop-in .yaml/.yml files under --config-dir (in file name
 * order). Maps are deep-merged; the {@code clusters} list is merged by the {@code name} field (a drop-in can
 * add a new cluster or override fields of an existing one); other lists are replaced entirely. YAML numbers keep
 * their original text (see {@link #readTree(JsonParser)}).
 */
public final class ConfigLoader {

	private static final Pattern CLUSTER_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_.-]{0,62}");

	/** Recommended minimum length of a non-empty apiKey; shorter keys only produce a startup warning. */
	public static final int RECOMMENDED_API_KEY_LENGTH = 16;

	private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
		.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

	private ConfigLoader() {
	}

	public static KafkaMcpConfig load(Path configFile, Path configDir) {
		ObjectNode root = YAML.createObjectNode();
		if (configFile != null) {
			merge(root, read(configFile));
		}
		if (configDir != null) {
			for (Path dropIn : dropIns(configDir)) {
				merge(root, read(dropIn));
			}
		}
		KafkaMcpConfig config;
		try {
			config = YAML.treeToValue(root, KafkaMcpConfig.class);
		}
		catch (com.fasterxml.jackson.core.JsonProcessingException e) {
			throw new ConfigException("Could not read config: " + e.getOriginalMessage() + location(e), e);
		}
		validate(config);
		return config;
	}

	public static KafkaMcpConfig parse(String yaml) {
		try {
			ObjectNode root = YAML.createObjectNode();
			JsonNode node;
			try (JsonParser p = YAML.getFactory().createParser(yaml)) {
				node = readTree(p);
			}
			if (node != null && node.isObject()) {
				merge(root, (ObjectNode) node);
			}
			KafkaMcpConfig config = YAML.treeToValue(root, KafkaMcpConfig.class);
			validate(config);
			return config;
		}
		catch (IOException e) {
			throw new ConfigException("Could not read config: " + e.getMessage(), e);
		}
	}

	private static String location(com.fasterxml.jackson.core.JsonProcessingException e) {
		if (e instanceof com.fasterxml.jackson.databind.JsonMappingException jme && !jme.getPath().isEmpty()) {
			StringBuilder sb = new StringBuilder(" (field: ");
			for (com.fasterxml.jackson.databind.JsonMappingException.Reference r : jme.getPath()) {
				sb.append(r.getFieldName() != null ? "." + r.getFieldName() : "[" + r.getIndex() + "]");
			}
			return sb.append(")").toString().replace("(field: .", "(field: ");
		}
		return "";
	}

	static List<Path> dropIns(Path dir) {
		if (!Files.isDirectory(dir)) {
			throw new ConfigException("--config-dir is not a directory: " + dir);
		}
		try (Stream<Path> files = Files.list(dir)) {
			return files.filter(Files::isRegularFile)
				.filter(p -> {
					String n = p.getFileName().toString();
					return !n.startsWith(".") && (n.endsWith(".yaml") || n.endsWith(".yml"));
				})
				.sorted()
				.toList();
		}
		catch (IOException e) {
			throw new ConfigException("Could not read --config-dir: " + dir + " (" + e.getMessage() + ")", e);
		}
	}

	private static ObjectNode read(Path file) {
		try (JsonParser p = YAML.getFactory().createParser(file.toFile())) {
			JsonNode node = readTree(p);
			if (node == null || node.isMissingNode() || node.isNull()) {
				return YAML.createObjectNode();
			}
			if (!node.isObject()) {
				throw new ConfigException("Config file must be a YAML map: " + file);
			}
			return (ObjectNode) node;
		}
		catch (IOException e) {
			throw new ConfigException("Could not read config file: " + file + " (" + e.getMessage() + ")", e);
		}
	}

	/**
	 * Reads the first YAML document into a tree in which numbers stay text: YAML 1.1 reads {@code 012345} as octal
	 * 5349, {@code 0x1F} as 31 and {@code 1_000} as 1000, so binding them to a String (apiKey, a {@code properties}
	 * value such as a password) would silently change the value. Numeric fields (port) are converted from the text
	 * when the tree is bound. Booleans and nulls keep their type.
	 */
	static JsonNode readTree(JsonParser p) throws IOException {
		JsonToken t = p.nextToken();
		return t == null ? null : node(p, t);
	}

	private static JsonNode node(JsonParser p, JsonToken t) throws IOException {
		JsonNodeFactory f = JsonNodeFactory.instance;
		switch (t) {
			case START_OBJECT -> {
				ObjectNode o = f.objectNode();
				while (p.nextToken() == JsonToken.FIELD_NAME) {
					String name = p.currentName();
					o.set(name, node(p, p.nextToken()));
				}
				return o;
			}
			case START_ARRAY -> {
				ArrayNode a = f.arrayNode();
				JsonToken next;
				while ((next = p.nextToken()) != JsonToken.END_ARRAY) {
					a.add(node(p, next));
				}
				return a;
			}
			case VALUE_TRUE -> {
				return f.booleanNode(true);
			}
			case VALUE_FALSE -> {
				return f.booleanNode(false);
			}
			case VALUE_NULL -> {
				return f.nullNode();
			}
			default -> {
				// strings and numbers (original text), and other scalars
				return f.textNode(p.getText());
			}
		}
	}

	static void merge(ObjectNode target, ObjectNode source) {
		Iterator<Map.Entry<String, JsonNode>> it = source.fields();
		while (it.hasNext()) {
			Map.Entry<String, JsonNode> e = it.next();
			String key = e.getKey();
			JsonNode value = e.getValue();
			JsonNode existing = target.get(key);
			if ("clusters".equals(key) && value.isArray() && existing != null && existing.isArray()) {
				target.set(key, mergeClusters((ArrayNode) existing, (ArrayNode) value));
			}
			else if (value.isObject() && existing != null && existing.isObject()) {
				ObjectNode copy = ((ObjectNode) existing).deepCopy();
				merge(copy, (ObjectNode) value);
				target.set(key, copy);
			}
			else {
				target.set(key, value.deepCopy());
			}
		}
	}

	private static ArrayNode mergeClusters(ArrayNode base, ArrayNode overlay) {
		ArrayNode result = base.deepCopy();
		for (JsonNode item : overlay) {
			JsonNode name = item.get("name");
			ObjectNode match = null;
			if (name != null && item.isObject()) {
				for (JsonNode existing : result) {
					if (existing.isObject() && name.equals(existing.get("name"))) {
						match = (ObjectNode) existing;
						break;
					}
				}
			}
			if (match != null) {
				merge(match, (ObjectNode) item);
			}
			else {
				result.add(item.deepCopy());
			}
		}
		return result;
	}

	static void validate(KafkaMcpConfig config) {
		List<String> errors = new ArrayList<>();
		if (config.getPort() != null && (config.getPort() < 0 || config.getPort() > 65535)) {
			errors.add("port must be between 0 and 65535");
		}
		String apiKeyError = apiKeyError(config.getApiKey());
		if (apiKeyError != null) {
			errors.add(apiKeyError);
		}
		if (config.getClusters().isEmpty()) {
			errors.add("at least one cluster must be defined (clusters)");
		}
		Set<String> names = new HashSet<>();
		for (ClusterConfig c : config.getClusters()) {
			if (c.getName() == null || !CLUSTER_NAME.matcher(c.getName()).matches()) {
				errors.add("invalid cluster name: " + c.getName() + " (letters, digits, _ . - ; at most 63 characters)");
				continue;
			}
			if (!names.add(c.getName())) {
				errors.add("duplicate cluster name: " + c.getName());
			}
			if (c.getBootstrapServers() == null || c.getBootstrapServers().isBlank()) {
				errors.add("cluster " + c.getName() + ": bootstrapServers is required");
			}
			checkUrl(errors, c.getName(), "connectUrl", c.getConnectUrl());
			checkUrl(errors, c.getName(), "schemaRegistryUrl", c.getSchemaRegistryUrl());
		}
		KerberosConfig k = config.getKerberos();
		if (k != null) {
			if (k.getPrincipal() == null || k.getPrincipal().isBlank()) {
				errors.add("kerberos.principal is required");
			}
			if (k.getKeytab() == null || k.getKeytab().isBlank()) {
				errors.add("kerberos.keytab is required");
			}
			if (k.getServiceName() == null || k.getServiceName().isBlank()) {
				k.setServiceName("kafka");
			}
		}
		Set<String> toolsets = new HashSet<>();
		for (String t : config.getToolsets()) {
			if (t == null || t.isBlank()) {
				errors.add("the toolsets list contains an empty value");
			}
			else if (!toolsets.add(t)) {
				errors.add("duplicate toolset: " + t);
			}
		}
		if (!errors.isEmpty()) {
			throw new ConfigException("Invalid config:\n - " + String.join("\n - ", errors));
		}
	}

	/**
	 * Null if the apiKey is acceptable: not set or empty (no authentication), or a value without surrounding
	 * whitespace that is not a template placeholder. Keys shorter than {@value #RECOMMENDED_API_KEY_LENGTH}
	 * characters are accepted (a warning is logged at startup).
	 */
	static String apiKeyError(String apiKey) {
		if (apiKey == null || apiKey.isEmpty()) {
			return null;
		}
		if (apiKey.isBlank()) {
			return "apiKey is blank; set a key, or leave it empty to turn authentication off";
		}
		if (!apiKey.strip().equals(apiKey)) {
			return "apiKey has leading or trailing whitespace (HTTP header values are trimmed, so it could never match)";
		}
		if (apiKey.contains("<") || apiKey.contains(">") || apiKey.contains("${")) {
			return "apiKey still holds a placeholder (<...> or ${...}); set a real key, e.g. from `openssl rand -hex 32`";
		}
		return null;
	}

	private static void checkUrl(List<String> errors, String cluster, String field, String url) {
		if (url == null || url.isBlank()) {
			return;
		}
		if (!url.startsWith("http://") && !url.startsWith("https://")) {
			errors.add("cluster " + cluster + ": " + field + " must start with http:// or https://");
			return;
		}
		String authority;
		try {
			authority = new URI(url).getRawAuthority();
		}
		catch (URISyntaxException e) {
			errors.add("cluster " + cluster + ": " + field + " is not a valid URL (" + e.getReason() + ")");
			return;
		}
		if (authority != null && authority.contains("@")) {
			// the REST clients do not send credentials, and the URL is shown in tool output and logs
			errors.add("cluster " + cluster + ": " + field
					+ " must not contain credentials (user:password@); they are not used and would be exposed in tool output");
		}
	}

}
