package io.github.srdrcn.kafkamcp.config;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ClusterConfig {

	private String name;

	private String bootstrapServers;

	private String connectUrl;

	private String schemaRegistryUrl;

	/** null ise global readOnly geçerlidir. */
	private Boolean readOnly;

	/** Kafka client property override'ları (AdminClient ve KafkaConsumer'a uygulanır). */
	private Map<String, String> properties = new LinkedHashMap<>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getConnectUrl() {
		return connectUrl;
	}

	public void setConnectUrl(String connectUrl) {
		this.connectUrl = connectUrl;
	}

	public String getSchemaRegistryUrl() {
		return schemaRegistryUrl;
	}

	public void setSchemaRegistryUrl(String schemaRegistryUrl) {
		this.schemaRegistryUrl = schemaRegistryUrl;
	}

	public Boolean getReadOnly() {
		return readOnly;
	}

	public void setReadOnly(Boolean readOnly) {
		this.readOnly = readOnly;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties == null ? new LinkedHashMap<>() : new LinkedHashMap<>(properties);
	}

	@JsonIgnore
	public boolean hasConnect() {
		return connectUrl != null && !connectUrl.isBlank();
	}

	@JsonIgnore
	public boolean hasSchemaRegistry() {
		return schemaRegistryUrl != null && !schemaRegistryUrl.isBlank();
	}

}
