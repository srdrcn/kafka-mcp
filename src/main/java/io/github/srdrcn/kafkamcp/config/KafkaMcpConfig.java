package io.github.srdrcn.kafkamcp.config;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Root structure of kafka-mcp.yaml. Field names match the YAML keys exactly.
 */
public class KafkaMcpConfig {

	/** HTTP port. If set (and --transport is not given) Streamable HTTP runs, otherwise stdio. */
	private Integer port;

	/** If not empty, the request's X-Authorization header must match this value exactly. */
	private String apiKey;

	private boolean readOnly = false;

	private boolean disableDestructive = false;

	private List<String> toolsets = new ArrayList<>(List.of("core"));

	private KerberosConfig kerberos;

	private List<ClusterConfig> clusters = new ArrayList<>();

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getApiKey() {
		return apiKey;
	}

	public void setApiKey(String apiKey) {
		this.apiKey = apiKey;
	}

	public boolean isReadOnly() {
		return readOnly;
	}

	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}

	public boolean isDisableDestructive() {
		return disableDestructive;
	}

	public void setDisableDestructive(boolean disableDestructive) {
		this.disableDestructive = disableDestructive;
	}

	public List<String> getToolsets() {
		return toolsets;
	}

	public void setToolsets(List<String> toolsets) {
		this.toolsets = toolsets == null ? new ArrayList<>() : new ArrayList<>(toolsets);
	}

	public KerberosConfig getKerberos() {
		return kerberos;
	}

	public void setKerberos(KerberosConfig kerberos) {
		this.kerberos = kerberos;
	}

	public List<ClusterConfig> getClusters() {
		return clusters;
	}

	public void setClusters(List<ClusterConfig> clusters) {
		this.clusters = clusters == null ? new ArrayList<>() : new ArrayList<>(clusters);
	}

	@JsonIgnore
	public boolean isApiKeyEnabled() {
		return apiKey != null && !apiKey.isBlank();
	}

	/** Effective readOnly value of the cluster: the cluster override if set, otherwise the global value. */
	public boolean isReadOnly(ClusterConfig cluster) {
		return cluster.getReadOnly() != null ? cluster.getReadOnly() : readOnly;
	}

	public ClusterConfig cluster(String name) {
		for (ClusterConfig c : clusters) {
			if (c.getName().equals(name)) {
				return c;
			}
		}
		return null;
	}

}
