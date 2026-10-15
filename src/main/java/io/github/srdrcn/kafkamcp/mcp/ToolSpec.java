package io.github.srdrcn.kafkamcp.mcp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A tool definition. The {@code cluster} parameter is not declared here; {@link ToolRegistry} adds it as an
 * enum of the eligible cluster names from the config.
 *
 * <pre>
 * ToolSpec.read("topics_get", "Topic details ...")
 *     .required("topic", P.string("Topic name"))
 *     .param("includeConfigs", P.bool("Also return configs"))
 *     .handler(call -&gt; ...);
 * </pre>
 */
public final class ToolSpec {

	public static final int DEFAULT_LIST_LIMIT = 100;

	public static final int MAX_LIST_LIMIT = 5000;

	public static final int DEFAULT_MAX_RECORDS = 50;

	public static final int MAX_RECORDS_LIMIT = 500;

	private final String name;

	private final String description;

	private final ToolKind kind;

	private final ClusterRequirement requirement;

	private final boolean clusterOptional;

	private final Map<String, Map<String, Object>> properties;

	private final List<String> required;

	private final ToolHandler handler;

	private ToolSpec(Builder b, ToolHandler handler) {
		this.name = b.name;
		this.description = b.description;
		this.kind = b.kind;
		this.requirement = b.requirement;
		this.clusterOptional = b.clusterOptional;
		this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(b.properties));
		this.required = List.copyOf(b.required);
		this.handler = handler;
	}

	public static Builder read(String name, String description) {
		return new Builder(name, description, ToolKind.READ);
	}

	/** Write tool; the description gets a "WRITE:" prefix unless it already starts with one. */
	public static Builder write(String name, String description) {
		return new Builder(name, description, ToolKind.WRITE);
	}

	/** Irreversible write tool (*_delete, *_reset); the description starts with "WRITE:". */
	public static Builder destructive(String name, String description) {
		return new Builder(name, description, ToolKind.DESTRUCTIVE);
	}

	public String name() {
		return name;
	}

	public String description() {
		return description;
	}

	public ToolKind kind() {
		return kind;
	}

	public ClusterRequirement requirement() {
		return requirement;
	}

	public boolean clusterOptional() {
		return clusterOptional;
	}

	public Map<String, Map<String, Object>> properties() {
		return properties;
	}

	public List<String> required() {
		return required;
	}

	public ToolHandler handler() {
		return handler;
	}

	public static final class Builder {

		private final String name;

		private final String description;

		private final ToolKind kind;

		private ClusterRequirement requirement = ClusterRequirement.ANY;

		private boolean clusterOptional;

		private final Map<String, Map<String, Object>> properties = new LinkedHashMap<>();

		private final List<String> required = new ArrayList<>();

		private Builder(String name, String description, ToolKind kind) {
			this.name = Objects.requireNonNull(name);
			String d = Objects.requireNonNull(description).trim();
			if (kind.writes() && !d.startsWith("WRITE:")) {
				d = "WRITE: " + d;
			}
			this.description = d;
			this.kind = kind;
		}

		/** Listed only on clusters that have a connectUrl. */
		public Builder connect() {
			this.requirement = ClusterRequirement.CONNECT;
			return this;
		}

		/** Listed only on clusters that have a schemaRegistryUrl. */
		public Builder schemaRegistry() {
			this.requirement = ClusterRequirement.SCHEMA_REGISTRY;
			return this;
		}

		/** Makes the cluster parameter optional (e.g. clusters_list). */
		public Builder clusterOptional() {
			this.clusterOptional = true;
			return this;
		}

		/** Optional parameter. */
		public Builder param(String name, Map<String, Object> schema) {
			if ("cluster".equals(name)) {
				throw new IllegalArgumentException("The cluster parameter is added automatically");
			}
			properties.put(name, schema);
			return this;
		}

		/** Required parameter. */
		public Builder required(String name, Map<String, Object> schema) {
			param(name, schema);
			required.add(name);
			return this;
		}

		/** Standard list parameters: filter (regex) and limit. */
		public Builder listParams() {
			return listParams(DEFAULT_LIST_LIMIT);
		}

		public Builder listParams(int defaultLimit) {
			param("filter", P.string("Name filter (regex, partial match; use (?i) for case-insensitive)"));
			Map<String, Object> limit = P.integer("Maximum number of items to return (default " + defaultLimit + ")", 1,
					MAX_LIST_LIMIT);
			limit.put("default", defaultLimit);
			param("limit", limit);
			return this;
		}

		/** maxRecords for tools that read messages (default 50, max 500). */
		public Builder maxRecordsParam() {
			Map<String, Object> m = P.integer("Maximum number of records to read (default " + DEFAULT_MAX_RECORDS
					+ ", max " + MAX_RECORDS_LIMIT + ")", 1, MAX_RECORDS_LIMIT);
			m.put("default", DEFAULT_MAX_RECORDS);
			param("maxRecords", m);
			return this;
		}

		public ToolSpec handler(ToolHandler handler) {
			return new ToolSpec(this, Objects.requireNonNull(handler));
		}

	}

}
