package io.github.srdrcn.kafkamcp.mcp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bir aracın tanımı. {@code cluster} parametresi burada tanımlanmaz; {@link ToolRegistry} onu config'teki
 * uygun cluster adlarından enum olarak ekler.
 *
 * <pre>
 * ToolSpec.read("topics_get", "Topic detayı ...")
 *     .required("topic", P.string("Topic adı"))
 *     .param("includeConfigs", P.bool("Config'leri de getir"))
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

	/** Yazma aracı; açıklama "YAZMA:" ile başlamıyorsa otomatik eklenir. */
	public static Builder write(String name, String description) {
		return new Builder(name, description, ToolKind.WRITE);
	}

	/** Geri alınamaz yazma aracı (*_delete, *_reset); açıklama "YAZMA:" ile başlar. */
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
			if (kind.writes() && !d.startsWith("YAZMA:")) {
				d = "YAZMA: " + d;
			}
			this.description = d;
			this.kind = kind;
		}

		/** Sadece connectUrl tanımlı cluster'larda listelenir. */
		public Builder connect() {
			this.requirement = ClusterRequirement.CONNECT;
			return this;
		}

		/** Sadece schemaRegistryUrl tanımlı cluster'larda listelenir. */
		public Builder schemaRegistry() {
			this.requirement = ClusterRequirement.SCHEMA_REGISTRY;
			return this;
		}

		/** cluster parametresi zorunlu olmasın (ör. clusters_list). */
		public Builder clusterOptional() {
			this.clusterOptional = true;
			return this;
		}

		/** Opsiyonel parametre. */
		public Builder param(String name, Map<String, Object> schema) {
			if ("cluster".equals(name)) {
				throw new IllegalArgumentException("cluster parametresi otomatik eklenir");
			}
			properties.put(name, schema);
			return this;
		}

		/** Zorunlu parametre. */
		public Builder required(String name, Map<String, Object> schema) {
			param(name, schema);
			required.add(name);
			return this;
		}

		/** Standart liste parametreleri: filter (regex) ve limit. */
		public Builder listParams() {
			return listParams(DEFAULT_LIST_LIMIT);
		}

		public Builder listParams(int defaultLimit) {
			param("filter", P.string("Ad filtresi (regex, kısmi eşleşme; büyük/küçük harf duyarsız için (?i))"));
			Map<String, Object> limit = P.integer("En fazla kaç kayıt dönsün (varsayılan " + defaultLimit + ")", 1,
					MAX_LIST_LIMIT);
			limit.put("default", defaultLimit);
			param("limit", limit);
			return this;
		}

		/** Mesaj okuyan araçlar için maxRecords (varsayılan 50, üst sınır 500). */
		public Builder maxRecordsParam() {
			Map<String, Object> m = P.integer("Okunacak en fazla kayıt (varsayılan " + DEFAULT_MAX_RECORDS
					+ ", en fazla " + MAX_RECORDS_LIMIT + ")", 1, MAX_RECORDS_LIMIT);
			m.put("default", DEFAULT_MAX_RECORDS);
			param("maxRecords", m);
			return this;
		}

		public ToolSpec handler(ToolHandler handler) {
			return new ToolSpec(this, Objects.requireNonNull(handler));
		}

	}

}
