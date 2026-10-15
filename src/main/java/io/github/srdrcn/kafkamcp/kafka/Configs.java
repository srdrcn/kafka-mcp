package io.github.srdrcn.kafkamcp.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Config helpers: a single describeConfigs, masking of sensitive values, updates with before/after values. */
public final class Configs {

	/** Mask written in place of sensitive config values. */
	public static final String MASK = "******";

	private Configs() {
	}

	/** TOPIC resource. */
	public static ConfigResource topic(String name) {
		return new ConfigResource(ConfigResource.Type.TOPIC, name);
	}

	/** BROKER resource (single broker). */
	public static ConfigResource broker(int id) {
		return new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(id));
	}

	/** Cluster-wide dynamic default broker config (BROKER, name ""). */
	public static ConfigResource brokerDefault() {
		return new ConfigResource(ConfigResource.Type.BROKER, "");
	}

	/** A single describeConfigs call; per-resource errors are returned separately. */
	public static Futures.Each<ConfigResource, Config> describe(ToolCall call, Collection<ConfigResource> resources) {
		if (resources.isEmpty()) {
			return new Futures.Each<>(new LinkedHashMap<>(), new LinkedHashMap<>());
		}
		return Futures.awaitEach(call, call.admin().describeConfigs(resources).values());
	}

	/** Single resource; "Topic not found" if the topic does not exist. */
	public static Config describeOne(ToolCall call, ConfigResource r) {
		return awaitOne(call, r, submitOne(call, r));
	}

	/** Sends describeConfigs for a single resource (does not wait); get the result with {@link #awaitOne}. */
	public static KafkaFuture<Config> submitOne(ToolCall call, ConfigResource r) {
		return call.admin().describeConfigs(List.of(r)).values().get(r);
	}

	/** Waits for the {@link #submitOne} result; "Topic not found" if the topic does not exist. */
	public static Config awaitOne(ToolCall call, ConfigResource r, KafkaFuture<Config> pending) {
		try {
			return call.await(pending);
		}
		catch (ToolException e) {
			throw r.type() == ConfigResource.Type.TOPIC ? Topics.notFound(e, r.name()) : e;
		}
	}

	/** Value; the mask for sensitive configs. */
	public static String value(ConfigEntry e) {
		if (e == null) {
			return null;
		}
		return e.isSensitive() ? MASK : e.value();
	}

	/** Source name (e.g. DYNAMIC_TOPIC_CONFIG) or null. */
	public static String source(ConfigEntry e) {
		return e == null || e.source() == null ? null : e.source().name();
	}

	/** Is it the default value (source DEFAULT_CONFIG)? */
	public static boolean isDefault(ConfigEntry e) {
		return e.source() == ConfigSource.DEFAULT_CONFIG || e.source() == ConfigSource.UNKNOWN && e.isDefault();
	}

	/** Config entries sorted by name. */
	public static List<ConfigEntry> sorted(Config c) {
		List<ConfigEntry> out = new ArrayList<>(c.entries());
		out.sort(Comparator.comparing(ConfigEntry::name));
		return out;
	}

	/** Compact row: name, value, source, readOnly (+ isDefault). */
	public static Map<String, Object> row(ConfigEntry e, boolean withIsDefault) {
		Map<String, Object> m = Results.map("name", e.name(), "value", value(e), "source", source(e));
		if (withIsDefault) {
			m.put("isDefault", isDefault(e));
		}
		m.put("readOnly", e.isReadOnly());
		return m;
	}

	/**
	 * Applies SET/DELETE with incrementalAlterConfigs and returns the before/after value of every requested key.
	 * The new value is re-read for a short while (at most ~2 s) until the change shows up.
	 */
	public static List<Map<String, Object>> alter(ToolCall call, ConfigResource r, Map<String, String> set,
			Collection<String> delete) {
		if (set.isEmpty() && delete.isEmpty()) {
			throw new ToolException("At least one of the set or delete parameters is required");
		}
		for (Map.Entry<String, String> e : set.entrySet()) {
			if (e.getValue() == null) {
				throw new ToolException("set value cannot be empty: " + e.getKey() + " (use delete to revert to the default)");
			}
		}
		for (String k : delete) {
			if (set.containsKey(k)) {
				throw new ToolException("Same key in both set and delete: " + k);
			}
		}
		Config before = describeOne(call, r);
		List<AlterConfigOp> ops = new ArrayList<>();
		set.forEach((k, v) -> ops.add(new AlterConfigOp(new ConfigEntry(k, v), AlterConfigOp.OpType.SET)));
		delete.forEach(k -> ops.add(new AlterConfigOp(new ConfigEntry(k, null), AlterConfigOp.OpType.DELETE)));
		call.await(call.admin().incrementalAlterConfigs(Map.of(r, ops)).all());
		Config after = describeOne(call, r);
		for (int i = 0; i < 10 && !applied(before, after, r, set, delete) && !call.nearDeadline(1_500); i++) {
			call.sleep(200);
			after = describeOne(call, r);
		}
		Set<String> keys = new LinkedHashSet<>(set.keySet());
		keys.addAll(delete);
		List<Map<String, Object>> changes = new ArrayList<>();
		for (String k : keys) {
			ConfigEntry b = before.get(k);
			ConfigEntry a = after.get(k);
			Map<String, Object> row = Results.map("name", k, "op", set.containsKey(k) ? "set" : "delete", "before",
					value(b), "beforeSource", source(b), "after", value(a), "afterSource", source(a));
			if (Objects.equals(value(b), value(a)) && Objects.equals(source(b), source(a))) {
				row.put("unchanged", true);
			}
			changes.add(row);
		}
		return changes;
	}

	/**
	 * Is the change visible in the config that was read? The broker updates the source before the value, so for
	 * a delete the value is also expected to differ from the old dynamic value (if the default is the same, this
	 * waits at most ~2 s).
	 */
	private static boolean applied(Config before, Config after, ConfigResource r, Map<String, String> set,
			Collection<String> delete) {
		for (Map.Entry<String, String> s : set.entrySet()) {
			ConfigEntry e = after.get(s.getKey());
			if (e == null || !e.isSensitive() && !Objects.equals(e.value(), s.getValue())) {
				return false;
			}
		}
		ConfigSource own = ownSource(r);
		for (String k : delete) {
			ConfigEntry e = after.get(k);
			ConfigEntry b = before.get(k);
			if (e == null || own == null) {
				continue;
			}
			if (e.source() == own || b != null && b.source() == own && Objects.equals(e.value(), b.value())) {
				return false;
			}
		}
		return true;
	}

	private static ConfigSource ownSource(ConfigResource r) {
		return switch (r.type()) {
			case TOPIC -> ConfigSource.DYNAMIC_TOPIC_CONFIG;
			case BROKER -> r.isDefault() ? ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG : ConfigSource.DYNAMIC_BROKER_CONFIG;
			default -> null;
		};
	}

}
