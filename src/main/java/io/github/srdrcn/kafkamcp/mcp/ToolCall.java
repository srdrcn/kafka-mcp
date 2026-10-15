package io.github.srdrcn.kafkamcp.mcp;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;

/**
 * Context of a single tool call: arguments, selected cluster, Kafka clients and the 15 s deadline.
 */
public final class ToolCall {

	/** Margin kept before the deadline in Kafka waits, so the error message is returned before the tool timeout. */
	private static final long MARGIN_MS = 300;

	private final String toolName;

	private final Map<String, Object> args;

	private final KafkaClients clients;

	private final Instant deadline;

	private Pattern filter;

	private boolean filterParsed;

	public ToolCall(String toolName, Map<String, Object> args, KafkaClients clients, Instant deadline) {
		this.toolName = toolName;
		this.args = args == null ? Map.of() : args;
		this.clients = clients;
		this.deadline = deadline;
	}

	public String toolName() {
		return toolName;
	}

	public Map<String, Object> args() {
		return args;
	}

	public KafkaClients clients() {
		return clients;
	}

	public Instant deadline() {
		return deadline;
	}

	public long remainingMillis() {
		return Math.max(0, Duration.between(Instant.now(), deadline).toMillis());
	}

	/** Selected cluster name (required). */
	public String cluster() {
		String c = str("cluster", null);
		if (c == null) {
			throw new ToolException("Missing parameter: cluster");
		}
		return c;
	}

	/** Selected cluster name or null (for tools with an optional cluster). */
	public String clusterOrNull() {
		return str("cluster", null);
	}

	public ClusterConfig clusterConfig() {
		return clients.cluster(cluster());
	}

	/** Shared AdminClient of the selected cluster (do not close it). */
	public Admin admin() {
		return clients.admin(cluster());
	}

	/** New assign-only consumer for the selected cluster (close it with try-with-resources). */
	public KafkaConsumer<byte[], byte[]> newConsumer() {
		return clients.newConsumer(cluster(), null);
	}

	public boolean has(String name) {
		Object v = args.get(name);
		return v != null && !(v instanceof String s && s.isBlank());
	}

	/** Required string parameter. */
	public String str(String name) {
		String v = str(name, null);
		if (v == null) {
			throw new ToolException("Missing parameter: " + name);
		}
		return v;
	}

	public String str(String name, String defaultValue) {
		Object v = args.get(name);
		if (v == null) {
			return defaultValue;
		}
		String s = String.valueOf(v).trim();
		return s.isEmpty() ? defaultValue : s;
	}

	public int integer(String name, int defaultValue) {
		Long v = optLong(name);
		return v == null ? defaultValue : Math.toIntExact(Math.max(Integer.MIN_VALUE, Math.min(Integer.MAX_VALUE, v)));
	}

	/** The value is clamped to [min, max]. */
	public int integer(String name, int defaultValue, int min, int max) {
		return Math.max(min, Math.min(max, integer(name, defaultValue)));
	}

	/** Required integer parameter. */
	public long requiredLong(String name) {
		Long v = optLong(name);
		if (v == null) {
			throw new ToolException("Missing parameter: " + name);
		}
		return v;
	}

	public long longValue(String name, long defaultValue) {
		Long v = optLong(name);
		return v == null ? defaultValue : v;
	}

	public Integer optInt(String name) {
		Long v = optLong(name);
		return v == null ? null : Math.toIntExact(v);
	}

	public Long optLong(String name) {
		Object v = args.get(name);
		if (v == null || v instanceof String s && s.isBlank()) {
			return null;
		}
		if (v instanceof Number n) {
			return n.longValue();
		}
		try {
			return Long.parseLong(String.valueOf(v).trim());
		}
		catch (NumberFormatException e) {
			throw new ToolException("Parameter must be an integer: " + name + "=" + v);
		}
	}

	public Double optDouble(String name) {
		Object v = args.get(name);
		if (v == null || v instanceof String s && s.isBlank()) {
			return null;
		}
		if (v instanceof Number n) {
			return n.doubleValue();
		}
		try {
			return Double.parseDouble(String.valueOf(v).trim());
		}
		catch (NumberFormatException e) {
			throw new ToolException("Parameter must be a number: " + name + "=" + v);
		}
	}

	public boolean bool(String name, boolean defaultValue) {
		Object v = args.get(name);
		if (v == null) {
			return defaultValue;
		}
		if (v instanceof Boolean b) {
			return b;
		}
		String s = String.valueOf(v).trim().toLowerCase();
		return switch (s) {
			case "true", "1", "yes" -> true;
			case "false", "0", "no" -> false;
			default -> defaultValue;
		};
	}

	/** List of strings; accepts an array or a comma-separated string. Empty list if absent. */
	public List<String> strings(String name) {
		Object v = args.get(name);
		List<String> out = new ArrayList<>();
		if (v instanceof Collection<?> c) {
			for (Object o : c) {
				if (o != null && !String.valueOf(o).isBlank()) {
					out.add(String.valueOf(o).trim());
				}
			}
		}
		else if (v != null) {
			for (String s : String.valueOf(v).split(",")) {
				if (!s.isBlank()) {
					out.add(s.trim());
				}
			}
		}
		return out;
	}

	/** JSON object parameter; empty map if absent. */
	@SuppressWarnings("unchecked")
	public Map<String, Object> map(String name) {
		Object v = args.get(name);
		if (v == null) {
			return new LinkedHashMap<>();
		}
		if (v instanceof Map<?, ?> m) {
			return new LinkedHashMap<>((Map<String, Object>) m);
		}
		throw new ToolException("Parameter must be a JSON object: " + name);
	}

	/** Map parameter with the values converted to strings (for configs). */
	public Map<String, String> stringMap(String name) {
		Map<String, String> out = new LinkedHashMap<>();
		map(name).forEach((k, v) -> out.put(k, v == null ? null : String.valueOf(v)));
		return out;
	}

	/** Regex compiled from the filter parameter, or null. */
	public Pattern filter() {
		if (!filterParsed) {
			filter = regex("filter");
			filterParsed = true;
		}
		return filter;
	}

	/** Compiles the given regex parameter; null if absent. */
	public Pattern regex(String name) {
		String f = str(name, null);
		if (f == null) {
			return null;
		}
		try {
			return Pattern.compile(f);
		}
		catch (PatternSyntaxException e) {
			throw new ToolException("Invalid regex (" + name + "): " + e.getDescription());
		}
	}

	/** True if there is no filter; otherwise a partial match (find). */
	public boolean matches(String value) {
		Pattern p = filter();
		return p == null || (value != null && find(p, value));
	}

	/**
	 * Partial match of a user-supplied regex that stops (with a {@link ToolException}) when the call is cancelled or
	 * its deadline passes.
	 */
	public boolean find(Pattern pattern, String value) {
		return Regexes.find(pattern, value, deadline);
	}

	/** Full match of a user-supplied regex; stops like {@link #find(Pattern, String)}. */
	public boolean fullMatch(Pattern pattern, String value) {
		return Regexes.matches(pattern, value, deadline);
	}

	public int limit() {
		return limit(ToolSpec.DEFAULT_LIST_LIMIT);
	}

	public int limit(int defaultLimit) {
		return integer("limit", defaultLimit, 1, ToolSpec.MAX_LIST_LIMIT);
	}

	/** maxRecords: default 50, max 500. */
	public int maxRecords() {
		return integer("maxRecords", ToolSpec.DEFAULT_MAX_RECORDS, 1, ToolSpec.MAX_RECORDS_LIMIT);
	}

	/** Waits on a Kafka future for the remaining time; turns a failure into a readable {@link ToolException}. */
	public <T> T await(KafkaFuture<T> future) {
		try {
			return future.get(waitMillis(), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Tool was cancelled");
		}
		catch (ExecutionException e) {
			throw new ToolException(KafkaErrors.explain(e.getCause()), e.getCause());
		}
		catch (TimeoutException e) {
			throw new ToolException("No response from Kafka within " + KafkaClients.TIMEOUT_MS / 1000 + " s (timeout)");
		}
	}

	public <T> T await(Future<T> future) {
		try {
			return future.get(waitMillis(), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Tool was cancelled");
		}
		catch (ExecutionException e) {
			throw new ToolException(KafkaErrors.explain(e.getCause()), e.getCause());
		}
		catch (TimeoutException e) {
			future.cancel(true);
			throw new ToolException("No response within " + KafkaClients.TIMEOUT_MS / 1000 + " s (timeout)");
		}
	}

	/** Remaining time for Kafka waits (minus the margin), at least 1 ms. */
	public long waitMillis() {
		return Math.max(1, remainingMillis() - MARGIN_MS);
	}

	/**
	 * The instant at which {@link #nearDeadline(long)} becomes true for the same reserve: work that is stopped then
	 * still leaves time to build a (partial) result.
	 */
	public Instant softDeadline(long reserveMillis) {
		return deadline.minusMillis(MARGIN_MS + reserveMillis);
	}

	/** Is the remaining time used up (margin included)? For leaving loops early. */
	public boolean nearDeadline(long reserveMillis) {
		return remainingMillis() <= MARGIN_MS + reserveMillis;
	}

	/** Sleeps for the given time (between two samples). Throws ToolException if that would pass the deadline. */
	public void sleep(long millis) {
		if (remainingMillis() - millis < MARGIN_MS + 1_000) {
			throw new ToolException("Wait time exceeds the tool time limit (15 s); use a shorter interval");
		}
		try {
			Thread.sleep(millis);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Tool was cancelled");
		}
	}

}
