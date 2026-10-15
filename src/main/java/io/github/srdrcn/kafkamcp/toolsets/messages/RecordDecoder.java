package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient;

/**
 * Converts key/value bytes into JSON-friendly values according to {@link MessageFormat} (string, json, hex, long,
 * avro). Avro schemas are fetched from Schema Registry and kept only for the lifetime of this object (a single tool
 * call). Schema lookups are limited per call by {@link #MAX_SCHEMA_LOOKUPS} and (when a ToolCall is given) by the
 * deadline; skipped schemas come back as hex + error, and the caller marks the result "partial" if
 * {@link #timeLimited()}. Structured values are returned as {@link JsonNode} (nested null fields are kept).
 */
public final class RecordDecoder {

	public static final int DEFAULT_MAX_CHARS = 4096;

	/**
	 * Maximum Schema Registry requests per call (prevents a flood of requests for bogus schema ids in data that looks
	 * like wire format).
	 */
	public static final int MAX_SCHEMA_LOOKUPS = 50;

	/** A schema lookup must finish at least this long before the deadline (margin to return the records read as partial). */
	static final long LOOKUP_RESERVE_MS = RecordScanner.DEFAULT_RESERVE_MS;

	/** No schema lookup is started if less time than this is left. */
	private static final long MIN_LOOKUP_MS = 200;

	private static final ObjectMapper MAPPER = JsonMapper.builder()
		.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
		.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
		.build();

	private static final HexFormat HEX = HexFormat.of();

	private static final int MAX_REFERENCE_DEPTH = 5;

	private final SchemaRegistryClient registry;

	private final String cluster;

	private final ToolCall call;

	private final Map<Integer, Schema> schemas = new HashMap<>();

	private final Map<Integer, String> schemaErrors = new HashMap<>();

	private final Set<Integer> skippedIds = new HashSet<>();

	private String registryDown;

	/** Why schema lookups were stopped, if they were (request limit or time limit). */
	private String stopReason;

	private boolean timeLimited;

	private int lookups;

	/**
	 * Decoder without a deadline (tests and use outside tools); schema lookups are still limited by
	 * {@link #MAX_SCHEMA_LOOKUPS}.
	 * @param registry Schema Registry client (may be null if avro is not needed)
	 * @param cluster cluster to fetch the schemas from
	 */
	public RecordDecoder(SchemaRegistryClient registry, String cluster) {
		this(registry, cluster, null);
	}

	/** Decoder bound to the tool call's cluster and deadline: schema lookups are cut off with a margin before the deadline. */
	public RecordDecoder(SchemaRegistryClient registry, ToolCall call) {
		this(registry, call.cluster(), call);
	}

	private RecordDecoder(SchemaRegistryClient registry, String cluster, ToolCall call) {
		this.registry = registry;
		this.cluster = cluster;
		this.call = call;
	}

	/** Number of distinct schema ids whose schema was not fetched because of the time limit or the request limit. */
	public int skippedSchemaLookups() {
		return skippedIds.size();
	}

	/** Did a schema lookup hit the time limit (the caller marks the output "partial")? */
	public boolean timeLimited() {
		return timeLimited;
	}

	/** Output note for skipped schema lookups; null if none were skipped. */
	public String skipNote() {
		if (skippedIds.isEmpty()) {
			return null;
		}
		return skippedIds.size() + " schema id(s) not fetched (" + stopReason
				+ "); these records were returned as hex + valueError/keyError";
	}

	/** If one of the selected formats is avro, the cluster must have a schemaRegistryUrl; otherwise an error. */
	public static void requireSupported(ToolCall call, MessageFormat... formats) {
		for (MessageFormat f : formats) {
			if (f != null && f.needsSchemaRegistry() && !call.clusterConfig().hasSchemaRegistry()) {
				throw new ToolException("format=avro requires a schemaRegistryUrl on cluster " + call.cluster()
						+ " (none configured). Use the string/json/hex/long formats.");
			}
		}
	}

	/**
	 * Decoded value. {@code value}: the value for the output (String, Long or {@link JsonNode}; a String if
	 * truncated). {@code text}: the untruncated text for filters. {@code error}: fallback/error description.
	 */
	public record Decoded(Object value, String text, boolean truncated, String error, Integer schemaId) {

		public static final Decoded NULL = new Decoded(null, null, false, null, null);

		public boolean isNull() {
			return value == null && text == null;
		}

	}

	/** Structured decode result (for shape inference): {@code value} or {@code error}. */
	public record Structured(JsonNode value, String error, Integer schemaId) {
	}

	/** Decodes the bytes; long values are truncated at {@code maxChars} characters. */
	public Decoded decode(byte[] bytes, MessageFormat format, int maxChars) {
		if (bytes == null) {
			return Decoded.NULL;
		}
		return switch (format) {
			case STRING -> decodeString(bytes, maxChars);
			case JSON -> decodeJson(bytes, maxChars);
			case HEX -> {
				String h = hex(bytes);
				yield new Decoded(cut(h, maxChars), h, h.length() > maxChars, null, null);
			}
			case LONG -> {
				if (bytes.length != Long.BYTES) {
					yield hexFallback(bytes, maxChars, "long expects 8 bytes, got " + bytes.length + "; shown as hex", null);
				}
				long v = ByteBuffer.wrap(bytes).getLong();
				yield new Decoded(v, Long.toString(v), false, null, null);
			}
			case AVRO -> decodeAvro(bytes, maxChars);
		};
	}

	/** Decodes a json/avro value structurally without truncation; {@code error} is set on failure. */
	public Structured structured(byte[] bytes, MessageFormat format) {
		if (bytes == null) {
			return new Structured(null, "null value (tombstone)", null);
		}
		if (format == MessageFormat.AVRO) {
			try {
				AvroValue a = avro(bytes);
				return new Structured(a.node(), null, a.schemaId());
			}
			catch (DecodeException e) {
				return new Structured(null, e.getMessage(), e.schemaId);
			}
		}
		String s = utf8(bytes);
		if (s == null) {
			return new Structured(null, "Not valid UTF-8", null);
		}
		if (s.isBlank()) {
			return new Structured(null, "Empty value; not JSON", null);
		}
		try {
			return new Structured(MAPPER.readTree(s), null, null);
		}
		catch (JsonProcessingException e) {
			return new Structured(null, "Not JSON: " + shortMessage(e), null);
		}
	}

	private Decoded decodeString(byte[] bytes, int maxChars) {
		String s = utf8(bytes);
		if (s == null) {
			return hexFallback(bytes, maxChars, "Not valid UTF-8; shown as hex", null);
		}
		return new Decoded(cut(s, maxChars), s, s.length() > maxChars, null, null);
	}

	private Decoded decodeJson(byte[] bytes, int maxChars) {
		String s = utf8(bytes);
		if (s == null) {
			return hexFallback(bytes, maxChars, "Not valid UTF-8; shown as hex", null);
		}
		if (s.isBlank()) {
			return new Decoded(s, s, false, "Empty value; not JSON", null);
		}
		if (s.length() > maxChars) {
			return new Decoded(cut(s, maxChars), s, true, null, null);
		}
		try {
			return new Decoded(MAPPER.readTree(s), s, false, null, null);
		}
		catch (JsonProcessingException e) {
			return new Decoded(s, s, false, "Not JSON: " + shortMessage(e) + "; shown as raw text", null);
		}
	}

	private Decoded decodeAvro(byte[] bytes, int maxChars) {
		try {
			AvroValue a = avro(bytes);
			String text = MAPPER.writeValueAsString(a.node());
			if (text.length() > maxChars) {
				return new Decoded(cut(text, maxChars), text, true, null, a.schemaId());
			}
			return new Decoded(a.node(), text, false, null, a.schemaId());
		}
		catch (DecodeException e) {
			return hexFallback(bytes, maxChars, e.getMessage() + "; shown as hex", e.schemaId);
		}
		catch (JsonProcessingException e) {
			return hexFallback(bytes, maxChars, "Could not convert the Avro value to JSON; shown as hex", null);
		}
	}

	private record AvroValue(JsonNode node, int schemaId) {
	}

	private AvroValue avro(byte[] bytes) throws DecodeException {
		if (bytes.length < 5 || bytes[0] != 0) {
			throw new DecodeException("Not Confluent wire format (no 0x0 magic byte + 4-byte schema id)", null);
		}
		int id = ByteBuffer.wrap(bytes, 1, 4).getInt();
		Schema schema = schema(id);
		try {
			GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
			BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, 5, bytes.length - 5, null);
			Object datum = reader.read(null, decoder);
			return new AvroValue(MAPPER.valueToTree(toJava(datum, schema)), id);
		}
		catch (IOException | RuntimeException e) {
			throw new DecodeException(cut("Could not decode Avro (schema " + id + "): " + e.getClass().getSimpleName()
					+ (e.getMessage() == null ? "" : ": " + e.getMessage()), 200), id);
		}
		catch (OutOfMemoryError e) {
			// with corrupt data or the wrong schema a length field tries to allocate one huge array; this single
			// failed allocation leaves nothing behind on the heap
			throw new DecodeException("Could not decode Avro (schema " + id
					+ "): corrupt length field (the data may not have been written with this schema)", id);
		}
	}

	private Schema schema(int id) throws DecodeException {
		Schema cached = schemas.get(id);
		if (cached != null) {
			return cached;
		}
		String known = schemaErrors.get(id);
		if (known != null) {
			throw new DecodeException(known, id);
		}
		if (registryDown != null) {
			throw new DecodeException(registryDown, id);
		}
		if (registry == null) {
			throw new DecodeException("No Schema Registry configured", id);
		}
		try {
			JsonNode node = fetch("/schemas/ids/" + id);
			String type = node.path("schemaType").asText("AVRO");
			if (!"AVRO".equalsIgnoreCase(type)) {
				throw new DecodeException("Schema " + id + " has type " + type + "; the avro format only decodes AVRO schemas", id);
			}
			Schema.Parser parser = new Schema.Parser();
			parseReferences(parser, node.path("references"), 0, new HashSet<>());
			Schema schema = parser.parse(node.path("schema").asText());
			schemas.put(id, schema);
			return schema;
		}
		catch (DecodeException e) {
			schemaErrors.put(id, e.getMessage());
			throw e;
		}
		catch (LookupSkipped e) {
			skippedIds.add(id);
			String msg = "Could not fetch schema " + id + ": " + e.getMessage();
			schemaErrors.put(id, msg);
			throw new DecodeException(msg, id);
		}
		catch (ToolException e) {
			String msg = "Could not fetch schema " + id + ": " + e.getMessage();
			if (e instanceof SchemaRegistryClient.UnreachableException) {
				// registry down: fail the remaining schema ids fast instead of trying each one
				registryDown = msg;
			}
			schemaErrors.put(id, msg);
			throw new DecodeException(msg, id);
		}
		catch (RuntimeException e) {
			String msg = "Could not parse schema " + id + ": " + e.getMessage();
			schemaErrors.put(id, msg);
			throw new DecodeException(msg, id);
		}
	}

	/**
	 * Schema Registry GET, limited by the per-call request limit and the deadline. With a ToolCall the request runs
	 * on a separate (virtual) thread and is awaited at most until {@link #LOOKUP_RESERVE_MS} before the deadline, so a
	 * slow registry does not push the records read into a timeout.
	 */
	private JsonNode fetch(String path) {
		if (stopReason == null && lookups >= MAX_SCHEMA_LOOKUPS) {
			stopReason = "at most " + MAX_SCHEMA_LOOKUPS + " schema lookups per call";
		}
		long budget = call == null ? Long.MAX_VALUE : call.remainingMillis() - LOOKUP_RESERVE_MS;
		if (stopReason == null && budget < MIN_LOOKUP_MS) {
			stopReason = "approaching the tool time limit";
			timeLimited = true;
		}
		if (stopReason != null) {
			throw new LookupSkipped("lookup skipped (" + stopReason + ")");
		}
		lookups++;
		if (call == null) {
			return registry.get(cluster, path);
		}
		FutureTask<JsonNode> task = new FutureTask<>(() -> registry.get(cluster, path));
		Thread.ofVirtual().name("kafka-mcp-schema-fetch").start(task);
		try {
			return task.get(budget, TimeUnit.MILLISECONDS);
		}
		catch (TimeoutException e) {
			task.cancel(true);
			stopReason = "Schema Registry did not respond within the tool time limit";
			timeLimited = true;
			throw new LookupSkipped(stopReason);
		}
		catch (ExecutionException e) {
			if (e.getCause() instanceof RuntimeException re) {
				throw re;
			}
			throw new ToolException("Schema Registry request failed: " + e.getCause());
		}
		catch (InterruptedException e) {
			task.cancel(true);
			Thread.currentThread().interrupt();
			throw new ToolException("Tool was cancelled");
		}
	}

	/** Registers the schema references (types in other subjects) with the parser first. */
	private void parseReferences(Schema.Parser parser, JsonNode refs, int depth, Set<String> seen) {
		if (!refs.isArray() || refs.isEmpty()) {
			return;
		}
		if (depth >= MAX_REFERENCE_DEPTH) {
			throw new ToolException("Schema reference depth exceeds " + MAX_REFERENCE_DEPTH);
		}
		for (JsonNode ref : refs) {
			String subject = ref.path("subject").asText();
			String version = ref.path("version").asText("latest");
			if (!seen.add(subject + "/" + version)) {
				continue;
			}
			JsonNode node = fetch("/subjects/" + SchemaRegistryClient.enc(subject) + "/versions/"
					+ SchemaRegistryClient.enc(version));
			parseReferences(parser, node.path("references"), depth + 1, seen);
			parser.parse(node.path("schema").asText());
		}
	}

	/** Converts an Avro datum into a JSON-friendly Java value (logical types in readable form). */
	static Object toJava(Object v, Schema s) {
		if (v == null) {
			return null;
		}
		LogicalType lt = s.getLogicalType();
		switch (s.getType()) {
			case UNION:
				return toJava(v, s.getTypes().get(GenericData.get().resolveUnion(s, v)));
			case RECORD: {
				IndexedRecord r = (IndexedRecord) v;
				Map<String, Object> m = new LinkedHashMap<>();
				for (Schema.Field f : s.getFields()) {
					m.put(f.name(), toJava(r.get(f.pos()), f.schema()));
				}
				return m;
			}
			case ARRAY: {
				List<Object> out = new ArrayList<>();
				for (Object o : (Collection<?>) v) {
					out.add(toJava(o, s.getElementType()));
				}
				return out;
			}
			case MAP: {
				Map<String, Object> m = new LinkedHashMap<>();
				((Map<?, ?>) v).forEach((k, val) -> m.put(String.valueOf(k), toJava(val, s.getValueType())));
				return m;
			}
			case ENUM:
			case STRING:
				return v.toString();
			case BYTES: {
				ByteBuffer b = ((ByteBuffer) v).duplicate();
				byte[] bytes = new byte[b.remaining()];
				b.get(bytes);
				return lt instanceof LogicalTypes.Decimal d ? new BigDecimal(new BigInteger(bytes), d.getScale())
						: hex(bytes);
			}
			case FIXED: {
				byte[] bytes = ((GenericFixed) v).bytes();
				return lt instanceof LogicalTypes.Decimal d ? new BigDecimal(new BigInteger(bytes), d.getScale())
						: hex(bytes);
			}
			case INT:
				return lt instanceof LogicalTypes.Date ? LocalDate.ofEpochDay(((Number) v).intValue()).toString() : v;
			case LONG: {
				long l = ((Number) v).longValue();
				if (lt instanceof LogicalTypes.TimestampMillis) {
					return Instant.ofEpochMilli(l).toString();
				}
				if (lt instanceof LogicalTypes.TimestampMicros) {
					return Instant.ofEpochSecond(Math.floorDiv(l, 1_000_000L), Math.floorMod(l, 1_000_000L) * 1_000L)
						.toString();
				}
				return l;
			}
			case FLOAT:
			case DOUBLE:
			case BOOLEAN:
				return v;
			default:
				return v.toString();
		}
	}

	/** Strict UTF-8 decoding; null if there is an invalid byte. */
	public static String utf8(byte[] bytes) {
		try {
			CharBuffer cb = StandardCharsets.UTF_8.newDecoder()
				.onMalformedInput(CodingErrorAction.REPORT)
				.onUnmappableCharacter(CodingErrorAction.REPORT)
				.decode(ByteBuffer.wrap(bytes));
			return cb.toString();
		}
		catch (CharacterCodingException e) {
			return null;
		}
	}

	public static String hex(byte[] bytes) {
		return HEX.formatHex(bytes);
	}

	/** At most {@code max} characters (a surrogate pair is never split). */
	public static String cut(String s, int max) {
		if (s == null || s.length() <= max) {
			return s;
		}
		int end = max;
		if (end > 0 && Character.isHighSurrogate(s.charAt(end - 1))) {
			end--;
		}
		return s.substring(0, end);
	}

	private static Decoded hexFallback(byte[] bytes, int maxChars, String error, Integer schemaId) {
		String h = hex(bytes);
		return new Decoded(cut(h, maxChars), h, h.length() > maxChars, error, schemaId);
	}

	private static String shortMessage(JsonProcessingException e) {
		String m = e.getOriginalMessage();
		if (m == null) {
			return e.getClass().getSimpleName();
		}
		return cut(m.replaceAll("\\s+", " "), 120);
	}

	/** The schema lookup was not made or not awaited because of the request limit or the time limit. */
	private static final class LookupSkipped extends RuntimeException {

		LookupSkipped(String message) {
			super(message, null, false, false);
		}

	}

	private static final class DecodeException extends Exception {

		private final Integer schemaId;

		DecodeException(String message, Integer schemaId) {
			super(message, null, false, false);
			this.schemaId = schemaId;
		}

	}

}
