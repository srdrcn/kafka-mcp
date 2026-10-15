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
 * Key/value baytlarını {@link MessageFormat}'a göre JSON'a uygun değere çevirir (string, json, hex, long,
 * avro). Avro şemaları Schema Registry'den alınır ve sadece bu nesne (tek araç çağrısı) boyunca tutulur.
 * Yapılandırılmış değerler {@link JsonNode} olarak döner (iç içe null alanlar korunur).
 */
public final class RecordDecoder {

	public static final int DEFAULT_MAX_CHARS = 4096;

	private static final ObjectMapper MAPPER = JsonMapper.builder()
		.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
		.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
		.build();

	private static final HexFormat HEX = HexFormat.of();

	private static final int MAX_REFERENCE_DEPTH = 5;

	private final SchemaRegistryClient registry;

	private final String cluster;

	private final Map<Integer, Schema> schemas = new HashMap<>();

	private final Map<Integer, String> schemaErrors = new HashMap<>();

	private String registryDown;

	/**
	 * @param registry Schema Registry client'ı (avro gerekmiyorsa null olabilir)
	 * @param cluster şemaların alınacağı cluster
	 */
	public RecordDecoder(SchemaRegistryClient registry, String cluster) {
		this.registry = registry;
		this.cluster = cluster;
	}

	/** Seçilen formatlardan biri avro ise cluster'da schemaRegistryUrl olmalı; değilse Türkçe hata. */
	public static void requireSupported(ToolCall call, MessageFormat... formats) {
		for (MessageFormat f : formats) {
			if (f != null && f.needsSchemaRegistry() && !call.clusterConfig().hasSchemaRegistry()) {
				throw new ToolException("format=avro için cluster " + call.cluster()
						+ " tanımında schemaRegistryUrl gerekli (tanımlı değil). string/json/hex/long formatlarını kullanın.");
			}
		}
	}

	/**
	 * Çözülmüş değer. {@code value}: çıktıya konacak değer (String, Long ya da {@link JsonNode}; kesilmişse
	 * String). {@code text}: filtreler için kesilmemiş metin. {@code error}: geri dönüş/hata açıklaması.
	 */
	public record Decoded(Object value, String text, boolean truncated, String error, Integer schemaId) {

		public static final Decoded NULL = new Decoded(null, null, false, null, null);

		public boolean isNull() {
			return value == null && text == null;
		}

	}

	/** Yapısal çözüm sonucu (şema çıkarımı için): {@code value} ya da {@code error}. */
	public record Structured(JsonNode value, String error, Integer schemaId) {
	}

	/** Baytları çözer; uzun değerler {@code maxChars} karakterde kesilir. */
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
					yield hexFallback(bytes, maxChars, "long için 8 bayt bekleniyordu, " + bytes.length + " bayt var; hex gösterildi",
							null);
				}
				long v = ByteBuffer.wrap(bytes).getLong();
				yield new Decoded(v, Long.toString(v), false, null, null);
			}
			case AVRO -> decodeAvro(bytes, maxChars);
		};
	}

	/** json/avro değerini kesmeden yapısal olarak çözer; başarısızsa {@code error} dolu döner. */
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
			return new Structured(null, "Geçerli UTF-8 değil", null);
		}
		if (s.isBlank()) {
			return new Structured(null, "Boş değer; JSON değil", null);
		}
		try {
			return new Structured(MAPPER.readTree(s), null, null);
		}
		catch (JsonProcessingException e) {
			return new Structured(null, "JSON değil: " + shortMessage(e), null);
		}
	}

	private Decoded decodeString(byte[] bytes, int maxChars) {
		String s = utf8(bytes);
		if (s == null) {
			return hexFallback(bytes, maxChars, "Geçerli UTF-8 değil; hex gösterildi", null);
		}
		return new Decoded(cut(s, maxChars), s, s.length() > maxChars, null, null);
	}

	private Decoded decodeJson(byte[] bytes, int maxChars) {
		String s = utf8(bytes);
		if (s == null) {
			return hexFallback(bytes, maxChars, "Geçerli UTF-8 değil; hex gösterildi", null);
		}
		if (s.isBlank()) {
			return new Decoded(s, s, false, "Boş değer; JSON değil", null);
		}
		if (s.length() > maxChars) {
			return new Decoded(cut(s, maxChars), s, true, null, null);
		}
		try {
			return new Decoded(MAPPER.readTree(s), s, false, null, null);
		}
		catch (JsonProcessingException e) {
			return new Decoded(s, s, false, "JSON değil: " + shortMessage(e) + "; ham metin gösterildi", null);
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
			return hexFallback(bytes, maxChars, e.getMessage() + "; hex gösterildi", e.schemaId);
		}
		catch (JsonProcessingException e) {
			return hexFallback(bytes, maxChars, "Avro değeri JSON'a çevrilemedi; hex gösterildi", null);
		}
	}

	private record AvroValue(JsonNode node, int schemaId) {
	}

	private AvroValue avro(byte[] bytes) throws DecodeException {
		if (bytes.length < 5 || bytes[0] != 0) {
			throw new DecodeException("Confluent wire format değil (0x0 magic byte + 4 bayt şema id yok)", null);
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
			throw new DecodeException(cut("Avro çözülemedi (şema " + id + "): " + e.getClass().getSimpleName()
					+ (e.getMessage() == null ? "" : ": " + e.getMessage()), 200), id);
		}
		catch (OutOfMemoryError e) {
			// bozuk/yanlış şemalı veride uzunluk alanı devasa tek bir dizi ayırmaya çalışır; başarısız olan bu tek
			// ayırma heap'te kalıcı bir şey bırakmaz
			throw new DecodeException("Avro çözülemedi (şema " + id
					+ "): bozuk uzunluk alanı (veri bu şemayla yazılmamış olabilir)", id);
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
			throw new DecodeException("Schema Registry tanımlı değil", id);
		}
		try {
			JsonNode node = registry.get(cluster, "/schemas/ids/" + id);
			String type = node.path("schemaType").asText("AVRO");
			if (!"AVRO".equalsIgnoreCase(type)) {
				throw new DecodeException("Şema " + id + " tipi " + type + "; avro formatı sadece AVRO şemalarını çözer", id);
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
		catch (ToolException e) {
			String msg = "Şema " + id + " alınamadı: " + e.getMessage();
			if (e.getMessage() != null && e.getMessage().contains("ulaşılamadı")) {
				registryDown = msg;
			}
			schemaErrors.put(id, msg);
			throw new DecodeException(msg, id);
		}
		catch (RuntimeException e) {
			String msg = "Şema " + id + " ayrıştırılamadı: " + e.getMessage();
			schemaErrors.put(id, msg);
			throw new DecodeException(msg, id);
		}
	}

	/** Şema referanslarını (başka subject'lerdeki tipler) önce parser'a tanıtır. */
	private void parseReferences(Schema.Parser parser, JsonNode refs, int depth, Set<String> seen) {
		if (!refs.isArray() || refs.isEmpty()) {
			return;
		}
		if (depth >= MAX_REFERENCE_DEPTH) {
			throw new ToolException("şema referans derinliği " + MAX_REFERENCE_DEPTH + "'i aşıyor");
		}
		for (JsonNode ref : refs) {
			String subject = ref.path("subject").asText();
			String version = ref.path("version").asText("latest");
			if (!seen.add(subject + "/" + version)) {
				continue;
			}
			JsonNode node = registry.get(cluster, "/subjects/" + SchemaRegistryClient.enc(subject) + "/versions/"
					+ SchemaRegistryClient.enc(version));
			parseReferences(parser, node.path("references"), depth + 1, seen);
			parser.parse(node.path("schema").asText());
		}
	}

	/** Avro datum'unu JSON'a uygun Java değerine çevirir (logical type'lar okunur biçimde). */
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

	/** Katı UTF-8 çözümü; geçersiz bayt varsa null. */
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

	/** En fazla {@code max} karakter (surrogate çifti bölünmez). */
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

	private static final class DecodeException extends Exception {

		private final Integer schemaId;

		DecodeException(String message, Integer schemaId) {
			super(message, null, false, false);
			this.schemaId = schemaId;
		}

	}

}
