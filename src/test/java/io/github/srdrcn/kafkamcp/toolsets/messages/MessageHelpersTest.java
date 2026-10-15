package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Ağa çıkmayan yardımcılar: RecordDecoder, ShapeInference, Timestamps, adil pay dağıtımı. */
class MessageHelpersTest {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private final RecordDecoder decoder = new RecordDecoder(null, "x");

	@Test
	void decodesStringWithHexFallbackAndTruncation() {
		RecordDecoder.Decoded d = decoder.decode("merhaba dünya".getBytes(StandardCharsets.UTF_8), MessageFormat.STRING,
				100);
		assertThat(d.value()).isEqualTo("merhaba dünya");
		assertThat(d.truncated()).isFalse();
		assertThat(d.error()).isNull();

		RecordDecoder.Decoded bad = decoder.decode(new byte[] { (byte) 0xff, (byte) 0xfe, 0x41 }, MessageFormat.STRING, 100);
		assertThat(bad.value()).isEqualTo("fffe41");
		assertThat(bad.error()).contains("UTF-8");

		RecordDecoder.Decoded cut = decoder.decode("x".repeat(50).getBytes(StandardCharsets.UTF_8), MessageFormat.STRING,
				16);
		assertThat(cut.value()).isEqualTo("x".repeat(16));
		assertThat(cut.truncated()).isTrue();
		assertThat(cut.text()).hasSize(50);
	}

	@Test
	void decodesJsonLongAndHex() {
		RecordDecoder.Decoded json = decoder.decode("{\"a\":1,\"b\":null,\"c\":[true]}".getBytes(StandardCharsets.UTF_8),
				MessageFormat.JSON, 100);
		assertThat(json.value()).isInstanceOf(JsonNode.class);
		JsonNode node = (JsonNode) json.value();
		assertThat(node.get("a").asInt()).isEqualTo(1);
		assertThat(node.has("b")).isTrue();
		assertThat(node.get("b").isNull()).isTrue();

		RecordDecoder.Decoded notJson = decoder.decode("hello world".getBytes(StandardCharsets.UTF_8), MessageFormat.JSON,
				100);
		assertThat(notJson.value()).isEqualTo("hello world");
		assertThat(notJson.error()).startsWith("JSON değil");

		RecordDecoder.Decoded trailing = decoder.decode("123 abc".getBytes(StandardCharsets.UTF_8), MessageFormat.JSON,
				100);
		assertThat(trailing.error()).startsWith("JSON değil");

		RecordDecoder.Decoded blank = decoder.decode(new byte[0], MessageFormat.JSON, 100);
		assertThat(blank.error()).isNotNull();

		RecordDecoder.Decoded l = decoder.decode(TestData.longBytes(-42L), MessageFormat.LONG, 100);
		assertThat(l.value()).isEqualTo(-42L);
		RecordDecoder.Decoded shortLong = decoder.decode(new byte[] { 1, 2, 3 }, MessageFormat.LONG, 100);
		assertThat(shortLong.value()).isEqualTo("010203");
		assertThat(shortLong.error()).contains("8 bayt");

		assertThat(decoder.decode(new byte[] { 0x0a, (byte) 0xbc }, MessageFormat.HEX, 100).value()).isEqualTo("0abc");
		assertThat(decoder.decode(null, MessageFormat.STRING, 100).isNull()).isTrue();
	}

	@Test
	void avroWithoutWireFormatFallsBackToHex() {
		RecordDecoder.Decoded d = decoder.decode("plain".getBytes(StandardCharsets.UTF_8), MessageFormat.AVRO, 100);
		assertThat(d.value()).isEqualTo("706c61696e");
		assertThat(d.error()).contains("wire format");

		RecordDecoder.Decoded noRegistry = decoder.decode(new byte[] { 0, 0, 0, 0, 7, 2 }, MessageFormat.AVRO, 100);
		assertThat(noRegistry.schemaId()).isEqualTo(7);
		assertThat(noRegistry.error()).contains("Schema Registry");
	}

	@Test
	void infersJsonPaths() throws Exception {
		ShapeInference s = new ShapeInference(50);
		s.add(MAPPER.readTree("{\"id\":1,\"user\":{\"name\":\"a\",\"tags\":[\"x\",\"y\"]},\"items\":[{\"sku\":\"s1\",\"qty\":2}]}"));
		s.add(MAPPER.readTree("{\"id\":2.5,\"user\":{\"name\":null},\"items\":[]}"));
		s.add(MAPPER.readTree("{\"id\":\"3\"}"));
		Map<String, Map<String, Object>> byPath = new LinkedHashMap<>();
		for (Map<String, Object> p : s.paths()) {
			byPath.put((String) p.get("path"), p);
		}
		assertThat(byPath).containsKeys("id", "user", "user.name", "user.tags", "user.tags[]", "items", "items[]",
				"items[].sku", "items[].qty");
		assertThat(byPath.get("id").get("types")).isEqualTo(Map.of("integer", 1, "number", 1, "string", 1));
		assertThat(byPath.get("id").get("missingRatio")).isEqualTo(0.0);
		assertThat(byPath.get("user.name").get("nullRatio")).isEqualTo(0.5);
		assertThat(byPath.get("user").get("missingRatio")).isEqualTo(0.333);
		assertThat(byPath.get("items[].sku").get("presentIn")).isEqualTo(1);
		assertThat(byPath.get("user.tags[]").get("example").toString()).isEqualTo("\"x\"");

		ShapeInference capped = new ShapeInference(2);
		capped.add(MAPPER.readTree("{\"a\":1,\"b\":2,\"c\":3,\"d\":4}"));
		assertThat(capped.paths()).hasSize(2);
		assertThat(capped.skippedPaths()).isEqualTo(2);

		ShapeInference scalar = new ShapeInference(5);
		scalar.add(MAPPER.readTree("42"));
		assertThat(scalar.paths().get(0).get("path")).isEqualTo("$");
	}

	@Test
	void parsesTimestamps() {
		assertThat(Timestamps.parse("t", "1700000000000")).isEqualTo(1_700_000_000_000L);
		assertThat(Timestamps.parse("t", "2023-11-14T22:13:20Z")).isEqualTo(1_700_000_000_000L);
		assertThat(Timestamps.parse("t", "2023-11-15T01:13:20+03:00")).isEqualTo(1_700_000_000_000L);
		assertThat(Timestamps.parse("t", "2023-11-14T22:13:20")).isEqualTo(1_700_000_000_000L);
		assertThat(Timestamps.parse("t", "2023-11-14 22:13:20")).isEqualTo(1_700_000_000_000L);
		assertThat(Timestamps.parse("t", "2023-11-14")).isEqualTo(1_699_920_000_000L);
		assertThat(Timestamps.iso(1_700_000_000_000L)).isEqualTo("2023-11-14T22:13:20Z");
		assertThat(Timestamps.iso(-1)).isNull();
		assertThatThrownBy(() -> Timestamps.parse("t", "dün")).isInstanceOf(ToolException.class)
			.hasMessageContaining("ISO-8601");
	}

	@Test
	void fairShareDistributesBudget() {
		TopicPartition p0 = new TopicPartition("t", 0);
		TopicPartition p1 = new TopicPartition("t", 1);
		TopicPartition p2 = new TopicPartition("t", 2);
		Map<TopicPartition, Long> share = RecordScanner.fairShare(Map.of(p0, 2L, p1, 100L, p2, 100L), 50);
		assertThat(share).containsEntry(p0, 2L);
		assertThat(share.get(p1) + share.get(p2)).isEqualTo(48);
		assertThat(Math.abs(share.get(p1) - share.get(p2))).isLessThanOrEqualTo(1);
		assertThat(RecordScanner.fairShare(Map.of(p0, 5L, p1, 0L), 100)).containsEntry(p0, 5L).containsEntry(p1, 0L);
		assertThat(RecordScanner.sum(RecordScanner.fairShare(Map.of(p0, 10L, p1, 10L, p2, 10L), 2))).isEqualTo(2);
		assertThat(MessageFormat.ids()).containsExactly("string", "json", "hex", "long", "avro");
		assertThatThrownBy(() -> MessageFormat.parse("format", "xml", MessageFormat.STRING))
			.isInstanceOf(ToolException.class);
		assertThat(List.of(MessageReader.From.parse("Earliest", null))).containsExactly(MessageReader.From.EARLIEST);
	}

}
