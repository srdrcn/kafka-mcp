package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient;

/**
 * Ağa çıkmayan yardımcılar: RecordDecoder (şema isteği limit/deadline), ShapeInference, Timestamps, adil pay
 * dağıtımı, offline partition seçimi, sınırlı consumer kapanışı.
 */
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
		// negatif değerler listOffsets'te LATEST(-1)/EARLIEST(-2)/MAX_TIMESTAMP(-3)... anlamına gelir
		assertThat(Timestamps.parse("t", "0")).isZero();
		assertThat(Timestamps.parse("t", "1970-01-01T00:00:00Z")).isZero();
		for (String v : List.of("-1", "-2", "-3", "-5", "-1700000000000", "1969-12-31T23:59:59.999Z", "1969-12-31")) {
			assertThatThrownBy(() -> Timestamps.parse("ts", v)).as(v)
				.isInstanceOf(ToolException.class)
				.hasMessageContaining("ts")
				.hasMessageContaining("negatif")
				.hasMessageContaining("1970");
		}
	}

	private static ConsumerRecord<byte[], byte[]> rec(int partition, long offset, long timestamp) {
		return new ConsumerRecord<>("t", partition, offset, timestamp,
				timestamp < 0 ? TimestampType.NO_TIMESTAMP_TYPE : TimestampType.CREATE_TIME, 0, 0, null, null,
				new RecordHeaders(), Optional.empty());
	}

	/** latest seçimi gibi: sınırlı min-heap en yenileri tutar. */
	private static List<String> newest(List<ConsumerRecord<byte[], byte[]>> records, PartitionOffsets.Bounds b,
			int max) {
		List<TopicPartition> tps = List.of(new TopicPartition("t", 0), new TopicPartition("t", 1),
				new TopicPartition("t", 2));
		PriorityQueue<ConsumerRecord<byte[], byte[]>> heap = new PriorityQueue<>(MessageReader.newestLast(tps, b));
		for (ConsumerRecord<byte[], byte[]> r : records) {
			heap.add(r);
			if (heap.size() > max) {
				heap.poll();
			}
		}
		List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>(heap);
		out.sort(MessageReader.BY_TIME);
		return out.stream().map(r -> r.partition() + "@" + r.offset()).toList();
	}

	@Test
	void latestOrderIsFairAcrossPartitionsForRecordsWithoutTimestamp() {
		// p0: 0..9, p1: 0..4, p2: 0..9 — hepsi zaman damgasız (v0): partition sonlarından sırayla
		Map<TopicPartition, Long> latest = Map.of(new TopicPartition("t", 0), 10L, new TopicPartition("t", 1), 5L,
				new TopicPartition("t", 2), 10L);
		PartitionOffsets.Bounds b = new PartitionOffsets.Bounds(Map.of(), latest);
		List<ConsumerRecord<byte[], byte[]>> untimed = new ArrayList<>();
		for (int p = 0; p < 3; p++) {
			for (long o = 0; o < latest.get(new TopicPartition("t", p)); o++) {
				untimed.add(rec(p, o, -1));
			}
		}
		assertThat(newest(untimed, b, 6)).containsExactly("0@8", "0@9", "1@3", "1@4", "2@8", "2@9");
		assertThat(MessageReader.untimed(untimed.get(0))).isTrue();
		// zamanlı kayıtlar: zamana göre (partition id'sinden bağımsız)
		List<ConsumerRecord<byte[], byte[]>> timed = new ArrayList<>(List.of(rec(2, 0, 100), rec(0, 0, 300),
				rec(1, 0, 200), rec(2, 1, 50), rec(0, 1, 400)));
		assertThat(newest(timed, b, 3)).containsExactly("1@0", "0@0", "0@1");
		// karışık: zamanlı kayıt zamansızlardan yeni sayılır
		List<ConsumerRecord<byte[], byte[]>> mixed = new ArrayList<>(untimed);
		mixed.add(rec(1, 5, 10));
		assertThat(newest(mixed, b, 4)).containsExactly("0@9", "1@4", "2@9", "1@5");
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

	private static ToolCall call(long remainingMillis) {
		return new ToolCall("test", Map.of("cluster", "x"), null, Instant.now().plusMillis(remainingMillis));
	}

	private static SchemaRegistryClient registry(Function<String, JsonNode> get) {
		return new SchemaRegistryClient(null) {
			@Override
			public JsonNode get(String cluster, String path) {
				return get.apply(path);
			}
		};
	}

	/** Confluent wire format: 0x0 + 4 bayt şema id + 1 bayt gövde. */
	private static byte[] wire(int schemaId) {
		return ByteBuffer.allocate(6).put((byte) 0).putInt(schemaId).put((byte) 2).array();
	}

	@Test
	void schemaLookupsAreCappedPerCall() {
		AtomicInteger calls = new AtomicInteger();
		RecordDecoder d = new RecordDecoder(registry(path -> {
			calls.incrementAndGet();
			throw new ToolException("Schema Registry hatası 404 (GET " + path + "): 40403 Schema not found");
		}), "x");
		int ids = RecordDecoder.MAX_SCHEMA_LOOKUPS + 10;
		RecordDecoder.Decoded last = null;
		for (int id = 1; id <= ids; id++) {
			last = d.decode(wire(id), MessageFormat.AVRO, 100);
		}
		assertThat(calls.get()).isEqualTo(RecordDecoder.MAX_SCHEMA_LOOKUPS);
		assertThat(d.skippedSchemaLookups()).isEqualTo(10);
		assertThat(d.timeLimited()).isFalse();
		assertThat(last.value()).isEqualTo(RecordDecoder.hex(wire(ids)));
		assertThat(last.error()).contains("en fazla " + RecordDecoder.MAX_SCHEMA_LOOKUPS);
		assertThat(d.skipNote()).startsWith("10 şema");
		// aynı id tekrar sorulmaz (çağrı içi memo)
		d.decode(wire(1), MessageFormat.AVRO, 100);
		assertThat(calls.get()).isEqualTo(RecordDecoder.MAX_SCHEMA_LOOKUPS);
	}

	@Test
	void slowSchemaRegistryIsCutBeforeToolDeadline() {
		AtomicInteger calls = new AtomicInteger();
		SchemaRegistryClient slow = registry(path -> {
			calls.incrementAndGet();
			try {
				Thread.sleep(60_000);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			throw new ToolException("kesildi");
		});
		ToolCall call = call(RecordDecoder.LOOKUP_RESERVE_MS + 2_000);
		RecordDecoder d = new RecordDecoder(slow, call);
		RecordDecoder.Decoded first = d.decode(wire(1), MessageFormat.AVRO, 100);
		// istek deadline'a pay kalırken bırakıldı: okunanlar partial dönebilir
		assertThat(call.remainingMillis()).isGreaterThan(500);
		assertThat(first.error()).contains("zaman sınırı");
		assertThat(first.value()).isEqualTo(RecordDecoder.hex(wire(1)));
		RecordDecoder.Decoded second = d.decode(wire(2), MessageFormat.AVRO, 100);
		assertThat(second.error()).contains("atlandı");
		assertThat(calls.get()).isEqualTo(1);
		assertThat(d.timeLimited()).isTrue();
		assertThat(d.skippedSchemaLookups()).isEqualTo(2);

		// deadline zaten yakınsa istek hiç başlatılmaz
		AtomicInteger none = new AtomicInteger();
		RecordDecoder late = new RecordDecoder(registry(path -> {
			none.incrementAndGet();
			return null;
		}), call(RecordDecoder.LOOKUP_RESERVE_MS - 500));
		assertThat(late.decode(wire(3), MessageFormat.AVRO, 100).error()).contains("zaman sınırı");
		assertThat(none.get()).isZero();
		assertThat(late.timeLimited()).isTrue();
	}

	@Test
	void offlinePartitionsAreSkippedAndReported() {
		Node n = new Node(1, "broker-1", 9092);
		TopicDescription d = new TopicDescription("t", false,
				List.of(new TopicPartitionInfo(0, n, List.of(n), List.of(n)),
						new TopicPartitionInfo(1, null, List.of(n), List.of()),
						new TopicPartitionInfo(2, Node.noNode(), List.of(n), List.of()),
						new TopicPartitionInfo(3, n, List.of(n), List.of(n))));
		PartitionOffsets.Selection all = PartitionOffsets.select(d, null);
		assertThat(all.online()).containsExactly(new TopicPartition("t", 0), new TopicPartition("t", 3));
		assertThat(all.offline()).containsExactly(1, 2);
		assertThat(all.note()).contains("offline").contains("[1, 2]");
		assertThat(PartitionOffsets.select(d, 3).online()).containsExactly(new TopicPartition("t", 3));
		assertThat(PartitionOffsets.select(d, 3).note()).isNull();
		assertThatThrownBy(() -> PartitionOffsets.select(d, 1)).isInstanceOf(ToolException.class)
			.hasMessageContaining("Partition 1 offline");
		assertThatThrownBy(() -> PartitionOffsets.select(d, 9)).hasMessageContaining("Partition 9 yok");
	}

	@Test
	@SuppressWarnings("unchecked")
	void consumerCloseIsBoundedByRemainingTime() {
		KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
		new BoundedConsumer(call(14_000), consumer).close();
		ArgumentCaptor<CloseOptions> options = ArgumentCaptor.forClass(CloseOptions.class);
		verify(consumer).close(options.capture());
		assertThat(options.getValue().timeout()).contains(Duration.ofMillis(BoundedConsumer.MAX_CLOSE_MS));
		assertThat(BoundedConsumer.closeMillis(800)).isEqualTo(300);
		assertThat(BoundedConsumer.closeMillis(100)).isZero();
	}

}
