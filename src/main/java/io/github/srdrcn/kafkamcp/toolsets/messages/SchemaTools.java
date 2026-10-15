package io.github.srdrcn.kafkamcp.toolsets.messages;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient;
import io.github.srdrcn.kafkamcp.toolsets.messages.MessageReader.From;

/** Structure/format tools: messages_sample_schema, serdes_list. */
@Component
public class SchemaTools {

	private static final int MAX_ERROR_SAMPLES = 3;

	private final SchemaRegistryClient registry;

	private final KafkaClients clients;

	public SchemaTools(SchemaRegistryClient registry, KafkaClients clients) {
		this.registry = registry;
		this.clients = clients;
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("messages_sample_schema",
					"Infer the value structure from recent messages: JSON paths, types, null/missing ratios, example values")
			.required("topic", MessageParams.topic())
			.param("partition", MessageParams.partition())
			.param("format", MessageParams.format("Value", List.of("json", "avro"), "json"))
			.maxRecordsParam()
			.handler(this::sampleSchema),
				ToolSpec
					.read("serdes_list",
							"List supported message formats (string/json/hex/long/avro) and avro support per cluster")
					.clusterOptional()
					.handler(this::serdesList));
	}

	Object sampleSchema(ToolCall call) {
		String topic = call.str("topic");
		MessageFormat format = MessageFormat.parse("format", call.str("format", null), MessageFormat.JSON);
		if (format != MessageFormat.JSON && format != MessageFormat.AVRO) {
			throw new ToolException("format must be json or avro for messages_sample_schema");
		}
		RecordDecoder.requireSupported(call, format);
		Integer partition = call.optInt("partition");
		MessageReader.Result r = MessageReader.read(call, new MessageReader.Request(topic, partition, From.LATEST, null,
				null, call.maxRecords(), MessageReader.DEFAULT_MAX_SCAN, null));
		RecordDecoder decoder = new RecordDecoder(registry, call);
		ShapeInference shape = new ShapeInference(ShapeInference.DEFAULT_MAX_PATHS);
		int failed = 0;
		int tombstones = 0;
		Map<String, Integer> errors = new LinkedHashMap<>();
		Map<Integer, Integer> schemaIds = new TreeMap<>();
		for (ConsumerRecord<byte[], byte[]> rec : r.records()) {
			if (rec.value() == null) {
				tombstones++;
				continue;
			}
			RecordDecoder.Structured s = decoder.structured(rec.value(), format);
			if (s.schemaId() != null) {
				schemaIds.merge(s.schemaId(), 1, Integer::sum);
			}
			if (s.error() != null) {
				failed++;
				if (errors.containsKey(s.error()) || errors.size() < MAX_ERROR_SAMPLES) {
					errors.merge(s.error(), 1, Integer::sum);
				}
				continue;
			}
			shape.add(s.value());
		}
		List<Map<String, Object>> errorRows = new ArrayList<>();
		errors.forEach((msg, n) -> errorRows.add(Results.map("error", msg, "count", n)));
		Map<String, Object> out = Results.map("topic", topic, "format", format.id(), "partition", partition,
				"recordsRead", r.records().size(), "parsed", shape.records(), "failed", failed, "tombstones",
				tombstones > 0 ? tombstones : null, "schemaIds", schemaIds.isEmpty() ? null : schemaIds, "errors",
				errorRows.isEmpty() ? null : errorRows);
		if (shape.skippedPaths() > 0) {
			out.put("pathsSkipped", shape.skippedPaths());
		}
		if (shape.arraysSampled()) {
			out.put("arraysSampled", "Only the first " + ShapeInference.MAX_ARRAY_ELEMENTS + " elements of each array were inspected");
		}
		ConsumeTools.status(out, r, decoder);
		out.put("paths", shape.paths());
		return out;
	}

	Object serdesList(ToolCall call) {
		List<Map<String, Object>> formats = new ArrayList<>();
		for (MessageFormat f : MessageFormat.values()) {
			formats.add(Results.map("name", f.id(), "description", f.description(), "requiresSchemaRegistry",
					f.needsSchemaRegistry() ? true : null));
		}
		String only = call.clusterOrNull();
		List<Map<String, Object>> clusters = new ArrayList<>();
		for (ClusterConfig c : clients.config().getClusters()) {
			if (only == null || only.equals(c.getName())) {
				clusters.add(Results.map("name", c.getName(), "avro", c.hasSchemaRegistry()));
			}
		}
		return Results.map("formats", formats, "clusters", clusters, "usage",
				"messages_consume/messages_get: format (value) and keyFormat (key); messages_sample_schema: format json|avro");
	}

}
