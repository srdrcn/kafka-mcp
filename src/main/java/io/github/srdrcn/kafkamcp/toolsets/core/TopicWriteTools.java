package io.github.srdrcn.kafkamcp.toolsets.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.Configs;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Topic yazma araçları: oluşturma, klonlama, config güncelleme, partition artırma, silme. */
@Component
public class TopicWriteTools {

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.write("topics_create", "Topic oluşturur (partition/RF boşsa broker varsayılanı)")
					.required("topic", P.string("Topic adı"))
					.param("partitions", P.integer("Partition sayısı (boşsa broker varsayılanı)", 1, 100_000))
					.param("replicationFactor", P.integer("Replication factor (boşsa broker varsayılanı)", 1, 32))
					.param("configs", P.stringMap("Topic config'leri (anahtar → değer)"))
					.handler(this::topicsCreate),
				ToolSpec.write("topics_clone",
						"Kaynak topic'in partition sayısı, RF ve topic seviyesindeki config'leriyle yeni topic oluşturur (veri kopyalanmaz)")
					.required("source", P.string("Kaynak topic"))
					.required("target", P.string("Oluşturulacak topic"))
					.param("partitions", P.integer("Partition sayısı (boşsa kaynaktaki)", 1, 100_000))
					.param("replicationFactor", P.integer("Replication factor (boşsa kaynaktaki)", 1, 32))
					.param("configs", P.stringMap("Kaynaktakilerin üzerine yazılacak config'ler (anahtar → değer)"))
					.handler(this::topicsClone),
				ToolSpec.write("topics_config_update", "Topic config'ini değiştirir (set/delete); önce/sonra değerlerini döner")
					.required("topic", P.string("Topic adı"))
					.param("set", P.stringMap("Atanacak config'ler (anahtar → değer)"))
					.param("delete", P.stringArray("Silinecek (varsayılana dönecek) config adları"))
					.handler(this::topicsConfigUpdate),
				ToolSpec.write("topics_partitions_increase",
						"Topic partition sayısını artırır (azaltılamaz; key'li verilerde key→partition eşlemesi değişir)")
					.required("topic", P.string("Topic adı"))
					.required("totalCount", P.integer("Yeni toplam partition sayısı (mevcuttan büyük)", 2, 100_000))
					.handler(this::topicsPartitionsIncrease),
				ToolSpec.destructive("topics_delete", "Topic'i ve tüm verisini siler (iç/sistem topic'leri reddedilir)")
					.required("topic", P.string("Silinecek topic adı (tek)"))
					.handler(this::topicsDelete));
	}

	Object topicsCreate(ToolCall call) {
		return create(call, call.str("topic"), call.optInt("partitions"), call.optInt("replicationFactor"),
				configs(call));
	}

	Object topicsClone(ToolCall call) {
		String source = call.str("source");
		String target = call.str("target");
		KafkaFuture<Map<TopicPartition, PartitionReassignment>> pendingReassignments = Topics
			.submitReassignments(call);
		TopicDescription d = Topics.describeOne(call, source);
		Map<String, String> copied = new TreeMap<>();
		for (ConfigEntry e : Configs.describeOne(call, Configs.topic(source)).entries()) {
			if (e.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG && !e.isSensitive() && e.value() != null) {
				copied.put(e.name(), e.value());
			}
		}
		Map<String, String> overrides = configs(call);
		Map<String, String> merged = new LinkedHashMap<>(copied);
		merged.putAll(overrides);
		Integer partitions = call.optInt("partitions");
		Integer rf = call.optInt("replicationFactor");
		if (rf == null) {
			// reassignment sürerken eklenen replikalar RF'yi şişirmesin
			rf = Topics.replicationFactor(d, Topics.reassignments(call, pendingReassignments));
		}
		Map<String, Object> out = create(call, target, partitions == null ? d.partitions().size() : partitions, rf,
				merged);
		out.put("source", source);
		out.put("copiedConfigs", copied);
		if (!overrides.isEmpty()) {
			out.put("overriddenConfigs", overrides);
		}
		out.put("note", "Sadece yapı kopyalandı; mesajlar kopyalanmadı");
		return out;
	}

	Object topicsConfigUpdate(ToolCall call) {
		String topic = call.str("topic");
		List<Map<String, Object>> changes = Configs.alter(call, Configs.topic(topic), call.stringMap("set"),
				call.strings("delete"));
		return Results.map("topic", topic, "changes", changes);
	}

	Object topicsPartitionsIncrease(ToolCall call) {
		String topic = call.str("topic");
		int total = Math.toIntExact(call.requiredLong("totalCount"));
		TopicDescription d = Topics.describeOne(call, topic);
		int current = d.partitions().size();
		if (total <= current) {
			throw new ToolException("totalCount (" + total + ") mevcut partition sayısından (" + current
					+ ") büyük olmalı; partition sayısı azaltılamaz");
		}
		call.await(call.admin().createPartitions(Map.of(topic, NewPartitions.increaseTo(total))).all());
		return Results.map("topic", topic, "before", current, "after", total, "warning",
				"Key'li mesajlarda key → partition eşlemesi değişti: aynı key'in yeni mesajları farklı partition'a "
						+ "gidebilir (key bazlı sıralama ve compaction etkilenir)");
	}

	Object topicsDelete(ToolCall call) {
		String topic = call.str("topic");
		TopicDescription d = Topics.describeOne(call, topic);
		String reason = Topics.protectedReason(topic, d.isInternal());
		if (reason != null) {
			throw new ToolException("Silme reddedildi: " + topic + " bir " + reason);
		}
		call.await(call.admin().deleteTopics(List.of(topic)).all());
		return Results.map("deleted", topic, "partitions", d.partitions().size());
	}

	private Map<String, Object> create(ToolCall call, String topic, Integer partitions, Integer rf,
			Map<String, String> configs) {
		if (partitions != null && partitions < 1) {
			throw new ToolException("partitions en az 1 olmalı");
		}
		if (rf != null) {
			if (rf < 1) {
				throw new ToolException("replicationFactor en az 1 olmalı");
			}
			int brokers = ClusterMeta.describe(call, false).brokers().size();
			if (rf > brokers) {
				throw new ToolException("replicationFactor (" + rf + ") broker sayısından (" + brokers
						+ ") büyük olamaz");
			}
		}
		NewTopic nt = new NewTopic(topic, Optional.ofNullable(partitions),
				Optional.ofNullable(rf == null ? null : rf.shortValue()))
			.configs(configs);
		CreateTopicsResult r = call.admin().createTopics(List.of(nt));
		call.await(r.all());
		Map<String, Object> out = Results.map("topic", topic);
		try {
			out.put("topicId", String.valueOf(call.await(r.topicId(topic))));
			out.put("partitions", call.await(r.numPartitions(topic)));
			out.put("replicationFactor", call.await(r.replicationFactor(topic)));
		}
		catch (ToolException e) {
			// topic oluştu; ayrıntılar okunamadı (ör. DESCRIBE_CONFIGS yetkisi yok ya da eski broker)
			out.putAll(Results.map("partitions", partitions, "replicationFactor", rf));
			out.put("note", "Topic oluşturuldu; ayrıntıları okunamadı (" + e.getMessage() + ")");
		}
		if (!configs.isEmpty()) {
			out.put("configs", configs);
		}
		return out;
	}

	private static Map<String, String> configs(ToolCall call) {
		Map<String, String> configs = call.stringMap("configs");
		configs.forEach((k, v) -> {
			if (v == null) {
				throw new ToolException("configs değeri boş olamaz: " + k);
			}
		});
		return configs;
	}

}
