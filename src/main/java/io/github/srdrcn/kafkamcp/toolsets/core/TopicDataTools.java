package io.github.srdrcn.kafkamcp.toolsets.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.LastRecords;
import io.github.srdrcn.kafkamcp.kafka.LogDirs;
import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Topic veri metrikleri: boyut, offset'ler, son yazma zamanı, yazma hızı. */
@Component
public class TopicDataTools {

	public List<ToolSpec> tools() {
		Map<String, Object> interval = P.integer("Örnekler arası saniye (varsayılan 5, 1-10)", 1, 10);
		interval.put("default", 5);
		return List.of(
				ToolSpec.read("topics_size",
						"Topic boyutu: lider replikalar (mantıksal) ve tüm replikalar; topic yoksa en büyük topic'ler")
					.param("topic", P.string("Topic adı (boşsa en büyük topic'ler listelenir)"))
					.param("includeInternal", P.bool("Topic verilmediğinde iç topic'leri de say (varsayılan false)"))
					.listParams()
					.handler(this::topicsSize),
				ToolSpec.read("topics_offsets",
						"Partition başına earliest/latest offset ve yaklaşık mesaj sayısı (latest - earliest), toplamlar")
					.required("topic", P.string("Topic adı"))
					.handler(this::topicsOffsets),
				ToolSpec.read("topics_last_write_time",
						"Topic'e son yazılma zamanı (partition bazında ve topic geneli, ISO-8601 ve yaş)")
					.required("topic", P.string("Topic adı"))
					.handler(this::topicsLastWriteTime),
				ToolSpec.read("topics_write_rate",
						"Yazma hızı (mesaj/sn): iki offset örneği arasındaki fark; topic yoksa en hızlı topic'ler")
					.param("topic", P.string("Topic adı (boşsa en hızlı topic'ler)"))
					.param("intervalSeconds", interval)
					.listParams()
					.handler(this::topicsWriteRate));
	}

	Object topicsSize(ToolCall call) {
		if (call.has("topic")) {
			String topic = call.str("topic");
			TopicDescription d = Topics.describeOne(call, topic);
			Futures.Each<Integer, Map<String, LogDirDescription>> dirs = LogDirs.describe(call,
					LogDirs.brokersHosting(List.of(d)));
			Map<TopicPartition, LogDirs.PartitionSize> sizes = LogDirs.partitionSizes(dirs.values(), Map.of(topic, d));
			long leader = 0;
			long total = 0;
			List<Map<String, Object>> rows = new ArrayList<>();
			for (Map.Entry<TopicPartition, LogDirs.PartitionSize> e : sizes.entrySet()) {
				LogDirs.PartitionSize s = e.getValue();
				leader += s.leaderBytes();
				total += s.totalBytes();
				rows.add(Results.map("partition", e.getKey().partition(), "sizeBytes", s.leaderBytes(), "size",
						Format.bytes(s.leaderBytes()), "totalSizeBytes", s.totalBytes(), "replicas", s.replicas()));
			}
			rows.sort(Comparator.comparingLong((Map<String, Object> m) -> (Long) m.get("sizeBytes"))
				.reversed()
				.thenComparingInt(m -> (Integer) m.get("partition")));
			Map<String, Object> out = Results.map("topic", topic, "partitionCount", d.partitions().size(),
					"sizeBytes", leader, "size", Format.bytes(leader), "totalSizeBytes", total, "totalSize",
					Format.bytes(total), "failedBrokers", dirs.errors().isEmpty() ? null : dirs.errors());
			out.putAll(Results.page("partitions", rows, call.limit()));
			return out;
		}
		Map<String, Boolean> listing = Topics.list(call, call.bool("includeInternal", false));
		List<String> names = listing.keySet().stream().filter(call::matches).toList();
		Futures.Each<String, TopicDescription> described = Topics.describe(call, names);
		Futures.Each<Integer, Map<String, LogDirDescription>> dirs = LogDirs.describe(call,
				LogDirs.brokersHosting(described.values().values()));
		Map<TopicPartition, LogDirs.PartitionSize> sizes = LogDirs.partitionSizes(dirs.values(), described.values());
		Map<String, long[]> byTopic = new TreeMap<>();
		described.values().keySet().forEach(t -> byTopic.put(t, new long[2]));
		long leader = 0;
		long total = 0;
		for (Map.Entry<TopicPartition, LogDirs.PartitionSize> e : sizes.entrySet()) {
			long[] a = byTopic.get(e.getKey().topic());
			a[0] += e.getValue().leaderBytes();
			a[1] += e.getValue().totalBytes();
			leader += e.getValue().leaderBytes();
			total += e.getValue().totalBytes();
		}
		List<Map<String, Object>> rows = new ArrayList<>();
		byTopic.forEach((t, a) -> rows.add(Results.map("topic", t, "sizeBytes", a[0], "size", Format.bytes(a[0]),
				"totalSizeBytes", a[1], "totalSize", Format.bytes(a[1]), "partitions",
				described.values().get(t).partitions().size())));
		rows.sort(Comparator.comparingLong((Map<String, Object> m) -> (Long) m.get("sizeBytes")).reversed());
		Map<String, Object> out = Results.map("topicsMatched", names.size(), "sizeBytes", leader, "size",
				Format.bytes(leader), "totalSizeBytes", total, "totalSize", Format.bytes(total), "failedBrokers",
				dirs.errors().isEmpty() ? null : dirs.errors(), "topicsNotDescribed",
				described.errors().isEmpty() ? null : described.errors().size());
		out.putAll(Results.page("topics", rows, call.limit()));
		return out;
	}

	Object topicsOffsets(ToolCall call) {
		String topic = call.str("topic");
		TopicDescription d = Topics.describeOne(call, topic);
		Map<TopicPartition, Offsets.Range> ranges = Offsets.ranges(call, Topics.onlinePartitions(List.of(d)));
		List<Map<String, Object>> rows = new ArrayList<>();
		long earliest = 0;
		long latest = 0;
		long count = 0;
		for (TopicPartitionInfo p : d.partitions()) {
			Offsets.Range r = ranges.get(new TopicPartition(topic, p.partition()));
			if (r == null) {
				rows.add(Results.map("partition", p.partition(), "error",
						Topics.offline(p) ? "offline (lider yok)" : "offset alınamadı"));
				continue;
			}
			earliest += r.earliest();
			latest += r.latest();
			count += r.count();
			rows.add(Results.map("partition", p.partition(), "earliest", r.earliest(), "latest", r.latest(), "count",
					r.count()));
		}
		return Results.map("topic", topic, "totals",
				Results.map("earliest", earliest, "latest", latest, "count", count), "partitions", rows);
	}

	Object topicsLastWriteTime(ToolCall call) {
		String topic = call.str("topic");
		TopicDescription d = Topics.describeOne(call, topic);
		List<TopicPartition> online = Topics.onlinePartitions(List.of(d));
		ListOffsetsResult maxTs = Offsets.submit(call, online, OffsetSpec.maxTimestamp());
		Map<TopicPartition, Offsets.Range> ranges = Offsets.ranges(call, online);
		Map<TopicPartition, ListOffsetsResultInfo> max = Offsets.awaitEach(call, online, maxTs).values();
		Map<TopicPartition, Offsets.Range> fallback = new LinkedHashMap<>();
		ranges.forEach((tp, r) -> {
			ListOffsetsResultInfo i = max.get(tp);
			if (r.count() > 0 && (i == null || i.timestamp() < 0)) {
				fallback.put(tp, r);
			}
		});
		Map<TopicPartition, LastRecords.Last> last = LastRecords.read(call, fallback, 5_000);
		List<Map<String, Object>> rows = new ArrayList<>();
		long topicMax = -1;
		Integer topicMaxPartition = null;
		int empty = 0;
		for (TopicPartitionInfo p : d.partitions()) {
			TopicPartition tp = new TopicPartition(topic, p.partition());
			Offsets.Range r = ranges.get(tp);
			if (r == null) {
				rows.add(Results.map("partition", p.partition(), "error",
						Topics.offline(p) ? "offline (lider yok)" : "offset alınamadı"));
				continue;
			}
			if (r.count() == 0) {
				empty++;
				rows.add(Results.map("partition", p.partition(), "empty", true));
				continue;
			}
			long ts;
			long offset;
			String source = null;
			if (fallback.containsKey(tp)) {
				LastRecords.Last l = last.get(tp);
				if (l == null || l.timestamp() < 0) {
					rows.add(Results.map("partition", p.partition(), "error", "son kayıt okunamadı"));
					continue;
				}
				ts = l.timestamp();
				offset = l.offset();
				source = "lastRecord";
			}
			else {
				ts = max.get(tp).timestamp();
				offset = max.get(tp).offset();
			}
			if (ts > topicMax) {
				topicMax = ts;
				topicMaxPartition = p.partition();
			}
			rows.add(Results.map("partition", p.partition(), "lastWriteTime", Format.iso(ts), "ageSeconds",
					Format.ageSeconds(ts), "offset", offset, "source", source));
		}
		return Results.map("topic", topic, "lastWriteTime", Format.iso(topicMax), "ageSeconds",
				Format.ageSeconds(topicMax), "partition", topicMaxPartition, "emptyPartitions", empty, "method",
				fallback.isEmpty() ? "maxTimestamp" : "maxTimestamp+lastRecord", "partitions", rows);
	}

	Object topicsWriteRate(ToolCall call) {
		int intervalMs = call.integer("intervalSeconds", 5, 1, 10) * 1000;
		String topic = call.str("topic", null);
		List<TopicPartition> tps;
		int topicsScanned;
		if (topic != null) {
			tps = Topics.onlinePartitions(List.of(Topics.describeOne(call, topic)));
			topicsScanned = 1;
		}
		else {
			List<String> names = Topics.list(call, false).keySet().stream().filter(call::matches).toList();
			Futures.Each<String, TopicDescription> described = Topics.describe(call, names);
			tps = Topics.onlinePartitions(described.values().values());
			topicsScanned = described.values().size();
		}
		long t0 = System.nanoTime();
		Map<TopicPartition, Long> first = Offsets.latest(call, tps);
		long t1 = System.nanoTime();
		long sampleMs = (t1 - t0) / 1_000_000;
		long available = call.remainingMillis() - 1_300 - Math.max(1_000, 2 * sampleMs);
		long sleepMs = Math.min(intervalMs, available);
		if (sleepMs < 500) {
			throw new ToolException("İki örnek için yeterli süre kalmadı; daha dar filtre ya da tek topic deneyin");
		}
		call.sleep(sleepMs);
		Map<TopicPartition, Long> second = Offsets.latest(call, tps);
		double elapsed = (System.nanoTime() - t1) / 1e9;
		Map<String, Object> out = new LinkedHashMap<>();
		if (topic != null) {
			out.put("topic", topic);
		}
		out.put("intervalSeconds", Format.round(elapsed, 2));
		if (sleepMs < intervalMs) {
			out.put("intervalReduced", true);
		}
		Map<String, long[]> byTopic = new TreeMap<>();
		List<Map<String, Object>> partitionRows = new ArrayList<>();
		long totalDelta = 0;
		int missing = 0;
		for (TopicPartition tp : tps) {
			Long a = first.get(tp);
			Long b = second.get(tp);
			if (a == null || b == null) {
				missing++;
				continue;
			}
			long delta = Math.max(0, b - a);
			totalDelta += delta;
			byTopic.computeIfAbsent(tp.topic(), k -> new long[1])[0] += delta;
			if (topic != null) {
				partitionRows.add(Results.map("partition", tp.partition(), "messages", delta, "msgsPerSec",
						Format.round(delta / elapsed, 2)));
			}
		}
		out.put("totalMessages", totalDelta);
		out.put("totalMsgsPerSec", Format.round(totalDelta / elapsed, 2));
		if (missing > 0) {
			out.put("partitionsWithoutOffsets", missing);
		}
		if (topic != null) {
			partitionRows.sort(Comparator.comparingInt(m -> (Integer) m.get("partition")));
			out.put("partitions", partitionRows);
			return out;
		}
		List<Map<String, Object>> rows = new ArrayList<>();
		byTopic.forEach((t, a) -> {
			if (a[0] > 0) {
				rows.add(Results.map("topic", t, "messages", a[0], "msgsPerSec", Format.round(a[0] / elapsed, 2)));
			}
		});
		rows.sort(Comparator.comparingLong((Map<String, Object> m) -> (Long) m.get("messages")).reversed());
		out.put("topicsScanned", topicsScanned);
		out.put("idleTopics", topicsScanned - rows.size());
		out.putAll(Results.page("activeTopics", rows, call.limit()));
		return out;
	}

}
