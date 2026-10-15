package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.GroupSubscribedToTopicException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.Committed;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.OffsetLookup;

/** Geri alınamaz offset/grup araçları: offsets_reset, offsets_delete, delete. */
@Component
public class OffsetTools {

	static final List<String> TARGETS = List.of("earliest", "latest", "offset", "timestamp", "shift");

	/**
	 * reset'in okuma adımları için deadline'dan ayrılan paylar: leader'ı olmayan partition okuma adımlarında
	 * bütün süreyi yiyemez, offset'leri değiştiren son istek için en az ~3 sn kalır.
	 */
	private static final long RESET_DESCRIBE_RESERVE_MS = 10_000;

	private static final long RESET_EARLIEST_RESERVE_MS = 8_000;

	private static final long RESET_LATEST_RESERVE_MS = 6_500;

	private static final long RESET_TIMESTAMP_RESERVE_MS = 5_000;

	private static final long RESET_COMMITTED_RESERVE_MS = 3_500;

	/** Bir partition'ın hedef offset'i (clamp öncesi) ya da atlanma sebebi. */
	record Target(Long offset, String skipped) {

		static Target of(long offset) {
			return new Target(offset, null);
		}

		static Target skip(String reason) {
			return new Target(null, reason);
		}

	}

	public List<ToolSpec> tools() {
		Map<String, Object> offset = P.integer("to=offset için hedef offset");
		offset.put("minimum", 0);
		return List.of(
				ToolSpec
					.destructive("consumer_groups_offsets_reset",
							"Grubun bir topic'teki offset'lerini earliest/latest/offset/timestamp/shift'e taşır; grup Empty olmalı (aktif üye yok)")
					.required("group", Params.group())
					.required("topic", P.string("Topic adı"))
					.param("partitions", Params.intArray("Partition numaraları (boşsa topic'in tüm partition'ları)"))
					.required("to", P.enumOf("Hedef: earliest, latest, offset, timestamp ya da shift", TARGETS))
					.param("offset", offset)
					.param("timestamp", P.string("to=timestamp için zaman (ISO-8601 ya da epoch ms)"))
					.param("shiftBy", P.integer("to=shift için mevcut commit'e eklenecek miktar (negatif olabilir)"))
					.handler(this::reset),
				ToolSpec
					.destructive("consumer_groups_offsets_delete",
							"Grubun bir topic'teki commit edilmiş offset'lerini siler (tüm ya da seçili partition'lar)")
					.required("group", Params.group())
					.required("topic", P.string("Topic adı"))
					.param("partitions", Params.intArray("Partition numaraları (boşsa commit'i olan tüm partition'lar)"))
					.handler(this::deleteOffsets),
				ToolSpec.destructive("consumer_groups_delete", "Consumer group'u siler; grup boş olmalı (aktif üye yok)")
					.required("group", Params.group())
					.handler(this::deleteGroup));
	}

	Object reset(ToolCall call) {
		String group = call.str("group");
		String topic = call.str("topic");
		String to = call.str("to").toLowerCase(Locale.ROOT);
		if (!TARGETS.contains(to)) {
			throw new ToolException("Geçersiz to: " + to + " (geçerli: " + TARGETS + ")");
		}
		Long offset = null;
		Long shiftBy = null;
		Instant timestamp = null;
		switch (to) {
			case "offset" -> {
				offset = call.optLong("offset");
				if (offset == null) {
					throw new ToolException("to=offset için offset parametresi gerekli");
				}
			}
			case "shift" -> {
				shiftBy = call.optLong("shiftBy");
				if (shiftBy == null) {
					throw new ToolException("to=shift için shiftBy parametresi gerekli");
				}
			}
			case "timestamp" -> timestamp = parseTimestamp(call.str("timestamp", null));
			default -> {
			}
		}
		String groupNote = checkResettable(call, group);
		List<TopicPartition> tps = targetPartitions(call, topic);
		OffsetLookup earliest = GroupLag.offsets(call, tps, OffsetSpec.earliest(), RESET_EARLIEST_RESERVE_MS);
		OffsetLookup latest = GroupLag.endOffsets(call, tps, RESET_LATEST_RESERVE_MS);
		OffsetLookup byTime = timestamp == null ? null
				: GroupLag.offsets(call, tps, OffsetSpec.forTimestamp(timestamp.toEpochMilli()),
						RESET_TIMESTAMP_RESERVE_MS);
		Committed committed = GroupLag.committed(call, List.of(group), topic::equals, RESET_COMMITTED_RESERVE_MS)
			.get(group);
		if (committed.error() != null && !committed.notFound()) {
			throw new ToolException("'" + group + "' grubunun mevcut offset'leri okunamadı; offset'ler değiştirilmedi: "
					+ committed.error());
		}
		Map<TopicPartition, Long> before = committed.error() == null ? committed.offsets() : Map.of();

		Map<TopicPartition, OffsetAndMetadata> targets = new LinkedHashMap<>();
		Map<Integer, Map<String, Object>> rows = new TreeMap<>();
		for (TopicPartition tp : tps) {
			Long lo = earliest.offsets().get(tp);
			Long hi = latest.offsets().get(tp);
			Long current = before.get(tp);
			if (lo == null || hi == null) {
				String err = lo == null ? earliest.errors().get(tp) : latest.errors().get(tp);
				rows.put(tp.partition(), Results.map("partition", tp.partition(), "before", current, "skipped",
						"Offset aralığı alınamadı: " + err));
				continue;
			}
			Target target = target(to, tp, lo, hi, current, offset, shiftBy, byTime);
			if (target.skipped() != null) {
				rows.put(tp.partition(),
						Results.map("partition", tp.partition(), "before", current, "skipped", target.skipped()));
				continue;
			}
			long clamped = Math.max(lo, Math.min(hi, target.offset()));
			targets.put(tp, new OffsetAndMetadata(clamped));
			rows.put(tp.partition(), Results.map("partition", tp.partition(), "before", current, "after", clamped,
					"earliest", lo, "latest", hi, "clamped", clamped != target.offset() ? Boolean.TRUE : null));
		}
		if (targets.isEmpty()) {
			throw new ToolException("Hiçbir partition için hedef offset hesaplanamadı: " + rows.values());
		}
		AlterConsumerGroupOffsetsResult r = call.admin().alterConsumerGroupOffsets(group, targets);
		Settled<Void> all = Settled.of(call, r.all());
		int changed = targets.size();
		if (!all.ok()) {
			changed = 0;
			Throwable first = null;
			for (TopicPartition tp : targets.keySet()) {
				Settled<Void> s = Settled.of(call, r.partitionResult(tp));
				if (s.ok()) {
					changed++;
				}
				else {
					first = first == null ? s.error() : first;
					Map<String, Object> row = rows.get(tp.partition());
					row.remove("after");
					row.put("error", resetError(group, s.error()));
				}
			}
			if (changed == 0) {
				Throwable e = first == null ? all.error() : first;
				throw new ToolException("Offset'ler değiştirilemedi: " + resetError(group, e), e);
			}
		}
		return Results.map("group", group, "topic", topic, "to", to, "timestamp",
				timestamp == null ? null : timestamp.toString(), "groupState", groupNote,
				"note",
				groupNote == null ? "Grup mevcut değildi; offset commit'i ile yeni (üyesiz) grup oluşturuldu" : null,
				"changedPartitions", changed, "partitions", new ArrayList<>(rows.values()));
	}

	/**
	 * Partition'ın hedef offset'i (clamp öncesi). timestamp: o andan sonra kayıt yoksa (offset -1) latest;
	 * timestamp sorgusu o partition için başarısız olduysa atlanır (asla sessizce latest'e taşınmaz).
	 */
	static Target target(String to, TopicPartition tp, long earliest, long latest, Long current, Long offset,
			Long shiftBy, OffsetLookup byTime) {
		return switch (to) {
			case "earliest" -> Target.of(earliest);
			case "latest" -> Target.of(latest);
			case "offset" -> Target.of(offset);
			case "timestamp" -> {
				Long t = byTime.offsets().get(tp);
				if (t == null) {
					String err = byTime.errors().get(tp);
					yield Target.skip("Timestamp offset'i alınamadı: " + (err == null ? "yanıt yok" : err));
				}
				yield Target.of(t < 0 ? latest : t);
			}
			default -> current == null ? Target.skip("Commit edilmiş offset yok; shift uygulanamaz")
					: Target.of(current + shiftBy);
		};
	}

	/** Grubun offset'i sıfırlanabilir mi; değilse ToolException. Grup state'ini, grup yoksa null döner. */
	private static String checkResettable(ToolCall call, String group) {
		Settled<ConsumerGroupDescription> s = ConsumerGroups.describe(call, List.of(group), RESET_DESCRIBE_RESERVE_MS)
			.get(group);
		if (!s.ok()) {
			if (ConsumerGroups.notFound(s.error())) {
				return null;
			}
			throw new ToolException(s.errorMessage(), s.error());
		}
		ConsumerGroupDescription d = s.value();
		String state = ConsumerGroups.state(d.groupState());
		if (!d.members().isEmpty()) {
			throw new ToolException("'" + group + "' grubunda " + d.members().size() + " aktif üye var (state=" + state
					+ "); offset sıfırlamak için grup Empty olmalı. Önce consumer'ları durdurun.");
		}
		GroupState gs = d.groupState();
		if (gs == GroupState.EMPTY) {
			return state;
		}
		if (ConsumerGroups.deadOrUnknown(d)) {
			return null;
		}
		throw new ToolException("'" + group + "' grubu " + state
				+ " durumunda; offset sıfırlamak için grup Empty olmalı (aktif üye olmamalı).");
	}

	/** partitions parametresi ya da topic'in tüm partition'ları (tek describeTopics). */
	private static List<TopicPartition> targetPartitions(ToolCall call, String topic) {
		TopicDescription td = call.await(call.admin().describeTopics(List.of(topic)).topicNameValues().get(topic));
		Set<Integer> existing = new TreeSet<>();
		for (TopicPartitionInfo p : td.partitions()) {
			existing.add(p.partition());
		}
		List<Integer> requested = Params.partitions(call);
		if (requested.isEmpty()) {
			requested = new ArrayList<>(existing);
		}
		List<Integer> missing = requested.stream().filter(p -> !existing.contains(p)).toList();
		if (!missing.isEmpty()) {
			throw new ToolException("'" + topic + "' topic'inde olmayan partition: " + missing + " (partition sayısı "
					+ existing.size() + ")");
		}
		return requested.stream().map(p -> new TopicPartition(topic, p)).toList();
	}

	private static String resetError(String group, Throwable e) {
		Throwable root = KafkaErrors.unwrap(e);
		if (root instanceof UnknownMemberIdException || root instanceof RebalanceInProgressException
				|| root instanceof FencedInstanceIdException) {
			return "'" + group + "' grubuna arada aktif üye katıldı; grup Empty olmalı (" + KafkaErrors.explain(root)
					+ ")";
		}
		return KafkaErrors.explain(root);
	}

	static Instant parseTimestamp(String value) {
		if (value == null) {
			throw new ToolException("to=timestamp için timestamp parametresi gerekli (ISO-8601 ya da epoch ms)");
		}
		String v = value.trim();
		if (v.matches("-?\\d+")) {
			return Instant.ofEpochMilli(Long.parseLong(v));
		}
		try {
			return Instant.parse(v);
		}
		catch (DateTimeParseException ignored) {
			// diğer biçimler
		}
		try {
			return OffsetDateTime.parse(v).toInstant();
		}
		catch (DateTimeParseException ignored) {
			// diğer biçimler
		}
		try {
			return ZonedDateTime.parse(v).toInstant();
		}
		catch (DateTimeParseException ignored) {
			// diğer biçimler
		}
		try {
			return LocalDateTime.parse(v).toInstant(ZoneOffset.UTC);
		}
		catch (DateTimeParseException ignored) {
			// diğer biçimler
		}
		try {
			return LocalDate.parse(v).atStartOfDay().toInstant(ZoneOffset.UTC);
		}
		catch (DateTimeParseException e) {
			throw new ToolException("Geçersiz timestamp: " + v
					+ " (ISO-8601, ör. 2024-05-01T10:00:00Z / 2024-05-01T13:00:00+03:00, ya da epoch ms)");
		}
	}

	Object deleteOffsets(ToolCall call) {
		String group = call.str("group");
		String topic = call.str("topic");
		Committed committed = GroupLag.committed(call, List.of(group), topic::equals).get(group);
		if (committed.error() != null) {
			throw committed.notFound() ? ConsumerGroups.notFoundError(group) : new ToolException(committed.error());
		}
		// Sadece commit'i olan partition'lar silinir: broker commit'i olmayan (ya da olmayan) partition için de
		// hatasız döner; bunlar "silindi" diye raporlanmasın.
		Set<TopicPartition> tps = new TreeSet<>(
				Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
		List<Integer> requested = Params.partitions(call);
		Map<Integer, Map<String, Object>> rows = new TreeMap<>();
		if (requested.isEmpty()) {
			tps.addAll(committed.offsets().keySet());
		}
		else {
			for (int p : requested) {
				TopicPartition tp = new TopicPartition(topic, p);
				if (committed.offsets().containsKey(tp)) {
					tps.add(tp);
				}
				else {
					rows.put(p, Results.map("partition", p, "deleted", false, "note",
							"Commit edilmiş offset yoktu (silinecek bir şey yok)"));
				}
			}
		}
		if (tps.isEmpty()) {
			throw new ToolException("'" + group + "' grubunun '" + topic + "' topic'inde"
					+ (requested.isEmpty() ? "" : " istenen partition'larda " + requested)
					+ " commit edilmiş offset'i yok");
		}
		DeleteConsumerGroupOffsetsResult r = call.admin().deleteConsumerGroupOffsets(group, tps);
		Settled<Void> all = Settled.of(call, r.all());
		int deleted = 0;
		Throwable first = null;
		for (TopicPartition tp : tps) {
			Settled<Void> s = all.ok() ? all : Settled.of(call, r.partitionResult(tp));
			Long before = committed.offsets().get(tp);
			if (s.ok()) {
				deleted++;
				rows.put(tp.partition(),
						Results.map("partition", tp.partition(), "committedBefore", before, "deleted", true));
			}
			else {
				first = first == null ? s.error() : first;
				rows.put(tp.partition(), Results.map("partition", tp.partition(), "committedBefore", before, "deleted",
						false, "error", deleteOffsetsError(group, topic, s.error())));
			}
		}
		if (deleted == 0) {
			Throwable e = first == null ? all.error() : first;
			if (ConsumerGroups.notFound(e)) {
				throw ConsumerGroups.notFoundError(group);
			}
			throw new ToolException("Offset'ler silinemedi: " + deleteOffsetsError(group, topic, e), e);
		}
		int withoutCommit = rows.size() - tps.size();
		return Results.map("group", group, "topic", topic, "deletedPartitions", deleted, "failedPartitions",
				tps.size() - deleted, "partitionsWithoutCommit", withoutCommit > 0 ? withoutCommit : null,
				"partitions", new ArrayList<>(rows.values()));
	}

	private static String deleteOffsetsError(String group, String topic, Throwable e) {
		Throwable root = KafkaErrors.unwrap(e);
		if (root instanceof GroupSubscribedToTopicException) {
			return "'" + group + "' grubu '" + topic + "' topic'ine hâlâ abone (aktif üyeler bu topic'i tüketiyor); "
					+ "Kafka silmeyi reddetti. Önce consumer'ları durdurun";
		}
		return KafkaErrors.explain(root);
	}

	Object deleteGroup(ToolCall call) {
		String group = call.str("group");
		Committed committed = GroupLag.committed(call, List.of(group), null).get(group);
		Settled<Void> s = Settled.of(call, call.admin().deleteConsumerGroups(List.of(group)).deletedGroups().get(group));
		if (!s.ok()) {
			Throwable root = KafkaErrors.unwrap(s.error());
			if (ConsumerGroups.notFound(root)) {
				throw ConsumerGroups.notFoundError(group);
			}
			if (root instanceof GroupNotEmptyException) {
				Settled<ConsumerGroupDescription> d = ConsumerGroups.describe(call, List.of(group)).get(group);
				String detail = d != null && d.ok() ? " (state=" + ConsumerGroups.state(d.value().groupState())
						+ ", üye=" + d.value().members().size() + ")" : "";
				throw new ToolException("'" + group + "' grubu silinemedi: grup boş değil, aktif üyeleri var" + detail
						+ ". Önce consumer'ları durdurun; grup Empty olunca tekrar deneyin.", root);
			}
			throw new ToolException("'" + group + "' grubu silinemedi: " + KafkaErrors.explain(root), root);
		}
		Map<String, Object> offsets = null;
		if (committed != null && committed.error() == null) {
			offsets = Results.map("topics", new ArrayList<>(ConsumerGroups.byTopic(committed.offsets().keySet()).keySet()),
					"partitions", committed.offsets().size());
		}
		return Results.map("group", group, "deleted", true, "deletedOffsets", offsets);
	}

}
