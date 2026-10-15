package io.github.srdrcn.kafkamcp.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Consumer group ve lag yardımcıları. Çok grup için tek listConsumerGroupOffsets / describeConsumerGroups
 * gönderilir; grup bazındaki hatalar aracı düşürmez.
 */
public final class ConsumerGroups {

	private ConsumerGroups() {
	}

	/** Bir grubun bir topic'teki özeti: durum, üye sayısı, toplam lag, commit'li partition sayısı. */
	public record TopicLag(String groupId, String state, Integer members, long lag, int partitions) {
	}

	/**
	 * {@link #forTopic} sonucu: gruplar (lag'e göre azalan), taranan grup sayısı, hatalar (grup id ya da
	 * listGroups#n anahtarıyla) ve latest offset'i alınamadığı için lag'e katılamayan partition sayısı.
	 */
	public record TopicConsumers(List<TopicLag> groups, int groupsScanned, Map<String, String> errors,
			int lagUnknownPartitions) {
	}

	/**
	 * {@link #forTopic}'te commit'li offset beklemesine ayrılan kalan süre payı; yüklenen/erişilemeyen bir group
	 * coordinator'ı tüm süreyi tüketip diğer grupların sonucunu düşürmesin.
	 */
	static final double COMMITTED_WAIT_SHARE = 0.6;

	/** Grup listesi ve yanıt vermeyen broker'ların hata mesajları. */
	public record GroupList(Map<String, GroupListing> groups, List<String> errors) {
	}

	/**
	 * Consumer group'lar (classic + consumer protokolü; share/streams hariç), id'ye göre sıralı. Bazı
	 * broker'lar hata verirse diğerlerinden gelenler döner, hatalar ayrıca raporlanır; hiçbiri yanıt
	 * vermezse hata fırlatılır.
	 */
	public static GroupList list(ToolCall call) {
		ListGroupsResult r = call.admin().listGroups();
		Collection<GroupListing> valid = call.await(r.valid());
		List<String> errors = call.await(r.errors()).stream().map(KafkaErrors::explain).toList();
		if (valid.isEmpty() && !errors.isEmpty()) {
			throw new ToolException(errors.get(0));
		}
		Map<String, GroupListing> out = new TreeMap<>();
		for (GroupListing g : valid) {
			GroupType type = g.type().orElse(GroupType.UNKNOWN);
			if (type == GroupType.SHARE || type == GroupType.STREAMS) {
				continue;
			}
			String protocol = g.protocol() == null ? "" : g.protocol();
			if (protocol.isEmpty() || "consumer".equals(protocol)) {
				out.put(g.groupId(), g);
			}
		}
		return new GroupList(out, errors);
	}

	/**
	 * Grupların commit'li offset'leri (tek listConsumerGroupOffsets). {@code tps} boşsa grubun tüm
	 * partition'ları; commit'i olmayan partition'lar sonuçta yoktur.
	 */
	public static Futures.Each<String, Map<TopicPartition, Long>> committed(ToolCall call, Collection<String> groupIds,
			Collection<TopicPartition> tps) {
		return committed(call, groupIds, tps, Long.MAX_VALUE);
	}

	/** {@link #committed(ToolCall, Collection, Collection)}; en fazla {@code maxMillis} bekler, yanıt vermeyen gruplar hatada. */
	public static Futures.Each<String, Map<TopicPartition, Long>> committed(ToolCall call, Collection<String> groupIds,
			Collection<TopicPartition> tps, long maxMillis) {
		Map<String, ListConsumerGroupOffsetsSpec> specs = new LinkedHashMap<>();
		for (String g : groupIds) {
			ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec();
			if (tps != null && !tps.isEmpty()) {
				spec.topicPartitions(tps);
			}
			specs.put(g, spec);
		}
		if (specs.isEmpty()) {
			return new Futures.Each<>(new LinkedHashMap<>(), new LinkedHashMap<>());
		}
		ListConsumerGroupOffsetsResult r = call.admin().listConsumerGroupOffsets(specs);
		Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = new LinkedHashMap<>();
		for (String g : specs.keySet()) {
			futures.put(g, r.partitionsToOffsetAndMetadata(g));
		}
		Futures.Each<String, Map<TopicPartition, OffsetAndMetadata>> raw = Futures.awaitEach(call, futures, maxMillis);
		Map<String, Map<TopicPartition, Long>> values = new LinkedHashMap<>();
		raw.values().forEach((g, m) -> {
			Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
			if (m != null) {
				m.forEach((tp, om) -> {
					if (om != null && om.offset() >= 0) {
						offsets.put(tp, om.offset());
					}
				});
			}
			values.put(g, offsets);
		});
		return new Futures.Each<>(values, raw.errors());
	}

	/** describeConsumerGroups (tek çağrı); grup bazında hatalar ayrı. */
	public static Futures.Each<String, ConsumerGroupDescription> describe(ToolCall call, Collection<String> groupIds) {
		if (groupIds.isEmpty()) {
			return new Futures.Each<>(new LinkedHashMap<>(), new LinkedHashMap<>());
		}
		return Futures.awaitEach(call, call.admin().describeConsumerGroups(groupIds).describedGroups());
	}

	/** lag = latest - committed (en az 0). */
	public static long lag(long committed, long latest) {
		return Math.max(0, latest - committed);
	}

	/** Partition bazında lag; latest offset'i bilinmeyen partition'lar atlanır. */
	public static Map<TopicPartition, Long> lagByPartition(Map<TopicPartition, Long> committed,
			Map<TopicPartition, Long> latest) {
		Map<TopicPartition, Long> out = new LinkedHashMap<>();
		committed.forEach((tp, c) -> {
			Long l = latest.get(tp);
			if (l != null) {
				out.put(tp, lag(c, l));
			}
		});
		return out;
	}

	/**
	 * Topic'te commit'li offset'i olan consumer group'lar ve bu topic'teki toplam lag'leri. Tek listGroups,
	 * tek listConsumerGroupOffsets (sadece bu topic'in partition'ları), tek listOffsets ve eşleşen gruplar için
	 * tek describeConsumerGroups. latest offset'ler commit'li offset'lerle paralel istenir; commit beklemesi
	 * kalan sürenin bir payıyla sınırlıdır, yanıt vermeyen gruplar hatalarda döner (kısmi sonuç).
	 */
	public static TopicConsumers forTopic(ToolCall call, TopicDescription topic) {
		List<TopicPartition> tps = Topics.partitions(topic);
		List<TopicPartition> online = Topics.onlinePartitions(List.of(topic));
		ListOffsetsResult pendingLatest = Offsets.submit(call, online, OffsetSpec.latest());
		GroupList listing = list(call);
		Map<String, GroupListing> groups = listing.groups();
		Futures.Each<String, Map<TopicPartition, Long>> committed = committed(call, groups.keySet(), tps,
				(long) (call.waitMillis() * COMMITTED_WAIT_SHARE));
		Map<String, Map<TopicPartition, Long>> matching = new LinkedHashMap<>();
		committed.values().forEach((g, offsets) -> {
			if (!offsets.isEmpty()) {
				matching.put(g, offsets);
			}
		});
		Map<String, String> errors = new LinkedHashMap<>(committed.errors());
		for (int i = 0; i < listing.errors().size(); i++) {
			errors.put("listGroups#" + (i + 1), listing.errors().get(i));
		}
		if (matching.isEmpty()) {
			return new TopicConsumers(List.of(), groups.size(), errors, 0);
		}
		Map<TopicPartition, Long> latest = new LinkedHashMap<>();
		Offsets.awaitEach(call, online, pendingLatest).values().forEach((tp, i) -> latest.put(tp, i.offset()));
		Futures.Each<String, ConsumerGroupDescription> described = describe(call, matching.keySet());
		List<TopicLag> out = new ArrayList<>();
		matching.forEach((g, offsets) -> {
			long total = lagByPartition(offsets, latest).values().stream().mapToLong(Long::longValue).sum();
			ConsumerGroupDescription d = described.values().get(g);
			String state = d != null ? String.valueOf(d.groupState())
					: groups.get(g).groupState().map(String::valueOf).orElse(null);
			Integer members = d != null ? d.members().size() : null;
			out.add(new TopicLag(g, state, members, total, offsets.size()));
		});
		out.sort(Comparator.comparingLong(TopicLag::lag).reversed().thenComparing(TopicLag::groupId));
		return new TopicConsumers(out, groups.size(), errors, tps.size() - latest.size());
	}

}
