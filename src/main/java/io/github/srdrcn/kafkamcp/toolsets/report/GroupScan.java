package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.toolsets.consumers.ConsumerGroups;
import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag;

/**
 * Consumer group'ların toplu taraması: tek listGroups ve en fazla {@code maxGroups} grup için tek
 * listConsumerGroupOffsets (grubun tüm partition'ları). Sınır aşılırsa önce aktif (Stable/rebalance), sonra
 * Empty gruplar seçilir; atlananlar sayılır ({@link #skipped()}).
 */
public final class GroupScan {

	/** Commit'li offset'leri okunan en fazla grup sayısı (toplu tek istek). */
	public static final int MAX_GROUPS = 1000;

	private final List<ConsumerGroups.GroupSummary> listed;

	private final List<String> listErrors;

	private final Map<String, GroupLag.Committed> committed;

	private final int skipped;

	GroupScan(List<ConsumerGroups.GroupSummary> listed, List<String> listErrors,
			Map<String, GroupLag.Committed> committed, int skipped) {
		this.listed = listed;
		this.listErrors = listErrors;
		this.committed = committed;
		this.skipped = skipped;
	}

	/**
	 * Grupları listeler ve seçilenlerin commit'li offset'lerini okur. İki adım da deadline'dan
	 * {@code reserveMillis} pay bırakır; yanıt vermeyen gruplar {@link GroupLag.Committed#timedOut()} olur.
	 */
	public static GroupScan run(ToolCall call, int maxGroups, long reserveMillis) {
		ConsumerGroups.GroupList list = ConsumerGroups.list(call, reserveMillis + 1_000);
		List<ConsumerGroups.GroupSummary> ordered = new ArrayList<>(list.groups());
		ordered.sort(Comparator.comparingInt((ConsumerGroups.GroupSummary g) -> priority(g.state()))
			.thenComparing(ConsumerGroups.GroupSummary::groupId));
		List<String> selected = new ArrayList<>();
		for (ConsumerGroups.GroupSummary g : ordered) {
			if (selected.size() >= maxGroups) {
				break;
			}
			selected.add(g.groupId());
		}
		Map<String, GroupLag.Committed> committed = GroupLag.committed(call, selected, null, reserveMillis);
		return new GroupScan(list.groups(), list.errors(), committed, ordered.size() - selected.size());
	}

	/** Örnekleme önceliği: aktif gruplar önce, sonra Empty, en son Dead/bilinmeyen. */
	static int priority(String state) {
		if (state == null) {
			return 3;
		}
		return switch (state) {
			case "Stable" -> 0;
			case "PreparingRebalance", "CompletingRebalance", "Assigning", "Reconciling" -> 1;
			case "Empty" -> 2;
			default -> 3;
		};
	}

	/** Listelenen tüm gruplar (groupId sırasıyla). */
	public List<ConsumerGroups.GroupSummary> listed() {
		return listed;
	}

	/** listGroups'ta hata veren broker'ların mesajları (doluysa liste eksik olabilir). */
	public List<String> listErrors() {
		return listErrors;
	}

	/** Offset'i istenen gruplar ve sonuçları (grup bazında hata/timedOut dahil). */
	public Map<String, GroupLag.Committed> committed() {
		return committed;
	}

	/** Sınır nedeniyle offset'i okunmayan grup sayısı. */
	public int skipped() {
		return skipped;
	}

	/** Grup id → durum (Stable, Empty, ...; bilinmiyorsa null değer). */
	public Map<String, String> states() {
		Map<String, String> out = new LinkedHashMap<>();
		listed.forEach(g -> out.put(g.groupId(), g.state()));
		return out;
	}

	/** Offset'leri yanıtsız kalan grup sayısı. */
	public int timedOut() {
		return (int) committed.values().stream().filter(GroupLag.Committed::timedOut).count();
	}

	/** Offset'leri hata veren (yanıtsız ya da arada silinmiş olmayan) grup sayısı. */
	public int failed() {
		return (int) committed.values()
			.stream()
			.filter(c -> c.error() != null && !c.timedOut() && !c.notFound())
			.count();
	}

	/** Tüm gruplar listelendi ve hepsinin offset'i okundu mu? */
	public boolean complete() {
		return skipped == 0 && listErrors.isEmpty() && timedOut() == 0 && failed() == 0;
	}

	/** Grup → commit'li offset'ler (sadece başarılı okunanlar; offset'siz partition'lar hariç). */
	public Map<String, Map<TopicPartition, Long>> offsets() {
		Map<String, Map<TopicPartition, Long>> out = new TreeMap<>();
		committed.forEach((g, c) -> {
			if (c.error() == null) {
				out.put(g, c.offsets());
			}
		});
		return out;
	}

	/** Topic → o topic'te commit'li offset'i olan gruplar (sıralı). */
	public Map<String, Set<String>> groupsByTopic() {
		Map<String, Set<String>> out = new TreeMap<>();
		offsets().forEach((g, offsets) -> {
			for (TopicPartition tp : offsets.keySet()) {
				out.computeIfAbsent(tp.topic(), k -> new TreeSet<>()).add(g);
			}
		});
		return out;
	}

	/** Commit'li offset'lerin dokunduğu tüm partition'lar. */
	public Set<TopicPartition> committedPartitions() {
		Set<TopicPartition> out = new java.util.HashSet<>();
		offsets().values().forEach(m -> out.addAll(m.keySet()));
		return out;
	}

	/** Kapsam bilgisini ekler: incelenen/atlanan grup sayıları, örnekleme ve hatalar. */
	public void describe(Coverage coverage) {
		coverage.examined("groups", committed.size());
		coverage.skipped("groupsOverLimit", skipped);
		if (skipped > 0) {
			coverage.sampled();
			coverage.note("Grup sayısı " + listed.size() + " > " + committed.size()
					+ ": sadece öncelikli (aktif, sonra Empty) gruplar incelendi");
		}
		int timedOut = timedOut();
		if (timedOut > 0) {
			coverage.partial();
			coverage.skipped("groupsNoResponse", timedOut);
		}
		coverage.skipped("groupsFailed", failed());
		if (!listErrors.isEmpty()) {
			coverage.partial();
			coverage.error("listGroups", listErrors);
		}
	}

}
