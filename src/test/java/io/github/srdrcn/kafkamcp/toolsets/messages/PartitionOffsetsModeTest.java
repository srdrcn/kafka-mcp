package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Mod farkları (ağa çıkmadan): ZooKeeper broker'larında ELR yok (null), KRaft'ta liste (boş olabilir);
 * eski broker'ın desteklemediği zaman araması partition başına not olur, diğer hatalar aracı düşürür.
 */
class PartitionOffsetsModeTest {

	private static final Node N1 = new Node(1, "b1", 9092);

	private static final Node N2 = new Node(2, "b2", 9092);

	private static final Node N3 = new Node(3, "b3", 9092);

	private static TopicDescription topic(TopicPartitionInfo... partitions) {
		return new TopicDescription("t", false, List.of(partitions));
	}

	@Test
	void zookeeperOfflinePartitionHasNullElrAndNoKraftHint() {
		// ZooKeeper (Metadata API): 4 argümanlı TopicPartitionInfo, elr/lastKnownElr null
		TopicPartitionInfo offline = new TopicPartitionInfo(0, null, List.of(N1), List.of());
		assertThat(offline.elr()).isNull();
		assertThat(offline.lastKnownElr()).isNull();
		TopicDescription zk = topic(offline, new TopicPartitionInfo(1, N1, List.of(N1), List.of(N1)));
		assertThatThrownBy(() -> PartitionOffsets.select(zk, 0)).isInstanceOf(ToolException.class)
			.hasMessageContaining("Partition 0 offline")
			.hasMessageNotContaining("ELR")
			.hasMessageNotContaining("KRaft");
		PartitionOffsets.Selection all = PartitionOffsets.select(zk, null);
		assertThat(all.offline()).containsExactly(0);
		assertThat(all.online()).containsExactly(new TopicPartition("t", 1));
	}

	@Test
	void kraftOfflinePartitionReportsElrMembers() {
		TopicDescription kraft = topic(
				new TopicPartitionInfo(0, null, List.of(N1, N2, N3), List.of(), List.of(N2), List.of(N3)));
		assertThatThrownBy(() -> PartitionOffsets.select(kraft, 0)).isInstanceOf(ToolException.class)
			.hasMessageContaining("Partition 0 offline")
			.hasMessageContaining("KRaft")
			.hasMessageContaining("ELR: [2]")
			.hasMessageContaining("son bilinen ELR: [3]");
		// ELR açık ama boş: ek bilgi yok
		TopicDescription empty = topic(new TopicPartitionInfo(0, Node.noNode(), List.of(N1), List.of(), List.of(),
				List.of()));
		assertThatThrownBy(() -> PartitionOffsets.select(empty, 0)).hasMessageNotContaining("ELR");
	}

	private static KafkaFuture<ListOffsetsResultInfo> failed(RuntimeException e) {
		KafkaFutureImpl<ListOffsetsResultInfo> f = new KafkaFutureImpl<>();
		f.completeExceptionally(e);
		return f;
	}

	private static ToolCall call() {
		return new ToolCall("test", Map.of(), null, Instant.now().plusSeconds(5));
	}

	@Test
	void unsupportedTimestampLookupBecomesPerPartitionReason() {
		TopicPartition p0 = new TopicPartition("t", 0);
		TopicPartition p1 = new TopicPartition("t", 1);
		TopicPartition p2 = new TopicPartition("t", 2);
		Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new LinkedHashMap<>();
		futures.put(p0, KafkaFuture.completedFuture(new ListOffsetsResultInfo(7, 1_000, Optional.empty())));
		futures.put(p1, failed(new UnsupportedForMessageFormatException("v0")));
		futures.put(p2, failed(new UnsupportedVersionException("old")));
		PartitionOffsets.Lookup l = PartitionOffsets.awaitEach(call(), List.of(p0, p1, p2),
				new ListOffsetsResult(futures));
		assertThat(l.offsets()).containsOnlyKeys(p0);
		assertThat(l.offsets().get(p0).offset()).isEqualTo(7);
		assertThat(l.unsupported().get(p1)).contains("desteklemiyor").contains("mesaj formatı");
		assertThat(l.unsupported().get(p2)).isEqualTo("broker bu işlemi desteklemiyor (sürüm)");
		assertThat(l.unsupportedByReason().values()).containsExactly(List.of(1), List.of(2));

		// yetki gibi diğer hatalar eskisi gibi tüm aracı Türkçe hatayla düşürür
		Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> denied = Map.of(p0,
				failed(new TopicAuthorizationException("denied")));
		assertThatThrownBy(() -> PartitionOffsets.awaitEach(call(), List.of(p0), new ListOffsetsResult(denied)))
			.isInstanceOf(ToolException.class)
			.hasMessageContaining("Topic yetkisi yok");
	}

}
