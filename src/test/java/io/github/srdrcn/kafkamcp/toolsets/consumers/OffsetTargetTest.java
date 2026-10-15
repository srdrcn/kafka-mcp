package io.github.srdrcn.kafkamcp.toolsets.consumers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.toolsets.consumers.GroupLag.OffsetLookup;
import io.github.srdrcn.kafkamcp.toolsets.consumers.OffsetTools.Target;

/** consumer_groups_offsets_reset'in partition bazında hedef offset hesabı (Kafka'sız). */
class OffsetTargetTest {

	private static final TopicPartition P0 = new TopicPartition("t", 0);

	private static final TopicPartition P1 = new TopicPartition("t", 1);

	private static final TopicPartition P2 = new TopicPartition("t", 2);

	@Test
	void timestampLookupFailureSkipsPartitionInsteadOfMovingToLatest() {
		OffsetLookup byTime = new OffsetLookup(Map.of(P0, 4L, P1, -1L), Map.of(P2, "Topic ya da partition bulunamadı"),
				0);
		assertThat(OffsetTools.target("timestamp", P0, 0, 10, 7L, null, null, byTime)).isEqualTo(Target.of(4));
		assertThat(OffsetTools.target("timestamp", P1, 0, 10, 7L, null, null, byTime))
			.as("timestamp'ten sonra kayıt yok → latest")
			.isEqualTo(Target.of(10));
		Target failed = OffsetTools.target("timestamp", P2, 0, 10, 7L, null, null, byTime);
		assertThat(failed.offset()).isNull();
		assertThat(failed.skipped()).contains("Timestamp offset'i alınamadı").contains("bulunamadı");
	}

	@Test
	void otherTargets() {
		OffsetLookup none = new OffsetLookup(Map.of(), Map.of(), 0);
		assertThat(OffsetTools.target("earliest", P0, 3, 10, 7L, null, null, none)).isEqualTo(Target.of(3));
		assertThat(OffsetTools.target("latest", P0, 3, 10, 7L, null, null, none)).isEqualTo(Target.of(10));
		assertThat(OffsetTools.target("offset", P0, 3, 10, 7L, 5L, null, none)).isEqualTo(Target.of(5));
		assertThat(OffsetTools.target("shift", P0, 3, 10, 7L, null, -2L, none)).isEqualTo(Target.of(5));
		assertThat(OffsetTools.target("shift", P0, 3, 10, null, null, -2L, none).skipped()).contains("shift");
	}

}
