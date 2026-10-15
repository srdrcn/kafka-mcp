package io.github.srdrcn.kafkamcp.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta.Mode;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** KRaft/ZooKeeper mod tespiti ve moda özgü alanların (ELR, topic id, desteklenmeyen API) saf mantığı. */
class ClusterModeTest {

	private static final Map<String, Short> KRAFT_FEATURES = Map.of("metadata.version", (short) 27, "group.version",
			(short) 1);

	@Test
	void modeFromFinalizedFeatures() {
		assertThat(ClusterMeta.modeOf(KRAFT_FEATURES)).isEqualTo(Mode.KRAFT);
		// ZooKeeper broker'ları metadata.version finalize etmez (describeFeatures boş döner)
		assertThat(ClusterMeta.modeOf(Map.of())).isEqualTo(Mode.ZOOKEEPER);
		assertThat(ClusterMeta.modeOf((Map<String, ?>) null)).isEqualTo(Mode.UNKNOWN);
	}

	@Test
	void resolveModeFallsBackToFeaturesWhenQuorumDeniedOrUnsupported() {
		// quorum yanıtı: KRaft
		assertThat(ClusterMeta.resolveMode(true, false, null)).isEqualTo(Mode.KRAFT);
		// quorum yetkisi yok / zaman aşımı: feature'lar karar verir
		assertThat(ClusterMeta.resolveMode(false, false, KRAFT_FEATURES)).isEqualTo(Mode.KRAFT);
		assertThat(ClusterMeta.resolveMode(false, false, Map.of())).isEqualTo(Mode.ZOOKEEPER);
		// DescribeQuorum desteklenmiyor: ZooKeeper; ama metadata.version varsa DescribeQuorum'u bilmeyen eski KRaft
		assertThat(ClusterMeta.resolveMode(false, true, Map.of())).isEqualTo(Mode.ZOOKEEPER);
		assertThat(ClusterMeta.resolveMode(false, true, null)).isEqualTo(Mode.ZOOKEEPER);
		assertThat(ClusterMeta.resolveMode(false, true, KRAFT_FEATURES)).isEqualTo(Mode.KRAFT);
		// ikisi de yanıtsız
		assertThat(ClusterMeta.resolveMode(false, false, null)).isEqualTo(Mode.UNKNOWN);
	}

	@Test
	void modeLabelsAndInfoAccessors() {
		assertThat(Mode.of(ClusterMeta.KRAFT)).isEqualTo(Mode.KRAFT);
		assertThat(Mode.of(ClusterMeta.ZOOKEEPER)).isEqualTo(Mode.ZOOKEEPER);
		assertThat(Mode.of(null)).isEqualTo(Mode.UNKNOWN);
		assertThat(Mode.UNKNOWN.label()).isEqualTo("unknown");
		assertThat(Mode.KRAFT.toString()).isEqualTo("KRaft");

		Node b1 = new Node(1, "h1", 9092);
		Node b2 = new Node(2, "h2", 9092);
		// KRaft, ayrı controller düğümü (3000): varsayılan broker en küçük id
		ClusterMeta.Info dedicated = new ClusterMeta.Info("c", List.of(b1, b2), 3000, ClusterMeta.KRAFT, null, null,
				KRAFT_FEATURES);
		assertThat(dedicated.controllerIsBroker()).isFalse();
		assertThat(dedicated.defaultBrokerId()).isEqualTo(1);
		assertThat(dedicated.metadataVersionLevel()).isEqualTo((short) 27);
		assertThat(dedicated.clusterMode()).isEqualTo(Mode.KRAFT);
		// ZooKeeper: controller broker'dır; eski 6 parametreli kurucu feature'sız
		ClusterMeta.Info zk = new ClusterMeta.Info("c", List.of(b1, b2), 2, ClusterMeta.ZOOKEEPER, null, null);
		assertThat(zk.controllerIsBroker()).isTrue();
		assertThat(zk.defaultBrokerId()).isEqualTo(2);
		assertThat(zk.metadataVersionLevel()).isNull();
		assertThat(new ClusterMeta.Info("c", List.of(b1), null, null, null, "x").clusterMode()).isEqualTo(Mode.UNKNOWN);
		// eski kurucular geçiş bilgisi taşımaz (bilinmiyor)
		assertThat(zk.zkMigration()).isNull();
		assertThat(zk.migration()).isFalse();
		assertThat(dedicated.migration()).isFalse();
		assertThat(new ClusterMeta.Info("c", List.of(b1), null, ClusterMeta.ZOOKEEPER, null, null, Map.of(), true)
			.migration()).isTrue();
	}

	@Test
	void migrationFlagFromBrokerConfig() {
		assertThat(ClusterMeta.migrationEnabled(null)).isFalse();
		assertThat(ClusterMeta.migrationEnabled(new Config(List.of()))).isFalse();
		assertThat(ClusterMeta.migrationEnabled(new Config(List.of(new ConfigEntry(ClusterMeta.MIGRATION_ENABLE, "false")))))
			.isFalse();
		assertThat(ClusterMeta.migrationEnabled(new Config(List.of(new ConfigEntry(ClusterMeta.MIGRATION_ENABLE, "true")))))
			.isTrue();
	}

	@Test
	void elrIsNullSafeForZooKeeperPartitions() {
		Node b1 = new Node(1, "h1", 9092);
		Node b2 = new Node(2, "h2", 9092);
		// ZooKeeper (Metadata API): client ELR alanlarını null bırakır
		TopicPartitionInfo zk = new TopicPartitionInfo(0, b1, List.of(b1, b2), List.of(b1));
		assertThat(zk.elr()).isNull();
		assertThat(Topics.elr(zk)).isNull();
		assertThat(Topics.lastKnownElr(zk)).isNull();
		// KRaft: ISR min.insync altına düşünce ELR dolu olabilir
		TopicPartitionInfo kraft = new TopicPartitionInfo(0, b1, List.of(b1, b2), List.of(b1), List.of(b2), List.of());
		assertThat(Topics.elr(kraft)).containsExactly(2);
		assertThat(Topics.lastKnownElr(kraft)).isEmpty();
	}

	@Test
	void topicIdHidesZeroUuidOfOldBrokers() {
		TopicDescription old = new TopicDescription("t", false, List.of(), java.util.Set.of(), Uuid.ZERO_UUID);
		assertThat(Topics.topicId(old)).isNull();
		Uuid id = Uuid.randomUuid();
		assertThat(Topics.topicId(new TopicDescription("t", false, List.of(), java.util.Set.of(), id)))
			.isEqualTo(id.toString());
	}

	@Test
	void unsupportedVersionIsDetectedThroughWrappersAndMessages() {
		UnsupportedVersionException uve = new UnsupportedVersionException("The node does not support DESCRIBE_QUORUM");
		assertThat(KafkaErrors.isUnsupported(uve)).isTrue();
		assertThat(KafkaErrors.isUnsupported(new ExecutionException(uve))).isTrue();
		assertThat(KafkaErrors.isUnsupported(new ToolException(KafkaErrors.explain(uve), uve))).isTrue();
		assertThat(KafkaErrors.isUnsupported(new ToolException(KafkaErrors.explain(uve)))).isTrue();
		assertThat(KafkaErrors.explain(uve)).startsWith(KafkaErrors.UNSUPPORTED);
		assertThat(KafkaErrors.isUnsupported(KafkaErrors.explain(uve))).isTrue();
		ClusterAuthorizationException denied = new ClusterAuthorizationException("denied");
		assertThat(KafkaErrors.isUnsupported(denied)).isFalse();
		assertThat(KafkaErrors.isUnsupported(new ToolException(KafkaErrors.explain(denied), denied))).isFalse();
		assertThat(KafkaErrors.isUnsupported((Throwable) null)).isFalse();
		assertThat(KafkaErrors.isUnsupported((String) null)).isFalse();
	}

}
