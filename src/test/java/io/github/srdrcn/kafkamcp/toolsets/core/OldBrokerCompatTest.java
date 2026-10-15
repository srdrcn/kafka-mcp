package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * APIs missing on old (e.g. 2.x) ZooKeeper brokers (describeProducers, listTransactions, ListOffsets
 * maxTimestamp 3.0+; DescribeLogDirs disk size 3.3+; no DescribeQuorum at all): an UnsupportedVersionException
 * does not fail the tool, it becomes a per-item note and returns without waiting. Admin is mocked (no real old
 * broker).
 */
class OldBrokerCompatTest {

	private static final String TOPIC = "orders";

	private final Node broker = new Node(1, "old-broker", 9092);

	private Admin admin;

	private KafkaClients clients;

	@BeforeEach
	void setUp() {
		admin = mock(Admin.class);
		clients = mock(KafkaClients.class);
		when(clients.admin("old")).thenReturn(admin);
		TopicDescription d = new TopicDescription(TOPIC, false,
				List.of(new TopicPartitionInfo(0, broker, List.of(broker), List.of(broker)),
						new TopicPartitionInfo(1, broker, List.of(broker), List.of(broker))));
		DescribeTopicsResult topics = mock(DescribeTopicsResult.class);
		when(topics.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(TOPIC, d)));
		when(admin.describeTopics(anyCollection())).thenReturn(topics);
		DescribeClusterResult cluster = mock(DescribeClusterResult.class);
		when(cluster.clusterId()).thenReturn(KafkaFuture.completedFuture("old-cluster"));
		when(cluster.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(broker)));
		when(cluster.controller()).thenReturn(KafkaFuture.completedFuture(broker));
		when(admin.describeCluster()).thenReturn(cluster);
	}

	@Test
	void topicsProducersReportsUnsupportedPartitionsInsteadOfFailing() {
		DescribeProducersResult producers = mock(DescribeProducersResult.class);
		when(producers.partitionResult(any())).thenAnswer(i -> unsupported());
		when(admin.describeProducers(anyCollection())).thenReturn(producers);

		@SuppressWarnings("unchecked")
		Map<String, Object> r = (Map<String, Object>) new TopicClientTools(null).topicsProducers(call(TOPIC));
		assertThat(r.get("producerCount")).isEqualTo(0);
		assertThat(r.get("unsupportedPartitions")).isEqualTo(2);
		assertThat(r).doesNotContainKey("failedPartitions");
		assertThat((String) r.get("note")).startsWith(KafkaErrors.UNSUPPORTED).contains("3.0");
		assertThat((List<?>) r.get("producers")).isEmpty();
	}

	@Test
	void topicsTransactionsReportsUnsupportedApisAsNotes() {
		DescribeProducersResult producers = mock(DescribeProducersResult.class);
		when(producers.partitionResult(any())).thenAnswer(i -> unsupported());
		when(admin.describeProducers(anyCollection())).thenReturn(producers);
		ListTransactionsResult txns = mock(ListTransactionsResult.class);
		Map<Integer, KafkaFuture<Collection<TransactionListing>>> byBroker = Map.of(1, unsupported());
		when(txns.byBrokerId()).thenReturn(KafkaFuture.completedFuture(byBroker));
		when(admin.listTransactions(any(ListTransactionsOptions.class))).thenReturn(txns);

		@SuppressWarnings("unchecked")
		Map<String, Object> r = (Map<String, Object>) new TopicClientTools(null).topicsTransactions(call(TOPIC));
		assertThat(r.get("openTransactionsInCluster")).isEqualTo(0);
		assertThat(r.get("hangingSuspects")).isEqualTo(0);
		@SuppressWarnings("unchecked")
		List<String> notes = (List<String>) r.get("notes");
		assertThat(notes).hasSize(2).allSatisfy(n -> assertThat(n).startsWith(KafkaErrors.UNSUPPORTED));
		assertThat(notes.get(1)).contains("listTransactions");
	}

	@Test
	void topicsLastWriteTimeNotesMissingMaxTimestamp() {
		// both partitions are empty: no last record to read, the note must not say "last record read"
		stubOffsets(Map.of());

		@SuppressWarnings("unchecked")
		Map<String, Object> r = (Map<String, Object>) new TopicDataTools().topicsLastWriteTime(call(TOPIC));
		assertThat(r.get("emptyPartitions")).isEqualTo(2);
		assertThat(r.get("method")).isEqualTo("maxTimestamp");
		assertThat((String) r.get("note")).startsWith(KafkaErrors.UNSUPPORTED)
			.contains("maxTimestamp")
			.doesNotContain("last record read");
		assertThat(r).doesNotContainKey("lastWriteTime");
	}

	@Test
	void topicsLastWriteTimeReadsLastRecordWhenMaxTimestampUnsupported() {
		// p0 has 5 records (0..4), p1 is empty; no maxTimestamp → the last record of p0 is read with a consumer
		stubOffsets(Map.of(0, 5L));
		long ts = 1_700_000_000_000L;
		TopicPartition p0 = new TopicPartition(TOPIC, 0);
		@SuppressWarnings("unchecked")
		KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
		ConsumerRecord<byte[], byte[]> last = new ConsumerRecord<>(TOPIC, 0, 4L, ts, TimestampType.CREATE_TIME, 0, 0,
				null, new byte[] { 1 }, new RecordHeaders(), Optional.empty());
		when(consumer.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Map.of(p0, List.of(last)), Map.of()))
			.thenReturn(ConsumerRecords.empty());
		when(consumer.position(eq(p0), any(Duration.class))).thenReturn(5L);
		when(clients.newConsumer("old", null)).thenReturn(consumer);

		@SuppressWarnings("unchecked")
		Map<String, Object> r = (Map<String, Object>) new TopicDataTools().topicsLastWriteTime(call(TOPIC));
		assertThat(r.get("method")).isEqualTo("maxTimestamp+lastRecord");
		assertThat((String) r.get("note")).startsWith(KafkaErrors.UNSUPPORTED).contains("last record read");
		assertThat(r.get("lastWriteTime")).isEqualTo(Instant.ofEpochMilli(ts).toString());
		assertThat(r.get("partition")).isEqualTo(0);
		assertThat(r.get("emptyPartitions")).isEqualTo(1);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> rows = (List<Map<String, Object>>) r.get("partitions");
		assertThat(rows.get(0)).containsEntry("source", "lastRecord")
			.containsEntry("offset", 4L)
			.containsEntry("lastWriteTime", Instant.ofEpochMilli(ts).toString());
		assertThat(rows.get(1)).containsEntry("empty", true);
		verify(consumer).seek(p0, 0L);
	}

	/**
	 * listOffsets: maxTimestamp unsupported (Kafka &lt; 3.0); earliest 0, latest the value in {@code latest} (or
	 * 0 = empty partition).
	 */
	private void stubOffsets(Map<Integer, Long> latest) {
		when(admin.listOffsets(anyMap())).thenAnswer(i -> {
			@SuppressWarnings("unchecked")
			Map<TopicPartition, OffsetSpec> specs = (Map<TopicPartition, OffsetSpec>) i.getArgument(0);
			Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new LinkedHashMap<>();
			specs.forEach((tp, spec) -> {
				long offset = spec instanceof OffsetSpec.LatestSpec ? latest.getOrDefault(tp.partition(), 0L) : 0;
				futures.put(tp, spec instanceof OffsetSpec.MaxTimestampSpec ? unsupported()
						: KafkaFuture.completedFuture(new ListOffsetsResultInfo(offset, -1, Optional.empty())));
			});
			ListOffsetsResult r = mock(ListOffsetsResult.class);
			when(r.partitionResult(any())).thenAnswer(a -> futures.get(a.getArgument(0)));
			return r;
		});
	}

	@Test
	void brokersLogdirsNotesMissingDiskSize() {
		DescribeLogDirsResult dirs = mock(DescribeLogDirsResult.class);
		// before DescribeLogDirs v4: no totalBytes/usableBytes
		LogDirDescription dir = new LogDirDescription(null,
				Map.of(new TopicPartition(TOPIC, 0), new ReplicaInfo(1234, 0, false)));
		when(dirs.descriptions()).thenReturn(Map.of(1, KafkaFuture.completedFuture(Map.of("/data", dir))));
		when(admin.describeLogDirs(anyCollection())).thenReturn(dirs);

		@SuppressWarnings("unchecked")
		Map<String, Object> r = (Map<String, Object>) new BrokerTools().brokersLogdirs(call(null));
		@SuppressWarnings("unchecked")
		Map<String, Object> row = ((List<Map<String, List<Map<String, Object>>>>) r.get("brokers")).get(0)
			.get("dirs")
			.get(0);
		assertThat(row.get("sizeBytes")).isEqualTo(1234L);
		assertThat(row).doesNotContainKey("totalBytes");
		assertThat((String) row.get("disk")).startsWith(KafkaErrors.UNSUPPORTED).contains("3.3");
	}

	@Test
	void clusterMetaModeOnBrokersWithoutDescribeQuorum() {
		// ZooKeeper: no DescribeQuorum, no finalized features → controller from describeCluster
		stubQuorum(unsupported());
		stubFeatures(KafkaFuture.completedFuture(features(Map.of())));
		ClusterMeta.Info zk = ClusterMeta.describe(call(null), true);
		assertThat(zk.clusterMode()).isEqualTo(ClusterMeta.Mode.ZOOKEEPER);
		assertThat(zk.controllerId()).isEqualTo(1);
		assertThat(zk.quorumError()).isNull();
		assertThat(ClusterMeta.mode(call(null))).isEqualTo(ClusterMeta.Mode.ZOOKEEPER);

		// KRaft without DescribeQuorum but with a finalized metadata.version: the describeCluster controller is random
		stubFeatures(KafkaFuture.completedFuture(
				features(Map.of("metadata.version", new FinalizedVersionRange((short) 7, (short) 7)))));
		ClusterMeta.Info oldKraft = ClusterMeta.describe(call(null), true);
		assertThat(oldKraft.clusterMode()).isEqualTo(ClusterMeta.Mode.KRAFT);
		assertThat(oldKraft.controllerId()).isNull();
		assertThat(oldKraft.quorumError()).startsWith(KafkaErrors.UNSUPPORTED);

		// no quorum permission and the feature query fails too: mode unknown, controller not reported
		stubQuorum(failed(new ClusterAuthorizationException("denied")));
		stubFeatures(failed(new org.apache.kafka.common.errors.TimeoutException("timeout")));
		ClusterMeta.Info unknown = ClusterMeta.describe(call(null), true);
		assertThat(unknown.clusterMode()).isEqualTo(ClusterMeta.Mode.UNKNOWN);
		assertThat(unknown.mode()).isNull();
		assertThat(unknown.controllerId()).isNull();
		assertThat(unknown.quorumError()).contains("Not authorized for the cluster");
		assertThat(ClusterMeta.mode(call(null))).isEqualTo(ClusterMeta.Mode.UNKNOWN);
		assertThat(ClusterTools.controllerNote(unknown)).contains("could not be determined");

		// feature query fails but DescribeQuorum is unsupported: ZooKeeper
		stubQuorum(unsupported());
		assertThat(ClusterMeta.mode(call(null))).isEqualTo(ClusterMeta.Mode.ZOOKEEPER);
	}

	private void stubQuorum(KafkaFuture<QuorumInfo> f) {
		DescribeMetadataQuorumResult q = mock(DescribeMetadataQuorumResult.class);
		when(q.quorumInfo()).thenReturn(f);
		when(admin.describeMetadataQuorum()).thenReturn(q);
	}

	private void stubFeatures(KafkaFuture<FeatureMetadata> f) {
		DescribeFeaturesResult r = mock(DescribeFeaturesResult.class);
		when(r.featureMetadata()).thenReturn(f);
		when(admin.describeFeatures()).thenReturn(r);
	}

	private static FeatureMetadata features(Map<String, FinalizedVersionRange> finalized) {
		FeatureMetadata m = mock(FeatureMetadata.class);
		when(m.finalizedFeatures()).thenReturn(finalized);
		return m;
	}

	private static <T> KafkaFuture<T> unsupported() {
		return failed(new UnsupportedVersionException("The node does not support this API"));
	}

	private static <T> KafkaFuture<T> failed(Throwable t) {
		KafkaFutureImpl<T> f = new KafkaFutureImpl<>();
		f.completeExceptionally(t);
		return f;
	}

	private ToolCall call(String topic) {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("cluster", "old");
		if (topic != null) {
			args.put("topic", topic);
		}
		return new ToolCall("test", args, clients, Instant.now().plusMillis(KafkaClients.TIMEOUT_MS));
	}

}
