package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * ZooKeeper→KRaft geçişi (zookeeper.metadata.migration.enable=true): KRaft controller'ı devraldıktan sonra
 * ZooKeeper broker'ları describeCluster'da rastgele bir canlı broker'ı controller diye döndürür; mod ZooKeeper
 * görünse de bu id aktif controller olarak raporlanmamalı. Gerçek geçiş ortamı kurulamadığı için Admin mock'lanır
 * (DescribeQuorum desteklenmiyor, finalize edilmiş feature yok = ZooKeeper broker'ı yanıtlıyor).
 */
class ZkMigrationTest {

	private static final String TOPIC = "orders";

	private final Node b1 = new Node(1, "zk-1", 9092);

	private final Node b2 = new Node(2, "zk-2", 9092);

	private final Node b3 = new Node(3, "zk-3", 9092);

	private Admin admin;

	private KafkaClients clients;

	@BeforeEach
	void setUp() {
		admin = mock(Admin.class);
		clients = mock(KafkaClients.class);
		when(clients.admin("mig")).thenReturn(admin);
		ClusterConfig c = new ClusterConfig();
		c.setName("mig");
		c.setBootstrapServers("zk-1:9092");
		KafkaMcpConfig config = new KafkaMcpConfig();
		config.setClusters(List.of(c));
		when(clients.config()).thenReturn(config);

		// describeCluster: geçişte rastgele broker (3) controller diye döner
		DescribeClusterResult cluster = mock(DescribeClusterResult.class);
		when(cluster.clusterId()).thenReturn(KafkaFuture.completedFuture("mig-cluster"));
		when(cluster.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(b1, b2, b3)));
		when(cluster.controller()).thenReturn(KafkaFuture.completedFuture(b3));
		when(admin.describeCluster()).thenReturn(cluster);
		when(admin.describeCluster(any(DescribeClusterOptions.class))).thenReturn(cluster);
		// ZooKeeper broker'ı: DescribeQuorum yok, metadata.version finalize edilmemiş
		DescribeMetadataQuorumResult quorum = mock(DescribeMetadataQuorumResult.class);
		when(quorum.quorumInfo()).thenReturn(failed(new UnsupportedVersionException("DESCRIBE_QUORUM yok")));
		when(admin.describeMetadataQuorum()).thenReturn(quorum);
		FeatureMetadata fm = mock(FeatureMetadata.class);
		when(fm.finalizedFeatures()).thenReturn(Map.<String, FinalizedVersionRange>of());
		DescribeFeaturesResult features = mock(DescribeFeaturesResult.class);
		when(features.featureMetadata()).thenReturn(KafkaFuture.completedFuture(fm));
		when(admin.describeFeatures()).thenReturn(features);
		when(admin.describeFeatures(any(DescribeFeaturesOptions.class))).thenReturn(features);

		TopicDescription d = new TopicDescription(TOPIC, false,
				List.of(new TopicPartitionInfo(0, b1, List.of(b1, b2), List.of(b1, b2)),
						new TopicPartitionInfo(1, b3, List.of(b3, b1), List.of(b3, b1))));
		ListTopicsResult listing = mock(ListTopicsResult.class);
		when(listing.listings())
			.thenReturn(KafkaFuture.completedFuture(List.of(new TopicListing(TOPIC, Uuid.randomUuid(), false))));
		when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(listing);
		DescribeTopicsResult topics = mock(DescribeTopicsResult.class);
		when(topics.topicNameValues()).thenReturn(Map.of(TOPIC, KafkaFuture.completedFuture(d)));
		when(topics.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(TOPIC, d)));
		when(admin.describeTopics(anyCollection())).thenReturn(topics);
		ListPartitionReassignmentsResult reassignments = mock(ListPartitionReassignmentsResult.class);
		when(reassignments.reassignments()).thenReturn(KafkaFuture.completedFuture(Map.of()));
		when(admin.listPartitionReassignments()).thenReturn(reassignments);
	}

	@Test
	void clusterMetaDoesNotTrustDescribeClusterControllerDuringMigration() {
		stubBrokerConfigs(KafkaFuture.completedFuture(zkBrokerConfig(true)));
		ClusterMeta.Info info = ClusterMeta.describe(call(), true);
		assertThat(info.clusterMode()).isEqualTo(ClusterMeta.Mode.ZOOKEEPER);
		assertThat(info.zkMigration()).isTrue();
		assertThat(info.migration()).isTrue();
		assertThat(info.controllerId()).as("rastgele broker controller diye raporlanmamalı").isNull();
		assertThat(info.controllerIsBroker()).isFalse();
		assertThat(ClusterTools.controllerNote(info)).isEqualTo(ClusterTools.MIGRATION_CONTROLLER_NOTE);
		assertThat(info.quorumError()).isNull();
	}

	@Test
	void clusterMetaKeepsZooKeeperControllerWithoutMigrationOrWhenConfigUnreadable() {
		stubBrokerConfigs(KafkaFuture.completedFuture(zkBrokerConfig(false)));
		ClusterMeta.Info plain = ClusterMeta.describe(call(), true);
		assertThat(plain.zkMigration()).isFalse();
		assertThat(plain.controllerId()).isEqualTo(3);
		assertThat(ClusterTools.controllerNote(plain)).isNull();

		// DESCRIBE_CONFIGS yetkisi yok: geçiş bilinmiyor; çoğu ZooKeeper cluster'ı geçişte olmadığından controller kalır
		stubBrokerConfigs(failed(new ClusterAuthorizationException("denied")));
		ClusterMeta.Info denied = ClusterMeta.describe(call(), true);
		assertThat(denied.zkMigration()).isNull();
		assertThat(denied.migration()).isFalse();
		assertThat(denied.controllerId()).isEqualTo(3);

		// KRaft yanıtı (geçişte KRaft'a taşınmış broker): controller quorum'dan, config'e bakılmaz
		FeatureMetadata kraft = mock(FeatureMetadata.class);
		when(kraft.finalizedFeatures())
			.thenReturn(Map.of("metadata.version", new FinalizedVersionRange((short) 21, (short) 21)));
		DescribeFeaturesResult features = mock(DescribeFeaturesResult.class);
		when(features.featureMetadata()).thenReturn(KafkaFuture.completedFuture(kraft));
		when(admin.describeFeatures()).thenReturn(features);
		ClusterMeta.Info k = ClusterMeta.describe(call(), true);
		assertThat(k.clusterMode()).isEqualTo(ClusterMeta.Mode.KRAFT);
		assertThat(k.zkMigration()).isNull();
		assertThat(k.controllerId()).isNull();
	}

	@Test
	void clustersGetReportsMigrationInsteadOfRandomController() {
		stubBrokerConfigs(KafkaFuture.completedFuture(zkBrokerConfig(true)));
		@SuppressWarnings("unchecked")
		Map<String, Object> r = (Map<String, Object>) new ClusterTools(clients).clustersGet(call());
		assertThat(r.get("mode")).isEqualTo(ClusterMeta.ZOOKEEPER);
		assertThat(r).doesNotContainKey("controllerId");
		assertThat(r.get("controllerNote")).isEqualTo(ClusterTools.MIGRATION_CONTROLLER_NOTE);
		assertThat(r.get("zkMigration")).isEqualTo(true);
		assertThat((String) r.get("modeNote")).contains("geçiş").contains("brokers_config_drift");
		@SuppressWarnings("unchecked")
		Map<String, Object> zk = (Map<String, Object>) r.get("zookeeper");
		assertThat(zk).containsEntry("connect", "zk:2181").containsEntry("migrationEnabled", true);
		// IBP açıkça verilmiş (sabitlenmiş): en düşük sürüm, yazılım daha yeni olabilir
		assertThat(r.get("minKafkaVersion")).isEqualTo("3.4");
		assertThat(r).doesNotContainKey("kafkaVersion");
		assertThat(r.get("versionPinned")).isEqualTo(true);
		assertThat(r.get("versionNote")).isEqualTo(ClusterTools.VERSION_NOTE);
		assertThat(r.get("brokerCount")).isEqualTo(3);
		assertThat(r.get("partitionCount")).isEqualTo(2);
	}

	@Test
	void brokersListMarksNoControllerDuringMigration() {
		stubBrokerConfigs(KafkaFuture.completedFuture(zkBrokerConfig(true)));
		@SuppressWarnings("unchecked")
		Map<String, Object> r = (Map<String, Object>) new BrokerTools().brokersList(call());
		assertThat(r).doesNotContainKey("controllerId");
		assertThat(r.get("controllerNote")).isEqualTo(ClusterTools.MIGRATION_CONTROLLER_NOTE);
		assertThat(r.get("zkMigration")).isEqualTo(true);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> rows = (List<Map<String, Object>>) r.get("brokers");
		assertThat(rows).hasSize(3).allSatisfy(b -> assertThat(b.get("isController")).isEqualTo(false));

		stubBrokerConfigs(KafkaFuture.completedFuture(zkBrokerConfig(false)));
		@SuppressWarnings("unchecked")
		Map<String, Object> plain = (Map<String, Object>) new BrokerTools().brokersList(call());
		assertThat(plain.get("controllerId")).isEqualTo(3);
		assertThat(plain).doesNotContainKey("zkMigration").doesNotContainKey("controllerNote");
	}

	@Test
	void clustersListOmitsControllerDuringMigration() throws Exception {
		stubBrokerConfigs(KafkaFuture.completedFuture(zkBrokerConfig(true)));
		Map<String, Object> row = clustersListRow();
		assertThat(row.get("status")).isEqualTo("ok");
		assertThat(row.get("mode")).isEqualTo(ClusterMeta.ZOOKEEPER);
		assertThat(row).doesNotContainKey("controllerId");
		assertThat(row.get("zkMigration")).isEqualTo(true);

		stubBrokerConfigs(KafkaFuture.completedFuture(zkBrokerConfig(false)));
		Map<String, Object> plain = clustersListRow();
		assertThat(plain.get("controllerId")).isEqualTo(3);
		assertThat(plain).doesNotContainKey("zkMigration");
	}

	private Map<String, Object> clustersListRow() throws Exception {
		ToolCall call = new ToolCall("clusters_list", new LinkedHashMap<>(), clients,
				Instant.now().plusMillis(KafkaClients.TIMEOUT_MS));
		@SuppressWarnings("unchecked")
		Map<String, Object> r = (Map<String, Object>) new ClusterTools(clients).clustersList(call);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> rows = (List<Map<String, Object>>) r.get("clusters");
		assertThat(rows).hasSize(1);
		return rows.get(0);
	}

	/** Her broker kaynağı için aynı config sonucu (iki describeConfigs overload'u). */
	private void stubBrokerConfigs(KafkaFuture<Config> config) {
		Map<ConfigResource, KafkaFuture<Config>> values = new LinkedHashMap<>();
		for (Node n : List.of(b1, b2, b3)) {
			values.put(new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(n.id())), config);
		}
		DescribeConfigsResult r = mock(DescribeConfigsResult.class);
		when(r.values()).thenReturn(values);
		when(admin.describeConfigs(anyCollection())).thenReturn(r);
		when(admin.describeConfigs(anyCollection(), any(DescribeConfigsOptions.class))).thenReturn(r);
	}

	private static Config zkBrokerConfig(boolean migration) {
		return new Config(List.of(entry("zookeeper.connect", "zk:2181", ConfigSource.STATIC_BROKER_CONFIG),
				entry(ClusterMeta.MIGRATION_ENABLE, String.valueOf(migration),
						migration ? ConfigSource.STATIC_BROKER_CONFIG : ConfigSource.DEFAULT_CONFIG),
				entry("inter.broker.protocol.version", "3.4-IV0", ConfigSource.STATIC_BROKER_CONFIG),
				entry("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer",
						ConfigSource.STATIC_BROKER_CONFIG)));
	}

	private static ConfigEntry entry(String name, String value, ConfigSource source) {
		return new ConfigEntry(name, value, source, false, false, List.of(), ConfigEntry.ConfigType.STRING, null);
	}

	private static <T> KafkaFuture<T> failed(Throwable t) {
		KafkaFutureImpl<T> f = new KafkaFutureImpl<>();
		f.completeExceptionally(t);
		return f;
	}

	private ToolCall call() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("cluster", "mig");
		return new ToolCall("test", args, clients, Instant.now().plusMillis(KafkaClients.TIMEOUT_MS));
	}

}
