package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.kafka.LogDirs;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;

/**
 * Kısmen erişilemeyen cluster ve az yetkili kullanıcı durumları. Araç metotları doğrudan, kendi
 * {@link KafkaClients}'ıyla çağrılır: {@code admin} (süper kullanıcı) ve {@code alice} (Cluster:Describe ve
 * DESCRIBE_CONFIGS yetkisi yok).
 */
class DegradedClusterTest {

	private static KafkaClients clients;

	private static Admin admin;

	@BeforeAll
	static void setUp() {
		KafkaMcpConfig config = new KafkaMcpConfig();
		config.setClusters(List.of(cluster("admin"), cluster("alice")));
		clients = new KafkaClients(config);
		admin = KafkaTestCluster.admin();
	}

	@AfterAll
	static void tearDown() {
		clients.destroy();
		admin.close();
	}

	@Test
	void logDirsDoesNotWaitForBrokersMissingFromMetadata() {
		ToolCall call = call("admin", Map.of());
		int live = ClusterMeta.describe(call, false).brokerIds().get(0);
		long start = System.nanoTime();
		// 99: replika listesinde kalmış ama kapalı broker gibi; admin client onun için isteği 15 sn bekletirdi
		Futures.Each<Integer, Map<String, LogDirDescription>> dirs = LogDirs.describe(call, List.of(live, 99));
		long elapsedMs = (System.nanoTime() - start) / 1_000_000;
		assertThat(elapsedMs).isLessThan(5_000);
		assertThat(dirs.values()).containsKey(live);
		assertThat(dirs.errors()).containsEntry(99, LogDirs.OFFLINE);
	}

	@Test
	void clustersGetWithoutClusterDescribeReportsKraftAndUnknownController() {
		@SuppressWarnings("unchecked")
		Map<String, Object> r = (Map<String, Object>) new ClusterTools(clients).clustersGet(call("alice", Map.of()));
		assertThat(r.get("quorumError")).isNotNull();
		// KRaft broker'ı describeCluster'da rastgele bir broker döndürür; controller diye raporlanmamalı
		assertThat(r).doesNotContainKey("controllerId");
		assertThat(r.get("controllerNote")).asString().contains("belirlenemedi");
		assertThat(r.get("mode")).isEqualTo(ClusterMeta.KRAFT);
		assertThat(r.get("minKafkaVersion")).isNotNull();
		assertThat(r.get("brokerCount")).isEqualTo(1);
		assertThat(r.get("reassignmentsError")).isNotNull();

		@SuppressWarnings("unchecked")
		Map<String, Object> brokers = (Map<String, Object>) new BrokerTools().brokersList(call("alice", Map.of()));
		assertThat(brokers).doesNotContainKey("controllerId");
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> rows = (List<Map<String, Object>>) brokers.get("brokers");
		assertThat(rows).isNotEmpty().allSatisfy(b -> assertThat(b.get("isController")).isEqualTo(false));
	}

	@Test
	void topicsCreateSucceedsWhenTopicDetailsCannotBeDescribed() throws Exception {
		String prefix = "alice-create-" + System.nanoTime() % 1_000_000;
		String topic = prefix + "-t";
		AclBinding create = new AclBinding(new ResourcePattern(ResourceType.TOPIC, prefix, PatternType.PREFIXED),
				new AccessControlEntry("User:alice", "*", AclOperation.CREATE, AclPermissionType.ALLOW));
		admin.createAcls(List.of(create)).all().get();
		try {
			awaitAcl(create.toFilter());
			Map<String, Object> args = new LinkedHashMap<>();
			args.put("topic", topic);
			args.put("partitions", 2);
			@SuppressWarnings("unchecked")
			Map<String, Object> r = (Map<String, Object>) new TopicWriteTools().topicsCreate(call("alice", args));
			assertThat(r.get("topic")).isEqualTo(topic);
			assertThat(r.get("partitions")).isEqualTo(2);
			assertThat(r.get("note")).asString().contains("Topic oluşturuldu");
			assertThat(admin.listTopics().names().get()).contains(topic);
		}
		finally {
			admin.deleteAcls(List.of(create.toFilter())).all().get();
			admin.deleteTopics(List.of(topic)).all().get();
		}
	}

	private static void awaitAcl(AclBindingFilter filter) throws Exception {
		for (int i = 0; i < 100 && admin.describeAcls(filter).values().get().isEmpty(); i++) {
			Thread.sleep(100);
		}
		// broker'ın authorizer'ı metadata'yı uygulasın
		Thread.sleep(500);
	}

	private static ToolCall call(String cluster, Map<String, Object> args) {
		Map<String, Object> all = new LinkedHashMap<>(args);
		all.put("cluster", cluster);
		return new ToolCall("test", all, clients, Instant.now().plusMillis(KafkaClients.TIMEOUT_MS));
	}

	private static ClusterConfig cluster(String user) {
		ClusterConfig c = new ClusterConfig();
		c.setName(user);
		c.setBootstrapServers(KafkaTestCluster.bootstrap());
		c.setProperties(KafkaTestCluster.saslProperties(user));
		return c;
	}

}
