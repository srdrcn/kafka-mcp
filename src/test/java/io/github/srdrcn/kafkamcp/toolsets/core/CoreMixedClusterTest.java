package io.github.srdrcn.kafkamcp.toolsets.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KafkaMcpConfig;
import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.support.MixedClusterIntegrationTest;
import io.github.srdrcn.kafkamcp.support.ZkKafkaTestCluster;

/**
 * core read tools on the ZooKeeper-mode cluster ({@code zk}, Kafka 3.9) with real data; outputs that depend on the
 * mode (mode, controller, version source, features, authorizer, ELR, quorum, group type) are checked against the
 * KRaft cluster ({@code test}, Kafka 4.1).
 */
class CoreMixedClusterTest extends MixedClusterIntegrationTest {

	/** zk topic: 10 messages in p0, 5 in p1 (fixed timestamps), p2 empty. */
	private static String topic;

	/** A topic on the KRaft cluster too (for comparing by mode). */
	private static String kraftTopic;

	/** Group without members that has committed offsets on zk. */
	private static String emptyGroup;

	/** Fixed timestamps within the retention (1 day): 2 and 1 hours ago (rounded to the second). */
	private static final long TS0 = System.currentTimeMillis() / 1000 * 1000 - 7_200_000L;

	private static final long TS1 = TS0 + 3_600_000L;

	/** Separate clients connecting to both clusters as a user with few permissions (alice: no Cluster:Describe). */
	private static KafkaClients aliceClients;

	@Autowired
	private KafkaClients clients;

	@BeforeAll
	static void setUp() throws Exception {
		topic = unique("zkcore");
		TestData.createTopic(zkAdmin(), topic, 3, Map.of("retention.ms", "86400000", "cleanup.policy", "delete"));
		TestData.produceZk(topic, 0, 10, TS0);
		TestData.produceZk(topic, 1, 5, TS1);
		// empty Classic group (no members); also creates __consumer_offsets on ZooKeeper
		emptyGroup = unique("zk-g-empty");
		zkAdmin().alterConsumerGroupOffsets(emptyGroup,
				Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(4), new TopicPartition(topic, 1),
						new OffsetAndMetadata(5)))
			.all()
			.get();
		kraftTopic = unique("krcore");
		TestData.createTopic(testAdmin(), kraftTopic, 2, Map.of());
		TestData.produce(kraftTopic, 0, 3, null);

		KafkaMcpConfig config = new KafkaMcpConfig();
		config.setClusters(List.of(cluster("zk-alice", ZkKafkaTestCluster.bootstrap()),
				cluster("kraft-alice", KafkaTestCluster.bootstrap())));
		aliceClients = new KafkaClients(config);
	}

	@AfterAll
	static void tearDown() {
		if (aliceClients != null) {
			aliceClients.destroy();
		}
	}

	@Test
	void clustersListShowsModePerClusterAndControllerOnlyForZooKeeper() {
		JsonNode r = ok("clusters_list", Map.of());
		assertThat(r.get("total").asInt()).isEqualTo(3);
		Map<String, JsonNode> byName = new LinkedHashMap<>();
		r.get("clusters").forEach(c -> byName.put(c.get("name").asText(), c));
		JsonNode zk = byName.get(ZK_CLUSTER);
		assertThat(zk.get("status").asText()).isEqualTo("ok");
		assertThat(zk.get("mode").asText()).isEqualTo("ZooKeeper");
		// on ZooKeeper the describeCluster controller is the real controller
		assertThat(zk.get("controllerId").asInt()).isEqualTo(1);
		assertThat(zk.has("zkMigration")).isFalse();
		assertThat(zk.get("brokerCount").asInt()).isEqualTo(1);
		for (String kraft : List.of("test", "test-ro")) {
			JsonNode k = byName.get(kraft);
			assertThat(k.get("status").asText()).isEqualTo("ok");
			assertThat(k.get("mode").asText()).as(kraft).isEqualTo("KRaft");
			// KRaft brokers return a random broker in describeCluster: it is not reported as the controller
			assertThat(k.has("controllerId")).as(kraft).isFalse();
		}
		assertThat(byName.get("test").get("clusterId").asText()).isNotEqualTo(zk.get("clusterId").asText());

		JsonNode noCheck = ok("clusters_list", zkArgs("check", false));
		assertThat(noCheck.at("/clusters/0/name").asText()).isEqualTo(ZK_CLUSTER);
		assertThat(noCheck.at("/clusters/0").has("mode")).isFalse();
	}

	@Test
	void clustersGetReportsModeSpecificSummary() {
		JsonNode zk = ok("clusters_get", zkArgs());
		assertThat(zk.get("cluster").asText()).isEqualTo(ZK_CLUSTER);
		assertThat(zk.get("clusterId").asText()).isNotBlank();
		assertThat(zk.get("mode").asText()).isEqualTo("ZooKeeper");
		assertThat(zk.get("controllerId").asInt()).isEqualTo(1);
		assertThat(zk.has("controllerNote")).isFalse();
		// no metadata.version on ZooKeeper: the version comes from the IBP; default IBP (not pinned) = broker software
		assertThat(zk.get("minKafkaVersion").asText()).isEqualTo("3.9");
		assertThat(zk.has("kafkaVersion")).isFalse();
		assertThat(zk.get("versionSource").asText()).startsWith("inter.broker.protocol.version=3.9");
		assertThat(zk.has("versionPinned")).isFalse();
		assertThat(zk.has("versionNote")).isFalse();
		assertThat(zk.has("zkMigration")).isFalse();
		assertThat(zk.has("modeNote")).isFalse();
		assertThat(zk.has("quorum")).isFalse();
		assertThat(zk.has("quorumError")).as("a missing DescribeQuorum on ZooKeeper is not an error").isFalse();
		assertThat(zk.has("features")).isFalse();
		assertThat(zk.at("/zookeeper/connect").asText()).isEqualTo("zookeeper:2181");
		assertThat(zk.at("/zookeeper").has("migrationEnabled")).isFalse();
		assertThat(zk.get("authorizer").asText()).isEqualTo("kafka.security.authorizer.AclAuthorizer");
		assertThat(zk.get("brokerCount").asInt()).isEqualTo(1);
		assertThat(zk.get("topicCount").asInt()).isPositive();
		assertThat(zk.get("partitionCount").asInt()).isGreaterThanOrEqualTo(3);
		assertThat(zk.get("underReplicatedPartitions").asInt()).isZero();
		assertThat(zk.get("offlinePartitions").asInt()).isZero();
		assertThat(zk.get("underMinIsrPartitions").asInt()).isZero();
		assertThat(zk.has("reassignmentsError")).isFalse();

		JsonNode kraft = ok("clusters_get", args());
		assertThat(kraft.get("mode").asText()).isEqualTo("KRaft");
		// on KRaft the active controller is the quorum leader
		assertThat(kraft.get("controllerId").asInt()).isEqualTo(kraft.at("/quorum/leaderId").asInt()).isEqualTo(1);
		assertThat(kraft.get("minKafkaVersion").asText()).isEqualTo("4.1");
		assertThat(kraft.get("versionSource").asText()).startsWith("metadata.version=");
		// metadata.version goes up after an upgrade: it can lag behind the software version
		assertThat(kraft.get("versionNote").asText()).contains("broker software may be newer");
		assertThat(kraft.has("zkMigration")).isFalse();
		assertThat(kraft.at("/features/metadata.version").asInt()).isGreaterThanOrEqualTo(27);
		assertThat(kraft.get("features").has("group.version")).isTrue();
		assertThat(kraft.has("zookeeper")).isFalse();
		assertThat(kraft.get("authorizer").asText()).isEqualTo("org.apache.kafka.metadata.authorizer.StandardAuthorizer");
		assertThat(kraft.at("/quorum/voters/0/id").asInt()).isEqualTo(1);
	}

	@Test
	void clusterModeHelperWorksForBothModesAndWithoutClusterDescribe() {
		assertThat(ClusterMeta.mode(call(clients, ZK_CLUSTER))).isEqualTo(ClusterMeta.Mode.ZOOKEEPER);
		assertThat(ClusterMeta.mode(call(clients, "test"))).isEqualTo(ClusterMeta.Mode.KRAFT);

		ClusterMeta.Info zk = ClusterMeta.describe(call(clients, ZK_CLUSTER), true);
		assertThat(zk.mode()).isEqualTo(ClusterMeta.ZOOKEEPER);
		// controller broker config was read: ZooKeeper→KRaft migration is off, the describeCluster controller is real
		assertThat(zk.zkMigration()).isFalse();
		assertThat(zk.controllerId()).isEqualTo(1);
		assertThat(zk.quorum()).isNull();
		assertThat(zk.quorumError()).isNull();
		assertThat(zk.finalizedFeatures()).isEmpty();
		assertThat(zk.metadataVersionLevel()).isNull();

		ClusterMeta.Info kraft = ClusterMeta.describe(call(clients, "test"), true);
		assertThat(kraft.clusterMode()).isEqualTo(ClusterMeta.Mode.KRAFT);
		assertThat(kraft.controllerId()).isEqualTo(kraft.quorum().leaderId());
		assertThat(kraft.zkMigration()).isNull();
		assertThat(kraft.metadataVersionLevel()).isNotNull();

		// alice has no Cluster:Describe permission: the quorum is denied on KRaft, the mode comes from the features
		ClusterMeta.Info denied = ClusterMeta.describe(call(aliceClients, "kraft-alice"), true);
		assertThat(denied.quorum()).isNull();
		assertThat(denied.quorumError()).isNotBlank();
		assertThat(denied.clusterMode()).isEqualTo(ClusterMeta.Mode.KRAFT);
		assertThat(denied.controllerId()).as("the random describeCluster controller must not be used").isNull();
		assertThat(ClusterMeta.mode(call(aliceClients, "kraft-alice"))).isEqualTo(ClusterMeta.Mode.KRAFT);
		// on ZooKeeper the mode and controller are known even for the unprivileged user
		ClusterMeta.Info zkAlice = ClusterMeta.describe(call(aliceClients, "zk-alice"), true);
		assertThat(zkAlice.clusterMode()).isEqualTo(ClusterMeta.Mode.ZOOKEEPER);
		assertThat(zkAlice.migration()).isFalse();
		assertThat(zkAlice.controllerId()).isEqualTo(1);
		assertThat(ClusterMeta.mode(call(aliceClients, "zk-alice"))).isEqualTo(ClusterMeta.Mode.ZOOKEEPER);
	}

	@Test
	void brokersListMarksControllerInBothModes() {
		JsonNode zk = ok("brokers_list", zkArgs());
		assertThat(zk.get("mode").asText()).isEqualTo("ZooKeeper");
		assertThat(zk.get("controllerId").asInt()).isEqualTo(1);
		assertThat(zk.has("controllerNote")).isFalse();
		JsonNode b = zk.get("brokers").get(0);
		assertThat(b.get("id").asInt()).isEqualTo(1);
		assertThat(b.get("host").asText()).isEqualTo("localhost");
		assertThat(b.get("port").asInt()).isEqualTo(Integer.parseInt(ZkKafkaTestCluster.bootstrap().split(":")[1]));
		assertThat(b.get("isController").asBoolean()).isTrue();
		assertThat(b.get("leaders").asInt()).isGreaterThanOrEqualTo(3);
		assertThat(b.get("replicas").asInt()).isEqualTo(b.get("leaders").asInt());

		JsonNode kraft = ok("brokers_list", args());
		assertThat(kraft.get("mode").asText()).isEqualTo("KRaft");
		assertThat(kraft.get("controllerId").asInt()).isEqualTo(1);
		assertThat(kraft.at("/brokers/0/isController").asBoolean()).isTrue();
	}

	@Test
	void brokersConfigGetShowsModeSpecificKeysAndMasksSecrets() {
		JsonNode zk = ok("brokers_config_get", zkArgs("limit", 1000));
		assertThat(zk.get("brokerId").asInt()).isEqualTo(1);
		JsonNode configs = zk.get("configs");
		assertThat(ClusterBrokerToolsTest.find(configs, "zookeeper.connect").get("value").asText())
			.isEqualTo("zookeeper:2181");
		assertThat(ClusterBrokerToolsTest.find(configs, "broker.id").get("value").asText()).isEqualTo("1");
		assertThat(ClusterBrokerToolsTest.find(configs, "authorizer.class.name").get("value").asText())
			.isEqualTo("kafka.security.authorizer.AclAuthorizer");
		assertThat(ClusterBrokerToolsTest.find(configs, "listener.name.sasl.plain.sasl.jaas.config").get("value")
			.asText()).isEqualTo("******");
		// authorizer configs (not defined in KafkaConfig) come back sensitive/without a value in both modes
		assertThat(ClusterBrokerToolsTest.find(configs, "super.users").get("value").asText()).isEqualTo("******");
		assertThat(ClusterBrokerToolsTest.find(configs, "allow.everyone.if.no.acl.found").get("value").asText())
			.isEqualTo("******");
		assertThat(names(configs)).doesNotContain("process.roles", "controller.quorum.voters");
		JsonNode ibp = ok("brokers_config_get",
				zkArgs("brokerId", 1, "includeDefaults", true, "filter", "^inter\\.broker\\.protocol\\.version$"));
		assertThat(ibp.at("/configs/0/value").asText()).startsWith("3.9");
		assertThat(ibp.at("/configs/0/source").asText()).isEqualTo("DEFAULT_CONFIG");

		JsonNode kraft = ok("brokers_config_get", args("limit", 1000));
		JsonNode kc = kraft.get("configs");
		assertThat(ClusterBrokerToolsTest.find(kc, "process.roles").get("value").asText()).isEqualTo("broker,controller");
		assertThat(ClusterBrokerToolsTest.find(kc, "controller.quorum.voters").get("value").asText()).isNotBlank();
		assertThat(ClusterBrokerToolsTest.find(kc, "authorizer.class.name").get("value").asText())
			.isEqualTo("org.apache.kafka.metadata.authorizer.StandardAuthorizer");
		assertThat(ClusterBrokerToolsTest.find(kc, "super.users").get("value").asText()).isEqualTo("******");
		assertThat(ok("brokers_config_get", args("includeDefaults", true, "filter", "^zookeeper\\.")).get("total")
			.asInt()).as("Kafka 4.x KRaft has no zookeeper.* keys").isZero();

		assertThat(error("brokers_config_get", zkArgs("brokerId", 99))).contains("Broker not found: 99");
	}

	@Test
	void brokersConfigDriftSkewAndLogdirsOnZooKeeper() {
		JsonNode drift = ok("brokers_config_drift", zkArgs());
		assertThat(drift.get("brokersCompared")).hasSize(1);
		assertThat(drift.get("differences")).isEmpty();
		assertThat(drift.get("note").asText()).contains("At least 2 brokers");
		assertThat(drift.has("brokerModes")).isFalse();

		JsonNode skew = ok("brokers_partition_skew", zkArgs());
		JsonNode s = skew.get("summary");
		assertThat(s.get("brokers").asInt()).isEqualTo(1);
		assertThat(s.get("partitions").asInt()).isGreaterThanOrEqualTo(3);
		assertThat(s.get("offlinePartitions").asInt()).isZero();
		assertThat(s.get("nonPreferredLeaderPartitions").asInt()).isZero();
		assertThat(s.get("skewed").asBoolean()).isFalse();
		assertThat(skew.at("/brokers/0/brokerId").asInt()).isEqualTo(1);
		assertThat(skew.at("/brokers/0/leaders").asInt()).isEqualTo(s.get("partitions").asInt());

		JsonNode dirs = ok("brokers_logdirs", zkArgs("filter", "^" + topic + "$"));
		JsonNode dir = dirs.at("/brokers/0/dirs/0");
		assertThat(dirs.at("/brokers/0/brokerId").asInt()).isEqualTo(1);
		assertThat(dir.get("path").asText()).isEqualTo("/var/lib/kafka/data");
		assertThat(dir.get("partitions").asInt()).isGreaterThanOrEqualTo(3);
		assertThat(dir.get("sizeBytes").asLong()).isPositive();
		// Kafka 3.9 returns the disk size (3.3+)
		assertThat(dir.get("totalBytes").asLong()).isPositive();
		assertThat(dir.get("disk").asText()).contains("free");
		assertThat(dirs.get("total").asInt()).isEqualTo(3);
		assertThat(dirs.at("/largestPartitions/0/topic").asText()).isEqualTo(topic);
		assertThat(dirs.at("/largestPartitions/0/partition").asInt()).isZero();
		assertThat(error("brokers_logdirs", zkArgs("brokerId", 42))).contains("Broker not found");
	}

	@Test
	void topicReadToolsOnZooKeeper() {
		JsonNode list = ok("topics_list", zkArgs("filter", "^" + topic + "$"));
		assertThat(list.get("total").asInt()).isEqualTo(1);
		assertThat(list.at("/topics/0/partitions").asInt()).isEqualTo(3);
		assertThat(list.at("/topics/0/replicationFactor").asInt()).isEqualTo(1);
		assertThat(names(ok("topics_list", zkArgs("limit", 5000)).get("topics"))).doesNotContain("__consumer_offsets");
		JsonNode internal = ok("topics_list", zkArgs("includeInternal", true, "filter", "^__consumer_offsets$"));
		assertThat(internal.at("/topics/0/internal").asBoolean()).isTrue();
		assertThat(internal.at("/topics/0/partitions").asInt()).isPositive();

		JsonNode get = ok("topics_get", zkArgs("topic", topic));
		assertThat(get.get("topicId").asText()).isNotBlank().isNotEqualTo("AAAAAAAAAAAAAAAAAAAAAA");
		assertThat(get.get("partitionCount").asInt()).isEqualTo(3);
		assertThat(get.get("replicationFactor").asInt()).isEqualTo(1);
		assertThat(get.get("underReplicatedPartitions").asInt()).isZero();
		assertThat(get.at("/offsets/latestSum").asLong()).isEqualTo(15);
		assertThat(get.at("/offsets/approxMessages").asLong()).isEqualTo(15);
		assertThat(get.get("sizeBytes").asLong()).isPositive();
		assertThat(ClusterBrokerToolsTest.find(get.get("nonDefaultConfigs"), "retention.ms").get("source").asText())
			.isEqualTo("DYNAMIC_TOPIC_CONFIG");
		for (JsonNode p : get.get("partitions")) {
			assertThat(p.get("leader").asInt()).isEqualTo(1);
			assertThat(p.get("isr").get(0).asInt()).isEqualTo(1);
			// ZooKeeper returns no ELR (null in the client): the field is absent, no NPE
			assertThat(p.has("elr")).isFalse();
			assertThat(p.has("lastKnownElr")).isFalse();
		}
		JsonNode kraft = ok("topics_get", args("topic", kraftTopic));
		assertThat(kraft.get("topicId").asText()).isNotBlank();
		// KRaft has an ELR list, but it is empty with a single broker: not shown
		kraft.get("partitions").forEach(p -> assertThat(p.has("elr")).isFalse());

		JsonNode cfg = ok("topics_config_get", zkArgs("topic", topic, "includeDefaults", false));
		assertThat(names(cfg.get("configs"))).contains("retention.ms", "cleanup.policy");
		JsonNode retention = ClusterBrokerToolsTest.find(cfg.get("configs"), "retention.ms");
		assertThat(retention.get("value").asText()).isEqualTo("86400000");
		assertThat(retention.get("isDefault").asBoolean()).isFalse();
		JsonNode seg = ok("topics_config_get", zkArgs("topic", topic, "filter", "^segment\\.bytes$"));
		assertThat(seg.at("/configs/0/source").asText()).isEqualTo("DEFAULT_CONFIG");
		assertThat(seg.at("/configs/0/isDefault").asBoolean()).isTrue();

		JsonNode found = ok("topics_find_by_config",
				zkArgs("key", "retention.ms", "operator", "lt", "value", "86400001", "onlyNonDefault", true));
		assertThat(names(found.get("topics"), "topic")).contains(topic);
		JsonNode eq = ok("topics_find_by_config",
				zkArgs("key", "cleanup.policy", "operator", "eq", "value", "delete", "filter", "^" + topic + "$"));
		assertThat(eq.get("total").asInt()).isEqualTo(1);
		assertThat(eq.at("/topics/0/source").asText()).isEqualTo("DYNAMIC_TOPIC_CONFIG");
		assertThat(error("topics_get", zkArgs("topic", unique("missing")))).contains("Topic not found");
	}

	@Test
	void topicDataToolsOnZooKeeper() throws Exception {
		JsonNode size = ok("topics_size", zkArgs("topic", topic));
		assertThat(size.get("partitionCount").asInt()).isEqualTo(3);
		assertThat(size.get("sizeBytes").asLong()).isPositive();
		assertThat(size.get("totalSizeBytes").asLong()).isEqualTo(size.get("sizeBytes").asLong());
		assertThat(size.at("/partitions/0/partition").asInt()).isZero();
		assertThat(size.has("failedBrokers")).isFalse();
		JsonNode all = ok("topics_size", zkArgs("filter", "^" + topic + "$"));
		assertThat(all.at("/topics/0/topic").asText()).isEqualTo(topic);
		assertThat(all.at("/topics/0/sizeBytes").asLong()).isEqualTo(size.get("sizeBytes").asLong());

		JsonNode offsets = ok("topics_offsets", zkArgs("topic", topic));
		assertThat(offsets.at("/totals/earliest").asLong()).isZero();
		assertThat(offsets.at("/totals/latest").asLong()).isEqualTo(15);
		assertThat(offsets.at("/totals/count").asLong()).isEqualTo(15);
		assertThat(offsets.at("/partitions/0/latest").asLong()).isEqualTo(10);
		assertThat(offsets.at("/partitions/1/latest").asLong()).isEqualTo(5);
		assertThat(offsets.at("/partitions/2/count").asLong()).isZero();

		JsonNode last = ok("topics_last_write_time", zkArgs("topic", topic));
		// Kafka 3.9 has ListOffsets maxTimestamp: the last record is not read
		assertThat(last.get("method").asText()).isEqualTo("maxTimestamp");
		assertThat(last.has("note")).isFalse();
		assertThat(last.get("lastWriteTime").asText()).isEqualTo(Instant.ofEpochMilli(TS1).toString());
		assertThat(last.get("partition").asInt()).isEqualTo(1);
		assertThat(last.get("emptyPartitions").asInt()).isEqualTo(1);
		assertThat(last.at("/partitions/0/lastWriteTime").asText()).isEqualTo(Instant.ofEpochMilli(TS0).toString());
		assertThat(last.at("/partitions/0/offset").asLong()).isBetween(0L, 9L);
		assertThat(last.at("/partitions/2/empty").asBoolean()).isTrue();

		String rateTopic = unique("zkrate");
		TestData.createTopic(zkAdmin(), rateTopic, 1, Map.of());
		AtomicBoolean running = new AtomicBoolean(true);
		Thread writer = Thread.ofVirtual().start(() -> {
			try (KafkaProducer<byte[], byte[]> p = ZkKafkaTestCluster.producer(null)) {
				while (running.get()) {
					p.send(new ProducerRecord<>(rateTopic, 0, null, new byte[] { 1 })).get();
					Thread.sleep(20);
				}
			}
			catch (Exception e) {
				// test finished
			}
		});
		try {
			// do not start measuring before the writer sends its first messages
			awaitLatest(rateTopic, 1);
			JsonNode rate = ok("topics_write_rate", zkArgs("topic", rateTopic, "intervalSeconds", 2));
			assertThat(rate.get("topic").asText()).isEqualTo(rateTopic);
			assertThat(rate.get("totalMessages").asLong()).isPositive();
			assertThat(rate.get("totalMsgsPerSec").asDouble()).isPositive();
			assertThat(rate.at("/partitions/0/partition").asInt()).isZero();
		}
		finally {
			running.set(false);
			writer.join(10_000);
		}
	}

	@Test
	void topicsConsumersShowsClassicGroupsOnZooKeeperAndConsumerGroupsOnKraft() throws Exception {
		String empty = emptyGroup;
		String live = unique("zk-g-live");
		try (KafkaConsumer<byte[], byte[]> c = consumer(ZkKafkaTestCluster.clientProperties("admin"), live, "classic")) {
			c.subscribe(List.of(topic));
			awaitAssignment(c);
			c.commitSync(Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(9)));

			JsonNode r = ok("topics_consumers", zkArgs("topic", topic, "filter", "^zk-g-"));
			Map<String, JsonNode> groups = byGroup(r.get("groups"));
			JsonNode e = groups.get(empty);
			assertThat(e.get("type").asText()).isEqualTo("Classic");
			assertThat(e.get("state").asText()).isEqualToIgnoringCase("empty");
			assertThat(e.get("members").asInt()).isZero();
			assertThat(e.get("lag").asLong()).isEqualTo(6);
			assertThat(e.get("partitions").asInt()).isEqualTo(2);
			JsonNode l = groups.get(live);
			// consumer groups on a ZooKeeper cluster are always Classic
			assertThat(l.get("type").asText()).isEqualTo("Classic");
			assertThat(l.get("state").asText()).isEqualToIgnoringCase("stable");
			assertThat(l.get("members").asInt()).isEqualTo(1);
			assertThat(l.get("lag").asLong()).isEqualTo(1);
			assertThat(r.has("partial")).isFalse();
		}

		String kip848 = unique("kr-g-consumer");
		try (KafkaConsumer<byte[], byte[]> c = consumer(KafkaTestCluster.clientProperties("admin"), kip848, "consumer")) {
			c.subscribe(List.of(kraftTopic));
			awaitAssignment(c);
			c.commitSync(Map.of(new TopicPartition(kraftTopic, 0), new OffsetAndMetadata(1)));

			JsonNode r = ok("topics_consumers", args("topic", kraftTopic, "filter", "^kr-g-"));
			JsonNode g = byGroup(r.get("groups")).get(kip848);
			// KRaft 4.x: a KIP-848 (group.protocol=consumer) group has the Consumer type
			assertThat(g.get("type").asText()).isEqualTo("Consumer");
			assertThat(g.get("members").asInt()).isEqualTo(1);
			assertThat(g.get("lag").asLong()).isEqualTo(2);
		}
	}

	@Test
	void topicsProducersAndTransactionsOnZooKeeper() throws Exception {
		String txTopic = unique("zktx");
		TestData.createTopic(zkAdmin(), txTopic, 2, Map.of());
		String txId = unique("zk-txid");
		try (KafkaProducer<byte[], byte[]> idempotent = ZkKafkaTestCluster
			.producer(Map.of(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true));
				KafkaProducer<byte[], byte[]> tx = ZkKafkaTestCluster
					.producer(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId))) {
			idempotent.send(new ProducerRecord<>(txTopic, 1, null, new byte[] { 1 })).get();
			tx.initTransactions();
			tx.beginTransaction();
			tx.send(new ProducerRecord<>(txTopic, 0, null, new byte[] { 2 })).get();
			tx.flush();

			JsonNode p = ok("topics_producers", zkArgs("topic", txTopic));
			assertThat(p.get("partitionsScanned").asInt()).isEqualTo(2);
			assertThat(p.get("producerCount").asInt()).isEqualTo(2);
			assertThat(p.get("partitionsWithOpenTransaction").asInt()).isEqualTo(1);
			assertThat(p.has("note")).isFalse();
			assertThat(p.has("unsupportedPartitions")).isFalse();
			boolean open = false;
			for (JsonNode row : p.get("producers")) {
				assertThat(row.get("producerId").asLong()).isNotNegative();
				assertThat(row.get("lastTimestamp").asText()).endsWith("Z");
				if (row.has("currentTransactionStartOffset")) {
					open = true;
					assertThat(row.get("partition").asInt()).isZero();
				}
			}
			assertThat(open).isTrue();

			JsonNode t = ok("topics_transactions", zkArgs("topic", txTopic));
			assertThat(t.get("openTransactionsInCluster").asInt()).isPositive();
			JsonNode txn = t.get("openTransactions").get(0);
			assertThat(txn.get("transactionalId").asText()).isEqualTo(txId);
			assertThat(txn.get("state").asText()).isEqualToIgnoringCase("ongoing");
			assertThat(txn.get("partitions").get(0).asInt()).isZero();
			JsonNode part = t.get("partitionsWithOpenTransaction").get(0);
			assertThat(part.get("producerId").asLong()).isEqualTo(txn.get("producerId").asLong());
			assertThat(t.get("hangingSuspects").asInt()).isZero();
			assertThat(t.has("notes")).isFalse();
			assertThat(t.has("errors")).isFalse();
			tx.abortTransaction();
		}
	}

	private static void awaitLatest(String t, long atLeast) throws Exception {
		TopicPartition tp = new TopicPartition(t, 0);
		long end = System.currentTimeMillis() + 30_000;
		while (zkAdmin().listOffsets(Map.of(tp, org.apache.kafka.clients.admin.OffsetSpec.latest()))
			.partitionResult(tp)
			.get()
			.offset() < atLeast) {
			assertThat(System.currentTimeMillis()).as("writer did not start").isLessThan(end);
			Thread.sleep(50);
		}
	}

	private static KafkaConsumer<byte[], byte[]> consumer(Map<String, Object> base, String group, String protocol) {
		Map<String, Object> p = new LinkedHashMap<>(base);
		p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		p.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol);
		p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		return new KafkaConsumer<>(p);
	}

	private static void awaitAssignment(KafkaConsumer<byte[], byte[]> c) {
		long end = System.currentTimeMillis() + 60_000;
		while (c.assignment().isEmpty()) {
			assertThat(System.currentTimeMillis()).as("no partition assigned to the consumer").isLessThan(end);
			c.poll(Duration.ofMillis(200));
		}
	}

	private static Map<String, JsonNode> byGroup(JsonNode groups) {
		Map<String, JsonNode> out = new LinkedHashMap<>();
		groups.forEach(g -> out.put(g.get("groupId").asText(), g));
		return out;
	}

	private static List<String> names(JsonNode array) {
		return names(array, "name");
	}

	private static List<String> names(JsonNode array, String field) {
		List<String> out = new ArrayList<>();
		array.forEach(n -> out.add(n.path(field).asText()));
		return out;
	}

	private static ToolCall call(KafkaClients clients, String cluster) {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("cluster", cluster);
		return new ToolCall("test", args, clients, Instant.now().plusMillis(KafkaClients.TIMEOUT_MS));
	}

	private static ClusterConfig cluster(String name, String bootstrap) {
		ClusterConfig c = new ClusterConfig();
		c.setName(name);
		c.setBootstrapServers(bootstrap);
		c.setProperties(KafkaTestCluster.saslProperties("alice"));
		return c;
	}

}
