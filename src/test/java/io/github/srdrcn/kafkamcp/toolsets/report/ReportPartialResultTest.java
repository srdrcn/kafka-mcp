package io.github.srdrcn.kafkamcp.toolsets.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;

/**
 * Yanıt vermeyen broker/koordinatör: maxTimestamp listOffsets'i ve seçili grupların offset sorgusu hiç
 * tamamlanmaz. Rapor deadline'dan önce kısmi sonuçla döner ve bunu {@code partial} ile belirtir.
 */
class ReportPartialResultTest extends ReportTestBase {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	@Autowired
	private KafkaClients clients;

	@Autowired
	private TopicUsageTools usageTools;

	@Autowired
	private InventoryTools inventoryTools;

	@Autowired
	private ClusterReportTools clusterTools;

	@Autowired
	private AuditTools auditTools;

	/** maxTimestamp sorgularını ve hangGroups'un offset sorgularını asılı bırakan Admin proxy'si. */
	static Admin hanging(Admin real, Set<String> hangGroups) {
		return proxy(real, hangGroups, false);
	}

	/**
	 * maxTimestamp: unsupported true ise eski broker gibi UnsupportedVersionException ile hemen biter, değilse
	 * hiç tamamlanmaz; hangGroups'un offset sorguları hiç tamamlanmaz.
	 */
	static Admin proxy(Admin real, Set<String> hangGroups, boolean unsupported) {
		return (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] { Admin.class },
				(proxy, method, args) -> {
					if (method.getName().equals("listOffsets") && args != null && args[0] instanceof Map<?, ?> specs
							&& specs.values().stream().anyMatch(s -> s instanceof OffsetSpec.MaxTimestampSpec)) {
						Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new HashMap<>();
						specs.keySet().forEach(tp -> {
							KafkaFutureImpl<ListOffsetsResultInfo> f = new KafkaFutureImpl<>();
							if (unsupported) {
								f.completeExceptionally(new UnsupportedVersionException(
										"Broker does not support MAX_TIMESTAMP offset spec"));
							}
							futures.put((TopicPartition) tp, f);
						});
						return new ListOffsetsResult(futures);
					}
					if (method.getName().equals("listConsumerGroupOffsets") && args != null
							&& args[0] instanceof Map<?, ?>) {
						return groupOffsets(real, args, hangGroups);
					}
					try {
						return method.invoke(real, args);
					}
					catch (InvocationTargetException e) {
						throw e.getCause();
					}
				});
	}

	@SuppressWarnings("unchecked")
	private static ListConsumerGroupOffsetsResult groupOffsets(Admin real, Object[] args, Set<String> hangGroups)
			throws ReflectiveOperationException {
		Map<String, ListConsumerGroupOffsetsSpec> specs = (Map<String, ListConsumerGroupOffsetsSpec>) args[0];
		ListConsumerGroupOffsetsOptions options = args.length > 1 ? (ListConsumerGroupOffsetsOptions) args[1]
				: new ListConsumerGroupOffsetsOptions();
		Map<String, ListConsumerGroupOffsetsSpec> ok = new LinkedHashMap<>(specs);
		ok.keySet().removeAll(hangGroups);
		ListConsumerGroupOffsetsResult r = ok.isEmpty() ? null : real.listConsumerGroupOffsets(ok, options);
		Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = new HashMap<>();
		for (String g : specs.keySet()) {
			futures.put(CoordinatorKey.byGroupId(g),
					hangGroups.contains(g) ? new KafkaFutureImpl<>() : r.partitionsToOffsetAndMetadata(g));
		}
		Constructor<ListConsumerGroupOffsetsResult> c = ListConsumerGroupOffsetsResult.class
			.getDeclaredConstructor(Map.class);
		c.setAccessible(true);
		return c.newInstance(futures);
	}

	/**
	 * Bozuk cluster: listGroups tüm broker'larda hata verir (grup taraması başarısız), describeLogDirs yetkisiz
	 * (boyutlar bilinmiyor), listPartitionReassignments yetkisiz; diğer istekler gerçek Admin'e gider.
	 */
	static Admin degraded(Admin real) {
		return (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] { Admin.class },
				(proxy, method, args) -> {
					switch (method.getName()) {
						case "listGroups" -> {
							return newResult(ListGroupsResult.class, KafkaFuture.class, KafkaFuture
								.completedFuture(List.<Object>of(new CoordinatorNotAvailableException("koordinatör yok"))));
						}
						case "describeLogDirs" -> {
							Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> futures = new HashMap<>();
							for (Object id : (Collection<?>) args[0]) {
								futures.put((Integer) id, failed(new ClusterAuthorizationException("log dizinleri yetkisiz")));
							}
							return newResult(DescribeLogDirsResult.class, Map.class, futures);
						}
						case "listPartitionReassignments" -> {
							return newResult(ListPartitionReassignmentsResult.class, KafkaFuture.class,
									failed(new ClusterAuthorizationException("reassignment listesi yetkisiz")));
						}
						default -> {
							try {
								return method.invoke(real, args);
							}
							catch (InvocationTargetException e) {
								throw e.getCause();
							}
						}
					}
				});
	}

	private static <T> KafkaFuture<T> failed(Throwable error) {
		KafkaFutureImpl<T> f = new KafkaFutureImpl<>();
		f.completeExceptionally(error);
		return f;
	}

	/** Admin sonuç sınıflarının paket-özel kurucusuyla örnek oluşturur. */
	private static <T> T newResult(Class<T> type, Class<?> argType, Object arg) throws ReflectiveOperationException {
		Constructor<T> c = type.getDeclaredConstructor(argType);
		c.setAccessible(true);
		return c.newInstance(arg);
	}

	private ToolCall call(String tool, Map<String, Object> args, Admin admin, long deadlineMillis) {
		KafkaClients faulty = new KafkaClients(clients.config()) {
			@Override
			public Admin admin(String cluster) {
				return admin;
			}
		};
		return new ToolCall(tool, args, faulty, Instant.now().plusMillis(deadlineMillis));
	}

	@Test
	void hangingBrokerRequestsYieldFlaggedPartialResultBeforeTheDeadline() throws Exception {
		String p = unique("rep-partial");
		String topic = p + "-a";
		createTopic(false, topic, 2, Map.of());
		produce(false, topic, 0, 3, null);
		String stuck = p + "-stuck";
		commit(false, stuck, topic, 0, 1);
		commit(false, p + "-fine", topic, 0, 2);
		Admin admin = hanging(testAdmin(), Set.of(stuck));

		long t0 = System.nanoTime();
		JsonNode idle = MAPPER.valueToTree(usageTools.idleTopics(call("report_idle_topics", args("filter", p), admin, 6_000)));
		assertThat((System.nanoTime() - t0) / 1_000_000).isLessThan(6_000);
		assertThat(idle.get("partial").asBoolean()).isTrue();
		assertThat(idle.get("idleCount").asInt()).isZero();
		assertThat(idle.at("/unknown/count").asInt()).isEqualTo(1);
		assertThat(idle.at("/unknown/byReason").toString()).contains("yanıt gelmedi").contains(topic);

		JsonNode noCons = MAPPER.valueToTree(
				usageTools.topicsWithoutConsumers(call("report_topics_without_consumers", args("filter", p), admin, 8_000)));
		assertThat(noCons.get("partial").asBoolean()).isTrue();
		assertThat(noCons.at("/skipped/groupsNoResponse").asInt()).isEqualTo(1);
		assertThat(noCons.get("uncertain").asBoolean()).isTrue();
		// "fine" grubunun commit'i görüldüğü için topic listede değil
		assertThat(noCons.get("count").asInt()).isZero();
	}

	@Test
	void failingGroupLogDirAndReassignmentRequestsDegradeReportsInsteadOfFailingThem() throws Exception {
		String p = unique("rep-degraded");
		String never = p + "-never";
		String infinite = p + "-inf";
		createTopic(false, never, 1, Map.of());
		createTopic(false, infinite, 1, Map.of("retention.ms", "-1"));
		produce(false, infinite, 0, 5, null);
		List<AclBinding> ghost = List.of(new AclBinding(
				new ResourcePattern(ResourceType.TOPIC, p + "-ghost", PatternType.LITERAL),
				new AccessControlEntry("User:" + p, "*", AclOperation.READ, AclPermissionType.ALLOW)));
		InventoryReportTest.createAcls(testAdmin(), ghost);
		Admin admin = degraded(testAdmin());
		try {
			// temizlik: grup taraması başarısız ama topic ve ACL adayları yine üretilir
			JsonNode cleanup = MAPPER.valueToTree(inventoryTools
				.cleanupCandidates(call("report_cleanup_candidates", args("filter", p), admin, 15_000)));
			assertThat(cleanup.get("partial").asBoolean()).isTrue();
			assertThat(cleanup.at("/errors/consumerGroups").asText()).contains("koordinatör");
			JsonNode candidates = cleanup.get("candidates");
			assertThat(texts(candidates, "name")).containsExactlyInAnyOrder(never, p + "-ghost");
			JsonNode neverRow = find(candidates, "name", never);
			assertThat(texts(neverRow.get("reasons"), null)).containsExactly("never_written");
			assertThat(neverRow.get("risk").asText()).isEqualTo("medium");
			assertThat(neverRow.get("consumersUnknown").asBoolean()).isTrue();
			assertThat(find(candidates, "name", p + "-ghost").get("reasons").get(0).asText())
				.isEqualTo("orphan_topic_acl");
			assertThat(cleanup.get("notes").toString()).contains("consumersUnknown").contains("sahipsiz GROUP ACL");

			// özet: reassignment hatası ve eksik boyutlar bildirilir, grup hatası tek alanda kalır
			JsonNode summary = MAPPER
				.valueToTree(clusterTools.clusterSummary(call("report_cluster_summary", args(), admin, 15_000)));
			assertThat(summary.at("/errors/listPartitionReassignments").asText()).contains("yetki");
			assertThat(summary.get("notes").toString()).contains("Reassignment listesi alınamadı");
			assertThat(summary.at("/consumerGroups/error").asText()).contains("koordinatör");
			assertThat(summary.at("/size/complete").asBoolean(true)).isFalse();
			assertThat(summary.at("/replication/underReplicatedPartitions").asInt()).isZero();

			// retention: boyutu bilinmeyen sınırsız topic "küçük" sayılmaz
			JsonNode retention = MAPPER.valueToTree(
					auditTools.retentionAudit(call("report_retention_audit", args("filter", p), admin, 15_000)));
			JsonNode unknown = find(retention.get("findings"), "check", "infinite_retention_size_unknown");
			assertThat(unknown.get("severity").asText()).isEqualTo("medium");
			assertThat(texts(unknown.get("items"), "topic")).containsExactly(infinite);
			assertThat(unknown.at("/items/0/value").asText()).isEqualTo("-1");
			assertThat(find(retention.get("findings"), "check", "infinite_retention").isMissingNode()).isTrue();
			assertThat(retention.at("/errors/logDirs").size()).isPositive();
		}
		finally {
			InventoryReportTest.deleteAcls(testAdmin(), ghost);
		}
	}

	@Test
	void unsupportedMaxTimestampMakesLastWriteUnknownInsteadOfFailing() throws Exception {
		String p = unique("rep-oldbroker");
		String written = p + "-written";
		String empty = p + "-empty";
		createTopic(false, written, 1, Map.of());
		createTopic(false, empty, 1, Map.of());
		produce(false, written, 0, 2, null);
		Admin admin = proxy(testAdmin(), Set.of(), true);

		JsonNode idle = MAPPER.valueToTree(usageTools.idleTopics(call("report_idle_topics", args("filter", p), admin, 15_000)));
		assertThat(idle.has("partial")).isFalse();
		assertThat(idle.get("idleCount").asInt()).isZero();
		assertThat(idle.at("/empty/count").asInt()).isEqualTo(1);
		assertThat(idle.at("/unknown/count").asInt()).isEqualTo(1);
		assertThat(idle.at("/unknown/byReason").toString()).contains("Kafka < 3.0").contains(written);
		assertThat(idle.get("notes").toString()).contains("maxTimestamp");
	}

}
