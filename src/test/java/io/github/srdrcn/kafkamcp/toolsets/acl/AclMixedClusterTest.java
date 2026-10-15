package io.github.srdrcn.kafkamcp.toolsets.acl;

import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.aclsOf;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.allowTopic;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.binding;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.create;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.createTopic;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.deleteAllFor;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.denyTopic;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.field;
import static io.github.srdrcn.kafkamcp.toolsets.acl.AclTestSupport.find;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.support.KafkaTestCluster;
import io.github.srdrcn.kafkamcp.support.MixedClusterIntegrationTest;
import io.github.srdrcn.kafkamcp.support.TestConfigs;
import io.github.srdrcn.kafkamcp.support.ZkKafkaTestCluster;

/**
 * acl ACL tools on a ZooKeeper (AclAuthorizer, Kafka 3.9) cluster: acls_list, acls_explain,
 * acls_who_can_access_topic, acls_principal_permissions, acls_audit, acls_create, acls_delete. Decisions are
 * compared with real broker authorization in both modes.
 */
class AclMixedClusterTest extends MixedClusterIntegrationTest {

	private static final String ACL_AUTHORIZER = "kafka.security.authorizer.AclAuthorizer";

	private static final String STANDARD_AUTHORIZER = "org.apache.kafka.metadata.authorizer.StandardAuthorizer";

	private static boolean zk(String cluster) {
		return ZK_CLUSTER.equals(cluster);
	}

	private static Admin adminOf(String cluster) {
		return zk(cluster) ? zkAdmin() : testAdmin();
	}

	private static Map<String, Object> clientProps(String cluster, String user) {
		return zk(cluster) ? ZkKafkaTestCluster.clientProperties(user) : KafkaTestCluster.clientProperties(user);
	}

	private static Map<String, Object> argsOn(String cluster, Object... kv) {
		Map<String, Object> m = args(kv);
		m.put("cluster", cluster);
		return m;
	}

	private JsonNode explain(String cluster, String principal, String op, String type, String name, Object... extra) {
		Object[] kv = new Object[8 + extra.length];
		Object[] base = { "principal", principal, "operation", op, "resourceType", type, "resourceName", name };
		System.arraycopy(base, 0, kv, 0, 8);
		System.arraycopy(extra, 0, kv, 8, extra.length);
		return ok("acls_explain", argsOn(cluster, kv));
	}

	/** describeTopics as the user: null on success, otherwise the root Kafka error. */
	private static Throwable describeError(String cluster, String user, String topic) {
		Map<String, Object> props = clientProps(cluster, user);
		props.put("default.api.timeout.ms", 10_000);
		props.put("request.timeout.ms", 5_000);
		try (Admin a = Admin.create(props)) {
			a.describeTopics(List.of(topic)).allTopicNames().get(15, TimeUnit.SECONDS);
			return null;
		}
		catch (Exception e) {
			return root(e);
		}
	}

	/** Produces a single message to the ZooKeeper cluster as the user: null on success, otherwise the root Kafka error. */
	private static Throwable zkProduceError(String user, String topic) {
		Map<String, Object> o = new java.util.HashMap<>(KafkaTestCluster.saslProperties(user));
		o.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
		o.put(ProducerConfig.ACKS_CONFIG, "1");
		o.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
		o.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
		o.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
		try (KafkaProducer<byte[], byte[]> producer = ZkKafkaTestCluster.producer(o)) {
			producer.send(new ProducerRecord<>(topic, "v".getBytes(StandardCharsets.UTF_8))).get(15, TimeUnit.SECONDS);
			return null;
		}
		catch (Exception e) {
			return root(e);
		}
	}

	private static Throwable root(Throwable e) {
		Throwable t = e;
		while (t.getCause() != null && !(t instanceof KafkaException)) {
			t = t.getCause();
		}
		return t;
	}

	private static void deleteTopics(Admin admin, String... topics) {
		try {
			admin.deleteTopics(List.of(topics)).all().get(15, TimeUnit.SECONDS);
		}
		catch (Exception ignored) {
			// cleanup
		}
	}

	/**
	 * acls_explain matches the broker's real decision in both modes: PREFIXED READ → implied DESCRIBE (ALLOW),
	 * LITERAL DENY (DENY), principal without ACLs (DENY). The authorizer field is correct for the mode.
	 */
	@ParameterizedTest
	@ValueSource(strings = { ZK_CLUSTER, TestConfigs.CLUSTER })
	void explainMatchesRealAuthorizerInBothModes(String cluster) throws Exception {
		Admin admin = adminOf(cluster);
		String prefix = unique("mxexp");
		String open = prefix + "-open";
		String denied = prefix + "-denied";
		createTopic(admin, open);
		createTopic(admin, denied);
		AclBinding allowPrefixed = binding(ResourceType.TOPIC, prefix, PatternType.PREFIXED, "User:alice",
				AclOperation.READ, AclPermissionType.ALLOW);
		AclBinding denyLiteral = denyTopic(denied, "User:alice", AclOperation.DESCRIBE);
		create(admin, allowPrefixed, denyLiteral);
		try {
			JsonNode allow = explain(cluster, "alice", "DESCRIBE", "TOPIC", open);
			assertThat(allow.get("decision").asText()).isEqualTo("ALLOW");
			assertThat(allow.get("reason").asText()).contains("READ").contains("is implied by");
			assertThat(allow.at("/matchedAllow/0/patternType").asText()).isEqualTo("PREFIXED");
			assertThat(allow.get("authorizer").asText()).isEqualTo(zk(cluster) ? ACL_AUTHORIZER : STANDARD_AUTHORIZER);
			assertThat(allow.path("notes").toString()).doesNotContain("is not built in");
			await().atMost(Duration.ofSeconds(30))
				.pollInterval(Duration.ofMillis(500))
				.untilAsserted(() -> assertThat(describeError(cluster, "alice", open)).isNull());

			JsonNode deny = explain(cluster, "User:alice", "DESCRIBE", "TOPIC", denied);
			assertThat(deny.get("decision").asText()).isEqualTo("DENY");
			assertThat(deny.get("reason").asText()).contains("DENY");
			assertThat(deny.at("/matchedDeny/0/resourceName").asText()).isEqualTo(denied);
			assertThat(deny.get("resourceAclCount").asInt()).isEqualTo(2);
			await().atMost(Duration.ofSeconds(30))
				.pollInterval(Duration.ofMillis(500))
				.untilAsserted(() -> assertThat(describeError(cluster, "alice", denied))
					.isInstanceOf(TopicAuthorizationException.class));

			JsonNode bob = explain(cluster, "bob", "DESCRIBE", "TOPIC", open);
			assertThat(bob.get("decision").asText()).isEqualTo("DENY");
			assertThat(bob.get("reason").asText()).contains("no matching ALLOW");
			assertThat(describeError(cluster, "bob", open)).isInstanceOf(TopicAuthorizationException.class);

			// super.users is hidden in both modes: a super user given as parameter gets ALLOW
			JsonNode admin1 = explain(cluster, "admin", "DESCRIBE", "TOPIC", denied, "superUsers", List.of("User:admin"));
			assertThat(admin1.get("decision").asText()).isEqualTo("ALLOW");
			assertThat(admin1.get("superUser").asBoolean()).isTrue();
			assertThat(describeError(cluster, "admin", denied)).isNull();
		}
		finally {
			AclTestSupport.delete(admin, allowPrefixed, denyLiteral);
			deleteTopics(admin, open, denied);
		}
	}

	@Test
	void listFiltersAndMatchOnZooKeeper() throws Exception {
		String p = "User:" + unique("mxlist");
		String prefix = unique("mxl");
		String topic = prefix + "-orders";
		String group = prefix + "-g";
		AclBinding literal = allowTopic(topic, p, AclOperation.READ);
		AclBinding prefixed = binding(ResourceType.TOPIC, prefix, PatternType.PREFIXED, p, AclOperation.WRITE,
				AclPermissionType.ALLOW);
		AclBinding groupRead = binding(ResourceType.GROUP, group, PatternType.LITERAL, p, "10.1.2.3", AclOperation.READ,
				AclPermissionType.DENY);
		create(zkAdmin(), literal, prefixed, groupRead);
		try {
			JsonNode all = ok("acls_list", zkArgs("principal", p));
			assertThat(all.get("total").asInt()).isEqualTo(3);
			assertThat(field(all.get("acls"), "resourceType")).containsExactly("GROUP", "TOPIC", "TOPIC");
			JsonNode g = find(all.get("acls"), "resourceType", "GROUP");
			assertThat(g.get("host").asText()).isEqualTo("10.1.2.3");
			assertThat(g.get("permission").asText()).isEqualTo("DENY");

			JsonNode match = ok("acls_list",
					zkArgs("principal", p, "resourceType", "TOPIC", "resourceName", topic, "patternType", "MATCH"));
			assertThat(match.get("total").asInt()).isEqualTo(2);
			assertThat(field(match.get("acls"), "patternType")).containsExactlyInAnyOrder("LITERAL", "PREFIXED");

			JsonNode deny = ok("acls_list", zkArgs("principal", p, "permission", "DENY"));
			assertThat(deny.get("total").asInt()).isEqualTo(1);
			assertThat(ok("acls_list", zkArgs("filter", prefix + "-orders$")).get("total").asInt()).isEqualTo(1);
			// the same principal does not exist on the KRaft cluster: clusters do not mix
			assertThat(ok("acls_list", args("principal", p)).get("total").asInt()).isZero();
		}
		finally {
			deleteAllFor(zkAdmin(), p);
		}
	}

	@Test
	void whoCanAccessTopicOnZooKeeper() throws Exception {
		String prefix = unique("mxwho");
		String topic = prefix + "-t";
		createTopic(zkAdmin(), topic);
		String alice = "User:" + prefix + "-alice";
		String bob = "User:" + prefix + "-bob";
		create(zkAdmin(), allowTopic(topic, alice, AclOperation.READ),
				binding(ResourceType.TOPIC, prefix, PatternType.PREFIXED, bob, AclOperation.READ, AclPermissionType.ALLOW),
				denyTopic(topic, bob, AclOperation.READ));
		try {
			JsonNode r = ok("acls_who_can_access_topic", zkArgs("topic", topic, "superUsers", List.of("admin")));
			assertThat(r.get("topicExists").asBoolean()).isTrue();
			assertThat(r.get("aclCount").asInt()).isEqualTo(3);
			// super.users is hidden on ZooKeeper 3.9 too: the list given as parameter is used
			assertThat(r.get("superUsers").toString()).contains("User:admin");
			assertThat(r.get("notes").toString()).contains("list from the superUsers parameter");
			JsonNode read = r.at("/operations/READ");
			assertThat(field(read.get("allowed"), "principal")).containsExactly(alice);
			assertThat(find(read.get("allowed"), "principal", alice).at("/via/0/resourceName").asText()).isEqualTo(topic);
			assertThat(field(read.get("denied"), "principal")).containsExactly(bob);
			assertThat(find(read.get("denied"), "principal", bob).at("/via/0/permission").asText()).isEqualTo("DENY");
			JsonNode write = r.at("/operations/WRITE");
			assertThat(write.get("allowed")).isEmpty();

			JsonNode missing = ok("acls_who_can_access_topic", zkArgs("topic", prefix + "-missing"));
			assertThat(missing.get("topicExists").asBoolean()).isFalse();
			// bob's PREFIXED ACL also applies to the missing topic
			assertThat(field(missing.at("/operations/READ/allowed"), "principal")).containsExactly(bob);
		}
		finally {
			deleteAllFor(zkAdmin(), alice);
			deleteAllFor(zkAdmin(), bob);
			deleteTopics(zkAdmin(), topic);
		}
	}

	@Test
	void principalPermissionsOnZooKeeper() throws Exception {
		String p = "User:" + unique("mxperm");
		String prefix = unique("mxp");
		String topic = prefix + "-in";
		create(zkAdmin(), allowTopic(topic, p, AclOperation.READ),
				binding(ResourceType.TOPIC, prefix, PatternType.PREFIXED, p, AclOperation.WRITE, AclPermissionType.ALLOW),
				denyTopic(prefix + "-secret", p, AclOperation.WRITE));
		try {
			JsonNode r = ok("acls_principal_permissions", zkArgs("principal", p, "includeWildcardPrincipal", false));
			assertThat(r.at("/summary/bindings").asInt()).isEqualTo(3);
			assertThat(r.at("/summary/allow").asInt()).isEqualTo(2);
			assertThat(r.at("/summary/deny").asInt()).isEqualTo(1);
			assertThat(r.at("/summary/byResourceType/TOPIC").asInt()).isEqualTo(3);
			JsonNode in = find(r.get("resources"), "resourceName", topic);
			// LITERAL READ + covering PREFIXED WRITE → READ, WRITE, DESCRIBE
			assertThat(in.get("effectiveAllow").toString()).contains("READ", "WRITE", "DESCRIBE");
			assertThat(in.get("alsoAllowedBy").toString()).contains("PREFIXED:" + prefix);
			JsonNode secret = find(r.get("resources"), "resourceName", prefix + "-secret");
			assertThat(secret.get("deny").toString()).contains("WRITE");
			assertThat(secret.get("effectiveAllow").toString()).doesNotContain("WRITE").contains("DESCRIBE");
			assertThat(r.get("superUser").asBoolean()).isFalse();
		}
		finally {
			deleteAllFor(zkAdmin(), p);
		}
	}

	@Test
	void auditFindsRiskyAndOrphanAclsOnZooKeeper() throws Exception {
		String token = unique("mxaud");
		String p = "User:" + token + "-svc";
		String topic = token + "-live";
		String group = token + "-grp";
		createTopic(zkAdmin(), topic);
		// create the group on the ZooKeeper cluster (empty Classic group with a committed offset)
		zkAdmin().alterConsumerGroupOffsets(group, Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(0)))
			.all()
			.get(15, TimeUnit.SECONDS);
		AclBinding everyone = binding(ResourceType.TOPIC, token + "-public", PatternType.LITERAL, "User:*",
				AclOperation.READ, AclPermissionType.ALLOW);
		create(zkAdmin(), allowTopic(topic, p, AclOperation.READ), allowTopic(token + "-gone", p, AclOperation.READ),
				binding(ResourceType.TOPIC, token + "-nomatch", PatternType.PREFIXED, p, AclOperation.ALL,
						AclPermissionType.ALLOW),
				binding(ResourceType.GROUP, group, PatternType.LITERAL, p, AclOperation.READ, AclPermissionType.ALLOW),
				binding(ResourceType.GROUP, token + "-oldgrp", PatternType.LITERAL, p, AclOperation.READ,
						AclPermissionType.ALLOW),
				binding(ResourceType.CLUSTER, Acls.CLUSTER_NAME, PatternType.LITERAL, p, AclOperation.ALTER,
						AclPermissionType.ALLOW),
				everyone);
		try {
			JsonNode r = ok("acls_audit", zkArgs("filter", token));
			assertThat(r.get("scannedAcls").asInt()).isEqualTo(7);
			JsonNode c = r.get("categories");
			assertThat(field(c.at("/allowAllPrincipals/samples"), "resourceName")).containsExactly(token + "-public");
			assertThat(field(c.at("/allowOperationAll/samples"), "resourceName")).containsExactly(token + "-nomatch");
			assertThat(field(c.at("/allowOnCluster/samples"), "principal")).containsExactly(p);
			assertThat(field(c.at("/orphanTopicAcls/samples"), "resourceName"))
				.containsExactlyInAnyOrder(token + "-gone", token + "-public");
			assertThat(field(c.at("/orphanGroupAcls/samples"), "resourceName")).containsExactly(token + "-oldgrp");
			assertThat(field(c.at("/unusedPrefixedTopicAcls/samples"), "resourceName"))
				.containsExactly(token + "-nomatch");
			assertThat(c.at("/unverifiedTopicAcls/count").asInt()).isZero();
			assertThat(r.get("topicCount").asInt()).isPositive();
			assertThat(r.get("groupCount").asInt()).isPositive();
			assertThat(r.get("notes").toString()).contains("super.users");
		}
		finally {
			deleteAllFor(zkAdmin(), p);
			AclTestSupport.delete(zkAdmin(), everyone);
			try {
				zkAdmin().deleteConsumerGroups(List.of(group)).all().get(15, TimeUnit.SECONDS);
			}
			catch (Exception ignored) {
				// cleanup
			}
			deleteTopics(zkAdmin(), topic);
		}
	}

	/**
	 * acls_create (preset + single DENY binding) and acls_delete on the ZooKeeper cluster: the effect is verified
	 * with zkAdmin and a real producer.
	 */
	@Test
	void createAndDeleteOnZooKeeper() throws Exception {
		String user = "alice";
		String p = "User:" + user;
		String topic = unique("mxcreate");
		String txId = topic + "-tx";
		String group = topic + "-g";
		createTopic(zkAdmin(), topic);
		try {
			assertThat(zkProduceError(user, topic)).isInstanceOf(TopicAuthorizationException.class);
			JsonNode producer = ok("acls_create",
					zkArgs("principal", user, "preset", "producer", "topic", topic, "transactionalId", txId));
			assertThat(producer.get("created").asInt()).isEqualTo(4);
			assertThat(producer.has("note")).isFalse();
			Collection<AclBinding> now = aclsOf(zkAdmin(), p);
			assertThat(now).contains(
					new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
							new AccessControlEntry(p, "*", AclOperation.WRITE, AclPermissionType.ALLOW)),
					new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
							new AccessControlEntry(p, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
					new AclBinding(new ResourcePattern(ResourceType.TRANSACTIONAL_ID, txId, PatternType.LITERAL),
							new AccessControlEntry(p, "*", AclOperation.WRITE, AclPermissionType.ALLOW)));
			await().atMost(Duration.ofSeconds(30))
				.pollInterval(Duration.ofMillis(500))
				.untilAsserted(() -> assertThat(zkProduceError(user, topic)).isNull());

			JsonNode deny = ok("acls_create", zkArgs("principal", p, "resourceType", "GROUP", "resourceName", group,
					"operation", "READ", "permission", "DENY", "host", "10.9.8.7"));
			assertThat(deny.at("/bindings/0/permission").asText()).isEqualTo("DENY");
			assertThat(aclsOf(zkAdmin(), p)).contains(new AclBinding(
					new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
					new AccessControlEntry(p, "10.9.8.7", AclOperation.READ, AclPermissionType.DENY)));

			JsonNode delTopic = ok("acls_delete",
					zkArgs("principal", user, "resourceType", "TOPIC", "resourceName", topic));
			assertThat(delTopic.get("deleted").asInt()).isEqualTo(2);
			assertThat(field(delTopic.get("bindings"), "operation")).containsExactlyInAnyOrder("WRITE", "DESCRIBE");
			assertThat(aclsOf(zkAdmin(), p)).noneMatch(b -> b.pattern().name().equals(topic))
				.anyMatch(b -> b.pattern().name().equals(txId));
			await().atMost(Duration.ofSeconds(30))
				.pollInterval(Duration.ofMillis(500))
				.untilAsserted(
						() -> assertThat(zkProduceError(user, topic)).isInstanceOf(TopicAuthorizationException.class));

			JsonNode delTx = ok("acls_delete", zkArgs("principal", user, "resourceType", "TRANSACTIONAL_ID",
					"resourceName", txId, "patternType", "ANY"));
			assertThat(delTx.get("deleted").asInt()).isEqualTo(2);
			JsonNode delGroup = ok("acls_delete",
					zkArgs("principal", user, "resourceType", "GROUP", "resourceName", group, "host", "10.9.8.7"));
			assertThat(delGroup.get("deleted").asInt()).isEqualTo(1);
			assertThat(aclsOf(zkAdmin(), p)).noneMatch(b -> b.pattern().name().startsWith(topic));
			JsonNode none = ok("acls_delete", zkArgs("principal", user, "resourceType", "TOPIC", "resourceName", topic));
			assertThat(none.get("deleted").asInt()).isZero();
			assertThat(none.get("note").asText()).contains("nothing was deleted");
		}
		finally {
			for (AclBinding b : aclsOf(zkAdmin(), p)) {
				if (b.pattern().name().startsWith(topic)) {
					AclTestSupport.delete(zkAdmin(), b);
				}
			}
			deleteTopics(zkAdmin(), topic);
		}
	}

}
