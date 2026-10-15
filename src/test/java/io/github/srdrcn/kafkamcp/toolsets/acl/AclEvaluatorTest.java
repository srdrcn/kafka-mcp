package io.github.srdrcn.kafkamcp.toolsets.acl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

/** Unit tests of the StandardAuthorizer rules (no Kafka needed). */
class AclEvaluatorTest {

	private static AclBinding acl(ResourceType type, String name, PatternType pattern, String principal,
			AclOperation op, AclPermissionType perm) {
		return acl(type, name, pattern, principal, "*", op, perm);
	}

	private static AclBinding acl(ResourceType type, String name, PatternType pattern, String principal, String host,
			AclOperation op, AclPermissionType perm) {
		return new AclBinding(new ResourcePattern(type, name, pattern), new AccessControlEntry(principal, host, op, perm));
	}

	private static AclEvaluator.Decision topic(List<AclBinding> acls, String principal, AclOperation op, String topic) {
		return new AclEvaluator(acls, Set.of(), false).authorize(principal, op, ResourceType.TOPIC, topic, "*");
	}

	@Test
	void literalAllowAndNoMatchingAllow() {
		List<AclBinding> acls = List.of(acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, "User:alice",
				AclOperation.READ, AclPermissionType.ALLOW));
		AclEvaluator.Decision ok = topic(acls, "User:alice", AclOperation.READ, "orders");
		assertThat(ok.allowed()).isTrue();
		assertThat(ok.matchedAllow()).hasSize(1);
		assertThat(ok.reason()).contains("ALLOW");

		AclEvaluator.Decision bob = topic(acls, "User:bob", AclOperation.READ, "orders");
		assertThat(bob.allowed()).isFalse();
		assertThat(bob.noAclOnResource()).isFalse();
		assertThat(bob.resourceAclCount()).isEqualTo(1);
		assertThat(bob.reason()).contains("no matching ALLOW");

		assertThat(topic(acls, "User:alice", AclOperation.WRITE, "orders").allowed()).isFalse();
		assertThat(topic(acls, "User:alice", AclOperation.READ, "orders-2").allowed()).isFalse();
		// a GROUP binding with the same name does not apply to the TOPIC
		List<AclBinding> group = List.of(acl(ResourceType.GROUP, "orders", PatternType.LITERAL, "User:alice",
				AclOperation.READ, AclPermissionType.ALLOW));
		assertThat(topic(group, "User:alice", AclOperation.READ, "orders").noAclOnResource()).isTrue();
	}

	@Test
	void denyWinsOverAllow() {
		List<AclBinding> acls = List.of(
				acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, "User:alice", AclOperation.ALL,
						AclPermissionType.ALLOW),
				acl(ResourceType.TOPIC, "ord", PatternType.PREFIXED, "User:*", AclOperation.WRITE,
						AclPermissionType.DENY));
		AclEvaluator.Decision d = topic(acls, "User:alice", AclOperation.WRITE, "orders");
		assertThat(d.allowed()).isFalse();
		assertThat(d.matchedDeny()).hasSize(1);
		assertThat(d.matchedAllow()).hasSize(1);
		assertThat(d.reason()).contains("DENY");
		assertThat(topic(acls, "User:alice", AclOperation.READ, "orders").allowed()).isTrue();
	}

	@Test
	void prefixedAndWildcardResourceAndPrincipal() {
		List<AclBinding> acls = List.of(
				acl(ResourceType.TOPIC, "pay-", PatternType.PREFIXED, "User:alice", AclOperation.READ,
						AclPermissionType.ALLOW),
				acl(ResourceType.TOPIC, "*", PatternType.LITERAL, "User:carol", AclOperation.DESCRIBE,
						AclPermissionType.ALLOW),
				acl(ResourceType.TOPIC, "public", PatternType.LITERAL, "User:*", AclOperation.READ,
						AclPermissionType.ALLOW));
		assertThat(topic(acls, "User:alice", AclOperation.READ, "pay-events").allowed()).isTrue();
		assertThat(topic(acls, "User:alice", AclOperation.READ, "pa").allowed()).isFalse();
		assertThat(topic(acls, "User:carol", AclOperation.DESCRIBE, "anything").allowed()).isTrue();
		assertThat(topic(acls, "User:dave", AclOperation.READ, "public").allowed()).isTrue();
		// the "*" literal binding counts as an ACL on every topic
		assertThat(topic(acls, "User:dave", AclOperation.READ, "other").noAclOnResource()).isFalse();
	}

	@Test
	void operationImplicationOnlyForAllow() {
		List<AclBinding> acls = List.of(
				acl(ResourceType.TOPIC, "t", PatternType.LITERAL, "User:alice", AclOperation.WRITE,
						AclPermissionType.ALLOW),
				acl(ResourceType.TOPIC, "t", PatternType.LITERAL, "User:alice", AclOperation.ALTER_CONFIGS,
						AclPermissionType.ALLOW),
				acl(ResourceType.TOPIC, "t", PatternType.LITERAL, "User:bob", AclOperation.READ,
						AclPermissionType.ALLOW),
				acl(ResourceType.TOPIC, "t", PatternType.LITERAL, "User:bob", AclOperation.READ,
						AclPermissionType.DENY));
		AclEvaluator.Decision describe = topic(acls, "User:alice", AclOperation.DESCRIBE, "t");
		assertThat(describe.allowed()).isTrue();
		assertThat(describe.reason()).contains("is implied by");
		assertThat(topic(acls, "User:alice", AclOperation.DESCRIBE_CONFIGS, "t").allowed()).isTrue();
		assertThat(topic(acls, "User:alice", AclOperation.READ, "t").allowed()).isFalse();
		// DENY READ does not deny DESCRIBE; ALLOW READ grants DESCRIBE
		assertThat(topic(acls, "User:bob", AclOperation.DESCRIBE, "t").allowed()).isTrue();
		assertThat(topic(acls, "User:bob", AclOperation.READ, "t").allowed()).isFalse();
	}

	@Test
	void hostMatching() {
		List<AclBinding> acls = List.of(acl(ResourceType.TOPIC, "t", PatternType.LITERAL, "User:alice", "10.0.0.1",
				AclOperation.READ, AclPermissionType.ALLOW));
		AclEvaluator e = new AclEvaluator(acls, Set.of(), false);
		assertThat(e.authorize("User:alice", AclOperation.READ, ResourceType.TOPIC, "t", "10.0.0.1").allowed())
			.isTrue();
		assertThat(e.authorize("User:alice", AclOperation.READ, ResourceType.TOPIC, "t", "10.0.0.2").allowed())
			.isFalse();
		assertThat(e.authorize("User:alice", AclOperation.READ, ResourceType.TOPIC, "t", "*").allowed()).isFalse();
	}

	@Test
	void superUserAndNoAclRule() {
		List<AclBinding> acls = List.of(acl(ResourceType.TOPIC, "t", PatternType.LITERAL, "User:admin",
				AclOperation.ALL, AclPermissionType.DENY));
		AclEvaluator.Decision su = new AclEvaluator(acls, Set.of("User:admin"), false).authorize("User:admin",
				AclOperation.WRITE, ResourceType.TOPIC, "t", "*");
		assertThat(su.allowed()).isTrue();
		assertThat(su.superUser()).isTrue();

		AclEvaluator.Decision open = new AclEvaluator(List.of(), Set.of(), true).authorize("User:x",
				AclOperation.READ, ResourceType.GROUP, "g", "*");
		assertThat(open.allowed()).isTrue();
		assertThat(open.noAclOnResource()).isTrue();
		assertThat(open.reason()).contains("allow.everyone.if.no.acl.found=true");

		AclEvaluator.Decision closed = new AclEvaluator(List.of(), Set.of(), false).authorize("User:x",
				AclOperation.READ, ResourceType.GROUP, "g", "*");
		assertThat(closed.allowed()).isFalse();
		assertThat(closed.reason()).contains("allow.everyone.if.no.acl.found=false");
	}

	@Test
	void effectiveAllowSummary() {
		assertThat(AclEvaluator.effectiveAllow(ResourceType.TOPIC, Set.of(AclOperation.READ), Set.of()))
			.containsExactly("READ", "DESCRIBE");
		assertThat(AclEvaluator.effectiveAllow(ResourceType.TOPIC, Set.of(AclOperation.ALL), Set.of()))
			.containsExactly("ALL");
		assertThat(AclEvaluator.effectiveAllow(ResourceType.GROUP, Set.of(AclOperation.ALL),
				Set.of(AclOperation.DELETE)))
			.containsExactly("READ", "DESCRIBE");
		assertThat(AclEvaluator.effectiveAllow(ResourceType.TOPIC, Set.of(AclOperation.WRITE),
				Set.of(AclOperation.ALL)))
			.isEmpty();
	}

	@Test
	void auditPrefixLookup() {
		java.util.TreeSet<String> topics = new java.util.TreeSet<>(List.of("app-a", "orders", "pay-1"));
		assertThat(AclListTools.anyStartsWith(topics, "pay-")).isTrue();
		assertThat(AclListTools.anyStartsWith(topics, "app-")).isTrue();
		assertThat(AclListTools.anyStartsWith(topics, "zzz")).isFalse();
		assertThat(AclListTools.anyStartsWith(topics, "orders-")).isFalse();
	}

	@Test
	void principalNormalizationAndJaasMasking() {
		assertThat(Acls.normalizePrincipal("alice")).isEqualTo("User:alice");
		assertThat(Acls.normalizePrincipal("Group:ops")).isEqualTo("Group:ops");
		String masked = ClientSecurity.maskJaas(
				"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"a\" password=\"s3cr\\\"et\";");
		assertThat(masked).doesNotContain("s3cr").contains("password=\"****\"").contains("username=\"a\"");
		assertThat(ClientSecurity.jaasOption("Krb5LoginModule required keyTab=\"/k/a.keytab\" principal=\"svc@R\";",
				"keyTab"))
			.isEqualTo("/k/a.keytab");
		assertThat(BrokerSecurity.parseSuperUsers("User:admin; User:ANONYMOUS")).containsExactly("User:admin",
				"User:ANONYMOUS");
	}

	/** Kafka JaasConfig also accepts single quotes: masking and option parsing must support both. */
	@Test
	void singleQuotedJaasValues() {
		String masked = ClientSecurity.maskJaas(
				"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"svc\" password='p4ss;w0rd x';");
		assertThat(masked).doesNotContain("p4ss").doesNotContain("w0rd").contains("password=\"****\";")
			.contains("username=\"svc\"");
		assertThat(ClientSecurity.maskJaas("X required secret='a\\'b' token=abc;")).doesNotContain("a\\'b", "abc");
		String krb = "Krb5LoginModule required useKeyTab=true keyTab='/k/my svc.keytab' principal='svc@R';";
		assertThat(ClientSecurity.jaasOption(krb, "keyTab")).isEqualTo("/k/my svc.keytab");
		assertThat(ClientSecurity.jaasOption(krb, "principal")).isEqualTo("svc@R");
		assertThat(ClientSecurity.jaasOption(krb, "useKeyTab")).isEqualTo("true");
	}

	@Test
	void patternCoverage() {
		ResourcePattern payments = new ResourcePattern(ResourceType.TOPIC, "payments", PatternType.LITERAL);
		ResourcePattern pay = new ResourcePattern(ResourceType.TOPIC, "pay", PatternType.PREFIXED);
		ResourcePattern all = new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL);
		assertThat(AclEvaluator.covers(pay, payments)).isTrue();
		assertThat(AclEvaluator.covers(all, payments)).isTrue();
		assertThat(AclEvaluator.covers(all, pay)).isTrue();
		assertThat(AclEvaluator.covers(new ResourcePattern(ResourceType.TOPIC, "pa", PatternType.PREFIXED), pay))
			.isTrue();
		assertThat(AclEvaluator.covers(payments, pay)).isFalse();
		assertThat(AclEvaluator.covers(pay, all)).isFalse();
		assertThat(AclEvaluator.covers(new ResourcePattern(ResourceType.TOPIC, "payx", PatternType.PREFIXED), pay))
			.isFalse();
		assertThat(AclEvaluator.covers(new ResourcePattern(ResourceType.GROUP, "*", PatternType.LITERAL), payments))
			.isFalse();
	}

}
