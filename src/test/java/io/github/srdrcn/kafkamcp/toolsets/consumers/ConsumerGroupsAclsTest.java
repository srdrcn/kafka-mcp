package io.github.srdrcn.kafkamcp.toolsets.consumers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.junit.jupiter.api.Test;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import com.fasterxml.jackson.databind.JsonNode;

/** consumer_groups_acls: literal + prefixed GROUP ACLs. */
class ConsumerGroupsAclsTest extends ConsumersTestBase {

	@Test
	void findsLiteralAndPrefixedGroupAcls() throws Exception {
		String prefix = unique("aclg");
		String group = prefix + "-orders";
		List<AclBinding> bindings = List.of(
				new AclBinding(new ResourcePattern(ResourceType.GROUP, group, PatternType.LITERAL),
						new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW)),
				new AclBinding(new ResourcePattern(ResourceType.GROUP, prefix + "-", PatternType.PREFIXED),
						new AccessControlEntry("User:bob", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
				new AclBinding(new ResourcePattern(ResourceType.GROUP, prefix + "-other", PatternType.LITERAL),
						new AccessControlEntry("User:bob", "*", AclOperation.DELETE, AclPermissionType.DENY)));
		testAdmin().createAcls(bindings).all().get(30, TimeUnit.SECONDS);
		try {
			waitUntil(() -> ok("consumer_groups_acls", args("group", group)).get("count").asInt() >= 2,
					"ACLs did not show up");
			JsonNode r = ok("consumer_groups_acls", args("group", group));
			assertThat(r.get("group").asText()).isEqualTo(group);
			assertThat(r.get("count").asInt()).isEqualTo(2);
			assertThat(r.get("principals").toString()).isEqualTo("[\"User:alice\",\"User:bob\"]");
			assertThat(r.at("/byPatternType/LITERAL").asInt()).isEqualTo(1);
			assertThat(r.at("/byPatternType/PREFIXED").asInt()).isEqualTo(1);
			List<String> rows = new ArrayList<>();
			r.get("acls")
				.forEach(a -> rows.add(a.get("principal").asText() + "|" + a.get("host").asText() + "|"
						+ a.get("operation").asText() + "|" + a.get("permission").asText() + "|"
						+ a.get("patternType").asText() + "|" + a.get("resourceName").asText()));
			assertThat(rows).containsExactly("User:alice|*|READ|ALLOW|LITERAL|" + group,
					"User:bob|*|DESCRIBE|ALLOW|PREFIXED|" + prefix + "-");

			JsonNode none = ok("consumer_groups_acls", args("group", unique("noacl")));
			assertThat(none.get("count").asInt()).isZero();
			assertThat(none.get("note").asText()).contains("No ACLs match");
		}
		finally {
			testAdmin().deleteAcls(bindings.stream().map(AclBinding::toFilter).toList()).all().get(30, TimeUnit.SECONDS);
		}
	}

}
