package io.github.srdrcn.kafkamcp.toolsets.acl;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import com.fasterxml.jackson.databind.JsonNode;

/** acl testleri için ACL/topic hazırlama yardımcıları (admin kullanıcısıyla). */
final class AclTestSupport {

	private AclTestSupport() {
	}

	static AclBinding binding(ResourceType type, String name, PatternType pattern, String principal,
			AclOperation op, AclPermissionType perm) {
		return binding(type, name, pattern, principal, "*", op, perm);
	}

	static AclBinding binding(ResourceType type, String name, PatternType pattern, String principal, String host,
			AclOperation op, AclPermissionType perm) {
		return new AclBinding(new ResourcePattern(type, name, pattern), new AccessControlEntry(principal, host, op, perm));
	}

	static AclBinding allowTopic(String topic, String principal, AclOperation op) {
		return binding(ResourceType.TOPIC, topic, PatternType.LITERAL, principal, op, AclPermissionType.ALLOW);
	}

	static AclBinding denyTopic(String topic, String principal, AclOperation op) {
		return binding(ResourceType.TOPIC, topic, PatternType.LITERAL, principal, op, AclPermissionType.DENY);
	}

	/** ACL'leri oluşturur ve broker'da görünür olana kadar bekler (KRaft'ta kısa gecikme olabilir). */
	static void create(Admin admin, AclBinding... bindings) throws Exception {
		admin.createAcls(List.of(bindings)).all().get(15, TimeUnit.SECONDS);
		await().atMost(Duration.ofSeconds(15))
			.pollInterval(Duration.ofMillis(100))
			.until(() -> admin.describeAcls(AclBindingFilter.ANY)
				.values()
				.get(15, TimeUnit.SECONDS)
				.containsAll(List.of(bindings)));
	}

	static void delete(Admin admin, AclBinding... bindings) throws Exception {
		List<AclBindingFilter> filters = new ArrayList<>();
		for (AclBinding b : bindings) {
			filters.add(b.toFilter());
		}
		admin.deleteAcls(filters).all().get(15, TimeUnit.SECONDS);
		await().atMost(Duration.ofSeconds(15))
			.pollInterval(Duration.ofMillis(100))
			.until(() -> admin.describeAcls(AclBindingFilter.ANY)
				.values()
				.get(15, TimeUnit.SECONDS)
				.stream()
				.noneMatch(List.of(bindings)::contains));
	}

	/** Principal'ın tüm ACL'lerini siler (test temizliği). */
	static void deleteAllFor(Admin admin, String principal) throws Exception {
		admin.deleteAcls(List.of(new AclBindingFilter(ResourcePatternFilter.ANY,
				new AccessControlEntryFilter(principal, null, AclOperation.ANY, AclPermissionType.ANY))))
			.all()
			.get(15, TimeUnit.SECONDS);
		await().atMost(Duration.ofSeconds(15))
			.pollInterval(Duration.ofMillis(100))
			.until(() -> aclsOf(admin, principal).isEmpty());
	}

	static Collection<AclBinding> aclsOf(Admin admin, String principal) throws Exception {
		return admin
			.describeAcls(new AclBindingFilter(ResourcePatternFilter.ANY,
					new AccessControlEntryFilter(principal, null, AclOperation.ANY, AclPermissionType.ANY)))
			.values()
			.get(15, TimeUnit.SECONDS);
	}

	static void createTopic(Admin admin, String topic) throws Exception {
		admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
	}

	/** Dizinin üst düzey elemanlarından alanı verilen değere eşit olan ilki. */
	static JsonNode find(JsonNode array, String field, String value) {
		if (array != null) {
			for (JsonNode n : array) {
				if (value.equals(n.path(field).asText())) {
					return n;
				}
			}
		}
		throw new AssertionError(field + "=" + value + " bulunamadı: " + array);
	}

	/** JSON dizisindeki nesnelerin verilen alan değerleri. */
	static List<String> field(JsonNode array, String field) {
		List<String> out = new ArrayList<>();
		if (array != null) {
			array.forEach(n -> out.add(n.path(field).asText()));
		}
		return out;
	}

}
