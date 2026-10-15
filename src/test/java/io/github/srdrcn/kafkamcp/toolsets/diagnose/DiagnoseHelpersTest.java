package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.toolsets.diagnose.Checks.Check;
import io.github.srdrcn.kafkamcp.toolsets.diagnose.OffsetWindow.GroupProgress;
import io.github.srdrcn.kafkamcp.toolsets.diagnose.OffsetWindow.PartitionProgress;

/** diagnose yardımcılarının birim testleri (Kafka gerekmez). */
class DiagnoseHelpersTest {

	private static ToolCall call(long deadlineMs) {
		return new ToolCall("diagnose_test", Map.of("cluster", "x"), null, Instant.now().plusMillis(deadlineMs));
	}

	@Test
	void diagnosisSortsFindingsAndDerivesStatus() {
		Diagnosis d = new Diagnosis();
		assertThat(d.status()).isEqualTo("ok");
		assertThat(d.summary()).isEqualTo("Sorun bulunmadı");
		d.info("B_INFO", "bilgi", null, null);
		assertThat(d.status()).isEqualTo("ok");
		d.warning("Z_WARN", "uyarı", Diagnosis.evidence("a", 1, "b", null), "öneri");
		d.critical("A_CRIT", "kritik", null, "düzelt");
		assertThat(d.status()).isEqualTo("critical");
		assertThat(d.findings()).extracting(Finding::code).containsExactly("A_CRIT", "Z_WARN", "B_INFO");
		assertThat(d.summary()).isEqualTo("1 kritik, 1 uyarı, 1 bilgi bulgusu");
		Map<String, Object> r = d.result(Map.of("topic", "t"), Map.of("x", 1));
		assertThat(r).containsEntry("topic", "t").containsEntry("status", "critical").doesNotContainKey("partial");
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> findings = (List<Map<String, Object>>) r.get("findings");
		assertThat(findings.get(1)).containsEntry("severity", "warning").containsEntry("evidence", Map.of("a", 1));
		assertThat(findings.get(2)).doesNotContainKeys("evidence", "recommendation");
		d.markPartial();
		assertThat(d.result(null, Map.of())).containsEntry("partial", true);
	}

	@Test
	void failingCheckBecomesFindingAndDoesNotFailTool() {
		Diagnosis d = new Diagnosis();
		try (Checks checks = new Checks(call(10_000), d)) {
			Check<String> bad = checks.start("bozuk kontrol", () -> {
				throw new ToolException("Topic yetkisi yok (ACL)");
			});
			Check<String> good = checks.start("iyi kontrol", () -> "ok");
			assertThat(bad.get()).isNull();
			assertThat(bad.get()).as("ikinci get bulguyu tekrarlamaz").isNull();
			assertThat(bad.ok()).isFalse();
			assertThat(good.get()).isEqualTo("ok");
			assertThat(good.ok()).isTrue();
		}
		assertThat(d.findings()).hasSize(1);
		Finding f = d.findings().get(0);
		assertThat(f.code()).isEqualTo(Checks.CHECK_FAILED);
		assertThat(f.severity()).isEqualTo(Severity.WARNING);
		assertThat(f.message()).contains("bozuk kontrol");
		assertThat(f.evidence()).containsEntry("error", "Topic yetkisi yok (ACL)");
		assertThat(d.partial()).isTrue();
	}

	@Test
	void slowCheckTimesOutWithinTheDeadline() {
		Diagnosis d = new Diagnosis();
		long start = System.nanoTime();
		try (Checks checks = new Checks(call(2_500), d)) {
			Check<String> slow = checks.start("yavaş kontrol", () -> {
				Thread.sleep(60_000);
				return "geç";
			});
			assertThat(slow.get()).isNull();
		}
		long elapsed = (System.nanoTime() - start) / 1_000_000;
		assertThat(elapsed).isLessThan(2_500);
		assertThat(d.findings()).extracting(Finding::code).containsExactly(Checks.CHECK_TIMEOUT);
		assertThat(d.partial()).isTrue();
	}

	/**
	 * Birbirini bekleyen çok sayıda kontrol (bağımlılıklar) sanal thread taşıyıcılarını bağlamamalı: hepsi
	 * temel kontrol bitince hemen biter (kilitli beklemede taşıyıcılar tükenip araç süre sınırına takılıyordu).
	 */
	@Test
	void manyDependentChecksDoNotStarveCarrierThreads() {
		Diagnosis d = new Diagnosis();
		long start = System.nanoTime();
		try (Checks checks = new Checks(call(14_000), d)) {
			Check<Integer> base = checks.start("temel", () -> {
				Thread.sleep(300);
				return 1;
			});
			List<Check<Integer>> dependents = new ArrayList<>();
			for (int i = 0; i < 32; i++) {
				int n = i;
				dependents.add(checks.start("bağımlı-" + i, () -> {
					Integer b = base.await(10_000);
					return b == null ? null : b + n;
				}));
			}
			assertThat(base.get()).isEqualTo(1);
			for (int i = 0; i < dependents.size(); i++) {
				assertThat(dependents.get(i).get()).isEqualTo(1 + i);
			}
		}
		assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(5_000);
		assertThat(d.findings()).isEmpty();
	}

	@Test
	void groupProgressDetectsStuckGrowingAndCatchingUp() {
		TopicPartition a0 = new TopicPartition("a", 0);
		TopicPartition a1 = new TopicPartition("a", 1);
		TopicPartition b0 = new TopicPartition("b", 0);
		List<PartitionProgress> rows = List.of(new PartitionProgress(a0, 3L, 3L, 10L, 20L),
				new PartitionProgress(a1, 5L, 9L, 10L, 10L), new PartitionProgress(b0, 0L, 0L, null, null));
		GroupProgress p = new GroupProgress("g", rows, true, 2.0);
		assertThat(p.stuck()).extracting(PartitionProgress::partition).containsExactly(a0);
		assertThat(p.lag0()).isEqualTo(7 + 5);
		assertThat(p.lag()).isEqualTo(17 + 1);
		assertThat(p.trend()).isEqualTo("growing");
		assertThat(p.consumeRate()).isEqualTo(2.0);
		assertThat(p.produceRate()).isEqualTo(5.0);
		assertThat(p.unknown()).isEqualTo(1);
		assertThat(p.etaSeconds()).isNull();
		assertThat(p.byTopic()).extracting(OffsetWindow.TopicProgress::topic).containsExactly("a", "b");
		assertThat(p.byTopic().get(0).stuck()).isEqualTo(1);
		assertThat(p.summary()).containsEntry("stuckPartitions", 1).containsEntry("lagUnknownPartitions", 1);

		GroupProgress catching = new GroupProgress("g",
				List.of(new PartitionProgress(a0, 0L, 50L, 100L, 110L)), true, 5.0);
		assertThat(catching.trend()).isEqualTo("shrinking");
		assertThat(catching.etaSeconds()).isEqualTo(Math.round(60 / (40 / 5.0)));
		assertThat(catching.stuck()).isEmpty();

		GroupProgress reset = new GroupProgress("g", List.of(new PartitionProgress(a0, 50L, 10L, 100L, 100L)), true,
				1.0);
		assertThat(reset.resetDetected()).isTrue();

		GroupProgress single = new GroupProgress("g", List.of(new PartitionProgress(a0, 3L, null, 10L, null)), false,
				0);
		assertThat(single.trend()).isNull();
		assertThat(single.lag()).isEqualTo(7);
		assertThat(single.stuck()).isEmpty();
	}

	@Test
	void bootstrapServersAreParsed() {
		Net.Parsed p = Net.parseServers("kafka-0.kafka:9092, SASL_SSL://kafka-1:9093,[::1]:9094,bad,host:x,:1");
		assertThat(p.endpoints()).extracting(Net.Endpoint::address)
			.containsExactly("kafka-0.kafka:9092", "kafka-1:9093", "[::1]:9094");
		assertThat(p.invalid()).containsExactly("bad", "host:x", ":1");
	}

	@Test
	void networkErrorsAreExplainedInTurkish() {
		Net.Resolution dns = Net.resolve("nonexistent.invalid", 3_000);
		assertThat(dns.ok()).isFalse();
		assertThat(dns.error()).contains("DNS");
		Net.Probe refused = Net.resolveAndConnect(new Net.Endpoint("127.0.0.1", 1), 1_000, 2_000);
		assertThat(refused.errorType()).isEqualTo(Net.REFUSED);
		assertThat(refused.error()).contains("Bağlantı reddedildi", "127.0.0.1:1");
		assertThat(Net.explain(Net.TIMEOUT, "h:1", 2_000, null)).contains("NetworkPolicy", "egress");
	}

	@Test
	void accessRequirementsPerIntent() {
		assertThat(DiagnoseAccessTools.requirements("produce", "t", null, null, false))
			.extracting(r -> r.type() + ":" + r.operation())
			.containsExactly("TOPIC:WRITE", "TOPIC:DESCRIBE");
		assertThat(DiagnoseAccessTools.requirements("produce", "t", null, null, true))
			.extracting(DiagnoseAccessTools.Requirement::operation)
			.contains(AclOperation.IDEMPOTENT_WRITE);
		assertThat(DiagnoseAccessTools.requirements("consume", "t", "g", null, false))
			.extracting(r -> r.type() + ":" + r.name() + ":" + r.operation())
			.containsExactly("TOPIC:t:READ", "TOPIC:t:DESCRIBE", "GROUP:g:READ");
		assertThat(DiagnoseAccessTools.requirements("transactional-produce", "t", "g", "tx", false))
			.extracting(DiagnoseAccessTools.Requirement::type)
			.containsExactly(ResourceType.TOPIC, ResourceType.TOPIC, ResourceType.TRANSACTIONAL_ID,
					ResourceType.TRANSACTIONAL_ID, ResourceType.GROUP);
		assertThat(DiagnoseAccessTools.requirements("admin-topic", "t", null, null, false)).hasSize(5);
		assertThat(DiagnoseAccessTools.olderThan28("2.7-IV2")).isTrue();
		assertThat(DiagnoseAccessTools.olderThan28("2.8")).isFalse();
		assertThat(DiagnoseAccessTools.olderThan28("3.9-IV0")).isFalse();
		assertThat(DiagnoseAccessTools.olderThan28("x")).isNull();
		assertThat(DiagnoseAccessTools.binding("User:a",
				new DiagnoseAccessTools.Requirement(ResourceType.GROUP, "g", AclOperation.READ, "p"), "*"))
			.containsExactly(Map.entry("principal", "User:a"), Map.entry("resourceType", "GROUP"),
					Map.entry("resourceName", "g"), Map.entry("patternType", "LITERAL"), Map.entry("operation", "READ"),
					Map.entry("permission", "ALLOW"), Map.entry("host", "*"));
	}

	@Test
	void connectorConfigReferences() {
		Map<String, String> config = Map.of("topics", "a, orders ,b", "topics.regex", "ord.*", "topic.prefix", "ord",
				"transforms.route.topic.format", "x", "name", "orders");
		assertThat(ConnectorRefs.configMatches(config, "orders")).containsExactlyInAnyOrder("config:topics",
				"config:topics.regex", "config:topic.prefix");
		assertThat(ConnectorRefs.configMatches(Map.of("topics.regex", "("), "orders")).isEmpty();
	}

}
