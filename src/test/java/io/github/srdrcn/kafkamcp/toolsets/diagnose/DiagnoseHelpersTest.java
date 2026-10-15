package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorStatus;
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
		// paylaşılan makinede GC/CPU gecikmesi: sadece araç sınırı (15 sn) kadar sıkı
		long elapsed = (System.nanoTime() - start) / 1_000_000;
		assertThat(elapsed).isLessThan(15_000);
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
		assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(15_000);
		assertThat(d.findings()).isEmpty();
	}

	/** Zorunlu bağımlılığı zamanında bitmeyen kontrol sessizce boş kalmaz: CHECK_TIMEOUT ve kısmi sonuç. */
	@Test
	void dependentCheckWhoseDependencyIsLateIsReported() {
		Diagnosis d = new Diagnosis();
		try (Checks checks = new Checks(call(14_000), d)) {
			Check<Integer> slow = checks.start("topic'ler", () -> {
				Thread.sleep(1_500);
				return 7;
			});
			Check<Integer> failing = checks.start("bozuk", () -> {
				throw new ToolException("yetki yok");
			});
			Check<Integer> dependent = checks.start("topic config'leri", () -> {
				Integer v = slow.require(100);
				return v == null ? null : v + 1;
			});
			Check<Integer> onFailed = checks.start("bozuğa bağlı", () -> {
				Integer v = failing.require(5_000);
				return v == null ? -1 : v;
			});
			assertThat(dependent.get()).isNull();
			assertThat(slow.get()).as("bağımlılık sonradan bitse de kendi sonucu gelir").isEqualTo(7);
			assertThat(onFailed.get()).as("hata veren bağımlılık null döner, bulgusunu kendisi verir").isEqualTo(-1);
			assertThat(failing.get()).isNull();
		}
		List<Finding> timeouts = d.findings().stream().filter(f -> f.code().equals(Checks.CHECK_TIMEOUT)).toList();
		assertThat(timeouts).hasSize(1);
		assertThat(timeouts.get(0).message()).contains("topic config'leri");
		assertThat(String.valueOf(timeouts.get(0).evidence().get("reason"))).contains("bağımlı", "topic'ler");
		assertThat(d.findings()).extracting(Finding::code).containsOnly(Checks.CHECK_TIMEOUT, Checks.CHECK_FAILED);
		assertThat(d.partial()).isTrue();
	}

	/**
	 * Commit'i pencerede ilerlemeyen partition: bekleyen en eski kayıt yeni ise periyodik commit (bulgu kritik
	 * değil), commit payını aşmışsa şüpheli, kayıt yaşıyla 5+ dk ise takılmış; lag/hız tahmini tek başına kritik
	 * olmaz.
	 */
	@Test
	void stuckCheckSeparatesPeriodicCommitFromRealStall() {
		TopicPartition p0 = new TopicPartition("t", 0);
		TopicPartition p1 = new TopicPartition("t", 1);
		TopicPartition p2 = new TopicPartition("t", 2);
		TopicPartition p3 = new TopicPartition("t", 3);
		Map<TopicPartition, Long> end0 = Map.of(p0, 1_000L, p1, 1_000L, p2, 1_000L, p3, 100_000L);
		Map<TopicPartition, Long> end1 = Map.of(p0, 1_050L, p1, 1_050L, p2, 1_050L, p3, 100_050L);
		Map<TopicPartition, Long> committed = Map.of(p0, 900L, p1, 800L, p2, 700L, p3, 50_000L);
		OffsetWindow w = OffsetWindow.of(end0, end1, Map.of("g", committed), Map.of("g", committed), Map.of(), 5.0);
		long now = 10_000_000L;
		StuckCheck sc = StuckCheck.of(w,
				Map.of(new StuckCheck.Position(p0, 900), now - 20_000, new StuckCheck.Position(p1, 800), now - 120_000,
						new StuckCheck.Position(p2, 700), now - 1_800_000),
				now);
		StuckCheck.Verdict v = sc.verdict("g");
		assertThat(v.periodic()).extracting(StuckCheck.Assessed::partition).containsExactly(p0);
		assertThat(v.suspect()).extracting(StuckCheck.Assessed::partition).containsExactlyInAnyOrder(p1, p3);
		assertThat(v.stuck()).extracting(StuckCheck.Assessed::partition).containsExactly(p2);
		assertThat(v.maxBehindSeconds(StuckCheck.Level.STUCK)).isEqualTo(1_800);
		StuckCheck.Assessed estimated = v.suspect().stream().filter(a -> a.partition().equals(p3)).findFirst().get();
		assertThat(estimated.basis()).isEqualTo(StuckCheck.ESTIMATE);
		assertThat(estimated.behindSeconds()).as("50000 lag / 10 msg/sn").isEqualTo(5_000);
		assertThat(v.evidence(StuckCheck.Level.STUCK, 10).get(0)).containsEntry("oldestPendingAgeSeconds", 1_800L)
			.containsEntry("ageBasis", StuckCheck.RECORD_AGE);
		assertThat(v.window()).containsEntry("windowSeconds", 5.0).containsEntry("commitAllowanceSeconds", 90L);

		// periyodik commit eden sağlıklı grup: lag 20 sn'lik üretim kadar, büyüyor görünse de pay içinde
		OffsetWindow.GroupProgress healthy = new OffsetWindow.GroupProgress("g",
				List.of(new PartitionProgress(p0, 800L, 800L, 1_000L, 1_050L)), true, 5.0);
		assertThat(healthy.trend()).isEqualTo("growing");
		assertThat(healthy.lagBeyondCommitAllowance()).isFalse();
		OffsetWindow.GroupProgress behind = new OffsetWindow.GroupProgress("g",
				List.of(new PartitionProgress(p0, 0L, 10L, 1_000L, 1_050L)), true, 5.0);
		assertThat(behind.lagBeyondCommitAllowance()).isTrue();
	}

	/** Connect sink'i varsayılan offset.flush.interval.ms (60 sn) ile commit ederken kritik SINK_STUCK verilmez. */
	@Test
	void sinkCommittingEverySixtySecondsIsNotReportedAsStuck() {
		TopicPartition tp = new TopicPartition("in", 0);
		String group = "connect-s";
		OffsetWindow w = OffsetWindow.of(Map.of(tp, 1_000L), Map.of(tp, 1_050L), Map.of(group, Map.of(tp, 800L)),
				Map.of(group, Map.of(tp, 800L)), Map.of(), 5.0);
		long now = 50_000_000L;
		for (long ageMs : List.of(40_000L, 600_000L)) {
			StuckCheck sc = StuckCheck.of(w, Map.of(new StuckCheck.Position(tp, 800), now - ageMs), now);
			DiagnoseConnectorTools.Activity a = new DiagnoseConnectorTools.Activity(group, null, w, Set.of(), 0,
					Map.of(), sc);
			Diagnosis d = new Diagnosis();
			Map<String, Object> facts = new LinkedHashMap<>();
			DiagnoseConnectorTools.activityFindings(d, facts, a, new ConnectorStatus("s", "sink", "RUNNING", "w:8083",
					null, List.of(new ConnectorStatus.Task(0, "RUNNING", "w:8083", null))));
			if (ageMs < 60_000) {
				assertThat(d.status()).as(d.findings().toString()).isEqualTo("ok");
				assertThat(d.findings()).extracting(Finding::code).containsExactly("SINK_COMMIT_NOT_ADVANCED");
				assertThat(d.findings().get(0).message()).contains("offset.flush.interval.ms");
			}
			else {
				assertThat(d.status()).isEqualTo("critical");
				assertThat(d.findings()).extracting(Finding::code).containsExactly("SINK_STUCK");
				assertThat(d.findings().get(0).evidence()).containsEntry("windowSeconds", 5.0);
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> lag = (Map<String, Object>) facts.get("sinkLag");
			assertThat(lag).containsEntry("commitNotAdvancedPartitions", 1)
				.containsEntry("stuckPartitions", ageMs < 60_000 ? 0 : 1);
		}
	}

	/** Commit'li offset'leri alınamayan gruplar varken "commit eden grup yok" denmez. */
	@Test
	void groupsWithFailedOffsetFetchAreNotReportedAsAbsent() {
		TopicPartition tp = new TopicPartition("t", 0);
		OffsetWindow w = OffsetWindow.of(Map.of(tp, 10L), Map.of(tp, 10L), Map.of(), Map.of(),
				Map.of("slow-group", "zaman aşımı"), 3.0);
		DiagnoseTopicTools.GroupScan gs = new DiagnoseTopicTools.GroupScan(4, List.of(), w, Map.of(),
				StuckCheck.of(w, Map.of(), System.currentTimeMillis()));
		DiagnoseTopicTools.OffsetsInfo oi = new DiagnoseTopicTools.OffsetsInfo(Map.of(tp, new Offsets.Range(0, 10)),
				Map.of());
		TopicDescription td = new TopicDescription("t", false,
				List.of(new TopicPartitionInfo(0, Node.noNode(), List.of(), List.of())));
		Diagnosis d = new Diagnosis();
		DiagnoseTopicTools.consumers(d, new LinkedHashMap<>(), gs, oi, td);
		assertThat(d.findings()).extracting(Finding::code).doesNotContain("NO_CONSUMER_GROUPS")
			.contains("GROUP_OFFSETS_UNAVAILABLE");
		assertThat(d.partial()).isTrue();

		OffsetWindow ok = OffsetWindow.of(Map.of(tp, 10L), Map.of(tp, 10L), Map.of(), Map.of(), Map.of(), 3.0);
		Diagnosis d2 = new Diagnosis();
		DiagnoseTopicTools.consumers(d2, new LinkedHashMap<>(),
				new DiagnoseTopicTools.GroupScan(4, List.of(), ok, Map.of(), StuckCheck.of(ok, Map.of(), 0)), oi, td);
		assertThat(d2.findings()).extracting(Finding::code).containsExactly("NO_CONSUMER_GROUPS");
	}

	/** Connect &lt; 3.2 connectorsOnly'yi yok sayar: converter listesi yoksa converter kontrolü yapılmaz. */
	@Test
	void pluginListingAndAliases() throws Exception {
		ObjectMapper m = new ObjectMapper();
		DiagnoseConnectorTools.Plugins old = DiagnoseConnectorTools.plugins(m.readTree("""
				[{"class":"org.apache.kafka.connect.file.FileStreamSinkConnector","type":"sink","version":"3.1.0"},
				 {"class":"org.apache.kafka.connect.file.FileStreamSourceConnector","type":"source","version":"3.1.0"}]
				"""));
		assertThat(old.includesConverters()).isFalse();
		assertThat(old.connectors()).hasSize(2);
		DiagnoseConnectorTools.Plugins all = DiagnoseConnectorTools.plugins(m.readTree("""
				[{"class":"org.apache.kafka.connect.file.FileStreamSinkConnector","type":"sink"},
				 {"class":"org.apache.kafka.connect.json.JsonConverter","type":"converter"},
				 {"class":"org.apache.kafka.connect.json.JsonConverter","type":"header_converter"}]
				"""));
		assertThat(all.includesConverters()).isTrue();
		assertThat(all.connectors()).containsExactly("org.apache.kafka.connect.file.FileStreamSinkConnector");
		assertThat(all.converters()).containsExactly("org.apache.kafka.connect.json.JsonConverter");
		for (String alias : List.of("org.apache.kafka.connect.file.FileStreamSinkConnector", "FileStreamSinkConnector",
				"FileStreamSink", "filestreamsink")) {
			assertThat(DiagnoseConnectorTools.pluginPresent(all.connectors(), alias, "Connector")).as(alias).isTrue();
		}
		assertThat(DiagnoseConnectorTools.pluginPresent(all.connectors(), "JdbcSink", "Connector")).isFalse();
		for (String alias : List.of("org.apache.kafka.connect.json.JsonConverter", "JsonConverter", "Json")) {
			assertThat(DiagnoseConnectorTools.pluginPresent(all.converters(), alias, "Converter")).as(alias).isTrue();
		}
		assertThat(DiagnoseConnectorTools.pluginPresent(all.converters(), "AvroConverter", "Converter")).isFalse();
	}

	/** REST adımının süresi istek görevinde ölçülür; sonucun okunduğu ana kadar geçen süre (önceki adımlar) eklenmez. */
	@Test
	void restStepDurationIsMeasuredInsideTheRequest() throws Exception {
		Diagnosis d = new Diagnosis();
		List<Map<String, Object>> steps = new ArrayList<>();
		long submittedLongAgo = System.nanoTime() - 8_000_000_000L;
		DiagnoseConnectivityTools.rest(call(14_000), d, steps,
				CompletableFuture.completedFuture(
						new DiagnoseConnectivityTools.RestResult(new ObjectMapper().readTree("{\"version\":\"4.1.1\"}"), null, 12)),
				submittedLongAgo, "connect", "http://connect:8083", "CONNECT_UNREACHABLE", "Kafka Connect REST",
				n -> Map.of("version", n.path("version").asText()));
		assertThat(steps).hasSize(1);
		assertThat(steps.get(0)).containsEntry("status", "ok").containsEntry("durationMs", 12L)
			.containsEntry("version", "4.1.1");
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
		assertThat(p.summary()).containsEntry("commitNotAdvancedPartitions", 1).containsEntry("lagUnknownPartitions", 1);

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
