package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.MAX_EVIDENCE;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.first;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.DiagnoseParams.omitted;
import static io.github.srdrcn.kafkamcp.toolsets.diagnose.Diagnosis.evidence;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientResponseException;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.Offsets;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectApi;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectJson;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectSecrets;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorInfo;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorOverview;
import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorStatus;
import io.github.srdrcn.kafkamcp.toolsets.diagnose.Checks.Check;

/** diagnose_connector: tek çağrıda Connect connector teşhisi (sadece connectUrl'i olan cluster'larda). */
@Component
public class DiagnoseConnectorTools {

	static final long HIGH_LAG = 10_000;

	static final long GROWING_LAG_MIN = 100;

	/** Kaynak topic'lerden en fazla bu kadarı ölçülür. */
	static final int MAX_SOURCE_TOPICS = 20;

	static final long SOURCE_IDLE_SECONDS = 3_600;

	private static final int CONFIG_VALUE_MAX = 200;

	private static final long WINDOW_RESERVE_MS = 2_000;

	private static final Set<String> SINGLE_TOPIC_KEYS = Set.of("topic", "topics", "kafka.topic", "topic.name");

	private final ConnectApi api;

	public DiagnoseConnectorTools(ConnectApi api) {
		this.api = api;
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("diagnose_connector",
					"Connector teşhisi: connector/görev durumu ve hata özetleri, config/plugin/converter, topic'ler, sink lag, kaynak yazma, worker dağılımı")
			.connect()
			.required("name", P.string("Connector adı"))
			.param("intervalSeconds", DiagnoseParams.interval())
			.handler(this::diagnose));
	}

	record Plugins(Set<String> classes, boolean includesConverters) {
	}

	record Validation(int errorCount, List<Map<String, Object>> fields) {
	}

	record TopicCheck(List<String> configured, Set<String> missing, String regex, Integer regexMatches,
			String dlq, boolean dlqExists) {
	}

	record Activity(String group, String dlq, OffsetWindow window, Set<String> sourceTopics,
			int sourceTopicsOmitted, Map<String, Long> lastWrite) {
	}

	Object diagnose(ToolCall call) {
		String cluster = call.cluster();
		String name = call.str("name");
		long intervalMs = DiagnoseParams.intervalMs(call);
		ConnectorStatus status = status(cluster, name);
		Diagnosis dx = new Diagnosis();
		Map<String, Object> facts = new LinkedHashMap<>();
		try (Checks checks = new Checks(call, dx)) {
			Check<ConnectorInfo> info = checks.start("connector config", () -> api.info(cluster, name));
			Check<List<ConnectorOverview>> overview = checks.start("worker dağılımı", () -> api.overview(cluster));
			Check<Plugins> plugins = checks.start("plugin'ler", () -> plugins(cluster));
			Check<List<String>> active = checks.start("aktif topic'ler", () -> activeTopics(cluster, name));
			Check<Validation> validation = checks.start("config doğrulama", () -> {
				ConnectorInfo i = info.await(5_000);
				return i == null || i.connectorClass() == null ? null : validate(cluster, i);
			});
			Check<TopicCheck> topics = checks.start("topic'lerin varlığı", () -> {
				ConnectorInfo i = info.await(5_000);
				return i == null ? null : topicCheck(call, i);
			});
			Check<Activity> activity = checks.start("sink lag / kaynak yazma", () -> {
				ConnectorInfo i = info.await(5_000);
				List<String> a = active.await(5_000);
				return activity(call, name, status, i, a, intervalMs);
			});

			states(dx, facts, status, name);
			ConnectorInfo i = info.get();
			configFindings(dx, facts, i, status, plugins.get(), validation.get());
			topicFindings(dx, facts, topics.get(), active.get(), status, i);
			activityFindings(dx, facts, activity.get(), status);
			workers(dx, facts, overview.get(), status);
		}
		Map<String, Object> head = Results.map("name", name, "cluster", cluster);
		return dx.result(head, facts);
	}

	/** Connector durumu; durum henüz yoksa (yeni oluşturulmuş) boş durum, connector hiç yoksa Türkçe hata. */
	private ConnectorStatus status(String cluster, String name) {
		try {
			return api.status(cluster, name);
		}
		catch (ToolException e) {
			if (!(e.getCause() instanceof RestClientResponseException r && r.getStatusCode().value() == 404)) {
				throw e;
			}
			ConnectorInfo info;
			try {
				info = api.info(cluster, name);
			}
			catch (ToolException notFound) {
				throw new ToolException("Connector bulunamadı: " + name + " (connectors_list ile bakın)", e);
			}
			return new ConnectorStatus(name, info.type(), null, null, null, List.of());
		}
	}

	// ---- alt kontroller ----

	private Plugins plugins(String cluster) {
		Set<String> classes = new TreeSet<>();
		boolean converters = true;
		JsonNode all;
		try {
			all = api.client().get(cluster, "/connector-plugins?connectorsOnly=false");
		}
		catch (ToolException e) {
			all = api.client().get(cluster, "/connector-plugins");
			converters = false;
		}
		for (JsonNode n : all) {
			String cls = ConnectJson.text(n, "class");
			if (cls != null) {
				classes.add(cls);
			}
		}
		return new Plugins(classes, converters);
	}

	private List<String> activeTopics(String cluster, String name) {
		JsonNode r = api.client().get(cluster, ConnectApi.path(name) + "/topics");
		JsonNode node = r.has(name) ? r.get(name) : null;
		List<String> out = new ArrayList<>();
		if (node != null) {
			node.path("topics").forEach(t -> out.add(t.asText()));
		}
		out.sort(null);
		return out;
	}

	private Validation validate(String cluster, ConnectorInfo info) {
		JsonNode r = api.client()
			.put(cluster, "/connector-plugins/" + ConnectClient.enc(info.connectorClass()) + "/config/validate",
					info.config());
		List<Map<String, Object>> fields = new ArrayList<>();
		for (JsonNode c : r.path("configs")) {
			JsonNode value = c.path("value");
			List<String> errors = new ArrayList<>();
			value.path("errors").forEach(e -> errors.add(e.asText()));
			if (errors.isEmpty()) {
				continue;
			}
			String field = ConnectJson.text(value, "name");
			fields.add(Results.map("name", field, "errors", ConnectSecrets.isSecret(field)
					? List.of("geçersiz değer (gizli alan; değer gösterilmiyor)") : first(errors, 3)));
		}
		return new Validation(r.path("error_count").asInt(fields.size()), fields);
	}

	private static TopicCheck topicCheck(ToolCall call, ConnectorInfo info) {
		Map<String, String> cfg = info.config();
		Set<String> configured = new LinkedHashSet<>();
		cfg.forEach((k, v) -> {
			if (SINGLE_TOPIC_KEYS.contains(k.toLowerCase(Locale.ROOT)) && v != null) {
				for (String t : v.split(",")) {
					if (!t.isBlank()) {
						configured.add(t.trim());
					}
				}
			}
		});
		String regex = cfg.get("topics.regex");
		String dlq = cfg.get("errors.deadletterqueue.topic.name");
		Set<String> names = new LinkedHashSet<>(configured);
		if (dlq != null && !dlq.isBlank()) {
			names.add(dlq.trim());
		}
		Set<String> missing = new TreeSet<>();
		Map<String, Boolean> existing = null;
		if (!names.isEmpty() || regex != null && !regex.isBlank()) {
			existing = Topics.list(call, true);
			for (String n : names) {
				if (!existing.containsKey(n)) {
					missing.add(n);
				}
			}
		}
		Integer regexMatches = null;
		if (regex != null && !regex.isBlank() && existing != null) {
			try {
				Pattern p = Pattern.compile(regex.trim());
				regexMatches = (int) existing.keySet().stream().filter(t -> p.matcher(t).matches()).count();
			}
			catch (PatternSyntaxException e) {
				regexMatches = -1;
			}
		}
		boolean dlqExists = dlq != null && !dlq.isBlank() && !missing.contains(dlq.trim());
		missing.remove(dlq == null ? "" : dlq.trim());
		return new TopicCheck(List.copyOf(configured), missing, regex, regexMatches, dlq == null ? null : dlq.trim(),
				dlqExists);
	}

	private static Activity activity(ToolCall call, String name, ConnectorStatus status, ConnectorInfo info,
			List<String> active, long intervalMs) {
		Map<String, String> cfg = info == null ? Map.of() : info.config();
		String type = status.type() != null ? status.type() : info == null ? null : info.type();
		boolean sink = "sink".equalsIgnoreCase(type);
		String group = sink ? cfg.getOrDefault("consumer.override.group.id", "connect-" + name) : null;
		Set<String> topics = new TreeSet<>();
		Set<String> sources = new TreeSet<>();
		int omittedTopics = 0;
		if (!sink && active != null) {
			List<String> all = new ArrayList<>(active);
			sources.addAll(first(all, MAX_SOURCE_TOPICS));
			topics.addAll(sources);
			omittedTopics = Math.max(0, all.size() - MAX_SOURCE_TOPICS);
		}
		String dlq = cfg.get("errors.deadletterqueue.topic.name");
		dlq = dlq == null || dlq.isBlank() ? null : dlq.trim();
		if (dlq != null) {
			topics.add(dlq);
		}
		List<TopicPartition> partitions = new ArrayList<>();
		if (!topics.isEmpty()) {
			Futures.Each<String, TopicDescription> described = Topics.describe(call, topics);
			partitions.addAll(Topics.onlinePartitions(described.values().values()));
		}
		ListOffsetsResult maxTs = partitions.isEmpty() || sink ? null
				: Offsets.submit(call, partitions, OffsetSpec.maxTimestamp());
		OffsetWindow w = OffsetWindow.measure(call, partitions, group == null ? List.of() : List.of(group), false,
				intervalMs, WINDOW_RESERVE_MS);
		Map<String, Long> lastWrite = new TreeMap<>();
		if (maxTs != null) {
			Map<TopicPartition, ListOffsetsResultInfo> max = Offsets.awaitEach(call, partitions, maxTs).values();
			max.forEach((tp, i) -> {
				if (i.timestamp() >= 0) {
					lastWrite.merge(tp.topic(), i.timestamp(), Math::max);
				}
			});
		}
		return new Activity(group, dlq, w, sources, omittedTopics, lastWrite);
	}

	// ---- değerlendirme ----

	private static void states(Diagnosis dx, Map<String, Object> facts, ConnectorStatus s, String name) {
		facts.put("type", s.type());
		facts.put("state", s.state());
		if (s.workerId() != null) {
			facts.put("worker", s.workerId());
		}
		facts.put("tasks", s.tasks().stream()
			.map(t -> Results.map("id", t.id(), "state", t.state(), "worker", t.workerId()))
			.toList());
		if (s.state() == null) {
			dx.warning("CONNECTOR_STATUS_PENDING",
					"Connector'ın durumu henüz yok (yeni oluşturulmuş ya da hiçbir worker'a atanmamış)", null,
					"Birkaç saniye sonra tekrar bakın; sürerse Connect worker'larını ve config topic'ini kontrol edin");
		}
		String state = s.state() == null ? "" : s.state();
		switch (state) {
			case ConnectorStatus.FAILED -> dx.critical("CONNECTOR_FAILED", "Connector FAILED durumda",
					evidence("worker", s.workerId(), "traceSummary", ConnectJson.traceSummary(s.trace())),
					"Trace'teki hatayı (config, erişim, plugin) giderin; sonra connectors_restart name=" + name);
			case ConnectorStatus.PAUSED -> dx.warning("CONNECTOR_PAUSED", "Connector PAUSED: veri akmıyor", null,
					"Bilinçli değilse connectors_resume ile devam ettirin");
			case ConnectorStatus.STOPPED -> dx.warning("CONNECTOR_STOPPED",
					"Connector STOPPED: görevleri kapalı, veri akmıyor", null,
					"Bilinçli değilse connectors_resume ile devam ettirin");
			case ConnectorStatus.UNASSIGNED, ConnectorStatus.RESTARTING -> dx.warning("CONNECTOR_UNASSIGNED",
					"Connector " + state + " durumda (worker'a atanmamış ya da yeniden başlıyor)", null,
					"Birkaç saniye sonra diagnose_connector ile tekrar bakın; sürerse worker'ları kontrol edin");
			default -> {
				// RUNNING
			}
		}
		List<ConnectorStatus.Task> failed = s.tasks().stream().filter(ConnectorStatus.Task::failed).toList();
		if (!failed.isEmpty()) {
			List<Map<String, Object>> ev = new ArrayList<>();
			for (ConnectorStatus.Task t : first(failed, MAX_EVIDENCE)) {
				ev.add(Results.map("id", t.id(), "worker", t.workerId(), "traceSummary",
						ConnectJson.traceSummary(t.trace())));
			}
			dx.critical("TASKS_FAILED",
					failed.size() + "/" + s.tasks().size() + " görev FAILED: bu görevlerin verisi akmıyor",
					evidence("tasks", ev, "omitted", omitted(failed)),
					"connectors_restart name=" + name + " includeTasks=true onlyFailed=true ile başarısız görevleri "
							+ "yeniden başlatın (tek görev: connectors_task_restart). Aynı hata tekrar ederse trace'teki "
							+ "nedeni (config, dosya/erişim, yetki, veri formatı) düzeltin");
		}
		long unassigned = s.tasksInState(ConnectorStatus.UNASSIGNED);
		if (unassigned > 0) {
			dx.warning("TASKS_UNASSIGNED", unassigned + " görev UNASSIGNED (worker'a atanmamış)", null,
					"Worker'ların ayakta olduğunu ve rebalance'ın bittiğini kontrol edin (connectors_task_distribution)");
		}
		if (ConnectorStatus.RUNNING.equals(s.state()) && s.tasks().isEmpty()) {
			dx.warning("NO_TASKS", "Connector RUNNING ama hiç görevi yok: veri işlenmiyor", null,
					"tasks.max, topic/tablo seçimi gibi görev üreten ayarları kontrol edin");
		}
	}

	private static void configFindings(Diagnosis dx, Map<String, Object> facts, ConnectorInfo info, ConnectorStatus s,
			Plugins plugins, Validation validation) {
		if (info == null) {
			return;
		}
		Map<String, String> cfg = info.config();
		String cls = info.connectorClass();
		facts.put("class", cls);
		Map<String, Object> masked = new TreeMap<>();
		ConnectSecrets.mask(cfg).forEach((k, v) -> {
			String text = v == null ? null : String.valueOf(v);
			masked.put(k, text != null && text.length() > CONFIG_VALUE_MAX ? text.substring(0, CONFIG_VALUE_MAX) + "…" : text);
		});
		facts.put("config", masked);
		if (plugins != null && cls != null && !plugins.classes().contains(cls)) {
			dx.critical("PLUGIN_MISSING", "connector.class worker'larda yüklü değil: " + cls,
					evidence("class", cls), "Plugin'i tüm worker'ların plugin.path'ine kurun (connect_plugins_list ile kontrol edin)");
		}
		List<String> missingConverters = new ArrayList<>();
		List<String> defaulted = new ArrayList<>();
		for (String key : List.of("key.converter", "value.converter")) {
			String conv = cfg.get(key);
			if (conv == null || conv.isBlank()) {
				defaulted.add(key);
			}
			else if (plugins != null && plugins.includesConverters() && !plugins.classes().contains(conv.trim())) {
				missingConverters.add(key + "=" + conv.trim());
			}
		}
		if (!missingConverters.isEmpty()) {
			dx.critical("CONVERTER_MISSING", "Converter sınıfı worker'larda yok: " + String.join(", ", missingConverters),
					evidence("converters", missingConverters),
					"Converter'ı (ör. Avro/Protobuf converter) worker plugin.path'ine kurun ya da config'te yüklü bir converter verin");
		}
		if (!defaulted.isEmpty()) {
			dx.info("CONVERTER_DEFAULT", String.join(", ", defaulted)
					+ " connector'da tanımlı değil: worker'ın varsayılan converter'ı kullanılır",
					evidence("keys", defaulted), "Veri formatı worker varsayılanından farklıysa converter'ı connector config'inde açıkça verin");
		}
		if (validation != null && validation.errorCount() > 0) {
			dx.warning("CONFIG_INVALID", "Connect config doğrulaması " + validation.errorCount() + " hata buldu",
					evidence("fields", first(validation.fields(), MAX_EVIDENCE), "omitted", omitted(validation.fields())),
					"Hatalı alanları düzeltip connectors_create_or_update ile güncelleyin (connect_plugins_validate ile önce deneyin)");
		}
		boolean sink = "sink".equalsIgnoreCase(s.type() != null ? s.type() : info.type());
		String tolerance = cfg.get("errors.tolerance");
		String dlq = cfg.get("errors.deadletterqueue.topic.name");
		if (sink && "all".equalsIgnoreCase(tolerance) && (dlq == null || dlq.isBlank())) {
			dx.warning("ERRORS_TOLERATED_WITHOUT_DLQ",
					"errors.tolerance=all ve DLQ yok: dönüştürülemeyen kayıtlar sessizce atlanır", null,
					"errors.deadletterqueue.topic.name ile DLQ tanımlayın ve errors.log.enable=true yapın");
		}
	}

	private static void topicFindings(Diagnosis dx, Map<String, Object> facts, TopicCheck tc, List<String> active,
			ConnectorStatus s, ConnectorInfo info) {
		boolean sink = "sink".equalsIgnoreCase(s.type() != null ? s.type() : info == null ? null : info.type());
		Map<String, Object> topics = new LinkedHashMap<>();
		if (active != null) {
			topics.put("active", first(active, 20));
			if (active.size() > 20) {
				topics.put("activeOmitted", active.size() - 20);
			}
		}
		if (tc != null) {
			topics.putAll(Results.map("configured", tc.configured().isEmpty() ? null : first(tc.configured(), 20),
					"regex", tc.regex(), "regexMatches", tc.regexMatches(), "missing",
					tc.missing().isEmpty() ? null : List.copyOf(tc.missing()), "deadLetterQueue", tc.dlq()));
			if (!tc.missing().isEmpty()) {
				if (sink) {
					dx.warning("TOPIC_MISSING",
							"Sink'in okuduğu topic(ler) yok: " + String.join(", ", first(tc.missing(), MAX_EVIDENCE)),
							evidence("topics", first(tc.missing(), MAX_EVIDENCE)),
							"Topic adını düzeltin ya da topic'i oluşturun (topics_create)");
				}
				else {
					dx.info("TOPIC_MISSING",
							"Config'teki topic(ler) henüz yok: " + String.join(", ", first(tc.missing(), MAX_EVIDENCE)),
							evidence("topics", first(tc.missing(), MAX_EVIDENCE)),
							"auto.create.topics.enable kapalıysa ve connector topic oluşturmuyorsa topics_create ile oluşturun");
				}
			}
			if (tc.regexMatches() != null && tc.regexMatches() == 0) {
				dx.warning("TOPICS_REGEX_NO_MATCH", "topics.regex hiçbir topic ile eşleşmiyor: " + tc.regex(), null,
						"Regex'i (tam eşleşme gerekir) ve topic adlarını kontrol edin");
			}
			else if (tc.regexMatches() != null && tc.regexMatches() < 0) {
				dx.warning("TOPICS_REGEX_INVALID", "topics.regex geçersiz: " + tc.regex(), null, "Regex'i düzeltin");
			}
			if (tc.dlq() != null && !tc.dlqExists()) {
				boolean running = s.tasksInState(ConnectorStatus.RUNNING) > 0;
				String msg = "DLQ topic'i yok: " + tc.dlq() + " (Connect sink görevi başlarken oluşturur)";
				String rec = "Worker'ın topic oluşturma yetkisini (TOPIC CREATE) ve errors.deadletterqueue.topic.replication.factor "
						+ "değerini kontrol edin ya da topics_create ile oluşturun";
				if (running) {
					dx.warning("DLQ_TOPIC_MISSING", msg, null, rec);
				}
				else {
					dx.info("DLQ_TOPIC_MISSING", msg, null, rec);
				}
			}
		}
		if (!topics.isEmpty()) {
			facts.put("topics", topics);
		}
	}

	private static void activityFindings(Diagnosis dx, Map<String, Object> facts, Activity a, ConnectorStatus s) {
		if (a == null) {
			return;
		}
		OffsetWindow w = a.window();
		boolean running = ConnectorStatus.RUNNING.equals(s.state()) && s.tasksInState(ConnectorStatus.RUNNING) > 0;
		if (a.group() != null) {
			OffsetWindow.GroupProgress p = w.progress(a.group());
			Map<String, Object> lag = Results.map("group", a.group());
			if (w.committed0().getOrDefault(a.group(), Map.of()).isEmpty()) {
				lag.put("note", "Grupta commit'li offset yok");
				dx.info("SINK_GROUP_NO_OFFSETS",
						"Sink consumer group'unda (" + a.group() + ") commit'li offset yok: henüz veri işlenmemiş olabilir",
						evidence("group", a.group(), "error", w.groupErrors().get(a.group())),
						"Görevler RUNNING ise topic'e veri gelince commit oluşur; consumer.override.group.id kullanılıyorsa adı kontrol edin");
			}
			else {
				lag.putAll(p.summary());
				List<OffsetWindow.PartitionProgress> stuck = p.stuck();
				if (!stuck.isEmpty()) {
					Map<String, Object> ev = evidence("partitions",
							first(stuck.stream().map(OffsetWindow.PartitionProgress::toMap).toList(), MAX_EVIDENCE),
							"group", a.group());
					if (running) {
						dx.critical("SINK_STUCK",
								stuck.size() + " partition'da sink commit'i ilerlemiyor ama yeni mesaj geliyor", ev,
								"Görev loglarını ve hedef sistemi (yazma hatası, yavaşlık) kontrol edin; connectors_restart "
										+ "includeTasks=true onlyFailed=false ile görevleri yeniden başlatmayı deneyin");
					}
					else {
						dx.warning("SINK_STUCK", stuck.size() + " partition'da sink ilerlemiyor (görevler RUNNING değil)", ev,
								"Önce görev durumlarını düzeltin (TASKS_FAILED / PAUSED bulgularına bakın)");
					}
				}
				else if ("growing".equals(p.trend()) && p.lag() >= GROWING_LAG_MIN) {
					dx.warning("SINK_LAG_GROWING",
							"Sink lag'i büyüyor: tüketim " + p.consumeRate() + " msg/sn, üretim " + p.produceRate() + " msg/sn",
							evidence("lag", p.lag(), "lagAtStart", p.lag0()),
							"tasks.max'ı partition sayısına kadar artırın; hedef sistemin yazma kapasitesini ve batch ayarlarını kontrol edin");
				}
				else if (p.lag() >= HIGH_LAG) {
					dx.info("SINK_LAG_HIGH", "Sink lag'i " + p.lag(), evidence("lag", p.lag()), "Eğilimi izleyin");
				}
			}
			lag.putAll(w.describe());
			facts.put("sinkLag", lag);
		}
		Map<TopicPartition, Long> delta = w.endDelta();
		if (a.group() == null) {
			Map<String, long[]> byTopic = new TreeMap<>();
			delta.forEach((tp, d) -> {
				if (a.sourceTopics().contains(tp.topic())) {
					byTopic.computeIfAbsent(tp.topic(), k -> new long[1])[0] += d;
				}
			});
			long total = byTopic.values().stream().mapToLong(v -> v[0]).sum();
			List<Map<String, Object>> rows = new ArrayList<>();
			byTopic.forEach((t, v) -> rows.add(Results.map("topic", t, "msgsPerSec", w.rate(v[0]), "lastWriteTime",
					Format.iso(a.lastWrite().getOrDefault(t, -1L)), "lastWriteAgeSeconds",
					Format.ageSeconds(a.lastWrite().getOrDefault(t, -1L)))));
			Map<String, Object> src = Results.map("msgsPerSec", w.rate(total), "topics", rows, "topicsOmitted",
					a.sourceTopicsOmitted() > 0 ? a.sourceTopicsOmitted() : null);
			src.putAll(w.describe());
			facts.put("sourceActivity", src);
			long newest = a.lastWrite().values().stream().mapToLong(Long::longValue).max().orElse(-1);
			if (running && w.complete() && total == 0 && !a.sourceTopics().isEmpty()
					&& (newest < 0 || Format.ageSeconds(newest) > SOURCE_IDLE_SECONDS)) {
				dx.info("SOURCE_IDLE", "Kaynak connector çalışıyor ama topic'lerine yazmıyor"
						+ (newest < 0 ? "" : " (son yazma " + Format.ageSeconds(newest) / 60 + " dk önce)"),
						evidence("topics", first(a.sourceTopics(), MAX_EVIDENCE)),
						"Kaynak sistemde yeni veri var mı, connector offset'i ilerliyor mu (connectors_offsets) kontrol edin");
			}
		}
		if (a.dlq() != null) {
			String dlqTopic = a.dlq();
			long dlqDelta = delta.entrySet().stream().filter(e -> e.getKey().topic().equals(dlqTopic))
				.mapToLong(Map.Entry::getValue)
				.sum();
			if (dlqDelta > 0) {
				dx.warning("DLQ_RECEIVING", "Ölçüm sırasında DLQ'ya (" + dlqTopic + ") " + dlqDelta + " kayıt yazıldı",
						evidence("topic", dlqTopic, "records", dlqDelta),
						"DLQ kayıtlarının header'larındaki hata nedenini inceleyin (messages_headers)");
			}
		}
		if (!w.groupErrors().isEmpty() || !w.endErrors().isEmpty()) {
			dx.markPartial();
		}
	}

	private static void workers(Diagnosis dx, Map<String, Object> facts, List<ConnectorOverview> all,
			ConnectorStatus s) {
		Map<String, Integer> mine = new TreeMap<>();
		s.tasks().forEach(t -> mine.merge(t.workerId() == null ? "(atanmamış)" : t.workerId(), 1, Integer::sum));
		Map<String, Object> w = Results.map("tasksByWorker", mine);
		if (all != null) {
			Map<String, Integer> load = new TreeMap<>();
			for (ConnectorOverview c : all) {
				for (ConnectorStatus.Task t : c.status().tasks()) {
					if (t.workerId() != null) {
						load.merge(t.workerId(), 1, Integer::sum);
					}
				}
			}
			Map<String, Integer> shown = new LinkedHashMap<>();
			first(load.entrySet(), 20).forEach(e -> shown.put(e.getKey(), e.getValue()));
			w.put("clusterTasksByWorker", shown);
			w.put("workers", load.size());
			if (load.size() > 1) {
				int max = load.values().stream().mapToInt(Integer::intValue).max().orElse(0);
				int min = load.values().stream().mapToInt(Integer::intValue).min().orElse(0);
				double avg = load.values().stream().mapToInt(Integer::intValue).average().orElse(0);
				if (max - min > 2 && max > 2 * avg) {
					dx.info("WORKER_IMBALANCE", "Worker'lar arasında görev dağılımı dengesiz (en fazla " + max
							+ ", en az " + min + ")", evidence("tasksByWorker", load),
							"Connect incremental cooperative rebalancing kısa sürede dengeler; sürerse worker'ları kontrol edin");
				}
			}
		}
		Set<String> failedWorkers = s.failedTaskWorkers();
		if (!failedWorkers.isEmpty()) {
			w.put("failedTaskWorkers", List.copyOf(failedWorkers));
		}
		facts.put("workers", w);
	}

}
