package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/**
 * Connector yazma araçları: pause/resume/stop/restart/task_restart/create_or_update ve destructive
 * connectors_delete, connectors_offsets_reset. Durum değiştiren araçlar yeni durumu kısa ve sınırlı bir
 * süre yoklayarak döner.
 */
@Component
public class ConnectorWriteTools {

	/** Durum geçişi için en fazla bekleme. */
	static final long STATE_WAIT_MS = 6_000;

	private final ConnectApi api;

	public ConnectorWriteTools(ConnectApi api) {
		this.api = api;
	}

	public List<ToolSpec> tools() {
		Map<String, Object> name = ConnectorReadTools.nameParam();
		return List.of(
				ToolSpec.write("connectors_pause", "Connector'ı ve görevlerini duraklatır (PAUSED)")
					.connect()
					.required("name", name)
					.handler(this::pause),
				ToolSpec.write("connectors_resume", "Duraklatılmış/durdurulmuş connector'ı devam ettirir (RUNNING)")
					.connect()
					.required("name", name)
					.handler(this::resume),
				ToolSpec.write("connectors_stop", "Connector'ı durdurur (STOPPED: görevler kapanır, config korunur)")
					.connect()
					.required("name", name)
					.handler(this::stop),
				ToolSpec
					.write("connectors_restart",
							"Connector'ı yeniden başlatır (isteğe bağlı görevleriyle ya da sadece FAILED olanları)")
					.connect()
					.required("name", name)
					.param("includeTasks", P.bool("Görevleri de yeniden başlat (varsayılan false)"))
					.param("onlyFailed", P.bool("Sadece FAILED connector/görevleri yeniden başlat (varsayılan false)"))
					.handler(this::restart),
				ToolSpec.write("connectors_task_restart", "Connector'ın tek bir görevini yeniden başlatır")
					.connect()
					.required("name", name)
					.required("taskId", P.integer("Görev id'si (0'dan başlar)"))
					.handler(this::taskRestart),
				ToolSpec.write("connectors_create_or_update", "Connector oluşturur ya da config'ini tamamen değiştirir")
					.connect()
					.required("name", name)
					.required("config", P.stringMap("Connector config'i (anahtar → string değer; connector.class dahil)"))
					.handler(this::createOrUpdate),
				ToolSpec.destructive("connectors_delete", "Connector'ı siler (geri alınamaz)")
					.connect()
					.required("name", name)
					.handler(this::delete),
				ToolSpec
					.destructive("connectors_offsets_reset",
							"STOPPED durumdaki connector'ın offset'lerini sıfırlar (geri alınamaz)")
					.connect()
					.required("name", name)
					.handler(this::offsetsReset));
	}

	private Map<String, Object> stateChange(ToolCall call, String action, String endpoint, String target,
			Predicate<ConnectorStatus> until) {
		String name = call.str("name");
		ConnectClient.Response r = api.client().send(call.cluster(), "PUT", ConnectApi.path(name) + endpoint, null);
		return withState(Results.map("name", name, "action", action, "httpStatus", r.status()), target, null,
				api.await(call, name, 0, STATE_WAIT_MS, until));
	}

	/**
	 * Yoklama sonucunu çıktıya ekler; yerleşmediyse nedenine göre kısa not düşer ({@code before}: çağrı öncesi
	 * durum, biliniyorsa).
	 */
	private static Map<String, Object> withState(Map<String, Object> out, String target, ConnectorStatus before,
			ConnectApi.Awaited a) {
		ConnectorStatus s = a.status();
		if (s != null) {
			out.put("status", s.states());
		}
		out.put("settled", a.settled());
		if (!a.settled()) {
			String note;
			if (s != null && s.equals(before)) {
				note = "Durum çağrı öncesiyle aynı: işlem henüz uygulanmamış ya da görülemeyecek kadar hızlı bitip aynı"
						+ " duruma dönmüş olabilir";
			}
			else if (target != null && s != null && target.equals(s.state())) {
				note = "Connector " + target + " ama görevler henüz oturmadı";
			}
			else {
				note = target == null ? "Durum henüz oturmadı" : "Durum henüz " + target + " değil";
			}
			out.put("note",
					note + "; connectors_status ile tekrar bakın" + (a.error() == null ? "" : " (" + a.error() + ")"));
		}
		return out;
	}

	Object pause(ToolCall call) {
		return stateChange(call, "pause", "/pause", ConnectorStatus.PAUSED, s -> s.allIn(ConnectorStatus.PAUSED));
	}

	Object resume(ToolCall call) {
		return stateChange(call, "resume", "/resume", ConnectorStatus.RUNNING,
				ConnectorSettle.resumed(System::nanoTime));
	}

	Object stop(ToolCall call) {
		return stateChange(call, "stop", "/stop", ConnectorStatus.STOPPED,
				s -> ConnectorStatus.STOPPED.equals(s.state()) && s.tasks().isEmpty());
	}

	Object restart(ToolCall call) {
		String name = call.str("name");
		boolean includeTasks = call.bool("includeTasks", false);
		boolean onlyFailed = call.bool("onlyFailed", false);
		ConnectorStatus before = api.statusOrNull(call.cluster(), name);
		ConnectClient.Response r = api.client()
			.send(call.cluster(), "POST", ConnectApi.path(name) + "/restart?includeTasks=" + includeTasks
					+ "&onlyFailed=" + onlyFailed, null);
		Map<String, Object> out = Results.map("name", name, "action", "restart", "includeTasks", includeTasks,
				"onlyFailed", onlyFailed, "httpStatus", r.status());
		// Yerleşmiş sayılmak için değişmesi (ya da RESTARTING/UNASSIGNED görülmesi) gereken öğeler. 202: istek
		// config topic'ine yazılır, worker'lar planı sonra uygular → plandaki her öğe. 204: connector zaten
		// yeniden başlatıldı, ama önceden FAILED ise status önbelleği eski FAILED'ı bir süre daha gösterebilir.
		List<Integer> mustChange = new ArrayList<>();
		if (r.body().isObject()) {
			// 202: Connect'in yeniden başlatacaklarını RESTARTING olarak işaretlediği plan
			ConnectorStatus planned = ConnectorStatus.from(r.body());
			boolean connector = ConnectorStatus.RESTARTING.equals(planned.state());
			List<Integer> tasks = planned.tasks()
				.stream()
				.filter(t -> ConnectorStatus.RESTARTING.equals(t.state()))
				.map(ConnectorStatus.Task::id)
				.toList();
			out.put("restarting", Results.map("connector", connector, "tasks", tasks));
			if (connector) {
				mustChange.add(ConnectorSettle.CONNECTOR);
			}
			mustChange.addAll(tasks);
		}
		else if (before != null && ConnectorStatus.FAILED.equals(before.state())) {
			mustChange.add(ConnectorSettle.CONNECTOR);
		}
		Predicate<ConnectorStatus> changed = ConnectorSettle.changedFrom(before, mustChange);
		// 202'de geçişi (RESTARTING/UNASSIGNED) kaçırmamak için hemen yokla; 204'te yeni durumun okunmasını bekle
		long initialDelay = r.body().isObject() ? 0 : 500;
		return withState(out, null, before,
				api.await(call, name, initialDelay, STATE_WAIT_MS, s -> changed.test(s) && !s.inTransition()));
	}

	Object taskRestart(ToolCall call) {
		String name = call.str("name");
		long taskId = call.requiredLong("taskId");
		if (taskId < 0) {
			throw new ToolException("taskId negatif olamaz");
		}
		ConnectorStatus before = api.statusOrNull(call.cluster(), name);
		ConnectClient.Response r = api.client()
			.send(call.cluster(), "POST", ConnectApi.path(name) + "/tasks/" + taskId + "/restart", null);
		int id = (int) taskId;
		// 204: görev yeniden başlatıldı; ama önceden FAILED ise status önbelleği eski FAILED'ı bir süre daha
		// gösterebilir → görev değişmeden (ya da geçişte görülmeden) yerleşmiş sayılmaz.
		ConnectorStatus.Task old = before == null ? null : task(before, id);
		Predicate<ConnectorStatus> changed = ConnectorSettle.changedFrom(before,
				old != null && old.failed() ? List.of(id) : List.of());
		ConnectApi.Awaited a = api.await(call, name, 500, STATE_WAIT_MS, s -> changed.test(s) && s.tasks()
			.stream()
			.anyMatch(t -> t.id() == id && (ConnectorStatus.RUNNING.equals(t.state()) || t.failed())));
		Map<String, Object> out = Results.map("name", name, "action", "task_restart", "taskId", taskId, "httpStatus",
				r.status());
		ConnectorStatus.Task now = a.status() == null ? null : task(a.status(), id);
		if (now != null) {
			out.put("task", now.toMap());
		}
		out.put("settled", a.settled());
		if (!a.settled()) {
			out.put("note", (now != null && now.equals(old)
					? "Görev durumu çağrı öncesiyle aynı: yeniden başlatma henüz yansımamış ya da görev aynı hatayla"
							+ " yine düşmüş olabilir"
					: "Görev henüz RUNNING/FAILED değil") + "; connectors_status ile tekrar bakın"
					+ (a.error() == null ? "" : " (" + a.error() + ")"));
		}
		return out;
	}

	private static ConnectorStatus.Task task(ConnectorStatus s, int id) {
		return s.tasks().stream().filter(t -> t.id() == id).findFirst().orElse(null);
	}

	Object createOrUpdate(ToolCall call) {
		String name = call.str("name");
		Map<String, String> config = call.stringMap("config");
		if (config.isEmpty()) {
			throw new ToolException("config boş olamaz (en az connector.class gerekli)");
		}
		config.forEach((k, v) -> {
			if (v == null) {
				throw new ToolException("config değeri null olamaz: " + k);
			}
		});
		ConnectorStatus before = api.statusOrNull(call.cluster(), name);
		ConnectClient.Response r = api.client().send(call.cluster(), "PUT", ConnectApi.path(name) + "/config", config);
		boolean created = r.status() == 201;
		JsonNode body = r.body();
		String connectorClass = ConnectJson.text(body.path("config"), ConnectorInfo.CONNECTOR_CLASS);
		Map<String, Object> out = Results.map("name", name, "result", created ? "created" : "updated",
				"httpStatus", r.status(), "type", ConnectJson.text(body, "type"), "class",
				connectorClass == null ? config.get(ConnectorInfo.CONNECTOR_CLASS) : connectorClass);
		// Güncellemede (200) Connect connector'ı/görevleri yanıttan sonra yeniden başlatır: çağrı öncesi durum
		// değişmeden (ya da geçişte görülmeden) yerleşmiş sayılmaz.
		ConnectorStatus old = created ? null : before;
		return withState(out, null, old,
				api.await(call, name, 0, STATE_WAIT_MS, ConnectorSettle.applied(old, System::nanoTime)));
	}

	Object delete(ToolCall call) {
		String name = call.str("name");
		ConnectClient.Response r = api.client().send(call.cluster(), "DELETE", ConnectApi.path(name), null);
		return Results.map("name", name, "deleted", true, "httpStatus", r.status());
	}

	Object offsetsReset(ToolCall call) {
		String name = call.str("name");
		ConnectorStatus s = api.status(call.cluster(), name);
		if (!ConnectorStatus.STOPPED.equals(s.state())) {
			throw new ToolException("Connector '" + name + "' durumu " + s.state()
					+ "; offset'ler sadece STOPPED durumunda sıfırlanabilir. Önce connectors_stop ile durdurun.");
		}
		ConnectClient.Response r = api.client()
			.send(call.cluster(), "DELETE", ConnectApi.path(name) + "/offsets", null);
		return Results.map("name", name, "reset", true, "httpStatus", r.status(), "message",
				ConnectJson.text(r.body(), "message"));
	}

}
