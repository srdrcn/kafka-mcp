package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * {@link ConnectClient} üzerinde ayrıştırılmış Connect REST çağrıları: tek çağrıda tüm connector'lar
 * (expand=status+info), tek connector'ın durumu/info'su/config'i, plugin çözümleme ve durum geçişi için
 * sınırlı bekleme.
 */
@Component
public class ConnectApi {

	/** Durum yoklamaları arası bekleme. */
	static final long POLL_MS = 300;

	/** Yoklamayı bırakırken araç deadline'ına bırakılan pay (son REST çağrısı + yanıt için). */
	static final long POLL_RESERVE_MS = 3_000;

	private final ConnectClient client;

	public ConnectApi(ConnectClient client) {
		this.client = client;
	}

	public ConnectClient client() {
		return client;
	}

	/** {@code /connectors/{kodlanmış ad}} */
	public static String path(String connector) {
		return "/connectors/" + ConnectClient.enc(connector);
	}

	/** Tüm connector'lar ada göre sıralı, tek REST çağrısıyla (expand=status&expand=info). */
	public List<ConnectorOverview> overview(String cluster) {
		JsonNode r = client.get(cluster, "/connectors?expand=status&expand=info");
		if (!r.isObject()) {
			throw new ToolException("Connect REST expand=status/info desteklemiyor (Connect 2.3+ gerekli)");
		}
		List<ConnectorOverview> out = new ArrayList<>();
		r.properties().forEach(e -> {
			JsonNode s = e.getValue().get("status");
			JsonNode i = e.getValue().get("info");
			ConnectorStatus status = s == null || s.isNull() ? new ConnectorStatus(e.getKey(), null, null, null, null,
					List.of()) : ConnectorStatus.from(s);
			out.add(new ConnectorOverview(e.getKey(), status, i == null || i.isNull() ? null : ConnectorInfo.from(i)));
		});
		out.sort(Comparator.comparing(ConnectorOverview::name));
		return out;
	}

	public ConnectorStatus status(String cluster, String connector) {
		return ConnectorStatus.from(client.get(cluster, path(connector) + "/status"));
	}

	public ConnectorInfo info(String cluster, String connector) {
		return ConnectorInfo.from(client.get(cluster, path(connector)));
	}

	/** Connector'ın ham (maskelenmemiş) config'i. */
	public Map<String, String> config(String cluster, String connector) {
		return ConnectJson.stringMap(client.get(cluster, path(connector) + "/config"));
	}

	/** Görev id → ham görev config'i ({@code GET /connectors/{name}/tasks}). */
	public Map<Integer, Map<String, String>> taskConfigs(String cluster, String connector) {
		Map<Integer, Map<String, String>> out = new LinkedHashMap<>();
		for (JsonNode t : client.get(cluster, path(connector) + "/tasks")) {
			out.put(t.path("id").path("task").asInt(), ConnectJson.stringMap(t.get("config")));
		}
		return out;
	}

	/** Yüklü connector plugin'lerinin tam sınıf adları ({@code GET /connector-plugins}). */
	public List<String> connectorPlugins(String cluster) {
		List<String> classes = new ArrayList<>();
		for (JsonNode n : client.get(cluster, "/connector-plugins")) {
			String cls = ConnectJson.text(n, "class");
			if (cls != null) {
				classes.add(cls);
			}
		}
		return classes;
	}

	/**
	 * Plugin adını (tam sınıf, kısa ad ya da "Connector" eki olmadan; büyük/küçük harf duyarsız) verilen
	 * plugin sınıflarından birine çevirir; bulunamazsa {@link ToolException}.
	 */
	public static String resolvePlugin(List<String> pluginClasses, String plugin) {
		String p = plugin.trim().toLowerCase(Locale.ROOT);
		for (String cls : pluginClasses) {
			String full = cls.toLowerCase(Locale.ROOT);
			String simple = ConnectJson.shortClass(cls).toLowerCase(Locale.ROOT);
			if (p.equals(full) || p.equals(simple)
					|| simple.endsWith("connector") && p.equals(simple.substring(0, simple.length() - 9))) {
				return cls;
			}
		}
		throw new ToolException("Connector plugin'i bulunamadı: " + plugin + " (yüklü: "
				+ pluginClasses.stream().map(ConnectJson::shortClass).toList() + ")");
	}

	/** Durum yoklamasının sonucu: son durum, hedefe ulaşıldı mı, son hata (durum okunamadıysa). */
	public record Awaited(ConnectorStatus status, boolean settled, String error) {
	}

	/**
	 * Connector durumu {@code until} sağlanana kadar en fazla {@code maxMillis} (ve araç deadline'ına
	 * {@value #POLL_RESERVE_MS} ms kalana kadar) yoklar. Durum okunamazsa (ör. henüz 404) denemeye devam
	 * eder.
	 */
	public Awaited await(ToolCall call, String connector, long initialDelayMillis, long maxMillis,
			Predicate<ConnectorStatus> until) {
		long end = System.nanoTime() + maxMillis * 1_000_000L;
		if (initialDelayMillis > 0 && !call.nearDeadline(POLL_RESERVE_MS + initialDelayMillis)) {
			pause(initialDelayMillis);
		}
		ConnectorStatus last = null;
		String error = null;
		while (true) {
			try {
				last = status(call.cluster(), connector);
				error = null;
				if (until.test(last)) {
					return new Awaited(last, true, null);
				}
			}
			catch (ToolException e) {
				error = e.getMessage();
			}
			if (System.nanoTime() >= end || call.nearDeadline(POLL_RESERVE_MS + POLL_MS)) {
				return new Awaited(last, false, error);
			}
			pause(POLL_MS);
		}
	}

	private static void pause(long millis) {
		try {
			Thread.sleep(millis);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ToolException("Araç iptal edildi");
		}
	}

}
