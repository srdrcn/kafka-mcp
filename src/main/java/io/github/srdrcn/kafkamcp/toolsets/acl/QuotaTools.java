package io.github.srdrcn.kafkamcp.toolsets.acl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** quotas_list ve quotas_update (client kotaları: user, client-id, ip; varsayılan entity "<default>"). */
@Component
public class QuotaTools {

	public static final String DEFAULT = "<default>";

	/** user/client-id entity'leri için geçerli kota anahtarları. */
	public static final List<String> CLIENT_KEYS = List.of("producer_byte_rate", "consumer_byte_rate",
			"request_percentage", "controller_mutation_rate");

	/** ip entity'leri için geçerli kota anahtarları. */
	public static final List<String> IP_KEYS = List.of("connection_creation_rate");

	/** Parametre adı → Kafka entity tipi. */
	private static final Map<String, String> ENTITY_PARAMS = new LinkedHashMap<>();

	static {
		ENTITY_PARAMS.put("user", ClientQuotaEntity.USER);
		ENTITY_PARAMS.put("clientId", ClientQuotaEntity.CLIENT_ID);
		ENTITY_PARAMS.put("ip", ClientQuotaEntity.IP);
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("quotas_list", "Client kotalarını (user/client-id/ip, <default> dahil) ve değerlerini listeler")
			.listParams()
			.param("filter", P.string("Entity filtresi (regex; ör. user=alice, client-id=<default>, ip=10\\.)"))
			.handler(this::list),
				ToolSpec
					.write("quotas_update",
							"Client kotası ayarlar/kaldırır (user, clientId, ip; <default> destekli); önce/sonra değerleri döner")
					.param("user", P.string("Kullanıcı (varsayılan kullanıcı için <default>)"))
					.param("clientId", P.string("client.id (varsayılan için <default>)"))
					.param("ip", P.string("IP adresi (varsayılan için <default>); user/clientId ile birlikte verilmez"))
					.param("set", P.object("Ayarlanacak kotalar: anahtar → sayı (ör. {\"producer_byte_rate\": 1048576}). "
							+ "Anahtarlar: " + String.join(", ", CLIENT_KEYS) + "; ip için " + IP_KEYS.get(0)))
					.param("remove", P.stringArray("Kaldırılacak kota anahtarları"))
					.handler(this::update));
	}

	Object list(ToolCall call) {
		Map<ClientQuotaEntity, Map<String, Double>> all;
		try {
			all = call.await(call.admin().describeClientQuotas(ClientQuotaFilter.all()).entities());
		}
		catch (ToolException e) {
			String note = Acls.unsupportedNote(e, SecurityTools.QUOTA_REQUIREMENT);
			if (note != null) {
				return Acls.unsupported(note);
			}
			throw e;
		}
		Map<String, Map<String, Object>> rows = new TreeMap<>();
		for (Map.Entry<ClientQuotaEntity, Map<String, Double>> e : all.entrySet()) {
			String text = entityText(e.getKey());
			if (call.matches(text)) {
				rows.put(text, Results.map("entity", renderEntity(e.getKey()), "quotas", renderQuotas(e.getValue())));
			}
		}
		return Results.page("quotas", new ArrayList<>(rows.values()), call.limit());
	}

	Object update(ToolCall call) {
		Map<String, String> entries = new LinkedHashMap<>();
		ENTITY_PARAMS.forEach((param, type) -> {
			String v = call.str(param, null);
			if (v != null) {
				entries.put(type, DEFAULT.equals(v) ? null : v);
			}
		});
		if (entries.isEmpty()) {
			throw new ToolException("En az bir entity verilmeli: user, clientId ya da ip");
		}
		boolean ip = entries.containsKey(ClientQuotaEntity.IP);
		if (ip && entries.size() > 1) {
			throw new ToolException("ip, user/clientId ile birlikte kullanılamaz (ip kotaları ayrı entity'dir)");
		}
		List<String> allowed = ip ? IP_KEYS : CLIENT_KEYS;
		Map<String, Double> set = new LinkedHashMap<>();
		call.map("set").forEach((k, v) -> set.put(checkKey(k, allowed), number(k, v)));
		List<String> remove = new ArrayList<>();
		for (String k : call.strings("remove")) {
			if (set.containsKey(k)) {
				throw new ToolException(k + " hem set hem remove içinde olamaz");
			}
			remove.add(checkKey(k, allowed));
		}
		if (set.isEmpty() && remove.isEmpty()) {
			throw new ToolException("set ya da remove verilmeli");
		}
		ClientQuotaEntity entity = new ClientQuotaEntity(entries);
		Map<String, Double> before = describe(call, entity);
		List<ClientQuotaAlteration.Op> ops = new ArrayList<>();
		set.forEach((k, v) -> ops.add(new ClientQuotaAlteration.Op(k, v)));
		remove.forEach(k -> ops.add(new ClientQuotaAlteration.Op(k, null)));
		call.await(call.admin().alterClientQuotas(List.of(new ClientQuotaAlteration(entity, ops))).all());

		Map<String, Double> expected = new TreeMap<>(before);
		expected.putAll(set);
		remove.forEach(expected::remove);
		Map<String, Double> after = describe(call, entity);
		// Broker metadata'yı controller'dan biraz geç alabilir: kısa süre beklenen değerleri bekle.
		while (!after.equals(expected) && !call.nearDeadline(2_000)) {
			call.sleep(150);
			after = describe(call, entity);
		}
		return Results.map("entity", renderEntity(entity), "before", renderQuotas(before), "after",
				renderQuotas(after), "set", set.isEmpty() ? null : renderQuotas(set), "removed",
				remove.isEmpty() ? null : remove, "note", after.equals(expected) ? null
						: "Broker kota bilgisini henüz güncellememiş olabilir; birkaç saniye sonra quotas_list ile kontrol edin");
	}

	private static String checkKey(String key, List<String> allowed) {
		if (!allowed.contains(key)) {
			throw new ToolException("Geçersiz kota anahtarı: " + key + " (geçerli: " + String.join(", ", allowed) + ")");
		}
		return key;
	}

	private static Double number(String key, Object v) {
		if (v instanceof Number n) {
			return n.doubleValue();
		}
		try {
			return Double.parseDouble(String.valueOf(v).trim());
		}
		catch (NumberFormatException | NullPointerException e) {
			throw new ToolException("Kota değeri sayı olmalı: " + key + "=" + v);
		}
	}

	/** Tek entity'nin kotaları (containsOnly ile tam eşleşme); yoksa boş. */
	public static Map<String, Double> describe(ToolCall call, ClientQuotaEntity entity) {
		List<ClientQuotaFilterComponent> components = new ArrayList<>();
		entity.entries()
			.forEach((type, name) -> components.add(name == null ? ClientQuotaFilterComponent.ofDefaultEntity(type)
					: ClientQuotaFilterComponent.ofEntity(type, name)));
		Map<ClientQuotaEntity, Map<String, Double>> m = call
			.await(call.admin().describeClientQuotas(ClientQuotaFilter.containsOnly(components)).entities());
		for (Map.Entry<ClientQuotaEntity, Map<String, Double>> e : m.entrySet()) {
			if (Objects.equals(e.getKey().entries(), entity.entries())) {
				return new TreeMap<>(e.getValue());
			}
		}
		return new TreeMap<>();
	}

	/** {user, clientId, ip}; varsayılan entity "<default>". */
	public static Map<String, Object> renderEntity(ClientQuotaEntity entity) {
		Map<String, Object> m = new LinkedHashMap<>();
		ENTITY_PARAMS.forEach((param, type) -> {
			if (entity.entries().containsKey(type)) {
				String v = entity.entries().get(type);
				m.put(param, v == null ? DEFAULT : v);
			}
		});
		return m;
	}

	/** "user=alice,client-id=<default>" (filtre ve sıralama için). */
	public static String entityText(ClientQuotaEntity entity) {
		List<String> parts = new ArrayList<>();
		for (String type : ENTITY_PARAMS.values()) {
			if (entity.entries().containsKey(type)) {
				String v = entity.entries().get(type);
				parts.add(type + "=" + (v == null ? DEFAULT : v));
			}
		}
		return String.join(",", parts);
	}

	/** Tam sayı değerler long olarak (1048576.0 yerine 1048576). */
	public static Map<String, Object> renderQuotas(Map<String, Double> quotas) {
		Map<String, Object> m = new TreeMap<>();
		quotas.forEach((k, v) -> m.put(k, v != null && v == Math.rint(v) && Math.abs(v) < 1e15 ? (Object) v.longValue() : v));
		return m;
	}

}
