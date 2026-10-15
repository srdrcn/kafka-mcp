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

/** quotas_list and quotas_update (client quotas: user, client-id, ip; default entity "<default>"). */
@Component
public class QuotaTools {

	public static final String DEFAULT = "<default>";

	/** Valid quota keys for user/client-id entities. */
	public static final List<String> CLIENT_KEYS = List.of("producer_byte_rate", "consumer_byte_rate",
			"request_percentage", "controller_mutation_rate");

	/** Valid quota keys for ip entities. */
	public static final List<String> IP_KEYS = List.of("connection_creation_rate");

	/** Parameter name → Kafka entity type. */
	private static final Map<String, String> ENTITY_PARAMS = new LinkedHashMap<>();

	static {
		ENTITY_PARAMS.put("user", ClientQuotaEntity.USER);
		ENTITY_PARAMS.put("clientId", ClientQuotaEntity.CLIENT_ID);
		ENTITY_PARAMS.put("ip", ClientQuotaEntity.IP);
	}

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("quotas_list", "List client quotas (user/client-id/ip, <default> included) and their values")
			.listParams()
			.param("filter", P.string("Entity filter (regex; e.g. user=alice, client-id=<default>, ip=10\\.)"))
			.handler(this::list),
				ToolSpec
					.write("quotas_update",
							"Set or remove client quotas (user, clientId, ip; <default> supported); returns the before/after values")
					.param("user", P.string("User (<default> for the default user)"))
					.param("clientId", P.string("client.id (<default> for the default)"))
					.param("ip", P.string("IP address (<default> for the default); cannot be combined with user/clientId"))
					.param("set", P.object("Quotas to set: key → number (e.g. {\"producer_byte_rate\": 1048576}). "
							+ "Keys: " + String.join(", ", CLIENT_KEYS) + "; " + IP_KEYS.get(0) + " for ip"))
					.param("remove", P.stringArray("Quota keys to remove"))
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
			throw new ToolException("At least one entity is required: user, clientId or ip");
		}
		boolean ip = entries.containsKey(ClientQuotaEntity.IP);
		if (ip && entries.size() > 1) {
			throw new ToolException("ip cannot be combined with user/clientId (ip quotas are a separate entity)");
		}
		List<String> allowed = ip ? IP_KEYS : CLIENT_KEYS;
		Map<String, Double> set = new LinkedHashMap<>();
		call.map("set").forEach((k, v) -> set.put(checkKey(k, allowed), number(k, v)));
		List<String> remove = new ArrayList<>();
		for (String k : call.strings("remove")) {
			if (set.containsKey(k)) {
				throw new ToolException(k + " cannot be in both set and remove");
			}
			remove.add(checkKey(k, allowed));
		}
		if (set.isEmpty() && remove.isEmpty()) {
			throw new ToolException("set or remove is required");
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
		// The broker may get the metadata from the controller a little later: briefly wait for the expected values.
		while (!after.equals(expected) && !call.nearDeadline(2_000)) {
			call.sleep(150);
			after = describe(call, entity);
		}
		return Results.map("entity", renderEntity(entity), "before", renderQuotas(before), "after",
				renderQuotas(after), "set", set.isEmpty() ? null : renderQuotas(set), "removed",
				remove.isEmpty() ? null : remove, "note", after.equals(expected) ? null
						: "The broker may not have updated the quotas yet; check with quotas_list in a few seconds");
	}

	private static String checkKey(String key, List<String> allowed) {
		if (!allowed.contains(key)) {
			throw new ToolException("Invalid quota key: " + key + " (valid: " + String.join(", ", allowed) + ")");
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
			throw new ToolException("Quota value must be a number: " + key + "=" + v);
		}
	}

	/** Quotas of a single entity (exact match via containsOnly); empty if none. */
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

	/** {user, clientId, ip}; default entity "<default>". */
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

	/** "user=alice,client-id=<default>" (for filtering and sorting). */
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

	/** Integral values as long (1048576 instead of 1048576.0). */
	public static Map<String, Object> renderQuotas(Map<String, Double> quotas) {
		Map<String, Object> m = new TreeMap<>();
		quotas.forEach((k, v) -> m.put(k, v != null && v == Math.rint(v) && Math.abs(v) < 1e15 ? (Object) v.longValue() : v));
		return m;
	}

}
