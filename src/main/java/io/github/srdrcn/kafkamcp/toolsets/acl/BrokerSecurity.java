package io.github.srdrcn.kafkamcp.toolsets.acl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Tek bir broker'ın güvenlik config'leri (tek describeConfigs). Kafka 4.x {@code super.users} ve
 * {@code allow.everyone.if.no.acl.found}'u sensitive (değersiz) döndürebilir; bu durumda değerleri
 * "gizli" kabul edilir ve çağıran not düşer.
 */
public final class BrokerSecurity {

	public static final String SUPER_USERS = "super.users";

	public static final String ALLOW_EVERYONE = "allow.everyone.if.no.acl.found";

	public static final String AUTHORIZER = "authorizer.class.name";

	/** security_summary'de gösterilen anahtarlar. */
	public static final List<String> KEYS = List.of("listener.security.protocol.map", "listeners",
			"inter.broker.listener.name", "sasl.enabled.mechanisms", "sasl.mechanism.inter.broker.protocol",
			AUTHORIZER, SUPER_USERS, ALLOW_EVERYONE, "ssl.client.auth");

	public static final String HIDDEN = "gizli (broker sensitive döndürdü)";

	private final int brokerId;

	private final Map<String, ConfigEntry> entries;

	private final String error;

	public BrokerSecurity(int brokerId, Collection<ConfigEntry> entries) {
		this(brokerId, entries, null);
	}

	private BrokerSecurity(int brokerId, Collection<ConfigEntry> entries, String error) {
		this.brokerId = brokerId;
		this.error = error;
		this.entries = new LinkedHashMap<>();
		for (ConfigEntry e : entries) {
			this.entries.put(e.name(), e);
		}
	}

	/** Config'ler okunamadığında: super.users ve allow.everyone.if.no.acl.found bilinmiyor kabul edilir. */
	public static BrokerSecurity unavailable(String error) {
		return new BrokerSecurity(-1, List.of(), error);
	}

	/** {@link #load(ToolCall)}; hata olursa {@link #unavailable(String)} (araç yine de çalışır). */
	public static BrokerSecurity tryLoad(ToolCall call) {
		try {
			return load(call);
		}
		catch (ToolException e) {
			return unavailable(e.getMessage());
		}
	}

	/** Config'ler okunamadıysa hata mesajı, yoksa null. */
	public String error() {
		return error;
	}

	/** describeCluster ile en küçük id'li broker'ı seçer ve onun config'lerini okur (2 istek). */
	public static BrokerSecurity load(ToolCall call) {
		Collection<Node> nodes = call.await(call.admin().describeCluster().nodes());
		Node node = nodes.stream()
			.min(Comparator.comparingInt(Node::id))
			.orElseThrow(() -> new ToolException("Cluster'da broker bulunamadı"));
		ConfigResource r = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node.id()));
		Config config = call.await(call.admin().describeConfigs(List.of(r)).all()).get(r);
		return new BrokerSecurity(node.id(), config == null ? List.of() : config.entries());
	}

	public int brokerId() {
		return brokerId;
	}

	/** Config değeri; yoksa, boşsa ya da gizliyse null. */
	public String value(String key) {
		ConfigEntry e = entries.get(key);
		if (e == null || e.value() == null || e.value().isBlank()) {
			return null;
		}
		return e.value();
	}

	/** Config broker tarafından sensitive olarak (değeri olmadan) mı döndü? */
	public boolean hidden(String key) {
		ConfigEntry e = entries.get(key);
		return e != null && e.isSensitive() && e.value() == null;
	}

	/** Gösterim değeri: değer, "gizli ..." ya da null. */
	public String display(String key) {
		return hidden(key) ? HIDDEN : value(key);
	}

	/** super.users; gizliyse ya da okunamadıysa null, tanımsızsa boş küme. */
	public Set<String> superUsers() {
		if (error != null || hidden(SUPER_USERS)) {
			return null;
		}
		return parseSuperUsers(value(SUPER_USERS));
	}

	/** allow.everyone.if.no.acl.found; gizliyse ya da okunamadıysa null. */
	public Boolean allowEveryoneIfNoAclFound() {
		if (error != null || hidden(ALLOW_EVERYONE)) {
			return null;
		}
		String v = value(ALLOW_EVERYONE);
		return v != null && Boolean.parseBoolean(v.trim());
	}

	public boolean authorizerEnabled() {
		return value(AUTHORIZER) != null;
	}

	/** listeners değerinden sadece listener adları (ör. SASL, INTERNAL). */
	public List<String> listenerNames() {
		String v = value("listeners");
		List<String> out = new ArrayList<>();
		if (v == null) {
			return out;
		}
		for (String l : v.split(",")) {
			String t = l.trim();
			int i = t.indexOf("://");
			if (!t.isEmpty()) {
				out.add(i < 0 ? t : t.substring(0, i));
			}
		}
		return out;
	}

	/** "User:a;User:b" → {User:a, User:b}. */
	public static Set<String> parseSuperUsers(String value) {
		Set<String> out = new LinkedHashSet<>();
		if (value == null) {
			return out;
		}
		for (String s : value.split(";")) {
			if (!s.isBlank()) {
				out.add(s.trim());
			}
		}
		return out;
	}

	/**
	 * Süper kullanıcı bilgisi: broker'dan okunan değer ile çağıranın verdiği liste birleşimi. Broker değeri
	 * gizliyse ve liste verilmemişse {@code known=false}.
	 */
	public record SuperUsers(Set<String> users, boolean known, String note) {

		public boolean contains(String principal) {
			return users.contains(principal);
		}

	}

	/** Süper kullanıcıları ve gerekirse açıklama notunu üretir. */
	public SuperUsers superUsers(Collection<String> provided) {
		Set<String> users = new LinkedHashSet<>();
		Set<String> fromBroker = superUsers();
		if (fromBroker != null) {
			users.addAll(fromBroker);
		}
		if (provided != null) {
			for (String p : provided) {
				String n = Acls.normalizePrincipal(p);
				if (n != null) {
					users.add(n);
				}
			}
		}
		String why = error != null ? "broker config'leri okunamadı (" + error + ")" : "broker'da gizli (sensitive) döndü";
		if (fromBroker == null && (provided == null || provided.isEmpty())) {
			return new SuperUsers(users, false, "super.users " + why + "; süper kullanıcı kontrolü yapılamadı. "
					+ "Biliyorsanız superUsers parametresiyle verin (süper kullanıcılar ACL'lerden bağımsız her şeye yetkilidir).");
		}
		if (fromBroker == null) {
			return new SuperUsers(users, true, "super.users " + why + "; superUsers parametresindeki liste kullanıldı.");
		}
		return new SuperUsers(users, true, null);
	}

	/** allow.everyone.if.no.acl.found için kullanılacak değer ve (gizliyse) notu. */
	public record AllowEveryone(boolean value, String note) {
	}

	public AllowEveryone allowEveryone() {
		Boolean v = allowEveryoneIfNoAclFound();
		if (v == null) {
			String why = error != null ? "okunamadı" : "broker'da gizli (sensitive) döndü";
			return new AllowEveryone(false, "allow.everyone.if.no.acl.found " + why + "; Kafka varsayılanı false kabul "
					+ "edildi. Broker'da true ise hiç ACL'i olmayan kaynaklar herkese açıktır.");
		}
		return new AllowEveryone(v, null);
	}

	/** security_summary için seçili config'ler (gizliler "gizli ..." olarak). */
	public Map<String, Object> render() {
		Map<String, Object> m = new LinkedHashMap<>();
		if (error != null) {
			m.put("error", error);
			return m;
		}
		m.put("brokerId", brokerId);
		for (String k : KEYS) {
			if ("listeners".equals(k)) {
				List<String> names = listenerNames();
				if (!names.isEmpty()) {
					m.put("listeners", names);
				}
				continue;
			}
			String v = display(k);
			if (v != null) {
				m.put(k, v);
			}
		}
		return m;
	}

}
