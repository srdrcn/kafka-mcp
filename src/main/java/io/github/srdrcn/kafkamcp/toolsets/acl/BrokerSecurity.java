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

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Tek bir broker'ın güvenlik config'leri (tek describeConfigs). {@code super.users} ve
 * {@code allow.everyone.if.no.acl.found} broker config tanımında olmayan authorizer ayarları olduğundan
 * Kafka bunları sensitive (değersiz) döndürür (ölçülen: ZooKeeper 3.9 AclAuthorizer ve KRaft 4.1
 * StandardAuthorizer); bu durumda değerleri "gizli" kabul edilir ve çağıran not düşer. Metadata modu
 * (KRaft/ZooKeeper) aynı config'lerden çıkarılır: {@code process.roles} doluysa KRaft, değilse
 * {@code zookeeper.connect} doluysa ZooKeeper.
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

	/** KRaft broker'ında dolu (broker, controller); ZooKeeper broker'ında boş ya da yok. */
	public static final String PROCESS_ROLES = "process.roles";

	/** ZooKeeper broker'ında dolu; KRaft'ta boş (3.x) ya da yok (4.x). */
	public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";

	/** ZooKeeper modunda ACL'leri ve SCRAM bilgilerini tutan znode'ların ZooKeeper ACL'iyle korunup korunmadığı. */
	public static final String ZOOKEEPER_SET_ACL = "zookeeper.set.acl";

	/**
	 * Sadece KRaft broker'larında anlamlı güvenlik anahtarları (broker→controller bağlantısı). ZooKeeper 3.x
	 * broker'ları bunları varsayılan değerleriyle (ör. sasl.mechanism.controller.protocol=GSSAPI) döndürür,
	 * bu yüzden mod bilinmeden gösterilmez.
	 */
	public static final List<String> KRAFT_KEYS = List.of(PROCESS_ROLES, "controller.listener.names",
			"sasl.mechanism.controller.protocol");

	/**
	 * Sadece ZooKeeper broker'larında anlamlı güvenlik anahtarları (ACL ve SCRAM bilgileri ZooKeeper'da
	 * saklanır). KRaft 3.x broker'ları bunları varsayılan değerleriyle döndürür, 4.x hiç döndürmez.
	 */
	public static final List<String> ZOOKEEPER_KEYS = List.of(ZOOKEEPER_SET_ACL, "zookeeper.ssl.client.enable");

	/** Kuralları {@link AclEvaluator}'ın uyguladığı yerleşik authorizer'lar (KRaft, ZooKeeper, eski ZooKeeper adı). */
	public static final List<String> BUILT_IN_AUTHORIZERS = List.of(
			"org.apache.kafka.metadata.authorizer.StandardAuthorizer", "kafka.security.authorizer.AclAuthorizer",
			"kafka.security.auth.SimpleAclAuthorizer");

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

	/** authorizer.class.name; tanımsızsa ya da config'ler okunamadıysa null. */
	public String authorizer() {
		return value(AUTHORIZER);
	}

	/**
	 * Authorizer yerleşik değilse (ör. Ranger, Confluent RBAC) kararların farklı olabileceğini söyleyen not;
	 * yerleşikse, tanımsızsa ya da bilinmiyorsa null.
	 */
	public String customAuthorizerNote() {
		String a = authorizer();
		if (a == null || BUILT_IN_AUTHORIZERS.contains(a)) {
			return null;
		}
		return "authorizer.class.name=" + a + " yerleşik değil (KRaft: StandardAuthorizer, ZooKeeper: AclAuthorizer); "
				+ "bu authorizer'ın ek kuralları (ör. RBAC rolleri, Ranger politikaları) değerlendirilmedi";
	}

	/**
	 * Broker config'lerinden metadata modu: {@link ClusterMeta#KRAFT} (process.roles dolu),
	 * {@link ClusterMeta#ZOOKEEPER} (process.roles boş, zookeeper.connect dolu; ZK→KRaft geçişindeki ZooKeeper
	 * broker'ları dahil) ya da config'ler okunamadıysa/belirsizse null.
	 */
	public String mode() {
		if (error != null) {
			return null;
		}
		if (value(PROCESS_ROLES) != null) {
			return ClusterMeta.KRAFT;
		}
		return value(ZOOKEEPER_CONNECT) != null ? ClusterMeta.ZOOKEEPER : null;
	}

	/** Moda özgü gösterilecek anahtarlar (mod bilinmiyorsa boş). */
	public List<String> modeKeys() {
		String mode = mode();
		if (ClusterMeta.KRAFT.equals(mode)) {
			return KRAFT_KEYS;
		}
		return ClusterMeta.ZOOKEEPER.equals(mode) ? ZOOKEEPER_KEYS : List.of();
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

	/**
	 * security_summary için seçili config'ler (gizliler "gizli ..." olarak): {@link #KEYS} ve moda özgü
	 * {@link #KRAFT_KEYS} / {@link #ZOOKEEPER_KEYS}; mod bilinmiyorsa moda özgü anahtarlar gösterilmez.
	 */
	public Map<String, Object> render() {
		Map<String, Object> m = new LinkedHashMap<>();
		if (error != null) {
			m.put("error", error);
			return m;
		}
		m.put("brokerId", brokerId);
		List<String> keys = new ArrayList<>(KEYS);
		keys.addAll(modeKeys());
		for (String k : keys) {
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
