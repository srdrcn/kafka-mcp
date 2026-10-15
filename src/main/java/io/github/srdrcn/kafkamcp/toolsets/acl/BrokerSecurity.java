package io.github.srdrcn.kafkamcp.toolsets.acl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.KafkaVersions;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Security configs of a single broker (one describeConfigs). {@code super.users} and
 * {@code allow.everyone.if.no.acl.found} are authorizer settings that are not in the broker config definition,
 * so Kafka returns them as sensitive (without a value) (measured: ZooKeeper 3.9 AclAuthorizer and KRaft 4.1
 * StandardAuthorizer); in that case their values are treated as "hidden" and the caller adds a note. The
 * metadata mode (KRaft/ZooKeeper) is derived from the same configs: KRaft if {@code process.roles} is set,
 * otherwise ZooKeeper if {@code zookeeper.connect} is set. {@code sasl.mechanism.inter.broker.protocol} and
 * {@code sasl.mechanism.controller.protocol} come back with the default (GSSAPI) even when the listener does not
 * use SASL; so the security protocol of the listener ({@code listener.security.protocol.map}) is shown and the
 * mechanism is shown only for a SASL_* listener.
 */
public final class BrokerSecurity {

	public static final String SUPER_USERS = "super.users";

	public static final String ALLOW_EVERYONE = "allow.everyone.if.no.acl.found";

	public static final String AUTHORIZER = "authorizer.class.name";

	public static final String INTER_BROKER_SASL_MECHANISM = "sasl.mechanism.inter.broker.protocol";

	public static final String CONTROLLER_SASL_MECHANISM = "sasl.mechanism.controller.protocol";

	/** inter.broker.protocol.version on a ZooKeeper broker (e.g. 3.9-IV0); absent on KRaft 4.x. */
	public static final String INTER_BROKER_PROTOCOL_VERSION = "inter.broker.protocol.version";

	/** render(): security protocol of the inter-broker listener (e.g. PLAINTEXT, SASL_SSL). */
	public static final String INTER_BROKER_SECURITY_PROTOCOL = "interBrokerSecurityProtocol";

	/** render(): security protocol of the listener a KRaft broker uses to connect to the controllers. */
	public static final String CONTROLLER_SECURITY_PROTOCOL = "controllerListenerSecurityProtocol";

	/** If absent from listener.security.protocol.map, the listener name may be the protocol name itself. */
	private static final List<String> SECURITY_PROTOCOLS = List.of("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL");

	/** Keys shown in security_summary. */
	public static final List<String> KEYS = List.of("listener.security.protocol.map", "listeners",
			"inter.broker.listener.name", "sasl.enabled.mechanisms", INTER_BROKER_SASL_MECHANISM, AUTHORIZER,
			SUPER_USERS, ALLOW_EVERYONE, "ssl.client.auth");

	public static final String HIDDEN = "hidden (broker returned it as sensitive)";

	/** Set on a KRaft broker (broker, controller); empty or absent on a ZooKeeper broker. */
	public static final String PROCESS_ROLES = "process.roles";

	/** Set on a ZooKeeper broker; empty (3.x) or absent (4.x) on KRaft. */
	public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";

	/** Whether the znodes holding ACLs and SCRAM credentials in ZooKeeper mode are protected by ZooKeeper ACLs. */
	public static final String ZOOKEEPER_SET_ACL = "zookeeper.set.acl";

	/**
	 * Security keys that only make sense on KRaft brokers (broker→controller connection). ZooKeeper 3.x brokers
	 * return them with their default values (e.g. sasl.mechanism.controller.protocol=GSSAPI), so they are not
	 * shown while the mode is unknown. On KRaft too, sasl.mechanism.controller.protocol is only shown if the
	 * controller listener is SASL_*.
	 */
	public static final List<String> KRAFT_KEYS = List.of(PROCESS_ROLES, "controller.listener.names",
			CONTROLLER_SASL_MECHANISM);

	/**
	 * Security keys that only make sense on ZooKeeper brokers (ACLs and SCRAM credentials are stored in
	 * ZooKeeper). KRaft 3.x brokers return them with their default values, 4.x does not return them at all.
	 */
	public static final List<String> ZOOKEEPER_KEYS = List.of(ZOOKEEPER_SET_ACL, "zookeeper.ssl.client.enable");

	/** Built-in authorizers whose rules {@link AclEvaluator} implements (KRaft, ZooKeeper, old ZooKeeper name). */
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

	/** When the configs cannot be read: super.users and allow.everyone.if.no.acl.found are treated as unknown. */
	public static BrokerSecurity unavailable(String error) {
		return new BrokerSecurity(-1, List.of(), error);
	}

	/** {@link #load(ToolCall)}; {@link #unavailable(String)} on error (the tool still works). */
	public static BrokerSecurity tryLoad(ToolCall call) {
		try {
			return load(call);
		}
		catch (ToolException e) {
			return unavailable(e.getMessage());
		}
	}

	/** Error message if the configs could not be read, otherwise null. */
	public String error() {
		return error;
	}

	/** Picks the broker with the lowest id via describeCluster and reads its configs (2 requests). */
	public static BrokerSecurity load(ToolCall call) {
		Collection<Node> nodes = call.await(call.admin().describeCluster().nodes());
		Node node = nodes.stream()
			.min(Comparator.comparingInt(Node::id))
			.orElseThrow(() -> new ToolException("No broker found in the cluster"));
		ConfigResource r = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node.id()));
		Config config = call.await(call.admin().describeConfigs(List.of(r)).all()).get(r);
		return new BrokerSecurity(node.id(), config == null ? List.of() : config.entries());
	}

	public int brokerId() {
		return brokerId;
	}

	/** Config value; null if absent, empty or hidden. */
	public String value(String key) {
		ConfigEntry e = entries.get(key);
		if (e == null || e.value() == null || e.value().isBlank()) {
			return null;
		}
		return e.value();
	}

	/** Did the broker return the config as sensitive (without a value)? */
	public boolean hidden(String key) {
		ConfigEntry e = entries.get(key);
		return e != null && e.isSensitive() && e.value() == null;
	}

	/** Display value: the value, "hidden ..." or null. */
	public String display(String key) {
		return hidden(key) ? HIDDEN : value(key);
	}

	/** super.users; null if hidden or unreadable, an empty set if not set. */
	public Set<String> superUsers() {
		if (error != null || hidden(SUPER_USERS)) {
			return null;
		}
		return parseSuperUsers(value(SUPER_USERS));
	}

	/** allow.everyone.if.no.acl.found; null if hidden or unreadable. */
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

	/** authorizer.class.name; null if not set or the configs could not be read. */
	public String authorizer() {
		return value(AUTHORIZER);
	}

	/**
	 * Note saying decisions may differ if the authorizer is not built in (e.g. Ranger, Confluent RBAC); null if
	 * it is built in, not set or unknown.
	 */
	public String customAuthorizerNote() {
		String a = authorizer();
		if (a == null || BUILT_IN_AUTHORIZERS.contains(a)) {
			return null;
		}
		return "authorizer.class.name=" + a + " is not built in (KRaft: StandardAuthorizer, ZooKeeper: AclAuthorizer); "
				+ "extra rules of this authorizer (e.g. RBAC roles, Ranger policies) were not evaluated";
	}

	/**
	 * Metadata mode from the broker configs: {@link ClusterMeta#KRAFT} (process.roles set),
	 * {@link ClusterMeta#ZOOKEEPER} (process.roles empty, zookeeper.connect set; ZooKeeper brokers in a ZK→KRaft
	 * migration included) or null if the configs could not be read or are ambiguous.
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

	/** Mode-specific keys to show (empty if the mode is unknown). */
	public List<String> modeKeys() {
		String mode = mode();
		if (ClusterMeta.KRAFT.equals(mode)) {
			return KRAFT_KEYS;
		}
		return ClusterMeta.ZOOKEEPER.equals(mode) ? ZOOKEEPER_KEYS : List.of();
	}

	/** Only the listener names from the listeners value (e.g. SASL, INTERNAL). */
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

	/**
	 * Security protocol of the listener name: its entry in listener.security.protocol.map; if absent from the
	 * map and the name is a protocol name (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL), the name itself; null if unknown.
	 */
	public String securityProtocolOf(String listenerName) {
		if (listenerName == null || listenerName.isBlank()) {
			return null;
		}
		String name = listenerName.trim();
		String map = value("listener.security.protocol.map");
		if (map != null) {
			for (String pair : map.split(",")) {
				int i = pair.indexOf(':');
				if (i > 0 && pair.substring(0, i).trim().equalsIgnoreCase(name)) {
					String protocol = pair.substring(i + 1).trim().toUpperCase(Locale.ROOT);
					return protocol.isEmpty() ? null : protocol;
				}
			}
		}
		String upper = name.toUpperCase(Locale.ROOT);
		return SECURITY_PROTOCOLS.contains(upper) ? upper : null;
	}

	/**
	 * Security protocol of the inter-broker connection: the protocol of inter.broker.listener.name if set,
	 * otherwise security.inter.broker.protocol; null if unknown.
	 */
	public String interBrokerSecurityProtocol() {
		if (error != null) {
			return null;
		}
		String listener = value("inter.broker.listener.name");
		if (listener != null) {
			return securityProtocolOf(listener);
		}
		String protocol = value("security.inter.broker.protocol");
		return protocol == null ? null : protocol.trim().toUpperCase(Locale.ROOT);
	}

	/** Listener a KRaft broker uses to connect to the controllers (first of controller.listener.names); null if not KRaft. */
	public String controllerListener() {
		if (!ClusterMeta.KRAFT.equals(mode())) {
			return null;
		}
		String v = value("controller.listener.names");
		if (v == null) {
			return null;
		}
		String first = v.split(",")[0].trim();
		return first.isEmpty() ? null : first;
	}

	/** KRaft: security protocol of the controller listener; null if not KRaft or unknown. */
	public String controllerSecurityProtocol() {
		return securityProtocolOf(controllerListener());
	}

	/** Is it SASL_PLAINTEXT or SASL_SSL? */
	public static boolean isSasl(String securityProtocol) {
		return securityProtocol != null && securityProtocol.startsWith("SASL_");
	}

	/**
	 * Does the non-transactional idempotent producer (3.0+ client default) need CLUSTER IDEMPOTENT_WRITE? Since
	 * Kafka 2.8 (KIP-679) WRITE on any topic is enough. true: ZooKeeper broker and inter.broker.protocol.version
	 * older than 2.8 or unreadable (conservative, since the IBP can be pinned below the software version); false:
	 * KRaft or IBP 2.8+; null: mode unknown.
	 */
	public Boolean idempotentWriteRequired() {
		String mode = mode();
		if (ClusterMeta.KRAFT.equals(mode)) {
			return false;
		}
		if (!ClusterMeta.ZOOKEEPER.equals(mode)) {
			return null;
		}
		return !atLeast(KafkaVersions.releaseOf(value(INTER_BROKER_PROTOCOL_VERSION)), 2, 8);
	}

	/** Is a version like "3.9" greater than or equal to major.minor? False if it cannot be parsed. */
	static boolean atLeast(String release, int major, int minor) {
		if (release == null) {
			return false;
		}
		String[] parts = release.split("\\.");
		try {
			int ma = Integer.parseInt(parts[0].trim());
			int mi = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
			return ma > major || (ma == major && mi >= minor);
		}
		catch (NumberFormatException e) {
			return false;
		}
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
	 * Super user info: union of the value read from the broker and the list given by the caller.
	 * {@code known=false} if the broker value is hidden and no list was given.
	 */
	public record SuperUsers(Set<String> users, boolean known, String note) {

		public boolean contains(String principal) {
			return users.contains(principal);
		}

	}

	/** Builds the super users and, if needed, an explanatory note. */
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
		String why = error != null ? "could not be read from the broker configs (" + error + ")" : "was returned as hidden (sensitive) by the broker";
		if (fromBroker == null && (provided == null || provided.isEmpty())) {
			return new SuperUsers(users, false, "super.users " + why + "; super users could not be checked. "
					+ "If you know them, pass them in superUsers (super users are authorized for everything regardless of ACLs).");
		}
		if (fromBroker == null) {
			return new SuperUsers(users, true, "super.users " + why + "; the list from the superUsers parameter was used.");
		}
		return new SuperUsers(users, true, null);
	}

	/**
	 * Value to use for allow.everyone.if.no.acl.found and (if hidden) its note. {@code assumed=true}: the broker
	 * value is hidden/unreadable and no parameter was given, so the Kafka default false was assumed.
	 */
	public record AllowEveryone(boolean value, String note, boolean assumed) {

		public AllowEveryone(boolean value, String note) {
			this(value, note, false);
		}

	}

	public AllowEveryone allowEveryone() {
		return allowEveryone(null);
	}

	/**
	 * The broker value if it can be read; if hidden/unreadable, the value given by the caller; otherwise false
	 * (assumed).
	 */
	public AllowEveryone allowEveryone(Boolean provided) {
		Boolean v = allowEveryoneIfNoAclFound();
		if (v != null) {
			return new AllowEveryone(v, provided != null && !provided.equals(v)
					? "allowEveryoneIfNoAclFound parameter ignored: the broker value " + v + " was used" : null);
		}
		String why = error != null ? "could not be read" : "was returned as hidden (sensitive) by the broker";
		if (provided != null) {
			return new AllowEveryone(provided, "allow.everyone.if.no.acl.found " + why
					+ "; the value from the allowEveryoneIfNoAclFound parameter (" + provided + ") was used.");
		}
		return new AllowEveryone(false, "allow.everyone.if.no.acl.found " + why + "; assumed the Kafka default "
				+ "false. If it is true on the broker, resources without any ACLs are open to everyone; if you know "
				+ "the value, pass it in allowEveryoneIfNoAclFound.", true);
	}

	/**
	 * Selected configs for security_summary (hidden ones as "hidden ..."): {@link #KEYS} and the mode-specific
	 * {@link #KRAFT_KEYS} / {@link #ZOOKEEPER_KEYS}; mode-specific keys are not shown if the mode is unknown.
	 * The security protocols of the inter-broker and (KRaft) controller listeners are added; SASL mechanisms
	 * are only shown if the corresponding listener is SASL_*.
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
			// If the listener is not SASL the SASL mechanism comes back as the default (GSSAPI): show the protocol, hide the mechanism
			String protocol = null;
			if (INTER_BROKER_SASL_MECHANISM.equals(k)) {
				protocol = interBrokerSecurityProtocol();
				putIfNotNull(m, INTER_BROKER_SECURITY_PROTOCOL, protocol);
			}
			else if (CONTROLLER_SASL_MECHANISM.equals(k)) {
				protocol = controllerSecurityProtocol();
				putIfNotNull(m, CONTROLLER_SECURITY_PROTOCOL, protocol);
			}
			if (protocol != null && !isSasl(protocol)) {
				continue;
			}
			String v = display(k);
			if (v != null) {
				m.put(k, v);
			}
		}
		return m;
	}

	private static void putIfNotNull(Map<String, Object> m, String key, Object value) {
		if (value != null) {
			m.put(key, value);
		}
	}

}
