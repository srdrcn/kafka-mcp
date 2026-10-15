package io.github.srdrcn.kafkamcp.toolsets.acl;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosKey;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KeyTab;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Checks this server's Kerberos setup: krb5.conf (path, default_realm, realms/kdcs, rdns), keytab (path,
 * readability, size; never the content), principal/realm match and a real login attempt with Krb5LoginModule
 * (in a separate thread, time-bounded). Errors are explained in plain language.
 */
public final class KerberosCheck {

	public static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";

	private static final Set<String> LIBDEFAULTS = Set.of("default_realm", "rdns", "dns_lookup_kdc",
			"dns_lookup_realm", "dns_fallback", "dns_canonicalize_hostname", "kdc_timeout", "max_retries", "udp_preference_limit",
			"ticket_lifetime", "renew_lifetime", "forwardable", "default_tkt_enctypes", "default_tgs_enctypes",
			"permitted_enctypes", "allow_weak_crypto");

	private static final Set<String> REALM_KEYS = Set.of("kdc", "admin_server", "master_kdc", "primary_kdc",
			"kpasswd_server");

	private static final Map<Integer, String> ENCTYPES = Map.of(16, "des3-cbc-sha1", 17, "aes128-cts-hmac-sha1-96",
			18, "aes256-cts-hmac-sha1-96", 19, "aes128-cts-hmac-sha256-128", 20, "aes256-cts-hmac-sha384-192", 23,
			"rc4-hmac", 25, "camellia128-cts-cmac", 26, "camellia256-cts-cmac");

	private KerberosCheck() {
	}

	/** Settings to check ({@code source}: kerberos block or cluster properties). */
	public record Settings(String source, String configuredKrb5Conf, String keytab, String principal,
			String serviceName) {
	}

	/** (Non-secret) information read from krb5.conf. */
	public record Krb5Conf(String path, boolean exists, boolean readable, String defaultRealm,
			Map<String, String> libdefaults, Map<String, Map<String, List<String>>> realms, List<String> includes,
			String error) {

		/** kdc addresses configured for the realm (empty if none). */
		public List<String> kdcs(String realm) {
			Map<String, List<String>> r = realms.get(realm);
			return r == null ? List.of() : r.getOrDefault("kdc", List.of());
		}

		/**
		 * JDK rule: dns_lookup_kdc, otherwise dns_fallback; true if neither is set (KDCs are looked up via DNS SRV).
		 */
		public boolean dnsLookupKdc() {
			Boolean v = bool(libdefaults.get("dns_lookup_kdc"));
			if (v == null) {
				v = bool(libdefaults.get("dns_fallback"));
			}
			return v == null || v;
		}

		private static Boolean bool(String value) {
			if (value == null) {
				return null;
			}
			return switch (value.trim().toLowerCase(Locale.ROOT)) {
				case "true", "yes" -> true;
				case "false", "no" -> false;
				default -> null;
			};
		}

		public Map<String, Object> render() {
			return Results.map("path", path, "exists", exists, "readable", exists ? readable : null, "defaultRealm",
					defaultRealm, "libdefaults", libdefaults.isEmpty() ? null : libdefaults, "realms",
					realms.isEmpty() ? null : realms, "includes", includes.isEmpty() ? null : includes, "error", error);
		}

	}

	/** Effective krb5.conf path: -Djava.security.krb5.conf, otherwise the one from the config, otherwise the JDK default. */
	public static String effectiveKrb5Conf(String configured) {
		String p = System.getProperty(KRB5_CONF_PROPERTY);
		if (p != null && !p.isBlank()) {
			return p;
		}
		if (configured != null && !configured.isBlank()) {
			return configured;
		}
		Path jdk = Path.of(System.getProperty("java.home"), "conf", "security", "krb5.conf");
		return Files.exists(jdk) ? jdk.toString() : "/etc/krb5.conf";
	}

	/** Reads krb5.conf: selected [libdefaults] keys, [realms] kdc/admin_server, include lines. */
	public static Krb5Conf parseKrb5Conf(String path) {
		Path p = Path.of(path);
		if (!Files.exists(p)) {
			return new Krb5Conf(path, false, false, null, Map.of(), Map.of(), List.of(), null);
		}
		if (!Files.isReadable(p) || Files.isDirectory(p)) {
			return new Krb5Conf(path, true, false, null, Map.of(), Map.of(), List.of(), "file is not readable");
		}
		List<String> lines;
		try {
			lines = Files.readAllLines(p, StandardCharsets.UTF_8);
		}
		catch (IOException e) {
			return new Krb5Conf(path, true, false, null, Map.of(), Map.of(), List.of(), e.getMessage());
		}
		Map<String, String> libdefaults = new LinkedHashMap<>();
		Map<String, Map<String, List<String>>> realms = new LinkedHashMap<>();
		List<String> includes = new ArrayList<>();
		String section = "";
		String realm = null;
		int depth = 0;
		for (String raw : lines) {
			String t = raw.trim();
			if (t.isEmpty() || t.startsWith("#") || t.startsWith(";")) {
				continue;
			}
			if (depth == 0 && t.startsWith("[") && t.endsWith("]")) {
				section = t.substring(1, t.length() - 1).trim().toLowerCase(Locale.ROOT);
				realm = null;
				continue;
			}
			if (depth == 0 && (t.startsWith("include ") || t.startsWith("includedir "))) {
				includes.add(t);
				continue;
			}
			if (t.startsWith("}")) {
				depth = Math.max(0, depth - 1);
				if (depth == 0) {
					realm = null;
				}
				continue;
			}
			int eq = t.indexOf('=');
			if (eq < 0) {
				continue;
			}
			String key = t.substring(0, eq).trim();
			String value = t.substring(eq + 1).trim();
			if (value.startsWith("{")) {
				if (depth == 0 && "realms".equals(section)) {
					realm = key;
					realms.putIfAbsent(realm, new LinkedHashMap<>());
				}
				depth++;
				continue;
			}
			String k = key.toLowerCase(Locale.ROOT);
			if (depth == 0 && "libdefaults".equals(section) && LIBDEFAULTS.contains(k)) {
				libdefaults.put(k, value);
			}
			else if (depth == 1 && realm != null && REALM_KEYS.contains(k)) {
				realms.get(realm).computeIfAbsent(k, x -> new ArrayList<>()).add(value);
			}
		}
		return new Krb5Conf(path, true, true, libdefaults.get("default_realm"), libdefaults, realms, includes, null);
	}

	/**
	 * Full check: krb5.conf, keytab, principal/realm and (if krb5.conf exists) a real login.
	 * @param loginTimeoutMillis maximum wait for the login attempt
	 */
	public static Map<String, Object> check(Settings s, long loginTimeoutMillis) {
		List<String> problems = new ArrayList<>();
		List<String> notes = new ArrayList<>();
		String krb5Path = effectiveKrb5Conf(s.configuredKrb5Conf());
		Krb5Conf krb5 = parseKrb5Conf(krb5Path);
		if (s.configuredKrb5Conf() != null && !s.configuredKrb5Conf().isBlank()
				&& !s.configuredKrb5Conf().equals(krb5Path)) {
			notes.add("The JVM uses " + krb5Path + " instead of krb5Conf from the config (" + s.configuredKrb5Conf()
					+ ") (-D" + KRB5_CONF_PROPERTY + " was already set)");
		}
		if (!krb5.exists()) {
			problems.add("krb5.conf not found: " + krb5Path);
		}
		else if (!krb5.readable()) {
			problems.add("krb5.conf is not readable: " + krb5Path + (krb5.error() == null ? "" : " (" + krb5.error() + ")"));
		}
		else if (krb5.defaultRealm() == null) {
			problems.add("krb5.conf has no [libdefaults] default_realm");
		}
		if (!krb5.includes().isEmpty()) {
			notes.add("krb5.conf contains include/includedir; included files were not parsed");
		}

		Map<String, Object> keytab = keytab(s.keytab(), s.principal(), problems);

		String principal = s.principal();
		String realm = principal != null && principal.contains("@") ? principal.substring(principal.indexOf('@') + 1)
				: null;
		String effectiveRealm = realm != null ? realm : krb5.defaultRealm();
		Boolean realmMatches = realm == null || krb5.defaultRealm() == null ? null : realm.equals(krb5.defaultRealm());
		if (principal == null || principal.isBlank()) {
			problems.add("principal is not configured");
		}
		else if (Boolean.FALSE.equals(realmMatches)) {
			problems.add("principal realm (" + realm + ") differs from default_realm (" + krb5.defaultRealm()
					+ "); unless this is cross-realm, the realm is wrong (case included)");
		}
		if (krb5.readable() && effectiveRealm != null && krb5.kdcs(effectiveRealm).isEmpty()) {
			if (krb5.dnsLookupKdc()) {
				notes.add("krb5.conf has no kdc entry for " + effectiveRealm + "; KDCs will be looked up via DNS SRV records "
						+ "(_kerberos._udp/_tcp." + effectiveRealm + ")");
			}
			else {
				problems.add("krb5.conf [realms] has no kdc entry for " + effectiveRealm
						+ " (dns_lookup_kdc/dns_fallback = false)");
			}
		}
		if ("true".equalsIgnoreCase(krb5.libdefaults().get("rdns"))) {
			notes.add("rdns=true: broker addresses are resolved with reverse DNS; try rdns = false on a service principal mismatch");
		}

		Map<String, Object> login;
		if (!krb5.exists()) {
			login = Results.map("status", "skipped", "reason", "krb5.conf not found; login not attempted");
		}
		else if (s.keytab() == null || s.keytab().isBlank() || principal == null || principal.isBlank()) {
			login = Results.map("status", "skipped", "reason", "keytab/principal not configured; login not attempted");
		}
		else {
			login = login(s.keytab(), principal, loginTimeoutMillis);
			if (!"ok".equals(login.get("status"))) {
				problems.add("Kerberos login failed: " + login.get("error"));
			}
		}

		return Results.map("source", s.source(), "krb5Conf", krb5.render(), "keytab", keytab, "principal",
				Results.map("name", principal, "realm", effectiveRealm, "defaultRealm", krb5.defaultRealm(),
						"realmMatchesDefault", realmMatches),
				"serviceName", s.serviceName(), "login", login, "ok", problems.isEmpty(), "problems", problems,
				"notes", notes.isEmpty() ? null : notes);
	}

	/** Keytab file status (path, exists, readable, size, key types of the principal); never the content. */
	public static Map<String, Object> keytab(String keytab, String principal, List<String> problems) {
		if (keytab == null || keytab.isBlank()) {
			problems.add("keytab is not configured");
			return Results.map("path", null, "exists", false);
		}
		Path p = Path.of(keytab);
		boolean exists = Files.exists(p);
		boolean readable = exists && Files.isReadable(p) && !Files.isDirectory(p);
		Long size = null;
		if (readable) {
			try {
				size = Files.size(p);
			}
			catch (IOException e) {
				readable = false;
			}
		}
		if (!exists) {
			problems.add("keytab file does not exist: " + keytab + " (check the Secret mount and the path)");
		}
		else if (!readable) {
			problems.add("keytab is not readable: " + keytab + " (check file permissions)");
		}
		else if (size != null && size == 0) {
			problems.add("keytab is empty: " + keytab);
		}
		Boolean contains = null;
		List<String> keyTypes = null;
		if (readable && size != null && size > 0 && principal != null && principal.contains("@")) {
			try {
				KerberosKey[] keys = KeyTab.getInstance(p.toFile()).getKeys(new KerberosPrincipal(principal));
				contains = keys.length > 0;
				Set<String> types = new LinkedHashSet<>();
				for (KerberosKey k : keys) {
					types.add(ENCTYPES.getOrDefault(k.getKeyType(), String.valueOf(k.getKeyType())));
					k.destroy();
				}
				keyTypes = types.isEmpty() ? null : List.copyOf(types);
				if (!contains) {
					problems.add("keytab has no key for " + principal + " (wrong principal name/realm or wrong keytab)");
				}
			}
			catch (Exception e) {
				// keytab could not be parsed; the login attempt will show the actual error
			}
		}
		return Results.map("path", keytab, "exists", exists, "readable", exists ? readable : null, "sizeBytes", size,
				"principalInKeytab", contains, "keyTypes", keyTypes);
	}

	/**
	 * Tries a login with Krb5LoginModule (useKeyTab, storeKey, doNotPrompt) in a separate thread; logs out on
	 * success. Result: {status: ok|error|timeout, durationMs, error (explained), detail}.
	 */
	public static Map<String, Object> login(String keytab, String principal, long timeoutMillis) {
		long start = System.nanoTime();
		FutureTask<Void> task = new FutureTask<>(() -> {
			LoginContext lc = new LoginContext("kafka-mcp-kerberos-check", new Subject(), null,
					jaasConfiguration(keytab, principal));
			lc.login();
			try {
				lc.logout();
			}
			catch (LoginException e) {
				// login succeeded; a logout error does not matter
			}
			return null;
		});
		Thread.ofPlatform().daemon().name("kerberos-check").start(task);
		try {
			task.get(Math.max(1, timeoutMillis), TimeUnit.MILLISECONDS);
			return Results.map("status", "ok", "durationMs", elapsed(start), "message",
					"Got a TGT with the keytab (KDC reachable); logged out");
		}
		catch (TimeoutException e) {
			task.cancel(true);
			return Results.map("status", "timeout", "durationMs", elapsed(start), "error",
					KafkaErrors.kerberos("cannot locate kdc") + ": login did not finish within " + timeoutMillis + " ms");
		}
		catch (ExecutionException e) {
			Throwable cause = e.getCause() == null ? e : e.getCause();
			return Results.map("status", "error", "durationMs", elapsed(start), "error", explain(cause), "detail",
					messages(cause));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			task.cancel(true);
			return Results.map("status", "error", "error", "Kerberos check was cancelled");
		}
	}

	/** Explains a Kerberos login error (network errors → KDC unreachable). */
	public static String explain(Throwable error) {
		String chain = messages(error);
		if (networkError(error)) {
			return KafkaErrors.kerberos("cannot locate kdc");
		}
		String k = KafkaErrors.kerberos(chain);
		return k != null ? k : "Kerberos login failed";
	}

	static Configuration jaasConfiguration(String keytab, String principal) {
		Map<String, String> options = new LinkedHashMap<>();
		options.put("useKeyTab", "true");
		options.put("keyTab", keytab);
		options.put("principal", principal);
		options.put("storeKey", "true");
		options.put("doNotPrompt", "true");
		options.put("useTicketCache", "false");
		options.put("refreshKrb5Config", "true");
		options.put("isInitiator", "true");
		AppConfigurationEntry entry = new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
				AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
		return new Configuration() {

			@Override
			public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
				return new AppConfigurationEntry[] { entry };
			}

		};
	}

	private static boolean networkError(Throwable error) {
		Set<Throwable> seen = new HashSet<>();
		for (Throwable t = error; t != null && seen.add(t); t = t.getCause()) {
			if (t instanceof SocketTimeoutException || t instanceof ConnectException
					|| t instanceof PortUnreachableException || t instanceof NoRouteToHostException
					|| t instanceof UnknownHostException) {
				return true;
			}
		}
		return false;
	}

	private static String messages(Throwable error) {
		List<String> parts = new ArrayList<>();
		Set<Throwable> seen = new HashSet<>();
		for (Throwable t = error; t != null && seen.add(t) && parts.size() < 4; t = t.getCause()) {
			String m = t.getMessage() == null ? "" : t.getMessage().strip();
			String s = t.getClass().getSimpleName() + (m.isEmpty() ? "" : ": " + m);
			if (parts.isEmpty() || m.isEmpty() || !parts.get(parts.size() - 1).contains(m)) {
				parts.add(s);
			}
		}
		return String.join(" <- ", parts);
	}

	private static long elapsed(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

}
