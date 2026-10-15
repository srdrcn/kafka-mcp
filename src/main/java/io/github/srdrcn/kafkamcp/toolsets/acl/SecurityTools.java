package io.github.srdrcn.kafkamcp.toolsets.acl;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.DelegationTokenDisabledException;
import org.apache.kafka.common.errors.UnsupportedByAuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.config.KerberosConfig;
import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** scram_users_list, delegation_tokens_list, kerberos_self_check, security_summary. */
@Component
public class SecurityTools {

	/** describeUserScramCredentials requirement: KIP-554 (2.7) on ZooKeeper, KIP-900 (3.5) on KRaft. */
	static final String SCRAM_REQUIREMENT = "listing SCRAM users requires Kafka 2.7+ on ZooKeeper, "
			+ "3.5+ on KRaft (metadata.version 3.5-IV2+)";

	/** describeClientQuotas (KIP-546) requirement. */
	static final String QUOTA_REQUIREMENT = "reading quotas with the Admin API requires Kafka 2.6+";

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("scram_users_list", "List SCRAM users with mechanism and iteration count (no secrets returned)")
			.listParams()
			.param("filter", P.string("User name filter (regex, partial match)"))
			.handler(this::scramUsers),
				ToolSpec
					.read("delegation_tokens_list",
							"List delegation tokens: id, owner, renewers, timestamps, expired or not (no HMAC returned)")
					.listParams()
					.param("filter", P.string("Token id or owner filter (regex, partial match)"))
					.handler(this::delegationTokens),
				ToolSpec
					.read("kerberos_self_check",
							"Check this server's Kerberos setup (krb5.conf, keytab, principal); tries a login and a Kafka connection")
					.handler(this::kerberosSelfCheck),
				ToolSpec
					.read("security_summary",
							"Security summary: metadata mode, listener/SASL/authorizer settings, ACL/SCRAM/quota/token counts, this server's connection")
					.handler(this::securitySummary));
	}

	Object scramUsers(ToolCall call) {
		Map<String, UserScramCredentialsDescription> all;
		try {
			all = call.await(call.admin().describeUserScramCredentials().all());
		}
		catch (ToolException e) {
			String note = Acls.unsupportedNote(e, SCRAM_REQUIREMENT);
			if (note != null) {
				return Acls.unsupported(note);
			}
			throw e;
		}
		List<Map<String, Object>> rows = new ArrayList<>();
		for (UserScramCredentialsDescription d : new TreeMap<>(all).values()) {
			if (!call.matches(d.name())) {
				continue;
			}
			List<Map<String, Object>> mechanisms = d.credentialInfos()
				.stream()
				.sorted(Comparator.comparing((ScramCredentialInfo i) -> i.mechanism().mechanismName()))
				.map(i -> Results.map("mechanism", i.mechanism().mechanismName(), "iterations", i.iterations()))
				.toList();
			rows.add(Results.map("user", d.name(), "mechanisms", mechanisms));
		}
		return Results.page("users", rows, call.limit());
	}

	Object delegationTokens(ToolCall call) {
		List<DelegationToken> tokens;
		try {
			tokens = call.await(call.admin().describeDelegationToken().delegationTokens());
		}
		catch (ToolException e) {
			String note = tokenNote(e);
			if (note != null) {
				return Results.map("enabled", false, "note", note);
			}
			throw e;
		}
		long now = System.currentTimeMillis();
		List<Map<String, Object>> rows = new ArrayList<>();
		int expired = 0;
		List<TokenInformation> infos = tokens.stream()
			.map(DelegationToken::tokenInfo)
			.sorted(Comparator.comparingLong(TokenInformation::expiryTimestamp))
			.toList();
		for (TokenInformation t : infos) {
			if (!call.matches(t.tokenId()) && !call.matches(t.ownerAsString())) {
				continue;
			}
			boolean isExpired = t.expiryTimestamp() < now;
			expired += isExpired ? 1 : 0;
			rows.add(Results.map("tokenId", t.tokenId(), "owner", t.ownerAsString(), "tokenRequester",
					t.tokenRequesterAsString(), "renewers", List.copyOf(t.renewersAsString()), "issueTimestamp",
					iso(t.issueTimestamp()), "expiryTimestamp", iso(t.expiryTimestamp()), "maxTimestamp",
					iso(t.maxTimestamp()), "expired", isExpired));
		}
		Map<String, Object> out = Results.map("enabled", true, "expiredCount", expired);
		out.putAll(Results.page("tokens", rows, call.limit()));
		return out;
	}

	/** Note for delegation-token-disabled/unsupported errors; null for any other error. */
	public static String tokenNote(Throwable e) {
		if (Acls.causedBy(e, DelegationTokenDisabledException.class)) {
			return "Delegation tokens are disabled on this cluster (delegation.token.secret.key is not set on the broker; "
					+ "delegation.token.master.key on ZooKeeper brokers older than Kafka 3.0)";
		}
		if (Acls.causedBy(e, UnsupportedByAuthenticationException.class)) {
			return "Delegation token requests are not allowed on this connection type (PLAINTEXT, one-way SSL or token authentication)";
		}
		if (Acls.causedBy(e, UnsupportedVersionException.class)) {
			return "Broker does not support the delegation token API (version): requires Kafka 1.1+ on ZooKeeper, 3.6+ on KRaft";
		}
		return null;
	}

	Object kerberosSelfCheck(ToolCall call) {
		ClusterConfig cluster = call.clusterConfig();
		KerberosConfig kerberos = call.clients().config().getKerberos();
		ClientSecurity client = ClientSecurity.of(call.clients(), cluster);
		Map<String, Object> out = Results.map("cluster", cluster.getName(), "kerberosConfigured", kerberos != null,
				"client", client.render());
		KerberosCheck.Settings settings = null;
		List<String> notes = new ArrayList<>();
		if (kerberos != null) {
			settings = new KerberosCheck.Settings("kerberos", kerberos.getKrb5Conf(), kerberos.getKeytab(),
					kerberos.getPrincipal(), kerberos.getServiceName());
			if (!client.gssapi()) {
				notes.add("A kerberos block is configured but this cluster uses sasl.mechanism=" + client.saslMechanism()
						+ " (" + client.securityProtocol() + ") via properties; the Kafka connection does not use Kerberos");
			}
		}
		else if (client.gssapi() && client.jaasOption("keyTab") != null) {
			settings = new KerberosCheck.Settings("cluster properties (sasl.jaas.config)", null,
					client.jaasOption("keyTab"), client.jaasOption("principal"),
					client.kerberosServiceName() == null ? "kafka" : client.kerberosServiceName());
		}
		if (settings == null) {
			String mech = client.saslMechanism();
			out.put("note", "Kerberos is not configured (no kerberos block in the config); this cluster is reached with security.protocol="
					+ client.securityProtocol() + (mech == null ? "" : ", sasl.mechanism=" + mech)
					+ ". Kerberos login was not attempted.");
		}
		else {
			long budget = Math.min(10_000, Math.max(1_000, call.waitMillis() - 4_000));
			out.putAll(KerberosCheck.check(settings, budget));
		}
		Object loginStatus = out.get("login") instanceof Map<?, ?> login ? login.get("status") : null;
		if (settings != null && client.gssapi() && !"ok".equals(loginStatus)
				&& Objects.equals(client.jaasOption("keyTab"), settings.keytab())) {
			// Admin.create performs the same Krb5 login synchronously; it can block for minutes if the KDC is unreachable.
			out.put("kafka", Results.map("status", "skipped", "reason",
					"Kerberos login " + ("skipped".equals(loginStatus) ? "could not be attempted" : "failed")
							+ "; the Kafka client cannot log in via GSSAPI with the same settings either, so the connection was not attempted"));
		}
		else {
			out.put("kafka", connectivity(call));
		}
		if (out.get("notes") instanceof List<?> checkNotes) {
			checkNotes.forEach(n -> notes.add(String.valueOf(n)));
		}
		if (!notes.isEmpty()) {
			out.put("notes", notes);
		}
		return out;
	}

	/**
	 * Quick Kafka connectivity check: describeCluster clusterId. Client creation (Kerberos login included) runs
	 * in a separate thread and is bounded by the remaining time; returns {status: timeout} when time runs out.
	 */
	public static Map<String, Object> connectivity(ToolCall call) {
		long timeout = Math.min(5_000, call.waitMillis());
		if (timeout < 500) {
			return Results.map("status", "skipped", "reason", "no time left");
		}
		long start = System.nanoTime();
		FutureTask<String> task = new FutureTask<>(() -> call
			.await(call.admin().describeCluster(new DescribeClusterOptions().timeoutMs((int) timeout)).clusterId()));
		Thread.ofPlatform().daemon().name("kafka-mcp-connectivity").start(task);
		try {
			String id = task.get(Math.min(call.waitMillis(), timeout + 500), TimeUnit.MILLISECONDS);
			return Results.map("status", "ok", "clusterId", id, "durationMs", elapsed(start));
		}
		catch (TimeoutException e) {
			task.cancel(true);
			return Results.map("status", "timeout", "durationMs", elapsed(start), "error",
					"Kafka client was not created or did not respond within " + timeout
							+ " ms (waiting for Kerberos login or network)");
		}
		catch (ExecutionException e) {
			Throwable cause = e.getCause() == null ? e : e.getCause();
			return Results.map("status", "error", "durationMs", elapsed(start), "error",
					cause instanceof ToolException ? cause.getMessage() : KafkaErrors.explain(cause));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			task.cancel(true);
			return Results.map("status", "error", "error", "Kafka connectivity check was cancelled");
		}
	}

	private static long elapsed(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

	Object securitySummary(ToolCall call) {
		Admin admin = call.admin();
		KafkaFuture<Collection<AclBinding>> aclsFuture = admin.describeAcls(AclBindingFilter.ANY).values();
		KafkaFuture<Map<String, UserScramCredentialsDescription>> scramFuture = admin.describeUserScramCredentials()
			.all();
		KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> quotasFuture = admin
			.describeClientQuotas(ClientQuotaFilter.all())
			.entities();
		KafkaFuture<List<DelegationToken>> tokensFuture = admin.describeDelegationToken().delegationTokens();
		BrokerSecurity broker = BrokerSecurity.tryLoad(call);

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("cluster", call.cluster());
		String mode = broker.mode();
		if (mode != null) {
			out.put("mode", mode);
		}
		out.put("broker", broker.render());
		try {
			List<AclBinding> acls = Acls.await(call, aclsFuture);
			if (acls == null) {
				out.put("acls", Acls.noAuthorizer());
			}
			else {
				Map<String, Integer> byType = new TreeMap<>();
				Map<String, Integer> byPermission = new TreeMap<>();
				for (AclBinding b : acls) {
					byType.merge(b.pattern().resourceType().name(), 1, Integer::sum);
					byPermission.merge(b.entry().permissionType().name(), 1, Integer::sum);
				}
				out.put("acls", Results.map("total", acls.size(), "byResourceType", byType, "byPermission", byPermission));
			}
		}
		catch (ToolException e) {
			out.put("acls", Results.map("error", e.getMessage()));
		}
		try {
			out.put("scram", Results.map("users", call.await(scramFuture).size()));
		}
		catch (ToolException e) {
			String note = Acls.unsupportedNote(e, SCRAM_REQUIREMENT);
			out.put("scram", note != null ? Acls.unsupported(note) : Results.map("error", e.getMessage()));
		}
		try {
			out.put("quotas", Results.map("entities", call.await(quotasFuture).size()));
		}
		catch (ToolException e) {
			String note = Acls.unsupportedNote(e, QUOTA_REQUIREMENT);
			out.put("quotas", note != null ? Acls.unsupported(note) : Results.map("error", e.getMessage()));
		}
		try {
			List<DelegationToken> tokens = call.await(tokensFuture);
			long now = System.currentTimeMillis();
			long expired = tokens.stream().filter(t -> t.tokenInfo().expiryTimestamp() < now).count();
			out.put("delegationTokens", Results.map("count", tokens.size(), "expired", expired));
		}
		catch (ToolException e) {
			String note = tokenNote(e);
			out.put("delegationTokens", note != null ? Results.map("enabled", false, "note", note)
					: Results.map("error", e.getMessage()));
		}
		out.put("client", ClientSecurity.of(call.clients(), call.clusterConfig()).render());
		List<String> notes = new ArrayList<>();
		if (broker.error() == null && (broker.hidden(BrokerSecurity.SUPER_USERS)
				|| broker.hidden(BrokerSecurity.ALLOW_EVERYONE))) {
			notes.add("super.users / allow.everyone.if.no.acl.found are hidden by the broker as sensitive "
					+ "(authorizer settings; same on ZooKeeper and KRaft); check their values in the broker config "
					+ "(server.properties/Helm)");
		}
		String customAuthorizer = broker.customAuthorizerNote();
		if (customAuthorizer != null) {
			notes.add(customAuthorizer);
		}
		if (ClusterMeta.ZOOKEEPER.equals(mode)
				&& "false".equalsIgnoreCase(broker.value(BrokerSecurity.ZOOKEEPER_SET_ACL))) {
			notes.add("ZooKeeper mode: ACLs and SCRAM credentials are stored in ZooKeeper and "
					+ "zookeeper.set.acl=false; anyone with network access to ZooKeeper can read and change them "
					+ "(restrict ZooKeeper access, or use zookeeper.set.acl=true with ZooKeeper SASL/mTLS)");
		}
		String controllerProtocol = broker.controllerSecurityProtocol();
		if ("PLAINTEXT".equals(controllerProtocol)) {
			notes.add("KRaft mode: the controller listener (" + broker.controllerListener() + ") is PLAINTEXT; ACL, SCRAM "
					+ "and delegation token metadata travel between brokers and controllers unencrypted and unauthenticated "
					+ "(controllers see the peer as ANONYMOUS). Restrict network access to the controller ports "
					+ "or use SSL/SASL_SSL");
		}
		if (!notes.isEmpty()) {
			out.put("notes", notes);
		}
		return out;
	}

	private static String iso(long epochMillis) {
		return epochMillis <= 0 ? null : Instant.ofEpochMilli(epochMillis).toString();
	}

}
