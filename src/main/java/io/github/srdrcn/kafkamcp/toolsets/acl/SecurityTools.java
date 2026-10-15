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
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** scram_users_list, delegation_tokens_list, kerberos_self_check, security_summary. */
@Component
public class SecurityTools {

	public List<ToolSpec> tools() {
		return List.of(ToolSpec
			.read("scram_users_list", "SCRAM kullanıcılarını mekanizma ve iterasyon sayısıyla listeler (gizli bilgi dönmez)")
			.listParams()
			.param("filter", P.string("Kullanıcı adı filtresi (regex, kısmi eşleşme)"))
			.handler(this::scramUsers),
				ToolSpec
					.read("delegation_tokens_list",
							"Delegation token'ları listeler: id, sahip, yenileyenler, süreler, süresi dolmuş mu (HMAC dönmez)")
					.listParams()
					.param("filter", P.string("Token id ya da sahip filtresi (regex, kısmi eşleşme)"))
					.handler(this::delegationTokens),
				ToolSpec
					.read("kerberos_self_check",
							"Bu sunucunun Kerberos ayarlarını (krb5.conf, keytab, principal) kontrol eder; login ve Kafka bağlantısını dener")
					.handler(this::kerberosSelfCheck),
				ToolSpec
					.read("security_summary",
							"Güvenlik özeti: listener/SASL/authorizer ayarları, ACL/SCRAM/kota/token sayıları, bu sunucunun bağlantısı")
					.handler(this::securitySummary));
	}

	Object scramUsers(ToolCall call) {
		Map<String, UserScramCredentialsDescription> all = call
			.await(call.admin().describeUserScramCredentials().all());
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

	/** Delegation token kapalı/desteklenmiyor hataları için Türkçe not; başka hata ise null. */
	public static String tokenNote(Throwable e) {
		if (Acls.causedBy(e, DelegationTokenDisabledException.class)) {
			return "Delegation token'lar bu cluster'da kapalı (broker'da delegation.token.secret.key tanımlı değil)";
		}
		if (Acls.causedBy(e, UnsupportedByAuthenticationException.class)) {
			return "Delegation token istekleri bu bağlantı tipinde yapılamaz (PLAINTEXT, tek yönlü SSL ya da token ile kimlik doğrulama)";
		}
		if (Acls.causedBy(e, UnsupportedVersionException.class)) {
			return "Broker delegation token API'sini desteklemiyor";
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
				notes.add("kerberos bloğu tanımlı ama bu cluster properties ile sasl.mechanism=" + client.saslMechanism()
						+ " (" + client.securityProtocol() + ") kullanıyor; Kafka bağlantısı Kerberos kullanmıyor");
			}
		}
		else if (client.gssapi() && client.jaasOption("keyTab") != null) {
			settings = new KerberosCheck.Settings("cluster properties (sasl.jaas.config)", null,
					client.jaasOption("keyTab"), client.jaasOption("principal"),
					client.kerberosServiceName() == null ? "kafka" : client.kerberosServiceName());
		}
		if (settings == null) {
			String mech = client.saslMechanism();
			out.put("note", "Kerberos yapılandırılmamış (config'te kerberos bloğu yok); bu cluster'a security.protocol="
					+ client.securityProtocol() + (mech == null ? "" : ", sasl.mechanism=" + mech)
					+ " ile bağlanılıyor. Kerberos login denenmedi.");
		}
		else {
			long budget = Math.min(10_000, Math.max(1_000, call.waitMillis() - 4_000));
			out.putAll(KerberosCheck.check(settings, budget));
		}
		Object loginStatus = out.get("login") instanceof Map<?, ?> login ? login.get("status") : null;
		if (settings != null && client.gssapi() && !"ok".equals(loginStatus)
				&& Objects.equals(client.jaasOption("keyTab"), settings.keytab())) {
			// Admin.create aynı Krb5 login'i senkron yapar; KDC'ye ulaşılamıyorsa dakikalarca bloklanabilir.
			out.put("kafka", Results.map("status", "skipped", "reason",
					"Kerberos login " + ("skipped".equals(loginStatus) ? "denenemedi" : "başarısız")
							+ "; Kafka client'ı da aynı ayarlarla GSSAPI login olamayacağı için bağlantı denenmedi"));
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
	 * Kısa Kafka bağlantı kontrolü: describeCluster clusterId. Client oluşturma (Kerberos login dahil) ayrı
	 * bir thread'de yapılır ve kalan süreyle sınırlıdır; süre dolarsa {status: timeout} döner.
	 */
	public static Map<String, Object> connectivity(ToolCall call) {
		long timeout = Math.min(5_000, call.waitMillis());
		if (timeout < 500) {
			return Results.map("status", "skipped", "reason", "süre kalmadı");
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
			return Results.map("status", "timeout", "durationMs", elapsed(start), "error", "Kafka client'ı "
					+ timeout + " ms içinde oluşturulamadı ya da yanıt vermedi (Kerberos login ya da ağ bekleniyor)");
		}
		catch (ExecutionException e) {
			Throwable cause = e.getCause() == null ? e : e.getCause();
			return Results.map("status", "error", "durationMs", elapsed(start), "error",
					cause instanceof ToolException ? cause.getMessage() : KafkaErrors.explain(cause));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			task.cancel(true);
			return Results.map("status", "error", "error", "Kafka bağlantı kontrolü iptal edildi");
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
			out.put("scram", Results.map("error", e.getMessage()));
		}
		try {
			out.put("quotas", Results.map("entities", call.await(quotasFuture).size()));
		}
		catch (ToolException e) {
			out.put("quotas", Results.map("error", e.getMessage()));
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
			notes.add("super.users / allow.everyone.if.no.acl.found broker tarafından sensitive olarak gizleniyor "
					+ "(Kafka 4.x); değerleri broker config'inden (server.properties/Helm) kontrol edin");
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
