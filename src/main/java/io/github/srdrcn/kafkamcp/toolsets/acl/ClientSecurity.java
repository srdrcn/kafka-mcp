package io.github.srdrcn.kafkamcp.toolsets.acl;

import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import io.github.srdrcn.kafkamcp.config.ClusterConfig;
import io.github.srdrcn.kafkamcp.kafka.KafkaClients;
import io.github.srdrcn.kafkamcp.mcp.Results;

/** Bu sunucunun bir cluster'a bağlanırken kullandığı efektif güvenlik ayarları (parolalar maskeli). */
public final class ClientSecurity {

	/** JAAS değeri: "çift tırnaklı", 'tek tırnaklı' (Kafka JaasConfig ikisini de kabul eder) ya da tırnaksız. */
	private static final String JAAS_VALUE = "\"((?:[^\"\\\\]|\\\\.)*)\"|'((?:[^'\\\\]|\\\\.)*)'|[^\\s;]+";

	private static final Pattern SECRET_OPTION = Pattern
		.compile("(?i)(\\b[\\w.]*(?:password|secret|token)[\\w.]*\\s*=\\s*)(?:" + JAAS_VALUE + ")");

	private static final String MASK = "\"****\"";

	private final String securityProtocol;

	private final String saslMechanism;

	private final String kerberosServiceName;

	private final String jaasConfig;

	private ClientSecurity(Map<String, Object> props) {
		this.securityProtocol = string(props.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), "PLAINTEXT")
			.toUpperCase(Locale.ROOT);
		String mech = string(props.get(SaslConfigs.SASL_MECHANISM), null);
		this.saslMechanism = mech != null ? mech : securityProtocol.startsWith("SASL") ? "GSSAPI" : null;
		this.kerberosServiceName = string(props.get(SaslConfigs.SASL_KERBEROS_SERVICE_NAME), null);
		this.jaasConfig = string(props.get(SaslConfigs.SASL_JAAS_CONFIG), null);
	}

	/** {@link KafkaClients#clientProperties(ClusterConfig)}'ten efektif ayarlar. */
	public static ClientSecurity of(KafkaClients clients, ClusterConfig cluster) {
		return new ClientSecurity(clients.clientProperties(cluster));
	}

	public static ClientSecurity of(Map<String, Object> clientProperties) {
		return new ClientSecurity(clientProperties);
	}

	public String securityProtocol() {
		return securityProtocol;
	}

	/** SASL mekanizması; SASL kullanılmıyorsa null. */
	public String saslMechanism() {
		return securityProtocol.startsWith("SASL") ? saslMechanism : null;
	}

	public String kerberosServiceName() {
		return kerberosServiceName;
	}

	public boolean gssapi() {
		return "GSSAPI".equalsIgnoreCase(saslMechanism());
	}

	/** JAAS seçeneği (ör. keyTab, principal, username); yoksa null. */
	public String jaasOption(String option) {
		return jaasOption(jaasConfig, option);
	}

	public static String jaasOption(String jaas, String option) {
		if (jaas == null) {
			return null;
		}
		Matcher m = Pattern.compile("(?:^|\\s)" + Pattern.quote(option) + "\\s*=\\s*(" + JAAS_VALUE + ")").matcher(jaas);
		if (!m.find()) {
			return null;
		}
		String v;
		if (m.group(2) != null) {
			v = m.group(2).replace("\\\"", "\"").replace("\\\\", "\\");
		}
		else if (m.group(3) != null) {
			v = m.group(3).replace("\\'", "'").replace("\\\\", "\\");
		}
		else {
			v = m.group(1);
		}
		return v.isBlank() ? null : v;
	}

	/** JAAS config'teki password/secret/token değerlerini maskeler. */
	public static String maskJaas(String jaas) {
		return jaas == null ? null : SECRET_OPTION.matcher(jaas).replaceAll("$1" + Matcher.quoteReplacement(MASK));
	}

	/** {securityProtocol, saslMechanism, saslKerberosServiceName, saslJaasConfig(maskeli)} */
	public Map<String, Object> render() {
		return Results.map("securityProtocol", securityProtocol, "saslMechanism", saslMechanism(),
				"saslKerberosServiceName", gssapi() ? kerberosServiceName : null, "saslJaasConfig", maskJaas(jaasConfig));
	}

	private static String string(Object v, String defaultValue) {
		if (v == null || String.valueOf(v).isBlank()) {
			return defaultValue;
		}
		return String.valueOf(v).trim();
	}

}
