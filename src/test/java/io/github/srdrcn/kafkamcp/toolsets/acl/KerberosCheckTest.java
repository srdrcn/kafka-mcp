package io.github.srdrcn.kafkamcp.toolsets.acl;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * KerberosCheck birim testleri: erişilemeyen KDC (127.0.0.1:1), olmayan keytab ve olmayan krb5.conf.
 * java.security.krb5.conf JVM genelinde olduğu için her testte ayarlanıp geri alınır.
 */
class KerberosCheckTest {

	private static final String REALM = "EXAMPLE.TEST";

	private static final String PRINCIPAL = "kafka-mcp@" + REALM;

	@TempDir
	Path dir;

	private String previous;

	@BeforeEach
	void saveProperty() {
		previous = System.getProperty(KerberosCheck.KRB5_CONF_PROPERTY);
	}

	@AfterEach
	void restoreProperty() {
		if (previous == null) {
			System.clearProperty(KerberosCheck.KRB5_CONF_PROPERTY);
		}
		else {
			System.setProperty(KerberosCheck.KRB5_CONF_PROPERTY, previous);
		}
	}

	private Path krb5Conf() throws IOException {
		return KerberosTestFiles.unreachableKdcKrb5Conf(dir, REALM);
	}

	@Test
	void parsesKrb5Conf() throws IOException {
		KerberosCheck.Krb5Conf c = KerberosCheck.parseKrb5Conf(krb5Conf().toString());
		assertThat(c.exists()).isTrue();
		assertThat(c.readable()).isTrue();
		assertThat(c.defaultRealm()).isEqualTo(REALM);
		assertThat(c.libdefaults()).containsEntry("rdns", "false").containsEntry("kdc_timeout", "300");
		assertThat(c.kdcs(REALM)).containsExactly("127.0.0.1:1");
		assertThat(c.realms().get(REALM)).containsKey("admin_server").doesNotContainKey("x");
	}

	@Test
	void missingKeytabIsExplainedInTurkish() throws IOException {
		System.setProperty(KerberosCheck.KRB5_CONF_PROPERTY, krb5Conf().toString());
		String keytab = dir.resolve("missing.keytab").toString();
		Map<String, Object> r = KerberosCheck.check(new KerberosCheck.Settings("kerberos", null, keytab, PRINCIPAL,
				"kafka"), 8_000);
		assertThat(r.get("ok")).isEqualTo(false);
		@SuppressWarnings("unchecked")
		Map<String, Object> kt = (Map<String, Object>) r.get("keytab");
		assertThat(kt.get("exists")).isEqualTo(false);
		@SuppressWarnings("unchecked")
		Map<String, Object> login = (Map<String, Object>) r.get("login");
		assertThat(login.get("status")).isEqualTo("error");
		assertThat((String) login.get("error")).contains("keytab bulunamadı");
		assertThat(problems(r)).anyMatch(p -> p.contains("keytab dosyası yok"));
		@SuppressWarnings("unchecked")
		Map<String, Object> principal = (Map<String, Object>) r.get("principal");
		assertThat(principal.get("realmMatchesDefault")).isEqualTo(true);
	}

	@Test
	void unreachableKdcIsExplainedInTurkish() throws IOException {
		System.setProperty(KerberosCheck.KRB5_CONF_PROPERTY, krb5Conf().toString());
		Path keytab = dir.resolve("svc.keytab");
		KerberosTestFiles.writeKeytab(keytab, "kafka-mcp", REALM);
		Map<String, Object> r = KerberosCheck.check(new KerberosCheck.Settings("kerberos", null, keytab.toString(),
				PRINCIPAL, "kafka"), 10_000);
		@SuppressWarnings("unchecked")
		Map<String, Object> kt = (Map<String, Object>) r.get("keytab");
		assertThat(kt.get("exists")).isEqualTo(true);
		assertThat(kt.get("readable")).isEqualTo(true);
		assertThat(((Number) kt.get("sizeBytes")).longValue()).isPositive();
		assertThat(kt.get("principalInKeytab")).isEqualTo(true);
		assertThat(kt.toString()).doesNotContain("0102030405");
		@SuppressWarnings("unchecked")
		Map<String, Object> login = (Map<String, Object>) r.get("login");
		assertThat(login.get("status")).isIn("error", "timeout");
		assertThat((String) login.get("error")).contains("KDC'ye ulaşılamadı");
	}

	@Test
	void missingKrb5ConfSkipsLogin() {
		String missing = dir.resolve("nope").resolve("krb5.conf").toString();
		System.setProperty(KerberosCheck.KRB5_CONF_PROPERTY, missing);
		Map<String, Object> r = KerberosCheck.check(new KerberosCheck.Settings("kerberos", "/etc/other-krb5.conf",
				dir.resolve("x.keytab").toString(), "svc", "kafka"), 5_000);
		@SuppressWarnings("unchecked")
		Map<String, Object> conf = (Map<String, Object>) r.get("krb5Conf");
		assertThat(conf.get("exists")).isEqualTo(false);
		assertThat(conf.get("path")).isEqualTo(missing);
		assertThat(problems(r)).anyMatch(p -> p.contains("krb5.conf bulunamadı"));
		@SuppressWarnings("unchecked")
		Map<String, Object> login = (Map<String, Object>) r.get("login");
		assertThat(login.get("status")).isEqualTo("skipped");
		assertThat(r.get("notes").toString()).contains("/etc/other-krb5.conf");
	}

	/** JDK: dns_lookup_kdc yoksa dns_fallback, o da yoksa true; kdc'siz realm DNS SRV ile bulunur. */
	@Test
	void realmWithoutKdcUsesDnsUnlessDisabled() throws IOException {
		Path dnsOnly = dir.resolve("dns-krb5.conf");
		Files.writeString(dnsOnly, """
				[libdefaults]
				  default_realm = CORP.TEST
				""");
		System.setProperty(KerberosCheck.KRB5_CONF_PROPERTY, dnsOnly.toString());
		assertThat(KerberosCheck.parseKrb5Conf(dnsOnly.toString()).dnsLookupKdc()).isTrue();
		Map<String, Object> r = KerberosCheck.check(new KerberosCheck.Settings("kerberos", null, null,
				"svc@CORP.TEST", "kafka"), 1_000);
		assertThat(problems(r)).noneMatch(p -> p.contains("kdc tanımı yok"));
		assertThat(r.get("notes").toString()).contains("DNS SRV");

		assertThat(dnsLookup("dns_lookup_kdc = false")).isFalse();
		assertThat(dnsLookup("dns_fallback = no")).isFalse();
		assertThat(dnsLookup("dns_lookup_kdc = true\n  dns_fallback = false")).isTrue();
		assertThat(dnsLookup("dns_lookup_kdc = yes")).isTrue();

		Path disabled = dir.resolve("nodns-krb5.conf");
		Files.writeString(disabled, """
				[libdefaults]
				  default_realm = CORP.TEST
				  dns_lookup_kdc = false
				""");
		System.setProperty(KerberosCheck.KRB5_CONF_PROPERTY, disabled.toString());
		Map<String, Object> off = KerberosCheck.check(new KerberosCheck.Settings("kerberos", null, null,
				"svc@CORP.TEST", "kafka"), 1_000);
		assertThat(problems(off)).anyMatch(p -> p.contains("kdc tanımı yok") && p.contains("false"));
	}

	private boolean dnsLookup(String libdefaults) throws IOException {
		Path conf = Files.createTempFile(dir, "krb5", ".conf");
		Files.writeString(conf, "[libdefaults]\n  default_realm = CORP.TEST\n  " + libdefaults + "\n");
		return KerberosCheck.parseKrb5Conf(conf.toString()).dnsLookupKdc();
	}

	@SuppressWarnings("unchecked")
	private static List<String> problems(Map<String, Object> r) {
		return (List<String>) r.get("problems");
	}

}
