package io.github.srdrcn.kafkamcp.kafka;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Kafka ve Kerberos hatalarını kısa Türkçe açıklamalara çevirir. Orijinal mesaj parantez içinde korunur.
 */
public final class KafkaErrors {

	private KafkaErrors() {
	}

	public static String explain(Throwable error) {
		Throwable root = unwrap(error);
		if (root instanceof ToolException te) {
			return te.getMessage();
		}
		String chain = messages(root);
		String kerberos = kerberos(chain);
		if (kerberos != null) {
			return kerberos + " (" + chain + ")";
		}
		String hint = hint(root);
		return hint == null ? chain : hint + " (" + chain + ")";
	}

	/** Kerberos/JAAS kaynaklı bilinen hata kalıplarını Türkçe açıklar; eşleşme yoksa null. */
	public static String kerberos(String text) {
		if (text == null) {
			return null;
		}
		String t = text.toLowerCase(Locale.ROOT);
		if (t.contains("clock skew")) {
			return "Kerberos clock skew: pod ile KDC arasındaki saat farkı izin verilenden (genelde 5 dk) fazla";
		}
		if (t.contains("cannot locate kdc") || t.contains("receive timed out") || t.contains("cannot get kdc")
				|| (t.contains("kdc") && (t.contains("connection refused") || t.contains("unreachable")))) {
			return "KDC'ye ulaşılamadı (krb5.conf'taki kdc adresleri, DNS, ağ/firewall, port 88)";
		}
		if (t.contains("unable to obtain password from user") || t.contains("keytab") && t.contains("not exist")
				|| t.contains("keytab") && t.contains("no such file")) {
			return "keytab bulunamadı/okunamadı ya da principal keytab'da yok (kerberos.keytab yolu, Secret mount'u, principal adı)";
		}
		if (t.contains("client not found in kerberos database")) {
			return "Principal KDC'de bulunamadı (kerberos.principal ve realm doğru mu?)";
		}
		if (t.contains("pre-authentication information was invalid") || t.contains("checksum failed")
				|| t.contains("integrity check on decrypted field failed")) {
			return "keytab'daki anahtar geçersiz (parola/kvno değişmiş olabilir; keytab'ı yenileyin)";
		}
		if (t.contains("server not found in kerberos database")) {
			return "Broker servis principal'ı (kafka/<host>@REALM) KDC'de yok: bootstrap adresi FQDN mi, krb5.conf'ta rdns = false mi?";
		}
		if (t.contains("cannot find default realm") || t.contains("krb5.conf") && t.contains("not found")
				|| t.contains("cannot get default realm")) {
			return "krb5.conf bulunamadı ya da default_realm tanımlı değil (-Djava.security.krb5.conf)";
		}
		if (t.contains("message stream modified")) {
			return "Kerberos 'message stream modified': realm adı büyük/küçük harf ya da enctype uyumsuzluğu";
		}
		if (t.contains("encryption type") && (t.contains("not supported") || t.contains("not enabled"))) {
			return "Kerberos şifreleme tipi desteklenmiyor (krb5.conf permitted_enctypes / keytab enctype)";
		}
		if (t.contains("gssexception") || t.contains("loginexception") || t.contains("krbexception")) {
			return "Kerberos login hatası";
		}
		return null;
	}

	private static String hint(Throwable e) {
		if (e instanceof TimeoutException) {
			return "Kafka zaman aşımı: broker'lara ulaşılamadı ya da yanıt gelmedi (bootstrapServers, ağ, Kerberos)";
		}
		if (e instanceof SaslAuthenticationException) {
			return "SASL kimlik doğrulaması başarısız";
		}
		if (e instanceof TopicAuthorizationException) {
			return "Topic yetkisi yok (ACL)";
		}
		if (e instanceof GroupAuthorizationException) {
			return "Consumer group yetkisi yok (ACL)";
		}
		if (e instanceof ClusterAuthorizationException) {
			return "Cluster yetkisi yok (ACL)";
		}
		if (e instanceof AuthorizationException) {
			return "Yetki yok (ACL)";
		}
		if (e instanceof UnknownTopicOrPartitionException) {
			return "Topic ya da partition bulunamadı";
		}
		if (e instanceof TopicExistsException) {
			return "Topic zaten var";
		}
		if (e instanceof InvalidTopicException) {
			return "Geçersiz topic adı";
		}
		if (e instanceof GroupIdNotFoundException) {
			return "Consumer group bulunamadı";
		}
		if (e instanceof GroupNotEmptyException) {
			return "Consumer group boş değil (aktif üyeler var)";
		}
		if (e instanceof InvalidReplicationFactorException) {
			return "Geçersiz replication factor";
		}
		if (e instanceof InvalidPartitionsException) {
			return "Geçersiz partition sayısı";
		}
		if (e instanceof InvalidConfigurationException || e instanceof InvalidRequestException) {
			return "Geçersiz istek/config";
		}
		if (e instanceof PolicyViolationException) {
			return "Broker politikası isteği reddetti";
		}
		if (e instanceof SecurityDisabledException) {
			return "Cluster'da authorizer kapalı (ACL desteklenmiyor)";
		}
		if (e instanceof UnsupportedVersionException) {
			return "Broker bu işlemi desteklemiyor (sürüm)";
		}
		return null;
	}

	public static Throwable unwrap(Throwable e) {
		Throwable t = e;
		while ((t instanceof ExecutionException || t instanceof CompletionException) && t.getCause() != null) {
			t = t.getCause();
		}
		return t;
	}

	private static String messages(Throwable e) {
		List<String> parts = new ArrayList<>();
		Set<Throwable> seen = new HashSet<>();
		Throwable t = e;
		while (t != null && seen.add(t) && parts.size() < 5) {
			String m = t.getMessage();
			String s = t.getClass().getSimpleName() + (m == null || m.isBlank() ? "" : ": " + m);
			if (parts.isEmpty() || !parts.get(parts.size() - 1).contains(m == null ? s : m)) {
				parts.add(s);
			}
			t = t.getCause();
		}
		return String.join(" <- ", parts);
	}

}
