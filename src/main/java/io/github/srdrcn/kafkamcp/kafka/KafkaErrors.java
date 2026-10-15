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
import org.apache.kafka.common.errors.NoReassignmentInProgressException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Translates Kafka and Kerberos errors into short explanations. The original message is kept in parentheses.
 */
public final class KafkaErrors {

	/**
	 * Explanation for when the broker does not support an API (or the requested version of it); older
	 * (e.g. 2.x ZooKeeper) brokers lack some operations. Tools report this as a per-item note instead of
	 * failing the whole tool.
	 */
	public static final String UNSUPPORTED = "Broker does not support this operation (version)";

	private KafkaErrors() {
	}

	/**
	 * Is the error (or the cause of a ToolException) an UnsupportedVersionException? ExecutionException/
	 * CompletionException wrappers and the ToolException cause chain are unwrapped.
	 */
	public static boolean isUnsupported(Throwable error) {
		Throwable t = error;
		for (int i = 0; t != null && i < 10; i++) {
			Throwable u = unwrap(t);
			if (u instanceof UnsupportedVersionException) {
				return true;
			}
			if (u instanceof ToolException && isUnsupported(u.getMessage())) {
				return true;
			}
			t = u.getCause() == u ? null : u.getCause();
		}
		return false;
	}

	/** Is a message produced by {@link #explain} the "does not support (version)" message? */
	public static boolean isUnsupported(String message) {
		return message != null && message.startsWith(UNSUPPORTED);
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

	/** Explains known Kerberos/JAAS error patterns; null if nothing matches. */
	public static String kerberos(String text) {
		if (text == null) {
			return null;
		}
		String t = text.toLowerCase(Locale.ROOT);
		if (t.contains("clock skew")) {
			return "Kerberos clock skew: the pod clock differs from the KDC by more than the allowed skew (usually 5 minutes)";
		}
		if (t.contains("cannot locate kdc") || t.contains("receive timed out") || t.contains("cannot get kdc")
				|| (t.contains("kdc") && (t.contains("connection refused") || t.contains("unreachable")))) {
			return "Cannot reach the KDC (check the kdc entries in krb5.conf, DNS, firewall, port 88)";
		}
		if (t.contains("unable to obtain password from user") || t.contains("keytab") && t.contains("not exist")
				|| t.contains("keytab") && t.contains("no such file")) {
			return "Keytab not found or unreadable, or the principal is not in the keytab (check kerberos.keytab, the Secret mount and the principal)";
		}
		if (t.contains("client not found in kerberos database")) {
			return "Principal not found in the KDC (check kerberos.principal and the realm)";
		}
		if (t.contains("pre-authentication information was invalid") || t.contains("checksum failed")
				|| t.contains("integrity check on decrypted field failed")) {
			return "Keytab key is invalid (password/kvno changed; regenerate the keytab)";
		}
		if (t.contains("server not found in kerberos database")) {
			return "Broker service principal (kafka/<host>@REALM) not found in the KDC: is the bootstrap address an FQDN, and is rdns = false set in krb5.conf?";
		}
		if (t.contains("cannot find default realm") || t.contains("krb5.conf") && t.contains("not found")
				|| t.contains("cannot get default realm")) {
			return "krb5.conf not found or default_realm missing (-Djava.security.krb5.conf)";
		}
		if (t.contains("message stream modified")) {
			return "Kerberos 'message stream modified': realm name case mismatch or enctype mismatch";
		}
		if (t.contains("encryption type") && (t.contains("not supported") || t.contains("not enabled"))) {
			return "Kerberos encryption type not supported (krb5.conf permitted_enctypes / keytab enctype)";
		}
		if (t.contains("gssexception") || t.contains("loginexception") || t.contains("krbexception")) {
			return "Kerberos login failed";
		}
		return null;
	}

	private static String hint(Throwable e) {
		if (e instanceof TimeoutException) {
			return "Kafka timeout: brokers unreachable or not responding (check bootstrapServers, network, Kerberos)";
		}
		if (e instanceof SaslAuthenticationException) {
			return "SASL authentication failed";
		}
		if (e instanceof TopicAuthorizationException) {
			return "Not authorized for the topic (ACL)";
		}
		if (e instanceof GroupAuthorizationException) {
			return "Not authorized for the consumer group (ACL)";
		}
		if (e instanceof ClusterAuthorizationException) {
			return "Not authorized for the cluster (ACL)";
		}
		if (e instanceof AuthorizationException) {
			return "Not authorized (ACL)";
		}
		if (e instanceof UnknownTopicOrPartitionException) {
			return "Topic or partition not found";
		}
		if (e instanceof TopicExistsException) {
			return "Topic already exists";
		}
		if (e instanceof InvalidTopicException) {
			return "Invalid topic name";
		}
		if (e instanceof GroupIdNotFoundException) {
			return "Consumer group not found";
		}
		if (e instanceof GroupNotEmptyException) {
			return "Consumer group is not empty (it has active members)";
		}
		if (e instanceof InvalidReplicationFactorException) {
			return "Invalid replication factor";
		}
		if (e instanceof InvalidPartitionsException) {
			return "Invalid partition count";
		}
		// UnsupportedVersionException is an InvalidRequestException: it must be checked first
		if (e instanceof UnsupportedVersionException) {
			return UNSUPPORTED;
		}
		if (e instanceof InvalidConfigurationException || e instanceof InvalidRequestException) {
			return "Invalid request/config";
		}
		if (e instanceof PolicyViolationException) {
			return "Broker policy rejected the request";
		}
		if (e instanceof SecurityDisabledException) {
			return "Authorizer is disabled on the cluster (ACLs not supported)";
		}
		if (e instanceof TopicDeletionDisabledException) {
			return "Topic deletion is disabled on the broker (delete.topic.enable=false)";
		}
		if (e instanceof NoReassignmentInProgressException) {
			return "No reassignment in progress for the partition";
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
