package io.github.srdrcn.kafkamcp.toolsets.acl;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** Kerberos testleri için krb5.conf ve sahte keytab üretir. */
final class KerberosTestFiles {

	private KerberosTestFiles() {
	}

	/** KDC'si erişilemeyen (127.0.0.1:1, kısa timeout, tek deneme) krb5.conf. */
	static Path unreachableKdcKrb5Conf(Path dir, String realm) throws IOException {
		Path conf = dir.resolve("krb5.conf");
		Files.writeString(conf, """
				# test
				[libdefaults]
				  default_realm = %s
				  dns_lookup_kdc = false
				  dns_lookup_realm = false
				  rdns = false
				  kdc_timeout = 300
				  max_retries = 1
				  udp_preference_limit = 65535

				[realms]
				  %s = {
				    kdc = 127.0.0.1:1
				    admin_server = 127.0.0.1:1
				    auth_to_local_names = {
				      x = y
				    }
				  }

				[domain_realm]
				  .example.test = %s
				""".formatted(realm, realm, realm), StandardCharsets.UTF_8);
		return conf;
	}

	/** MIT keytab (v0x502) biçiminde tek girdili sahte keytab (aes128-cts-hmac-sha1-96, sabit anahtar). */
	static void writeKeytab(Path file, String name, String realm) throws IOException {
		ByteArrayOutputStream entry = new ByteArrayOutputStream();
		DataOutputStream e = new DataOutputStream(entry);
		e.writeShort(1);
		writeString(e, realm);
		writeString(e, name);
		e.writeInt(1);
		e.writeInt((int) (System.currentTimeMillis() / 1000));
		e.writeByte(1);
		e.writeShort(17);
		byte[] key = new byte[16];
		for (int i = 0; i < key.length; i++) {
			key[i] = (byte) (i + 1);
		}
		e.writeShort(key.length);
		e.write(key);
		e.writeInt(1);
		e.flush();
		try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(file))) {
			out.writeShort(0x0502);
			out.writeInt(entry.size());
			out.write(entry.toByteArray());
		}
	}

	private static void writeString(DataOutputStream out, String s) throws IOException {
		byte[] b = s.getBytes(StandardCharsets.UTF_8);
		out.writeShort(b.length);
		out.write(b);
	}

}
