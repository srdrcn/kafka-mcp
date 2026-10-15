package io.github.srdrcn.kafkamcp.config;

public class KerberosConfig {

	private String krb5Conf;

	private String keytab;

	private String principal;

	private String serviceName = "kafka";

	public String getKrb5Conf() {
		return krb5Conf;
	}

	public void setKrb5Conf(String krb5Conf) {
		this.krb5Conf = krb5Conf;
	}

	public String getKeytab() {
		return keytab;
	}

	public void setKeytab(String keytab) {
		this.keytab = keytab;
	}

	public String getPrincipal() {
		return principal;
	}

	public void setPrincipal(String principal) {
		this.principal = principal;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

}
