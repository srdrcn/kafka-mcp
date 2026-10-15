package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.Locale;

/** Bulgu önem derecesi; sıralama kritikten bilgiye doğrudur. */
public enum Severity {

	CRITICAL, WARNING, INFO;

	/** JSON'daki değer: critical, warning, info. */
	public String label() {
		return name().toLowerCase(Locale.ROOT);
	}

}
