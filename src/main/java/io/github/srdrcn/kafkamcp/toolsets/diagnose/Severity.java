package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.Locale;

/** Finding severity; ordered from critical to info. */
public enum Severity {

	CRITICAL, WARNING, INFO;

	/** Value in the JSON: critical, warning, info. */
	public String label() {
		return name().toLowerCase(Locale.ROOT);
	}

}
