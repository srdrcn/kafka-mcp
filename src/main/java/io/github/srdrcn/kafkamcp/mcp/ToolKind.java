package io.github.srdrcn.kafkamcp.mcp;

/**
 * READ: always listed. WRITE: only on readOnly=false clusters. DESTRUCTIVE: on top of WRITE, also
 * requires disableDestructive=false (*_delete, *_reset tools).
 */
public enum ToolKind {

	READ, WRITE, DESTRUCTIVE;

	public boolean writes() {
		return this != READ;
	}

}
