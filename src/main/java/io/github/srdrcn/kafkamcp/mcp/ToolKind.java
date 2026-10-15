package io.github.srdrcn.kafkamcp.mcp;

/**
 * READ: her zaman listelenir. WRITE: sadece readOnly=false cluster'larda. DESTRUCTIVE: WRITE'a ek
 * olarak disableDestructive=false ister (*_delete, *_reset araçları).
 */
public enum ToolKind {

	READ, WRITE, DESTRUCTIVE;

	public boolean writes() {
		return this != READ;
	}

}
