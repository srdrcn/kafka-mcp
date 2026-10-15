package io.github.srdrcn.kafkamcp.mcp;

/**
 * Error shown as is to the user (the LLM). The tool result gets {@code isError: true}.
 */
public class ToolException extends RuntimeException {

	public ToolException(String message) {
		super(message);
	}

	public ToolException(String message, Throwable cause) {
		super(message, cause);
	}

}
