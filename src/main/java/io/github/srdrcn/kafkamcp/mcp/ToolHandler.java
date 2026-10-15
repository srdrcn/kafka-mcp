package io.github.srdrcn.kafkamcp.mcp;

/**
 * The tool body. The returned object is serialized to JSON (a String is returned as plain text). Throw
 * {@link ToolException} for errors shown to the user; Kafka errors are explained automatically.
 */
@FunctionalInterface
public interface ToolHandler {

	Object handle(ToolCall call) throws Exception;

}
