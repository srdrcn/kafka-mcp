package io.github.srdrcn.kafkamcp.mcp;

/**
 * Kullanıcıya (LLM'e) olduğu gibi gösterilecek Türkçe hata. Araç sonucu {@code isError: true} olur.
 */
public class ToolException extends RuntimeException {

	public ToolException(String message) {
		super(message);
	}

	public ToolException(String message, Throwable cause) {
		super(message, cause);
	}

}
