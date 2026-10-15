package io.github.srdrcn.kafkamcp.mcp;

/**
 * Aracın gövdesi. Dönen nesne JSON'a çevrilir (String ise olduğu gibi metin döner). Kullanıcıya
 * gösterilecek hatalar için {@link ToolException} fırlatın; Kafka hataları otomatik Türkçeleştirilir.
 */
@FunctionalInterface
public interface ToolHandler {

	Object handle(ToolCall call) throws Exception;

}
