package io.github.srdrcn.kafkamcp.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;

import io.github.srdrcn.kafkamcp.mcp.ToolRegistry;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

/** Araçları {@link ToolRegistry} üzerinden çağırıp sonucu doğrulayan yardımcılar. */
public final class ToolAssertions {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private ToolAssertions() {
	}

	/** {@code args("topic", "x", "limit", 5)}; cluster verilmemişse {@link TestConfigs#CLUSTER} eklenir. */
	public static Map<String, Object> args(Object... keyValues) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("cluster", TestConfigs.CLUSTER);
		for (int i = 0; i < keyValues.length; i += 2) {
			m.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
		}
		return m;
	}

	public static String text(CallToolResult r) {
		assertThat(r.content()).hasSize(1);
		return ((McpSchema.TextContent) r.content().get(0)).text();
	}

	/** Başarılı olmalı; JSON ise parse edilmiş, değilse TextNode döner. */
	public static JsonNode ok(ToolRegistry registry, String tool, Map<String, Object> args) {
		CallToolResult r = registry.call(tool, args);
		String text = text(r);
		assertThat(r.isError()).as("%s hata döndü: %s", tool, text).isFalse();
		try {
			return MAPPER.readTree(text);
		}
		catch (Exception e) {
			return TextNode.valueOf(text);
		}
	}

	/** Hata dönmeli; hata mesajını döner. */
	public static String error(ToolRegistry registry, String tool, Map<String, Object> args) {
		CallToolResult r = registry.call(tool, args);
		String text = text(r);
		assertThat(r.isError()).as("%s hata dönmeliydi, dönen: %s", tool, text).isTrue();
		return text;
	}

}
