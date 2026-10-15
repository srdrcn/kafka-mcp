package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Schema Registry şema referansı ({@code name, subject, version}); araç argümanından ve SR yanıtından okunur. */
public record SchemaReference(String name, String subject, int version) {

	/** {@code references} parametresinin şeması (araçlar arasında ortak). */
	public static Map<String, Object> param() {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("name", P.string("Referans adı (Avro'da tam tip adı, JSON/PROTOBUF'ta dosya/URL adı)"));
		item.put("subject", P.string("Referans verilen subject"));
		item.put("version", P.integer("Referans verilen versiyon"));
		return P.objectArray("Şema referansları (opsiyonel)", item, List.of("name", "subject", "version"));
	}

	/** Araç argümanından referans listesi; yoksa boş liste. Eksik alanlarda Türkçe hata. */
	public static List<SchemaReference> fromArg(Object arg) {
		List<SchemaReference> out = new ArrayList<>();
		if (arg == null) {
			return out;
		}
		if (!(arg instanceof Collection<?> c)) {
			throw new ToolException("references bir dizi olmalı: [{name, subject, version}]");
		}
		for (Object o : c) {
			if (!(o instanceof Map<?, ?> m)) {
				throw new ToolException("references elemanları nesne olmalı: {name, subject, version}");
			}
			Object name = m.get("name");
			Object subject = m.get("subject");
			Object version = m.get("version");
			if (name == null || subject == null || version == null || String.valueOf(name).isBlank()
					|| String.valueOf(subject).isBlank()) {
				throw new ToolException("Her referansta name, subject ve version zorunlu");
			}
			int v;
			try {
				v = version instanceof Number n ? n.intValue() : Integer.parseInt(String.valueOf(version).trim());
			}
			catch (NumberFormatException e) {
				throw new ToolException("Referans versiyonu tamsayı olmalı: " + version);
			}
			out.add(new SchemaReference(String.valueOf(name).trim(), String.valueOf(subject).trim(), v));
		}
		return out;
	}

	/** SR yanıtındaki {@code references} dizisi; yoksa boş liste. */
	public static List<SchemaReference> fromJson(JsonNode node) {
		List<SchemaReference> out = new ArrayList<>();
		if (node != null && node.isArray()) {
			for (JsonNode r : node) {
				out.add(new SchemaReference(r.path("name").asText(), r.path("subject").asText(),
						r.path("version").asInt()));
			}
		}
		return out;
	}

	/** SR istek gövdesi için. */
	public Map<String, Object> toMap() {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("name", name);
		m.put("subject", subject);
		m.put("version", version);
		return m;
	}

}
