package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.Incompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * İki Avro şeması arasında alan bazlı fark (eklenen/silinen/yeniden adlandırılan alan, tip/varsayılan değişikliği,
 * enum sembolü, union dalı, ad/namespace, fixed boyutu, mantıksal tip) ve Avro okuma kurallarına göre
 * BACKWARD/FORWARD uyumluluk ipucu.
 */
public final class AvroSchemaDiff {

	public static final int MAX_CHANGES = 200;

	private static final int MAX_ISSUES = 10;

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private AvroSchemaDiff() {
	}

	/** Tek fark: path (ör. {@code Order.customer.email}), kind (ör. FIELD_ADDED), Türkçe açıklama, eski/yeni değer. */
	public record Change(String path, String kind, String detail, Object from, Object to) {

		public Map<String, Object> toMap() {
			return Results.map("path", path, "kind", kind, "detail", detail, "from", from, "to", to);
		}

	}

	/**
	 * Fark sonucu. backward: yeni şema (to) eski veriyi (from) okuyabilir; forward: eski şema yeni veriyi okuyabilir
	 * (Avro {@link SchemaCompatibility}; hesaplanamadıysa null).
	 */
	public record Result(List<Change> changes, int totalChanges, Boolean backward, Boolean forward,
			List<String> backwardIssues, List<String> forwardIssues) {

		public String hint() {
			if (backward == null || forward == null) {
				return "Uyumluluk hesaplanamadı";
			}
			if (backward && forward) {
				return "FULL uyumlu: yeni şema eski veriyi, eski şema yeni veriyi okuyabilir";
			}
			if (backward) {
				return "Sadece BACKWARD uyumlu: yeni şema eski veriyi okur; eski şemalı consumer'lar yeni veriyi okuyamayabilir";
			}
			if (forward) {
				return "Sadece FORWARD uyumlu: eski şema yeni veriyi okur; yeni şemalı consumer'lar eski veriyi okuyamaz";
			}
			return "Uyumsuz: ne BACKWARD ne FORWARD (sadece NONE seviyesinde kabul edilir)";
		}

		/** Seviyenin bu iki versiyon için koşulu sağlanıyor mu (TRANSITIVE seviyelerde sadece bu çift); bilinmiyorsa null. */
		public Boolean satisfies(String level) {
			if (level == null || backward == null || forward == null) {
				return null;
			}
			return switch (level) {
				case "BACKWARD", "BACKWARD_TRANSITIVE" -> backward;
				case "FORWARD", "FORWARD_TRANSITIVE" -> forward;
				case "FULL", "FULL_TRANSITIVE" -> backward && forward;
				case "NONE" -> true;
				default -> null;
			};
		}

		public Map<String, Object> toMap() {
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("totalChanges", totalChanges);
			m.put("changes", changes.stream().map(Change::toMap).toList());
			if (totalChanges > changes.size()) {
				m.put("changesTruncated", true);
				m.put("omittedChanges", totalChanges - changes.size());
			}
			m.put("compatibility", Results.map("backward", backward, "forward", forward, "hint", hint(),
					"backwardIssues", backwardIssues.isEmpty() ? null : backwardIssues, "forwardIssues",
					forwardIssues.isEmpty() ? null : forwardIssues));
			return m;
		}

	}

	public static Result diff(Schema from, Schema to) {
		Differ d = new Differ();
		d.compare(rootPath(to), from, to);
		Boolean backward = null;
		Boolean forward = null;
		List<String> backwardIssues = new ArrayList<>();
		List<String> forwardIssues = new ArrayList<>();
		try {
			SchemaPairCompatibility b = SchemaCompatibility.checkReaderWriterCompatibility(to, from);
			backward = b.getType() == SchemaCompatibilityType.COMPATIBLE;
			issues(b, backwardIssues);
			SchemaPairCompatibility f = SchemaCompatibility.checkReaderWriterCompatibility(from, to);
			forward = f.getType() == SchemaCompatibilityType.COMPATIBLE;
			issues(f, forwardIssues);
		}
		catch (RuntimeException e) {
			// uyumluluk hesaplanamadı: alan farkı yine döner
		}
		return new Result(d.changes, d.total, backward, forward, backwardIssues, forwardIssues);
	}

	private static void issues(SchemaPairCompatibility p, List<String> out) {
		if (p.getResult() == null) {
			return;
		}
		for (Incompatibility i : p.getResult().getIncompatibilities()) {
			if (out.size() >= MAX_ISSUES) {
				break;
			}
			out.add(i.getType() + " @" + i.getLocation() + ": " + i.getMessage());
		}
	}

	/** Kısa tip gösterimi: {@code long(timestamp-millis)}, {@code array<string>}, {@code union[null, string]}. */
	public static String typeName(Schema s) {
		return switch (s.getType()) {
			case RECORD -> s.getFullName();
			case ENUM -> "enum " + s.getFullName();
			case FIXED -> "fixed " + s.getFullName() + "(" + s.getFixedSize() + ")";
			case ARRAY -> "array<" + typeName(s.getElementType()) + ">";
			case MAP -> "map<" + typeName(s.getValueType()) + ">";
			case UNION -> "union[" + String.join(", ", s.getTypes().stream().map(AvroSchemaDiff::typeName).toList())
					+ "]";
			default -> {
				String l = logicalName(s);
				yield s.getName() + (l == null ? "" : "(" + l + ")");
			}
		};
	}

	/** Kök yolu: adlı tiplerde kısa ad (ör. Order), diğerlerinde "$". */
	private static String rootPath(Schema s) {
		return switch (s.getType()) {
			case RECORD, ENUM, FIXED -> s.getName();
			default -> "$";
		};
	}

	private static String logicalName(Schema s) {
		LogicalType lt = s.getLogicalType();
		if (lt instanceof LogicalTypes.Decimal d) {
			return "decimal(" + d.getPrecision() + "," + d.getScale() + ")";
		}
		if (lt != null) {
			return lt.getName();
		}
		return s.getProp("logicalType");
	}

	/** Union dalı anahtarı: adlı tiplerde tam ad, diğerlerinde tip adı. */
	private static String key(Schema s) {
		return switch (s.getType()) {
			case RECORD, ENUM, FIXED -> s.getFullName();
			default -> s.getType().getName();
		};
	}

	/** Avro tip yükseltmesi: writer tipiyle yazılmış veri reader tipiyle okunabilir mi. */
	private static boolean promotable(Type writer, Type reader) {
		if (writer == reader) {
			return true;
		}
		return switch (writer) {
			case INT -> reader == Type.LONG || reader == Type.FLOAT || reader == Type.DOUBLE;
			case LONG -> reader == Type.FLOAT || reader == Type.DOUBLE;
			case FLOAT -> reader == Type.DOUBLE;
			case STRING -> reader == Type.BYTES;
			case BYTES -> reader == Type.STRING;
			default -> false;
		};
	}

	/** Avro varsayılan değerini JSON metnine çevirir (null → "null"). */
	static String json(Object defaultValue) {
		try {
			return MAPPER.writeValueAsString(plain(defaultValue));
		}
		catch (Exception e) {
			return String.valueOf(defaultValue);
		}
	}

	private static Object plain(Object v) {
		if (v == null || v == JsonProperties.NULL_VALUE) {
			return null;
		}
		if (v instanceof byte[] b) {
			return new String(b, StandardCharsets.ISO_8859_1);
		}
		if (v instanceof Map<?, ?> m) {
			Map<String, Object> out = new LinkedHashMap<>();
			m.forEach((k, x) -> out.put(String.valueOf(k), plain(x)));
			return out;
		}
		if (v instanceof Collection<?> c) {
			return c.stream().map(AvroSchemaDiff::plain).toList();
		}
		return v;
	}

	private static final class Differ {

		private final List<Change> changes = new ArrayList<>();

		private final Set<String> visited = new HashSet<>();

		private int total;

		private void add(String path, String kind, String detail, Object from, Object to) {
			total++;
			if (changes.size() < MAX_CHANGES) {
				changes.add(new Change(path, kind, detail, from, to));
			}
		}

		void compare(String path, Schema a, Schema b) {
			Type ta = a.getType();
			Type tb = b.getType();
			if (ta == Type.UNION && tb == Type.UNION) {
				compareUnions(path, a, b);
				return;
			}
			if (ta == Type.UNION) {
				Schema match = branch(a, key(b));
				add(path, "TYPE_CHANGED", "Union tek tipe çevrildi: " + typeName(a) + " → " + typeName(b)
						+ (match != null ? " (diğer dallarla yazılmış eski veri okunamaz → BACKWARD uyumsuz)"
								: " (eski union'da bu tip yok → uyumsuz)"),
						typeName(a), typeName(b));
				if (match != null) {
					compareSame(path, match, b);
				}
				return;
			}
			if (tb == Type.UNION) {
				Schema match = branch(b, key(a));
				add(path, "TYPE_CHANGED", "Tip union'a çevrildi: " + typeName(a) + " → " + typeName(b)
						+ (match != null ? " (eski tip union'da var → BACKWARD uyumlu; eski okuyucular diğer dalları okuyamaz)"
								: " (eski tip union'da yok → uyumsuz)"),
						typeName(a), typeName(b));
				if (match != null) {
					compareSame(path, a, match);
				}
				return;
			}
			if (ta != tb) {
				add(path, "TYPE_CHANGED", "Tip değişti: " + typeName(a) + " → " + typeName(b) + promotionNote(ta, tb),
						typeName(a), typeName(b));
				return;
			}
			compareSame(path, a, b);
		}

		private void compareSame(String path, Schema a, Schema b) {
			switch (a.getType()) {
				case RECORD -> compareRecords(path, a, b);
				case ENUM -> compareEnums(path, a, b);
				case FIXED -> {
					compareNames(path, a, b);
					if (a.getFixedSize() != b.getFixedSize()) {
						add(path, "FIXED_SIZE_CHANGED", "fixed boyutu değişti (uyumsuz)", a.getFixedSize(),
								b.getFixedSize());
					}
					compareLogical(path, a, b);
				}
				case ARRAY -> compare(path + "[]", a.getElementType(), b.getElementType());
				case MAP -> compare(path + "{}", a.getValueType(), b.getValueType());
				default -> compareLogical(path, a, b);
			}
		}

		private void compareRecords(String path, Schema a, Schema b) {
			compareNames(path, a, b);
			if (!visited.add(a.getFullName() + "|" + b.getFullName())) {
				return;
			}
			Set<String> matched = new HashSet<>();
			for (Field fb : b.getFields()) {
				Field fa = a.getField(fb.name());
				String renamedFrom = null;
				if (fa == null) {
					for (String alias : fb.aliases()) {
						Field x = a.getField(alias);
						if (x != null) {
							fa = x;
							renamedFrom = alias;
							break;
						}
					}
				}
				String fp = path + "." + fb.name();
				if (fa == null) {
					add(fp, "FIELD_ADDED",
							fb.hasDefaultValue() ? "Alan eklendi (varsayılan: " + json(fb.defaultVal()) + ")"
									: "Alan eklendi, varsayılanı yok (yeni şema eski veriyi okuyamaz → BACKWARD uyumsuz)",
							null, typeName(fb.schema()));
					continue;
				}
				matched.add(fa.name());
				if (renamedFrom != null) {
					add(fp, "FIELD_RENAMED", "Alan yeniden adlandırıldı (alias ile): " + renamedFrom + " → " + fb.name(),
							renamedFrom, fb.name());
				}
				compare(fp, fa.schema(), fb.schema());
				compareDefaults(fp, fa, fb);
			}
			for (Field fa : a.getFields()) {
				if (!matched.contains(fa.name())) {
					add(path + "." + fa.name(), "FIELD_REMOVED", fa.hasDefaultValue()
							? "Alan silindi (eski şemada varsayılanı vardı)"
							: "Alan silindi, eski şemada varsayılanı yoktu (eski okuyucular yeni veriyi okuyamaz → FORWARD uyumsuz)",
							typeName(fa.schema()), null);
				}
			}
		}

		private void compareDefaults(String path, Field fa, Field fb) {
			boolean ha = fa.hasDefaultValue();
			boolean hb = fb.hasDefaultValue();
			if (ha && !hb) {
				add(path, "DEFAULT_REMOVED", "Varsayılan değer kaldırıldı", json(fa.defaultVal()), null);
			}
			else if (!ha && hb) {
				add(path, "DEFAULT_ADDED", "Varsayılan değer eklendi", null, json(fb.defaultVal()));
			}
			else if (ha) {
				String da = json(fa.defaultVal());
				String db = json(fb.defaultVal());
				if (!da.equals(db)) {
					add(path, "DEFAULT_CHANGED", "Varsayılan değer değişti", da, db);
				}
			}
		}

		private void compareEnums(String path, Schema a, Schema b) {
			compareNames(path, a, b);
			List<String> removed = a.getEnumSymbols().stream().filter(s -> !b.getEnumSymbols().contains(s)).toList();
			List<String> added = b.getEnumSymbols().stream().filter(s -> !a.getEnumSymbols().contains(s)).toList();
			if (!removed.isEmpty()) {
				add(path, "ENUM_SYMBOLS_REMOVED", "Enum sembolü silindi" + (b.getEnumDefault() != null
						? " (yeni şemada enum varsayılanı var: " + b.getEnumDefault() + ")"
						: " (bu sembollerle yazılmış eski veri okunamaz → BACKWARD uyumsuz)"), removed, null);
			}
			if (!added.isEmpty()) {
				add(path, "ENUM_SYMBOLS_ADDED", "Enum sembolü eklendi" + (a.getEnumDefault() != null ? ""
						: " (eski okuyucular yeni sembolleri okuyamaz → FORWARD uyumsuz)"), null, added);
			}
			if (!Objects.equals(a.getEnumDefault(), b.getEnumDefault())) {
				add(path, "ENUM_DEFAULT_CHANGED", "Enum varsayılanı değişti", a.getEnumDefault(), b.getEnumDefault());
			}
		}

		private void compareUnions(String path, Schema a, Schema b) {
			Map<String, Schema> ma = branches(a);
			Map<String, Schema> mb = branches(b);
			List<Schema> nonNullA = a.getTypes().stream().filter(s -> s.getType() != Type.NULL).toList();
			List<Schema> nonNullB = b.getTypes().stream().filter(s -> s.getType() != Type.NULL).toList();
			boolean nullable = ma.containsKey("null") && mb.containsKey("null");
			if (nonNullA.size() == 1 && nonNullB.size() == 1 && ma.size() == mb.size()
					&& !key(nonNullA.get(0)).equals(key(nonNullB.get(0)))) {
				// ["null", X] → ["null", Y]: dal ekleme/silme yerine tip değişikliği olarak göster
				compare(path, nonNullA.get(0), nonNullB.get(0));
				return;
			}
			for (String k : ma.keySet()) {
				if (!mb.containsKey(k)) {
					add(path, "UNION_BRANCH_REMOVED",
							"Union dalı silindi: " + k + " (bu dalla yazılmış eski veri okunamaz → BACKWARD uyumsuz)",
							typeName(ma.get(k)), null);
				}
			}
			for (String k : mb.keySet()) {
				if (!ma.containsKey(k)) {
					add(path, "UNION_BRANCH_ADDED",
							"Union dalı eklendi: " + k + " (eski okuyucular bu dalı okuyamaz → FORWARD uyumsuz)", null,
							typeName(mb.get(k)));
				}
			}
			String firstA = key(a.getTypes().get(0));
			String firstB = key(b.getTypes().get(0));
			if (!firstA.equals(firstB) && ma.containsKey(firstB) && mb.containsKey(firstA)) {
				add(path, "UNION_ORDER_CHANGED", "Union'un ilk dalı değişti (alan varsayılanı ilk dalın tipinde olmalı)",
						firstA, firstB);
			}
			boolean single = nullable && nonNullA.size() == 1 && nonNullB.size() == 1;
			for (Map.Entry<String, Schema> e : ma.entrySet()) {
				Schema other = mb.get(e.getKey());
				if (other != null && e.getValue().getType() != Type.NULL) {
					compare(single || ma.size() == 1 ? path : path + "<" + simple(e.getKey()) + ">", e.getValue(),
							other);
				}
			}
		}

		private void compareNames(String path, Schema a, Schema b) {
			if (Objects.equals(a.getFullName(), b.getFullName())) {
				return;
			}
			boolean aliased = b.getAliases().contains(a.getFullName());
			String note = aliased ? " (eski ad alias olarak tanımlı)" : " (alias yok: Avro okumasında ad uyuşmazlığı)";
			if (Objects.equals(a.getName(), b.getName())) {
				add(path, "NAMESPACE_CHANGED", "Namespace değişti" + note, nz(a.getNamespace()), nz(b.getNamespace()));
			}
			else {
				add(path, "NAME_CHANGED", "Tip adı değişti" + note, a.getFullName(), b.getFullName());
			}
		}

		private void compareLogical(String path, Schema a, Schema b) {
			String la = logicalName(a);
			String lb = logicalName(b);
			if (!Objects.equals(la, lb)) {
				add(path, "LOGICAL_TYPE_CHANGED", "Mantıksal tip değişti (fiziksel tip aynı: " + a.getType().getName()
						+ ")", la == null ? "(yok)" : la, lb == null ? "(yok)" : lb);
			}
		}

		private static Map<String, Schema> branches(Schema union) {
			Map<String, Schema> m = new LinkedHashMap<>();
			for (Schema s : union.getTypes()) {
				m.put(key(s), s);
			}
			return m;
		}

		private static Schema branch(Schema union, String key) {
			return branches(union).get(key);
		}

		private static String simple(String key) {
			int i = key.lastIndexOf('.');
			return i < 0 ? key : key.substring(i + 1);
		}

		private static String nz(String s) {
			return s == null || s.isEmpty() ? "(yok)" : s;
		}

		private static String promotionNote(Type from, Type to) {
			boolean back = promotable(from, to);
			boolean fwd = promotable(to, from);
			if (back) {
				return " (Avro tip yükseltmesi: BACKWARD uyumlu" + (fwd ? ", FORWARD uyumlu)" : ", FORWARD değil)");
			}
			if (fwd) {
				return " (FORWARD uyumlu, BACKWARD değil)";
			}
			return " (uyumsuz tip değişikliği)";
		}

	}

}
