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
 * Field-level diff between two Avro schemas (added/removed/renamed field, type/default change, enum symbol, union
 * branch, name/namespace, fixed size, logical type) and a BACKWARD/FORWARD compatibility hint based on the Avro
 * reading rules.
 */
public final class AvroSchemaDiff {

	public static final int MAX_CHANGES = 200;

	private static final int MAX_ISSUES = 10;

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private AvroSchemaDiff() {
	}

	/** A single change: path (e.g. {@code Order.customer.email}), kind (e.g. FIELD_ADDED), detail, old/new value. */
	public record Change(String path, String kind, String detail, Object from, Object to) {

		public Map<String, Object> toMap() {
			return Results.map("path", path, "kind", kind, "detail", detail, "from", from, "to", to);
		}

	}

	/**
	 * Diff result. backward: the new schema (to) can read old data (from); forward: the old schema can read new data
	 * (Avro {@link SchemaCompatibility}; null if it could not be computed). The *Issues lists hold at most 10 entries;
	 * *IssueCount is the total number of incompatibilities.
	 */
	public record Result(List<Change> changes, int totalChanges, Boolean backward, Boolean forward,
			List<String> backwardIssues, List<String> forwardIssues, int backwardIssueCount, int forwardIssueCount) {

		public String hint() {
			if (backward == null || forward == null) {
				return "Compatibility could not be computed";
			}
			if (backward && forward) {
				return "FULL compatible: the new schema can read old data and the old schema can read new data";
			}
			if (backward) {
				return "BACKWARD compatible only: the new schema reads old data; consumers on the old schema may be "
						+ "unable to read new data";
			}
			if (forward) {
				return "FORWARD compatible only: the old schema reads new data; consumers on the new schema cannot "
						+ "read old data";
			}
			return "Incompatible: neither BACKWARD nor FORWARD (accepted only at level NONE)";
		}

		/**
		 * Is the level's condition met for these two versions (only this pair for TRANSITIVE levels); null if unknown.
		 */
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
			Map<String, Object> compat = Results.map("backward", backward, "forward", forward, "hint", hint(),
					"backwardIssues", backwardIssues.isEmpty() ? null : backwardIssues, "forwardIssues",
					forwardIssues.isEmpty() ? null : forwardIssues);
			if (backwardIssueCount > backwardIssues.size()) {
				compat.put("backwardIssuesOmitted", backwardIssueCount - backwardIssues.size());
			}
			if (forwardIssueCount > forwardIssues.size()) {
				compat.put("forwardIssuesOmitted", forwardIssueCount - forwardIssues.size());
			}
			m.put("compatibility", compat);
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
		int backwardCount = 0;
		int forwardCount = 0;
		try {
			SchemaPairCompatibility b = SchemaCompatibility.checkReaderWriterCompatibility(to, from);
			backward = b.getType() == SchemaCompatibilityType.COMPATIBLE;
			backwardCount = issues(b, backwardIssues);
			SchemaPairCompatibility f = SchemaCompatibility.checkReaderWriterCompatibility(from, to);
			forward = f.getType() == SchemaCompatibilityType.COMPATIBLE;
			forwardCount = issues(f, forwardIssues);
		}
		catch (RuntimeException e) {
			// compatibility could not be computed: the field diff is still returned
		}
		return new Result(d.changes, d.total, backward, forward, backwardIssues, forwardIssues,
				Math.max(backwardCount, backwardIssues.size()), Math.max(forwardCount, forwardIssues.size()));
	}

	/** Adds the first {@link #MAX_ISSUES} incompatibilities to out; returns the total number of incompatibilities. */
	private static int issues(SchemaPairCompatibility p, List<String> out) {
		if (p.getResult() == null) {
			return 0;
		}
		List<Incompatibility> all = p.getResult().getIncompatibilities();
		for (Incompatibility i : all) {
			if (out.size() >= MAX_ISSUES) {
				break;
			}
			out.add(i.getType() + " @" + i.getLocation() + ": " + i.getMessage());
		}
		return all.size();
	}

	/** Short type notation: {@code long(timestamp-millis)}, {@code array<string>}, {@code union[null, string]}. */
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

	/** Root path: the short name for named types (e.g. Order), "$" otherwise. */
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

	/** Union branch key: the full name for named types, the type name otherwise. */
	private static String key(Schema s) {
		return switch (s.getType()) {
			case RECORD, ENUM, FIXED -> s.getFullName();
			default -> s.getType().getName();
		};
	}

	/** Avro type promotion: can data written with the writer type be read with the reader type? */
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

	/**
	 * Shallow Avro reading rule: can data written with writer be read with reader (union branches, type promotion,
	 * name/alias and fixed size; record fields and enum symbols are not checked).
	 */
	static boolean readable(Schema writer, Schema reader) {
		if (writer.getType() == Type.UNION) {
			return writer.getTypes().stream().allMatch(w -> readable(w, reader));
		}
		if (reader.getType() == Type.UNION) {
			return reader.getTypes().stream().anyMatch(r -> readable(writer, r));
		}
		return switch (writer.getType()) {
			case RECORD, ENUM -> reader.getType() == writer.getType()
					&& SchemaCompatibility.schemaNameEquals(reader, writer);
			case FIXED -> reader.getType() == Type.FIXED && SchemaCompatibility.schemaNameEquals(reader, writer)
					&& reader.getFixedSize() == writer.getFixedSize();
			case ARRAY -> reader.getType() == Type.ARRAY && readable(writer.getElementType(), reader.getElementType());
			case MAP -> reader.getType() == Type.MAP && readable(writer.getValueType(), reader.getValueType());
			default -> promotable(writer.getType(), reader.getType());
		};
	}

	/** Converts an Avro default value to JSON text (null → "null"). */
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
				add(path, "TYPE_CHANGED",
						"Union changed to a single type: " + typeName(a) + " → " + typeName(b) + readNote(a, b), typeName(a),
						typeName(b));
				Schema match = branch(a, key(b));
				if (match != null) {
					compareSame(path, match, b);
				}
				return;
			}
			if (tb == Type.UNION) {
				add(path, "TYPE_CHANGED",
						"Type changed to a union: " + typeName(a) + " → " + typeName(b) + readNote(a, b), typeName(a),
						typeName(b));
				Schema match = branch(b, key(a));
				if (match != null) {
					compareSame(path, a, match);
				}
				return;
			}
			if (ta != tb) {
				add(path, "TYPE_CHANGED", "Type changed: " + typeName(a) + " → " + typeName(b) + promotionNote(ta, tb),
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
						add(path, "FIXED_SIZE_CHANGED", "fixed size changed (incompatible)", a.getFixedSize(),
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
							fb.hasDefaultValue() ? "Field added (default: " + json(fb.defaultVal()) + ")"
									: "Field added without a default (the new schema cannot read old data → BACKWARD "
											+ "incompatible)",
							null, typeName(fb.schema()));
					continue;
				}
				matched.add(fa.name());
				if (renamedFrom != null) {
					add(fp, "FIELD_RENAMED", "Field renamed (via alias): " + renamedFrom + " → " + fb.name(),
							renamedFrom, fb.name());
				}
				compare(fp, fa.schema(), fb.schema());
				compareDefaults(fp, fa, fb);
			}
			for (Field fa : a.getFields()) {
				if (!matched.contains(fa.name())) {
					add(path + "." + fa.name(), "FIELD_REMOVED", fa.hasDefaultValue()
							? "Field removed (it had a default in the old schema)"
							: "Field removed; it had no default in the old schema (old readers cannot read new data "
									+ "→ FORWARD incompatible)",
							typeName(fa.schema()), null);
				}
			}
		}

		private void compareDefaults(String path, Field fa, Field fb) {
			boolean ha = fa.hasDefaultValue();
			boolean hb = fb.hasDefaultValue();
			if (ha && !hb) {
				add(path, "DEFAULT_REMOVED", "Default value removed", json(fa.defaultVal()), null);
			}
			else if (!ha && hb) {
				add(path, "DEFAULT_ADDED", "Default value added", null, json(fb.defaultVal()));
			}
			else if (ha) {
				String da = json(fa.defaultVal());
				String db = json(fb.defaultVal());
				if (!da.equals(db)) {
					add(path, "DEFAULT_CHANGED", "Default value changed", da, db);
				}
			}
		}

		private void compareEnums(String path, Schema a, Schema b) {
			compareNames(path, a, b);
			List<String> removed = a.getEnumSymbols().stream().filter(s -> !b.getEnumSymbols().contains(s)).toList();
			List<String> added = b.getEnumSymbols().stream().filter(s -> !a.getEnumSymbols().contains(s)).toList();
			if (!removed.isEmpty()) {
				add(path, "ENUM_SYMBOLS_REMOVED", "Enum symbols removed" + (b.getEnumDefault() != null
						? " (the new schema has an enum default: " + b.getEnumDefault() + ")"
						: " (old data written with these symbols cannot be read → BACKWARD incompatible)"), removed, null);
			}
			if (!added.isEmpty()) {
				add(path, "ENUM_SYMBOLS_ADDED", "Enum symbols added" + (a.getEnumDefault() != null ? ""
						: " (old readers cannot read the new symbols → FORWARD incompatible)"), null, added);
			}
			if (!Objects.equals(a.getEnumDefault(), b.getEnumDefault())) {
				add(path, "ENUM_DEFAULT_CHANGED", "Enum default changed", a.getEnumDefault(), b.getEnumDefault());
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
				// ["null", X] → ["null", Y]: report it as a type change instead of a branch addition/removal
				compare(path, nonNullA.get(0), nonNullB.get(0));
				return;
			}
			for (String k : ma.keySet()) {
				if (!mb.containsKey(k)) {
					Schema reader = readingBranch(ma.get(k), b);
					add(path, "UNION_BRANCH_REMOVED", "Union branch removed: " + k + (reader != null
							? " (old data written with this branch is read by the " + typeName(reader)
									+ " branch → BACKWARD compatible)"
							: " (old data written with this branch cannot be read → BACKWARD incompatible)"),
							typeName(ma.get(k)), null);
				}
			}
			for (String k : mb.keySet()) {
				if (!ma.containsKey(k)) {
					Schema reader = readingBranch(mb.get(k), a);
					add(path, "UNION_BRANCH_ADDED", "Union branch added: " + k + (reader != null
							? " (old readers read this branch with the " + typeName(reader)
									+ " branch → FORWARD compatible)"
							: " (old readers cannot read this branch → FORWARD incompatible)"), null,
							typeName(mb.get(k)));
				}
			}
			String firstA = key(a.getTypes().get(0));
			String firstB = key(b.getTypes().get(0));
			if (!firstA.equals(firstB) && ma.containsKey(firstB) && mb.containsKey(firstA)) {
				add(path, "UNION_ORDER_CHANGED",
						"First union branch changed (a field default must match the type of the first branch)", firstA,
						firstB);
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
			String note = aliased ? " (old name defined as an alias)" : " (no alias: name mismatch when Avro reads the data)";
			if (Objects.equals(a.getName(), b.getName())) {
				add(path, "NAMESPACE_CHANGED", "Namespace changed" + note, nz(a.getNamespace()), nz(b.getNamespace()));
			}
			else {
				add(path, "NAME_CHANGED", "Type name changed" + note, a.getFullName(), b.getFullName());
			}
		}

		private void compareLogical(String path, Schema a, Schema b) {
			String la = logicalName(a);
			String lb = logicalName(b);
			if (!Objects.equals(la, lb)) {
				add(path, "LOGICAL_TYPE_CHANGED", "Logical type changed (physical type unchanged: " + a.getType().getName()
						+ ")", la == null ? "(none)" : la, lb == null ? "(none)" : lb);
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
			return s == null || s.isEmpty() ? "(none)" : s;
		}

		/**
		 * BACKWARD/FORWARD note for a type change to or from a union: the Avro reading rules (including type promotion
		 * and name matching) are applied branch by branch; nested fields are not checked since they are reported as
		 * separate changes.
		 */
		private static String readNote(Schema from, Schema to) {
			List<String> oldLost = unreadable(from, to);
			List<String> newLost = unreadable(to, from);
			String back = oldLost.isEmpty() ? "BACKWARD compatible" + (promoted(from, to) ? ", via type promotion" : "")
					: "BACKWARD incompatible: old data written with " + String.join(", ", oldLost) + " cannot be read";
			String fwd = newLost.isEmpty() ? "FORWARD compatible" + (promoted(to, from) ? ", via type promotion" : "")
					: "FORWARD incompatible: old readers cannot read new data written with "
							+ String.join(", ", newLost);
			return " (" + back + "; " + fwd + ")";
		}

		/** The parts of writer (its branches if it is a union) that reader cannot read (as short type names). */
		private static List<String> unreadable(Schema writer, Schema reader) {
			return branchesOf(writer).stream().filter(w -> !readable(w, reader)).map(AvroSchemaDiff::typeName).toList();
		}

		/** Is at least one primitive branch of writer read via type promotion, without an exact match in reader? */
		private static boolean promoted(Schema writer, Schema reader) {
			Set<String> exact = new HashSet<>();
			branchesOf(reader).forEach(r -> exact.add(key(r)));
			return branchesOf(writer).stream()
				.anyMatch(w -> !exact.contains(key(w)) && !isComplex(w.getType()) && readable(w, reader));
		}

		/**
		 * First reader branch that reads data written with the writer branch (reader itself if not a union); null if
		 * none.
		 */
		private static Schema readingBranch(Schema writer, Schema reader) {
			return branchesOf(reader).stream().filter(r -> readable(writer, r)).findFirst().orElse(null);
		}

		private static List<Schema> branchesOf(Schema s) {
			return s.getType() == Type.UNION ? s.getTypes() : List.of(s);
		}

		private static boolean isComplex(Type t) {
			return switch (t) {
				case RECORD, ENUM, FIXED, ARRAY, MAP, UNION -> true;
				default -> false;
			};
		}

		private static String promotionNote(Type from, Type to) {
			boolean back = promotable(from, to);
			boolean fwd = promotable(to, from);
			if (back) {
				return " (Avro type promotion: BACKWARD compatible" + (fwd ? ", FORWARD compatible)" : ", not FORWARD)");
			}
			if (fwd) {
				return " (FORWARD compatible, not BACKWARD)";
			}
			return " (incompatible type change)";
		}

	}

}
