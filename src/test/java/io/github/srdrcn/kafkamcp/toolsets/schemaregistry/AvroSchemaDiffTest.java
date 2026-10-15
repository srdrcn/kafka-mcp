package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.toolsets.schemaregistry.AvroSchemaDiff.Change;

class AvroSchemaDiffTest {

	private static Schema parse(String json) {
		return new Schema.Parser().parse(json);
	}

	private static Change find(AvroSchemaDiff.Result r, String path, String kind) {
		return r.changes()
			.stream()
			.filter(c -> c.path().equals(path) && c.kind().equals(kind))
			.findFirst()
			.orElseThrow(() -> new AssertionError(path + " " + kind + " yok: " + r.changes()));
	}

	@Test
	void fieldAddedRemovedTypeAndDefaultChanges() {
		Schema a = parse("""
				{"type":"record","name":"User","namespace":"x","fields":[
				 {"name":"id","type":"string"},
				 {"name":"age","type":"int","default":0},
				 {"name":"old","type":"string"},
				 {"name":"nick","type":["null","string"],"default":null}]}""");
		Schema b = parse("""
				{"type":"record","name":"User","namespace":"x","fields":[
				 {"name":"id","type":"int"},
				 {"name":"age","type":"long","default":1},
				 {"name":"email","type":"string"},
				 {"name":"nick","type":["null","string","int"],"default":null}]}""");
		AvroSchemaDiff.Result r = AvroSchemaDiff.diff(a, b);
		assertThat(find(r, "User.id", "TYPE_CHANGED").detail()).contains("uyumsuz");
		Change age = find(r, "User.age", "TYPE_CHANGED");
		assertThat(age.from()).isEqualTo("int");
		assertThat(age.to()).isEqualTo("long");
		assertThat(age.detail()).contains("BACKWARD uyumlu");
		Change def = find(r, "User.age", "DEFAULT_CHANGED");
		assertThat(def.from()).isEqualTo("0");
		assertThat(def.to()).isEqualTo("1");
		assertThat(find(r, "User.email", "FIELD_ADDED").detail()).contains("varsayılanı yok");
		assertThat(find(r, "User.old", "FIELD_REMOVED").detail()).contains("FORWARD");
		assertThat(find(r, "User.nick", "UNION_BRANCH_ADDED").to()).isEqualTo("int");
		assertThat(r.backward()).isFalse();
		assertThat(r.forward()).isFalse();
		assertThat(r.hint()).contains("Uyumsuz");
		assertThat(r.backwardIssues()).isNotEmpty();
		assertThat(r.satisfies("NONE")).isTrue();
		assertThat(r.satisfies("BACKWARD")).isFalse();
	}

	@Test
	void nestedRecordsArraysMapsEnumsAndNames() {
		Schema a = parse("""
				{"type":"record","name":"Order","namespace":"shop","fields":[
				 {"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW","PAID","LOST"]}},
				 {"name":"items","type":{"type":"array","items":{"type":"record","name":"Item","fields":[
				   {"name":"sku","type":"string"},{"name":"qty","type":"int"}]}}},
				 {"name":"tags","type":{"type":"map","values":"string"}},
				 {"name":"address","type":["null",{"type":"record","name":"Address","fields":[{"name":"city","type":"string"}]}],"default":null},
				 {"name":"ts","type":"long"},
				 {"name":"hash","type":{"type":"fixed","name":"Hash","size":16}}]}""");
		Schema b = parse("""
				{"type":"record","name":"Order","namespace":"shop.v2","aliases":["shop.Order"],"fields":[
				 {"name":"status","type":{"type":"enum","name":"Status","namespace":"shop","symbols":["NEW","PAID","SHIPPED"],"default":"NEW"}},
				 {"name":"items","type":{"type":"array","items":{"type":"record","name":"Item","namespace":"shop","fields":[
				   {"name":"sku","type":"string"},{"name":"qty","type":"long"},{"name":"note","type":"string","default":""}]}}},
				 {"name":"tags","type":{"type":"map","values":"bytes"}},
				 {"name":"address","type":["null",{"type":"record","name":"Address","namespace":"shop","fields":[{"name":"city","type":"string"},{"name":"zip","type":["null","string"],"default":null}]}],"default":null},
				 {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
				 {"name":"hash","type":{"type":"fixed","name":"Hash","namespace":"shop","size":32}}]}""");
		AvroSchemaDiff.Result r = AvroSchemaDiff.diff(a, b);
		Change ns = find(r, "Order", "NAMESPACE_CHANGED");
		assertThat(ns.from()).isEqualTo("shop");
		assertThat(ns.to()).isEqualTo("shop.v2");
		assertThat(ns.detail()).contains("alias");
		assertThat(find(r, "Order.status", "ENUM_SYMBOLS_REMOVED").from()).isEqualTo(List.of("LOST"));
		assertThat(find(r, "Order.status", "ENUM_SYMBOLS_ADDED").to()).isEqualTo(List.of("SHIPPED"));
		find(r, "Order.status", "ENUM_DEFAULT_CHANGED");
		find(r, "Order.items[].qty", "TYPE_CHANGED");
		find(r, "Order.items[].note", "FIELD_ADDED");
		assertThat(find(r, "Order.tags{}", "TYPE_CHANGED").detail()).contains("BACKWARD uyumlu");
		find(r, "Order.address.zip", "FIELD_ADDED");
		assertThat(find(r, "Order.ts", "LOGICAL_TYPE_CHANGED").to()).isEqualTo("timestamp-millis");
		assertThat(find(r, "Order.hash", "FIXED_SIZE_CHANGED").to()).isEqualTo(32);
		assertThat(r.totalChanges()).isEqualTo(r.changes().size());
	}

	@Test
	void unionConversionsRenamesAndRecursiveTypes() {
		Schema a = parse("""
				{"type":"record","name":"Node","fields":[
				 {"name":"value","type":"string"},
				 {"name":"label","type":["null","string"],"default":null},
				 {"name":"count","type":["null","int"],"default":null},
				 {"name":"next","type":["null","Node"],"default":null},
				 {"name":"title","type":"string"}]}""");
		Schema b = parse("""
				{"type":"record","name":"Node","fields":[
				 {"name":"value","type":["null","string"],"default":null},
				 {"name":"label","type":"string","default":""},
				 {"name":"count","type":["null","long"],"default":null},
				 {"name":"next","type":["null","Node"],"default":null},
				 {"name":"heading","type":"string","aliases":["title"]}]}""");
		AvroSchemaDiff.Result r = AvroSchemaDiff.diff(a, b);
		assertThat(find(r, "Node.value", "TYPE_CHANGED").detail()).contains("union'a");
		find(r, "Node.value", "DEFAULT_ADDED");
		assertThat(find(r, "Node.label", "TYPE_CHANGED").detail()).contains("tek tipe");
		Change count = find(r, "Node.count", "TYPE_CHANGED");
		assertThat(count.from()).isEqualTo("int");
		assertThat(count.to()).isEqualTo("long");
		Change renamed = find(r, "Node.heading", "FIELD_RENAMED");
		assertThat(renamed.from()).isEqualTo("title");
		assertThat(r.changes()).noneMatch(c -> c.path().startsWith("Node.next"));
		assertThat(r.backward()).isFalse();
	}

	@Test
	void unionNotesFollowTypePromotionAndAgreeWithCompatibility() {
		Schema a = parse("""
				{"type":"record","name":"P","fields":[
				 {"name":"n","type":"int"},
				 {"name":"u","type":["null","int","string"],"default":null},
				 {"name":"w","type":["int","long"]},
				 {"name":"s","type":"long"},
				 {"name":"x","type":["null","string"],"default":null}]}""");
		Schema b = parse("""
				{"type":"record","name":"P","fields":[
				 {"name":"n","type":["null","long"],"default":null},
				 {"name":"u","type":["null","long","string"],"default":null},
				 {"name":"w","type":"long"},
				 {"name":"s","type":["int","long"]},
				 {"name":"x","type":"string","default":""}]}""");
		AvroSchemaDiff.Result r = AvroSchemaDiff.diff(a, b);
		// int → ["null","long"]: int long dalına yükseltilerek okunur
		String n = find(r, "P.n", "TYPE_CHANGED").detail();
		assertThat(n).contains("union'a").contains("BACKWARD uyumlu, tip yükseltmesiyle")
			.contains("FORWARD uyumsuz: eski okuyucular null")
			.doesNotContain("BACKWARD uyumsuz");
		// union'da int → long: silinen int dalı long dalıyla okunur
		assertThat(find(r, "P.u", "UNION_BRANCH_REMOVED").detail()).contains("long dalıyla okunur")
			.contains("BACKWARD uyumlu");
		assertThat(find(r, "P.u", "UNION_BRANCH_ADDED").detail()).contains("FORWARD uyumsuz");
		// ["int","long"] → long: her iki dal da long ile okunur; long eski union'da da var
		assertThat(find(r, "P.w", "TYPE_CHANGED").detail()).contains("tek tipe")
			.contains("BACKWARD uyumlu, tip yükseltmesiyle")
			.contains("FORWARD uyumlu");
		// long → ["int","long"]: eski okuyucu (long) yeni int dalını yükselterek okur
		assertThat(find(r, "P.s", "TYPE_CHANGED").detail()).contains("BACKWARD uyumlu;")
			.contains("FORWARD uyumlu, tip yükseltmesiyle");
		// ["null","string"] → string: null ile yazılmış eski veri okunamaz
		assertThat(find(r, "P.x", "TYPE_CHANGED").detail()).contains("BACKWARD uyumsuz: null")
			.contains("FORWARD uyumlu");
		// tek BACKWARD uyumsuzluğu x alanındaki null dalı
		assertThat(r.backward()).isFalse();
		assertThat(r.backwardIssueCount()).isEqualTo(1);

		// x olmadan: alan notları Avro uyumluluk sonucuyla çelişmez (BACKWARD uyumlu, FORWARD değil)
		AvroSchemaDiff.Result promoted = AvroSchemaDiff.diff(parse("""
				{"type":"record","name":"P","fields":[
				 {"name":"n","type":"int"},
				 {"name":"u","type":["null","int","string"],"default":null},
				 {"name":"w","type":["int","long"]}]}"""), parse("""
				{"type":"record","name":"P","fields":[
				 {"name":"n","type":["null","long"],"default":null},
				 {"name":"u","type":["null","long","string"],"default":null},
				 {"name":"w","type":"long"}]}"""));
		assertThat(promoted.backward()).isTrue();
		assertThat(promoted.forward()).isFalse();
		assertThat(promoted.changes()).noneMatch(c -> c.detail().contains("BACKWARD uyumsuz"));
		assertThat(promoted.changes()).anyMatch(c -> c.detail().contains("FORWARD uyumsuz"));
	}

	@Test
	void compatibilityIssuesBeyondCapAreCounted() {
		StringBuilder fields = new StringBuilder("{\"name\":\"id\",\"type\":\"string\"}");
		for (int i = 0; i < 12; i++) {
			fields.append(",{\"name\":\"f").append(i).append("\",\"type\":\"int\"}");
		}
		Schema a = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");
		Schema b = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":[" + fields + "]}");
		AvroSchemaDiff.Result r = AvroSchemaDiff.diff(a, b);
		assertThat(r.backward()).isFalse();
		assertThat(r.backwardIssues()).hasSize(10);
		assertThat(r.backwardIssueCount()).isEqualTo(12);
		assertThat(r.forward()).isTrue();
		assertThat(r.forwardIssueCount()).isZero();
		@SuppressWarnings("unchecked")
		java.util.Map<String, Object> compat = (java.util.Map<String, Object>) r.toMap().get("compatibility");
		assertThat(compat).containsEntry("backwardIssuesOmitted", 2).doesNotContainKey("forwardIssuesOmitted");

		// sınırın altında omitted alanı yok
		java.util.Map<String, Object> small = AvroSchemaDiff.diff(a, parse("""
				{"type":"record","name":"R","fields":[{"name":"id","type":"string"},{"name":"z","type":"int"}]}"""))
			.toMap();
		assertThat(small.get("compatibility").toString()).contains("backwardIssues").doesNotContain("Omitted");
	}

	@Test
	void identicalAndCompatibleSchemas() {
		String s = """
				{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}""";
		AvroSchemaDiff.Result same = AvroSchemaDiff.diff(parse(s), parse(s));
		assertThat(same.totalChanges()).isZero();
		assertThat(same.hint()).contains("FULL");
		AvroSchemaDiff.Result added = AvroSchemaDiff.diff(parse(s), parse("""
				{"type":"record","name":"A","fields":[{"name":"x","type":"int"},{"name":"y","type":"int","default":0}]}"""));
		assertThat(added.backward()).isTrue();
		assertThat(added.forward()).isTrue();
		assertThat(added.satisfies("FULL_TRANSITIVE")).isTrue();
		assertThat(added.toMap()).containsKeys("totalChanges", "changes", "compatibility");
		AvroSchemaDiff.Result primitive = AvroSchemaDiff.diff(parse("\"int\""), parse("\"string\""));
		assertThat(primitive.changes()).singleElement().satisfies(c -> {
			assertThat(c.path()).isEqualTo("$");
			assertThat(c.kind()).isEqualTo("TYPE_CHANGED");
		});
	}

}
