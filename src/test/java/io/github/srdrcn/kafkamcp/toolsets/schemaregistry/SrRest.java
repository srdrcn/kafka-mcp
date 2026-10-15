package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.srdrcn.kafkamcp.schemaregistry.SchemaRegistryClient;
import io.github.srdrcn.kafkamcp.support.SchemaRegistryTestCluster;

/** Direct REST access to Schema Registry for test setup and verification (independent of the tools). */
final class SrRest {

	/** Avro v1: id (string), amount (int). */
	static final String V1 = """
			{"type":"record","name":"Order","namespace":"com.example","fields":[
			{"name":"id","type":"string"},{"name":"amount","type":"int"}]}""";

	/** BACKWARD compatible with v1: amount int → long, currency (with a default) added. */
	static final String V2 = """
			{"type":"record","name":"Order","namespace":"com.example","fields":[
			{"name":"id","type":"string"},{"name":"amount","type":"long"},
			{"name":"currency","type":"string","default":"TRY"}]}""";

	/** Incompatible with v1/v2: id string → int, customer added without a default. */
	static final String INCOMPATIBLE = """
			{"type":"record","name":"Order","namespace":"com.example","fields":[
			{"name":"id","type":"int"},{"name":"amount","type":"long"},
			{"name":"currency","type":"string","default":"TRY"},{"name":"customer","type":"string"}]}""";

	static final String JSON_V1 = """
			{"type":"object","properties":{"a":{"type":"string"}}}""";

	static final String JSON_V2 = """
			{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}}}""";

	static final String PROTO = """
			syntax = "proto3";
			package com.example;
			message Ping {
			  string id = 1;
			}
			""";

	private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private SrRest() {
	}

	record Resp(int status, JsonNode body) {
	}

	static Resp call(String method, String path, Object body) {
		try {
			HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(SchemaRegistryTestCluster.url() + path))
				.timeout(Duration.ofSeconds(20))
				.header("Content-Type", "application/vnd.schemaregistry.v1+json")
				.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
						: HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)));
			HttpResponse<String> r = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
			return new Resp(r.statusCode(), r.body() == null || r.body().isBlank() ? null : MAPPER.readTree(r.body()));
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	static JsonNode ok(String method, String path, Object body) {
		Resp r = call(method, path, body);
		assertThat(r.status()).as("%s %s → %s", method, path, r.body()).isBetween(200, 299);
		return r.body();
	}

	static String subjectPath(String subject) {
		return "/subjects/" + SchemaRegistryClient.enc(subject);
	}

	static int register(String subject, String schema) {
		return register(subject, schema, "AVRO");
	}

	static int register(String subject, String schema, String schemaType) {
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("schema", schema);
		body.put("schemaType", schemaType);
		return ok("POST", subjectPath(subject) + "/versions", body).get("id").asInt();
	}

	/** Active versions (soft-deleted ones included if deleted=true); empty if the subject does not exist. */
	static List<Integer> versions(String subject, boolean deleted) {
		Resp r = call("GET", subjectPath(subject) + "/versions" + (deleted ? "?deleted=true" : ""), null);
		List<Integer> out = new ArrayList<>();
		if (r.status() == 404) {
			return out;
		}
		assertThat(r.status()).isEqualTo(200);
		r.body().forEach(v -> out.add(v.asInt()));
		return out;
	}

	static void softDelete(String subject, int version) {
		ok("DELETE", subjectPath(subject) + "/versions/" + version, null);
	}

	static String globalLevel() {
		return ok("GET", "/config", null).get("compatibilityLevel").asText();
	}

	static void setGlobalLevel(String level) {
		ok("PUT", "/config", Map.of("compatibility", level));
	}

	/** Subject-level compatibility; null if not configured. */
	static String subjectLevel(String subject) {
		Resp r = call("GET", "/config/" + SchemaRegistryClient.enc(subject), null);
		return r.status() == 404 ? null : r.body().get("compatibilityLevel").asText();
	}

	static void setSubjectLevel(String subject, String level) {
		ok("PUT", "/config/" + SchemaRegistryClient.enc(subject), Map.of("compatibility", level));
	}

}
