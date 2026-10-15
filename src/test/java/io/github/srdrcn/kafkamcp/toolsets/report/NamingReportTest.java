package io.github.srdrcn.kafkamcp.toolsets.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;

/** report_naming_check and report_duplicate_like_topics (KRaft and ZooKeeper). */
class NamingReportTest extends ReportTestBase {

	@ParameterizedTest(name = "zk={0}")
	@ValueSource(booleans = { false, true })
	void namingCheckListsTopicsNotMatchingThePattern(boolean zk) throws Exception {
		String p = unique("rep-name");
		String good = p + ".orders.v1";
		String upper = p + "_Orders";
		String noVersion = p + ".payments";
		createTopics(zk, List.of(new NewTopic(good, 1, (short) 1), new NewTopic(upper, 1, (short) 1),
				new NewTopic(noVersion, 1, (short) 1)));
		String pattern = Pattern.quote(p) + "\\.[a-z]+\\.v[0-9]+";

		JsonNode r = ok("report_naming_check", on(zk, "pattern", pattern, "filter", Pattern.quote(p)));
		assertThat(r.at("/examined/topics").asInt()).isEqualTo(3);
		assertThat(r.get("matching").asInt()).isEqualTo(1);
		assertThat(r.get("nonMatching").asInt()).isEqualTo(2);
		assertThat(r.get("nonMatchingPct").asDouble()).isEqualTo(66.7);
		assertThat(texts(r.get("topics"), null)).containsExactly(noVersion, upper);
		assertThat(r.get("nonMatchingByFirstToken").fieldNames().next()).isEqualTo("rep");
		// a partial match is not enough: the full name must match
		JsonNode partial = ok("report_naming_check", on(zk, "pattern", "orders", "filter", Pattern.quote(p)));
		assertThat(partial.get("matching").asInt()).isZero();

		assertThat(error("report_naming_check", on(zk, "pattern", "([a-z"))).contains("Invalid regex");
	}

	@ParameterizedTest(name = "zk={0}")
	@ValueSource(booleans = { false, true })
	void duplicateLikeTopicsGroupsNormalizedCollisionsAndSimilarPairs(boolean zk) throws Exception {
		String p = unique("rep-dup");
		String v1 = p + "-orders-v1";
		String v2 = p + "_orders_v2";
		String prod = p + ".Orders.prod";
		String customer = p + "-customer";
		String typo = p + "-custmer";
		String shard1 = p + "-shard-01";
		String shard2 = p + "-shard-02";
		String unrelated = p + "-payments";
		createTopics(zk, List.of(v1, v2, prod, customer, typo, shard1, shard2, unrelated)
			.stream()
			.map(t -> new NewTopic(t, 1, (short) 1))
			.toList());

		JsonNode r = ok("report_duplicate_like_topics", on(zk, "filter", Pattern.quote(p)));
		assertThat(r.at("/examined/topics").asInt()).isEqualTo(8);
		JsonNode groups = r.at("/collisionGroups/groups");
		assertThat(groups.size()).isEqualTo(1);
		assertThat(groups.get(0).get("count").asInt()).isEqualTo(3);
		assertThat(texts(groups.get(0).get("topics"), null)).containsExactlyInAnyOrder(v1, v2, prod);
		assertThat(groups.get(0).get("normalized").asText()).endsWith(".orders");
		JsonNode pairs = r.at("/similarPairs/pairs");
		assertThat(pairs.size()).isEqualTo(1);
		assertThat(pairs.get(0).get("a").asText()).isEqualTo(typo);
		assertThat(pairs.get(0).get("b").asText()).isEqualTo(customer);
		assertThat(pairs.get(0).get("distance").asInt()).isEqualTo(1);
		assertThat(r.at("/similarPairs/numericVariants/total").asInt()).isEqualTo(1);
		assertThat(r.at("/similarPairs/numericVariants/pairs/0/a").asText()).isEqualTo(shard1);
		assertThat(r.at("/similarPairs/numericVariants/pairs/0/b").asText()).isEqualTo(shard2);
		assertThat(r.at("/similarPairs/numericVariants/truncated").asBoolean()).isFalse();
		assertThat(r.at("/similarPairs/comparisons").asLong()).isPositive();
		assertThat(r.get("rules").toString()).contains("dev|prod|stg|test|uat");
		assertThat(r.has("partial")).isFalse();
	}

	@Test
	void namingCheckRequiresPattern() {
		assertThat(error("report_naming_check", args("filter", "x"))).contains("pattern");
		JsonNode none = ok("report_naming_check", args("pattern", ".*", "filter", "^$"));
		assertThat(none.get("nonMatching").asInt()).isZero();
		assertThat(none.get("nonMatchingPct").asDouble()).isZero();
	}

}
