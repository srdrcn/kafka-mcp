package io.github.srdrcn.kafkamcp.toolsets.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

class NameSimilarityTest {

	@Test
	void normalizeUnifiesSeparatorsAndStripsEnvAndVersionTokens() {
		assertThat(NameSimilarity.normalize("Payments-Orders_v2")).isEqualTo("payments.orders");
		assertThat(NameSimilarity.normalize("prod.payments.orders.v1")).isEqualTo("payments.orders");
		assertThat(NameSimilarity.normalize("payments_orders.v10.stg")).isEqualTo("payments.orders");
		assertThat(NameSimilarity.normalize("dev-orders")).isEqualTo("orders");
		// versions are only dropped at the end; "testing" is not an environment token and is kept
		assertThat(NameSimilarity.normalize("v1.orders.testing")).isEqualTo("v1.orders.testing");
		// if everything is dropped, the lower-case name
		assertThat(NameSimilarity.normalize("TEST")).isEqualTo("test");
	}

	@Test
	void collisionsGroupOnlyRealDuplicates() {
		var groups = NameSimilarity.collisions(List.of("orders-v1", "orders_v2", "ORDERS.prod", "payments", "refunds"));
		assertThat(groups).containsOnlyKeys("orders");
		assertThat(groups.get("orders")).containsExactly("ORDERS.prod", "orders-v1", "orders_v2");
	}

	@Test
	void boundedDistanceMatchesFullLevenshteinWithinTheBand() {
		Random rnd = new Random(42);
		for (int i = 0; i < 5_000; i++) {
			String a = random(rnd);
			String b = rnd.nextBoolean() ? mutate(a, rnd) : random(rnd);
			int full = levenshtein(a, b);
			int bounded = NameSimilarity.boundedDistance(a, b, 2);
			assertThat(bounded).as("%s / %s", a, b).isEqualTo(Math.min(full, 3));
		}
		assertThat(NameSimilarity.boundedDistance("customer", "custmer", 2)).isEqualTo(1);
		assertThat(NameSimilarity.boundedDistance("abc", "abcdef", 2)).isEqualTo(3);
		assertThat(NameSimilarity.boundedDistance("", "ab", 2)).isEqualTo(2);
	}

	@Test
	void similarPairsSkipCollisionsAndShortNamesAndListNumericVariantsSeparately() {
		List<String> names = List.of("orders.customer", "orders.custmer", "orders-v1", "orders_v2", "shard-01",
				"shard-02", "abc", "abd", "inventory-events", "inventory-evnts", "payments.v1.events",
				"payments.v2.events", "events-2023", "events-2024");
		NameSimilarity.Similar s = NameSimilarity.similarPairs(names, 2, 8, () -> false);
		assertThat(s.pairs()).extracting(NameSimilarity.Pair::a)
			.containsExactly("inventory-evnts", "orders.custmer");
		assertThat(s.pairs().get(1).b()).isEqualTo("orders.customer");
		assertThat(s.pairs().get(1).distance()).isEqualTo(1);
		// pairs that differ only in digits are not lost: they are listed separately by name
		assertThat(s.numericVariants()).isEqualTo(3);
		assertThat(s.numericPairs()).extracting(p -> p.a() + "/" + p.b())
			.containsExactly("events-2023/events-2024", "payments.v1.events/payments.v2.events", "shard-01/shard-02");
		assertThat(s.numericPairs()).allMatch(p -> p.distance() == 1);
		assertThat(s.partial()).isFalse();
		assertThat(s.capped()).isFalse();
	}

	@Test
	void similarPairsStopWhenAskedAndScaleToThousandsOfNames() {
		Random rnd = new Random(7);
		List<String> names = new ArrayList<>();
		for (int i = 0; i < 2_000; i++) {
			StringBuilder word = new StringBuilder();
			for (int j = 0; j < 6 + rnd.nextInt(14); j++) {
				word.append((char) ('a' + rnd.nextInt(26)));
			}
			names.add("team" + (i % 20) + "." + word + ".events");
		}
		NameSimilarity.Similar full = NameSimilarity.similarPairs(names, 2, 8, () -> false);
		assertThat(full.partial()).isFalse();
		assertThat(full.comparisons()).isPositive();
		NameSimilarity.Similar stopped = NameSimilarity.similarPairs(names, 2, 8, () -> true);
		assertThat(stopped.partial()).isTrue();
		assertThat(stopped.comparisons()).isLessThan(full.comparisons());
	}

	private static String random(Random rnd) {
		int len = 3 + rnd.nextInt(12);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			sb.append((char) ('a' + rnd.nextInt(4)));
		}
		return sb.toString();
	}

	private static String mutate(String s, Random rnd) {
		StringBuilder sb = new StringBuilder(s);
		int edits = rnd.nextInt(4);
		for (int i = 0; i < edits && sb.length() > 0; i++) {
			int pos = rnd.nextInt(sb.length());
			switch (rnd.nextInt(3)) {
				case 0 -> sb.deleteCharAt(pos);
				case 1 -> sb.insert(pos, (char) ('a' + rnd.nextInt(4)));
				default -> sb.setCharAt(pos, (char) ('a' + rnd.nextInt(4)));
			}
		}
		return sb.toString();
	}

	private static int levenshtein(String a, String b) {
		int[][] d = new int[a.length() + 1][b.length() + 1];
		for (int i = 0; i <= a.length(); i++) {
			d[i][0] = i;
		}
		for (int j = 0; j <= b.length(); j++) {
			d[0][j] = j;
		}
		for (int i = 1; i <= a.length(); i++) {
			for (int j = 1; j <= b.length(); j++) {
				int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
				d[i][j] = Math.min(d[i - 1][j - 1] + cost, Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1));
			}
		}
		return d[a.length()][b.length()];
	}

}
