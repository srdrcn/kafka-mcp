package io.github.srdrcn.kafkamcp.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

/** Regexes: guarded matching stops a catastrophic regex at the deadline or on interruption. */
class RegexesTest {

	/** Polynomial backtracking (minutes) on a run of 'a's without a 'b'. */
	private static final Pattern CATASTROPHIC = Pattern.compile("(.*a){8}b");

	private static final String RUN = "a".repeat(60);

	@Test
	void matchesLikeThePattern() {
		Pattern p = Pattern.compile("err(or)?");
		assertThat(Regexes.find(p, "an error here", null)).isTrue();
		assertThat(Regexes.find(p, "all good", null)).isFalse();
		assertThat(Regexes.matches(Pattern.compile("[a-z]+-[0-9]+"), "orders-1", null)).isTrue();
		assertThat(Regexes.matches(Pattern.compile("[a-z]+"), "orders-1", null)).isFalse();
	}

	@Test
	void stopsAtTheDeadline() {
		long start = System.nanoTime();
		assertThatThrownBy(() -> Regexes.find(CATASTROPHIC, RUN, Instant.now().plusMillis(200)))
			.isInstanceOf(Regexes.RegexTimeoutException.class)
			.isInstanceOf(ToolException.class)
			.hasMessageContaining("time limit");
		assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(5_000);
	}

	@Test
	void stopsWhenTheThreadIsInterrupted() throws Exception {
		CompletableFuture<Throwable> result = new CompletableFuture<>();
		Thread t = Thread.ofVirtual().start(() -> {
			try {
				Regexes.find(CATASTROPHIC, RUN, null);
				result.complete(null);
			}
			catch (Throwable e) {
				result.complete(e);
			}
		});
		Thread.sleep(100);
		t.interrupt();
		assertThat(result.get(5, TimeUnit.SECONDS)).isInstanceOf(Regexes.RegexTimeoutException.class)
			.hasMessageContaining("cancelled");
	}

	@Test
	void toolCallFilterIsGuarded() {
		ToolCall call = new ToolCall("t", Map.of("filter", CATASTROPHIC.pattern()), null, Instant.now().plusMillis(200));
		assertThat(call.matches("aaaaaaaab")).isTrue();
		assertThatThrownBy(() -> call.matches(RUN)).isInstanceOf(Regexes.RegexTimeoutException.class);
		assertThat(call.find(Pattern.compile("x"), "axb")).isTrue();
		assertThat(call.fullMatch(Pattern.compile("x"), "axb")).isFalse();
	}

}
