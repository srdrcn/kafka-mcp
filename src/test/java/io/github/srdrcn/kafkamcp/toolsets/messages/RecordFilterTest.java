package io.github.srdrcn.kafkamcp.toolsets.messages;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.mcp.Regexes;

/** RecordFilter: the searched text is capped and a slow regex is stopped at the deadline. */
class RecordFilterTest {

	private final RecordDecoder decoder = new RecordDecoder(null, "x");

	private static ConsumerRecord<byte[], byte[]> record(String value) {
		return new ConsumerRecord<>("t", 0, 0, null, value.getBytes(StandardCharsets.UTF_8));
	}

	private RecordFilter valueFilter(String regex, Instant deadline) {
		return new RecordFilter(null, Pattern.compile(regex), null, null, decoder, MessageFormat.STRING,
				MessageFormat.STRING, deadline);
	}

	@Test
	void searchesOnlyTheStartOfLongValues() {
		String head = "x".repeat(1_000) + "NEEDLE" + "x".repeat(RecordFilter.MAX_FILTER_CHARS);
		String tail = "x".repeat(RecordFilter.MAX_FILTER_CHARS + 10) + "NEEDLE";
		RecordFilter f = valueFilter("NEEDLE", null);
		assertThat(f.test(record(head))).isTrue();
		assertThat(f.test(record(tail))).isFalse();
	}

	@Test
	void slowRegexStopsAtTheDeadline() {
		RecordFilter f = valueFilter(".*error.*timeout.*", Instant.now().plusMillis(300));
		long start = System.nanoTime();
		assertThatThrownBy(() -> f.test(record("error ".repeat(20_000))))
			.isInstanceOf(Regexes.RegexTimeoutException.class);
		assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(5_000);
	}

}
