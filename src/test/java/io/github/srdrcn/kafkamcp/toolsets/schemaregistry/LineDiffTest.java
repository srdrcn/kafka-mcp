package io.github.srdrcn.kafkamcp.toolsets.schemaregistry;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class LineDiffTest {

	@Test
	void reportsReplacedAddedAndRemovedLinesWithLineNumbers() {
		LineDiff.Result r = LineDiff.diff("a\nb\nc\nd", "a\nx\nc\nd\ne", 100);
		assertThat(r.lines()).containsExactly("-2: b", "+2: x", "+5: e");
		assertThat(r.added()).isEqualTo(2);
		assertThat(r.removed()).isEqualTo(1);
		assertThat(r.exact()).isTrue();
		assertThat(r.identical()).isFalse();
		assertThat(LineDiff.diff("same\ntext", "same\r\ntext", 10).identical()).isTrue();
		assertThat(LineDiff.diff("", "one", 10).lines()).containsExactly("+1: one");
	}

	@Test
	void truncatesOutputAndFallsBackForHugeInputs() {
		String a = IntStream.range(0, 50).mapToObj(i -> "a" + i).collect(Collectors.joining("\n"));
		String b = IntStream.range(0, 50).mapToObj(i -> "b" + i).collect(Collectors.joining("\n"));
		LineDiff.Result r = LineDiff.diff(a, b, 10);
		assertThat(r.lines()).hasSize(10);
		assertThat(r.truncated()).isTrue();
		assertThat(r.added() + r.removed()).isEqualTo(100);
		assertThat(r.toMap()).containsEntry("omittedLines", 90).containsEntry("truncated", true);

		String big1 = IntStream.range(0, 3000).mapToObj(i -> "line" + i).collect(Collectors.joining("\n"));
		String big2 = IntStream.range(0, 3000).mapToObj(i -> i % 2 == 0 ? "line" + i : "LINE" + i)
			.collect(Collectors.joining("\n"));
		LineDiff.Result huge = LineDiff.diff(big1, big2, 5);
		assertThat(huge.exact()).isFalse();
		assertThat(huge.added()).isEqualTo(1500);
		assertThat(huge.removed()).isEqualTo(1500);
		assertThat(huge.toMap()).containsKey("note");
	}

}
