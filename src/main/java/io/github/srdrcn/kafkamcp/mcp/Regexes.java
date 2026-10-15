package io.github.srdrcn.kafkamcp.mcp;

import java.time.Instant;
import java.util.regex.Pattern;

/**
 * Regex matching that a cancelled or timed-out tool call can stop. {@code java.util.regex} never checks for
 * interruption, so a catastrophic pattern (e.g. {@code (\w+\s?)+$}) on a long text would keep a thread busy long
 * after the tool returned its timeout. The text is wrapped in a {@link CharSequence} whose {@code charAt()} checks,
 * every {@value #CHECK_INTERVAL} calls, whether the thread was interrupted or the deadline passed, and then throws
 * {@link RegexTimeoutException}.
 */
public final class Regexes {

	/** charAt() calls between two interruption/deadline checks. */
	static final int CHECK_INTERVAL = 4096;

	private Regexes() {
	}

	/** Partial match ({@code find}); {@code deadline} may be null (then only interruption stops the match). */
	public static boolean find(Pattern pattern, String text, Instant deadline) {
		return pattern.matcher(new Guarded(text, deadline)).find();
	}

	/** Full match ({@code matches}); {@code deadline} may be null. */
	public static boolean matches(Pattern pattern, String text, Instant deadline) {
		return pattern.matcher(new Guarded(text, deadline)).matches();
	}

	/** The regex match was stopped: the tool call was cancelled or its time limit was reached. */
	public static class RegexTimeoutException extends ToolException {

		public RegexTimeoutException(String message) {
			super(message);
		}

	}

	/** Read-only view of a String; checks for interruption and the deadline while the regex engine reads it. */
	private static final class Guarded implements CharSequence {

		private final String text;

		private final Instant deadline;

		private int reads;

		Guarded(String text, Instant deadline) {
			this.text = text;
			this.deadline = deadline;
		}

		@Override
		public char charAt(int index) {
			if (++reads >= CHECK_INTERVAL) {
				reads = 0;
				check();
			}
			return text.charAt(index);
		}

		private void check() {
			if (Thread.currentThread().isInterrupted()) {
				throw new RegexTimeoutException("Regex match stopped: the tool call was cancelled");
			}
			if (deadline != null && Instant.now().isAfter(deadline)) {
				throw new RegexTimeoutException(
						"Regex match stopped at the tool time limit; use a simpler or more specific regex");
			}
		}

		@Override
		public int length() {
			return text.length();
		}

		@Override
		public CharSequence subSequence(int start, int end) {
			// used for group()/toString(), not for matching
			return text.substring(start, end);
		}

		@Override
		public String toString() {
			return text;
		}

	}

}
