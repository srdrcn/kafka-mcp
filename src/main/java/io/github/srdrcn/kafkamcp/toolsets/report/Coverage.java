package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Coverage of a report: what was examined ({@code examined}), what was skipped ({@code skipped}, counts by
 * reason), whether it was sampled ({@code sampled}), whether it is incomplete because time ran out
 * ({@code partial}), errors and notes. Added to every report output with {@link #addTo(Map)} so nothing is
 * truncated silently.
 */
public final class Coverage {

	private final Map<String, Integer> examined = new LinkedHashMap<>();

	private final Map<String, Integer> skipped = new LinkedHashMap<>();

	private final Map<String, Object> errors = new LinkedHashMap<>();

	private final List<String> notes = new ArrayList<>();

	private boolean sampled;

	private boolean partial;

	/** Number of examined items (e.g. topics, partitions, groups). */
	public Coverage examined(String what, int count) {
		examined.put(what, count);
		return this;
	}

	/** Number of skipped items; omitted if 0. Repeated keys are summed. */
	public Coverage skipped(String why, int count) {
		if (count > 0) {
			skipped.merge(why, count, Integer::sum);
		}
		return this;
	}

	/** Error of a step (a short message, or an error count/map per key). */
	public Coverage error(String what, Object detail) {
		if (detail != null) {
			errors.put(what, detail);
		}
		return this;
	}

	/** Sampled because of a limit (not all items were examined). */
	public Coverage sampled() {
		sampled = true;
		return this;
	}

	/** The result is incomplete because time ran out or no response arrived. */
	public Coverage partial() {
		partial = true;
		return this;
	}

	public Coverage note(String note) {
		if (note != null && !notes.contains(note)) {
			notes.add(note);
		}
		return this;
	}

	public boolean isPartial() {
		return partial;
	}

	public boolean isSampled() {
		return sampled;
	}

	public Map<String, Integer> examinedCounts() {
		return examined;
	}

	public Map<String, Integer> skippedCounts() {
		return skipped;
	}

	/** Adds the examined, skipped, sampled, partial, errors and notes fields (empty ones omitted). */
	public void addTo(Map<String, Object> out) {
		out.put("examined", new LinkedHashMap<>(examined));
		if (!skipped.isEmpty()) {
			out.put("skipped", new LinkedHashMap<>(skipped));
		}
		if (sampled) {
			out.put("sampled", true);
		}
		if (partial) {
			out.put("partial", true);
		}
		if (!errors.isEmpty()) {
			out.put("errors", new LinkedHashMap<>(errors));
		}
		if (!notes.isEmpty()) {
			out.put("notes", List.copyOf(notes));
		}
	}

}
