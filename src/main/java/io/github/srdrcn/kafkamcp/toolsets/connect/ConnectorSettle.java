package io.github.srdrcn.kafkamcp.toolsets.connect;

import static io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorStatus.PAUSED;
import static io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorStatus.RESTARTING;
import static io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorStatus.RUNNING;
import static io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorStatus.STOPPED;
import static io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorStatus.UNASSIGNED;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * "Has it settled?" conditions for the status polling of the write tools. The conditions keep state between polls
 * (a new instance per tool call) and avoid two traps: taking the (old) status from before Connect applied the
 * operation as the result, and waiting for tasks of a connector that never creates any.
 */
public final class ConnectorSettle {

	/** How long a task-less RUNNING status must stay unchanged to count as permanent ("creates no tasks"). */
	public static final long ZERO_TASK_STABLE_MS = 3_000;

	/** Id standing for the connector itself in {@link #changedFrom} (task ids start at 0). */
	public static final int CONNECTOR = -1;

	private ConnectorSettle() {
	}

	/**
	 * After resume: the connector/tasks are not PAUSED/STOPPED and not in transition; and there are tasks, there is
	 * a failure, or the task-less RUNNING status stayed unchanged for {@link #ZERO_TASK_STABLE_MS}.
	 */
	public static Predicate<ConnectorStatus> resumed(LongSupplier nanoClock) {
		ZeroTasks zero = new ZeroTasks(nanoClock);
		return s -> {
			boolean zeroStable = zero.stable(s);
			return !held(s) && !s.inTransition() && (s.hasFailure() || !s.tasks().isEmpty() || zeroStable);
		};
	}

	/**
	 * After a config write: a status that is not in transition and has tasks, a failure, a state other than
	 * RUNNING, or permanently no tasks. If {@code before} (the status before the call, on update) is given, some
	 * poll must also have seen the status differ from it or be in transition; otherwise the old status would be
	 * taken as the result of the new config.
	 */
	public static Predicate<ConnectorStatus> applied(ConnectorStatus before, LongSupplier nanoClock) {
		ZeroTasks zero = new ZeroTasks(nanoClock);
		boolean[] changed = { before == null };
		return s -> {
			boolean zeroStable = zero.stable(s);
			if (s.inTransition() || !s.equals(before)) {
				changed[0] = true;
			}
			return changed[0] && !s.inTransition()
					&& (s.hasFailure() || !s.tasks().isEmpty() || !RUNNING.equals(s.state()) || zeroStable);
		};
	}

	/**
	 * True once every item in {@code ids} (a task id or {@link #CONNECTOR}) has been seen RESTARTING/UNASSIGNED in
	 * a poll, or with a state/worker/trace different from {@code before} (an item seen changed once stays changed).
	 * Immediately true if {@code before} is null.
	 */
	public static Predicate<ConnectorStatus> changedFrom(ConnectorStatus before, Collection<Integer> ids) {
		Set<Integer> pending = before == null ? new HashSet<>() : new HashSet<>(ids);
		return s -> {
			pending.removeIf(id -> changed(before, s, id));
			return pending.isEmpty();
		};
	}

	/** Is the connector or a task PAUSED/STOPPED (resume not applied yet)? */
	private static boolean held(ConnectorStatus s) {
		return PAUSED.equals(s.state()) || STOPPED.equals(s.state())
				|| s.tasks().stream().anyMatch(t -> PAUSED.equals(t.state()));
	}

	private static boolean changed(ConnectorStatus before, ConnectorStatus now, int id) {
		Item n = item(now, id);
		if (n != null && (RESTARTING.equals(n.state()) || UNASSIGNED.equals(n.state()))) {
			return true;
		}
		return !Objects.equals(item(before, id), n);
	}

	private record Item(String state, String workerId, String trace) {
	}

	private static Item item(ConnectorStatus s, int id) {
		if (id == CONNECTOR) {
			return new Item(s.state(), s.workerId(), s.trace());
		}
		return s.tasks()
			.stream()
			.filter(t -> t.id() == id)
			.findFirst()
			.map(t -> new Item(t.state(), t.workerId(), t.trace()))
			.orElse(null);
	}

	/** Tracks how long a task-less RUNNING status has stayed the same. */
	private static final class ZeroTasks {

		private final LongSupplier clock;

		private ConnectorStatus seen;

		private long since;

		ZeroTasks(LongSupplier clock) {
			this.clock = clock;
		}

		/** Processes a poll; true if the status is task-less RUNNING and unchanged for {@link #ZERO_TASK_STABLE_MS}. */
		boolean stable(ConnectorStatus s) {
			if (!RUNNING.equals(s.state()) || !s.tasks().isEmpty()) {
				seen = null;
				return false;
			}
			long now = clock.getAsLong();
			if (!s.equals(seen)) {
				seen = s;
				since = now;
			}
			return now - since >= ZERO_TASK_STABLE_MS * 1_000_000L;
		}

	}

}
