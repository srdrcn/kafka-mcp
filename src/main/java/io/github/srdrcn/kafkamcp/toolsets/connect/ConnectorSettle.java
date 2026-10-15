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
 * Yazma araçlarının durum yoklaması için "yerleşti mi?" koşulları. Koşullar yoklamalar arasında durum tutar
 * (her araç çağrısı için yeni örnek) ve iki tuzağı önler: Connect işlemi henüz uygulamadan önceki (eski)
 * durumu sonuç sanmak ve hiç görev üretmeyen connector için görev beklemek.
 */
public final class ConnectorSettle {

	/** Görevsiz RUNNING durumun kalıcı ("görev üretmiyor") sayılması için değişmeden görülmesi gereken süre. */
	public static final long ZERO_TASK_STABLE_MS = 3_000;

	/** {@link #changedFrom} için connector'ın kendisini temsil eden id (görev id'leri 0'dan başlar). */
	public static final int CONNECTOR = -1;

	private ConnectorSettle() {
	}

	/**
	 * Resume sonrası: connector/görevler PAUSED/STOPPED değil, geçişte değil; ve görev var, bir hata var ya da
	 * görevsiz RUNNING durum {@link #ZERO_TASK_STABLE_MS} boyunca değişmedi.
	 */
	public static Predicate<ConnectorStatus> resumed(LongSupplier nanoClock) {
		ZeroTasks zero = new ZeroTasks(nanoClock);
		return s -> {
			boolean zeroStable = zero.stable(s);
			return !held(s) && !s.inTransition() && (s.hasFailure() || !s.tasks().isEmpty() || zeroStable);
		};
	}

	/**
	 * Config yazımı sonrası: geçişte olmayan ve görevli, hatalı, RUNNING dışı ya da kalıcı görevsiz durum.
	 * {@code before} (güncellemede çağrı öncesi durum) verilirse ayrıca durumun ondan farklılaştığı ya da
	 * geçişte görüldüğü bir yoklama gerekir: aksi halde eski durum yeni config'in sonucu sanılır.
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
	 * {@code ids} içindeki her öğe (görev id'si ya da {@link #CONNECTOR}) bir yoklamada RESTARTING/UNASSIGNED
	 * görüldüyse ya da durumu/worker'ı/trace'i {@code before}'dakinden farklıysa true (bir kez görülen öğe
	 * değişmiş sayılır). {@code before} null ise hemen true.
	 */
	public static Predicate<ConnectorStatus> changedFrom(ConnectorStatus before, Collection<Integer> ids) {
		Set<Integer> pending = before == null ? new HashSet<>() : new HashSet<>(ids);
		return s -> {
			pending.removeIf(id -> changed(before, s, id));
			return pending.isEmpty();
		};
	}

	/** Connector ya da görevi PAUSED/STOPPED mı (resume henüz uygulanmamış)? */
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

	/** Görevsiz RUNNING durumun aynen sürdüğü süreyi izler. */
	private static final class ZeroTasks {

		private final LongSupplier clock;

		private ConnectorStatus seen;

		private long since;

		ZeroTasks(LongSupplier clock) {
			this.clock = clock;
		}

		/** Yoklamayı işler; durum görevsiz RUNNING ve {@link #ZERO_TASK_STABLE_MS} boyunca aynıysa true. */
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
