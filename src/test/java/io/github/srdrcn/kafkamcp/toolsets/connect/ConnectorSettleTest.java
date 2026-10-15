package io.github.srdrcn.kafkamcp.toolsets.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import io.github.srdrcn.kafkamcp.toolsets.connect.ConnectorStatus.Task;

/**
 * Yazma araçlarının durum yoklama koşulları (ağsız birim testleri): görevsiz RUNNING connector'ın yerleşmesi,
 * güncelleme/restart sonrası çağrı öncesi (eski) durumun sonuç sayılmaması ve connector adlarının path
 * kodlaması.
 */
class ConnectorSettleTest {

	private static final String W = "connect:8083";

	private final AtomicLong clock = new AtomicLong();

	private static ConnectorStatus status(String state, Task... tasks) {
		return new ConnectorStatus("c", "sink", state, W, null, Arrays.asList(tasks));
	}

	private static Task task(int id, String state) {
		return new Task(id, state, W, ConnectorStatus.FAILED.equals(state) ? "boom\n\tat x" : null);
	}

	private void at(long millis) {
		clock.set(millis * 1_000_000L);
	}

	@Test
	void resumeSettlesZeroTaskConnectorOnlyAfterStableWindow() {
		Predicate<ConnectorStatus> p = ConnectorSettle.resumed(clock::get);
		at(0);
		assertThat(p.test(status("PAUSED"))).isFalse();
		ConnectorStatus zero = status("RUNNING");
		assertThat(p.test(zero)).isFalse();
		at(1_000);
		assertThat(p.test(zero)).isFalse();
		at(ConnectorSettle.ZERO_TASK_STABLE_MS);
		assertThat(p.test(zero)).isTrue();

		// süre kesintisiz olmalı: arada başka bir durum görülürse pencere yeniden başlar
		Predicate<ConnectorStatus> q = ConnectorSettle.resumed(clock::get);
		at(0);
		assertThat(q.test(zero)).isFalse();
		at(2_000);
		assertThat(q.test(status("PAUSED"))).isFalse();
		at(2_500);
		assertThat(q.test(zero)).isFalse();
		at(4_000);
		assertThat(q.test(zero)).isFalse();
		at(2_500 + ConnectorSettle.ZERO_TASK_STABLE_MS);
		assertThat(q.test(zero)).isTrue();
	}

	@Test
	void resumeSettlesWhenTasksRunOrFailButNotWhilePausedOrInTransition() {
		at(0);
		assertThat(ConnectorSettle.resumed(clock::get).test(status("RUNNING", task(0, "RUNNING")))).isTrue();
		assertThat(ConnectorSettle.resumed(clock::get).test(status("RUNNING", task(0, "RUNNING"), task(1, "FAILED"))))
			.isTrue();
		assertThat(ConnectorSettle.resumed(clock::get).test(status("FAILED"))).isTrue();
		assertThat(ConnectorSettle.resumed(clock::get).test(status("RUNNING", task(0, "PAUSED"), task(1, "FAILED"))))
			.isFalse();
		assertThat(ConnectorSettle.resumed(clock::get).test(status("PAUSED", task(0, "FAILED")))).isFalse();
		assertThat(ConnectorSettle.resumed(clock::get).test(status("STOPPED"))).isFalse();
		assertThat(ConnectorSettle.resumed(clock::get).test(status("RUNNING", task(0, "UNASSIGNED")))).isFalse();
	}

	@Test
	void updateDoesNotAcceptThePreUpdateStatus() {
		ConnectorStatus failed = status("RUNNING", task(0, "FAILED"));
		Predicate<ConnectorStatus> p = ConnectorSettle.applied(failed, clock::get);
		at(0);
		assertThat(p.test(failed)).isFalse();
		at(60_000);
		assertThat(p.test(status("RUNNING", task(0, "FAILED")))).isFalse();
		assertThat(p.test(status("RUNNING", task(0, "RUNNING")))).isTrue();

		// geçiş görüldükten sonra aynı FAILED durum yeni config'in sonucudur
		Predicate<ConnectorStatus> q = ConnectorSettle.applied(failed, clock::get);
		assertThat(q.test(status("RUNNING", task(0, "UNASSIGNED")))).isFalse();
		assertThat(q.test(failed)).isTrue();

		// tasks.max 1 → 2: eski tek görevli RUNNING durum kabul edilmez
		ConnectorStatus one = status("RUNNING", task(0, "RUNNING"));
		Predicate<ConnectorStatus> r = ConnectorSettle.applied(one, clock::get);
		assertThat(r.test(one)).isFalse();
		assertThat(r.test(status("RUNNING", task(0, "RUNNING"), task(1, "UNASSIGNED")))).isFalse();
		assertThat(r.test(status("RUNNING", task(0, "RUNNING"), task(1, "RUNNING")))).isTrue();
	}

	@Test
	void createSettlesOnTasksFailureOrStableZeroTasks() {
		at(0);
		assertThat(ConnectorSettle.applied(null, clock::get).test(status("RUNNING", task(0, "RUNNING")))).isTrue();
		assertThat(ConnectorSettle.applied(null, clock::get).test(status("FAILED"))).isTrue();
		assertThat(ConnectorSettle.applied(null, clock::get).test(status(null))).isFalse();
		assertThat(ConnectorSettle.applied(null, clock::get).test(status("UNASSIGNED"))).isFalse();
		Predicate<ConnectorStatus> p = ConnectorSettle.applied(null, clock::get);
		assertThat(p.test(status("RUNNING"))).isFalse();
		at(ConnectorSettle.ZERO_TASK_STABLE_MS);
		assertThat(p.test(status("RUNNING"))).isTrue();
	}

	@Test
	void restartWaitsUntilEveryPlannedItemChanged() {
		ConnectorStatus before = status("RUNNING", task(0, "FAILED"), task(1, "RUNNING"));
		Predicate<ConnectorStatus> p = ConnectorSettle.changedFrom(before, List.of(0));
		assertThat(p.test(before)).isFalse();
		assertThat(p.test(status("RUNNING", task(0, "FAILED"), task(1, "RUNNING")))).isFalse();
		assertThat(p.test(status("RUNNING", task(0, "RESTARTING"), task(1, "RUNNING")))).isTrue();
		// bir kez değişmiş görülen öğe değişmiş kalır (yeniden FAILED olsa da)
		assertThat(p.test(before)).isTrue();

		Predicate<ConnectorStatus> q = ConnectorSettle.changedFrom(before, List.of(ConnectorSettle.CONNECTOR, 0));
		assertThat(q.test(status("RESTARTING", task(0, "FAILED"), task(1, "RUNNING")))).isFalse();
		assertThat(q.test(status("RUNNING", task(0, "RUNNING"), task(1, "RUNNING")))).isTrue();

		// trace ya da worker farkı da değişimdir
		Predicate<ConnectorStatus> t = ConnectorSettle.changedFrom(before, List.of(0));
		assertThat(t.test(status("RUNNING", new Task(0, "FAILED", W, "other"), task(1, "RUNNING")))).isTrue();

		assertThat(ConnectorSettle.changedFrom(before, List.of()).test(before)).isTrue();
		assertThat(ConnectorSettle.changedFrom(null, List.of(0)).test(before)).isTrue();
	}

	@Test
	void connectorPathEncodesEveryReservedCharacter() {
		assertThat(ConnectApi.path("orders;v2")).isEqualTo("/connectors/orders%3Bv2");
		assertThat(ConnectApi.path("mcp a b")).isEqualTo("/connectors/mcp%20a%20b");
		assertThat(ConnectApi.path("x=1&y/z+q")).isEqualTo("/connectors/x%3D1%26y%2Fz%2Bq");
		assertThat(ConnectApi.path("a.b_c-d~e")).isEqualTo("/connectors/a.b_c-d~e");
	}

}
