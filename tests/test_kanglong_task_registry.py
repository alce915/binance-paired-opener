from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from paired_opener.kanglong import task_registry as task_registry_module
from paired_opener.kanglong.task_registry import KanglongExecutionTaskRegistry
from paired_opener.storage import SqliteRepository
from tests.test_kanglong_batch_storage import _plan


class FakeExecutor:
    def __init__(self) -> None:
        self.current = 0
        self.maximum = 0

    async def run_next(self, run_id, lease_token, fencing_token):
        self.current += 1
        self.maximum = max(self.maximum, self.current)
        await asyncio.sleep(0)
        self.current -= 1
        return {"run_id": run_id, "status": "completed", "progress": {}}


class FailingExecutor:
    async def run_next(self, run_id, lease_token, fencing_token):
        raise RuntimeError("injected worker failure")


class ControlledExitExecutor:
    def __init__(self, repository: SqliteRepository, final_status: str) -> None:
        self.repository = repository
        self.final_status = final_status
        self.calls = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def run_next(self, run_id, lease_token, fencing_token):
        self.calls += 1
        if self.calls == 1:
            self.entered.set()
            await self.release.wait()
            return {"run_id": run_id, "status": "paused_by_user", "progress": {}}
        self.repository.update_kanglong_run(
            run_id,
            status=self.final_status,
            available_actions=["view_report"],
        )
        return {"run_id": run_id, "status": self.final_status, "progress": {}}


async def _wait_until(predicate, *, timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not reached before timeout")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_registry_deduplicates_run_and_shutdown_drains_tasks(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="registry-run", account_ids=("a1",))
        repository.save_batch_plan(plan, status="execution_starting")
        executor = FakeExecutor()
        registry = KanglongExecutionTaskRegistry(repository, executor)
        assert registry.start(plan.run_id) is True
        assert registry.start(plan.run_id) is False
        assert registry.active_run_ids() == {plan.run_id}
        await asyncio.sleep(0)
        await registry.aclose(grace_seconds=15)
        assert registry.active_run_ids() == set()
        assert executor.maximum == 1
    finally:
        repository.close()


@pytest.mark.asyncio
async def test_registry_refuses_new_tasks_after_shutdown(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="registry-closed", account_ids=("a1",))
        repository.save_batch_plan(plan, status="execution_starting")
        registry = KanglongExecutionTaskRegistry(repository, FakeExecutor())
        await registry.aclose(grace_seconds=0)
        assert registry.start(plan.run_id) is False
    finally:
        repository.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("control_status", "final_status"),
    [("running", "completed"), ("stop_pending", "stopped_by_user")],
)
async def test_wake_during_worker_exit_restarts_for_latest_control(
    tmp_path: Path,
    control_status: str,
    final_status: str,
) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id=f"registry-exit-wake-{control_status}", account_ids=("a1",))
        repository.save_batch_plan(plan, status="running")
        executor = ControlledExitExecutor(repository, final_status)
        registry = KanglongExecutionTaskRegistry(repository, executor)

        assert registry.start(plan.run_id) is True
        await executor.entered.wait()
        repository.update_kanglong_run(
            plan.run_id,
            status=control_status,
            available_actions=["stop", "view_report"] if control_status == "running" else ["view_report"],
        )
        assert registry.wake(plan.run_id) is True
        executor.release.set()
        await _wait_until(lambda: executor.calls == 2)
        await _wait_until(lambda: not registry.active_run_ids())
        stored = repository.get_kanglong_run(plan.run_id)
        await registry.aclose(grace_seconds=1)
    finally:
        repository.close()

    assert stored["status"] == final_status


@pytest.mark.asyncio
async def test_due_timer_wake_is_not_lost_before_worker_done_callback(tmp_path: Path) -> None:
    class DueWakeExecutor:
        def __init__(self) -> None:
            self.calls = 0

        async def run_next(self, run_id, lease_token, fencing_token):
            self.calls += 1
            if self.calls == 1:
                return {
                    "run_id": run_id,
                    "status": "running",
                    "progress": {"next_wakeup_at": (datetime.now(UTC) - timedelta(seconds=1)).isoformat()},
                }
            return {"run_id": run_id, "status": "completed", "progress": {}}

    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="registry-due-timer", account_ids=("a1",))
        repository.save_batch_plan(plan, status="running")
        executor = DueWakeExecutor()
        registry = KanglongExecutionTaskRegistry(repository, executor)

        assert registry.start(plan.run_id) is True
        await _wait_until(lambda: executor.calls == 2)
        await _wait_until(lambda: not registry.active_run_ids())
        await registry.aclose(grace_seconds=1)
    finally:
        repository.close()

    assert executor.calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("progress", "expected_status"),
    [
        ({}, "paused_plan_recheck_changed"),
        ({"batch_pending_operation": {"operation_id": "pending-leg"}}, "needs_abort_recover"),
    ],
)
async def test_worker_failure_is_persisted_as_safe_explainable_state(
    tmp_path: Path,
    monkeypatch,
    progress: dict,
    expected_status: str,
) -> None:
    monkeypatch.setattr(task_registry_module, "_FROZEN_LOCK_HEARTBEAT_SECONDS", 0.01)
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id=f"registry-failure-{expected_status}", account_ids=("a1",))
        repository.save_batch_plan(plan, status="execution_starting")
        if progress:
            repository.update_kanglong_run(
                plan.run_id,
                status="execution_starting",
                progress=progress,
            )
        registry = KanglongExecutionTaskRegistry(repository, FailingExecutor())

        assert registry.start(plan.run_id) is True
        await _wait_until(lambda: not registry.active_run_ids())
        stored = repository.get_kanglong_run(plan.run_id)
        events = repository.list_kanglong_events(plan.run_id)["events"]
        await asyncio.sleep(0.025)
        lock = repository.get_kanglong_lock("kanglong:account:a1")
        await registry.aclose(grace_seconds=1)
    finally:
        repository.close()

    assert stored["status"] == expected_status
    assert stored["progress"]["worker_failure"]["code"] == "kanglong_batch_worker_failed"
    assert stored["progress"]["worker_failure"]["error_type"] == "RuntimeError"
    assert events[-1]["event_type"] == "kanglong_batch_worker_failed"
    assert events[-1]["payload"]["next_status"] == expected_status
    assert lock["run_id"] == plan.run_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pending_status", "expected_status"),
    [("pause_pending", "paused_by_user"), ("stop_pending", "stopped_by_user")],
)
async def test_worker_status_conflict_honors_pending_operator_control(
    tmp_path: Path,
    pending_status: str,
    expected_status: str,
) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id=f"registry-control-{pending_status}", account_ids=("a1",))
        repository.save_batch_plan(plan, status=pending_status)
        repository.acquire_kanglong_locks(
            run_id=plan.run_id,
            lock_scopes=["kanglong:account:a1"],
            ttl_ms=60_000,
        )
        registry = KanglongExecutionTaskRegistry(repository, FailingExecutor())

        assert registry.start(plan.run_id) is True
        await _wait_until(lambda: not registry.active_run_ids())
        stored = repository.get_kanglong_run(plan.run_id)
        events = repository.list_kanglong_events(plan.run_id)["events"]
        lock = repository.get_kanglong_lock("kanglong:account:a1")
        await registry.aclose(grace_seconds=1)
    finally:
        repository.close()

    assert stored["status"] == expected_status
    assert "worker_failure" not in stored["progress"]
    expected_event = "kanglong_batch_paused" if pending_status == "pause_pending" else "kanglong_batch_stopped"
    assert events[-1]["event_type"] == expected_event
    if pending_status == "stop_pending":
        assert lock is None
    else:
        assert lock["run_id"] == plan.run_id


@pytest.mark.asyncio
async def test_worker_failure_retries_when_running_changes_to_pause_pending(tmp_path: Path, monkeypatch) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="registry-running-to-pause", account_ids=("a1",))
        repository.save_batch_plan(plan, status="running")
        registry = KanglongExecutionTaskRegistry(repository, FailingExecutor())
        original = repository.commit_kanglong_action
        injected = False

        def racing_commit(**kwargs):
            nonlocal injected
            if not injected and kwargs["mutation"].next_status == "paused_plan_recheck_changed":
                injected = True
                repository.update_kanglong_run(
                    plan.run_id,
                    status="pause_pending",
                    progress={"action_version": 1, "control_request": {"action": "pause"}},
                )
            return original(**kwargs)

        monkeypatch.setattr(repository, "commit_kanglong_action", racing_commit)
        registry._record_worker_failure(plan.run_id, RuntimeError("injected"))
        stored = repository.get_kanglong_run(plan.run_id)
    finally:
        repository.close()

    assert stored["status"] == "paused_by_user"
    assert stored["progress"]["action_version"] == 1
    assert "worker_failure" not in stored["progress"]


@pytest.mark.asyncio
async def test_worker_failure_retries_when_pause_pending_changes_to_stop_pending(tmp_path: Path, monkeypatch) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="registry-pause-to-stop", account_ids=("a1",))
        repository.save_batch_plan(plan, status="pause_pending")
        repository.acquire_kanglong_locks(
            run_id=plan.run_id, lock_scopes=["kanglong:account:a1"], ttl_ms=60_000,
        )
        registry = KanglongExecutionTaskRegistry(repository, FailingExecutor())
        original = repository.commit_kanglong_action
        injected = False

        def racing_commit(**kwargs):
            nonlocal injected
            if not injected and kwargs["mutation"].next_status == "paused_by_user":
                injected = True
                repository.update_kanglong_run(
                    plan.run_id,
                    status="stop_pending",
                    progress={"action_version": 2, "control_request": {"action": "stop"}},
                )
            return original(**kwargs)

        monkeypatch.setattr(repository, "commit_kanglong_action", racing_commit)
        registry._record_worker_failure(plan.run_id, RuntimeError("injected"))
        stored = repository.get_kanglong_run(plan.run_id)
        lock = repository.get_kanglong_lock("kanglong:account:a1")
    finally:
        repository.close()

    assert stored["status"] == "stopped_by_user"
    assert stored["progress"]["action_version"] == 2
    assert lock is None


@pytest.mark.asyncio
async def test_startup_recovery_retries_worker_after_previous_lease_expires(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(task_registry_module, "_FROZEN_LOCK_HEARTBEAT_SECONDS", 0.01)
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="registry-live-old-lease", account_ids=("a1",))
        repository.save_batch_plan(plan, status="execution_starting")
        old_lease = repository.acquire_kanglong_run_lease(
            run_id=plan.run_id,
            worker_id="previous-process",
            ttl_seconds=30,
        )
        expires_at = (datetime.now(UTC) + timedelta(seconds=0.06)).isoformat()
        repository._connection.execute(
            "UPDATE kanglong_locks SET expires_at = ? WHERE lease_token = ?",
            (expires_at, old_lease["lease_token"]),
        )
        repository._connection.commit()
        executor = FakeExecutor()
        registry = KanglongExecutionTaskRegistry(repository, executor)

        scheduled = await registry.initialize_startup_recovery()
        await _wait_until(lambda: executor.maximum >= 1)
        await _wait_until(lambda: not registry.active_run_ids())
        await registry.aclose(grace_seconds=1)
    finally:
        repository.close()

    assert scheduled == []
    assert executor.maximum == 1


class WaitingExecutor:
    def __init__(self) -> None:
        self.calls = 0

    async def run_next(self, run_id, lease_token, fencing_token):
        self.calls += 1
        if self.calls == 1:
            return {
                "run_id": run_id,
                "status": "running",
                "progress": {
                    "next_wakeup_at": (datetime.now(UTC) + timedelta(seconds=0.06)).isoformat(),
                },
            }
        return {"run_id": run_id, "status": "completed", "progress": {}}


@pytest.mark.asyncio
async def test_long_round_wait_keeps_frozen_lock_and_wakes_worker(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(task_registry_module, "_FROZEN_LOCK_HEARTBEAT_SECONDS", 0.01)
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="registry-wait", account_ids=("a1",))
        repository.save_batch_plan(plan, status="execution_starting")
        executor = WaitingExecutor()
        original_reacquire = repository.reacquire_kanglong_batch_locks
        reacquire_count = 0

        def counting_reacquire(run_id, *, ttl_ms):
            nonlocal reacquire_count
            reacquire_count += 1
            return original_reacquire(run_id, ttl_ms=ttl_ms)

        repository.reacquire_kanglong_batch_locks = counting_reacquire  # type: ignore[method-assign]
        registry = KanglongExecutionTaskRegistry(repository, executor)
        assert registry.start(plan.run_id) is True
        await asyncio.sleep(0.30)
        await registry.aclose(grace_seconds=1)
    finally:
        repository.close()

    assert executor.calls == 2
    assert reacquire_count >= 3


@pytest.mark.asyncio
async def test_startup_recovery_reacquires_paused_run_locks(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(task_registry_module, "_FROZEN_LOCK_HEARTBEAT_SECONDS", 0.01)
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="registry-paused", account_ids=("a1",))
        repository.save_batch_plan(plan, status="paused_by_user")
        registry = KanglongExecutionTaskRegistry(repository, FakeExecutor())

        scheduled = await registry.initialize_startup_recovery()
        await asyncio.sleep(0.025)
        lock = repository.get_kanglong_lock("kanglong:account:a1")
        await registry.aclose(grace_seconds=1)
    finally:
        repository.close()

    assert scheduled == []
    assert lock["run_id"] == plan.run_id
    assert lock["status"] == "active"


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_status", ["paused_plan_recheck_changed", "running"])
async def test_startup_recovery_keeps_locks_for_recheck_and_unsafe_checkpoint(
    tmp_path: Path,
    monkeypatch,
    initial_status: str,
) -> None:
    monkeypatch.setattr(task_registry_module, "_FROZEN_LOCK_TTL_MS", 1_000)
    monkeypatch.setattr(task_registry_module, "_FROZEN_LOCK_HEARTBEAT_SECONDS", 0.01)
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id=f"registry-protected-{initial_status}", account_ids=("a1",))
        repository.save_batch_plan(plan, status=initial_status)
        if initial_status == "running":
            repository._connection.execute(
                """
                INSERT INTO kanglong_run_checkpoints (
                    run_id, checkpoint_id, previous_ledger_hash, ledger_hash,
                    ledger_state_hash, ledger_entry_count, events_high_watermark,
                    is_safe, created_at
                ) VALUES (?, 1, 'genesis', 'unsafe-ledger', 'unsafe-state', 0, 0, 0, ?)
                """,
                (plan.run_id, datetime.now(UTC).isoformat()),
            )
            repository._connection.commit()
        registry = KanglongExecutionTaskRegistry(repository, FakeExecutor())

        scheduled = await registry.initialize_startup_recovery()
        await asyncio.sleep(1.05)
        conflict = repository.acquire_kanglong_locks(
            run_id="competing-run",
            lock_scopes=["kanglong:account:a1"],
            ttl_ms=1_000,
        )
        stored = repository.get_kanglong_run(plan.run_id)
        lock = repository.get_kanglong_lock("kanglong:account:a1")
        await registry.aclose(grace_seconds=1)
    finally:
        repository.close()

    assert scheduled == []
    assert conflict["run_id"] == plan.run_id
    assert lock["run_id"] == plan.run_id
    assert stored["status"] == (
        "needs_abort_recover" if initial_status == "running" else "paused_plan_recheck_changed"
    )
