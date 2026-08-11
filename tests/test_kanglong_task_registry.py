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
