from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from paired_opener.kanglong.models import KanglongRunStatus, available_actions_for_status


_FROZEN_LOCK_TTL_MS = 600_000
_FROZEN_LOCK_HEARTBEAT_SECONDS = 20


_BATCH_WORKER_STATUSES = {
    KanglongRunStatus.EXECUTION_STARTING.value,
    KanglongRunStatus.RUNNING.value,
    KanglongRunStatus.PAUSE_PENDING.value,
    KanglongRunStatus.STOP_PENDING.value,
}
_BATCH_TERMINAL_OR_PAUSED = {
    KanglongRunStatus.COMPLETED.value,
    KanglongRunStatus.COMPLETED_WITH_DUST_RESIDUAL.value,
    KanglongRunStatus.STOPPED_BY_USER.value,
    KanglongRunStatus.PAUSED_BY_USER.value,
    KanglongRunStatus.PAUSED_MARKET_UNSTABLE.value,
    KanglongRunStatus.PAUSED_PLAN_RECHECK_CHANGED.value,
    KanglongRunStatus.BLOCKED_PLAN_STALE.value,
    KanglongRunStatus.NEEDS_ABORT_RECOVER.value,
}
_BATCH_LOCK_HOLD_STATUSES = (_BATCH_WORKER_STATUSES | _BATCH_TERMINAL_OR_PAUSED) - {
    KanglongRunStatus.COMPLETED.value,
    KanglongRunStatus.COMPLETED_WITH_DUST_RESIDUAL.value,
    KanglongRunStatus.STOPPED_BY_USER.value,
    KanglongRunStatus.BLOCKED_PLAN_STALE.value,
}
_BATCH_PAUSED_LOCK_HOLD_STATUSES = _BATCH_LOCK_HOLD_STATUSES - _BATCH_WORKER_STATUSES


class KanglongExecutionTaskRegistry:
    def __init__(
        self,
        repository: Any,
        batch_executor: Any,
        *,
        transfer_worker: Callable[[str], Awaitable[None]] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._repository = repository
        self._batch_executor = batch_executor
        self._transfer_worker = transfer_worker
        self._logger = logger or logging.getLogger(__name__)
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._wake_handles: dict[str, asyncio.TimerHandle] = {}
        self._accepting = True
        self._shutdown_requested = asyncio.Event()

    def start(self, run_id: str) -> bool:
        if not self._accepting or run_id in self._tasks:
            return False
        if self._repository.has_live_kanglong_lease(run_id):
            return False
        handle = self._wake_handles.pop(run_id, None)
        if handle is not None:
            handle.cancel()
        stored = self._repository.get_kanglong_run(run_id)
        if stored is None:
            return False
        task = asyncio.create_task(self._worker_loop(run_id), name=f"kanglong:{run_id}")
        self._tasks[run_id] = task
        task.add_done_callback(lambda completed: self._finish(run_id, completed))
        return True

    wake = start

    def active_run_ids(self) -> set[str]:
        return set(self._tasks)

    async def initialize_startup_recovery(self) -> list[str]:
        scheduled: list[str] = []
        for stored in self._repository.list_active_kanglong_batch_runs():
            if stored["status"] not in _BATCH_LOCK_HOLD_STATUSES:
                continue
            checkpoint = self._repository.latest_kanglong_checkpoint(stored["run_id"])
            recovered = self._repository.reacquire_kanglong_batch_locks(
                stored["run_id"],
                ttl_ms=_FROZEN_LOCK_TTL_MS,
            )
            if not recovered.get("acquired"):
                self._record_lock_conflict(stored["run_id"], recovered)
                continue
            if checkpoint is not None and not bool(checkpoint.get("is_safe")):
                self._repository.update_kanglong_run(
                    stored["run_id"],
                    status=KanglongRunStatus.NEEDS_ABORT_RECOVER.value,
                    available_actions=["recover", "view_report"],
                )
                self._schedule_paused_lock_heartbeat(stored["run_id"])
                continue
            if stored["status"] in _BATCH_WORKER_STATUSES and self.start(stored["run_id"]):
                scheduled.append(stored["run_id"])
            elif stored["status"] in _BATCH_LOCK_HOLD_STATUSES:
                self._schedule_paused_lock_heartbeat(stored["run_id"])
        transfer = self._repository.get_active_kanglong_run()
        if (
            transfer is not None
            and self._transfer_worker is not None
            and transfer["status"] in _BATCH_WORKER_STATUSES
            and self.start(transfer["run_id"])
        ):
            scheduled.append(transfer["run_id"])
        return scheduled

    async def _worker_loop(self, run_id: str) -> None:
        stored = self._repository.get_kanglong_run(run_id)
        if stored is None:
            return
        if stored.get("run_kind") != "kanglong_batch":
            if self._transfer_worker is not None:
                await self._transfer_worker(run_id)
            return
        lease = self._repository.acquire_kanglong_run_lease(
            run_id=run_id,
            worker_id=f"registry-{uuid4().hex}",
            ttl_seconds=30,
        )
        if lease.get("conflict"):
            return
        try:
            while not self._shutdown_requested.is_set():
                if not self._ensure_frozen_locks(run_id):
                    self._record_lock_conflict(run_id)
                    break
                result = await self._batch_executor.run_next(
                    run_id,
                    lease["lease_token"],
                    lease["fencing_token"],
                )
                status = str(result.get("status") or "")
                if status in _BATCH_TERMINAL_OR_PAUSED or status not in _BATCH_WORKER_STATUSES:
                    break
                next_wakeup_at = (result.get("progress") or {}).get("next_wakeup_at")
                if next_wakeup_at:
                    self._schedule_wake(run_id, str(next_wakeup_at))
                    break
                await asyncio.sleep(0)
        finally:
            self._repository.release_kanglong_run_lease(
                run_id=run_id,
                lease_token=lease["lease_token"],
                fencing_token=lease["fencing_token"],
            )
            status = (self._repository.get_kanglong_run(run_id) or {}).get("status")
            if status in {
                KanglongRunStatus.COMPLETED.value,
                KanglongRunStatus.COMPLETED_WITH_DUST_RESIDUAL.value,
                KanglongRunStatus.STOPPED_BY_USER.value,
            }:
                self._repository.release_kanglong_locks(run_id)
            elif status in _BATCH_PAUSED_LOCK_HOLD_STATUSES:
                self._schedule_paused_lock_heartbeat(run_id)

    def _finish(self, run_id: str, completed: asyncio.Task[None]) -> None:
        if self._tasks.get(run_id) is completed:
            self._tasks.pop(run_id, None)
        try:
            completed.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            self._logger.error(
                "kanglong_worker_failed",
                extra={"run_id": run_id, "error_type": type(exc).__name__},
            )

    def _schedule_wake(self, run_id: str, wake_at: str) -> None:
        if not self._accepting or self._shutdown_requested.is_set():
            return
        target = datetime.fromisoformat(wake_at.replace("Z", "+00:00"))
        if target.tzinfo is None:
            target = target.replace(tzinfo=UTC)
        delay = max((target.astimezone(UTC) - datetime.now(UTC)).total_seconds(), 0)
        previous = self._wake_handles.pop(run_id, None)
        if previous is not None:
            previous.cancel()

        def wake() -> None:
            self._wake_handles.pop(run_id, None)
            if delay > _FROZEN_LOCK_HEARTBEAT_SECONDS:
                if self._ensure_frozen_locks(run_id):
                    self._schedule_wake(run_id, wake_at)
                else:
                    self._record_lock_conflict(run_id)
                return
            self.start(run_id)

        self._wake_handles[run_id] = asyncio.get_running_loop().call_later(
            min(delay, _FROZEN_LOCK_HEARTBEAT_SECONDS),
            wake,
        )

    def _ensure_frozen_locks(self, run_id: str) -> bool:
        recovered = self._repository.reacquire_kanglong_batch_locks(
            run_id,
            ttl_ms=_FROZEN_LOCK_TTL_MS,
        )
        return bool(recovered.get("acquired"))

    def _record_lock_conflict(self, run_id: str, conflict: dict[str, Any] | None = None) -> None:
        stored = self._repository.get_kanglong_run(run_id)
        if stored is None:
            return
        progress = stored.get("progress") or {}
        checkpoint = self._repository.latest_kanglong_checkpoint(run_id)
        unsafe = bool(progress.get("batch_pending_operation")) or (
            checkpoint is not None and not bool(checkpoint.get("is_safe"))
        )
        status = (
            KanglongRunStatus.NEEDS_ABORT_RECOVER.value
            if unsafe
            else KanglongRunStatus.PAUSED_PLAN_RECHECK_CHANGED.value
        )
        self._repository.update_kanglong_run(
            run_id,
            status=status,
            available_actions=available_actions_for_status(status),
            progress={
                **progress,
                "frozen_lock_conflict": {
                    "code": "kanglong_batch_lock_conflict",
                    "lock_scope": (conflict or {}).get("lock_scope"),
                    "conflicting_run_id": (conflict or {}).get("conflicting_run_id"),
                    "detected_at": datetime.now(UTC).isoformat(),
                },
            },
        )

    def _schedule_paused_lock_heartbeat(self, run_id: str) -> None:
        if not self._accepting or self._shutdown_requested.is_set():
            return
        previous = self._wake_handles.pop(run_id, None)
        if previous is not None:
            previous.cancel()

        def heartbeat() -> None:
            self._wake_handles.pop(run_id, None)
            stored = self._repository.get_kanglong_run(run_id)
            if stored is None or stored.get("status") not in _BATCH_LOCK_HOLD_STATUSES:
                return
            if self._ensure_frozen_locks(run_id):
                self._schedule_paused_lock_heartbeat(run_id)
            else:
                self._record_lock_conflict(run_id)

        self._wake_handles[run_id] = asyncio.get_running_loop().call_later(
            _FROZEN_LOCK_HEARTBEAT_SECONDS,
            heartbeat,
        )

    async def aclose(self, grace_seconds: int = 15) -> None:
        self._accepting = False
        self._shutdown_requested.set()
        for handle in self._wake_handles.values():
            handle.cancel()
        self._wake_handles.clear()
        tasks = tuple(self._tasks.values())
        if not tasks:
            return
        _, pending = await asyncio.wait(tasks, timeout=max(int(grace_seconds), 0))
        for task in pending:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class KanglongCompatibilityTaskRegistry:
    """仅用于未运行 FastAPI lifespan 的嵌入式调用和测试。"""

    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task[None]] = {}

    def start(self, run_id: str, worker: Callable[[], Awaitable[None]]) -> bool:
        if not run_id or run_id in self._tasks:
            return False
        task = asyncio.create_task(worker(), name=f"kanglong-compat:{run_id}")
        self._tasks[run_id] = task
        task.add_done_callback(lambda completed: self._finish(run_id, completed))
        return True

    def _finish(self, run_id: str, completed: asyncio.Task[None]) -> None:
        if self._tasks.get(run_id) is completed:
            self._tasks.pop(run_id, None)
        try:
            completed.result()
        except (asyncio.CancelledError, Exception):
            return
