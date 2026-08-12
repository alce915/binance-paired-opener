from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path

import pytest

from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.batch_planner import KanglongBatchPlanner
from paired_opener.storage import (
    KanglongActionMutation,
    KanglongLeaseExpectation,
    SqliteRepository,
)


def _plan(run_id: str = "batch-1", account_ids=("a3", "a1", "a2")):
    return KanglongBatchPlanner().plan_open(
        account_ids=account_ids,
        credential_revision="revision-1",
        symbol="ETHUSDC",
        preferred_side=PositionSide.LONG,
        leverage=100,
        per_leg_notional=Decimal("250000"),
        reference_price=Decimal("2000"),
        rules=SymbolRules(
            symbol="ETHUSDC",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        run_id=run_id,
    )


def test_batch_run_uses_queue_as_source_of_truth_and_populates_legacy_fields(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan()
        repository.save_batch_plan(plan)
        stored = repository.get_kanglong_run(plan.run_id)
        queue = repository.list_batch_accounts(plan.run_id)
        restored = repository.get_kanglong_batch_plan(plan.run_id)
    finally:
        repository.close()
    assert stored["run_kind"] == "kanglong_batch"
    assert stored["main_account_id"] == "a3"
    assert stored["subaccount_ids"] == ["a1", "a2"]
    assert [row["account_id"] for row in queue] == ["a3", "a1", "a2"]
    assert restored == plan


def test_existing_run_defaults_to_transfer_and_schema_adds_only_batch_queue(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        repository.create_kanglong_run(
            {
                "run_id": "transfer-1",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub"],
                "status": "chain_ready",
            }
        )
        stored = repository.get_kanglong_run("transfer-1")
        columns = {
            row["name"]
            for row in repository._connection.execute("PRAGMA table_info(kanglong_runs)").fetchall()
        }
        tables = {
            row["name"]
            for row in repository._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        repository.close()
    assert stored["run_kind"] == "transfer"
    assert "run_kind" in columns
    assert "kanglong_batch_accounts" in tables


def test_batch_plan_does_not_replace_active_transfer_lookup(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        repository.create_kanglong_run(
            {
                "run_id": "transfer-active",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub"],
                "status": "chain_ready",
            }
        )
        repository.save_batch_plan(_plan(run_id="batch-active", account_ids=("a1",)))
        transfer = repository.get_active_kanglong_run()
        batches = repository.list_active_kanglong_batch_runs()
    finally:
        repository.close()
    assert transfer["run_id"] == "transfer-active"
    assert [item["run_id"] for item in batches] == ["batch-active"]


def test_source_open_lock_conflicts_even_for_different_accounts(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        first = repository.acquire_kanglong_locks(
            run_id="close-1",
            lock_scopes=["kanglong:account:a1", "kanglong:source-open-run:open-1"],
            ttl_ms=60_000,
        )
        second = repository.acquire_kanglong_locks(
            run_id="close-2",
            lock_scopes=["kanglong:account:a2", "kanglong:source-open-run:open-1"],
            ttl_ms=60_000,
        )
        leaked = repository.get_kanglong_lock("kanglong:account:a2")
    finally:
        repository.close()
    assert first is None
    assert second is not None
    assert leaked is None


def test_execution_start_rechecks_credential_revision_before_acquiring_locks(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="batch-revision", account_ids=("a1",))
        repository.save_batch_plan(plan, status="plan_confirmed")
        result = repository.begin_kanglong_batch_execution(
            run_id=plan.run_id,
            expected_plan_version=plan.plan_version,
            current_credential_revision="revision-2",
        )
        stored = repository.get_kanglong_run(plan.run_id)
        lock = repository.get_kanglong_lock("kanglong:account:a1")
    finally:
        repository.close()
    assert result["code"] == "credential_revision_conflict"
    assert stored["status"] == "blocked_plan_stale"
    assert stored["available_actions"] == ["refresh_plan", "view_report"]
    assert lock is None


def test_execution_start_acquires_the_exact_frozen_lock_list(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="batch-locks", account_ids=("a2", "a1"))
        repository.save_batch_plan(plan, status="plan_confirmed")
        result = repository.begin_kanglong_batch_execution(
            run_id=plan.run_id,
            expected_plan_version=plan.plan_version,
            current_credential_revision=plan.credential_revision,
        )
        held = [repository.get_kanglong_lock(scope)["run_id"] for scope in plan.lock_scopes]
    finally:
        repository.close()
    assert result["started"] is True
    assert result["lock_scopes"] == list(plan.lock_scopes)
    assert held == [plan.run_id, plan.run_id]


def test_frozen_lock_heartbeat_does_not_extend_worker_lease(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="batch-heartbeat", account_ids=("a1",))
        repository.save_batch_plan(plan, status="plan_confirmed")
        repository.begin_kanglong_batch_execution(
            run_id=plan.run_id,
            expected_plan_version=plan.plan_version,
            current_credential_revision=plan.credential_revision,
            ttl_ms=1_000,
        )
        lease = repository.acquire_kanglong_run_lease(
            run_id=plan.run_id,
            worker_id="worker-heartbeat",
            ttl_seconds=60,
        )
        frozen_before = repository.get_kanglong_lock("kanglong:account:a1")
        lease_before = repository.get_kanglong_lock(f"kanglong:run:{plan.run_id}:lease")

        repository.heartbeat_kanglong_locks(run_id=plan.run_id, ttl_ms=600_000)

        frozen_after = repository.get_kanglong_lock("kanglong:account:a1")
        lease_after = repository.get_kanglong_lock(f"kanglong:run:{plan.run_id}:lease")
    finally:
        repository.close()

    assert frozen_after["expires_at"] > frozen_before["expires_at"]
    assert lease_after["expires_at"] == lease_before["expires_at"] == lease["lock_expires_at"]


def test_recovery_reacquires_frozen_source_lock_scope(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        close_plan = KanglongBatchPlanner().plan_close(
            source_open_run={
                "run_id": "open-source-recovery",
                "symbol": "ETHUSDC",
                "preferred_side": "LONG",
                "credential_revision": "revision-1",
                "accounts": [
                    {
                        "account_id": "a1",
                        "source_long_remaining_qty": "1",
                        "source_short_remaining_qty": "1",
                    }
                ],
            },
            run_id="close-recovery",
        )
        repository.save_batch_plan(close_plan, status="execution_starting")
        recovered = repository.reacquire_kanglong_batch_locks(close_plan.run_id, ttl_ms=60_000)
        source_lock = repository.get_kanglong_lock(
            "kanglong:source-open-run:open-source-recovery"
        )
    finally:
        repository.close()
    assert recovered["acquired"] is True
    assert source_lock["run_id"] == close_plan.run_id


def test_batch_cursor_is_ordered_and_rejects_stale_fencing_token(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(account_ids=("a1", "a2"))
        repository.save_batch_plan(plan)
        first_lease = repository.acquire_kanglong_run_lease(
            run_id=plan.run_id,
            worker_id="worker-1",
            ttl_seconds=60,
        )
        with pytest.raises(ValueError, match="cursor_out_of_order"):
            repository.update_kanglong_batch_account(
                run_id=plan.run_id,
                account_id="a2",
                status="first_leg",
                fencing_token=first_lease["fencing_token"],
            )
        repository.update_kanglong_batch_account(
            run_id=plan.run_id,
            account_id="a1",
            status="completed",
            fencing_token=first_lease["fencing_token"],
        )
        repository.release_kanglong_run_lease(
            run_id=plan.run_id,
            lease_token=first_lease["lease_token"],
            fencing_token=first_lease["fencing_token"],
        )
        second_lease = repository.acquire_kanglong_run_lease(
            run_id=plan.run_id,
            worker_id="worker-2",
            ttl_seconds=60,
        )
        with pytest.raises(ValueError, match="stale_fencing_token"):
            repository.update_kanglong_batch_account(
                run_id=plan.run_id,
                account_id="a2",
                status="first_leg",
                fencing_token=first_lease["fencing_token"],
            )
        updated = repository.update_kanglong_batch_account(
            run_id=plan.run_id,
            account_id="a2",
            status="first_leg",
            fencing_token=second_lease["fencing_token"],
        )
    finally:
        repository.close()
    assert updated["status"] == "first_leg"


def test_close_availability_comes_from_source_ledger_and_subtracts_prior_closes(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        source = _plan(run_id="open-source", account_ids=("a1",))
        repository.save_batch_plan(source, status="completed")
        rows = [
            ("open-source", 1, 1, "open-long", "a1", "open_position", "12.345", {"position_side": "LONG"}),
            ("open-source", 1, 2, "open-short", "a1", "open_position", "12.3445", {"position_side": "SHORT"}),
            ("close-prior", 1, 1, "close-long", "a1", "close_position", "-2", {"position_side": "LONG", "source_open_run_id": "open-source"}),
        ]
        for run_id, checkpoint_id, sequence, operation_id, account_id, entry_type, qty_delta, payload in rows:
            repository._connection.execute(
                """
                INSERT INTO kanglong_ledger_entries (
                    run_id, checkpoint_id, sequence, operation_id, account_id,
                    entry_type, qty_delta, operation_payload_hash, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    checkpoint_id,
                    sequence,
                    operation_id,
                    account_id,
                    entry_type,
                    qty_delta,
                    f"hash-{operation_id}",
                    json.dumps(payload),
                    "2026-08-12T00:00:00+00:00",
                ),
            )
        repository._connection.commit()
        availability = repository.get_kanglong_batch_source_availability("open-source", ["a1"])
    finally:
        repository.close()
    account = availability["accounts"][0]
    assert account["source_long_remaining_qty"] == Decimal("10.345")
    assert account["source_short_remaining_qty"] == Decimal("12.3445")
    assert account["target_long_qty"] == account["source_long_remaining_qty"]
    assert account["target_short_qty"] == account["source_short_remaining_qty"]


def test_resume_close_reacquires_locks_after_its_own_partial_close(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        source = _plan(run_id="open-for-resume", account_ids=("a1",))
        repository.save_batch_plan(source, status="completed")
        for sequence, side in enumerate(("LONG", "SHORT"), start=1):
            repository._connection.execute(
                """
                INSERT INTO kanglong_ledger_entries (
                    run_id, checkpoint_id, sequence, operation_id, account_id,
                    entry_type, qty_delta, operation_payload_hash, payload_json, created_at
                ) VALUES (?, 1, ?, ?, 'a1', 'open_position', '10', ?, ?, ?)
                """,
                (
                    source.run_id,
                    sequence,
                    f"open-{side.lower()}",
                    f"hash-open-{side.lower()}",
                    json.dumps({"position_side": side}),
                    "2026-08-12T00:00:00+00:00",
                ),
            )
        availability = repository.get_kanglong_batch_source_availability(source.run_id, ["a1"])
        close_plan = KanglongBatchPlanner().plan_close(
            source_open_run={
                "run_id": source.run_id,
                "symbol": source.symbol,
                "preferred_side": source.preferred_side.value,
                "credential_revision": source.credential_revision,
                "accounts": availability["accounts"],
            },
            run_id="close-resume-partial",
        )
        repository.save_batch_plan(close_plan, status="paused_by_user")
        for sequence, side in enumerate(("LONG", "SHORT"), start=1):
            repository._connection.execute(
                """
                INSERT INTO kanglong_ledger_entries (
                    run_id, checkpoint_id, sequence, operation_id, account_id,
                    entry_type, qty_delta, operation_payload_hash, payload_json, created_at
                ) VALUES (?, 1, ?, ?, 'a1', 'close_position', '-2', ?, ?, ?)
                """,
                (
                    close_plan.run_id,
                    sequence,
                    f"close-{side.lower()}",
                    f"hash-close-{side.lower()}",
                    json.dumps({"position_side": side, "source_open_run_id": source.run_id}),
                    "2026-08-12T00:01:00+00:00",
                ),
            )
        repository._connection.commit()

        resumed = repository.commit_kanglong_action(
            run_id=close_plan.run_id,
            mutation=KanglongActionMutation(
                expected_statuses=("paused_by_user",),
                expected_plan_version=close_plan.plan_version,
                expected_action_version=0,
                next_status="execution_starting",
                available_actions=("pause", "stop", "view_report"),
                acquire_frozen_locks=True,
                current_credential_revision=close_plan.credential_revision,
                lock_ttl_ms=600_000,
                increment_action_version=True,
            ),
            idempotency_key="resume-partial-close-0001",
            request_hash="resume-partial-close-hash",
            response={"run_id": close_plan.run_id},
        )
        source_lock = repository.get_kanglong_lock(f"kanglong:source-open-run:{source.run_id}")
    finally:
        repository.close()

    assert resumed["status"] == "execution_starting"
    assert source_lock["run_id"] == close_plan.run_id


def _action(plan, *, expected_status: str, next_status: str, action_version: int | None = None):
    return KanglongActionMutation(
        expected_statuses=(expected_status,),
        expected_plan_version=plan.plan_version,
        expected_action_version=action_version,
        next_status=next_status,
        available_actions=("view_report",),
        events=({"event_type": f"to_{next_status}", "payload": {}},),
        increment_action_version=True,
    )


def test_action_and_idempotency_are_atomic_and_duplicate_returns_first_response(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="action-atomic", account_ids=("a1",))
        repository.save_batch_plan(plan, status="plan_confirmed")
        mutation = _action(plan, expected_status="plan_confirmed", next_status="execution_starting")
        first = repository.commit_kanglong_action(
            run_id=plan.run_id,
            mutation=mutation,
            idempotency_key="execute-action-0001",
            request_hash="hash-a",
            response={"run_id": plan.run_id},
        )
        duplicate = repository.commit_kanglong_action(
            run_id=plan.run_id,
            mutation=mutation,
            idempotency_key="execute-action-0001",
            request_hash="hash-a",
            response={"ignored": True},
        )
        with pytest.raises(ValueError, match="idempotency_key_conflict"):
            repository.commit_kanglong_action(
                run_id=plan.run_id,
                mutation=mutation,
                idempotency_key="execute-action-0001",
                request_hash="hash-b",
                response={},
            )
        stored = repository.get_kanglong_run(plan.run_id)
        event_count = repository.latest_kanglong_event_id(plan.run_id)
    finally:
        repository.close()
    assert duplicate == first
    assert stored["progress"]["action_version"] == 1
    assert event_count == 1


def test_action_failpoint_rolls_back_run_event_and_idempotency(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="action-crash", account_ids=("a1",))
        repository.save_batch_plan(plan, status="plan_confirmed")
        repository.enable_failpoint("after_run_update_before_idempotency_insert")
        with pytest.raises(RuntimeError, match="injected crash"):
            repository.commit_kanglong_action(
                run_id=plan.run_id,
                mutation=_action(plan, expected_status="plan_confirmed", next_status="execution_starting"),
                idempotency_key="execute-crash-0001",
                request_hash="hash-crash",
                response={},
            )
        stored = repository.get_kanglong_run(plan.run_id)
        event_count = repository.latest_kanglong_event_id(plan.run_id)
        remembered = repository.get_kanglong_idempotency("execute-crash-0001", "hash-crash")
    finally:
        repository.close()
    assert stored["status"] == "plan_confirmed"
    assert event_count == 0
    assert remembered is None


def test_retry_wait_transition_rolls_back_with_run_progress_and_event(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="retry-wait-atomic", account_ids=("a1",))
        repository.save_batch_plan(plan, status="running")
        repository.enable_failpoint("after_run_update_before_idempotency_insert")
        with pytest.raises(RuntimeError, match="injected crash"):
            repository.commit_kanglong_action(
                run_id=plan.run_id,
                mutation=KanglongActionMutation(
                    expected_statuses=("running",),
                    expected_plan_version=plan.plan_version,
                    expected_action_version=None,
                    next_status="running",
                    available_actions=("pause", "stop", "view_report"),
                    progress={"next_wakeup_at": "2026-08-12T01:00:00+00:00", "transport_retry_count": 1},
                    events=({"event_type": "kanglong_batch_transport_retry", "payload": {}},),
                    batch_account_transition={
                        "account_id": "a1",
                        "status": "retry_wait",
                        "expected_status": "pending",
                    },
                ),
                idempotency_key="retry-wait-atomic-0001",
                request_hash="retry-wait-atomic-hash",
                response={"run_id": plan.run_id},
            )
        stored = repository.get_kanglong_run(plan.run_id)
        account = repository.get_kanglong_batch_account(plan.run_id, "a1")
        event_count = repository.latest_kanglong_event_id(plan.run_id)
        remembered = repository.get_kanglong_idempotency(
            "retry-wait-atomic-0001", "retry-wait-atomic-hash",
        )
    finally:
        repository.close()

    assert stored["progress"].get("next_wakeup_at") is None
    assert account["status"] == "pending"
    assert event_count == 0
    assert remembered is None


def test_worker_action_rejects_stale_fencing_token(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="worker-fence", account_ids=("a1",))
        repository.save_batch_plan(plan, status="running")
        with pytest.raises(ValueError, match="stale_fencing_token"):
            repository.commit_kanglong_action(
                run_id=plan.run_id,
                mutation=_action(plan, expected_status="running", next_status="running"),
                idempotency_key="worker-fence-0001",
                request_hash="hash-fence",
                response={},
                lease_expectation=KanglongLeaseExpectation("old-lease", "old-fence"),
            )
    finally:
        repository.close()


def test_expired_idempotency_key_can_be_reused(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        plan = _plan(run_id="expired-key", account_ids=("a1",))
        repository.save_batch_plan(plan, status="plan_confirmed")
        repository.commit_kanglong_action(
            run_id=plan.run_id,
            mutation=_action(plan, expected_status="plan_confirmed", next_status="execution_starting"),
            idempotency_key="expired-action-key",
            request_hash="old-hash",
            response={},
        )
        repository._connection.execute(
            "UPDATE kanglong_idempotency SET expires_at = ? WHERE idempotency_key = ?",
            ("2000-01-01T00:00:00+00:00", "expired-action-key"),
        )
        repository._connection.commit()
        reused = repository.commit_kanglong_action(
            run_id=plan.run_id,
            mutation=_action(plan, expected_status="execution_starting", next_status="running", action_version=1),
            idempotency_key="expired-action-key",
            request_hash="new-hash",
            response={},
        )
        count = repository.count_kanglong_idempotency("expired-action-key")
    finally:
        repository.close()
    assert reused["status"] == "running"
    assert count == 1
