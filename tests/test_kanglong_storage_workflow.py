from __future__ import annotations

from pathlib import Path

import pytest

from paired_opener.storage import SqliteRepository


def test_kanglong_run_persists_plan_metadata(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        repository.create_kanglong_run(
            {
                "run_id": "run-1",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub1"],
                "status": "chain_ready",
                "plan_version": "plan-1",
                "snapshot_bundle_id": "snap-1",
                "available_actions": ["confirm", "refresh_plan"],
                "request": {"mode": "simulation"},
                "plan": {"groups": []},
                "report": {"summary": {"group_count": 0}},
            }
        )

        stored = repository.get_kanglong_run("run-1")
    finally:
        repository.close()

    assert stored is not None
    assert stored["plan_version"] == "plan-1"
    assert stored["snapshot_bundle_id"] == "snap-1"
    assert stored["available_actions"] == ["confirm", "refresh_plan"]


def test_kanglong_events_are_incremental_and_paged(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        repository.create_kanglong_run(
            {
                "run_id": "run-1",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub1"],
                "status": "execution_starting",
            }
        )
        first = repository.add_kanglong_event("run-1", "kanglong_log", {"message_key": "a"})
        second = repository.add_kanglong_event("run-1", "kanglong_log", {"message_key": "b"})

        page = repository.list_kanglong_events("run-1", after_event_id=first, limit=1)
        latest = repository.latest_kanglong_event_id("run-1")
    finally:
        repository.close()

    assert second > first
    assert latest == second
    assert page["events"][0]["event_id"] == second
    assert page["next_after_event_id"] == second
    assert page["latest_event_id"] == second
    assert page["has_more"] is False


def test_kanglong_idempotency_reuses_same_response_and_blocks_conflict(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        created = repository.remember_kanglong_idempotency(
            key="execute-1",
            request_hash="hash-a",
            response={"status": "execution_starting"},
        )
        repeated = repository.remember_kanglong_idempotency(
            key="execute-1",
            request_hash="hash-a",
            response={"status": "ignored"},
        )
        conflict = repository.get_kanglong_idempotency("execute-1", "hash-b")
    finally:
        repository.close()

    assert created["response"]["status"] == "execution_starting"
    assert repeated["response"]["status"] == "execution_starting"
    assert conflict["conflict"] is True


def test_schema_creates_engine_version_and_ledger_tables(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        tables = {
            row["name"]
            for row in repository._connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        run_columns = {
            row["name"]
            for row in repository._connection.execute("PRAGMA table_info(kanglong_runs)").fetchall()
        }
        event_columns = {
            row["name"]
            for row in repository._connection.execute("PRAGMA table_info(kanglong_events)").fetchall()
        }
        lock_columns = {
            row["name"]
            for row in repository._connection.execute("PRAGMA table_info(kanglong_locks)").fetchall()
        }
        repository.create_kanglong_run(
            {
                "run_id": "run-engine-v2",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub1"],
                "status": "chain_ready",
            }
        )
        stored = repository.get_kanglong_run("run-engine-v2")
    finally:
        repository.close()

    assert "kanglong_ledger_baselines" in tables
    assert "kanglong_run_checkpoints" in tables
    assert "kanglong_ledger_entries" in tables
    assert "engine_version" in run_columns
    assert "checkpoint_id" in event_columns
    assert {"lease_token", "fencing_token", "worker_epoch"} <= lock_columns
    assert stored["engine_version"] == 2


def test_legacy_kanglong_run_reads_as_readonly_and_does_not_block_active_run(
    tmp_path: Path,
) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        now = "2026-06-09T00:00:00+00:00"
        repository._connection.execute(
            """
            INSERT INTO kanglong_runs (
                run_id, engine_version, symbol, main_account_id, subaccount_ids_json, status,
                request_json, plan_json, report_json, available_actions_json, progress_json,
                report_summary_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-run",
                1,
                "ETHUSDC",
                "main",
                '["sub1"]',
                "chain_ready",
                "{}",
                "{}",
                "{}",
                '["confirm"]',
                "{}",
                "{}",
                now,
                now,
            ),
        )
        legacy = repository.get_kanglong_run("legacy-run")
        active = repository.get_active_kanglong_run()
    finally:
        repository.close()

    assert legacy["engine_version"] == 1
    assert legacy["status"] == "legacy_readonly"
    assert legacy["available_actions"] == ["refresh_plan", "view_report"]
    assert active is None


def test_commit_checkpoint_inserts_events_entries_and_checkpoint_atomically(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        repository.create_kanglong_run(
            {
                "run_id": "run-checkpoint",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub1"],
                "status": "execution_starting",
            }
        )
        repository.save_kanglong_ledger_baselines(
            "run-checkpoint",
            [
                {
                    "account_id": "main",
                    "wallet_balance": "1000",
                    "available_balance": "900",
                    "equity": "1000",
                    "margin": "100",
                    "margin_deficit": "0",
                    "total_unrealized_pnl": "0",
                    "long_qty": "0",
                    "long_entry_price": "0",
                    "long_mark_price": "0",
                    "long_leverage": 75,
                    "short_qty": "0",
                    "short_entry_price": "0",
                    "short_mark_price": "0",
                    "short_leverage": 75,
                    "baseline_hash": "baseline-main",
                }
            ],
        )

        committed = repository.commit_kanglong_checkpoint(
            run_id="run-checkpoint",
            checkpoint_id=1,
            expected_previous_checkpoint_id=0,
            expected_previous_ledger_hash="genesis",
            previous_ledger_hash="genesis",
            ledger_hash="ledger-1",
            ledger_state_hash="state-1",
            ledger_entries=[
                {
                    "sequence": 1,
                    "operation_id": "op-1",
                    "account_id": "main",
                    "entry_type": "fee",
                    "asset": "USDC",
                    "amount": "-1",
                    "fee_amount": "1",
                    "fee_asset": "USDC",
                    "operation_payload_hash": "op-hash-1",
                    "payload": {"leg": "close"},
                }
            ],
            events=[
                {
                    "group_id": "group-1",
                    "round_id": "round-1",
                    "event_type": "kanglong_round_completed",
                    "payload": {"matched_qty": "1"},
                }
            ],
            status="running",
            available_actions=["pause", "stop", "view_report"],
            progress={"checkpoint_id": 1},
            report_summary={"summary_status": "running"},
            is_safe=True,
        )
        stored = repository.get_kanglong_run("run-checkpoint")
        checkpoint = repository.latest_kanglong_checkpoint("run-checkpoint")
        entries = repository.list_kanglong_ledger_entries("run-checkpoint")
        events = repository.list_kanglong_events("run-checkpoint", after_event_id=0, limit=10)["events"]
        baselines = repository.list_kanglong_ledger_baselines("run-checkpoint")
    finally:
        repository.close()

    assert committed["checkpoint_id"] == 1
    assert committed["event_ids"]
    assert checkpoint["ledger_hash"] == committed["ledger_hash"]
    assert checkpoint["ledger_state_hash"] == committed["ledger_state_hash"]
    assert checkpoint["ledger_hash"].startswith("sha256:")
    assert checkpoint["ledger_state_hash"].startswith("sha256:")
    assert checkpoint["events_high_watermark"] == committed["event_ids"][-1]
    assert entries[0]["operation_id"] == "op-1"
    assert entries[0]["fee_amount"] == "1"
    assert events[0]["checkpoint_id"] == 1
    assert stored["status"] == "running"
    assert stored["available_actions"] == ["pause", "stop", "view_report"]
    assert stored["progress"]["checkpoint_id"] == 1
    assert stored["report_summary"]["summary_status"] == "running"
    assert baselines[0]["baseline_hash"].startswith("sha256:")


def test_checkpoint_hash_chain_rejects_previous_hash_mismatch(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        repository.create_kanglong_run(
            {
                "run_id": "run-mismatch",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub1"],
                "status": "running",
            }
        )
        repository.commit_kanglong_checkpoint(
            run_id="run-mismatch",
            checkpoint_id=1,
            expected_previous_checkpoint_id=0,
            expected_previous_ledger_hash="genesis",
            previous_ledger_hash="genesis",
            ledger_hash="ledger-1",
            ledger_state_hash="state-1",
            ledger_entries=[],
            events=[],
        )

        with pytest.raises(ValueError, match="kanglong_ledger_hash_mismatch"):
            repository.commit_kanglong_checkpoint(
                run_id="run-mismatch",
                checkpoint_id=2,
                expected_previous_checkpoint_id=1,
                expected_previous_ledger_hash="wrong-ledger",
                previous_ledger_hash="wrong-ledger",
                ledger_hash="ledger-2",
                ledger_state_hash="state-2",
                ledger_entries=[],
                events=[
                    {
                        "event_type": "kanglong_should_not_commit",
                        "payload": {"bad": True},
                    }
                ],
            )
        checkpoint = repository.latest_kanglong_checkpoint("run-mismatch")
        events = repository.list_kanglong_events("run-mismatch", after_event_id=0, limit=10)["events"]
    finally:
        repository.close()

    assert checkpoint["checkpoint_id"] == 1
    assert [event["event_type"] for event in events] == []


def test_lock_lease_uses_fencing_token_and_expires(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        first = repository.acquire_kanglong_run_lease(
            run_id="run-lease",
            worker_id="worker-a",
            ttl_seconds=60,
        )
        conflict = repository.acquire_kanglong_run_lease(
            run_id="run-lease",
            worker_id="worker-b",
            ttl_seconds=60,
        )
        renewed = repository.renew_kanglong_run_lease(
            run_id="run-lease",
            lease_token=first["lease_token"],
            fencing_token=first["fencing_token"],
            ttl_seconds=60,
        )
        bad_release = repository.release_kanglong_run_lease(
            run_id="run-lease",
            lease_token="wrong",
            fencing_token=first["fencing_token"],
        )
        released = repository.release_kanglong_run_lease(
            run_id="run-lease",
            lease_token=first["lease_token"],
            fencing_token=first["fencing_token"],
        )
        second = repository.acquire_kanglong_run_lease(
            run_id="run-lease",
            worker_id="worker-b",
            ttl_seconds=60,
        )
    finally:
        repository.close()

    assert first["run_id"] == "run-lease"
    assert first["worker_id"] == "worker-a"
    assert first["lease_token"]
    assert first["fencing_token"]
    assert first["worker_epoch"] == 1
    assert conflict["conflict"] is True
    assert conflict["fencing_token"] == first["fencing_token"]
    assert renewed["lease_token"] == first["lease_token"]
    assert bad_release is False
    assert released is True
    assert second["worker_id"] == "worker-b"
    assert second["worker_epoch"] == 2


def test_account_lock_release_does_not_drop_run_lease(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        lease = repository.acquire_kanglong_run_lease(
            run_id="run-lock-mix",
            worker_id="worker-a",
            ttl_seconds=60,
        )
        repository.acquire_kanglong_locks(
            run_id="run-lock-mix",
            lock_scopes=["account:main", "account:sub1"],
            ttl_ms=60000,
        )

        repository.release_kanglong_locks("run-lock-mix")
        renewed = repository.renew_kanglong_run_lease(
            run_id="run-lock-mix",
            lease_token=lease["lease_token"],
            fencing_token=lease["fencing_token"],
            ttl_seconds=60,
        )
    finally:
        repository.close()

    assert renewed is not None
    assert renewed["worker_epoch"] == 1


def test_control_request_uses_action_version_compare_and_swap(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        repository.create_kanglong_run(
            {
                "run_id": "run-control",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub1"],
                "status": "running",
                "progress": {"action_version": 0},
            }
        )
        pause = repository.request_kanglong_control_action(
            run_id="run-control",
            action="pause",
            expected_action_version=0,
        )
        with pytest.raises(ValueError, match="kanglong_stale_action_version"):
            repository.request_kanglong_control_action(
                run_id="run-control",
                action="stop",
                expected_action_version=0,
            )
        stop = repository.request_kanglong_control_action(
            run_id="run-control",
            action="stop",
            expected_action_version=1,
        )
        not_downgraded = repository.request_kanglong_control_action(
            run_id="run-control",
            action="pause",
            expected_action_version=2,
        )
        stored = repository.get_kanglong_run("run-control")
    finally:
        repository.close()

    assert pause["status"] == "pause_pending"
    assert pause["progress"]["action_version"] == 1
    assert pause["progress"]["control_request"]["action"] == "pause"
    assert stop["status"] == "stop_pending"
    assert stop["progress"]["action_version"] == 2
    assert stop["progress"]["control_request"]["action"] == "stop"
    assert not_downgraded["status"] == "stop_pending"
    assert not_downgraded["progress"]["action_version"] == 2
    assert stored["status"] == "stop_pending"
