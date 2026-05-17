from __future__ import annotations

from pathlib import Path

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
