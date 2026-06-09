from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.executor import KanglongTransferExecutor, KanglongTransferExecutorConfig
from paired_opener.kanglong.ledger import KanglongLedgerBaseline
from paired_opener.simulation_matching import (
    DeterministicMarketDataProvider,
    OrderbookLevel,
    OrderbookMatcher,
    OrderbookSnapshot,
)
from paired_opener.storage import SqliteRepository


FIXED_NOW = datetime(2026, 6, 9, 12, 0, 10, tzinfo=UTC)
FIXTURE_PATH = Path(__file__).parent / "fixtures" / "kanglong_market" / "ethusdc_rounds.json"


def rules() -> SymbolRules:
    return SymbolRules(
        symbol="ETHUSDC",
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        min_notional=Decimal("5"),
        max_leverage=125,
    )


def load_snapshots(*names: str) -> list[OrderbookSnapshot]:
    rows = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    by_name = {row["name"]: row for row in rows}
    return [_snapshot_from_fixture(by_name[name]) for name in names]


def _snapshot_from_fixture(payload: dict[str, Any]) -> OrderbookSnapshot:
    return OrderbookSnapshot(
        symbol=payload["symbol"],
        event_time=datetime.fromisoformat(payload["event_time"]),
        bids=[OrderbookLevel(price=Decimal(price), qty=Decimal(qty)) for price, qty in payload["bids"]],
        asks=[OrderbookLevel(price=Decimal(price), qty=Decimal(qty)) for price, qty in payload["asks"]],
        source=payload["name"],
    )


def _baseline(run_id: str, account_id: str) -> KanglongLedgerBaseline:
    return KanglongLedgerBaseline(
        run_id=run_id,
        account_id=account_id,
        wallet_balance=Decimal("10000"),
        available_balance=Decimal("9000"),
        equity=Decimal("10000"),
        margin=Decimal("1000"),
        margin_deficit=Decimal("0"),
        total_unrealized_pnl=Decimal("0"),
        long_qty=Decimal("10"),
        long_entry_price=Decimal("900"),
        long_mark_price=Decimal("1000"),
        long_leverage=75,
        short_qty=Decimal("0"),
        short_entry_price=Decimal("0"),
        short_mark_price=Decimal("0"),
        short_leverage=75,
    )


def create_run(
    repository: SqliteRepository,
    *,
    run_id: str = "run-1",
    status: str = "running",
    round_qtys: list[str] | None = None,
    progress: dict[str, Any] | None = None,
) -> None:
    repository.create_kanglong_run(
        {
            "run_id": run_id,
            "symbol": "ETHUSDC",
            "main_account_id": "main",
            "subaccount_ids": ["sub1"],
            "status": status,
            "available_actions": ["pause", "stop", "view_report"],
            "progress": progress or {"checkpoint_id": 0, "action_version": 0},
            "plan": {
                "symbol": "ETHUSDC",
                "direction": "long",
                "groups": [
                    {
                        "group_id": "group-0001",
                        "from_account_id": "sub1",
                        "to_account_id": "main",
                        "symbol": "ETHUSDC",
                        "side": "LONG",
                        "round_qtys": round_qtys or ["1.000"],
                    }
                ],
            },
        }
    )
    repository.save_kanglong_ledger_baselines(
        run_id,
        [_baseline(run_id, "sub1"), _baseline(run_id, "main")],
    )


def executor(
    repository: SqliteRepository,
    snapshots: list[OrderbookSnapshot],
    *,
    config: KanglongTransferExecutorConfig | None = None,
) -> KanglongTransferExecutor:
    matcher = OrderbookMatcher(maker_fee_rate=Decimal("0.0002"), taker_fee_rate=Decimal("0.0005"))
    provider = DeterministicMarketDataProvider(snapshots, now=lambda: FIXED_NOW, max_age_seconds=30)
    return KanglongTransferExecutor(
        repository=repository,
        market_data=provider,
        matcher=matcher,
        rules=rules(),
        clock=lambda: FIXED_NOW,
        config=config or KanglongTransferExecutorConfig(round_interval_seconds=3),
    )


@pytest.mark.asyncio
async def test_transfer_round_closes_source_and_opens_target_with_same_filled_qty(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        create_run(repository)
        result = await executor(repository, load_snapshots("full-close", "full-open")).run_next("run-1")
        entries = repository.list_kanglong_ledger_entries("run-1")
        events = repository.list_kanglong_events("run-1", after_event_id=0, limit=20)["events"]
        checkpoint = repository.latest_kanglong_checkpoint("run-1")
    finally:
        repository.close()

    assert result["status"] == "completed"
    assert result["matched_qty"] == "1"
    assert checkpoint["checkpoint_id"] == 1
    assert checkpoint["ledger_hash"].startswith("sha256:")
    assert [entry["entry_type"] for entry in entries[:2]] == ["close_position", "open_position"]
    assert entries[0]["account_id"] == "sub1"
    assert entries[0]["qty_delta"] == "-1"
    assert entries[1]["account_id"] == "main"
    assert entries[1]["qty_delta"] == "1"
    assert any(event["event_type"] == "kanglong_round_completed" for event in events)


@pytest.mark.asyncio
async def test_partial_second_leg_records_residual_and_supplemental_round(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        create_run(repository, round_qtys=["0.800"])
        result = await executor(repository, load_snapshots("partial-close", "partial-open")).run_next("run-1")
        stored = repository.get_kanglong_run("run-1")
        entries = repository.list_kanglong_ledger_entries("run-1")
    finally:
        repository.close()

    assert result["status"] == "running"
    assert result["matched_qty"] == "0.4"
    assert stored["progress"]["residual_qty"] == "0.4"
    assert stored["progress"]["scheduled_reason"] == "supplemental_residual"
    assert stored["progress"]["supplemental_rounds"] == 1
    assert entries[0]["qty_delta"] == "-0.4"
    assert entries[1]["qty_delta"] == "0.4"


@pytest.mark.asyncio
async def test_supplemental_round_completes_previous_residual(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        create_run(repository, round_qtys=["0.800"])
        await executor(repository, load_snapshots("partial-close", "partial-open")).run_next("run-1")
        result = await executor(repository, load_snapshots("supplemental-close", "supplemental-open")).run_next("run-1")
        stored = repository.get_kanglong_run("run-1")
        checkpoints = repository._connection.execute(
            "SELECT COUNT(*) AS count FROM kanglong_run_checkpoints WHERE run_id = ?",
            ("run-1",),
        ).fetchone()["count"]
    finally:
        repository.close()

    assert result["status"] == "completed"
    assert result["matched_qty"] == "0.4"
    assert stored["progress"]["residual_qty"] == "0"
    assert checkpoints == 2


@pytest.mark.asyncio
async def test_zero_fill_round_waits_for_interval_and_keeps_state_running(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        create_run(repository)
        result = await executor(repository, load_snapshots("zero-close")).run_next("run-1")
        stored = repository.get_kanglong_run("run-1")
        events = repository.list_kanglong_events("run-1", after_event_id=0, limit=20)["events"]
    finally:
        repository.close()

    assert result["status"] == "running"
    assert result["matched_qty"] == "0"
    assert stored["progress"]["consecutive_unfilled"] == 1
    assert stored["progress"]["scheduled_reason"] == "round_interval"
    assert stored["progress"]["next_wake_at"] == "2026-06-09T12:00:13+00:00"
    assert events[-1]["event_type"] == "kanglong_round_zero_fill"


@pytest.mark.asyncio
async def test_stale_market_data_pauses_market_unstable(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        create_run(repository)
        result = await executor(repository, load_snapshots("stale-close")).run_next("run-1")
        events = repository.list_kanglong_events("run-1", after_event_id=0, limit=20)["events"]
    finally:
        repository.close()

    assert result["status"] == "paused_market_unstable"
    assert events[-1]["event_type"] == "kanglong_market_data_stale"


@pytest.mark.asyncio
async def test_max_consecutive_unfilled_pauses_market_unstable(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        create_run(repository, progress={"checkpoint_id": 0, "consecutive_unfilled": 4})
        result = await executor(
            repository,
            load_snapshots("zero-close"),
            config=KanglongTransferExecutorConfig(max_consecutive_unfilled=5),
        ).run_next("run-1")
        stored = repository.get_kanglong_run("run-1")
    finally:
        repository.close()

    assert result["status"] == "paused_market_unstable"
    assert stored["status"] == "paused_market_unstable"
    assert stored["available_actions"] == ["resume", "stop", "recover", "view_report"]


@pytest.mark.asyncio
async def test_max_events_per_run_pauses_with_final_warning_report(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        create_run(repository)
        result = await executor(
            repository,
            load_snapshots("full-close", "full-open"),
            config=KanglongTransferExecutorConfig(max_events_per_run=0),
        ).run_next("run-1")
        stored = repository.get_kanglong_run("run-1")
        events = repository.list_kanglong_events("run-1", after_event_id=0, limit=20)["events"]
    finally:
        repository.close()

    assert result["status"] == "paused_market_unstable"
    assert stored["report_summary"]["warning_code"] == "kanglong_event_limit_reached"
    assert events[-1]["event_type"] == "kanglong_event_limit_reached"


@pytest.mark.asyncio
async def test_pause_request_stops_after_current_round_checkpoint(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        create_run(repository, status="pause_pending")
        result = await executor(repository, load_snapshots("full-close", "full-open")).run_next("run-1")
        stored = repository.get_kanglong_run("run-1")
    finally:
        repository.close()

    assert result["status"] == "paused_by_user"
    assert stored["progress"]["checkpoint_id"] == 1
    assert stored["available_actions"] == ["resume", "stop", "view_report"]


@pytest.mark.asyncio
async def test_stop_request_records_stopped_by_user_without_new_market_orders(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        create_run(repository, status="stop_pending")
        result = await executor(repository, load_snapshots("full-close", "full-open")).run_next("run-1")
        entries = repository.list_kanglong_ledger_entries("run-1")
        events = repository.list_kanglong_events("run-1", after_event_id=0, limit=20)["events"]
    finally:
        repository.close()

    assert result["status"] == "stopped_by_user"
    assert [entry["entry_type"] for entry in entries] == ["control"]
    assert events[-1]["event_type"] == "kanglong_run_stopped"


@pytest.mark.asyncio
async def test_operation_retry_is_idempotent_for_same_payload_hash(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        create_run(repository)
        transfer = executor(repository, load_snapshots("full-close", "full-open"))
        first = await transfer.run_next("run-1")
        repository.update_kanglong_run(
            "run-1",
            status="running",
            progress={"checkpoint_id": 0, "action_version": 0},
        )
        repeated = await executor(repository, load_snapshots("full-close", "full-open")).run_next("run-1")
        checkpoints = repository._connection.execute(
            "SELECT COUNT(*) AS count FROM kanglong_run_checkpoints WHERE run_id = ?",
            ("run-1",),
        ).fetchone()["count"]
    finally:
        repository.close()

    assert first["checkpoint_id"] == 1
    assert repeated["idempotent"] is True
    assert checkpoints == 1


@pytest.mark.asyncio
async def test_operation_retry_with_changed_payload_enters_needs_abort_recover(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    try:
        create_run(repository, round_qtys=["1.000"])
        await executor(repository, load_snapshots("full-close", "full-open")).run_next("run-1")
        stored = repository.get_kanglong_run("run-1")
        plan = stored["plan"]
        plan["groups"][0]["round_qtys"] = ["0.900"]
        repository.update_kanglong_run(
            "run-1",
            status="running",
            progress={"checkpoint_id": 0, "action_version": 0},
            plan=plan,
        )
        result = await executor(repository, load_snapshots("full-close", "full-open")).run_next("run-1")
        events = repository.list_kanglong_events("run-1", after_event_id=0, limit=20)["events"]
    finally:
        repository.close()

    assert result["status"] == "needs_abort_recover"
    assert events[-1]["event_type"] == "kanglong_operation_payload_mismatch"
