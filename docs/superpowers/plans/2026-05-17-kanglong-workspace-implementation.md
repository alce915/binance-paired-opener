# Kanglong Workspace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the independent `亢龙有悔移仓模拟` workspace with split plan/confirm/execute/events APIs, recoverable run state, compact account selection, execution logs, and cost feedback.

**Architecture:** Keep Kanglong planning and execution contracts inside `paired_opener/kanglong/`, expose workflow APIs from `paired_opener/api.py`, persist run state/events/idempotency in `SqliteRepository`, and render a third top-level frontend page separate from `real` and `simulation`. First implementation remains simulation-only and preserves the existing live/simulation execution separation.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, Decimal arithmetic, SQLite, existing account/runtime gateways, vanilla JS, existing i18n JSON registries, pytest, and node-based frontend checks.

---

## Scope Boundaries

- Work in the main checkout `D:\codex\币安自动开单系统`, per user preference.
- Build simulation-only Kanglong workflow.
- Do not place live exchange orders.
- Do not auto-execute market reduce.
- Do not allow manual release quantity in this phase.
- Do not let the frontend infer continuation/recovery permissions; render `available_actions` from the backend.
- Keep every user-visible Kanglong string in i18n files.
- Preserve existing unrelated files and do not revert user changes.

## File Structure

Create:

- `tests/test_kanglong_workflow_contracts.py`: API/service contract tests for plan, confirm, execute, active, events, recover, idempotency, and old endpoint migration.
- `tests/test_kanglong_storage_workflow.py`: repository tests for run state, event pagination, idempotency keys, and active run lookup.
- `tests/test_kanglong_snapshot_adapter.py`: snapshot conversion tests for account monitor payloads into Kanglong snapshots and `snapshot_bundle_id`.
- `tests/test_kanglong_golden_plan.py`: golden planner output and cost summary tests.
- `paired_opener/kanglong/snapshots.py`: account snapshot collector/converter for main/subaccount state used by `plan` and recheck.

Modify:

- `paired_opener/kanglong/models.py`: add workflow statuses, `snapshot_bundle_id`, event ids, action models, and payload helpers.
- `paired_opener/kanglong/service.py`: split current draft/simulate path into `create_plan`, `confirm_plan`, `execute_plan`, `active_run`, `list_events`, and `recover_run`.
- `paired_opener/kanglong/reporter.py`: enforce cost/PnL sign rules and estimated/actual summary fields.
- `paired_opener/schemas.py`: add split Kanglong request/response models.
- `paired_opener/storage.py`: extend Kanglong tables, idempotency persistence, event pagination, and active run lookup.
- `paired_opener/api.py`: add split endpoints and deprecate or wrap old `/kanglong/simulation/run`.
- `paired_opener/account_runtime.py`: expose safe temporary account gateway access for snapshot collection without switching active account.
- `paired_opener/static/index.html`: add third top-level nav/page and remove old simulation-page Kanglong panel.
- `paired_opener/static/app.js`: add `appPage = "kanglong"`, account pool, plan/confirm/execute/events workflow, logs, filters, and cost UI.
- `i18n/messages/zh-CN.json`: add workspace, account card, plan, execution, action, and log filter labels.
- `i18n/registry/events.json`: add Kanglong workflow event display contracts.
- `i18n/registry/logs.json`: add Kanglong workflow log/action display contracts.
- `i18n/registry/reasons.json`: add split workflow blocked/stale/idempotency reason codes.
- `i18n/registry/precheck.json`: add workspace precheck item labels.
- `tests/test_app_kanglong_display.mjs`: replace old panel assertions with new workspace assertions.
- `tests/test_kanglong_i18n_contracts.py`: require all new keys/registries.
- `tests/test_kanglong_api.py`: update old `/run` expectations to the split workflow.

## Verification Commands

Use these throughout the plan:

```powershell
pytest tests/test_kanglong_workflow_contracts.py -q
pytest tests/test_kanglong_storage_workflow.py -q
pytest tests/test_kanglong_snapshot_adapter.py -q
pytest tests/test_kanglong_golden_plan.py -q
pytest tests/test_kanglong_api.py tests/test_kanglong_planner.py tests/test_kanglong_simulator.py tests/test_kanglong_i18n_contracts.py -q
node tests\test_app_kanglong_display.mjs
node tests\test_app_simulation_payloads.mjs
node --check paired_opener\static\app.js
```

Memory note for frontend workers: existing simulation frontend tests have caught partial-payload reducer regressions before. When touching SSE/polling state, preserve absent fields as "no update" rather than resetting them to zero.

---

### Task 1: Add Workflow Schemas And Status Contracts

**Files:**
- Modify: `paired_opener/kanglong/models.py`
- Modify: `paired_opener/schemas.py`
- Create: `tests/test_kanglong_workflow_contracts.py`

- [ ] **Step 1: Write failing schema/status tests**

Create `tests/test_kanglong_workflow_contracts.py` with:

```python
from __future__ import annotations

from paired_opener.kanglong.models import KanglongRunStatus
from paired_opener.schemas import (
    KanglongActionRequest,
    KanglongEventsResponse,
    KanglongPlanRequest,
    KanglongPlanResponse,
)


def test_kanglong_workflow_status_values_are_stable() -> None:
    assert KanglongRunStatus.DRAFT_PLAN.value == "draft_plan"
    assert KanglongRunStatus.PLAN_CONFIRMED.value == "plan_confirmed"
    assert KanglongRunStatus.EXECUTION_STARTING.value == "execution_starting"
    assert KanglongRunStatus.BLOCKED_PLAN_STALE.value == "blocked_plan_stale"
    assert KanglongRunStatus.BLOCKED_PLAN_RECHECK_FAILED.value == "blocked_plan_recheck_failed"
    assert KanglongRunStatus.PAUSED_PLAN_RECHECK_CHANGED.value == "paused_plan_recheck_changed"


def test_plan_request_defaults_to_simulation_ethusdc_auto_side() -> None:
    request = KanglongPlanRequest(main_account_id="main", subaccount_ids=["sub1"])

    assert request.mode == "simulation"
    assert request.symbol == "ETHUSDC"
    assert request.selected_side is None


def test_action_request_requires_idempotency_key() -> None:
    request = KanglongActionRequest(plan_version="plan-1", idempotency_key="confirm-1")

    assert request.plan_version == "plan-1"
    assert request.idempotency_key == "confirm-1"


def test_plan_response_exposes_actions_and_snapshot_bundle() -> None:
    response = KanglongPlanResponse(
        run_id="run-1",
        status="chain_ready",
        plan_version="plan-1",
        snapshot_bundle_id="snap-1",
        available_actions=["confirm", "refresh_plan"],
        report={"summary": {"group_count": 2}},
    )

    assert response.snapshot_bundle_id == "snap-1"
    assert response.available_actions == ["confirm", "refresh_plan"]


def test_events_response_has_incremental_cursor_fields() -> None:
    response = KanglongEventsResponse(
        run_id="run-1",
        events=[],
        next_after_event_id=10,
        latest_event_id=10,
        has_more=False,
    )

    assert response.next_after_event_id == 10
    assert response.latest_event_id == 10
    assert response.has_more is False
```

Run: `pytest tests/test_kanglong_workflow_contracts.py -q`

Expected: FAIL because the new schema/status classes do not exist yet.

- [ ] **Step 2: Add missing Kanglong statuses**

Modify `paired_opener/kanglong/models.py`:

```python
class KanglongRunStatus(StrEnum):
    DRAFT_PLAN = "draft_plan"
    PRECHECK = "precheck"
    CHAIN_READY = "chain_ready"
    PLAN_CONFIRMED = "plan_confirmed"
    EXECUTION_STARTING = "execution_starting"
    GROUP_READY = "group_ready"
    ROUND_SIMULATED = "round_simulated"
    GROUP_COMPLETED = "group_completed"
    PLAN_ADJUSTED = "plan_adjusted"
    REBALANCE_READY = "rebalance_ready"
    COMPLETED = "completed"
    BLOCKED_MAIN_INSUFFICIENT_CAPACITY = "blocked_main_insufficient_capacity"
    BLOCKED_MAIN_NOT_FLAT = "blocked_main_not_flat"
    BLOCKED_NO_PROFITABLE_ACCOUNT = "blocked_no_profitable_account"
    BLOCKED_MANUAL_SIDE_NOT_PROFITABLE = "blocked_manual_side_not_profitable"
    BLOCKED_INITIAL_SUBACCOUNT_UNBALANCED = "blocked_initial_subaccount_unbalanced"
    BLOCKED_PLAN_STALE = "blocked_plan_stale"
    BLOCKED_PLAN_RECHECK_FAILED = "blocked_plan_recheck_failed"
    PAUSED_GROUP_NOT_EXECUTABLE = "paused_group_not_executable"
    PAUSED_PLAN_RECHECK_CHANGED = "paused_plan_recheck_changed"
    NEEDS_MARKET_REDUCE_CONFIRMATION = "needs_market_reduce_confirmation"
    NEEDS_ABORT_RECOVER = "needs_abort_recover"
    ABORT_RECOVERING = "abort_recovering"
    ABORTED_RECOVERED = "aborted_recovered"
    UNSAFE_DUST_RESIDUAL = "unsafe_dust_residual"
    UNSAFE_UNCLOSED = "unsafe_unclosed"
```

- [ ] **Step 3: Add split workflow schemas**

Modify `paired_opener/schemas.py` near the existing Kanglong models:

```python
class KanglongPlanRequest(BaseModel):
    mode: str = Field(default="simulation", pattern="^simulation$")
    symbol: str = Field(default=DEFAULT_TRADING_SYMBOL)
    main_account_id: str
    subaccount_ids: list[str] = Field(..., min_length=1)
    selected_side: PositionSide | None = None


class KanglongActionRequest(BaseModel):
    plan_version: str
    idempotency_key: str = Field(..., min_length=8, max_length=128)
    operator: str = Field(default="manual")
    confirmed_warning_codes: list[str] = Field(default_factory=list)


class KanglongRecoverRequest(BaseModel):
    idempotency_key: str = Field(..., min_length=8, max_length=128)
    operator: str = Field(default="manual")
    release_reason: str = Field(..., min_length=3, max_length=500)


class KanglongPlanResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    run_id: str
    status: str
    plan_version: str
    snapshot_bundle_id: str
    result_grade: str | None = None
    available_actions: list[str] = Field(default_factory=list)
    report: dict[str, Any] = Field(default_factory=dict)


class KanglongRunStateResponse(KanglongPlanResponse):
    confirmed_at: str | None = None
    selected_side: PositionSide | None = None
    symbol: str = Field(default=DEFAULT_TRADING_SYMBOL)
    main_account_id: str | None = None
    subaccount_ids: list[str] = Field(default_factory=list)
    current_group_id: str | None = None
    current_round_id: str | None = None
    progress: dict[str, Any] = Field(default_factory=dict)
    latest_event_id: int = 0


class KanglongEventsResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    run_id: str
    events: list[dict[str, Any]]
    next_after_event_id: int
    latest_event_id: int
    has_more: bool
```

Keep `KanglongSimulationRunRequest` and `KanglongSimulationRunResponse` temporarily for the old endpoint compatibility task.

- [ ] **Step 4: Run schema tests**

Run: `pytest tests/test_kanglong_workflow_contracts.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add paired_opener/kanglong/models.py paired_opener/schemas.py tests/test_kanglong_workflow_contracts.py
git commit -m "feat: add kanglong workflow contracts"
```

---

### Task 2: Add Repository State, Events, And Idempotency

**Files:**
- Modify: `paired_opener/storage.py`
- Create: `tests/test_kanglong_storage_workflow.py`

- [ ] **Step 1: Write failing repository tests**

Create `tests/test_kanglong_storage_workflow.py`:

```python
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
```

Run: `pytest tests/test_kanglong_storage_workflow.py -q`

Expected: FAIL because repository methods/columns do not exist.

- [ ] **Step 2: Extend Kanglong table schema**

Modify the `kanglong_runs` table in `paired_opener/storage.py` schema creation to include:

```sql
plan_version TEXT,
snapshot_bundle_id TEXT,
confirmed_at TEXT,
available_actions_json TEXT NOT NULL DEFAULT '[]',
progress_json TEXT NOT NULL DEFAULT '{}',
report_summary_json TEXT NOT NULL DEFAULT '{}'
```

Add migration-safe column creation after table creation:

```python
def _ensure_column(self, table: str, column: str, definition: str) -> None:
    rows = self._connection.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {str(row["name"]) for row in rows}
    if column not in existing:
        self._connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
```

Call:

```python
self._ensure_column("kanglong_runs", "plan_version", "TEXT")
self._ensure_column("kanglong_runs", "snapshot_bundle_id", "TEXT")
self._ensure_column("kanglong_runs", "confirmed_at", "TEXT")
self._ensure_column("kanglong_runs", "available_actions_json", "TEXT NOT NULL DEFAULT '[]'")
self._ensure_column("kanglong_runs", "progress_json", "TEXT NOT NULL DEFAULT '{}'")
self._ensure_column("kanglong_runs", "report_summary_json", "TEXT NOT NULL DEFAULT '{}'")
```

- [ ] **Step 3: Add idempotency table**

In schema creation, add:

```sql
CREATE TABLE IF NOT EXISTS kanglong_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
```

- [ ] **Step 4: Update repository serializers**

Update `create_kanglong_run`, `update_kanglong_run`, and `_deserialize_kanglong_run_row` so payloads include:

```python
"plan_version": payload.get("plan_version"),
"snapshot_bundle_id": payload.get("snapshot_bundle_id"),
"confirmed_at": payload.get("confirmed_at"),
"available_actions": payload.get("available_actions") or [],
"progress": payload.get("progress") or {},
"report_summary": payload.get("report_summary") or {},
```

Store list/dict values in the matching `*_json` columns using existing `_json_dumps`.

- [ ] **Step 5: Add event pagination methods**

Add these repository methods:

```python
def list_kanglong_events(self, run_id: str, after_event_id: int | None = None, limit: int = 200) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 1000))
    rows = self._connection.execute(
        """
        SELECT * FROM kanglong_events
        WHERE run_id = ? AND event_id > ?
        ORDER BY event_id ASC
        LIMIT ?
        """,
        (run_id, int(after_event_id or 0), bounded_limit + 1),
    ).fetchall()
    visible = rows[:bounded_limit]
    events = [self._deserialize_kanglong_event_row(row) for row in visible]
    next_after_event_id = events[-1]["event_id"] if events else int(after_event_id or 0)
    return {
        "events": events,
        "next_after_event_id": next_after_event_id,
        "latest_event_id": self.latest_kanglong_event_id(run_id),
        "has_more": len(rows) > bounded_limit,
    }


def latest_kanglong_event_id(self, run_id: str) -> int:
    row = self._connection.execute(
        "SELECT COALESCE(MAX(event_id), 0) AS latest_event_id FROM kanglong_events WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    return int(row["latest_event_id"] or 0) if row is not None else 0
```

Change `add_kanglong_event` to return `int(cursor.lastrowid)`.

- [ ] **Step 6: Add idempotency helpers**

Add:

```python
def remember_kanglong_idempotency(self, *, key: str, request_hash: str, response: dict[str, Any], expires_at: str | None = None) -> dict[str, Any]:
    existing = self._connection.execute(
        "SELECT * FROM kanglong_idempotency WHERE idempotency_key = ?",
        (key,),
    ).fetchone()
    if existing is not None:
        if existing["request_hash"] != request_hash:
            return {"conflict": True, "response": _json_load(existing["response_json"], {})}
        return {"conflict": False, "response": _json_load(existing["response_json"], {})}
    now = datetime.now(UTC).isoformat()
    expiry = expires_at or now
    with self._lock, self._connection:
        self._connection.execute(
            """
            INSERT INTO kanglong_idempotency (idempotency_key, request_hash, response_json, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (key, request_hash, _json_dumps(response), now, expiry),
        )
    return {"conflict": False, "response": response}


def get_kanglong_idempotency(self, key: str, request_hash: str) -> dict[str, Any] | None:
    row = self._connection.execute(
        "SELECT * FROM kanglong_idempotency WHERE idempotency_key = ?",
        (key,),
    ).fetchone()
    if row is None:
        return None
    return {
        "conflict": row["request_hash"] != request_hash,
        "response": _json_load(row["response_json"], {}),
    }
```

- [ ] **Step 7: Run repository tests and commit**

Run:

```powershell
pytest tests/test_kanglong_storage_workflow.py -q
```

Expected: PASS.

Commit:

```powershell
git add paired_opener/storage.py tests/test_kanglong_storage_workflow.py
git commit -m "feat: persist kanglong workflow state"
```

---

### Task 3: Add Snapshot Bundle Adapter

**Files:**
- Create: `paired_opener/kanglong/snapshots.py`
- Modify: `paired_opener/account_runtime.py`
- Create: `tests/test_kanglong_snapshot_adapter.py`

- [ ] **Step 1: Write failing snapshot adapter tests**

Create `tests/test_kanglong_snapshot_adapter.py`:

```python
from __future__ import annotations

from decimal import Decimal

from paired_opener.domain import PositionSide
from paired_opener.kanglong.snapshots import build_snapshot_bundle, monitor_account_to_kanglong_snapshot


def test_monitor_payload_converts_to_kanglong_snapshot_for_symbol() -> None:
    account = {
        "account_id": "sub1",
        "account_name": "Sub 1",
        "updated_at": "2026-05-17T00:00:00+00:00",
        "totals": {
            "available_balance": "1000",
            "equity": "1200",
            "margin": "100",
        },
        "positions": [
            {
                "symbol": "ETHUSDC",
                "position_side": "LONG",
                "position_amt": "1.5",
                "entry_price": "3000",
                "mark_price": "3100",
                "unrealized_pnl": "150",
            },
            {
                "symbol": "ETHUSDC",
                "position_side": "SHORT",
                "position_amt": "1.5",
                "entry_price": "3200",
                "mark_price": "3100",
                "unrealized_pnl": "150",
            },
        ],
    }

    snapshot = monitor_account_to_kanglong_snapshot(account, symbol="ETHUSDC", leverage=75)

    assert snapshot.account_id == "sub1"
    assert snapshot.available_balance == Decimal("1000")
    assert snapshot.qty(PositionSide.LONG) == Decimal("1.5")
    assert snapshot.pnl(PositionSide.SHORT) == Decimal("150")
    assert snapshot.snapshot_version == "sub1:2026-05-17T00:00:00+00:00"


def test_snapshot_bundle_id_is_stable_for_same_inputs() -> None:
    bundle_a = build_snapshot_bundle(
        symbol="ETHUSDC",
        accounts=[
            {
                "account_id": "sub1",
                "account_name": "Sub 1",
                "updated_at": "2026-05-17T00:00:00+00:00",
                "totals": {"available_balance": "1", "equity": "1", "margin": "0"},
                "positions": [],
            }
        ],
        config_version="cfg-1",
        symbol_rule_version="rules-1",
        price_version="price-1",
        leverage=75,
    )
    bundle_b = build_snapshot_bundle(
        symbol="ETHUSDC",
        accounts=[
            {
                "account_id": "sub1",
                "account_name": "Sub 1",
                "updated_at": "2026-05-17T00:00:00+00:00",
                "totals": {"available_balance": "1", "equity": "1", "margin": "0"},
                "positions": [],
            }
        ],
        config_version="cfg-1",
        symbol_rule_version="rules-1",
        price_version="price-1",
        leverage=75,
    )

    assert bundle_a["snapshot_bundle_id"] == bundle_b["snapshot_bundle_id"]
    assert bundle_a["accounts"][0].account_id == "sub1"
```

Run: `pytest tests/test_kanglong_snapshot_adapter.py -q`

Expected: FAIL because `snapshots.py` does not exist.

- [ ] **Step 2: Implement snapshot conversion**

Create `paired_opener/kanglong/snapshots.py`:

```python
from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any

from paired_opener.domain import PositionSide
from paired_opener.kanglong.models import KanglongAccountSnapshot, KanglongPositionSnapshot


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or "0"))


def _position_side(value: Any) -> PositionSide:
    raw = str(value or "").upper()
    return PositionSide.SHORT if raw == "SHORT" else PositionSide.LONG


def monitor_account_to_kanglong_snapshot(account: dict[str, Any], *, symbol: str, leverage: int) -> KanglongAccountSnapshot:
    positions: dict[PositionSide, KanglongPositionSnapshot] = {}
    for raw_position in account.get("positions") or []:
        if str(raw_position.get("symbol") or "").upper() != symbol.upper():
            continue
        side = _position_side(raw_position.get("position_side"))
        positions[side] = KanglongPositionSnapshot(
            symbol=symbol.upper(),
            side=side,
            qty=abs(_decimal(raw_position.get("position_amt") or raw_position.get("qty"))),
            entry_price=_decimal(raw_position.get("entry_price")),
            mark_price=_decimal(raw_position.get("mark_price")),
            unrealized_pnl=_decimal(raw_position.get("unrealized_pnl")),
        )
    updated_at = str(account.get("updated_at") or "")
    account_id = str(account.get("account_id") or "")
    totals = account.get("totals") or {}
    return KanglongAccountSnapshot(
        account_id=account_id,
        account_name=str(account.get("account_name") or account_id),
        available_balance=_decimal(totals.get("available_balance")),
        equity=_decimal(totals.get("equity")),
        margin=_decimal(totals.get("margin")),
        leverage=int(leverage),
        positions=positions,
        open_orders=list(account.get("open_orders") or []),
        snapshot_version=f"{account_id}:{updated_at}",
    )


def build_snapshot_bundle(
    *,
    symbol: str,
    accounts: list[dict[str, Any]],
    config_version: str,
    symbol_rule_version: str,
    price_version: str,
    leverage: int,
) -> dict[str, Any]:
    snapshots = [
        monitor_account_to_kanglong_snapshot(account, symbol=symbol, leverage=leverage)
        for account in accounts
    ]
    fingerprint_payload = {
        "symbol": symbol.upper(),
        "config_version": config_version,
        "symbol_rule_version": symbol_rule_version,
        "price_version": price_version,
        "versions": [snapshot.snapshot_version for snapshot in snapshots],
    }
    raw = json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":"))
    return {
        "snapshot_bundle_id": hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24],
        "accounts": snapshots,
        "fingerprint": fingerprint_payload,
    }
```

- [ ] **Step 3: Add temporary gateway helper**

Modify `paired_opener/account_runtime.py`:

```python
    def build_temporary_gateway(self, account_id: str) -> ClassifiedExchangeGateway:
        normalized = account_id.strip().lower()
        if normalized not in self._settings.accounts:
            raise ValueError(f"Unknown account {account_id}")
        return ClassifiedExchangeGateway(BinanceFuturesGateway(self._settings, self._settings.accounts[normalized]))
```

This helper does not switch the active runtime and the caller must close the returned gateway.

- [ ] **Step 4: Run snapshot tests and commit**

Run:

```powershell
pytest tests/test_kanglong_snapshot_adapter.py -q
```

Expected: PASS.

Commit:

```powershell
git add paired_opener/kanglong/snapshots.py paired_opener/account_runtime.py tests/test_kanglong_snapshot_adapter.py
git commit -m "feat: add kanglong snapshot bundle adapter"
```

---

### Task 4: Split Service Workflow

**Files:**
- Modify: `paired_opener/kanglong/service.py`
- Modify: `paired_opener/kanglong/models.py`
- Test: `tests/test_kanglong_workflow_contracts.py`
- Test: `tests/test_kanglong_api.py`

- [ ] **Step 1: Add failing service workflow test**

Append to `tests/test_kanglong_workflow_contracts.py`:

```python
from decimal import Decimal

from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.service import KanglongSimulationService
from paired_opener.storage import SqliteRepository
from tests.test_kanglong_precheck import snapshot


def test_service_plan_confirm_execute_records_state_and_events(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)
    try:
        plan = service.create_plan(
            run_id="run-1",
            symbol="ETHUSDC",
            main_snapshot=snapshot("main", "0", "0", "0", "0"),
            subaccount_snapshots=[
                snapshot("sub1", "1", "1", "100", "0"),
                snapshot("sub2", "1", "1", "80", "0"),
            ],
            main_account_id="main",
            subaccount_ids=["sub1", "sub2"],
            selected_side=PositionSide.LONG,
            snapshot_bundle_id="snap-1",
            config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
            rules=SymbolRules("ETHUSDC", Decimal("0.01"), Decimal("0.001"), Decimal("0.001"), Decimal("5"), 125),
            close_price=Decimal("3100.00"),
            open_price=Decimal("3100.50"),
            fee_rate=Decimal("0.0005"),
        )
        confirmed = service.confirm_plan(
            run_id="run-1",
            plan_version=plan["plan_version"],
            idempotency_key="confirm-0001",
            operator="tester",
            confirmed_warning_codes=[],
        )
        executed = service.execute_plan(
            run_id="run-1",
            plan_version=plan["plan_version"],
            idempotency_key="execute-0001",
            close_price=Decimal("3100.00"),
            open_price=Decimal("3100.50"),
            fee_rate=Decimal("0.0005"),
        )
        events = service.list_events("run-1", after_event_id=0, limit=10)
    finally:
        repository.close()

    assert plan["status"] == "chain_ready"
    assert "confirm" in plan["available_actions"]
    assert confirmed["status"] == "plan_confirmed"
    assert "execute" in confirmed["available_actions"]
    assert executed["status"] == "completed"
    assert events["latest_event_id"] > 0
```

Run: `pytest tests/test_kanglong_workflow_contracts.py::test_service_plan_confirm_execute_records_state_and_events -q`

Expected: FAIL because service methods are not split yet.

- [ ] **Step 2: Add service helper methods**

In `paired_opener/kanglong/service.py`, add:

```python
from hashlib import sha256
from uuid import uuid4


def _new_plan_version() -> str:
    return f"plan-{uuid4().hex}"


def _request_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(_payloadify(payload), sort_keys=True, separators=(",", ":"))
    return sha256(encoded.encode("utf-8")).hexdigest()
```

Add `import json` at the top.

- [ ] **Step 3: Implement `create_plan`**

Add a method that runs precheck/planner but does not execute events:

```python
    def create_plan(
        self,
        *,
        run_id: str,
        symbol: str,
        main_snapshot: KanglongAccountSnapshot,
        subaccount_snapshots: list[KanglongAccountSnapshot],
        main_account_id: str,
        subaccount_ids: list[str],
        selected_side: PositionSide | None,
        snapshot_bundle_id: str,
        config: KanglongSymbolConfig,
        rules: SymbolRules,
        close_price: Decimal,
        open_price: Decimal,
        fee_rate: Decimal,
    ) -> dict[str, Any]:
        self.create_draft_run(
            run_id=run_id,
            symbol=symbol,
            main_account_id=main_account_id,
            subaccount_ids=subaccount_ids,
        )
        precheck = run_static_precheck(
            main=main_snapshot,
            subaccounts=subaccount_snapshots,
            symbol=symbol,
            manual_side=selected_side,
            config=config,
        )
        plan_version = _new_plan_version()
        if not precheck.ok or precheck.selected_side is None or precheck.first_donor_account_id is None:
            payload = _blocked_payload(run_id, precheck)
            payload["plan_version"] = plan_version
            payload["snapshot_bundle_id"] = snapshot_bundle_id
            payload["available_actions"] = ["refresh_plan"]
            self._repository.update_kanglong_run(
                run_id,
                status=payload["status"],
                report=payload["report"],
                result_grade=payload["result_grade"],
                plan={"plan_version": plan_version},
                snapshot_bundle_id=snapshot_bundle_id,
                plan_version=plan_version,
                available_actions=payload["available_actions"],
            )
            return payload
        planning_accounts = build_planning_accounts(subaccount_snapshots, precheck.selected_side, config)
        plan = build_kanglong_plan(
            run_id=run_id,
            symbol=symbol,
            selected_side=precheck.selected_side,
            main_account_id=main_snapshot.account_id,
            first_donor_account_id=precheck.first_donor_account_id,
            planned_release_qty=precheck.planned_release_qty,
            accounts=planning_accounts,
            config=config,
        )
        estimate_events = []
        estimate_residuals = []
        for group in plan.groups:
            result = simulate_group(
                run_id=run_id,
                group=group,
                rules=rules,
                close_price=close_price,
                open_price=open_price,
                fee_rate=fee_rate,
                config=config,
            )
            estimate_events.extend(result.events)
            estimate_residuals.extend(result.residual_ledger)
        costs = summarize_costs(estimate_events, estimate_residuals)
        plan_payload = {
            "plan_version": plan_version,
            "snapshot_bundle_id": snapshot_bundle_id,
            "selected_side": precheck.selected_side.value,
            "groups": [_group_payload(group) for group in plan.groups],
            "batch_debt_buffers": _payloadify(plan.batch_debt_buffers),
        }
        report = {
            "summary": {
                "selected_side": precheck.selected_side.value,
                "group_count": len(plan.groups),
                "round_count": sum(len(group.round_qtys) for group in plan.groups),
                "planned_release_qty": decimal_text(precheck.planned_release_qty),
            },
            "plan": plan_payload,
            "costs": _payloadify(costs),
            "other_side_preview": _payloadify(precheck.other_side_preview),
            "warnings": [],
            "blocks": [],
        }
        payload = {
            "run_id": run_id,
            "status": KanglongRunStatus.CHAIN_READY.value,
            "plan_version": plan_version,
            "snapshot_bundle_id": snapshot_bundle_id,
            "available_actions": ["confirm", "refresh_plan"],
            "report": report,
        }
        self._repository.update_kanglong_run(
            run_id,
            status=payload["status"],
            plan=plan_payload,
            report=report,
            plan_version=plan_version,
            snapshot_bundle_id=snapshot_bundle_id,
            available_actions=payload["available_actions"],
            report_summary=report["summary"],
        )
        return payload
```

Also import `decimal_text`.

- [ ] **Step 4: Implement `confirm_plan`**

Add:

```python
    def confirm_plan(
        self,
        *,
        run_id: str,
        plan_version: str,
        idempotency_key: str,
        operator: str,
        confirmed_warning_codes: list[str],
    ) -> dict[str, Any]:
        request_hash = _request_hash(
            {
                "action": "confirm",
                "run_id": run_id,
                "plan_version": plan_version,
                "operator": operator,
                "confirmed_warning_codes": confirmed_warning_codes,
            }
        )
        existing = self._repository.get_kanglong_idempotency(idempotency_key, request_hash)
        if existing is not None:
            if existing["conflict"]:
                return {"run_id": run_id, "status": "idempotency_conflict", "available_actions": ["refresh_plan"]}
            return existing["response"]
        run = self._repository.get_kanglong_run(run_id)
        if run is None:
            return {"run_id": run_id, "status": "kanglong_run_not_found", "available_actions": ["refresh_plan"]}
        if run.get("plan_version") != plan_version:
            return {"run_id": run_id, "status": KanglongRunStatus.BLOCKED_PLAN_STALE.value, "available_actions": ["refresh_plan"]}
        payload = {
            "run_id": run_id,
            "status": KanglongRunStatus.PLAN_CONFIRMED.value,
            "plan_version": plan_version,
            "snapshot_bundle_id": run.get("snapshot_bundle_id") or "",
            "available_actions": ["execute", "refresh_plan"],
            "report": run.get("report") or {},
        }
        self._repository.update_kanglong_run(
            run_id,
            status=payload["status"],
            available_actions=payload["available_actions"],
            confirmed_at=_now_text(),
        )
        return self._repository.remember_kanglong_idempotency(
            key=idempotency_key,
            request_hash=request_hash,
            response=payload,
        )["response"]
```

- [ ] **Step 5: Implement `execute_plan` and `list_events`**

Add:

```python
    def execute_plan(
        self,
        *,
        run_id: str,
        plan_version: str,
        idempotency_key: str,
        close_price: Decimal,
        open_price: Decimal,
        fee_rate: Decimal,
    ) -> dict[str, Any]:
        request_hash = _request_hash(
            {
                "action": "execute",
                "run_id": run_id,
                "plan_version": plan_version,
                "close_price": close_price,
                "open_price": open_price,
                "fee_rate": fee_rate,
            }
        )
        existing = self._repository.get_kanglong_idempotency(idempotency_key, request_hash)
        if existing is not None:
            if existing["conflict"]:
                return {"run_id": run_id, "status": "idempotency_conflict", "available_actions": ["refresh_plan"]}
            return existing["response"]
        run = self._repository.get_kanglong_run(run_id)
        if run is None:
            return {"run_id": run_id, "status": "kanglong_run_not_found", "available_actions": ["refresh_plan"]}
        if run.get("status") != KanglongRunStatus.PLAN_CONFIRMED.value or run.get("plan_version") != plan_version:
            return {"run_id": run_id, "status": KanglongRunStatus.BLOCKED_PLAN_STALE.value, "available_actions": ["refresh_plan"]}
        plan = run.get("plan") or {}
        groups = plan.get("groups") or []
        self._repository.update_kanglong_run(run_id, status=KanglongRunStatus.EXECUTION_STARTING.value, available_actions=[])
        for group_payload in groups:
            self._repository.add_kanglong_event(
                run_id,
                "kanglong_group_simulated",
                {
                    "message_key": "events.kanglong.group_simulated",
                    "group_id": group_payload.get("group_id"),
                    "plan_version": plan_version,
                },
                group_id=group_payload.get("group_id"),
            )
        final_payload = {
            "run_id": run_id,
            "status": KanglongRunStatus.COMPLETED.value,
            "result_grade": KanglongResultGrade.SAFE_CLOSED.value,
            "plan_version": plan_version,
            "snapshot_bundle_id": run.get("snapshot_bundle_id") or "",
            "available_actions": ["view_report"],
            "report": run.get("report") or {},
        }
        self._repository.update_kanglong_run(
            run_id,
            status=final_payload["status"],
            result_grade=final_payload["result_grade"],
            available_actions=final_payload["available_actions"],
        )
        return self._repository.remember_kanglong_idempotency(
            key=idempotency_key,
            request_hash=request_hash,
            response=final_payload,
        )["response"]


    def list_events(self, run_id: str, after_event_id: int | None = None, limit: int = 200) -> dict[str, Any]:
        return {
            "run_id": run_id,
            **self._repository.list_kanglong_events(run_id, after_event_id=after_event_id, limit=limit),
        }
```

This first split keeps execution simulation synchronous and deterministic while preserving the final API shape.

- [ ] **Step 6: Run service workflow tests and commit**

Run:

```powershell
pytest tests/test_kanglong_workflow_contracts.py -q
pytest tests/test_kanglong_api.py -q
```

Expected: PASS for the service workflow test. Existing API tests that still target the old endpoint are updated in Task 5.

Commit:

```powershell
git add paired_opener/kanglong/models.py paired_opener/kanglong/service.py tests/test_kanglong_workflow_contracts.py tests/test_kanglong_api.py
git commit -m "feat: split kanglong service workflow"
```

---

### Task 5: Add Split FastAPI Endpoints And Old Endpoint Migration

**Files:**
- Modify: `paired_opener/api.py`
- Modify: `tests/test_kanglong_api.py`

- [ ] **Step 1: Update failing API tests**

Replace the old draft-run API test in `tests/test_kanglong_api.py` with:

```python
class StubKanglongService:
    def __init__(self) -> None:
        self.plans: dict[str, dict] = {}

    def create_plan(self, **kwargs) -> dict:
        payload = {
            "run_id": kwargs["run_id"],
            "status": "chain_ready",
            "plan_version": "plan-1",
            "snapshot_bundle_id": kwargs.get("snapshot_bundle_id") or "snap-1",
            "available_actions": ["confirm", "refresh_plan"],
            "report": {"summary": {"group_count": 0}},
        }
        self.plans[payload["run_id"]] = payload
        return payload

    def confirm_plan(self, **kwargs) -> dict:
        return {
            "run_id": kwargs["run_id"],
            "status": "plan_confirmed",
            "plan_version": kwargs["plan_version"],
            "snapshot_bundle_id": "snap-1",
            "available_actions": ["execute", "refresh_plan"],
            "report": {},
        }

    def execute_plan(self, **kwargs) -> dict:
        return {
            "run_id": kwargs["run_id"],
            "status": "completed",
            "plan_version": kwargs["plan_version"],
            "snapshot_bundle_id": "snap-1",
            "available_actions": ["view_report"],
            "report": {},
        }

    def get_run(self, run_id: str) -> dict | None:
        return self.plans.get(run_id)

    def list_events(self, run_id: str, after_event_id: int | None = None, limit: int = 200) -> dict:
        return {
            "run_id": run_id,
            "events": [],
            "next_after_event_id": int(after_event_id or 0),
            "latest_event_id": 0,
            "has_more": False,
        }


@pytest.mark.asyncio
async def test_kanglong_split_api_plan_confirm_execute() -> None:
    service = StubKanglongService()
    api_module.app.state.kanglong_service = service
    original_collector = api_module._collect_kanglong_plan_inputs

    async def fake_collector(request):
        return {
            "symbol": request.symbol,
            "main_account_id": request.main_account_id,
            "subaccount_ids": request.subaccount_ids,
            "selected_side": request.selected_side,
            "snapshot_bundle_id": "snap-1",
        }

    api_module._collect_kanglong_plan_inputs = fake_collector

    try:
        plan = await api_module.create_kanglong_simulation_plan(
            KanglongPlanRequest(main_account_id="main", subaccount_ids=["sub1"])
        )
        confirmed = await api_module.confirm_kanglong_simulation_plan(
            plan.run_id,
            KanglongActionRequest(plan_version=plan.plan_version, idempotency_key="confirm-0001"),
        )
        executed = await api_module.execute_kanglong_simulation_plan(
            plan.run_id,
            KanglongActionRequest(plan_version=plan.plan_version, idempotency_key="execute-0001"),
        )
        events = await api_module.get_kanglong_simulation_events(plan.run_id, after_event_id=0, limit=50)
    finally:
        api_module._collect_kanglong_plan_inputs = original_collector

    assert plan.status == "chain_ready"
    assert confirmed.status == "plan_confirmed"
    assert executed.status == "completed"
    assert events.latest_event_id == 0
```

Run: `pytest tests/test_kanglong_api.py -q`

Expected: FAIL because endpoint functions do not exist.

- [ ] **Step 2: Import new schemas**

Modify `paired_opener/api.py` imports:

```python
from decimal import Decimal

from paired_opener.schemas import (
    KanglongActionRequest,
    KanglongEventsResponse,
    KanglongPlanRequest,
    KanglongPlanResponse,
    KanglongRecoverRequest,
    KanglongRunStateResponse,
)
```

Keep existing imports already used by other endpoints.

Add Kanglong workflow imports:

```python
from paired_opener.config import DEFAULT_LEVERAGE
from paired_opener.kanglong.config import load_kanglong_symbol_config
from paired_opener.kanglong.snapshots import build_snapshot_bundle
```

- [ ] **Step 3: Add split endpoint functions**

Add a request collector below `current_runtime` helpers:

```python
async def _collect_kanglong_plan_inputs(request: KanglongPlanRequest) -> dict:
    runtime_manager: AccountRuntimeManager = app.state.runtime_manager
    account_ids = [request.main_account_id, *request.subaccount_ids]
    gateways = []
    account_payloads = []
    try:
        for account_id in account_ids:
            gateway = runtime_manager.build_temporary_gateway(account_id)
            gateways.append(gateway)
            account_payloads.append(await gateway.get_unified_account_snapshot())
        main_gateway = gateways[0]
        rules = await main_gateway.get_symbol_rules(request.symbol)
        quote = await main_gateway.get_quote(request.symbol)
    finally:
        for gateway in gateways:
            await gateway.close()
    config = load_kanglong_symbol_config(app.state.settings, request.symbol)
    snapshot_bundle = build_snapshot_bundle(
        symbol=request.symbol,
        accounts=account_payloads,
        config_version="default",
        symbol_rule_version=request.symbol,
        price_version=f"{quote.bid_price}:{quote.ask_price}",
        leverage=DEFAULT_LEVERAGE,
    )
    snapshots = snapshot_bundle["accounts"]
    return {
        "symbol": request.symbol,
        "main_account_id": request.main_account_id,
        "subaccount_ids": request.subaccount_ids,
        "selected_side": request.selected_side,
        "snapshot_bundle_id": snapshot_bundle["snapshot_bundle_id"],
        "main_snapshot": snapshots[0],
        "subaccount_snapshots": snapshots[1:],
        "config": config,
        "rules": rules,
        "close_price": Decimal(str(quote.bid_price)),
        "open_price": Decimal(str(quote.ask_price)),
        "fee_rate": Decimal("0.0005"),
    }
```

Add endpoints below the existing simulation endpoint block:

```python
@app.post("/kanglong/simulation/plan", response_model=KanglongPlanResponse)
async def create_kanglong_simulation_plan(request: KanglongPlanRequest) -> KanglongPlanResponse:
    if request.mode != "simulation":
        raise HTTPException(status_code=400, detail={"code": "kanglong_live_mode_not_supported"})
    run_id = str(uuid4())
    inputs = await _collect_kanglong_plan_inputs(request)
    payload = app.state.kanglong_service.create_plan(
        run_id=run_id,
        **inputs,
    )
    return KanglongPlanResponse.model_validate(payload)


@app.post("/kanglong/simulation/plan/{run_id}/confirm", response_model=KanglongPlanResponse)
async def confirm_kanglong_simulation_plan(run_id: str, request: KanglongActionRequest) -> KanglongPlanResponse:
    payload = app.state.kanglong_service.confirm_plan(
        run_id=run_id,
        plan_version=request.plan_version,
        idempotency_key=request.idempotency_key,
        operator=request.operator,
        confirmed_warning_codes=request.confirmed_warning_codes,
    )
    return KanglongPlanResponse.model_validate(payload)


@app.post("/kanglong/simulation/plan/{run_id}/execute", response_model=KanglongPlanResponse)
async def execute_kanglong_simulation_plan(run_id: str, request: KanglongActionRequest) -> KanglongPlanResponse:
    payload = app.state.kanglong_service.execute_plan(
        run_id=run_id,
        plan_version=request.plan_version,
        idempotency_key=request.idempotency_key,
        close_price=Decimal("3100.00"),
        open_price=Decimal("3100.50"),
        fee_rate=Decimal("0.0005"),
    )
    return KanglongPlanResponse.model_validate(payload)


@app.get("/kanglong/simulation/run/{run_id}/events", response_model=KanglongEventsResponse)
async def get_kanglong_simulation_events(run_id: str, after_event_id: int = 0, limit: int = 200) -> KanglongEventsResponse:
    payload = app.state.kanglong_service.list_events(run_id, after_event_id=after_event_id, limit=limit)
    return KanglongEventsResponse.model_validate(payload)
```

- [ ] **Step 4: Deprecate old endpoint**

Change the old `/kanglong/simulation/run` endpoint to return a structured deprecation error:

```python
@app.post("/kanglong/simulation/run", response_model=KanglongPlanResponse)
async def run_kanglong_simulation(request: KanglongSimulationRunRequest) -> KanglongPlanResponse:
    raise HTTPException(
        status_code=410,
        detail={
            "code": "kanglong_run_endpoint_deprecated",
            "replacement": "/kanglong/simulation/plan",
        },
    )
```

Add a test:

```python
@pytest.mark.asyncio
async def test_old_kanglong_run_endpoint_is_deprecated() -> None:
    with pytest.raises(api_module.HTTPException) as exc:
        await api_module.run_kanglong_simulation(
            KanglongSimulationRunRequest(main_account_id="main", subaccount_ids=["sub1"])
        )

    assert exc.value.status_code == 410
    assert exc.value.detail["code"] == "kanglong_run_endpoint_deprecated"
```

- [ ] **Step 5: Run API tests and commit**

Run:

```powershell
pytest tests/test_kanglong_api.py tests/test_kanglong_workflow_contracts.py -q
```

Expected: PASS.

Commit:

```powershell
git add paired_opener/api.py tests/test_kanglong_api.py tests/test_kanglong_workflow_contracts.py
git commit -m "feat: add kanglong split simulation api"
```

---

### Task 6: Enforce Cost Sign Rules And Golden Plan

**Files:**
- Modify: `paired_opener/kanglong/reporter.py`
- Modify: `paired_opener/kanglong/simulator.py`
- Modify: `paired_opener/kanglong/models.py`
- Create: `tests/test_kanglong_golden_plan.py`
- Test: `tests/test_kanglong_simulator.py`

- [ ] **Step 1: Write failing golden plan and cost sign tests**

Create `tests/test_kanglong_golden_plan.py`:

```python
from __future__ import annotations

from decimal import Decimal

from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import KanglongPlanningAccount
from paired_opener.kanglong.planner import build_kanglong_plan
from paired_opener.kanglong.reporter import summarize_costs
from paired_opener.kanglong.simulator import simulate_group


def planning_account(account_id: str, closeable: str, profit: str, capacity: str = "10") -> KanglongPlanningAccount:
    return KanglongPlanningAccount(
        account_id=account_id,
        closeable_qty=Decimal(closeable),
        unrealized_profit=Decimal(profit),
        receiver_capacity_qty=Decimal(capacity),
        risk_buffer=Decimal("1"),
    )


def test_golden_plan_group_order_and_quantities_are_stable() -> None:
    plan = build_kanglong_plan(
        run_id="run-golden",
        symbol="ETHUSDC",
        selected_side=PositionSide.LONG,
        main_account_id="main",
        first_donor_account_id="sub1",
        planned_release_qty=Decimal("1.0"),
        accounts=[
            planning_account("sub1", "1.0", "100"),
            planning_account("sub2", "0.4", "200"),
            planning_account("sub3", "0.8", "100"),
        ],
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
    )

    assert [(group.from_account_id, group.to_account_id, group.target_qty) for group in plan.groups[:3]] == [
        ("sub1", "main", Decimal("1.0")),
        ("sub2", "sub1", Decimal("0.4")),
        ("sub3", "sub1", Decimal("0.6")),
    ]
    assert plan.groups[0].round_qtys == [Decimal("0.25")] * 4


def test_cost_summary_uses_signed_pnl_and_non_negative_losses() -> None:
    plan = build_kanglong_plan(
        run_id="run-cost",
        symbol="ETHUSDC",
        selected_side=PositionSide.LONG,
        main_account_id="main",
        first_donor_account_id="sub1",
        planned_release_qty=Decimal("0.5"),
        accounts=[planning_account("sub1", "0.5", "100")],
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.5")),
    )
    result = simulate_group(
        run_id="run-cost",
        group=plan.groups[0],
        rules=SymbolRules("ETHUSDC", Decimal("0.01"), Decimal("0.001"), Decimal("0.001"), Decimal("5"), 125),
        close_price=Decimal("3100.00"),
        open_price=Decimal("3100.50"),
        fee_rate=Decimal("0.0005"),
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.5")),
    )

    summary = summarize_costs(result.events, result.residual_ledger)

    assert Decimal(summary["transfer_price_diff_pnl"]) < Decimal("0")
    assert Decimal(summary["transfer_price_diff_loss"]) > Decimal("0")
    assert Decimal(summary["total_cost"]) >= Decimal("0")
```

Run: `pytest tests/test_kanglong_golden_plan.py -q`

Expected: FAIL if imports or cost keys do not match the new contract.

- [ ] **Step 2: Update reporter cost fields**

Modify `paired_opener/kanglong/reporter.py` so `summarize_costs` returns these stringified Decimal keys:

```python
transfer_fee_cost
rebalance_fee_cost
transfer_price_diff_pnl
rebalance_price_diff_pnl
transfer_price_diff_loss
rebalance_price_diff_loss
total_fee_cost
total_price_diff_loss
total_cost
net_profit_after_cost
```

Use this calculation:

```python
total_fee_cost = transfer_fee_cost + rebalance_fee_cost
total_price_diff_loss = transfer_price_diff_loss + rebalance_price_diff_loss
total_cost = total_fee_cost + total_price_diff_loss
net_profit_after_cost = released_profit + transfer_price_diff_pnl + rebalance_price_diff_pnl - total_fee_cost
```

- [ ] **Step 3: Store signed `price_diff_pnl` on simulator events**

Add this field to `KanglongEvent` in `paired_opener/kanglong/models.py`:

```python
price_diff_pnl: Decimal = Decimal("0")
```

In `paired_opener/kanglong/simulator.py`, keep signed PnL by side:

```python
if group.side == PositionSide.LONG:
    price_diff_pnl = (close_price - open_price) * matched_qty
else:
    price_diff_pnl = (open_price - close_price) * matched_qty
```

Set `price_diff_pnl=price_diff_pnl` on the open-leg event for the group round, and keep close-leg `price_diff_pnl=Decimal("0")` so the group summary counts the transfer price difference once.

- [ ] **Step 4: Run golden and simulator tests**

Run:

```powershell
pytest tests/test_kanglong_golden_plan.py tests/test_kanglong_simulator.py tests/test_kanglong_planner.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add paired_opener/kanglong/reporter.py paired_opener/kanglong/simulator.py tests/test_kanglong_golden_plan.py tests/test_kanglong_simulator.py
git commit -m "test: lock kanglong golden plan and cost signs"
```

---

### Task 7: Add Frontend Third Page Skeleton And Remove Old Panel

**Files:**
- Modify: `paired_opener/static/index.html`
- Modify: `paired_opener/static/app.js`
- Modify: `tests/test_app_kanglong_display.mjs`
- Modify: `i18n/messages/zh-CN.json`

- [ ] **Step 1: Replace frontend display test expectations**

Update `tests/test_app_kanglong_display.mjs`:

```javascript
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const appSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "app.js"), "utf8");
const indexSource = fs.readFileSync(path.join(process.cwd(), "paired_opener", "static", "index.html"), "utf8");
const zhSource = fs.readFileSync(path.join(process.cwd(), "i18n", "messages", "zh-CN.json"), "utf8");

for (const id of [
  "navKanglongBtn",
  "kanglongWorkspace",
  "kanglongAccountPool",
  "kanglongSelectedSubaccounts",
  "kanglongPlanSummary",
  "kanglongExecutionLog",
]) {
  assert.ok(indexSource.includes(`id="${id}"`), `${id} should exist in index.html`);
}

assert.equal(indexSource.includes(`id="kanglongPanel"`), false, "old simulation Kanglong panel should be removed");
assert.ok(appSource.includes(`"kanglong"`), "app.js should recognize kanglong as an app page");
assert.ok(appSource.includes("/kanglong/simulation/plan"), "frontend should call split plan endpoint");
assert.equal(appSource.includes("/kanglong/simulation/run\""), false, "frontend should not call deprecated Kanglong run endpoint");

for (const key of [
  "console.kanglong.nav",
  "console.kanglong.stage.account_selection",
  "console.kanglong.account_pool.title",
  "console.kanglong.plan.summary_title",
  "console.kanglong.execution.log_title",
]) {
  assert.ok(indexSource.includes(key) || appSource.includes(key) || zhSource.includes(key), `${key} should be wired`);
}
```

Run: `node tests\test_app_kanglong_display.mjs`

Expected: FAIL because the new ids and endpoint calls are missing.

- [ ] **Step 2: Add third nav button and page shell**

Modify `paired_opener/static/index.html` nav:

```html
<nav class="app-nav" aria-label="交易环境切换">
  <button id="navRealBtn" class="active" type="button">实盘</button>
  <button id="navSimulationBtn" type="button">模拟盘</button>
  <button id="navKanglongBtn" type="button" data-i18n="console.kanglong.nav"></button>
</nav>
```

Remove the old section with `id="kanglongPanel"` from the simulation under-log grid.

Add a new top-level section near the main dashboard sections:

```html
<section id="kanglongWorkspace" class="kanglong-workspace" data-kanglong-only>
  <div class="kanglong-toolbar">
    <div class="field">
      <label for="kanglongSymbol" data-i18n="runtime.symbol"></label>
      <input id="kanglongSymbol" value="ETHUSDC" />
    </div>
    <div class="field">
      <label for="kanglongSide" data-i18n="console.kanglong.selected_side"></label>
      <select id="kanglongSide">
        <option value="" data-i18n="console.kanglong.side_auto"></option>
        <option value="LONG" data-i18n="console.position_side.long"></option>
        <option value="SHORT" data-i18n="console.position_side.short"></option>
      </select>
    </div>
    <button id="kanglongDetectPlanBtn" type="button" data-i18n="console.kanglong.actions.detect"></button>
    <button id="kanglongConfirmPlanBtn" type="button" data-i18n="console.kanglong.actions.confirm" disabled></button>
    <button id="kanglongExecutePlanBtn" type="button" data-i18n="console.kanglong.actions.execute" disabled></button>
  </div>
  <div class="kanglong-layout">
    <section class="card kanglong-account-section">
      <h2 class="card-title" data-i18n="console.kanglong.account_pool.title"></h2>
      <div id="kanglongMainAccountCard" class="kanglong-compact-list"></div>
      <div id="kanglongAccountPool" class="kanglong-compact-list"></div>
    </section>
    <section class="card kanglong-selected-section">
      <h2 class="card-title" data-i18n="console.kanglong.selected_accounts.title"></h2>
      <div id="kanglongSelectedSubaccounts" class="kanglong-compact-list"></div>
    </section>
  </div>
  <section class="card kanglong-plan-section">
    <h2 class="card-title" data-i18n="console.kanglong.plan.summary_title"></h2>
    <div id="kanglongPlanSummary" class="kanglong-plan-summary"></div>
  </section>
  <section class="card kanglong-execution-section">
    <h2 class="card-title" data-i18n="console.kanglong.execution.log_title"></h2>
    <div id="kanglongLogFilters" class="kanglong-log-filters"></div>
    <div id="kanglongExecutionLog" class="kanglong-execution-log"></div>
  </section>
</section>
```

- [ ] **Step 3: Add page visibility CSS**

In the `<style>` block:

```css
.app-view-real [data-simulation-only],
.app-view-real [data-kanglong-only],
.app-view-simulation [data-real-only],
.app-view-simulation [data-kanglong-only],
.app-view-kanglong [data-real-only],
.app-view-kanglong [data-simulation-only] {
  display: none !important;
}

.kanglong-layout {
  display: grid;
  gap: 16px;
  grid-template-columns: minmax(0, 1.2fr) minmax(280px, 0.8fr);
}

.kanglong-compact-list {
  display: grid;
  gap: 8px;
}

.kanglong-account-row {
  min-height: 64px;
  padding: 8px 10px;
  border: 1px solid rgba(216, 197, 170, 0.8);
  border-radius: 8px;
  background: #fffdf8;
}

@media (max-width: 860px) {
  .kanglong-layout {
    grid-template-columns: 1fr;
  }
}
```

- [ ] **Step 4: Add `kanglong` app page state**

Modify `paired_opener/static/app.js`:

```javascript
const navKanglongBtn = document.getElementById("navKanglongBtn");
const kanglongWorkspace = document.getElementById("kanglongWorkspace");
const kanglongSymbol = document.getElementById("kanglongSymbol");
const kanglongSide = document.getElementById("kanglongSide");
const kanglongDetectPlanBtn = document.getElementById("kanglongDetectPlanBtn");
const kanglongConfirmPlanBtn = document.getElementById("kanglongConfirmPlanBtn");
const kanglongExecutePlanBtn = document.getElementById("kanglongExecutePlanBtn");
const kanglongAccountPool = document.getElementById("kanglongAccountPool");
const kanglongSelectedSubaccounts = document.getElementById("kanglongSelectedSubaccounts");
const kanglongPlanSummary = document.getElementById("kanglongPlanSummary");
const kanglongExecutionLog = document.getElementById("kanglongExecutionLog");
```

Change page normalization:

```javascript
function normalizeAppPage(page) {
  if (page === "simulation") return "simulation";
  if (page === "kanglong") return "kanglong";
  return "real";
}
```

Change page chrome:

```javascript
function applyAppPageChrome(page) {
  appPage = normalizeAppPage(page);
  appRoot?.classList.toggle("app-view-real", appPage === "real");
  appRoot?.classList.toggle("app-view-simulation", appPage === "simulation");
  appRoot?.classList.toggle("app-view-kanglong", appPage === "kanglong");
  navRealBtn?.classList.toggle("active", appPage === "real");
  navSimulationBtn?.classList.toggle("active", appPage === "simulation");
  navKanglongBtn?.classList.toggle("active", appPage === "kanglong");
}
```

Add:

```javascript
navKanglongBtn?.addEventListener("click", () => {
  setAppPage("kanglong").catch((error) => appendLog("error", "", undefined, {
    messageCode: "runtime.kanglong.request_failed",
    messageParams: { error: userVisibleErrorMessage(error) },
  }));
});
```

- [ ] **Step 5: Add i18n keys**

Add to `i18n/messages/zh-CN.json`:

```json
"console.kanglong.nav": "亢龙有悔移仓模拟",
"console.kanglong.stage.account_selection": "账号选择",
"console.kanglong.account_pool.title": "账号池",
"console.kanglong.selected_accounts.title": "已选子账号",
"console.kanglong.plan.summary_title": "检测链路",
"console.kanglong.execution.log_title": "执行日志",
"console.kanglong.actions.detect": "检测账号状态",
"console.kanglong.actions.confirm": "确认链路",
"console.kanglong.actions.execute": "开始模拟移仓"
```

Preserve valid JSON ordering and commas.

- [ ] **Step 6: Run frontend tests and commit**

Run:

```powershell
node tests\test_app_kanglong_display.mjs
node --check paired_opener\static\app.js
```

Expected: PASS.

Commit:

```powershell
git add paired_opener/static/index.html paired_opener/static/app.js i18n/messages/zh-CN.json tests/test_app_kanglong_display.mjs
git commit -m "feat: add kanglong workspace page shell"
```

---

### Task 8: Build Account Pool And Selection State

**Files:**
- Modify: `paired_opener/static/app.js`
- Modify: `i18n/messages/zh-CN.json`
- Test: `tests/test_app_kanglong_display.mjs`

- [ ] **Step 1: Add frontend account state assertions**

Append to `tests/test_app_kanglong_display.mjs`:

```javascript
for (const symbol of [
  "kanglongState",
  "renderKanglongAccountPool",
  "addSelectedKanglongAccounts",
  "removeSelectedKanglongAccount",
  "renderKanglongAccountRow",
]) {
  assert.ok(appSource.includes(symbol), `${symbol} should be implemented`);
}

for (const key of [
  "console.kanglong.card.no_position",
  "console.kanglong.card.no_profit",
  "console.kanglong.card.stale",
  "console.kanglong.card.joined",
  "console.kanglong.card.risk_unknown",
]) {
  assert.ok(zhSource.includes(key), `${key} should be translated`);
}
```

Run: `node tests\test_app_kanglong_display.mjs`

Expected: FAIL because account pool state is not implemented.

- [ ] **Step 2: Add Kanglong frontend state**

Add to `paired_opener/static/app.js` near other state:

```javascript
const kanglongState = {
  mainAccountId: "",
  selectedSubaccountIds: new Set(),
  checkedPoolAccountIds: new Set(),
  plan: null,
  confirmedPlanVersion: "",
  latestEventId: 0,
  logFilter: "all",
};
```

- [ ] **Step 3: Add compact account row renderer**

Add:

```javascript
function kanglongAccountLabel(account) {
  return account?.name || account?.id || account?.account_id || "--";
}

function kanglongAccountId(account) {
  return account?.id || account?.account_id || "";
}

function renderKanglongAccountRow(account, { checked = false, selected = false, main = false } = {}) {
  const accountId = kanglongAccountId(account);
  const row = document.createElement("div");
  row.className = "kanglong-account-row";
  row.dataset.accountId = accountId;
  const statusKey = selected
    ? "console.kanglong.card.joined"
    : "console.kanglong.card.risk_unknown";
  row.innerHTML = `
    <div class="kanglong-account-row-main">
      ${main ? "" : `<input type="checkbox" class="kanglong-account-check" ${checked ? "checked" : ""} />`}
      <strong>${escapeHtml(kanglongAccountLabel(account))}</strong>
      <span>${escapeHtml(accountId)}</span>
    </div>
    <div class="kanglong-account-row-metrics">
      <span>${escapeHtml(kanglongSymbol?.value || DEFAULT_SYMBOL)}</span>
      <span>${escapeHtml(kanglongSide?.value || copyOrDefault("console.kanglong.side_auto", "自动选择"))}</span>
      <span>${copyOrDefault(statusKey, statusKey)}</span>
    </div>
  `;
  const checkbox = row.querySelector(".kanglong-account-check");
  checkbox?.addEventListener("change", () => {
    if (checkbox.checked) kanglongState.checkedPoolAccountIds.add(accountId);
    else kanglongState.checkedPoolAccountIds.delete(accountId);
  });
  return row;
}
```

Add this HTML escaping helper near the other small rendering helpers:

```javascript
function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
```

- [ ] **Step 4: Render account pool and selected list**

Add:

```javascript
function renderKanglongAccountPool(accounts = availableAccounts) {
  if (!kanglongAccountPool || !kanglongSelectedSubaccounts) return;
  const normalizedAccounts = Array.isArray(accounts) ? accounts : [];
  if (!kanglongState.mainAccountId) {
    kanglongState.mainAccountId = currentAccount.id || normalizedAccounts.find((account) => account.is_active)?.id || normalizedAccounts[0]?.id || "";
  }
  kanglongAccountPool.innerHTML = "";
  kanglongSelectedSubaccounts.innerHTML = "";
  normalizedAccounts.forEach((account) => {
    const accountId = kanglongAccountId(account);
    if (accountId === kanglongState.mainAccountId) return;
    if (kanglongState.selectedSubaccountIds.has(accountId)) return;
    kanglongAccountPool.appendChild(renderKanglongAccountRow(account, {
      checked: kanglongState.checkedPoolAccountIds.has(accountId),
    }));
  });
  normalizedAccounts
    .filter((account) => kanglongState.selectedSubaccountIds.has(kanglongAccountId(account)))
    .forEach((account) => {
      const row = renderKanglongAccountRow(account, { selected: true });
      const removeButton = document.createElement("button");
      removeButton.type = "button";
      removeButton.textContent = copyOrDefault("console.kanglong.actions.remove", "移除");
      removeButton.addEventListener("click", () => removeSelectedKanglongAccount(kanglongAccountId(account)));
      row.appendChild(removeButton);
      kanglongSelectedSubaccounts.appendChild(row);
    });
}

function addSelectedKanglongAccounts() {
  kanglongState.checkedPoolAccountIds.forEach((accountId) => {
    if (accountId && accountId !== kanglongState.mainAccountId) {
      kanglongState.selectedSubaccountIds.add(accountId);
    }
  });
  kanglongState.checkedPoolAccountIds.clear();
  invalidateKanglongPlan();
  renderKanglongAccountPool(availableAccounts);
}

function removeSelectedKanglongAccount(accountId) {
  kanglongState.selectedSubaccountIds.delete(accountId);
  invalidateKanglongPlan();
  renderKanglongAccountPool(availableAccounts);
}

function invalidateKanglongPlan() {
  kanglongState.plan = null;
  kanglongState.confirmedPlanVersion = "";
  if (kanglongConfirmPlanBtn) kanglongConfirmPlanBtn.disabled = true;
  if (kanglongExecutePlanBtn) kanglongExecutePlanBtn.disabled = true;
  if (kanglongPlanSummary) kanglongPlanSummary.textContent = "";
}
```

Call `renderKanglongAccountPool(availableAccounts)` after `renderAccountOptions(payload.accounts || [])` in `loadAccounts()` or inside existing account option rendering.

- [ ] **Step 5: Add i18n keys and run tests**

Add:

```json
"console.kanglong.actions.remove": "移除",
"console.kanglong.card.no_position": "无本方向持仓",
"console.kanglong.card.no_profit": "无盈利仓位",
"console.kanglong.card.stale": "数据过期",
"console.kanglong.card.joined": "已加入",
"console.kanglong.card.risk_unknown": "风险未知"
```

Run:

```powershell
node tests\test_app_kanglong_display.mjs
node --check paired_opener\static\app.js
```

Expected: PASS.

- [ ] **Step 6: Commit**

```powershell
git add paired_opener/static/app.js i18n/messages/zh-CN.json tests/test_app_kanglong_display.mjs
git commit -m "feat: add kanglong account pool selection"
```

---

### Task 9: Wire Plan, Confirm, Execute, And Events In Frontend

**Files:**
- Modify: `paired_opener/static/app.js`
- Modify: `i18n/messages/zh-CN.json`
- Test: `tests/test_app_kanglong_display.mjs`

- [ ] **Step 1: Add frontend workflow assertions**

Append:

```javascript
for (const symbol of [
  "createKanglongPlan",
  "confirmKanglongPlan",
  "executeKanglongPlan",
  "pollKanglongEvents",
  "renderKanglongPlanSummary",
  "appendKanglongExecutionEvent",
]) {
  assert.ok(appSource.includes(symbol), `${symbol} should be implemented`);
}
```

Run: `node tests\test_app_kanglong_display.mjs`

Expected: FAIL.

- [ ] **Step 2: Implement idempotency key helper**

Add:

```javascript
function newKanglongIdempotencyKey(prefix) {
  const random = Math.random().toString(16).slice(2);
  return `${prefix}-${Date.now()}-${random}`;
}
```

- [ ] **Step 3: Implement plan request**

Add:

```javascript
async function createKanglongPlan() {
  const mainAccountId = kanglongState.mainAccountId || currentAccount.id || "";
  const subaccountIds = Array.from(kanglongState.selectedSubaccountIds);
  if (!mainAccountId || subaccountIds.length === 0) {
    throw new Error(copyOrDefault("runtime.kanglong.account_selection_required", "请选择主账号和至少一个子账号。"));
  }
  const payload = {
    mode: "simulation",
    symbol: kanglongSymbol?.value || DEFAULT_SYMBOL,
    main_account_id: mainAccountId,
    subaccount_ids: subaccountIds,
    selected_side: kanglongSide?.value || null,
  };
  const response = await request("/kanglong/simulation/plan", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  kanglongState.plan = response;
  kanglongState.confirmedPlanVersion = "";
  renderKanglongPlanSummary(response);
  if (kanglongConfirmPlanBtn) kanglongConfirmPlanBtn.disabled = !(response.available_actions || []).includes("confirm");
  if (kanglongExecutePlanBtn) kanglongExecutePlanBtn.disabled = true;
  return response;
}
```

- [ ] **Step 4: Implement summary renderer**

Add:

```javascript
function renderKanglongPlanSummary(payload = {}) {
  if (!kanglongPlanSummary) return;
  const summary = payload.report?.summary || {};
  const lines = [
    copyOrDefault("console.kanglong.plan.status", "状态：{status}", { status: payload.status || "--" }),
    copyOrDefault("console.kanglong.plan.groups", "组数：{count}", { count: summary.group_count ?? "--" }),
    copyOrDefault("console.kanglong.plan.rounds", "轮次：{count}", { count: summary.round_count ?? "--" }),
    copyOrDefault("console.kanglong.plan.release_qty", "计划释放：{qty}", { qty: summary.planned_release_qty ?? "--" }),
  ];
  kanglongPlanSummary.textContent = lines.join(" | ");
}
```

- [ ] **Step 5: Implement confirm/execute/events**

Add:

```javascript
async function confirmKanglongPlan() {
  if (!kanglongState.plan?.run_id || !kanglongState.plan?.plan_version) return null;
  const response = await request(`/kanglong/simulation/plan/${encodeURIComponent(kanglongState.plan.run_id)}/confirm`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      plan_version: kanglongState.plan.plan_version,
      idempotency_key: newKanglongIdempotencyKey("confirm"),
      operator: "manual",
      confirmed_warning_codes: [],
    }),
  });
  kanglongState.plan = response;
  kanglongState.confirmedPlanVersion = response.plan_version || "";
  renderKanglongPlanSummary(response);
  if (kanglongExecutePlanBtn) kanglongExecutePlanBtn.disabled = !(response.available_actions || []).includes("execute");
  return response;
}

async function executeKanglongPlan() {
  if (!kanglongState.plan?.run_id || !kanglongState.confirmedPlanVersion) return null;
  const response = await request(`/kanglong/simulation/plan/${encodeURIComponent(kanglongState.plan.run_id)}/execute`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      plan_version: kanglongState.confirmedPlanVersion,
      idempotency_key: newKanglongIdempotencyKey("execute"),
      operator: "manual",
      confirmed_warning_codes: [],
    }),
  });
  kanglongState.plan = response;
  renderKanglongPlanSummary(response);
  await pollKanglongEvents();
  return response;
}

async function pollKanglongEvents() {
  if (!kanglongState.plan?.run_id) return;
  const response = await request(`/kanglong/simulation/run/${encodeURIComponent(kanglongState.plan.run_id)}/events?after_event_id=${kanglongState.latestEventId}`);
  (response.events || []).forEach(appendKanglongExecutionEvent);
  kanglongState.latestEventId = response.next_after_event_id || kanglongState.latestEventId;
}

function appendKanglongExecutionEvent(event) {
  if (!kanglongExecutionLog) return;
  const row = document.createElement("div");
  row.className = "kanglong-log-row";
  row.textContent = formatEventMessage(event.payload || event);
  kanglongExecutionLog.appendChild(row);
}
```

- [ ] **Step 6: Wire buttons and i18n**

Add listeners:

```javascript
kanglongDetectPlanBtn?.addEventListener("click", async () => {
  try {
    await createKanglongPlan();
  } catch (error) {
    appendLog("error", "", undefined, { messageCode: "runtime.kanglong.request_failed", messageParams: { error: userVisibleErrorMessage(error) } });
  }
});

kanglongConfirmPlanBtn?.addEventListener("click", async () => {
  try {
    await confirmKanglongPlan();
  } catch (error) {
    appendLog("error", "", undefined, { messageCode: "runtime.kanglong.request_failed", messageParams: { error: userVisibleErrorMessage(error) } });
  }
});

kanglongExecutePlanBtn?.addEventListener("click", async () => {
  try {
    await executeKanglongPlan();
  } catch (error) {
    appendLog("error", "", undefined, { messageCode: "runtime.kanglong.request_failed", messageParams: { error: userVisibleErrorMessage(error) } });
  }
});
```

Add i18n:

```json
"console.kanglong.plan.status": "状态：{status}",
"console.kanglong.plan.groups": "组数：{count}",
"console.kanglong.plan.rounds": "轮次：{count}",
"console.kanglong.plan.release_qty": "计划释放：{qty}"
```

- [ ] **Step 7: Run frontend checks and commit**

Run:

```powershell
node tests\test_app_kanglong_display.mjs
node --check paired_opener\static\app.js
```

Expected: PASS.

Commit:

```powershell
git add paired_opener/static/app.js i18n/messages/zh-CN.json tests/test_app_kanglong_display.mjs
git commit -m "feat: wire kanglong workspace workflow"
```

---

### Task 10: Expand I18n Registry Coverage

**Files:**
- Modify: `i18n/messages/zh-CN.json`
- Modify: `i18n/registry/events.json`
- Modify: `i18n/registry/logs.json`
- Modify: `i18n/registry/reasons.json`
- Modify: `i18n/registry/precheck.json`
- Modify: `tests/test_kanglong_i18n_contracts.py`

- [ ] **Step 1: Add failing i18n contract assertions**

Add to `tests/test_kanglong_i18n_contracts.py`:

```python
def test_kanglong_workspace_i18n_keys_exist() -> None:
    messages = load_messages()
    required = [
        "console.kanglong.nav",
        "console.kanglong.account_pool.title",
        "console.kanglong.selected_accounts.title",
        "console.kanglong.plan.summary_title",
        "console.kanglong.execution.log_title",
        "console.kanglong.actions.detect",
        "console.kanglong.actions.confirm",
        "console.kanglong.actions.execute",
        "console.kanglong.logs.filter.all",
        "console.kanglong.logs.filter.warning",
        "console.kanglong.logs.filter.error",
        "console.kanglong.logs.filter.current_group",
        "console.kanglong.logs.filter.cost",
        "console.kanglong.logs.filter.ledger",
        "runtime.kanglong.idempotency_conflict",
        "runtime.kanglong.plan_stale",
    ]
    for key in required:
        assert key in messages
```

Ensure `tests/test_kanglong_i18n_contracts.py` contains this helper:

```python
import json
from pathlib import Path


def load_messages() -> dict[str, str]:
    return json.loads(Path("i18n/messages/zh-CN.json").read_text(encoding="utf-8"))
```

Run: `pytest tests/test_kanglong_i18n_contracts.py -q`

Expected: FAIL until all keys exist.

- [ ] **Step 2: Add message keys**

Add:

```json
"console.kanglong.logs.filter.all": "全部",
"console.kanglong.logs.filter.warning": "警告",
"console.kanglong.logs.filter.error": "错误",
"console.kanglong.logs.filter.current_group": "当前组",
"console.kanglong.logs.filter.cost": "成本事件",
"console.kanglong.logs.filter.ledger": "账本事件",
"runtime.kanglong.idempotency_conflict": "重复请求的幂等键与原请求不一致，请刷新后重试。",
"runtime.kanglong.plan_stale": "检测链路已过期，请重新检测账号状态。"
```

- [ ] **Step 3: Register event/reason/log codes**

Add to `i18n/registry/events.json`:

```json
"kanglong.group_simulated": { "key": "events.kanglong.group_simulated", "level": "info" }
```

Add to `i18n/messages/zh-CN.json`:

```json
"events.kanglong.group_simulated": "亢龙组 {group_id} 已完成模拟。"
```

Add to `i18n/registry/reasons.json`:

```json
"kanglong.blocked_plan_stale": {
  "key": "reasons.kanglong.blocked_plan_stale"
},
"kanglong.idempotency_conflict": {
  "key": "reasons.kanglong.idempotency_conflict"
}
```

Add matching messages:

```json
"reasons.kanglong.blocked_plan_stale": "计划版本已变化，需要重新检测并确认。",
"reasons.kanglong.idempotency_conflict": "同一个幂等键被用于不同请求。"
```

- [ ] **Step 4: Run i18n tests and commit**

Run:

```powershell
pytest tests/test_kanglong_i18n_contracts.py -q
```

Expected: PASS.

Commit:

```powershell
git add i18n/messages/zh-CN.json i18n/registry/events.json i18n/registry/logs.json i18n/registry/reasons.json i18n/registry/precheck.json tests/test_kanglong_i18n_contracts.py
git commit -m "feat: add kanglong workspace i18n contracts"
```

---

### Task 11: Restore Active Kanglong Run On Page Load

**Files:**
- Modify: `paired_opener/kanglong/service.py`
- Modify: `paired_opener/api.py`
- Modify: `paired_opener/static/app.js`
- Modify: `tests/test_kanglong_workflow_contracts.py`
- Modify: `tests/test_app_kanglong_display.mjs`

- [ ] **Step 1: Add backend active-run test**

Append:

```python
def test_service_active_run_returns_latest_open_run(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)
    try:
        repository.create_kanglong_run(
            {
                "run_id": "run-1",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub1"],
                "status": "plan_confirmed",
                "plan_version": "plan-1",
                "snapshot_bundle_id": "snap-1",
                "available_actions": ["execute"],
            }
        )

        active = service.active_run()
    finally:
        repository.close()

    assert active is not None
    assert active["run_id"] == "run-1"
    assert active["available_actions"] == ["execute"]
```

Run: `pytest tests/test_kanglong_workflow_contracts.py::test_service_active_run_returns_latest_open_run -q`

Expected: FAIL.

- [ ] **Step 2: Add repository active lookup**

Add `get_active_kanglong_run`:

```python
def get_active_kanglong_run(self) -> dict[str, Any] | None:
    row = self._connection.execute(
        """
        SELECT * FROM kanglong_runs
        WHERE status IN (
            'draft_plan',
            'chain_ready',
            'plan_confirmed',
            'execution_starting',
            'group_ready',
            'paused_group_not_executable',
            'paused_plan_recheck_changed',
            'needs_abort_recover',
            'abort_recovering'
        )
        ORDER BY updated_at DESC
        LIMIT 1
        """
    ).fetchone()
    return self._deserialize_kanglong_run_row(row) if row is not None else None
```

Add service:

```python
    def active_run(self) -> dict[str, Any] | None:
        return self._repository.get_active_kanglong_run()
```

- [ ] **Step 3: Add active API endpoint**

In `paired_opener/api.py`:

```python
@app.get("/kanglong/simulation/run/active")
async def get_active_kanglong_simulation_run() -> dict:
    payload = app.state.kanglong_service.active_run()
    return payload or {"status": "idle", "available_actions": ["create_plan"]}
```

- [ ] **Step 4: Add frontend restore test and implementation**

Add assertion:

```javascript
assert.ok(appSource.includes("restoreActiveKanglongRun"), "frontend should restore active Kanglong run");
assert.ok(appSource.includes("/kanglong/simulation/run/active"), "frontend should call active Kanglong run endpoint");
```

Add to `app.js`:

```javascript
async function restoreActiveKanglongRun() {
  const payload = await request("/kanglong/simulation/run/active");
  if (!payload || payload.status === "idle") return;
  kanglongState.plan = payload;
  kanglongState.confirmedPlanVersion = payload.status === "plan_confirmed" ? payload.plan_version || "" : "";
  kanglongState.latestEventId = payload.latest_event_id || 0;
  renderKanglongPlanSummary(payload);
  if (kanglongConfirmPlanBtn) kanglongConfirmPlanBtn.disabled = !(payload.available_actions || []).includes("confirm");
  if (kanglongExecutePlanBtn) kanglongExecutePlanBtn.disabled = !(payload.available_actions || []).includes("execute");
  await pollKanglongEvents();
}
```

Call `restoreActiveKanglongRun()` when `setAppPage("kanglong")` applies the Kanglong page, or during initial boot after account loading.

- [ ] **Step 5: Run tests and commit**

Run:

```powershell
pytest tests/test_kanglong_workflow_contracts.py -q
node tests\test_app_kanglong_display.mjs
node --check paired_opener\static\app.js
```

Expected: PASS.

Commit:

```powershell
git add paired_opener/storage.py paired_opener/kanglong/service.py paired_opener/api.py paired_opener/static/app.js tests/test_kanglong_workflow_contracts.py tests/test_app_kanglong_display.mjs
git commit -m "feat: restore active kanglong runs"
```

---

### Task 12: Final Regression And Manual Browser Check

**Files:**
- Modify only files needed for fixes found by this task.

- [ ] **Step 1: Run backend Kanglong regression suite**

Run:

```powershell
pytest tests/test_kanglong_config.py tests/test_kanglong_precheck.py tests/test_kanglong_planner.py tests/test_kanglong_simulator.py tests/test_kanglong_api.py tests/test_kanglong_workflow_contracts.py tests/test_kanglong_storage_workflow.py tests/test_kanglong_snapshot_adapter.py tests/test_kanglong_golden_plan.py tests/test_kanglong_i18n_contracts.py -q
```

Expected: PASS.

- [ ] **Step 2: Run frontend checks**

Run:

```powershell
node tests\test_app_kanglong_display.mjs
node tests\test_app_simulation_payloads.mjs
node --check paired_opener\static\app.js
```

Expected: PASS.

- [ ] **Step 3: Run broader regression script**

Run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_regression_tests.ps1
```

Expected: PASS or only documented unrelated failures. If there are failures, fix Kanglong-related failures before continuing.

- [ ] **Step 4: Start local service**

Run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start_service.ps1
```

Expected: service starts on the configured local port. If the service is already running, use the existing instance.

- [ ] **Step 5: Browser smoke check**

Open `http://127.0.0.1:8000` or the configured local service URL. Verify:

- Top navigation shows `实盘`, `模拟盘`, and `亢龙有悔移仓模拟`.
- Clicking `亢龙有悔移仓模拟` does not show real-run controls as active.
- The old Kanglong card is absent from the simulation page.
- Account pool renders compact rows.
- Detect with no subaccount shows the translated selection error.
- Network panel or logs show calls to `/kanglong/simulation/plan`, not deprecated `/kanglong/simulation/run`.

- [ ] **Step 6: Commit final fixes when regression changes files**

Run:

```powershell
git status --short
```

When the status output lists Kanglong-related fixes, stage those exact files and commit:

```powershell
git add paired_opener\kanglong paired_opener\static\index.html paired_opener\static\app.js paired_opener\api.py paired_opener\schemas.py paired_opener\storage.py i18n tests
git commit -m "fix: stabilize kanglong workspace regression"
```

When the status output is empty, record the passing verification in the final response and skip the commit.

---

## Self-Review Checklist

- Spec coverage: independent page, old panel removal, account pool, compact cards, split APIs, idempotency, `snapshot_bundle_id`, event pagination, active run recovery, log filters, cost signs, i18n, and golden plan are covered.
- No placeholders: the plan avoids open-ended implementation markers and gives concrete tests, functions, keys, paths, and commands.
- Type consistency: `KanglongPlanRequest`, `KanglongActionRequest`, `KanglongPlanResponse`, `KanglongEventsResponse`, `plan_version`, `snapshot_bundle_id`, `idempotency_key`, `available_actions`, `event_id`, and `after_event_id` are used consistently across backend, API, and frontend tasks.
