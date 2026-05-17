# Kanglong Transfer Simulation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first simulation-only version of the “亢龙有悔” cross-account transfer module, including deterministic planning, precheck blocking, in-memory simulated execution, reporting, i18n display contracts, and default parameter alignment.

**Architecture:** Keep Kanglong in a dedicated `paired_opener/kanglong/` package. The package owns domain models, symbol config, precheck, planner, simulation executor, cost reporter, and service orchestration; existing modules expose only API, storage, account gateway creation, and frontend entrypoints. First version does not place live orders and does not auto-execute market reduce.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, Decimal arithmetic, SQLite via the existing `SqliteRepository`, existing `ExchangeGateway` interface, vanilla JS frontend, `zh-CN` i18n JSON registries, pytest, and node-based frontend checks.

---

## Scope Boundaries

- Build simulation mode only.
- Reuse existing account loading, symbol rules, orderbook/quote access, rounding helpers, and i18n runtime.
- Do not modify live order engines for the first version.
- Do not auto-switch to the alternative profitable direction.
- Do not auto-execute market reduce; return a structured proposal that requires a separate confirmation flow.
- Keep every user-visible Kanglong text behind i18n keys.
- Preserve all unrelated dirty files in the working tree.

## File Structure

Create:

- `paired_opener/kanglong/__init__.py`: package exports.
- `paired_opener/kanglong/models.py`: enums and dataclasses for snapshots, plans, groups, events, ledgers, reports, and statuses.
- `paired_opener/kanglong/config.py`: per-symbol Kanglong config loader and default config.
- `paired_opener/kanglong/precheck.py`: snapshot collection, main-account/sub-account validation, direction selection, capacity checks.
- `paired_opener/kanglong/planner.py`: deterministic chain planner, FIFO pending-debt queue, donor batch segmentation.
- `paired_opener/kanglong/simulator.py`: simulation-only round/group executor that emits unified Kanglong events.
- `paired_opener/kanglong/reporter.py`: cost, residual, audit, and result summarization.
- `paired_opener/kanglong/service.py`: orchestration, storage calls, run lock lifecycle, abort/recover state transitions.
- `tests/test_kanglong_config.py`: defaults and symbol config tests.
- `tests/test_kanglong_precheck.py`: direction, main flat, subaccount baseline, and capacity tests.
- `tests/test_kanglong_planner.py`: deterministic planner, FIFO queue, donor batch, and tie-break tests.
- `tests/test_kanglong_simulator.py`: round execution, matched quantity, residual ledger, and abort/recover tests.
- `tests/test_kanglong_api.py`: API contract and simulation-only guard tests.
- `tests/test_kanglong_i18n_contracts.py`: language-pack and registry coverage tests.
- `tests/test_app_kanglong_display.mjs`: frontend rendering and no hard-coded Kanglong display strings.

Modify:

- `paired_opener/config.py`: default trading symbol/round/leverage constants and Kanglong config file path.
- `paired_opener/schemas.py`: default request values and Kanglong request/response models.
- `paired_opener/account_runtime.py`: create disposable gateways for selected Kanglong accounts without changing the active account.
- `paired_opener/api.py`: add Kanglong endpoints and expose i18n namespaces already needed by frontend.
- `paired_opener/storage.py`: add Kanglong run/event/snapshot/lock tables and repository methods.
- `paired_opener/static/index.html`: add the Kanglong simulation panel and report container.
- `paired_opener/static/app.js`: add Kanglong state, API calls, report rendering, i18n display, and validation.
- `i18n/messages/zh-CN.json`: add `console.kanglong.*`, `runtime.kanglong.*`, `reasons.kanglong.*`, `events.kanglong.*`, and `log.kanglong.*` messages.
- `i18n/registry/reasons.json`: add Kanglong reason codes.
- `i18n/registry/events.json`: add Kanglong event codes.
- `i18n/registry/logs.json`: add Kanglong audit/log codes.
- `i18n/registry/precheck.json`: add Kanglong precheck item codes.

---

### Task 1: Align Default Parameters

**Files:**
- Modify: `paired_opener/config.py`
- Modify: `paired_opener/schemas.py`
- Modify: `paired_opener/static/index.html`
- Modify: `paired_opener/static/app.js`
- Test: `tests/test_service_config.py`
- Test: `tests/test_app_simulation_payloads.mjs`

- [ ] **Step 1: Add failing backend default tests**

Add assertions that schema defaults match the requested defaults:

```python
from paired_opener.schemas import MarketConnectRequest, SimulationRunRequest


def test_default_trading_inputs_are_eth_usdc_75x_30_rounds() -> None:
    assert MarketConnectRequest().symbol == "ETHUSDC"

    request = SimulationRunRequest(round_count=30)

    assert request.symbol == "ETHUSDC"
    assert request.round_count == 30
```

Run: `pytest tests/test_service_config.py -q`
Expected: FAIL while `MarketConnectRequest` or `SimulationRunRequest` still defaults to `BTCUSDC`.

- [ ] **Step 2: Centralize default constants**

Add the constants near the existing project constants in `paired_opener/config.py`:

```python
DEFAULT_TRADING_SYMBOL = "ETHUSDC"
DEFAULT_LEVERAGE = 75
DEFAULT_ROUND_COUNT = 30
```

- [ ] **Step 3: Use the constants in schemas**

Update `paired_opener/schemas.py`:

```python
from paired_opener.config import DEFAULT_ROUND_COUNT, DEFAULT_TRADING_SYMBOL


class MarketConnectRequest(BaseModel):
    symbol: str = Field(default=DEFAULT_TRADING_SYMBOL)


class SimulationRunRequest(ExecutionPolicyFields):
    session_kind: SessionKind = SessionKind.PAIRED_OPEN
    symbol: str = Field(default=DEFAULT_TRADING_SYMBOL)
    round_count: int = Field(default=DEFAULT_ROUND_COUNT, ge=1, le=10_000)
```

- [ ] **Step 4: Verify frontend defaults remain aligned**

Confirm these values in `paired_opener/static/index.html` and `paired_opener/static/app.js`:

```html
<input id="executionSymbol" value="ETHUSDC" readonly />
<input id="leverage" type="number" value="75" min="1" />
<input id="calcRounds" type="number" min="1" step="1" value="30" />
<input id="closeRounds" type="number" min="1" step="1" value="30" />
```

```javascript
const DEFAULT_SYMBOL = "ETHUSDC";
```

If a default is missing, update only that value.

- [ ] **Step 5: Run verification and commit**

Run:

```powershell
pytest tests/test_service_config.py -q
node --check paired_opener\static\app.js
node tests\test_app_simulation_payloads.mjs
```

Expected: all commands exit 0.

Commit:

```powershell
git add paired_opener/config.py paired_opener/schemas.py paired_opener/static/index.html paired_opener/static/app.js tests/test_service_config.py tests/test_app_simulation_payloads.mjs
git commit -m "feat: align default trading parameters"
```

---

### Task 2: Add Kanglong Domain Models

**Files:**
- Create: `paired_opener/kanglong/__init__.py`
- Create: `paired_opener/kanglong/models.py`
- Test: `tests/test_kanglong_config.py`

- [ ] **Step 1: Write model serialization tests**

Create `tests/test_kanglong_config.py` with the first model checks:

```python
from decimal import Decimal

from paired_opener.domain import PositionSide
from paired_opener.kanglong.models import KanglongEvent, KanglongEventStatus, KanglongRunStatus, ResidualLedgerEntry


def test_kanglong_event_serializes_decimal_values_as_strings() -> None:
    event = KanglongEvent(
        run_id="run-1",
        group_id="group-1",
        round_id="round-1",
        mode="simulation",
        account_id="sub-1",
        symbol="ETHUSDC",
        position_side=PositionSide.LONG,
        action_type="single_close",
        leg_id="leg-close",
        paired_leg_id="leg-open",
        round_match_id="match-1",
        planned_qty=Decimal("0.010"),
        submitted_qty=Decimal("0.010"),
        filled_qty=Decimal("0.009"),
        matched_qty=Decimal("0.009"),
        close_residual_qty=Decimal("0.001"),
        open_residual_qty=Decimal("0"),
        avg_price=Decimal("3000.1"),
        status=KanglongEventStatus.PARTIAL_FILLED,
        reason="limit_order_unfilled",
    )

    payload = event.to_payload()

    assert payload["filled_qty"] == "0.009"
    assert payload["status"] == "partial_filled"
    assert payload["position_side"] == "LONG"


def test_residual_ledger_keeps_account_side_and_leg_type() -> None:
    entry = ResidualLedgerEntry(
        account_id="sub-1",
        side=PositionSide.LONG,
        leg_type="close",
        signed_qty=Decimal("0.001"),
        reason="step_size_rounding",
        event_id="event-1",
    )

    assert entry.to_payload()["signed_qty"] == "0.001"
```

Run: `pytest tests/test_kanglong_config.py -q`
Expected: FAIL because `paired_opener.kanglong.models` does not exist.

- [ ] **Step 2: Create package export**

Create `paired_opener/kanglong/__init__.py`:

```python
"""Kanglong cross-account transfer simulation package."""
```

- [ ] **Step 3: Add core enums and Decimal-safe payload helpers**

Create `paired_opener/kanglong/models.py` with these starting definitions:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from paired_opener.domain import PositionSide


def utc_now() -> datetime:
    return datetime.now(UTC)


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def payload_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return value


class KanglongRunStatus(StrEnum):
    DRAFT_PLAN = "draft_plan"
    PRECHECK = "precheck"
    CHAIN_READY = "chain_ready"
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
    PAUSED_GROUP_NOT_EXECUTABLE = "paused_group_not_executable"
    NEEDS_MARKET_REDUCE_CONFIRMATION = "needs_market_reduce_confirmation"
    NEEDS_ABORT_RECOVER = "needs_abort_recover"
    ABORT_RECOVERING = "abort_recovering"
    ABORTED_RECOVERED = "aborted_recovered"
    UNSAFE_DUST_RESIDUAL = "unsafe_dust_residual"
    UNSAFE_UNCLOSED = "unsafe_unclosed"


class KanglongResultGrade(StrEnum):
    SAFE_CLOSED = "safe_closed"
    MARKET_REDUCE_REQUIRED = "market_reduce_required"
    UNSAFE_UNCLOSED = "unsafe_unclosed"


class KanglongEventStatus(StrEnum):
    FILLED = "filled"
    PARTIAL_FILLED = "partial_filled"
    REJECTED = "rejected"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass(slots=True)
class ResidualLedgerEntry:
    account_id: str
    side: PositionSide
    leg_type: str
    signed_qty: Decimal
    reason: str
    event_id: str

    def to_payload(self) -> dict[str, Any]:
        return {name: payload_value(getattr(self, name)) for name in self.__dataclass_fields__}


@dataclass(slots=True)
class KanglongFill:
    trade_id: str
    fill_qty: Decimal
    fill_price: Decimal
    fee: Decimal
    fee_asset: str
    liquidity_role: str
    filled_at: datetime = field(default_factory=utc_now)

    def to_payload(self) -> dict[str, Any]:
        return {name: payload_value(getattr(self, name)) for name in self.__dataclass_fields__}


@dataclass(slots=True)
class KanglongEvent:
    run_id: str
    group_id: str
    round_id: str
    mode: str
    account_id: str
    symbol: str
    position_side: PositionSide
    action_type: str
    leg_id: str
    paired_leg_id: str | None
    round_match_id: str
    planned_qty: Decimal
    submitted_qty: Decimal
    filled_qty: Decimal
    matched_qty: Decimal
    close_residual_qty: Decimal
    open_residual_qty: Decimal
    avg_price: Decimal
    status: KanglongEventStatus
    reason: str | None = None
    fills: list[KanglongFill] = field(default_factory=list)
    fee: Decimal = Decimal("0")
    fee_asset: str = "USDC"
    realized_pnl: Decimal = Decimal("0")
    pnl_asset: str = "USDC"
    event_time: datetime = field(default_factory=utc_now)

    def to_payload(self) -> dict[str, Any]:
        payload = {name: payload_value(getattr(self, name)) for name in self.__dataclass_fields__ if name != "fills"}
        payload["fills"] = [item.to_payload() for item in self.fills]
        return payload
```

- [ ] **Step 4: Run model tests**

Run: `pytest tests/test_kanglong_config.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add paired_opener/kanglong/__init__.py paired_opener/kanglong/models.py tests/test_kanglong_config.py
git commit -m "feat: add kanglong domain models"
```

---

### Task 3: Add Per-Symbol Kanglong Config

**Files:**
- Modify: `paired_opener/config.py`
- Create: `paired_opener/kanglong/config.py`
- Modify: `tests/test_kanglong_config.py`

- [ ] **Step 1: Add failing config tests**

Extend `tests/test_kanglong_config.py`:

```python
from decimal import Decimal

from paired_opener.config import Settings
from paired_opener.kanglong.config import KanglongSymbolConfig, load_kanglong_symbol_config


def test_ethusdc_kanglong_config_defaults() -> None:
    config = load_kanglong_symbol_config(Settings(_env_file=None), "ETHUSDC")

    assert config.per_round_qty_limit == Decimal("0.05")
    assert config.qty_tolerance == Decimal("0.0001")
    assert config.max_rounds_per_group == 30
    assert config.max_chain_groups == 100


def test_symbol_config_file_overrides_defaults(tmp_path) -> None:
    config_file = tmp_path / "kanglong_symbol_configs.json"
    config_file.write_text(
        '{"ETHUSDC":{"per_round_qty_limit":"0.02","qty_tolerance":"0.0002","max_rounds_per_group":10}}',
        encoding="utf-8",
    )
    settings = Settings(_env_file=None, kanglong_symbol_configs_file=config_file)

    config = load_kanglong_symbol_config(settings, "ETHUSDC")

    assert config.per_round_qty_limit == Decimal("0.02")
    assert config.qty_tolerance == Decimal("0.0002")
    assert config.max_rounds_per_group == 10
```

Run: `pytest tests/test_kanglong_config.py -q`
Expected: FAIL because config loader does not exist.

- [ ] **Step 2: Add settings path**

Add to `Settings` in `paired_opener/config.py`:

```python
kanglong_symbol_configs_file: Path = CONFIG_DIR / "kanglong_symbol_configs.json"
```

- [ ] **Step 3: Implement config loader**

Create `paired_opener/kanglong/config.py`:

```python
from __future__ import annotations

import json
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

from paired_opener.config import Settings


@dataclass(frozen=True, slots=True)
class KanglongSymbolConfig:
    per_round_qty_limit: Decimal = Decimal("0.05")
    qty_tolerance: Decimal = Decimal("0.0001")
    max_rounds_per_group: int = 30
    max_chain_groups: int = 100
    max_main_temp_qty: Decimal = Decimal("1.50")
    max_main_temp_notional_ratio: Decimal = Decimal("0.80")
    price_buffer_bps: int = 5
    margin_safety_ratio: Decimal = Decimal("0.20")
    min_liquidation_buffer_ratio: Decimal = Decimal("0.15")
    snapshot_ttl_ms: int = 5000
    price_ttl_ms: int = 2000
    run_lock_ttl_ms: int = 600000
    simulation_result_ttl_ms: int = 60000


def _decimal(value: Any, default: Decimal) -> Decimal:
    if value is None:
        return default
    return Decimal(str(value))


def _int(value: Any, default: int) -> int:
    if value is None:
        return default
    return int(value)


def _apply_overrides(base: KanglongSymbolConfig, payload: dict[str, Any]) -> KanglongSymbolConfig:
    return replace(
        base,
        per_round_qty_limit=_decimal(payload.get("per_round_qty_limit"), base.per_round_qty_limit),
        qty_tolerance=_decimal(payload.get("qty_tolerance"), base.qty_tolerance),
        max_rounds_per_group=_int(payload.get("max_rounds_per_group"), base.max_rounds_per_group),
        max_chain_groups=_int(payload.get("max_chain_groups"), base.max_chain_groups),
        max_main_temp_qty=_decimal(payload.get("max_main_temp_qty"), base.max_main_temp_qty),
        max_main_temp_notional_ratio=_decimal(payload.get("max_main_temp_notional_ratio"), base.max_main_temp_notional_ratio),
        price_buffer_bps=_int(payload.get("price_buffer_bps"), base.price_buffer_bps),
        margin_safety_ratio=_decimal(payload.get("margin_safety_ratio"), base.margin_safety_ratio),
        min_liquidation_buffer_ratio=_decimal(payload.get("min_liquidation_buffer_ratio"), base.min_liquidation_buffer_ratio),
        snapshot_ttl_ms=_int(payload.get("snapshot_ttl_ms"), base.snapshot_ttl_ms),
        price_ttl_ms=_int(payload.get("price_ttl_ms"), base.price_ttl_ms),
        run_lock_ttl_ms=_int(payload.get("run_lock_ttl_ms"), base.run_lock_ttl_ms),
        simulation_result_ttl_ms=_int(payload.get("simulation_result_ttl_ms"), base.simulation_result_ttl_ms),
    )


def load_kanglong_symbol_config(settings: Settings, symbol: str) -> KanglongSymbolConfig:
    normalized = symbol.strip().upper()
    base = KanglongSymbolConfig()
    path = settings.kanglong_symbol_configs_file
    if not path.exists():
        return base
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return base
    if not isinstance(payload, dict):
        return base
    raw = payload.get(normalized)
    if not isinstance(raw, dict):
        return base
    return _apply_overrides(base, raw)
```

- [ ] **Step 4: Run config tests and commit**

Run: `pytest tests/test_kanglong_config.py -q`
Expected: PASS.

Commit:

```powershell
git add paired_opener/config.py paired_opener/kanglong/config.py tests/test_kanglong_config.py
git commit -m "feat: add kanglong symbol config"
```

---

### Task 4: Add Account Snapshot and Gateway Factory

**Files:**
- Modify: `paired_opener/account_runtime.py`
- Modify: `paired_opener/kanglong/models.py`
- Test: `tests/test_kanglong_precheck.py`

- [ ] **Step 1: Add failing disposable gateway test**

Create `tests/test_kanglong_precheck.py`:

```python
from paired_opener.account_runtime import AccountRuntimeManager
from paired_opener.config import AccountConfig, Settings
from paired_opener.storage import SqliteRepository


def test_runtime_manager_exposes_configured_kanglong_accounts(tmp_path) -> None:
    settings = Settings(_env_file=None, database_path=tmp_path / "db.sqlite3")
    settings.accounts = {
        "main": AccountConfig(account_id="main", name="主账号", api_key="k", api_secret="s"),
        "sub1": AccountConfig(account_id="sub1", name="子账号1", api_key="k", api_secret="s"),
    }
    settings.active_account_id = "main"
    manager = AccountRuntimeManager(settings, SqliteRepository(tmp_path / "db.sqlite3"))

    accounts = manager.get_accounts_by_ids(["main", "sub1"])

    assert [account.account_id for account in accounts] == ["main", "sub1"]
```

Run: `pytest tests/test_kanglong_precheck.py -q`
Expected: FAIL because `get_accounts_by_ids` does not exist.

- [ ] **Step 2: Add account lookup method**

Add to `AccountRuntimeManager`:

```python
def get_accounts_by_ids(self, account_ids: list[str]) -> list[AccountConfig]:
    accounts: list[AccountConfig] = []
    for raw_id in account_ids:
        account_id = raw_id.strip().lower()
        if account_id not in self._settings.accounts:
            raise ValueError(f"Unknown account {raw_id}")
        accounts.append(self._settings.accounts[account_id])
    return accounts
```

- [ ] **Step 3: Add snapshot dataclasses**

Extend `paired_opener/kanglong/models.py`:

```python
@dataclass(slots=True)
class KanglongPositionSnapshot:
    symbol: str
    side: PositionSide
    qty: Decimal
    entry_price: Decimal
    mark_price: Decimal
    unrealized_pnl: Decimal


@dataclass(slots=True)
class KanglongAccountSnapshot:
    account_id: str
    account_name: str
    available_balance: Decimal
    equity: Decimal
    margin: Decimal
    leverage: int
    positions: dict[PositionSide, KanglongPositionSnapshot]
    open_orders: list[dict[str, Any]]
    snapshot_version: str
    captured_at: datetime = field(default_factory=utc_now)

    def qty(self, side: PositionSide) -> Decimal:
        position = self.positions.get(side)
        return position.qty if position else Decimal("0")

    def pnl(self, side: PositionSide) -> Decimal:
        position = self.positions.get(side)
        return position.unrealized_pnl if position else Decimal("0")
```

- [ ] **Step 4: Run test and commit**

Run: `pytest tests/test_kanglong_precheck.py -q`
Expected: PASS.

Commit:

```powershell
git add paired_opener/account_runtime.py paired_opener/kanglong/models.py tests/test_kanglong_precheck.py
git commit -m "feat: expose kanglong account lookup"
```

---

### Task 5: Implement Precheck and Direction Selection

**Files:**
- Create: `paired_opener/kanglong/precheck.py`
- Modify: `paired_opener/kanglong/models.py`
- Modify: `tests/test_kanglong_precheck.py`

- [ ] **Step 1: Add failing direction and blocker tests**

Extend `tests/test_kanglong_precheck.py`:

```python
from decimal import Decimal

from paired_opener.domain import PositionSide
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import KanglongAccountSnapshot, KanglongPositionSnapshot, KanglongRunStatus
from paired_opener.kanglong.precheck import choose_selected_side, run_static_precheck


def position(symbol: str, side: PositionSide, qty: str, pnl: str) -> KanglongPositionSnapshot:
    return KanglongPositionSnapshot(
        symbol=symbol,
        side=side,
        qty=Decimal(qty),
        entry_price=Decimal("3000"),
        mark_price=Decimal("3100"),
        unrealized_pnl=Decimal(pnl),
    )


def snapshot(account_id: str, long_qty: str, short_qty: str, long_pnl: str, short_pnl: str) -> KanglongAccountSnapshot:
    return KanglongAccountSnapshot(
        account_id=account_id,
        account_name=account_id,
        available_balance=Decimal("10000"),
        equity=Decimal("10000"),
        margin=Decimal("0"),
        leverage=75,
        positions={
            PositionSide.LONG: position("ETHUSDC", PositionSide.LONG, long_qty, long_pnl),
            PositionSide.SHORT: position("ETHUSDC", PositionSide.SHORT, short_qty, short_pnl),
        },
        open_orders=[],
        snapshot_version=f"{account_id}-v1",
    )


def test_choose_selected_side_prefers_more_profitable_side() -> None:
    selected, preview = choose_selected_side(
        [snapshot("sub1", "1", "1", "10", "30")],
        manual_side=None,
        config=KanglongSymbolConfig(),
    )

    assert selected == PositionSide.SHORT
    assert preview["preview_side"] == "LONG"


def test_precheck_blocks_when_main_account_is_not_flat() -> None:
    result = run_static_precheck(
        main=snapshot("main", "0.01", "0", "0", "0"),
        subaccounts=[snapshot("sub1", "1", "1", "10", "0")],
        symbol="ETHUSDC",
        manual_side=None,
        config=KanglongSymbolConfig(),
    )

    assert result.status == KanglongRunStatus.BLOCKED_MAIN_NOT_FLAT
    assert result.reason_code == "blocked_main_not_flat"


def test_precheck_blocks_when_initial_subaccount_is_unbalanced() -> None:
    result = run_static_precheck(
        main=snapshot("main", "0", "0", "0", "0"),
        subaccounts=[snapshot("sub1", "1.5", "1", "10", "0")],
        symbol="ETHUSDC",
        manual_side=None,
        config=KanglongSymbolConfig(),
    )

    assert result.status == KanglongRunStatus.BLOCKED_INITIAL_SUBACCOUNT_UNBALANCED
```

Run: `pytest tests/test_kanglong_precheck.py -q`
Expected: FAIL because precheck functions do not exist.

- [ ] **Step 2: Add precheck result dataclass**

Add to `paired_opener/kanglong/models.py`:

```python
@dataclass(slots=True)
class KanglongPrecheckResult:
    ok: bool
    status: KanglongRunStatus
    reason_code: str | None
    selected_side: PositionSide | None
    first_donor_account_id: str | None
    planned_release_qty: Decimal
    other_side_preview: dict[str, Any]
    details: dict[str, Any] = field(default_factory=dict)
```

- [ ] **Step 3: Implement static precheck**

Create `paired_opener/kanglong/precheck.py`:

```python
from __future__ import annotations

from decimal import Decimal

from paired_opener.domain import PositionSide
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import KanglongAccountSnapshot, KanglongPrecheckResult, KanglongRunStatus


def closeable_profitable_qty(snapshot: KanglongAccountSnapshot, side: PositionSide, config: KanglongSymbolConfig) -> Decimal:
    qty = snapshot.qty(side)
    if qty <= config.qty_tolerance:
        return Decimal("0")
    if snapshot.pnl(side) <= Decimal("0"):
        return Decimal("0")
    return qty


def _side_summary(subaccounts: list[KanglongAccountSnapshot], side: PositionSide, config: KanglongSymbolConfig) -> dict[str, object]:
    profitable = [
        {
            "account_id": snapshot.account_id,
            "profit": snapshot.pnl(side),
            "closeable_qty": closeable_profitable_qty(snapshot, side, config),
        }
        for snapshot in subaccounts
    ]
    total_profit = sum((item["profit"] for item in profitable), Decimal("0"))
    total_closeable_qty = sum((item["closeable_qty"] for item in profitable), Decimal("0"))
    first = sorted(
        (item for item in profitable if item["closeable_qty"] > config.qty_tolerance),
        key=lambda item: (-item["closeable_qty"], item["account_id"]),
    )
    return {
        "side": side.value,
        "total_profit": total_profit,
        "total_closeable_qty": total_closeable_qty,
        "first_donor_account_id": first[0]["account_id"] if first else None,
    }


def choose_selected_side(
    subaccounts: list[KanglongAccountSnapshot],
    *,
    manual_side: PositionSide | None,
    config: KanglongSymbolConfig,
) -> tuple[PositionSide | None, dict[str, object]]:
    summaries = {
        PositionSide.LONG: _side_summary(subaccounts, PositionSide.LONG, config),
        PositionSide.SHORT: _side_summary(subaccounts, PositionSide.SHORT, config),
    }
    if manual_side is not None:
        selected = manual_side
    else:
        ordered = sorted(
            summaries.items(),
            key=lambda item: (-item[1]["total_profit"], -item[1]["total_closeable_qty"], item[0].value),
        )
        selected = ordered[0][0] if ordered and ordered[0][1]["total_profit"] > Decimal("0") else None
    other = PositionSide.SHORT if selected == PositionSide.LONG else PositionSide.LONG
    return selected, {"preview_side": other.value, **summaries[other]}


def run_static_precheck(
    *,
    main: KanglongAccountSnapshot,
    subaccounts: list[KanglongAccountSnapshot],
    symbol: str,
    manual_side: PositionSide | None,
    config: KanglongSymbolConfig,
) -> KanglongPrecheckResult:
    if abs(main.qty(PositionSide.LONG)) > config.qty_tolerance or abs(main.qty(PositionSide.SHORT)) > config.qty_tolerance:
        return KanglongPrecheckResult(
            ok=False,
            status=KanglongRunStatus.BLOCKED_MAIN_NOT_FLAT,
            reason_code="blocked_main_not_flat",
            selected_side=None,
            first_donor_account_id=None,
            planned_release_qty=Decimal("0"),
            other_side_preview={},
            details={"main_long_qty": main.qty(PositionSide.LONG), "main_short_qty": main.qty(PositionSide.SHORT)},
        )
    for snapshot in subaccounts:
        if abs(snapshot.qty(PositionSide.LONG) - snapshot.qty(PositionSide.SHORT)) > config.qty_tolerance:
            return KanglongPrecheckResult(
                ok=False,
                status=KanglongRunStatus.BLOCKED_INITIAL_SUBACCOUNT_UNBALANCED,
                reason_code="blocked_initial_subaccount_unbalanced",
                selected_side=None,
                first_donor_account_id=None,
                planned_release_qty=Decimal("0"),
                other_side_preview={},
                details={"account_id": snapshot.account_id},
            )
    selected_side, other_side_preview = choose_selected_side(subaccounts, manual_side=manual_side, config=config)
    if selected_side is None:
        return KanglongPrecheckResult(False, KanglongRunStatus.BLOCKED_NO_PROFITABLE_ACCOUNT, "blocked_no_profitable_account", None, None, Decimal("0"), other_side_preview)
    if manual_side is not None and sum((snapshot.pnl(selected_side) for snapshot in subaccounts), Decimal("0")) <= Decimal("0"):
        return KanglongPrecheckResult(False, KanglongRunStatus.BLOCKED_MANUAL_SIDE_NOT_PROFITABLE, "blocked_manual_side_not_profitable", selected_side, None, Decimal("0"), other_side_preview)
    donors = sorted(
        subaccounts,
        key=lambda snapshot: (-closeable_profitable_qty(snapshot, selected_side, config), snapshot.account_id),
    )
    first = donors[0]
    planned_release_qty = closeable_profitable_qty(first, selected_side, config)
    return KanglongPrecheckResult(True, KanglongRunStatus.CHAIN_READY, None, selected_side, first.account_id, planned_release_qty, other_side_preview)
```

- [ ] **Step 4: Run precheck tests and commit**

Run: `pytest tests/test_kanglong_precheck.py -q`
Expected: PASS.

Commit:

```powershell
git add paired_opener/kanglong/models.py paired_opener/kanglong/precheck.py tests/test_kanglong_precheck.py
git commit -m "feat: add kanglong precheck rules"
```

---

### Task 6: Add Capacity Checks

**Files:**
- Modify: `paired_opener/kanglong/precheck.py`
- Modify: `paired_opener/kanglong/models.py`
- Modify: `tests/test_kanglong_precheck.py`

- [ ] **Step 1: Add failing main-capacity test**

Add:

```python
def test_precheck_blocks_when_main_capacity_is_below_first_release_qty() -> None:
    config = KanglongSymbolConfig(max_main_temp_qty=Decimal("0.50"))
    result = run_static_precheck(
        main=snapshot("main", "0", "0", "0", "0"),
        subaccounts=[snapshot("sub1", "1", "1", "10", "0")],
        symbol="ETHUSDC",
        manual_side=None,
        config=config,
    )

    assert result.status == KanglongRunStatus.BLOCKED_MAIN_INSUFFICIENT_CAPACITY
    assert result.details["main_receivable_qty"] == Decimal("0.50")
    assert result.details["capacity_gap_qty"] == Decimal("0.50")
```

Run: `pytest tests/test_kanglong_precheck.py -q`
Expected: FAIL because main capacity is not evaluated.

- [ ] **Step 2: Add capacity calculation function**

Add to `paired_opener/kanglong/precheck.py`:

```python
def estimate_main_receivable_qty(main: KanglongAccountSnapshot, selected_side: PositionSide, config: KanglongSymbolConfig) -> Decimal:
    current_temp_qty = main.qty(selected_side)
    remaining_temp_qty = max(config.max_main_temp_qty - current_temp_qty, Decimal("0"))
    return remaining_temp_qty
```

Then in `run_static_precheck`, after `planned_release_qty` is computed:

```python
main_receivable_qty = estimate_main_receivable_qty(main, selected_side, config)
if main_receivable_qty + config.qty_tolerance < planned_release_qty:
    return KanglongPrecheckResult(
        ok=False,
        status=KanglongRunStatus.BLOCKED_MAIN_INSUFFICIENT_CAPACITY,
        reason_code="blocked_main_insufficient_capacity",
        selected_side=selected_side,
        first_donor_account_id=first.account_id,
        planned_release_qty=planned_release_qty,
        other_side_preview=other_side_preview,
        details={
            "main_receivable_qty": main_receivable_qty,
            "capacity_gap_qty": planned_release_qty - main_receivable_qty,
        },
    )
```

- [ ] **Step 3: Run tests and commit**

Run: `pytest tests/test_kanglong_precheck.py -q`
Expected: PASS.

Commit:

```powershell
git add paired_opener/kanglong/precheck.py tests/test_kanglong_precheck.py
git commit -m "feat: block kanglong when main capacity is insufficient"
```

---

### Task 7: Implement Deterministic Planner

**Files:**
- Create: `paired_opener/kanglong/planner.py`
- Modify: `paired_opener/kanglong/models.py`
- Test: `tests/test_kanglong_planner.py`

- [ ] **Step 1: Add failing planner tests**

Create `tests/test_kanglong_planner.py`:

```python
from decimal import Decimal

from paired_opener.domain import PositionSide
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import KanglongBatchDebtBuffer, KanglongPlanningAccount, PendingDebt
from paired_opener.kanglong.planner import build_kanglong_plan


def account(account_id: str, closeable: str, profit: str, capacity: str = "10") -> KanglongPlanningAccount:
    return KanglongPlanningAccount(
        account_id=account_id,
        closeable_qty=Decimal(closeable),
        unrealized_profit=Decimal(profit),
        receiver_capacity_qty=Decimal(capacity),
        risk_buffer=Decimal("1"),
    )


def test_planner_first_group_is_first_donor_to_main() -> None:
    plan = build_kanglong_plan(
        run_id="run-1",
        symbol="ETHUSDC",
        selected_side=PositionSide.LONG,
        main_account_id="main",
        first_donor_account_id="sub1",
        planned_release_qty=Decimal("1.0"),
        accounts=[account("sub1", "1.0", "100"), account("sub2", "0.5", "50")],
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
    )

    assert plan.groups[0].from_account_id == "sub1"
    assert plan.groups[0].to_account_id == "main"
    assert plan.groups[0].target_qty == Decimal("1.0")


def test_planner_segments_donor_batch_by_fifo_receiver_capacity() -> None:
    plan = build_kanglong_plan(
        run_id="run-1",
        symbol="ETHUSDC",
        selected_side=PositionSide.LONG,
        main_account_id="main",
        first_donor_account_id="sub1",
        planned_release_qty=Decimal("1.0"),
        accounts=[
            account("sub1", "1.0", "100"),
            account("sub2", "0.6", "90", capacity="0.4"),
            account("sub3", "0.6", "80", capacity="0.3"),
        ],
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
    )

    batch_groups = [group for group in plan.groups if group.batch_id is not None]
    assert [group.to_account_id for group in batch_groups] == ["sub1"]
    assert batch_groups[0].target_qty <= Decimal("0.4")


def test_planner_records_batch_debt_buffer_for_batch_groups() -> None:
    plan = build_kanglong_plan(
        run_id="run-1",
        symbol="ETHUSDC",
        selected_side=PositionSide.LONG,
        main_account_id="main",
        first_donor_account_id="sub1",
        planned_release_qty=Decimal("1.0"),
        accounts=[
            account("sub1", "1.0", "100"),
            account("sub2", "1.2", "90", capacity="1.2"),
        ],
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
    )

    assert plan.batch_debt_buffers
    assert isinstance(plan.batch_debt_buffers[0], KanglongBatchDebtBuffer)
    assert plan.batch_debt_buffers[0].donor_account_id == "sub2"
    assert plan.batch_debt_buffers[0].repair_status == "open"
```

Run: `pytest tests/test_kanglong_planner.py -q`
Expected: FAIL because planner models do not exist.

- [ ] **Step 2: Add planning dataclasses**

Add to `paired_opener/kanglong/models.py`:

```python
@dataclass(slots=True)
class KanglongPlanningAccount:
    account_id: str
    closeable_qty: Decimal
    unrealized_profit: Decimal
    receiver_capacity_qty: Decimal
    risk_buffer: Decimal
    has_pending_debt: bool = False


@dataclass(slots=True)
class PendingDebt:
    account_id: str
    qty: Decimal


@dataclass(slots=True)
class KanglongBatchDebtBuffer:
    batch_id: str
    donor_account_id: str
    side: PositionSide
    matched_qty: Decimal
    completed_group_ids: list[str]
    failed_group_id: str | None = None
    repair_status: str = "open"


@dataclass(slots=True)
class KanglongGroupPlan:
    group_id: str
    from_account_id: str
    to_account_id: str
    symbol: str
    side: PositionSide
    target_qty: Decimal
    round_qtys: list[Decimal]
    batch_id: str | None = None


@dataclass(slots=True)
class KanglongPlan:
    run_id: str
    symbol: str
    selected_side: PositionSide
    main_account_id: str
    groups: list[KanglongGroupPlan]
    batch_debt_buffers: list[KanglongBatchDebtBuffer]
```

- [ ] **Step 3: Implement deterministic planner**

Create `paired_opener/kanglong/planner.py` with:

```python
from __future__ import annotations

from decimal import Decimal

from paired_opener.domain import PositionSide
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import KanglongBatchDebtBuffer, KanglongGroupPlan, KanglongPlan, KanglongPlanningAccount, PendingDebt
from paired_opener.rounding import quantize_step


def split_round_qtys(target_qty: Decimal, per_round_qty_limit: Decimal) -> list[Decimal]:
    remaining = target_qty
    rounds: list[Decimal] = []
    while remaining > Decimal("0"):
        qty = min(per_round_qty_limit, remaining)
        rounds.append(qty)
        remaining -= qty
    return rounds


def _score(account: KanglongPlanningAccount) -> tuple[Decimal, Decimal, int, Decimal, str]:
    return (
        -account.unrealized_profit,
        -account.closeable_qty,
        0 if not account.has_pending_debt else 1,
        -account.risk_buffer,
        account.account_id,
    )


def _group(group_index: int, from_account_id: str, to_account_id: str, symbol: str, side: PositionSide, qty: Decimal, config: KanglongSymbolConfig, batch_id: str | None = None) -> KanglongGroupPlan:
    return KanglongGroupPlan(
        group_id=f"group-{group_index:04d}",
        from_account_id=from_account_id,
        to_account_id=to_account_id,
        symbol=symbol,
        side=side,
        target_qty=qty,
        round_qtys=split_round_qtys(qty, config.per_round_qty_limit),
        batch_id=batch_id,
    )


def build_kanglong_plan(
    *,
    run_id: str,
    symbol: str,
    selected_side: PositionSide,
    main_account_id: str,
    first_donor_account_id: str,
    planned_release_qty: Decimal,
    accounts: list[KanglongPlanningAccount],
    config: KanglongSymbolConfig,
) -> KanglongPlan:
    groups: list[KanglongGroupPlan] = []
    batch_buffers: list[KanglongBatchDebtBuffer] = []
    group_index = 1
    groups.append(_group(group_index, first_donor_account_id, main_account_id, symbol, selected_side, planned_release_qty, config))
    group_index += 1
    debts: list[PendingDebt] = [PendingDebt(first_donor_account_id, planned_release_qty)]
    donor_pool = [account for account in accounts if account.account_id != first_donor_account_id]
    for donor in sorted(donor_pool, key=_score):
        if len(groups) >= config.max_chain_groups or not debts:
            break
        if donor.closeable_qty <= config.qty_tolerance or donor.unrealized_profit <= Decimal("0"):
            continue
        remaining_donor_qty = donor.closeable_qty
        batch_id = f"batch-{group_index:04d}" if len(debts) > 1 or remaining_donor_qty > debts[0].qty else None
        while remaining_donor_qty > config.qty_tolerance and debts:
            receiver = debts[0]
            segment_qty = min(remaining_donor_qty, receiver.qty, donor.receiver_capacity_qty)
            if segment_qty <= config.qty_tolerance:
                break
            group = _group(group_index, donor.account_id, receiver.account_id, symbol, selected_side, segment_qty, config, batch_id=batch_id)
            groups.append(group)
            group_index += 1
            remaining_donor_qty -= segment_qty
            receiver.qty -= segment_qty
            if receiver.qty <= config.qty_tolerance:
                debts.pop(0)
        transferred = donor.closeable_qty - remaining_donor_qty
        if transferred > config.qty_tolerance:
            if batch_id is not None:
                batch_buffers.append(
                    KanglongBatchDebtBuffer(
                        batch_id=batch_id,
                        donor_account_id=donor.account_id,
                        side=selected_side,
                        matched_qty=transferred,
                        completed_group_ids=[group.group_id for group in groups if group.batch_id == batch_id],
                    )
                )
            debts.append(PendingDebt(donor.account_id, transferred))
    for debt in debts:
        if debt.qty > config.qty_tolerance:
            groups.append(_group(group_index, main_account_id, debt.account_id, symbol, selected_side, debt.qty, config))
            group_index += 1
    return KanglongPlan(run_id=run_id, symbol=symbol, selected_side=selected_side, main_account_id=main_account_id, groups=groups, batch_debt_buffers=batch_buffers)
```

- [ ] **Step 4: Run planner tests and commit**

Run: `pytest tests/test_kanglong_planner.py -q`
Expected: PASS.

Commit:

```powershell
git add paired_opener/kanglong/models.py paired_opener/kanglong/planner.py tests/test_kanglong_planner.py
git commit -m "feat: add kanglong deterministic planner"
```

---

### Task 8: Add Simulation Executor and Cost Reporter

**Files:**
- Create: `paired_opener/kanglong/simulator.py`
- Create: `paired_opener/kanglong/reporter.py`
- Modify: `paired_opener/kanglong/models.py`
- Test: `tests/test_kanglong_simulator.py`

- [ ] **Step 1: Add failing executor test**

Create `tests/test_kanglong_simulator.py`:

```python
from decimal import Decimal

from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import KanglongGroupPlan
from paired_opener.kanglong.simulator import simulate_group


def test_simulate_group_emits_matched_close_and_open_events() -> None:
    group = KanglongGroupPlan(
        group_id="group-0001",
        from_account_id="sub1",
        to_account_id="main",
        symbol="ETHUSDC",
        side=PositionSide.LONG,
        target_qty=Decimal("0.02"),
        round_qtys=[Decimal("0.01"), Decimal("0.01")],
    )
    rules = SymbolRules("ETHUSDC", Decimal("0.01"), Decimal("0.001"), Decimal("0.001"), Decimal("5"), 125)

    result = simulate_group(
        run_id="run-1",
        group=group,
        rules=rules,
        close_price=Decimal("3100.00"),
        open_price=Decimal("3100.50"),
        fee_rate=Decimal("0.0005"),
        config=KanglongSymbolConfig(),
    )

    assert result.matched_qty == Decimal("0.02")
    assert len(result.events) == 4
    assert result.events[0].round_match_id == result.events[1].round_match_id
```

Run: `pytest tests/test_kanglong_simulator.py -q`
Expected: FAIL because simulator does not exist.

- [ ] **Step 2: Add group result dataclass**

Add to `paired_opener/kanglong/models.py`:

```python
@dataclass(slots=True)
class KanglongGroupResult:
    group_id: str
    matched_qty: Decimal
    residual_ledger: list[ResidualLedgerEntry]
    events: list[KanglongEvent]
```

- [ ] **Step 3: Implement simulation-only group execution**

Create `paired_opener/kanglong/simulator.py`:

```python
from __future__ import annotations

from decimal import Decimal

from paired_opener.domain import OrderSide, PositionSide, SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import KanglongEvent, KanglongEventStatus, KanglongFill, KanglongGroupPlan, KanglongGroupResult, ResidualLedgerEntry, utc_now
from paired_opener.rounding import normalize_qty


def _order_side_for_close(side: PositionSide) -> OrderSide:
    return OrderSide.SELL if side == PositionSide.LONG else OrderSide.BUY


def _order_side_for_open(side: PositionSide) -> OrderSide:
    return OrderSide.BUY if side == PositionSide.LONG else OrderSide.SELL


def _price_diff_pnl(side: PositionSide, close_price: Decimal, open_price: Decimal, qty: Decimal) -> Decimal:
    if side == PositionSide.LONG:
        return (close_price - open_price) * qty
    return (open_price - close_price) * qty


def simulate_group(
    *,
    run_id: str,
    group: KanglongGroupPlan,
    rules: SymbolRules,
    close_price: Decimal,
    open_price: Decimal,
    fee_rate: Decimal,
    config: KanglongSymbolConfig,
) -> KanglongGroupResult:
    events: list[KanglongEvent] = []
    residuals: list[ResidualLedgerEntry] = []
    matched_total = Decimal("0")
    for index, planned_qty in enumerate(group.round_qtys, start=1):
        submitted_qty = normalize_qty(planned_qty, rules)
        rounding_residual = planned_qty - submitted_qty
        round_id = f"{group.group_id}-round-{index:04d}"
        match_id = f"{round_id}-match"
        matched_qty = submitted_qty
        matched_total += matched_qty
        if rounding_residual > Decimal("0"):
            residuals.append(ResidualLedgerEntry(group.from_account_id, group.side, "rounding", rounding_residual, "step_size_rounding", match_id))
        close_fee = matched_qty * close_price * fee_rate
        open_fee = matched_qty * open_price * fee_rate
        close_event = KanglongEvent(
            run_id=run_id,
            group_id=group.group_id,
            round_id=round_id,
            mode="simulation",
            account_id=group.from_account_id,
            symbol=group.symbol,
            position_side=group.side,
            action_type="single_close",
            leg_id=f"{round_id}-close",
            paired_leg_id=f"{round_id}-open",
            round_match_id=match_id,
            planned_qty=planned_qty,
            submitted_qty=submitted_qty,
            filled_qty=matched_qty,
            matched_qty=matched_qty,
            close_residual_qty=Decimal("0"),
            open_residual_qty=Decimal("0"),
            avg_price=close_price,
            status=KanglongEventStatus.FILLED,
            fee=close_fee,
            realized_pnl=_price_diff_pnl(group.side, close_price, open_price, matched_qty),
            fills=[KanglongFill(f"{round_id}-close-fill", matched_qty, close_price, close_fee, "USDC", "taker", utc_now())],
        )
        open_event = KanglongEvent(
            run_id=run_id,
            group_id=group.group_id,
            round_id=round_id,
            mode="simulation",
            account_id=group.to_account_id,
            symbol=group.symbol,
            position_side=group.side,
            action_type="single_open",
            leg_id=f"{round_id}-open",
            paired_leg_id=f"{round_id}-close",
            round_match_id=match_id,
            planned_qty=planned_qty,
            submitted_qty=submitted_qty,
            filled_qty=matched_qty,
            matched_qty=matched_qty,
            close_residual_qty=Decimal("0"),
            open_residual_qty=Decimal("0"),
            avg_price=open_price,
            status=KanglongEventStatus.FILLED,
            fee=open_fee,
            fills=[KanglongFill(f"{round_id}-open-fill", matched_qty, open_price, open_fee, "USDC", "taker", utc_now())],
        )
        events.extend([close_event, open_event])
    return KanglongGroupResult(group.group_id, matched_total, residuals, events)
```

- [ ] **Step 4: Add reporter aggregation**

Create `paired_opener/kanglong/reporter.py`:

```python
from __future__ import annotations

from decimal import Decimal
from typing import Any

from paired_opener.kanglong.models import KanglongEvent, ResidualLedgerEntry


def summarize_costs(events: list[KanglongEvent], residuals: list[ResidualLedgerEntry]) -> dict[str, Any]:
    fee_cost = sum((event.fee for event in events), Decimal("0"))
    realized_pnl = sum((event.realized_pnl for event in events), Decimal("0"))
    return {
        "transfer_fee_cost": fee_cost,
        "rebalance_fee_cost": Decimal("0"),
        "transfer_price_diff_pnl": realized_pnl,
        "transfer_price_diff_loss": max(-realized_pnl, Decimal("0")),
        "residual_count": len(residuals),
    }
```

- [ ] **Step 5: Run executor tests and commit**

Run: `pytest tests/test_kanglong_simulator.py -q`
Expected: PASS.

Commit:

```powershell
git add paired_opener/kanglong/models.py paired_opener/kanglong/simulator.py paired_opener/kanglong/reporter.py tests/test_kanglong_simulator.py
git commit -m "feat: simulate kanglong groups and costs"
```

---

### Task 9: Add Storage, Locks, and Service Orchestration

**Files:**
- Modify: `paired_opener/storage.py`
- Create: `paired_opener/kanglong/service.py`
- Modify: `tests/test_kanglong_simulator.py`

- [ ] **Step 1: Add failing storage/service test**

Add:

```python
from paired_opener.kanglong.service import KanglongSimulationService
from paired_opener.storage import SqliteRepository


def test_kanglong_service_persists_run_payload(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)

    payload = service.create_draft_run(
        run_id="run-1",
        symbol="ETHUSDC",
        main_account_id="main",
        subaccount_ids=["sub1", "sub2"],
    )

    stored = repository.get_kanglong_run("run-1")
    assert payload["run_id"] == "run-1"
    assert stored["status"] == "draft_plan"
```

Run: `pytest tests/test_kanglong_simulator.py -q`
Expected: FAIL because service/storage methods do not exist.

- [ ] **Step 2: Add SQLite tables**

Extend `SqliteRepository._initialize()`:

```sql
CREATE TABLE IF NOT EXISTS kanglong_runs (
    run_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    main_account_id TEXT NOT NULL,
    subaccount_ids_json TEXT NOT NULL,
    status TEXT NOT NULL,
    result_grade TEXT,
    request_json TEXT NOT NULL,
    plan_json TEXT NOT NULL DEFAULT '{}',
    report_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS kanglong_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    group_id TEXT,
    round_id TEXT,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS kanglong_locks (
    lock_scope TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
```

Add repository methods:

```python
def create_kanglong_run(self, payload: dict[str, Any]) -> None: ...
def get_kanglong_run(self, run_id: str) -> dict[str, Any]: ...
def update_kanglong_run(self, run_id: str, *, status: str, plan: dict[str, Any] | None = None, report: dict[str, Any] | None = None, result_grade: str | None = None) -> None: ...
def add_kanglong_event(self, run_id: str, event_type: str, payload: dict[str, Any], *, group_id: str | None = None, round_id: str | None = None) -> int: ...
```

Use the existing `_json_dumps()` and `_json_load()` helpers for all JSON columns.

- [ ] **Step 3: Add service draft and lock skeleton**

Create `paired_opener/kanglong/service.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from paired_opener.kanglong.models import KanglongRunStatus
from paired_opener.storage import SqliteRepository


def _now_text() -> str:
    return datetime.now(UTC).isoformat()


class KanglongSimulationService:
    def __init__(self, repository: SqliteRepository) -> None:
        self._repository = repository

    def create_draft_run(self, *, run_id: str, symbol: str, main_account_id: str, subaccount_ids: list[str]) -> dict[str, Any]:
        payload = {
            "run_id": run_id,
            "symbol": symbol,
            "main_account_id": main_account_id,
            "subaccount_ids": subaccount_ids,
            "status": KanglongRunStatus.DRAFT_PLAN.value,
            "created_at": _now_text(),
            "updated_at": _now_text(),
        }
        self._repository.create_kanglong_run(payload)
        return payload
```

- [ ] **Step 4: Run service tests and commit**

Run: `pytest tests/test_kanglong_simulator.py -q`
Expected: PASS.

Commit:

```powershell
git add paired_opener/storage.py paired_opener/kanglong/service.py tests/test_kanglong_simulator.py
git commit -m "feat: persist kanglong simulation runs"
```

---

### Task 10: Add API Schemas and Endpoints

**Files:**
- Modify: `paired_opener/schemas.py`
- Modify: `paired_opener/api.py`
- Modify: `paired_opener/account_runtime.py`
- Test: `tests/test_kanglong_api.py`

- [ ] **Step 1: Add failing API schema tests**

Create `tests/test_kanglong_api.py`:

```python
from paired_opener.schemas import KanglongSimulationRunRequest


def test_kanglong_request_defaults_to_ethusdc_and_auto_side() -> None:
    request = KanglongSimulationRunRequest(main_account_id="main", subaccount_ids=["sub1"])

    assert request.symbol == "ETHUSDC"
    assert request.selected_side is None
    assert request.mode == "simulation"
```

Run: `pytest tests/test_kanglong_api.py -q`
Expected: FAIL because schema does not exist.

- [ ] **Step 2: Add schemas**

Add to `paired_opener/schemas.py`:

```python
class KanglongSimulationRunRequest(BaseModel):
    mode: str = Field(default="simulation", pattern="^simulation$")
    symbol: str = Field(default=DEFAULT_TRADING_SYMBOL)
    main_account_id: str
    subaccount_ids: list[str] = Field(..., min_length=1)
    selected_side: PositionSide | None = None


class KanglongSimulationRunResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    run_id: str
    status: str
    result_grade: str | None = None
    report: dict[str, Any] = Field(default_factory=dict)
```

- [ ] **Step 3: Wire service into app state**

In `create_app()` after repository and runtime manager are created:

```python
from paired_opener.kanglong.service import KanglongSimulationService

app.state.kanglong_service = KanglongSimulationService(repository)
```

- [ ] **Step 4: Add endpoints**

Add to `paired_opener/api.py`:

```python
@app.post('/kanglong/simulation/run', response_model=KanglongSimulationRunResponse)
async def run_kanglong_simulation(request: KanglongSimulationRunRequest) -> KanglongSimulationRunResponse:
    if request.mode != "simulation":
        raise HTTPException(status_code=400, detail={"code": "kanglong_live_mode_not_supported"})
    run_id = str(uuid4())
    payload = app.state.kanglong_service.create_draft_run(
        run_id=run_id,
        symbol=request.symbol,
        main_account_id=request.main_account_id,
        subaccount_ids=request.subaccount_ids,
    )
    return KanglongSimulationRunResponse(run_id=run_id, status=payload["status"], report={})


@app.get('/kanglong/simulation/run/{run_id}')
async def get_kanglong_simulation(run_id: str) -> dict:
    return app.state.kanglong_service.get_run(run_id)
```

Import `uuid4` at the top of `paired_opener/api.py`.

- [ ] **Step 5: Run API tests and commit**

Run:

```powershell
pytest tests/test_kanglong_api.py -q
python -m compileall paired_opener
```

Expected: all commands exit 0.

Commit:

```powershell
git add paired_opener/schemas.py paired_opener/api.py paired_opener/account_runtime.py tests/test_kanglong_api.py
git commit -m "feat: add kanglong simulation api"
```

---

### Task 11: Add i18n Contract

**Files:**
- Modify: `i18n/messages/zh-CN.json`
- Modify: `i18n/registry/reasons.json`
- Modify: `i18n/registry/events.json`
- Modify: `i18n/registry/logs.json`
- Modify: `i18n/registry/precheck.json`
- Create: `tests/test_kanglong_i18n_contracts.py`

- [ ] **Step 1: Add failing i18n tests**

Create `tests/test_kanglong_i18n_contracts.py`:

```python
from app_i18n.runtime import event_registry, log_registry, messages, precheck_registry, reason_registry


def test_kanglong_i18n_messages_and_registries_exist() -> None:
    catalog = messages()
    required_message_keys = {
        "console.kanglong.title",
        "console.kanglong.run_simulation",
        "console.kanglong.report.result_grade",
        "runtime.kanglong.status.blocked_main_not_flat",
        "reasons.kanglong.blocked_main_not_flat",
        "events.kanglong.round_completed",
        "log.kanglong.abort_recovered",
        "precheck.kanglong.main_flat.fail",
    }

    assert required_message_keys.issubset(catalog)
    assert reason_registry()["kanglong.blocked_main_not_flat"]["key"] == "reasons.kanglong.blocked_main_not_flat"
    assert event_registry()["kanglong.round_completed"]["key"] == "events.kanglong.round_completed"
    assert log_registry()["kanglong.abort_recovered"]["key"] == "log.kanglong.abort_recovered"
    assert precheck_registry()["kanglong.main_flat"]["fail_key"] == "precheck.kanglong.main_flat.fail"
```

Run: `pytest tests/test_kanglong_i18n_contracts.py -q`
Expected: FAIL because keys are missing.

- [ ] **Step 2: Add zh-CN messages**

Add keys with complete Chinese templates:

```json
"console.kanglong.title": "亢龙有悔移仓模拟",
"console.kanglong.run_simulation": "开始模拟",
"console.kanglong.report.result_grade": "结果等级：{result_grade}",
"runtime.kanglong.status.blocked_main_not_flat": "主账号本交易对不是空仓",
"reasons.kanglong.blocked_main_not_flat": "主账号当前 {symbol} 持仓未清空，多仓 {long_qty}，空仓 {short_qty}，容忍数量 {qty_tolerance}。",
"events.kanglong.round_completed": "亢龙第 {group_id} 组第 {round_id} 轮完成，配对数量 {matched_qty}",
"log.kanglong.abort_recovered": "亢龙运行 {run_id} 已完成人工恢复，释放锁原因：{release_reason}",
"precheck.kanglong.main_flat.fail": "主账号存在本交易对持仓，需要先清空。"
```

- [ ] **Step 3: Add registry entries**

Add these records:

```json
"kanglong.blocked_main_not_flat": {
  "key": "reasons.kanglong.blocked_main_not_flat",
  "params": ["symbol", "long_qty", "short_qty", "qty_tolerance"]
}
```

```json
"kanglong.round_completed": {
  "key": "events.kanglong.round_completed",
  "params": ["group_id", "round_id", "matched_qty"]
}
```

```json
"kanglong.abort_recovered": {
  "key": "log.kanglong.abort_recovered",
  "params": ["run_id", "release_reason"]
}
```

```json
"kanglong.main_flat": {
  "label_key": "precheck.labels.kanglong.main_flat",
  "fail_key": "precheck.kanglong.main_flat.fail"
}
```

Also add `"precheck.labels.kanglong.main_flat": "主账号空仓"` to `zh-CN.json`.

- [ ] **Step 4: Run i18n tests and commit**

Run:

```powershell
pytest tests/test_i18n_contracts.py tests/test_kanglong_i18n_contracts.py -q
```

Expected: PASS and no mojibake assertions fail.

Commit:

```powershell
git add i18n/messages/zh-CN.json i18n/registry/reasons.json i18n/registry/events.json i18n/registry/logs.json i18n/registry/precheck.json tests/test_kanglong_i18n_contracts.py
git commit -m "feat: add kanglong i18n contract"
```

---

### Task 12: Add Frontend Simulation Panel and Report Rendering

**Files:**
- Modify: `paired_opener/static/index.html`
- Modify: `paired_opener/static/app.js`
- Test: `tests/test_app_kanglong_display.mjs`

- [ ] **Step 1: Add failing frontend rendering test**

Create `tests/test_app_kanglong_display.mjs`:

```javascript
const fs = require("fs");
const vm = require("vm");

const source = fs.readFileSync("paired_opener/static/app.js", "utf8");

if (!source.includes("console.kanglong.title")) {
  throw new Error("Kanglong title must be rendered through i18n key");
}
if (source.includes("主账号本交易对不是空仓")) {
  throw new Error("Kanglong user-facing Chinese text must not be hard-coded in app.js");
}
if (!source.includes("/kanglong/simulation/run")) {
  throw new Error("Kanglong simulation endpoint is not wired");
}
```

Run: `node tests\test_app_kanglong_display.mjs`
Expected: FAIL because frontend wiring is missing.

- [ ] **Step 2: Add panel markup**

Add a Kanglong section in `paired_opener/static/index.html` near the simulation controls:

```html
<section class="panel kanglong-panel" id="kanglongPanel">
  <div class="panel-header">
    <h2 data-i18n="console.kanglong.title">亢龙有悔移仓模拟</h2>
  </div>
  <div class="form-grid">
    <label for="kanglongMainAccount" data-i18n="console.kanglong.main_account">主账号</label>
    <select id="kanglongMainAccount"></select>
    <label for="kanglongSubaccounts" data-i18n="console.kanglong.subaccounts">子账号</label>
    <select id="kanglongSubaccounts" multiple></select>
    <label for="kanglongSelectedSide" data-i18n="console.kanglong.selected_side">盈利方向</label>
    <select id="kanglongSelectedSide">
      <option value="" data-i18n="console.kanglong.side_auto">自动选择</option>
      <option value="LONG" data-i18n="console.position_side.long">多</option>
      <option value="SHORT" data-i18n="console.position_side.short">空</option>
    </select>
  </div>
  <button id="kanglongRunSimulation" type="button" data-i18n="console.kanglong.run_simulation">开始模拟</button>
  <div id="kanglongReport" class="kanglong-report"></div>
</section>
```

- [ ] **Step 3: Add JS state and request**

Add to `paired_opener/static/app.js`:

```javascript
const kanglongMainAccount = document.getElementById("kanglongMainAccount");
const kanglongSubaccounts = document.getElementById("kanglongSubaccounts");
const kanglongSelectedSide = document.getElementById("kanglongSelectedSide");
const kanglongRunSimulation = document.getElementById("kanglongRunSimulation");
const kanglongReport = document.getElementById("kanglongReport");

async function runKanglongSimulation() {
  const subaccountIds = Array.from(kanglongSubaccounts.selectedOptions || []).map((option) => option.value);
  const payload = {
    mode: "simulation",
    symbol: executionSymbol.value || DEFAULT_SYMBOL,
    main_account_id: kanglongMainAccount.value,
    subaccount_ids: subaccountIds,
    selected_side: kanglongSelectedSide.value || null
  };
  const response = await fetch("/kanglong/simulation/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!response.ok) {
    throw new Error(copyOrDefault("runtime.kanglong.request_failed", "亢龙模拟请求失败"));
  }
  renderKanglongReport(await response.json());
}

function renderKanglongReport(payload) {
  if (!kanglongReport) return;
  const report = payload.report || {};
  const grade = payload.result_grade || "--";
  kanglongReport.textContent = copyOrDefault("console.kanglong.report.result_grade", "结果等级：{result_grade}", {
    result_grade: grade
  });
}

if (kanglongRunSimulation) {
  kanglongRunSimulation.addEventListener("click", () => {
    runKanglongSimulation().catch((error) => appendLog("error", error.message));
  });
}
```

- [ ] **Step 4: Run frontend checks and commit**

Run:

```powershell
node --check paired_opener\static\app.js
node tests\test_app_kanglong_display.mjs
```

Expected: PASS.

Commit:

```powershell
git add paired_opener/static/index.html paired_opener/static/app.js tests/test_app_kanglong_display.mjs
git commit -m "feat: add kanglong simulation panel"
```

---

### Task 13: End-to-End Simulation Assembly

**Files:**
- Modify: `paired_opener/kanglong/service.py`
- Modify: `paired_opener/kanglong/precheck.py`
- Modify: `paired_opener/kanglong/planner.py`
- Modify: `paired_opener/kanglong/simulator.py`
- Modify: `paired_opener/kanglong/reporter.py`
- Modify: `tests/test_kanglong_api.py`

- [ ] **Step 1: Add failing assembled-service test**

Add to `tests/test_kanglong_api.py`:

```python
from decimal import Decimal

from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.service import KanglongSimulationService
from paired_opener.storage import SqliteRepository
from tests.test_kanglong_precheck import snapshot


def test_kanglong_service_report_contains_plan_events_and_costs(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)
    service.create_draft_run(
        run_id="run-1",
        symbol="ETHUSDC",
        main_account_id="main",
        subaccount_ids=["sub1", "sub2"],
    )

    payload = service.simulate(
        run_id="run-1",
        symbol="ETHUSDC",
        main_snapshot=snapshot("main", "0", "0", "0", "0"),
        subaccount_snapshots=[
            snapshot("sub1", "1", "1", "100", "0"),
            snapshot("sub2", "1", "1", "80", "0"),
        ],
        selected_side=PositionSide.LONG,
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
        rules=SymbolRules("ETHUSDC", Decimal("0.01"), Decimal("0.001"), Decimal("0.001"), Decimal("5"), 125),
        close_price=Decimal("3100.00"),
        open_price=Decimal("3100.50"),
        fee_rate=Decimal("0.0005"),
    )

    assert payload["result_grade"] == "safe_closed"
    assert payload["report"]["selected_side"] == "LONG"
    assert payload["report"]["groups"][0]["from_account_id"] == "sub1"
    assert payload["report"]["costs"]["transfer_fee_cost"] != "0"
```

- [ ] **Step 2: Add service orchestration method**

Add a `simulate()` method to `KanglongSimulationService`:

```python
def simulate(
    self,
    *,
    run_id: str,
    symbol: str,
    main_snapshot: KanglongAccountSnapshot,
    subaccount_snapshots: list[KanglongAccountSnapshot],
    selected_side: PositionSide | None,
    config: KanglongSymbolConfig,
    rules: SymbolRules,
    close_price: Decimal,
    open_price: Decimal,
    fee_rate: Decimal,
) -> dict[str, Any]:
    precheck = run_static_precheck(
        main=main_snapshot,
        subaccounts=subaccount_snapshots,
        symbol=symbol,
        manual_side=selected_side,
        config=config,
    )
    if not precheck.ok:
        report = {"precheck": precheck.details, "other_side_preview": precheck.other_side_preview}
        self._repository.update_kanglong_run(run_id, status=precheck.status.value, report=report, result_grade="unsafe_unclosed")
        return {"run_id": run_id, "status": precheck.status.value, "result_grade": "unsafe_unclosed", "report": report}
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
    events = []
    residuals = []
    for group in plan.groups:
        result = simulate_group(run_id=run_id, group=group, rules=rules, close_price=close_price, open_price=open_price, fee_rate=fee_rate, config=config)
        events.extend(result.events)
        residuals.extend(result.residual_ledger)
    costs = summarize_costs(events, residuals)
    report = {"selected_side": precheck.selected_side.value, "groups": [group.__dict__ for group in plan.groups], "costs": costs, "residual_ledger": [entry.to_payload() for entry in residuals]}
    self._repository.update_kanglong_run(run_id, status="completed", report=report, result_grade="safe_closed")
    return {"run_id": run_id, "status": "completed", "result_grade": "safe_closed", "report": report}
```

The implementation must import the referenced model classes and add a small `build_planning_accounts()` helper in `planner.py`.

- [ ] **Step 3: Run assembled tests and commit**

Run:

```powershell
pytest tests/test_kanglong_precheck.py tests/test_kanglong_planner.py tests/test_kanglong_simulator.py tests/test_kanglong_api.py -q
```

Expected: PASS.

Commit:

```powershell
git add paired_opener/kanglong tests/test_kanglong_api.py
git commit -m "feat: assemble kanglong simulation report"
```

---

### Task 14: Final Regression Pass

**Files:**
- No new files.

- [ ] **Step 1: Run backend targeted tests**

Run:

```powershell
pytest tests/test_kanglong_config.py tests/test_kanglong_precheck.py tests/test_kanglong_planner.py tests/test_kanglong_simulator.py tests/test_kanglong_api.py tests/test_kanglong_i18n_contracts.py -q
```

Expected: PASS.

- [ ] **Step 2: Run existing simulation and i18n regression tests**

Run:

```powershell
pytest tests/test_simulation_service.py tests/test_simulation_api.py tests/test_i18n_contracts.py tests/test_service_config.py -q
```

Expected: PASS.

- [ ] **Step 3: Run frontend checks**

Run:

```powershell
node --check paired_opener\static\app.js
node tests\test_app_simulation_payloads.mjs
node tests\test_app_kanglong_display.mjs
```

Expected: PASS.

- [ ] **Step 4: Compile package**

Run:

```powershell
python -m compileall paired_opener app_i18n
```

Expected: PASS.

- [ ] **Step 5: Commit final verification notes if a test file or doc changed**

If verification required a code or doc adjustment, commit the adjustment:

```powershell
git status --short
git add <changed-files-from-this-task>
git commit -m "test: verify kanglong simulation flow"
```

If no files changed, record the command outputs in the final implementation response.

---

## Implementation Order Checklist

- [ ] Task 1: Defaults.
- [ ] Task 2: Domain models.
- [ ] Task 3: Symbol config.
- [ ] Task 4: Account lookup and snapshots.
- [ ] Task 5: Precheck and direction.
- [ ] Task 6: Capacity checks.
- [ ] Task 7: Planner.
- [ ] Task 8: Simulator and reporter.
- [ ] Task 9: Storage and service skeleton.
- [ ] Task 10: API.
- [ ] Task 11: i18n.
- [ ] Task 12: Frontend panel.
- [ ] Task 13: End-to-end assembly.
- [ ] Task 14: Regression pass.

## Risk Controls

- Every task starts with a failing test.
- Every task commits only the files listed for that task.
- Kanglong simulation cannot call `place_limit_order()` or `place_market_order()` in first version.
- `mode` is constrained to `"simulation"` until a separate live-mode plan is approved.
- `aborted_recovered` remains an audit recovery state, not a successful simulation result.
- Market reduce output remains a proposal, not an executable action.
- New user-visible strings must be added to `zh-CN.json` and registry files before frontend rendering uses them.

## Self-Review Notes

- Spec coverage: defaults, precheck, first donor, deterministic planner, FIFO debt queue, donor batch, per-round quantity limit, Decimal rounding, residual ledger, cost report, abort/recover states, i18n rules, simulation-only scope, and future live gating are represented by tasks.
- Red-flag scan: this plan avoids unresolved implementation markers and names concrete files, commands, and expected results.
- Type consistency: plan uses `PositionSide`, `SymbolRules`, `Decimal`, `KanglongSymbolConfig`, `KanglongEvent`, and `KanglongPlan` consistently across tasks.
