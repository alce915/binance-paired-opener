from __future__ import annotations

import asyncio
import csv
import io
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Awaitable, Callable
from uuid import uuid4

from app_i18n.runtime import CATALOG_VERSION, CONTRACT_VERSION, format_copy
from paired_opener.domain import OrderSide, PositionSide, SessionKind, SingleCloseMode, SingleOpenMode, SymbolRules, TrendBias
from paired_opener.exchange import ExchangeGateway
from paired_opener.rounding import normalize_qty
from paired_opener.schemas import SimulationRunRequest
from paired_opener.storage import SqliteRepository


SIMULATION_SCHEMA_VERSION = 1
SIMULATION_ENGINE_VERSION = "sim-engine-v1"
DEFAULT_INITIAL_BALANCE = Decimal("7000")
DEFAULT_MAKER_FEE_RATE = Decimal("0")
DEFAULT_TAKER_FEE_RATE = Decimal("0.0005")
MONEY_SCALE = Decimal("0.000000001")
ORDERBOOK_MAX_AGE_SECONDS = 5
SIMULATION_MAKER_POLL_SECONDS = Decimal("0.5")
SIMULATION_LIMIT_ORDER_WAIT_SECONDS = Decimal("10")
MAX_SIMULATION_DURATION_SECONDS = 2 * 60 * 60

Publisher = Callable[[str, dict[str, Any]], Awaitable[None]]


class SimulationError(RuntimeError):
    pass


class SimulationStopReason:
    RUNNING = "running"
    FILLED = "filled"
    USER_ABORTED = "user_aborted"
    INSUFFICIENT_SIM_BALANCE = "insufficient_sim_balance"
    INSUFFICIENT_SIM_POSITION = "insufficient_sim_position"
    STALE_ORDERBOOK = "stale_orderbook"
    MIN_NOTIONAL_BLOCKED = "min_notional_blocked"
    LIMIT_ORDER_UNFILLED = "limit_order_unfilled"
    PRICE_GUARD_BLOCKED = "price_guard_blocked"
    SIMULATION_ACCOUNT_INVARIANT_FAILED = "simulation_account_invariant_failed"
    MAX_EXTENSION_ROUNDS_REACHED = "max_extension_rounds_reached"
    MAX_SIMULATION_DURATION_REACHED = "max_simulation_duration_reached"
    INTERRUPTED = "interrupted"
    EXCEPTION = "exception"


class SimulationRunStage:
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    WAITING_FILL = "waiting_fill"
    WAITING_INTERVAL = "waiting_interval"
    FINALIZING = "finalizing"
    ABORTING = "aborting"
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"


class SimulationLockReason:
    SIMULATION_RUNNING = "simulation_running"


@dataclass(slots=True)
class MatchResult:
    requested_qty: Decimal
    filled_qty: Decimal
    avg_price: Decimal
    notional: Decimal
    fee: Decimal
    residual_qty: Decimal
    depth_levels_consumed: int
    slippage_bps: Decimal
    liquidity: str
    side: OrderSide
    position_side: PositionSide
    wait_seconds_consumed: Decimal = Decimal("0")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _json_dumps(payload: dict[str, Any]) -> str:
    def encode(value: Any) -> Any:
        if isinstance(value, Decimal):
            return _format_decimal(value)
        if isinstance(value, datetime):
            return value.isoformat()
        if hasattr(value, "value"):
            return value.value
        return value

    return json.dumps(payload, default=encode, ensure_ascii=True, sort_keys=True)


def _json_load(payload: str | None, default: Any) -> Any:
    if not payload:
        return default
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return default


def _to_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_SCALE)


def _format_decimal(value: Decimal) -> str:
    return format(value, "f")


def _format_plain_decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _format_account_money(value: Decimal) -> str:
    if value == value.to_integral():
        return _format_plain_decimal(value)
    return _format_decimal(value)


def _enum_value(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "value"):
        return str(value.value)
    return str(value)


class SimulationService:
    def __init__(
        self,
        gateway: ExchangeGateway,
        repository: SqliteRepository,
        *,
        publisher: Publisher | None = None,
        account_id: str = "simulation",
    ) -> None:
        self._gateway = gateway
        self._repository = repository
        self._publisher = publisher
        self._account_id = account_id
        self._lock = asyncio.Lock()
        self._abort_event = asyncio.Event()
        self._abort_requested = False
        self._active_run_id: str | None = None
        self._active_task: asyncio.Task[dict[str, Any]] | None = None
        self._active_request: SimulationRunRequest | None = None
        self._active_stage = SimulationRunStage.IDLE
        self._active_status = "idle"
        self._active_heartbeat_at: str | None = None
        self._active_last_event_at: str | None = None
        self._active_latest_event_id = 0
        self._initialize_storage()
        self._ensure_default_settings()
        self._mark_interrupted_runs()

    def is_active(self) -> bool:
        return self._lock.locked() or (self._active_task is not None and not self._active_task.done())

    def _ensure_not_active(self) -> None:
        if self.is_active():
            raise SimulationError(format_copy("runtime.simulation_busy"))

    async def publish_state(self) -> None:
        await self._publish("simulation_account", await self.get_account())

    async def get_account(self) -> dict[str, Any]:
        settings = self._get_settings()
        self._reconcile_position_margins()
        positions = self._list_positions()
        wallet_balance = self._wallet_balance(settings["initial_balance"])
        margin = _money(sum((_to_decimal(position["margin"]) for position in positions), Decimal("0")))
        position_payloads: list[dict[str, Any]] = []
        unrealized_pnl = Decimal("0")
        for position in positions:
            payload = await self._serialize_position(position)
            position_payloads.append(payload)
            unrealized_pnl += _to_decimal(payload.get("unrealized_pnl"))
        unrealized_pnl = _money(unrealized_pnl)
        equity = _money(wallet_balance + unrealized_pnl)
        available_balance = _money(equity - margin)
        return {
            "contract_version": CONTRACT_VERSION,
            "simulation_schema_version": SIMULATION_SCHEMA_VERSION,
            "engine_version": SIMULATION_ENGINE_VERSION,
            "settings": {
                "initial_balance": _format_decimal(settings["initial_balance"]),
                "maker_fee_rate": _format_decimal(settings["maker_fee_rate"]),
                "taker_fee_rate": _format_decimal(settings["taker_fee_rate"]),
            },
            "totals": {
                "wallet_balance": _format_account_money(wallet_balance),
                "available_balance": _format_account_money(available_balance),
                "margin": _format_account_money(margin),
                "equity": _format_account_money(equity),
                "unrealized_pnl": _format_account_money(_money(unrealized_pnl)),
            },
            "positions": position_payloads,
            "updated_at": _utc_now().isoformat(),
        }

    async def update_account_settings(
        self,
        *,
        initial_balance: Decimal | None = None,
        maker_fee_rate: Decimal | None = None,
        taker_fee_rate: Decimal | None = None,
    ) -> dict[str, Any]:
        self._ensure_not_active()
        current = self._get_settings()
        next_initial = _to_decimal(initial_balance, current["initial_balance"])
        next_maker = _to_decimal(maker_fee_rate, current["maker_fee_rate"])
        next_taker = _to_decimal(taker_fee_rate, current["taker_fee_rate"])
        if next_initial <= Decimal("0"):
            raise ValueError("initial_balance must be greater than zero")
        if next_maker < Decimal("0") or next_taker < Decimal("0"):
            raise ValueError("fee rates must be non-negative")
        before = await self.get_account()
        current_wallet = _to_decimal(before.get("totals", {}).get("wallet_balance"))
        initial_changed = next_initial != current["initial_balance"]
        run_id = str(uuid4()) if initial_changed else None
        now = _utc_now().isoformat()
        created_at = _utc_now()
        with self._repository._lock, self._repository._connection:
            self._repository._connection.execute(
                """
                INSERT INTO simulation_account_settings (
                    account_id, initial_balance, maker_fee_rate, taker_fee_rate, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    initial_balance = excluded.initial_balance,
                    maker_fee_rate = excluded.maker_fee_rate,
                    taker_fee_rate = excluded.taker_fee_rate,
                    updated_at = excluded.updated_at
                """,
                (self._account_id, str(next_initial), str(next_maker), str(next_taker), now),
            )
            if initial_changed and run_id is not None:
                offset = _money(current["initial_balance"] - next_initial)
                request = {
                    "initial_balance": _format_decimal(next_initial),
                    "maker_fee_rate": _format_decimal(next_maker),
                    "taker_fee_rate": _format_decimal(next_taker),
                }
                result = {
                    "status": "completed",
                    "stop_reason": SimulationStopReason.FILLED,
                    "wallet_balance_preserved": _format_decimal(current_wallet),
                }
                self._insert_run_locked(
                    run_id=run_id,
                    event_type="account_settings_update",
                    request=request,
                    result=result,
                    status="completed",
                    stop_reason=SimulationStopReason.FILLED,
                    created_at=created_at,
                )
                self._insert_ledger_locked(
                    run_id=run_id,
                    event_type="initial_balance_setting_changed",
                    amount=offset,
                    balance_after=current_wallet,
                    payload={
                        "previous_initial_balance": current["initial_balance"],
                        "next_initial_balance": next_initial,
                        "wallet_balance_preserved": current_wallet,
                    },
                    created_at=created_at,
                )
                self._insert_snapshot_locked(run_id, "before_settings_update", before, created_at)
        account = await self.get_account()
        await self._publish("simulation_account", account)
        if run_id is not None:
            await self._publish("simulation_run", {"run_id": run_id, "event_type": "account_settings_update", "account": account})
        return account

    async def reset_account(self) -> dict[str, Any]:
        self._ensure_not_active()
        settings = self._get_settings()
        run_id = str(uuid4())
        now = _utc_now()
        snapshot = await self.get_account()
        current_wallet = _to_decimal(snapshot.get("totals", {}).get("wallet_balance"))
        reset_adjustment = settings["initial_balance"] - current_wallet
        with self._repository._lock, self._repository._connection:
            self._repository._connection.execute(
                "DELETE FROM simulation_positions WHERE account_id = ?",
                (self._account_id,),
            )
            self._insert_run_locked(
                run_id=run_id,
                event_type="account_reset",
                request={},
                result={"status": "completed", "stop_reason": SimulationStopReason.FILLED},
                status="completed",
                stop_reason=SimulationStopReason.FILLED,
                created_at=now,
            )
            self._insert_ledger_locked(
                run_id=run_id,
                event_type="account_reset",
                amount=reset_adjustment,
                balance_after=settings["initial_balance"],
                payload={"before": snapshot, "initial_balance": settings["initial_balance"], "reset_adjustment": reset_adjustment},
                created_at=now,
            )
            self._insert_snapshot_locked(run_id, "before_reset", snapshot, now)
        account = await self.get_account()
        await self._publish("simulation_account", account)
        await self._publish("simulation_run", {"run_id": run_id, "event_type": "account_reset", "account": account})
        return {"run_id": run_id, "account": account}

    async def clear_history(self) -> dict[str, Any]:
        self._ensure_not_active()
        with self._repository._lock, self._repository._connection:
            for table in ("simulation_events", "simulation_fills", "simulation_snapshots", "simulation_runs"):
                self._repository._connection.execute(f"DELETE FROM {table} WHERE account_id = ?", (self._account_id,))
        payload = {"cleared": True, "updated_at": _utc_now().isoformat()}
        await self._publish("simulation_history", payload)
        return payload

    async def list_history(self, *, page: int = 1, page_size: int = 20) -> dict[str, Any]:
        page = max(int(page), 1)
        page_size = min(max(int(page_size), 1), 200)
        offset = (page - 1) * page_size
        total_row = self._repository._connection.execute(
            "SELECT COUNT(*) AS total FROM simulation_runs WHERE account_id = ?",
            (self._account_id,),
        ).fetchone()
        rows = self._repository._connection.execute(
            """
            SELECT *
            FROM simulation_runs
            WHERE account_id = ?
            ORDER BY created_at DESC, rowid DESC
            LIMIT ? OFFSET ?
            """,
            (self._account_id, page_size, offset),
        ).fetchall()
        return {
            "page": page,
            "page_size": page_size,
            "total": int(total_row["total"] or 0) if total_row else 0,
            "items": [self._serialize_run(row) for row in rows],
        }

    async def get_history_detail(self, run_id: str) -> dict[str, Any]:
        row = self._repository._connection.execute(
            "SELECT * FROM simulation_runs WHERE account_id = ? AND run_id = ?",
            (self._account_id, run_id),
        ).fetchone()
        if row is None:
            raise KeyError(run_id)
        fills = self._repository._connection.execute(
            "SELECT * FROM simulation_fills WHERE account_id = ? AND run_id = ? ORDER BY fill_id ASC",
            (self._account_id, run_id),
        ).fetchall()
        snapshots = self._repository._connection.execute(
            "SELECT * FROM simulation_snapshots WHERE account_id = ? AND run_id = ? ORDER BY snapshot_id ASC",
            (self._account_id, run_id),
        ).fetchall()
        ledger = self._repository._connection.execute(
            "SELECT * FROM simulation_ledger WHERE account_id = ? AND run_id = ? ORDER BY ledger_id ASC",
            (self._account_id, run_id),
        ).fetchall()
        payload = self._serialize_run(row)
        payload["fills"] = [self._serialize_fill(fill) for fill in fills]
        payload["snapshots"] = [self._serialize_snapshot(snapshot) for snapshot in snapshots]
        payload["ledger"] = [self._serialize_ledger(entry) for entry in ledger]
        return payload

    async def rerun(self, run_id: str) -> dict[str, Any]:
        detail = await self.get_history_detail(run_id)
        request = SimulationRunRequest.model_validate(detail["request"])
        return await self.run(request, rerun_source_run_id=run_id)

    async def export_history_csv(self) -> str:
        rows = [
            self._serialize_run(row)
            for row in self._repository._connection.execute(
                """
                SELECT *
                FROM simulation_runs
                WHERE account_id = ?
                ORDER BY created_at DESC, rowid DESC
                """,
                (self._account_id,),
            ).fetchall()
        ]
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=[
                "run_id",
                "event_type",
                "session_kind",
                "symbol",
                "status",
                "stop_reason",
                "filled_qty",
                "avg_fill_price",
                "fee",
                "realized_pnl",
                "created_at",
                "engine_version",
                "simulation_schema_version",
            ],
        )
        writer.writeheader()
        for row in rows:
            result = row.get("result") or {}
            writer.writerow(
                {
                    "run_id": row.get("run_id"),
                    "event_type": row.get("event_type"),
                    "session_kind": row.get("session_kind"),
                    "symbol": row.get("symbol"),
                    "status": row.get("status"),
                    "stop_reason": row.get("stop_reason"),
                    "filled_qty": result.get("filled_qty", "0"),
                    "avg_fill_price": result.get("avg_fill_price", "0"),
                    "fee": result.get("fee", "0"),
                    "realized_pnl": result.get("realized_pnl", "0"),
                    "created_at": row.get("created_at"),
                    "engine_version": row.get("engine_version"),
                    "simulation_schema_version": row.get("simulation_schema_version"),
                }
            )
        return output.getvalue()

    async def list_templates(self) -> dict[str, Any]:
        rows = self._repository._connection.execute(
            "SELECT * FROM simulation_templates WHERE account_id = ? ORDER BY updated_at DESC",
            (self._account_id,),
        ).fetchall()
        return {"items": [self._serialize_template(row) for row in rows]}

    async def save_template(self, *, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        template_id = str(uuid4())
        now = _utc_now().isoformat()
        with self._repository._lock, self._repository._connection:
            self._repository._connection.execute(
                """
                INSERT INTO simulation_templates (template_id, account_id, name, payload_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (template_id, self._account_id, name.strip(), _json_dumps(payload), now, now),
            )
        return {"template_id": template_id, "name": name.strip(), "payload": payload, "created_at": now, "updated_at": now}

    async def delete_template(self, template_id: str) -> dict[str, Any]:
        with self._repository._lock, self._repository._connection:
            self._repository._connection.execute(
                "DELETE FROM simulation_templates WHERE account_id = ? AND template_id = ?",
                (self._account_id, template_id),
            )
        return {"deleted": True, "template_id": template_id}

    async def abort(self) -> dict[str, Any]:
        if not self.is_active():
            return {"requested": False, "requested_action": "abort", "status": "idle"}
        self._abort_requested = True
        self._abort_event.set()
        run_id = self._active_run_id
        now = _utc_now()
        if run_id:
            self._active_status = "aborting"
            self._active_stage = SimulationRunStage.ABORTING
            self._active_heartbeat_at = now.isoformat()
            with self._repository._lock, self._repository._connection:
                event_id = self._insert_event_locked(
                    run_id=run_id,
                    event_type="simulation_abort_requested",
                    level="warn",
                    message_code="runtime.simulation_abort_requested",
                    message_params={},
                    payload={"status": "aborting", "stage": SimulationRunStage.ABORTING},
                    created_at=now,
                )
                self._active_latest_event_id = event_id
                self._active_last_event_at = now.isoformat()
                self._update_run_locked(
                    run_id=run_id,
                    status="aborting",
                    stage=SimulationRunStage.ABORTING,
                    heartbeat_at=now,
                    last_event_at=now,
                )
        payload = {
            "requested": True,
            "requested_action": "abort",
            "run_id": run_id,
            "status": "aborting",
            "stage": SimulationRunStage.ABORTING,
            "heartbeat_at": self._active_heartbeat_at,
        }
        await self._publish("simulation_run", payload)
        return payload

    async def start_run(self, request: SimulationRunRequest, *, rerun_source_run_id: str | None = None) -> dict[str, Any]:
        if self.is_active():
            raise SimulationError(format_copy("runtime.simulation_busy"))
        run_id = str(uuid4())
        now = _utc_now()
        request_payload = self._request_payload(request)
        start_payload = self._start_payload(run_id, request, rerun_source_run_id, created_at=now)
        with self._repository._lock, self._repository._connection:
            self._insert_run_locked(
                run_id=run_id,
                event_type="simulation_run",
                request=request_payload,
                result=start_payload,
                status="running",
                stop_reason=SimulationStopReason.RUNNING,
                created_at=now,
                rerun_source_run_id=rerun_source_run_id,
                stage=SimulationRunStage.STARTING,
                heartbeat_at=now,
                last_event_at=now,
                lock_reason=SimulationLockReason.SIMULATION_RUNNING,
            )
            event_id = self._insert_event_locked(
                run_id=run_id,
                event_type="simulation_started",
                level="info",
                message_code="runtime.simulation_run_started",
                message_params={
                    "mode": request.session_kind.value,
                    "symbol": request.symbol.upper(),
                    "round_count": int(request.round_count or 0),
                },
                payload=start_payload,
                created_at=now,
            )
            start_payload["event_id"] = event_id
        self._active_run_id = run_id
        self._active_task = asyncio.create_task(self._run_background(run_id, request, rerun_source_run_id=rerun_source_run_id))
        self._active_request = request
        self._active_status = "running"
        self._active_stage = SimulationRunStage.STARTING
        self._active_heartbeat_at = now.isoformat()
        self._active_last_event_at = now.isoformat()
        self._active_latest_event_id = event_id
        await self._publish("simulation_run", start_payload)
        return start_payload

    async def start_rerun(self, run_id: str) -> dict[str, Any]:
        detail = await self.get_history_detail(run_id)
        request_payload = dict(detail.get("request") or {})
        if not request_payload:
            raise KeyError(run_id)
        return await self.start_run(SimulationRunRequest.model_validate(request_payload), rerun_source_run_id=run_id)

    async def active_run(self) -> dict[str, Any]:
        if not self.is_active() or not self._active_run_id:
            return {"active": False, "status": "idle", "stage": SimulationRunStage.IDLE}
        return await self.run_updates(self._active_run_id, after_event_id=0, active=True)

    async def run_updates(self, run_id: str, *, after_event_id: int = 0, active: bool | None = None) -> dict[str, Any]:
        with self._repository._lock:
            row = self._repository._connection.execute(
                "SELECT * FROM simulation_runs WHERE account_id = ? AND run_id = ?",
                (self._account_id, run_id),
            ).fetchone()
            if row is None:
                raise KeyError(run_id)
            event_rows = self._repository._connection.execute(
                """
                SELECT *
                FROM simulation_events
                WHERE account_id = ? AND run_id = ? AND event_id > ?
                ORDER BY event_id ASC
                """,
                (self._account_id, run_id, int(after_event_id or 0)),
            ).fetchall()
            latest_row = self._repository._connection.execute(
                "SELECT COALESCE(MAX(event_id), 0) AS latest_event_id FROM simulation_events WHERE account_id = ? AND run_id = ?",
                (self._account_id, run_id),
            ).fetchone()
        events = [self._event_payload_from_row(event_row) for event_row in event_rows]
        result = _json_load(row["result_json"], {})
        request = _json_load(row["request_json"], {})
        status = str(row["status"] or result.get("status") or "idle")
        stage = str(row["stage"] or result.get("stage") or SimulationRunStage.IDLE)
        latest_event_id = int(latest_row["latest_event_id"] or 0) if latest_row else 0
        run_summary = {
            "contract_version": CONTRACT_VERSION,
            "catalog_version": CATALOG_VERSION,
            "active": self.is_active() if active is None else bool(active),
            "run_id": run_id,
            "status": status,
            "stage": stage,
            "stop_reason": row["stop_reason"],
            "lock_reason": row["lock_reason"],
            "heartbeat_at": row["heartbeat_at"],
            "last_event_at": row["last_event_at"],
            "latest_event_id": latest_event_id,
            "rounds_completed": int(result.get("rounds_completed") or 0),
            "rounds_total": int(result.get("rounds_total") or request.get("round_count") or 0),
            "session_kind": row["session_kind"] or request.get("session_kind"),
            "symbol": row["symbol"] or request.get("symbol"),
            "result": result,
            "request": request,
        }
        return {
            **run_summary,
            "run": run_summary,
            "events": events,
            "latest_event_id": latest_event_id,
            "account": await self.get_account(),
        }

    async def _run_background(
        self,
        run_id: str,
        request: SimulationRunRequest,
        *,
        rerun_source_run_id: str | None,
    ) -> dict[str, Any]:
        try:
            result = await self._run_with_id(run_id, request, rerun_source_run_id=rerun_source_run_id)
        finally:
            self._abort_requested = False
            self._abort_event.clear()
            self._active_run_id = None
            self._active_task = None
            self._active_request = None
            self._active_stage = SimulationRunStage.IDLE
            self._active_status = "idle"
            self._active_heartbeat_at = None
            self._active_last_event_at = None
            self._active_latest_event_id = 0
        return result

    async def run(self, request: SimulationRunRequest, *, rerun_source_run_id: str | None = None) -> dict[str, Any]:
        if self._lock.locked():
            raise SimulationError(format_copy("runtime.simulation_busy"))
        return await self._run_with_id(str(uuid4()), request, rerun_source_run_id=rerun_source_run_id)

    async def _run_with_id(self, run_id: str, request: SimulationRunRequest, *, rerun_source_run_id: str | None = None) -> dict[str, Any]:
        async with self._lock:
            if not self._abort_requested:
                self._abort_event.clear()
            before = await self.get_account()
            started_at = _utc_now()
            try:
                result = await asyncio.wait_for(
                    self._execute_run(run_id, request, before=before, rerun_source_run_id=rerun_source_run_id),
                    timeout=MAX_SIMULATION_DURATION_SECONDS,
                )
            except TimeoutError:
                finished_at = _utc_now()
                result = self._blocked_result(
                    run_id,
                    request,
                    "blocked",
                    SimulationStopReason.MAX_SIMULATION_DURATION_REACHED,
                    rerun_source_run_id,
                )
                with self._repository._lock, self._repository._connection:
                    run_created_at = self._run_created_at_locked(run_id) or started_at
                    self._insert_run_locked(
                        run_id=run_id,
                        event_type="simulation_run",
                        request=self._request_payload(request),
                        result=result,
                        status="blocked",
                        stop_reason=SimulationStopReason.MAX_SIMULATION_DURATION_REACHED,
                        created_at=run_created_at,
                        rerun_source_run_id=rerun_source_run_id,
                        stage=SimulationRunStage.COMPLETED,
                        heartbeat_at=finished_at,
                        last_event_at=finished_at,
                    )
                    self._insert_snapshot_locked(run_id, "before", before, finished_at)
                    event_id = self._insert_event_locked(
                        run_id=run_id,
                        event_type="simulation_finished",
                        level="error",
                        message_code="runtime.simulation_run_finished",
                        message_params={"stop_reason": self._localized_stop_reason(SimulationStopReason.MAX_SIMULATION_DURATION_REACHED)},
                        payload=result,
                        created_at=finished_at,
                    )
                    result["event_id"] = event_id
                self._active_latest_event_id = event_id
                self._active_last_event_at = finished_at.isoformat()
                self._active_heartbeat_at = finished_at.isoformat()
                await self._publish("simulation_run", result)
                return result
            except Exception as exc:
                finished_at = _utc_now()
                result = {
                    "run_id": run_id,
                    "event_type": "simulation_run",
                    "status": "exception",
                    "stop_reason": SimulationStopReason.EXCEPTION,
                    "message": str(exc),
                    "message_code": "runtime.simulation_run_failed",
                    "message_params": {"error": str(exc)},
                    "rounds_total": int(request.round_count or 0),
                    "rounds_completed": 0,
                    "rerun_source_run_id": rerun_source_run_id,
                }
                with self._repository._lock, self._repository._connection:
                    run_created_at = self._run_created_at_locked(run_id) or started_at
                    self._insert_run_locked(
                        run_id=run_id,
                        event_type="simulation_run",
                        request=self._request_payload(request),
                        result=result,
                        status="exception",
                        stop_reason=SimulationStopReason.EXCEPTION,
                        created_at=run_created_at,
                        rerun_source_run_id=rerun_source_run_id,
                        stage=SimulationRunStage.COMPLETED,
                        heartbeat_at=finished_at,
                        last_event_at=finished_at,
                    )
                    self._insert_snapshot_locked(run_id, "before", before, finished_at)
                    event_id = self._insert_event_locked(
                        run_id=run_id,
                        event_type="simulation_finished",
                        level="error",
                        message_code="runtime.simulation_run_failed",
                        message_params={"error": str(exc)},
                        payload=result,
                        created_at=finished_at,
                    )
                    result["event_id"] = event_id
                self._active_latest_event_id = event_id
                self._active_last_event_at = finished_at.isoformat()
                self._active_heartbeat_at = finished_at.isoformat()
                await self._publish("simulation_run", result)
                return result
            finally:
                self._abort_requested = False
                self._abort_event.clear()
            await self._publish("simulation_run", result)
            await self._publish("simulation_account", await self.get_account())
            return result

    def _initialize_storage(self) -> None:
        with self._repository._lock, self._repository._connection:
            self._repository._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS simulation_account_settings (
                    account_id TEXT PRIMARY KEY,
                    initial_balance TEXT NOT NULL,
                    maker_fee_rate TEXT NOT NULL,
                    taker_fee_rate TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS simulation_positions (
                    account_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    position_side TEXT NOT NULL,
                    qty TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    margin TEXT NOT NULL,
                    leverage INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (account_id, symbol, position_side)
                );
                CREATE TABLE IF NOT EXISTS simulation_ledger (
                    ledger_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    amount TEXT NOT NULL,
                    balance_after TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS simulation_runs (
                    run_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    session_kind TEXT,
                    symbol TEXT,
                    status TEXT NOT NULL,
                    stop_reason TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    rerun_source_run_id TEXT,
                    simulation_schema_version INTEGER NOT NULL,
                    engine_version TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS simulation_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message_code TEXT,
                    message_params_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS simulation_fills (
                    fill_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    round_index INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    position_side TEXT NOT NULL,
                    side TEXT NOT NULL,
                    qty TEXT NOT NULL,
                    avg_price TEXT NOT NULL,
                    notional TEXT NOT NULL,
                    fee TEXT NOT NULL,
                    liquidity TEXT NOT NULL,
                    depth_levels_consumed INTEGER NOT NULL,
                    slippage_bps TEXT NOT NULL,
                    residual_qty TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS simulation_snapshots (
                    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    snapshot_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS simulation_templates (
                    template_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self._ensure_column_locked("simulation_runs", "stage", "TEXT NOT NULL DEFAULT 'completed'")
            self._ensure_column_locked("simulation_runs", "heartbeat_at", "TEXT")
            self._ensure_column_locked("simulation_runs", "last_event_at", "TEXT")
            self._ensure_column_locked("simulation_runs", "lock_reason", "TEXT")

    def _ensure_column_locked(self, table: str, column: str, definition: str) -> None:
        existing = {
            str(row["name"])
            for row in self._repository._connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column in existing:
            return
        self._repository._connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _mark_interrupted_runs(self) -> None:
        now = _utc_now()
        rows = self._repository._connection.execute(
            """
            SELECT run_id, request_json, result_json
            FROM simulation_runs
            WHERE account_id = ? AND status IN ('running', 'aborting')
            """,
            (self._account_id,),
        ).fetchall()
        if not rows:
            return
        with self._repository._lock, self._repository._connection:
            for row in rows:
                result = _json_load(row["result_json"], {})
                result.update(
                    {
                        "status": "interrupted",
                        "stage": SimulationRunStage.INTERRUPTED,
                        "stop_reason": SimulationStopReason.INTERRUPTED,
                        "message_code": "runtime.simulation_run_interrupted",
                        "message_params": {},
                        "updated_at": now.isoformat(),
                    }
                )
                self._repository._connection.execute(
                    """
                    UPDATE simulation_runs
                    SET status = ?, stop_reason = ?, stage = ?, result_json = ?, heartbeat_at = ?, last_event_at = ?, updated_at = ?
                    WHERE account_id = ? AND run_id = ?
                    """,
                    (
                        "interrupted",
                        SimulationStopReason.INTERRUPTED,
                        SimulationRunStage.INTERRUPTED,
                        _json_dumps(result),
                        now.isoformat(),
                        now.isoformat(),
                        now.isoformat(),
                        self._account_id,
                        row["run_id"],
                    ),
                )
                self._insert_event_locked(
                    run_id=row["run_id"],
                    event_type="simulation_interrupted",
                    level="warn",
                    message_code="runtime.simulation_run_interrupted",
                    message_params={},
                    payload={"status": "interrupted", "stop_reason": SimulationStopReason.INTERRUPTED},
                    created_at=now,
                )

    def _ensure_default_settings(self) -> None:
        if self._repository._connection.execute(
            "SELECT 1 FROM simulation_account_settings WHERE account_id = ?",
            (self._account_id,),
        ).fetchone():
            return
        now = _utc_now().isoformat()
        with self._repository._lock, self._repository._connection:
            self._repository._connection.execute(
                """
                INSERT INTO simulation_account_settings (account_id, initial_balance, maker_fee_rate, taker_fee_rate, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    self._account_id,
                    str(DEFAULT_INITIAL_BALANCE),
                    str(DEFAULT_MAKER_FEE_RATE),
                    str(DEFAULT_TAKER_FEE_RATE),
                    now,
                ),
            )

    async def _execute_run(
        self,
        run_id: str,
        request: SimulationRunRequest,
        *,
        before: dict[str, Any],
        rerun_source_run_id: str | None,
    ) -> dict[str, Any]:
        if self._abort_requested:
            return self._blocked_result(run_id, request, "aborted", SimulationStopReason.USER_ABORTED, rerun_source_run_id)

        symbol = request.symbol.upper()
        rules = await self._gateway.get_symbol_rules(symbol)
        result: dict[str, Any]
        if request.session_kind == SessionKind.SINGLE_OPEN:
            result = await self._run_single_open(run_id, request, rules, rerun_source_run_id)
        elif request.session_kind == SessionKind.SINGLE_CLOSE:
            result = await self._run_single_close(run_id, request, rules, rerun_source_run_id)
        elif request.session_kind == SessionKind.PAIRED_OPEN:
            result = await self._run_paired_open(run_id, request, rules, rerun_source_run_id)
        elif request.session_kind == SessionKind.PAIRED_CLOSE:
            result = await self._run_paired_close(run_id, request, rules, rerun_source_run_id)
        else:
            result = self._blocked_result(run_id, request, "blocked", SimulationStopReason.EXCEPTION, rerun_source_run_id)

        after = await self.get_account()
        status = str(result.get("status") or "completed")
        stop_reason = str(result.get("stop_reason") or SimulationStopReason.FILLED)
        now = _utc_now()
        with self._repository._lock, self._repository._connection:
            run_created_at = self._run_created_at_locked(run_id) or now
            self._insert_run_locked(
                run_id=run_id,
                event_type="simulation_run",
                request=self._request_payload(request),
                result=result,
                status=status,
                stop_reason=stop_reason,
                created_at=run_created_at,
                rerun_source_run_id=rerun_source_run_id,
                stage=SimulationRunStage.COMPLETED,
                heartbeat_at=now,
                last_event_at=now,
            )
            self._insert_snapshot_locked(run_id, "before", before, now)
            self._insert_snapshot_locked(run_id, "after", after, now)
            event_id = self._insert_event_locked(
                run_id=run_id,
                event_type="simulation_finished",
                level="success" if status == "completed" else "warn" if status in {"completed_with_skips", "aborted"} else "error",
                message_code=result.get("message_code") or "runtime.simulation_run_finished",
                message_params=result.get("message_params") or {"stop_reason": self._localized_stop_reason(stop_reason)},
                payload=result,
                created_at=now,
            )
            result["event_id"] = event_id
        self._active_status = status
        self._active_stage = SimulationRunStage.COMPLETED
        self._active_heartbeat_at = now.isoformat()
        self._active_last_event_at = now.isoformat()
        self._active_latest_event_id = event_id
        return result

    async def _run_single_open(
        self,
        run_id: str,
        request: SimulationRunRequest,
        rules: SymbolRules,
        rerun_source_run_id: str | None,
    ) -> dict[str, Any]:
        symbol = request.symbol.upper()
        side = request.selected_position_side or PositionSide.LONG
        if request.open_mode == SingleOpenMode.ALIGN:
            side = self._alignment_open_side(symbol)
            if side is None:
                return self._blocked_result(run_id, request, "blocked", SimulationStopReason.MIN_NOTIONAL_BLOCKED, rerun_source_run_id)
        qty = normalize_qty(_to_decimal(request.open_qty), rules)
        if qty <= Decimal("0"):
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.MIN_NOTIONAL_BLOCKED, rerun_source_run_id)
        leverage = int(request.leverage or 1)
        return await self._run_open_round_plan(
            run_id=run_id,
            request=request,
            symbol=symbol,
            position_side=side,
            planned_qtys=self._planned_round_qtys(qty, request.round_count, rules),
            leverage=leverage,
            rules=rules,
            rerun_source_run_id=rerun_source_run_id,
        )

    async def _run_single_close(
        self,
        run_id: str,
        request: SimulationRunRequest,
        rules: SymbolRules,
        rerun_source_run_id: str | None,
    ) -> dict[str, Any]:
        symbol = request.symbol.upper()
        side = request.selected_position_side or PositionSide.LONG
        if request.close_mode == SingleCloseMode.ALIGN:
            side = self._alignment_close_side(symbol)
            if side is None:
                return self._blocked_result(run_id, request, "blocked", SimulationStopReason.INSUFFICIENT_SIM_POSITION, rerun_source_run_id)
        if request.close_qty is None:
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.MIN_NOTIONAL_BLOCKED, rerun_source_run_id)
        qty = normalize_qty(_to_decimal(request.close_qty), rules)
        if qty <= Decimal("0"):
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.MIN_NOTIONAL_BLOCKED, rerun_source_run_id)
        return await self._run_close_round_plan(
            run_id=run_id,
            request=request,
            symbol=symbol,
            position_side=side,
            planned_qtys=self._planned_round_qtys(qty, request.round_count, rules),
            rules=rules,
            rerun_source_run_id=rerun_source_run_id,
        )

    async def _run_paired_open(
        self,
        run_id: str,
        request: SimulationRunRequest,
        rules: SymbolRules,
        rerun_source_run_id: str | None,
    ) -> dict[str, Any]:
        symbol = request.symbol.upper()
        leverage = int(request.leverage or 1)
        trend = request.trend_bias or TrendBias.LONG
        first = PositionSide.LONG if trend == TrendBias.LONG else PositionSide.SHORT
        second = PositionSide.SHORT if first == PositionSide.LONG else PositionSide.LONG
        first_snapshot = await self._fresh_orderbook(symbol)
        if first_snapshot is None:
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.STALE_ORDERBOOK, rerun_source_run_id)
        reference_price = self._paired_open_reference_price(first_snapshot, first_snapshot, first, second)
        open_amount = _to_decimal(request.open_amount)
        if reference_price <= Decimal("0"):
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.MIN_NOTIONAL_BLOCKED, rerun_source_run_id)
        per_leg_notional = (open_amount * Decimal(leverage)) / Decimal("2")
        target_qty = normalize_qty(per_leg_notional / reference_price, rules)
        if target_qty <= Decimal("0"):
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.MIN_NOTIONAL_BLOCKED, rerun_source_run_id)
        planned_qtys = self._planned_round_qtys(target_qty, request.round_count, rules)
        paired_planned_qtys = [qty * Decimal("2") for qty in planned_qtys]
        extension_cap_qty = max(planned_qtys) if planned_qtys else target_qty
        planned_rounds_total = len(planned_qtys)
        rounds_total_for_result = planned_rounds_total
        round_results: list[dict[str, Any]] = []
        for round_index, planned_qty in enumerate(planned_qtys, start=1):
            if planned_qty <= Decimal("0"):
                continue
            if self._abort_requested:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="aborted",
                    stop_reason=SimulationStopReason.USER_ABORTED,
                    rounds_total=rounds_total_for_result,
                )
            checkpoint = self._mutation_checkpoint()
            first_match, second_match, blocked_reason = await self._paired_open_matches(
                symbol=symbol,
                first=first,
                second=second,
                planned_qty=planned_qty,
                rules=rules,
                wait_seconds=self._order_wait_seconds(request),
                snapshots=(first_snapshot, None) if round_index == 1 else None,
            )
            if blocked_reason is not None or first_match is None or second_match is None:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="blocked" if not round_results else "completed_with_skips",
                    stop_reason=blocked_reason or SimulationStopReason.EXCEPTION,
                    rounds_total=rounds_total_for_result,
                )
            first_result = await self._execute_open_qty(
                run_id=run_id,
                request=request,
                symbol=symbol,
                position_side=first,
                qty=planned_qty,
                leverage=leverage,
                rules=rules,
                rerun_source_run_id=rerun_source_run_id,
                round_index=round_index,
                defer_run_record=True,
                match=first_match,
            )
            if first_result["status"] not in {"completed", "completed_with_skips"}:
                self._rollback_to_checkpoint(checkpoint)
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="blocked" if not round_results else "completed_with_skips",
                    stop_reason=str(first_result.get("stop_reason") or SimulationStopReason.EXCEPTION),
                    rounds_total=rounds_total_for_result,
                )
            second_result = await self._execute_open_qty(
                run_id=run_id,
                request=request,
                symbol=symbol,
                position_side=second,
                qty=planned_qty,
                leverage=leverage,
                rules=rules,
                rerun_source_run_id=rerun_source_run_id,
                round_index=round_index,
                defer_run_record=True,
                match=second_match,
            )
            if second_result["status"] not in {"completed", "completed_with_skips"}:
                self._rollback_to_checkpoint(checkpoint)
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="blocked" if not round_results else "completed_with_skips",
                    stop_reason=str(second_result.get("stop_reason") or SimulationStopReason.EXCEPTION),
                    rounds_total=rounds_total_for_result,
                )
            round_results.append(self._combine_leg_results(run_id, request, [first_result, second_result], rerun_source_run_id))
            await self._publish_round_progress(
                run_id=run_id,
                request=request,
                symbol=symbol,
                planned_qtys=paired_planned_qtys,
                results=round_results,
                rules=rules,
                round_index=round_index,
                rounds_total=rounds_total_for_result,
            )
            aborted = await self._wait_between_rounds(
                request,
                round_index,
                len(planned_qtys),
                consumed_wait_seconds=_to_decimal(round_results[-1].get("wait_seconds_consumed")),
            )
            if aborted:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="aborted",
                    stop_reason=SimulationStopReason.USER_ABORTED,
                    rounds_total=rounds_total_for_result,
                )
        extension_round_index = 1
        while True:
            filled_per_side = self._paired_filled_qty_per_side(round_results)
            remaining_qty = normalize_qty(max(target_qty - filled_per_side, Decimal("0")), rules)
            if remaining_qty <= Decimal("0"):
                break
            planned_qty = normalize_qty(min(remaining_qty, extension_cap_qty), rules)
            if planned_qty <= Decimal("0"):
                break
            round_index = planned_rounds_total + extension_round_index
            rounds_total_for_result = round_index
            if self._abort_requested:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="aborted",
                    stop_reason=SimulationStopReason.USER_ABORTED,
                    rounds_total=rounds_total_for_result,
                )
            checkpoint = self._mutation_checkpoint()
            first_match, second_match, blocked_reason = await self._paired_open_matches(
                symbol=symbol,
                first=first,
                second=second,
                planned_qty=planned_qty,
                rules=rules,
                wait_seconds=self._order_wait_seconds(request),
            )
            if blocked_reason is not None or first_match is None or second_match is None:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="blocked" if not round_results else "completed_with_skips",
                    stop_reason=blocked_reason or SimulationStopReason.EXCEPTION,
                    rounds_total=rounds_total_for_result,
                )
            first_result = await self._execute_open_qty(
                run_id=run_id,
                request=request,
                symbol=symbol,
                position_side=first,
                qty=planned_qty,
                leverage=leverage,
                rules=rules,
                rerun_source_run_id=rerun_source_run_id,
                round_index=round_index,
                defer_run_record=True,
                match=first_match,
            )
            if first_result["status"] not in {"completed", "completed_with_skips"}:
                self._rollback_to_checkpoint(checkpoint)
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="blocked" if not round_results else "completed_with_skips",
                    stop_reason=str(first_result.get("stop_reason") or SimulationStopReason.EXCEPTION),
                    rounds_total=rounds_total_for_result,
                )
            second_result = await self._execute_open_qty(
                run_id=run_id,
                request=request,
                symbol=symbol,
                position_side=second,
                qty=planned_qty,
                leverage=leverage,
                rules=rules,
                rerun_source_run_id=rerun_source_run_id,
                round_index=round_index,
                defer_run_record=True,
                match=second_match,
            )
            if second_result["status"] not in {"completed", "completed_with_skips"}:
                self._rollback_to_checkpoint(checkpoint)
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="blocked" if not round_results else "completed_with_skips",
                    stop_reason=str(second_result.get("stop_reason") or SimulationStopReason.EXCEPTION),
                    rounds_total=rounds_total_for_result,
                )
            round_results.append(self._combine_leg_results(run_id, request, [first_result, second_result], rerun_source_run_id))
            await self._publish_round_progress(
                run_id=run_id,
                request=request,
                symbol=symbol,
                planned_qtys=paired_planned_qtys,
                results=round_results,
                rules=rules,
                round_index=round_index,
                rounds_total=rounds_total_for_result,
                extension_rounds_unlimited=True,
            )
            aborted = await self._wait_between_rounds(
                request,
                0,
                1,
                consumed_wait_seconds=_to_decimal(round_results[-1].get("wait_seconds_consumed")),
            )
            if aborted or self._abort_requested:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="aborted",
                    stop_reason=SimulationStopReason.USER_ABORTED,
                    rounds_total=rounds_total_for_result,
                )
            extension_round_index += 1
        return self._aggregate_round_results(
            run_id,
            request,
            paired_planned_qtys,
            round_results,
            rerun_source_run_id,
            rounds_total=rounds_total_for_result,
        )

    async def _run_paired_close(
        self,
        run_id: str,
        request: SimulationRunRequest,
        rules: SymbolRules,
        rerun_source_run_id: str | None,
    ) -> dict[str, Any]:
        symbol = request.symbol.upper()
        if request.close_qty is None:
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.MIN_NOTIONAL_BLOCKED, rerun_source_run_id)
        qty = normalize_qty(_to_decimal(request.close_qty), rules)
        if qty <= Decimal("0"):
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.MIN_NOTIONAL_BLOCKED, rerun_source_run_id)
        trend = request.trend_bias or TrendBias.LONG
        first = PositionSide.SHORT if trend == TrendBias.LONG else PositionSide.LONG
        second = PositionSide.LONG if first == PositionSide.SHORT else PositionSide.SHORT
        closeable_qty = self._paired_closeable_qty(symbol, first, second, rules)
        if closeable_qty <= Decimal("0") or qty > closeable_qty:
            return self._blocked_result(
                run_id,
                request,
                "blocked",
                SimulationStopReason.INSUFFICIENT_SIM_POSITION,
                rerun_source_run_id,
            )
        planned_qtys = self._planned_round_qtys(qty, request.round_count, rules)
        paired_planned_qtys = [qty * Decimal("2") for qty in planned_qtys]
        extension_cap_qty = max(planned_qtys) if planned_qtys else qty
        planned_rounds_total = len(planned_qtys)
        rounds_total_for_result = planned_rounds_total
        round_results: list[dict[str, Any]] = []
        for round_index, planned_qty in enumerate(planned_qtys, start=1):
            if planned_qty <= Decimal("0"):
                continue
            if self._abort_requested:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="aborted",
                    stop_reason=SimulationStopReason.USER_ABORTED,
                    rounds_total=rounds_total_for_result,
                )
            checkpoint = self._mutation_checkpoint()
            first_match, second_match, blocked_reason = await self._paired_close_matches(
                symbol=symbol,
                first=first,
                second=second,
                planned_qty=planned_qty,
                rules=rules,
                wait_seconds=self._order_wait_seconds(request),
            )
            if blocked_reason is not None or first_match is None or second_match is None:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="blocked" if not round_results else "completed_with_skips",
                    stop_reason=blocked_reason or SimulationStopReason.EXCEPTION,
                    rounds_total=rounds_total_for_result,
                )
            first_result = await self._execute_close_qty(
                run_id=run_id,
                request=request,
                symbol=symbol,
                position_side=first,
                qty=planned_qty,
                rules=rules,
                rerun_source_run_id=rerun_source_run_id,
                round_index=round_index,
                defer_run_record=True,
                match=first_match,
            )
            if first_result["status"] not in {"completed", "completed_with_skips"}:
                self._rollback_to_checkpoint(checkpoint)
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="blocked" if not round_results else "completed_with_skips",
                    stop_reason=str(first_result.get("stop_reason") or SimulationStopReason.EXCEPTION),
                    rounds_total=rounds_total_for_result,
                )
            second_result = await self._execute_close_qty(
                run_id=run_id,
                request=request,
                symbol=symbol,
                position_side=second,
                qty=planned_qty,
                rules=rules,
                rerun_source_run_id=rerun_source_run_id,
                round_index=round_index,
                defer_run_record=True,
                match=second_match,
            )
            if second_result["status"] not in {"completed", "completed_with_skips"}:
                self._rollback_to_checkpoint(checkpoint)
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="blocked" if not round_results else "completed_with_skips",
                    stop_reason=str(second_result.get("stop_reason") or SimulationStopReason.EXCEPTION),
                    rounds_total=rounds_total_for_result,
                )
            round_results.append(self._combine_leg_results(run_id, request, [first_result, second_result], rerun_source_run_id))
            await self._publish_round_progress(
                run_id=run_id,
                request=request,
                symbol=symbol,
                planned_qtys=paired_planned_qtys,
                results=round_results,
                rules=rules,
                round_index=round_index,
                rounds_total=rounds_total_for_result,
            )
            aborted = await self._wait_between_rounds(
                request,
                round_index,
                len(planned_qtys),
                consumed_wait_seconds=_to_decimal(round_results[-1].get("wait_seconds_consumed")),
            )
            if aborted:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="aborted",
                    stop_reason=SimulationStopReason.USER_ABORTED,
                    rounds_total=rounds_total_for_result,
                )
        extension_round_index = 1
        while True:
            filled_per_side = self._paired_filled_qty_per_side(round_results)
            remaining_qty = normalize_qty(max(qty - filled_per_side, Decimal("0")), rules)
            closeable_qty = self._paired_closeable_qty(symbol, first, second, rules)
            if remaining_qty <= Decimal("0") or closeable_qty <= Decimal("0"):
                break
            planned_qty = normalize_qty(min(remaining_qty, closeable_qty, extension_cap_qty), rules)
            if planned_qty <= Decimal("0"):
                break
            round_index = planned_rounds_total + extension_round_index
            rounds_total_for_result = round_index
            if self._abort_requested:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="aborted",
                    stop_reason=SimulationStopReason.USER_ABORTED,
                    rounds_total=rounds_total_for_result,
                )
            checkpoint = self._mutation_checkpoint()
            first_match, second_match, blocked_reason = await self._paired_close_matches(
                symbol=symbol,
                first=first,
                second=second,
                planned_qty=planned_qty,
                rules=rules,
                wait_seconds=self._order_wait_seconds(request),
            )
            if blocked_reason is not None or first_match is None or second_match is None:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="blocked" if not round_results else "completed_with_skips",
                    stop_reason=blocked_reason or SimulationStopReason.EXCEPTION,
                    rounds_total=rounds_total_for_result,
                )
            first_result = await self._execute_close_qty(
                run_id=run_id,
                request=request,
                symbol=symbol,
                position_side=first,
                qty=planned_qty,
                rules=rules,
                rerun_source_run_id=rerun_source_run_id,
                round_index=round_index,
                defer_run_record=True,
                match=first_match,
            )
            if first_result["status"] not in {"completed", "completed_with_skips"}:
                self._rollback_to_checkpoint(checkpoint)
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="blocked" if not round_results else "completed_with_skips",
                    stop_reason=str(first_result.get("stop_reason") or SimulationStopReason.EXCEPTION),
                    rounds_total=rounds_total_for_result,
                )
            second_result = await self._execute_close_qty(
                run_id=run_id,
                request=request,
                symbol=symbol,
                position_side=second,
                qty=planned_qty,
                rules=rules,
                rerun_source_run_id=rerun_source_run_id,
                round_index=round_index,
                defer_run_record=True,
                match=second_match,
            )
            if second_result["status"] not in {"completed", "completed_with_skips"}:
                self._rollback_to_checkpoint(checkpoint)
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="blocked" if not round_results else "completed_with_skips",
                    stop_reason=str(second_result.get("stop_reason") or SimulationStopReason.EXCEPTION),
                    rounds_total=rounds_total_for_result,
                )
            round_results.append(self._combine_leg_results(run_id, request, [first_result, second_result], rerun_source_run_id))
            await self._publish_round_progress(
                run_id=run_id,
                request=request,
                symbol=symbol,
                planned_qtys=paired_planned_qtys,
                results=round_results,
                rules=rules,
                round_index=round_index,
                rounds_total=rounds_total_for_result,
                extension_rounds_unlimited=True,
            )
            aborted = await self._wait_between_rounds(
                request,
                0,
                1,
                consumed_wait_seconds=_to_decimal(round_results[-1].get("wait_seconds_consumed")),
            )
            if aborted or self._abort_requested:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    paired_planned_qtys,
                    round_results,
                    rerun_source_run_id,
                    status="aborted",
                    stop_reason=SimulationStopReason.USER_ABORTED,
                    rounds_total=rounds_total_for_result,
                )
            extension_round_index += 1
        return self._aggregate_round_results(
            run_id,
            request,
            paired_planned_qtys,
            round_results,
            rerun_source_run_id,
            rounds_total=rounds_total_for_result,
        )

    def _planned_round_qtys(self, total_qty: Decimal, round_count: int, rules: SymbolRules) -> list[Decimal]:
        rounds = max(int(round_count or 1), 1)
        total = normalize_qty(total_qty, rules)
        if total <= Decimal("0"):
            return []
        base_qty = normalize_qty(total / Decimal(rounds), rules)
        planned: list[Decimal] = []
        assigned = Decimal("0")
        for _ in range(max(rounds - 1, 0)):
            planned.append(base_qty)
            assigned += base_qty
        final_qty = normalize_qty(max(total - assigned, Decimal("0")), rules)
        planned.append(final_qty)
        return planned

    async def _run_open_round_plan(
        self,
        *,
        run_id: str,
        request: SimulationRunRequest,
        symbol: str,
        position_side: PositionSide,
        planned_qtys: list[Decimal],
        leverage: int,
        rules: SymbolRules,
        rerun_source_run_id: str | None,
    ) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        for round_index, planned_qty in enumerate(planned_qtys, start=1):
            if planned_qty <= Decimal("0"):
                continue
            if self._abort_requested:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    planned_qtys,
                    results,
                    rerun_source_run_id,
                    status="aborted",
                    stop_reason=SimulationStopReason.USER_ABORTED,
                )
            result = await self._execute_open_qty(
                run_id=run_id,
                request=request,
                symbol=symbol,
                position_side=position_side,
                qty=planned_qty,
                leverage=leverage,
                rules=rules,
                rerun_source_run_id=rerun_source_run_id,
                round_index=round_index,
            )
            if result["status"] not in {"completed", "completed_with_skips"}:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    planned_qtys,
                    results,
                    rerun_source_run_id,
                    status="blocked" if not results else "completed_with_skips",
                    stop_reason=str(result.get("stop_reason") or SimulationStopReason.EXCEPTION),
                )
            results.append(result)
            await self._publish_round_progress(
                run_id=run_id,
                request=request,
                symbol=symbol,
                planned_qtys=planned_qtys,
                results=results,
                rules=rules,
                round_index=round_index,
            )
            aborted = await self._wait_between_rounds(
                request,
                round_index,
                len(planned_qtys),
                consumed_wait_seconds=_to_decimal(results[-1].get("wait_seconds_consumed")),
            )
            if aborted:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    planned_qtys,
                    results,
                    rerun_source_run_id,
                    status="aborted",
                    stop_reason=SimulationStopReason.USER_ABORTED,
                )
        return self._aggregate_round_results(run_id, request, planned_qtys, results, rerun_source_run_id)

    async def _run_close_round_plan(
        self,
        *,
        run_id: str,
        request: SimulationRunRequest,
        symbol: str,
        position_side: PositionSide,
        planned_qtys: list[Decimal],
        rules: SymbolRules,
        rerun_source_run_id: str | None,
    ) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        for round_index, planned_qty in enumerate(planned_qtys, start=1):
            if planned_qty <= Decimal("0"):
                continue
            if self._abort_requested:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    planned_qtys,
                    results,
                    rerun_source_run_id,
                    status="aborted",
                    stop_reason=SimulationStopReason.USER_ABORTED,
                )
            result = await self._execute_close_qty(
                run_id=run_id,
                request=request,
                symbol=symbol,
                position_side=position_side,
                qty=planned_qty,
                rules=rules,
                rerun_source_run_id=rerun_source_run_id,
                round_index=round_index,
            )
            if result["status"] not in {"completed", "completed_with_skips"}:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    planned_qtys,
                    results,
                    rerun_source_run_id,
                    status="blocked" if not results else "completed_with_skips",
                    stop_reason=str(result.get("stop_reason") or SimulationStopReason.EXCEPTION),
                )
            results.append(result)
            await self._publish_round_progress(
                run_id=run_id,
                request=request,
                symbol=symbol,
                planned_qtys=planned_qtys,
                results=results,
                rules=rules,
                round_index=round_index,
            )
            aborted = await self._wait_between_rounds(
                request,
                round_index,
                len(planned_qtys),
                consumed_wait_seconds=_to_decimal(results[-1].get("wait_seconds_consumed")),
            )
            if aborted:
                return self._aggregate_round_results(
                    run_id,
                    request,
                    planned_qtys,
                    results,
                    rerun_source_run_id,
                    status="aborted",
                    stop_reason=SimulationStopReason.USER_ABORTED,
                )
        return self._aggregate_round_results(run_id, request, planned_qtys, results, rerun_source_run_id)

    async def _wait_between_rounds(
        self,
        request: SimulationRunRequest,
        round_index: int,
        total_rounds: int,
        *,
        consumed_wait_seconds: Decimal = Decimal("0"),
    ) -> bool:
        interval = Decimal(str(request.round_interval_seconds or 0))
        if round_index >= total_rounds or interval <= 0:
            return False
        await self._publish_active_stage(SimulationRunStage.WAITING_INTERVAL)
        try:
            await asyncio.wait_for(self._abort_event.wait(), timeout=float(interval))
        except TimeoutError:
            return False
        return self._abort_requested

    def _order_wait_seconds(self, request: SimulationRunRequest) -> Decimal:
        return SIMULATION_LIMIT_ORDER_WAIT_SECONDS

    def _aggregate_round_results(
        self,
        run_id: str,
        request: SimulationRunRequest,
        planned_qtys: list[Decimal],
        results: list[dict[str, Any]],
        rerun_source_run_id: str | None,
        *,
        status: str | None = None,
        stop_reason: str | None = None,
        rounds_total: int | None = None,
    ) -> dict[str, Any]:
        planned_total = sum(planned_qtys, Decimal("0"))
        filled = sum((_to_decimal(result.get("filled_qty")) for result in results), Decimal("0"))
        fee = sum((_to_decimal(result.get("fee")) for result in results), Decimal("0"))
        realized = sum((_to_decimal(result.get("realized_pnl")) for result in results), Decimal("0"))
        wait_seconds_consumed = max(
            (_to_decimal(result.get("wait_seconds_consumed")) for result in results),
            default=Decimal("0"),
        )
        residual = max(planned_total - filled, Decimal("0"))
        notional = sum(
            (_to_decimal(result.get("filled_qty")) * _to_decimal(result.get("avg_fill_price")) for result in results),
            Decimal("0"),
        )
        avg_price = notional / filled if filled > Decimal("0") else Decimal("0")
        terminal_status = status or ("completed" if residual <= Decimal("0") else "completed_with_skips")
        if stop_reason:
            terminal_reason = stop_reason
        elif terminal_status == "completed":
            terminal_reason = SimulationStopReason.FILLED
        elif results:
            terminal_reason = str(results[-1].get("stop_reason") or SimulationStopReason.LIMIT_ORDER_UNFILLED)
        else:
            terminal_reason = SimulationStopReason.LIMIT_ORDER_UNFILLED
        return self._result_payload(
            run_id,
            request,
            terminal_status,
            terminal_reason,
            rerun_source_run_id,
            filled_qty=filled,
            avg_fill_price=avg_price,
            fee=fee,
            residual_qty=residual,
            realized_pnl=realized,
            rounds_completed=len(results),
            rounds_total=rounds_total,
            wait_seconds_consumed=wait_seconds_consumed,
        )

    def _paired_filled_qty_per_side(self, results: list[dict[str, Any]]) -> Decimal:
        filled_combined = sum((_to_decimal(result.get("filled_qty")) for result in results), Decimal("0"))
        return filled_combined / Decimal("2") if filled_combined > Decimal("0") else Decimal("0")

    def _paired_closeable_qty(self, symbol: str, first: PositionSide, second: PositionSide, rules: SymbolRules) -> Decimal:
        first_position = self._get_position(symbol, first)
        second_position = self._get_position(symbol, second)
        if first_position is None or second_position is None:
            return Decimal("0")
        return normalize_qty(min(_to_decimal(first_position["qty"]), _to_decimal(second_position["qty"])), rules)

    def _localized_stop_reason(self, stop_reason: str) -> str:
        stop_reason_text = str(stop_reason)
        key = f"runtime.stop_reason.{stop_reason_text}"
        localized = format_copy(key)
        return stop_reason_text if localized == key else localized

    async def _publish_round_progress(
        self,
        *,
        run_id: str,
        request: SimulationRunRequest,
        symbol: str,
        planned_qtys: list[Decimal],
        results: list[dict[str, Any]],
        rules: SymbolRules,
        round_index: int,
        rounds_total: int | None = None,
        extension_rounds_unlimited: bool = False,
    ) -> None:
        if not results:
            return
        latest = results[-1]
        completed = len(results)
        planned_completed = sum(planned_qtys[:round_index], Decimal("0"))
        filled_total = sum((_to_decimal(result.get("filled_qty")) for result in results), Decimal("0"))
        fee_total = sum((_to_decimal(result.get("fee")) for result in results), Decimal("0"))
        realized_total = sum((_to_decimal(result.get("realized_pnl")) for result in results), Decimal("0"))
        total_notional = sum(
            (_to_decimal(result.get("filled_qty")) * _to_decimal(result.get("avg_fill_price")) for result in results),
            Decimal("0"),
        )
        latest_filled = _to_decimal(latest.get("filled_qty"))
        latest_price = _to_decimal(latest.get("avg_fill_price"))
        latest_fee = _to_decimal(latest.get("fee"))
        latest_residual = _to_decimal(latest.get("residual_qty"))
        latest_notional = latest_filled * latest_price
        carryover_qty = max(planned_completed - filled_total, Decimal("0"))
        now = _utc_now().isoformat()
        planned_round_count = len(planned_qtys)
        rounds_total_value = int(rounds_total or planned_round_count)
        is_extension_round = round_index > planned_round_count
        extension_round_index = max(round_index - planned_round_count, 0) if is_extension_round else 0
        max_extension_rounds = 0 if extension_rounds_unlimited else max(rounds_total_value - planned_round_count, extension_round_index) if is_extension_round else 0
        is_unfilled_attempt = latest_filled <= Decimal("0")
        if is_extension_round and is_unfilled_attempt:
            message_code = "log.simulation.extension_round_unfilled"
        elif is_extension_round:
            message_code = "log.simulation.extension_round_completed"
        elif is_unfilled_attempt:
            message_code = "log.simulation.round_unfilled"
        else:
            message_code = "log.simulation.round_completed"
        message_params = {
            "round_index": round_index,
            "round_count": rounds_total_value,
            "symbol": symbol,
            "filled_qty": _format_decimal(latest_filled),
            "avg_fill_price": _format_plain_decimal(latest_price),
            "fee": _format_decimal(_money(latest_fee)),
            "residual_qty": _format_decimal(latest_residual),
        }
        if is_extension_round:
            message_params.update(
                {
                    "extension_round_index": extension_round_index,
                    "max_extension_rounds": max_extension_rounds,
                    "total_round_index": round_index,
                }
            )
        stats_payload = {
            "contract_version": CONTRACT_VERSION,
            "catalog_version": CATALOG_VERSION,
            "event_type": "simulation_round_progress",
            "run_id": run_id,
            "status": "running",
            "session_kind": request.session_kind.value,
            "mode": request.session_kind.value,
            "symbol": symbol,
            "round_index": round_index,
            "rounds_completed": completed,
            "rounds_total": rounds_total_value,
            "is_extension_round": is_extension_round,
            "extension_rounds_unlimited": extension_rounds_unlimited,
            "extension_round_index": extension_round_index,
            "max_extension_rounds": max_extension_rounds,
            "filled_qty": _format_decimal(filled_total),
            "avg_fill_price": _format_plain_decimal(total_notional / filled_total if filled_total > Decimal("0") else Decimal("0")),
            "fee": _format_decimal(_money(fee_total)),
            "realized_pnl": _format_decimal(_money(realized_total)),
            "residual_qty": _format_decimal(carryover_qty),
            "total_notional": _format_decimal(_money(total_notional)),
            "notional_per_round": _format_decimal(_money(latest_notional)),
            "last_qty": _format_decimal(latest_filled),
            "min_notional": _format_decimal(rules.min_notional),
            "final_alignment_status": "not_triggered",
            "message_code": message_code,
            "message_params": message_params,
            "created_at": now,
        }
        log_payload = {
            "contract_version": CONTRACT_VERSION,
            "catalog_version": CATALOG_VERSION,
            "level": "success" if latest_residual <= Decimal("0") else "warn",
            "created_at": now,
            "message_code": message_code,
            "message_params": message_params,
            "message": format_copy(message_code, message_params),
        }
        now_dt = datetime.fromisoformat(now)
        with self._repository._lock, self._repository._connection:
            event_id = self._insert_event_locked(
                run_id=run_id,
                event_type="simulation_round_progress",
                level=log_payload["level"],
                message_code=message_code,
                message_params=message_params,
                payload=stats_payload,
                created_at=now_dt,
            )
            self._update_run_locked(
                run_id=run_id,
                status="running",
                stage=SimulationRunStage.RUNNING,
                result=stats_payload,
                stop_reason=SimulationStopReason.RUNNING,
                heartbeat_at=now_dt,
                last_event_at=now_dt,
                lock_reason=SimulationLockReason.SIMULATION_RUNNING,
            )
        stats_payload["event_id"] = event_id
        log_payload["event_id"] = event_id
        self._active_status = "running"
        self._active_stage = SimulationRunStage.RUNNING
        self._active_heartbeat_at = now
        self._active_last_event_at = now
        self._active_latest_event_id = event_id
        await self._publish("execution_stats", stats_payload)
        await self._publish("execution_log", log_payload)
        await self._publish("simulation_account", await self.get_account())

    def _mutation_checkpoint(self) -> dict[str, Any]:
        with self._repository._lock:
            ledger_row = self._repository._connection.execute(
                "SELECT COALESCE(MAX(ledger_id), 0) AS max_id FROM simulation_ledger WHERE account_id = ?",
                (self._account_id,),
            ).fetchone()
            fill_row = self._repository._connection.execute(
                "SELECT COALESCE(MAX(fill_id), 0) AS max_id FROM simulation_fills WHERE account_id = ?",
                (self._account_id,),
            ).fetchone()
            return {
                "ledger_id": int(ledger_row["max_id"] or 0) if ledger_row else 0,
                "fill_id": int(fill_row["max_id"] or 0) if fill_row else 0,
                "positions": [dict(position) for position in self._list_positions()],
            }

    def _rollback_to_checkpoint(self, checkpoint: dict[str, Any]) -> None:
        with self._repository._lock, self._repository._connection:
            self._repository._connection.execute(
                "DELETE FROM simulation_ledger WHERE account_id = ? AND ledger_id > ?",
                (self._account_id, int(checkpoint.get("ledger_id") or 0)),
            )
            self._repository._connection.execute(
                "DELETE FROM simulation_fills WHERE account_id = ? AND fill_id > ?",
                (self._account_id, int(checkpoint.get("fill_id") or 0)),
            )
            self._repository._connection.execute(
                "DELETE FROM simulation_positions WHERE account_id = ?",
                (self._account_id,),
            )
            for position in checkpoint.get("positions") or []:
                self._repository._connection.execute(
                    """
                    INSERT INTO simulation_positions (account_id, symbol, position_side, qty, entry_price, margin, leverage, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self._account_id,
                        position["symbol"],
                        position["position_side"],
                        position["qty"],
                        position["entry_price"],
                        position["margin"],
                        int(position["leverage"]),
                        position["updated_at"],
                    ),
                )

    async def _paired_open_matches(
        self,
        *,
        symbol: str,
        first: PositionSide,
        second: PositionSide,
        planned_qty: Decimal,
        rules: SymbolRules,
        wait_seconds: Decimal,
        snapshots: tuple[dict[str, Any], dict[str, Any] | None] | None = None,
    ) -> tuple[MatchResult | None, MatchResult | None, str | None]:
        second_snapshot: dict[str, Any] | None = None
        if snapshots is None:
            first_snapshot = await self._fresh_orderbook(symbol)
            if first_snapshot is None:
                return None, None, SimulationStopReason.STALE_ORDERBOOK
        else:
            first_snapshot, second_snapshot = snapshots
        first_order_side = OrderSide.BUY if first == PositionSide.LONG else OrderSide.SELL
        second_order_side = OrderSide.BUY if second == PositionSide.LONG else OrderSide.SELL
        first_full = await self._match_limit_orderbook(
            symbol,
            first_order_side,
            first,
            planned_qty,
            rules,
            wait_seconds=wait_seconds,
            initial_snapshot=first_snapshot,
        )
        if first_full is None:
            return None, None, SimulationStopReason.STALE_ORDERBOOK
        stage2_target_qty = normalize_qty(first_full.filled_qty, rules)
        if stage2_target_qty <= Decimal("0"):
            return first_full, self._zero_match(planned_qty=planned_qty, order_side=second_order_side, position_side=second), None
        second_full = await self._match_limit_orderbook(
            symbol,
            second_order_side,
            second,
            stage2_target_qty,
            rules,
            wait_seconds=wait_seconds,
            initial_snapshot=second_snapshot,
        )
        if second_full is None:
            return None, None, SimulationStopReason.STALE_ORDERBOOK
        paired_qty = normalize_qty(min(first_full.filled_qty, second_full.filled_qty), rules)
        if paired_qty <= Decimal("0"):
            return (
                self._cap_match_to_planned_qty(first_full, Decimal("0"), planned_qty, rules),
                self._cap_match_to_planned_qty(second_full, Decimal("0"), planned_qty, rules),
                None,
            )
        first_match = self._cap_match_to_planned_qty(first_full, paired_qty, planned_qty, rules)
        second_match = self._cap_match_to_planned_qty(second_full, paired_qty, planned_qty, rules)
        return first_match, second_match, None

    def _paired_open_reference_price(
        self,
        first_snapshot: dict[str, Any],
        second_snapshot: dict[str, Any],
        first: PositionSide,
        second: PositionSide,
    ) -> Decimal:
        first_price = self._best_orderbook_price(
            first_snapshot,
            OrderSide.BUY if first == PositionSide.LONG else OrderSide.SELL,
        )
        second_price = self._best_orderbook_price(
            second_snapshot,
            OrderSide.BUY if second == PositionSide.LONG else OrderSide.SELL,
        )
        if first_price <= Decimal("0") or second_price <= Decimal("0"):
            return Decimal("0")
        return (first_price + second_price) / Decimal("2")

    def _best_orderbook_price(self, snapshot: dict[str, Any], side: OrderSide) -> Decimal:
        levels = snapshot.get("asks") if side == OrderSide.BUY else snapshot.get("bids")
        if not levels:
            return Decimal("0")
        return _to_decimal(levels[0].get("price"))

    def _zero_match(
        self,
        *,
        planned_qty: Decimal,
        order_side: OrderSide,
        position_side: PositionSide,
        liquidity: str = "maker",
        wait_seconds_consumed: Decimal = Decimal("0"),
    ) -> MatchResult:
        return MatchResult(
            requested_qty=planned_qty,
            filled_qty=Decimal("0"),
            avg_price=Decimal("0"),
            notional=Decimal("0"),
            fee=Decimal("0"),
            residual_qty=planned_qty,
            depth_levels_consumed=0,
            slippage_bps=Decimal("0"),
            liquidity=liquidity,
            side=order_side,
            position_side=position_side,
            wait_seconds_consumed=wait_seconds_consumed,
        )

    async def _paired_close_matches(
        self,
        *,
        symbol: str,
        first: PositionSide,
        second: PositionSide,
        planned_qty: Decimal,
        rules: SymbolRules,
        wait_seconds: Decimal,
    ) -> tuple[MatchResult | None, MatchResult | None, str | None]:
        first_position = self._get_position(symbol, first)
        second_position = self._get_position(symbol, second)
        if (
            first_position is None
            or second_position is None
            or _to_decimal(first_position["qty"]) < planned_qty
            or _to_decimal(second_position["qty"]) < planned_qty
        ):
            return None, None, SimulationStopReason.INSUFFICIENT_SIM_POSITION
        first_order_side = OrderSide.SELL if first == PositionSide.LONG else OrderSide.BUY
        second_order_side = OrderSide.SELL if second == PositionSide.LONG else OrderSide.BUY
        first_full = await self._match_limit_orderbook(
            symbol,
            first_order_side,
            first,
            planned_qty,
            rules,
            wait_seconds=wait_seconds,
        )
        if first_full is None:
            return None, None, SimulationStopReason.STALE_ORDERBOOK
        stage2_target_qty = normalize_qty(first_full.filled_qty, rules)
        if stage2_target_qty <= Decimal("0"):
            return first_full, self._zero_match(planned_qty=planned_qty, order_side=second_order_side, position_side=second), None
        second_full = await self._match_limit_orderbook(
            symbol,
            second_order_side,
            second,
            stage2_target_qty,
            rules,
            wait_seconds=wait_seconds,
        )
        if second_full is None:
            return None, None, SimulationStopReason.STALE_ORDERBOOK
        paired_qty = normalize_qty(min(first_full.filled_qty, second_full.filled_qty), rules)
        if paired_qty <= Decimal("0"):
            return (
                self._cap_match_to_planned_qty(first_full, Decimal("0"), planned_qty, rules),
                self._cap_match_to_planned_qty(second_full, Decimal("0"), planned_qty, rules),
                None,
            )
        first_match = self._cap_match_to_planned_qty(first_full, paired_qty, planned_qty, rules)
        second_match = self._cap_match_to_planned_qty(second_full, paired_qty, planned_qty, rules)
        return first_match, second_match, None

    def _with_planned_residual(self, match: MatchResult, planned_qty: Decimal, rules: SymbolRules) -> MatchResult:
        return MatchResult(
            requested_qty=planned_qty,
            filled_qty=match.filled_qty,
            avg_price=match.avg_price,
            notional=match.notional,
            fee=match.fee,
            residual_qty=normalize_qty(max(planned_qty - match.filled_qty, Decimal("0")), rules),
            depth_levels_consumed=match.depth_levels_consumed,
            slippage_bps=match.slippage_bps,
            liquidity=match.liquidity,
            side=match.side,
            position_side=match.position_side,
            wait_seconds_consumed=match.wait_seconds_consumed,
        )

    def _cap_match_to_planned_qty(
        self,
        match: MatchResult,
        filled_qty: Decimal,
        planned_qty: Decimal,
        rules: SymbolRules,
    ) -> MatchResult:
        capped_qty = normalize_qty(min(filled_qty, match.filled_qty), rules)
        if capped_qty <= Decimal("0") or match.filled_qty <= Decimal("0"):
            return MatchResult(
                requested_qty=planned_qty,
                filled_qty=Decimal("0"),
                avg_price=Decimal("0"),
                notional=Decimal("0"),
                fee=Decimal("0"),
                residual_qty=planned_qty,
                depth_levels_consumed=match.depth_levels_consumed,
                slippage_bps=Decimal("0"),
                liquidity=match.liquidity,
                side=match.side,
                position_side=match.position_side,
                wait_seconds_consumed=match.wait_seconds_consumed,
            )
        ratio = capped_qty / match.filled_qty
        return MatchResult(
            requested_qty=planned_qty,
            filled_qty=capped_qty,
            avg_price=match.avg_price,
            notional=_money(match.notional * ratio),
            fee=_money(match.fee * ratio),
            residual_qty=normalize_qty(max(planned_qty - capped_qty, Decimal("0")), rules),
            depth_levels_consumed=match.depth_levels_consumed,
            slippage_bps=match.slippage_bps,
            liquidity=match.liquidity,
            side=match.side,
            position_side=match.position_side,
            wait_seconds_consumed=match.wait_seconds_consumed,
        )

    async def _execute_open_qty(
        self,
        *,
        run_id: str,
        request: SimulationRunRequest,
        symbol: str,
        position_side: PositionSide,
        qty: Decimal,
        leverage: int,
        rules: SymbolRules,
        rerun_source_run_id: str | None,
        round_index: int = 1,
        defer_run_record: bool = False,
        match: MatchResult | None = None,
        margin_ratio: Decimal = Decimal("1"),
    ) -> dict[str, Any]:
        order_side = OrderSide.BUY if position_side == PositionSide.LONG else OrderSide.SELL
        match = match or await self._match_limit_orderbook(
            symbol,
            order_side,
            position_side,
            qty,
            rules,
            wait_seconds=self._order_wait_seconds(request),
        )
        if match is None:
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.STALE_ORDERBOOK, rerun_source_run_id)
        if match.filled_qty <= Decimal("0"):
            return self._result_payload(
                run_id,
                request,
                "completed_with_skips",
                SimulationStopReason.LIMIT_ORDER_UNFILLED,
                rerun_source_run_id,
                filled_qty=Decimal("0"),
                avg_fill_price=Decimal("0"),
                fee=Decimal("0"),
                residual_qty=qty,
                realized_pnl=Decimal("0"),
                wait_seconds_consumed=match.wait_seconds_consumed,
                defer_run_record=defer_run_record,
            )
        if match.notional < rules.min_notional:
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.MIN_NOTIONAL_BLOCKED, rerun_source_run_id)

        margin = _money((match.notional / Decimal(max(leverage, 1))) * margin_ratio)
        settings = self._get_settings()
        available = await self._available_balance(settings)
        if margin + match.fee > available:
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.INSUFFICIENT_SIM_BALANCE, rerun_source_run_id)

        now = _utc_now()
        with self._repository._lock, self._repository._connection:
            wallet_after_fee = _money(self._wallet_balance(settings["initial_balance"]) - match.fee)
            self._insert_ledger_locked(
                run_id=run_id,
                event_type="fee",
                amount=-match.fee,
                balance_after=wallet_after_fee,
                payload={"symbol": symbol, "position_side": position_side.value, "liquidity": match.liquidity},
                created_at=now,
            )
            self._insert_ledger_locked(
                run_id=run_id,
                event_type="margin_reserved",
                amount=Decimal("0"),
                balance_after=wallet_after_fee,
                payload={"symbol": symbol, "position_side": position_side.value, "margin": margin},
                created_at=now,
            )
            self._upsert_open_position_locked(symbol, position_side, match.filled_qty, match.avg_price, margin, leverage, rules, now)
            self._insert_fill_locked(run_id, round_index, symbol, match, now)

        invariant = self._validate_account_invariants()
        if invariant is not None:
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.SIMULATION_ACCOUNT_INVARIANT_FAILED, rerun_source_run_id)
        return self._result_payload(
            run_id,
            request,
            "completed" if match.residual_qty <= Decimal("0") else "completed_with_skips",
            SimulationStopReason.FILLED if match.residual_qty <= Decimal("0") else SimulationStopReason.LIMIT_ORDER_UNFILLED,
            rerun_source_run_id,
            filled_qty=match.filled_qty,
            avg_fill_price=match.avg_price,
            fee=match.fee,
            residual_qty=match.residual_qty,
            realized_pnl=Decimal("0"),
            wait_seconds_consumed=match.wait_seconds_consumed,
            defer_run_record=defer_run_record,
        )

    async def _execute_close_qty(
        self,
        *,
        run_id: str,
        request: SimulationRunRequest,
        symbol: str,
        position_side: PositionSide,
        qty: Decimal,
        rules: SymbolRules,
        rerun_source_run_id: str | None,
        round_index: int = 1,
        defer_run_record: bool = False,
        match: MatchResult | None = None,
    ) -> dict[str, Any]:
        position = self._get_position(symbol, position_side)
        if position is None or _to_decimal(position["qty"]) < qty or qty <= Decimal("0"):
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.INSUFFICIENT_SIM_POSITION, rerun_source_run_id)
        order_side = OrderSide.SELL if position_side == PositionSide.LONG else OrderSide.BUY
        match = match or await self._match_limit_orderbook(
            symbol,
            order_side,
            position_side,
            qty,
            rules,
            wait_seconds=self._order_wait_seconds(request),
        )
        if match is None:
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.STALE_ORDERBOOK, rerun_source_run_id)
        if match.filled_qty <= Decimal("0"):
            return self._result_payload(
                run_id,
                request,
                "completed_with_skips",
                SimulationStopReason.LIMIT_ORDER_UNFILLED,
                rerun_source_run_id,
                filled_qty=Decimal("0"),
                avg_fill_price=Decimal("0"),
                fee=Decimal("0"),
                residual_qty=qty,
                realized_pnl=Decimal("0"),
                wait_seconds_consumed=match.wait_seconds_consumed,
                defer_run_record=defer_run_record,
            )
        if match.notional < rules.min_notional:
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.MIN_NOTIONAL_BLOCKED, rerun_source_run_id)

        old_qty = _to_decimal(position["qty"])
        entry_price = _to_decimal(position["entry_price"])
        old_margin = _to_decimal(position["margin"])
        close_ratio = match.filled_qty / old_qty
        released_margin = _money(old_margin * close_ratio)
        realized_pnl = _money(
            (match.avg_price - entry_price) * match.filled_qty
            if position_side == PositionSide.LONG
            else (entry_price - match.avg_price) * match.filled_qty
        )
        settings = self._get_settings()
        now = _utc_now()
        with self._repository._lock, self._repository._connection:
            wallet_after_pnl = _money(self._wallet_balance(settings["initial_balance"]) + realized_pnl)
            self._insert_ledger_locked(
                run_id=run_id,
                event_type="realized_pnl",
                amount=realized_pnl,
                balance_after=wallet_after_pnl,
                payload={"symbol": symbol, "position_side": position_side.value},
                created_at=now,
            )
            wallet_after_fee = _money(wallet_after_pnl - match.fee)
            self._insert_ledger_locked(
                run_id=run_id,
                event_type="fee",
                amount=-match.fee,
                balance_after=wallet_after_fee,
                payload={"symbol": symbol, "position_side": position_side.value, "liquidity": match.liquidity},
                created_at=now,
            )
            self._insert_ledger_locked(
                run_id=run_id,
                event_type="margin_released",
                amount=Decimal("0"),
                balance_after=wallet_after_fee,
                payload={"symbol": symbol, "position_side": position_side.value, "margin": released_margin},
                created_at=now,
            )
            self._reduce_position_locked(symbol, position_side, match.filled_qty, released_margin, now)
            self._insert_fill_locked(run_id, round_index, symbol, match, now)

        invariant = self._validate_account_invariants()
        if invariant is not None:
            return self._blocked_result(run_id, request, "blocked", SimulationStopReason.SIMULATION_ACCOUNT_INVARIANT_FAILED, rerun_source_run_id)
        return self._result_payload(
            run_id,
            request,
            "completed" if match.residual_qty <= Decimal("0") else "completed_with_skips",
            SimulationStopReason.FILLED if match.residual_qty <= Decimal("0") else SimulationStopReason.LIMIT_ORDER_UNFILLED,
            rerun_source_run_id,
            filled_qty=match.filled_qty,
            avg_fill_price=match.avg_price,
            fee=match.fee,
            residual_qty=match.residual_qty,
            realized_pnl=realized_pnl,
            wait_seconds_consumed=match.wait_seconds_consumed,
            defer_run_record=defer_run_record,
        )

    async def _match_orderbook(
        self,
        symbol: str,
        order_side: OrderSide,
        position_side: PositionSide,
        target_qty: Decimal,
        rules: SymbolRules,
        *,
        liquidity: str,
    ) -> MatchResult | None:
        snapshot = await self._fresh_orderbook(symbol)
        if snapshot is None:
            return None
        return self._match_orderbook_snapshot(snapshot, order_side, position_side, target_qty, rules, liquidity=liquidity)

    async def _match_limit_orderbook(
        self,
        symbol: str,
        order_side: OrderSide,
        position_side: PositionSide,
        target_qty: Decimal,
        rules: SymbolRules,
        *,
        wait_seconds: Decimal,
        initial_snapshot: dict[str, Any] | None = None,
    ) -> MatchResult | None:
        snapshot = initial_snapshot or await self._fresh_orderbook(symbol)
        if snapshot is None:
            return None
        limit_price = self._passive_limit_price(snapshot, order_side)
        if limit_price <= Decimal("0"):
            return self._match_orderbook_snapshot(snapshot, order_side, position_side, target_qty, rules, liquidity="taker")
        if self._limit_order_crosses(snapshot, order_side, limit_price):
            return self._match_orderbook_snapshot(
                snapshot,
                order_side,
                position_side,
                target_qty,
                rules,
                liquidity="taker",
                limit_price=limit_price,
            )
        return await self._poll_passive_limit_fill(
            symbol=symbol,
            order_side=order_side,
            position_side=position_side,
            target_qty=target_qty,
            rules=rules,
            limit_price=limit_price,
            wait_seconds=wait_seconds,
        )

    def _match_orderbook_snapshot(
        self,
        snapshot: dict[str, Any],
        order_side: OrderSide,
        position_side: PositionSide,
        target_qty: Decimal,
        rules: SymbolRules,
        *,
        liquidity: str,
        limit_price: Decimal | None = None,
    ) -> MatchResult:
        levels_key = "asks" if order_side == OrderSide.BUY else "bids"
        levels = snapshot.get(levels_key) or []
        remaining = target_qty
        filled = Decimal("0")
        notional = Decimal("0")
        consumed = 0
        best_price = None
        for raw_level in levels:
            price = _to_decimal(raw_level.get("price"))
            if limit_price is not None:
                if order_side == OrderSide.BUY and price > limit_price:
                    break
                if order_side == OrderSide.SELL and price < limit_price:
                    break
            available_qty = normalize_qty(_to_decimal(raw_level.get("qty")), rules)
            if available_qty <= Decimal("0"):
                continue
            if best_price is None:
                best_price = price
            fill_qty = min(remaining, available_qty)
            if fill_qty <= Decimal("0"):
                continue
            filled += fill_qty
            notional += fill_qty * price
            remaining -= fill_qty
            consumed += 1
            if remaining <= Decimal("0"):
                break
        filled = normalize_qty(filled, rules)
        remaining = normalize_qty(max(remaining, Decimal("0")), rules)
        if filled <= Decimal("0"):
            return MatchResult(
                requested_qty=target_qty,
                filled_qty=Decimal("0"),
                avg_price=Decimal("0"),
                notional=Decimal("0"),
                fee=Decimal("0"),
                residual_qty=target_qty,
                depth_levels_consumed=0,
                slippage_bps=Decimal("0"),
                liquidity=liquidity,
                side=order_side,
                position_side=position_side,
            )
        avg_price = notional / filled
        settings = self._get_settings()
        fee_rate = settings["maker_fee_rate"] if liquidity == "maker" else settings["taker_fee_rate"]
        fee = _money(notional * fee_rate)
        if best_price and best_price > Decimal("0"):
            if order_side == OrderSide.BUY:
                slippage = ((avg_price - best_price) / best_price) * Decimal("10000")
            else:
                slippage = ((best_price - avg_price) / best_price) * Decimal("10000")
        else:
            slippage = Decimal("0")
        return MatchResult(
            requested_qty=target_qty,
            filled_qty=filled,
            avg_price=_money(avg_price).quantize(MONEY_SCALE).normalize(),
            notional=_money(notional),
            fee=fee,
            residual_qty=remaining,
            depth_levels_consumed=consumed,
            slippage_bps=_money(slippage),
            liquidity=liquidity,
            side=order_side,
            position_side=position_side,
        )

    async def _poll_passive_limit_fill(
        self,
        *,
        symbol: str,
        order_side: OrderSide,
        position_side: PositionSide,
        target_qty: Decimal,
        rules: SymbolRules,
        limit_price: Decimal,
        wait_seconds: Decimal,
    ) -> MatchResult:
        remaining = normalize_qty(target_qty, rules)
        filled = Decimal("0")
        notional = Decimal("0")
        polls = 0
        elapsed = Decimal("0")
        await self._publish_active_stage(SimulationRunStage.WAITING_FILL)
        while remaining > Decimal("0") and elapsed < wait_seconds:
            step = min(SIMULATION_MAKER_POLL_SECONDS, wait_seconds - elapsed)
            if step > Decimal("0"):
                try:
                    await asyncio.wait_for(self._abort_event.wait(), timeout=float(step))
                    elapsed += step
                    break
                except TimeoutError:
                    elapsed += step
                    await self._publish_active_stage(SimulationRunStage.WAITING_FILL)
            if self._abort_requested:
                break
            snapshot = await self._fresh_orderbook(symbol)
            if snapshot is None:
                break
            polls += 1
            fill_qty = self._passive_fill_qty_from_snapshot(snapshot, order_side, limit_price, remaining, rules)
            if fill_qty <= Decimal("0"):
                continue
            filled += fill_qty
            notional += fill_qty * limit_price
            remaining = normalize_qty(max(remaining - fill_qty, Decimal("0")), rules)
        filled = normalize_qty(filled, rules)
        remaining = normalize_qty(max(target_qty - filled, Decimal("0")), rules)
        if filled <= Decimal("0"):
            return MatchResult(
                requested_qty=target_qty,
                filled_qty=Decimal("0"),
                avg_price=Decimal("0"),
                notional=Decimal("0"),
                fee=Decimal("0"),
                residual_qty=target_qty,
                depth_levels_consumed=polls,
                slippage_bps=Decimal("0"),
                liquidity="maker",
                side=order_side,
                position_side=position_side,
                wait_seconds_consumed=elapsed,
            )
        settings = self._get_settings()
        fee = _money(notional * settings["maker_fee_rate"])
        return MatchResult(
            requested_qty=target_qty,
            filled_qty=filled,
            avg_price=_money(limit_price).quantize(MONEY_SCALE).normalize(),
            notional=_money(notional),
            fee=fee,
            residual_qty=remaining,
            depth_levels_consumed=polls,
            slippage_bps=Decimal("0"),
            liquidity="maker",
            side=order_side,
            position_side=position_side,
            wait_seconds_consumed=elapsed,
        )

    def _passive_limit_price(self, snapshot: dict[str, Any], order_side: OrderSide) -> Decimal:
        levels = snapshot.get("bids") if order_side == OrderSide.BUY else snapshot.get("asks")
        if not levels:
            return Decimal("0")
        return _to_decimal(levels[0].get("price"))

    def _limit_order_crosses(self, snapshot: dict[str, Any], order_side: OrderSide, limit_price: Decimal) -> bool:
        opposite_levels = snapshot.get("asks") if order_side == OrderSide.BUY else snapshot.get("bids")
        if not opposite_levels:
            return False
        opposite_best = _to_decimal(opposite_levels[0].get("price"))
        if opposite_best <= Decimal("0"):
            return False
        if order_side == OrderSide.BUY:
            return opposite_best <= limit_price
        return opposite_best >= limit_price

    def _passive_fill_qty_from_snapshot(
        self,
        snapshot: dict[str, Any],
        order_side: OrderSide,
        limit_price: Decimal,
        remaining: Decimal,
        rules: SymbolRules,
    ) -> Decimal:
        fillable = Decimal("0")
        opposite_levels = snapshot.get("asks") if order_side == OrderSide.BUY else snapshot.get("bids")
        for raw_level in opposite_levels or []:
            price = _to_decimal(raw_level.get("price"))
            if order_side == OrderSide.BUY and price > limit_price:
                break
            if order_side == OrderSide.SELL and price < limit_price:
                break
            fillable += normalize_qty(_to_decimal(raw_level.get("qty")), rules)
        if fillable <= Decimal("0"):
            same_levels = snapshot.get("bids") if order_side == OrderSide.BUY else snapshot.get("asks")
            same_best = _to_decimal((same_levels or [{}])[0].get("price"))
            if order_side == OrderSide.BUY and same_best > Decimal("0") and same_best < limit_price:
                fillable = remaining
            elif order_side == OrderSide.SELL and same_best > limit_price:
                fillable = remaining
        return normalize_qty(min(remaining, fillable), rules)

    async def _fresh_orderbook(self, symbol: str) -> dict[str, Any] | None:
        try:
            snapshot = await self._gateway.get_order_book(symbol, limit=20)
        except Exception:
            snapshot = None
        if self._orderbook_is_fresh(snapshot):
            return snapshot
        try:
            snapshot = await self._gateway.refresh_order_book(symbol, limit=20)
        except Exception:
            return None
        if self._orderbook_is_fresh(snapshot):
            return snapshot
        return None

    def _orderbook_is_fresh(self, snapshot: dict[str, Any] | None) -> bool:
        if snapshot is None:
            return False
        event_time = snapshot.get("event_time") or snapshot.get("updated_at")
        if event_time is None:
            return False
        if isinstance(event_time, (int, float)):
            event_dt = datetime.fromtimestamp(float(event_time) / 1000, tz=UTC)
        elif isinstance(event_time, str):
            event_dt = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
            if event_dt.tzinfo is None:
                event_dt = event_dt.replace(tzinfo=UTC)
        elif isinstance(event_time, datetime):
            event_dt = event_time if event_time.tzinfo else event_time.replace(tzinfo=UTC)
        else:
            return False
        age = (_utc_now() - event_dt.astimezone(UTC)).total_seconds()
        return age <= ORDERBOOK_MAX_AGE_SECONDS

    def _get_settings(self) -> dict[str, Decimal]:
        row = self._repository._connection.execute(
            "SELECT * FROM simulation_account_settings WHERE account_id = ?",
            (self._account_id,),
        ).fetchone()
        if row is None:
            return {
                "initial_balance": DEFAULT_INITIAL_BALANCE,
                "maker_fee_rate": DEFAULT_MAKER_FEE_RATE,
                "taker_fee_rate": DEFAULT_TAKER_FEE_RATE,
            }
        return {
            "initial_balance": _to_decimal(row["initial_balance"], DEFAULT_INITIAL_BALANCE),
            "maker_fee_rate": _to_decimal(row["maker_fee_rate"], DEFAULT_MAKER_FEE_RATE),
            "taker_fee_rate": _to_decimal(row["taker_fee_rate"], DEFAULT_TAKER_FEE_RATE),
        }

    def _wallet_balance(self, initial_balance: Decimal) -> Decimal:
        rows = self._repository._connection.execute(
            "SELECT amount FROM simulation_ledger WHERE account_id = ?",
            (self._account_id,),
        ).fetchall()
        ledger_total = sum((_to_decimal(row["amount"]) for row in rows), Decimal("0"))
        return _money(initial_balance + ledger_total)

    def _current_margin(self) -> Decimal:
        self._reconcile_position_margins()
        return _money(sum((_to_decimal(position["margin"]) for position in self._list_positions()), Decimal("0")))

    async def _available_balance(self, settings: dict[str, Decimal]) -> Decimal:
        self._reconcile_position_margins()
        positions = self._list_positions()
        wallet = self._wallet_balance(settings["initial_balance"])
        margin = _money(sum((_to_decimal(position["margin"]) for position in positions), Decimal("0")))
        return _money(wallet + await self._unrealized_pnl(positions) - margin)

    async def _unrealized_pnl(self, positions: list[sqlite3.Row]) -> Decimal:
        total = Decimal("0")
        for position in positions:
            total += await self._position_unrealized_pnl(position)
        return _money(total)

    async def _position_unrealized_pnl(self, position: sqlite3.Row) -> Decimal:
        mark_price = await self._position_mark_price(position)
        return self._position_unrealized_pnl_at_mark(position, mark_price)

    def _position_unrealized_pnl_at_mark(self, position: sqlite3.Row, mark_price: Decimal) -> Decimal:
        qty = _to_decimal(position["qty"])
        if qty <= Decimal("0"):
            return Decimal("0")
        entry_price = _to_decimal(position["entry_price"])
        if position["position_side"] == PositionSide.SHORT.value:
            return _money((entry_price - mark_price) * qty)
        return _money((mark_price - entry_price) * qty)

    async def _position_mark_price(self, position: sqlite3.Row) -> Decimal:
        entry_price = _to_decimal(position["entry_price"])
        try:
            quote = await self._gateway.get_quote(position["symbol"])
        except Exception:
            return entry_price
        if position["position_side"] == PositionSide.SHORT.value:
            return _to_decimal(getattr(quote, "ask_price", None), entry_price)
        return _to_decimal(getattr(quote, "bid_price", None), entry_price)

    def _list_positions(self) -> list[sqlite3.Row]:
        rows = self._repository._connection.execute(
            "SELECT * FROM simulation_positions WHERE account_id = ? ORDER BY symbol ASC, position_side ASC",
            (self._account_id,),
        ).fetchall()
        return list(rows)

    def _reconcile_position_margins(self) -> None:
        with self._repository._lock, self._repository._connection:
            self._reconcile_position_margins_locked()

    def _reconcile_position_margins_locked(self) -> None:
        rows = self._repository._connection.execute(
            "SELECT * FROM simulation_positions WHERE account_id = ?",
            (self._account_id,),
        ).fetchall()
        now = _utc_now().isoformat()
        for row in rows:
            expected_margin = self._expected_position_margin(row)
            if expected_margin is None:
                continue
            current_margin = _to_decimal(row["margin"])
            if current_margin == expected_margin:
                continue
            self._repository._connection.execute(
                """
                UPDATE simulation_positions
                SET margin = ?, updated_at = ?
                WHERE account_id = ? AND symbol = ? AND position_side = ?
                """,
                (
                    _format_decimal(expected_margin),
                    now,
                    self._account_id,
                    row["symbol"],
                    row["position_side"],
                ),
            )

    def _expected_position_margin(self, position: sqlite3.Row) -> Decimal | None:
        qty = _to_decimal(position["qty"])
        entry_price = _to_decimal(position["entry_price"])
        leverage = max(int(position["leverage"] or 1), 1)
        if qty <= Decimal("0") or entry_price <= Decimal("0"):
            return None
        return _money((qty * entry_price) / Decimal(leverage))

    def _get_position(self, symbol: str, position_side: PositionSide) -> sqlite3.Row | None:
        return self._repository._connection.execute(
            "SELECT * FROM simulation_positions WHERE account_id = ? AND symbol = ? AND position_side = ?",
            (self._account_id, symbol, position_side.value),
        ).fetchone()

    def _alignment_open_side(self, symbol: str) -> PositionSide | None:
        long_position = self._get_position(symbol, PositionSide.LONG)
        short_position = self._get_position(symbol, PositionSide.SHORT)
        long_qty = _to_decimal(long_position["qty"] if long_position is not None else None)
        short_qty = _to_decimal(short_position["qty"] if short_position is not None else None)
        if long_qty > short_qty:
            return PositionSide.SHORT
        if short_qty > long_qty:
            return PositionSide.LONG
        return PositionSide.LONG

    def _alignment_close_side(self, symbol: str) -> PositionSide | None:
        long_position = self._get_position(symbol, PositionSide.LONG)
        short_position = self._get_position(symbol, PositionSide.SHORT)
        long_qty = _to_decimal(long_position["qty"] if long_position is not None else None)
        short_qty = _to_decimal(short_position["qty"] if short_position is not None else None)
        if long_qty <= Decimal("0") and short_qty <= Decimal("0"):
            return None
        return PositionSide.LONG if long_qty >= short_qty else PositionSide.SHORT

    def _upsert_open_position_locked(
        self,
        symbol: str,
        position_side: PositionSide,
        qty: Decimal,
        avg_price: Decimal,
        margin: Decimal,
        leverage: int,
        rules: SymbolRules,
        updated_at: datetime,
    ) -> None:
        existing = self._get_position(symbol, position_side)
        if existing is None:
            next_qty = qty
            next_entry = avg_price
            next_margin = margin
        else:
            old_qty = _to_decimal(existing["qty"])
            old_entry = _to_decimal(existing["entry_price"])
            old_margin = _to_decimal(existing["margin"])
            next_qty = normalize_qty(old_qty + qty, rules)
            next_entry = ((old_entry * old_qty) + (avg_price * qty)) / next_qty
            next_margin = _money(old_margin + margin)
        self._repository._connection.execute(
            """
            INSERT INTO simulation_positions (account_id, symbol, position_side, qty, entry_price, margin, leverage, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, symbol, position_side) DO UPDATE SET
                qty = excluded.qty,
                entry_price = excluded.entry_price,
                margin = excluded.margin,
                leverage = excluded.leverage,
                updated_at = excluded.updated_at
            """,
            (
                self._account_id,
                symbol,
                position_side.value,
                _format_decimal(next_qty),
                _format_plain_decimal(next_entry),
                _format_decimal(next_margin),
                int(leverage),
                updated_at.isoformat(),
            ),
        )

    def _reduce_position_locked(
        self,
        symbol: str,
        position_side: PositionSide,
        qty: Decimal,
        released_margin: Decimal,
        updated_at: datetime,
    ) -> None:
        existing = self._get_position(symbol, position_side)
        if existing is None:
            raise SimulationError("position missing")
        old_qty = _to_decimal(existing["qty"])
        old_margin = _to_decimal(existing["margin"])
        next_qty = old_qty - qty
        if next_qty <= Decimal("0"):
            self._repository._connection.execute(
                "DELETE FROM simulation_positions WHERE account_id = ? AND symbol = ? AND position_side = ?",
                (self._account_id, symbol, position_side.value),
            )
            return
        next_margin = max(Decimal("0"), _money(old_margin - released_margin))
        self._repository._connection.execute(
            """
            UPDATE simulation_positions
            SET qty = ?, margin = ?, updated_at = ?
            WHERE account_id = ? AND symbol = ? AND position_side = ?
            """,
            (_format_decimal(next_qty), _format_decimal(next_margin), updated_at.isoformat(), self._account_id, symbol, position_side.value),
        )

    def _insert_run_locked(
        self,
        *,
        run_id: str,
        event_type: str,
        request: dict[str, Any],
        result: dict[str, Any],
        status: str,
        stop_reason: str,
        created_at: datetime,
        rerun_source_run_id: str | None = None,
        stage: str = SimulationRunStage.COMPLETED,
        heartbeat_at: datetime | None = None,
        last_event_at: datetime | None = None,
        lock_reason: str | None = None,
    ) -> None:
        self._repository._connection.execute(
            """
            INSERT OR REPLACE INTO simulation_runs (
                run_id, account_id, event_type, session_kind, symbol, status, stop_reason,
                request_json, result_json, rerun_source_run_id, simulation_schema_version,
                engine_version, created_at, updated_at, stage, heartbeat_at, last_event_at, lock_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                self._account_id,
                event_type,
                request.get("session_kind"),
                request.get("symbol"),
                status,
                stop_reason,
                _json_dumps(request),
                _json_dumps(result),
                rerun_source_run_id,
                SIMULATION_SCHEMA_VERSION,
                SIMULATION_ENGINE_VERSION,
                created_at.isoformat(),
                _utc_now().isoformat(),
                stage,
                (heartbeat_at or created_at).isoformat(),
                (last_event_at or heartbeat_at or created_at).isoformat(),
                lock_reason,
            ),
        )

    def _update_run_locked(
        self,
        *,
        run_id: str,
        status: str | None = None,
        stage: str | None = None,
        result: dict[str, Any] | None = None,
        stop_reason: str | None = None,
        heartbeat_at: datetime | None = None,
        last_event_at: datetime | None = None,
        lock_reason: str | None = None,
    ) -> None:
        updates: list[str] = ["updated_at = ?"]
        params: list[Any] = [_utc_now().isoformat()]
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if stage is not None:
            updates.append("stage = ?")
            params.append(stage)
        if result is not None:
            updates.append("result_json = ?")
            params.append(_json_dumps(result))
        if stop_reason is not None:
            updates.append("stop_reason = ?")
            params.append(stop_reason)
        if heartbeat_at is not None:
            updates.append("heartbeat_at = ?")
            params.append(heartbeat_at.isoformat())
        if last_event_at is not None:
            updates.append("last_event_at = ?")
            params.append(last_event_at.isoformat())
        if lock_reason is not None:
            updates.append("lock_reason = ?")
            params.append(lock_reason)
        params.extend([self._account_id, run_id])
        self._repository._connection.execute(
            f"UPDATE simulation_runs SET {', '.join(updates)} WHERE account_id = ? AND run_id = ?",
            tuple(params),
        )

    def _run_created_at_locked(self, run_id: str) -> datetime | None:
        row = self._repository._connection.execute(
            "SELECT created_at FROM simulation_runs WHERE account_id = ? AND run_id = ?",
            (self._account_id, run_id),
        ).fetchone()
        if row is None or not row["created_at"]:
            return None
        try:
            created_at = datetime.fromisoformat(str(row["created_at"]))
        except ValueError:
            return None
        if created_at.tzinfo is None:
            return created_at.replace(tzinfo=UTC)
        return created_at

    def _insert_event_locked(
        self,
        *,
        run_id: str,
        event_type: str,
        level: str,
        message_code: str | None,
        message_params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        created_at: datetime | None = None,
    ) -> int:
        now = created_at or _utc_now()
        cursor = self._repository._connection.execute(
            """
            INSERT INTO simulation_events (
                account_id, run_id, event_type, level, message_code, message_params_json, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self._account_id,
                run_id,
                event_type,
                level,
                message_code,
                _json_dumps(message_params or {}),
                _json_dumps(payload or {}),
                now.isoformat(),
            ),
        )
        return int(cursor.lastrowid or 0)

    async def _publish_active_stage(self, stage: str) -> None:
        run_id = self._active_run_id
        if not run_id:
            return
        now = _utc_now()
        with self._repository._lock, self._repository._connection:
            self._update_run_locked(
                run_id=run_id,
                status="running",
                stage=stage,
                heartbeat_at=now,
            )
        self._active_status = "running"
        self._active_stage = stage
        self._active_heartbeat_at = now.isoformat()
        await self._publish(
            "simulation_run",
            {
                "contract_version": CONTRACT_VERSION,
                "catalog_version": CATALOG_VERSION,
                "event_type": "simulation_run",
                "run_id": run_id,
                "status": "running",
                "stage": stage,
                "heartbeat_at": now.isoformat(),
            },
        )

    def _event_payload_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = _json_load(row["payload_json"], {})
        message_params = _json_load(row["message_params_json"], {})
        return {
            "contract_version": CONTRACT_VERSION,
            "catalog_version": CATALOG_VERSION,
            "event_id": int(row["event_id"]),
            "run_id": row["run_id"],
            "event_type": row["event_type"],
            "level": row["level"],
            "message_code": row["message_code"],
            "message_params": message_params,
            "message": format_copy(row["message_code"], message_params) if row["message_code"] else "",
            "payload": payload,
            "created_at": row["created_at"],
        }

    def _insert_ledger_locked(
        self,
        *,
        run_id: str,
        event_type: str,
        amount: Decimal,
        balance_after: Decimal,
        payload: dict[str, Any],
        created_at: datetime,
    ) -> None:
        self._repository._connection.execute(
            """
            INSERT INTO simulation_ledger (account_id, run_id, event_type, amount, balance_after, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self._account_id,
                run_id,
                event_type,
                _format_decimal(_money(amount)),
                _format_decimal(_money(balance_after)),
                _json_dumps(payload),
                created_at.isoformat(),
            ),
        )

    def _insert_fill_locked(
        self,
        run_id: str,
        round_index: int,
        symbol: str,
        match: MatchResult,
        created_at: datetime,
    ) -> None:
        self._repository._connection.execute(
            """
            INSERT INTO simulation_fills (
                account_id, run_id, round_index, symbol, position_side, side, qty, avg_price, notional,
                fee, liquidity, depth_levels_consumed, slippage_bps, residual_qty, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self._account_id,
                run_id,
                int(round_index),
                symbol,
                match.position_side.value,
                match.side.value,
                _format_decimal(match.filled_qty),
                _format_plain_decimal(match.avg_price),
                _format_decimal(match.notional),
                _format_decimal(match.fee),
                match.liquidity,
                int(match.depth_levels_consumed),
                _format_decimal(match.slippage_bps),
                _format_decimal(match.residual_qty),
                _json_dumps({"requested_qty": match.requested_qty, "wait_seconds_consumed": match.wait_seconds_consumed}),
                created_at.isoformat(),
            ),
        )

    def _insert_snapshot_locked(self, run_id: str, snapshot_type: str, payload: dict[str, Any], created_at: datetime) -> None:
        self._repository._connection.execute(
            """
            INSERT INTO simulation_snapshots (account_id, run_id, snapshot_type, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (self._account_id, run_id, snapshot_type, _json_dumps(payload), created_at.isoformat()),
        )

    def _validate_account_invariants(self) -> str | None:
        settings = self._get_settings()
        wallet = self._wallet_balance(settings["initial_balance"])
        if wallet < Decimal("0"):
            return "wallet_negative"
        for position in self._list_positions():
            if _to_decimal(position["qty"]) < Decimal("0") or _to_decimal(position["margin"]) < Decimal("0"):
                return "position_negative"
        return None

    def _start_payload(
        self,
        run_id: str,
        request: SimulationRunRequest,
        rerun_source_run_id: str | None,
        *,
        created_at: datetime,
    ) -> dict[str, Any]:
        return {
            "contract_version": CONTRACT_VERSION,
            "catalog_version": CATALOG_VERSION,
            "simulation_schema_version": SIMULATION_SCHEMA_VERSION,
            "engine_version": SIMULATION_ENGINE_VERSION,
            "run_id": run_id,
            "event_type": "simulation_run",
            "requested": True,
            "requested_action": "run",
            "status": "running",
            "stage": SimulationRunStage.STARTING,
            "stop_reason": SimulationStopReason.RUNNING,
            "lock_reason": SimulationLockReason.SIMULATION_RUNNING,
            "session_kind": request.session_kind.value,
            "symbol": request.symbol.upper(),
            "rounds_total": int(request.round_count or 0),
            "rounds_completed": 0,
            "message_code": "runtime.simulation_run_started",
            "message_params": {
                "mode": request.session_kind.value,
                "symbol": request.symbol.upper(),
                "round_count": int(request.round_count or 0),
            },
            "rerun_source_run_id": rerun_source_run_id,
            "heartbeat_at": created_at.isoformat(),
            "last_event_at": created_at.isoformat(),
            "created_at": created_at.isoformat(),
        }

    def _blocked_result(
        self,
        run_id: str,
        request: SimulationRunRequest,
        status: str,
        stop_reason: str,
        rerun_source_run_id: str | None,
    ) -> dict[str, Any]:
        return self._result_payload(
            run_id,
            request,
            status,
            stop_reason,
            rerun_source_run_id,
            filled_qty=Decimal("0"),
            avg_fill_price=Decimal("0"),
            fee=Decimal("0"),
            residual_qty=Decimal("0"),
            realized_pnl=Decimal("0"),
            rounds_completed=0,
        )

    def _result_payload(
        self,
        run_id: str,
        request: SimulationRunRequest,
        status: str,
        stop_reason: str,
        rerun_source_run_id: str | None,
        *,
        filled_qty: Decimal,
        avg_fill_price: Decimal,
        fee: Decimal,
        residual_qty: Decimal,
        realized_pnl: Decimal,
        rounds_completed: int | None = None,
        rounds_total: int | None = None,
        wait_seconds_consumed: Decimal = Decimal("0"),
        defer_run_record: bool = False,
    ) -> dict[str, Any]:
        stop_reason_text = str(stop_reason)
        stop_reason_label = self._localized_stop_reason(stop_reason_text)
        return {
            "contract_version": CONTRACT_VERSION,
            "simulation_schema_version": SIMULATION_SCHEMA_VERSION,
            "engine_version": SIMULATION_ENGINE_VERSION,
            "run_id": run_id,
            "event_type": "simulation_run",
            "status": status,
            "stage": SimulationRunStage.COMPLETED,
            "stop_reason": stop_reason_text,
            "session_kind": request.session_kind.value,
            "symbol": request.symbol.upper(),
            "rounds_total": int(rounds_total if rounds_total is not None else (request.round_count or 0)),
            "rounds_completed": int(rounds_completed or 0),
            "filled_qty": _format_decimal(filled_qty),
            "avg_fill_price": _format_plain_decimal(avg_fill_price),
            "fee": _format_decimal(_money(fee)),
            "residual_qty": _format_decimal(residual_qty),
            "realized_pnl": _format_decimal(_money(realized_pnl)),
            "wait_seconds_consumed": _format_decimal(wait_seconds_consumed),
            "message_code": "runtime.simulation_run_finished",
            "message_params": {"stop_reason": stop_reason_label},
            "rerun_source_run_id": rerun_source_run_id,
            "defer_run_record": defer_run_record,
            "created_at": _utc_now().isoformat(),
        }

    def _combine_leg_results(
        self,
        run_id: str,
        request: SimulationRunRequest,
        results: list[dict[str, Any]],
        rerun_source_run_id: str | None,
    ) -> dict[str, Any]:
        successful_statuses = {"completed", "completed_with_skips"}
        if any(result["status"] not in successful_statuses for result in results):
            return next(result for result in results if result["status"] not in successful_statuses)
        filled = sum((_to_decimal(result.get("filled_qty")) for result in results), Decimal("0"))
        fee = sum((_to_decimal(result.get("fee")) for result in results), Decimal("0"))
        realized = sum((_to_decimal(result.get("realized_pnl")) for result in results), Decimal("0"))
        residual = sum((_to_decimal(result.get("residual_qty")) for result in results), Decimal("0"))
        wait_seconds_consumed = max(
            (_to_decimal(result.get("wait_seconds_consumed")) for result in results),
            default=Decimal("0"),
        )
        notional = sum(
            (_to_decimal(result.get("filled_qty")) * _to_decimal(result.get("avg_fill_price")) for result in results),
            Decimal("0"),
        )
        avg_price = notional / filled if filled > Decimal("0") else Decimal("0")
        return self._result_payload(
            run_id,
            request,
            "completed" if residual <= Decimal("0") else "completed_with_skips",
            SimulationStopReason.FILLED if residual <= Decimal("0") else SimulationStopReason.LIMIT_ORDER_UNFILLED,
            rerun_source_run_id,
            filled_qty=filled,
            avg_fill_price=avg_price,
            fee=fee,
            residual_qty=residual,
            realized_pnl=realized,
            wait_seconds_consumed=wait_seconds_consumed,
        )

    def _request_payload(self, request: SimulationRunRequest) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "session_kind": _enum_value(request.session_kind),
                "symbol": request.symbol.upper(),
                "trend_bias": _enum_value(request.trend_bias),
                "open_mode": _enum_value(request.open_mode),
                "close_mode": _enum_value(request.close_mode),
                "selected_position_side": _enum_value(request.selected_position_side),
                "open_amount": _format_decimal(request.open_amount) if request.open_amount is not None else None,
                "open_qty": _format_decimal(request.open_qty) if request.open_qty is not None else None,
                "close_qty": _format_decimal(request.close_qty) if request.close_qty is not None else None,
                "leverage": request.leverage,
                "round_count": request.round_count,
                "round_interval_seconds": request.round_interval_seconds,
                "execution_profile": _enum_value(request.execution_profile),
                "market_fallback_max_ratio": _format_decimal(request.market_fallback_max_ratio)
                if request.market_fallback_max_ratio is not None
                else None,
                "market_fallback_min_residual_qty": _format_decimal(request.market_fallback_min_residual_qty)
                if request.market_fallback_min_residual_qty is not None
                else None,
                "max_reprice_ticks": request.max_reprice_ticks,
                "max_spread_bps": request.max_spread_bps,
                "max_reference_deviation_bps": request.max_reference_deviation_bps,
            }.items()
            if value is not None
        }

    async def _serialize_position(self, position: sqlite3.Row) -> dict[str, Any]:
        qty = _to_decimal(position["qty"])
        mark_price = await self._position_mark_price(position)
        unrealized_pnl = self._position_unrealized_pnl_at_mark(position, mark_price)
        notional = _money(qty * mark_price)
        return {
            "symbol": position["symbol"],
            "position_side": position["position_side"],
            "qty": position["qty"],
            "entry_price": position["entry_price"],
            "mark_price": _format_plain_decimal(mark_price),
            "unrealized_pnl": _format_account_money(unrealized_pnl),
            "notional": _format_account_money(notional),
            "margin": position["margin"],
            "leverage": int(position["leverage"]),
            "updated_at": position["updated_at"],
        }

    def _serialize_run(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "run_id": row["run_id"],
            "account_id": row["account_id"],
            "event_type": row["event_type"],
            "session_kind": row["session_kind"],
            "symbol": row["symbol"],
            "status": row["status"],
            "stage": row["stage"],
            "stop_reason": row["stop_reason"],
            "heartbeat_at": row["heartbeat_at"],
            "last_event_at": row["last_event_at"],
            "lock_reason": row["lock_reason"],
            "request": _json_load(row["request_json"], {}),
            "result": _json_load(row["result_json"], {}),
            "rerun_source_run_id": row["rerun_source_run_id"],
            "simulation_schema_version": int(row["simulation_schema_version"]),
            "engine_version": row["engine_version"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _serialize_fill(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "fill_id": int(row["fill_id"]),
            "run_id": row["run_id"],
            "round_index": int(row["round_index"]),
            "symbol": row["symbol"],
            "position_side": row["position_side"],
            "side": row["side"],
            "qty": row["qty"],
            "avg_price": row["avg_price"],
            "notional": row["notional"],
            "fee": row["fee"],
            "liquidity": row["liquidity"],
            "depth_levels_consumed": int(row["depth_levels_consumed"]),
            "slippage_bps": row["slippage_bps"],
            "residual_qty": row["residual_qty"],
            "payload": _json_load(row["payload_json"], {}),
            "created_at": row["created_at"],
        }

    def _serialize_snapshot(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "snapshot_id": int(row["snapshot_id"]),
            "run_id": row["run_id"],
            "snapshot_type": row["snapshot_type"],
            "payload": _json_load(row["payload_json"], {}),
            "created_at": row["created_at"],
        }

    def _serialize_ledger(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "ledger_id": int(row["ledger_id"]),
            "run_id": row["run_id"],
            "event_type": row["event_type"],
            "amount": row["amount"],
            "balance_after": row["balance_after"],
            "payload": _json_load(row["payload_json"], {}),
            "created_at": row["created_at"],
        }

    def _serialize_template(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "template_id": row["template_id"],
            "name": row["name"],
            "payload": _json_load(row["payload_json"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    async def _publish(self, event: str, payload: dict[str, Any]) -> None:
        if self._publisher is None:
            return
        await self._publisher(event, payload)
