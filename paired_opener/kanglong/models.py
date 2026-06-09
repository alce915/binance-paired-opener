from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from paired_opener.domain import OrderSide, PositionSide


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
    PLAN_CONFIRMED = "plan_confirmed"
    EXECUTION_STARTING = "execution_starting"
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
    BLOCKED_PLAN_STALE = "blocked_plan_stale"
    BLOCKED_PLAN_RECHECK_FAILED = "blocked_plan_recheck_failed"
    BLOCKED_INITIAL_SUBACCOUNT_UNBALANCED = "blocked_initial_subaccount_unbalanced"
    BLOCKED_RUN_LOCK_EXISTS = "blocked_run_lock_exists"
    BLOCKED_GROUP_ROUND_LIMIT_EXCEEDED = "blocked_group_round_limit_exceeded"
    PAUSED_GROUP_NOT_EXECUTABLE = "paused_group_not_executable"
    PAUSED_PLAN_RECHECK_CHANGED = "paused_plan_recheck_changed"
    PAUSED_GROUP_ROUND_LIMIT_EXCEEDED = "paused_group_round_limit_exceeded"
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


def available_actions_for_status(status: str | KanglongRunStatus) -> list[str]:
    normalized = status.value if isinstance(status, KanglongRunStatus) else str(status or "")
    matrix = {
        KanglongRunStatus.DRAFT_PLAN.value: ["refresh_plan"],
        KanglongRunStatus.CHAIN_READY.value: ["confirm", "refresh_plan"],
        KanglongRunStatus.PLAN_CONFIRMED.value: ["execute", "refresh_plan"],
        KanglongRunStatus.EXECUTION_STARTING.value: ["view_report"],
        "running": ["pause", "stop", "view_report"],
        "pause_pending": ["stop", "view_report"],
        "stop_pending": ["view_report"],
        "paused_by_user": ["resume", "stop", "view_report"],
        "paused_market_unstable": ["resume", "stop", "recover", "view_report"],
        "paused_plan_stale": ["refresh_plan", "recover", "view_report"],
        "stopped_by_user": ["view_report", "refresh_plan"],
        KanglongRunStatus.COMPLETED.value: ["view_report"],
        "completed_with_dust_residual": ["view_report"],
        KanglongRunStatus.NEEDS_ABORT_RECOVER.value: ["recover", "view_report"],
        KanglongRunStatus.ABORTED_RECOVERED.value: ["refresh_plan", "view_report"],
        "legacy_readonly": ["refresh_plan", "view_report"],
    }
    if normalized.startswith("blocked_"):
        return ["refresh_plan"]
    return list(matrix.get(normalized, []))


def _position_side(value: PositionSide | str) -> PositionSide:
    if isinstance(value, PositionSide):
        return value
    normalized = str(value or "").strip().upper()
    if normalized == "LONG":
        return PositionSide.LONG
    if normalized == "SHORT":
        return PositionSide.SHORT
    raise ValueError("kanglong_invalid_transfer_setting")


def _transfer_order_sides(direction: PositionSide) -> tuple[OrderSide, OrderSide]:
    if direction == PositionSide.LONG:
        return OrderSide.SELL, OrderSide.BUY
    return OrderSide.BUY, OrderSide.SELL


@dataclass(slots=True)
class TransferExecutionSettings:
    symbol: str
    direction: PositionSide
    mode: str
    order_side: PositionSide
    close_order_side: OrderSide
    open_order_side: OrderSide
    leverage: int
    transfer_percent: Decimal
    round_count: int
    round_interval_seconds: int
    per_round_qty: Decimal

    @classmethod
    def from_input(
        cls,
        *,
        symbol: str,
        direction: PositionSide | str,
        transfer_percent: Decimal | str | int,
        round_count: int | str,
        round_interval_seconds: int | str,
        per_round_qty: Decimal | str | int = Decimal("0"),
        mode: str = "transfer",
        leverage: int | str = 75,
        order_side: PositionSide | str | None = None,
    ) -> TransferExecutionSettings:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_direction = _position_side(direction)
        normalized_order_side = _position_side(order_side) if order_side is not None else normalized_direction
        normalized_leverage = int(leverage)
        normalized_percent = Decimal(str(transfer_percent))
        normalized_round_count = int(round_count)
        normalized_interval = int(round_interval_seconds)
        normalized_per_round_qty = Decimal(str(per_round_qty))
        if (
            not normalized_symbol
            or mode != "transfer"
            or normalized_leverage != 75
            or normalized_order_side != normalized_direction
            or normalized_percent <= Decimal("0")
            or normalized_percent > Decimal("100")
            or normalized_round_count < 1
            or normalized_interval < 0
            or normalized_per_round_qty < Decimal("0")
        ):
            raise ValueError("kanglong_invalid_transfer_setting")
        close_order_side, open_order_side = _transfer_order_sides(normalized_direction)
        return cls(
            symbol=normalized_symbol,
            direction=normalized_direction,
            mode="transfer",
            order_side=normalized_order_side,
            close_order_side=close_order_side,
            open_order_side=open_order_side,
            leverage=75,
            transfer_percent=normalized_percent,
            round_count=normalized_round_count,
            round_interval_seconds=normalized_interval,
            per_round_qty=normalized_per_round_qty,
        )

    def with_per_round_qty(self, per_round_qty: Decimal) -> TransferExecutionSettings:
        return TransferExecutionSettings.from_input(
            symbol=self.symbol,
            direction=self.direction,
            transfer_percent=self.transfer_percent,
            round_count=self.round_count,
            round_interval_seconds=self.round_interval_seconds,
            per_round_qty=per_round_qty,
            mode=self.mode,
            leverage=self.leverage,
            order_side=self.order_side,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "direction": self.direction.value.lower(),
            "mode": self.mode,
            "order_side": self.order_side.value,
            "close_order_side": self.close_order_side.value,
            "open_order_side": self.open_order_side.value,
            "leverage": self.leverage,
            "transfer_percent": decimal_text(self.transfer_percent),
            "round_count": self.round_count,
            "round_interval_seconds": self.round_interval_seconds,
            "per_round_qty": decimal_text(self.per_round_qty),
        }


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
    price_diff_pnl: Decimal = Decimal("0")
    pnl_asset: str = "USDC"
    event_time: datetime = field(default_factory=utc_now)

    def to_payload(self) -> dict[str, Any]:
        payload = {
            name: payload_value(getattr(self, name))
            for name in self.__dataclass_fields__
            if name != "fills"
        }
        payload["fills"] = [item.to_payload() for item in self.fills]
        return payload


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


@dataclass(slots=True)
class KanglongGroupResult:
    group_id: str
    matched_qty: Decimal
    residual_ledger: list[ResidualLedgerEntry]
    events: list[KanglongEvent]
