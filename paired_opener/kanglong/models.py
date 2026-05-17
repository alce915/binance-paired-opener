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
