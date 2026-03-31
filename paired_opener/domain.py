from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import uuid4

from paired_opener.errors import ExchangeStateError


def utc_now() -> datetime:
    return datetime.now(UTC)


class TrendBias(StrEnum):
    LONG = "long"
    SHORT = "short"


class SingleCloseMode(StrEnum):
    REGULAR = "regular"
    ALIGN = "align"


class SingleOpenMode(StrEnum):
    REGULAR = "regular"
    ALIGN = "align"


class SessionKind(StrEnum):
    PAIRED_OPEN = "paired_open"
    PAIRED_CLOSE = "paired_close"
    SINGLE_OPEN = "single_open"
    SINGLE_CLOSE = "single_close"


class SessionStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    COMPLETED_WITH_SKIPS = "completed_with_skips"
    ABORTED = "aborted"
    EXCEPTION = "exception"


class RecoveryStatus(StrEnum):
    RECOVERABLE = "recoverable"
    NON_RECOVERABLE = "non_recoverable"
    MANUAL_CONFIRMATION = "manual_confirmation"


class RoundStatus(StrEnum):
    STAGE1_PENDING = "stage1_pending"
    STAGE1_RETRYING = "stage1_retrying"
    STAGE1_FILLED = "stage1_filled"
    STAGE1_PARTIAL = "stage1_partial"
    STAGE1_SKIPPED = "stage1_skipped"
    STAGE2_PENDING = "stage2_pending"
    STAGE2_RETRYING = "stage2_retrying"
    STAGE2_MARKET_FALLBACK = "stage2_market_fallback"
    ROUND_COMPLETED = "round_completed"


class FinalAlignmentStatus(StrEnum):
    NOT_NEEDED = "not_needed"
    CARRYOVER_PENDING = "carryover_pending"
    MARKET_ALIGNED = "market_aligned"
    FLATTENED_BOTH_SIDES = "flattened_both_sides"
    FAILED = "failed"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class PositionSide(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class ExchangeOrderStatus(StrEnum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


@dataclass(slots=True)
class SymbolRules:
    symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal
    max_leverage: int


@dataclass(slots=True)
class Quote:
    symbol: str
    bid_price: Decimal
    ask_price: Decimal
    event_time: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class ExchangeOrder:
    symbol: str
    order_id: str
    client_order_id: str
    side: OrderSide
    position_side: PositionSide
    type: str
    price: Decimal
    orig_qty: Decimal
    executed_qty: Decimal
    status: ExchangeOrderStatus
    update_time: datetime = field(default_factory=utc_now)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SessionSpec:
    symbol: str
    trend_bias: TrendBias
    leverage: int
    round_count: int
    round_qty: Decimal
    poll_interval_ms: int
    order_ttl_ms: int
    max_zero_fill_retries: int
    market_fallback_attempts: int
    round_interval_seconds: int = 3
    created_by: str = "system"
    session_kind: SessionKind = SessionKind.PAIRED_OPEN
    open_mode: SingleOpenMode | None = None
    close_mode: SingleCloseMode | None = None
    selected_position_side: PositionSide | None = None
    target_open_qty: Decimal = Decimal("0")
    target_close_qty: Decimal = Decimal("0")


@dataclass(slots=True)
class OpenSession:
    session_id: str
    spec: SessionSpec
    account_id: str = "default"
    account_name: str = "默认账户"
    status: SessionStatus = SessionStatus.PENDING
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    last_error: str | None = None
    last_error_category: str | None = None
    last_error_strategy: str | None = None
    last_error_code: str | None = None
    last_error_operator_action: str | None = None
    recovery_status: RecoveryStatus | None = None
    recovery_summary: str | None = None
    recovery_checked_at: datetime | None = None
    recovery_details: dict[str, Any] = field(default_factory=dict)
    stage2_carryover_qty: Decimal = Decimal("0")
    final_alignment_status: FinalAlignmentStatus = FinalAlignmentStatus.NOT_NEEDED
    final_unaligned_qty: Decimal = Decimal("0")
    completed_with_final_alignment: bool = False

    @staticmethod
    def create(spec: SessionSpec, *, account_id: str = "default", account_name: str = "默认账户") -> "OpenSession":
        return OpenSession(session_id=str(uuid4()), spec=spec, account_id=account_id, account_name=account_name)


@dataclass(slots=True)
class RoundExecution:
    session_id: str
    round_index: int
    status: RoundStatus
    stage1_filled_qty: Decimal = Decimal("0")
    stage2_filled_qty: Decimal = Decimal("0")
    stage1_zero_fill_retries: int = 0
    stage2_zero_fill_retries: int = 0
    market_fallback_used: bool = False
    notes: dict[str, Any] = field(default_factory=dict)
    started_at: datetime = field(default_factory=utc_now)
    ended_at: datetime | None = None


@dataclass(slots=True)
class PollObservation:
    order: ExchangeOrder
    filled_qty: Decimal
    had_fill: bool
    terminal: bool


class SessionConflictError(RuntimeError):
    pass


class SessionAbortedError(RuntimeError):
    pass







