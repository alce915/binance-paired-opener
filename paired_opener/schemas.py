from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

from paired_opener.domain import FinalAlignmentStatus, PositionSide, SessionKind, SessionStatus, SingleCloseMode, SingleOpenMode, TrendBias


class OpenSessionRequest(BaseModel):
    symbol: str = Field(..., examples=["BTCUSDT"])
    trend_bias: TrendBias
    leverage: int = Field(..., ge=1, le=125)
    round_count: int = Field(..., ge=1, le=10_000)
    round_qty: Decimal = Field(..., gt=0)
    poll_interval_ms: int | None = Field(default=None, ge=10)
    order_ttl_ms: int | None = Field(default=None, ge=100)
    max_zero_fill_retries: int | None = Field(default=None, ge=1, le=100)
    market_fallback_attempts: int | None = Field(default=None, ge=1, le=20)
    round_interval_seconds: int | None = Field(default=None, ge=0, le=3600)
    created_by: str = "manual"


class CloseSessionRequest(BaseModel):
    symbol: str = Field(..., examples=["BTCUSDT"])
    trend_bias: TrendBias
    close_qty: Decimal = Field(..., gt=0)
    round_count: int = Field(..., ge=1, le=10_000)
    poll_interval_ms: int | None = Field(default=None, ge=10)
    order_ttl_ms: int | None = Field(default=None, ge=100)
    max_zero_fill_retries: int | None = Field(default=None, ge=1, le=100)
    market_fallback_attempts: int | None = Field(default=None, ge=1, le=20)
    round_interval_seconds: int | None = Field(default=None, ge=0, le=3600)
    created_by: str = "manual"


class SingleCloseSessionRequest(BaseModel):
    symbol: str = Field(..., examples=["BTCUSDT"])
    close_mode: SingleCloseMode
    selected_position_side: PositionSide | None = None
    close_qty: Decimal = Field(..., gt=0)
    round_count: int = Field(..., ge=1, le=10_000)
    poll_interval_ms: int | None = Field(default=None, ge=10)
    order_ttl_ms: int | None = Field(default=None, ge=100)
    max_zero_fill_retries: int | None = Field(default=None, ge=1, le=100)
    market_fallback_attempts: int | None = Field(default=None, ge=1, le=20)
    round_interval_seconds: int | None = Field(default=None, ge=0, le=3600)
    created_by: str = "manual"


class SingleOpenSessionRequest(BaseModel):
    symbol: str = Field(..., examples=["BTCUSDT"])
    open_mode: SingleOpenMode
    selected_position_side: PositionSide | None = None
    open_qty: Decimal = Field(..., gt=0)
    leverage: int = Field(..., ge=1, le=125)
    round_count: int = Field(..., ge=1, le=10_000)
    poll_interval_ms: int | None = Field(default=None, ge=10)
    order_ttl_ms: int | None = Field(default=None, ge=100)
    max_zero_fill_retries: int | None = Field(default=None, ge=1, le=100)
    market_fallback_attempts: int | None = Field(default=None, ge=1, le=20)
    round_interval_seconds: int | None = Field(default=None, ge=0, le=3600)
    created_by: str = "manual"

class MarketConnectRequest(BaseModel):
    symbol: str = Field(default="BTCUSDT")


class SimulationRequest(BaseModel):
    symbol: str = Field(default="BTCUSDT")
    trend_bias: TrendBias = TrendBias.LONG
    open_amount: Decimal = Field(..., gt=0)
    leverage: int = Field(..., ge=1, le=125)
    round_count: int = Field(..., ge=1, le=10_000)


class SessionSummary(BaseModel):
    session_id: str
    session_kind: SessionKind = SessionKind.PAIRED_OPEN
    account_id: str = "default"
    account_name: str = "默认账户"
    symbol: str
    trend_bias: TrendBias
    leverage: int
    round_count: int
    round_qty: Decimal
    open_mode: SingleOpenMode | None = None
    close_mode: SingleCloseMode | None = None
    selected_position_side: PositionSide | None = None
    target_open_qty: Decimal = Decimal("0")
    target_close_qty: Decimal = Decimal("0")
    status: SessionStatus
    created_at: datetime
    updated_at: datetime
    last_error: str | None = None


class SessionDetail(BaseModel):
    session_id: str
    session_kind: SessionKind = SessionKind.PAIRED_OPEN
    account_id: str = "default"
    account_name: str = "默认账户"
    symbol: str
    trend_bias: TrendBias
    leverage: int
    round_count: int
    round_qty: Decimal
    open_mode: SingleOpenMode | None = None
    close_mode: SingleCloseMode | None = None
    selected_position_side: PositionSide | None = None
    target_open_qty: Decimal = Decimal("0")
    target_close_qty: Decimal = Decimal("0")
    poll_interval_ms: int
    order_ttl_ms: int
    max_zero_fill_retries: int
    market_fallback_attempts: int
    round_interval_seconds: int | None = None
    status: SessionStatus
    created_at: datetime
    updated_at: datetime
    last_error: str | None = None
    stage2_carryover_qty: Decimal = Decimal("0")
    final_alignment_status: FinalAlignmentStatus = FinalAlignmentStatus.NOT_NEEDED
    final_unaligned_qty: Decimal = Decimal("0")
    completed_with_final_alignment: bool = False
    rounds: list[dict[str, Any]]
    events: list[dict[str, Any]]


class SessionActionResponse(BaseModel):
    session_id: str
    status: SessionStatus
    requested: bool = False
    requested_action: str | None = None
    message: str | None = None


class SymbolInfoResponse(BaseModel):
    symbol: str
    allowed: bool
    max_leverage: int
    current_leverage: int = 1
    min_qty: Decimal
    step_size: Decimal
    tick_size: Decimal
    min_notional: Decimal


class WhitelistUpdateRequest(BaseModel):
    symbols: list[str] = Field(default_factory=list)


class WhitelistResponse(BaseModel):
    symbols: list[str]


class AccountSummary(BaseModel):
    id: str
    name: str
    is_active: bool


class AccountListResponse(BaseModel):
    accounts: list[AccountSummary]


class AccountSelectRequest(BaseModel):
    account_id: str


class AccountSelectResponse(BaseModel):
    account: AccountSummary





