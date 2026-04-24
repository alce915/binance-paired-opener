from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

from app_i18n.runtime import CONTRACT_VERSION, DEFAULT_ACCOUNT_NAME
from paired_opener.domain import ExecutionProfile, FinalAlignmentStatus, PositionSide, RecoveryStatus, SessionKind, SessionStatus, SessionStopReason, SingleCloseMode, SingleOpenMode, TrendBias


class ExecutionPolicyFields(BaseModel):
    execution_profile: ExecutionProfile | None = None
    market_fallback_max_ratio: Decimal | None = Field(default=None, ge=0)
    market_fallback_min_residual_qty: Decimal | None = Field(default=None, ge=0)
    max_reprice_ticks: int | None = Field(default=None, ge=0, le=10_000)
    max_spread_bps: int | None = Field(default=None, ge=0, le=10_000)
    max_reference_deviation_bps: int | None = Field(default=None, ge=0, le=10_000)


class OpenSessionRequest(ExecutionPolicyFields):
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


class CloseSessionRequest(ExecutionPolicyFields):
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


class SingleCloseSessionRequest(ExecutionPolicyFields):
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


class SingleOpenSessionRequest(ExecutionPolicyFields):
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


class SessionPrecheckRequest(ExecutionPolicyFields):
    session_kind: SessionKind
    symbol: str = Field(..., examples=["BTCUSDT"])
    trend_bias: TrendBias | None = None
    leverage: int | None = Field(default=None, ge=1, le=125)
    round_count: int = Field(default=1, ge=1, le=10_000)
    round_qty: Decimal | None = Field(default=None, gt=0)
    close_qty: Decimal | None = Field(default=None, gt=0)
    open_qty: Decimal | None = Field(default=None, gt=0)
    selected_position_side: PositionSide | None = None
    open_mode: SingleOpenMode | None = None
    close_mode: SingleCloseMode | None = None
    poll_interval_ms: int | None = Field(default=None, ge=10)
    order_ttl_ms: int | None = Field(default=None, ge=100)
    max_zero_fill_retries: int | None = Field(default=None, ge=1, le=100)
    market_fallback_attempts: int | None = Field(default=None, ge=1, le=20)
    round_interval_seconds: int | None = Field(default=None, ge=0, le=3600)


class MarketConnectRequest(BaseModel):
    symbol: str = Field(default="BTCUSDT")


class SimulationRunRequest(ExecutionPolicyFields):
    session_kind: SessionKind = SessionKind.PAIRED_OPEN
    symbol: str = Field(default="BTCUSDT")
    trend_bias: TrendBias | None = None
    open_mode: SingleOpenMode | None = None
    close_mode: SingleCloseMode | None = None
    selected_position_side: PositionSide | None = None
    open_amount: Decimal | None = Field(default=None, gt=0)
    open_qty: Decimal | None = Field(default=None, gt=0)
    close_qty: Decimal | None = Field(default=None, gt=0)
    leverage: int | None = Field(default=None, ge=1, le=125)
    round_count: int = Field(..., ge=1, le=10_000)
    round_interval_seconds: int | None = Field(default=3, ge=0, le=3600)


SimulationRequest = SimulationRunRequest

class PrecheckItem(BaseModel):
    contract_version: str = CONTRACT_VERSION
    code: str
    label_key: str | None = None
    label: str
    status: str
    message_key: str | None = None
    message_params: dict[str, Any] = Field(default_factory=dict)
    message: str
    details: dict[str, Any] | None = None


class SessionPrecheckResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    ok: bool
    summary_code: str | None = None
    summary_params: dict[str, Any] = Field(default_factory=dict)
    summary: str
    checks: list[PrecheckItem]
    derived: dict[str, Any] = Field(default_factory=dict)


class SessionSummary(BaseModel):
    session_id: str
    session_kind: SessionKind = SessionKind.PAIRED_OPEN
    account_id: str = "default"
    account_name: str = DEFAULT_ACCOUNT_NAME
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
    planned_round_qtys: list[Decimal] = Field(default_factory=list)
    final_round_qty: Decimal = Decimal("0")
    extension_round_cap_qty: Decimal = Decimal("0")
    max_extension_rounds: int = 5
    max_session_duration_seconds: int = 1800
    execution_profile: ExecutionProfile = ExecutionProfile.BALANCED
    market_fallback_max_ratio: Decimal = Decimal("1")
    market_fallback_min_residual_qty: Decimal = Decimal("0")
    max_reprice_ticks: int | None = 8
    max_spread_bps: int | None = 20
    max_reference_deviation_bps: int | None = 40
    status: SessionStatus
    created_at: datetime
    updated_at: datetime
    last_error: str | None = None
    last_error_category: str | None = None
    last_error_strategy: str | None = None
    last_error_code: str | None = None
    last_error_operator_action: str | None = None
    last_error_params: dict[str, Any] = Field(default_factory=dict)
    last_error_contract_version: str | None = None
    recovery_status: RecoveryStatus | None = None
    recovery_summary: str | None = None
    recovery_checked_at: datetime | None = None
    stage2_carryover_qty: Decimal = Decimal("0")
    final_alignment_status: FinalAlignmentStatus = FinalAlignmentStatus.NOT_NEEDED
    final_unaligned_qty: Decimal = Decimal("0")
    session_deadline_at: datetime | None = None
    extension_rounds_used: int = 0
    remaining_extension_rounds: int = 0
    stop_reason: SessionStopReason | None = None
    residual_source: str | None = None


class SessionDetail(BaseModel):
    session_id: str
    session_kind: SessionKind = SessionKind.PAIRED_OPEN
    account_id: str = "default"
    account_name: str = DEFAULT_ACCOUNT_NAME
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
    planned_round_qtys: list[Decimal] = Field(default_factory=list)
    final_round_qty: Decimal = Decimal("0")
    extension_round_cap_qty: Decimal = Decimal("0")
    max_extension_rounds: int = 5
    max_session_duration_seconds: int = 1800
    poll_interval_ms: int
    order_ttl_ms: int
    max_zero_fill_retries: int
    market_fallback_attempts: int
    execution_profile: ExecutionProfile = ExecutionProfile.BALANCED
    market_fallback_max_ratio: Decimal = Decimal("1")
    market_fallback_min_residual_qty: Decimal = Decimal("0")
    max_reprice_ticks: int | None = 8
    max_spread_bps: int | None = 20
    max_reference_deviation_bps: int | None = 40
    round_interval_seconds: int | None = None
    status: SessionStatus
    created_at: datetime
    updated_at: datetime
    last_error: str | None = None
    last_error_category: str | None = None
    last_error_strategy: str | None = None
    last_error_code: str | None = None
    last_error_operator_action: str | None = None
    last_error_params: dict[str, Any] = Field(default_factory=dict)
    last_error_contract_version: str | None = None
    recovery_status: RecoveryStatus | None = None
    recovery_summary: str | None = None
    recovery_checked_at: datetime | None = None
    stage2_carryover_qty: Decimal = Decimal("0")
    final_alignment_status: FinalAlignmentStatus = FinalAlignmentStatus.NOT_NEEDED
    final_unaligned_qty: Decimal = Decimal("0")
    completed_with_final_alignment: bool = False
    session_deadline_at: datetime | None = None
    extension_rounds_used: int = 0
    remaining_extension_rounds: int = 0
    stop_reason: SessionStopReason | None = None
    residual_source: str | None = None
    rounds: list[dict[str, Any]]
    events: list[dict[str, Any]]


class SessionUpdatesResponse(BaseModel):
    session: SessionSummary
    changed_rounds: list[dict[str, Any]] = Field(default_factory=list)
    events: list[dict[str, Any]] = Field(default_factory=list)
    latest_event_id: int = 0


class SessionActionResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    session_id: str
    status: SessionStatus
    requested: bool = False
    requested_action: str | None = None
    message_code: str | None = None
    message_params: dict[str, Any] = Field(default_factory=dict)
    message: str | None = None


class SimulationActionResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    status: str
    requested: bool = False
    requested_action: str | None = None
    message_code: str | None = None
    message_params: dict[str, Any] = Field(default_factory=dict)
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



