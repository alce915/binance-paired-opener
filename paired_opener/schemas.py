from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from app_i18n.runtime import CONTRACT_VERSION, DEFAULT_ACCOUNT_NAME
from paired_opener.config import DEFAULT_LEVERAGE, DEFAULT_ROUND_COUNT, DEFAULT_TRADING_SYMBOL
from paired_opener.domain import ExecutionProfile, FinalAlignmentStatus, PositionSide, RecoveryStatus, SessionKind, SessionStatus, SessionStopReason, SingleCloseMode, SingleOpenMode, TrendBias


class ExecutionPolicyFields(BaseModel):
    execution_profile: ExecutionProfile | None = None
    market_fallback_max_ratio: Decimal | None = Field(default=None, ge=0)
    market_fallback_min_residual_qty: Decimal | None = Field(default=None, ge=0)
    max_reprice_ticks: int | None = Field(default=None, ge=0, le=10_000)
    max_spread_bps: int | None = Field(default=None, ge=0, le=10_000)
    max_reference_deviation_bps: int | None = Field(default=None, ge=0, le=10_000)


class OpenSessionRequest(ExecutionPolicyFields):
    symbol: str = Field(..., examples=["BTCUSDC"])
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
    symbol: str = Field(..., examples=["BTCUSDC"])
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
    symbol: str = Field(..., examples=["BTCUSDC"])
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
    symbol: str = Field(..., examples=["BTCUSDC"])
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
    symbol: str = Field(..., examples=["BTCUSDC"])
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
    symbol: str = Field(default=DEFAULT_TRADING_SYMBOL)


class SimulationRunRequest(ExecutionPolicyFields):
    session_kind: SessionKind = SessionKind.PAIRED_OPEN
    symbol: str = Field(default=DEFAULT_TRADING_SYMBOL)
    trend_bias: TrendBias | None = None
    open_mode: SingleOpenMode | None = None
    close_mode: SingleCloseMode | None = None
    selected_position_side: PositionSide | None = None
    open_amount: Decimal | None = Field(default=None, gt=0)
    open_qty: Decimal | None = Field(default=None, gt=0)
    close_qty: Decimal | None = Field(default=None, gt=0)
    leverage: int | None = Field(default=DEFAULT_LEVERAGE, ge=1, le=125)
    round_count: int = Field(default=DEFAULT_ROUND_COUNT, ge=1, le=10_000)
    round_interval_seconds: int | None = Field(default=3, ge=0, le=3600)


SimulationRequest = SimulationRunRequest


class AccountCredentialSummary(BaseModel):
    account_id: str = Field(..., min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    name: str = Field(..., min_length=1, max_length=100)
    api_key_masked: str
    has_api_secret: bool
    account_mode: Literal["portfolio_margin"] = "portfolio_margin"
    enabled: bool = True
    order: int = Field(..., ge=0)


class AccountCredentialCreateRequest(BaseModel):
    account_id: str = Field(..., min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    name: str = Field(..., min_length=1, max_length=100)
    api_key: str = Field(..., min_length=8, max_length=256)
    api_secret: str = Field(..., min_length=8, max_length=256)
    credential_type: Literal["hmac"] = "hmac"
    account_mode: Literal["portfolio_margin"] = "portfolio_margin"
    enabled: bool = True


class AccountCredentialUpdateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    api_key: str | None = Field(default=None, min_length=8, max_length=256)
    api_secret: str | None = Field(default=None, min_length=8, max_length=256)
    enabled: bool | None = None

    @model_validator(mode="after")
    def require_at_least_one_change(self) -> "AccountCredentialUpdateRequest":
        if not self.model_fields_set:
            raise ValueError("at least one field must be supplied")
        return self


class AccountCredentialImportPreviewRequest(BaseModel):
    accounts: list[AccountCredentialCreateRequest] = Field(..., min_length=1, max_length=100)
    mode: Literal["merge", "replace"] = "merge"

    @model_validator(mode="after")
    def require_unique_account_ids(self) -> "AccountCredentialImportPreviewRequest":
        account_ids = [account.account_id for account in self.accounts]
        if len(account_ids) != len(set(account_ids)):
            raise ValueError("duplicate account_id")
        return self


class AccountCredentialImportCommitRequest(BaseModel):
    preview_token: str = Field(..., min_length=32, max_length=128)


class AccountCredentialImportChanges(BaseModel):
    added_account_ids: list[str]
    updated_account_ids: list[str]
    unchanged_account_ids: list[str]
    removed_account_ids: list[str]


class AccountCredentialImportPreviewResponse(BaseModel):
    preview_token: str
    credential_revision: str
    expires_at: datetime
    final_accounts: list[AccountCredentialSummary]
    changes: AccountCredentialImportChanges


class AccountCredentialOrderRequest(BaseModel):
    account_ids: list[str] = Field(..., min_length=1, max_length=100)

    @model_validator(mode="after")
    def require_unique_account_ids(self) -> "AccountCredentialOrderRequest":
        if len(self.account_ids) != len(set(self.account_ids)):
            raise ValueError("duplicate account_id")
        return self


class KanglongBatchPlanRequest(BaseModel):
    operation: Literal["open", "close"]
    symbol: str = Field(..., min_length=1, max_length=32)
    preferred_side: PositionSide
    leverage: int = Field(default=100, ge=1, le=125)
    per_leg_notional: Decimal = Field(default=Decimal("250000"), gt=0)
    account_ids: list[str] = Field(..., min_length=1, max_length=100)
    source_open_run_id: str | None = None
    round_count: int = Field(default=30, ge=1, le=500)
    round_interval_seconds: int = Field(default=3, ge=0, le=3600)

    @model_validator(mode="after")
    def validate_batch_request(self) -> "KanglongBatchPlanRequest":
        if self.operation == "close" and not self.source_open_run_id:
            raise ValueError("source_open_run_id is required for close")
        if len(self.account_ids) != len(set(self.account_ids)):
            raise ValueError("duplicate account_id")
        return self


class KanglongBatchCapacityPreviewRequest(BaseModel):
    operation: Literal["open"] = "open"
    symbol: str = Field(..., min_length=1, max_length=32)
    preferred_side: PositionSide
    leverage: int = Field(default=100, ge=1, le=125)
    per_leg_notional: Decimal = Field(default=Decimal("250000"), gt=0)
    account_ids: list[str] = Field(..., min_length=1, max_length=100)
    round_count: int = Field(default=30, ge=1, le=500)
    round_interval_seconds: int = Field(default=3, ge=0, le=3600)
    request_seq: int = Field(..., ge=0)
    input_hash: str = Field(..., min_length=8, max_length=128)

    @model_validator(mode="after")
    def require_unique_account_ids(self) -> "KanglongBatchCapacityPreviewRequest":
        if len(self.account_ids) != len(set(self.account_ids)):
            raise ValueError("duplicate account_id")
        return self


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


class KanglongPlanRequest(BaseModel):
    mode: str = Field(default="simulation", pattern="^simulation$")
    symbol: str = Field(default=DEFAULT_TRADING_SYMBOL)
    main_account_id: str
    subaccount_ids: list[str] = Field(..., min_length=1)
    selected_side: PositionSide | None = None
    transfer_mode: str = Field(default="transfer", pattern="^transfer$")
    leverage: int = Field(default=75, ge=75, le=75)
    order_side: PositionSide | None = None
    transfer_percent: Decimal = Field(default=Decimal("100"), gt=0, le=100)
    round_count: int = Field(default=30, ge=1, le=10_000)
    round_interval_seconds: int = Field(default=3, ge=0, le=3600)
    account_source: str = Field(default="runtime", pattern="^(runtime|test_template)$")
    test_template_id: str | None = None
    template_content_hash: str | None = None
    market_data_account_id: str | None = None


class KanglongTemplatePreviewRequest(BaseModel):
    market_data_account_id: str = Field(..., min_length=1)


class KanglongTemplateMutationResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    template: dict[str, Any]


class KanglongTemplateListResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    version: int
    templates: list[dict[str, Any]]
    recoverable_backup: bool = False


class KanglongTemplateDeleteResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    status: str
    template_id: str


class KanglongActionRequest(BaseModel):
    plan_version: str
    plan_input_hash: str | None = None
    confirmed_plan_hash: str | None = None
    idempotency_key: str = Field(..., min_length=8, max_length=128)
    operator: str = Field(default="manual")
    confirmed_warning_codes: list[str] = Field(default_factory=list)


class KanglongControlRequest(BaseModel):
    plan_version: str
    expected_action_version: int = Field(..., ge=0)
    idempotency_key: str = Field(..., min_length=8, max_length=128)
    operator: str = Field(default="manual")


class KanglongRecoverRequest(BaseModel):
    idempotency_key: str = Field(..., min_length=8, max_length=128)
    operator: str = Field(default="manual")
    release_reason: str = Field(..., min_length=3, max_length=500)


class KanglongBatchRecoverRequest(BaseModel):
    plan_version: str
    expected_action_version: int = Field(..., ge=0)
    idempotency_key: str = Field(..., min_length=8, max_length=128)
    operator: str = Field(default="manual")
    release_reason: str = Field(..., min_length=3, max_length=500)


class KanglongBatchRunResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    run_id: str
    status: str
    plan_version: str
    action_version: int = Field(default=0, ge=0)
    available_actions: list[str] = Field(default_factory=list)
    report: dict[str, Any] = Field(default_factory=dict)
    plan: dict[str, Any] = Field(default_factory=dict)
    accounts: list[dict[str, Any]] = Field(default_factory=list)
    latest_event_id: int = 0


class KanglongPlanResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    run_id: str
    status: str
    plan_version: str
    plan_input_hash: str | None = None
    confirmed_plan_hash: str | None = None
    transfer_settings: dict[str, Any] | None = None
    snapshot_bundle_id: str
    result_grade: str | None = None
    error_code: str | None = None
    requested_plan_version: str | None = None
    current_status: str | None = None
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
    run_id: str | None = None
    active: bool | None = None
    status: str
    stage: str | None = None
    requested: bool = False
    requested_action: str | None = None
    heartbeat_at: str | None = None
    last_event_at: str | None = None
    latest_event_id: int | None = None
    rounds_completed: int | None = None
    rounds_total: int | None = None
    lock_reason: str | None = None
    message_code: str | None = None
    message_params: dict[str, Any] = Field(default_factory=dict)
    message: str | None = None


class SimulationAccountSettingsRequest(BaseModel):
    initial_balance: Decimal | None = Field(default=None, gt=0)
    maker_fee_rate: Decimal | None = Field(default=None, ge=0)
    taker_fee_rate: Decimal | None = Field(default=None, ge=0)


class SimulationTemplateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    payload: dict[str, Any] = Field(default_factory=dict)


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
    positions: list[dict[str, Any]] = Field(default_factory=list)
    totals: dict[str, Any] = Field(default_factory=dict)
    snapshot_version: str | None = None
    risk_unknown: bool = False
    status: str | None = None
    error: str | None = None


class AccountListResponse(BaseModel):
    accounts: list[AccountSummary]


class AccountSelectRequest(BaseModel):
    account_id: str


class AccountSelectResponse(BaseModel):
    account: AccountSummary



