from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from app_i18n.runtime import CONTRACT_VERSION, DEFAULT_ACCOUNT_NAME, format_copy, make_precheck_item
from paired_opener.config import Settings
from paired_opener.domain import (
    ExecutionProfile,
    ExchangeStateError,
    FinalAlignmentStatus,
    OpenSession,
    PositionSide,
    RecoveryStatus,
    RoundStatus,
    SessionConflictError,
    SessionKind,
    SessionSpec,
    SessionStatus,
    SessionStopReason,
    SingleCloseMode,
    SingleOpenMode,
    TrendBias,
)
from paired_opener.engine import (
    PairedClosingEngine,
    PairedOpeningEngine,
    SessionControl,
    SingleClosingEngine,
    SingleOpeningEngine,
)
from paired_opener.execution_policy import ResolvedExecutionPolicy, resolve_execution_policy
from paired_opener.errors import ErrorStrategy, TradingError, ensure_trading_error
from paired_opener.exchange import ExchangeGateway
from paired_opener.rounding import normalize_price, normalize_qty, validate_qty_and_notional
from paired_opener.schemas import (
    CloseSessionRequest,
    OpenSessionRequest,
    SingleCloseSessionRequest,
    SingleOpenSessionRequest,
    SessionPrecheckRequest,
)
from paired_opener.storage import SqliteRepository


@dataclass(slots=True)
class ManagedSession:
    symbol: str
    control: SessionControl
    task: asyncio.Task[None]



SYSTEM_ORDER_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-",
    re.IGNORECASE,
)

DEFAULT_SINGLE_MAX_EXTENSION_ROUNDS = 5
DEFAULT_SINGLE_MAX_SESSION_DURATION_SECONDS = 30 * 60


class SessionPrecheckFailed(ValueError):
    def __init__(self, precheck: dict[str, Any]) -> None:
        self.precheck = precheck
        summary = str(precheck.get("summary") or "")
        if not summary:
            summary = format_copy(str(precheck.get("summary_code") or "reasons.session_precheck_failed"), precheck.get("summary_params") or {})
        super().__init__(summary or "预检失败")

class OpenSessionService:
    def __init__(
        self,
        settings: Settings,
        repository: SqliteRepository,
        gateway: ExchangeGateway,
        engine: PairedOpeningEngine,
        close_engine: PairedClosingEngine | None = None,
        single_open_engine: SingleOpeningEngine | None = None,
        single_close_engine: SingleClosingEngine | None = None,
        account_id: str = "default",
        account_name: str = DEFAULT_ACCOUNT_NAME,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._gateway = gateway
        self._engine = engine
        self._close_engine = close_engine or PairedClosingEngine(gateway, repository)
        self._single_open_engine = single_open_engine or SingleOpeningEngine(gateway, repository)
        self._single_close_engine = single_close_engine or SingleClosingEngine(gateway, repository)
        self._account_id = account_id
        self._account_name = account_name
        self._managed: dict[str, ManagedSession] = {}
        self._session_creation_lock = asyncio.Lock()
        self._hedge_mode_fallback_confirmed = False


    async def close(self, *, timeout_seconds: float = 5.0) -> None:
        managed_items = list(self._managed.items())
        if not managed_items:
            return
        for _, managed in managed_items:
            managed.control.paused = False
            managed.control.aborted = True
        tasks = [managed.task for _, managed in managed_items if not managed.task.done()]
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=timeout_seconds)
            except asyncio.TimeoutError:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        self._managed.clear()
    def _parse_datetime(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if value:
            return datetime.fromisoformat(str(value))
        return datetime.now(UTC)

    def _enum_value(self, value: Any, enum_type: Any, default: Any = None):
        if value is None:
            return default
        if isinstance(value, enum_type):
            return value
        return enum_type(str(value))

    def _build_session_from_payload(self, payload: dict[str, Any]) -> OpenSession:
        spec = SessionSpec(
            symbol=str(payload["symbol"]),
            trend_bias=self._enum_value(payload.get("trend_bias"), TrendBias, TrendBias.LONG),
            leverage=int(payload.get("leverage") or 1),
            round_count=int(payload.get("round_count") or 1),
            round_qty=Decimal(str(payload.get("round_qty") or "0")),
            poll_interval_ms=int(payload.get("poll_interval_ms") or self._settings.default_poll_interval_ms),
            order_ttl_ms=int(payload.get("order_ttl_ms") or self._settings.default_order_ttl_ms),
            max_zero_fill_retries=int(payload.get("max_zero_fill_retries") or self._settings.default_max_zero_fill_retries),
            market_fallback_attempts=int(payload.get("market_fallback_attempts") or self._settings.default_market_fallback_attempts),
            execution_profile=self._enum_value(payload.get("execution_profile"), ExecutionProfile, ExecutionProfile.BALANCED),
            market_fallback_max_ratio=Decimal(str(payload.get("market_fallback_max_ratio")))
            if payload.get("market_fallback_max_ratio") is not None
            else Decimal("1"),
            market_fallback_min_residual_qty=Decimal(str(payload.get("market_fallback_min_residual_qty")))
            if payload.get("market_fallback_min_residual_qty") is not None
            else Decimal("0"),
            max_reprice_ticks=int(payload.get("max_reprice_ticks")) if payload.get("max_reprice_ticks") is not None else None,
            max_spread_bps=int(payload.get("max_spread_bps")) if payload.get("max_spread_bps") is not None else None,
            max_reference_deviation_bps=int(payload.get("max_reference_deviation_bps"))
            if payload.get("max_reference_deviation_bps") is not None
            else None,
            round_interval_seconds=int(payload.get("round_interval_seconds")) if payload.get("round_interval_seconds") is not None else 3,
            created_by=str(payload.get("created_by") or "manual"),
            session_kind=self._enum_value(payload.get("session_kind"), SessionKind, SessionKind.PAIRED_OPEN),
            open_mode=self._enum_value(payload.get("open_mode"), SingleOpenMode),
            close_mode=self._enum_value(payload.get("close_mode"), SingleCloseMode),
            selected_position_side=self._enum_value(payload.get("selected_position_side"), PositionSide),
            target_open_qty=Decimal(str(payload.get("target_open_qty") or "0")),
            target_close_qty=Decimal(str(payload.get("target_close_qty") or "0")),
            planned_round_qtys=[Decimal(str(item)) for item in payload.get("planned_round_qtys") or []],
            final_round_qty=Decimal(str(payload.get("final_round_qty") or "0")),
            extension_round_cap_qty=Decimal(str(payload.get("extension_round_cap_qty") or "0")),
            max_extension_rounds=int(payload.get("max_extension_rounds") or DEFAULT_SINGLE_MAX_EXTENSION_ROUNDS),
            max_session_duration_seconds=int(payload.get("max_session_duration_seconds") or DEFAULT_SINGLE_MAX_SESSION_DURATION_SECONDS),
        )
        return OpenSession(
            session_id=str(payload["session_id"]),
            spec=spec,
            account_id=str(payload.get("account_id") or self._account_id),
            account_name=str(payload.get("account_name") or self._account_name),
            status=self._enum_value(payload.get("status"), SessionStatus, SessionStatus.PENDING),
            created_at=self._parse_datetime(payload.get("created_at")),
            updated_at=self._parse_datetime(payload.get("updated_at")),
            last_error=payload.get("last_error"),
            last_error_category=payload.get("last_error_category"),
            last_error_strategy=payload.get("last_error_strategy"),
            last_error_code=payload.get("last_error_code"),
            last_error_operator_action=payload.get("last_error_operator_action"),
            last_error_params=dict(payload.get("last_error_params") or {}),
            last_error_raw_message=payload.get("last_error_raw_message"),
            last_error_contract_version=payload.get("last_error_contract_version"),
            recovery_status=self._enum_value(payload.get("recovery_status"), RecoveryStatus),
            recovery_summary=payload.get("recovery_summary"),
            recovery_checked_at=self._parse_datetime(payload.get("recovery_checked_at")) if payload.get("recovery_checked_at") else None,
            recovery_details=dict(payload.get("recovery_details") or {}),
            stage2_carryover_qty=Decimal(str(payload.get("stage2_carryover_qty") or "0")),
            final_alignment_status=self._enum_value(payload.get("final_alignment_status"), FinalAlignmentStatus, FinalAlignmentStatus.NOT_NEEDED),
            final_unaligned_qty=Decimal(str(payload.get("final_unaligned_qty") or "0")),
            completed_with_final_alignment=bool(payload.get("completed_with_final_alignment")),
            session_deadline_at=self._parse_datetime(payload.get("session_deadline_at")) if payload.get("session_deadline_at") else None,
            extension_rounds_used=int(payload.get("extension_rounds_used") or 0),
            remaining_extension_rounds=int(payload.get("remaining_extension_rounds") or 0),
            stop_reason=self._enum_value(payload.get("stop_reason"), SessionStopReason),
            residual_source=payload.get("residual_source"),
        )

    def _extract_session_order_ids(self, payload: dict[str, Any]) -> list[str]:
        order_ids: set[str] = set()
        for event in payload.get("events", []):
            event_payload = event.get("payload") or {}
            order_id = event_payload.get("order_id")
            if order_id:
                order_ids.add(str(order_id))
        return sorted(order_ids)

    def _recovery_round_progress(self, payload: dict[str, Any]) -> dict[str, Any]:
        completed_rounds = 0
        skipped_rounds = 0
        regular_completed_rounds = 0
        regular_skipped_rounds = 0
        extension_rounds_used = 0
        stage1_filled_total = Decimal("0")
        stage2_filled_total = Decimal("0")
        non_terminal_rounds: list[dict[str, Any]] = []
        next_round_index = 1
        regular_round_limit = int(payload.get("round_count") or 0)
        for round_payload in payload.get("rounds", []):
            round_index = int(round_payload.get("round_index") or 0)
            next_round_index = max(next_round_index, round_index + 1)
            status_value = str(round_payload.get("status") or "")
            notes = round_payload.get("notes") or {}
            is_extension_round = bool(notes.get("is_extension_round")) or round_index > regular_round_limit
            stage1_filled_total += Decimal(str(round_payload.get("stage1_filled_qty") or "0"))
            stage2_filled_total += Decimal(str(round_payload.get("stage2_filled_qty") or "0"))
            if status_value == RoundStatus.ROUND_COMPLETED.value:
                completed_rounds += 1
                if not is_extension_round:
                    regular_completed_rounds += 1
            elif status_value == RoundStatus.STAGE1_SKIPPED.value:
                skipped_rounds += 1
                if not is_extension_round:
                    regular_skipped_rounds += 1
            elif status_value:
                non_terminal_rounds.append({"round_index": round_index, "status": status_value})
            if is_extension_round:
                extension_rounds_used += 1
        return {
            "completed_rounds": completed_rounds,
            "skipped_rounds": skipped_rounds,
            "regular_completed_rounds": regular_completed_rounds,
            "regular_skipped_rounds": regular_skipped_rounds,
            "extension_rounds_used": extension_rounds_used,
            "stage1_filled_total": stage1_filled_total,
            "stage2_filled_total": stage2_filled_total,
            "next_round_index": next_round_index,
            "non_terminal_rounds": non_terminal_rounds,
        }

    def _recovery_result(
        self,
        recovery_status: RecoveryStatus,
        summary: str,
        checked_at: datetime,
        details: dict[str, Any],
        *,
        final_status: SessionStatus | None = None,
    ) -> dict[str, Any]:
        return {
            "recovery_status": recovery_status,
            "recovery_summary": summary,
            "recovery_checked_at": checked_at,
            "recovery_details": details,
            "final_status": final_status,
        }

    def _completed_status_for_recovery(self, session: OpenSession, progress: dict[str, Any]) -> SessionStatus:
        return (
            SessionStatus.COMPLETED_WITH_SKIPS
            if progress["skipped_rounds"] > 0 or session.final_unaligned_qty > Decimal("0")
            else SessionStatus.COMPLETED
        )

    def _completed_status_with_remaining_qty(
        self,
        session: OpenSession,
        progress: dict[str, Any],
        remaining_qty: Decimal,
    ) -> SessionStatus:
        return (
            SessionStatus.COMPLETED_WITH_SKIPS
            if remaining_qty > Decimal("0") or progress["skipped_rounds"] > 0 or session.final_unaligned_qty > Decimal("0")
            else SessionStatus.COMPLETED
        )

    async def _evaluate_recovery_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        session = self._build_session_from_payload(payload)
        checked_at = datetime.now(UTC)
        progress = self._recovery_round_progress(payload)
        stored_extension_rounds_used = session.extension_rounds_used
        stored_remaining_extension_rounds = session.remaining_extension_rounds
        if session.spec.session_kind in (SessionKind.SINGLE_OPEN, SessionKind.SINGLE_CLOSE):
            if session.stop_reason == SessionStopReason.MAX_EXTENSION_ROUNDS_REACHED:
                session.extension_rounds_used = max(int(progress["extension_rounds_used"]), stored_extension_rounds_used)
                session.remaining_extension_rounds = 0
            else:
                session.extension_rounds_used = int(progress["extension_rounds_used"])
                session.remaining_extension_rounds = max(
                    int(session.spec.max_extension_rounds or 0) - session.extension_rounds_used,
                    0,
                )
            if (
                session.extension_rounds_used != stored_extension_rounds_used
                or session.remaining_extension_rounds != stored_remaining_extension_rounds
            ):
                self._repository.update_session_runtime(session)
        details: dict[str, Any] = {
            "session_kind": session.spec.session_kind.value,
            "account_id": session.account_id,
            "account_name": session.account_name,
            "symbol": session.spec.symbol,
            "round_progress": {
                "completed_rounds": progress["completed_rounds"],
                "skipped_rounds": progress["skipped_rounds"],
                "regular_completed_rounds": progress["regular_completed_rounds"],
                "regular_skipped_rounds": progress["regular_skipped_rounds"],
                "extension_rounds_used": progress["extension_rounds_used"],
                "next_round_index": progress["next_round_index"],
                "stage1_filled_total": str(progress["stage1_filled_total"]),
                "stage2_filled_total": str(progress["stage2_filled_total"]),
                "non_terminal_rounds": progress["non_terminal_rounds"],
            },
            "stage2_carryover_qty": str(session.stage2_carryover_qty),
            "remaining_extension_rounds": session.remaining_extension_rounds,
            "stored_extension_rounds_used": stored_extension_rounds_used,
            "stored_remaining_extension_rounds": stored_remaining_extension_rounds,
            "session_deadline_at": session.session_deadline_at.isoformat() if session.session_deadline_at else None,
            "stop_reason": session.stop_reason.value if session.stop_reason else None,
            "residual_source": session.residual_source,
        }
        processed_rounds = progress["completed_rounds"] + progress["skipped_rounds"]
        details["processed_rounds"] = processed_rounds
        if progress["non_terminal_rounds"]:
            return self._recovery_result(
                RecoveryStatus.MANUAL_CONFIRMATION,
                "会话存在未终态轮次，需人工确认后处理。",
                checked_at,
                details,
            )

        try:
            open_orders = await self._gateway.get_open_orders(session.spec.symbol)
        except Exception as exc:
            details["open_order_query_error"] = str(exc)
            return self._recovery_result(
                RecoveryStatus.MANUAL_CONFIRMATION,
                f"恢复判断时无法读取当前挂单状态: {exc}",
                checked_at,
                details,
            )

        session_prefix = f"{session.session_id}-"
        active_session_orders: list[dict[str, Any]] = []
        for order in open_orders:
            client_order_id = str(order.get("clientOrderId") or order.get("client_order_id") or "")
            if client_order_id.startswith(session_prefix):
                active_session_orders.append(
                    {
                        "order_id": str(order.get("orderId") or order.get("order_id") or ""),
                        "client_order_id": client_order_id,
                        "status": str(order.get("status") or ""),
                    }
                )
        details["active_session_orders"] = active_session_orders
        if active_session_orders:
            return self._recovery_result(
                RecoveryStatus.MANUAL_CONFIRMATION,
                "会话仍存在未完成系统挂单，需人工确认后处理。",
                checked_at,
                details,
            )

        known_order_statuses: list[dict[str, Any]] = []
        for order_id in self._extract_session_order_ids(payload):
            try:
                order = await self._gateway.get_order(symbol=session.spec.symbol, order_id=order_id)
            except Exception as exc:
                details["order_query_error"] = {"order_id": order_id, "error": str(exc)}
                return self._recovery_result(
                    RecoveryStatus.MANUAL_CONFIRMATION,
                    f"恢复判断时无法确认订单状态: {exc}",
                    checked_at,
                    details,
                )
            status_value = order.status.value if hasattr(order.status, "value") else str(order.status)
            known_order_statuses.append({"order_id": order_id, "status": status_value})
            if status_value in {"NEW", "PARTIALLY_FILLED"}:
                details["known_order_statuses"] = known_order_statuses
                return self._recovery_result(
                    RecoveryStatus.MANUAL_CONFIRMATION,
                    "会话存在未终态订单，需人工确认后处理。",
                    checked_at,
                    details,
                )
        details["known_order_statuses"] = known_order_statuses

        try:
            overview = await self._gateway.get_account_overview()
            rules = await self._gateway.get_symbol_rules(session.spec.symbol)
            quote = await self._gateway.get_quote(session.spec.symbol)
        except Exception as exc:
            details["context_error"] = str(exc)
            return self._recovery_result(
                RecoveryStatus.NON_RECOVERABLE,
                f"当前账户或交易对上下文不可读，无法恢复: {exc}",
                checked_at,
                details,
            )

        long_qty = self._position_qty(overview, session.spec.symbol, PositionSide.LONG)
        short_qty = self._position_qty(overview, session.spec.symbol, PositionSide.SHORT)
        details["account_context"] = {
            "available_balance": str(overview.get("totals", {}).get("available_balance") or "0"),
            "long_qty": str(long_qty),
            "short_qty": str(short_qty),
            "min_notional": str(rules.min_notional),
            "bid_price": str(quote.bid_price),
            "ask_price": str(quote.ask_price),
        }

        if session.spec.session_kind == SessionKind.PAIRED_CLOSE:
            remaining_close_qty = max(Decimal("0"), session.spec.target_close_qty - progress["stage1_filled_total"])
            details["remaining_target_close_qty"] = str(remaining_close_qty)
            if remaining_close_qty <= Decimal("0") and session.stage2_carryover_qty <= Decimal("0"):
                return self._recovery_result(
                    RecoveryStatus.NON_RECOVERABLE,
                    "双向平仓剩余数量已完成，无需恢复。",
                    checked_at,
                    details,
                    final_status=self._completed_status_for_recovery(session, progress),
                )
            if long_qty <= Decimal("0") or short_qty <= Decimal("0"):
                return self._recovery_result(
                    RecoveryStatus.NON_RECOVERABLE,
                    "当前已不存在可继续双向平仓的双边持仓。",
                    checked_at,
                    details,
                )
        elif session.spec.session_kind == SessionKind.PAIRED_OPEN:
            if processed_rounds >= session.spec.round_count and session.stage2_carryover_qty <= Decimal("0"):
                return self._recovery_result(
                    RecoveryStatus.NON_RECOVERABLE,
                    "双向开仓已完成，无需恢复。",
                    checked_at,
                    details,
                    final_status=self._completed_status_for_recovery(session, progress),
                )
        elif session.spec.session_kind == SessionKind.SINGLE_CLOSE:
            remaining_close_qty = max(Decimal("0"), session.spec.target_close_qty - progress["stage1_filled_total"])
            details["remaining_target_close_qty"] = str(remaining_close_qty)
            if session.spec.close_mode == SingleCloseMode.ALIGN:
                if long_qty == short_qty:
                    return self._recovery_result(
                        RecoveryStatus.NON_RECOVERABLE,
                        "当前对齐差值已消失，无法继续恢复。",
                        checked_at,
                        details,
                    )
                expected_side = PositionSide.LONG if long_qty > short_qty else PositionSide.SHORT
                details["align_expected_side"] = expected_side.value
                if session.spec.selected_position_side and expected_side != session.spec.selected_position_side:
                    return self._recovery_result(
                        RecoveryStatus.NON_RECOVERABLE,
                        "当前对齐方向已变化，无法继续恢复。",
                        checked_at,
                        details,
                    )
            else:
                available_qty = long_qty if session.spec.selected_position_side == PositionSide.LONG else short_qty
                details["available_close_qty"] = str(available_qty)
                if available_qty <= Decimal("0"):
                    return self._recovery_result(
                        RecoveryStatus.NON_RECOVERABLE,
                        "当前交易对不存在持仓。",
                        checked_at,
                        details,
                    )
            if remaining_close_qty <= Decimal("0") and session.stage2_carryover_qty <= Decimal("0"):
                return self._recovery_result(
                    RecoveryStatus.NON_RECOVERABLE,
                    "单向平仓剩余数量已完成，无需恢复。",
                    checked_at,
                    details,
                    final_status=self._completed_status_for_recovery(session, progress),
                )
            if session.session_deadline_at and checked_at >= session.session_deadline_at:
                return self._recovery_result(
                    RecoveryStatus.NON_RECOVERABLE,
                    "单向平仓会话已超过最长执行时长，无法继续恢复。",
                    checked_at,
                    details,
                    final_status=self._completed_status_with_remaining_qty(session, progress, remaining_close_qty),
                )
            regular_processed_rounds = progress["regular_completed_rounds"] + progress["regular_skipped_rounds"]
            if regular_processed_rounds >= session.spec.round_count and session.remaining_extension_rounds <= 0:
                return self._recovery_result(
                    RecoveryStatus.NON_RECOVERABLE,
                    "单向平仓补充轮已耗尽，无需恢复。",
                    checked_at,
                    details,
                    final_status=self._completed_status_with_remaining_qty(session, progress, remaining_close_qty),
                )
        elif session.spec.session_kind == SessionKind.SINGLE_OPEN:
            remaining_open_qty = max(Decimal("0"), session.spec.target_open_qty - progress["stage1_filled_total"])
            details["remaining_target_open_qty"] = str(remaining_open_qty)
            if remaining_open_qty <= Decimal("0") and session.stage2_carryover_qty <= Decimal("0"):
                return self._recovery_result(
                    RecoveryStatus.NON_RECOVERABLE,
                    "单向开仓剩余数量已完成，无需恢复。",
                    checked_at,
                    details,
                    final_status=self._completed_status_for_recovery(session, progress),
                )
            if session.session_deadline_at and checked_at >= session.session_deadline_at:
                return self._recovery_result(
                    RecoveryStatus.NON_RECOVERABLE,
                    "单向开仓会话已超过最长执行时长，无法继续恢复。",
                    checked_at,
                    details,
                    final_status=self._completed_status_with_remaining_qty(session, progress, remaining_open_qty),
                )
            regular_processed_rounds = progress["regular_completed_rounds"] + progress["regular_skipped_rounds"]
            if regular_processed_rounds >= session.spec.round_count and session.remaining_extension_rounds <= 0:
                return self._recovery_result(
                    RecoveryStatus.NON_RECOVERABLE,
                    "单向开仓补充轮已耗尽，无需恢复。",
                    checked_at,
                    details,
                    final_status=self._completed_status_with_remaining_qty(session, progress, remaining_open_qty),
                )
            if session.spec.open_mode == SingleOpenMode.ALIGN:
                if long_qty == short_qty:
                    return self._recovery_result(
                        RecoveryStatus.NON_RECOVERABLE,
                        "当前对齐差值已消失，无法继续恢复。",
                        checked_at,
                        details,
                    )
                expected_side = PositionSide.LONG if long_qty < short_qty else PositionSide.SHORT
                details["align_expected_side"] = expected_side.value
                if session.spec.selected_position_side and expected_side != session.spec.selected_position_side:
                    return self._recovery_result(
                        RecoveryStatus.NON_RECOVERABLE,
                        "当前对齐方向已变化，无法继续恢复。",
                        checked_at,
                        details,
                    )

        return self._recovery_result(
            RecoveryStatus.RECOVERABLE,
            "会话已无未决系统单，且当前上下文允许手动恢复。",
            checked_at,
            details,
        )

    async def evaluate_startup_recovery(self) -> list[dict[str, Any]]:
        evaluated: list[dict[str, Any]] = []
        for payload in self._repository.list_incomplete_sessions(self._account_id):
            full_payload = self._repository.get_session(payload["session_id"], self._account_id)
            if full_payload is None:
                continue
            recovery = await self._evaluate_recovery_payload(full_payload)
            final_status = recovery.get("final_status")
            if isinstance(final_status, SessionStatus):
                self._repository.update_session_status(
                    full_payload["session_id"],
                    final_status,
                    clear_recovery=True,
                )
            else:
                self._repository.update_session_status(
                    full_payload["session_id"],
                    SessionStatus.EXCEPTION,
                    last_error="Service restarted before session completion",
                )
                self._repository.update_session_recovery(
                    full_payload["session_id"],
                    recovery["recovery_status"],
                    recovery["recovery_summary"],
                    recovery["recovery_checked_at"],
                    recovery["recovery_details"],
                )
            self._repository.add_event(
                full_payload["session_id"],
                "session_reconciled_on_recovery",
                {
                    "status": final_status.value if isinstance(final_status, SessionStatus) else SessionStatus.EXCEPTION.value,
                    "recovery_status": recovery["recovery_status"].value,
                    "summary": recovery["recovery_summary"],
                    "details": recovery["recovery_details"],
                },
            )
            self._repository.add_event(
                full_payload["session_id"],
                "session_recovery_evaluated",
                {
                    "status": final_status.value if isinstance(final_status, SessionStatus) else SessionStatus.EXCEPTION.value,
                    "recovery_status": recovery["recovery_status"].value,
                    "summary": recovery["recovery_summary"],
                    "checked_at": recovery["recovery_checked_at"].isoformat(),
                },
            )
            evaluated.append({
                "session_id": full_payload["session_id"],
                "recovery_status": recovery["recovery_status"].value,
                "summary": recovery["recovery_summary"],
            })
        return evaluated

    def _error_context(self, *, session: OpenSession | None = None, stage: str | None = None, **context: Any) -> dict[str, Any]:
        payload = dict(context)
        if session is not None:
            payload.setdefault("session_id", session.session_id)
            payload.setdefault("symbol", session.spec.symbol)
            payload.setdefault("account_id", session.account_id)
            payload.setdefault("account_name", session.account_name)
            payload.setdefault("session_kind", session.spec.session_kind.value)
        if stage is not None:
            payload.setdefault("stage", stage)
        return {key: value for key, value in payload.items() if value is not None}

    def _normalize_trading_error(
        self,
        exc: Exception,
        *,
        source: str,
        code: str | None = None,
        session: OpenSession | None = None,
        stage: str | None = None,
        default_message: str | None = None,
        operator_action: str | None = None,
        **context: Any,
    ) -> TradingError:
        return ensure_trading_error(
            exc,
            source=source,
            code=code,
            context=self._error_context(session=session, stage=stage, **context),
            default_message=default_message,
            operator_action=operator_action,
        )

    def _persist_session_error(self, session_id: str, status: SessionStatus, error: TradingError) -> None:
        self._repository.update_session_status(
            session_id,
            status,
            last_error=str(error),
            last_error_category=error.category.value,
            last_error_strategy=error.strategy.value,
            last_error_code=error.code,
            last_error_operator_action=error.operator_action,
            last_error_params=error.params,
            last_error_raw_message=error.raw_message,
            last_error_contract_version=CONTRACT_VERSION,
        )

    def _error_event_payload(self, error: TradingError, **extra: Any) -> dict[str, Any]:
        payload = error.to_event_payload()
        payload.update({key: value for key, value in extra.items() if value is not None})
        payload["requires_operator_action"] = error.strategy == ErrorStrategy.MANUAL_INTERVENTION
        return payload

    def _precheck_item(
        self,
        code: str,
        label: str,
        status: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        label_key = f"precheck.labels.{code}"
        message_key = "runtime.precheck_legacy_message"
        message_params: dict[str, Any] = {"message": message}
        if code == "system_open_orders" and status == "fail" and "读取失败" in message:
            message_key = "precheck.system_open_orders.fail_read_error"
            suffix = message.split(":", 1)[1].strip() if ":" in message else message
            message_params = {"error": suffix}
        return make_precheck_item(
            code=code,
            status=status,
            label_key=label_key,
            message_key=message_key,
            message_params=message_params,
            details=details,
            legacy_message=message,
            legacy_label=label,
        )

    def _finalize_precheck(self, checks: list[dict[str, Any]], derived: dict[str, Any], *, default_summary: str) -> dict[str, Any]:
        failures = [item for item in checks if item["status"] == "fail"]
        warnings = [item for item in checks if item["status"] == "warn"]
        summary_item: dict[str, Any] | None = None
        if failures:
            summary_item = failures[0]
        elif warnings:
            summary_item = warnings[0]
        summary_code = str(summary_item.get("message_key")) if summary_item else "runtime.precheck_legacy_message"
        summary_params = dict(summary_item.get("message_params") or {}) if summary_item else {"message": default_summary}
        summary = str(summary_item.get("message")) if summary_item else default_summary
        return {
            "contract_version": CONTRACT_VERSION,
            "ok": not failures,
            "summary_code": summary_code,
            "summary_params": summary_params,
            "summary": summary,
            "checks": checks,
            "derived": derived,
        }

    def _stringify_decimal(self, value: Decimal | int | float | str | None) -> str:
        if value is None:
            return "0"
        return str(value)

    def _resolved_execution_policy(
        self,
        *,
        session_kind: SessionKind,
        execution_profile: ExecutionProfile | str | None = None,
        market_fallback_max_ratio: Decimal | str | None = None,
        market_fallback_min_residual_qty: Decimal | str | None = None,
        max_reprice_ticks: int | None = None,
        max_spread_bps: int | None = None,
        max_reference_deviation_bps: int | None = None,
    ) -> ResolvedExecutionPolicy:
        return resolve_execution_policy(
            self._settings,
            session_kind=session_kind,
            execution_profile=execution_profile,
            market_fallback_max_ratio=market_fallback_max_ratio,
            market_fallback_min_residual_qty=market_fallback_min_residual_qty,
            max_reprice_ticks=max_reprice_ticks,
            max_spread_bps=max_spread_bps,
            max_reference_deviation_bps=max_reference_deviation_bps,
        )

    def _resolved_execution_policy_payload(
        self,
        *,
        session_kind: SessionKind,
        execution_profile: ExecutionProfile | str | None = None,
        market_fallback_max_ratio: Decimal | str | None = None,
        market_fallback_min_residual_qty: Decimal | str | None = None,
        max_reprice_ticks: int | None = None,
        max_spread_bps: int | None = None,
        max_reference_deviation_bps: int | None = None,
        prefix: str = "resolved_",
    ) -> dict[str, Any]:
        policy = self._resolved_execution_policy(
            session_kind=session_kind,
            execution_profile=execution_profile,
            market_fallback_max_ratio=market_fallback_max_ratio,
            market_fallback_min_residual_qty=market_fallback_min_residual_qty,
            max_reprice_ticks=max_reprice_ticks,
            max_spread_bps=max_spread_bps,
            max_reference_deviation_bps=max_reference_deviation_bps,
        )
        return policy.to_payload(prefix=prefix)

    def _format_money(self, value: Decimal | int | float | str | None) -> str:
        if value is None:
            return "0.00"
        return str(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    def _build_single_round_plan(
        self,
        *,
        normalized_target_qty: Decimal,
        round_count: int,
        rules,
    ) -> tuple[Decimal, Decimal, list[Decimal], Decimal]:
        round_qty = normalize_qty(normalized_target_qty / Decimal(round_count), rules)
        if round_qty <= Decimal("0"):
            raise ValueError("每轮数量归一化后为 0，无法执行")
        final_round_qty = normalize_qty(
            normalized_target_qty - (round_qty * Decimal(max(round_count - 1, 0))),
            rules,
        )
        if final_round_qty <= Decimal("0"):
            raise ValueError("最后一轮数量归一化后为 0，无法执行")
        planned_round_qtys = [round_qty for _ in range(max(round_count - 1, 0))] + [final_round_qty]
        extension_round_cap_qty = max(planned_round_qtys) if planned_round_qtys else round_qty
        return round_qty, final_round_qty, planned_round_qtys, extension_round_cap_qty
    async def _get_open_order_counts(self, symbol: str) -> tuple[int | None, int | None, str | None]:
        try:
            open_orders = await self._gateway.get_open_orders(symbol)
        except Exception as exc:
            return None, None, str(exc)
        system_count = 0
        manual_count = 0
        for order in open_orders:
            client_order_id = str(order.get("clientOrderId") or order.get("client_order_id") or "")
            if SYSTEM_ORDER_ID_RE.match(client_order_id):
                system_count += 1
            else:
                manual_count += 1
        return system_count, manual_count, None

    async def precheck_request(self, request: SessionPrecheckRequest, *, strict_hedge_mode: bool = False) -> dict[str, Any]:
        if request.session_kind == SessionKind.PAIRED_OPEN:
            return await self._precheck_paired_open(request, strict_hedge_mode=strict_hedge_mode)
        if request.session_kind == SessionKind.PAIRED_CLOSE:
            return await self._precheck_paired_close(request, strict_hedge_mode=strict_hedge_mode)
        if request.session_kind == SessionKind.SINGLE_OPEN:
            return await self._precheck_single_open(request, strict_hedge_mode=strict_hedge_mode)
        if request.session_kind == SessionKind.SINGLE_CLOSE:
            return await self._precheck_single_close(request, strict_hedge_mode=strict_hedge_mode)
        return self._finalize_precheck(
            [self._precheck_item("unsupported_session_kind", "会话类型", "fail", f"不支持的会话类型: {request.session_kind}")],
            {"session_kind": str(request.session_kind), "account_id": self._account_id, "account_name": self._account_name},
            default_summary="不支持的会话类型",
        )
    async def precheck_open_request(self, request: OpenSessionRequest | SingleOpenSessionRequest, *, strict_hedge_mode: bool = False) -> dict[str, Any]:
        if isinstance(request, OpenSessionRequest):
            return await self.precheck_request(
                SessionPrecheckRequest(
                    session_kind=SessionKind.PAIRED_OPEN,
                    symbol=request.symbol,
                    trend_bias=request.trend_bias,
                    leverage=request.leverage,
                    round_count=request.round_count,
                    round_qty=request.round_qty,
                    execution_profile=request.execution_profile,
                    market_fallback_max_ratio=request.market_fallback_max_ratio,
                    market_fallback_min_residual_qty=request.market_fallback_min_residual_qty,
                    max_reprice_ticks=request.max_reprice_ticks,
                    max_spread_bps=request.max_spread_bps,
                    max_reference_deviation_bps=request.max_reference_deviation_bps,
                ),
                strict_hedge_mode=strict_hedge_mode,
            )
        return await self.precheck_request(
            SessionPrecheckRequest(
                session_kind=SessionKind.SINGLE_OPEN,
                symbol=request.symbol,
                leverage=request.leverage,
                round_count=request.round_count,
                open_qty=request.open_qty,
                selected_position_side=request.selected_position_side,
                open_mode=request.open_mode,
                execution_profile=request.execution_profile,
                market_fallback_max_ratio=request.market_fallback_max_ratio,
                market_fallback_min_residual_qty=request.market_fallback_min_residual_qty,
                max_reprice_ticks=request.max_reprice_ticks,
                max_spread_bps=request.max_spread_bps,
                max_reference_deviation_bps=request.max_reference_deviation_bps,
            ),
            strict_hedge_mode=strict_hedge_mode,
        )
    async def _account_status_snapshot(self, symbol: str, *, strict_hedge_mode: bool = False) -> tuple[list[dict[str, Any]], dict[str, Any] | None, Any | None, int, Any | None]:
        checks: list[dict[str, Any]] = []
        overview = None
        rules = None
        quote = None
        current_leverage = 1
        try:
            overview = await self._gateway.get_account_overview()
            checks.append(self._precheck_item("account_readable", "账户状态", "pass", "统一账户可读"))
        except Exception as exc:
            checks.append(self._precheck_item("account_readable", "账户状态", "fail", f"统一账户不可读: {exc}"))
        try:
            hedge_enabled = await self._gateway.is_hedge_mode_enabled()
            checks.append(
                self._precheck_item(
                    "hedge_mode",
                    "双向持仓",
                    "pass" if hedge_enabled else "fail",
                    "Hedge Mode 已启用" if hedge_enabled else "Hedge Mode 未启用",
                )
            )
        except Exception as exc:
            auth_read_failed = overview is not None and self._is_hedge_mode_read_auth_failure(exc)
            if auth_read_failed:
                if strict_hedge_mode:
                    try:
                        await self._confirm_hedge_mode_for_execution()
                        checks.append(
                            self._precheck_item(
                                "hedge_mode",
                                "双向持仓",
                                "pass",
                                "无法直接读取 FAPI 双向持仓状态，已按执行链路确认 Hedge Mode 可用。",
                            )
                        )
                    except Exception as confirm_exc:
                        checks.append(
                            self._precheck_item(
                                "hedge_mode",
                                "双向持仓",
                                "fail",
                                str(confirm_exc),
                            )
                        )
                else:
                    checks.append(
                        self._precheck_item(
                            "hedge_mode",
                            "双向持仓",
                            "skip",
                            "普通预检阶段已跳过 Hedge Mode 严格确认。",
                        )
                    )
            else:
                checks.append(self._precheck_item("hedge_mode", "双向持仓", "fail", f"双向持仓状态读取失败: {exc}"))
        try:
            rules = await self._gateway.get_symbol_rules(symbol)
            current_leverage = max(int(await self._gateway.get_symbol_leverage(symbol) or 1), 1)
            checks.append(self._precheck_item("symbol_rules", "交易对规则", "pass", f"交易对规则可读，当前杠杆 {current_leverage}x"))
        except Exception as exc:
            checks.append(self._precheck_item("symbol_rules", "交易对规则", "fail", f"交易对规则或杠杆读取失败: {exc}"))
        try:
            quote = await self._gateway.get_quote(symbol)
            checks.append(self._precheck_item("quote", "参考价格", "pass", "订单簿参考价格可读"))
        except Exception as exc:
            checks.append(self._precheck_item("quote", "参考价格", "fail", f"订单簿参考价格读取失败: {exc}"))
        return checks, overview, rules, current_leverage, quote

    def _is_hedge_mode_read_auth_failure(self, exc: Exception) -> bool:
        message = str(exc)
        return (
            ("401 Unauthorized" in message)
            or ("fapi/v1/positionSide/dual" in message)
            or ("Binance API 鉴权失败" in message)
            or (("鉴权失败" in message) and ("双向持仓" in message or "position side" in message or "hedge mode" in message))
        )

    async def _confirm_hedge_mode_for_execution(self) -> None:
        try:
            hedge_enabled = await self._gateway.is_hedge_mode_enabled()
        except Exception as exc:
            if not self._is_hedge_mode_read_auth_failure(exc):
                raise RuntimeError(f"双向持仓状态读取失败: {exc}") from exc
            if self._hedge_mode_fallback_confirmed:
                return
            try:
                await self._gateway.ensure_hedge_mode()
            except Exception as confirm_exc:
                raise RuntimeError(f"当前账户无法读取 FAPI 双向持仓状态，且执行链路确认失败: {confirm_exc}") from confirm_exc
            self._hedge_mode_fallback_confirmed = True
            return
        if hedge_enabled:
            return
        try:
            await self._gateway.ensure_hedge_mode()
        except Exception as exc:
            raise RuntimeError(f"Hedge Mode 未启用，且开启失败: {exc}") from exc

    def _with_hedge_mode_failure(self, precheck: dict[str, Any], message: str) -> dict[str, Any]:
        checks = [dict(item) for item in precheck.get("checks", [])]
        updated = False
        for item in checks:
            if item.get("code") == "hedge_mode":
                replacement = self._precheck_item("hedge_mode", "双向持仓", "fail", message, item.get("details"))
                item.clear()
                item.update(replacement)
                updated = True
                break
        if not updated:
            checks.append(self._precheck_item("hedge_mode", "双向持仓", "fail", message))
        payload = dict(precheck)
        payload["ok"] = False
        payload["checks"] = checks
        payload["summary_code"] = "runtime.precheck_legacy_message"
        payload["summary_params"] = {"message": message}
        payload["summary"] = format_copy("runtime.precheck_legacy_message", {"message": message})
        return payload
    async def _common_precheck_context(self, request: SessionPrecheckRequest, *, strict_hedge_mode: bool = False) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any] | None, Any | None, int, Any | None]:
        symbol = request.symbol.upper()
        checks, overview, rules, current_leverage, quote = await self._account_status_snapshot(symbol, strict_hedge_mode=strict_hedge_mode)
        system_open_order_count, manual_open_order_count, open_order_error = await self._get_open_order_counts(symbol)
        if open_order_error is not None:
            checks.append(
                self._precheck_item(
                    "system_open_orders",
                    "系统挂单冲突",
                    "fail",
                    f"系统挂单状态读取失败: {open_order_error}",
                )
            )
            system_open_order_count = 0
            manual_open_order_count = 0
        else:
            checks.append(
                self._precheck_item(
                    "system_open_orders",
                    "系统挂单冲突",
                    "fail" if system_open_order_count > 0 else "pass",
                    f"当前交易对存在 {system_open_order_count} 笔系统未完成挂单，无法开始" if system_open_order_count > 0 else "当前交易对不存在系统挂单冲突",
                    {"system_open_order_count": system_open_order_count},
                )
            )
            if manual_open_order_count > 0:
                checks.append(
                    self._precheck_item(
                        "manual_open_orders",
                        "手工挂单",
                        "warn",
                        f"当前交易对存在 {manual_open_order_count} 笔手工挂单，将忽略但请注意风险",
                        {"manual_open_order_count": manual_open_order_count},
                    )
                )
        long_qty = self._position_qty(overview or {}, symbol, PositionSide.LONG) if overview else Decimal("0")
        short_qty = self._position_qty(overview or {}, symbol, PositionSide.SHORT) if overview else Decimal("0")
        available_balance = Decimal(str((overview or {}).get("totals", {}).get("available_balance") or "0"))
        derived: dict[str, Any] = {
            "symbol": symbol,
            "session_kind": request.session_kind.value,
            "account_id": self._account_id,
            "account_name": self._account_name,
            "available_balance": self._stringify_decimal(available_balance),
            "max_open_amount_95": self._stringify_decimal(available_balance * Decimal("0.95")),
            "current_leverage": current_leverage,
            "long_qty": self._stringify_decimal(long_qty),
            "short_qty": self._stringify_decimal(short_qty),
            "system_open_order_count": system_open_order_count,
            "manual_open_order_count": manual_open_order_count,
            "min_notional": self._stringify_decimal(getattr(rules, "min_notional", Decimal("0"))),
            "normalized_round_qty": "0",
            "per_round_notional": "0",
            "total_notional": "0",
            "implied_margin_amount": "0",
        }
        derived.update(
            self._resolved_execution_policy_payload(
                session_kind=request.session_kind,
                execution_profile=request.execution_profile,
                market_fallback_max_ratio=request.market_fallback_max_ratio,
                market_fallback_min_residual_qty=request.market_fallback_min_residual_qty,
                max_reprice_ticks=request.max_reprice_ticks,
                max_spread_bps=request.max_spread_bps,
                max_reference_deviation_bps=request.max_reference_deviation_bps,
            )
        )
        return checks, derived, overview, rules, current_leverage, quote

    async def _precheck_paired_open(self, request: SessionPrecheckRequest, *, strict_hedge_mode: bool = False) -> dict[str, Any]:
        checks, derived, overview, rules, current_leverage, quote = await self._common_precheck_context(request, strict_hedge_mode=strict_hedge_mode)
        symbol = request.symbol.upper()
        requested_leverage = int(request.leverage or 1)
        allowed = symbol in self._settings.normalized_whitelist
        checks.append(self._precheck_item("whitelist", "白名单", "pass" if allowed else "fail", "交易对白名单校验通过" if allowed else f"{symbol} 不在白名单中，无法真实开仓"))
        if request.round_qty is None or request.trend_bias is None:
            checks.append(self._precheck_item("request", "参数完整性", "fail", "缺少双向开仓必要参数"))
            return self._finalize_precheck(checks, derived, default_summary="双向开仓预检完成")
        if rules and quote:
            requested_round_qty = Decimal(request.round_qty)
            normalized_round_qty = normalize_qty(requested_round_qty, rules)
            stage1_price = normalize_price(self._open_stage1_price(request.trend_bias, quote), rules)
            requested_per_round_notional = requested_round_qty * stage1_price
            total_notional = requested_per_round_notional * Decimal(request.round_count)
            implied_margin_amount = total_notional / Decimal(requested_leverage)
            derived.update({
                "normalized_round_qty": self._stringify_decimal(normalized_round_qty),
                "per_round_notional": self._stringify_decimal(requested_per_round_notional),
                "total_notional": self._stringify_decimal(total_notional),
                "implied_margin_amount": self._stringify_decimal(implied_margin_amount),
                "requested_leverage": requested_leverage,
            })
            if requested_leverage > rules.max_leverage:
                checks.append(self._precheck_item("leverage", "杠杆限制", "fail", f"杠杆 {requested_leverage}x 超过交易对最大杠杆 {rules.max_leverage}x"))
            else:
                checks.append(self._precheck_item("leverage", "杠杆限制", "pass", f"杠杆 {requested_leverage}x 合法"))
            try:
                validate_qty_and_notional(normalized_round_qty, stage1_price, rules)
                checks.append(self._precheck_item("minimum_order", "最小下单金额", "pass", "每轮下单金额满足交易所要求"))
            except ValueError as exc:
                checks.append(self._precheck_item("minimum_order", "最小下单金额", "fail", f"每轮开单金额 {self._format_money(requested_per_round_notional)} 低于交易所最小下单金额 {self._format_money(getattr(rules, "min_notional", Decimal("0")))}，无法开单。"))
            max_open_amount = Decimal(str(derived["max_open_amount_95"]))
            if implied_margin_amount > max_open_amount:
                checks.append(self._precheck_item("max_open_amount", "最大可承受仓位", "fail", "开仓总金额超过可用余额的95%，无法开单。"))
            else:
                checks.append(self._precheck_item("max_open_amount", "最大可承受仓位", "pass", "开单金额未超过可用余额 95% 限制"))
        if overview:
            checks.append(self._precheck_item("position_state", "持仓状态", "warn", f"当前双边持仓 LONG={derived['long_qty']} / SHORT={derived['short_qty']}，双向开仓允许继续"))
        return self._finalize_precheck(checks, derived, default_summary="双向开仓预检通过")

    async def _precheck_paired_close(self, request: SessionPrecheckRequest, *, strict_hedge_mode: bool = False) -> dict[str, Any]:
        checks, derived, overview, rules, current_leverage, quote = await self._common_precheck_context(request, strict_hedge_mode=strict_hedge_mode)
        if request.close_qty is None or request.trend_bias is None:
            checks.append(self._precheck_item("request", "参数完整性", "fail", "缺少双向平仓必要参数"))
            return self._finalize_precheck(checks, derived, default_summary="双向平仓预检完成")
        if rules and quote:
            normalized_round_qty = normalize_qty(request.close_qty / Decimal(request.round_count), rules)
            stage1_price, stage2_price = self._close_stage_prices(request.trend_bias, quote)
            stage1_price = normalize_price(stage1_price, rules)
            per_round_notional = normalized_round_qty * stage1_price
            total_notional = Decimal(request.close_qty) * stage1_price
            derived.update({
                "normalized_round_qty": self._stringify_decimal(normalized_round_qty),
                "per_round_notional": self._stringify_decimal(per_round_notional),
                "total_notional": self._stringify_decimal(total_notional),
            })
            max_closeable_qty = min(Decimal(derived["long_qty"]), Decimal(derived["short_qty"]))
            if max_closeable_qty <= Decimal("0"):
                checks.append(self._precheck_item("position_state", "持仓状态", "fail", "当前账户不存在可双向平仓的双边持仓"))
            elif Decimal(request.close_qty) > max_closeable_qty:
                checks.append(self._precheck_item("position_state", "持仓状态", "fail", f"平仓数量 {request.close_qty} 超过可双向平仓数量 {self._stringify_decimal(max_closeable_qty)}"))
            else:
                checks.append(self._precheck_item("position_state", "持仓状态", "pass", "双边持仓数量满足双向平仓条件"))
            try:
                validate_qty_and_notional(normalized_round_qty, stage1_price, rules)
                validate_qty_and_notional(normalized_round_qty, normalize_price(stage2_price, rules), rules)
                checks.append(self._precheck_item("minimum_order", "最小下单金额", "pass", "每轮平仓金额满足交易所要求"))
            except ValueError as exc:
                checks.append(
                    self._precheck_item(
                        "minimum_order",
                        "最小下单金额",
                        "fail",
                        f"每轮平仓金额 {self._format_money(per_round_notional)} 低于交易所最小下单金额 {self._format_money(getattr(rules, 'min_notional', Decimal('0')))}，无法平仓。",
                    )
                )
            checks.append(self._precheck_item("max_open_amount", "最大可承受仓位", "skip", "平仓类不校验可用余额 95% 限制"))
        return self._finalize_precheck(checks, derived, default_summary="双向平仓预检通过")

    async def _precheck_single_open(self, request: SessionPrecheckRequest, *, strict_hedge_mode: bool = False) -> dict[str, Any]:
        checks, derived, overview, rules, current_leverage, quote = await self._common_precheck_context(request, strict_hedge_mode=strict_hedge_mode)
        symbol = request.symbol.upper()
        allowed = symbol in self._settings.normalized_whitelist
        checks.append(self._precheck_item("whitelist", "白名单", "pass" if allowed else "fail", "交易对白名单校验通过" if allowed else f"{symbol} 不在白名单中，无法真实开仓"))
        if request.open_qty is None or request.open_mode is None:
            checks.append(self._precheck_item("request", "参数完整性", "fail", "缺少单向开仓必要参数"))
            return self._finalize_precheck(checks, derived, default_summary="单向开仓预检完成")
        long_qty = Decimal(derived["long_qty"])
        short_qty = Decimal(derived["short_qty"])
        selected_position_side = request.selected_position_side
        open_qty = Decimal(request.open_qty)
        if request.open_mode == SingleOpenMode.ALIGN:
            if long_qty == short_qty:
                checks.append(self._precheck_item("position_state", "持仓状态", "fail", "当前双边持仓数量已对齐，无需单向开仓"))
                selected_position_side = PositionSide.LONG if long_qty <= short_qty else PositionSide.SHORT
                open_qty = Decimal("0")
            else:
                selected_position_side = PositionSide.LONG if long_qty < short_qty else PositionSide.SHORT
                open_qty = abs(long_qty - short_qty)
                checks.append(self._precheck_item("position_state", "持仓状态", "pass", f"订单对齐将自动补齐 {selected_position_side.value}，差值数量 {self._stringify_decimal(open_qty)}"))
        elif selected_position_side is None:
            checks.append(self._precheck_item("position_state", "持仓状态", "fail", "常规单向开仓需要选择开仓订单方向"))
        if rules and quote:
            effective_leverage = int(request.leverage or current_leverage or 1)
            has_positions = long_qty > Decimal("0") or short_qty > Decimal("0")
            actual_leverage = self._position_leverage(overview or {}, symbol) or current_leverage or 1
            if has_positions:
                effective_leverage = int(actual_leverage)
                if int(request.leverage or effective_leverage) != effective_leverage:
                    checks.append(self._precheck_item("leverage", "杠杆限制", "fail", f"当前交易对已有持仓，杠杆必须与现有持仓一致：{effective_leverage}x"))
                else:
                    checks.append(self._precheck_item("leverage", "杠杆限制", "pass", f"当前交易对已有持仓，杠杆锁定为 {effective_leverage}x"))
            elif int(request.leverage or 1) > rules.max_leverage:
                checks.append(self._precheck_item("leverage", "杠杆限制", "fail", f"杠杆 {request.leverage}x 超过交易对最大杠杆 {rules.max_leverage}x"))
            else:
                checks.append(self._precheck_item("leverage", "杠杆限制", "pass", f"杠杆 {effective_leverage}x 合法"))
            side_price = normalize_price(self._single_open_params(selected_position_side or PositionSide.LONG, quote)[1], rules)
            requested_round_qty = open_qty / Decimal(request.round_count)
            normalized_round_qty = normalize_qty(requested_round_qty, rules)
            requested_per_round_notional = requested_round_qty * side_price
            total_notional = open_qty * side_price
            implied_margin_amount = total_notional / Decimal(max(effective_leverage, 1))
            derived.update({
                "selected_position_side": selected_position_side.value if selected_position_side else None,
                "normalized_round_qty": self._stringify_decimal(normalized_round_qty),
                "per_round_notional": self._stringify_decimal(requested_per_round_notional),
                "total_notional": self._stringify_decimal(total_notional),
                "implied_margin_amount": self._stringify_decimal(implied_margin_amount),
                "current_leverage": effective_leverage,
            })
            try:
                validate_qty_and_notional(normalized_round_qty, side_price, rules)
                checks.append(self._precheck_item("minimum_order", "最小下单金额", "pass", "每轮开仓金额满足交易所要求"))
            except ValueError as exc:
                checks.append(self._precheck_item("minimum_order", "最小下单金额", "fail", f"每轮开单金额 {self._format_money(requested_per_round_notional)} 低于交易所最小下单金额 {self._format_money(getattr(rules, "min_notional", Decimal("0")))}，无法开单。"))
            max_open_amount = Decimal(str(derived["max_open_amount_95"]))
            if implied_margin_amount > max_open_amount:
                checks.append(self._precheck_item("max_open_amount", "最大可承受仓位", "fail", "开仓总金额超过可用余额的95%，无法开单。"))
            else:
                checks.append(self._precheck_item("max_open_amount", "最大可承受仓位", "pass", "开单金额未超过可用余额 95% 限制"))
        return self._finalize_precheck(checks, derived, default_summary="单向开仓预检通过")

    async def _precheck_single_close(self, request: SessionPrecheckRequest, *, strict_hedge_mode: bool = False) -> dict[str, Any]:
        checks, derived, overview, rules, current_leverage, quote = await self._common_precheck_context(request, strict_hedge_mode=strict_hedge_mode)
        if request.close_qty is None or request.close_mode is None:
            checks.append(self._precheck_item("request", "参数完整性", "fail", "缺少单向平仓必要参数"))
            return self._finalize_precheck(checks, derived, default_summary="单向平仓预检完成")
        long_qty = Decimal(derived["long_qty"])
        short_qty = Decimal(derived["short_qty"])
        selected_position_side = request.selected_position_side
        close_qty = Decimal(request.close_qty)
        if request.close_mode == SingleCloseMode.ALIGN:
            if long_qty == short_qty:
                checks.append(
                    self._precheck_item(
                        "position_state",
                        "持仓状态",
                        "fail",
                        "当前交易对不存在持仓" if long_qty <= Decimal("0") else "当前双边持仓数量已对齐，无需单向平仓",
                    )
                )
                selected_position_side = PositionSide.LONG if long_qty >= short_qty else PositionSide.SHORT
                close_qty = Decimal("0")
            else:
                selected_position_side = PositionSide.LONG if long_qty > short_qty else PositionSide.SHORT
                close_qty = abs(long_qty - short_qty)
                checks.append(self._precheck_item("position_state", "持仓状态", "pass", f"订单对齐将自动平掉 {selected_position_side.value}，差值数量 {self._stringify_decimal(close_qty)}"))
        elif selected_position_side is None:
            checks.append(self._precheck_item("position_state", "持仓状态", "fail", "常规单向平仓需要选择平仓订单方向"))
        else:
            available_qty = long_qty if selected_position_side == PositionSide.LONG else short_qty
            if available_qty <= Decimal("0"):
                checks.append(self._precheck_item("position_state", "持仓状态", "fail", "当前交易对不存在持仓"))
            elif close_qty > available_qty:
                checks.append(self._precheck_item("position_state", "持仓状态", "fail", f"平仓数量 {self._stringify_decimal(close_qty)} 超过所选持仓数量 {self._stringify_decimal(available_qty)}"))
            else:
                checks.append(self._precheck_item("position_state", "持仓状态", "pass", "所选持仓数量满足单向平仓条件"))
        if rules and quote:
            side_price = normalize_price(self._single_close_params(selected_position_side or PositionSide.LONG, quote)[1], rules)
            normalized_round_qty = normalize_qty(close_qty / Decimal(request.round_count), rules)
            per_round_notional = normalized_round_qty * side_price
            total_notional = close_qty * side_price
            derived.update({
                "selected_position_side": selected_position_side.value if selected_position_side else None,
                "normalized_round_qty": self._stringify_decimal(normalized_round_qty),
                "per_round_notional": self._stringify_decimal(per_round_notional),
                "total_notional": self._stringify_decimal(total_notional),
            })
            try:
                validate_qty_and_notional(normalized_round_qty, side_price, rules)
                checks.append(self._precheck_item("minimum_order", "最小下单金额", "pass", "每轮平仓金额满足交易所要求"))
            except ValueError as exc:
                checks.append(
                    self._precheck_item(
                        "minimum_order",
                        "最小下单金额",
                        "fail",
                        f"每轮平仓金额 {self._format_money(per_round_notional)} 低于交易所最小下单金额 {self._format_money(getattr(rules, 'min_notional', Decimal('0')))}，无法平仓。",
                    )
                )
            checks.append(self._precheck_item("max_open_amount", "最大可承受仓位", "skip", "平仓类不校验可用余额 95% 限制"))
        return self._finalize_precheck(checks, derived, default_summary="单向平仓预检通过")
    async def create_session(self, request: OpenSessionRequest) -> OpenSession:
        return await self.create_open_session(request)

    async def create_open_session(self, request: OpenSessionRequest) -> OpenSession:
        symbol = request.symbol.upper()
        execution_policy = self._resolved_execution_policy(
            session_kind=SessionKind.PAIRED_OPEN,
            execution_profile=request.execution_profile,
            market_fallback_max_ratio=request.market_fallback_max_ratio,
            market_fallback_min_residual_qty=request.market_fallback_min_residual_qty,
            max_reprice_ticks=request.max_reprice_ticks,
            max_spread_bps=request.max_spread_bps,
            max_reference_deviation_bps=request.max_reference_deviation_bps,
        )
        async with self._session_creation_lock:
            if symbol not in self._settings.normalized_whitelist:
                raise ValueError(f"Symbol {symbol} is not in whitelist")
            self._ensure_no_active_symbol_session(symbol)
            rules = await self._gateway.get_symbol_rules(symbol)
            if request.leverage > rules.max_leverage:
                raise ValueError(f"Leverage {request.leverage} exceeds max {rules.max_leverage} for {symbol}")
            normalized_qty = normalize_qty(request.round_qty, rules)
            if normalized_qty <= Decimal("0"):
                raise ValueError(f"Round quantity {request.round_qty} becomes zero after normalization")
            quote = await self._gateway.get_quote(symbol)
            stage1_price = normalize_price(self._open_stage1_price(request.trend_bias, quote), rules)
            try:
                validate_qty_and_notional(normalized_qty, stage1_price, rules)
            except ValueError as exc:
                raise ValueError("每轮开单金额低于交易所最小开单金额，无法开单") from exc
            account_overview = await self._gateway.get_account_overview()
            available_balance = Decimal(str(account_overview.get("totals", {}).get("available_balance") or "0"))
            max_open_amount = available_balance * Decimal("0.95")
            total_notional = normalized_qty * stage1_price * Decimal(request.round_count)
            implied_open_amount = total_notional / Decimal(request.leverage)
            if implied_open_amount > max_open_amount:
                raise ValueError(
                    f"开单金额 {implied_open_amount} 超过当前可用余额 {available_balance} 的 95%，无法开单"
                )
            precheck = await self.precheck_open_request(request, strict_hedge_mode=False)
            if not precheck['ok']:
                raise SessionPrecheckFailed(precheck)
            try:
                await self._confirm_hedge_mode_for_execution()
            except Exception as exc:
                raise SessionPrecheckFailed(self._with_hedge_mode_failure(precheck, str(exc))) from exc

            spec = SessionSpec(
                symbol=symbol,
                trend_bias=request.trend_bias,
                leverage=request.leverage,
                round_count=request.round_count,
                round_qty=normalized_qty,
                poll_interval_ms=request.poll_interval_ms or self._settings.default_poll_interval_ms,
                order_ttl_ms=request.order_ttl_ms or self._settings.default_order_ttl_ms,
                max_zero_fill_retries=request.max_zero_fill_retries or self._settings.default_max_zero_fill_retries,
                market_fallback_attempts=request.market_fallback_attempts or self._settings.default_market_fallback_attempts,
                execution_profile=execution_policy.execution_profile,
                market_fallback_max_ratio=execution_policy.market_fallback_max_ratio,
                market_fallback_min_residual_qty=execution_policy.market_fallback_min_residual_qty,
                max_reprice_ticks=execution_policy.max_reprice_ticks,
                max_spread_bps=execution_policy.max_spread_bps,
                max_reference_deviation_bps=execution_policy.max_reference_deviation_bps,
                round_interval_seconds=request.round_interval_seconds if request.round_interval_seconds is not None else 3,
                created_by=request.created_by,
                session_kind=SessionKind.PAIRED_OPEN,
            )
            session = OpenSession.create(spec, account_id=self._account_id, account_name=self._account_name)
            self._repository.create_session(session)
            self._repository.add_event(
                session.session_id,
                "session_created",
                {
                    "session_kind": spec.session_kind.value,
                    "symbol": symbol,
                    "trend_bias": spec.trend_bias.value,
                    "round_count": spec.round_count,
                    "round_qty": str(spec.round_qty),
                    "leverage": spec.leverage,
                    "round_interval_seconds": spec.round_interval_seconds,
                    "account_id": self._account_id,
                    "account_name": self._account_name,
                    "min_notional": str(rules.min_notional),
                    "stage1_price": str(stage1_price),
                    **execution_policy.to_payload(),
                },
            )
        try:
            await self._gateway.ensure_hedge_mode()
            await self._gateway.ensure_cross_margin(symbol)
            await self._gateway.ensure_leverage(symbol, spec.leverage)
        except Exception as exc:
            error = self._normalize_trading_error(exc, source="service", code="session_preflight_failed", session=session, stage="preflight")
            self._persist_session_error(session.session_id, SessionStatus.EXCEPTION, error)
            self._repository.add_event(
                session.session_id,
                "session_preflight_failed",
                self._error_event_payload(error, session_kind=session.spec.session_kind.value),
            )
            raise
        return self._launch_session(session)

    async def create_single_open_session(self, request: SingleOpenSessionRequest) -> OpenSession:
        symbol = request.symbol.upper()
        execution_policy = self._resolved_execution_policy(
            session_kind=SessionKind.SINGLE_OPEN,
            execution_profile=request.execution_profile,
            market_fallback_max_ratio=request.market_fallback_max_ratio,
            market_fallback_min_residual_qty=request.market_fallback_min_residual_qty,
            max_reprice_ticks=request.max_reprice_ticks,
            max_spread_bps=request.max_spread_bps,
            max_reference_deviation_bps=request.max_reference_deviation_bps,
        )
        async with self._session_creation_lock:
            if symbol not in self._settings.normalized_whitelist:
                raise ValueError(f"Symbol {symbol} is not in whitelist")
            self._ensure_no_active_symbol_session(symbol)
            rules = await self._gateway.get_symbol_rules(symbol)
            account_overview = await self._gateway.get_account_overview()
            long_qty = normalize_qty(self._position_qty(account_overview, symbol, PositionSide.LONG), rules)
            short_qty = normalize_qty(self._position_qty(account_overview, symbol, PositionSide.SHORT), rules)

            if request.open_mode == SingleOpenMode.ALIGN:
                if long_qty == short_qty:
                    raise ValueError("当前双边持仓数量已对齐，无需单向开仓")
                selected_position_side = PositionSide.LONG if long_qty < short_qty else PositionSide.SHORT
                normalized_open_qty = normalize_qty(abs(long_qty - short_qty), rules)
            else:
                if request.selected_position_side is None:
                    raise ValueError("常规开仓需要先选择开仓订单")
                selected_position_side = request.selected_position_side
                normalized_open_qty = normalize_qty(request.open_qty, rules)

            if normalized_open_qty <= Decimal("0"):
                raise ValueError("开仓数量归一化后为 0，无法单向开仓")

            round_qty, final_round_qty, planned_round_qtys, extension_round_cap_qty = self._build_single_round_plan(
                normalized_target_qty=normalized_open_qty,
                round_count=request.round_count,
                rules=rules,
            )

            available_balance = Decimal(str(account_overview.get("totals", {}).get("available_balance") or "0"))
            has_existing_positions = long_qty > Decimal("0") or short_qty > Decimal("0")
            requested_leverage = int(request.leverage)
            if has_existing_positions:
                effective_leverage = self._position_leverage(account_overview, symbol) or max(
                    int(await self._gateway.get_symbol_leverage(symbol) or 1),
                    1,
                )
                if requested_leverage != effective_leverage:
                    raise ValueError(f"当前交易对已有持仓，杠杆必须与现有持仓一致：{effective_leverage}x")
            else:
                if requested_leverage > rules.max_leverage:
                    raise ValueError(f"Leverage {requested_leverage} exceeds max {rules.max_leverage} for {symbol}")
                effective_leverage = requested_leverage

            quote = await self._gateway.get_quote(symbol)
            _, single_open_price = self._single_open_params(selected_position_side, quote)
            single_open_price = normalize_price(single_open_price, rules)
            try:
                validate_qty_and_notional(round_qty, single_open_price, rules)
                validate_qty_and_notional(final_round_qty, single_open_price, rules)
            except ValueError as exc:
                raise ValueError("每轮开仓数量按当前价格换算后低于交易所最小下单金额，无法单向开仓") from exc

            total_notional = normalized_open_qty * single_open_price
            implied_open_amount = total_notional / Decimal(effective_leverage)
            max_open_amount = available_balance * Decimal("0.95")
            if implied_open_amount > max_open_amount:
                raise ValueError(
                    f"开单金额 {implied_open_amount} 超过当前可用余额 {available_balance} 的 95%，无法单向开仓"
                )
            precheck = await self.precheck_open_request(request, strict_hedge_mode=False)
            if not precheck['ok']:
                raise SessionPrecheckFailed(precheck)
            try:
                await self._confirm_hedge_mode_for_execution()
            except Exception as exc:
                raise SessionPrecheckFailed(self._with_hedge_mode_failure(precheck, str(exc))) from exc

            trend_bias = TrendBias.LONG if selected_position_side == PositionSide.LONG else TrendBias.SHORT
            spec = SessionSpec(
                symbol=symbol,
                trend_bias=trend_bias,
                leverage=effective_leverage,
                round_count=request.round_count,
                round_qty=round_qty,
                poll_interval_ms=request.poll_interval_ms or self._settings.default_poll_interval_ms,
                order_ttl_ms=request.order_ttl_ms or self._settings.default_order_ttl_ms,
                max_zero_fill_retries=request.max_zero_fill_retries or self._settings.default_max_zero_fill_retries,
                market_fallback_attempts=request.market_fallback_attempts or self._settings.default_market_fallback_attempts,
                execution_profile=execution_policy.execution_profile,
                market_fallback_max_ratio=execution_policy.market_fallback_max_ratio,
                market_fallback_min_residual_qty=execution_policy.market_fallback_min_residual_qty,
                max_reprice_ticks=execution_policy.max_reprice_ticks,
                max_spread_bps=execution_policy.max_spread_bps,
                max_reference_deviation_bps=execution_policy.max_reference_deviation_bps,
                round_interval_seconds=request.round_interval_seconds if request.round_interval_seconds is not None else 3,
                created_by=request.created_by,
                session_kind=SessionKind.SINGLE_OPEN,
                open_mode=request.open_mode,
                selected_position_side=selected_position_side,
                target_open_qty=normalized_open_qty,
                planned_round_qtys=planned_round_qtys,
                final_round_qty=final_round_qty,
                extension_round_cap_qty=extension_round_cap_qty,
                max_extension_rounds=DEFAULT_SINGLE_MAX_EXTENSION_ROUNDS,
                max_session_duration_seconds=DEFAULT_SINGLE_MAX_SESSION_DURATION_SECONDS,
            )
            session = OpenSession.create(spec, account_id=self._account_id, account_name=self._account_name)
            self._repository.create_session(session)
            self._repository.add_event(
                session.session_id,
                "single_open_session_created",
                {
                    "session_kind": spec.session_kind.value,
                    "symbol": symbol,
                    "open_mode": spec.open_mode.value if spec.open_mode else None,
                    "selected_position_side": selected_position_side.value,
                    "open_qty": str(normalized_open_qty),
                    "round_count": spec.round_count,
                    "round_qty": str(spec.round_qty),
                    "planned_round_qtys": [str(item) for item in planned_round_qtys],
                    "final_round_qty": str(final_round_qty),
                    "extension_round_cap_qty": str(extension_round_cap_qty),
                    "max_extension_rounds": spec.max_extension_rounds,
                    "max_session_duration_seconds": spec.max_session_duration_seconds,
                    "session_deadline_at": session.session_deadline_at.isoformat() if session.session_deadline_at else None,
                    "round_interval_seconds": spec.round_interval_seconds,
                    "account_id": self._account_id,
                    "account_name": self._account_name,
                    "long_qty": str(long_qty),
                    "short_qty": str(short_qty),
                    "min_notional": str(rules.min_notional),
                    "requested_leverage": requested_leverage,
                    "leverage": effective_leverage,
                    **execution_policy.to_payload(),
                },
            )
        try:
            await self._gateway.ensure_hedge_mode()
            await self._gateway.ensure_cross_margin(symbol)
            await self._gateway.ensure_leverage(symbol, spec.leverage)
        except Exception as exc:
            error = self._normalize_trading_error(exc, source="service", code="single_open_session_preflight_failed", session=session, stage="preflight")
            self._persist_session_error(session.session_id, SessionStatus.EXCEPTION, error)
            self._repository.add_event(
                session.session_id,
                "single_open_session_preflight_failed",
                self._error_event_payload(error, session_kind=session.spec.session_kind.value),
            )
            raise
        return self._launch_session(session)

    async def create_close_session(self, request: CloseSessionRequest) -> OpenSession:
        symbol = request.symbol.upper()
        execution_policy = self._resolved_execution_policy(
            session_kind=SessionKind.PAIRED_CLOSE,
            execution_profile=request.execution_profile,
            market_fallback_max_ratio=request.market_fallback_max_ratio,
            market_fallback_min_residual_qty=request.market_fallback_min_residual_qty,
            max_reprice_ticks=request.max_reprice_ticks,
            max_spread_bps=request.max_spread_bps,
            max_reference_deviation_bps=request.max_reference_deviation_bps,
        )
        async with self._session_creation_lock:
            self._ensure_no_active_symbol_session(symbol)
            rules = await self._gateway.get_symbol_rules(symbol)
            normalized_close_qty = normalize_qty(request.close_qty, rules)
            if normalized_close_qty <= Decimal("0"):
                raise ValueError("平仓数量归一化后为 0，无法平仓")

            account_overview = await self._gateway.get_account_overview()
            long_qty = self._position_qty(account_overview, symbol, PositionSide.LONG)
            short_qty = self._position_qty(account_overview, symbol, PositionSide.SHORT)
            max_closeable_qty = normalize_qty(min(long_qty, short_qty), rules)
            if max_closeable_qty <= Decimal("0"):
                raise ValueError("当前账户不存在可双向平仓的双边持仓")
            if normalized_close_qty > max_closeable_qty:
                raise ValueError(
                    f"平仓数量 {normalized_close_qty} 超过当前可双向平仓数量 {max_closeable_qty}，无法平仓"
                )

            round_qty = normalize_qty(normalized_close_qty / Decimal(request.round_count), rules)
            if round_qty <= Decimal("0"):
                raise ValueError("每轮数量归一化后为 0，无法平仓")

            quote = await self._gateway.get_quote(symbol)
            stage1_price, stage2_price = self._close_stage_prices(request.trend_bias, quote)
            stage1_price = normalize_price(stage1_price, rules)
            stage2_price = normalize_price(stage2_price, rules)
            try:
                validate_qty_and_notional(round_qty, stage1_price, rules)
                validate_qty_and_notional(round_qty, stage2_price, rules)
            except ValueError as exc:
                raise ValueError(
                    f"每轮平仓金额 {self._format_money(round_qty * stage1_price)} 低于交易所最小下单金额 {self._format_money(getattr(rules, 'min_notional', Decimal('0')))}，无法平仓。"
                ) from exc
            precheck = await self.precheck_request(
                SessionPrecheckRequest(
                    session_kind=SessionKind.PAIRED_CLOSE,
                    symbol=request.symbol,
                    trend_bias=request.trend_bias,
                    close_qty=request.close_qty,
                    round_count=request.round_count,
                    execution_profile=request.execution_profile,
                    market_fallback_max_ratio=request.market_fallback_max_ratio,
                    market_fallback_min_residual_qty=request.market_fallback_min_residual_qty,
                    max_reprice_ticks=request.max_reprice_ticks,
                    max_spread_bps=request.max_spread_bps,
                    max_reference_deviation_bps=request.max_reference_deviation_bps,
                ),
                strict_hedge_mode=False,
            )
            if not precheck['ok']:
                raise SessionPrecheckFailed(precheck)
            try:
                await self._confirm_hedge_mode_for_execution()
            except Exception as exc:
                raise SessionPrecheckFailed(self._with_hedge_mode_failure(precheck, str(exc))) from exc

            spec = SessionSpec(
                symbol=symbol,
                trend_bias=request.trend_bias,
                leverage=1,
                round_count=request.round_count,
                round_qty=round_qty,
                poll_interval_ms=request.poll_interval_ms or self._settings.default_poll_interval_ms,
                order_ttl_ms=request.order_ttl_ms or self._settings.default_order_ttl_ms,
                max_zero_fill_retries=request.max_zero_fill_retries or self._settings.default_max_zero_fill_retries,
                market_fallback_attempts=request.market_fallback_attempts or self._settings.default_market_fallback_attempts,
                execution_profile=execution_policy.execution_profile,
                market_fallback_max_ratio=execution_policy.market_fallback_max_ratio,
                market_fallback_min_residual_qty=execution_policy.market_fallback_min_residual_qty,
                max_reprice_ticks=execution_policy.max_reprice_ticks,
                max_spread_bps=execution_policy.max_spread_bps,
                max_reference_deviation_bps=execution_policy.max_reference_deviation_bps,
                round_interval_seconds=request.round_interval_seconds if request.round_interval_seconds is not None else 3,
                created_by=request.created_by,
                session_kind=SessionKind.PAIRED_CLOSE,
                target_close_qty=normalized_close_qty,
            )
            session = OpenSession.create(spec, account_id=self._account_id, account_name=self._account_name)
            session.final_alignment_status = FinalAlignmentStatus.NOT_NEEDED
            self._repository.create_session(session)
            self._repository.add_event(
                session.session_id,
                "close_session_created",
                {
                    "session_kind": spec.session_kind.value,
                    "symbol": symbol,
                    "trend_bias": spec.trend_bias.value,
                    "close_qty": str(normalized_close_qty),
                    "round_count": spec.round_count,
                    "round_qty": str(spec.round_qty),
                    "round_interval_seconds": spec.round_interval_seconds,
                    "account_id": self._account_id,
                    "account_name": self._account_name,
                    "max_closeable_qty": str(max_closeable_qty),
                    "min_notional": str(rules.min_notional),
                    "stage1_price": str(stage1_price),
                    "stage2_price": str(stage2_price),
                    **execution_policy.to_payload(),
                },
            )
        try:
            await self._gateway.ensure_hedge_mode()
        except Exception as exc:
            error = self._normalize_trading_error(exc, source="service", code="close_session_preflight_failed", session=session, stage="preflight")
            self._persist_session_error(session.session_id, SessionStatus.EXCEPTION, error)
            self._repository.add_event(
                session.session_id,
                "close_session_preflight_failed",
                self._error_event_payload(error, session_kind=session.spec.session_kind.value),
            )
            raise
        return self._launch_session(session)

    async def create_single_close_session(self, request: SingleCloseSessionRequest) -> OpenSession:
        symbol = request.symbol.upper()
        execution_policy = self._resolved_execution_policy(
            session_kind=SessionKind.SINGLE_CLOSE,
            execution_profile=request.execution_profile,
            market_fallback_max_ratio=request.market_fallback_max_ratio,
            market_fallback_min_residual_qty=request.market_fallback_min_residual_qty,
            max_reprice_ticks=request.max_reprice_ticks,
            max_spread_bps=request.max_spread_bps,
            max_reference_deviation_bps=request.max_reference_deviation_bps,
        )
        async with self._session_creation_lock:
            self._ensure_no_active_symbol_session(symbol)
            rules = await self._gateway.get_symbol_rules(symbol)
            account_overview = await self._gateway.get_account_overview()
            long_qty = normalize_qty(self._position_qty(account_overview, symbol, PositionSide.LONG), rules)
            short_qty = normalize_qty(self._position_qty(account_overview, symbol, PositionSide.SHORT), rules)

            if request.close_mode == SingleCloseMode.ALIGN:
                if long_qty == short_qty:
                    raise ValueError("当前交易对不存在持仓" if long_qty <= Decimal("0") else "当前双边持仓数量已对齐，无需单向平仓")
                selected_position_side = PositionSide.LONG if long_qty > short_qty else PositionSide.SHORT
                normalized_close_qty = normalize_qty(abs(long_qty - short_qty), rules)
            else:
                if request.selected_position_side is None:
                    raise ValueError("常规平仓需要先选择平仓订单")
                selected_position_side = request.selected_position_side
                available_qty = long_qty if selected_position_side == PositionSide.LONG else short_qty
                if available_qty <= Decimal("0"):
                    raise ValueError("当前交易对不存在持仓")
                normalized_close_qty = normalize_qty(request.close_qty, rules)
                if normalized_close_qty > available_qty:
                    raise ValueError(
                        f"平仓数量 {normalized_close_qty} 超过所选持仓数量 {available_qty}，无法平仓"
                    )

            if normalized_close_qty <= Decimal("0"):
                raise ValueError("平仓数量归一化后为 0，无法单向平仓")

            round_qty, final_round_qty, planned_round_qtys, extension_round_cap_qty = self._build_single_round_plan(
                normalized_target_qty=normalized_close_qty,
                round_count=request.round_count,
                rules=rules,
            )

            quote = await self._gateway.get_quote(symbol)
            single_close_side, single_close_price = self._single_close_params(selected_position_side, quote)
            single_close_price = normalize_price(single_close_price, rules)
            try:
                validate_qty_and_notional(round_qty, single_close_price, rules)
                validate_qty_and_notional(final_round_qty, single_close_price, rules)
            except ValueError as exc:
                raise ValueError(
                    f"每轮平仓金额 {self._format_money(round_qty * single_close_price)} 低于交易所最小下单金额 {self._format_money(getattr(rules, 'min_notional', Decimal('0')))}，无法平仓。"
                ) from exc
            precheck = await self.precheck_request(
                SessionPrecheckRequest(
                    session_kind=SessionKind.SINGLE_CLOSE,
                    symbol=request.symbol,
                    close_qty=request.close_qty,
                    round_count=request.round_count,
                    selected_position_side=request.selected_position_side,
                    close_mode=request.close_mode,
                    execution_profile=request.execution_profile,
                    market_fallback_max_ratio=request.market_fallback_max_ratio,
                    market_fallback_min_residual_qty=request.market_fallback_min_residual_qty,
                    max_reprice_ticks=request.max_reprice_ticks,
                    max_spread_bps=request.max_spread_bps,
                    max_reference_deviation_bps=request.max_reference_deviation_bps,
                ),
                strict_hedge_mode=False,
            )
            if not precheck['ok']:
                raise SessionPrecheckFailed(precheck)
            try:
                await self._confirm_hedge_mode_for_execution()
            except Exception as exc:
                raise SessionPrecheckFailed(self._with_hedge_mode_failure(precheck, str(exc))) from exc

            trend_bias = TrendBias.LONG if selected_position_side == PositionSide.LONG else TrendBias.SHORT
            spec = SessionSpec(
                symbol=symbol,
                trend_bias=trend_bias,
                leverage=1,
                round_count=request.round_count,
                round_qty=round_qty,
                poll_interval_ms=request.poll_interval_ms or self._settings.default_poll_interval_ms,
                order_ttl_ms=request.order_ttl_ms or self._settings.default_order_ttl_ms,
                max_zero_fill_retries=request.max_zero_fill_retries or self._settings.default_max_zero_fill_retries,
                market_fallback_attempts=request.market_fallback_attempts or self._settings.default_market_fallback_attempts,
                execution_profile=execution_policy.execution_profile,
                market_fallback_max_ratio=execution_policy.market_fallback_max_ratio,
                market_fallback_min_residual_qty=execution_policy.market_fallback_min_residual_qty,
                max_reprice_ticks=execution_policy.max_reprice_ticks,
                max_spread_bps=execution_policy.max_spread_bps,
                max_reference_deviation_bps=execution_policy.max_reference_deviation_bps,
                round_interval_seconds=request.round_interval_seconds if request.round_interval_seconds is not None else 3,
                created_by=request.created_by,
                session_kind=SessionKind.SINGLE_CLOSE,
                close_mode=request.close_mode,
                selected_position_side=selected_position_side,
                target_close_qty=normalized_close_qty,
                planned_round_qtys=planned_round_qtys,
                final_round_qty=final_round_qty,
                extension_round_cap_qty=extension_round_cap_qty,
                max_extension_rounds=DEFAULT_SINGLE_MAX_EXTENSION_ROUNDS,
                max_session_duration_seconds=DEFAULT_SINGLE_MAX_SESSION_DURATION_SECONDS,
            )
            session = OpenSession.create(spec, account_id=self._account_id, account_name=self._account_name)
            self._repository.create_session(session)
            self._repository.add_event(
                session.session_id,
                "single_close_session_created",
                {
                    "session_kind": spec.session_kind.value,
                    "symbol": symbol,
                    "close_mode": spec.close_mode.value if spec.close_mode else None,
                    "selected_position_side": selected_position_side.value,
                    "close_qty": str(normalized_close_qty),
                    "round_count": spec.round_count,
                    "round_qty": str(spec.round_qty),
                    "planned_round_qtys": [str(item) for item in planned_round_qtys],
                    "final_round_qty": str(final_round_qty),
                    "extension_round_cap_qty": str(extension_round_cap_qty),
                    "max_extension_rounds": spec.max_extension_rounds,
                    "max_session_duration_seconds": spec.max_session_duration_seconds,
                    "session_deadline_at": session.session_deadline_at.isoformat() if session.session_deadline_at else None,
                    "round_interval_seconds": spec.round_interval_seconds,
                    "account_id": self._account_id,
                    "account_name": self._account_name,
                    "long_qty": str(long_qty),
                    "short_qty": str(short_qty),
                    "min_notional": str(rules.min_notional),
                    **execution_policy.to_payload(),
                },
            )
        try:
            await self._gateway.ensure_hedge_mode()
        except Exception as exc:
            error = self._normalize_trading_error(exc, source="service", code="single_close_session_preflight_failed", session=session, stage="preflight")
            self._persist_session_error(session.session_id, SessionStatus.EXCEPTION, error)
            self._repository.add_event(
                session.session_id,
                "single_close_session_preflight_failed",
                self._error_event_payload(error, session_kind=session.spec.session_kind.value),
            )
            raise
        return self._launch_session(session)

    def _launch_session(self, session: OpenSession) -> OpenSession:
        control = SessionControl()
        task = asyncio.create_task(self._run_session(session, control))
        self._managed[session.session_id] = ManagedSession(symbol=session.spec.symbol, control=control, task=task)
        return session

    async def _run_session(self, session: OpenSession, control: SessionControl) -> None:
        engine, completed_event, failed_event = self._engine_for_session(session.spec.session_kind)
        try:
            self._repository.update_session_status(session.session_id, SessionStatus.RUNNING)
            completed_rounds, skipped_rounds = await engine.execute_session(session, control)
            self._repository.update_session_runtime(session)
            final_status = (
                SessionStatus.COMPLETED_WITH_SKIPS
                if skipped_rounds > 0 or session.final_unaligned_qty > Decimal("0")
                else SessionStatus.COMPLETED
            )
            self._repository.update_session_status(session.session_id, final_status)
            self._repository.add_event(
                session.session_id,
                completed_event,
                {
                    "session_kind": session.spec.session_kind.value,
                    "completed_rounds": completed_rounds,
                    "skipped_rounds": skipped_rounds,
                    "planned_round_qtys": [str(item) for item in session.spec.planned_round_qtys],
                    "extension_rounds_used": session.extension_rounds_used,
                    "remaining_extension_rounds": session.remaining_extension_rounds,
                    "stage2_carryover_qty": str(session.stage2_carryover_qty),
                    "final_alignment_status": session.final_alignment_status.value,
                    "final_unaligned_qty": str(session.final_unaligned_qty),
                    "completed_with_final_alignment": session.completed_with_final_alignment,
                    "session_deadline_at": session.session_deadline_at.isoformat() if session.session_deadline_at else None,
                    "stop_reason": session.stop_reason.value if session.stop_reason else None,
                    "residual_source": session.residual_source,
                },
            )
        except Exception as exc:
            status = SessionStatus.ABORTED if control.aborted else SessionStatus.EXCEPTION
            self._repository.update_session_runtime(session)
            if status == SessionStatus.ABORTED and not isinstance(exc, TradingError):
                self._repository.update_session_status(session.session_id, status, last_error=str(exc))
                self._repository.add_event(
                    session.session_id,
                    failed_event,
                    {"session_kind": session.spec.session_kind.value, "error": str(exc), "status": status.value},
                )
            else:
                error = self._normalize_trading_error(exc, source="service", code="session_failed", session=session, stage="session_run")
                self._persist_session_error(session.session_id, status, error)
                self._repository.add_event(
                    session.session_id,
                    failed_event,
                    self._error_event_payload(error, session_kind=session.spec.session_kind.value, status=status.value),
                )
        finally:
            self._managed.pop(session.session_id, None)
    def get_session(self, session_id: str) -> dict:
        session = self._repository.get_session(session_id, self._account_id)
        if session is None:
            raise KeyError(session_id)
        return session

    def get_session_updates(self, session_id: str, after_event_id: int = 0) -> dict[str, Any]:
        session = self._repository.get_session_record(session_id, self._account_id)
        if session is None:
            raise KeyError(session_id)
        events = self._repository.list_events(session_id, after_event_id=max(int(after_event_id), 0))
        changed_round_indexes = {
            int(event['round_index'])
            for event in events
            if event.get('round_index') is not None
        }
        return {
            'session': session,
            'changed_rounds': self._repository.list_rounds_by_indexes(session_id, changed_round_indexes),
            'events': events,
            'latest_event_id': self._repository.latest_event_id(session_id),
        }

    def list_sessions(self) -> list[dict]:
        return self._repository.list_sessions(self._account_id)

    async def pause_session(self, session_id: str) -> SessionStatus:
        managed = self._managed.get(session_id)
        if managed is None:
            raise KeyError(session_id)
        managed.control.paused = True
        self._repository.update_session_status(session_id, SessionStatus.PAUSED)
        self._repository.add_event(session_id, "session_paused", {})
        return SessionStatus.PAUSED

    async def resume_session(self, session_id: str) -> SessionStatus:
        managed = self._managed.get(session_id)
        if managed is not None:
            managed.control.paused = False
            self._repository.update_session_status(session_id, SessionStatus.RUNNING)
            self._repository.add_event(session_id, "session_resumed", {})
            return SessionStatus.RUNNING

        payload = self._repository.get_session(session_id, self._account_id)
        if payload is None:
            raise KeyError(session_id)
        recovery_status = self._enum_value(payload.get("recovery_status"), RecoveryStatus)
        if self._enum_value(payload.get("status"), SessionStatus) != SessionStatus.EXCEPTION:
            raise ExchangeStateError("当前会话不处于可恢复状态。", code="session_resume_invalid_status")
        if recovery_status == RecoveryStatus.MANUAL_CONFIRMATION:
            raise ExchangeStateError("当前会话需人工确认后才能恢复。", code="session_resume_manual_confirmation")
        if recovery_status != RecoveryStatus.RECOVERABLE:
            raise ExchangeStateError("当前会话已判定为不可恢复。", code="session_resume_non_recoverable")

        session = self._build_session_from_payload(payload)
        self._ensure_no_active_symbol_session(session.spec.symbol)
        session.status = SessionStatus.RUNNING
        session.recovery_status = None
        session.recovery_summary = None
        session.recovery_checked_at = None
        session.recovery_details = {}
        self._repository.update_session_status(session_id, SessionStatus.RUNNING, clear_recovery=True)
        self._repository.add_event(session_id, "session_resumed", {"recovered_from_restart": True})
        self._launch_session(session)
        return SessionStatus.RUNNING

    async def abort_session(self, session_id: str) -> SessionStatus:
        managed = self._managed.get(session_id)
        if managed is None:
            raise KeyError(session_id)
        managed.control.paused = False
        managed.control.aborted = True
        self._repository.add_event(session_id, "session_abort_requested", {"status": "abort_requested"})
        payload = self._repository.get_session(session_id, self._account_id)
        current_status = SessionStatus(payload["status"]) if payload is not None else SessionStatus.RUNNING
        return current_status

    async def get_symbol_info(self, symbol: str) -> dict:
        normalized_symbol = symbol.upper()
        rules = await self._gateway.get_symbol_rules(normalized_symbol)
        current_leverage = max(int(await self._gateway.get_symbol_leverage(normalized_symbol) or 1), 1)
        return {
            "symbol": normalized_symbol,
            "allowed": normalized_symbol in self._settings.normalized_whitelist,
            "max_leverage": rules.max_leverage,
            "current_leverage": current_leverage,
            "min_qty": rules.min_qty,
            "step_size": rules.step_size,
            "tick_size": rules.tick_size,
            "min_notional": rules.min_notional,
        }
    def get_whitelist(self) -> list[str]:
        return sorted(self._settings.normalized_whitelist)

    async def update_whitelist(self, symbols: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for symbol in symbols:
            candidate = symbol.strip().upper()
            if not candidate or candidate in seen:
                continue
            await self._gateway.get_symbol_rules(candidate)
            normalized.append(candidate)
            seen.add(candidate)
        if not normalized:
            raise ValueError(format_copy("reasons.whitelist_empty"))
        return self._settings.persist_whitelist(normalized)

    def has_active_sessions(self) -> bool:
        if self._repository.has_active_sessions(self._account_id):
            return True
        return any(not managed.task.done() for managed in self._managed.values())

    def current_account(self) -> dict[str, str]:
        return {"id": self._account_id, "name": self._account_name}

    def _ensure_no_active_symbol_session(self, symbol: str) -> None:
        if self._repository.has_active_symbol_session(symbol, self._account_id) or self._has_managed_symbol_session(symbol):
            raise SessionConflictError(f"Symbol {symbol} already has an active session")

    def _engine_for_session(self, session_kind: SessionKind):
        if session_kind == SessionKind.PAIRED_CLOSE:
            return self._close_engine, "close_session_completed", "close_session_failed"
        if session_kind == SessionKind.SINGLE_OPEN:
            return self._single_open_engine, "single_open_session_completed", "single_open_session_failed"
        if session_kind == SessionKind.SINGLE_CLOSE:
            return self._single_close_engine, "single_close_session_completed", "single_close_session_failed"
        return self._engine, "session_completed", "session_failed"

    def _open_stage1_price(self, trend_bias: TrendBias, quote) -> Decimal:
        if trend_bias == TrendBias.LONG:
            return quote.bid_price
        return quote.ask_price

    def _close_stage_prices(self, trend_bias: TrendBias, quote) -> tuple[Decimal, Decimal]:
        if trend_bias == TrendBias.LONG:
            return quote.ask_price, quote.bid_price
        return quote.bid_price, quote.ask_price

    def _single_close_params(self, position_side: PositionSide, quote) -> tuple[object, Decimal]:
        if position_side == PositionSide.LONG:
            from paired_opener.domain import OrderSide
            return OrderSide.SELL, quote.bid_price
        from paired_opener.domain import OrderSide
        return OrderSide.BUY, quote.ask_price

    def _single_open_params(self, position_side: PositionSide, quote) -> tuple[object, Decimal]:
        if position_side == PositionSide.LONG:
            from paired_opener.domain import OrderSide
            return OrderSide.BUY, quote.bid_price
        from paired_opener.domain import OrderSide
        return OrderSide.SELL, quote.ask_price

    def _position_qty(self, overview: dict, symbol: str, position_side: PositionSide) -> Decimal:
        for item in overview.get("positions", []):
            if item.get("symbol") != symbol:
                continue
            if str(item.get("position_side")) != position_side.value:
                continue
            return Decimal(str(item.get("qty") or "0"))
        return Decimal("0")

    def _position_leverage(self, overview: dict, symbol: str) -> int | None:
        leverage_values: list[int] = []
        for item in overview.get("positions", []):
            if item.get("symbol") != symbol:
                continue
            qty = Decimal(str(item.get("qty") or "0"))
            if qty <= Decimal("0"):
                continue
            leverage = int(item.get("leverage") or 0)
            if leverage > 0:
                leverage_values.append(leverage)
        return max(leverage_values) if leverage_values else None

    def _has_managed_symbol_session(self, symbol: str) -> bool:
        stale: list[str] = []
        for session_id, managed in self._managed.items():
            if managed.task.done():
                stale.append(session_id)
                continue
            if managed.symbol == symbol:
                return True
        for session_id in stale:
            self._managed.pop(session_id, None)
        return False




