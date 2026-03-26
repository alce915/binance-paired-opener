from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal

from paired_opener.config import Settings
from paired_opener.domain import (
    FinalAlignmentStatus,
    OpenSession,
    PositionSide,
    SessionConflictError,
    SessionKind,
    SessionSpec,
    SessionStatus,
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
from paired_opener.exchange import ExchangeGateway
from paired_opener.rounding import normalize_price, normalize_qty, validate_qty_and_notional
from paired_opener.schemas import (
    CloseSessionRequest,
    OpenSessionRequest,
    SingleCloseSessionRequest,
    SingleOpenSessionRequest,
)
from paired_opener.storage import SqliteRepository


@dataclass(slots=True)
class ManagedSession:
    symbol: str
    control: SessionControl
    task: asyncio.Task[None]


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
        account_name: str = "默认账户",
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
        for session_id in self._repository.fail_incomplete_sessions("Service restarted before session completion"):
            self._repository.add_event(
                session_id,
                "session_recovered_on_startup",
                {"error": "Service restarted before session completion", "status": SessionStatus.EXCEPTION.value},
            )


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

    async def create_session(self, request: OpenSessionRequest) -> OpenSession:
        return await self.create_open_session(request)

    async def create_open_session(self, request: OpenSessionRequest) -> OpenSession:
        symbol = request.symbol.upper()
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
                },
            )
        try:
            await self._gateway.ensure_hedge_mode()
            await self._gateway.ensure_cross_margin(symbol)
            await self._gateway.ensure_leverage(symbol, spec.leverage)
        except Exception as exc:
            self._repository.update_session_status(session.session_id, SessionStatus.EXCEPTION, last_error=str(exc))
            self._repository.add_event(session.session_id, "session_preflight_failed", {"error": str(exc)})
            raise
        return self._launch_session(session)

    async def create_single_open_session(self, request: SingleOpenSessionRequest) -> OpenSession:
        symbol = request.symbol.upper()
        async with self._session_creation_lock:
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

            round_qty = normalize_qty(normalized_open_qty / Decimal(request.round_count), rules)
            if round_qty <= Decimal("0"):
                raise ValueError("每轮数量归一化后为 0，无法单向开仓")

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
            final_round_qty = normalize_qty(
                normalized_open_qty - (round_qty * Decimal(max(request.round_count - 1, 0))),
                rules,
            )
            if final_round_qty <= Decimal("0"):
                raise ValueError("最后一轮数量归一化后为 0，无法单向开仓")
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
                round_interval_seconds=request.round_interval_seconds if request.round_interval_seconds is not None else 3,
                created_by=request.created_by,
                session_kind=SessionKind.SINGLE_OPEN,
                open_mode=request.open_mode,
                selected_position_side=selected_position_side,
                target_open_qty=normalized_open_qty,
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
                    "round_interval_seconds": spec.round_interval_seconds,
                    "account_id": self._account_id,
                    "account_name": self._account_name,
                    "long_qty": str(long_qty),
                    "short_qty": str(short_qty),
                    "min_notional": str(rules.min_notional),
                    "requested_leverage": requested_leverage,
                    "leverage": effective_leverage,
                },
            )
        try:
            await self._gateway.ensure_hedge_mode()
            await self._gateway.ensure_cross_margin(symbol)
            await self._gateway.ensure_leverage(symbol, spec.leverage)
        except Exception as exc:
            self._repository.update_session_status(session.session_id, SessionStatus.EXCEPTION, last_error=str(exc))
            self._repository.add_event(session.session_id, "single_open_session_preflight_failed", {"error": str(exc)})
            raise
        return self._launch_session(session)
    async def create_close_session(self, request: CloseSessionRequest) -> OpenSession:
        symbol = request.symbol.upper()
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
                raise ValueError("每轮平仓数量按当前价格换算后低于交易所最小下单金额，无法平仓") from exc

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
                },
            )
        try:
            await self._gateway.ensure_hedge_mode()
        except Exception as exc:
            self._repository.update_session_status(session.session_id, SessionStatus.EXCEPTION, last_error=str(exc))
            self._repository.add_event(session.session_id, "close_session_preflight_failed", {"error": str(exc)})
            raise
        return self._launch_session(session)

    async def create_single_close_session(self, request: SingleCloseSessionRequest) -> OpenSession:
        symbol = request.symbol.upper()
        async with self._session_creation_lock:
            self._ensure_no_active_symbol_session(symbol)
            rules = await self._gateway.get_symbol_rules(symbol)
            account_overview = await self._gateway.get_account_overview()
            long_qty = normalize_qty(self._position_qty(account_overview, symbol, PositionSide.LONG), rules)
            short_qty = normalize_qty(self._position_qty(account_overview, symbol, PositionSide.SHORT), rules)

            if request.close_mode == SingleCloseMode.ALIGN:
                if long_qty == short_qty:
                    raise ValueError("当前双边持仓数量已对齐，无需单向平仓")
                selected_position_side = PositionSide.LONG if long_qty > short_qty else PositionSide.SHORT
                normalized_close_qty = normalize_qty(abs(long_qty - short_qty), rules)
            else:
                if request.selected_position_side is None:
                    raise ValueError("常规平仓需要先选择平仓订单")
                selected_position_side = request.selected_position_side
                available_qty = long_qty if selected_position_side == PositionSide.LONG else short_qty
                if available_qty <= Decimal("0"):
                    raise ValueError(f"当前交易对没有 {selected_position_side.value} 持仓可用于单向平仓")
                normalized_close_qty = normalize_qty(request.close_qty, rules)
                if normalized_close_qty > available_qty:
                    raise ValueError(
                        f"平仓数量 {normalized_close_qty} 超过所选持仓数量 {available_qty}，无法平仓"
                    )

            if normalized_close_qty <= Decimal("0"):
                raise ValueError("平仓数量归一化后为 0，无法单向平仓")

            round_qty = normalize_qty(normalized_close_qty / Decimal(request.round_count), rules)
            if round_qty <= Decimal("0"):
                raise ValueError("每轮数量归一化后为 0，无法单向平仓")

            quote = await self._gateway.get_quote(symbol)
            single_close_side, single_close_price = self._single_close_params(selected_position_side, quote)
            single_close_price = normalize_price(single_close_price, rules)
            final_round_qty = normalize_qty(
                normalized_close_qty - (round_qty * Decimal(max(request.round_count - 1, 0))),
                rules,
            )
            if final_round_qty <= Decimal("0"):
                raise ValueError("最后一轮数量归一化后为 0，无法单向平仓")
            try:
                validate_qty_and_notional(round_qty, single_close_price, rules)
                validate_qty_and_notional(final_round_qty, single_close_price, rules)
            except ValueError as exc:
                raise ValueError("每轮平仓数量按当前价格换算后低于交易所最小下单金额，无法单向平仓") from exc

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
                round_interval_seconds=request.round_interval_seconds if request.round_interval_seconds is not None else 3,
                created_by=request.created_by,
                session_kind=SessionKind.SINGLE_CLOSE,
                close_mode=request.close_mode,
                selected_position_side=selected_position_side,
                target_close_qty=normalized_close_qty,
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
                    "round_interval_seconds": spec.round_interval_seconds,
                    "account_id": self._account_id,
                    "account_name": self._account_name,
                    "long_qty": str(long_qty),
                    "short_qty": str(short_qty),
                    "min_notional": str(rules.min_notional),
                },
            )
        try:
            await self._gateway.ensure_hedge_mode()
        except Exception as exc:
            self._repository.update_session_status(session.session_id, SessionStatus.EXCEPTION, last_error=str(exc))
            self._repository.add_event(session.session_id, "single_close_session_preflight_failed", {"error": str(exc)})
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
            final_status = SessionStatus.COMPLETED_WITH_SKIPS if skipped_rounds > 0 else SessionStatus.COMPLETED
            self._repository.update_session_status(session.session_id, final_status)
            self._repository.add_event(
                session.session_id,
                completed_event,
                {
                    "session_kind": session.spec.session_kind.value,
                    "completed_rounds": completed_rounds,
                    "skipped_rounds": skipped_rounds,
                    "stage2_carryover_qty": str(session.stage2_carryover_qty),
                    "final_alignment_status": session.final_alignment_status.value,
                    "final_unaligned_qty": str(session.final_unaligned_qty),
                    "completed_with_final_alignment": session.completed_with_final_alignment,
                },
            )
        except Exception as exc:
            status = SessionStatus.ABORTED if control.aborted else SessionStatus.EXCEPTION
            self._repository.update_session_runtime(session)
            self._repository.update_session_status(session.session_id, status, last_error=str(exc))
            self._repository.add_event(
                session.session_id,
                failed_event,
                {"session_kind": session.spec.session_kind.value, "error": str(exc), "status": status.value},
            )
        finally:
            self._managed.pop(session.session_id, None)

    def get_session(self, session_id: str) -> dict:
        session = self._repository.get_session(session_id, self._account_id)
        if session is None:
            raise KeyError(session_id)
        return session

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
        if managed is None:
            raise KeyError(session_id)
        managed.control.paused = False
        self._repository.update_session_status(session_id, SessionStatus.RUNNING)
        self._repository.add_event(session_id, "session_resumed", {})
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
            raise ValueError("Whitelist cannot be empty")
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



