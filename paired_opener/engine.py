from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Awaitable, Callable

from paired_opener.domain import (
    ExchangeOrder,
    ExchangeOrderStatus,
    ExchangeStateError,
    FinalAlignmentStatus,
    OpenSession,
    OrderSide,
    PollObservation,
    PositionSide,
    Quote,
    RoundExecution,
    RoundStatus,
    SessionAbortedError,
    SessionSpec,
    SymbolRules,
    TrendBias,
)
from paired_opener.exchange import ExchangeGateway
from paired_opener.rounding import min_qty_for_notional, normalize_price, normalize_qty, validate_qty_and_notional
from paired_opener.storage import SqliteRepository


SleepCallable = Callable[[float], Awaitable[None]]


@dataclass(slots=True)
class SessionControl:
    paused: bool = False
    aborted: bool = False


@dataclass(slots=True)
class _StageResult:
    filled_qty: Decimal
    remaining_qty: Decimal
    zero_fill_retries: int
    market_fallback_used: bool


class PairedOpeningEngine:
    def __init__(
        self,
        gateway: ExchangeGateway,
        repository: SqliteRepository,
        *,
        sleep_func: SleepCallable | None = None,
        monotonic_func: Callable[[], float] | None = None,
    ) -> None:
        self._gateway = gateway
        self._repository = repository
        self._sleep = sleep_func or asyncio.sleep
        self._monotonic = monotonic_func or time.monotonic

    async def execute_session(self, session: OpenSession, control: SessionControl) -> tuple[int, int]:
        completed_rounds = 0
        skipped_rounds = 0
        for round_index in range(1, session.spec.round_count + 1):
            await self._respect_control(control)
            execution = await self.execute_round(session=session, round_index=round_index, control=control)
            self._repository.update_session_runtime(session)
            if execution.status == RoundStatus.STAGE1_SKIPPED:
                skipped_rounds += 1
            else:
                completed_rounds += 1
            if control.aborted:
                raise SessionAbortedError("Session aborted by operator after safe checkpoint")
            if round_index < session.spec.round_count and session.spec.round_interval_seconds > 0:
                self._repository.add_event(
                    session.session_id,
                    "round_interval_wait",
                    {
                        "round_index": round_index,
                        "wait_seconds": session.spec.round_interval_seconds,
                    },
                    round_index=round_index,
                )
                await self._sleep_with_control(session.spec.round_interval_seconds, control)
        if session.stage2_carryover_qty > Decimal("0"):
            await self._run_final_alignment(session, control)
            self._repository.update_session_runtime(session)
            if control.aborted:
                raise SessionAbortedError("Session aborted by operator after final alignment")
        return completed_rounds, skipped_rounds

    async def execute_round(
        self,
        *,
        session: OpenSession,
        round_index: int,
        control: SessionControl,
    ) -> RoundExecution:
        spec = session.spec
        execution = RoundExecution(session_id=session.session_id, round_index=round_index, status=RoundStatus.STAGE1_PENDING)
        execution.notes["stage2_carryover_in"] = str(session.stage2_carryover_qty)
        self._repository.upsert_round(execution)
        self._repository.add_event(session.session_id, "round_started", {"round_index": round_index}, round_index=round_index)

        stage1_side, stage1_position_side, stage1_selector = self._stage1_params(spec.trend_bias)
        stage1_result = await self._run_limit_stage(
            session=session,
            execution=execution,
            control=control,
            label="stage1",
            round_index=round_index,
            side=stage1_side,
            position_side=stage1_position_side,
            qty=spec.round_qty,
            price_selector=stage1_selector,
            max_zero_fill_retries=spec.max_zero_fill_retries,
        )
        execution.stage1_zero_fill_retries = stage1_result.zero_fill_retries
        execution.stage1_filled_qty = stage1_result.filled_qty

        if stage1_result.filled_qty <= Decimal("0"):
            execution.status = RoundStatus.STAGE1_SKIPPED
            execution.notes["skip_reason"] = "stage1_zero_fill_retries_exhausted"
            execution.ended_at = datetime.now(UTC)
            session.final_unaligned_qty = session.stage2_carryover_qty
            if session.stage2_carryover_qty > Decimal("0"):
                session.final_alignment_status = FinalAlignmentStatus.CARRYOVER_PENDING
            self._repository.upsert_round(execution)
            self._repository.add_event(session.session_id, "round_skipped", {"round_index": round_index}, round_index=round_index)
            return execution

        stage2_target_qty = stage1_result.filled_qty + session.stage2_carryover_qty
        execution.notes["stage2_target_qty"] = str(stage2_target_qty)
        stage2_side, stage2_position_side, stage2_selector = self._stage2_params(spec.trend_bias)
        execution.status = RoundStatus.STAGE2_PENDING
        self._repository.upsert_round(execution)
        stage2_result = await self._run_limit_stage(
            session=session,
            execution=execution,
            control=control,
            label="stage2",
            round_index=round_index,
            side=stage2_side,
            position_side=stage2_position_side,
            qty=stage2_target_qty,
            price_selector=stage2_selector,
            max_zero_fill_retries=spec.max_zero_fill_retries,
            market_fallback_attempts=spec.market_fallback_attempts,
        )
        execution.stage2_zero_fill_retries = stage2_result.zero_fill_retries
        execution.stage2_filled_qty = stage2_result.filled_qty
        execution.market_fallback_used = stage2_result.market_fallback_used
        execution.status = RoundStatus.ROUND_COMPLETED
        execution.notes["stage2_remaining_qty"] = str(stage2_result.remaining_qty)
        session.stage2_carryover_qty = stage2_result.remaining_qty
        session.final_unaligned_qty = stage2_result.remaining_qty
        session.final_alignment_status = (
            FinalAlignmentStatus.CARRYOVER_PENDING if stage2_result.remaining_qty > Decimal("0") else FinalAlignmentStatus.NOT_NEEDED
        )
        if stage2_result.remaining_qty > Decimal("0"):
            self._repository.add_event(
                session.session_id,
                "stage2_carryover_persisted",
                {
                    "round_index": round_index,
                    "carryover_qty": str(stage2_result.remaining_qty),
                },
                round_index=round_index,
            )
        execution.ended_at = datetime.now(UTC)
        self._repository.upsert_round(execution)
        self._repository.add_event(
            session.session_id,
            "round_completed",
            {
                "round_index": round_index,
                "stage1_filled_qty": str(execution.stage1_filled_qty),
                "stage2_filled_qty": str(execution.stage2_filled_qty),
                "stage2_remaining_qty": str(stage2_result.remaining_qty),
                "market_fallback_used": execution.market_fallback_used,
            },
            round_index=round_index,
        )
        return execution

    async def _run_limit_stage(
        self,
        *,
        session: OpenSession,
        execution: RoundExecution,
        control: SessionControl,
        label: str,
        round_index: int,
        side: OrderSide,
        position_side: PositionSide,
        qty: Decimal,
        price_selector: Callable[[Quote], Decimal],
        max_zero_fill_retries: int,
        market_fallback_attempts: int = 0,
    ) -> _StageResult:
        spec = session.spec
        rules = await self._gateway.get_symbol_rules(spec.symbol)
        remaining_qty = normalize_qty(qty, rules)
        total_filled = Decimal("0")
        zero_fill_retries = 0
        market_fallback_used = False

        while remaining_qty > Decimal("0"):
            await self._respect_control(control)
            quote = await self._gateway.get_quote(spec.symbol)
            price = normalize_price(price_selector(quote), rules)
            try:
                validate_qty_and_notional(remaining_qty, price, rules)
            except ValueError as exc:
                if label != "stage2":
                    raise
                self._repository.add_event(
                    session.session_id,
                    "stage2_below_min_carryover",
                    {
                        "round_index": round_index,
                        "remaining_qty": str(remaining_qty),
                        "price": str(price),
                        "error": str(exc),
                    },
                    round_index=round_index,
                )
                return _StageResult(
                    filled_qty=total_filled,
                    remaining_qty=remaining_qty,
                    zero_fill_retries=zero_fill_retries,
                    market_fallback_used=market_fallback_used,
                )
            order = await self._gateway.place_limit_order(
                symbol=spec.symbol,
                side=side,
                position_side=position_side,
                qty=remaining_qty,
                price=price,
                client_order_id=f"{session.session_id}-{round_index}-{label}-{zero_fill_retries + 1}",
            )
            self._repository.add_event(
                session.session_id,
                f"{label}_order_placed",
                {
                    "round_index": round_index,
                    "order_id": order.order_id,
                    "qty": str(remaining_qty),
                    "price": str(price),
                    "side": side.value,
                    "position_side": position_side.value,
                },
                round_index=round_index,
            )
            observation = await self._observe_order(spec, order, control=control)
            if observation.order.executed_qty > Decimal("0"):
                cancel_state = await self._cancel_if_open(observation.order)
                filled_qty = max(observation.order.executed_qty, cancel_state.executed_qty)
                total_filled += filled_qty
                remaining_qty = normalize_qty(remaining_qty - filled_qty, rules)
                zero_fill_retries = 0
                self._repository.add_event(
                    session.session_id,
                    f"{label}_fill",
                    {
                        "round_index": round_index,
                        "order_id": order.order_id,
                        "filled_qty": str(filled_qty),
                        "remaining_qty": str(remaining_qty),
                    },
                    round_index=round_index,
                )
                if label == "stage1":
                    return _StageResult(filled_qty=total_filled, remaining_qty=remaining_qty, zero_fill_retries=0, market_fallback_used=False)
                if remaining_qty <= Decimal("0"):
                    return _StageResult(
                        filled_qty=total_filled,
                        remaining_qty=Decimal("0"),
                        zero_fill_retries=0,
                        market_fallback_used=market_fallback_used,
                    )
                continue

            cancel_state = await self._cancel_if_open(observation.order)
            if cancel_state.executed_qty > Decimal("0"):
                filled_qty = cancel_state.executed_qty
                total_filled += filled_qty
                remaining_qty = normalize_qty(remaining_qty - filled_qty, rules)
                zero_fill_retries = 0
                self._repository.add_event(
                    session.session_id,
                    f"{label}_late_fill",
                    {
                        "round_index": round_index,
                        "order_id": order.order_id,
                        "filled_qty": str(filled_qty),
                        "remaining_qty": str(remaining_qty),
                    },
                    round_index=round_index,
                )
                if label == "stage1":
                    return _StageResult(filled_qty=total_filled, remaining_qty=remaining_qty, zero_fill_retries=0, market_fallback_used=False)
                if remaining_qty <= Decimal("0"):
                    return _StageResult(
                        filled_qty=total_filled,
                        remaining_qty=Decimal("0"),
                        zero_fill_retries=0,
                        market_fallback_used=market_fallback_used,
                    )
                continue

            zero_fill_retries += 1
            self._repository.add_event(
                session.session_id,
                f"{label}_zero_fill_retry",
                {"round_index": round_index, "order_id": order.order_id, "retry": zero_fill_retries},
                round_index=round_index,
            )
            if zero_fill_retries >= max_zero_fill_retries:
                if label == "stage1":
                    return _StageResult(
                        filled_qty=Decimal("0"),
                        remaining_qty=remaining_qty,
                        zero_fill_retries=zero_fill_retries,
                        market_fallback_used=False,
                    )
                market_fallback_used = True
                total_filled += await self._market_fill_remaining(
                    session=session,
                    round_index=round_index,
                    side=side,
                    position_side=position_side,
                    remaining_qty=remaining_qty,
                    attempts=market_fallback_attempts,
                    label=label,
                    control=control,
                )
                return _StageResult(
                    filled_qty=total_filled,
                    remaining_qty=Decimal("0"),
                    zero_fill_retries=zero_fill_retries,
                    market_fallback_used=True,
                )

        return _StageResult(
            filled_qty=total_filled,
            remaining_qty=remaining_qty,
            zero_fill_retries=zero_fill_retries,
            market_fallback_used=market_fallback_used,
        )

    async def _run_final_alignment(self, session: OpenSession, control: SessionControl) -> None:
        session.final_alignment_status = FinalAlignmentStatus.CARRYOVER_PENDING
        session.final_unaligned_qty = session.stage2_carryover_qty
        self._repository.update_session_runtime(session)

        spec = session.spec
        rules = await self._gateway.get_symbol_rules(spec.symbol)
        overview = await self._gateway.get_account_overview()
        account_long_qty = self._position_qty(overview, spec.symbol, PositionSide.LONG)
        account_short_qty = self._position_qty(overview, spec.symbol, PositionSide.SHORT)
        session_long_qty, session_short_qty = self._session_position_qty(session)
        self._repository.add_event(
            session.session_id,
            "final_alignment_started",
            {
                "symbol": spec.symbol,
                "carryover_qty": str(session.stage2_carryover_qty),
                "session_long_qty": str(session_long_qty),
                "session_short_qty": str(session_short_qty),
                "account_long_qty": str(account_long_qty),
                "account_short_qty": str(account_short_qty),
            },
        )

        if session_long_qty == session_short_qty:
            session.stage2_carryover_qty = Decimal("0")
            session.final_unaligned_qty = Decimal("0")
            session.final_alignment_status = FinalAlignmentStatus.MARKET_ALIGNED
            self._repository.add_event(
                session.session_id,
                "final_alignment_completed",
                {
                    "mode": "already_aligned",
                    "session_long_qty": str(session_long_qty),
                    "session_short_qty": str(session_short_qty),
                },
            )
            return

        if account_long_qty < session_long_qty or account_short_qty < session_short_qty:
            raise ExchangeStateError(
                f"Account positions are smaller than session-owned positions for {spec.symbol}: "
                f"account_long={account_long_qty}, account_short={account_short_qty}, "
                f"session_long={session_long_qty}, session_short={session_short_qty}"
            )

        quote = await self._gateway.get_quote(spec.symbol)
        long_min_qty = min_qty_for_notional(normalize_price(quote.bid_price, rules), rules)
        short_min_qty = min_qty_for_notional(normalize_price(quote.ask_price, rules), rules)

        if session_long_qty <= session_short_qty:
            small_side = PositionSide.LONG
            small_qty = session_long_qty
            large_side = PositionSide.SHORT
            large_qty = session_short_qty
            small_min_qty = long_min_qty
        else:
            small_side = PositionSide.SHORT
            small_qty = session_short_qty
            large_side = PositionSide.LONG
            large_qty = session_long_qty
            small_min_qty = short_min_qty

        gap_qty = normalize_qty(large_qty - small_qty, rules)
        try:
            if small_qty < small_min_qty:
                await self._flatten_both_sides(session, control, session_long_qty, session_short_qty)
                session.completed_with_final_alignment = True
                session.stage2_carryover_qty = Decimal("0")
                session.final_unaligned_qty = Decimal("0")
                session.final_alignment_status = FinalAlignmentStatus.FLATTENED_BOTH_SIDES
                self._repository.add_event(
                    session.session_id,
                    "final_alignment_completed",
                    {
                        "mode": "flattened_both_sides",
                        "session_long_qty": str(session_long_qty),
                        "session_short_qty": str(session_short_qty),
                    },
                )
                return

            small_reduce_qty = small_min_qty
            large_reduce_qty = normalize_qty(small_min_qty + gap_qty, rules)
            await self._reduce_position_market(session, control, small_side, small_reduce_qty, "small_side")
            await self._reduce_position_market(session, control, large_side, large_reduce_qty, "large_side")
            residual_small_qty = normalize_qty(small_qty - small_reduce_qty, rules)
            residual_large_qty = normalize_qty(large_qty - large_reduce_qty, rules)
            if residual_small_qty != residual_large_qty:
                raise ExchangeStateError(
                    f"Final alignment did not converge for {spec.symbol}: residual_small={residual_small_qty}, residual_large={residual_large_qty}"
                )
            session.completed_with_final_alignment = True
            session.stage2_carryover_qty = Decimal("0")
            session.final_unaligned_qty = Decimal("0")
            session.final_alignment_status = FinalAlignmentStatus.MARKET_ALIGNED
            self._repository.add_event(
                session.session_id,
                "final_alignment_completed",
                {
                    "mode": "market_aligned",
                    "residual_session_qty": str(residual_small_qty),
                },
            )
        except Exception as exc:
            session.final_alignment_status = FinalAlignmentStatus.FAILED
            session.final_unaligned_qty = session.stage2_carryover_qty
            self._repository.add_event(
                session.session_id,
                "final_alignment_failed",
                {"error": str(exc), "carryover_qty": str(session.stage2_carryover_qty)},
            )
            raise

    async def _flatten_both_sides(
        self,
        session: OpenSession,
        control: SessionControl,
        long_qty: Decimal,
        short_qty: Decimal,
    ) -> None:
        self._repository.add_event(
            session.session_id,
            "final_alignment_flatten_both_sides",
            {"long_qty": str(long_qty), "short_qty": str(short_qty)},
        )
        if long_qty > Decimal("0"):
            await self._reduce_position_market(session, control, PositionSide.LONG, long_qty, "flatten_long")
        if short_qty > Decimal("0"):
            await self._reduce_position_market(session, control, PositionSide.SHORT, short_qty, "flatten_short")

    async def _reduce_position_market(
        self,
        session: OpenSession,
        control: SessionControl,
        position_side: PositionSide,
        qty: Decimal,
        label: str,
    ) -> Decimal:
        rules = await self._gateway.get_symbol_rules(session.spec.symbol)
        reduce_qty = normalize_qty(qty, rules)
        if reduce_qty <= Decimal("0"):
            return Decimal("0")
        side = OrderSide.SELL if position_side == PositionSide.LONG else OrderSide.BUY
        self._repository.add_event(
            session.session_id,
            "final_alignment_market_reduce",
            {"position_side": position_side.value, "side": side.value, "qty": str(reduce_qty), "label": label},
        )
        return await self._market_fill_remaining(
            session=session,
            round_index=session.spec.round_count,
            side=side,
            position_side=position_side,
            remaining_qty=reduce_qty,
            attempts=max(session.spec.market_fallback_attempts, 1),
            label=f"final_alignment_{label}",
            control=control,
        )

    async def _market_fill_remaining(
        self,
        *,
        session: OpenSession,
        round_index: int,
        side: OrderSide,
        position_side: PositionSide,
        remaining_qty: Decimal,
        attempts: int,
        label: str,
        control: SessionControl,
    ) -> Decimal:
        spec = session.spec
        rules = await self._gateway.get_symbol_rules(spec.symbol)
        rest_qty = normalize_qty(remaining_qty, rules)
        filled_total = Decimal("0")
        for attempt in range(1, attempts + 1):
            if rest_qty <= Decimal("0"):
                return filled_total
            order = await self._gateway.place_market_order(
                symbol=spec.symbol,
                side=side,
                position_side=position_side,
                qty=rest_qty,
                client_order_id=f"{session.session_id}-{round_index}-{label}-market-{attempt}",
            )
            observation = await self._observe_order(spec, order, control=control, ttl_override_ms=1000)
            filled_qty = observation.order.executed_qty
            filled_total += filled_qty
            rest_qty = normalize_qty(rest_qty - filled_qty, rules)
            self._repository.add_event(
                session.session_id,
                f"{label}_market_fallback",
                {
                    "round_index": round_index,
                    "attempt": attempt,
                    "order_id": order.order_id,
                    "filled_qty": str(filled_qty),
                    "remaining_qty": str(rest_qty),
                },
                round_index=round_index,
            )
        if rest_qty > Decimal("0"):
            raise ExchangeStateError(f"Market fallback failed for session {session.session_id}, remaining {rest_qty}")
        return filled_total

    async def _observe_order(
        self,
        spec: SessionSpec,
        order: ExchangeOrder,
        *,
        control: SessionControl,
        ttl_override_ms: int | None = None,
    ) -> PollObservation:
        deadline = self._monotonic() + ((ttl_override_ms or spec.order_ttl_ms) / 1000)
        current = order
        while self._monotonic() < deadline:
            await self._respect_control(control, allow_abort=False, allow_pause=False)
            current = await self._gateway.get_order(symbol=spec.symbol, order_id=order.order_id)
            if current.status == ExchangeOrderStatus.FILLED:
                return PollObservation(order=current, filled_qty=current.executed_qty, had_fill=True, terminal=True)
            if current.executed_qty > Decimal("0"):
                return PollObservation(order=current, filled_qty=current.executed_qty, had_fill=True, terminal=False)
            await self._sleep_with_control(spec.poll_interval_ms / 1000, control, allow_abort=False, allow_pause=False)
        await self._respect_control(control, allow_abort=False, allow_pause=False)
        current = await self._gateway.get_order(symbol=spec.symbol, order_id=order.order_id)
        return PollObservation(
            order=current,
            filled_qty=current.executed_qty,
            had_fill=current.executed_qty > Decimal("0"),
            terminal=current.status == ExchangeOrderStatus.FILLED,
        )

    async def _cancel_if_open(self, order: ExchangeOrder) -> ExchangeOrder:
        if order.status in (
            ExchangeOrderStatus.CANCELED,
            ExchangeOrderStatus.EXPIRED,
            ExchangeOrderStatus.FILLED,
            ExchangeOrderStatus.REJECTED,
        ):
            return order
        return await self._gateway.cancel_order(symbol=order.symbol, order_id=order.order_id)

    async def _sleep_with_control(
        self,
        seconds: float,
        control: SessionControl,
        *,
        allow_abort: bool = True,
        allow_pause: bool = True,
    ) -> None:
        remaining = max(seconds, 0.0)
        while remaining > 0:
            await self._respect_control(control, allow_abort=allow_abort, allow_pause=allow_pause)
            chunk = min(remaining, 0.1)
            await self._sleep(chunk)
            remaining -= chunk

    async def _respect_control(
        self,
        control: SessionControl,
        *,
        allow_abort: bool = True,
        allow_pause: bool = True,
    ) -> None:
        while allow_pause and control.paused:
            if allow_abort and control.aborted:
                raise SessionAbortedError("Session aborted by operator")
            await self._sleep(0.1)
        if allow_abort and control.aborted:
            raise SessionAbortedError("Session aborted by operator")

    def _position_qty(self, overview: dict, symbol: str, position_side: PositionSide) -> Decimal:
        for item in overview.get("positions", []):
            if item.get("symbol") != symbol:
                continue
            if str(item.get("position_side")) != position_side.value:
                continue
            return Decimal(str(item.get("qty") or "0"))
        return Decimal("0")

    def _session_position_qty(self, session: OpenSession) -> tuple[Decimal, Decimal]:
        long_qty = Decimal("0")
        short_qty = Decimal("0")
        for round_payload in self._repository.list_rounds(session.session_id):
            stage1_filled = Decimal(str(round_payload.get("stage1_filled_qty") or "0"))
            stage2_filled = Decimal(str(round_payload.get("stage2_filled_qty") or "0"))
            if session.spec.trend_bias == TrendBias.LONG:
                long_qty += stage1_filled
                short_qty += stage2_filled
            else:
                short_qty += stage1_filled
                long_qty += stage2_filled
        return long_qty, short_qty

    def _stage1_params(self, trend_bias: TrendBias) -> tuple[OrderSide, PositionSide, Callable[[Quote], Decimal]]:
        if trend_bias == TrendBias.LONG:
            return OrderSide.BUY, PositionSide.LONG, lambda quote: quote.bid_price
        return OrderSide.SELL, PositionSide.SHORT, lambda quote: quote.ask_price

    def _stage2_params(self, trend_bias: TrendBias) -> tuple[OrderSide, PositionSide, Callable[[Quote], Decimal]]:
        if trend_bias == TrendBias.LONG:
            return OrderSide.SELL, PositionSide.SHORT, lambda quote: quote.ask_price
        return OrderSide.BUY, PositionSide.LONG, lambda quote: quote.bid_price





class SingleOpeningEngine(PairedOpeningEngine):
    async def execute_session(self, session: OpenSession, control: SessionControl) -> tuple[int, int]:
        completed_rounds = 0
        skipped_rounds = 0
        for round_index in range(1, session.spec.round_count + 1):
            await self._respect_control(control)
            execution = await self.execute_round(session=session, round_index=round_index, control=control)
            self._repository.update_session_runtime(session)
            if execution.status == RoundStatus.STAGE1_SKIPPED:
                skipped_rounds += 1
            else:
                completed_rounds += 1
            if control.aborted:
                raise SessionAbortedError("Session aborted by operator after safe checkpoint")
            if round_index < session.spec.round_count and session.spec.round_interval_seconds > 0:
                self._repository.add_event(
                    session.session_id,
                    "single_open_round_interval_wait",
                    {"round_index": round_index, "wait_seconds": session.spec.round_interval_seconds},
                    round_index=round_index,
                )
                await self._sleep_with_control(session.spec.round_interval_seconds, control)
        return completed_rounds, skipped_rounds

    async def execute_round(
        self,
        *,
        session: OpenSession,
        round_index: int,
        control: SessionControl,
    ) -> RoundExecution:
        spec = session.spec
        if spec.selected_position_side is None:
            raise ExchangeStateError(f"Single open session {session.session_id} is missing selected_position_side")

        rules = await self._gateway.get_symbol_rules(spec.symbol)
        already_opened_qty = self._single_open_opened_qty(session)
        remaining_target_qty = normalize_qty(spec.target_open_qty - already_opened_qty, rules)
        target_qty = normalize_qty(min(spec.round_qty, remaining_target_qty), rules)

        execution = RoundExecution(session_id=session.session_id, round_index=round_index, status=RoundStatus.STAGE1_PENDING)
        execution.notes["open_mode"] = spec.open_mode.value if spec.open_mode else None
        execution.notes["selected_position_side"] = spec.selected_position_side.value
        execution.notes["remaining_target_qty_in"] = str(remaining_target_qty)
        self._repository.upsert_round(execution)
        self._repository.add_event(
            session.session_id,
            "single_open_round_started",
            {
                "round_index": round_index,
                "selected_position_side": spec.selected_position_side.value,
                "remaining_target_qty": str(remaining_target_qty),
            },
            round_index=round_index,
        )

        if remaining_target_qty <= Decimal("0") or target_qty <= Decimal("0"):
            execution.status = RoundStatus.STAGE1_SKIPPED
            execution.notes["skip_reason"] = "target_completed"
            execution.ended_at = datetime.now(UTC)
            self._repository.upsert_round(execution)
            self._repository.add_event(
                session.session_id,
                "single_open_round_skipped",
                {
                    "round_index": round_index,
                    "reason": execution.notes["skip_reason"],
                    "remaining_target_qty": str(remaining_target_qty),
                },
                round_index=round_index,
            )
            return execution

        side, price_selector = self._single_open_params(spec.selected_position_side)
        stage_result = await self._run_single_open_limit_stage(
            session=session,
            control=control,
            label="single_open",
            round_index=round_index,
            side=side,
            position_side=spec.selected_position_side,
            qty=target_qty,
            price_selector=price_selector,
            max_zero_fill_retries=spec.max_zero_fill_retries,
            market_fallback_attempts=spec.market_fallback_attempts,
        )
        execution.stage1_zero_fill_retries = stage_result.zero_fill_retries
        execution.stage1_filled_qty = stage_result.filled_qty
        execution.market_fallback_used = stage_result.market_fallback_used
        execution.notes["round_target_qty"] = str(target_qty)
        execution.notes["round_remaining_qty"] = str(stage_result.remaining_qty)
        execution.notes["remaining_target_qty_out"] = str(normalize_qty(remaining_target_qty - stage_result.filled_qty, rules))
        execution.status = RoundStatus.ROUND_COMPLETED if stage_result.filled_qty > Decimal("0") else RoundStatus.STAGE1_SKIPPED
        execution.ended_at = datetime.now(UTC)
        self._repository.upsert_round(execution)
        event_name = "single_open_round_completed" if execution.status == RoundStatus.ROUND_COMPLETED else "single_open_round_skipped"
        self._repository.add_event(
            session.session_id,
            event_name,
            {
                "round_index": round_index,
                "filled_qty": str(stage_result.filled_qty),
                "remaining_qty": str(stage_result.remaining_qty),
                "market_fallback_used": stage_result.market_fallback_used,
                "selected_position_side": spec.selected_position_side.value,
            },
            round_index=round_index,
        )
        return execution

    async def _run_single_open_limit_stage(
        self,
        *,
        session: OpenSession,
        control: SessionControl,
        label: str,
        round_index: int,
        side: OrderSide,
        position_side: PositionSide,
        qty: Decimal,
        price_selector: Callable[[Quote], Decimal],
        max_zero_fill_retries: int,
        market_fallback_attempts: int = 0,
    ) -> _StageResult:
        spec = session.spec
        rules = await self._gateway.get_symbol_rules(spec.symbol)
        target_qty = normalize_qty(qty, rules)
        zero_fill_retries = 0
        market_fallback_used = False

        while target_qty > Decimal("0"):
            await self._respect_control(control)
            quote = await self._gateway.get_quote(spec.symbol)
            price = normalize_price(price_selector(quote), rules)
            validate_qty_and_notional(target_qty, price, rules)
            order = await self._gateway.place_limit_order(
                symbol=spec.symbol,
                side=side,
                position_side=position_side,
                qty=target_qty,
                price=price,
                client_order_id=f"{session.session_id}-{round_index}-{label}-{zero_fill_retries + 1}",
            )
            self._repository.add_event(
                session.session_id,
                f"{label}_order_placed",
                {
                    "round_index": round_index,
                    "order_id": order.order_id,
                    "qty": str(target_qty),
                    "price": str(price),
                    "side": side.value,
                    "position_side": position_side.value,
                },
                round_index=round_index,
            )
            observation = await self._observe_order(spec, order, control=control)
            if observation.order.executed_qty > Decimal("0"):
                cancel_state = await self._cancel_if_open(observation.order)
                filled_qty = max(observation.order.executed_qty, cancel_state.executed_qty)
                remaining_qty = normalize_qty(target_qty - filled_qty, rules)
                self._repository.add_event(
                    session.session_id,
                    f"{label}_fill",
                    {
                        "round_index": round_index,
                        "order_id": order.order_id,
                        "filled_qty": str(filled_qty),
                        "remaining_qty": str(remaining_qty),
                    },
                    round_index=round_index,
                )
                return _StageResult(
                    filled_qty=filled_qty,
                    remaining_qty=remaining_qty,
                    zero_fill_retries=zero_fill_retries,
                    market_fallback_used=market_fallback_used,
                )

            cancel_state = await self._cancel_if_open(observation.order)
            if cancel_state.executed_qty > Decimal("0"):
                filled_qty = cancel_state.executed_qty
                remaining_qty = normalize_qty(target_qty - filled_qty, rules)
                self._repository.add_event(
                    session.session_id,
                    f"{label}_late_fill",
                    {
                        "round_index": round_index,
                        "order_id": order.order_id,
                        "filled_qty": str(filled_qty),
                        "remaining_qty": str(remaining_qty),
                    },
                    round_index=round_index,
                )
                return _StageResult(
                    filled_qty=filled_qty,
                    remaining_qty=remaining_qty,
                    zero_fill_retries=zero_fill_retries,
                    market_fallback_used=market_fallback_used,
                )

            zero_fill_retries += 1
            self._repository.add_event(
                session.session_id,
                f"{label}_zero_fill_retry",
                {"round_index": round_index, "order_id": order.order_id, "retry": zero_fill_retries},
                round_index=round_index,
            )
            if zero_fill_retries >= max_zero_fill_retries:
                market_fallback_used = True
                filled_qty = await self._market_fill_remaining(
                    session=session,
                    round_index=round_index,
                    side=side,
                    position_side=position_side,
                    remaining_qty=target_qty,
                    attempts=market_fallback_attempts,
                    label=label,
                    control=control,
                )
                return _StageResult(
                    filled_qty=filled_qty,
                    remaining_qty=Decimal("0"),
                    zero_fill_retries=zero_fill_retries,
                    market_fallback_used=True,
                )

        return _StageResult(
            filled_qty=Decimal("0"),
            remaining_qty=Decimal("0"),
            zero_fill_retries=zero_fill_retries,
            market_fallback_used=market_fallback_used,
        )

    def _single_open_opened_qty(self, session: OpenSession) -> Decimal:
        total = Decimal("0")
        for round_payload in self._repository.list_rounds(session.session_id):
            total += Decimal(str(round_payload.get("stage1_filled_qty") or "0"))
        return total

    def _single_open_params(self, position_side: PositionSide) -> tuple[OrderSide, Callable[[Quote], Decimal]]:
        if position_side == PositionSide.LONG:
            return OrderSide.BUY, lambda quote: quote.bid_price
        return OrderSide.SELL, lambda quote: quote.ask_price


class SingleClosingEngine(PairedOpeningEngine):
    async def execute_session(self, session: OpenSession, control: SessionControl) -> tuple[int, int]:
        completed_rounds = 0
        skipped_rounds = 0
        session.stage2_carryover_qty = Decimal("0")
        session.final_unaligned_qty = Decimal("0")
        session.final_alignment_status = FinalAlignmentStatus.NOT_NEEDED
        for round_index in range(1, session.spec.round_count + 1):
            await self._respect_control(control)
            execution = await self.execute_round(session=session, round_index=round_index, control=control)
            self._repository.update_session_runtime(session)
            if execution.status == RoundStatus.STAGE1_SKIPPED:
                skipped_rounds += 1
            else:
                completed_rounds += 1
            if control.aborted:
                raise SessionAbortedError("Session aborted by operator after safe checkpoint")
            if round_index < session.spec.round_count and session.spec.round_interval_seconds > 0:
                self._repository.add_event(
                    session.session_id,
                    "single_close_round_interval_wait",
                    {"round_index": round_index, "wait_seconds": session.spec.round_interval_seconds},
                    round_index=round_index,
                )
                await self._sleep_with_control(session.spec.round_interval_seconds, control)
        return completed_rounds, skipped_rounds

    async def execute_round(
        self,
        *,
        session: OpenSession,
        round_index: int,
        control: SessionControl,
    ) -> RoundExecution:
        spec = session.spec
        if spec.selected_position_side is None:
            raise ExchangeStateError(f"Single close session {session.session_id} is missing selected_position_side")

        rules = await self._gateway.get_symbol_rules(spec.symbol)
        already_closed_qty = self._single_close_closed_qty(session)
        remaining_target_qty = normalize_qty(spec.target_close_qty - already_closed_qty, rules)
        overview = await self._gateway.get_account_overview()
        available_qty = normalize_qty(self._position_qty(overview, spec.symbol, spec.selected_position_side), rules)
        target_qty = normalize_qty(min(spec.round_qty, remaining_target_qty, available_qty), rules)

        execution = RoundExecution(session_id=session.session_id, round_index=round_index, status=RoundStatus.STAGE1_PENDING)
        execution.notes["close_mode"] = spec.close_mode.value if spec.close_mode else None
        execution.notes["selected_position_side"] = spec.selected_position_side.value
        execution.notes["remaining_target_qty_in"] = str(remaining_target_qty)
        execution.notes["available_position_qty_in"] = str(available_qty)
        self._repository.upsert_round(execution)
        self._repository.add_event(
            session.session_id,
            "single_close_round_started",
            {
                "round_index": round_index,
                "selected_position_side": spec.selected_position_side.value,
                "remaining_target_qty": str(remaining_target_qty),
                "available_qty": str(available_qty),
            },
            round_index=round_index,
        )

        if remaining_target_qty <= Decimal("0") or available_qty <= Decimal("0") or target_qty <= Decimal("0"):
            execution.status = RoundStatus.STAGE1_SKIPPED
            execution.notes["skip_reason"] = (
                "target_completed" if remaining_target_qty <= Decimal("0") else "no_available_position"
            )
            execution.ended_at = datetime.now(UTC)
            self._repository.upsert_round(execution)
            self._repository.add_event(
                session.session_id,
                "single_close_round_skipped",
                {
                    "round_index": round_index,
                    "reason": execution.notes["skip_reason"],
                    "remaining_target_qty": str(remaining_target_qty),
                    "available_qty": str(available_qty),
                },
                round_index=round_index,
            )
            return execution

        side, price_selector = self._single_close_params(spec.selected_position_side)
        stage_result = await self._run_single_close_limit_stage(
            session=session,
            control=control,
            label="single_close",
            round_index=round_index,
            side=side,
            position_side=spec.selected_position_side,
            qty=target_qty,
            price_selector=price_selector,
            max_zero_fill_retries=spec.max_zero_fill_retries,
            market_fallback_attempts=spec.market_fallback_attempts,
        )
        execution.stage1_zero_fill_retries = stage_result.zero_fill_retries
        execution.stage1_filled_qty = stage_result.filled_qty
        execution.market_fallback_used = stage_result.market_fallback_used
        execution.notes["round_target_qty"] = str(target_qty)
        execution.notes["round_remaining_qty"] = str(stage_result.remaining_qty)
        execution.notes["remaining_target_qty_out"] = str(normalize_qty(remaining_target_qty - stage_result.filled_qty, rules))
        execution.status = RoundStatus.ROUND_COMPLETED if stage_result.filled_qty > Decimal("0") else RoundStatus.STAGE1_SKIPPED
        execution.ended_at = datetime.now(UTC)
        self._repository.upsert_round(execution)
        event_name = "single_close_round_completed" if execution.status == RoundStatus.ROUND_COMPLETED else "single_close_round_skipped"
        self._repository.add_event(
            session.session_id,
            event_name,
            {
                "round_index": round_index,
                "filled_qty": str(stage_result.filled_qty),
                "remaining_qty": str(stage_result.remaining_qty),
                "market_fallback_used": stage_result.market_fallback_used,
                "selected_position_side": spec.selected_position_side.value,
            },
            round_index=round_index,
        )
        return execution

    async def _run_single_close_limit_stage(
        self,
        *,
        session: OpenSession,
        control: SessionControl,
        label: str,
        round_index: int,
        side: OrderSide,
        position_side: PositionSide,
        qty: Decimal,
        price_selector: Callable[[Quote], Decimal],
        max_zero_fill_retries: int,
        market_fallback_attempts: int = 0,
    ) -> _StageResult:
        spec = session.spec
        rules = await self._gateway.get_symbol_rules(spec.symbol)
        target_qty = normalize_qty(qty, rules)
        zero_fill_retries = 0
        market_fallback_used = False

        while target_qty > Decimal("0"):
            await self._respect_control(control)
            quote = await self._gateway.get_quote(spec.symbol)
            price = normalize_price(price_selector(quote), rules)
            validate_qty_and_notional(target_qty, price, rules)
            order = await self._gateway.place_limit_order(
                symbol=spec.symbol,
                side=side,
                position_side=position_side,
                qty=target_qty,
                price=price,
                client_order_id=f"{session.session_id}-{round_index}-{label}-{zero_fill_retries + 1}",
            )
            self._repository.add_event(
                session.session_id,
                f"{label}_order_placed",
                {
                    "round_index": round_index,
                    "order_id": order.order_id,
                    "qty": str(target_qty),
                    "price": str(price),
                    "side": side.value,
                    "position_side": position_side.value,
                },
                round_index=round_index,
            )
            observation = await self._observe_order(spec, order, control=control)
            if observation.order.executed_qty > Decimal("0"):
                cancel_state = await self._cancel_if_open(observation.order)
                filled_qty = max(observation.order.executed_qty, cancel_state.executed_qty)
                remaining_qty = normalize_qty(target_qty - filled_qty, rules)
                self._repository.add_event(
                    session.session_id,
                    f"{label}_fill",
                    {
                        "round_index": round_index,
                        "order_id": order.order_id,
                        "filled_qty": str(filled_qty),
                        "remaining_qty": str(remaining_qty),
                    },
                    round_index=round_index,
                )
                return _StageResult(
                    filled_qty=filled_qty,
                    remaining_qty=remaining_qty,
                    zero_fill_retries=zero_fill_retries,
                    market_fallback_used=market_fallback_used,
                )

            cancel_state = await self._cancel_if_open(observation.order)
            if cancel_state.executed_qty > Decimal("0"):
                filled_qty = cancel_state.executed_qty
                remaining_qty = normalize_qty(target_qty - filled_qty, rules)
                self._repository.add_event(
                    session.session_id,
                    f"{label}_late_fill",
                    {
                        "round_index": round_index,
                        "order_id": order.order_id,
                        "filled_qty": str(filled_qty),
                        "remaining_qty": str(remaining_qty),
                    },
                    round_index=round_index,
                )
                return _StageResult(
                    filled_qty=filled_qty,
                    remaining_qty=remaining_qty,
                    zero_fill_retries=zero_fill_retries,
                    market_fallback_used=market_fallback_used,
                )

            zero_fill_retries += 1
            self._repository.add_event(
                session.session_id,
                f"{label}_zero_fill_retry",
                {"round_index": round_index, "order_id": order.order_id, "retry": zero_fill_retries},
                round_index=round_index,
            )
            if zero_fill_retries >= max_zero_fill_retries:
                market_fallback_used = True
                filled_qty = await self._market_fill_remaining(
                    session=session,
                    round_index=round_index,
                    side=side,
                    position_side=position_side,
                    remaining_qty=target_qty,
                    attempts=market_fallback_attempts,
                    label=label,
                    control=control,
                )
                return _StageResult(
                    filled_qty=filled_qty,
                    remaining_qty=Decimal("0"),
                    zero_fill_retries=zero_fill_retries,
                    market_fallback_used=True,
                )

        return _StageResult(
            filled_qty=Decimal("0"),
            remaining_qty=Decimal("0"),
            zero_fill_retries=zero_fill_retries,
            market_fallback_used=market_fallback_used,
        )

    def _single_close_closed_qty(self, session: OpenSession) -> Decimal:
        total = Decimal("0")
        for round_payload in self._repository.list_rounds(session.session_id):
            total += Decimal(str(round_payload.get("stage1_filled_qty") or "0"))
        return total

    def _single_close_params(self, position_side: PositionSide) -> tuple[OrderSide, Callable[[Quote], Decimal]]:
        if position_side == PositionSide.LONG:
            return OrderSide.SELL, lambda quote: quote.bid_price
        return OrderSide.BUY, lambda quote: quote.ask_price
class PairedClosingEngine(PairedOpeningEngine):
    async def execute_session(self, session: OpenSession, control: SessionControl) -> tuple[int, int]:
        completed_rounds = 0
        skipped_rounds = 0
        session.stage2_carryover_qty = Decimal("0")
        session.final_unaligned_qty = Decimal("0")
        session.final_alignment_status = FinalAlignmentStatus.NOT_NEEDED
        for round_index in range(1, session.spec.round_count + 1):
            await self._respect_control(control)
            execution = await self.execute_round(session=session, round_index=round_index, control=control)
            self._repository.update_session_runtime(session)
            if execution.status == RoundStatus.STAGE1_SKIPPED:
                skipped_rounds += 1
            else:
                completed_rounds += 1
            if control.aborted:
                raise SessionAbortedError("Session aborted by operator after safe checkpoint")
            if round_index < session.spec.round_count and session.spec.round_interval_seconds > 0:
                self._repository.add_event(
                    session.session_id,
                    "close_round_interval_wait",
                    {"round_index": round_index, "wait_seconds": session.spec.round_interval_seconds},
                    round_index=round_index,
                )
                await self._sleep_with_control(session.spec.round_interval_seconds, control)
        if session.stage2_carryover_qty > Decimal("0"):
            await self._run_close_final_alignment(session, control)
            self._repository.update_session_runtime(session)
            if control.aborted:
                raise SessionAbortedError("Session aborted by operator after close final alignment")
        return completed_rounds, skipped_rounds

    async def execute_round(
        self,
        *,
        session: OpenSession,
        round_index: int,
        control: SessionControl,
    ) -> RoundExecution:
        spec = session.spec
        execution = RoundExecution(session_id=session.session_id, round_index=round_index, status=RoundStatus.STAGE1_PENDING)
        self._repository.upsert_round(execution)
        self._repository.add_event(session.session_id, "close_round_started", {"round_index": round_index}, round_index=round_index)

        rules = await self._gateway.get_symbol_rules(spec.symbol)
        stage1_side, stage1_position_side, stage1_selector = self._close_stage1_params(spec.trend_bias)
        stage2_side, stage2_position_side, stage2_selector = self._close_stage2_params(spec.trend_bias)
        overview = await self._gateway.get_account_overview()
        stage1_available_qty = self._position_qty(overview, spec.symbol, stage1_position_side)
        stage2_available_qty = self._position_qty(overview, spec.symbol, stage2_position_side)
        carryover_in = normalize_qty(session.stage2_carryover_qty, rules)
        closeable_stage2_qty = normalize_qty(max(Decimal("0"), stage2_available_qty - carryover_in), rules)
        target_qty = normalize_qty(min(spec.round_qty, stage1_available_qty, closeable_stage2_qty), rules)
        execution.notes["stage1_available_qty"] = str(stage1_available_qty)
        execution.notes["stage2_available_qty"] = str(stage2_available_qty)
        execution.notes["carryover_in_qty"] = str(carryover_in)
        execution.notes["target_qty"] = str(target_qty)

        if target_qty <= Decimal("0"):
            execution.status = RoundStatus.STAGE1_SKIPPED
            execution.notes["skip_reason"] = "no_closeable_positions"
            execution.ended_at = datetime.now(UTC)
            self._repository.upsert_round(execution)
            self._repository.add_event(
                session.session_id,
                "close_round_skipped",
                {
                    "round_index": round_index,
                    "stage1_available_qty": str(stage1_available_qty),
                    "stage2_available_qty": str(stage2_available_qty),
                },
                round_index=round_index,
            )
            return execution

        stage1_result = await self._run_close_limit_stage(
            session=session,
            execution=execution,
            control=control,
            label="close_stage1",
            round_index=round_index,
            side=stage1_side,
            position_side=stage1_position_side,
            qty=target_qty,
            price_selector=stage1_selector,
            max_zero_fill_retries=spec.max_zero_fill_retries,
        )
        execution.stage1_zero_fill_retries = stage1_result.zero_fill_retries
        execution.stage1_filled_qty = stage1_result.filled_qty

        if stage1_result.filled_qty <= Decimal("0"):
            execution.status = RoundStatus.STAGE1_SKIPPED
            execution.notes["skip_reason"] = "close_stage1_zero_fill_retries_exhausted"
            execution.ended_at = datetime.now(UTC)
            self._repository.upsert_round(execution)
            self._repository.add_event(
                session.session_id,
                "close_round_skipped",
                {"round_index": round_index, "reason": "close_stage1_zero_fill_retries_exhausted"},
                round_index=round_index,
            )
            return execution

        execution.status = RoundStatus.STAGE2_PENDING
        self._repository.upsert_round(execution)
        stage2_target_qty = normalize_qty(stage1_result.filled_qty + carryover_in, rules)
        execution.notes["stage2_target_qty"] = str(stage2_target_qty)
        stage2_result = await self._run_close_limit_stage(
            session=session,
            execution=execution,
            control=control,
            label="close_stage2",
            round_index=round_index,
            side=stage2_side,
            position_side=stage2_position_side,
            qty=stage2_target_qty,
            price_selector=stage2_selector,
            max_zero_fill_retries=spec.max_zero_fill_retries,
            market_fallback_attempts=spec.market_fallback_attempts,
        )
        execution.stage2_zero_fill_retries = stage2_result.zero_fill_retries
        execution.stage2_filled_qty = stage2_result.filled_qty
        execution.market_fallback_used = stage2_result.market_fallback_used
        execution.notes["stage2_remaining_qty"] = str(stage2_result.remaining_qty)
        session.stage2_carryover_qty = stage2_result.remaining_qty
        session.final_unaligned_qty = stage2_result.remaining_qty
        session.final_alignment_status = (
            FinalAlignmentStatus.CARRYOVER_PENDING
            if stage2_result.remaining_qty > Decimal("0")
            else FinalAlignmentStatus.NOT_NEEDED
        )
        if stage2_result.remaining_qty > Decimal("0"):
            self._repository.add_event(
                session.session_id,
                "close_stage2_carryover_persisted",
                {
                    "round_index": round_index,
                    "carryover_qty": str(stage2_result.remaining_qty),
                    "stage2_target_qty": str(stage2_target_qty),
                },
                round_index=round_index,
            )
        execution.status = RoundStatus.ROUND_COMPLETED
        execution.ended_at = datetime.now(UTC)
        self._repository.upsert_round(execution)
        self._repository.add_event(
            session.session_id,
            "close_round_completed",
            {
                "round_index": round_index,
                "stage1_filled_qty": str(execution.stage1_filled_qty),
                "stage2_filled_qty": str(execution.stage2_filled_qty),
                "market_fallback_used": execution.market_fallback_used,
            },
            round_index=round_index,
        )
        return execution

    async def _run_close_limit_stage(
        self,
        *,
        session: OpenSession,
        execution: RoundExecution,
        control: SessionControl,
        label: str,
        round_index: int,
        side: OrderSide,
        position_side: PositionSide,
        qty: Decimal,
        price_selector: Callable[[Quote], Decimal],
        max_zero_fill_retries: int,
        market_fallback_attempts: int = 0,
    ) -> _StageResult:
        spec = session.spec
        rules = await self._gateway.get_symbol_rules(spec.symbol)
        remaining_qty = normalize_qty(qty, rules)
        total_filled = Decimal("0")
        zero_fill_retries = 0
        market_fallback_used = False

        while remaining_qty > Decimal("0"):
            await self._respect_control(control)
            quote = await self._gateway.get_quote(spec.symbol)
            price = normalize_price(price_selector(quote), rules)
            try:
                validate_qty_and_notional(remaining_qty, price, rules)
            except ValueError:
                if label != "close_stage2":
                    raise
                self._repository.add_event(
                    session.session_id,
                    "close_stage2_below_min_carryover",
                    {
                        "round_index": round_index,
                        "remaining_qty": str(remaining_qty),
                        "price": str(price),
                    },
                    round_index=round_index,
                )
                return _StageResult(
                    filled_qty=total_filled,
                    remaining_qty=remaining_qty,
                    zero_fill_retries=zero_fill_retries,
                    market_fallback_used=market_fallback_used,
                )
            order = await self._gateway.place_limit_order(
                symbol=spec.symbol,
                side=side,
                position_side=position_side,
                qty=remaining_qty,
                price=price,
                client_order_id=f"{session.session_id}-{round_index}-{label}-{zero_fill_retries + 1}",
            )
            self._repository.add_event(
                session.session_id,
                f"{label}_order_placed",
                {
                    "round_index": round_index,
                    "order_id": order.order_id,
                    "qty": str(remaining_qty),
                    "price": str(price),
                    "side": side.value,
                    "position_side": position_side.value,
                },
                round_index=round_index,
            )
            observation = await self._observe_order(spec, order, control=control)
            if observation.order.executed_qty > Decimal("0"):
                cancel_state = await self._cancel_if_open(observation.order)
                filled_qty = max(observation.order.executed_qty, cancel_state.executed_qty)
                total_filled += filled_qty
                remaining_qty = normalize_qty(remaining_qty - filled_qty, rules)
                zero_fill_retries = 0
                self._repository.add_event(
                    session.session_id,
                    f"{label}_fill",
                    {
                        "round_index": round_index,
                        "order_id": order.order_id,
                        "filled_qty": str(filled_qty),
                        "remaining_qty": str(remaining_qty),
                    },
                    round_index=round_index,
                )
                if label == "close_stage1":
                    return _StageResult(filled_qty=total_filled, remaining_qty=remaining_qty, zero_fill_retries=0, market_fallback_used=False)
                if remaining_qty <= Decimal("0"):
                    return _StageResult(
                        filled_qty=total_filled,
                        remaining_qty=Decimal("0"),
                        zero_fill_retries=0,
                        market_fallback_used=market_fallback_used,
                    )
                continue

            cancel_state = await self._cancel_if_open(observation.order)
            if cancel_state.executed_qty > Decimal("0"):
                filled_qty = cancel_state.executed_qty
                total_filled += filled_qty
                remaining_qty = normalize_qty(remaining_qty - filled_qty, rules)
                zero_fill_retries = 0
                self._repository.add_event(
                    session.session_id,
                    f"{label}_late_fill",
                    {
                        "round_index": round_index,
                        "order_id": order.order_id,
                        "filled_qty": str(filled_qty),
                        "remaining_qty": str(remaining_qty),
                    },
                    round_index=round_index,
                )
                if label == "close_stage1":
                    return _StageResult(filled_qty=total_filled, remaining_qty=remaining_qty, zero_fill_retries=0, market_fallback_used=False)
                if remaining_qty <= Decimal("0"):
                    return _StageResult(
                        filled_qty=total_filled,
                        remaining_qty=Decimal("0"),
                        zero_fill_retries=0,
                        market_fallback_used=market_fallback_used,
                    )
                continue

            zero_fill_retries += 1
            self._repository.add_event(
                session.session_id,
                f"{label}_zero_fill_retry",
                {"round_index": round_index, "order_id": order.order_id, "retry": zero_fill_retries},
                round_index=round_index,
            )
            if zero_fill_retries >= max_zero_fill_retries:
                if label == "close_stage1":
                    return _StageResult(
                        filled_qty=Decimal("0"),
                        remaining_qty=remaining_qty,
                        zero_fill_retries=zero_fill_retries,
                        market_fallback_used=False,
                    )
                market_fallback_used = True
                total_filled += await self._market_fill_remaining(
                    session=session,
                    round_index=round_index,
                    side=side,
                    position_side=position_side,
                    remaining_qty=remaining_qty,
                    attempts=market_fallback_attempts,
                    label=label,
                    control=control,
                )
                return _StageResult(
                    filled_qty=total_filled,
                    remaining_qty=Decimal("0"),
                    zero_fill_retries=zero_fill_retries,
                    market_fallback_used=True,
                )

        return _StageResult(
            filled_qty=total_filled,
            remaining_qty=remaining_qty,
            zero_fill_retries=zero_fill_retries,
            market_fallback_used=market_fallback_used,
        )

    async def _run_close_final_alignment(self, session: OpenSession, control: SessionControl) -> None:
        session.final_alignment_status = FinalAlignmentStatus.CARRYOVER_PENDING
        session.final_unaligned_qty = session.stage2_carryover_qty
        self._repository.update_session_runtime(session)

        spec = session.spec
        rules = await self._gateway.get_symbol_rules(spec.symbol)
        carryover_qty = normalize_qty(session.stage2_carryover_qty, rules)
        if carryover_qty <= Decimal("0"):
            session.final_alignment_status = FinalAlignmentStatus.NOT_NEEDED
            session.final_unaligned_qty = Decimal("0")
            return

        stage1_side, stage1_position_side, stage1_selector = self._close_stage1_params(spec.trend_bias)
        stage2_side, stage2_position_side, stage2_selector = self._close_stage2_params(spec.trend_bias)
        overview = await self._gateway.get_account_overview()
        stage1_available_qty = self._position_qty(overview, spec.symbol, stage1_position_side)
        stage2_available_qty = self._position_qty(overview, spec.symbol, stage2_position_side)
        quote = await self._gateway.get_quote(spec.symbol)
        stage1_price = normalize_price(stage1_selector(quote), rules)
        stage2_price = normalize_price(stage2_selector(quote), rules)
        self._repository.add_event(
            session.session_id,
            "close_final_alignment_started",
            {
                "symbol": spec.symbol,
                "carryover_qty": str(carryover_qty),
                "stage1_available_qty": str(stage1_available_qty),
                "stage2_available_qty": str(stage2_available_qty),
            },
        )

        try:
            if stage2_available_qty < carryover_qty:
                raise ExchangeStateError(
                    f"双向平仓最终对齐时可平仓位不足: stage2_available={stage2_available_qty}, carryover={carryover_qty}"
                )
            try:
                validate_qty_and_notional(carryover_qty, stage2_price, rules)
                await self._market_fill_remaining(
                    session=session,
                    round_index=spec.round_count,
                    side=stage2_side,
                    position_side=stage2_position_side,
                    remaining_qty=carryover_qty,
                    attempts=max(spec.market_fallback_attempts, 1),
                    label="close_final_alignment_direct",
                    control=control,
                )
                session.completed_with_final_alignment = True
                session.stage2_carryover_qty = Decimal("0")
                session.final_unaligned_qty = Decimal("0")
                session.final_alignment_status = FinalAlignmentStatus.MARKET_ALIGNED
                self._repository.add_event(
                    session.session_id,
                    "close_final_alignment_completed",
                    {
                        "mode": "direct_market",
                        "carryover_qty": str(carryover_qty),
                    },
                )
                return
            except ValueError:
                pass

            stage1_min_qty = min_qty_for_notional(stage1_price, rules)
            stage1_reduce_qty = stage1_min_qty
            stage2_reduce_qty = normalize_qty(stage1_min_qty + carryover_qty, rules)
            if stage1_available_qty < stage1_reduce_qty or stage2_available_qty < stage2_reduce_qty:
                await self._flatten_close_positions(
                    session=session,
                    control=control,
                    stage1_side=stage1_side,
                    stage1_position_side=stage1_position_side,
                    stage1_available_qty=stage1_available_qty,
                    stage2_side=stage2_side,
                    stage2_position_side=stage2_position_side,
                    stage2_available_qty=stage2_available_qty,
                )
                session.completed_with_final_alignment = True
                session.stage2_carryover_qty = Decimal("0")
                session.final_unaligned_qty = Decimal("0")
                session.final_alignment_status = FinalAlignmentStatus.FLATTENED_BOTH_SIDES
                self._repository.add_event(
                    session.session_id,
                    "close_final_alignment_completed",
                    {
                        "mode": "flattened_both_sides",
                        "stage1_available_qty": str(stage1_available_qty),
                        "stage2_available_qty": str(stage2_available_qty),
                    },
                )
                return

            await self._market_fill_remaining(
                session=session,
                round_index=spec.round_count,
                side=stage1_side,
                position_side=stage1_position_side,
                remaining_qty=stage1_reduce_qty,
                attempts=max(spec.market_fallback_attempts, 1),
                label="close_final_alignment_stage1",
                control=control,
            )
            await self._market_fill_remaining(
                session=session,
                round_index=spec.round_count,
                side=stage2_side,
                position_side=stage2_position_side,
                remaining_qty=stage2_reduce_qty,
                attempts=max(spec.market_fallback_attempts, 1),
                label="close_final_alignment_stage2",
                control=control,
            )
            residual_stage1_qty = normalize_qty(stage1_available_qty - stage1_reduce_qty, rules)
            residual_stage2_qty = normalize_qty(stage2_available_qty - stage2_reduce_qty, rules)
            if residual_stage1_qty != residual_stage2_qty:
                raise ExchangeStateError(
                    f"双向平仓最终对齐未收敛: residual_stage1={residual_stage1_qty}, residual_stage2={residual_stage2_qty}"
                )
            session.completed_with_final_alignment = True
            session.stage2_carryover_qty = Decimal("0")
            session.final_unaligned_qty = Decimal("0")
            session.final_alignment_status = FinalAlignmentStatus.MARKET_ALIGNED
            self._repository.add_event(
                session.session_id,
                "close_final_alignment_completed",
                {
                    "mode": "balanced_market_reduce",
                    "residual_qty": str(residual_stage1_qty),
                },
            )
        except Exception as exc:
            session.final_alignment_status = FinalAlignmentStatus.FAILED
            session.final_unaligned_qty = session.stage2_carryover_qty
            self._repository.add_event(
                session.session_id,
                "close_final_alignment_failed",
                {"error": str(exc), "carryover_qty": str(session.stage2_carryover_qty)},
            )
            raise

    async def _flatten_close_positions(
        self,
        *,
        session: OpenSession,
        control: SessionControl,
        stage1_side: OrderSide,
        stage1_position_side: PositionSide,
        stage1_available_qty: Decimal,
        stage2_side: OrderSide,
        stage2_position_side: PositionSide,
        stage2_available_qty: Decimal,
    ) -> None:
        self._repository.add_event(
            session.session_id,
            "close_final_alignment_flatten_both_sides",
            {
                "stage1_available_qty": str(stage1_available_qty),
                "stage2_available_qty": str(stage2_available_qty),
            },
        )
        if stage1_available_qty > Decimal("0"):
            await self._market_fill_remaining(
                session=session,
                round_index=session.spec.round_count,
                side=stage1_side,
                position_side=stage1_position_side,
                remaining_qty=stage1_available_qty,
                attempts=max(session.spec.market_fallback_attempts, 1),
                label="close_final_alignment_flatten_stage1",
                control=control,
            )
        if stage2_available_qty > Decimal("0"):
            await self._market_fill_remaining(
                session=session,
                round_index=session.spec.round_count,
                side=stage2_side,
                position_side=stage2_position_side,
                remaining_qty=stage2_available_qty,
                attempts=max(session.spec.market_fallback_attempts, 1),
                label="close_final_alignment_flatten_stage2",
                control=control,
            )
    def _close_stage1_params(self, trend_bias: TrendBias) -> tuple[OrderSide, PositionSide, Callable[[Quote], Decimal]]:
        if trend_bias == TrendBias.LONG:
            return OrderSide.BUY, PositionSide.SHORT, lambda quote: quote.ask_price
        return OrderSide.SELL, PositionSide.LONG, lambda quote: quote.bid_price

    def _close_stage2_params(self, trend_bias: TrendBias) -> tuple[OrderSide, PositionSide, Callable[[Quote], Decimal]]:
        if trend_bias == TrendBias.LONG:
            return OrderSide.SELL, PositionSide.LONG, lambda quote: quote.bid_price
        return OrderSide.BUY, PositionSide.SHORT, lambda quote: quote.ask_price


