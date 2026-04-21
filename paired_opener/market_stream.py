from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from app_i18n.runtime import CATALOG_VERSION, CONTRACT_VERSION, DEFAULT_ACCOUNT_NAME, format_copy
from paired_opener.config import Settings
from paired_opener.domain import FinalAlignmentStatus, OrderSide, PositionSide, Quote, SessionKind, SingleCloseMode, SingleOpenMode, SymbolRules, TrendBias
from paired_opener.errors import ExchangeStateError
from paired_opener.execution_policy import compute_market_fallback_qty, evaluate_price_guard, resolve_execution_policy
from paired_opener.exchange import ExchangeGateway
from paired_opener.rounding import min_qty_for_notional, normalize_price, normalize_qty, validate_qty_and_notional

SYSTEM_ORDER_ID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-', re.IGNORECASE)


class MarketStreamController:
    def __init__(
        self,
        gateway: ExchangeGateway,
        settings: Settings | None = None,
        account_id: str = "default",
        account_name: str = DEFAULT_ACCOUNT_NAME,
    ) -> None:
        self._gateway = gateway
        self._settings = settings or Settings()
        self._account_id = account_id
        self._account_name = account_name
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._lock = asyncio.Lock()
        self._simulation_lock = asyncio.Lock()
        self._simulation_abort_event = asyncio.Event()
        self._simulation_abort_requested = False
        self._market_task: asyncio.Task[None] | None = None
        self._account_task: asyncio.Task[None] | None = None
        self._disconnect_task: asyncio.Task[dict[str, Any]] | None = None
        self._last_account_error: str | None = None
        self._last_orderbook_signature: tuple[Any, ...] | None = None
        self._orderbook_interval_seconds = 0.25
        self._account_refresh_interval_seconds = max(self._settings.market_account_refresh_interval_ms / 1000, 1.0)
        self._state = {
            "contract_version": CONTRACT_VERSION,
            "catalog_version": CATALOG_VERSION,
            "connected": False,
            "status": "disconnected",
            "symbol": "BTCUSDT",
            "account_id": self._account_id,
            "account_name": self._account_name,
            "updated_at": self._utc_now(),
            "message_code": "runtime.connection_disconnected",
            "message_params": {},
            "message": format_copy("runtime.connection_disconnected"),
        }
        self._account_overview = {
            "contract_version": CONTRACT_VERSION,
            "catalog_version": CATALOG_VERSION,
            "status": "idle",
            "symbol": "BTCUSDT",
            "account_id": self._account_id,
            "account_name": self._account_name,
            "updated_at": self._utc_now(),
            "message_code": "runtime.connection_disconnected",
            "message_params": {},
            "message": format_copy("runtime.connection_disconnected"),
            "system_open_order_count": 0,
            "manual_open_order_count": 0,
            "totals": {
                "equity": "0",
                "margin": "0",
                "available_balance": "0",
                "unrealized_pnl": "0",
            },
            "positions": [],
        }
        self._execution_stats = {
            "contract_version": CONTRACT_VERSION,
            "catalog_version": CATALOG_VERSION,
            "status": "idle",
            "session_kind": SessionKind.PAIRED_OPEN.value,
            "mode": None,
            "selected_position_side": None,
            "symbol": "BTCUSDT",
            "rounds_total": 0,
            "rounds_completed": 0,
            "total_notional": "0",
            "notional_per_round": "0",
            "last_qty": "0",
            "min_notional": "0",
            "carryover_qty": "0",
            "final_alignment_status": "not_needed",
            "resolved_execution_profile": "balanced",
            "resolved_market_fallback_max_ratio": "1",
            "resolved_market_fallback_min_residual_qty": "0",
            "resolved_max_reprice_ticks": 8,
            "resolved_max_spread_bps": 20,
            "resolved_max_reference_deviation_bps": 40,
            "message_code": "runtime.connection_idle",
            "message_params": {},
            "message": format_copy("runtime.connection_idle"),
            "updated_at": self._utc_now(),
        }
        self._resolved_simulation_policy = resolve_execution_policy(
            self._settings,
            session_kind=SessionKind.PAIRED_CLOSE,
        )
        self._resolved_simulation_policy_payload = self._resolved_simulation_policy.to_payload(prefix="resolved_")

    def _utc_now(self) -> str:
        return datetime.now(UTC).isoformat()

    def _normalize(self, payload: Any) -> Any:
        if isinstance(payload, Decimal):
            return str(payload)
        if isinstance(payload, datetime):
            return payload.isoformat()
        if isinstance(payload, dict):
            return {key: self._normalize(value) for key, value in payload.items()}
        if isinstance(payload, list):
            return [self._normalize(value) for value in payload]
        return payload

    def _message_fields(
        self,
        *,
        message_code: str | None = None,
        message_params: dict[str, Any] | None = None,
        message: str | None = None,
        legacy_key: str = "runtime.execution_legacy_message",
    ) -> dict[str, Any]:
        params = dict(message_params or {})
        resolved_key = message_code
        if resolved_key is None and message:
            resolved_key = legacy_key
            params = {"message": message}
        rendered = format_copy(resolved_key, params) if resolved_key else str(message or "")
        return {
            "contract_version": CONTRACT_VERSION,
            "catalog_version": CATALOG_VERSION,
            "message_code": resolved_key,
            "message_params": params,
            "message": rendered,
        }

    def _execution_log_payload(
        self,
        *,
        level: str,
        message_code: str | None = None,
        message_params: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "level": level,
            "created_at": self._utc_now(),
        }
        payload.update(
            self._message_fields(
                message_code=message_code or "runtime.execution_message_unavailable",
                message_params=message_params,
            )
        )
        return payload

    async def publish(self, event: str, payload: dict[str, Any]) -> None:
        message = {"event": event, "data": self._normalize(payload)}
        stale: list[asyncio.Queue[dict[str, Any]]] = []
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                stale.append(queue)
        for queue in stale:
            self._subscribers.discard(queue)

    async def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        if self._disconnect_task is not None and not self._disconnect_task.done():
            self._disconnect_task.cancel()
        self._disconnect_task = None
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self._settings.sse_queue_maxsize)
        self._subscribers.add(queue)
        await queue.put({"event": "connection_status", "data": self._normalize(self._state)})
        await queue.put({"event": "account_overview", "data": self._normalize(self._account_overview)})
        await queue.put({"event": "execution_stats", "data": self._normalize(self._execution_stats)})
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(queue)
        if (
            not self._subscribers
            and (self._market_task is not None or self._account_task is not None)
            and (self._disconnect_task is None or self._disconnect_task.done())
        ):
            self._disconnect_task = asyncio.create_task(self._disconnect_after_unsubscribe())

    async def connect(self, symbol: str) -> dict:
        symbol = symbol.upper()
        if self._disconnect_task is not None and not self._disconnect_task.done():
            self._disconnect_task.cancel()
        self._disconnect_task = None
        async with self._lock:
            old_market_task = self._market_task
            old_account_task = self._account_task
            self._market_task = None
            self._account_task = None
            self._last_orderbook_signature = None
            self._state = {
                "connected": False,
                "status": "connecting",
                "symbol": symbol,
                "account_id": self._account_id,
                "account_name": self._account_name,
                "updated_at": self._utc_now(),
                "message_code": "runtime.connection_connecting",
                "message_params": {},
                "message": format_copy("runtime.connection_connecting"),
            }
            self._account_overview = {
                **self._account_overview,
                "status": "loading",
                "symbol": symbol,
                "updated_at": self._utc_now(),
                "message_code": "runtime.monitor_waiting_connection",
                "message_params": {},
                "message": format_copy("runtime.monitor_waiting_connection"),
            }
        await self._cancel_tasks(old_market_task, old_account_task)
        await self.publish("connection_status", self._state)
        await self.publish("account_overview", self._account_overview)
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level="info",
                message_code="log.market_stream.connect_started",
                message_params={"symbol": symbol},
            ),
        )
        async with self._lock:
            self._market_task = asyncio.create_task(self._market_loop(symbol))
            self._account_task = asyncio.create_task(self._account_loop(symbol))
            return self._normalize(self._state)

    async def _disconnect_after_unsubscribe(self) -> dict:
        try:
            return await self.disconnect()
        finally:
            self._disconnect_task = None

    async def disconnect(self) -> dict:
        current_task = asyncio.current_task()
        if (
            self._disconnect_task is not None
            and self._disconnect_task is not current_task
            and not self._disconnect_task.done()
        ):
            self._disconnect_task.cancel()
        async with self._lock:
            market_task = self._market_task
            account_task = self._account_task
            self._market_task = None
            self._account_task = None
            symbol = self._state.get("symbol", "BTCUSDT")
        await self._cancel_tasks(market_task, account_task)
        self._last_orderbook_signature = None
        self._state = {
            "connected": False,
            "status": "disconnected",
            "symbol": symbol,
            "account_id": self._account_id,
            "account_name": self._account_name,
            "updated_at": self._utc_now(),
            "message_code": "runtime.connection_disconnected",
            "message_params": {},
            "message": format_copy("runtime.connection_disconnected"),
        }
        self._account_overview = {
            **self._account_overview,
            "symbol": symbol,
            "status": "idle",
            "updated_at": self._utc_now(),
            "message_code": "runtime.connection_disconnected",
            "message_params": {},
            "message": format_copy("runtime.connection_disconnected"),
        }
        await self.publish("connection_status", self._state)
        await self.publish("account_overview", self._account_overview)
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level="warn",
                message_code="log.market_stream.connect_stopped",
            ),
        )
        return self._normalize(self._state)

    async def _cancel_tasks(self, *tasks: asyncio.Task[Any] | None) -> None:
        active_tasks = [task for task in tasks if task is not None and not task.done()]
        if not active_tasks:
            return
        for task in active_tasks:
            task.cancel()
        await asyncio.gather(*active_tasks, return_exceptions=True)

    async def _refresh_account_overview(self) -> None:
        symbol = str(self._state.get("symbol") or self._account_overview.get("symbol") or "BTCUSDT").upper()
        try:
            overview = await self._gateway.get_account_overview()
            open_orders = await self._gateway.get_open_orders(symbol)
            system_open_order_count = 0
            manual_open_order_count = 0
            for order in open_orders:
                client_order_id = str(order.get("clientOrderId") or "")
                if SYSTEM_ORDER_ID_RE.match(client_order_id):
                    system_open_order_count += 1
                else:
                    manual_open_order_count += 1
            overview = {
                **overview,
                "status": "ok",
                "account_id": self._account_id,
                "account_name": self._account_name,
                "symbol": symbol,
                "system_open_order_count": system_open_order_count,
                "manual_open_order_count": manual_open_order_count,
                "updated_at": self._utc_now(),
                "message_code": "runtime.account_snapshot_updated",
                "message_params": {},
                "message": format_copy("runtime.account_snapshot_updated"),
            }
            self._account_overview = overview
            await self.publish("account_overview", self._account_overview)
            if self._last_account_error is not None:
                await self.publish(
                    "execution_log",
                    self._execution_log_payload(
                        level="success",
                        message_code="log.account_overview.recovered",
                    ),
                )
            self._last_account_error = None
        except Exception as exc:
            error_message = str(exc)
            self._account_overview = {
                **self._account_overview,
                "status": "error",
                "symbol": symbol,
                "updated_at": self._utc_now(),
                "message_code": "runtime.monitor_account_refresh_failed",
                "message_params": {},
                "message": format_copy("runtime.monitor_account_refresh_failed"),
            }
            await self.publish("account_overview", self._account_overview)
            if error_message != self._last_account_error:
                await self.publish(
                    "execution_log",
                    self._execution_log_payload(
                        level="warn",
                        message_code="log.account_overview.refresh_failed",
                    ),
                )
            self._last_account_error = error_message

    def _orderbook_signature(self, snapshot: dict[str, Any]) -> tuple[Any, ...]:
        asks = tuple((str(level.get("price")), str(level.get("qty"))) for level in snapshot.get("asks", [])[:2])
        bids = tuple((str(level.get("price")), str(level.get("qty"))) for level in snapshot.get("bids", [])[:2])
        return snapshot.get("lastUpdateId"), asks, bids

    async def _publish_orderbook(self, symbol: str, snapshot: dict[str, Any]) -> None:
        raw_asks = snapshot.get("asks") or []
        raw_bids = snapshot.get("bids") or []
        asks = [raw_asks[index] for index in (0, 1) if index < len(raw_asks)]
        bids = [raw_bids[index] for index in (0, 1) if index < len(raw_bids)]
        max_qty = max([level["qty"] for level in asks + bids], default=Decimal("1"))
        await self.publish(
            "orderbook",
            {
                "symbol": symbol,
                "updated_at": self._utc_now(),
                "asks": [
                    {
                        "price": level["price"],
                        "qty": level["qty"],
                        "depth_ratio": (level["qty"] / max_qty) if max_qty > 0 else Decimal("0"),
                    }
                    for level in asks
                ],
                "bids": [
                    {
                        "price": level["price"],
                        "qty": level["qty"],
                        "depth_ratio": (level["qty"] / max_qty) if max_qty > 0 else Decimal("0"),
                    }
                    for level in bids
                ],
            },
        )

    async def _market_loop(self, symbol: str) -> None:
        self._state = {
            "connected": True,
            "status": "connected",
            "symbol": symbol,
            "account_id": self._account_id,
            "account_name": self._account_name,
            "updated_at": self._utc_now(),
            "message": f"已连接 {symbol}",
        }
        await self.publish("connection_status", self._state)
        try:
            while True:
                snapshot = await self._gateway.get_order_book(symbol, limit=5)
                signature = self._orderbook_signature(snapshot)
                if signature != self._last_orderbook_signature:
                    self._last_orderbook_signature = signature
                    await self._publish_orderbook(symbol, snapshot)
                await asyncio.sleep(self._orderbook_interval_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._state = {
                "connected": False,
                "status": "error",
                "symbol": symbol,
                "account_id": self._account_id,
                "account_name": self._account_name,
                "updated_at": self._utc_now(),
                "message_code": "runtime.connection_error",
                "message_params": {},
                "message": format_copy("runtime.connection_error"),
            }
            await self.publish("connection_status", self._state)
            await self.publish(
                "execution_log",
                self._execution_log_payload(
                    level="error",
                    message_code="log.market_stream.orderbook_refresh_failed",
                    message_params={"symbol": symbol},
                ),
            )
            async with self._lock:
                self._market_task = None

    async def _account_loop(self, symbol: str) -> None:
        try:
            while True:
                await self._refresh_account_overview()
                await asyncio.sleep(self._account_refresh_interval_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self.publish(
                "execution_log",
                self._execution_log_payload(
                    level="warn",
                    message_code="log.account_overview.polling_failed",
                ),
            )
            async with self._lock:
                self._account_task = None

    async def _run_paired_open_simulation(
        self,
        *,
        symbol: str,
        trend_bias: TrendBias,
        open_amount: Decimal,
        leverage: int,
        round_count: int,
        round_interval_seconds: int = 3,
    ) -> dict[str, Any]:
        symbol = symbol.upper()
        rules = await self._gateway.get_symbol_rules(symbol)
        available_balance: Decimal | None = None
        try:
            available_balance = self._available_balance(await self._gateway.get_account_overview())
        except Exception:
            available_balance = None
        if available_balance is not None:
            max_open_amount = available_balance * Decimal("0.95")
            if open_amount > max_open_amount:
                return await self._block_simulation(
                    session_kind=SessionKind.PAIRED_OPEN,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    message_code="log.simulation.paired_open.balance_limit_exceeded",
                    message_params={
                        "open_amount": self._stringify_decimal(open_amount),
                        "available_balance": self._stringify_decimal(available_balance),
                        "max_open_amount": self._stringify_decimal(max_open_amount),
                    },
                    mode=trend_bias.value,
                )

        total_notional = open_amount * Decimal(leverage)
        notional_per_round = total_notional / Decimal(round_count)
        if notional_per_round < rules.min_notional:
            return await self._block_simulation(
                session_kind=SessionKind.PAIRED_OPEN,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message=f"模拟执行已阻止：每轮开单金额 {notional_per_round} 低于交易所最小下单金额 {rules.min_notional}",
                message_code="log.simulation.blocked_min_notional",
                message_params={
                    "per_round_notional": self._stringify_decimal(notional_per_round),
                    "min_notional": self._stringify_decimal(rules.min_notional),
                    "symbol": symbol,
                },
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                mode=trend_bias.value,
            )

        initial_quote = await self._gateway.get_quote(symbol)
        initial_bid = normalize_price(initial_quote.bid_price, rules)
        initial_ask = normalize_price(initial_quote.ask_price, rules)
        if trend_bias == TrendBias.LONG:
            stage1_position_side = PositionSide.LONG
            stage1_reference_price = initial_bid
            stage2_position_side = PositionSide.SHORT
            stage2_reference_price = initial_ask
        else:
            stage1_position_side = PositionSide.SHORT
            stage1_reference_price = initial_ask
            stage2_position_side = PositionSide.LONG
            stage2_reference_price = initial_bid
        blocked = await self._maybe_block_for_price_guard(
            session_kind=SessionKind.PAIRED_OPEN,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            side=self._open_order_side(stage1_position_side),
            quote=initial_quote,
            candidate_price=stage1_reference_price,
            reference_price=stage1_reference_price,
            rules=rules,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            mode=trend_bias.value,
        )
        if blocked is not None:
            return blocked
        blocked = await self._maybe_block_for_price_guard(
            session_kind=SessionKind.PAIRED_OPEN,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            side=self._open_order_side(stage2_position_side),
            quote=initial_quote,
            candidate_price=stage2_reference_price,
            reference_price=stage2_reference_price,
            rules=rules,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            mode=trend_bias.value,
        )
        if blocked is not None:
            return blocked

        await self._set_execution_stats(
            status="running",
            session_kind=SessionKind.PAIRED_OPEN,
            symbol=symbol,
            round_count=round_count,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            min_notional=rules.min_notional,
            mode=trend_bias.value,
            message_code="log.simulation.paired_open_running",
        )
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level="info",
                message_code="log.simulation.paired_open_started",
                message_params={
                    "symbol": symbol,
                    "mode": trend_bias.value,
                    "open_amount": self._stringify_decimal(open_amount),
                    "leverage": leverage,
                    "round_count": round_count,
                },
            ),
        )
        aborted = await self._abort_simulation_if_requested(
            session_kind=SessionKind.PAIRED_OPEN,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            mode=trend_bias.value,
        )
        if aborted is not None:
            return aborted

        last_qty = Decimal("0")
        carryover_qty = Decimal("0")
        rounds_completed = 0
        skipped_rounds = 0
        session_long_qty = Decimal("0")
        session_short_qty = Decimal("0")
        for round_index in range(1, round_count + 1):
            aborted = await self._abort_simulation_if_requested(
                session_kind=SessionKind.PAIRED_OPEN,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                mode=trend_bias.value,
                rounds_completed=rounds_completed,
                carryover_qty=carryover_qty,
                final_alignment_status=(
                    FinalAlignmentStatus.CARRYOVER_PENDING if carryover_qty > Decimal("0") else FinalAlignmentStatus.NOT_NEEDED
                ),
            )
            if aborted is not None:
                return aborted
            snapshot = await self._gateway.get_order_book(symbol, limit=10)
            ask1 = normalize_price(Decimal(str(snapshot["asks"][0]["price"])), rules)
            bid1 = normalize_price(Decimal(str(snapshot["bids"][0]["price"])), rules)
            live_quote = Quote(symbol=symbol, bid_price=bid1, ask_price=ask1)
            if trend_bias == TrendBias.LONG:
                stage1_position_side = PositionSide.LONG
                stage1_side = stage1_position_side.value
                stage1_price = bid1
                stage2_position_side = PositionSide.SHORT
                stage2_side = stage2_position_side.value
                stage2_price = ask1
            else:
                stage1_position_side = PositionSide.SHORT
                stage1_side = stage1_position_side.value
                stage1_price = ask1
                stage2_position_side = PositionSide.LONG
                stage2_side = stage2_position_side.value
                stage2_price = bid1
            last_qty = normalize_qty((notional_per_round / stage1_price) if stage1_price > 0 else Decimal("0"), rules)
            remaining_qty_on_guard = normalize_qty(
                carryover_qty + (last_qty * Decimal(max(round_count - round_index + 1, 0))),
                rules,
            )

            blocked = await self._maybe_block_for_price_guard(
                session_kind=SessionKind.PAIRED_OPEN,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                side=self._open_order_side(stage1_position_side),
                quote=live_quote,
                candidate_price=stage1_price,
                reference_price=stage1_price,
                rules=rules,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                mode=trend_bias.value,
                rounds_completed=rounds_completed,
                carryover_qty=carryover_qty,
                final_alignment_status=FinalAlignmentStatus.CARRYOVER_PENDING,
                remaining_qty_on_guard=remaining_qty_on_guard,
            )
            if blocked is not None:
                return blocked
            stage1_filled_qty, _ = self._simulate_leg_fill(
                requested_qty=last_qty,
                rules=rules,
                allow_market_fallback=False,
            )
            if stage1_filled_qty <= Decimal("0"):
                skipped_rounds += 1
                await self.publish(
                    "execution_log",
                    self._execution_log_payload(
                        level="warn",
                        message_code="log.simulation.paired_open_stage1_skipped",
                        message_params={
                            "round_index": round_index,
                            "stage1_side": stage1_side,
                            "stage1_price": self._stringify_decimal(stage1_price),
                        },
                    ),
                )
                await self._set_execution_stats(
                    status="running",
                    session_kind=SessionKind.PAIRED_OPEN,
                    symbol=symbol,
                    round_count=round_count,
                    rounds_completed=rounds_completed,
                    total_notional=total_notional,
                    notional_per_round=notional_per_round,
                    last_qty=last_qty,
                    min_notional=rules.min_notional,
                    mode=trend_bias.value,
                    carryover_qty=carryover_qty,
                    final_alignment_status=(
                        FinalAlignmentStatus.CARRYOVER_PENDING if carryover_qty > Decimal("0") else FinalAlignmentStatus.NOT_NEEDED
                    ),
                    message_code="log.simulation.paired_open_stage1_skipped_running",
                    message_params={"round_index": round_index},
                )
                if round_index < round_count and round_interval_seconds > 0:
                    aborted = await self._wait_or_abort(
                        round_interval_seconds,
                        session_kind=SessionKind.PAIRED_OPEN,
                        symbol=symbol,
                        round_count=round_count,
                        min_notional=rules.min_notional,
                        total_notional=total_notional,
                        notional_per_round=notional_per_round,
                        last_qty=last_qty,
                        mode=trend_bias.value,
                        rounds_completed=rounds_completed,
                        carryover_qty=carryover_qty,
                        final_alignment_status=(
                            FinalAlignmentStatus.CARRYOVER_PENDING if carryover_qty > Decimal("0") else FinalAlignmentStatus.NOT_NEEDED
                        ),
                    )
                    if aborted is not None:
                        return aborted
                continue
            stage2_quote = await self._gateway.get_quote(symbol)
            stage2_bid = Decimal(stage2_quote.bid_price)
            stage2_ask = Decimal(stage2_quote.ask_price)
            stage2_price = stage2_bid if stage2_position_side == PositionSide.LONG else stage2_ask
            stage2_target_qty = normalize_qty(stage1_filled_qty + carryover_qty, rules)
            stage2_remaining_on_guard = normalize_qty(
                stage2_target_qty + (last_qty * Decimal(max(round_count - round_index, 0))),
                rules,
            )
            blocked = await self._maybe_block_for_price_guard(
                session_kind=SessionKind.PAIRED_OPEN,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                side=self._open_order_side(stage2_position_side),
                quote=stage2_quote,
                candidate_price=stage2_price,
                reference_price=stage2_price,
                rules=rules,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                mode=trend_bias.value,
                rounds_completed=rounds_completed + 1,
                carryover_qty=carryover_qty,
                has_partial_progress=stage1_filled_qty > Decimal("0"),
                final_alignment_status=FinalAlignmentStatus.CARRYOVER_PENDING,
                remaining_qty_on_guard=stage2_remaining_on_guard,
            )
            if blocked is not None:
                return blocked
            stage2_filled_qty, carryover_qty = self._simulate_leg_fill(
                requested_qty=stage2_target_qty,
                rules=rules,
                allow_market_fallback=True,
            )
            rounds_completed += 1
            if stage1_position_side == PositionSide.LONG:
                session_long_qty = normalize_qty(session_long_qty + stage1_filled_qty, rules)
            else:
                session_short_qty = normalize_qty(session_short_qty + stage1_filled_qty, rules)
            if stage2_position_side == PositionSide.LONG:
                session_long_qty = normalize_qty(session_long_qty + stage2_filled_qty, rules)
            else:
                session_short_qty = normalize_qty(session_short_qty + stage2_filled_qty, rules)

            round_residual_qty = normalize_qty(abs(stage1_filled_qty - stage2_filled_qty), rules)
            if stage1_filled_qty >= stage2_filled_qty:
                residual_side_label = self._position_side_log_label(stage1_position_side)
            else:
                residual_side_label = self._position_side_log_label(stage2_position_side)
            wait_seconds = round_interval_seconds if round_index < round_count and round_interval_seconds > 0 else None
            round_message_code, round_message_params = self._paired_open_round_summary_contract(
                round_index=round_index,
                stage1_side=stage1_side,
                stage1_price=stage1_price,
                stage1_filled_qty=stage1_filled_qty,
                stage2_side=stage2_side,
                stage2_price=stage2_price,
                stage2_filled_qty=stage2_filled_qty,
                round_residual_qty=round_residual_qty,
                residual_side_label=residual_side_label,
                carryover_qty=carryover_qty,
                wait_seconds=wait_seconds,
            )
            await self.publish(
                "execution_log",
                self._execution_log_payload(
                    level="info",
                    message_code=round_message_code,
                    message_params=round_message_params,
                ),
            )
            await self._set_execution_stats(
                status="running",
                session_kind=SessionKind.PAIRED_OPEN,
                symbol=symbol,
                round_count=round_count,
                rounds_completed=rounds_completed,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                min_notional=rules.min_notional,
                mode=trend_bias.value,
                carryover_qty=carryover_qty,
                final_alignment_status=(
                    FinalAlignmentStatus.CARRYOVER_PENDING if carryover_qty > Decimal("0") else FinalAlignmentStatus.NOT_NEEDED
                ),
                message_code="log.simulation.paired_open_round_running",
                message_params={"round_index": round_index},
            )
            if round_index < round_count and round_interval_seconds > 0:
                aborted = await self._wait_or_abort(
                    round_interval_seconds,
                    session_kind=SessionKind.PAIRED_OPEN,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    total_notional=total_notional,
                    notional_per_round=notional_per_round,
                    last_qty=last_qty,
                    mode=trend_bias.value,
                    rounds_completed=rounds_completed,
                    carryover_qty=carryover_qty,
                    final_alignment_status=(
                        FinalAlignmentStatus.CARRYOVER_PENDING if carryover_qty > Decimal("0") else FinalAlignmentStatus.NOT_NEEDED
                    ),
                )
                if aborted is not None:
                    return aborted

        final_alignment_status = FinalAlignmentStatus.NOT_NEEDED
        aborted = await self._abort_simulation_if_requested(
            session_kind=SessionKind.PAIRED_OPEN,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            mode=trend_bias.value,
            rounds_completed=rounds_completed,
            carryover_qty=carryover_qty,
            final_alignment_status=(
                FinalAlignmentStatus.CARRYOVER_PENDING if carryover_qty > Decimal("0") else FinalAlignmentStatus.NOT_NEEDED
            ),
        )
        if aborted is not None:
            return aborted
        if carryover_qty > Decimal("0"):
            session_long_qty, session_short_qty, carryover_qty, final_alignment_status = await self._simulate_paired_open_final_alignment(
                symbol=symbol,
                rules=rules,
                session_long_qty=session_long_qty,
                session_short_qty=session_short_qty,
                carryover_qty=carryover_qty,
            )
        aborted = await self._abort_simulation_if_requested(
            session_kind=SessionKind.PAIRED_OPEN,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            mode=trend_bias.value,
            rounds_completed=rounds_completed,
            carryover_qty=carryover_qty,
            final_alignment_status=(
                final_alignment_status
                if final_alignment_status != FinalAlignmentStatus.NOT_NEEDED or carryover_qty <= Decimal("0")
                else FinalAlignmentStatus.CARRYOVER_PENDING
            ),
        )
        if aborted is not None:
            return aborted

        completion_message_code = (
            "log.simulation.paired_open_completed"
            if carryover_qty <= Decimal("0")
            else "log.simulation.paired_open_completed_with_residual"
        )
        completion_message_params = {
            "symbol": symbol,
            "completed_rounds": rounds_completed,
        }
        if completion_message_code == "log.simulation.paired_open_completed_with_residual":
            completion_message_params["carryover_qty"] = self._stringify_decimal(carryover_qty)
        payload = await self._set_execution_stats(
            status=("completed_with_skips" if carryover_qty > Decimal("0") or skipped_rounds > 0 else "completed"),
            session_kind=SessionKind.PAIRED_OPEN,
            symbol=symbol,
            round_count=round_count,
            rounds_completed=rounds_completed,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            min_notional=rules.min_notional,
            mode=trend_bias.value,
            carryover_qty=carryover_qty,
            final_alignment_status=(
                final_alignment_status
                if final_alignment_status != FinalAlignmentStatus.NOT_NEEDED or carryover_qty <= Decimal("0")
                else FinalAlignmentStatus.CARRYOVER_PENDING
            ),
            message_code=completion_message_code,
            message_params=completion_message_params,
        )
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level=("success" if carryover_qty <= Decimal("0") and skipped_rounds <= 0 else "warn"),
                message_code=completion_message_code,
                message_params=completion_message_params,
            ),
        )
        return payload

    def _stringify_decimal(self, value: Decimal | int | str) -> str:
        return str(Decimal(str(value)))

    def _position_qty(self, overview: dict[str, Any], symbol: str, position_side: PositionSide) -> Decimal:
        for item in overview.get("positions", []):
            if item.get("symbol") != symbol:
                continue
            if str(item.get("position_side")) != position_side.value:
                continue
            return Decimal(str(item.get("qty") or "0"))
        return Decimal("0")

    def _position_leverage(self, overview: dict[str, Any], symbol: str) -> int | None:
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

    def _available_balance(self, overview: dict[str, Any]) -> Decimal | None:
        totals = overview.get("totals") or {}
        if overview.get("status") != "ok" or "available_balance" not in totals:
            return None
        return Decimal(str(totals.get("available_balance") or "0"))

    def _simulation_payload(
        self,
        *,
        status: str,
        session_kind: SessionKind,
        symbol: str,
        round_count: int,
        rounds_completed: int = 0,
        total_notional: Decimal = Decimal("0"),
        notional_per_round: Decimal = Decimal("0"),
        last_qty: Decimal = Decimal("0"),
        min_notional: Decimal = Decimal("0"),
        mode: str | None = None,
        selected_position_side: PositionSide | None = None,
        carryover_qty: Decimal = Decimal("0"),
        final_alignment_status: FinalAlignmentStatus | str = FinalAlignmentStatus.NOT_NEEDED,
        message: str = "",
        message_code: str | None = None,
        message_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "contract_version": CONTRACT_VERSION,
            "catalog_version": CATALOG_VERSION,
            "status": status,
            "session_kind": session_kind.value,
            "mode": mode,
            "selected_position_side": selected_position_side.value if selected_position_side else None,
            "symbol": symbol,
            "rounds_total": round_count,
            "rounds_completed": rounds_completed,
            "total_notional": self._stringify_decimal(total_notional),
            "notional_per_round": self._stringify_decimal(notional_per_round),
            "last_qty": self._stringify_decimal(last_qty),
            "min_notional": self._stringify_decimal(min_notional),
            "carryover_qty": self._stringify_decimal(carryover_qty),
            "final_alignment_status": (
                final_alignment_status.value if isinstance(final_alignment_status, FinalAlignmentStatus) else str(final_alignment_status)
            ),
            **self._resolved_simulation_policy_payload,
            "updated_at": self._utc_now(),
        }
        payload.update(
            self._message_fields(
                message_code=message_code,
                message_params=message_params,
                message=message,
                legacy_key="runtime.execution_legacy_message",
            )
        )
        return payload

    async def _set_execution_stats(self, **kwargs: Any) -> dict[str, Any]:
        self._execution_stats = self._simulation_payload(**kwargs)
        await self.publish("execution_stats", self._execution_stats)
        return self._normalize(self._execution_stats)

    def _current_simulation_context(self) -> dict[str, Any]:
        current = self._execution_stats or {}
        session_kind = SessionKind(str(current.get("session_kind") or SessionKind.PAIRED_OPEN.value))
        selected_position_side_raw = current.get("selected_position_side")
        selected_position_side = (
            PositionSide(str(selected_position_side_raw))
            if selected_position_side_raw
            else None
        )
        return {
            "session_kind": session_kind,
            "symbol": str(current.get("symbol") or "BTCUSDT"),
            "round_count": int(current.get("rounds_total") or 0),
            "rounds_completed": int(current.get("rounds_completed") or 0),
            "total_notional": Decimal(str(current.get("total_notional") or "0")),
            "notional_per_round": Decimal(str(current.get("notional_per_round") or "0")),
            "last_qty": Decimal(str(current.get("last_qty") or "0")),
            "min_notional": Decimal(str(current.get("min_notional") or "0")),
            "mode": current.get("mode"),
            "selected_position_side": selected_position_side,
            "carryover_qty": Decimal(str(current.get("carryover_qty") or "0")),
            "final_alignment_status": str(current.get("final_alignment_status") or FinalAlignmentStatus.NOT_NEEDED.value),
        }

    async def _set_current_simulation_stats(
        self,
        *,
        status: str,
        message_code: str,
        message_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        context = self._current_simulation_context()
        return await self._set_execution_stats(
            status=status,
            session_kind=context["session_kind"],
            symbol=context["symbol"],
            round_count=context["round_count"],
            rounds_completed=context["rounds_completed"],
            total_notional=context["total_notional"],
            notional_per_round=context["notional_per_round"],
            last_qty=context["last_qty"],
            min_notional=context["min_notional"],
            mode=context["mode"],
            selected_position_side=context["selected_position_side"],
            carryover_qty=context["carryover_qty"],
            final_alignment_status=context["final_alignment_status"],
            message_code=message_code,
            message_params=message_params,
        )

    def _simulation_is_active(self) -> bool:
        status = str(self._execution_stats.get("status") or "idle")
        return self._simulation_lock.locked() and status not in {"idle", "blocked", "completed", "completed_with_skips", "aborted", "exception"}

    async def abort_simulation(self) -> dict[str, Any]:
        if not self._simulation_is_active():
            return self._normalize(
                {
                    "contract_version": CONTRACT_VERSION,
                    "status": str(self._execution_stats.get("status") or "idle"),
                    "requested": False,
                    "requested_action": "abort",
                    "message_code": "runtime.simulation_abort_not_running",
                    "message_params": {},
                    "message": format_copy("runtime.simulation_abort_not_running"),
                }
            )
        if self._simulation_abort_requested:
            return self._normalize(
                {
                    "contract_version": CONTRACT_VERSION,
                    "status": "aborting",
                    "requested": False,
                    "requested_action": "abort",
                    "message_code": "runtime.simulation_abort_requested",
                    "message_params": {},
                    "message": format_copy("runtime.simulation_abort_requested"),
                }
            )
        self._simulation_abort_requested = True
        self._simulation_abort_event.set()
        await self._set_current_simulation_stats(
            status="aborting",
            message_code="runtime.simulation_abort_requested",
        )
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level="warn",
                message_code="runtime.simulation_abort_requested",
            ),
        )
        return self._normalize(
            {
                "contract_version": CONTRACT_VERSION,
                "status": "aborting",
                "requested": True,
                "requested_action": "abort",
                "message_code": "runtime.simulation_abort_requested",
                "message_params": {},
                "message": format_copy("runtime.simulation_abort_requested"),
            }
        )

    async def _abort_simulation_if_requested(
        self,
        *,
        session_kind: SessionKind,
        symbol: str,
        round_count: int,
        min_notional: Decimal,
        total_notional: Decimal = Decimal("0"),
        notional_per_round: Decimal = Decimal("0"),
        last_qty: Decimal = Decimal("0"),
        mode: str | None = None,
        selected_position_side: PositionSide | None = None,
        rounds_completed: int = 0,
        carryover_qty: Decimal = Decimal("0"),
        final_alignment_status: FinalAlignmentStatus | str = FinalAlignmentStatus.NOT_NEEDED,
    ) -> dict[str, Any] | None:
        if not self._simulation_abort_requested:
            return None
        payload = await self._set_execution_stats(
            status="aborted",
            session_kind=session_kind,
            symbol=symbol,
            round_count=round_count,
            rounds_completed=rounds_completed,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            min_notional=min_notional,
            mode=mode,
            selected_position_side=selected_position_side,
            carryover_qty=carryover_qty,
            final_alignment_status=final_alignment_status,
            message_code="runtime.simulation_aborted",
        )
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level="warn",
                message_code="runtime.simulation_aborted",
                message_params={
                    "symbol": symbol,
                    "rounds_completed": rounds_completed,
                },
            ),
        )
        return payload

    async def _wait_or_abort(
        self,
        seconds: int,
        **abort_kwargs: Any,
    ) -> dict[str, Any] | None:
        if seconds <= 0:
            return await self._abort_simulation_if_requested(**abort_kwargs)
        try:
            await asyncio.wait_for(self._simulation_abort_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return await self._abort_simulation_if_requested(**abort_kwargs)
        return await self._abort_simulation_if_requested(**abort_kwargs)

    async def _block_simulation(
        self,
        *,
        session_kind: SessionKind,
        symbol: str,
        round_count: int,
        min_notional: Decimal,
        message: str = "",
        total_notional: Decimal = Decimal("0"),
        notional_per_round: Decimal = Decimal("0"),
        last_qty: Decimal = Decimal("0"),
        mode: str | None = None,
        selected_position_side: PositionSide | None = None,
        rounds_completed: int = 0,
        carryover_qty: Decimal = Decimal("0"),
        final_alignment_status: FinalAlignmentStatus | str = FinalAlignmentStatus.NOT_NEEDED,
        message_code: str | None = None,
        message_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = await self._set_execution_stats(
            status="blocked",
            session_kind=session_kind,
            symbol=symbol,
            round_count=round_count,
            rounds_completed=rounds_completed,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            min_notional=min_notional,
            mode=mode,
            selected_position_side=selected_position_side,
            carryover_qty=carryover_qty,
            final_alignment_status=final_alignment_status,
            message=message,
            message_code=message_code,
            message_params=message_params,
        )
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level="warn",
                message_code=message_code,
                message_params=message_params,
                message=message,
            ),
        )
        return payload

    async def _complete_simulation_with_skips(
        self,
        *,
        session_kind: SessionKind,
        symbol: str,
        round_count: int,
        min_notional: Decimal,
        message: str,
        total_notional: Decimal = Decimal("0"),
        notional_per_round: Decimal = Decimal("0"),
        last_qty: Decimal = Decimal("0"),
        mode: str | None = None,
        selected_position_side: PositionSide | None = None,
        rounds_completed: int = 0,
        carryover_qty: Decimal = Decimal("0"),
        final_alignment_status: FinalAlignmentStatus | str = FinalAlignmentStatus.NOT_NEEDED,
        message_code: str | None = None,
        message_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = await self._set_execution_stats(
            status="completed_with_skips",
            session_kind=session_kind,
            symbol=symbol,
            round_count=round_count,
            rounds_completed=rounds_completed,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            min_notional=min_notional,
            mode=mode,
            selected_position_side=selected_position_side,
            carryover_qty=carryover_qty,
            final_alignment_status=final_alignment_status,
            message=message,
            message_code=message_code,
            message_params=message_params,
        )
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level="warn",
                message_code=message_code,
                message_params=message_params,
                message=message,
            ),
        )
        return payload

    def _single_side_price(self, position_side: PositionSide, bid_price: Decimal, ask_price: Decimal) -> Decimal:
        return bid_price if position_side == PositionSide.LONG else ask_price

    def _paired_close_stage_prices(self, trend_bias: TrendBias, bid_price: Decimal, ask_price: Decimal) -> tuple[Decimal, Decimal, str, str]:
        if trend_bias == TrendBias.LONG:
            return ask_price, bid_price, "平空", "平多"
        return bid_price, ask_price, "平多", "平空"

    def _position_side_log_label(self, position_side: PositionSide) -> str:
        return "多" if position_side == PositionSide.LONG else "空"

    def _paired_open_round_summary_contract(
        self,
        *,
        round_index: int,
        stage1_side: str,
        stage1_price: Decimal,
        stage1_filled_qty: Decimal,
        stage2_side: str,
        stage2_price: Decimal,
        stage2_filled_qty: Decimal,
        round_residual_qty: Decimal,
        residual_side_label: str,
        carryover_qty: Decimal,
        wait_seconds: int | None = None,
    ) -> tuple[str, dict[str, Any]]:
        has_residual = round_residual_qty > Decimal("0")
        has_wait = wait_seconds is not None and wait_seconds > 0
        if has_residual and has_wait:
            message_code = "log.simulation.paired_open_round_summary_with_residual_wait"
        elif has_residual:
            message_code = "log.simulation.paired_open_round_summary_with_residual"
        elif has_wait:
            message_code = "log.simulation.paired_open_round_summary_aligned_wait"
        else:
            message_code = "log.simulation.paired_open_round_summary_aligned"
        message_params = {
            "round_index": round_index,
            "stage1_side": stage1_side,
            "stage1_price": self._stringify_decimal(stage1_price),
            "stage1_qty": self._stringify_decimal(stage1_filled_qty),
            "stage2_side": stage2_side,
            "stage2_price": self._stringify_decimal(stage2_price),
            "stage2_qty": self._stringify_decimal(stage2_filled_qty),
            "round_residual_qty": self._stringify_decimal(round_residual_qty),
            "residual_side": residual_side_label,
            "carryover_qty": self._stringify_decimal(carryover_qty),
        }
        if has_wait:
            message_params["wait_seconds"] = wait_seconds
        return message_code, message_params

    async def _simulate_paired_open_final_alignment(
        self,
        *,
        symbol: str,
        rules: SymbolRules,
        session_long_qty: Decimal,
        session_short_qty: Decimal,
        carryover_qty: Decimal,
    ) -> tuple[Decimal, Decimal, Decimal, FinalAlignmentStatus]:
        normalized_carryover = normalize_qty(carryover_qty, rules)
        if normalized_carryover <= Decimal("0"):
            return session_long_qty, session_short_qty, Decimal("0"), FinalAlignmentStatus.NOT_NEEDED

        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level="info",
                message_code="log.simulation.paired_open_final_alignment_started",
                message_params={
                    "symbol": symbol,
                    "carryover_qty": self._stringify_decimal(normalized_carryover),
                    "session_long_qty": self._stringify_decimal(session_long_qty),
                    "session_short_qty": self._stringify_decimal(session_short_qty),
                },
            ),
        )

        try:
            quote = await self._gateway.get_quote(symbol)
            long_price = normalize_price(Decimal(quote.bid_price), rules)
            short_price = normalize_price(Decimal(quote.ask_price), rules)

            if session_long_qty == session_short_qty:
                await self.publish(
                    "execution_log",
                    self._execution_log_payload(
                        level="success",
                        message_code="log.simulation.paired_open_final_alignment_completed_market_aligned",
                        message_params={
                            "symbol": symbol,
                            "aligned_qty": self._stringify_decimal(session_long_qty),
                        },
                    ),
                )
                return session_long_qty, session_short_qty, Decimal("0"), FinalAlignmentStatus.MARKET_ALIGNED

            if session_long_qty <= session_short_qty:
                small_side = PositionSide.LONG
                small_qty = session_long_qty
                large_qty = session_short_qty
                small_price = long_price
            else:
                small_side = PositionSide.SHORT
                small_qty = session_short_qty
                large_qty = session_long_qty
                small_price = short_price

            small_min_qty = min_qty_for_notional(small_price, rules)
            if small_qty < small_min_qty:
                await self.publish(
                    "execution_log",
                    self._execution_log_payload(
                        level="warn",
                        message_code="log.simulation.paired_open_final_alignment_completed_flattened",
                        message_params={
                            "symbol": symbol,
                            "session_long_qty": self._stringify_decimal(session_long_qty),
                            "session_short_qty": self._stringify_decimal(session_short_qty),
                        },
                    ),
                )
                return Decimal("0"), Decimal("0"), Decimal("0"), FinalAlignmentStatus.FLATTENED_BOTH_SIDES

            gap_qty = normalize_qty(large_qty - small_qty, rules)
            small_reduce_qty = small_min_qty
            large_reduce_qty = normalize_qty(small_min_qty + gap_qty, rules)
            residual_small_qty = normalize_qty(small_qty - small_reduce_qty, rules)
            residual_large_qty = normalize_qty(large_qty - large_reduce_qty, rules)
            if residual_small_qty != residual_large_qty:
                raise RuntimeError(
                    f"simulated final alignment did not converge: residual_small={residual_small_qty}, residual_large={residual_large_qty}"
                )

            if small_side == PositionSide.LONG:
                session_long_qty = residual_small_qty
                session_short_qty = residual_large_qty
            else:
                session_long_qty = residual_large_qty
                session_short_qty = residual_small_qty

            await self.publish(
                "execution_log",
                self._execution_log_payload(
                    level="success",
                    message_code="log.simulation.paired_open_final_alignment_completed_market_aligned",
                    message_params={
                        "symbol": symbol,
                        "aligned_qty": self._stringify_decimal(residual_small_qty),
                    },
                ),
            )
            return session_long_qty, session_short_qty, Decimal("0"), FinalAlignmentStatus.MARKET_ALIGNED
        except Exception as exc:
            await self.publish(
                "execution_log",
                self._execution_log_payload(
                    level="warn",
                    message_code="log.simulation.paired_open_final_alignment_failed",
                    message_params={
                        "symbol": symbol,
                        "carryover_qty": self._stringify_decimal(normalized_carryover),
                        "error": str(exc),
                    },
                ),
            )
            return session_long_qty, session_short_qty, normalized_carryover, FinalAlignmentStatus.FAILED

    def _open_order_side(self, position_side: PositionSide) -> OrderSide:
        return OrderSide.BUY if position_side == PositionSide.LONG else OrderSide.SELL

    def _close_order_side(self, position_side: PositionSide) -> OrderSide:
        return OrderSide.SELL if position_side == PositionSide.LONG else OrderSide.BUY

    def _format_price_guard_message(self, code: str, details: dict[str, Any]) -> str:
        if code == "max_spread_bps":
            return f"模拟执行已阻止：当前价差 {details.get('spread_bps')} bps 超过允许阈值 {details.get('max_spread_bps')} bps"
        if code == "max_reprice_ticks":
            return (
                f"模拟执行已阻止：价格相对参考价偏移 {details.get('ticks')} 个 tick，"
                f"超过允许阈值 {details.get('max_reprice_ticks')}"
            )
        if code == "max_reference_deviation_bps":
            return (
                f"模拟执行已阻止：价格相对参考价偏离 {details.get('deviation_bps')} bps，"
                f"超过允许阈值 {details.get('max_reference_deviation_bps')} bps"
            )
        return "模拟执行已阻止：执行价格保护规则触发"

    def _format_partial_price_guard_message(self, code: str, details: dict[str, Any]) -> str:
        return self._format_price_guard_message(code, details).replace("模拟执行已阻止：", "模拟执行提前结束：", 1)

    def _price_guard_message_contract(self, code: str, details: dict[str, Any], *, partial: bool) -> tuple[str, dict[str, Any]]:
        suffix_map = {
            "max_spread_bps": "max_spread_bps",
            "max_reprice_ticks": "max_reprice_ticks",
            "max_reference_deviation_bps": "max_reference_deviation_bps",
        }
        key_suffix = suffix_map.get(code, "generic")
        message_code = f"log.simulation.price_guard.{ 'partial' if partial else 'blocked' }.{key_suffix}"
        message_params = {key: self._normalize(value) for key, value in details.items()}
        return message_code, message_params

    def _remaining_single_round_qty(
        self,
        *,
        round_index: int,
        round_count: int,
        round_qty: Decimal,
        final_round_qty: Decimal,
        carryover_qty: Decimal,
        rules: SymbolRules,
    ) -> Decimal:
        current_round_qty = final_round_qty if round_index == round_count else round_qty
        future_middle_rounds = max(round_count - round_index - 1, 0)
        future_round_qty = (round_qty * Decimal(future_middle_rounds)) + (final_round_qty if round_index < round_count else Decimal("0"))
        return normalize_qty(carryover_qty + current_round_qty + future_round_qty, rules)

    async def _maybe_block_for_price_guard(
        self,
        *,
        session_kind: SessionKind,
        symbol: str,
        round_count: int,
        min_notional: Decimal,
        side: OrderSide,
        quote: Quote,
        candidate_price: Decimal,
        reference_price: Decimal,
        rules: SymbolRules,
        total_notional: Decimal = Decimal("0"),
        notional_per_round: Decimal = Decimal("0"),
        last_qty: Decimal = Decimal("0"),
        mode: str | None = None,
        selected_position_side: PositionSide | None = None,
        rounds_completed: int = 0,
        carryover_qty: Decimal = Decimal("0"),
        has_partial_progress: bool = False,
        final_alignment_status: FinalAlignmentStatus | str = FinalAlignmentStatus.NOT_NEEDED,
        remaining_qty_on_guard: Decimal = Decimal("0"),
    ) -> dict[str, Any] | None:
        guard = evaluate_price_guard(
            policy=self._resolved_simulation_policy,
            side=side,
            quote=quote,
            candidate_price=candidate_price,
            reference_price=reference_price,
            rules=rules,
        )
        if guard is None:
            return None
        if rounds_completed > 0 or carryover_qty > Decimal("0") or has_partial_progress:
            message_code, message_params = self._price_guard_message_contract(guard.code, guard.details, partial=True)
            residual_qty = normalize_qty(
                remaining_qty_on_guard if remaining_qty_on_guard > Decimal("0") else (carryover_qty + last_qty),
                rules,
            )
            return await self._complete_simulation_with_skips(
                session_kind=session_kind,
                symbol=symbol,
                round_count=round_count,
                min_notional=min_notional,
                message="",
                message_code=message_code,
                message_params=message_params,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                mode=mode,
                selected_position_side=selected_position_side,
                rounds_completed=rounds_completed,
                carryover_qty=residual_qty,
                final_alignment_status=final_alignment_status,
            )
        message_code, message_params = self._price_guard_message_contract(guard.code, guard.details, partial=False)
        return await self._block_simulation(
            session_kind=session_kind,
            symbol=symbol,
            round_count=round_count,
            min_notional=min_notional,
            message_code=message_code,
            message_params=message_params,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            mode=mode,
            selected_position_side=selected_position_side,
            rounds_completed=rounds_completed,
            carryover_qty=carryover_qty,
            final_alignment_status=final_alignment_status,
        )

    def _simulate_leg_fill(
        self,
        *,
        requested_qty: Decimal,
        rules: SymbolRules,
        allow_market_fallback: bool,
    ) -> tuple[Decimal, Decimal]:
        normalized_qty = normalize_qty(requested_qty, rules)
        if normalized_qty <= Decimal("0"):
            return Decimal("0"), Decimal("0")
        if not allow_market_fallback:
            simulated_fill_qty = compute_market_fallback_qty(self._resolved_simulation_policy, normalized_qty, rules)
            if simulated_fill_qty <= Decimal("0"):
                return Decimal("0"), normalized_qty
            return simulated_fill_qty, normalize_qty(normalized_qty - simulated_fill_qty, rules)
        fallback_qty = compute_market_fallback_qty(self._resolved_simulation_policy, normalized_qty, rules)
        if fallback_qty <= Decimal("0"):
            return Decimal("0"), normalized_qty
        return fallback_qty, normalize_qty(normalized_qty - fallback_qty, rules)

    async def _run_paired_close_simulation(
        self,
        *,
        symbol: str,
        trend_bias: TrendBias,
        close_qty: Decimal,
        round_count: int,
        round_interval_seconds: int = 3,
    ) -> dict[str, Any]:
        symbol = symbol.upper()
        rules = await self._gateway.get_symbol_rules(symbol)
        overview = await self._gateway.get_account_overview()
        long_qty = normalize_qty(self._position_qty(overview, symbol, PositionSide.LONG), rules)
        short_qty = normalize_qty(self._position_qty(overview, symbol, PositionSide.SHORT), rules)
        max_closeable_qty = normalize_qty(min(long_qty, short_qty), rules)
        if max_closeable_qty <= Decimal("0"):
            return await self._block_simulation(
                session_kind=SessionKind.PAIRED_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message_code="log.simulation.paired_close.no_position",
                mode=trend_bias.value,
            )
        normalized_close_qty = normalize_qty(close_qty, rules)
        if normalized_close_qty <= Decimal("0"):
            return await self._block_simulation(
                session_kind=SessionKind.PAIRED_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message_code="log.simulation.paired_close.zero_qty",
                mode=trend_bias.value,
            )
        if normalized_close_qty > max_closeable_qty:
            return await self._block_simulation(
                session_kind=SessionKind.PAIRED_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message_code="log.simulation.paired_close.qty_exceeds_position",
                message_params={
                    "normalized_close_qty": self._stringify_decimal(normalized_close_qty),
                    "available_qty": self._stringify_decimal(max_closeable_qty),
                },
                mode=trend_bias.value,
            )
        round_qty = normalize_qty(normalized_close_qty / Decimal(round_count), rules)
        final_round_qty = normalize_qty(normalized_close_qty - (round_qty * Decimal(max(round_count - 1, 0))), rules)
        if round_qty <= Decimal("0") or final_round_qty <= Decimal("0"):
            return await self._block_simulation(
                session_kind=SessionKind.PAIRED_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message_code="log.simulation.paired_close.round_qty_zero",
                mode=trend_bias.value,
            )
        quote = await self._gateway.get_quote(symbol)
        stage1_price, stage2_price, stage1_label, stage2_label = self._paired_close_stage_prices(
            trend_bias,
            quote.bid_price,
            quote.ask_price,
        )
        stage1_price = normalize_price(stage1_price, rules)
        stage2_price = normalize_price(stage2_price, rules)
        try:
            validate_qty_and_notional(round_qty, stage1_price, rules)
            validate_qty_and_notional(round_qty, stage2_price, rules)
            validate_qty_and_notional(final_round_qty, stage1_price, rules)
            validate_qty_and_notional(final_round_qty, stage2_price, rules)
        except ValueError:
            per_round_notional = round_qty * stage1_price
            return await self._block_simulation(
                session_kind=SessionKind.PAIRED_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message=f"每轮平仓金额 {per_round_notional} 低于交易所最小下单金额 {rules.min_notional}，无法平仓。",
                message_code="log.simulation.blocked_min_notional_close",
                message_params={
                    "per_round_notional": self._stringify_decimal(per_round_notional),
                    "min_notional": self._stringify_decimal(rules.min_notional),
                    "symbol": symbol,
                },
                total_notional=normalized_close_qty * stage1_price,
                notional_per_round=per_round_notional,
                last_qty=round_qty,
                mode=trend_bias.value,
            )
        total_notional = normalized_close_qty * stage1_price
        notional_per_round = round_qty * stage1_price
        stage1_position_side = PositionSide.SHORT if trend_bias == TrendBias.LONG else PositionSide.LONG
        stage2_position_side = PositionSide.LONG if trend_bias == TrendBias.LONG else PositionSide.SHORT
        blocked = await self._maybe_block_for_price_guard(
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            side=self._close_order_side(stage1_position_side),
            quote=quote,
            candidate_price=stage1_price,
            reference_price=stage1_price,
            rules=rules,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            mode=trend_bias.value,
        )
        if blocked is not None:
            return blocked
        blocked = await self._maybe_block_for_price_guard(
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            side=self._close_order_side(stage2_position_side),
            quote=quote,
            candidate_price=stage2_price,
            reference_price=stage2_price,
            rules=rules,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            mode=trend_bias.value,
        )
        if blocked is not None:
            return blocked
        await self._set_execution_stats(
            status="running",
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol=symbol,
            round_count=round_count,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            min_notional=rules.min_notional,
            mode=trend_bias.value,
            message_code="log.simulation.paired_close.running",
        )
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level="info",
                message_code="log.simulation.paired_close.started",
                message_params={
                    "symbol": symbol,
                    "mode": trend_bias.value,
                    "qty": self._stringify_decimal(normalized_close_qty),
                    "round_count": round_count,
                },
            ),
        )
        aborted = await self._abort_simulation_if_requested(
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            mode=trend_bias.value,
        )
        if aborted is not None:
            return aborted
        last_qty = Decimal("0")
        carryover_qty = Decimal("0")
        for round_index in range(1, round_count + 1):
            aborted = await self._abort_simulation_if_requested(
                session_kind=SessionKind.PAIRED_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                mode=trend_bias.value,
                rounds_completed=max(round_index - 1, 0),
                carryover_qty=carryover_qty,
                final_alignment_status=(
                    FinalAlignmentStatus.CARRYOVER_PENDING if carryover_qty > Decimal("0") else FinalAlignmentStatus.NOT_NEEDED
                ),
            )
            if aborted is not None:
                return aborted
            snapshot = await self._gateway.get_order_book(symbol, limit=10)
            ask1 = normalize_price(Decimal(str(snapshot["asks"][0]["price"])), rules)
            bid1 = normalize_price(Decimal(str(snapshot["bids"][0]["price"])), rules)
            live_quote = Quote(symbol=symbol, bid_price=bid1, ask_price=ask1)
            live_stage1_price, live_stage2_price, stage1_label, stage2_label = self._paired_close_stage_prices(trend_bias, bid1, ask1)
            last_qty = final_round_qty if round_index == round_count else round_qty
            blocked = await self._maybe_block_for_price_guard(
                session_kind=SessionKind.PAIRED_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                side=self._close_order_side(stage1_position_side),
                quote=live_quote,
                candidate_price=live_stage1_price,
                reference_price=live_stage1_price,
                rules=rules,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                mode=trend_bias.value,
                rounds_completed=max(round_index - 1, 0),
                carryover_qty=carryover_qty,
                final_alignment_status=FinalAlignmentStatus.CARRYOVER_PENDING,
                remaining_qty_on_guard=self._remaining_single_round_qty(
                    round_index=round_index,
                    round_count=round_count,
                    round_qty=round_qty,
                    final_round_qty=final_round_qty,
                    carryover_qty=carryover_qty,
                    rules=rules,
                ),
            )
            if blocked is not None:
                return blocked
            blocked = await self._maybe_block_for_price_guard(
                session_kind=SessionKind.PAIRED_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                side=self._close_order_side(stage2_position_side),
                quote=live_quote,
                candidate_price=live_stage2_price,
                reference_price=live_stage2_price,
                rules=rules,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                mode=trend_bias.value,
                rounds_completed=max(round_index - 1, 0),
                carryover_qty=carryover_qty,
                final_alignment_status=FinalAlignmentStatus.CARRYOVER_PENDING,
                remaining_qty_on_guard=self._remaining_single_round_qty(
                    round_index=round_index,
                    round_count=round_count,
                    round_qty=round_qty,
                    final_round_qty=final_round_qty,
                    carryover_qty=carryover_qty,
                    rules=rules,
                ),
            )
            if blocked is not None:
                return blocked
            _, carryover_qty = self._simulate_leg_fill(
                requested_qty=last_qty + carryover_qty,
                rules=rules,
                allow_market_fallback=True,
            )
            round_message_code = (
                "log.simulation.paired_close.round_with_residual"
                if carryover_qty > Decimal("0")
                else "log.simulation.paired_close.round"
            )
            round_message_params = {
                "round_index": round_index,
                "stage1_label": stage1_label,
                "stage1_price": self._stringify_decimal(live_stage1_price),
                "stage2_label": stage2_label,
                "stage2_price": self._stringify_decimal(live_stage2_price),
                "qty": self._stringify_decimal(last_qty),
            }
            if carryover_qty > Decimal("0"):
                round_message_params["residual_qty"] = self._stringify_decimal(carryover_qty)
            await self.publish(
                "execution_log",
                self._execution_log_payload(
                    level="info",
                    message_code=round_message_code,
                    message_params=round_message_params,
                ),
            )
            await self._set_execution_stats(
                status="running",
                session_kind=SessionKind.PAIRED_CLOSE,
                symbol=symbol,
                round_count=round_count,
                rounds_completed=round_index,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                min_notional=rules.min_notional,
                mode=trend_bias.value,
                carryover_qty=carryover_qty,
                final_alignment_status=(
                    FinalAlignmentStatus.CARRYOVER_PENDING if carryover_qty > Decimal("0") else FinalAlignmentStatus.NOT_NEEDED
                ),
                message_code="log.simulation.paired_close.round_running",
                message_params={"round_index": round_index},
            )
            if round_index < round_count and round_interval_seconds > 0:
                aborted = await self._wait_or_abort(
                    round_interval_seconds,
                    session_kind=SessionKind.PAIRED_CLOSE,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    total_notional=total_notional,
                    notional_per_round=notional_per_round,
                    last_qty=last_qty,
                    mode=trend_bias.value,
                    rounds_completed=round_index,
                    carryover_qty=carryover_qty,
                    final_alignment_status=(
                        FinalAlignmentStatus.CARRYOVER_PENDING if carryover_qty > Decimal("0") else FinalAlignmentStatus.NOT_NEEDED
                    ),
                )
                if aborted is not None:
                    return aborted
        aborted = await self._abort_simulation_if_requested(
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            mode=trend_bias.value,
            rounds_completed=round_count,
            carryover_qty=carryover_qty,
            final_alignment_status=(
                FinalAlignmentStatus.CARRYOVER_PENDING if carryover_qty > Decimal("0") else FinalAlignmentStatus.NOT_NEEDED
            ),
        )
        if aborted is not None:
            return aborted
        completion_message_code = (
            "log.simulation.paired_close.completed"
            if carryover_qty <= Decimal("0")
            else "log.simulation.paired_close.completed_with_residual"
        )
        completion_message_params = {
            "symbol": symbol,
            "round_count": round_count,
        }
        if carryover_qty > Decimal("0"):
            completion_message_params["carryover_qty"] = self._stringify_decimal(carryover_qty)
        payload = await self._set_execution_stats(
            status=("completed_with_skips" if carryover_qty > Decimal("0") else "completed"),
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol=symbol,
            round_count=round_count,
            rounds_completed=round_count,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            min_notional=rules.min_notional,
            mode=trend_bias.value,
            carryover_qty=carryover_qty,
            final_alignment_status=(
                FinalAlignmentStatus.CARRYOVER_PENDING if carryover_qty > Decimal("0") else FinalAlignmentStatus.NOT_NEEDED
            ),
            message_code=completion_message_code,
            message_params=completion_message_params,
        )
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level=("success" if carryover_qty <= Decimal("0") else "warn"),
                message_code=completion_message_code,
                message_params=completion_message_params,
            ),
        )
        return payload

    async def _run_single_open_simulation(
        self,
        *,
        symbol: str,
        open_mode: SingleOpenMode,
        selected_position_side: PositionSide | None,
        open_qty: Decimal,
        leverage: int | None,
        round_count: int,
        round_interval_seconds: int = 3,
    ) -> dict[str, Any]:
        symbol = symbol.upper()
        rules = await self._gateway.get_symbol_rules(symbol)
        overview = await self._gateway.get_account_overview()
        long_qty = normalize_qty(self._position_qty(overview, symbol, PositionSide.LONG), rules)
        short_qty = normalize_qty(self._position_qty(overview, symbol, PositionSide.SHORT), rules)
        if open_mode == SingleOpenMode.ALIGN:
            if long_qty == short_qty:
                return await self._block_simulation(
                    session_kind=SessionKind.SINGLE_OPEN,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    message_code="log.simulation.single_open.align_not_needed",
                    mode=open_mode.value,
                )
            selected_position_side = PositionSide.LONG if long_qty < short_qty else PositionSide.SHORT
            normalized_open_qty = normalize_qty(abs(long_qty - short_qty), rules)
        else:
            if selected_position_side is None:
                return await self._block_simulation(
                    session_kind=SessionKind.SINGLE_OPEN,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    message_code="log.simulation.single_open.selection_required",
                    mode=open_mode.value,
                )
            normalized_open_qty = normalize_qty(open_qty, rules)
        if normalized_open_qty <= Decimal("0"):
            return await self._block_simulation(
                session_kind=SessionKind.SINGLE_OPEN,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message_code="log.simulation.single_open.zero_qty",
                mode=open_mode.value,
                selected_position_side=selected_position_side,
            )
        round_qty = normalize_qty(normalized_open_qty / Decimal(round_count), rules)
        final_round_qty = normalize_qty(normalized_open_qty - (round_qty * Decimal(max(round_count - 1, 0))), rules)
        if round_qty <= Decimal("0") or final_round_qty <= Decimal("0"):
            return await self._block_simulation(
                session_kind=SessionKind.SINGLE_OPEN,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message_code="log.simulation.single_open.round_qty_zero",
                mode=open_mode.value,
                selected_position_side=selected_position_side,
            )
        available_balance = self._available_balance(overview)
        has_existing_positions = long_qty > Decimal("0") or short_qty > Decimal("0")
        requested_leverage = int(leverage or 0)
        if has_existing_positions:
            effective_leverage = self._position_leverage(overview, symbol) or max(int(await self._gateway.get_symbol_leverage(symbol) or 1), 1)
            if requested_leverage and requested_leverage != effective_leverage:
                return await self._block_simulation(
                    session_kind=SessionKind.SINGLE_OPEN,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    message_code="log.simulation.single_open.leverage_mismatch",
                    message_params={"effective_leverage": effective_leverage},
                    mode=open_mode.value,
                    selected_position_side=selected_position_side,
                )
        else:
            effective_leverage = requested_leverage or max(int(await self._gateway.get_symbol_leverage(symbol) or 1), 1)
            if effective_leverage > rules.max_leverage:
                return await self._block_simulation(
                    session_kind=SessionKind.SINGLE_OPEN,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    message_code="log.simulation.single_open.leverage_exceeds_max",
                    message_params={
                        "effective_leverage": effective_leverage,
                        "max_leverage": rules.max_leverage,
                    },
                    mode=open_mode.value,
                    selected_position_side=selected_position_side,
                )
        quote = await self._gateway.get_quote(symbol)
        side_price = normalize_price(self._single_side_price(selected_position_side, quote.bid_price, quote.ask_price), rules)
        try:
            validate_qty_and_notional(round_qty, side_price, rules)
            validate_qty_and_notional(final_round_qty, side_price, rules)
        except ValueError:
            per_round_notional = round_qty * side_price
            return await self._block_simulation(
                session_kind=SessionKind.SINGLE_OPEN,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message=f"每轮开单金额 {per_round_notional} 低于交易所最小下单金额 {rules.min_notional}，无法开单。",
                message_code="log.simulation.blocked_min_notional",
                message_params={
                    "per_round_notional": self._stringify_decimal(per_round_notional),
                    "min_notional": self._stringify_decimal(rules.min_notional),
                    "symbol": symbol,
                },
                total_notional=normalized_open_qty * side_price,
                notional_per_round=per_round_notional,
                last_qty=round_qty,
                mode=open_mode.value,
                selected_position_side=selected_position_side,
            )
        total_notional = normalized_open_qty * side_price
        notional_per_round = round_qty * side_price
        blocked = await self._maybe_block_for_price_guard(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            side=self._open_order_side(selected_position_side),
            quote=quote,
            candidate_price=side_price,
            reference_price=side_price,
            rules=rules,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            mode=open_mode.value,
            selected_position_side=selected_position_side,
        )
        if blocked is not None:
            return blocked
        if available_balance is not None:
            max_open_amount = available_balance * Decimal("0.95")
            implied_open_amount = total_notional / Decimal(effective_leverage)
            if implied_open_amount > max_open_amount:
                return await self._block_simulation(
                    session_kind=SessionKind.SINGLE_OPEN,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    message_code="log.simulation.single_open.balance_limit_exceeded",
                    message_params={
                        "implied_open_amount": self._stringify_decimal(implied_open_amount),
                        "available_balance": self._stringify_decimal(available_balance),
                        "max_open_amount": self._stringify_decimal(max_open_amount),
                    },
                    total_notional=total_notional,
                    notional_per_round=notional_per_round,
                    last_qty=round_qty,
                    mode=open_mode.value,
                    selected_position_side=selected_position_side,
                )
        await self._set_execution_stats(
            status="running",
            session_kind=SessionKind.SINGLE_OPEN,
            symbol=symbol,
            round_count=round_count,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            min_notional=rules.min_notional,
            mode=open_mode.value,
            selected_position_side=selected_position_side,
            message_code="log.simulation.single_open_running",
        )
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level="info",
                message_code="log.simulation.single_open_started",
                message_params={
                    "symbol": symbol,
                    "mode": open_mode.value,
                    "selected_position_side": selected_position_side.value,
                    "qty": self._stringify_decimal(normalized_open_qty),
                    "leverage": effective_leverage,
                    "round_count": round_count,
                },
            ),
        )
        aborted = await self._abort_simulation_if_requested(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            mode=open_mode.value,
            selected_position_side=selected_position_side,
        )
        if aborted is not None:
            return aborted
        last_qty = Decimal("0")
        total_residual_qty = Decimal("0")
        for round_index in range(1, round_count + 1):
            aborted = await self._abort_simulation_if_requested(
                session_kind=SessionKind.SINGLE_OPEN,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                mode=open_mode.value,
                selected_position_side=selected_position_side,
                rounds_completed=max(round_index - 1, 0),
                carryover_qty=total_residual_qty,
            )
            if aborted is not None:
                return aborted
            snapshot = await self._gateway.get_order_book(symbol, limit=10)
            ask1 = normalize_price(Decimal(str(snapshot["asks"][0]["price"])), rules)
            bid1 = normalize_price(Decimal(str(snapshot["bids"][0]["price"])), rules)
            live_quote = Quote(symbol=symbol, bid_price=bid1, ask_price=ask1)
            live_price = self._single_side_price(selected_position_side, bid1, ask1)
            last_qty = final_round_qty if round_index == round_count else round_qty
            blocked = await self._maybe_block_for_price_guard(
                session_kind=SessionKind.SINGLE_OPEN,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                side=self._open_order_side(selected_position_side),
                quote=live_quote,
                candidate_price=live_price,
                reference_price=live_price,
                rules=rules,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                mode=open_mode.value,
                selected_position_side=selected_position_side,
                rounds_completed=max(round_index - 1, 0),
                carryover_qty=total_residual_qty,
                remaining_qty_on_guard=self._remaining_single_round_qty(
                    round_index=round_index,
                    round_count=round_count,
                    round_qty=round_qty,
                    final_round_qty=final_round_qty,
                    carryover_qty=total_residual_qty,
                    rules=rules,
                ),
            )
            if blocked is not None:
                return blocked
            _, round_residual_qty = self._simulate_leg_fill(
                requested_qty=last_qty,
                rules=rules,
                allow_market_fallback=True,
            )
            total_residual_qty = normalize_qty(total_residual_qty + round_residual_qty, rules)
            await self.publish(
                "execution_log",
                self._execution_log_payload(
                    level="info",
                    message_code=(
                        "log.simulation.single_open_round_with_residual"
                        if round_residual_qty > Decimal("0")
                        else "log.simulation.single_open_round"
                    ),
                    message_params={
                        "round_index": round_index,
                        "selected_position_side": selected_position_side.value,
                        "price": self._stringify_decimal(live_price),
                        "qty": self._stringify_decimal(last_qty),
                        "residual_qty": self._stringify_decimal(round_residual_qty),
                    },
                ),
            )
            await self._set_execution_stats(
                status="running",
                session_kind=SessionKind.SINGLE_OPEN,
                symbol=symbol,
                round_count=round_count,
                rounds_completed=round_index,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                min_notional=rules.min_notional,
                mode=open_mode.value,
                selected_position_side=selected_position_side,
                carryover_qty=total_residual_qty,
                message_code="log.simulation.single_open_round_running",
                message_params={"round_index": round_index},
            )
            if round_index < round_count and round_interval_seconds > 0:
                aborted = await self._wait_or_abort(
                    round_interval_seconds,
                    session_kind=SessionKind.SINGLE_OPEN,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    total_notional=total_notional,
                    notional_per_round=notional_per_round,
                    last_qty=last_qty,
                    mode=open_mode.value,
                    selected_position_side=selected_position_side,
                    rounds_completed=round_index,
                    carryover_qty=total_residual_qty,
                )
                if aborted is not None:
                    return aborted
        aborted = await self._abort_simulation_if_requested(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            mode=open_mode.value,
            selected_position_side=selected_position_side,
            rounds_completed=round_count,
            carryover_qty=total_residual_qty,
        )
        if aborted is not None:
            return aborted
        payload = await self._set_execution_stats(
            status=("completed_with_skips" if total_residual_qty > Decimal("0") else "completed"),
            session_kind=SessionKind.SINGLE_OPEN,
            symbol=symbol,
            round_count=round_count,
            rounds_completed=round_count,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            min_notional=rules.min_notional,
            mode=open_mode.value,
            selected_position_side=selected_position_side,
            carryover_qty=total_residual_qty,
            message_code=(
                "log.simulation.single_open_completed"
                if total_residual_qty <= Decimal("0")
                else "log.simulation.single_open_completed_with_residual"
            ),
            message_params={
                "symbol": symbol,
                "round_count": round_count,
                "carryover_qty": self._stringify_decimal(total_residual_qty),
            },
        )
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level=("success" if total_residual_qty <= Decimal("0") else "warn"),
                message_code=(
                    "log.simulation.single_open_completed"
                    if total_residual_qty <= Decimal("0")
                    else "log.simulation.single_open_completed_with_residual"
                ),
                message_params={
                    "symbol": symbol,
                    "round_count": round_count,
                    "carryover_qty": self._stringify_decimal(total_residual_qty),
                },
            ),
        )
        return payload

    async def _run_single_close_simulation(
        self,
        *,
        symbol: str,
        close_mode: SingleCloseMode,
        selected_position_side: PositionSide | None,
        close_qty: Decimal,
        round_count: int,
        round_interval_seconds: int = 3,
    ) -> dict[str, Any]:
        symbol = symbol.upper()
        rules = await self._gateway.get_symbol_rules(symbol)
        overview = await self._gateway.get_account_overview()
        long_qty = normalize_qty(self._position_qty(overview, symbol, PositionSide.LONG), rules)
        short_qty = normalize_qty(self._position_qty(overview, symbol, PositionSide.SHORT), rules)
        if close_mode == SingleCloseMode.ALIGN:
            if long_qty == short_qty:
                return await self._block_simulation(
                    session_kind=SessionKind.SINGLE_CLOSE,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    message_code=(
                        "log.simulation.single_close.no_position"
                        if long_qty <= Decimal("0")
                        else "log.simulation.single_close.align_not_needed"
                    ),
                    mode=close_mode.value,
                )
            selected_position_side = PositionSide.LONG if long_qty > short_qty else PositionSide.SHORT
            normalized_close_qty = normalize_qty(abs(long_qty - short_qty), rules)
        else:
            if selected_position_side is None:
                return await self._block_simulation(
                    session_kind=SessionKind.SINGLE_CLOSE,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    message_code="log.simulation.single_close.selection_required",
                    mode=close_mode.value,
                )
            available_qty = long_qty if selected_position_side == PositionSide.LONG else short_qty
            if available_qty <= Decimal("0"):
                return await self._block_simulation(
                    session_kind=SessionKind.SINGLE_CLOSE,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    message_code="log.simulation.single_close.no_position",
                    mode=close_mode.value,
                    selected_position_side=selected_position_side,
                )
            normalized_close_qty = normalize_qty(close_qty, rules)
            if normalized_close_qty > available_qty:
                return await self._block_simulation(
                    session_kind=SessionKind.SINGLE_CLOSE,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    message_code="log.simulation.single_close.qty_exceeds_position",
                    message_params={
                        "normalized_close_qty": self._stringify_decimal(normalized_close_qty),
                        "available_qty": self._stringify_decimal(available_qty),
                    },
                    mode=close_mode.value,
                    selected_position_side=selected_position_side,
                )
        if normalized_close_qty <= Decimal("0"):
            return await self._block_simulation(
                session_kind=SessionKind.SINGLE_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message_code="log.simulation.single_close.zero_qty",
                mode=close_mode.value,
                selected_position_side=selected_position_side,
            )
        round_qty = normalize_qty(normalized_close_qty / Decimal(round_count), rules)
        final_round_qty = normalize_qty(normalized_close_qty - (round_qty * Decimal(max(round_count - 1, 0))), rules)
        if round_qty <= Decimal("0") or final_round_qty <= Decimal("0"):
            return await self._block_simulation(
                session_kind=SessionKind.SINGLE_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message_code="log.simulation.single_close.round_qty_zero",
                mode=close_mode.value,
                selected_position_side=selected_position_side,
            )
        quote = await self._gateway.get_quote(symbol)
        side_price = normalize_price(self._single_side_price(selected_position_side, quote.bid_price, quote.ask_price), rules)
        try:
            validate_qty_and_notional(round_qty, side_price, rules)
            validate_qty_and_notional(final_round_qty, side_price, rules)
        except ValueError:
            per_round_notional = round_qty * side_price
            return await self._block_simulation(
                session_kind=SessionKind.SINGLE_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message=f"每轮平仓金额 {per_round_notional} 低于交易所最小下单金额 {rules.min_notional}，无法平仓。",
                message_code="log.simulation.blocked_min_notional_close",
                message_params={
                    "per_round_notional": self._stringify_decimal(per_round_notional),
                    "min_notional": self._stringify_decimal(rules.min_notional),
                    "symbol": symbol,
                },
                total_notional=normalized_close_qty * side_price,
                notional_per_round=per_round_notional,
                last_qty=round_qty,
                mode=close_mode.value,
                selected_position_side=selected_position_side,
            )
        total_notional = normalized_close_qty * side_price
        notional_per_round = round_qty * side_price
        blocked = await self._maybe_block_for_price_guard(
            session_kind=SessionKind.SINGLE_CLOSE,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            side=self._close_order_side(selected_position_side),
            quote=quote,
            candidate_price=side_price,
            reference_price=side_price,
            rules=rules,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            mode=close_mode.value,
            selected_position_side=selected_position_side,
        )
        if blocked is not None:
            return blocked
        await self._set_execution_stats(
            status="running",
            session_kind=SessionKind.SINGLE_CLOSE,
            symbol=symbol,
            round_count=round_count,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            min_notional=rules.min_notional,
            mode=close_mode.value,
            selected_position_side=selected_position_side,
            message_code="log.simulation.single_close_running",
        )
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level="info",
                message_code="log.simulation.single_close_started",
                message_params={
                    "symbol": symbol,
                    "mode": close_mode.value,
                    "selected_position_side": selected_position_side.value,
                    "qty": self._stringify_decimal(normalized_close_qty),
                    "round_count": round_count,
                },
            ),
        )
        aborted = await self._abort_simulation_if_requested(
            session_kind=SessionKind.SINGLE_CLOSE,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            mode=close_mode.value,
            selected_position_side=selected_position_side,
        )
        if aborted is not None:
            return aborted
        last_qty = Decimal("0")
        total_residual_qty = Decimal("0")
        for round_index in range(1, round_count + 1):
            aborted = await self._abort_simulation_if_requested(
                session_kind=SessionKind.SINGLE_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                mode=close_mode.value,
                selected_position_side=selected_position_side,
                rounds_completed=max(round_index - 1, 0),
                carryover_qty=total_residual_qty,
            )
            if aborted is not None:
                return aborted
            snapshot = await self._gateway.get_order_book(symbol, limit=10)
            ask1 = normalize_price(Decimal(str(snapshot["asks"][0]["price"])), rules)
            bid1 = normalize_price(Decimal(str(snapshot["bids"][0]["price"])), rules)
            live_quote = Quote(symbol=symbol, bid_price=bid1, ask_price=ask1)
            live_price = self._single_side_price(selected_position_side, bid1, ask1)
            last_qty = final_round_qty if round_index == round_count else round_qty
            blocked = await self._maybe_block_for_price_guard(
                session_kind=SessionKind.SINGLE_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                side=self._close_order_side(selected_position_side),
                quote=live_quote,
                candidate_price=live_price,
                reference_price=live_price,
                rules=rules,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                mode=close_mode.value,
                selected_position_side=selected_position_side,
                rounds_completed=max(round_index - 1, 0),
                carryover_qty=total_residual_qty,
                remaining_qty_on_guard=self._remaining_single_round_qty(
                    round_index=round_index,
                    round_count=round_count,
                    round_qty=round_qty,
                    final_round_qty=final_round_qty,
                    carryover_qty=total_residual_qty,
                    rules=rules,
                ),
            )
            if blocked is not None:
                return blocked
            _, round_residual_qty = self._simulate_leg_fill(
                requested_qty=last_qty,
                rules=rules,
                allow_market_fallback=True,
            )
            total_residual_qty = normalize_qty(total_residual_qty + round_residual_qty, rules)
            await self.publish(
                "execution_log",
                self._execution_log_payload(
                    level="info",
                    message_code=(
                        "log.simulation.single_close_round_with_residual"
                        if round_residual_qty > Decimal("0")
                        else "log.simulation.single_close_round"
                    ),
                    message_params={
                        "round_index": round_index,
                        "selected_position_side": selected_position_side.value,
                        "price": self._stringify_decimal(live_price),
                        "qty": self._stringify_decimal(last_qty),
                        "residual_qty": self._stringify_decimal(round_residual_qty),
                    },
                ),
            )
            await self._set_execution_stats(
                status="running",
                session_kind=SessionKind.SINGLE_CLOSE,
                symbol=symbol,
                round_count=round_count,
                rounds_completed=round_index,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                min_notional=rules.min_notional,
                mode=close_mode.value,
                selected_position_side=selected_position_side,
                carryover_qty=total_residual_qty,
                message_code="log.simulation.single_close_round_running",
                message_params={"round_index": round_index},
            )
            if round_index < round_count and round_interval_seconds > 0:
                aborted = await self._wait_or_abort(
                    round_interval_seconds,
                    session_kind=SessionKind.SINGLE_CLOSE,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    total_notional=total_notional,
                    notional_per_round=notional_per_round,
                    last_qty=last_qty,
                    mode=close_mode.value,
                    selected_position_side=selected_position_side,
                    rounds_completed=round_index,
                    carryover_qty=total_residual_qty,
                )
                if aborted is not None:
                    return aborted
        aborted = await self._abort_simulation_if_requested(
            session_kind=SessionKind.SINGLE_CLOSE,
            symbol=symbol,
            round_count=round_count,
            min_notional=rules.min_notional,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            mode=close_mode.value,
            selected_position_side=selected_position_side,
            rounds_completed=round_count,
            carryover_qty=total_residual_qty,
        )
        if aborted is not None:
            return aborted
        payload = await self._set_execution_stats(
            status=("completed_with_skips" if total_residual_qty > Decimal("0") else "completed"),
            session_kind=SessionKind.SINGLE_CLOSE,
            symbol=symbol,
            round_count=round_count,
            rounds_completed=round_count,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            min_notional=rules.min_notional,
            mode=close_mode.value,
            selected_position_side=selected_position_side,
            carryover_qty=total_residual_qty,
            message_code=(
                "log.simulation.single_close_completed"
                if total_residual_qty <= Decimal("0")
                else "log.simulation.single_close_completed_with_residual"
            ),
            message_params={
                "symbol": symbol,
                "round_count": round_count,
                "carryover_qty": self._stringify_decimal(total_residual_qty),
            },
        )
        await self.publish(
            "execution_log",
            self._execution_log_payload(
                level=("success" if total_residual_qty <= Decimal("0") else "warn"),
                message_code=(
                    "log.simulation.single_close_completed"
                    if total_residual_qty <= Decimal("0")
                    else "log.simulation.single_close_completed_with_residual"
                ),
                message_params={
                    "symbol": symbol,
                    "round_count": round_count,
                    "carryover_qty": self._stringify_decimal(total_residual_qty),
                },
            ),
        )
        return payload

    async def run_simulation(
        self,
        *,
        session_kind: SessionKind = SessionKind.PAIRED_OPEN,
        symbol: str,
        trend_bias: TrendBias | None = None,
        open_mode: SingleOpenMode | None = None,
        close_mode: SingleCloseMode | None = None,
        selected_position_side: PositionSide | None = None,
        open_amount: Decimal | None = None,
        open_qty: Decimal | None = None,
        close_qty: Decimal | None = None,
        leverage: int | None = None,
        round_count: int,
        round_interval_seconds: int | None = 3,
        execution_profile: str | None = None,
        market_fallback_max_ratio: Decimal | None = None,
        market_fallback_min_residual_qty: Decimal | None = None,
        max_reprice_ticks: int | None = None,
        max_spread_bps: int | None = None,
        max_reference_deviation_bps: int | None = None,
    ) -> dict[str, Any]:
        if not isinstance(session_kind, SessionKind):
            session_kind = SessionKind(str(session_kind))
        wait_seconds = 3 if round_interval_seconds is None else round_interval_seconds
        if self._simulation_lock.locked():
            raise ExchangeStateError("已有模拟执行正在进行，请等待当前任务完成", code="simulation_already_running")
        async with self._simulation_lock:
            self._simulation_abort_requested = False
            self._simulation_abort_event.clear()
            try:
                resolved_policy = resolve_execution_policy(
                    self._settings,
                    session_kind=session_kind,
                    execution_profile=execution_profile,
                    market_fallback_max_ratio=market_fallback_max_ratio,
                    market_fallback_min_residual_qty=market_fallback_min_residual_qty,
                    max_reprice_ticks=max_reprice_ticks,
                    max_spread_bps=max_spread_bps,
                    max_reference_deviation_bps=max_reference_deviation_bps,
                )
                self._resolved_simulation_policy = resolved_policy
                self._resolved_simulation_policy_payload = resolved_policy.to_payload(prefix="resolved_")
                if session_kind == SessionKind.PAIRED_OPEN:
                    if trend_bias is None:
                        raise ValueError("双向开仓模拟需要趋势方向")
                    if open_amount is None:
                        raise ValueError("双向开仓模拟需要开单金额")
                    if leverage is None:
                        raise ValueError("双向开仓模拟需要杠杆倍数")
                    return await self._run_paired_open_simulation(symbol=symbol, trend_bias=trend_bias, open_amount=open_amount, leverage=leverage, round_count=round_count, round_interval_seconds=wait_seconds)
                if session_kind == SessionKind.PAIRED_CLOSE:
                    if trend_bias is None:
                        raise ValueError("双向平仓模拟需要趋势方向")
                    if close_qty is None:
                        raise ValueError("双向平仓模拟需要平仓数量")
                    return await self._run_paired_close_simulation(symbol=symbol, trend_bias=trend_bias, close_qty=close_qty, round_count=round_count, round_interval_seconds=wait_seconds)
                if session_kind == SessionKind.SINGLE_OPEN:
                    if open_mode is None:
                        raise ValueError("单向开仓模拟需要开仓模式")
                    if open_qty is None:
                        raise ValueError("单向开仓模拟需要开仓数量")
                    return await self._run_single_open_simulation(symbol=symbol, open_mode=open_mode, selected_position_side=selected_position_side, open_qty=open_qty, leverage=leverage, round_count=round_count, round_interval_seconds=wait_seconds)
                if session_kind == SessionKind.SINGLE_CLOSE:
                    if close_mode is None:
                        raise ValueError("单向平仓模拟需要平仓模式")
                    if close_qty is None:
                        raise ValueError("单向平仓模拟需要平仓数量")
                    return await self._run_single_close_simulation(symbol=symbol, close_mode=close_mode, selected_position_side=selected_position_side, close_qty=close_qty, round_count=round_count, round_interval_seconds=wait_seconds)
                raise ValueError(f"不支持的模拟模式：{session_kind}")
            finally:
                self._simulation_abort_requested = False
                self._simulation_abort_event.clear()


def format_sse(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"



