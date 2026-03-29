from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from paired_opener.config import Settings
from paired_opener.domain import PositionSide, SessionKind, SingleCloseMode, SingleOpenMode, TrendBias
from paired_opener.exchange import ExchangeGateway
from paired_opener.rounding import normalize_price, normalize_qty, validate_qty_and_notional

SYSTEM_ORDER_ID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-', re.IGNORECASE)


class MarketStreamController:
    def __init__(
        self,
        gateway: ExchangeGateway,
        settings: Settings | None = None,
        account_id: str = "default",
        account_name: str = "default",
    ) -> None:
        self._gateway = gateway
        self._settings = settings or Settings()
        self._account_id = account_id
        self._account_name = account_name
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._lock = asyncio.Lock()
        self._simulation_lock = asyncio.Lock()
        self._market_task: asyncio.Task[None] | None = None
        self._account_task: asyncio.Task[None] | None = None
        self._disconnect_task: asyncio.Task[dict[str, Any]] | None = None
        self._last_account_error: str | None = None
        self._last_orderbook_signature: tuple[Any, ...] | None = None
        self._orderbook_interval_seconds = 0.25
        self._account_refresh_interval_seconds = max(self._settings.market_account_refresh_interval_ms / 1000, 1.0)
        self._state = {
            "connected": False,
            "status": "disconnected",
            "symbol": "BTCUSDT",
            "account_id": self._account_id,
            "account_name": self._account_name,
            "updated_at": self._utc_now(),
            "message": "idle",
        }
        self._account_overview = {
            "status": "idle",
            "symbol": "BTCUSDT",
            "account_id": self._account_id,
            "account_name": self._account_name,
            "updated_at": self._utc_now(),
            "message": "idle",
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
            "message": "idle",
            "updated_at": self._utc_now(),
        }

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
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=100)
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
                "message": f"正在连接 {symbol}",
            }
            self._account_overview = {
                **self._account_overview,
                "status": "loading",
                "symbol": symbol,
                "updated_at": self._utc_now(),
                "message": f"正在加载 {symbol} 账户概览",
            }
        await self._cancel_tasks(old_market_task, old_account_task)
        await self.publish("connection_status", self._state)
        await self.publish("account_overview", self._account_overview)
        await self.publish(
            "execution_log",
            {
                "level": "info",
                "message": f"开始连接行情流：{symbol}",
                "created_at": self._utc_now(),
            },
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
            "message": "已断开",
        }
        self._account_overview = {
            **self._account_overview,
            "symbol": symbol,
            "status": "idle",
            "updated_at": self._utc_now(),
            "message": "已断开",
        }
        await self.publish("connection_status", self._state)
        await self.publish("account_overview", self._account_overview)
        await self.publish(
            "execution_log",
            {
                "level": "warn",
                "message": "已停止行情连接",
                "created_at": self._utc_now(),
            },
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
                "message": "账户概览已同步",
            }
            self._account_overview = overview
            await self.publish("account_overview", self._account_overview)
            if self._last_account_error is not None:
                await self.publish(
                    "execution_log",
                    {
                        "level": "success",
                        "message": "账户概览已恢复",
                        "created_at": self._utc_now(),
                    },
                )
            self._last_account_error = None
        except Exception as exc:
            error_message = str(exc)
            self._account_overview = {
                **self._account_overview,
                "status": "error",
                "symbol": symbol,
                "updated_at": self._utc_now(),
                "message": error_message,
            }
            await self.publish("account_overview", self._account_overview)
            if error_message != self._last_account_error:
                await self.publish(
                    "execution_log",
                    {
                        "level": "warn",
                        "message": f"账户概览刷新失败：{error_message}",
                        "created_at": self._utc_now(),
                    },
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
                "message": str(exc),
            }
            await self.publish("connection_status", self._state)
            await self.publish(
                "execution_log",
                {
                    "level": "error",
                    "message": f"订单簿获取失败：{exc}",
                    "created_at": self._utc_now(),
                },
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
                {
                    "level": "warn",
                    "message": f"账户概览轮询失败：{exc}",
                    "created_at": self._utc_now(),
                },
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
                    message=f"模拟执行已阻止：开单金额 {open_amount} 超过可用余额 {available_balance} 的 95% 上限 {max_open_amount}",
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
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                mode=trend_bias.value,
            )

        await self._set_execution_stats(
            status="running",
            session_kind=SessionKind.PAIRED_OPEN,
            symbol=symbol,
            round_count=round_count,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            min_notional=rules.min_notional,
            mode=trend_bias.value,
            message="模拟执行进行中",
        )
        await self.publish(
            "execution_log",
            {
                "level": "info",
                "message": f"开始模拟执行：{symbol} | 趋势={trend_bias.value} | 开单金额={open_amount} | 杠杆={leverage}x | 轮次={round_count}",
                "created_at": self._utc_now(),
            },
        )

        last_qty = Decimal("0")
        for round_index in range(1, round_count + 1):
            snapshot = await self._gateway.get_order_book(symbol, limit=10)
            ask1 = Decimal(str(snapshot["asks"][0]["price"]))
            bid1 = Decimal(str(snapshot["bids"][0]["price"]))
            if trend_bias == TrendBias.LONG:
                stage1_side = "LONG"
                stage1_price = normalize_price(bid1, rules)
                stage2_side = "SHORT"
                stage2_price = normalize_price(ask1, rules)
            else:
                stage1_side = "SHORT"
                stage1_price = normalize_price(ask1, rules)
                stage2_side = "LONG"
                stage2_price = normalize_price(bid1, rules)

            last_qty = normalize_qty((notional_per_round / stage1_price) if stage1_price > 0 else Decimal("0"), rules)
            await self.publish(
                "execution_log",
                {
                    "level": "info",
                    "message": f"第 {round_index} 轮：Stage1 {stage1_side} @ {stage1_price} | Stage2 {stage2_side} @ {stage2_price} | 预计数量 {last_qty:.8f}",
                    "created_at": self._utc_now(),
                },
            )
            await self._set_execution_stats(
                status="running",
                session_kind=SessionKind.PAIRED_OPEN,
                symbol=symbol,
                round_count=round_count,
                rounds_completed=round_index,
                total_notional=total_notional,
                notional_per_round=notional_per_round,
                last_qty=last_qty,
                min_notional=rules.min_notional,
                mode=trend_bias.value,
                message=f"第 {round_index} 轮模拟执行中",
            )
            if round_index < round_count and round_interval_seconds > 0:
                await self.publish(
                    "execution_log",
                    {
                        "level": "info",
                        "message": f"第 {round_index} 轮结束，等待 {round_interval_seconds} 秒进入下一轮",
                        "created_at": self._utc_now(),
                    },
                )
                await asyncio.sleep(round_interval_seconds)

        payload = await self._set_execution_stats(
            status="completed",
            session_kind=SessionKind.PAIRED_OPEN,
            symbol=symbol,
            round_count=round_count,
            rounds_completed=round_count,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            min_notional=rules.min_notional,
            mode=trend_bias.value,
            message=f"模拟执行完成：{symbol} 共 {round_count} 轮",
        )
        await self.publish(
            "execution_log",
            {
                "level": "success",
                "message": f"模拟执行完成：{symbol} 共 {round_count} 轮",
                "created_at": self._utc_now(),
            },
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
        message: str = "",
    ) -> dict[str, Any]:
        return {
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
            "carryover_qty": "0",
            "final_alignment_status": "not_needed",
            "message": message,
            "updated_at": self._utc_now(),
        }

    async def _set_execution_stats(self, **kwargs: Any) -> dict[str, Any]:
        self._execution_stats = self._simulation_payload(**kwargs)
        await self.publish("execution_stats", self._execution_stats)
        return self._normalize(self._execution_stats)

    async def _block_simulation(
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
    ) -> dict[str, Any]:
        payload = await self._set_execution_stats(
            status="blocked",
            session_kind=session_kind,
            symbol=symbol,
            round_count=round_count,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            min_notional=min_notional,
            mode=mode,
            selected_position_side=selected_position_side,
            message=message,
        )
        await self.publish("execution_log", {"level": "warn", "message": message, "created_at": self._utc_now()})
        return payload

    def _single_side_price(self, position_side: PositionSide, bid_price: Decimal, ask_price: Decimal) -> Decimal:
        return bid_price if position_side == PositionSide.LONG else ask_price

    def _paired_close_stage_prices(self, trend_bias: TrendBias, bid_price: Decimal, ask_price: Decimal) -> tuple[Decimal, Decimal, str, str]:
        if trend_bias == TrendBias.LONG:
            return ask_price, bid_price, "平空", "平多"
        return bid_price, ask_price, "平多", "平空"

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
                message="当前账户不存在可双向平仓的双边持仓",
                mode=trend_bias.value,
            )
        normalized_close_qty = normalize_qty(close_qty, rules)
        if normalized_close_qty <= Decimal("0"):
            return await self._block_simulation(
                session_kind=SessionKind.PAIRED_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message="平仓数量归一化后为 0，无法平仓",
                mode=trend_bias.value,
            )
        if normalized_close_qty > max_closeable_qty:
            return await self._block_simulation(
                session_kind=SessionKind.PAIRED_CLOSE,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message=f"模拟执行已阻止：平仓数量 {normalized_close_qty} 超过当前可双向平仓数量 {max_closeable_qty}",
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
                message="每轮数量归一化后为 0，无法平仓",
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
                total_notional=normalized_close_qty * stage1_price,
                notional_per_round=per_round_notional,
                last_qty=round_qty,
                mode=trend_bias.value,
            )
        total_notional = normalized_close_qty * stage1_price
        notional_per_round = round_qty * stage1_price
        await self._set_execution_stats(
            status="running",
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol=symbol,
            round_count=round_count,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            min_notional=rules.min_notional,
            mode=trend_bias.value,
            message="双向平仓模拟执行中",
        )
        await self.publish(
            "execution_log",
            {
                "level": "info",
                "message": f"开始模拟双向平仓：{symbol} | 趋势={trend_bias.value} | 平仓数量={normalized_close_qty} | 轮次={round_count}",
                "created_at": self._utc_now(),
            },
        )
        last_qty = Decimal("0")
        for round_index in range(1, round_count + 1):
            snapshot = await self._gateway.get_order_book(symbol, limit=10)
            ask1 = normalize_price(Decimal(str(snapshot["asks"][0]["price"])), rules)
            bid1 = normalize_price(Decimal(str(snapshot["bids"][0]["price"])), rules)
            live_stage1_price, live_stage2_price, stage1_label, stage2_label = self._paired_close_stage_prices(trend_bias, bid1, ask1)
            last_qty = final_round_qty if round_index == round_count else round_qty
            await self.publish(
                "execution_log",
                {
                    "level": "info",
                    "message": f"第 {round_index} 轮：{stage1_label} @ {live_stage1_price} | {stage2_label} @ {live_stage2_price} | 预计数量 {last_qty:.8f}",
                    "created_at": self._utc_now(),
                },
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
                message=f"第 {round_index} 轮双向平仓模拟中",
            )
            if round_index < round_count and round_interval_seconds > 0:
                await self.publish(
                    "execution_log",
                    {
                        "level": "info",
                        "message": f"第 {round_index} 轮结束，等待 {round_interval_seconds} 秒进入下一轮双向平仓",
                        "created_at": self._utc_now(),
                    },
                )
                await asyncio.sleep(round_interval_seconds)
        payload = await self._set_execution_stats(
            status="completed",
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol=symbol,
            round_count=round_count,
            rounds_completed=round_count,
            total_notional=total_notional,
            notional_per_round=notional_per_round,
            last_qty=last_qty,
            min_notional=rules.min_notional,
            mode=trend_bias.value,
            message=f"模拟双向平仓完成：{symbol} 共 {round_count} 轮",
        )
        await self.publish("execution_log", {"level": "success", "message": f"模拟双向平仓完成：{symbol} 共 {round_count} 轮", "created_at": self._utc_now()})
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
                    message="当前双边持仓数量已对齐，无需单向开仓",
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
                    message="常规开仓需要先选择开仓订单",
                    mode=open_mode.value,
                )
            normalized_open_qty = normalize_qty(open_qty, rules)
        if normalized_open_qty <= Decimal("0"):
            return await self._block_simulation(
                session_kind=SessionKind.SINGLE_OPEN,
                symbol=symbol,
                round_count=round_count,
                min_notional=rules.min_notional,
                message="开仓数量归一化后为 0，无法单向开仓",
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
                message="每轮数量归一化后为 0，无法单向开仓",
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
                    message=f"当前交易对已有持仓，杠杆必须与现有持仓一致：{effective_leverage}x",
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
                    message=f"杠杆 {effective_leverage}x 超过交易对最大杠杆 {rules.max_leverage}x",
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
                total_notional=normalized_open_qty * side_price,
                notional_per_round=per_round_notional,
                last_qty=round_qty,
                mode=open_mode.value,
                selected_position_side=selected_position_side,
            )
        total_notional = normalized_open_qty * side_price
        notional_per_round = round_qty * side_price
        if available_balance is not None:
            max_open_amount = available_balance * Decimal("0.95")
            implied_open_amount = total_notional / Decimal(effective_leverage)
            if implied_open_amount > max_open_amount:
                return await self._block_simulation(
                    session_kind=SessionKind.SINGLE_OPEN,
                    symbol=symbol,
                    round_count=round_count,
                    min_notional=rules.min_notional,
                    message=f"模拟执行已阻止：开单金额 {implied_open_amount} 超过可用余额 {available_balance} 的 95% 上限 {max_open_amount}",
                    total_notional=total_notional,
                    notional_per_round=notional_per_round,
                    last_qty=round_qty,
                    mode=open_mode.value,
                    selected_position_side=selected_position_side,
                )
        await self._set_execution_stats(status="running", session_kind=SessionKind.SINGLE_OPEN, symbol=symbol, round_count=round_count, total_notional=total_notional, notional_per_round=notional_per_round, min_notional=rules.min_notional, mode=open_mode.value, selected_position_side=selected_position_side, message="单向开仓模拟执行中")
        await self.publish("execution_log", {"level": "info", "message": f"开始模拟单向开仓：{symbol} | 模式={open_mode.value} | 方向={selected_position_side.value} | 数量={normalized_open_qty} | 杠杆={effective_leverage}x | 轮次={round_count}", "created_at": self._utc_now()})
        last_qty = Decimal("0")
        for round_index in range(1, round_count + 1):
            snapshot = await self._gateway.get_order_book(symbol, limit=10)
            ask1 = normalize_price(Decimal(str(snapshot["asks"][0]["price"])), rules)
            bid1 = normalize_price(Decimal(str(snapshot["bids"][0]["price"])), rules)
            live_price = self._single_side_price(selected_position_side, bid1, ask1)
            last_qty = final_round_qty if round_index == round_count else round_qty
            await self.publish("execution_log", {"level": "info", "message": f"第 {round_index} 轮：{selected_position_side.value} @ {live_price} | 预计数量 {last_qty:.8f}", "created_at": self._utc_now()})
            await self._set_execution_stats(status="running", session_kind=SessionKind.SINGLE_OPEN, symbol=symbol, round_count=round_count, rounds_completed=round_index, total_notional=total_notional, notional_per_round=notional_per_round, last_qty=last_qty, min_notional=rules.min_notional, mode=open_mode.value, selected_position_side=selected_position_side, message=f"第 {round_index} 轮单向开仓模拟中")
            if round_index < round_count and round_interval_seconds > 0:
                await self.publish("execution_log", {"level": "info", "message": f"第 {round_index} 轮结束，等待 {round_interval_seconds} 秒进入下一轮单向开仓", "created_at": self._utc_now()})
                await asyncio.sleep(round_interval_seconds)
        payload = await self._set_execution_stats(status="completed", session_kind=SessionKind.SINGLE_OPEN, symbol=symbol, round_count=round_count, rounds_completed=round_count, total_notional=total_notional, notional_per_round=notional_per_round, last_qty=last_qty, min_notional=rules.min_notional, mode=open_mode.value, selected_position_side=selected_position_side, message=f"模拟单向开仓完成：{symbol} 共 {round_count} 轮")
        await self.publish("execution_log", {"level": "success", "message": f"模拟单向开仓完成：{symbol} 共 {round_count} 轮", "created_at": self._utc_now()})
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
                return await self._block_simulation(session_kind=SessionKind.SINGLE_CLOSE, symbol=symbol, round_count=round_count, min_notional=rules.min_notional, message=("当前交易对不存在持仓" if long_qty <= Decimal("0") else "当前双边持仓数量已对齐，无需单向平仓"), mode=close_mode.value)
            selected_position_side = PositionSide.LONG if long_qty > short_qty else PositionSide.SHORT
            normalized_close_qty = normalize_qty(abs(long_qty - short_qty), rules)
        else:
            if selected_position_side is None:
                return await self._block_simulation(session_kind=SessionKind.SINGLE_CLOSE, symbol=symbol, round_count=round_count, min_notional=rules.min_notional, message="常规平仓需要先选择平仓订单", mode=close_mode.value)
            available_qty = long_qty if selected_position_side == PositionSide.LONG else short_qty
            if available_qty <= Decimal("0"):
                return await self._block_simulation(session_kind=SessionKind.SINGLE_CLOSE, symbol=symbol, round_count=round_count, min_notional=rules.min_notional, message="当前交易对不存在持仓", mode=close_mode.value, selected_position_side=selected_position_side)
            normalized_close_qty = normalize_qty(close_qty, rules)
            if normalized_close_qty > available_qty:
                return await self._block_simulation(session_kind=SessionKind.SINGLE_CLOSE, symbol=symbol, round_count=round_count, min_notional=rules.min_notional, message=f"模拟执行已阻止：平仓数量 {normalized_close_qty} 超过所选持仓数量 {available_qty}", mode=close_mode.value, selected_position_side=selected_position_side)
        if normalized_close_qty <= Decimal("0"):
            return await self._block_simulation(session_kind=SessionKind.SINGLE_CLOSE, symbol=symbol, round_count=round_count, min_notional=rules.min_notional, message="平仓数量归一化后为 0，无法单向平仓", mode=close_mode.value, selected_position_side=selected_position_side)
        round_qty = normalize_qty(normalized_close_qty / Decimal(round_count), rules)
        final_round_qty = normalize_qty(normalized_close_qty - (round_qty * Decimal(max(round_count - 1, 0))), rules)
        if round_qty <= Decimal("0") or final_round_qty <= Decimal("0"):
            return await self._block_simulation(session_kind=SessionKind.SINGLE_CLOSE, symbol=symbol, round_count=round_count, min_notional=rules.min_notional, message="每轮数量归一化后为 0，无法单向平仓", mode=close_mode.value, selected_position_side=selected_position_side)
        quote = await self._gateway.get_quote(symbol)
        side_price = normalize_price(self._single_side_price(selected_position_side, quote.bid_price, quote.ask_price), rules)
        try:
            validate_qty_and_notional(round_qty, side_price, rules)
            validate_qty_and_notional(final_round_qty, side_price, rules)
        except ValueError:
            per_round_notional = round_qty * side_price
            return await self._block_simulation(session_kind=SessionKind.SINGLE_CLOSE, symbol=symbol, round_count=round_count, min_notional=rules.min_notional, message=f"每轮平仓金额 {per_round_notional} 低于交易所最小下单金额 {rules.min_notional}，无法平仓。", total_notional=normalized_close_qty * side_price, notional_per_round=per_round_notional, last_qty=round_qty, mode=close_mode.value, selected_position_side=selected_position_side)
        total_notional = normalized_close_qty * side_price
        notional_per_round = round_qty * side_price
        await self._set_execution_stats(status="running", session_kind=SessionKind.SINGLE_CLOSE, symbol=symbol, round_count=round_count, total_notional=total_notional, notional_per_round=notional_per_round, min_notional=rules.min_notional, mode=close_mode.value, selected_position_side=selected_position_side, message="单向平仓模拟执行中")
        await self.publish("execution_log", {"level": "info", "message": f"开始模拟单向平仓：{symbol} | 模式={close_mode.value} | 方向={selected_position_side.value} | 数量={normalized_close_qty} | 轮次={round_count}", "created_at": self._utc_now()})
        last_qty = Decimal("0")
        for round_index in range(1, round_count + 1):
            snapshot = await self._gateway.get_order_book(symbol, limit=10)
            ask1 = normalize_price(Decimal(str(snapshot["asks"][0]["price"])), rules)
            bid1 = normalize_price(Decimal(str(snapshot["bids"][0]["price"])), rules)
            live_price = self._single_side_price(selected_position_side, bid1, ask1)
            last_qty = final_round_qty if round_index == round_count else round_qty
            await self.publish("execution_log", {"level": "info", "message": f"第 {round_index} 轮：{selected_position_side.value} @ {live_price} | 预计数量 {last_qty:.8f}", "created_at": self._utc_now()})
            await self._set_execution_stats(status="running", session_kind=SessionKind.SINGLE_CLOSE, symbol=symbol, round_count=round_count, rounds_completed=round_index, total_notional=total_notional, notional_per_round=notional_per_round, last_qty=last_qty, min_notional=rules.min_notional, mode=close_mode.value, selected_position_side=selected_position_side, message=f"第 {round_index} 轮单向平仓模拟中")
            if round_index < round_count and round_interval_seconds > 0:
                await self.publish("execution_log", {"level": "info", "message": f"第 {round_index} 轮结束，等待 {round_interval_seconds} 秒进入下一轮单向平仓", "created_at": self._utc_now()})
                await asyncio.sleep(round_interval_seconds)
        payload = await self._set_execution_stats(status="completed", session_kind=SessionKind.SINGLE_CLOSE, symbol=symbol, round_count=round_count, rounds_completed=round_count, total_notional=total_notional, notional_per_round=notional_per_round, last_qty=last_qty, min_notional=rules.min_notional, mode=close_mode.value, selected_position_side=selected_position_side, message=f"模拟单向平仓完成：{symbol} 共 {round_count} 轮")
        await self.publish("execution_log", {"level": "success", "message": f"模拟单向平仓完成：{symbol} 共 {round_count} 轮", "created_at": self._utc_now()})
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
    ) -> dict[str, Any]:
        if not isinstance(session_kind, SessionKind):
            session_kind = SessionKind(str(session_kind))
        wait_seconds = 3 if round_interval_seconds is None else round_interval_seconds
        if self._simulation_lock.locked():
            raise RuntimeError("已有模拟执行正在进行，请等待当前任务完成")
        async with self._simulation_lock:
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


def format_sse(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"



