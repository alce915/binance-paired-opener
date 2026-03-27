from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from paired_opener.config import Settings
from paired_opener.domain import TrendBias
from paired_opener.exchange import ExchangeGateway


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
            "account_id": self._account_id,
            "account_name": self._account_name,
            "updated_at": self._utc_now(),
            "message": "idle",
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
            "symbol": "BTCUSDT",
            "rounds_total": 0,
            "rounds_completed": 0,
            "total_notional": "0",
            "notional_per_round": "0",
            "last_qty": "0",
            "min_notional": "0",
            "carryover_qty": "0",
            "final_alignment_status": "not_needed",
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
                "message": f"???? {symbol}",
            }
            self._account_overview = {
                **self._account_overview,
                "status": "loading",
                "updated_at": self._utc_now(),
                "message": f"???? {symbol} ????",
            }
        await self._cancel_tasks(old_market_task, old_account_task)
        await self.publish("connection_status", self._state)
        await self.publish("account_overview", self._account_overview)
        await self.publish(
            "execution_log",
            {
                "level": "info",
                "message": f"???????: {symbol}",
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
            "message": "?????",
        }
        self._account_overview = {
            **self._account_overview,
            "status": "idle",
            "updated_at": self._utc_now(),
            "message": "?????",
        }
        await self.publish("connection_status", self._state)
        await self.publish("account_overview", self._account_overview)
        await self.publish(
            "execution_log",
            {
                "level": "warn",
                "message": "??????????",
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
        try:
            overview = await self._gateway.get_account_overview()
            overview = {
                **overview,
                "status": "ok",
                "account_id": self._account_id,
                "account_name": self._account_name,
                "updated_at": self._utc_now(),
                "message": "???????",
            }
            self._account_overview = overview
            await self.publish("account_overview", self._account_overview)
            if self._last_account_error is not None:
                await self.publish(
                    "execution_log",
                    {
                        "level": "success",
                        "message": "?????????",
                        "created_at": self._utc_now(),
                    },
                )
            self._last_account_error = None
        except Exception as exc:
            error_message = str(exc)
            self._account_overview = {
                **self._account_overview,
                "status": "error",
                "updated_at": self._utc_now(),
                "message": error_message,
            }
            await self.publish("account_overview", self._account_overview)
            if error_message != self._last_account_error:
                await self.publish(
                    "execution_log",
                    {
                        "level": "warn",
                        "message": f"????????: {error_message}",
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
            "message": f"??? {symbol}",
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
                    "message": f"???????: {exc}",
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
                    "message": f"????????: {exc}",
                    "created_at": self._utc_now(),
                },
            )
            async with self._lock:
                self._account_task = None

    async def run_simulation(
        self,
        *,
        symbol: str,
        trend_bias: TrendBias,
        open_amount: Decimal,
        leverage: int,
        round_count: int,
    ) -> dict[str, Any]:
        if self._simulation_lock.locked():
            raise RuntimeError("已有模拟执行正在进行，请等待当前任务完成")
        async with self._simulation_lock:
            symbol = symbol.upper()
            rules = await self._gateway.get_symbol_rules(symbol)
            available_balance: Decimal | None = None
            try:
                overview = await self._gateway.get_account_overview()
                totals = overview.get("totals") or {}
                if overview.get("status") == "ok" and "available_balance" in totals:
                    available_balance = Decimal(str(totals.get("available_balance") or "0"))
            except Exception:
                available_balance = None
            if available_balance is not None:
                max_open_amount = available_balance * Decimal("0.95")
                if open_amount > max_open_amount:
                    self._execution_stats = {
                        "status": "blocked",
                        "symbol": symbol,
                        "rounds_total": round_count,
                        "rounds_completed": 0,
                        "total_notional": "0",
                        "notional_per_round": "0",
                        "last_qty": "0",
                        "min_notional": str(rules.min_notional),
                        "carryover_qty": "0",
                        "final_alignment_status": "not_needed",
                        "updated_at": self._utc_now(),
                    }
                    await self.publish("execution_stats", self._execution_stats)
                    await self.publish(
                        "execution_log",
                        {
                            "level": "warn",
                            "message": f"???????: ???? {open_amount} ???????? {available_balance} ? 95% ?? {max_open_amount}",
                            "created_at": self._utc_now(),
                        },
                    )
                    return self._normalize(self._execution_stats)
            total_notional = open_amount * Decimal(leverage)
            notional_per_round = total_notional / Decimal(round_count)
            if notional_per_round < rules.min_notional:
                self._execution_stats = {
                    "status": "blocked",
                    "symbol": symbol,
                    "rounds_total": round_count,
                    "rounds_completed": 0,
                    "total_notional": str(total_notional),
                    "notional_per_round": str(notional_per_round),
                    "last_qty": "0",
                    "min_notional": str(rules.min_notional),
                    "carryover_qty": "0",
                    "final_alignment_status": "not_needed",
                    "updated_at": self._utc_now(),
                }
                await self.publish("execution_stats", self._execution_stats)
                await self.publish(
                    "execution_log",
                    {
                        "level": "warn",
                        "message": f"???????: ?????? {notional_per_round} ???????? {rules.min_notional}",
                        "created_at": self._utc_now(),
                    },
                )
                return self._normalize(self._execution_stats)
            self._execution_stats = {
                "status": "running",
                "symbol": symbol,
                "rounds_total": round_count,
                "rounds_completed": 0,
                "total_notional": str(total_notional),
                "notional_per_round": str(notional_per_round),
                "last_qty": "0",
                "min_notional": str(rules.min_notional),
                "carryover_qty": "0",
                "final_alignment_status": "not_needed",
                "updated_at": self._utc_now(),
            }
            await self.publish("execution_stats", self._execution_stats)
            await self.publish(
                "execution_log",
                {
                    "level": "info",
                    "message": f"??????: {symbol} | ??={trend_bias.value} | ????={open_amount} | ??={leverage}x | ??={round_count}",
                    "created_at": self._utc_now(),
                },
            )

            for round_index in range(1, round_count + 1):
                snapshot = await self._gateway.get_order_book(symbol, limit=10)
                ask1 = snapshot["asks"][0]
                bid1 = snapshot["bids"][0]
                if trend_bias == TrendBias.LONG:
                    stage1_side = "LONG"
                    stage1_price = bid1["price"]
                    stage2_side = "SHORT"
                    stage2_price = ask1["price"]
                else:
                    stage1_side = "SHORT"
                    stage1_price = ask1["price"]
                    stage2_side = "LONG"
                    stage2_price = bid1["price"]

                est_qty = (notional_per_round / stage1_price) if stage1_price > 0 else Decimal("0")
                await self.publish(
                    "execution_log",
                    {
                        "level": "info",
                        "message": f"? {round_index} ?: Stage1 {stage1_side} @ {stage1_price} | Stage2 {stage2_side} @ {stage2_price} | ???? {est_qty:.8f}",
                        "created_at": self._utc_now(),
                    },
                )
                self._execution_stats = {
                    "status": "running",
                    "symbol": symbol,
                    "rounds_total": round_count,
                    "rounds_completed": round_index,
                    "total_notional": str(total_notional),
                    "notional_per_round": str(notional_per_round),
                    "last_qty": str(est_qty),
                    "min_notional": str(rules.min_notional),
                    "carryover_qty": "0",
                    "final_alignment_status": "not_needed",
                    "updated_at": self._utc_now(),
                }
                await self.publish("execution_stats", self._execution_stats)
                if round_index < round_count:
                    await self.publish(
                        "execution_log",
                        {
                            "level": "info",
                            "message": f"? {round_index} ?????? 3 ???????",
                            "created_at": self._utc_now(),
                        },
                    )
                    await asyncio.sleep(3)

            self._execution_stats["status"] = "completed"
            self._execution_stats["updated_at"] = self._utc_now()
            await self.publish("execution_stats", self._execution_stats)
            await self.publish(
                "execution_log",
                {
                    "level": "success",
                    "message": f"??????: {symbol} ? {round_count} ?",
                    "created_at": self._utc_now(),
                },
            )
            return self._normalize(self._execution_stats)


def format_sse(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
