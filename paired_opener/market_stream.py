from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from paired_opener.domain import TrendBias
from paired_opener.exchange import ExchangeGateway


class MarketStreamController:
    def __init__(self, gateway: ExchangeGateway, account_id: str = "default", account_name: str = "默认账户") -> None:
        self._gateway = gateway
        self._account_id = account_id
        self._account_name = account_name
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._lock = asyncio.Lock()
        self._simulation_lock = asyncio.Lock()
        self._market_task: asyncio.Task[None] | None = None
        self._last_account_error: str | None = None
        self._state = {
            "connected": False,
            "status": "disconnected",
            "symbol": "BTCUSDT",
            "account_id": self._account_id,
            "account_name": self._account_name,
            "updated_at": self._utc_now(),
            "message": "未连接",
        }
        self._account_overview = {
            "status": "idle",
            "account_id": self._account_id,
            "account_name": self._account_name,
            "updated_at": self._utc_now(),
            "message": "未连接",
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
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=100)
        self._subscribers.add(queue)
        await queue.put({"event": "connection_status", "data": self._normalize(self._state)})
        await queue.put({"event": "account_overview", "data": self._normalize(self._account_overview)})
        await queue.put({"event": "execution_stats", "data": self._normalize(self._execution_stats)})
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(queue)
        if not self._subscribers and self._market_task is not None and not self._market_task.done():
            asyncio.create_task(self.disconnect())

    async def connect(self, symbol: str) -> dict[str, Any]:
        symbol = symbol.upper()
        old_task: asyncio.Task[None] | None = None
        async with self._lock:
            old_task = self._market_task
            self._market_task = None
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
                "updated_at": self._utc_now(),
                "message": f"正在获取 {symbol} 账户概览",
            }
        if old_task is not None:
            old_task.cancel()
            try:
                await old_task
            except asyncio.CancelledError:
                pass
        await self.publish("connection_status", self._state)
        await self.publish("account_overview", self._account_overview)
        await self.publish("execution_log", {
            "level": "info",
            "message": f"开始连接行情流: {symbol}",
            "created_at": self._utc_now(),
        })
        async with self._lock:
            self._market_task = asyncio.create_task(self._market_loop(symbol))
            return self._normalize(self._state)

    async def disconnect(self) -> dict[str, Any]:
        async with self._lock:
            task = self._market_task
            self._market_task = None
            symbol = self._state.get("symbol", "BTCUSDT")
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._state = {
            "connected": False,
            "status": "disconnected",
            "symbol": symbol,
            "account_id": self._account_id,
            "account_name": self._account_name,
            "updated_at": self._utc_now(),
            "message": "连接已关闭",
        }
        self._account_overview = {
            **self._account_overview,
            "status": "idle",
            "updated_at": self._utc_now(),
            "message": "连接已关闭",
        }
        await self.publish("connection_status", self._state)
        await self.publish("account_overview", self._account_overview)
        await self.publish("execution_log", {
            "level": "warn",
            "message": "已停止获取订单簿数据",
            "created_at": self._utc_now(),
        })
        return self._normalize(self._state)

    async def _refresh_account_overview(self) -> None:
        try:
            overview = await self._gateway.get_account_overview()
            overview = {
                **overview,
                "status": "ok",
                "account_id": self._account_id,
                "account_name": self._account_name,
                "updated_at": self._utc_now(),
                "message": "账户概览已更新",
            }
            self._account_overview = overview
            await self.publish("account_overview", self._account_overview)
            if self._last_account_error is not None:
                await self.publish("execution_log", {
                    "level": "success",
                    "message": "账户概览连接已恢复",
                    "created_at": self._utc_now(),
                })
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
                await self.publish("execution_log", {
                    "level": "warn",
                    "message": f"账户概览获取失败: {error_message}",
                    "created_at": self._utc_now(),
                })
            self._last_account_error = error_message

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
        next_account_refresh_at = 0.0
        try:
            while True:
                snapshot = await self._gateway.get_order_book(symbol, limit=10)
                raw_asks = snapshot["asks"]
                raw_bids = snapshot["bids"]
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
                loop_now = asyncio.get_running_loop().time()
                if loop_now >= next_account_refresh_at:
                    await self._refresh_account_overview()
                    next_account_refresh_at = loop_now + 2.0
                await asyncio.sleep(0.5)
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
            await self.publish("execution_log", {
                "level": "error",
                "message": f"订单簿获取失败: {exc}",
                "created_at": self._utc_now(),
            })
            async with self._lock:
                self._market_task = None

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
                    await self.publish("execution_log", {
                        "level": "warn",
                        "message": f"模拟执行已阻止: 开单金额 {open_amount} 超过当前可用余额 {available_balance} 的 95% 上限 {max_open_amount}",
                        "created_at": self._utc_now(),
                    })
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
                await self.publish("execution_log", {
                    "level": "warn",
                    "message": f"模拟执行已阻止: 每轮开单金额 {notional_per_round} 低于最小开单金额 {rules.min_notional}",
                    "created_at": self._utc_now(),
                })
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
            await self.publish("execution_log", {
                "level": "info",
                "message": f"开始模拟执行: {symbol} | 趋势={trend_bias.value} | 总保证金={open_amount} | 杠杆={leverage}x | 轮次={round_count}",
                "created_at": self._utc_now(),
            })

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
                await self.publish("execution_log", {
                    "level": "info",
                    "message": f"第 {round_index} 轮: Stage1 {stage1_side} @ {stage1_price} | Stage2 {stage2_side} @ {stage2_price} | 预计数量 {est_qty:.8f}",
                    "created_at": self._utc_now(),
                })
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
                    await self.publish("execution_log", {
                        "level": "info",
                        "message": f"第 {round_index} 轮完成，等待 3 秒后进入下一轮",
                        "created_at": self._utc_now(),
                    })
                    await asyncio.sleep(3)

            self._execution_stats["status"] = "completed"
            self._execution_stats["updated_at"] = self._utc_now()
            await self.publish("execution_stats", self._execution_stats)
            await self.publish("execution_log", {
                "level": "success",
                "message": f"模拟执行完成: {symbol} 共 {round_count} 轮",
                "created_at": self._utc_now(),
            })
            return self._normalize(self._execution_stats)


def format_sse(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"









