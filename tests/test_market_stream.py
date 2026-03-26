from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from paired_opener.domain import Quote, SymbolRules, TrendBias
from paired_opener.exchange import ExchangeGateway
from paired_opener.market_stream import MarketStreamController


class SlowGateway(ExchangeGateway):
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def ensure_hedge_mode(self) -> None:
        return None

    async def ensure_cross_margin(self, symbol: str) -> None:
        return None

    async def ensure_leverage(self, symbol: str, leverage: int) -> None:
        return None

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        return SymbolRules(
            symbol=symbol,
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        )

    async def get_quote(self, symbol: str) -> Quote:
        return Quote(symbol=symbol, bid_price=Decimal("100"), ask_price=Decimal("101"))

    async def get_order_book(self, symbol: str, limit: int = 10) -> dict:
        self.started.set()
        await self.release.wait()
        return {
            "symbol": symbol,
            "bids": [{"price": Decimal("100"), "qty": Decimal("1")}],
            "asks": [{"price": Decimal("101"), "qty": Decimal("1")}],
            "event_time": datetime.now(UTC),
        }

    async def get_symbol_leverage(self, symbol: str) -> int:
        return 50

    async def get_account_overview(self) -> dict:
        return {
            "status": "ok",
            "totals": {"available_balance": Decimal("100")},
            "positions": [],
            "updated_at": datetime.now(UTC),
        }

    async def place_limit_order(self, **kwargs):
        raise NotImplementedError

    async def place_market_order(self, **kwargs):
        raise NotImplementedError

    async def get_order(self, *, symbol: str, order_id: str):
        raise NotImplementedError

    async def cancel_order(self, *, symbol: str, order_id: str):
        raise NotImplementedError


@pytest.mark.asyncio
async def test_run_simulation_rejects_concurrent_execution() -> None:
    gateway = SlowGateway()
    controller = MarketStreamController(gateway)
    first = asyncio.create_task(
        controller.run_simulation(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            open_amount=Decimal("90"),
            leverage=10,
            round_count=1,
        )
    )
    await gateway.started.wait()

    with pytest.raises(RuntimeError, match="已有模拟执行正在进行"):
        await controller.run_simulation(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            open_amount=Decimal("90"),
            leverage=10,
            round_count=1,
        )

    gateway.release.set()
    await first


@pytest.mark.asyncio
async def test_run_simulation_blocks_when_per_round_notional_below_min_notional() -> None:
    gateway = SlowGateway()
    controller = MarketStreamController(gateway)

    payload = await controller.run_simulation(
        symbol="BTCUSDT",
        trend_bias=TrendBias.LONG,
        open_amount=Decimal("0.1"),
        leverage=10,
        round_count=1,
    )

    assert payload["status"] == "blocked"
    assert payload["min_notional"] == "5"

@pytest.mark.asyncio
async def test_run_simulation_keeps_min_notional_fields_in_progress_stats() -> None:
    gateway = SlowGateway()
    gateway.release.set()
    controller = MarketStreamController(gateway)

    payload = await controller.run_simulation(
        symbol="BTCUSDT",
        trend_bias=TrendBias.LONG,
        open_amount=Decimal("90"),
        leverage=10,
        round_count=1,
    )

    assert payload["status"] == "completed"
    assert payload["min_notional"] == "5"
    assert payload["carryover_qty"] == "0"
    assert payload["final_alignment_status"] == "not_needed"


@pytest.mark.asyncio
async def test_run_simulation_blocks_when_open_amount_exceeds_available_balance_ratio() -> None:
    gateway = SlowGateway()
    controller = MarketStreamController(gateway)

    payload = await controller.run_simulation(
        symbol="BTCUSDT",
        trend_bias=TrendBias.LONG,
        open_amount=Decimal("96"),
        leverage=10,
        round_count=1,
    )

    assert payload["status"] == "blocked"
    assert payload["rounds_completed"] == 0

