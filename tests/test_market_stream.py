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

    async def is_hedge_mode_enabled(self) -> bool:
        return True

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

    async def get_open_orders(self, symbol: str) -> list[dict[str, object]]:
        return []

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



@pytest.mark.asyncio
async def test_unsubscribe_without_subscribers_stops_market_loop() -> None:
    gateway = SlowGateway()
    controller = MarketStreamController(gateway)
    queue = await controller.subscribe()
    await controller.connect("BTCUSDT")
    await gateway.started.wait()

    controller.unsubscribe(queue)
    await asyncio.sleep(0.05)

    assert controller._state["status"] == "disconnected"
    assert controller._market_task is None


@pytest.mark.asyncio
async def test_subscribe_cancels_pending_disconnect() -> None:
    gateway = SlowGateway()
    controller = MarketStreamController(gateway)
    queue = await controller.subscribe()
    await controller.connect("BTCUSDT")
    await gateway.started.wait()

    controller.unsubscribe(queue)
    new_queue = await controller.subscribe()
    await asyncio.sleep(0.05)

    assert controller._state["status"] == "connected"
    assert controller._market_task is not None
    controller.unsubscribe(new_queue)
    gateway.release.set()
    await controller.disconnect()

@pytest.mark.asyncio
async def test_refresh_account_overview_normalizes_mark_and_liquidation_prices() -> None:
    gateway = SlowGateway()
    controller = MarketStreamController(gateway)

    async def fake_get_account_overview() -> dict:
        return {
            "status": "ok",
            "totals": {"available_balance": Decimal("100")},
            "positions": [
                {
                    "symbol": "BTCUSDT",
                    "position_side": "LONG",
                    "qty": Decimal("0.01"),
                    "entry_price": Decimal("80000"),
                    "mark_price": Decimal("80500"),
                    "unrealized_pnl": Decimal("5"),
                    "notional": Decimal("805"),
                    "leverage": 10,
                    "liquidation_price": Decimal("70000"),
                },
                {
                    "symbol": "ETHUSDT",
                    "position_side": "SHORT",
                    "qty": Decimal("0.5"),
                    "entry_price": Decimal("2000"),
                    "mark_price": Decimal("0"),
                    "unrealized_pnl": Decimal("2.5"),
                    "notional": Decimal("997.5"),
                    "leverage": 5,
                    "liquidation_price": Decimal("0"),
                },
            ],
            "updated_at": datetime.now(UTC),
        }

    gateway.get_account_overview = fake_get_account_overview  # type: ignore[method-assign]
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()

    await controller._refresh_account_overview()
    message = await asyncio.wait_for(queue.get(), timeout=1)

    assert message["event"] == "account_overview"
    assert message["data"]["positions"][0]["mark_price"] == "80500"
    assert message["data"]["positions"][0]["liquidation_price"] == "70000"
    assert message["data"]["positions"][1]["mark_price"] == "0"
    assert message["data"]["positions"][1]["liquidation_price"] == "0"

    controller.unsubscribe(queue)
