from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from paired_opener.domain import PositionSide, Quote, SessionKind, SingleCloseMode, SingleOpenMode, SymbolRules, TrendBias
from paired_opener import api as api_module
from paired_opener.exchange import ExchangeGateway
from paired_opener.market_stream import MarketStreamController
from paired_opener.schemas import SimulationRunRequest


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
        return Quote(symbol=symbol, bid_price=Decimal("100"), ask_price=Decimal("100.01"))

    async def get_order_book(self, symbol: str, limit: int = 10) -> dict:
        self.started.set()
        await self.release.wait()
        return {
            "symbol": symbol,
            "bids": [{"price": Decimal("100"), "qty": Decimal("1")}],
            "asks": [{"price": Decimal("100.01"), "qty": Decimal("1")}],
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

    assert payload["status"] == "completed_with_skips"
    assert payload["min_notional"] == "5"
    assert payload["carryover_qty"] == "1.688"
    assert payload["final_alignment_status"] == "carryover_pending"


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


class SimulationGateway(SlowGateway):
    def __init__(self) -> None:
        super().__init__()
        self.release.set()
        self.available_balance = Decimal("900")
        self.positions: list[dict[str, object]] = []
        self.open_orders: list[dict[str, object]] = []

    async def get_order_book(self, symbol: str, limit: int = 10) -> dict:
        return {
            "symbol": symbol,
            "bids": [{"price": Decimal("100"), "qty": Decimal("1")}],
            "asks": [{"price": Decimal("100.01"), "qty": Decimal("1")}],
            "event_time": datetime.now(UTC),
        }

    async def get_account_overview(self) -> dict:
        return {
            "status": "ok",
            "totals": {"available_balance": self.available_balance},
            "positions": list(self.positions),
            "updated_at": datetime.now(UTC),
        }

    async def get_open_orders(self, symbol: str) -> list[dict[str, object]]:
        return list(self.open_orders)


class SequencedSimulationGateway(SimulationGateway):
    def __init__(self, order_books: list[tuple[Decimal, Decimal]]) -> None:
        super().__init__()
        self._order_books = list(order_books)
        self._order_book_index = 0

    async def get_order_book(self, symbol: str, limit: int = 10) -> dict:
        if self._order_book_index < len(self._order_books):
            bid_price, ask_price = self._order_books[self._order_book_index]
            self._order_book_index += 1
        else:
            bid_price, ask_price = self._order_books[-1]
        return {
            "symbol": symbol,
            "bids": [{"price": bid_price, "qty": Decimal("1")}],
            "asks": [{"price": ask_price, "qty": Decimal("1")}],
            "event_time": datetime.now(UTC),
        }


@pytest.mark.asyncio
async def test_run_simulation_paired_close_blocks_qty_above_max_closeable() -> None:
    gateway = SimulationGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.200"), "leverage": 20},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.100"), "leverage": 20},
    ]
    controller = MarketStreamController(gateway)

    payload = await controller.run_simulation(
        session_kind=SessionKind.PAIRED_CLOSE,
        symbol="BTCUSDT",
        trend_bias=TrendBias.LONG,
        close_qty=Decimal("0.150"),
        round_count=1,
    )

    assert payload["status"] == "blocked"
    assert payload["session_kind"] == SessionKind.PAIRED_CLOSE.value
    assert "超过当前可双向平仓数量" in payload["message"]


@pytest.mark.asyncio
async def test_run_simulation_single_open_align_uses_smaller_side_difference() -> None:
    gateway = SimulationGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.100"), "leverage": 25},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.300"), "leverage": 25},
    ]
    controller = MarketStreamController(gateway)

    payload = await controller.run_simulation(
        session_kind=SessionKind.SINGLE_OPEN,
        symbol="BTCUSDT",
        open_mode=SingleOpenMode.ALIGN,
        open_qty=Decimal("0.001"),
        leverage=25,
        round_count=2,
        round_interval_seconds=0,
    )

    assert payload["status"] == "completed_with_skips"
    assert payload["session_kind"] == SessionKind.SINGLE_OPEN.value
    assert payload["selected_position_side"] == PositionSide.LONG.value
    assert payload["carryover_qty"] == "0.150"
    assert Decimal(payload["total_notional"]) > Decimal("0")


@pytest.mark.asyncio
async def test_run_simulation_single_close_without_positions_returns_blocked() -> None:
    gateway = SimulationGateway()
    controller = MarketStreamController(gateway)

    payload = await controller.run_simulation(
        session_kind=SessionKind.SINGLE_CLOSE,
        symbol="BTCUSDT",
        close_mode=SingleCloseMode.REGULAR,
        selected_position_side=PositionSide.LONG,
        close_qty=Decimal("0.010"),
        round_count=1,
    )

    assert payload["status"] == "blocked"
    assert payload["session_kind"] == SessionKind.SINGLE_CLOSE.value
    assert payload["message"] == "当前交易对不存在持仓"


@pytest.mark.asyncio
async def test_run_simulation_single_close_align_uses_larger_side_difference() -> None:
    gateway = SimulationGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.300"), "leverage": 15},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.100"), "leverage": 15},
    ]
    controller = MarketStreamController(gateway)

    payload = await controller.run_simulation(
        session_kind=SessionKind.SINGLE_CLOSE,
        symbol="BTCUSDT",
        close_mode=SingleCloseMode.ALIGN,
        close_qty=Decimal("0.001"),
        round_count=2,
        round_interval_seconds=0,
    )

    assert payload["status"] == "completed"
    assert payload["session_kind"] == SessionKind.SINGLE_CLOSE.value
    assert payload["selected_position_side"] == PositionSide.LONG.value
    assert Decimal(payload["total_notional"]) > Decimal("0")



@pytest.mark.asyncio
async def test_api_run_simulation_smoke_for_single_open(monkeypatch) -> None:
    gateway = SimulationGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.100"), "leverage": 25},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.300"), "leverage": 25},
    ]
    controller = MarketStreamController(gateway)
    runtime = SimpleNamespace(market=controller)
    monkeypatch.setattr(api_module, "current_runtime", lambda _app: runtime)

    payload = await api_module.run_simulation(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.ALIGN,
            open_qty=Decimal("0.001"),
            leverage=25,
            round_count=2,
            round_interval_seconds=0,
        )
    )

    assert payload["status"] == "completed_with_skips"
    assert payload["session_kind"] == SessionKind.SINGLE_OPEN.value
    assert payload["selected_position_side"] == PositionSide.LONG.value
    assert payload["carryover_qty"] == "0.150"


@pytest.mark.asyncio
async def test_run_simulation_returns_resolved_execution_policy_fields() -> None:
    gateway = SimulationGateway()
    controller = MarketStreamController(gateway)

    payload = await controller.run_simulation(
        session_kind=SessionKind.SINGLE_OPEN,
        symbol="BTCUSDT",
        open_mode=SingleOpenMode.REGULAR,
        selected_position_side=PositionSide.LONG,
        open_qty=Decimal("0.050"),
        leverage=25,
        round_count=1,
        round_interval_seconds=0,
        execution_profile="maker_first",
        market_fallback_max_ratio=Decimal("0.4"),
        market_fallback_min_residual_qty=Decimal("0.001"),
        max_reprice_ticks=2,
        max_spread_bps=7,
        max_reference_deviation_bps=12,
    )

    assert payload["status"] == "completed_with_skips"
    assert payload["carryover_qty"] == "0.030"
    assert payload["resolved_execution_profile"] == "maker_first"
    assert payload["resolved_market_fallback_max_ratio"] == "0.4"
    assert payload["resolved_market_fallback_min_residual_qty"] == "0.001"
    assert payload["resolved_max_reprice_ticks"] == 2
    assert payload["resolved_max_spread_bps"] == 7
    assert payload["resolved_max_reference_deviation_bps"] == 12


@pytest.mark.asyncio
async def test_run_simulation_paired_open_derives_stage2_from_stage1_fill(monkeypatch) -> None:
    gateway = SimulationGateway()
    controller = MarketStreamController(gateway)
    calls: list[tuple[Decimal, bool]] = []

    def scripted_simulate_leg_fill(*, requested_qty: Decimal, rules: SymbolRules, allow_market_fallback: bool) -> tuple[Decimal, Decimal]:
        calls.append((requested_qty, allow_market_fallback))
        if len(calls) == 1:
            return Decimal("4"), Decimal("5")
        if len(calls) == 2:
            return Decimal("3"), Decimal("1")
        raise AssertionError("unexpected extra stage simulation call")

    monkeypatch.setattr(controller, "_simulate_leg_fill", scripted_simulate_leg_fill)

    payload = await controller.run_simulation(
        session_kind=SessionKind.PAIRED_OPEN,
        symbol="BTCUSDT",
        trend_bias=TrendBias.LONG,
        open_amount=Decimal("90"),
        leverage=10,
        round_count=1,
        round_interval_seconds=0,
    )

    assert calls == [(Decimal("9"), False), (Decimal("4"), True)]
    assert payload["status"] == "completed_with_skips"
    assert payload["carryover_qty"] == "1"


@pytest.mark.asyncio
async def test_run_simulation_paired_open_keeps_stage1_progress_when_stage2_guard_hits(monkeypatch) -> None:
    class Stage2GuardGateway(SimulationGateway):
        def __init__(self) -> None:
            super().__init__()
            self._quote_calls = 0

        async def get_quote(self, symbol: str) -> Quote:
            self._quote_calls += 1
            if self._quote_calls == 1:
                return Quote(symbol=symbol, bid_price=Decimal("100"), ask_price=Decimal("100.01"))
            return Quote(symbol=symbol, bid_price=Decimal("100"), ask_price=Decimal("102"))

    gateway = Stage2GuardGateway()
    controller = MarketStreamController(gateway)

    def scripted_simulate_leg_fill(*, requested_qty: Decimal, rules: SymbolRules, allow_market_fallback: bool) -> tuple[Decimal, Decimal]:
        if allow_market_fallback:
            raise AssertionError("stage2 fill should not run once the guard blocks")
        return Decimal("4"), Decimal("5")

    monkeypatch.setattr(controller, "_simulate_leg_fill", scripted_simulate_leg_fill)

    payload = await controller.run_simulation(
        session_kind=SessionKind.PAIRED_OPEN,
        symbol="BTCUSDT",
        trend_bias=TrendBias.LONG,
        open_amount=Decimal("90"),
        leverage=10,
        round_count=1,
        round_interval_seconds=0,
        max_spread_bps=8,
    )

    assert payload["status"] == "completed_with_skips"
    assert payload["rounds_completed"] == 1
    assert Decimal(payload["carryover_qty"]) == Decimal("4")


@pytest.mark.asyncio
async def test_run_simulation_uses_default_maker_first_fallback_without_explicit_override() -> None:
    gateway = SimulationGateway()
    controller = MarketStreamController(gateway)

    payload = await controller.run_simulation(
        session_kind=SessionKind.SINGLE_OPEN,
        symbol="BTCUSDT",
        open_mode=SingleOpenMode.REGULAR,
        selected_position_side=PositionSide.LONG,
        open_qty=Decimal("0.080"),
        leverage=25,
        round_count=1,
        round_interval_seconds=0,
    )

    assert payload["resolved_execution_profile"] == "maker_first"
    assert payload["resolved_market_fallback_max_ratio"] == "0.25"
    assert payload["status"] == "completed_with_skips"
    assert payload["carryover_qty"] == "0.060"


@pytest.mark.asyncio
async def test_run_simulation_resets_price_guard_reference_each_round() -> None:
    gateway = SequencedSimulationGateway(
        [
            (Decimal("100"), Decimal("100.01")),
            (Decimal("101"), Decimal("101.01")),
        ]
    )
    controller = MarketStreamController(gateway)

    payload = await controller.run_simulation(
        session_kind=SessionKind.SINGLE_OPEN,
        symbol="BTCUSDT",
        open_mode=SingleOpenMode.REGULAR,
        selected_position_side=PositionSide.LONG,
        open_qty=Decimal("0.100"),
        leverage=25,
        round_count=2,
        round_interval_seconds=0,
        execution_profile="maker_first",
        market_fallback_max_ratio=Decimal("1"),
        max_reprice_ticks=3,
    )

    assert payload["status"] == "completed"
    assert payload["rounds_completed"] == 2
    assert Decimal(payload["carryover_qty"]) == Decimal("0")


@pytest.mark.asyncio
async def test_run_simulation_keeps_partial_progress_when_price_guard_hits_mid_session() -> None:
    gateway = SequencedSimulationGateway(
        [
            (Decimal("100"), Decimal("100.01")),
            (Decimal("100"), Decimal("102")),
        ]
    )
    controller = MarketStreamController(gateway)

    payload = await controller.run_simulation(
        session_kind=SessionKind.SINGLE_OPEN,
        symbol="BTCUSDT",
        open_mode=SingleOpenMode.REGULAR,
        selected_position_side=PositionSide.LONG,
        open_qty=Decimal("0.100"),
        leverage=25,
        round_count=2,
        round_interval_seconds=0,
        execution_profile="maker_first",
        market_fallback_max_ratio=Decimal("1"),
        max_spread_bps=8,
    )

    assert payload["status"] == "completed_with_skips"
    assert payload["rounds_completed"] == 1
    assert payload["carryover_qty"] == "0.050"
    assert "价差" in payload["message"]


@pytest.mark.asyncio
async def test_run_simulation_blocks_when_price_guard_hits() -> None:
    gateway = SimulationGateway()
    controller = MarketStreamController(gateway)

    payload = await controller.run_simulation(
        session_kind=SessionKind.SINGLE_OPEN,
        symbol="BTCUSDT",
        open_mode=SingleOpenMode.REGULAR,
        selected_position_side=PositionSide.LONG,
        open_qty=Decimal("0.050"),
        leverage=25,
        round_count=1,
        round_interval_seconds=0,
        execution_profile="maker_first",
        max_spread_bps=0,
    )

    assert payload["status"] == "blocked"
    assert "价差" in payload["message"]




@pytest.mark.asyncio
async def test_refresh_account_overview_includes_open_order_counts() -> None:
    gateway = SimulationGateway()
    gateway.open_orders = [
        {"clientOrderId": "123e4567-e89b-12d3-a456-426614174000-order"},
        {"clientOrderId": "manual-order"},
    ]
    controller = MarketStreamController(gateway)
    await controller._refresh_account_overview()

    assert controller._account_overview["symbol"] == "BTCUSDT"
    assert controller._account_overview["system_open_order_count"] == 1
    assert controller._account_overview["manual_open_order_count"] == 1

@pytest.mark.asyncio
async def test_subscribe_uses_configured_sse_queue_maxsize() -> None:
    gateway = SlowGateway()
    settings = api_module.Settings(_env_file=None, sse_queue_maxsize=7)
    controller = MarketStreamController(gateway, settings=settings)

    queue = await controller.subscribe()

    assert queue.maxsize == 7
    controller.unsubscribe(queue)
