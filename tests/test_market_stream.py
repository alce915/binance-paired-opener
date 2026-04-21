from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from paired_opener.domain import PositionSide, Quote, SessionKind, SingleCloseMode, SingleOpenMode, SymbolRules, TrendBias
from paired_opener import api as api_module
from paired_opener import market_stream as market_stream_module
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
async def test_abort_simulation_returns_structured_not_running_payload() -> None:
    controller = MarketStreamController(SlowGateway())

    payload = await controller.abort_simulation()

    assert payload["requested"] is False
    assert payload["requested_action"] == "abort"
    assert payload["status"] == "idle"
    assert payload["message_code"] == "runtime.simulation_abort_not_running"


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
async def test_run_simulation_emits_structured_execution_log_messages() -> None:
    gateway = SlowGateway()
    controller = MarketStreamController(gateway)
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()

    payload = await controller.run_simulation(
        symbol="BTCUSDT",
        trend_bias=TrendBias.LONG,
        open_amount=Decimal("0.1"),
        leverage=10,
        round_count=1,
    )

    stats_message = await asyncio.wait_for(queue.get(), timeout=1)
    log_message = await asyncio.wait_for(queue.get(), timeout=1)

    assert payload["message_code"] == "log.simulation.blocked_min_notional"
    assert payload["contract_version"]
    assert stats_message["event"] == "execution_stats"
    assert stats_message["data"]["message_code"] == "log.simulation.blocked_min_notional"
    assert log_message["event"] == "execution_log"
    assert log_message["data"]["message_code"] == "log.simulation.blocked_min_notional"
    assert log_message["data"]["message_params"]["symbol"] == "BTCUSDT"
    controller.unsubscribe(queue)

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
    assert payload["final_alignment_status"] == "market_aligned"


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
async def test_connect_and_disconnect_emit_structured_lifecycle_logs() -> None:
    gateway = SlowGateway()
    controller = MarketStreamController(gateway)
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()

    await controller.connect("BTCUSDT")
    await gateway.started.wait()

    connect_log = None
    for _ in range(6):
        event = await asyncio.wait_for(queue.get(), timeout=1)
        if event["event"] == "execution_log" and event["data"].get("message_code") == "log.market_stream.connect_started":
            connect_log = event["data"]
            break

    assert connect_log is not None
    assert connect_log["message_code"] == "log.market_stream.connect_started"
    assert connect_log["message_params"]["symbol"] == "BTCUSDT"

    await controller.disconnect()

    disconnect_log = None
    for _ in range(6):
        event = await asyncio.wait_for(queue.get(), timeout=1)
        if event["event"] == "execution_log" and event["data"].get("message_code") == "log.market_stream.connect_stopped":
            disconnect_log = event["data"]
            break

    assert disconnect_log is not None
    assert disconnect_log["message_code"] == "log.market_stream.connect_stopped"
    controller.unsubscribe(queue)
    if controller._disconnect_task is not None:
        await controller._disconnect_task


class OrderBookFailureGateway(SlowGateway):
    async def get_order_book(self, symbol: str, limit: int = 10) -> dict:
        raise RuntimeError("orderbook failed")


@pytest.mark.asyncio
async def test_market_loop_emits_structured_orderbook_failure_log() -> None:
    gateway = OrderBookFailureGateway()
    controller = MarketStreamController(gateway)
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()

    await controller.connect("BTCUSDT")

    failure_log = None
    for _ in range(8):
        event = await asyncio.wait_for(queue.get(), timeout=1)
        if event["event"] == "execution_log" and event["data"].get("message_code") == "log.market_stream.orderbook_refresh_failed":
            failure_log = event["data"]
            break

    assert failure_log is not None
    assert failure_log["message_code"] == "log.market_stream.orderbook_refresh_failed"
    assert failure_log["message_params"]["symbol"] == "BTCUSDT"
    await controller.disconnect()
    controller.unsubscribe(queue)
    if controller._disconnect_task is not None:
        await controller._disconnect_task


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


@pytest.mark.asyncio
async def test_refresh_account_overview_failure_emits_structured_execution_log() -> None:
    class FailingOverviewGateway(SlowGateway):
        async def get_account_overview(self) -> dict:
            raise RuntimeError("upstream timeout")

    gateway = FailingOverviewGateway()
    controller = MarketStreamController(gateway)
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()

    await controller._refresh_account_overview()

    overview_message = await asyncio.wait_for(queue.get(), timeout=1)
    log_message = await asyncio.wait_for(queue.get(), timeout=1)

    assert overview_message["event"] == "account_overview"
    assert overview_message["data"]["status"] == "error"
    assert log_message["event"] == "execution_log"
    assert log_message["data"]["message_code"] == "log.account_overview.refresh_failed"
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
async def test_abort_simulation_stops_waiting_run_and_emits_structured_messages() -> None:
    gateway = SimulationGateway()
    controller = MarketStreamController(gateway)
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()

    task = asyncio.create_task(
        controller.run_simulation(
            session_kind=SessionKind.PAIRED_OPEN,
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            open_amount=Decimal("90"),
            leverage=10,
            round_count=2,
            round_interval_seconds=5,
        )
    )

    observed_codes: list[str] = []
    for _ in range(6):
        event = await asyncio.wait_for(queue.get(), timeout=1)
        if event["event"] == "execution_log":
            observed_codes.append(str(event["data"].get("message_code") or ""))
        if any(code.startswith("log.simulation.paired_open_round_summary") for code in observed_codes):
            break

    abort_response = await controller.abort_simulation()
    result = await asyncio.wait_for(task, timeout=1)

    assert abort_response["requested"] is True
    assert abort_response["message_code"] == "runtime.simulation_abort_requested"
    assert result["status"] == "aborted"
    assert result["message_code"] == "runtime.simulation_aborted"

    execution_log_codes: list[str] = []
    execution_stat_codes: list[str] = []
    for _ in range(8):
        event = await asyncio.wait_for(queue.get(), timeout=1)
        if event["event"] == "execution_log":
            execution_log_codes.append(str(event["data"].get("message_code") or ""))
        elif event["event"] == "execution_stats":
            execution_stat_codes.append(str(event["data"].get("message_code") or ""))
        if "runtime.simulation_aborted" in execution_log_codes and "runtime.simulation_aborted" in execution_stat_codes:
            break

    assert "runtime.simulation_abort_requested" in execution_log_codes
    assert "runtime.simulation_aborted" in execution_log_codes
    assert "runtime.simulation_aborted" in execution_stat_codes
    controller.unsubscribe(queue)


@pytest.mark.asyncio
async def test_run_simulation_paired_close_blocks_qty_above_max_closeable() -> None:
    gateway = SimulationGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.200"), "leverage": 20},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.100"), "leverage": 20},
    ]
    controller = MarketStreamController(gateway)
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()

    payload = await controller.run_simulation(
        session_kind=SessionKind.PAIRED_CLOSE,
        symbol="BTCUSDT",
        trend_bias=TrendBias.LONG,
        close_qty=Decimal("0.150"),
        round_count=1,
    )

    stats_message = await asyncio.wait_for(queue.get(), timeout=1)
    log_message = await asyncio.wait_for(queue.get(), timeout=1)

    assert payload["status"] == "blocked"
    assert payload["session_kind"] == SessionKind.PAIRED_CLOSE.value
    assert payload["message_code"] == "log.simulation.paired_close.qty_exceeds_position"
    assert stats_message["event"] == "execution_stats"
    assert stats_message["data"]["message_code"] == "log.simulation.paired_close.qty_exceeds_position"
    assert log_message["event"] == "execution_log"
    assert log_message["data"]["message_code"] == "log.simulation.paired_close.qty_exceeds_position"
    controller.unsubscribe(queue)


@pytest.mark.asyncio
async def test_run_simulation_paired_close_emits_structured_execution_logs(monkeypatch) -> None:
    gateway = SimulationGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.200"), "leverage": 20},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.200"), "leverage": 20},
    ]
    controller = MarketStreamController(gateway)
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()

    def scripted_simulate_leg_fill(*, requested_qty: Decimal, rules: SymbolRules, allow_market_fallback: bool) -> tuple[Decimal, Decimal]:
        assert allow_market_fallback is True
        return Decimal("0.090"), Decimal("0.010")

    monkeypatch.setattr(controller, "_simulate_leg_fill", scripted_simulate_leg_fill)

    payload = await controller.run_simulation(
        session_kind=SessionKind.PAIRED_CLOSE,
        symbol="BTCUSDT",
        trend_bias=TrendBias.LONG,
        close_qty=Decimal("0.100"),
        round_count=1,
        round_interval_seconds=0,
    )

    assert payload["status"] == "completed_with_skips"
    assert payload["message_code"] == "log.simulation.paired_close.completed_with_residual"

    messages: list[dict[str, object]] = []
    while True:
        try:
            messages.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            break

    execution_logs = [message["data"] for message in messages if message["event"] == "execution_log"]
    execution_stats = [message["data"] for message in messages if message["event"] == "execution_stats"]

    assert execution_logs
    assert all(log.get("message_code") for log in execution_logs)
    assert all(log["message_code"] not in {"runtime.execution_message_unavailable", "runtime.execution_legacy_message"} for log in execution_logs)
    assert any(log["message_code"] == "log.simulation.paired_close.started" for log in execution_logs)
    assert any(
        log["message_code"] in {"log.simulation.paired_close.round", "log.simulation.paired_close.round_with_residual"}
        for log in execution_logs
    )
    assert any(log["message_code"] == "log.simulation.paired_close.completed_with_residual" for log in execution_logs)
    assert any(stat["message_code"] == "log.simulation.paired_close.running" for stat in execution_stats)
    assert any(stat["message_code"] == "log.simulation.paired_close.completed_with_residual" for stat in execution_stats)
    controller.unsubscribe(queue)


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
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()

    payload = await controller.run_simulation(
        session_kind=SessionKind.SINGLE_CLOSE,
        symbol="BTCUSDT",
        close_mode=SingleCloseMode.REGULAR,
        selected_position_side=PositionSide.LONG,
        close_qty=Decimal("0.010"),
        round_count=1,
    )

    stats_message = await asyncio.wait_for(queue.get(), timeout=1)
    log_message = await asyncio.wait_for(queue.get(), timeout=1)

    assert payload["status"] == "blocked"
    assert payload["session_kind"] == SessionKind.SINGLE_CLOSE.value
    assert payload["message_code"] == "log.simulation.single_close.no_position"
    assert payload["message"] == "当前交易对不存在持仓。"
    assert stats_message["event"] == "execution_stats"
    assert stats_message["data"]["message_code"] == "log.simulation.single_close.no_position"
    assert log_message["event"] == "execution_log"
    assert log_message["data"]["message_code"] == "log.simulation.single_close.no_position"
    controller.unsubscribe(queue)


@pytest.mark.asyncio
async def test_run_simulation_single_open_emits_structured_execution_logs() -> None:
    gateway = SimulationGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.100"), "leverage": 25},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.300"), "leverage": 25},
    ]
    controller = MarketStreamController(gateway)
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()

    await controller.run_simulation(
        session_kind=SessionKind.SINGLE_OPEN,
        symbol="BTCUSDT",
        open_mode=SingleOpenMode.ALIGN,
        open_qty=Decimal("0.001"),
        leverage=25,
        round_count=1,
        round_interval_seconds=0,
    )

    messages: list[dict[str, object]] = []
    while True:
        try:
            messages.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            break

    execution_logs = [message["data"] for message in messages if message["event"] == "execution_log"]

    assert execution_logs
    assert all(log["message_code"] != "runtime.execution_legacy_message" for log in execution_logs)
    assert any(log["message_code"] == "log.simulation.single_open_started" for log in execution_logs)
    assert any(
        log["message_code"] in {"log.simulation.single_open_round", "log.simulation.single_open_round_with_residual"}
        for log in execution_logs
    )
    controller.unsubscribe(queue)


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
async def test_api_abort_simulation_smoke(monkeypatch) -> None:
    runtime = SimpleNamespace(
        market=SimpleNamespace(
            abort_simulation=lambda: asyncio.sleep(
                0,
                result={
                    "status": "aborting",
                    "requested": True,
                    "requested_action": "abort",
                    "message_code": "runtime.simulation_abort_requested",
                    "message_params": {},
                    "message": "ok",
                },
            )
        )
    )
    monkeypatch.setattr(api_module, "current_runtime", lambda _app: runtime)

    payload = await api_module.abort_simulation()

    assert payload.requested is True
    assert payload.status == "aborting"
    assert payload.message_code == "runtime.simulation_abort_requested"


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
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()
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
    assert payload["status"] == "completed"
    assert payload["carryover_qty"] == "0"
    assert payload["final_alignment_status"] == "market_aligned"
    assert payload["message_code"] == "log.simulation.paired_open_completed"

    messages: list[dict[str, object]] = []
    while True:
        try:
            messages.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            break

    execution_logs = [message["data"] for message in messages if message["event"] == "execution_log"]
    execution_stats = [message["data"] for message in messages if message["event"] == "execution_stats"]

    assert execution_logs
    assert all(log.get("message_code") for log in execution_logs)
    assert all(log["message_code"] not in {"runtime.execution_message_unavailable", "runtime.execution_legacy_message"} for log in execution_logs)
    assert any(log["message_code"] == "log.simulation.paired_open_started" for log in execution_logs)
    round_log = next(log for log in execution_logs if log["message_code"] == "log.simulation.paired_open_round_summary_with_residual")
    assert "Stage1 LONG @ 100.0 成交 4" in round_log["message"]
    assert "Stage2 SHORT @ 100.01 成交 3" in round_log["message"]
    assert "单边残量（多）1" in round_log["message"]
    assert "累计残量 1" in round_log["message"]
    assert any(log["message_code"] == "log.simulation.paired_open_final_alignment_started" for log in execution_logs)
    assert any(log["message_code"] == "log.simulation.paired_open_final_alignment_completed_market_aligned" for log in execution_logs)
    assert any(log["message_code"] == "log.simulation.paired_open_completed" for log in execution_logs)
    assert any(stat["message_code"] == "log.simulation.paired_open_running" for stat in execution_stats)
    assert any(stat["message_code"] == "log.simulation.paired_open_completed" for stat in execution_stats)
    controller.unsubscribe(queue)


@pytest.mark.asyncio
async def test_run_simulation_paired_open_round_summary_embeds_wait_hint(monkeypatch) -> None:
    gateway = SimulationGateway()
    controller = MarketStreamController(gateway)
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()

    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(market_stream_module.asyncio, "sleep", fake_sleep)

    def scripted_simulate_leg_fill(*, requested_qty: Decimal, rules: SymbolRules, allow_market_fallback: bool) -> tuple[Decimal, Decimal]:
        return requested_qty, Decimal("0")

    monkeypatch.setattr(controller, "_simulate_leg_fill", scripted_simulate_leg_fill)

    payload = await controller.run_simulation(
        session_kind=SessionKind.PAIRED_OPEN,
        symbol="BTCUSDT",
        trend_bias=TrendBias.LONG,
        open_amount=Decimal("180"),
        leverage=10,
        round_count=2,
        round_interval_seconds=3,
    )

    assert payload["status"] == "completed"
    assert payload["carryover_qty"] == "0"

    messages: list[dict[str, object]] = []
    while True:
        try:
            messages.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            break

    execution_logs = [message["data"] for message in messages if message["event"] == "execution_log"]
    round_log = next(log for log in execution_logs if log["message_code"] == "log.simulation.paired_open_round_summary_aligned_wait")
    assert "3 秒后进入下一轮" in round_log["message"]
    assert not any(log["message_code"] == "log.simulation.paired_open_wait_next_round" for log in execution_logs)
    controller.unsubscribe(queue)


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
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()

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
    assert payload["message_code"] == "log.simulation.price_guard.partial.max_spread_bps"
    assert "价差" in payload["message"]

    messages: list[dict[str, object]] = []
    while True:
        try:
            messages.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            break

    execution_logs = [message["data"] for message in messages if message["event"] == "execution_log"]
    execution_stats = [message["data"] for message in messages if message["event"] == "execution_stats"]

    assert any(log["message_code"] == "log.simulation.price_guard.partial.max_spread_bps" for log in execution_logs)
    assert any(stat["message_code"] == "log.simulation.price_guard.partial.max_spread_bps" for stat in execution_stats)
    controller.unsubscribe(queue)


@pytest.mark.asyncio
async def test_run_simulation_blocks_when_price_guard_hits() -> None:
    gateway = SimulationGateway()
    controller = MarketStreamController(gateway)
    queue = await controller.subscribe()
    await queue.get()
    await queue.get()
    await queue.get()

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
    assert payload["message_code"] == "log.simulation.price_guard.blocked.max_spread_bps"
    assert "价差" in payload["message"]

    stats_message = await asyncio.wait_for(queue.get(), timeout=1)
    log_message = await asyncio.wait_for(queue.get(), timeout=1)

    assert stats_message["event"] == "execution_stats"
    assert stats_message["data"]["message_code"] == "log.simulation.price_guard.blocked.max_spread_bps"
    assert log_message["event"] == "execution_log"
    assert log_message["data"]["message_code"] == "log.simulation.price_guard.blocked.max_spread_bps"
    controller.unsubscribe(queue)




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
