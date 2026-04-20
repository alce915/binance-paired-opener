from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from paired_opener.domain import (
    ExecutionProfile,
    ExchangeOrder,
    ExchangeOrderStatus,
    FinalAlignmentStatus,
    OpenSession,
    OrderSide,
    PositionSide,
    Quote,
    RoundStatus,
    SessionKind,
    SessionSpec,
    SymbolRules,
    TrendBias,
)
from paired_opener.engine import PairedOpeningEngine, SessionControl, SingleOpeningEngine
from paired_opener.exchange import ExchangeGateway
from paired_opener.storage import SqliteRepository


@dataclass
class ScriptedOrder:
    snapshots: list[ExchangeOrder]
    cancel_snapshot: ExchangeOrder | None = None
    cursor: int = 0

    def next_snapshot(self) -> ExchangeOrder:
        index = min(self.cursor, len(self.snapshots) - 1)
        self.cursor += 1
        return self.snapshots[index]


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


class FakeGateway(ExchangeGateway):
    def __init__(
        self,
        *,
        rules: SymbolRules,
        quote: Quote,
        limit_scripts: list[ScriptedOrder],
        market_scripts: list[ScriptedOrder] | None = None,
        account_overviews: list[dict] | None = None,
        stream_healthy: bool | None = None,
    ) -> None:
        self.rules = rules
        self.quote = quote
        self.limit_scripts = deque(limit_scripts)
        self.market_scripts = deque(market_scripts or [])
        self.account_overviews = deque(account_overviews or [])
        self.orders: dict[str, ScriptedOrder] = {}
        self.market_order_requests: list[dict[str, object]] = []
        self.cached_orders: dict[str, ExchangeOrder] = {}
        self.get_order_calls = 0
        self.stream_healthy = stream_healthy

    async def ensure_hedge_mode(self) -> None:
        return None

    async def is_hedge_mode_enabled(self) -> bool:
        return True

    async def ensure_cross_margin(self, symbol: str) -> None:
        return None

    async def ensure_leverage(self, symbol: str, leverage: int) -> None:
        return None

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        return self.rules

    async def get_quote(self, symbol: str) -> Quote:
        return self.quote

    async def get_order_book(self, symbol: str, limit: int = 10) -> dict:
        return {
            "symbol": symbol,
            "bids": [{"price": Decimal("96500"), "qty": Decimal("1.2")}],
            "asks": [{"price": Decimal("96501"), "qty": Decimal("1.1")}],
            "event_time": datetime.now(UTC),
        }

    async def get_symbol_leverage(self, symbol: str) -> int:
        return 50

    async def get_open_orders(self, symbol: str) -> list[dict[str, object]]:
        return []

    async def get_account_overview(self) -> dict:
        if not self.account_overviews:
            positions = []
        elif len(self.account_overviews) == 1:
            positions = self.account_overviews[0]
        else:
            positions = self.account_overviews.popleft()
        return {
            "status": "ok",
            "totals": {
                "equity": Decimal("1000"),
                "margin": Decimal("100"),
                "available_balance": Decimal("900"),
                "unrealized_pnl": Decimal("0"),
            },
            "positions": positions,
            "updated_at": datetime.now(UTC),
        }

    async def place_limit_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        position_side: PositionSide,
        qty: Decimal,
        price: Decimal,
        client_order_id: str,
    ) -> ExchangeOrder:
        script = self.limit_scripts.popleft()
        order = script.snapshots[0]
        self.orders[order.order_id] = script
        return order

    async def place_market_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        position_side: PositionSide,
        qty: Decimal,
        client_order_id: str,
    ) -> ExchangeOrder:
        self.market_order_requests.append(
            {
                "symbol": symbol,
                "side": side,
                "position_side": position_side,
                "qty": qty,
                "client_order_id": client_order_id,
            }
        )
        script = self.market_scripts.popleft()
        order = script.snapshots[0]
        self.orders[order.order_id] = script
        return order

    async def get_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        self.get_order_calls += 1
        return self.orders[order_id].next_snapshot()

    async def cancel_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        script = self.orders[order_id]
        return script.cancel_snapshot or script.next_snapshot()

    def get_cached_order(self, symbol: str, order_id: str) -> ExchangeOrder | None:
        return self.cached_orders.get(order_id)

    def is_order_stream_healthy(self) -> bool:
        if self.stream_healthy is not None:
            return self.stream_healthy
        return bool(self.cached_orders)


def make_order(
    order_id: str,
    *,
    side: OrderSide,
    position_side: PositionSide,
    qty: str,
    executed_qty: str,
    status: ExchangeOrderStatus,
    price: str = "96500",
) -> ExchangeOrder:
    return ExchangeOrder(
        symbol="BTCUSDT",
        order_id=order_id,
        client_order_id=order_id,
        side=side,
        position_side=position_side,
        type="LIMIT",
        price=Decimal(price),
        orig_qty=Decimal(qty),
        executed_qty=Decimal(executed_qty),
        status=status,
        update_time=datetime.now(UTC),
    )


def build_session(*, round_count: int = 1) -> OpenSession:
    return OpenSession.create(
        SessionSpec(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            leverage=50,
            round_count=round_count,
            round_qty=Decimal("0.010"),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=2,
            market_fallback_attempts=2,
            execution_profile=ExecutionProfile.BALANCED,
            created_by="test",
        )
    )


def build_repository(tmp_path: Path) -> SqliteRepository:
    return SqliteRepository(tmp_path / "test.db")


@pytest.mark.asyncio
async def test_round_completes_when_stage1_and_stage2_fill(tmp_path: Path) -> None:
    gateway = FakeGateway(
        rules=SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        quote=Quote(symbol="BTCUSDT", bid_price=Decimal("96500"), ask_price=Decimal("96501")),
        limit_scripts=[
            ScriptedOrder(snapshots=[make_order("s1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.010", status=ExchangeOrderStatus.FILLED)]),
            ScriptedOrder(snapshots=[make_order("s2", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.010", executed_qty="0.010", status=ExchangeOrderStatus.FILLED, price="96501")]),
        ],
    )
    repo = build_repository(tmp_path)
    clock = FakeClock()
    engine = PairedOpeningEngine(gateway, repo, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = build_session()
    repo.create_session(session)
    execution = await engine.execute_round(session=session, round_index=1, control=SessionControl())
    assert execution.stage1_filled_qty == Decimal("0.010")
    assert execution.stage2_filled_qty == Decimal("0.010")
    assert execution.market_fallback_used is False


@pytest.mark.asyncio
async def test_round_skips_after_stage1_zero_fill_limit(tmp_path: Path) -> None:
    gateway = FakeGateway(
        rules=SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        quote=Quote(symbol="BTCUSDT", bid_price=Decimal("96500"), ask_price=Decimal("96501")),
        limit_scripts=[
            ScriptedOrder(
                snapshots=[make_order("s1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.000", status=ExchangeOrderStatus.NEW)],
                cancel_snapshot=make_order("s1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.000", status=ExchangeOrderStatus.CANCELED),
            ),
            ScriptedOrder(
                snapshots=[make_order("s2", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.000", status=ExchangeOrderStatus.NEW)],
                cancel_snapshot=make_order("s2", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.000", status=ExchangeOrderStatus.CANCELED),
            ),
        ],
    )
    repo = build_repository(tmp_path)
    clock = FakeClock()
    engine = PairedOpeningEngine(gateway, repo, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = build_session()
    repo.create_session(session)
    execution = await engine.execute_round(session=session, round_index=1, control=SessionControl())
    assert execution.stage1_filled_qty == Decimal("0")
    assert execution.status.value == "stage1_skipped"


@pytest.mark.asyncio
async def test_stage2_uses_market_fallback_after_zero_fill_retries(tmp_path: Path) -> None:
    gateway = FakeGateway(
        rules=SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        quote=Quote(symbol="BTCUSDT", bid_price=Decimal("96500"), ask_price=Decimal("96501")),
        limit_scripts=[
            ScriptedOrder(
                snapshots=[make_order("s1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.006", status=ExchangeOrderStatus.PARTIALLY_FILLED)],
                cancel_snapshot=make_order("s1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.006", status=ExchangeOrderStatus.CANCELED),
            ),
            ScriptedOrder(
                snapshots=[make_order("s2a", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.006", executed_qty="0.000", status=ExchangeOrderStatus.NEW, price="96501")],
                cancel_snapshot=make_order("s2a", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.006", executed_qty="0.000", status=ExchangeOrderStatus.CANCELED, price="96501"),
            ),
            ScriptedOrder(
                snapshots=[make_order("s2b", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.006", executed_qty="0.000", status=ExchangeOrderStatus.NEW, price="96501")],
                cancel_snapshot=make_order("s2b", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.006", executed_qty="0.000", status=ExchangeOrderStatus.CANCELED, price="96501"),
            ),
        ],
        market_scripts=[
            ScriptedOrder(snapshots=[make_order("m1", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.006", executed_qty="0.006", status=ExchangeOrderStatus.FILLED, price="96501")]),
        ],
    )
    repo = build_repository(tmp_path)
    clock = FakeClock()
    engine = PairedOpeningEngine(gateway, repo, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = build_session()
    repo.create_session(session)
    execution = await engine.execute_round(session=session, round_index=1, control=SessionControl())
    assert execution.stage1_filled_qty == Decimal("0.006")
    assert execution.stage2_filled_qty == Decimal("0.006")
    assert execution.market_fallback_used is True


@pytest.mark.asyncio
async def test_stage2_carryover_is_applied_to_next_round(tmp_path: Path) -> None:
    gateway = FakeGateway(
        rules=SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        quote=Quote(symbol="BTCUSDT", bid_price=Decimal("2000"), ask_price=Decimal("2001")),
        limit_scripts=[
            ScriptedOrder(snapshots=[make_order("r1s1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.010", status=ExchangeOrderStatus.FILLED, price="1000")]),
            ScriptedOrder(
                snapshots=[make_order("r1s2", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.010", executed_qty="0.009", status=ExchangeOrderStatus.PARTIALLY_FILLED, price="1001")],
                cancel_snapshot=make_order("r1s2", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.010", executed_qty="0.009", status=ExchangeOrderStatus.CANCELED, price="1001"),
            ),
            ScriptedOrder(snapshots=[make_order("r2s1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.010", status=ExchangeOrderStatus.FILLED, price="1000")]),
            ScriptedOrder(snapshots=[make_order("r2s2", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.011", executed_qty="0.011", status=ExchangeOrderStatus.FILLED, price="1001")]),
        ],
    )
    repo = build_repository(tmp_path)
    clock = FakeClock()
    engine = PairedOpeningEngine(gateway, repo, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = build_session(round_count=2)
    repo.create_session(session)

    completed_rounds, skipped_rounds = await engine.execute_session(session, SessionControl())
    rounds = repo.list_rounds(session.session_id)

    assert completed_rounds == 2
    assert skipped_rounds == 0
    assert rounds[0]["notes"]["stage2_remaining_qty"] == "0.001"
    assert rounds[1]["notes"]["stage2_target_qty"] == "0.011"
    assert session.stage2_carryover_qty == Decimal("0")


@pytest.mark.asyncio
async def test_final_alignment_reduces_both_sides_to_match(tmp_path: Path) -> None:
    gateway = FakeGateway(
        rules=SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        quote=Quote(symbol="BTCUSDT", bid_price=Decimal("2000"), ask_price=Decimal("2001")),
        limit_scripts=[
            ScriptedOrder(snapshots=[make_order("s1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.010", status=ExchangeOrderStatus.FILLED, price="1000")]),
            ScriptedOrder(
                snapshots=[make_order("s2", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.010", executed_qty="0.009", status=ExchangeOrderStatus.PARTIALLY_FILLED, price="1001")],
                cancel_snapshot=make_order("s2", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.010", executed_qty="0.009", status=ExchangeOrderStatus.CANCELED, price="1001"),
            ),
        ],
        market_scripts=[
            ScriptedOrder(snapshots=[make_order("fa-small", side=OrderSide.BUY, position_side=PositionSide.SHORT, qty="0.005", executed_qty="0.005", status=ExchangeOrderStatus.FILLED, price="1001")]),
            ScriptedOrder(snapshots=[make_order("fa-large", side=OrderSide.SELL, position_side=PositionSide.LONG, qty="0.006", executed_qty="0.006", status=ExchangeOrderStatus.FILLED, price="1000")]),
        ],
        account_overviews=[
            [
                {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.010")},
                {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.009")},
            ],
            [
                {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.004")},
                {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.004")},
            ],
        ],
    )
    repo = build_repository(tmp_path)
    clock = FakeClock()
    engine = PairedOpeningEngine(gateway, repo, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = build_session()
    repo.create_session(session)

    await engine.execute_session(session, SessionControl())

    assert session.final_alignment_status == FinalAlignmentStatus.MARKET_ALIGNED
    assert session.completed_with_final_alignment is True
    assert session.final_unaligned_qty == Decimal("0")


@pytest.mark.asyncio
async def test_single_open_market_fallback_respects_ratio_and_leaves_residual(tmp_path: Path) -> None:
    gateway = FakeGateway(
        rules=SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        quote=Quote(symbol="BTCUSDT", bid_price=Decimal("96500"), ask_price=Decimal("96501")),
        limit_scripts=[
            ScriptedOrder(
                snapshots=[make_order("so1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.000", status=ExchangeOrderStatus.NEW)],
                cancel_snapshot=make_order("so1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.000", status=ExchangeOrderStatus.CANCELED),
            ),
        ],
        market_scripts=[
            ScriptedOrder(snapshots=[make_order("som1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.002", executed_qty="0.002", status=ExchangeOrderStatus.FILLED)]),
        ],
    )
    repo = build_repository(tmp_path)
    clock = FakeClock()
    engine = SingleOpeningEngine(gateway, repo, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = OpenSession.create(
        SessionSpec(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            leverage=50,
            round_count=1,
            round_qty=Decimal("0.010"),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=1,
            market_fallback_attempts=1,
            execution_profile=ExecutionProfile.MAKER_FIRST,
            market_fallback_max_ratio=Decimal("0.25"),
            market_fallback_min_residual_qty=Decimal("0"),
            max_reprice_ticks=3,
            max_spread_bps=8,
            max_reference_deviation_bps=15,
            selected_position_side=PositionSide.LONG,
            target_open_qty=Decimal("0.010"),
            session_kind=SessionKind.SINGLE_OPEN,
            created_by="test",
        )
    )
    repo.create_session(session)

    execution = await engine.execute_round(session=session, round_index=1, control=SessionControl())

    assert execution.market_fallback_used is True
    assert execution.stage1_filled_qty == Decimal("0.002")
    assert Decimal(execution.notes["round_remaining_qty"]) == Decimal("0.008")
    assert session.final_unaligned_qty == Decimal("0.008")
    assert gateway.market_order_requests[0]["qty"] == Decimal("0.002")


@pytest.mark.asyncio
async def test_single_open_price_guard_blocks_before_order_placement(tmp_path: Path) -> None:
    gateway = FakeGateway(
        rules=SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        quote=Quote(symbol="BTCUSDT", bid_price=Decimal("100"), ask_price=Decimal("102")),
        limit_scripts=[],
    )
    repo = build_repository(tmp_path)
    clock = FakeClock()
    engine = SingleOpeningEngine(gateway, repo, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = OpenSession.create(
        SessionSpec(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            leverage=50,
            round_count=1,
            round_qty=Decimal("0.100"),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=1,
            market_fallback_attempts=1,
            execution_profile=ExecutionProfile.MAKER_FIRST,
            market_fallback_max_ratio=Decimal("0.25"),
            market_fallback_min_residual_qty=Decimal("0"),
            max_reprice_ticks=3,
            max_spread_bps=5,
            max_reference_deviation_bps=15,
            selected_position_side=PositionSide.LONG,
            target_open_qty=Decimal("0.100"),
            session_kind=SessionKind.SINGLE_OPEN,
            created_by="test",
        )
    )
    repo.create_session(session)

    execution = await engine.execute_round(session=session, round_index=1, control=SessionControl())
    payload = repo.get_session(session.session_id)

    assert execution.stage1_filled_qty == Decimal("0")
    assert execution.status == RoundStatus.STAGE1_SKIPPED
    assert session.final_unaligned_qty == Decimal("0.100")
    assert gateway.orders == {}
    assert any(event["event_type"] == "price_guard_blocked" for event in payload["events"])


@pytest.mark.asyncio
async def test_observe_order_prefers_cached_user_stream_state(tmp_path: Path) -> None:
    gateway = FakeGateway(
        rules=SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        quote=Quote(symbol="BTCUSDT", bid_price=Decimal("96500"), ask_price=Decimal("96501")),
        limit_scripts=[
            ScriptedOrder(
                snapshots=[make_order("cache-1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.000", status=ExchangeOrderStatus.NEW)],
                cancel_snapshot=make_order("cache-1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.010", status=ExchangeOrderStatus.FILLED),
            ),
            ScriptedOrder(
                snapshots=[make_order("cache-2", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.010", executed_qty="0.010", status=ExchangeOrderStatus.FILLED, price="96501")],
            ),
        ],
    )
    gateway.cached_orders["cache-1"] = make_order(
        "cache-1",
        side=OrderSide.BUY,
        position_side=PositionSide.LONG,
        qty="0.010",
        executed_qty="0.010",
        status=ExchangeOrderStatus.FILLED,
    )
    gateway.cached_orders["cache-2"] = make_order(
        "cache-2",
        side=OrderSide.SELL,
        position_side=PositionSide.SHORT,
        qty="0.010",
        executed_qty="0.010",
        status=ExchangeOrderStatus.FILLED,
        price="96501",
    )
    repo = build_repository(tmp_path)
    clock = FakeClock()
    engine = PairedOpeningEngine(gateway, repo, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = build_session()
    repo.create_session(session)

    execution = await engine.execute_round(session=session, round_index=1, control=SessionControl())

    assert execution.stage1_filled_qty == Decimal("0.010")
    assert gateway.get_order_calls == 0


@pytest.mark.asyncio
async def test_observe_order_falls_back_to_rest_when_cached_stream_is_unhealthy(tmp_path: Path) -> None:
    gateway = FakeGateway(
        rules=SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        quote=Quote(symbol="BTCUSDT", bid_price=Decimal("96500"), ask_price=Decimal("96501")),
        limit_scripts=[
            ScriptedOrder(
                snapshots=[make_order("cache-rest-1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.010", status=ExchangeOrderStatus.FILLED)],
            ),
            ScriptedOrder(
                snapshots=[make_order("cache-rest-2", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.010", executed_qty="0.010", status=ExchangeOrderStatus.FILLED, price="96501")],
            ),
        ],
        stream_healthy=False,
    )
    gateway.cached_orders["cache-rest-1"] = make_order(
        "cache-rest-1",
        side=OrderSide.BUY,
        position_side=PositionSide.LONG,
        qty="0.010",
        executed_qty="0.000",
        status=ExchangeOrderStatus.NEW,
    )
    repo = build_repository(tmp_path)
    clock = FakeClock()
    engine = PairedOpeningEngine(gateway, repo, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = build_session()
    repo.create_session(session)

    execution = await engine.execute_round(session=session, round_index=1, control=SessionControl())
    payload = repo.get_session(session.session_id)

    assert execution.stage1_filled_qty == Decimal("0.010")
    assert gateway.get_order_calls >= 1
    assert any(event["event_type"] == "user_stream_degraded" for event in payload["events"])


@pytest.mark.asyncio
async def test_final_alignment_flattens_both_sides_when_small_side_too_small(tmp_path: Path) -> None:
    gateway = FakeGateway(
        rules=SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        quote=Quote(symbol="BTCUSDT", bid_price=Decimal("4000"), ask_price=Decimal("4001")),
        limit_scripts=[
            ScriptedOrder(snapshots=[make_order("s1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.002", executed_qty="0.002", status=ExchangeOrderStatus.FILLED, price="4000")]),
            ScriptedOrder(
                snapshots=[make_order("s2", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.002", executed_qty="0.001", status=ExchangeOrderStatus.PARTIALLY_FILLED, price="4001")],
                cancel_snapshot=make_order("s2", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.002", executed_qty="0.001", status=ExchangeOrderStatus.CANCELED, price="4001"),
            ),
        ],
        market_scripts=[
            ScriptedOrder(snapshots=[make_order("flat-long", side=OrderSide.SELL, position_side=PositionSide.LONG, qty="0.002", executed_qty="0.002", status=ExchangeOrderStatus.FILLED, price="4000")]),
            ScriptedOrder(snapshots=[make_order("flat-short", side=OrderSide.BUY, position_side=PositionSide.SHORT, qty="0.001", executed_qty="0.001", status=ExchangeOrderStatus.FILLED, price="4001")]),
        ],
        account_overviews=[
            [
                {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.002")},
                {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.001")},
            ],
        ],
    )
    repo = build_repository(tmp_path)
    clock = FakeClock()
    engine = PairedOpeningEngine(gateway, repo, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = OpenSession.create(
        SessionSpec(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            leverage=50,
            round_count=1,
            round_qty=Decimal("0.002"),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=2,
            market_fallback_attempts=2,
            created_by="test",
        )
    )
    repo.create_session(session)

    await engine.execute_session(session, SessionControl())

    assert session.final_alignment_status == FinalAlignmentStatus.FLATTENED_BOTH_SIDES
    assert session.completed_with_final_alignment is True
    assert session.final_unaligned_qty == Decimal("0")


@pytest.mark.asyncio
async def test_final_alignment_failure_sets_failed_status(tmp_path: Path) -> None:
    gateway = FakeGateway(
        rules=SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        quote=Quote(symbol="BTCUSDT", bid_price=Decimal("2000"), ask_price=Decimal("2001")),
        limit_scripts=[
            ScriptedOrder(snapshots=[make_order("s1", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.010", executed_qty="0.010", status=ExchangeOrderStatus.FILLED, price="1000")]),
            ScriptedOrder(
                snapshots=[make_order("s2", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.010", executed_qty="0.009", status=ExchangeOrderStatus.PARTIALLY_FILLED, price="1001")],
                cancel_snapshot=make_order("s2", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.010", executed_qty="0.009", status=ExchangeOrderStatus.CANCELED, price="1001"),
            ),
        ],
        market_scripts=[
            ScriptedOrder(snapshots=[make_order("fa-small-1", side=OrderSide.BUY, position_side=PositionSide.SHORT, qty="0.005", executed_qty="0.000", status=ExchangeOrderStatus.NEW, price="1001")]),
            ScriptedOrder(snapshots=[make_order("fa-small-2", side=OrderSide.BUY, position_side=PositionSide.SHORT, qty="0.005", executed_qty="0.000", status=ExchangeOrderStatus.NEW, price="1001")]),
        ],
        account_overviews=[
            [
                {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.010")},
                {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.009")},
            ],
        ],
    )
    repo = build_repository(tmp_path)
    clock = FakeClock()
    engine = PairedOpeningEngine(gateway, repo, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = build_session()
    repo.create_session(session)

    with pytest.raises(Exception):
        await engine.execute_session(session, SessionControl())

    assert session.final_alignment_status == FinalAlignmentStatus.FAILED
    assert session.final_unaligned_qty == Decimal("0.001")


@pytest.mark.asyncio
async def test_final_alignment_uses_session_owned_qty_instead_of_account_totals(tmp_path: Path) -> None:
    gateway = FakeGateway(
        rules=SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        quote=Quote(symbol="BTCUSDT", bid_price=Decimal("4000"), ask_price=Decimal("4001")),
        limit_scripts=[
            ScriptedOrder(snapshots=[make_order("s1-own", side=OrderSide.BUY, position_side=PositionSide.LONG, qty="0.002", executed_qty="0.002", status=ExchangeOrderStatus.FILLED, price="4000")]),
            ScriptedOrder(
                snapshots=[make_order("s2-own", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.002", executed_qty="0.001", status=ExchangeOrderStatus.PARTIALLY_FILLED, price="4001")],
                cancel_snapshot=make_order("s2-own", side=OrderSide.SELL, position_side=PositionSide.SHORT, qty="0.002", executed_qty="0.001", status=ExchangeOrderStatus.CANCELED, price="4001"),
            ),
        ],
        market_scripts=[
            ScriptedOrder(snapshots=[make_order("flat-long-own", side=OrderSide.SELL, position_side=PositionSide.LONG, qty="0.002", executed_qty="0.002", status=ExchangeOrderStatus.FILLED, price="4000")]),
            ScriptedOrder(snapshots=[make_order("flat-short-own", side=OrderSide.BUY, position_side=PositionSide.SHORT, qty="0.001", executed_qty="0.001", status=ExchangeOrderStatus.FILLED, price="4001")]),
        ],
        account_overviews=[
            [
                {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("1.002")},
                {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("1.001")},
            ],
        ],
    )
    repo = build_repository(tmp_path)
    clock = FakeClock()
    engine = PairedOpeningEngine(gateway, repo, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = OpenSession.create(
        SessionSpec(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            leverage=50,
            round_count=1,
            round_qty=Decimal("0.002"),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=2,
            market_fallback_attempts=2,
            created_by="test",
        )
    )
    repo.create_session(session)

    await engine.execute_session(session, SessionControl())

    assert session.final_alignment_status == FinalAlignmentStatus.FLATTENED_BOTH_SIDES
    assert [request["qty"] for request in gateway.market_order_requests] == [Decimal("0.002"), Decimal("0.001")]






