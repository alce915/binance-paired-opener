from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from paired_opener.config import Settings
from paired_opener.domain import (
    ExchangeOrder,
    ExchangeOrderStatus,
    FinalAlignmentStatus,
    OpenSession,
    OrderSide,
    PositionSide,
    Quote,
    SessionAbortedError,
    SessionSpec,
    SessionStatus,
    SymbolRules,
    TrendBias,
)
from paired_opener.engine import PairedOpeningEngine, SessionControl
from paired_opener.exchange import ExchangeGateway
from paired_opener.service import ManagedSession, OpenSessionService
from paired_opener.schemas import CloseSessionRequest, OpenSessionRequest, SingleCloseSessionRequest, SingleOpenSessionRequest
from paired_opener.storage import SqliteRepository


class SimpleGateway(ExchangeGateway):
    def __init__(self) -> None:
        self.rules = SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        )
        self.orders: dict[str, ExchangeOrder] = {}
        self.positions: list[dict[str, Decimal | str | int]] = []
        self.order_calls: list[dict[str, object]] = []

    async def ensure_hedge_mode(self) -> None:
        return None

    async def ensure_cross_margin(self, symbol: str) -> None:
        return None

    async def ensure_leverage(self, symbol: str, leverage: int) -> None:
        return None

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        return SymbolRules(
            symbol=symbol,
            tick_size=self.rules.tick_size,
            step_size=self.rules.step_size,
            min_qty=self.rules.min_qty,
            min_notional=self.rules.min_notional,
            max_leverage=self.rules.max_leverage,
        )

    async def get_quote(self, symbol: str) -> Quote:
        return Quote(symbol=symbol, bid_price=Decimal("100"), ask_price=Decimal("101"))

    async def get_order_book(self, symbol: str, limit: int = 10) -> dict:
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
            "totals": {
                "equity": Decimal("1000"),
                "margin": Decimal("100"),
                "available_balance": Decimal("900"),
                "unrealized_pnl": Decimal("0"),
            },
            "positions": list(self.positions),
            "updated_at": datetime.now(UTC),
        }

    async def place_limit_order(self, **kwargs) -> ExchangeOrder:
        order = ExchangeOrder(
            symbol=kwargs["symbol"],
            order_id=kwargs["client_order_id"],
            client_order_id=kwargs["client_order_id"],
            side=kwargs["side"],
            position_side=kwargs["position_side"],
            type="LIMIT",
            price=kwargs["price"],
            orig_qty=kwargs["qty"],
            executed_qty=kwargs["qty"],
            status=ExchangeOrderStatus.FILLED,
            update_time=datetime.now(UTC),
        )
        self.orders[order.order_id] = order
        self.order_calls.append({
            "side": kwargs["side"],
            "position_side": kwargs["position_side"],
            "type": "LIMIT",
            "qty": kwargs["qty"],
        })
        return order

    async def place_market_order(self, **kwargs) -> ExchangeOrder:
        order = ExchangeOrder(
            symbol=kwargs["symbol"],
            order_id=kwargs["client_order_id"],
            client_order_id=kwargs["client_order_id"],
            side=kwargs["side"],
            position_side=kwargs["position_side"],
            type="MARKET",
            price=Decimal("0"),
            orig_qty=kwargs["qty"],
            executed_qty=kwargs["qty"],
            status=ExchangeOrderStatus.FILLED,
            update_time=datetime.now(UTC),
        )
        self.orders[order.order_id] = order
        self.order_calls.append({
            "side": kwargs["side"],
            "position_side": kwargs["position_side"],
            "type": "MARKET",
            "qty": kwargs["qty"],
        })
        return order

    async def get_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        return self.orders[order_id]

    async def cancel_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        return self.orders[order_id]


class IntervalAbortGateway(SimpleGateway):
    def __init__(self) -> None:
        super().__init__()
        self.order_count = 0

    async def place_limit_order(self, **kwargs) -> ExchangeOrder:
        self.order_count += 1
        return await super().place_limit_order(**kwargs)


class DustCarryoverGateway(SimpleGateway):
    def __init__(self) -> None:
        super().__init__()
        self.rules = SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        )
        self.positions = [
            {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.200")},
            {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.200")},
        ]
        self._close_stage2_partial_applied = False

    def _apply_fill(self, position_side: PositionSide, qty: Decimal) -> None:
        for position in self.positions:
            if position["symbol"] == "BTCUSDT" and position["position_side"] == position_side.value:
                position["qty"] = max(Decimal("0"), Decimal(str(position["qty"])) - qty)
                return

    async def place_limit_order(self, **kwargs) -> ExchangeOrder:
        order = await super().place_limit_order(**kwargs)
        if kwargs["position_side"] == PositionSide.LONG and not self._close_stage2_partial_applied:
            order.executed_qty = Decimal("0")
            order.status = ExchangeOrderStatus.NEW
            self.orders[order.order_id] = order
            return order
        self._apply_fill(kwargs["position_side"], kwargs["qty"])
        return order

    async def place_market_order(self, **kwargs) -> ExchangeOrder:
        order = await super().place_market_order(**kwargs)
        self._apply_fill(kwargs["position_side"], kwargs["qty"])
        return order

    async def get_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        order = self.orders[order_id]
        if order.position_side == PositionSide.LONG and not self._close_stage2_partial_applied:
            order.executed_qty = Decimal("0.002")
            order.status = ExchangeOrderStatus.PARTIALLY_FILLED
            self._close_stage2_partial_applied = True
            self._apply_fill(PositionSide.LONG, Decimal("0.002"))
        return order

class AbortingClock:
    def __init__(self, control: SessionControl) -> None:
        self.control = control
        self.now = 0.0
        self.aborted = False

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds
        if not self.aborted and seconds == 0.1:
            self.control.aborted = True
            self.aborted = True

class RoundAbortClock:
    def __init__(self, control: SessionControl) -> None:
        self.control = control
        self.now = 0.0
        self.aborted = False

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds
        if not self.aborted and seconds == 0.05:
            self.control.aborted = True
            self.aborted = True

@pytest.mark.asyncio
async def test_update_whitelist_persists_symbols(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"], symbol_whitelist_file=tmp_path / "wl.json")
    repository = SqliteRepository(tmp_path / "test.db")
    service = OpenSessionService(settings, repository, SimpleGateway(), object())

    updated = await service.update_whitelist(["ethusdt", "btcusdt", "ethusdt"])

    assert updated == ["ETHUSDT", "BTCUSDT"]
    assert settings.symbol_whitelist_file.exists()
    assert 'ETHUSDT' in settings.symbol_whitelist_file.read_text(encoding="utf-8")


def test_load_persisted_whitelist_ignores_invalid_json(tmp_path: Path) -> None:
    file_path = tmp_path / "broken.json"
    file_path.write_text('{broken', encoding="utf-8")
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"], symbol_whitelist_file=file_path)

    settings.load_persisted_whitelist()

    assert settings.symbol_whitelist == ["BTCUSDT"]


@pytest.mark.asyncio
async def test_execute_session_can_abort_during_round_interval(tmp_path: Path) -> None:
    gateway = IntervalAbortGateway()
    repository = SqliteRepository(tmp_path / "engine.db")
    control = SessionControl()
    clock = AbortingClock(control)
    engine = PairedOpeningEngine(gateway, repository, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = OpenSession.create(
        SessionSpec(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            leverage=50,
            round_count=2,
            round_qty=Decimal("0.100"),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=2,
            market_fallback_attempts=2,
            round_interval_seconds=3,
            created_by="test",
        )
    )
    repository.create_session(session)

    with pytest.raises(SessionAbortedError):
        await engine.execute_session(session, control)

    assert gateway.order_count == 2


def test_repository_backfills_round_interval_column_for_existing_db(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    repository = SqliteRepository(db_path)
    with repository._connection:
        repository._connection.execute("DROP TABLE sessions")
        repository._connection.execute(
            '''
            CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                trend_bias TEXT NOT NULL,
                leverage INTEGER NOT NULL,
                round_count INTEGER NOT NULL,
                round_qty TEXT NOT NULL,
                poll_interval_ms INTEGER NOT NULL,
                order_ttl_ms INTEGER NOT NULL,
                max_zero_fill_retries INTEGER NOT NULL,
                market_fallback_attempts INTEGER NOT NULL,
                created_by TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_error TEXT
            )
            '''
        )
    repository._initialize()

    columns = {
        row["name"]
        for row in repository._connection.execute("PRAGMA table_info(sessions)").fetchall()
    }

    assert "round_interval_seconds" in columns


def test_repository_persists_round_interval_seconds(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "session.db")
    session = OpenSession.create(
        SessionSpec(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            leverage=50,
            round_count=2,
            round_qty=Decimal("0.010"),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=2,
            market_fallback_attempts=2,
            round_interval_seconds=7,
            created_by="test",
        )
    )

    repository.create_session(session)
    payload = repository.get_session(session.session_id)

    assert payload is not None
    assert payload["round_interval_seconds"] == 7


def test_service_marks_incomplete_sessions_exception_on_startup(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "restart.db")
    session = OpenSession.create(
        SessionSpec(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            leverage=50,
            round_count=1,
            round_qty=Decimal("0.010"),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=2,
            market_fallback_attempts=2,
            round_interval_seconds=3,
            created_by="test",
        )
    )
    repository.create_session(session)
    repository.update_session_status(session.session_id, SessionStatus.RUNNING)

    service = OpenSessionService(Settings(_env_file=None), repository, SimpleGateway(), object())
    payload = service.get_session(session.session_id)

    assert payload["status"] == SessionStatus.EXCEPTION.value
    assert payload["last_error"] == "Service restarted before session completion"
    assert any(event["event_type"] == "session_recovered_on_startup" for event in payload["events"])


@pytest.mark.asyncio
async def test_create_session_rejects_round_notional_below_minimum(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "reject.db")
    gateway = SimpleGateway()
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)

    with pytest.raises(ValueError, match="最小开单金额"):
        await service.create_session(
            OpenSessionRequest(
                symbol="BTCUSDT",
                trend_bias=TrendBias.LONG,
                leverage=1,
                round_count=1,
                round_qty=Decimal("0.001"),
            )
        )




@pytest.mark.asyncio
async def test_execute_session_finishes_current_round_before_abort(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "abort-safe.db")
    gateway = SimpleGateway()
    original_place_limit_order = gateway.place_limit_order
    stage2_polled = False
    stage2_query_count = 0

    async def scripted_place_limit_order(**kwargs) -> ExchangeOrder:
        nonlocal stage2_polled
        order = await original_place_limit_order(**kwargs)
        if kwargs["position_side"] == PositionSide.SHORT and not stage2_polled:
            order.status = ExchangeOrderStatus.NEW
            order.executed_qty = Decimal("0")
            gateway.orders[order.order_id] = order
        return order

    async def scripted_get_order(*, symbol: str, order_id: str) -> ExchangeOrder:
        nonlocal stage2_polled, stage2_query_count
        order = gateway.orders[order_id]
        if order.position_side == PositionSide.SHORT:
            stage2_query_count += 1
            if stage2_query_count == 1:
                return order
            if not stage2_polled:
                stage2_polled = True
                order.executed_qty = order.orig_qty
                order.status = ExchangeOrderStatus.FILLED
        return order

    gateway.place_limit_order = scripted_place_limit_order  # type: ignore[method-assign]
    gateway.get_order = scripted_get_order  # type: ignore[method-assign]

    control = SessionControl()
    clock = RoundAbortClock(control)
    engine = PairedOpeningEngine(gateway, repository, sleep_func=clock.sleep, monotonic_func=clock.monotonic)
    session = OpenSession.create(
        SessionSpec(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            leverage=50,
            round_count=2,
            round_qty=Decimal("0.100"),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=2,
            market_fallback_attempts=2,
            round_interval_seconds=3,
            created_by="test",
        )
    )
    repository.create_session(session)

    with pytest.raises(SessionAbortedError, match="safe checkpoint"):
        await engine.execute_session(session, control)

    rounds = repository.list_rounds(session.session_id)
    assert len(rounds) == 1
    assert rounds[0]["status"] == "round_completed"


@pytest.mark.asyncio
async def test_abort_session_does_not_mark_status_aborted_before_task_exits(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "abort-status.db")
    gateway = SimpleGateway()
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)
    session = OpenSession.create(
        SessionSpec(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            leverage=50,
            round_count=1,
            round_qty=Decimal("0.100"),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=2,
            market_fallback_attempts=2,
            round_interval_seconds=3,
            created_by="test",
        )
    )
    repository.create_session(session)
    repository.update_session_status(session.session_id, SessionStatus.RUNNING)

    async def blocker() -> None:
        await asyncio.Future()

    task = asyncio.create_task(blocker())
    service._managed[session.session_id] = ManagedSession(symbol="BTCUSDT", control=SessionControl(), task=task)
    try:
        status = await service.abort_session(session.session_id)
        payload = service.get_session(session.session_id)
        assert status == SessionStatus.RUNNING
        assert payload["status"] == SessionStatus.RUNNING.value
        assert any(event["event_type"] == "session_abort_requested" for event in payload["events"])
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_service_close_cancels_managed_tasks(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "service-close.db")
    gateway = SimpleGateway()
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)

    async def blocker() -> None:
        await asyncio.Future()

    task = asyncio.create_task(blocker())
    service._managed["test-session"] = ManagedSession(symbol="BTCUSDT", control=SessionControl(), task=task)

    await service.close(timeout_seconds=0.05)

    assert task.cancelled()
    assert service._managed == {}




@pytest.mark.asyncio
async def test_create_session_rejects_open_amount_above_available_balance_ratio(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "available-balance.db")
    gateway = SimpleGateway()
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)

    with pytest.raises(ValueError, match="可用余额"):
        await service.create_session(
            OpenSessionRequest(
                symbol="BTCUSDT",
                trend_bias=TrendBias.LONG,
                leverage=10,
                round_count=10,
                round_qty=Decimal("10"),
            )
        )


@pytest.mark.asyncio
async def test_create_close_session_rejects_qty_above_max_closeable(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "close-reject.db")
    gateway = SimpleGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.010")},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.005")},
    ]
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)

    with pytest.raises(ValueError, match="可双向平仓数量"):
        await service.create_close_session(
            CloseSessionRequest(
                symbol="BTCUSDT",
                trend_bias=TrendBias.LONG,
                close_qty=Decimal("0.010"),
                round_count=1,
            )
        )


@pytest.mark.asyncio
async def test_create_close_session_long_closes_short_then_long(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "close-success.db")
    gateway = SimpleGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.100")},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.100")},
    ]
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)

    session = await service.create_close_session(
        CloseSessionRequest(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            close_qty=Decimal("0.050"),
            round_count=1,
        )
    )
    task = service._managed[session.session_id].task
    await task
    payload = service.get_session(session.session_id)

    assert payload["session_kind"] == "paired_close"
    assert payload["status"] == SessionStatus.COMPLETED.value
    assert gateway.order_calls[0]["side"] == OrderSide.BUY
    assert gateway.order_calls[0]["position_side"] == PositionSide.SHORT
    assert gateway.order_calls[1]["side"] == OrderSide.SELL
    assert gateway.order_calls[1]["position_side"] == PositionSide.LONG

@pytest.mark.asyncio
async def test_close_session_final_alignment_handles_stage2_dust(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "close-dust.db")
    gateway = DustCarryoverGateway()
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)

    session = await service.create_close_session(
        CloseSessionRequest(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            close_qty=Decimal("0.051"),
            round_count=1,
            market_fallback_attempts=1,
        )
    )
    await service._managed[session.session_id].task
    payload = service.get_session(session.session_id)

    assert payload["status"] == SessionStatus.COMPLETED.value
    assert payload["final_alignment_status"] == FinalAlignmentStatus.MARKET_ALIGNED.value
    assert Decimal(payload["stage2_carryover_qty"]) == Decimal("0")
    assert payload["completed_with_final_alignment"] is True
    assert any(event["event_type"] == "close_stage2_below_min_carryover" for event in payload["events"])
    assert any(event["event_type"] == "close_final_alignment_completed" for event in payload["events"])
    market_calls = [call for call in gateway.order_calls if call["type"] == "MARKET"]
    assert len(market_calls) >= 2

@pytest.mark.asyncio
async def test_create_single_close_session_rejects_qty_above_selected_position(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "single-close-reject.db")
    gateway = SimpleGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.010")},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.005")},
    ]
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)

    with pytest.raises(ValueError, match="超过所选持仓数量"):
        await service.create_single_close_session(
            SingleCloseSessionRequest(
                symbol="BTCUSDT",
                close_mode="regular",
                selected_position_side=PositionSide.SHORT,
                close_qty=Decimal("0.010"),
                round_count=1,
            )
        )


@pytest.mark.asyncio
async def test_create_single_close_session_align_mode_uses_larger_side_difference(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "single-close-align.db")
    gateway = SimpleGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.200")},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.100")},
    ]
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)

    session = await service.create_single_close_session(
        SingleCloseSessionRequest(
            symbol="BTCUSDT",
            close_mode="align",
            close_qty=Decimal("0.001"),
            round_count=2,
        )
    )

    payload = service.get_session(session.session_id)
    assert payload["session_kind"] == "single_close"
    assert payload["close_mode"] == "align"
    assert payload["selected_position_side"] == PositionSide.LONG.value
    assert Decimal(payload["target_close_qty"]) == Decimal("0.100")
    await service.close()


@pytest.mark.asyncio
async def test_create_single_close_session_regular_long_uses_sell_long_orders(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "single-close-regular.db")
    gateway = SimpleGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.100")},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.080")},
    ]
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)

    session = await service.create_single_close_session(
        SingleCloseSessionRequest(
            symbol="BTCUSDT",
            close_mode="regular",
            selected_position_side=PositionSide.LONG,
            close_qty=Decimal("0.050"),
            round_count=1,
        )
    )
    await service._managed[session.session_id].task
    payload = service.get_session(session.session_id)

    assert payload["status"] == SessionStatus.COMPLETED.value
    assert payload["session_kind"] == "single_close"
    assert gateway.order_calls[0]["side"] == OrderSide.SELL
    assert gateway.order_calls[0]["position_side"] == PositionSide.LONG
    assert any(event["event_type"] == "single_close_round_completed" for event in payload["events"])




@pytest.mark.asyncio
async def test_create_single_open_session_align_mode_uses_smaller_side_difference(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "single-open-align.db")
    gateway = SimpleGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.100"), "leverage": 50},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.250"), "leverage": 50},
    ]
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)

    session = await service.create_single_open_session(
        SingleOpenSessionRequest(
            symbol="BTCUSDT",
            open_mode="align",
            open_qty=Decimal("0.001"),
            leverage=50,
            round_count=2,
        )
    )

    payload = service.get_session(session.session_id)
    assert payload["session_kind"] == "single_open"
    assert payload["open_mode"] == "align"
    assert payload["selected_position_side"] == PositionSide.LONG.value
    assert Decimal(payload["target_open_qty"]) == Decimal("0.150")
    assert payload["leverage"] == 50
    await service.close()


@pytest.mark.asyncio
async def test_create_single_open_session_regular_short_uses_sell_short_orders(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "single-open-regular.db")
    gateway = SimpleGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.100"), "leverage": 50},
        {"symbol": "BTCUSDT", "position_side": "SHORT", "qty": Decimal("0.080"), "leverage": 50},
    ]
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)

    session = await service.create_single_open_session(
        SingleOpenSessionRequest(
            symbol="BTCUSDT",
            open_mode="regular",
            selected_position_side=PositionSide.SHORT,
            open_qty=Decimal("0.050"),
            leverage=50,
            round_count=1,
        )
    )
    await service._managed[session.session_id].task
    payload = service.get_session(session.session_id)

    assert payload["status"] == SessionStatus.COMPLETED.value
    assert payload["session_kind"] == "single_open"
    assert gateway.order_calls[0]["side"] == OrderSide.SELL
    assert gateway.order_calls[0]["position_side"] == PositionSide.SHORT
    assert any(event["event_type"] == "single_open_round_completed" for event in payload["events"])
    assert payload["leverage"] == 50


@pytest.mark.asyncio
async def test_create_single_open_session_rejects_when_existing_position_leverage_mismatches(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "single-open-leverage-mismatch.db")
    gateway = SimpleGateway()
    gateway.positions = [
        {"symbol": "BTCUSDT", "position_side": "LONG", "qty": Decimal("0.100"), "leverage": 50},
    ]
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)

    with pytest.raises(ValueError, match="杠杆必须与现有持仓一致"):
        await service.create_single_open_session(
            SingleOpenSessionRequest(
                symbol="BTCUSDT",
                open_mode="regular",
                selected_position_side=PositionSide.LONG,
                open_qty=Decimal("0.010"),
                leverage=20,
                round_count=1,
            )
        )


@pytest.mark.asyncio
async def test_create_single_open_session_rejects_when_implied_open_amount_exceeds_balance_limit(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, symbol_whitelist=["BTCUSDT"])
    repository = SqliteRepository(tmp_path / "single-open-balance.db")
    gateway = SimpleGateway()
    engine = PairedOpeningEngine(gateway, repository)
    service = OpenSessionService(settings, repository, gateway, engine)

    with pytest.raises(ValueError, match="95%"):
        await service.create_single_open_session(
            SingleOpenSessionRequest(
                symbol="BTCUSDT",
                open_mode="regular",
                selected_position_side=PositionSide.LONG,
                open_qty=Decimal("500"),
                leverage=1,
                round_count=1,
            )
        )
