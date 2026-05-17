from __future__ import annotations

import asyncio
import csv
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from paired_opener import simulation as simulation_module
from paired_opener.domain import ExchangeOrder, OrderSide, PositionSide, Quote, SessionKind, SingleCloseMode, SingleOpenMode, SymbolRules
from paired_opener.exchange import ExchangeGateway
from paired_opener.schemas import SimulationRunRequest
from paired_opener.simulation import SimulationError, SimulationService
from paired_opener.storage import SqliteRepository


class SimulationGateway(ExchangeGateway):
    def __init__(
        self,
        *,
        order_books: list[dict] | None = None,
        refresh_order_books: list[dict] | None = None,
        refresh_order_book_error: Exception | None = None,
    ) -> None:
        self.order_books = list(order_books or [])
        self.refresh_order_books = list(refresh_order_books or [])
        self.refresh_order_book_error = refresh_order_book_error
        self.quote_bid = Decimal("10000")
        self.quote_ask = Decimal("10001")
        self.refresh_order_book_calls = 0
        self.place_limit_order_calls = 0
        self.place_market_order_calls = 0
        self.account_overview_calls = 0

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
        return Quote(symbol=symbol, bid_price=self.quote_bid, ask_price=self.quote_ask)

    async def get_order_book(self, symbol: str, limit: int = 10) -> dict:
        if self.order_books:
            return self.order_books.pop(0)
        now = datetime.now(UTC)
        return {
            "symbol": symbol,
            "bids": [{"price": Decimal("10990"), "qty": Decimal("1")}],
            "asks": [{"price": Decimal("10000"), "qty": Decimal("0.004")}, {"price": Decimal("10100"), "qty": Decimal("1")}],
            "event_time": now,
        }

    async def refresh_order_book(self, symbol: str, limit: int = 10) -> dict:
        self.refresh_order_book_calls += 1
        if self.refresh_order_book_error is not None:
            raise self.refresh_order_book_error
        if self.refresh_order_books:
            return self.refresh_order_books.pop(0)
        return await self.get_order_book(symbol, limit=limit)

    async def get_account_overview(self) -> dict:
        self.account_overview_calls += 1
        return {
            "status": "ok",
            "totals": {"available_balance": Decimal("999999")},
            "positions": [{"symbol": "BTCUSDT", "position_side": "LONG", "qty": "999"}],
            "updated_at": datetime.now(UTC),
        }

    async def get_symbol_leverage(self, symbol: str) -> int:
        return 50

    async def get_open_orders(self, symbol: str) -> list[dict[str, object]]:
        return []

    async def place_limit_order(self, **kwargs) -> ExchangeOrder:
        self.place_limit_order_calls += 1
        raise AssertionError("simulation must not place real limit orders")

    async def place_market_order(self, **kwargs) -> ExchangeOrder:
        self.place_market_order_calls += 1
        raise AssertionError("simulation must not place real market orders")

    async def get_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        raise NotImplementedError

    async def cancel_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        raise NotImplementedError


def make_service(tmp_path: Path, gateway: SimulationGateway | None = None) -> SimulationService:
    repository = SqliteRepository(tmp_path / "simulation.sqlite3")
    return SimulationService(gateway or SimulationGateway(), repository)


class FastOrderWaitSimulationService(SimulationService):
    def _order_wait_seconds(self, request: SimulationRunRequest) -> Decimal:
        return Decimal("0")


class ShortOrderWaitSimulationService(SimulationService):
    def _order_wait_seconds(self, request: SimulationRunRequest) -> Decimal:
        return Decimal("1")


class SlowCompletingSimulationService(SimulationService):
    async def _execute_run(self, run_id, request, *, before, rerun_source_run_id):
        await asyncio.sleep(0.05)
        return self._result_payload(
            run_id,
            request,
            "completed",
            "filled",
            rerun_source_run_id,
            filled_qty=Decimal("0.010"),
            avg_fill_price=Decimal("10000"),
            fee=Decimal("0"),
            residual_qty=Decimal("0"),
            realized_pnl=Decimal("0"),
            rounds_completed=1,
            rounds_total=1,
        )


def make_fast_order_wait_service(tmp_path: Path, gateway: SimulationGateway | None = None) -> SimulationService:
    repository = SqliteRepository(tmp_path / "simulation.sqlite3")
    return FastOrderWaitSimulationService(gateway or SimulationGateway(), repository)


def make_short_order_wait_service(tmp_path: Path, gateway: SimulationGateway | None = None) -> SimulationService:
    repository = SqliteRepository(tmp_path / "simulation.sqlite3")
    return ShortOrderWaitSimulationService(gateway or SimulationGateway(), repository)


def make_slow_completing_service(tmp_path: Path, gateway: SimulationGateway | None = None) -> SimulationService:
    repository = SqliteRepository(tmp_path / "simulation.sqlite3")
    return SlowCompletingSimulationService(gateway or SimulationGateway(), repository)


def test_order_wait_seconds_is_fixed_ten_seconds(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    request = SimulationRunRequest(
        session_kind=SessionKind.PAIRED_OPEN,
        symbol="BTCUSDT",
        open_amount=Decimal("100"),
        leverage=10,
        round_count=2,
        round_interval_seconds=7,
    )

    assert service._order_wait_seconds(request) == Decimal("10")


async def start_long_running_simulation(service: SimulationService) -> asyncio.Task[dict]:
    task = asyncio.create_task(
        service.run(
            SimulationRunRequest(
                session_kind=SessionKind.SINGLE_OPEN,
                symbol="BTCUSDT",
                open_mode=SingleOpenMode.REGULAR,
                selected_position_side=PositionSide.LONG,
                open_qty=Decimal("0.010"),
                leverage=10,
                round_count=2,
                round_interval_seconds=30,
            )
        )
    )
    for _ in range(100):
        if service.is_active():
            return task
        await asyncio.sleep(0.01)
    raise AssertionError("simulation did not become active")


async def wait_for_active_run(service: SimulationService, run_id: str) -> None:
    for _ in range(100):
        active = await service.active_run()
        if active.get("active") and active.get("run_id") == run_id:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("simulation run did not become active")


@pytest.mark.asyncio
async def test_start_run_returns_immediately_and_active_updates_can_recover(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    request = SimulationRunRequest(
        session_kind=SessionKind.SINGLE_OPEN,
        symbol="BTCUSDT",
        open_mode=SingleOpenMode.REGULAR,
        selected_position_side=PositionSide.LONG,
        open_qty=Decimal("0.010"),
        leverage=10,
        round_count=2,
        round_interval_seconds=30,
    )

    payload = await service.start_run(request)
    await wait_for_active_run(service, payload["run_id"])
    active = await service.active_run()
    updates = await service.run_updates(payload["run_id"], after_event_id=0)

    assert payload["status"] == "running"
    assert payload["stage"] == "starting"
    assert active["active"] is True
    assert active["status"] in {"running", "aborting"}
    assert active["run_id"] == payload["run_id"]
    assert updates["latest_event_id"] >= 1
    assert updates["events"][0]["event_id"] == 1
    assert updates["events"][0]["message_code"] == "runtime.simulation_run_started"
    assert updates["account"]["contract_version"]

    await service.abort()
    if service._active_task is not None:
        await asyncio.wait_for(service._active_task, timeout=2)


@pytest.mark.asyncio
async def test_background_run_preserves_start_created_at_in_history(tmp_path: Path) -> None:
    service = make_fast_order_wait_service(tmp_path)
    request = SimulationRunRequest(
        session_kind=SessionKind.SINGLE_OPEN,
        symbol="BTCUSDT",
        open_mode=SingleOpenMode.REGULAR,
        selected_position_side=PositionSide.LONG,
        open_qty=Decimal("0.010"),
        leverage=10,
        round_count=1,
        round_interval_seconds=0,
    )

    payload = await service.start_run(request)
    task = service._active_task
    assert task is not None
    await asyncio.wait_for(task, timeout=2)
    detail = await service.get_history_detail(payload["run_id"])

    assert detail["created_at"] == payload["created_at"]


@pytest.mark.asyncio
async def test_passive_wait_updates_active_stage_and_heartbeat(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    stable_book = {
        "symbol": "BTCUSDT",
        "bids": [{"price": Decimal("10000"), "qty": Decimal("1")}],
        "asks": [{"price": Decimal("10001"), "qty": Decimal("1")}],
        "event_time": now,
    }
    gateway = SimulationGateway(order_books=[dict(stable_book) for _ in range(8)])
    service = make_short_order_wait_service(tmp_path, gateway)
    request = SimulationRunRequest(
        session_kind=SessionKind.SINGLE_OPEN,
        symbol="BTCUSDT",
        open_mode=SingleOpenMode.REGULAR,
        selected_position_side=PositionSide.LONG,
        open_qty=Decimal("0.010"),
        leverage=10,
        round_count=1,
        round_interval_seconds=0,
    )

    payload = await service.start_run(request)
    active = None
    for _ in range(100):
        candidate = await service.active_run()
        if candidate.get("stage") == "waiting_fill":
            active = candidate
            break
        await asyncio.sleep(0.01)

    try:
        assert active is not None
        assert active["run_id"] == payload["run_id"]
        assert active["heartbeat_at"] is not None
    finally:
        await service.abort()
        if service._active_task is not None:
            await asyncio.wait_for(service._active_task, timeout=2)


@pytest.mark.asyncio
async def test_start_run_rejects_second_background_simulation_with_lock_reason(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    request = SimulationRunRequest(
        session_kind=SessionKind.SINGLE_OPEN,
        symbol="BTCUSDT",
        open_mode=SingleOpenMode.REGULAR,
        selected_position_side=PositionSide.LONG,
        open_qty=Decimal("0.010"),
        leverage=10,
        round_count=2,
        round_interval_seconds=30,
    )

    first = await service.start_run(request)
    await wait_for_active_run(service, first["run_id"])

    try:
        with pytest.raises(SimulationError):
            await service.start_run(request)
    finally:
        await service.abort()
        if service._active_task is not None:
            await asyncio.wait_for(service._active_task, timeout=2)


@pytest.mark.asyncio
async def test_service_start_marks_stale_running_simulations_interrupted(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / "simulation.sqlite3")
    first = SimulationService(SimulationGateway(), repository)
    request = SimulationRunRequest(
        session_kind=SessionKind.SINGLE_OPEN,
        symbol="BTCUSDT",
        open_mode=SingleOpenMode.REGULAR,
        selected_position_side=PositionSide.LONG,
        open_qty=Decimal("0.010"),
        leverage=10,
        round_count=1,
        round_interval_seconds=0,
    )
    now = datetime.now(UTC)
    with first._repository._lock, first._repository._connection:
        first._insert_run_locked(
            run_id="stale-run",
            event_type="simulation_run",
            request=first._request_payload(request),
            result={"run_id": "stale-run", "status": "running"},
            status="running",
            stop_reason="running",
            created_at=now,
            stage="running",
        )

    restarted = SimulationService(SimulationGateway(), repository)
    updates = await restarted.run_updates("stale-run")

    assert updates["status"] == "interrupted"
    assert updates["stop_reason"] == "interrupted"
    assert updates["events"][-1]["message_code"] == "runtime.simulation_run_interrupted"


@pytest.mark.asyncio
async def test_default_account_can_be_reconfigured_and_reset_keeps_history(tmp_path: Path) -> None:
    service = make_service(tmp_path)

    account = await service.get_account()
    assert account["settings"]["initial_balance"] == "7000"

    await service.update_account_settings(initial_balance=Decimal("9000"), maker_fee_rate=Decimal("0"), taker_fee_rate=Decimal("0.0005"))
    await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )
    await service.reset_account()

    reset_account = await service.get_account()
    history = await service.list_history(page=1, page_size=10)

    assert reset_account["settings"]["initial_balance"] == "9000"
    assert reset_account["totals"]["wallet_balance"] == "9000"
    assert reset_account["positions"] == []
    assert history["total"] == 3
    assert history["items"][0]["event_type"] == "account_reset"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["settings", "reset", "clear_history"])
async def test_account_and_history_writes_are_blocked_while_simulation_runs(tmp_path: Path, operation: str) -> None:
    service = make_service(tmp_path)
    task = await start_long_running_simulation(service)

    try:
        with pytest.raises(SimulationError):
            if operation == "settings":
                await service.update_account_settings(initial_balance=Decimal("9000"), maker_fee_rate=None, taker_fee_rate=None)
            elif operation == "reset":
                await service.reset_account()
            else:
                await service.clear_history()
    finally:
        await service.abort()
        await asyncio.wait_for(task, timeout=2)


@pytest.mark.asyncio
async def test_initial_balance_setting_change_preserves_current_wallet_and_writes_ledger(tmp_path: Path) -> None:
    service = make_service(tmp_path)

    await service.update_account_settings(initial_balance=Decimal("9000"), maker_fee_rate=None, taker_fee_rate=None)
    account = await service.get_account()
    history = await service.list_history(page=1, page_size=10)
    detail = await service.get_history_detail(history["items"][0]["run_id"])

    assert account["settings"]["initial_balance"] == "9000"
    assert account["totals"]["wallet_balance"] == "7000"
    assert detail["event_type"] == "account_settings_update"
    assert detail["ledger"][0]["event_type"] == "initial_balance_setting_changed"
    assert detail["ledger"][0]["amount"] == "-2000.000000000"


@pytest.mark.asyncio
async def test_wallet_balance_sums_ledger_amounts_with_decimal_precision(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    now = datetime.now(UTC)
    with service._repository._lock, service._repository._connection:
        service._insert_ledger_locked(
            run_id="precision-large",
            event_type="precision_test",
            amount=Decimal("1000000000000000"),
            balance_after=Decimal("0"),
            payload={},
            created_at=now,
        )
        service._insert_ledger_locked(
            run_id="precision-small",
            event_type="precision_test",
            amount=Decimal("0.000000001"),
            balance_after=Decimal("0"),
            payload={},
            created_at=now,
        )

    account = await service.get_account()

    assert account["totals"]["wallet_balance"] == "1000000000007000.000000001"


@pytest.mark.asyncio
async def test_single_open_uses_orderbook_depth_fees_ledger_and_never_real_orders(tmp_path: Path) -> None:
    gateway = SimulationGateway()
    service = make_service(tmp_path, gateway)

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )
    account = await service.get_account()
    detail = await service.get_history_detail(result["run_id"])

    assert result["status"] == "completed"
    assert result["stop_reason"] == "filled"
    assert result["filled_qty"] == "0.010"
    assert result["avg_fill_price"] == "10060"
    assert result["fee"] == "0.050300000"
    assert detail["fills"][0]["depth_levels_consumed"] == 2
    assert detail["fills"][0]["liquidity"] == "taker"
    assert account["positions"][0]["position_side"] == "LONG"
    assert account["positions"][0]["qty"] == "0.010"
    assert account["totals"]["unrealized_pnl"] == "-0.600000000"
    assert account["totals"]["equity"] == "6999.349700000"
    assert account["totals"]["margin"] == "10.060000000"
    assert account["totals"]["available_balance"] == "6989.289700000"
    assert gateway.place_limit_order_calls == 0
    assert gateway.place_market_order_calls == 0
    assert gateway.account_overview_calls == 0


@pytest.mark.asyncio
async def test_single_open_passive_limit_order_polls_orderbook_and_uses_maker_fee(tmp_path: Path) -> None:
    fresh_time = datetime.now(UTC)
    gateway = SimulationGateway(
        order_books=[
            {
                "symbol": "BTCUSDT",
                "bids": [{"price": Decimal("10000"), "qty": Decimal("1")}],
                "asks": [{"price": Decimal("10001"), "qty": Decimal("1")}],
                "event_time": fresh_time,
            },
            {
                "symbol": "BTCUSDT",
                "bids": [{"price": Decimal("9999"), "qty": Decimal("1")}],
                "asks": [{"price": Decimal("10001"), "qty": Decimal("1")}],
                "event_time": fresh_time,
            },
        ]
    )
    service = make_service(tmp_path, gateway)
    await service.update_account_settings(initial_balance=None, maker_fee_rate=Decimal("0"), taker_fee_rate=Decimal("0.0005"))

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=1,
            round_interval_seconds=1,
        )
    )
    detail = await service.get_history_detail(result["run_id"])

    assert result["status"] == "completed"
    assert result["avg_fill_price"] == "10000"
    assert result["fee"] == "0.000000000"
    assert result["wait_seconds_consumed"] == "0.5"
    assert detail["fills"][0]["liquidity"] == "maker"


@pytest.mark.asyncio
async def test_single_open_zero_passive_fill_is_reported_as_unfilled_not_min_notional(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    gateway = SimulationGateway(
        order_books=[
            {
                "symbol": "BTCUSDT",
                "bids": [{"price": Decimal("10000"), "qty": Decimal("1")}],
                "asks": [{"price": Decimal("10001"), "qty": Decimal("1")}],
                "event_time": now,
            },
            {
                "symbol": "BTCUSDT",
                "bids": [{"price": Decimal("10000"), "qty": Decimal("1")}],
                "asks": [{"price": Decimal("10001"), "qty": Decimal("1")}],
                "event_time": now,
            },
        ]
    )
    service = make_fast_order_wait_service(tmp_path, gateway)

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=2,
            round_interval_seconds=0,
        )
    )

    assert result["status"] == "completed_with_skips"
    assert result["stop_reason"] == "limit_order_unfilled"
    assert result["rounds_completed"] == 2
    assert result["filled_qty"] == "0"


@pytest.mark.asyncio
async def test_paired_open_zero_passive_fill_keeps_extending_until_abort(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    stable_book = {
        "symbol": "BTCUSDT",
        "bids": [{"price": Decimal("10000"), "qty": Decimal("1")}],
        "asks": [{"price": Decimal("10001"), "qty": Decimal("1")}],
        "event_time": now,
    }
    gateway = SimulationGateway(order_books=[dict(stable_book) for _ in range(12)])
    service = make_fast_order_wait_service(tmp_path, gateway)
    await service.update_account_settings(initial_balance=Decimal("1000"), maker_fee_rate=Decimal("0"), taker_fee_rate=Decimal("0"))
    events: list[tuple[str, dict]] = []
    original_publish = service._publish

    async def capture(event_type: str, payload: dict) -> None:
        events.append((event_type, payload))
        if (
            event_type == "execution_stats"
            and payload.get("event_type") == "simulation_round_progress"
            and payload.get("extension_round_index") == 6
        ):
            await service.abort()
        await original_publish(event_type, payload)

    service._publish = capture

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.PAIRED_OPEN,
            symbol="BTCUSDT",
            open_amount=Decimal("100"),
            trend_bias="long",
            leverage=10,
            round_count=2,
            round_interval_seconds=0,
        )
    )

    assert result["status"] == "aborted"
    assert result["stop_reason"] == "user_aborted"
    assert result["rounds_completed"] == 8
    assert result["rounds_total"] == 8
    assert result["filled_qty"] == "0"
    round_stats = [
        payload
        for event_type, payload in events
        if event_type == "execution_stats" and payload.get("event_type") == "simulation_round_progress"
    ]
    round_logs = [
        payload
        for event_type, payload in events
        if event_type == "execution_log" and payload.get("message_code") in {"log.simulation.round_unfilled", "log.simulation.extension_round_unfilled"}
    ]
    assert [payload["rounds_completed"] for payload in round_stats] == [1, 2, 3, 4, 5, 6, 7, 8]
    assert [payload["rounds_total"] for payload in round_stats] == [2, 2, 3, 4, 5, 6, 7, 8]
    assert [payload.get("extension_rounds_unlimited") for payload in round_stats[2:]] == [True, True, True, True, True, True]
    assert [payload.get("max_extension_rounds") for payload in round_stats[2:]] == [0, 0, 0, 0, 0, 0]
    assert [payload["message_code"] for payload in round_logs] == [
        "log.simulation.round_unfilled",
        "log.simulation.round_unfilled",
        "log.simulation.extension_round_unfilled",
        "log.simulation.extension_round_unfilled",
        "log.simulation.extension_round_unfilled",
        "log.simulation.extension_round_unfilled",
        "log.simulation.extension_round_unfilled",
        "log.simulation.extension_round_unfilled",
    ]
    account = await service.get_account()
    assert account["positions"] == []


@pytest.mark.asyncio
async def test_background_simulation_is_not_cut_off_by_hard_duration_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(simulation_module, "MAX_SIMULATION_DURATION_SECONDS", 0.001)
    service = make_slow_completing_service(tmp_path)

    await service.start_run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )
    task = service._active_task
    assert task is not None

    result = await asyncio.wait_for(task, timeout=1)

    assert result["status"] == "completed"
    assert result["stop_reason"] == "filled"


@pytest.mark.asyncio
async def test_round_progress_execution_log_carries_run_id_for_polling_dedupe(tmp_path: Path) -> None:
    published: list[tuple[str, dict]] = []

    async def capture(event_type: str, payload: dict) -> None:
        published.append((event_type, payload))

    repository = SqliteRepository(tmp_path / "simulation.sqlite3")
    gateway = SimulationGateway()
    service = SimulationService(gateway, repository, publisher=capture)
    request = SimulationRunRequest(
        session_kind=SessionKind.SINGLE_OPEN,
        symbol="BTCUSDT",
        open_mode=SingleOpenMode.REGULAR,
        selected_position_side=PositionSide.LONG,
        open_qty=Decimal("0.010"),
        leverage=10,
        round_count=1,
        round_interval_seconds=0,
    )
    run_id = "run-progress-dedupe"
    now = datetime.now(UTC)
    with repository._lock, repository._connection:
        service._insert_run_locked(
            run_id=run_id,
            event_type="simulation_run",
            request=service._request_payload(request),
            result={},
            status="running",
            stop_reason="running",
            created_at=now,
            stage="running",
            heartbeat_at=now,
            last_event_at=now,
            lock_reason="simulation_running",
        )
    result = service._result_payload(
        run_id,
        request,
        "completed",
        "filled",
        None,
        filled_qty=Decimal("0.010"),
        avg_fill_price=Decimal("10000"),
        fee=Decimal("0"),
        residual_qty=Decimal("0"),
        realized_pnl=Decimal("0"),
        rounds_completed=1,
        rounds_total=1,
    )

    await service._publish_round_progress(
        run_id=run_id,
        request=request,
        symbol="BTCUSDT",
        planned_qtys=[Decimal("0.010")],
        results=[result],
        rules=await gateway.get_symbol_rules("BTCUSDT"),
        round_index=1,
    )

    execution_logs = [payload for event_type, payload in published if event_type == "execution_log"]
    assert execution_logs
    assert execution_logs[0]["run_id"] == run_id
    assert execution_logs[0]["event_id"] > 0


@pytest.mark.asyncio
async def test_account_marks_open_positions_to_current_quote(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    gateway = SimulationGateway(
        order_books=[
            {
                "symbol": "BTCUSDT",
                "bids": [{"price": Decimal("10000"), "qty": Decimal("1")}],
                "asks": [{"price": Decimal("10000"), "qty": Decimal("1")}],
                "event_time": now,
            }
        ]
    )
    service = make_service(tmp_path, gateway)
    await service.update_account_settings(initial_balance=None, maker_fee_rate=Decimal("0"), taker_fee_rate=Decimal("0"))

    await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )
    gateway.quote_bid = Decimal("10200")
    gateway.quote_ask = Decimal("10201")

    account = await service.get_account()

    assert account["positions"][0]["entry_price"] == "10000"
    assert account["positions"][0]["mark_price"] == "10200"
    assert account["positions"][0]["unrealized_pnl"] == "2"
    assert account["totals"]["unrealized_pnl"] == "2"
    assert account["totals"]["equity"] == "7002"
    assert account["totals"]["available_balance"] == "6992"


@pytest.mark.asyncio
async def test_single_open_splits_target_quantity_across_rounds(tmp_path: Path) -> None:
    service = make_service(tmp_path)

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=3,
            round_interval_seconds=0,
        )
    )
    detail = await service.get_history_detail(result["run_id"])
    account = await service.get_account()

    assert result["status"] == "completed"
    assert [fill["round_index"] for fill in detail["fills"]] == [1, 2, 3]
    assert [fill["qty"] for fill in detail["fills"]] == ["0.003", "0.003", "0.004"]
    assert account["positions"][0]["qty"] == "0.010"


@pytest.mark.asyncio
async def test_single_open_publishes_round_progress_logs_and_stats(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    events: list[tuple[str, dict]] = []
    original_publish = service._publish

    async def capture_publish(event: str, payload: dict) -> None:
        events.append((event, payload))
        await original_publish(event, payload)

    service._publish = capture_publish

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=3,
            round_interval_seconds=0,
        )
    )

    progress_logs = [
        payload
        for event, payload in events
        if event == "execution_log" and payload.get("message_code") == "log.simulation.round_completed"
    ]
    progress_stats = [
        payload
        for event, payload in events
        if event == "execution_stats" and payload.get("message_code") == "log.simulation.round_completed"
    ]

    assert result["status"] == "completed"
    assert [payload["message_params"]["round_index"] for payload in progress_logs] == [1, 2, 3]
    assert [payload["rounds_completed"] for payload in progress_stats] == [1, 2, 3]
    assert progress_logs[-1]["message"] == "第 3/3 轮完成：BTCUSDT 成交 0.004，均价 10000，手续费 0.020000000，残量 0.000。"


@pytest.mark.asyncio
async def test_single_close_uses_only_simulated_position_and_records_realized_pnl(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_CLOSE,
            symbol="BTCUSDT",
            close_mode=SingleCloseMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            close_qty=Decimal("0.004"),
            round_count=1,
            round_interval_seconds=0,
        )
    )
    account = await service.get_account()

    assert result["status"] == "completed"
    assert result["realized_pnl"] == "3.720000000"
    assert account["positions"][0]["qty"] == "0.006"
    assert account["positions"][0]["position_side"] == "LONG"


@pytest.mark.asyncio
async def test_single_close_splits_target_quantity_across_rounds(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_CLOSE,
            symbol="BTCUSDT",
            close_mode=SingleCloseMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            close_qty=Decimal("0.010"),
            round_count=3,
            round_interval_seconds=0,
        )
    )
    detail = await service.get_history_detail(result["run_id"])
    account = await service.get_account()

    assert result["status"] == "completed"
    assert [fill["round_index"] for fill in detail["fills"]] == [1, 2, 3]
    assert [fill["qty"] for fill in detail["fills"]] == ["0.003", "0.003", "0.004"]
    assert account["positions"] == []


@pytest.mark.asyncio
async def test_single_close_blocks_when_simulated_position_is_insufficient(tmp_path: Path) -> None:
    gateway = SimulationGateway()
    service = make_service(tmp_path, gateway)

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_CLOSE,
            symbol="BTCUSDT",
            close_mode=SingleCloseMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            close_qty=Decimal("0.010"),
            round_count=1,
            round_interval_seconds=0,
        )
    )

    assert result["status"] == "blocked"
    assert result["stop_reason"] == "insufficient_sim_position"
    assert gateway.account_overview_calls == 0


@pytest.mark.asyncio
async def test_single_close_requires_close_qty_without_mutation(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    now = datetime.now(UTC).isoformat()
    with service._repository._lock, service._repository._connection:
        service._repository._connection.execute(
            """
            INSERT INTO simulation_positions (account_id, symbol, position_side, qty, entry_price, margin, leverage, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("simulation", "BTCUSDT", "LONG", "0.005", "10000", "5.000000000", 10, now),
        )

    before = await service.get_account()
    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_CLOSE,
            symbol="BTCUSDT",
            close_mode=SingleCloseMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            round_count=10,
            round_interval_seconds=0,
        )
    )
    after = await service.get_account()
    detail = await service.get_history_detail(result["run_id"])

    assert result["status"] == "blocked"
    assert result["stop_reason"] == "min_notional_blocked"
    assert result["rounds_completed"] == 0
    assert after["positions"] == before["positions"]
    assert after["totals"] == before["totals"]
    assert detail["fills"] == []
    assert detail["ledger"] == []


@pytest.mark.asyncio
async def test_stale_orderbook_refreshes_once_then_stops_with_structured_reason(tmp_path: Path) -> None:
    stale_time = datetime.now(UTC) - timedelta(seconds=30)
    gateway = SimulationGateway(
        order_books=[
            {"symbol": "BTCUSDT", "bids": [], "asks": [], "event_time": stale_time},
            {"symbol": "BTCUSDT", "bids": [], "asks": [], "event_time": stale_time},
        ]
    )
    service = make_service(tmp_path, gateway)

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )

    assert result["status"] == "blocked"
    assert result["stop_reason"] == "stale_orderbook"


@pytest.mark.asyncio
async def test_stale_orderbook_is_refreshed_once_before_matching(tmp_path: Path) -> None:
    stale_time = datetime.now(UTC) - timedelta(seconds=30)
    fresh_time = datetime.now(UTC)
    gateway = SimulationGateway(
        order_books=[
            {"symbol": "BTCUSDT", "bids": [], "asks": [], "event_time": stale_time},
        ],
        refresh_order_books=[
            {
                "symbol": "BTCUSDT",
                "bids": [{"price": Decimal("10990"), "qty": Decimal("1")}],
                "asks": [{"price": Decimal("10000"), "qty": Decimal("1")}],
                "event_time": fresh_time,
            },
        ],
    )
    service = make_service(tmp_path, gateway)

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )

    assert result["status"] == "completed"
    assert gateway.refresh_order_book_calls == 1


@pytest.mark.asyncio
async def test_orderbook_refresh_failure_stops_as_stale_orderbook(tmp_path: Path) -> None:
    stale_time = datetime.now(UTC) - timedelta(seconds=30)
    gateway = SimulationGateway(
        order_books=[
            {"symbol": "BTCUSDT", "bids": [], "asks": [], "event_time": stale_time},
        ],
        refresh_order_book_error=RuntimeError("depth refresh failed"),
    )
    service = make_service(tmp_path, gateway)

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )

    assert result["status"] == "blocked"
    assert result["stop_reason"] == "stale_orderbook"


@pytest.mark.asyncio
async def test_paired_open_rolls_back_first_leg_when_second_leg_blocks(tmp_path: Path) -> None:
    fresh_time = datetime.now(UTC)
    stale_time = fresh_time - timedelta(seconds=30)
    gateway = SimulationGateway(
        order_books=[
            {
                "symbol": "BTCUSDT",
                "bids": [{"price": Decimal("9990"), "qty": Decimal("1")}],
                "asks": [{"price": Decimal("9990"), "qty": Decimal("1")}],
                "event_time": fresh_time,
            },
            {"symbol": "BTCUSDT", "bids": [], "asks": [], "event_time": stale_time},
            {"symbol": "BTCUSDT", "bids": [], "asks": [], "event_time": stale_time},
        ]
    )
    service = make_service(tmp_path, gateway)

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.PAIRED_OPEN,
            symbol="BTCUSDT",
            trend_bias="long",
            open_amount=Decimal("10"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )
    account = await service.get_account()
    detail = await service.get_history_detail(result["run_id"])

    assert result["status"] == "blocked"
    assert result["stop_reason"] == "stale_orderbook"
    assert account["positions"] == []
    assert detail["fills"] == []
    assert detail["ledger"] == []


@pytest.mark.asyncio
async def test_paired_open_uses_extension_round_to_complete_balanced_partial_fill(tmp_path: Path) -> None:
    fresh_time = datetime.now(UTC)
    gateway = SimulationGateway(
        order_books=[
            {
                "symbol": "BTCUSDT",
                "bids": [{"price": Decimal("10000"), "qty": Decimal("1")}],
                "asks": [{"price": Decimal("10000"), "qty": Decimal("0.004")}],
                "event_time": fresh_time,
            },
            {
                "symbol": "BTCUSDT",
                "bids": [{"price": Decimal("10000"), "qty": Decimal("0.006")}],
                "asks": [{"price": Decimal("10000"), "qty": Decimal("1")}],
                "event_time": fresh_time,
            },
        ]
    )
    service = make_service(tmp_path, gateway)

    events: list[tuple[str, dict]] = []
    original_publish = service._publish

    async def capture(event_type: str, payload: dict) -> None:
        events.append((event_type, payload))
        await original_publish(event_type, payload)

    service._publish = capture

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.PAIRED_OPEN,
            symbol="BTCUSDT",
            trend_bias="long",
            open_amount=Decimal("10"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )
    account = await service.get_account()
    detail = await service.get_history_detail(result["run_id"])
    round_stats = [
        payload
        for event_type, payload in events
        if event_type == "execution_stats" and payload.get("event_type") == "simulation_round_progress"
    ]
    account_events = [payload for event_type, payload in events if event_type == "simulation_account"]
    round_logs = [payload for event_type, payload in events if event_type == "execution_log"]

    assert result["status"] == "completed"
    assert result["filled_qty"] == "0.010"
    assert result["rounds_total"] == 2
    assert result["rounds_completed"] == 2
    assert sorted((position["position_side"], position["qty"]) for position in account["positions"]) == [
        ("LONG", "0.005"),
        ("SHORT", "0.005"),
    ]
    assert [fill["qty"] for fill in detail["fills"]] == ["0.004", "0.004", "0.001", "0.001"]
    assert [payload["rounds_completed"] for payload in round_stats] == [1, 2]
    assert [payload["rounds_total"] for payload in round_stats] == [1, 2]
    assert round_stats[-1]["extension_rounds_unlimited"] is True
    assert round_stats[-1]["max_extension_rounds"] == 0
    assert len(account_events) >= 2
    assert account_events[0]["positions"]
    assert sorted((position["position_side"], position["qty"]) for position in account_events[-1]["positions"]) == [
        ("LONG", "0.005"),
        ("SHORT", "0.005"),
    ]
    assert [payload["message_params"]["round_index"] for payload in round_logs] == [1, 2]
    assert [payload["message_code"] for payload in round_logs] == [
        "log.simulation.round_completed",
        "log.simulation.extension_round_completed",
    ]
    assert round_logs[-1]["message_params"]["extension_round_index"] == 1
    assert round_logs[-1]["message_params"]["max_extension_rounds"] == 0


@pytest.mark.asyncio
async def test_paired_open_uses_open_amount_as_total_paired_margin_budget(tmp_path: Path) -> None:
    fresh_time = datetime.now(UTC)
    deep_book = {
        "symbol": "BTCUSDT",
        "bids": [{"price": Decimal("10000"), "qty": Decimal("100")}],
        "asks": [{"price": Decimal("10000"), "qty": Decimal("100")}],
        "event_time": fresh_time,
    }
    gateway = SimulationGateway(order_books=[deep_book, deep_book])
    service = make_service(tmp_path, gateway)
    await service.update_account_settings(initial_balance=Decimal("20000"))
    await service.reset_account()

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.PAIRED_OPEN,
            symbol="BTCUSDT",
            trend_bias="long",
            open_amount=Decimal("6500"),
            leverage=75,
            round_count=1,
            round_interval_seconds=0,
        )
    )
    account = await service.get_account()

    assert result["status"] == "completed"
    assert Decimal(account["totals"]["margin"]) == Decimal("6500.000000000")
    assert sorted((position["position_side"], position["qty"]) for position in account["positions"]) == [
        ("LONG", "24.375"),
        ("SHORT", "24.375"),
    ]
    assert sorted((position["position_side"], position["margin"]) for position in account["positions"]) == [
        ("LONG", "3250.000000000"),
        ("SHORT", "3250.000000000"),
    ]
    for position in account["positions"]:
        notional = Decimal(position["qty"]) * Decimal(position["entry_price"])
        assert Decimal(position["margin"]) == (notional / Decimal("75")).quantize(Decimal("0.000000001"))


@pytest.mark.asyncio
async def test_get_account_repairs_legacy_half_margin_positions(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    now = datetime.now(UTC).isoformat()
    with service._repository._lock, service._repository._connection:
        service._repository._connection.execute(
            """
            INSERT INTO simulation_positions (account_id, symbol, position_side, qty, entry_price, margin, leverage, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("simulation", "ETHUSDC", "LONG", "31.402", "2301.78", "481.869800000", 75, now),
        )

    account = await service.get_account()

    position = account["positions"][0]
    expected_margin = (Decimal("31.402") * Decimal("2301.78") / Decimal("75")).quantize(Decimal("0.000000001"))
    assert Decimal(position["margin"]) == expected_margin
    assert Decimal(account["totals"]["margin"]) == expected_margin


@pytest.mark.asyncio
async def test_paired_close_rolls_back_first_leg_when_second_leg_blocks(tmp_path: Path) -> None:
    fresh_time = datetime.now(UTC)
    stale_time = fresh_time - timedelta(seconds=30)
    service = make_service(tmp_path)
    await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.PAIRED_OPEN,
            symbol="BTCUSDT",
            trend_bias="long",
            open_amount=Decimal("10"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )
    before = await service.get_account()
    service._gateway = SimulationGateway(
        order_books=[
            {
                "symbol": "BTCUSDT",
                "bids": [{"price": Decimal("10990"), "qty": Decimal("1")}],
                "asks": [{"price": Decimal("10000"), "qty": Decimal("1")}],
                "event_time": fresh_time,
            },
            {"symbol": "BTCUSDT", "bids": [], "asks": [], "event_time": stale_time},
            {"symbol": "BTCUSDT", "bids": [], "asks": [], "event_time": stale_time},
        ]
    )

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol="BTCUSDT",
            trend_bias="long",
            close_qty=Decimal("0.004"),
            round_count=1,
            round_interval_seconds=0,
        )
    )
    after = await service.get_account()
    detail = await service.get_history_detail(result["run_id"])

    assert result["status"] == "blocked"
    assert result["stop_reason"] == "stale_orderbook"
    assert after["positions"] == before["positions"]
    assert after["totals"] == before["totals"]
    assert detail["fills"] == []
    assert detail["ledger"] == []


@pytest.mark.asyncio
async def test_paired_close_long_trend_closes_short_before_long(tmp_path: Path) -> None:
    fresh_time = datetime.now(UTC)
    service = make_fast_order_wait_service(
        tmp_path,
        SimulationGateway(
            order_books=[
                {
                    "symbol": "BTCUSDT",
                    "bids": [{"price": Decimal("10001"), "qty": Decimal("0.004")}],
                    "asks": [{"price": Decimal("10000"), "qty": Decimal("0.004")}],
                    "event_time": fresh_time,
                },
                {
                    "symbol": "BTCUSDT",
                    "bids": [{"price": Decimal("10001"), "qty": Decimal("0.004")}],
                    "asks": [{"price": Decimal("10000"), "qty": Decimal("0.004")}],
                    "event_time": fresh_time,
                },
            ]
        ),
    )
    now = datetime.now(UTC).isoformat()
    with service._repository._lock, service._repository._connection:
        for side in ("LONG", "SHORT"):
            service._repository._connection.execute(
                """
                INSERT INTO simulation_positions (account_id, symbol, position_side, qty, entry_price, margin, leverage, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("simulation", "BTCUSDT", side, "0.010", "10000", "10.000000000", 10, now),
            )

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol="BTCUSDT",
            trend_bias="long",
            close_qty=Decimal("0.004"),
            round_count=1,
            round_interval_seconds=0,
        )
    )
    detail = await service.get_history_detail(result["run_id"])

    assert result["status"] == "completed"
    assert [(fill["position_side"], fill["side"]) for fill in detail["fills"]] == [
        ("SHORT", "BUY"),
        ("LONG", "SELL"),
    ]


@pytest.mark.asyncio
async def test_paired_close_blocks_total_qty_above_closeable_without_mutation(tmp_path: Path) -> None:
    fresh_time = datetime.now(UTC)
    deep_book = {
        "symbol": "BTCUSDT",
        "bids": [{"price": Decimal("10000"), "qty": Decimal("1")}],
        "asks": [{"price": Decimal("10000"), "qty": Decimal("1")}],
        "event_time": fresh_time,
    }
    service = make_service(
        tmp_path,
        SimulationGateway(order_books=[dict(deep_book) for _ in range(20)]),
    )
    now = datetime.now(UTC).isoformat()
    with service._repository._lock, service._repository._connection:
        for side in ("LONG", "SHORT"):
            service._repository._connection.execute(
                """
                INSERT INTO simulation_positions (account_id, symbol, position_side, qty, entry_price, margin, leverage, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("simulation", "BTCUSDT", side, "0.005", "10000", "5.000000000", 10, now),
            )

    before = await service.get_account()
    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol="BTCUSDT",
            trend_bias="long",
            close_qty=Decimal("0.010"),
            round_count=10,
            round_interval_seconds=0,
        )
    )
    after = await service.get_account()
    detail = await service.get_history_detail(result["run_id"])

    assert result["status"] == "blocked"
    assert result["stop_reason"] == "insufficient_sim_position"
    assert result["rounds_completed"] == 0
    assert after["totals"] == before["totals"]
    assert after["positions"] == before["positions"]
    assert detail["fills"] == []
    assert detail["ledger"] == []


@pytest.mark.asyncio
async def test_paired_close_requires_close_qty_without_mutation(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    now = datetime.now(UTC).isoformat()
    with service._repository._lock, service._repository._connection:
        for side in ("LONG", "SHORT"):
            service._repository._connection.execute(
                """
                INSERT INTO simulation_positions (account_id, symbol, position_side, qty, entry_price, margin, leverage, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("simulation", "BTCUSDT", side, "0.005", "10000", "5.000000000", 10, now),
            )

    before = await service.get_account()
    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol="BTCUSDT",
            trend_bias="long",
            round_count=10,
            round_interval_seconds=0,
        )
    )
    after = await service.get_account()
    detail = await service.get_history_detail(result["run_id"])

    assert result["status"] == "blocked"
    assert result["stop_reason"] == "min_notional_blocked"
    assert result["rounds_completed"] == 0
    assert after["positions"] == before["positions"]
    assert after["totals"] == before["totals"]
    assert detail["fills"] == []
    assert detail["ledger"] == []


@pytest.mark.asyncio
async def test_paired_close_uses_extension_round_to_complete_balanced_partial_fill(tmp_path: Path) -> None:
    fresh_time = datetime.now(UTC)
    partial_book = {
        "symbol": "BTCUSDT",
        "bids": [{"price": Decimal("10000"), "qty": Decimal("0.004")}],
        "asks": [{"price": Decimal("10000"), "qty": Decimal("0.004")}],
        "event_time": fresh_time,
    }
    extension_book = {
        "symbol": "BTCUSDT",
        "bids": [{"price": Decimal("10000"), "qty": Decimal("0.001")}],
        "asks": [{"price": Decimal("10000"), "qty": Decimal("0.001")}],
        "event_time": fresh_time,
    }
    service = make_service(
        tmp_path,
        SimulationGateway(order_books=[partial_book, partial_book, extension_book, extension_book]),
    )
    now = datetime.now(UTC).isoformat()
    with service._repository._lock, service._repository._connection:
        for side in ("LONG", "SHORT"):
            service._repository._connection.execute(
                """
                INSERT INTO simulation_positions (account_id, symbol, position_side, qty, entry_price, margin, leverage, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("simulation", "BTCUSDT", side, "0.005", "10000", "5.000000000", 10, now),
            )

    events: list[tuple[str, dict]] = []
    original_publish = service._publish

    async def capture(event_type: str, payload: dict) -> None:
        events.append((event_type, payload))
        await original_publish(event_type, payload)

    service._publish = capture

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol="BTCUSDT",
            trend_bias="long",
            close_qty=Decimal("0.005"),
            round_count=1,
            round_interval_seconds=0,
        )
    )
    account = await service.get_account()
    detail = await service.get_history_detail(result["run_id"])
    round_stats = [
        payload
        for event_type, payload in events
        if event_type == "execution_stats" and payload.get("event_type") == "simulation_round_progress"
    ]
    round_logs = [payload for event_type, payload in events if event_type == "execution_log"]

    assert result["status"] == "completed"
    assert result["filled_qty"] == "0.010"
    assert result["rounds_total"] == 2
    assert result["rounds_completed"] == 2
    assert account["positions"] == []
    assert [(fill["position_side"], fill["qty"]) for fill in detail["fills"]] == [
        ("SHORT", "0.004"),
        ("LONG", "0.004"),
        ("SHORT", "0.001"),
        ("LONG", "0.001"),
    ]
    assert [payload["rounds_completed"] for payload in round_stats] == [1, 2]
    assert [payload["rounds_total"] for payload in round_stats] == [1, 2]
    assert round_stats[-1]["extension_rounds_unlimited"] is True
    assert round_stats[-1]["max_extension_rounds"] == 0
    assert [payload["message_code"] for payload in round_logs] == [
        "log.simulation.round_completed",
        "log.simulation.extension_round_completed",
    ]
    assert round_logs[-1]["message_params"]["extension_round_index"] == 1


@pytest.mark.asyncio
async def test_paired_close_keeps_balanced_partial_fill_when_both_legs_partially_match(tmp_path: Path) -> None:
    fresh_time = datetime.now(UTC)
    stale_time = fresh_time - timedelta(seconds=30)
    service = make_service(
        tmp_path,
        SimulationGateway(
            order_books=[
                {
                    "symbol": "BTCUSDT",
                    "bids": [{"price": Decimal("11000"), "qty": Decimal("1")}],
                    "asks": [{"price": Decimal("11000"), "qty": Decimal("0.004")}],
                    "event_time": fresh_time,
                },
                {
                    "symbol": "BTCUSDT",
                    "bids": [{"price": Decimal("10990"), "qty": Decimal("1")}],
                    "asks": [{"price": Decimal("10990"), "qty": Decimal("1")}],
                    "event_time": fresh_time,
                },
                {"symbol": "BTCUSDT", "bids": [], "asks": [], "event_time": stale_time},
            ],
            refresh_order_book_error=RuntimeError("stale depth"),
        ),
    )
    now = datetime.now(UTC).isoformat()
    with service._repository._lock, service._repository._connection:
        for side in ("LONG", "SHORT"):
            service._repository._connection.execute(
                """
                INSERT INTO simulation_positions (account_id, symbol, position_side, qty, entry_price, margin, leverage, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("simulation", "BTCUSDT", side, "0.009", "10000", "9.000000000", 10, now),
            )

    result = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.PAIRED_CLOSE,
            symbol="BTCUSDT",
            trend_bias="long",
            close_qty=Decimal("0.009"),
            round_count=1,
            round_interval_seconds=0,
        )
    )
    account = await service.get_account()
    detail = await service.get_history_detail(result["run_id"])

    assert result["status"] == "completed_with_skips"
    assert result["filled_qty"] == "0.008"
    assert result["avg_fill_price"] == "10995"
    assert sorted((position["position_side"], position["qty"]) for position in account["positions"]) == [
        ("LONG", "0.005"),
        ("SHORT", "0.005"),
    ]
    assert [fill["qty"] for fill in detail["fills"]] == ["0.004", "0.004"]


@pytest.mark.asyncio
async def test_history_rerun_uses_current_market_and_creates_new_run(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    original = await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )

    rerun = await service.rerun(original["run_id"])
    history = await service.list_history(page=1, page_size=10)

    assert rerun["status"] == "completed"
    assert rerun["run_id"] != original["run_id"]
    assert rerun["rerun_source_run_id"] == original["run_id"]
    assert history["total"] == 2
    assert history["items"][0]["run_id"] == rerun["run_id"]
    assert history["items"][1]["run_id"] == original["run_id"]


@pytest.mark.asyncio
async def test_export_history_csv_includes_more_than_first_page(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    for _ in range(201):
        await service.run(
            SimulationRunRequest(
                session_kind=SessionKind.SINGLE_OPEN,
                symbol="BTCUSDT",
                open_mode=SingleOpenMode.REGULAR,
                selected_position_side=PositionSide.LONG,
                open_qty=Decimal("0.001"),
                leverage=10,
                round_count=1,
                round_interval_seconds=0,
            )
        )

    exported = await service.export_history_csv()
    rows = list(csv.DictReader(exported.splitlines()))

    assert len(rows) == 201


@pytest.mark.asyncio
async def test_clear_history_does_not_change_current_account_state(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    await service.run(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )
    before = await service.get_account()

    await service.clear_history()
    after = await service.get_account()
    history = await service.list_history(page=1, page_size=10)

    assert history["total"] == 0
    assert after["totals"] == before["totals"]
    assert after["positions"] == before["positions"]
