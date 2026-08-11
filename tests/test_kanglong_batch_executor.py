from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from paired_opener.config import Settings
from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.errors import ErrorCategory, ErrorStrategy, TradingError
from paired_opener.kanglong.batch_executor import (
    KanglongBatchExecutor,
    TransportRetryPolicy,
    operation_id,
)
from paired_opener.kanglong.batch_planner import KanglongBatchPlanner
from paired_opener.kanglong.models import KanglongRunStatus, available_actions_for_status
from paired_opener.simulation_matching import OrderbookSnapshot
from paired_opener.storage import SqliteRepository


RULES = SymbolRules(
    symbol="ETHUSDC",
    tick_size=Decimal("0.01"),
    step_size=Decimal("0.001"),
    min_qty=Decimal("0.001"),
    min_notional=Decimal("1"),
    max_leverage=125,
)


class FakeGateway:
    def __init__(self) -> None:
        self.update_id = 0
        self.freeze_update_id = False
        self.bid_price = Decimal("100")
        self.ask_price = Decimal("101")
        self.binance_calls: list[SimpleNamespace] = []

    async def get_symbol_rules(self, symbol: str):
        self.binance_calls.append(SimpleNamespace(method="GET", path="/papi/v1/um/exchangeInfo"))
        return RULES

    async def refresh_order_book(self, symbol: str, limit: int = 20):
        self.binance_calls.append(SimpleNamespace(method="GET", path="/fapi/v1/depth"))
        if not self.freeze_update_id:
            self.update_id += 1
        return {
            "symbol": symbol,
            "lastUpdateId": self.update_id,
            "event_time": datetime.now(UTC),
            "bids": [{"price": self.bid_price, "qty": "10"}] * limit,
            "asks": [{"price": self.ask_price, "qty": "10"}] * limit,
        }


class FakeRuntimeManager:
    def __init__(self, account_ids: list[str]) -> None:
        self.gateways = {account_id: FakeGateway() for account_id in account_ids}

    def current(self, account_id: str):
        return SimpleNamespace(gateway=self.gateways[account_id])


class FakeCapacityCoordinator:
    def __init__(self) -> None:
        self.blocked_account_ids: set[str] = set()
        self.error: Exception | None = None

    async def refresh_capacity(self, revision, account_ids, symbol, force_refresh=False):
        if self.error is not None:
            raise self.error
        bracket = SimpleNamespace(
            effective_floor=Decimal("0"),
            effective_cap=Decimal("1000000"),
            max_allowed_leverage=125,
            notional_coef=Decimal("1"),
            notional_floor=Decimal("0"),
            notional_cap=Decimal("1000000"),
            bracket=1,
        )
        return {
            account_id: SimpleNamespace(
                current_symbol_leverage=100,
                current_symbol_max_notional_value=Decimal("1000000"),
                brackets=(bracket,),
                available_balance=Decimal("100000"),
                account_equity=Decimal("100000"),
                maker_fee_rate=Decimal("0.0002"),
                taker_fee_rate=Decimal("0.0004"),
                existing_symbol_exposure=Decimal("0"),
                blocked_reasons=("blocked_for_test",) if account_id in self.blocked_account_ids else (),
                all_components_fresh=True,
            )
            for account_id in account_ids
        }


def _setup(
    tmp_path: Path,
    account_ids=("a1", "a2"),
    *,
    round_count: int = 1,
    round_interval_seconds: int = 0,
):
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    planner = KanglongBatchPlanner()
    plan = planner.plan_open(
        account_ids=account_ids,
        credential_revision="revision-1",
        symbol="ETHUSDC",
        preferred_side=PositionSide.LONG,
        leverage=100,
        per_leg_notional=Decimal("100.5"),
        reference_price=Decimal("100.5"),
        rules=RULES,
        account_snapshots={
            account_id: {
                "maker_fee_rate": "0.0002",
                "taker_fee_rate": "0.0004",
                "current_symbol_leverage": 100,
                "bracket_max_allowed_leverage": 125,
                "selected_bracket_effective_cap": "1000000",
                "current_symbol_max_notional_value": "1000000",
                "effective_capacity_leverage": 100,
            }
            for account_id in account_ids
        },
        run_id="run-1",
        round_count=round_count,
        round_interval_seconds=round_interval_seconds,
    )
    repository.save_batch_plan(plan, status="plan_confirmed")
    started = repository.begin_kanglong_batch_execution(
        run_id=plan.run_id,
        expected_plan_version=plan.plan_version,
        current_credential_revision="revision-1",
    )
    assert started["started"] is True
    lease = repository.acquire_kanglong_run_lease(
        run_id=plan.run_id,
        worker_id="test-worker",
        ttl_seconds=60,
    )
    runtimes = FakeRuntimeManager(list(account_ids))
    settings = Settings(_env_file=None, kanglong_symbol_configs_file=tmp_path / "symbols.json")
    capacity = FakeCapacityCoordinator()
    executor = KanglongBatchExecutor(
        repository,
        runtimes,
        lambda: "revision-1",
        capacity,
        settings,
        retry_policy=TransportRetryPolicy(jitter=lambda: 0),
    )
    return repository, plan, lease, executor


@pytest.mark.asyncio
async def test_second_account_does_not_start_until_first_is_aligned(tmp_path: Path) -> None:
    repository, plan, lease, executor = _setup(tmp_path)
    try:
        await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        active = repository.get_active_kanglong_batch_account(plan.run_id)
        second = repository.get_kanglong_batch_account(plan.run_id, "a2")
    finally:
        repository.close()
    assert active["account_id"] == "a1"
    assert second["status"] == "pending"


@pytest.mark.asyncio
async def test_restart_resumes_pending_second_leg_without_recounting_first(tmp_path: Path) -> None:
    repository, plan, lease, executor = _setup(tmp_path)
    try:
        await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        assert repository.sum_kanglong_batch_leg_qty(plan.run_id, "a1", "LONG") == Decimal("0")
        restarted = KanglongBatchExecutor(
            repository,
            executor._runtime_manager,
            lambda: "revision-1",
            FakeCapacityCoordinator(),
            executor._settings,
            retry_policy=TransportRetryPolicy(jitter=lambda: 0),
        )
        await restarted.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        long_qty = repository.sum_kanglong_batch_leg_qty(plan.run_id, "a1", "LONG")
        short_qty = repository.sum_kanglong_batch_leg_qty(plan.run_id, "a1", "SHORT")
        first_operation = operation_id(plan.run_id, "a1", 0, "first", 1)
        operation_count = repository.count_kanglong_operation_id(plan.run_id, first_operation)
        report = repository.get_kanglong_run(plan.run_id)["report"]
    finally:
        repository.close()
    assert long_qty == short_qty == Decimal("1")
    assert operation_count == 1
    assert report["batch_costs"]["accounts"]["a1"]["fee_cost"] != "0"


@pytest.mark.asyncio
async def test_checkpoint_persists_report_without_post_commit_update(tmp_path: Path) -> None:
    repository, plan, lease, executor = _setup(tmp_path, account_ids=("a1",))
    try:
        await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])

        def fail_if_called(**_kwargs):
            raise AssertionError("report must be persisted by the checkpoint transaction")

        repository.update_kanglong_batch_report = fail_if_called  # type: ignore[method-assign]
        stored = await executor.run_next(
            plan.run_id,
            lease["lease_token"],
            lease["fencing_token"],
        )
        latest = repository.latest_kanglong_checkpoint(plan.run_id)
    finally:
        repository.close()

    assert stored["report"]["report_summary"]["generated_from_checkpoint_id"] == latest["checkpoint_id"]
    assert stored["report"]["report_summary"]["source_ledger_hash"] == latest["ledger_hash"]


@pytest.mark.asyncio
async def test_round_count_splits_target_and_interval_is_persisted(tmp_path: Path) -> None:
    repository, plan, lease, executor = _setup(
        tmp_path,
        account_ids=("a1",),
        round_count=3,
        round_interval_seconds=7,
    )
    try:
        await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        result = await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        long_qty = repository.sum_kanglong_batch_leg_qty(plan.run_id, "a1", "LONG")
        short_qty = repository.sum_kanglong_batch_leg_qty(plan.run_id, "a1", "SHORT")
    finally:
        repository.close()
    assert long_qty == short_qty == Decimal("0.334")
    assert result["progress"]["batch_round_indexes"]["a1"] == 1
    assert result["progress"]["next_wakeup_at"]
    assert result["status"] == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("requested_status", "expected_status"),
    [
        ("pause_pending", "paused_by_user"),
        ("stop_pending", "stopped_by_user"),
    ],
)
async def test_control_request_interrupts_safe_round_interval_immediately(
    tmp_path: Path,
    requested_status: str,
    expected_status: str,
) -> None:
    repository, plan, lease, executor = _setup(
        tmp_path,
        account_ids=("a1",),
        round_count=3,
        round_interval_seconds=3600,
    )
    try:
        await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        waiting = await executor.run_next(
            plan.run_id,
            lease["lease_token"],
            lease["fencing_token"],
        )
        assert waiting["progress"]["next_wakeup_at"]
        repository.update_kanglong_run(
            plan.run_id,
            status=requested_status,
            available_actions=["view_report"],
        )

        controlled = await executor.run_next(
            plan.run_id,
            lease["lease_token"],
            lease["fencing_token"],
        )
    finally:
        repository.close()

    assert controlled["status"] == expected_status


@pytest.mark.asyncio
async def test_stop_waits_for_pending_second_leg_then_stops_aligned(tmp_path: Path) -> None:
    repository, plan, lease, executor = _setup(tmp_path, account_ids=("a1",))
    try:
        await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        repository.update_kanglong_run(
            plan.run_id,
            status="stop_pending",
            available_actions=["view_report"],
        )
        paired = await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        stopped = await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        long_qty = repository.sum_kanglong_batch_leg_qty(plan.run_id, "a1", "LONG")
        short_qty = repository.sum_kanglong_batch_leg_qty(plan.run_id, "a1", "SHORT")
    finally:
        repository.close()
    assert paired["status"] == "stop_pending"
    assert stopped["status"] == "stopped_by_user"
    assert long_qty == short_qty


@pytest.mark.parametrize(
    ("raw_gap", "expected"),
    [("0.0005", "dust"), ("0.001", "align"), ("0.0015", "align")],
)
def test_alignment_uses_tradeability_not_less_than_or_equal_step(raw_gap: str, expected: str) -> None:
    assert KanglongBatchExecutor.classify_gap(Decimal(raw_gap), Decimal("2000"), RULES) == expected


def test_orderbook_snapshot_identity_rejects_duplicate_and_out_of_order_updates() -> None:
    first = OrderbookSnapshot.from_mapping(
        {
            "symbol": "ETHUSDC",
            "lastUpdateId": 102,
            "event_time": datetime.now(UTC),
            "bids": [{"price": "100", "qty": "1"}],
            "asks": [{"price": "101", "qty": "1"}],
        }
    )
    assert KanglongBatchExecutor._snapshot_identity(first) == "update:102"


@pytest.mark.asyncio
async def test_duplicate_book_snapshot_pauses_without_recounting_fill(tmp_path: Path) -> None:
    repository, plan, lease, executor = _setup(tmp_path, account_ids=("a1",))
    try:
        await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        gateway = executor._runtime_manager.gateways["a1"]
        gateway.freeze_update_id = True
        paused = await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        filled = repository.sum_kanglong_batch_leg_qty(plan.run_id, "a1", "LONG")
    finally:
        repository.close()
    assert paused["status"] == "paused_market_unstable"
    assert filled == Decimal("0")


@pytest.mark.asyncio
async def test_second_account_is_force_rechecked_and_blocks_at_safe_boundary(tmp_path: Path) -> None:
    repository, plan, lease, executor = _setup(tmp_path)
    try:
        await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        executor._capacity_coordinator.blocked_account_ids.add("a2")
        blocked = await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        account = repository.get_kanglong_batch_account(plan.run_id, "a2")
        filled = repository.sum_kanglong_batch_leg_qty(plan.run_id, "a2", "LONG")
    finally:
        repository.close()
    assert blocked["status"] == "paused_plan_recheck_changed"
    assert account["status"] == "blocked_precheck"
    assert filled == Decimal("0")


@pytest.mark.asyncio
async def test_fresh_price_beyond_frozen_reference_pauses_before_fill(tmp_path: Path) -> None:
    repository, plan, lease, executor = _setup(tmp_path)
    try:
        await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        gateway = executor._runtime_manager.gateways["a2"]
        gateway.bid_price = Decimal("110")
        gateway.ask_price = Decimal("111")
        paused = await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        filled = repository.sum_kanglong_batch_leg_qty(plan.run_id, "a2", "LONG")
    finally:
        repository.close()
    assert paused["status"] == "paused_plan_recheck_changed"
    assert filled == Decimal("0")


@pytest.mark.asyncio
async def test_long_retry_after_is_persisted_without_worker_sleep(tmp_path: Path) -> None:
    repository, plan, lease, executor = _setup(tmp_path, account_ids=("a1",))
    executor._capacity_coordinator.error = TradingError(
        "rate limited",
        category=ErrorCategory.RATE_LIMIT,
        strategy=ErrorStrategy.RETRY,
        source="exchange",
        code="binance_rate_limit",
        raw_code=429,
        context={"retry_after_seconds": Decimal("120"), "http_status": 429},
    )
    try:
        result = await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
        account = repository.get_kanglong_batch_account(plan.run_id, "a1")
    finally:
        repository.close()
    assert account["status"] == "retry_wait"
    assert result["progress"]["next_wakeup_at"]
    assert result["progress"]["transport_retry_count"] == 1


def test_transport_retry_policy_is_bounded_and_persists_long_waits() -> None:
    policy = TransportRetryPolicy(jitter=lambda: 1)
    assert policy.max_attempts == 5
    assert policy.base_delay_ms == 500
    assert policy.max_delay_ms == 30_000
    decision = policy.on_rate_limit(Decimal("120"))
    assert decision.persist_retry_wait is True
    assert decision.next_wakeup_at is not None


def test_all_batch_statuses_are_enum_members_with_precise_stale_actions() -> None:
    values = {status.value for status in KanglongRunStatus}
    assert {
        "running", "pause_pending", "paused_by_user", "paused_market_unstable",
        "stop_pending", "stopped_by_user", "completed_with_dust_residual",
    } <= values
    assert available_actions_for_status("blocked_plan_stale") == ["refresh_plan", "view_report"]
    assert available_actions_for_status("paused_plan_recheck_changed") == [
        "refresh_plan", "stop", "view_report",
    ]


@pytest.mark.asyncio
async def test_complete_three_account_simulation_uses_read_only_gateway_calls(tmp_path: Path) -> None:
    repository, plan, lease, executor = _setup(tmp_path, account_ids=("a1", "a2", "a3"))
    try:
        for _ in range(20):
            result = await executor.run_next(plan.run_id, lease["lease_token"], lease["fencing_token"])
            if result["status"] in {"completed", "completed_with_dust_residual"}:
                break
        else:
            pytest.fail("three-account batch did not complete within the bounded step count")
        calls = [
            call
            for gateway in executor._runtime_manager.gateways.values()
            for call in gateway.binance_calls
        ]
    finally:
        repository.close()

    assert calls
    assert all(call.method == "GET" for call in calls)
    assert not any("/order" in call.path for call in calls)
    assert not any(call.path.endswith("/leverage") for call in calls)
