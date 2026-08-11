from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from paired_opener.domain import Quote
from paired_opener.domain import SymbolRules
from paired_opener.kanglong.batch_capacity import (
    CapacityPolicy,
    CapacitySnapshotCoordinator,
    EffectiveLeverageBracket,
    _SharedRateLimitBudget,
    estimate_account_capacity,
)
from paired_opener.exchange import RateLimitObservation


def _brackets(*rows: tuple[str, str, int, str]) -> list[EffectiveLeverageBracket]:
    return [
        EffectiveLeverageBracket(
            bracket=index + 1,
            effective_floor=Decimal(floor),
            effective_cap=Decimal(cap),
            max_allowed_leverage=leverage,
            notional_coef=Decimal(coef),
            notional_floor=Decimal(floor) / Decimal(coef),
            notional_cap=Decimal(cap) / Decimal(coef),
        )
        for index, (floor, cap, leverage, coef) in enumerate(rows)
    ]


def _policy() -> CapacityPolicy:
    return CapacityPolicy(
        margin_safety_ratio=Decimal("0.10"),
        price_buffer_bps=20,
        max_notional_ratio=Decimal("0.90"),
        min_liquidation_buffer_ratio=Decimal("0.20"),
    )


def _capacity_inputs() -> dict:
    return {
        "requested_leverage": 100,
        "current_symbol_leverage": 100,
        "current_symbol_max_notional_value": Decimal("1000000"),
        "brackets": _brackets(("0", "1000000", 125, "1")),
        "available_balance": Decimal("100000"),
        "equity": Decimal("100000"),
        "maker_fee_rate": Decimal("0"),
        "taker_fee_rate": Decimal("0"),
        "existing_symbol_exposure": Decimal("0"),
        "policy": _policy(),
    }


def test_capacity_uses_gross_two_leg_notional() -> None:
    result = estimate_account_capacity(per_leg_notional=Decimal("250000"), **_capacity_inputs())
    assert result.requested_gross_notional == Decimal("500000")
    assert result.estimated_capacity_usage_percent is not None
    assert result.estimated_capacity_usage_percent > Decimal("0")


def test_existing_exposure_reduces_bracket_capacity() -> None:
    inputs = _capacity_inputs()
    inputs.update(
        current_symbol_max_notional_value=Decimal("600000"),
        brackets=_brackets(("0", "600000", 125, "1")),
        existing_symbol_exposure=Decimal("200000"),
    )
    result = estimate_account_capacity(per_leg_notional=Decimal("250000"), **inputs)
    assert result.bracket_remaining_notional == Decimal("400000")
    assert result.estimated_capacity_usage_percent == Decimal("125")
    assert result.blocked is True


def test_projected_two_leg_exposure_selects_the_final_bracket() -> None:
    inputs = _capacity_inputs()
    inputs.update(
        current_symbol_max_notional_value=Decimal("800000"),
        brackets=_brackets(("0", "400000", 125, "1"), ("400000", "800000", 75, "1")),
    )
    result = estimate_account_capacity(per_leg_notional=Decimal("250000"), **inputs)
    assert result.projected_symbol_exposure == Decimal("500000")
    assert result.bracket_max_allowed_leverage == 75
    assert result.blocked_reason == "requested_leverage_exceeds_projected_bracket"


def test_symbol_config_max_notional_is_a_capacity_limit() -> None:
    inputs = _capacity_inputs()
    inputs["current_symbol_max_notional_value"] = Decimal("450000")
    result = estimate_account_capacity(per_leg_notional=Decimal("250000"), **inputs)
    assert result.symbol_config_remaining_notional == Decimal("450000")
    assert result.blocked is True


def test_capacity_uses_current_leverage_when_requested_leverage_is_not_active() -> None:
    inputs = _capacity_inputs()
    inputs.update(current_symbol_leverage=20, available_balance=Decimal("10000"), equity=Decimal("10000"))
    result = estimate_account_capacity(per_leg_notional=Decimal("250000"), **inputs)
    assert result.requested_leverage == 100
    assert result.current_symbol_leverage == 20
    assert result.effective_capacity_leverage == 20
    assert result.margin_capacity_notional < Decimal("1000000")


def test_negative_fee_values_do_not_increase_capacity() -> None:
    inputs = _capacity_inputs()
    inputs.update(maker_fee_rate=Decimal("-0.1"), taker_fee_rate=Decimal("-0.2"))
    negative = estimate_account_capacity(per_leg_notional=Decimal("1000"), **inputs)
    inputs.update(maker_fee_rate=Decimal("0"), taker_fee_rate=Decimal("0"))
    zero = estimate_account_capacity(per_leg_notional=Decimal("1000"), **inputs)
    assert negative.margin_capacity_notional == zero.margin_capacity_notional


def test_effective_bracket_coefficient_is_not_applied_twice() -> None:
    inputs = _capacity_inputs()
    inputs.update(
        current_symbol_max_notional_value=Decimal("800000"),
        brackets=_brackets(("0", "800000", 125, "2")),
    )
    result = estimate_account_capacity(per_leg_notional=Decimal("250000"), **inputs)
    assert result.selected_bracket_effective_cap == Decimal("800000")
    assert result.bracket_notional_coef == Decimal("2")


def test_unrounded_capacity_boundary_controls_blocking() -> None:
    inputs = _capacity_inputs()
    inputs["current_symbol_max_notional_value"] = Decimal("499999.9999")
    result = estimate_account_capacity(per_leg_notional=Decimal("250000"), **inputs)
    assert result.estimated_capacity_usage_percent > Decimal("100")
    assert result.blocked is True


class FakeCapacityGateway:
    def __init__(self, shared_private: dict[str, int]) -> None:
        self.shared_private = shared_private
        self.private_calls: dict[tuple[str, str], int] = {}
        self.quote_calls = 0
        self.order_book_calls = 0
        self.concurrent_private = 0
        self.max_concurrent_private = 0
        self.precheck_requests: list[tuple[int, Decimal]] = []

    async def get_portfolio_margin_precheck(self, symbol, requested_leverage, additional_gross_notional):
        self.precheck_requests.append((requested_leverage, Decimal(additional_gross_notional)))
        account_id = self.account_id
        key = (account_id, symbol)
        self.private_calls[key] = self.private_calls.get(key, 0) + 1
        self.concurrent_private += 1
        self.shared_private["current"] += 1
        self.shared_private["max"] = max(self.shared_private["max"], self.shared_private["current"])
        self.max_concurrent_private = max(self.max_concurrent_private, self.concurrent_private)
        await asyncio.sleep(0)
        self.concurrent_private -= 1
        self.shared_private["current"] -= 1
        return {
            "account_status": "NORMAL",
            "hedge_mode": True,
            "account_equity": Decimal("10000") + Decimal(account_id.removeprefix("a") or "0"),
            "available_balance": Decimal("10000"),
            "current_leverage": 100,
            "current_symbol_max_notional_value": Decimal("1000000"),
            "notional_coef": Decimal("1"),
            "brackets": [
                {
                    "bracket": 1,
                    "max_allowed_leverage": 125,
                    "notional_floor": Decimal("0"),
                    "notional_cap": Decimal("1000000"),
                    "notional_coef": Decimal("1"),
                    "effective_floor": Decimal("0"),
                    "effective_cap": Decimal("1000000"),
                }
            ],
            "existing_symbol_exposure": Decimal("0"),
            "positions": [],
            "open_orders": [],
            "commission_rates": {"maker": Decimal("0.0002"), "taker": Decimal("0")},
            "blocked_reasons": ["requested_leverage_exceeds_bracket"] if requested_leverage > 100 else [],
        }

    async def get_quote(self, symbol):
        self.quote_calls += 1
        return Quote(symbol, Decimal("100"), Decimal("101"), datetime.now(UTC))

    async def refresh_quote(self, symbol):
        return await self.get_quote(symbol)

    async def get_order_book(self, symbol, limit=20):
        self.order_book_calls += 1
        return {
            "symbol": symbol,
            "bids": [{"price": Decimal("100"), "qty": Decimal("1")}] * limit,
            "asks": [{"price": Decimal("101"), "qty": Decimal("1")}] * limit,
            "event_time": datetime.now(UTC),
        }

    async def refresh_order_book(self, symbol, limit=20):
        return await self.get_order_book(symbol, limit)

    async def get_symbol_rules(self, symbol):
        return SymbolRules(
            symbol=symbol,
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        )


class FakeRuntimeManager:
    def __init__(self, account_ids: list[str]) -> None:
        self.shared_private = {"current": 0, "max": 0}
        self.gateways: dict[str, FakeCapacityGateway] = {}
        for account_id in account_ids:
            gateway = FakeCapacityGateway(self.shared_private)
            gateway.account_id = account_id
            self.gateways[account_id] = gateway

    def current(self, account_id: str):
        return SimpleNamespace(gateway=self.gateways[account_id])


@pytest.mark.asyncio
async def test_concurrent_preview_requests_share_one_upstream_snapshot() -> None:
    runtimes = FakeRuntimeManager(["a1"])
    coordinator = CapacitySnapshotCoordinator(runtimes)
    await asyncio.gather(
        *[coordinator.get_snapshot("revision-1", "a1", "ETHUSDC") for _ in range(20)]
    )
    assert runtimes.gateways["a1"].private_calls[("a1", "ETHUSDC")] == 1
    assert runtimes.gateways["a1"].precheck_requests == [(1, Decimal("0"))]


@pytest.mark.asyncio
async def test_raw_snapshot_collection_does_not_apply_internal_125x_blocker() -> None:
    runtimes = FakeRuntimeManager(["a1"])
    snapshot = await CapacitySnapshotCoordinator(runtimes).get_snapshot(
        "revision-1", "a1", "ETHUSDC"
    )
    assert "requested_leverage_exceeds_bracket" not in snapshot.blocked_reasons


@pytest.mark.asyncio
async def test_force_refresh_and_new_revision_bypass_private_cache() -> None:
    runtimes = FakeRuntimeManager(["a1"])
    coordinator = CapacitySnapshotCoordinator(runtimes)
    first = await coordinator.get_snapshot("revision-1", "a1", "ETHUSDC")
    second = await coordinator.get_snapshot("revision-1", "a1", "ETHUSDC", force_refresh=True)
    third = await coordinator.get_snapshot("revision-2", "a1", "ETHUSDC")
    assert first.account_equity == second.account_equity == third.account_equity
    assert runtimes.gateways["a1"].private_calls[("a1", "ETHUSDC")] == 3


@pytest.mark.asyncio
async def test_public_market_snapshot_is_shared_and_private_concurrency_is_bounded() -> None:
    account_ids = [f"a{index}" for index in range(1, 101)]
    runtimes = FakeRuntimeManager(account_ids)
    coordinator = CapacitySnapshotCoordinator(runtimes, private_concurrency=4)
    snapshots = await asyncio.gather(
        *[
            coordinator.get_snapshot("revision-1", account_id, "ETHUSDC", force_refresh=True)
            for account_id in account_ids
        ]
    )
    assert sum(gateway.quote_calls for gateway in runtimes.gateways.values()) == 1
    assert sum(gateway.order_book_calls for gateway in runtimes.gateways.values()) == 1
    assert runtimes.shared_private["max"] == 4
    for snapshot in snapshots:
        assert snapshot.oldest_component_at <= snapshot.assembled_at
        assert set(snapshot.snapshot_components) >= {
            "account", "positions", "open_orders", "symbol_config",
            "leverage_bracket", "commission_rate", "quote", "order_book",
        }


@pytest.mark.asyncio
async def test_retry_after_pauses_the_shared_rate_limit_budget(monkeypatch) -> None:
    clock = {"now": 100.0}
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("paired_opener.kanglong.batch_capacity.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("paired_opener.kanglong.batch_capacity.asyncio.sleep", fake_sleep)
    budget = _SharedRateLimitBudget()
    budget.observe(RateLimitObservation(http_status=429, used_weight_by_window={}, retry_after_seconds=Decimal("2.5")))
    await budget.wait()
    assert sleeps == [2.5]
