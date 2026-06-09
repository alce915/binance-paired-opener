from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from paired_opener.domain import OrderSide, PositionSide, SymbolRules
from paired_opener.simulation_matching import (
    DeterministicMarketDataProvider,
    MarketDataStaleError,
    OrderbookLevel,
    OrderbookMatcher,
    OrderbookSnapshot,
)


FIXED_NOW = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)


def rules(symbol: str = "ETHUSDC") -> SymbolRules:
    return SymbolRules(
        symbol=symbol,
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        min_notional=Decimal("5"),
        max_leverage=125,
    )


def snapshot(
    *,
    symbol: str = "ETHUSDC",
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
    event_time: datetime = FIXED_NOW,
) -> OrderbookSnapshot:
    return OrderbookSnapshot(
        symbol=symbol,
        bids=[OrderbookLevel(price=Decimal(price), qty=Decimal(qty)) for price, qty in bids or [("999", "2")]],
        asks=[OrderbookLevel(price=Decimal(price), qty=Decimal(qty)) for price, qty in asks or [("1000", "2")]],
        event_time=event_time,
    )


async def async_noop(_: Decimal) -> None:
    return None


def test_taker_match_consumes_orderbook_depth_with_weighted_average() -> None:
    matcher = OrderbookMatcher(maker_fee_rate=Decimal("0"), taker_fee_rate=Decimal("0.0005"))
    result = matcher.match_orderbook_snapshot(
        snapshot(
            asks=[
                ("1000", "0.500"),
                ("1002", "0.700"),
            ]
        ),
        order_side=OrderSide.BUY,
        position_side=PositionSide.LONG,
        target_qty=Decimal("1.000"),
        rules=rules(),
        liquidity="taker",
    )

    assert result.requested_qty == Decimal("1.000")
    assert result.filled_qty == Decimal("1.000")
    assert result.avg_price == Decimal("1001")
    assert result.notional == Decimal("1001.000000000")
    assert result.fee == Decimal("0.500500000")
    assert result.residual_qty == Decimal("0.000")
    assert result.depth_levels_consumed == 2
    assert result.slippage_bps == Decimal("10.000000000")
    assert result.liquidity == "taker"


@pytest.mark.asyncio
async def test_passive_limit_waits_across_snapshots_and_reports_residual() -> None:
    matcher = OrderbookMatcher(
        maker_fee_rate=Decimal("0.0002"),
        taker_fee_rate=Decimal("0.0005"),
        sleep=async_noop,
    )
    provider = DeterministicMarketDataProvider(
        [
            snapshot(asks=[("1001", "0.100")]),
            snapshot(asks=[("999", "0.400"), ("998", "0.700")]),
        ],
        now=lambda: FIXED_NOW,
    )

    result = await matcher.poll_passive_limit_fill(
        symbol="ETHUSDC",
        order_side=OrderSide.BUY,
        position_side=PositionSide.LONG,
        target_qty=Decimal("0.600"),
        rules=rules(),
        limit_price=Decimal("999"),
        wait_seconds=Decimal("1.0"),
        market_data=provider,
        poll_interval_seconds=Decimal("0.5"),
    )

    assert result.filled_qty == Decimal("0.600")
    assert result.avg_price == Decimal("999")
    assert result.notional == Decimal("599.400000000")
    assert result.fee == Decimal("0.119880000")
    assert result.residual_qty == Decimal("0.000")
    assert result.depth_levels_consumed == 2
    assert result.wait_seconds_consumed == Decimal("1.0")
    assert result.liquidity == "maker"


@pytest.mark.asyncio
async def test_passive_limit_returns_zero_fill_when_price_never_crosses() -> None:
    matcher = OrderbookMatcher(
        maker_fee_rate=Decimal("0.0002"),
        taker_fee_rate=Decimal("0.0005"),
        sleep=async_noop,
    )
    provider = DeterministicMarketDataProvider(
        [
            snapshot(bids=[("999", "2")], asks=[("1001", "2")]),
            snapshot(bids=[("999.5", "2")], asks=[("1000.5", "2")]),
        ],
        now=lambda: FIXED_NOW,
    )

    result = await matcher.poll_passive_limit_fill(
        symbol="ETHUSDC",
        order_side=OrderSide.BUY,
        position_side=PositionSide.LONG,
        target_qty=Decimal("0.600"),
        rules=rules(),
        limit_price=Decimal("999"),
        wait_seconds=Decimal("1.0"),
        market_data=provider,
        poll_interval_seconds=Decimal("0.5"),
    )

    assert result.filled_qty == Decimal("0")
    assert result.avg_price == Decimal("0")
    assert result.notional == Decimal("0")
    assert result.fee == Decimal("0")
    assert result.residual_qty == Decimal("0.600")
    assert result.depth_levels_consumed == 2
    assert result.wait_seconds_consumed == Decimal("1.0")


@pytest.mark.asyncio
async def test_stale_market_snapshot_raises_market_data_stale() -> None:
    provider = DeterministicMarketDataProvider(
        [
            snapshot(event_time=FIXED_NOW - timedelta(seconds=6)),
        ],
        now=lambda: FIXED_NOW,
        max_age_seconds=5,
    )

    with pytest.raises(MarketDataStaleError):
        await provider.get_orderbook("ETHUSDC")


def test_fee_is_calculated_from_frozen_fee_rate_and_notional() -> None:
    matcher = OrderbookMatcher(maker_fee_rate=Decimal("0.0001"), taker_fee_rate=Decimal("0.0007"))
    result = matcher.match_orderbook_snapshot(
        snapshot(asks=[("2400", "1.250")]),
        order_side=OrderSide.BUY,
        position_side=PositionSide.LONG,
        target_qty=Decimal("1.250"),
        rules=rules(),
        liquidity="taker",
    )

    assert result.notional == Decimal("3000.000000000")
    assert result.fee == Decimal("2.100000000")
