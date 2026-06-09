from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

from paired_opener.domain import OrderSide, PositionSide, SymbolRules
from paired_opener.rounding import normalize_qty


MONEY_SCALE = Decimal("0.000000001")
SIMULATION_MAKER_POLL_SECONDS = Decimal("0.5")
ORDERBOOK_MAX_AGE_SECONDS = 5


class MarketDataStaleError(RuntimeError):
    pass


@dataclass(slots=True)
class MatchResult:
    requested_qty: Decimal
    filled_qty: Decimal
    avg_price: Decimal
    notional: Decimal
    fee: Decimal
    residual_qty: Decimal
    depth_levels_consumed: int
    slippage_bps: Decimal
    liquidity: str
    side: OrderSide
    position_side: PositionSide
    wait_seconds_consumed: Decimal = Decimal("0")


@dataclass(frozen=True, slots=True)
class OrderbookLevel:
    price: Decimal
    qty: Decimal


@dataclass(frozen=True, slots=True)
class OrderbookSnapshot:
    symbol: str
    bids: list[OrderbookLevel]
    asks: list[OrderbookLevel]
    event_time: datetime
    source: str = "deterministic"

    @classmethod
    def from_mapping(cls, payload: dict[str, Any], *, source: str = "gateway") -> OrderbookSnapshot:
        event_time = _event_time_from_payload(payload)
        return cls(
            symbol=str(payload.get("symbol") or ""),
            bids=[_level_from_mapping(level) for level in payload.get("bids") or []],
            asks=[_level_from_mapping(level) for level in payload.get("asks") or []],
            event_time=event_time,
            source=source,
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "bids": [{"price": level.price, "qty": level.qty} for level in self.bids],
            "asks": [{"price": level.price, "qty": level.qty} for level in self.asks],
            "event_time": self.event_time,
        }


class MarketDataProvider(Protocol):
    async def get_orderbook(self, symbol: str) -> OrderbookSnapshot:
        ...


class DeterministicMarketDataProvider:
    def __init__(
        self,
        snapshots: list[OrderbookSnapshot],
        *,
        now: Callable[[], datetime] | None = None,
        max_age_seconds: int = ORDERBOOK_MAX_AGE_SECONDS,
    ) -> None:
        if not snapshots:
            raise ValueError("snapshots must not be empty")
        self._snapshots = list(snapshots)
        self._index = 0
        self._now = now or (lambda: datetime.now(UTC))
        self._max_age_seconds = max_age_seconds

    async def get_orderbook(self, symbol: str) -> OrderbookSnapshot:
        index = min(self._index, len(self._snapshots) - 1)
        self._index += 1
        snapshot = self._snapshots[index]
        if snapshot.symbol and snapshot.symbol != symbol:
            raise ValueError(f"snapshot symbol {snapshot.symbol} does not match requested symbol {symbol}")
        if not orderbook_is_fresh(snapshot.event_time, now=self._now(), max_age_seconds=self._max_age_seconds):
            raise MarketDataStaleError(f"stale orderbook for {symbol}")
        return snapshot


class GatewayMarketDataProvider:
    def __init__(
        self,
        gateway: Any,
        *,
        now: Callable[[], datetime] | None = None,
        max_age_seconds: int = ORDERBOOK_MAX_AGE_SECONDS,
        limit: int = 20,
    ) -> None:
        self._gateway = gateway
        self._now = now or (lambda: datetime.now(UTC))
        self._max_age_seconds = max_age_seconds
        self._limit = max(int(limit), 5)

    async def get_orderbook(self, symbol: str) -> OrderbookSnapshot:
        snapshot = await self._load_fresh_orderbook(symbol, refresh=False)
        if snapshot is not None:
            return snapshot
        snapshot = await self._load_fresh_orderbook(symbol, refresh=True)
        if snapshot is not None:
            return snapshot
        raise MarketDataStaleError(f"stale orderbook for {symbol}")

    async def _load_fresh_orderbook(self, symbol: str, *, refresh: bool) -> OrderbookSnapshot | None:
        raw_snapshot = (
            await self._gateway.refresh_order_book(symbol, limit=self._limit)
            if refresh
            else await self._gateway.get_order_book(symbol, limit=self._limit)
        )
        snapshot = OrderbookSnapshot.from_mapping(raw_snapshot, source="gateway")
        if not orderbook_is_fresh(snapshot.event_time, now=self._now(), max_age_seconds=self._max_age_seconds):
            return None
        return snapshot


class OrderbookMatcher:
    def __init__(
        self,
        *,
        maker_fee_rate: Decimal,
        taker_fee_rate: Decimal,
        sleep: Callable[[Decimal], Awaitable[None]] | None = None,
    ) -> None:
        self._maker_fee_rate = maker_fee_rate
        self._taker_fee_rate = taker_fee_rate
        self._sleep = sleep or _sleep_seconds

    def match_orderbook_snapshot(
        self,
        snapshot: OrderbookSnapshot | dict[str, Any],
        *,
        order_side: OrderSide,
        position_side: PositionSide,
        target_qty: Decimal,
        rules: SymbolRules,
        liquidity: str,
        limit_price: Decimal | None = None,
    ) -> MatchResult:
        normalized_snapshot = _coerce_snapshot(snapshot)
        levels = normalized_snapshot.asks if order_side == OrderSide.BUY else normalized_snapshot.bids
        remaining = target_qty
        filled = Decimal("0")
        notional = Decimal("0")
        consumed = 0
        best_price = None
        for level in levels:
            price = level.price
            if limit_price is not None:
                if order_side == OrderSide.BUY and price > limit_price:
                    break
                if order_side == OrderSide.SELL and price < limit_price:
                    break
            available_qty = normalize_qty(level.qty, rules)
            if available_qty <= Decimal("0"):
                continue
            if best_price is None:
                best_price = price
            fill_qty = min(remaining, available_qty)
            if fill_qty <= Decimal("0"):
                continue
            filled += fill_qty
            notional += fill_qty * price
            remaining -= fill_qty
            consumed += 1
            if remaining <= Decimal("0"):
                break
        filled = normalize_qty(filled, rules)
        remaining = normalize_qty(max(remaining, Decimal("0")), rules)
        if filled <= Decimal("0"):
            return MatchResult(
                requested_qty=target_qty,
                filled_qty=Decimal("0"),
                avg_price=Decimal("0"),
                notional=Decimal("0"),
                fee=Decimal("0"),
                residual_qty=target_qty,
                depth_levels_consumed=0,
                slippage_bps=Decimal("0"),
                liquidity=liquidity,
                side=order_side,
                position_side=position_side,
            )

        avg_price = notional / filled
        fee_rate = self._maker_fee_rate if liquidity == "maker" else self._taker_fee_rate
        fee = money(notional * fee_rate)
        if best_price and best_price > Decimal("0"):
            if order_side == OrderSide.BUY:
                slippage = ((avg_price - best_price) / best_price) * Decimal("10000")
            else:
                slippage = ((best_price - avg_price) / best_price) * Decimal("10000")
        else:
            slippage = Decimal("0")
        return MatchResult(
            requested_qty=target_qty,
            filled_qty=filled,
            avg_price=money(avg_price).quantize(MONEY_SCALE).normalize(),
            notional=money(notional),
            fee=fee,
            residual_qty=remaining,
            depth_levels_consumed=consumed,
            slippage_bps=money(slippage),
            liquidity=liquidity,
            side=order_side,
            position_side=position_side,
        )

    async def poll_passive_limit_fill(
        self,
        *,
        symbol: str,
        order_side: OrderSide,
        position_side: PositionSide,
        target_qty: Decimal,
        rules: SymbolRules,
        limit_price: Decimal,
        wait_seconds: Decimal,
        market_data: MarketDataProvider,
        poll_interval_seconds: Decimal = SIMULATION_MAKER_POLL_SECONDS,
        abort_requested: Callable[[], bool] | None = None,
        on_wait: Callable[[], Awaitable[None]] | None = None,
    ) -> MatchResult:
        remaining = normalize_qty(target_qty, rules)
        filled = Decimal("0")
        notional = Decimal("0")
        polls = 0
        elapsed = Decimal("0")
        while remaining > Decimal("0") and elapsed < wait_seconds:
            step = min(poll_interval_seconds, wait_seconds - elapsed)
            if step > Decimal("0"):
                await self._sleep(step)
                elapsed += step
                if on_wait is not None:
                    await on_wait()
            if abort_requested is not None and abort_requested():
                break
            snapshot = await market_data.get_orderbook(symbol)
            polls += 1
            fill_qty = self.passive_fill_qty_from_snapshot(snapshot, order_side, limit_price, remaining, rules)
            if fill_qty <= Decimal("0"):
                continue
            filled += fill_qty
            notional += fill_qty * limit_price
            remaining = normalize_qty(max(remaining - fill_qty, Decimal("0")), rules)
        filled = normalize_qty(filled, rules)
        remaining = normalize_qty(max(target_qty - filled, Decimal("0")), rules)
        if filled <= Decimal("0"):
            return MatchResult(
                requested_qty=target_qty,
                filled_qty=Decimal("0"),
                avg_price=Decimal("0"),
                notional=Decimal("0"),
                fee=Decimal("0"),
                residual_qty=target_qty,
                depth_levels_consumed=polls,
                slippage_bps=Decimal("0"),
                liquidity="maker",
                side=order_side,
                position_side=position_side,
                wait_seconds_consumed=elapsed,
            )
        return MatchResult(
            requested_qty=target_qty,
            filled_qty=filled,
            avg_price=money(limit_price).quantize(MONEY_SCALE).normalize(),
            notional=money(notional),
            fee=money(notional * self._maker_fee_rate),
            residual_qty=remaining,
            depth_levels_consumed=polls,
            slippage_bps=Decimal("0"),
            liquidity="maker",
            side=order_side,
            position_side=position_side,
            wait_seconds_consumed=elapsed,
        )

    def passive_limit_price(self, snapshot: OrderbookSnapshot | dict[str, Any], order_side: OrderSide) -> Decimal:
        normalized_snapshot = _coerce_snapshot(snapshot)
        levels = normalized_snapshot.bids if order_side == OrderSide.BUY else normalized_snapshot.asks
        if not levels:
            return Decimal("0")
        return levels[0].price

    def limit_order_crosses(
        self,
        snapshot: OrderbookSnapshot | dict[str, Any],
        order_side: OrderSide,
        limit_price: Decimal,
    ) -> bool:
        normalized_snapshot = _coerce_snapshot(snapshot)
        opposite_levels = normalized_snapshot.asks if order_side == OrderSide.BUY else normalized_snapshot.bids
        if not opposite_levels:
            return False
        opposite_best = opposite_levels[0].price
        if opposite_best <= Decimal("0"):
            return False
        if order_side == OrderSide.BUY:
            return opposite_best <= limit_price
        return opposite_best >= limit_price

    def passive_fill_qty_from_snapshot(
        self,
        snapshot: OrderbookSnapshot | dict[str, Any],
        order_side: OrderSide,
        limit_price: Decimal,
        remaining: Decimal,
        rules: SymbolRules,
    ) -> Decimal:
        normalized_snapshot = _coerce_snapshot(snapshot)
        fillable = Decimal("0")
        opposite_levels = normalized_snapshot.asks if order_side == OrderSide.BUY else normalized_snapshot.bids
        for level in opposite_levels:
            price = level.price
            if order_side == OrderSide.BUY and price > limit_price:
                break
            if order_side == OrderSide.SELL and price < limit_price:
                break
            fillable += normalize_qty(level.qty, rules)
        if fillable <= Decimal("0"):
            same_levels = normalized_snapshot.bids if order_side == OrderSide.BUY else normalized_snapshot.asks
            same_best = same_levels[0].price if same_levels else Decimal("0")
            if order_side == OrderSide.BUY and same_best > Decimal("0") and same_best < limit_price:
                fillable = remaining
            elif order_side == OrderSide.SELL and same_best > limit_price:
                fillable = remaining
        return normalize_qty(min(remaining, fillable), rules)


def orderbook_is_fresh(event_time: datetime, *, now: datetime | None = None, max_age_seconds: int = ORDERBOOK_MAX_AGE_SECONDS) -> bool:
    reference = now or datetime.now(UTC)
    normalized_event_time = event_time if event_time.tzinfo else event_time.replace(tzinfo=UTC)
    age = (reference.astimezone(UTC) - normalized_event_time.astimezone(UTC)).total_seconds()
    return age <= max_age_seconds


def money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_SCALE)


def to_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _coerce_snapshot(snapshot: OrderbookSnapshot | dict[str, Any]) -> OrderbookSnapshot:
    if isinstance(snapshot, OrderbookSnapshot):
        return snapshot
    return OrderbookSnapshot.from_mapping(snapshot)


def _level_from_mapping(payload: Any) -> OrderbookLevel:
    if isinstance(payload, OrderbookLevel):
        return payload
    return OrderbookLevel(price=to_decimal(payload.get("price")), qty=to_decimal(payload.get("qty")))


def _event_time_from_payload(payload: dict[str, Any]) -> datetime:
    raw_event_time = payload.get("event_time") or payload.get("updated_at")
    if isinstance(raw_event_time, datetime):
        return raw_event_time if raw_event_time.tzinfo else raw_event_time.replace(tzinfo=UTC)
    if isinstance(raw_event_time, (int, float)):
        return datetime.fromtimestamp(float(raw_event_time) / 1000, tz=UTC)
    if isinstance(raw_event_time, str):
        event_dt = datetime.fromisoformat(raw_event_time.replace("Z", "+00:00"))
        return event_dt if event_dt.tzinfo else event_dt.replace(tzinfo=UTC)
    raise MarketDataStaleError("orderbook event time is missing")


async def _sleep_seconds(seconds: Decimal) -> None:
    await asyncio.sleep(float(seconds))
