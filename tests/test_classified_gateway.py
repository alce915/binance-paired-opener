from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from paired_opener.classified_gateway import ClassifiedExchangeGateway
from paired_opener.domain import ExchangeOrder, ExchangeOrderStatus, ExchangeStateError, OpenSession, OrderSide, PositionSide, Quote, SessionSpec, SessionStatus, SymbolRules, TrendBias
from paired_opener.errors import ErrorCategory, ErrorStrategy, TradingError
from paired_opener.exchange import ExchangeGateway
from paired_opener.storage import SqliteRepository


class StubGateway(ExchangeGateway):
    def __init__(self) -> None:
        self.calls = 0

    async def ensure_hedge_mode(self) -> None:
        return None

    async def is_hedge_mode_enabled(self) -> bool:
        return True

    async def ensure_cross_margin(self, symbol: str) -> None:
        return None

    async def ensure_leverage(self, symbol: str, leverage: int) -> None:
        return None

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        return SymbolRules(symbol=symbol, tick_size=Decimal("0.1"), step_size=Decimal("0.001"), min_qty=Decimal("0.001"), min_notional=Decimal("5"), max_leverage=125)

    async def get_quote(self, symbol: str) -> Quote:
        self.calls += 1
        if self.calls == 1:
            request = httpx.Request("GET", "https://fapi.binance.com/fapi/v1/ticker/bookTicker")
            response = httpx.Response(429, request=request, text='{"code":-1003,"msg":"Too many requests"}')
            raise httpx.HTTPStatusError("rate limited", request=request, response=response)
        return Quote(symbol=symbol, bid_price=Decimal("100"), ask_price=Decimal("101"), event_time=datetime.now(UTC))

    async def get_order_book(self, symbol: str, limit: int = 10) -> dict:
        return {"symbol": symbol, "bids": [], "asks": []}

    async def get_account_overview(self) -> dict:
        raise ExchangeStateError("Binance API credentials are not configured")

    async def get_symbol_leverage(self, symbol: str) -> int:
        return 10

    async def get_open_orders(self, symbol: str) -> list[dict]:
        return []

    async def place_limit_order(self, *, symbol: str, side: OrderSide, position_side: PositionSide, qty: Decimal, price: Decimal, client_order_id: str) -> ExchangeOrder:
        raise NotImplementedError

    async def place_market_order(self, *, symbol: str, side: OrderSide, position_side: PositionSide, qty: Decimal, client_order_id: str) -> ExchangeOrder:
        raise NotImplementedError

    async def get_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        raise NotImplementedError

    async def cancel_order(self, *, symbol: str, order_id: str) -> ExchangeOrder:
        raise NotImplementedError


@pytest.mark.asyncio
async def test_classified_gateway_retries_rate_limit_then_succeeds() -> None:
    gateway = ClassifiedExchangeGateway(StubGateway())

    quote = await gateway.get_quote("BTCUSDT")

    assert quote.symbol == "BTCUSDT"
    assert gateway._delegate.calls == 2


@pytest.mark.asyncio
async def test_classified_gateway_maps_missing_credentials_to_auth_error() -> None:
    gateway = ClassifiedExchangeGateway(StubGateway())

    with pytest.raises(TradingError) as exc_info:
        await gateway.get_account_overview()

    error = exc_info.value
    assert error.category == ErrorCategory.AUTH_ERROR
    assert error.strategy == ErrorStrategy.MANUAL_INTERVENTION
    assert error.code == "binance_credentials_missing"


def test_repository_persists_structured_error_fields(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "structured.db")
    session = OpenSession.create(
        SessionSpec(
            symbol="BTCUSDT",
            trend_bias=TrendBias.LONG,
            leverage=10,
            round_count=1,
            round_qty=Decimal("0.01"),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=1,
            market_fallback_attempts=1,
            created_by="test",
        )
    )
    repository.create_session(session)
    repository.update_session_status(
        session.session_id,
        SessionStatus.EXCEPTION,
        last_error="账户可用余额或保证金不足。",
        last_error_category="insufficient_balance",
        last_error_strategy="manual_intervention",
        last_error_code="binance_insufficient_balance",
        last_error_operator_action="补充保证金或降低下单规模后重试。",
    )

    payload = repository.get_session(session.session_id)

    assert payload is not None
    assert payload["last_error_category"] == "insufficient_balance"
    assert payload["last_error_strategy"] == "manual_intervention"
    assert payload["last_error_code"] == "binance_insufficient_balance"
    assert payload["last_error_operator_action"] == "补充保证金或降低下单规模后重试。"
