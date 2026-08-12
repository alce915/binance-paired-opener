from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from paired_opener.binance import BinanceFuturesGateway
from paired_opener.classified_gateway import ClassifiedExchangeGateway
from paired_opener.config import Settings
from paired_opener.domain import ExchangeOrder, ExchangeOrderStatus, OrderSide, PositionSide, Quote
from paired_opener.errors import TradingError
from paired_opener.exchange import RateLimitObservation


@pytest.mark.asyncio
async def test_get_order_refreshes_partial_fill_cache() -> None:
    gateway = BinanceFuturesGateway(Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret"))
    gateway._order_cache["123"] = ExchangeOrder(
        symbol="BTCUSDT",
        order_id="123",
        client_order_id="123",
        side=OrderSide.BUY,
        position_side=PositionSide.LONG,
        type="LIMIT",
        price=Decimal("100"),
        orig_qty=Decimal("1"),
        executed_qty=Decimal("0.4"),
        status=ExchangeOrderStatus.PARTIALLY_FILLED,
        update_time=datetime.now(UTC),
    )
    calls: list[tuple[str, str, dict[str, str]]] = []

    async def fake_signed_request(method: str, path: str, params: dict[str, str]):
        calls.append((method, path, params))
        return {
            "symbol": "BTCUSDT",
            "orderId": 123,
            "clientOrderId": "123",
            "side": "BUY",
            "positionSide": "LONG",
            "type": "LIMIT",
            "price": "100",
            "origQty": "1",
            "executedQty": "1",
            "status": "FILLED",
        }

    gateway._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        order = await gateway.get_order(symbol="BTCUSDT", order_id="123")
    finally:
        await gateway.close()

    assert len(calls) == 1
    assert order.status == ExchangeOrderStatus.FILLED
    assert order.executed_qty == Decimal("1")

@pytest.mark.asyncio
async def test_ensure_hedge_mode_is_idempotent_when_already_enabled() -> None:
    gateway = BinanceFuturesGateway(Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret"))

    async def fake_ensure_user_stream() -> None:
        return None

    async def fake_signed_request(method: str, path: str, params: dict[str, str]):
        request = httpx.Request(method, f"https://fapi.binance.com{path}")
        response = httpx.Response(400, request=request, text='{"code":-4059,"msg":"No need to change position side."}')
        raise httpx.HTTPStatusError("already hedge mode", request=request, response=response)

    gateway._ensure_user_stream = fake_ensure_user_stream  # type: ignore[method-assign]
    gateway._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        await gateway.ensure_hedge_mode()
    finally:
        await gateway.close()

@pytest.mark.asyncio
async def test_get_symbol_leverage_does_not_cache_transient_failure_as_one() -> None:
    gateway = BinanceFuturesGateway(Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret"))
    calls = 0

    async def fake_signed_request(method: str, path: str, params: dict[str, str] | None = None, *, use_papi: bool = False):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary papi failure")
        return [{"symbol": "BTCUSDT", "leverage": 25}]

    gateway._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        first = await gateway.get_symbol_leverage("BTCUSDT")
        second = await gateway.get_symbol_leverage("BTCUSDT")
    finally:
        await gateway.close()

    assert first == 1
    assert second == 25
    assert calls == 2


@pytest.mark.asyncio
async def test_get_account_overview_uses_unified_account_only() -> None:
    gateway = BinanceFuturesGateway(Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret"))
    calls: list[str] = []

    async def fake_papi() -> dict[str, str]:
        calls.append("papi")
        return {"status": "ok", "source": "papi"}

    async def fake_fapi() -> dict[str, str]:
        calls.append("fapi")
        return {"status": "ok", "source": "fapi"}

    gateway._get_account_overview_from_papi = fake_papi  # type: ignore[method-assign]
    gateway._get_account_overview_from_fapi = fake_fapi  # type: ignore[method-assign]
    try:
        payload = await gateway.get_account_overview()
    finally:
        await gateway.close()

    assert payload["source"] == "papi"
    assert calls == ["papi"]

@pytest.mark.asyncio
async def test_get_account_overview_includes_mark_and_liquidation_prices() -> None:
    gateway = BinanceFuturesGateway(Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret"))

    async def fake_signed_request(method: str, path: str, params: dict[str, str] | None = None, *, use_papi: bool = False):
        if path == "/papi/v1/account":
            return {
                "accountEquity": "1500.5",
                "accountInitialMargin": "210.2",
                "totalAvailableBalance": "1001.1",
            }
        if path == "/papi/v1/um/positionRisk":
            return [
                {
                    "symbol": "BTCUSDT",
                    "positionSide": "LONG",
                    "positionAmt": "0.02",
                    "entryPrice": "80000",
                    "markPrice": "80500",
                    "unRealizedProfit": "10.0",
                    "notional": "1610",
                    "leverage": "10",
                    "liquidationPrice": "70000",
                }
            ]
        raise AssertionError(path)

    gateway._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        payload = await gateway.get_account_overview()
    finally:
        await gateway.close()

    assert payload["source"] == "papi"
    assert payload["positions"][0]["mark_price"] == Decimal("80500")
    assert payload["positions"][0]["liquidation_price"] == Decimal("70000")


@pytest.mark.asyncio
async def test_gateway_exposes_cached_order_and_stream_health() -> None:
    gateway = BinanceFuturesGateway(Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret"))
    gateway._order_cache["123"] = ExchangeOrder(
        symbol="BTCUSDT",
        order_id="123",
        client_order_id="123",
        side=OrderSide.BUY,
        position_side=PositionSide.LONG,
        type="LIMIT",
        price=Decimal("100"),
        orig_qty=Decimal("1"),
        executed_qty=Decimal("0.4"),
        status=ExchangeOrderStatus.PARTIALLY_FILLED,
        update_time=datetime.now(UTC),
    )
    gateway._user_stream_last_activity = gateway._monotonic()
    gateway._user_stream_connected = True
    gateway._user_stream_task = asyncio.create_task(asyncio.sleep(1))
    try:
        cached = gateway.get_cached_order("BTCUSDT", "123")
        healthy = gateway.is_order_stream_healthy()
    finally:
        await gateway.close()

    assert cached is not None
    assert cached.order_id == "123"
    assert healthy is True


@pytest.mark.asyncio
async def test_gateway_treats_new_user_stream_as_healthy_before_first_event() -> None:
    gateway = BinanceFuturesGateway(Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret"))
    started = asyncio.Event()

    async def fake_start_listen_key() -> str:
        return "listen-key"

    async def fake_run_user_stream() -> None:
        gateway._user_stream_connected = True
        started.set()
        await asyncio.sleep(10)

    gateway._start_listen_key = fake_start_listen_key  # type: ignore[method-assign]
    gateway._run_user_stream = fake_run_user_stream  # type: ignore[method-assign]
    try:
        await gateway._ensure_user_stream()
        await asyncio.wait_for(started.wait(), timeout=1)
        healthy = gateway.is_order_stream_healthy()
    finally:
        await gateway.close()

    assert healthy is True


@pytest.mark.asyncio
async def test_gateway_keeps_connected_but_quiet_user_stream_healthy() -> None:
    gateway = BinanceFuturesGateway(Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret"))
    gateway._user_stream_last_activity = gateway._monotonic() - 30
    gateway._user_stream_connected = True
    gateway._user_stream_task = asyncio.create_task(asyncio.sleep(1))
    try:
        healthy = gateway.is_order_stream_healthy()
    finally:
        await gateway.close()

    assert healthy is True


@pytest.mark.asyncio
async def test_refresh_quote_bypasses_stale_stream_cache() -> None:
    gateway = BinanceFuturesGateway(Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret"))
    gateway._quote_cache["BTCUSDT"] = Quote(
        symbol="BTCUSDT",
        bid_price=Decimal("100"),
        ask_price=Decimal("101"),
        event_time=datetime(2020, 1, 1, tzinfo=UTC),
    )
    calls: list[tuple[str, str, dict[str, str]]] = []

    async def fake_public_request(method: str, path: str, params: dict[str, str]):
        calls.append((method, path, params))
        return {"symbol": "BTCUSDT", "bidPrice": "110", "askPrice": "111"}

    gateway._public_request = fake_public_request  # type: ignore[method-assign]
    try:
        quote = await gateway.refresh_quote("BTCUSDT")
        cached = await gateway.get_quote("BTCUSDT")
    finally:
        await gateway.close()

    assert calls == [("GET", "/fapi/v1/ticker/bookTicker", {"symbol": "BTCUSDT"})]
    assert quote.bid_price == Decimal("110")
    assert quote.ask_price == Decimal("111")
    assert cached.bid_price == Decimal("110")


@pytest.mark.asyncio
async def test_get_open_orders_strict_raises_instead_of_returning_stale_cache() -> None:
    gateway = BinanceFuturesGateway(Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret"))
    gateway._open_orders_cache["BTCUSDT"] = (
        gateway._monotonic(),
        [{"symbol": "BTCUSDT", "orderId": 123, "clientOrderId": "cached-order"}],
    )

    async def fake_signed_request(method: str, path: str, params: dict[str, str]):
        raise RuntimeError("open orders unavailable")

    gateway._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        with pytest.raises(RuntimeError, match="open orders unavailable"):
            await gateway.get_open_orders_strict("BTCUSDT")
    finally:
        await gateway.close()


@pytest.mark.asyncio
async def test_gateway_treats_reconnecting_user_stream_as_unhealthy() -> None:
    gateway = BinanceFuturesGateway(Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret"))
    gateway._user_stream_last_activity = gateway._monotonic()
    gateway._user_stream_connected = False
    gateway._user_stream_task = asyncio.create_task(asyncio.sleep(1))
    try:
        healthy = gateway.is_order_stream_healthy()
    finally:
        await gateway.close()

    assert healthy is False


def _portfolio_margin_payload(path: str):
    if path == "/papi/v1/account":
        return {
            "accountStatus": "NORMAL",
            "accountEquity": "100000",
            "totalAvailableBalance": "50000",
        }
    if path == "/papi/v1/um/positionSide/dual":
        return {"dualSidePosition": True}
    if path == "/papi/v1/um/symbolConfig":
        return [{"symbol": "ETHUSDC", "leverage": 100, "maxNotionalValue": "1500000"}]
    if path == "/papi/v1/um/leverageBracket":
        return [
            {
                "symbol": "ETHUSDC",
                "notionalCoef": "1",
                "brackets": [
                    {"bracket": 1, "initialLeverage": 125, "notionalFloor": "0", "notionalCap": "1000000"},
                    {"bracket": 2, "initialLeverage": 50, "notionalFloor": "1000000", "notionalCap": "2000000"},
                ],
            }
        ]
    if path == "/papi/v1/um/positionRisk":
        return []
    if path == "/papi/v1/um/openOrders":
        return []
    if path == "/papi/v1/um/commissionRate":
        return {"makerCommissionRate": "0.0002", "takerCommissionRate": "0"}
    raise AssertionError(f"unexpected path {path}")


@pytest.mark.asyncio
async def test_portfolio_margin_precheck_uses_only_read_endpoints() -> None:
    gateway = BinanceFuturesGateway(
        Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret")
    )
    calls: list[tuple[str, str, dict | None, bool]] = []

    async def fake_signed_request(method, path, params=None, *, use_papi=False):
        calls.append((method, path, params, use_papi))
        return _portfolio_margin_payload(path)

    gateway._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        result = await gateway.get_portfolio_margin_precheck("ETHUSDC", 100, Decimal("500000"))
    finally:
        await gateway.close()

    assert result["hedge_mode"] is True
    assert result["projected_symbol_exposure"] == Decimal("500000")
    assert result["selected_bracket"]["max_allowed_leverage"] >= 100
    assert result["current_symbol_max_notional_value"] == Decimal("1500000")
    assert set(result["component_observed_at"]) == {
        "account",
        "positions",
        "open_orders",
        "symbol_config",
        "leverage_bracket",
        "commission_rate",
    }
    assert all(
        observed_at.tzinfo is UTC
        for observed_at in result["component_observed_at"].values()
    )
    assert all(method == "GET" and use_papi for method, _, _, use_papi in calls)
    assert not any("/order" in path.lower() or path.endswith("/leverage") for _, path, _, _ in calls)


@pytest.mark.asyncio
async def test_portfolio_margin_account_timestamp_uses_oldest_dependency() -> None:
    gateway = BinanceFuturesGateway(
        Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret")
    )
    hedge_returned_at: datetime | None = None

    async def fake_signed_request(method, path, params=None, *, use_papi=False):
        nonlocal hedge_returned_at
        if path == "/papi/v1/um/positionSide/dual":
            await asyncio.sleep(0.03)
            hedge_returned_at = datetime.now(UTC)
        return _portfolio_margin_payload(path)

    gateway._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        result = await gateway.get_portfolio_margin_precheck("ETHUSDC", 100, Decimal("500000"))
    finally:
        await gateway.close()

    assert hedge_returned_at is not None
    assert result["component_observed_at"]["account"] < hedge_returned_at


@pytest.mark.asyncio
async def test_commission_rates_are_account_specific() -> None:
    gateway = BinanceFuturesGateway(
        Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret")
    )

    async def fake_signed_request(method, path, params=None, *, use_papi=False):
        assert (method, path, use_papi) == ("GET", "/papi/v1/um/commissionRate", True)
        return _portfolio_margin_payload(path)

    gateway._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        rates = await gateway.get_commission_rates("ETHUSDC")
    finally:
        await gateway.close()
    assert rates == {"maker": Decimal("0.0002"), "taker": Decimal("0")}


@pytest.mark.asyncio
async def test_notional_coefficient_is_applied_exactly_once_to_every_bracket() -> None:
    gateway = BinanceFuturesGateway(
        Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret")
    )

    async def fake_signed_request(method, path, params=None, *, use_papi=False):
        payload = _portfolio_margin_payload(path)
        if path == "/papi/v1/um/leverageBracket":
            payload[0]["notionalCoef"] = "0.5"
        return payload

    gateway._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        result = await gateway.get_portfolio_margin_precheck("ETHUSDC", 100, Decimal("400000"))
    finally:
        await gateway.close()
    first = result["brackets"][0]
    assert first["notional_cap"] == Decimal("1000000")
    assert first["notional_coef"] == Decimal("0.5")
    assert first["effective_cap"] == Decimal("500000")
    assert result["selected_bracket"]["bracket"] == 1


@pytest.mark.asyncio
async def test_depth_cache_refreshes_when_fewer_than_requested_levels() -> None:
    gateway = BinanceFuturesGateway(Settings(_env_file=None))
    gateway._depth_cache["ETHUSDC"] = {
        "bids": [{"price": Decimal("100"), "qty": Decimal("1")}] * 5,
        "asks": [{"price": Decimal("101"), "qty": Decimal("1")}] * 5,
    }
    calls: list[dict] = []

    async def no_stream(_symbol):
        return None

    async def fake_public_request(method, path, params=None):
        calls.append(dict(params or {}))
        return {
            "lastUpdateId": 1,
            "bids": [[str(100 - index), "1"] for index in range(20)],
            "asks": [[str(101 + index), "1"] for index in range(20)],
        }

    gateway._ensure_depth_stream = no_stream  # type: ignore[method-assign]
    gateway._public_request = fake_public_request  # type: ignore[method-assign]
    try:
        order_book = await gateway.get_order_book("ETHUSDC", limit=20)
    finally:
        await gateway.close()
    assert len(order_book["bids"]) == len(order_book["asks"]) == 20
    assert calls == [{"symbol": "ETHUSDC", "limit": 20}]


@pytest.mark.asyncio
async def test_gateway_reports_weight_and_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    observations: list[RateLimitObservation] = []
    gateway = BinanceFuturesGateway(
        Settings(_env_file=None, binance_api_key="test-key", binance_api_secret="test-secret"),
        rate_limit_observer=observations.append,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            request=request,
            headers={"X-MBX-USED-WEIGHT-1M": "5900", "Retry-After": "2"},
            json={"code": -1003, "msg": "Too many requests"},
        )

    await gateway._papi_client.aclose()
    gateway._papi_client = httpx.AsyncClient(
        base_url="https://papi.binance.com",
        headers={"X-MBX-APIKEY": "test-key"},
        transport=httpx.MockTransport(handler),
    )

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr("paired_opener.classified_gateway.asyncio.sleep", no_sleep)
    classified = ClassifiedExchangeGateway(gateway)
    try:
        with pytest.raises(TradingError) as exc_info:
            await classified.get_commission_rates("ETHUSDC")
    finally:
        await classified.close()
    assert observations[-1].used_weight_by_window["1m"] == 5900
    assert observations[-1].retry_after_seconds == Decimal("2")
    assert exc_info.value.context["retry_after_seconds"] == Decimal("2")
