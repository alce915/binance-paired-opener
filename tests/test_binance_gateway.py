from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from paired_opener.binance import BinanceFuturesGateway
from paired_opener.config import Settings
from paired_opener.domain import ExchangeOrder, ExchangeOrderStatus, OrderSide, PositionSide


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




