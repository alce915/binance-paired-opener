from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from paired_opener import api as api_module
from paired_opener.account_runtime import KanglongReadOnlyRuntimeManager
from paired_opener.kanglong.read_only_gateway import KanglongReadOnlyGateway


class RecordingGateway:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get_symbol_rules(self, symbol: str):
        self.calls.append("get_symbol_rules")
        return {"symbol": symbol}

    def single_attempt(self):
        return self

    async def get_portfolio_margin_precheck(self, symbol: str, leverage: int, notional: Decimal):
        self.calls.append("get_portfolio_margin_precheck")
        return {"symbol": symbol, "leverage": leverage, "notional": notional}

    async def place_market_order(self, **_kwargs):
        self.calls.append("place_market_order")


@pytest.mark.asyncio
async def test_kanglong_read_only_gateway_exposes_only_allowlisted_reads() -> None:
    delegate = RecordingGateway()
    gateway = KanglongReadOnlyGateway(delegate)

    assert await gateway.get_symbol_rules("ETHUSDC") == {"symbol": "ETHUSDC"}
    snapshot = await gateway.get_portfolio_margin_precheck("ETHUSDC", 1, Decimal("0"))
    assert snapshot["leverage"] == 1
    assert not hasattr(gateway, "place_market_order")
    assert not hasattr(gateway, "cancel_order")
    assert not hasattr(gateway, "ensure_leverage")
    assert not hasattr(gateway, "ensure_hedge_mode")
    assert delegate.calls == ["get_symbol_rules", "get_portfolio_margin_precheck"]


def test_kanglong_runtime_manager_never_returns_full_gateway() -> None:
    delegate = RecordingGateway()
    full_runtime = SimpleNamespace(account=SimpleNamespace(account_id="a1"), gateway=delegate)
    manager = KanglongReadOnlyRuntimeManager(SimpleNamespace(current=lambda _account_id=None: full_runtime))

    runtime = manager.current("a1")

    assert isinstance(runtime.gateway, KanglongReadOnlyGateway)
    assert runtime.gateway is not delegate
    assert not hasattr(runtime.gateway, "place_market_order")
    assert runtime.gateway._rate_limit_observer_owner is delegate


def test_real_session_mutations_require_local_management_token(monkeypatch: pytest.MonkeyPatch) -> None:
    token = "local-management-token-for-real-session-test"
    monkeypatch.setattr(api_module.app.state, "local_management_token", token, raising=False)
    client = TestClient(
        api_module.app,
        base_url="http://127.0.0.1:8000",
        client=("127.0.0.1", 50123),
    )
    valid_headers = {
        "Host": "127.0.0.1:8000",
        "Origin": "http://127.0.0.1:8000",
        "X-Local-Management-Token": token,
    }
    invalid_headers = {**valid_headers, "X-Local-Management-Token": "wrong"}
    open_request = {
        "symbol": "ETHUSDC",
        "trend_bias": "long",
        "leverage": 100,
        "round_count": 1,
        "round_qty": "0.01",
    }

    protected = (
        ("/sessions/open", open_request),
        ("/sessions/close", {"symbol": "ETHUSDC", "trend_bias": "long", "close_qty": "0.01", "round_count": 1}),
        ("/sessions/single-open", {"symbol": "ETHUSDC", "open_mode": "regular", "selected_position_side": "LONG", "open_qty": "0.01", "leverage": 100, "round_count": 1}),
        ("/sessions/single-close", {"symbol": "ETHUSDC", "close_mode": "regular", "selected_position_side": "LONG", "close_qty": "0.01", "round_count": 1}),
        ("/sessions/session-1/pause", {}),
        ("/sessions/session-1/resume", {}),
        ("/sessions/session-1/abort", {}),
    )
    for path, payload in protected:
        response = client.post(path, json=payload, headers=invalid_headers)
        assert response.status_code == 403, path
        assert response.json()["detail"]["code"] == "local_management_forbidden"


def test_production_config_mutations_require_local_management_token(monkeypatch: pytest.MonkeyPatch) -> None:
    token = "local-management-token-for-config-test"
    monkeypatch.setattr(api_module.app.state, "local_management_token", token, raising=False)
    client = TestClient(
        api_module.app,
        base_url="http://127.0.0.1:8000",
        client=("127.0.0.1", 50124),
    )
    headers = {
        "Host": "127.0.0.1:8000",
        "Origin": "http://127.0.0.1:8000",
        "X-Local-Management-Token": "wrong",
    }

    responses = (
        client.put("/config/whitelist", json={"symbols": ["ETHUSDC"]}, headers=headers),
        client.post("/config/accounts/select", json={"account_id": "a1"}, headers=headers),
    )

    for response in responses:
        assert response.status_code == 403
        assert response.json()["detail"]["code"] == "local_management_forbidden"
