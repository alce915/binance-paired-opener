from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path

import pytest

from paired_opener.account_monitor import AccountMonitorController
from paired_opener.binance import BinanceFuturesGateway
from paired_opener.config import AccountConfig, Settings


class FakeMonitorGateway:
    def __init__(self, account: AccountConfig) -> None:
        self.account = account
        self.closed = False

    async def get_unified_account_snapshot(self, *, history_window_days: int = 7, income_limit: int = 100, interest_limit: int = 100) -> dict:
        if self.account.account_id == "beta":
            raise RuntimeError("beta unavailable")
        return {
            "status": "ok",
            "source": "papi",
            "account_id": self.account.account_id,
            "account_name": self.account.name,
            "account_status": "NORMAL",
            "updated_at": "2026-03-25T00:00:00+00:00",
            "message": "ok",
            "totals": {
                "equity": Decimal("1200"),
                "margin": Decimal("200"),
                "available_balance": Decimal("950"),
                "unrealized_pnl": Decimal("18"),
                "total_income": Decimal("7.5"),
                "total_interest": Decimal("1.2"),
            },
            "positions": [
                {
                    "symbol": "BTCUSDT",
                    "position_side": "LONG",
                    "qty": Decimal("0.01"),
                    "entry_price": Decimal("80000"),
                    "mark_price": Decimal("80500"),
                    "unrealized_pnl": Decimal("5"),
                    "notional": Decimal("805"),
                    "leverage": 10,
                }
            ],
            "assets": [],
            "income_summary": {
                "window_days": history_window_days,
                "records": 1,
                "total_income": Decimal("7.5"),
                "by_type": {"FUNDING_FEE": Decimal("7.5")},
                "by_asset": {"USDT": Decimal("7.5")},
            },
            "interest_summary": {
                "window_days": history_window_days,
                "records": 1,
                "margin_interest_total": Decimal("1.2"),
                "negative_balance_interest_total": Decimal("0"),
                "total_interest": Decimal("1.2"),
            },
            "section_errors": {},
        }

    async def close(self) -> None:
        self.closed = True


def test_load_accounts_from_json_file(tmp_path: Path) -> None:
    accounts_file = tmp_path / "accounts.json"
    accounts_file.write_text(
        json.dumps(
            {
                "accounts": [
                    {
                        "account_id": "alpha",
                        "name": "Alpha",
                        "api_key": "k1",
                        "api_secret": "s1",
                    },
                    {
                        "account_id": "beta",
                        "name": "Beta",
                        "api_key": "k2",
                        "api_secret": "s2",
                        "use_testnet": True,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        _env_file=None,
        binance_accounts_file=accounts_file,
        active_account_file=tmp_path / "active.json",
    )

    settings.load_accounts(include_accounts_file=True)

    assert sorted(settings.accounts.keys()) == ["alpha", "beta"]
    assert settings.accounts["beta"].use_testnet is True
    assert settings.active_account_id == "alpha"


@pytest.mark.asyncio
async def test_get_unified_account_snapshot_aggregates_income_and_interest() -> None:
    settings = Settings(_env_file=None)
    gateway = BinanceFuturesGateway(
        settings,
        AccountConfig(account_id="alpha", name="Alpha", api_key="k", api_secret="s"),
    )

    async def fake_signed_request(method: str, path: str, params: dict | None = None, *, use_papi: bool = False):
        if path == "/papi/v1/account":
            return {
                "accountStatus": "NORMAL",
                "accountEquity": "1500.5",
                "accountInitialMargin": "210.2",
                "totalAvailableBalance": "1001.1",
            }
        if path == "/papi/v1/um/account":
            return {
                "assets": [
                    {
                        "asset": "USDT",
                        "crossWalletBalance": "1200",
                        "crossUnPnl": "12.5",
                        "availableBalance": "1001.1",
                        "initialMargin": "210.2",
                        "maintMargin": "20",
                        "marginBalance": "1212.5",
                        "maxWithdrawAmount": "1000",
                    }
                ],
                "positions": [
                    {
                        "symbol": "BTCUSDT",
                        "positionSide": "LONG",
                        "positionAmt": "0.02",
                        "entryPrice": "80000",
                        "markPrice": "80500",
                        "unrealizedProfit": "10.0",
                        "notional": "1610",
                        "leverage": "10",
                        "liquidationPrice": "70000",
                    },
                    {
                        "symbol": "ETHUSDT",
                        "positionSide": "SHORT",
                        "positionAmt": "-0.5",
                        "entryPrice": "2000",
                        "markPrice": "1995",
                        "unrealizedProfit": "2.5",
                        "notional": "997.5",
                        "leverage": "5",
                        "liquidationPrice": "2500",
                    },
                ],
            }
        if path == "/papi/v1/um/income":
            return [
                {"incomeType": "FUNDING_FEE", "asset": "USDT", "income": "7.2"},
                {"incomeType": "REALIZED_PNL", "asset": "USDT", "income": "-1.2"},
            ]
        if path == "/papi/v1/margin/marginInterestHistory":
            return {"rows": [{"interest": "0.3"}, {"interest": "0.2"}]}
        if path == "/papi/v1/portfolio/interest-history":
            return [{"interest": "0.4"}]
        raise AssertionError(path)

    gateway._signed_request = fake_signed_request  # type: ignore[method-assign]
    try:
        snapshot = await gateway.get_unified_account_snapshot(history_window_days=5)
    finally:
        await gateway.close()

    assert snapshot["status"] == "ok"
    assert snapshot["totals"]["equity"] == Decimal("1500.5")
    assert snapshot["totals"]["unrealized_pnl"] == Decimal("12.5")
    assert snapshot["totals"]["total_income"] == Decimal("6.0")
    assert snapshot["totals"]["total_interest"] == Decimal("0.9")
    assert len(snapshot["positions"]) == 2
    assert snapshot["income_summary"]["by_type"]["FUNDING_FEE"] == Decimal("7.2")


@pytest.mark.asyncio
async def test_account_monitor_controller_filters_accounts_for_subscriber(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        monitor_refresh_interval_ms=50,
        monitor_history_window_days=3,
        active_account_file=tmp_path / "active.json",
    )
    settings.accounts = {
        "alpha": AccountConfig(account_id="alpha", name="Alpha", api_key="k1", api_secret="s1"),
        "beta": AccountConfig(account_id="beta", name="Beta", api_key="k2", api_secret="s2"),
    }
    settings.active_account_id = "alpha"
    controller = AccountMonitorController(settings, gateway_factory=lambda account: FakeMonitorGateway(account))

    queue = await controller.subscribe(["alpha"])
    try:
        initial = await asyncio.wait_for(queue.get(), timeout=1)
        assert initial["event"] == "accounts_snapshot"
        refreshed = await asyncio.wait_for(queue.get(), timeout=1)
    finally:
        controller.unsubscribe(queue)
        await controller.close()

    assert initial["data"]["status"] == "idle"
    assert refreshed["data"]["status"] == "ok"
    assert refreshed["data"]["summary"]["account_count"] == 1
    assert refreshed["data"]["summary"]["equity"] == "1200"
    assert refreshed["data"]["accounts"][0]["account_id"] == "alpha"

