from __future__ import annotations

from decimal import Decimal

import pytest

from monitor_app.binance import BinanceMonitorGateway
from monitor_app.config import MonitorAccountConfig, Settings


@pytest.mark.asyncio
async def test_get_unified_account_snapshot_aggregates_income_and_interest() -> None:
    settings = Settings(_env_file=None)
    gateway = BinanceMonitorGateway(
        settings,
        MonitorAccountConfig(
            account_id="group_a.sub1",
            child_account_id="sub1",
            child_account_name="Sub One",
            main_account_id="group_a",
            main_account_name="Group A",
            api_key="k",
            api_secret="s",
        ),
    )

    async def fake_signed_request(path: str, params: dict | None = None):
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
    assert snapshot["account_id"] == "group_a.sub1"
    assert snapshot["totals"]["equity"] == Decimal("1500.5")
    assert snapshot["totals"]["unrealized_pnl"] == Decimal("12.5")
    assert snapshot["totals"]["total_income"] == Decimal("6.0")
    assert snapshot["totals"]["total_interest"] == Decimal("0.9")
    assert len(snapshot["positions"]) == 2