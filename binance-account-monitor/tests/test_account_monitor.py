from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from monitor_app.account_monitor import AccountMonitorController
from monitor_app.config import MainAccountConfig, MonitorAccountConfig, Settings


class FakeMonitorGateway:
    def __init__(self, account: MonitorAccountConfig) -> None:
        self.account = account

    async def get_unified_account_snapshot(self, *, history_window_days: int = 7, income_limit: int = 100, interest_limit: int = 100) -> dict:
        if self.account.account_id == "group_a.sub2":
            raise RuntimeError("sub2 unavailable")
        return {
            "status": "ok",
            "source": "papi",
            "account_id": self.account.account_id,
            "account_name": self.account.display_name,
            "main_account_id": self.account.main_account_id,
            "main_account_name": self.account.main_account_name,
            "child_account_id": self.account.child_account_id,
            "child_account_name": self.account.child_account_name,
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
            "positions": [],
            "assets": [],
            "income_summary": {"window_days": history_window_days, "records": 1, "total_income": Decimal("7.5"), "by_type": {}, "by_asset": {}},
            "interest_summary": {"window_days": history_window_days, "records": 1, "margin_interest_total": Decimal("1.2"), "negative_balance_interest_total": Decimal("0"), "total_interest": Decimal("1.2")},
            "section_errors": {},
        }

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_account_monitor_controller_groups_and_filters_accounts() -> None:
    settings = Settings(_env_file=None, monitor_refresh_interval_ms=50, monitor_history_window_days=3)
    account1 = MonitorAccountConfig(account_id="group_a.sub1", child_account_id="sub1", child_account_name="Sub One", main_account_id="group_a", main_account_name="Group A", api_key="k1", api_secret="s1")
    account2 = MonitorAccountConfig(account_id="group_a.sub2", child_account_id="sub2", child_account_name="Sub Two", main_account_id="group_a", main_account_name="Group A", api_key="k2", api_secret="s2")
    account3 = MonitorAccountConfig(account_id="group_b.sub1", child_account_id="sub1", child_account_name="Sub Three", main_account_id="group_b", main_account_name="Group B", api_key="k3", api_secret="s3")
    settings.monitor_accounts = {account.account_id: account for account in (account1, account2, account3)}
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account1, account2)),
        "group_b": MainAccountConfig(main_id="group_b", name="Group B", children=(account3,)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda account: FakeMonitorGateway(account))

    queue = await controller.subscribe(["group_a.sub1", "group_b.sub1"])
    try:
        initial = await asyncio.wait_for(queue.get(), timeout=1)
        refreshed = await asyncio.wait_for(queue.get(), timeout=1)
        current_summary = controller.current_summary(["group_a.sub1", "group_b.sub1"])
        current_groups = controller.current_groups(["group_a.sub1", "group_b.sub1"])
        current_accounts = controller.current_accounts(["group_a.sub1", "group_b.sub1"])
    finally:
        controller.unsubscribe(queue)
        await controller.close()

    assert initial["event"] == "monitor_snapshot"
    assert refreshed["data"]["summary"]["account_count"] == 2
    assert len(refreshed["data"]["groups"]) == 2
    assert refreshed["data"]["groups"][0]["main_account_id"] == "group_a"
    assert refreshed["data"]["summary"]["equity"] == "2400"
    assert refreshed["data"]["message_code"] == "runtime.monitor_all_healthy"
    assert refreshed["data"]["message_params"] == {}
    assert current_summary["message_code"] == "runtime.monitor_all_healthy"
    assert current_summary["message_params"] == {}
    assert current_groups["message_code"] == "runtime.monitor_all_healthy"
    assert current_groups["message_params"] == {}
    assert current_accounts["message_code"] == "runtime.monitor_all_healthy"
    assert current_accounts["message_params"] == {}


@pytest.mark.asyncio
async def test_account_monitor_controller_uses_safe_message_for_failed_account_snapshot() -> None:
    settings = Settings(_env_file=None, monitor_refresh_interval_ms=50, monitor_history_window_days=3)
    account1 = MonitorAccountConfig(account_id="group_a.sub1", child_account_id="sub1", child_account_name="Sub One", main_account_id="group_a", main_account_name="Group A", api_key="k1", api_secret="s1")
    account2 = MonitorAccountConfig(account_id="group_a.sub2", child_account_id="sub2", child_account_name="Sub Two", main_account_id="group_a", main_account_name="Group A", api_key="k2", api_secret="s2")
    settings.monitor_accounts = {account.account_id: account for account in (account1, account2)}
    settings.monitor_main_accounts = {
        "group_a": MainAccountConfig(main_id="group_a", name="Group A", children=(account1, account2)),
    }
    controller = AccountMonitorController(settings, gateway_factory=lambda account: FakeMonitorGateway(account))

    queue = await controller.subscribe()
    try:
        await asyncio.wait_for(queue.get(), timeout=1)
        refreshed = await asyncio.wait_for(queue.get(), timeout=1)
    finally:
        controller.unsubscribe(queue)
        await controller.close()

    failed_account = next(item for item in refreshed["data"]["accounts"] if item["account_id"] == "group_a.sub2")

    assert refreshed["data"]["status"] == "partial"
    assert refreshed["data"]["message_code"] == "runtime.monitor_partial_failed"
    assert failed_account["status"] == "error"
    assert failed_account["message_code"] == "runtime.monitor_account_refresh_failed"
    assert "sub2 unavailable" not in failed_account["message"]
