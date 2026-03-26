from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Callable, Protocol

from paired_opener.binance import BinanceFuturesGateway
from paired_opener.config import AccountConfig, Settings


class UnifiedAccountGateway(Protocol):
    async def get_unified_account_snapshot(
        self,
        *,
        history_window_days: int = 7,
        income_limit: int = 100,
        interest_limit: int = 100,
    ) -> dict[str, Any]: ...

    async def close(self) -> None: ...


class AccountMonitorController:
    def __init__(
        self,
        settings: Settings,
        gateway_factory: Callable[[AccountConfig], UnifiedAccountGateway] | None = None,
    ) -> None:
        self._settings = settings
        self._gateway_factory = gateway_factory or (lambda account: BinanceFuturesGateway(settings, account))
        self._gateways: dict[str, UnifiedAccountGateway] = {}
        self._subscriptions: dict[asyncio.Queue[dict[str, Any]], set[str] | None] = {}
        self._lock = asyncio.Lock()
        self._refresh_task: asyncio.Task[None] | None = None
        self._last_payload = self._build_idle_payload("idle", "等待监控连接")

    def _utc_now(self) -> str:
        return datetime.now(UTC).isoformat()

    def _normalize(self, payload: Any) -> Any:
        if isinstance(payload, Decimal):
            return str(payload)
        if isinstance(payload, datetime):
            return payload.isoformat()
        if isinstance(payload, dict):
            return {key: self._normalize(value) for key, value in payload.items()}
        if isinstance(payload, list):
            return [self._normalize(value) for value in payload]
        return payload

    def current_snapshot(self, account_ids: list[str] | None = None) -> dict[str, Any]:
        normalized_ids = self._normalize_account_ids(account_ids)
        return self._normalize(self._filter_payload(self._last_payload, normalized_ids))

    async def subscribe(self, account_ids: list[str] | None = None) -> asyncio.Queue[dict[str, Any]]:
        normalized_ids = self._normalize_account_ids(account_ids)
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=20)
        async with self._lock:
            self._subscriptions[queue] = normalized_ids
            if self._refresh_task is None:
                self._refresh_task = asyncio.create_task(self._run_loop())
        await queue.put(
            {
                "event": "accounts_snapshot",
                "data": self._normalize(self._filter_payload(self._last_payload, normalized_ids)),
            }
        )
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscriptions.pop(queue, None)
        if not self._subscriptions and self._refresh_task is not None:
            self._refresh_task.cancel()
            self._refresh_task = None

    async def close(self) -> None:
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None
        for gateway in self._gateways.values():
            await gateway.close()
        self._gateways.clear()

    async def _run_loop(self) -> None:
        interval_seconds = max(self._settings.monitor_refresh_interval_ms / 1000, 1.0)
        while True:
            try:
                await self._refresh_once()
                await asyncio.sleep(interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_payload = self._build_idle_payload("error", str(exc))
                await self._broadcast(self._last_payload)
                await asyncio.sleep(interval_seconds)

    async def _refresh_once(self) -> None:
        accounts = list(self._settings.accounts.values())
        if not accounts:
            payload = self._build_idle_payload("error", "未配置可监控的币安账户")
        else:
            snapshots = await asyncio.gather(*(self._fetch_account_snapshot(account) for account in accounts))
            payload = self._build_payload(snapshots)
        self._last_payload = payload
        await self._broadcast(payload)

    async def _fetch_account_snapshot(self, account: AccountConfig) -> dict[str, Any]:
        gateway = self._gateways.get(account.account_id)
        if gateway is None:
            gateway = self._gateway_factory(account)
            self._gateways[account.account_id] = gateway
        try:
            snapshot = await gateway.get_unified_account_snapshot(
                history_window_days=self._settings.monitor_history_window_days,
            )
            snapshot.setdefault("account_id", account.account_id)
            snapshot.setdefault("account_name", account.name)
            snapshot.setdefault("message", "账户监控数据已更新")
            return snapshot
        except Exception as exc:
            return {
                "status": "error",
                "source": "papi",
                "account_id": account.account_id,
                "account_name": account.name,
                "updated_at": datetime.now(UTC),
                "message": str(exc),
                "totals": {
                    "equity": Decimal("0"),
                    "margin": Decimal("0"),
                    "available_balance": Decimal("0"),
                    "unrealized_pnl": Decimal("0"),
                    "total_income": Decimal("0"),
                    "total_interest": Decimal("0"),
                },
                "positions": [],
                "assets": [],
                "income_summary": {
                    "window_days": self._settings.monitor_history_window_days,
                    "records": 0,
                    "total_income": Decimal("0"),
                    "by_type": {},
                    "by_asset": {},
                },
                "interest_summary": {
                    "window_days": self._settings.monitor_history_window_days,
                    "records": 0,
                    "margin_interest_total": Decimal("0"),
                    "negative_balance_interest_total": Decimal("0"),
                    "total_interest": Decimal("0"),
                },
                "section_errors": {},
            }

    async def _broadcast(self, payload: dict[str, Any]) -> None:
        stale: list[asyncio.Queue[dict[str, Any]]] = []
        for queue, account_ids in list(self._subscriptions.items()):
            try:
                queue.put_nowait(
                    {
                        "event": "accounts_snapshot",
                        "data": self._normalize(self._filter_payload(payload, account_ids)),
                    }
                )
            except asyncio.QueueFull:
                stale.append(queue)
        for queue in stale:
            self._subscriptions.pop(queue, None)
        if not self._subscriptions and self._refresh_task is not None:
            self._refresh_task.cancel()
            self._refresh_task = None

    def _filter_payload(self, payload: dict[str, Any], account_ids: set[str] | None) -> dict[str, Any]:
        if not account_ids:
            return payload
        filtered_accounts = [
            account
            for account in payload.get("accounts", [])
            if str(account.get("account_id", "")).lower() in account_ids
        ]
        summary = self._summarize_accounts(filtered_accounts)
        status, message = self._status_and_message(summary)
        return {
            **payload,
            "status": status,
            "message": message,
            "accounts": filtered_accounts,
            "requested_account_ids": sorted(account_ids),
            "summary": summary,
        }

    def _build_payload(self, accounts: list[dict[str, Any]]) -> dict[str, Any]:
        summary = self._summarize_accounts(accounts)
        status, message = self._status_and_message(summary)
        return {
            "status": status,
            "updated_at": self._utc_now(),
            "message": message,
            "service": {
                "refresh_interval_ms": self._settings.monitor_refresh_interval_ms,
                "history_window_days": self._settings.monitor_history_window_days,
                "account_ids": sorted(self._settings.accounts.keys()),
            },
            "summary": summary,
            "accounts": accounts,
        }

    def _build_idle_payload(self, status: str, message: str) -> dict[str, Any]:
        return {
            "status": status,
            "updated_at": self._utc_now(),
            "message": message,
            "service": {
                "refresh_interval_ms": self._settings.monitor_refresh_interval_ms,
                "history_window_days": self._settings.monitor_history_window_days,
                "account_ids": sorted(self._settings.accounts.keys()),
            },
            "summary": self._summarize_accounts([]),
            "accounts": [],
        }

    def _status_and_message(self, summary: dict[str, Any]) -> tuple[str, str]:
        if summary["account_count"] == 0:
            return "idle", "暂无账户数据"
        if summary["error_count"] == 0:
            return "ok", "全部账户监控正常"
        if summary["success_count"] == 0:
            return "error", "全部账户获取失败"
        return "partial", "部分账户获取失败"

    def _summarize_accounts(self, accounts: list[dict[str, Any]]) -> dict[str, Any]:
        totals = {
            "equity": Decimal("0"),
            "margin": Decimal("0"),
            "available_balance": Decimal("0"),
            "unrealized_pnl": Decimal("0"),
            "total_income": Decimal("0"),
            "total_interest": Decimal("0"),
        }
        success_count = 0
        error_count = 0
        for account in accounts:
            if account.get("status") == "ok":
                success_count += 1
                account_totals = account.get("totals") or {}
                for key in totals:
                    totals[key] += Decimal(str(account_totals.get(key) or "0"))
            else:
                error_count += 1
        return {
            "account_count": len(accounts),
            "success_count": success_count,
            "error_count": error_count,
            **totals,
        }

    def _normalize_account_ids(self, account_ids: list[str] | None) -> set[str] | None:
        if not account_ids:
            return None
        normalized = {
            account_id.strip().lower()
            for account_id in account_ids
            if account_id and account_id.strip()
        }
        return normalized or None
