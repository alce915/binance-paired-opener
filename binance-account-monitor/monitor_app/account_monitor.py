from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Callable, Protocol

from app_i18n.runtime import CATALOG_VERSION, CONTRACT_VERSION, format_copy
from monitor_app.binance import BinanceMonitorGateway
from monitor_app.config import MonitorAccountConfig, Settings


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
        gateway_factory: Callable[[MonitorAccountConfig], UnifiedAccountGateway] | None = None,
    ) -> None:
        self._settings = settings
        self._gateway_factory = gateway_factory or (lambda account: BinanceMonitorGateway(settings, account))
        self._gateways: dict[str, UnifiedAccountGateway] = {}
        self._subscriptions: dict[asyncio.Queue[dict[str, Any]], set[str] | None] = {}
        self._lock = asyncio.Lock()
        self._refresh_task: asyncio.Task[None] | None = None
        self._last_payload = self._build_idle_payload("idle", "runtime.monitor_waiting_connection")

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

    def _message_fields(
        self,
        message_code: str,
        message_params: dict[str, Any] | None = None,
        *,
        fallback_message: str | None = None,
    ) -> dict[str, Any]:
        params = dict(message_params or {})
        return {
            "contract_version": CONTRACT_VERSION,
            "catalog_version": CATALOG_VERSION,
            "message_code": message_code,
            "message_params": params,
            "message": fallback_message or format_copy(message_code, params),
        }

    def current_snapshot(self, account_ids: list[str] | None = None) -> dict[str, Any]:
        normalized_ids = self._normalize_account_ids(account_ids)
        return self._normalize(self._filter_payload(self._last_payload, normalized_ids))

    def _public_payload_fields(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "contract_version": payload["contract_version"],
            "catalog_version": payload["catalog_version"],
            "status": payload["status"],
            "updated_at": payload["updated_at"],
            "message_code": payload["message_code"],
            "message_params": payload["message_params"],
            "message": payload["message"],
            "service": payload["service"],
        }

    def current_summary(self, account_ids: list[str] | None = None) -> dict[str, Any]:
        payload = self.current_snapshot(account_ids)
        return {
            **self._public_payload_fields(payload),
            "summary": payload["summary"],
        }

    def current_groups(self, account_ids: list[str] | None = None) -> dict[str, Any]:
        payload = self.current_snapshot(account_ids)
        return {
            **self._public_payload_fields(payload),
            "summary": payload["summary"],
            "groups": payload["groups"],
        }

    def current_accounts(self, account_ids: list[str] | None = None) -> dict[str, Any]:
        payload = self.current_snapshot(account_ids)
        return {
            **self._public_payload_fields(payload),
            "summary": payload["summary"],
            "accounts": payload["accounts"],
        }

    async def subscribe(self, account_ids: list[str] | None = None) -> asyncio.Queue[dict[str, Any]]:
        normalized_ids = self._normalize_account_ids(account_ids)
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=20)
        async with self._lock:
            self._subscriptions[queue] = normalized_ids
            if self._refresh_task is None:
                self._refresh_task = asyncio.create_task(self._run_loop())
        await queue.put(
            {
                "event": "monitor_snapshot",
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
            except Exception:
                self._last_payload = self._build_idle_payload("error", "runtime.monitor_refresh_failed")
                await self._broadcast(self._last_payload)
                await asyncio.sleep(interval_seconds)

    async def _refresh_once(self) -> None:
        accounts = list(self._settings.monitor_accounts.values())
        if not accounts:
            payload = self._build_idle_payload("error", "runtime.monitor_accounts_missing")
        else:
            snapshots = await asyncio.gather(*(self._fetch_account_snapshot(account) for account in accounts))
            payload = self._build_payload(snapshots)
        self._last_payload = payload
        await self._broadcast(payload)

    async def _fetch_account_snapshot(self, account: MonitorAccountConfig) -> dict[str, Any]:
        gateway = self._gateways.get(account.account_id)
        if gateway is None:
            gateway = self._gateway_factory(account)
            self._gateways[account.account_id] = gateway
        try:
            snapshot = await gateway.get_unified_account_snapshot(
                history_window_days=self._settings.monitor_history_window_days,
            )
            snapshot.setdefault("account_id", account.account_id)
            snapshot.setdefault("account_name", account.display_name)
            snapshot.setdefault("main_account_id", account.main_account_id)
            snapshot.setdefault("main_account_name", account.main_account_name)
            snapshot.setdefault("child_account_id", account.child_account_id)
            snapshot.setdefault("child_account_name", account.child_account_name)
            snapshot.setdefault("contract_version", CONTRACT_VERSION)
            snapshot.setdefault("catalog_version", CATALOG_VERSION)
            snapshot.setdefault("message_code", "runtime.account_snapshot_updated")
            snapshot.setdefault("message_params", {})
            snapshot.setdefault("message", format_copy("runtime.account_snapshot_updated"))
            return snapshot
        except Exception:
            return {
                "contract_version": CONTRACT_VERSION,
                "catalog_version": CATALOG_VERSION,
                "status": "error",
                "source": "papi",
                "account_id": account.account_id,
                "account_name": account.display_name,
                "main_account_id": account.main_account_id,
                "main_account_name": account.main_account_name,
                "child_account_id": account.child_account_id,
                "child_account_name": account.child_account_name,
                "updated_at": datetime.now(UTC),
                **self._message_fields("runtime.monitor_account_refresh_failed"),
                "totals": self._empty_totals(),
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
                        "event": "monitor_snapshot",
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
        return self._compose_payload(filtered_accounts)

    def _build_payload(self, accounts: list[dict[str, Any]]) -> dict[str, Any]:
        return self._compose_payload(accounts)

    def _compose_payload(self, accounts: list[dict[str, Any]]) -> dict[str, Any]:
        groups = self._build_groups(accounts)
        summary = self._summarize_accounts(accounts)
        status, message_code, message = self._status_and_message(summary)
        return {
            "contract_version": CONTRACT_VERSION,
            "catalog_version": CATALOG_VERSION,
            "status": status,
            "updated_at": self._utc_now(),
            "message_code": message_code,
            "message_params": {},
            "message": message,
            "service": {
                "refresh_interval_ms": self._settings.monitor_refresh_interval_ms,
                "history_window_days": self._settings.monitor_history_window_days,
                "account_ids": sorted(self._settings.monitor_accounts.keys()),
                "main_account_ids": sorted(self._settings.monitor_main_accounts.keys()),
            },
            "summary": summary,
            "groups": groups,
            "accounts": accounts,
        }

    def _build_idle_payload(
        self,
        status: str,
        message_code: str,
        message_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "contract_version": CONTRACT_VERSION,
            "catalog_version": CATALOG_VERSION,
            "status": status,
            "updated_at": self._utc_now(),
            **self._message_fields(message_code, message_params),
            "service": {
                "refresh_interval_ms": self._settings.monitor_refresh_interval_ms,
                "history_window_days": self._settings.monitor_history_window_days,
                "account_ids": sorted(self._settings.monitor_accounts.keys()),
                "main_account_ids": sorted(self._settings.monitor_main_accounts.keys()),
            },
            "summary": self._summarize_accounts([]),
            "groups": [],
            "accounts": [],
        }

    def _build_groups(self, accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for account in accounts:
            main_account_id = str(account.get("main_account_id") or "unknown")
            grouped.setdefault(main_account_id, []).append(account)
        groups: list[dict[str, Any]] = []
        for main_account_id in sorted(grouped):
            group_accounts = sorted(grouped[main_account_id], key=lambda item: str(item.get("account_id", "")))
            groups.append(
                {
                    "main_account_id": main_account_id,
                    "main_account_name": group_accounts[0].get("main_account_name", main_account_id),
                    "summary": self._summarize_accounts(group_accounts),
                    "accounts": group_accounts,
                }
            )
        return groups

    def _status_and_message(self, summary: dict[str, Any]) -> tuple[str, str, str]:
        if summary["account_count"] == 0:
            return "idle", "runtime.monitor_no_accounts", format_copy("runtime.monitor_no_accounts")
        if summary["error_count"] == 0:
            return "ok", "runtime.monitor_all_healthy", format_copy("runtime.monitor_all_healthy")
        if summary["success_count"] == 0:
            return "error", "runtime.monitor_all_failed", format_copy("runtime.monitor_all_failed")
        return "partial", "runtime.monitor_partial_failed", format_copy("runtime.monitor_partial_failed")

    def _summarize_accounts(self, accounts: list[dict[str, Any]]) -> dict[str, Any]:
        totals = self._empty_totals()
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

    def _empty_totals(self) -> dict[str, Decimal]:
        return {
            "equity": Decimal("0"),
            "margin": Decimal("0"),
            "available_balance": Decimal("0"),
            "unrealized_pnl": Decimal("0"),
            "total_income": Decimal("0"),
            "total_interest": Decimal("0"),
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
