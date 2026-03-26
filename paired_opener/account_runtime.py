from __future__ import annotations

import asyncio
from dataclasses import dataclass

from paired_opener.binance import BinanceFuturesGateway
from paired_opener.config import AccountConfig, Settings
from paired_opener.engine import PairedClosingEngine, PairedOpeningEngine
from paired_opener.market_stream import MarketStreamController
from paired_opener.service import OpenSessionService
from paired_opener.storage import SqliteRepository


@dataclass(slots=True)
class RuntimeBundle:
    account: AccountConfig
    gateway: BinanceFuturesGateway
    engine: PairedOpeningEngine
    close_engine: PairedClosingEngine
    service: OpenSessionService
    market: MarketStreamController


class AccountRuntimeManager:
    def __init__(self, settings: Settings, repository: SqliteRepository) -> None:
        self._settings = settings
        self._repository = repository
        self._lock = asyncio.Lock()
        self._runtime = self._build_runtime(settings.active_account)

    def _build_runtime(self, account: AccountConfig) -> RuntimeBundle:
        gateway = BinanceFuturesGateway(self._settings, account)
        engine = PairedOpeningEngine(gateway, self._repository)
        close_engine = PairedClosingEngine(gateway, self._repository)
        service = OpenSessionService(
            self._settings,
            self._repository,
            gateway,
            engine,
            close_engine,
            account_id=account.account_id,
            account_name=account.name,
        )
        market = MarketStreamController(gateway, account.account_id, account.name)
        return RuntimeBundle(account=account, gateway=gateway, engine=engine, close_engine=close_engine, service=service, market=market)

    def current(self) -> RuntimeBundle:
        return self._runtime

    def list_accounts(self) -> list[dict[str, object]]:
        active_id = self._settings.active_account_id
        return [
            {
                "id": account.account_id,
                "name": account.name,
                "is_active": account.account_id == active_id,
            }
            for account in self._settings.accounts.values()
        ]

    async def switch_account(self, account_id: str) -> dict[str, object]:
        normalized = account_id.strip().lower()
        async with self._lock:
            current_runtime = self._runtime
            if normalized == current_runtime.account.account_id:
                return {
                    "id": current_runtime.account.account_id,
                    "name": current_runtime.account.name,
                    "is_active": True,
                }
            if current_runtime.service.has_active_sessions():
                raise ValueError("当前账户存在活动真实开单会话，禁止切换账户")
            if normalized not in self._settings.accounts:
                raise ValueError(f"Unknown account {account_id}")
            await current_runtime.market.disconnect()
            await current_runtime.gateway.close()
            self._settings.persist_active_account(normalized)
            next_runtime = self._build_runtime(self._settings.active_account)
            self._runtime = next_runtime
            return {
                "id": next_runtime.account.account_id,
                "name": next_runtime.account.name,
                "is_active": True,
            }

    async def close(self) -> None:
        await self._runtime.market.disconnect()
        await self._runtime.gateway.close()
