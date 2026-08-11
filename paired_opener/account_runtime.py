from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from types import MappingProxyType
from typing import Callable, Mapping, Protocol

from paired_opener.account_credentials import (
    AccountCredentialStore,
    PreparedCredentialWrite,
    VerifiedCredentialCandidate,
)
from paired_opener.binance import BinanceFuturesGateway
from paired_opener.classified_gateway import ClassifiedExchangeGateway
from paired_opener.config import AccountConfig, Settings
from paired_opener.engine import PairedClosingEngine, PairedOpeningEngine
from paired_opener.market_stream import MarketStreamController
from paired_opener.service import OpenSessionService
from paired_opener.simulation import SimulationService
from paired_opener.storage import SqliteRepository


class AccountCredentialsNotConfigured(RuntimeError):
    def __init__(self, account_id: str | None = None) -> None:
        self.account_id = account_id
        super().__init__("account credentials are not configured")


class AccountCredentialsLockedByActiveBatch(RuntimeError):
    pass


class AccountMutationGuard(Protocol):
    def ensure_mutation_allowed(self) -> None: ...


@dataclass(slots=True)
class RuntimeBundle:
    account: AccountConfig
    gateway: ClassifiedExchangeGateway
    engine: PairedOpeningEngine
    close_engine: PairedClosingEngine
    service: OpenSessionService
    market: MarketStreamController
    simulation: SimulationService
    is_closed: bool = False

    async def verify_read_only_access(self) -> None:
        await self.gateway.get_account_overview()

    async def aclose(self) -> None:
        if self.is_closed:
            return
        self.is_closed = True
        await asyncio.gather(
            self.market.disconnect(),
            self.service.close(),
            self.gateway.close(),
            return_exceptions=True,
        )


@dataclass(frozen=True, slots=True)
class PreparedRuntimeSet:
    accounts: tuple[AccountConfig, ...]
    runtimes: Mapping[str, object]


class RepositoryAccountMutationGuard:
    def __init__(self, repository: SqliteRepository) -> None:
        self._repository = repository

    def ensure_mutation_allowed(self) -> None:
        if (
            self._repository.has_active_sessions()
            or self._repository.get_active_kanglong_run() is not None
            or self._repository.has_active_kanglong_batch_run()
        ):
            raise AccountCredentialsLockedByActiveBatch("account credentials are locked by active work")


class AccountRuntimeManager:
    def __init__(
        self,
        settings: Settings,
        repository: SqliteRepository,
        *,
        runtime_factory: Callable[[AccountConfig], object] | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._mutation_lock = asyncio.Lock()
        self._runtime_factory = runtime_factory or self._build_runtime
        self._runtimes: dict[str, object] = {
            account.account_id: self._runtime_factory(account)
            for account in settings.accounts.values()
        }
        enabled_ids = [account.account_id for account in settings.accounts.values() if account.enabled]
        if settings.active_account_id not in enabled_ids:
            settings.active_account_id = enabled_ids[0] if enabled_ids else ""

    def _build_runtime(self, account: AccountConfig) -> RuntimeBundle:
        gateway = ClassifiedExchangeGateway(BinanceFuturesGateway(self._settings, account))
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
        market = MarketStreamController(gateway, self._settings, account.account_id, account.name)
        simulation = SimulationService(gateway, self._repository, publisher=market.publish)
        return RuntimeBundle(
            account=account,
            gateway=gateway,
            engine=engine,
            close_engine=close_engine,
            service=service,
            market=market,
            simulation=simulation,
        )

    def current(self, account_id: str | None = None):
        normalized = (account_id or self._settings.active_account_id).strip().lower()
        runtime = self._runtimes.get(normalized)
        account = self._settings.accounts.get(normalized)
        if runtime is None or account is None or not account.enabled:
            raise AccountCredentialsNotConfigured(normalized or None)
        return runtime

    async def initialize_startup_recovery(self) -> list[dict[str, object]]:
        evaluated: list[dict[str, object]] = []
        for runtime in tuple(self._runtimes.values()):
            if not runtime.account.enabled:
                continue
            evaluated.extend(await runtime.service.evaluate_startup_recovery())
        return evaluated

    def list_accounts(self) -> list[dict[str, object]]:
        active_id = self._settings.active_account_id
        return [
            {
                "id": account.account_id,
                "name": account.name,
                "is_active": account.account_id == active_id,
            }
            for account in self._settings.accounts.values()
            if account.enabled
        ]

    def get_accounts_by_ids(self, account_ids: list[str]) -> list[AccountConfig]:
        accounts: list[AccountConfig] = []
        for raw_id in account_ids:
            account_id = raw_id.strip().lower()
            account = self._settings.accounts.get(account_id)
            if account is None or not account.enabled:
                raise ValueError(f"Unknown account {raw_id}")
            accounts.append(account)
        return accounts

    def build_temporary_gateway(self, account_id: str) -> ClassifiedExchangeGateway:
        normalized = account_id.strip().lower()
        account = self._settings.accounts.get(normalized)
        if account is None or not account.enabled:
            raise ValueError(f"Unknown account {account_id}")
        return ClassifiedExchangeGateway(BinanceFuturesGateway(self._settings, account))

    async def switch_account(self, account_id: str) -> dict[str, object]:
        normalized = account_id.strip().lower()
        async with self._mutation_lock:
            target = self._runtimes.get(normalized)
            if target is None:
                raise ValueError(f"Unknown account {account_id}")
            try:
                current = self.current()
            except AccountCredentialsNotConfigured:
                current = None
            if current is not None and current.account.account_id != normalized and current.service.has_active_sessions():
                raise ValueError("当前账户存在活动真实开单会话，禁止切换账户")
            self._settings.persist_active_account(normalized)
            return {"id": target.account.account_id, "name": target.account.name, "is_active": True}

    async def prepare_accounts(self, accounts: list[AccountConfig]) -> PreparedRuntimeSet:
        candidates: dict[str, object] = {}
        try:
            for account in accounts:
                runtime = self._runtime_factory(account)
                candidates[account.account_id] = runtime
                await runtime.verify_read_only_access()
        except BaseException:
            await self._close_non_cancellable(tuple(candidates.values()))
            raise
        return PreparedRuntimeSet(
            accounts=tuple(accounts),
            runtimes=MappingProxyType(dict(candidates)),
        )

    def commit_accounts(self, prepared: PreparedRuntimeSet) -> tuple[object, ...]:
        account_ids = [account.account_id for account in prepared.accounts]
        if len(account_ids) != len(set(account_ids)):
            raise ValueError("duplicate account_id")
        expected_runtime_ids = {account.account_id for account in prepared.accounts}
        if set(prepared.runtimes) != expected_runtime_ids:
            raise ValueError("prepared runtime set does not match accounts")
        next_runtimes = dict(prepared.runtimes)
        next_accounts = {account.account_id: account for account in prepared.accounts}
        previous = self._runtimes
        current_active = self._settings.active_account_id
        enabled_ids = [account.account_id for account in prepared.accounts if account.enabled]
        next_active = current_active if current_active in enabled_ids else (enabled_ids[0] if enabled_ids else "")
        self._runtimes = next_runtimes
        self._settings.accounts = next_accounts
        self._settings.active_account_id = next_active
        return tuple(
            runtime
            for account_id, runtime in previous.items()
            if next_runtimes.get(account_id) is not runtime
        )

    async def close_replaced(self, replaced: tuple[object, ...]) -> None:
        if not replaced:
            return
        results = await asyncio.gather(*(runtime.aclose() for runtime in replaced), return_exceptions=True)
        logger = logging.getLogger(__name__)
        for result in results:
            if isinstance(result, BaseException):
                logger.warning(
                    "account_runtime_close_failed",
                    extra={"error_type": type(result).__name__},
                )

    async def _close_non_cancellable(self, runtimes: tuple[object, ...]) -> None:
        cleanup = asyncio.create_task(self.close_replaced(runtimes))
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise
    async def aclose(self) -> None:
        async with self._mutation_lock:
            current = tuple(self._runtimes.values())
            self._runtimes = {}
        await self.close_replaced(current)

    async def close(self) -> None:
        await self.aclose()


class AccountCredentialCommitCoordinator:
    def __init__(
        self,
        store: AccountCredentialStore,
        runtime_manager: AccountRuntimeManager,
        mutation_guard: AccountMutationGuard,
    ) -> None:
        self._store = store
        self._runtime_manager = runtime_manager
        self._mutation_guard = mutation_guard
        self._mutation_lock = asyncio.Lock()

    async def commit(
        self,
        *,
        candidate: VerifiedCredentialCandidate,
        prepared_write: PreparedCredentialWrite,
        prepared_runtime: PreparedRuntimeSet,
    ) -> None:
        try:
            async with self._mutation_lock:
                self._mutation_guard.ensure_mutation_allowed()
                self._store.commit(
                    prepared_write,
                    expected_revision=candidate.credential_revision,
                )
                replaced = self._runtime_manager.commit_accounts(prepared_runtime)
        except BaseException:
            prepared_write.discard()
            await self._await_cleanup(tuple(prepared_runtime.runtimes.values()))
            raise
        await self._await_cleanup(replaced)

    async def _await_cleanup(self, runtimes: tuple[object, ...]) -> None:
        cleanup = asyncio.create_task(self._runtime_manager.close_replaced(runtimes))
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise
