from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from paired_opener.account_runtime import AccountRuntimeManager
from paired_opener.account_credentials import AccountCredentialStore, VerifiedCredentialCandidate
from paired_opener.account_runtime import AccountCredentialCommitCoordinator
from paired_opener import config as config_module
from paired_opener.config import Settings
from paired_opener.domain import OpenSession, SessionSpec, SessionStatus, TrendBias
from paired_opener.storage import SqliteRepository


class FakeManagedRuntime:
    def __init__(self, account) -> None:
        self.account = account
        self.is_closed = False
        self.verified = False

    async def verify_read_only_access(self) -> None:
        self.verified = True

    async def aclose(self) -> None:
        self.is_closed = True


def _write_multi_account_env(root: Path) -> None:
    config_dir = root / 'config'
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / 'binance_api.env').write_text(
        '\n'.join(
            [
                'BINANCE_ACCOUNTS=main,sub1',
                'BINANCE_ACCOUNT_MAIN_NAME=主账户',
                'BINANCE_ACCOUNT_MAIN_API_KEY=main-key',
                'BINANCE_ACCOUNT_MAIN_API_SECRET=main-secret',
                'BINANCE_ACCOUNT_MAIN_USE_TESTNET=false',
                'BINANCE_ACCOUNT_SUB1_NAME=子账户1',
                'BINANCE_ACCOUNT_SUB1_API_KEY=sub1-key',
                'BINANCE_ACCOUNT_SUB1_API_SECRET=sub1-secret',
                'BINANCE_ACCOUNT_SUB1_USE_TESTNET=true',
            ]
        ),
        encoding='utf-8',
    )


def _build_settings(root: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setattr(config_module, 'ENV_FILES', (root / 'config' / 'binance_api.env',))
    settings = Settings(
        _env_file=None,
        active_account_file=Path('config/active_account.json'),
        symbol_whitelist_file=Path('config/symbol_whitelist.json'),
        database_path=Path('data/test.db'),
        binance_accounts_file=Path('config/binance_accounts.json'),
    )
    settings.load_accounts()
    return settings


def test_settings_load_accounts_from_env_and_restore_active_account(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_multi_account_env(tmp_path)
    (tmp_path / 'config' / 'active_account.json').write_text('{"account_id": "sub1"}', encoding='utf-8')
    monkeypatch.chdir(tmp_path)

    settings = _build_settings(tmp_path, monkeypatch)

    assert set(settings.accounts) == {'main', 'sub1'}
    assert settings.active_account_id == 'sub1'
    assert settings.active_account.name == '子账户1'
    assert settings.active_account.use_testnet is True


@pytest.mark.asyncio
async def test_runtime_manager_switches_account_and_persists_selection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_multi_account_env(tmp_path)
    monkeypatch.chdir(tmp_path)
    settings = _build_settings(tmp_path, monkeypatch)
    repository = SqliteRepository(tmp_path / 'data' / 'runtime.db')
    manager = AccountRuntimeManager(settings, repository)

    try:
        assert manager.current().account.account_id == 'main'

        payload = await manager.switch_account('sub1')

        assert payload == {'id': 'sub1', 'name': '子账户1', 'is_active': True}
        assert manager.current().account.account_id == 'sub1'
        persisted = json.loads((tmp_path / 'config' / 'active_account.json').read_text(encoding='utf-8'))
        assert persisted['account_id'] == 'sub1'
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_runtime_manager_rejects_switch_when_current_account_has_active_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_multi_account_env(tmp_path)
    monkeypatch.chdir(tmp_path)
    settings = _build_settings(tmp_path, monkeypatch)
    repository = SqliteRepository(tmp_path / 'data' / 'runtime-block.db')
    manager = AccountRuntimeManager(settings, repository)
    session = OpenSession.create(
        SessionSpec(
            symbol='BTCUSDT',
            trend_bias=TrendBias.LONG,
            leverage=10,
            round_count=1,
            round_qty=Decimal('0.01'),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=1,
            market_fallback_attempts=1,
            round_interval_seconds=3,
            created_by='test',
        ),
        account_id='main',
        account_name='主账户',
    )
    repository.create_session(session)
    repository.update_session_status(session.session_id, SessionStatus.RUNNING)

    try:
        with pytest.raises(ValueError, match='活动真实开单会话'):
            await manager.switch_account('sub1')
    finally:
        await manager.close()


def test_repository_filters_sessions_by_account(tmp_path: Path) -> None:
    repository = SqliteRepository(tmp_path / 'data' / 'filter.db')
    session_main = OpenSession.create(
        SessionSpec(
            symbol='BTCUSDT',
            trend_bias=TrendBias.LONG,
            leverage=10,
            round_count=1,
            round_qty=Decimal('0.01'),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=1,
            market_fallback_attempts=1,
            round_interval_seconds=3,
            created_by='test',
        ),
        account_id='main',
        account_name='主账户',
    )
    session_sub = OpenSession.create(
        SessionSpec(
            symbol='ETHUSDT',
            trend_bias=TrendBias.SHORT,
            leverage=10,
            round_count=1,
            round_qty=Decimal('0.02'),
            poll_interval_ms=50,
            order_ttl_ms=3000,
            max_zero_fill_retries=1,
            market_fallback_attempts=1,
            round_interval_seconds=3,
            created_by='test',
        ),
        account_id='sub1',
        account_name='子账户1',
    )
    repository.create_session(session_main)
    repository.create_session(session_sub)

    main_sessions = repository.list_sessions('main')
    sub_sessions = repository.list_sessions('sub1')

    assert [item['session_id'] for item in main_sessions] == [session_main.session_id]
    assert [item['session_id'] for item in sub_sessions] == [session_sub.session_id]
    assert repository.get_session(session_main.session_id, 'sub1') is None

def test_settings_falls_back_to_prefixed_accounts_when_account_list_is_not_id_based(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir = tmp_path / 'config'
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / 'binance_api.env').write_text(
        '\n'.join(
            [
                'BINANCE_ACCOUNTS=主账户的子账户1,主账户的子账户2,主账户的子账户3',
                'BINANCE_ACCOUNT_MAIN_NAME=主账户的子账户1',
                'BINANCE_ACCOUNT_MAIN_API_KEY=main-key',
                'BINANCE_ACCOUNT_MAIN_API_SECRET=main-secret',
                'BINANCE_ACCOUNT_SUB1_NAME=主账户的子账户2',
                'BINANCE_ACCOUNT_SUB1_API_KEY=sub1-key',
                'BINANCE_ACCOUNT_SUB1_API_SECRET=sub1-secret',
                'BINANCE_ACCOUNT_SUB2_NAME=主账户的子账户3',
                'BINANCE_ACCOUNT_SUB2_API_KEY=sub2-key',
                'BINANCE_ACCOUNT_SUB2_API_SECRET=sub2-secret',
            ]
        ),
        encoding='utf-8',
    )
    monkeypatch.chdir(tmp_path)

    settings = _build_settings(tmp_path, monkeypatch)

    assert set(settings.accounts) == {'main', 'sub1', 'sub2'}
    assert settings.accounts['main'].name == '主账户的子账户1'
    assert settings.accounts['sub1'].name == '主账户的子账户2'
    assert settings.accounts['sub2'].name == '主账户的子账户3'



@pytest.mark.asyncio
async def test_runtime_manager_switch_account_does_not_wait_for_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_multi_account_env(tmp_path)
    monkeypatch.chdir(tmp_path)
    settings = _build_settings(tmp_path, monkeypatch)
    repository = SqliteRepository(tmp_path / 'data' / 'runtime-fast-switch.db')
    manager = AccountRuntimeManager(settings, repository)
    release = asyncio.Event()
    current_runtime = manager.current()

    async def slow_disconnect() -> dict[str, object]:
        await release.wait()
        return {}

    async def slow_close_service(*args, **kwargs) -> None:
        await release.wait()

    async def slow_close_gateway() -> None:
        await release.wait()

    current_runtime.market.disconnect = slow_disconnect  # type: ignore[method-assign]
    current_runtime.service.close = slow_close_service  # type: ignore[method-assign]
    current_runtime.gateway.close = slow_close_gateway  # type: ignore[method-assign]

    try:
        payload = await asyncio.wait_for(manager.switch_account('sub1'), timeout=0.05)
        assert payload == {'id': 'sub1', 'name': '子账户1', 'is_active': True}
        assert manager.current().account.account_id == 'sub1'
    finally:
        release.set()
        await manager.close()


@pytest.mark.asyncio
async def test_runtime_swap_closes_replaced_runtime_before_return(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, active_account_file=tmp_path / "active.json")
    settings.accounts = {
        "a1": config_module.AccountConfig("a1", "旧账号", "KEY-123456", "SECRET-123456")
    }
    settings.active_account_id = "a1"
    repository = SqliteRepository(tmp_path / "runtime-swap.db")
    manager = AccountRuntimeManager(settings, repository, runtime_factory=FakeManagedRuntime)
    old = manager.current("a1")
    prepared = await manager.prepare_accounts(
        [config_module.AccountConfig("a1", "新账号", "KEY-654321", "SECRET-654321")]
    )
    replaced = manager.commit_accounts(prepared)
    await manager.close_replaced(replaced)
    assert old.is_closed is True
    assert manager.current("a1").account.name == "新账号"
    await manager.aclose()
    repository.close()


@pytest.mark.asyncio
async def test_runtime_manager_shutdown_closes_current_runtime(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, active_account_file=tmp_path / "active.json")
    settings.accounts = {
        "a1": config_module.AccountConfig("a1", "账号", "KEY-123456", "SECRET-123456")
    }
    settings.active_account_id = "a1"
    repository = SqliteRepository(tmp_path / "runtime-close.db")
    manager = AccountRuntimeManager(settings, repository, runtime_factory=FakeManagedRuntime)
    current = manager.current("a1")
    await manager.aclose()
    assert current.is_closed is True
    repository.close()


@pytest.mark.asyncio
async def test_runtime_prepare_failure_closes_all_candidates(tmp_path: Path) -> None:
    created: list[FakeManagedRuntime] = []

    class RejectingRuntime(FakeManagedRuntime):
        async def verify_read_only_access(self) -> None:
            if self.account.account_id == "a2":
                raise RuntimeError("verification failed")
            await super().verify_read_only_access()

    def factory(account):
        runtime = RejectingRuntime(account)
        created.append(runtime)
        return runtime

    settings = Settings(_env_file=None, active_account_file=tmp_path / "active.json")
    settings.accounts = {}
    settings.active_account_id = ""
    repository = SqliteRepository(tmp_path / "runtime-prepare-failure.db")
    manager = AccountRuntimeManager(settings, repository, runtime_factory=factory)
    accounts = [
        config_module.AccountConfig("a1", "账号 1", "KEY-123456", "SECRET-123456"),
        config_module.AccountConfig("a2", "账号 2", "KEY-234567", "SECRET-234567"),
    ]
    with pytest.raises(RuntimeError, match="verification failed"):
        await manager.prepare_accounts(accounts)
    assert created and all(runtime.is_closed for runtime in created)
    await manager.aclose()
    repository.close()


@pytest.mark.asyncio
async def test_cancelled_commit_finishes_runtime_cleanup_without_file_runtime_split(tmp_path: Path) -> None:
    close_started = asyncio.Event()
    release_close = asyncio.Event()
    created = 0

    class BlockingOldRuntime(FakeManagedRuntime):
        async def aclose(self) -> None:
            close_started.set()
            await release_close.wait()
            await super().aclose()

    def factory(account):
        nonlocal created
        created += 1
        if created == 1:
            return BlockingOldRuntime(account)
        return FakeManagedRuntime(account)

    class FakeProtector:
        def protect(self, value: bytes) -> bytes:
            return b"protected:" + value[::-1]

        def unprotect(self, value: bytes) -> bytes:
            return value[10:][::-1]

    class AllowMutation:
        def ensure_mutation_allowed(self) -> None:
            return None

    old_account = config_module.AccountConfig("a1", "旧账号", "KEY-123456", "SECRET-123456")
    new_account = config_module.AccountConfig("a1", "新账号", "KEY-654321", "SECRET-654321")
    settings = Settings(_env_file=None, active_account_file=tmp_path / "active.json")
    settings.accounts = {"a1": old_account}
    settings.active_account_id = "a1"
    repository = SqliteRepository(tmp_path / "runtime-cancel.db")
    manager = AccountRuntimeManager(settings, repository, runtime_factory=factory)
    old_runtime = manager.current("a1")
    store = AccountCredentialStore(
        tmp_path / "accounts.secure.json",
        FakeProtector(),
        permission_hardener=lambda _path: None,
    )
    initial_write = store.prepare([old_account])
    store.commit(initial_write, expected_revision=store.current_revision())
    revision = store.current_revision()
    prepared_runtime = await manager.prepare_accounts([new_account])
    prepared_write = store.prepare([new_account])
    candidate = VerifiedCredentialCandidate(
        accounts=(new_account,),
        credential_revision=revision,
        content_hash="candidate-hash",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
        preview_token="",
        changes={
            "added_account_ids": [],
            "updated_account_ids": ["a1"],
            "unchanged_account_ids": [],
            "removed_account_ids": [],
        },
    )
    coordinator = AccountCredentialCommitCoordinator(store, manager, AllowMutation())
    task = asyncio.create_task(
        coordinator.commit(
            candidate=candidate,
            prepared_write=prepared_write,
            prepared_runtime=prepared_runtime,
        )
    )
    await asyncio.wait_for(close_started.wait(), timeout=1)
    task.cancel()
    release_close.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert old_runtime.is_closed is True
    assert manager.current("a1").account.name == "新账号"
    assert store.load()[0].name == "新账号"
    await manager.aclose()
    repository.close()


