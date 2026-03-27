from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path

import pytest

from paired_opener.account_runtime import AccountRuntimeManager
from paired_opener import config as config_module
from paired_opener.config import Settings
from paired_opener.domain import OpenSession, SessionSpec, SessionStatus, TrendBias
from paired_opener.storage import SqliteRepository


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


