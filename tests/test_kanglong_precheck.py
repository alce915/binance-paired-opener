from __future__ import annotations

import pytest

from paired_opener.account_runtime import AccountRuntimeManager
from paired_opener.config import AccountConfig, Settings
from paired_opener.storage import SqliteRepository


@pytest.mark.asyncio
async def test_runtime_manager_exposes_configured_kanglong_accounts(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    settings = Settings(_env_file=None, database_path=tmp_path / "db.sqlite3")
    settings.accounts = {
        "main": AccountConfig(account_id="main", name="主账号", api_key="k", api_secret="s"),
        "sub1": AccountConfig(account_id="sub1", name="子账号1", api_key="k", api_secret="s"),
    }
    settings.active_account_id = "main"
    manager = AccountRuntimeManager(settings, repository)

    try:
        accounts = manager.get_accounts_by_ids(["main", "sub1"])
    finally:
        await manager.close()
        repository.close()

    assert [account.account_id for account in accounts] == ["main", "sub1"]
