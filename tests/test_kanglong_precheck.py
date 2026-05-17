from __future__ import annotations

from decimal import Decimal

import pytest

from paired_opener.account_runtime import AccountRuntimeManager
from paired_opener.config import AccountConfig, Settings
from paired_opener.domain import PositionSide
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import KanglongAccountSnapshot, KanglongPositionSnapshot, KanglongRunStatus
from paired_opener.kanglong.precheck import choose_selected_side, run_static_precheck
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


def position(symbol: str, side: PositionSide, qty: str, pnl: str) -> KanglongPositionSnapshot:
    return KanglongPositionSnapshot(
        symbol=symbol,
        side=side,
        qty=Decimal(qty),
        entry_price=Decimal("3000"),
        mark_price=Decimal("3100"),
        unrealized_pnl=Decimal(pnl),
    )


def snapshot(account_id: str, long_qty: str, short_qty: str, long_pnl: str, short_pnl: str) -> KanglongAccountSnapshot:
    return KanglongAccountSnapshot(
        account_id=account_id,
        account_name=account_id,
        available_balance=Decimal("10000"),
        equity=Decimal("10000"),
        margin=Decimal("0"),
        leverage=75,
        positions={
            PositionSide.LONG: position("ETHUSDC", PositionSide.LONG, long_qty, long_pnl),
            PositionSide.SHORT: position("ETHUSDC", PositionSide.SHORT, short_qty, short_pnl),
        },
        open_orders=[],
        snapshot_version=f"{account_id}-v1",
    )


def test_choose_selected_side_prefers_more_profitable_side() -> None:
    selected, preview = choose_selected_side(
        [snapshot("sub1", "1", "1", "10", "30")],
        manual_side=None,
        config=KanglongSymbolConfig(),
    )

    assert selected == PositionSide.SHORT
    assert preview["preview_side"] == "LONG"


def test_precheck_blocks_when_main_account_is_not_flat() -> None:
    result = run_static_precheck(
        main=snapshot("main", "0.01", "0", "0", "0"),
        subaccounts=[snapshot("sub1", "1", "1", "10", "0")],
        symbol="ETHUSDC",
        manual_side=None,
        config=KanglongSymbolConfig(),
    )

    assert result.status == KanglongRunStatus.BLOCKED_MAIN_NOT_FLAT
    assert result.reason_code == "blocked_main_not_flat"


def test_precheck_blocks_when_initial_subaccount_is_unbalanced() -> None:
    result = run_static_precheck(
        main=snapshot("main", "0", "0", "0", "0"),
        subaccounts=[snapshot("sub1", "1.5", "1", "10", "0")],
        symbol="ETHUSDC",
        manual_side=None,
        config=KanglongSymbolConfig(),
    )

    assert result.status == KanglongRunStatus.BLOCKED_INITIAL_SUBACCOUNT_UNBALANCED
