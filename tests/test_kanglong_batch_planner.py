from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.batch_models import KanglongBatchAccountPlan, KanglongBatchPlan
from paired_opener.kanglong.batch_planner import KanglongBatchPlanner, UnsafeBatchRefresh


def _rules(step_size: str = "0.001") -> SymbolRules:
    return SymbolRules(
        symbol="ETHUSDC",
        tick_size=Decimal("0.01"),
        step_size=Decimal(step_size),
        min_qty=Decimal(step_size),
        min_notional=Decimal("5"),
        max_leverage=125,
    )


def _open_plan(account_ids=("a1", "a2", "a3"), run_id="open-1") -> KanglongBatchPlan:
    return KanglongBatchPlanner().plan_open(
        account_ids=account_ids,
        credential_revision="revision-1",
        symbol="ETHUSDC",
        preferred_side=PositionSide.LONG,
        leverage=100,
        per_leg_notional=Decimal("250000"),
        reference_price=Decimal("2000"),
        rules=_rules(),
        run_id=run_id,
    )


def test_open_plan_preserves_account_order_and_targets_each_leg() -> None:
    plan = _open_plan(("a3", "a1", "a2"))
    assert [item.account_id for item in plan.accounts] == ["a3", "a1", "a2"]
    assert all(item.target_long_qty == Decimal("125") for item in plan.accounts)
    assert all(item.target_short_qty == Decimal("125") for item in plan.accounts)
    assert plan.lock_scopes == tuple(sorted(plan.lock_scopes))


def test_plan_payload_round_trip_preserves_decimal_and_hash() -> None:
    original = _open_plan()
    restored = KanglongBatchPlan.from_payload(original.to_payload())
    assert restored == original
    assert restored.plan_version == original.plan_version


def _source_run(long_qty: str, short_qty: str | None = None) -> dict:
    return {
        "run_id": "open-1",
        "symbol": "ETHUSDC",
        "preferred_side": "LONG",
        "requested_leverage": 100,
        "credential_revision": "revision-1",
        "accounts": [
            {
                "account_id": "a1",
                "target_long_qty": long_qty,
                "target_short_qty": short_qty if short_qty is not None else long_qty,
                "source_long_remaining_qty": long_qty,
                "source_short_remaining_qty": short_qty if short_qty is not None else long_qty,
                "source_ledger_hash": "ledger-open-1",
                "source_checkpoint_id": 7,
                "reference_mid_price": "2000",
            }
        ],
    }


def test_close_plan_cannot_exceed_source_batch_remaining_qty() -> None:
    plan = KanglongBatchPlanner().plan_close(source_open_run=_source_run("12.345"))
    assert plan.accounts[0].target_long_qty == Decimal("12.345")
    assert plan.accounts[0].target_short_qty == Decimal("12.345")


def test_close_plan_preserves_unequal_source_dust_without_expanding_either_leg() -> None:
    plan = KanglongBatchPlanner().plan_close(source_open_run=_source_run("12.345", "12.3445"))
    assert plan.accounts[0].target_long_qty == Decimal("12.345")
    assert plan.accounts[0].target_short_qty == Decimal("12.3445")
    assert plan.open_capacity_check_applied is False
    assert plan.blocked is False
    assert "kanglong:source-open-run:open-1" in plan.lock_scopes


def _refreshed(account: KanglongBatchAccountPlan) -> KanglongBatchAccountPlan:
    return replace(
        account,
        maker_fee_rate=Decimal("0.0003"),
        reference_mid_price=Decimal("2100"),
        capacity_snapshot_id=f"refreshed-{account.account_id}",
    )


def test_refresh_plan_keeps_completed_prefix_immutable() -> None:
    original = _open_plan()
    refreshed = KanglongBatchPlanner().refresh_pending_suffix(
        stored_plan=original,
        account_statuses={"a1": "completed", "a2": "blocked_precheck", "a3": "pending"},
        refreshed_accounts={
            "a2": _refreshed(original.accounts[1]),
            "a3": _refreshed(original.accounts[2]),
        },
        credential_revision="revision-2",
    )
    assert refreshed.plan_version != original.plan_version
    assert refreshed.accounts[0] == original.accounts[0]
    assert refreshed.accounts[1:] != original.accounts[1:]
    assert refreshed.completed_prefix_length == 1


def test_refresh_plan_rejects_unbalanced_active_account() -> None:
    original = _open_plan(("a1", "a2"))
    with pytest.raises(UnsafeBatchRefresh):
        KanglongBatchPlanner().refresh_pending_suffix(
            stored_plan=original,
            account_statuses={"a1": "completed", "a2": "second_leg"},
            refreshed_accounts={"a2": _refreshed(original.accounts[1])},
            credential_revision="revision-1",
        )
