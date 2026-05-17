from __future__ import annotations

from decimal import Decimal

from paired_opener.domain import PositionSide
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import KanglongBatchDebtBuffer, KanglongPlanningAccount
import pytest

from paired_opener.kanglong.planner import KanglongGroupRoundLimitExceeded, build_kanglong_plan


def account(account_id: str, closeable: str, profit: str, capacity: str = "10") -> KanglongPlanningAccount:
    return KanglongPlanningAccount(
        account_id=account_id,
        closeable_qty=Decimal(closeable),
        unrealized_profit=Decimal(profit),
        receiver_capacity_qty=Decimal(capacity),
        risk_buffer=Decimal("1"),
    )


def test_planner_first_group_is_first_donor_to_main() -> None:
    plan = build_kanglong_plan(
        run_id="run-1",
        symbol="ETHUSDC",
        selected_side=PositionSide.LONG,
        main_account_id="main",
        first_donor_account_id="sub1",
        planned_release_qty=Decimal("1.0"),
        accounts=[account("sub1", "1.0", "100"), account("sub2", "0.5", "50")],
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
    )

    assert plan.groups[0].from_account_id == "sub1"
    assert plan.groups[0].to_account_id == "main"
    assert plan.groups[0].target_qty == Decimal("1.0")
    assert plan.groups[0].round_qtys == [Decimal("0.25")] * 4


def test_planner_scores_next_donor_by_profit_before_transfer_size() -> None:
    plan = build_kanglong_plan(
        run_id="run-1",
        symbol="ETHUSDC",
        selected_side=PositionSide.LONG,
        main_account_id="main",
        first_donor_account_id="sub1",
        planned_release_qty=Decimal("1.0"),
        accounts=[
            account("sub1", "1.0", "100"),
            account("sub2", "0.4", "200"),
            account("sub3", "0.8", "100"),
        ],
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
    )

    assert plan.groups[1].from_account_id == "sub2"
    assert plan.groups[1].to_account_id == "sub1"


def test_planner_segments_donor_batch_by_fifo_receiver_capacity() -> None:
    plan = build_kanglong_plan(
        run_id="run-1",
        symbol="ETHUSDC",
        selected_side=PositionSide.LONG,
        main_account_id="main",
        first_donor_account_id="sub1",
        planned_release_qty=Decimal("1.0"),
        accounts=[
            account("sub1", "1.0", "100", capacity="10"),
            account("sub2", "0.4", "90", capacity="0.3"),
            account("sub3", "1.0", "80", capacity="10"),
        ],
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
    )

    batch_groups = [group for group in plan.groups if group.batch_id is not None]

    assert [group.to_account_id for group in batch_groups] == ["sub1", "sub2"]
    assert [group.target_qty for group in batch_groups] == [Decimal("0.6"), Decimal("0.3")]


def test_planner_records_batch_debt_buffer_for_batch_groups() -> None:
    plan = build_kanglong_plan(
        run_id="run-1",
        symbol="ETHUSDC",
        selected_side=PositionSide.LONG,
        main_account_id="main",
        first_donor_account_id="sub1",
        planned_release_qty=Decimal("1.0"),
        accounts=[
            account("sub1", "1.0", "100", capacity="10"),
            account("sub2", "0.4", "90", capacity="0.3"),
            account("sub3", "1.0", "80", capacity="10"),
        ],
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
    )

    assert plan.batch_debt_buffers
    assert isinstance(plan.batch_debt_buffers[0], KanglongBatchDebtBuffer)
    assert plan.batch_debt_buffers[0].donor_account_id == "sub3"
    assert plan.batch_debt_buffers[0].matched_qty == Decimal("0.9")
    assert plan.batch_debt_buffers[0].repair_status == "open"


def test_planner_raises_when_group_requires_more_than_configured_rounds() -> None:
    with pytest.raises(KanglongGroupRoundLimitExceeded) as exc:
        build_kanglong_plan(
            run_id="run-1",
            symbol="ETHUSDC",
            selected_side=PositionSide.LONG,
            main_account_id="main",
            first_donor_account_id="sub1",
            planned_release_qty=Decimal("1.0"),
            accounts=[account("sub1", "1.0", "100")],
            config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25"), max_rounds_per_group=3),
        )

    assert exc.value.group_index == 1
    assert exc.value.required_rounds == 4
