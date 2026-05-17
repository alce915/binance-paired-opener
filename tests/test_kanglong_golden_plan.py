from __future__ import annotations

from decimal import Decimal

from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import KanglongPlanningAccount
from paired_opener.kanglong.planner import build_kanglong_plan
from paired_opener.kanglong.reporter import summarize_costs
from paired_opener.kanglong.simulator import simulate_group


def planning_account(account_id: str, closeable: str, profit: str, capacity: str = "10") -> KanglongPlanningAccount:
    return KanglongPlanningAccount(
        account_id=account_id,
        closeable_qty=Decimal(closeable),
        unrealized_profit=Decimal(profit),
        receiver_capacity_qty=Decimal(capacity),
        risk_buffer=Decimal("1"),
    )


def test_golden_plan_group_order_and_quantities_are_stable() -> None:
    plan = build_kanglong_plan(
        run_id="run-golden",
        symbol="ETHUSDC",
        selected_side=PositionSide.LONG,
        main_account_id="main",
        first_donor_account_id="sub1",
        planned_release_qty=Decimal("1.0"),
        accounts=[
            planning_account("sub1", "1.0", "100"),
            planning_account("sub2", "0.4", "200"),
            planning_account("sub3", "0.8", "100"),
        ],
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
    )

    assert [(group.from_account_id, group.to_account_id, group.target_qty) for group in plan.groups[:3]] == [
        ("sub1", "main", Decimal("1.0")),
        ("sub2", "sub1", Decimal("0.4")),
        ("sub3", "sub1", Decimal("0.6")),
    ]
    assert plan.groups[0].round_qtys == [Decimal("0.25")] * 4


def test_cost_summary_uses_signed_pnl_and_non_negative_losses() -> None:
    plan = build_kanglong_plan(
        run_id="run-cost",
        symbol="ETHUSDC",
        selected_side=PositionSide.LONG,
        main_account_id="main",
        first_donor_account_id="sub1",
        planned_release_qty=Decimal("0.5"),
        accounts=[planning_account("sub1", "0.5", "100")],
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.5")),
    )
    result = simulate_group(
        run_id="run-cost",
        group=plan.groups[0],
        rules=SymbolRules("ETHUSDC", Decimal("0.01"), Decimal("0.001"), Decimal("0.001"), Decimal("5"), 125),
        close_price=Decimal("3100.00"),
        open_price=Decimal("3100.50"),
        fee_rate=Decimal("0.0005"),
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.5")),
    )

    summary = summarize_costs(result.events, result.residual_ledger)

    assert Decimal(summary["transfer_price_diff_pnl"]) < Decimal("0")
    assert Decimal(summary["transfer_price_diff_loss"]) > Decimal("0")
    assert Decimal(summary["total_cost"]) >= Decimal("0")
