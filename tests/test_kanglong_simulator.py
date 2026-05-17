from __future__ import annotations

from decimal import Decimal

from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import KanglongGroupPlan
from paired_opener.kanglong.reporter import summarize_costs
from paired_opener.kanglong.service import KanglongSimulationService
from paired_opener.kanglong.simulator import simulate_group
from paired_opener.storage import SqliteRepository


def group(round_qtys: list[Decimal], side: PositionSide = PositionSide.LONG) -> KanglongGroupPlan:
    return KanglongGroupPlan(
        group_id="group-0001",
        from_account_id="sub1",
        to_account_id="main",
        symbol="ETHUSDC",
        side=side,
        target_qty=sum(round_qtys, Decimal("0")),
        round_qtys=round_qtys,
    )


def rules() -> SymbolRules:
    return SymbolRules(
        "ETHUSDC",
        Decimal("0.01"),
        Decimal("0.001"),
        Decimal("0.001"),
        Decimal("5"),
        125,
    )


def test_simulate_group_emits_matched_close_and_open_events() -> None:
    result = simulate_group(
        run_id="run-1",
        group=group([Decimal("0.01"), Decimal("0.01")]),
        rules=rules(),
        close_price=Decimal("3100.00"),
        open_price=Decimal("3100.50"),
        fee_rate=Decimal("0.0005"),
        config=KanglongSymbolConfig(),
    )

    assert result.matched_qty == Decimal("0.02")
    assert len(result.events) == 4
    assert result.events[0].round_match_id == result.events[1].round_match_id
    assert result.events[0].account_id == "sub1"
    assert result.events[1].account_id == "main"


def test_simulate_group_tracks_rounding_residual_and_transfer_costs() -> None:
    result = simulate_group(
        run_id="run-1",
        group=group([Decimal("0.0105")]),
        rules=rules(),
        close_price=Decimal("3100.00"),
        open_price=Decimal("3100.50"),
        fee_rate=Decimal("0.0005"),
        config=KanglongSymbolConfig(),
    )

    costs = summarize_costs(result.events, result.residual_ledger)

    assert result.matched_qty == Decimal("0.010")
    assert result.residual_ledger[0].account_id == "sub1"
    assert result.residual_ledger[0].side == PositionSide.LONG
    assert result.residual_ledger[0].signed_qty == Decimal("0.0005")
    assert Decimal(costs["transfer_fee_cost"]) == Decimal("31.00000") * Decimal("0.0005") + Decimal(
        "31.00500"
    ) * Decimal("0.0005")
    assert Decimal(costs["transfer_price_diff_loss"]) == Decimal("0.00500")
    assert all(event.realized_pnl == Decimal("0") for event in result.events)


def test_short_transfer_costs_use_open_leg_signed_price_diff_only() -> None:
    result = simulate_group(
        run_id="run-1",
        group=group([Decimal("0.5")], side=PositionSide.SHORT),
        rules=rules(),
        close_price=Decimal("3100.50"),
        open_price=Decimal("3100.00"),
        fee_rate=Decimal("0.0005"),
        config=KanglongSymbolConfig(),
    )

    costs = summarize_costs(result.events, result.residual_ledger)
    nonzero_price_diff_events = [event for event in result.events if event.price_diff_pnl != Decimal("0")]

    assert Decimal(costs["transfer_price_diff_pnl"]) < Decimal("0")
    assert Decimal(costs["transfer_price_diff_loss"]) > Decimal("0")
    assert [(event.action_type, event.price_diff_pnl) for event in nonzero_price_diff_events] == [
        ("single_open", Decimal("-0.250"))
    ]
    assert all(event.realized_pnl == Decimal("0") for event in result.events)


def test_kanglong_service_persists_run_payload(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)

    try:
        payload = service.create_draft_run(
            run_id="run-1",
            symbol="ETHUSDC",
            main_account_id="main",
            subaccount_ids=["sub1", "sub2"],
        )

        stored = repository.get_kanglong_run("run-1")
    finally:
        repository.close()

    assert payload["run_id"] == "run-1"
    assert stored is not None
    assert stored["status"] == "draft_plan"
    assert stored["subaccount_ids"] == ["sub1", "sub2"]
