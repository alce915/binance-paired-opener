from __future__ import annotations

from decimal import Decimal

from paired_opener.domain import PositionSide
from paired_opener.kanglong.models import KanglongEvent, KanglongEventStatus, KanglongRunStatus, ResidualLedgerEntry


def test_kanglong_event_serializes_decimal_values_as_strings() -> None:
    event = KanglongEvent(
        run_id="run-1",
        group_id="group-1",
        round_id="round-1",
        mode="simulation",
        account_id="sub-1",
        symbol="ETHUSDC",
        position_side=PositionSide.LONG,
        action_type="single_close",
        leg_id="leg-close",
        paired_leg_id="leg-open",
        round_match_id="match-1",
        planned_qty=Decimal("0.010"),
        submitted_qty=Decimal("0.010"),
        filled_qty=Decimal("0.009"),
        matched_qty=Decimal("0.009"),
        close_residual_qty=Decimal("0.001"),
        open_residual_qty=Decimal("0"),
        avg_price=Decimal("3000.1"),
        status=KanglongEventStatus.PARTIAL_FILLED,
        reason="limit_order_unfilled",
    )

    payload = event.to_payload()

    assert KanglongRunStatus.DRAFT_PLAN.value == "draft_plan"
    assert payload["filled_qty"] == "0.009"
    assert payload["status"] == "partial_filled"
    assert payload["position_side"] == "LONG"


def test_residual_ledger_keeps_account_side_and_leg_type() -> None:
    entry = ResidualLedgerEntry(
        account_id="sub-1",
        side=PositionSide.LONG,
        leg_type="close",
        signed_qty=Decimal("0.001"),
        reason="step_size_rounding",
        event_id="event-1",
    )

    payload = entry.to_payload()

    assert payload["account_id"] == "sub-1"
    assert payload["side"] == "LONG"
    assert payload["leg_type"] == "close"
    assert payload["signed_qty"] == "0.001"
