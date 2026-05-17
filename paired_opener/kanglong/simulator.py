from __future__ import annotations

from decimal import Decimal

from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import (
    KanglongEvent,
    KanglongEventStatus,
    KanglongFill,
    KanglongGroupPlan,
    KanglongGroupResult,
    ResidualLedgerEntry,
    utc_now,
)
from paired_opener.rounding import normalize_qty


def _price_diff_pnl(side: PositionSide, close_price: Decimal, open_price: Decimal, qty: Decimal) -> Decimal:
    if side == PositionSide.LONG:
        return (close_price - open_price) * qty
    return (open_price - close_price) * qty


def simulate_group(
    *,
    run_id: str,
    group: KanglongGroupPlan,
    rules: SymbolRules,
    close_price: Decimal,
    open_price: Decimal,
    fee_rate: Decimal,
    config: KanglongSymbolConfig,
) -> KanglongGroupResult:
    _ = config
    events: list[KanglongEvent] = []
    residuals: list[ResidualLedgerEntry] = []
    matched_total = Decimal("0")

    for index, planned_qty in enumerate(group.round_qtys, start=1):
        submitted_qty = normalize_qty(planned_qty, rules)
        rounding_residual = planned_qty - submitted_qty
        round_id = f"{group.group_id}-round-{index:04d}"
        match_id = f"{round_id}-match"
        matched_qty = submitted_qty
        matched_total += matched_qty
        price_diff_pnl = _price_diff_pnl(group.side, close_price, open_price, matched_qty)

        if rounding_residual > Decimal("0"):
            residuals.append(
                ResidualLedgerEntry(
                    account_id=group.from_account_id,
                    side=group.side,
                    leg_type="rounding",
                    signed_qty=rounding_residual,
                    reason="step_size_rounding",
                    event_id=match_id,
                )
            )

        close_fee = matched_qty * close_price * fee_rate
        open_fee = matched_qty * open_price * fee_rate
        close_event = KanglongEvent(
            run_id=run_id,
            group_id=group.group_id,
            round_id=round_id,
            mode="simulation",
            account_id=group.from_account_id,
            symbol=group.symbol,
            position_side=group.side,
            action_type="single_close",
            leg_id=f"{round_id}-close",
            paired_leg_id=f"{round_id}-open",
            round_match_id=match_id,
            planned_qty=planned_qty,
            submitted_qty=submitted_qty,
            filled_qty=matched_qty,
            matched_qty=matched_qty,
            close_residual_qty=Decimal("0"),
            open_residual_qty=Decimal("0"),
            avg_price=close_price,
            status=KanglongEventStatus.FILLED,
            fee=close_fee,
            realized_pnl=Decimal("0"),
            price_diff_pnl=Decimal("0"),
            fills=[
                KanglongFill(
                    trade_id=f"{round_id}-close-fill",
                    fill_qty=matched_qty,
                    fill_price=close_price,
                    fee=close_fee,
                    fee_asset="USDC",
                    liquidity_role="taker",
                    filled_at=utc_now(),
                )
            ],
        )
        open_event = KanglongEvent(
            run_id=run_id,
            group_id=group.group_id,
            round_id=round_id,
            mode="simulation",
            account_id=group.to_account_id,
            symbol=group.symbol,
            position_side=group.side,
            action_type="single_open",
            leg_id=f"{round_id}-open",
            paired_leg_id=f"{round_id}-close",
            round_match_id=match_id,
            planned_qty=planned_qty,
            submitted_qty=submitted_qty,
            filled_qty=matched_qty,
            matched_qty=matched_qty,
            close_residual_qty=Decimal("0"),
            open_residual_qty=Decimal("0"),
            avg_price=open_price,
            status=KanglongEventStatus.FILLED,
            fee=open_fee,
            price_diff_pnl=price_diff_pnl,
            fills=[
                KanglongFill(
                    trade_id=f"{round_id}-open-fill",
                    fill_qty=matched_qty,
                    fill_price=open_price,
                    fee=open_fee,
                    fee_asset="USDC",
                    liquidity_role="taker",
                    filled_at=utc_now(),
                )
            ],
        )
        events.extend([close_event, open_event])

    return KanglongGroupResult(
        group_id=group.group_id,
        matched_qty=matched_total,
        residual_ledger=residuals,
        events=events,
    )
