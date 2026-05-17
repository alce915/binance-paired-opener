from __future__ import annotations

from decimal import Decimal
from typing import Any

from paired_opener.domain import PositionSide
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import KanglongAccountSnapshot, KanglongPrecheckResult, KanglongRunStatus


def closeable_profitable_qty(
    snapshot: KanglongAccountSnapshot,
    side: PositionSide,
    config: KanglongSymbolConfig,
) -> Decimal:
    qty = snapshot.qty(side)
    if qty <= config.qty_tolerance:
        return Decimal("0")
    if snapshot.pnl(side) <= Decimal("0"):
        return Decimal("0")
    return qty


def _side_summary(
    subaccounts: list[KanglongAccountSnapshot],
    side: PositionSide,
    config: KanglongSymbolConfig,
) -> dict[str, Any]:
    profitable = [
        {
            "account_id": snapshot.account_id,
            "profit": snapshot.pnl(side),
            "closeable_qty": closeable_profitable_qty(snapshot, side, config),
        }
        for snapshot in subaccounts
    ]
    total_profit = sum((item["profit"] for item in profitable), Decimal("0"))
    total_closeable_qty = sum((item["closeable_qty"] for item in profitable), Decimal("0"))
    donors = sorted(
        (item for item in profitable if item["closeable_qty"] > config.qty_tolerance),
        key=lambda item: (-item["closeable_qty"], item["account_id"]),
    )
    return {
        "side": side.value,
        "total_profit": total_profit,
        "total_closeable_qty": total_closeable_qty,
        "first_donor_account_id": donors[0]["account_id"] if donors else None,
    }


def choose_selected_side(
    subaccounts: list[KanglongAccountSnapshot],
    *,
    manual_side: PositionSide | None,
    config: KanglongSymbolConfig,
) -> tuple[PositionSide | None, dict[str, Any]]:
    summaries = {
        PositionSide.LONG: _side_summary(subaccounts, PositionSide.LONG, config),
        PositionSide.SHORT: _side_summary(subaccounts, PositionSide.SHORT, config),
    }
    if manual_side is not None:
        selected = manual_side
    else:
        ordered = sorted(
            summaries.items(),
            key=lambda item: (
                -item[1]["total_profit"],
                -item[1]["total_closeable_qty"],
                item[0].value,
            ),
        )
        selected = ordered[0][0] if ordered and ordered[0][1]["total_profit"] > Decimal("0") else None
    other = PositionSide.SHORT if selected == PositionSide.LONG else PositionSide.LONG
    return selected, {"preview_side": other.value, **summaries[other]}


def run_static_precheck(
    *,
    main: KanglongAccountSnapshot,
    subaccounts: list[KanglongAccountSnapshot],
    symbol: str,
    manual_side: PositionSide | None,
    config: KanglongSymbolConfig,
) -> KanglongPrecheckResult:
    _ = symbol
    if (
        abs(main.qty(PositionSide.LONG)) > config.qty_tolerance
        or abs(main.qty(PositionSide.SHORT)) > config.qty_tolerance
    ):
        return KanglongPrecheckResult(
            ok=False,
            status=KanglongRunStatus.BLOCKED_MAIN_NOT_FLAT,
            reason_code="blocked_main_not_flat",
            selected_side=None,
            first_donor_account_id=None,
            planned_release_qty=Decimal("0"),
            other_side_preview={},
            details={
                "main_long_qty": main.qty(PositionSide.LONG),
                "main_short_qty": main.qty(PositionSide.SHORT),
            },
        )

    for snapshot in subaccounts:
        if abs(snapshot.qty(PositionSide.LONG) - snapshot.qty(PositionSide.SHORT)) > config.qty_tolerance:
            return KanglongPrecheckResult(
                ok=False,
                status=KanglongRunStatus.BLOCKED_INITIAL_SUBACCOUNT_UNBALANCED,
                reason_code="blocked_initial_subaccount_unbalanced",
                selected_side=None,
                first_donor_account_id=None,
                planned_release_qty=Decimal("0"),
                other_side_preview={},
                details={"account_id": snapshot.account_id},
            )

    selected_side, other_side_preview = choose_selected_side(
        subaccounts,
        manual_side=manual_side,
        config=config,
    )
    if selected_side is None:
        return KanglongPrecheckResult(
            False,
            KanglongRunStatus.BLOCKED_NO_PROFITABLE_ACCOUNT,
            "blocked_no_profitable_account",
            None,
            None,
            Decimal("0"),
            other_side_preview,
        )
    if manual_side is not None and sum((snapshot.pnl(selected_side) for snapshot in subaccounts), Decimal("0")) <= Decimal("0"):
        return KanglongPrecheckResult(
            False,
            KanglongRunStatus.BLOCKED_MANUAL_SIDE_NOT_PROFITABLE,
            "blocked_manual_side_not_profitable",
            selected_side,
            None,
            Decimal("0"),
            other_side_preview,
        )

    donors = sorted(
        subaccounts,
        key=lambda snapshot: (
            -closeable_profitable_qty(snapshot, selected_side, config),
            snapshot.account_id,
        ),
    )
    first = donors[0]
    planned_release_qty = closeable_profitable_qty(first, selected_side, config)
    return KanglongPrecheckResult(
        True,
        KanglongRunStatus.CHAIN_READY,
        None,
        selected_side,
        first.account_id,
        planned_release_qty,
        other_side_preview,
    )
