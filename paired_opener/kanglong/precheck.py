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


def estimate_main_receivable_qty(
    main: KanglongAccountSnapshot,
    selected_side: PositionSide,
    config: KanglongSymbolConfig,
    *,
    reference_price: Decimal | None = None,
    fee_rate: Decimal = Decimal("0.0005"),
) -> Decimal:
    return estimate_main_receivable_capacity(
        main,
        selected_side,
        config,
        reference_price=reference_price,
        fee_rate=fee_rate,
    )["main_receivable_qty"]


def estimate_main_receivable_capacity(
    main: KanglongAccountSnapshot,
    selected_side: PositionSide,
    config: KanglongSymbolConfig,
    *,
    reference_price: Decimal | None = None,
    fee_rate: Decimal = Decimal("0.0005"),
) -> dict[str, Decimal]:
    current_temp_qty = main.qty(selected_side)
    temp_qty_capacity = max(config.max_main_temp_qty - current_temp_qty, Decimal("0"))
    price = reference_price or Decimal("0")
    leverage = Decimal(max(int(main.leverage or 1), 1))
    if price <= Decimal("0"):
        notional_capacity_qty = temp_qty_capacity
        margin_capacity_qty = temp_qty_capacity
        liquidation_buffer_qty = temp_qty_capacity
    else:
        notional_capacity_qty = max(
            (main.equity * leverage * config.max_main_temp_notional_ratio) / price,
            Decimal("0"),
        )
        available_after_safety = max(
            main.available_balance * (Decimal("1") - config.margin_safety_ratio),
            Decimal("0"),
        )
        price_buffer_rate = Decimal(config.price_buffer_bps) / Decimal("10000")
        estimated_margin_per_qty = price / leverage
        estimated_fee_per_qty = price * fee_rate * Decimal("2")
        estimated_buffer_per_qty = price * price_buffer_rate
        cost_per_qty = estimated_margin_per_qty + estimated_fee_per_qty + estimated_buffer_per_qty
        margin_capacity_qty = available_after_safety / cost_per_qty if cost_per_qty > Decimal("0") else temp_qty_capacity
        liquidation_buffer_qty = max(
            (main.equity * leverage * (Decimal("1") - config.min_liquidation_buffer_ratio)) / price,
            Decimal("0"),
        )
    main_receivable_qty = min(
        temp_qty_capacity,
        notional_capacity_qty,
        margin_capacity_qty,
        liquidation_buffer_qty,
    )
    return {
        "main_receivable_qty": max(main_receivable_qty, Decimal("0")),
        "temp_qty_capacity": temp_qty_capacity,
        "notional_capacity_qty": notional_capacity_qty,
        "margin_capacity_qty": margin_capacity_qty,
        "liquidation_buffer_qty": liquidation_buffer_qty,
        "reference_price": price,
    }


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
    reference_price: Decimal | None = None,
    fee_rate: Decimal = Decimal("0.0005"),
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
    selected_position = first.positions.get(selected_side)
    capacity_reference_price = reference_price
    if capacity_reference_price is None and selected_position is not None:
        capacity_reference_price = selected_position.mark_price
    capacity = estimate_main_receivable_capacity(
        main,
        selected_side,
        config,
        reference_price=capacity_reference_price,
        fee_rate=fee_rate,
    )
    main_receivable_qty = capacity["main_receivable_qty"]
    if main_receivable_qty + config.qty_tolerance < planned_release_qty:
        return KanglongPrecheckResult(
            ok=False,
            status=KanglongRunStatus.BLOCKED_MAIN_INSUFFICIENT_CAPACITY,
            reason_code="blocked_main_insufficient_capacity",
            selected_side=selected_side,
            first_donor_account_id=first.account_id,
            planned_release_qty=planned_release_qty,
            other_side_preview=other_side_preview,
            details={
                **capacity,
                "planned_release_qty": planned_release_qty,
                "capacity_gap_qty": planned_release_qty - main_receivable_qty,
            },
        )
    return KanglongPrecheckResult(
        True,
        KanglongRunStatus.CHAIN_READY,
        None,
        selected_side,
        first.account_id,
        planned_release_qty,
        other_side_preview,
    )
