from __future__ import annotations

from decimal import Decimal
from typing import Any

from paired_opener.kanglong.models import KanglongEvent, ResidualLedgerEntry


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def summarize_costs(events: list[KanglongEvent], residuals: list[ResidualLedgerEntry]) -> dict[str, Any]:
    transfer_fee_cost = sum((event.fee for event in events), Decimal("0"))
    rebalance_fee_cost = Decimal("0")
    transfer_price_diff_pnl = sum((event.price_diff_pnl for event in events), Decimal("0"))
    rebalance_price_diff_pnl = Decimal("0")
    transfer_price_diff_loss = max(-transfer_price_diff_pnl, Decimal("0"))
    rebalance_price_diff_loss = max(-rebalance_price_diff_pnl, Decimal("0"))
    total_fee_cost = transfer_fee_cost + rebalance_fee_cost
    total_price_diff_loss = transfer_price_diff_loss + rebalance_price_diff_loss
    total_cost = total_fee_cost + total_price_diff_loss
    released_profit = Decimal("0")
    net_profit_after_cost = (
        released_profit + transfer_price_diff_pnl + rebalance_price_diff_pnl - total_fee_cost
    )
    return {
        "transfer_fee_cost": _decimal_text(transfer_fee_cost),
        "rebalance_fee_cost": _decimal_text(rebalance_fee_cost),
        "transfer_price_diff_pnl": _decimal_text(transfer_price_diff_pnl),
        "rebalance_price_diff_pnl": _decimal_text(rebalance_price_diff_pnl),
        "transfer_price_diff_loss": _decimal_text(transfer_price_diff_loss),
        "rebalance_price_diff_loss": _decimal_text(rebalance_price_diff_loss),
        "total_fee_cost": _decimal_text(total_fee_cost),
        "total_price_diff_loss": _decimal_text(total_price_diff_loss),
        "total_cost": _decimal_text(total_cost),
        "net_profit_after_cost": _decimal_text(net_profit_after_cost),
        "residual_count": len(residuals),
    }
