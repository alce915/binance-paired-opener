from __future__ import annotations

from decimal import Decimal
from typing import Any

from paired_opener.kanglong.models import KanglongEvent, ResidualLedgerEntry


def summarize_costs(events: list[KanglongEvent], residuals: list[ResidualLedgerEntry]) -> dict[str, Any]:
    fee_cost = sum((event.fee for event in events), Decimal("0"))
    realized_pnl = sum((event.realized_pnl for event in events), Decimal("0"))
    return {
        "transfer_fee_cost": fee_cost,
        "rebalance_fee_cost": Decimal("0"),
        "transfer_price_diff_pnl": realized_pnl,
        "transfer_price_diff_loss": max(-realized_pnl, Decimal("0")),
        "residual_count": len(residuals),
    }
