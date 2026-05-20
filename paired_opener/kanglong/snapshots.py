from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any

from paired_opener.domain import PositionSide
from paired_opener.kanglong.models import KanglongAccountSnapshot, KanglongPositionSnapshot


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or "0"))


def _position_side(value: Any, quantity: Decimal, *, account_id: str, symbol: str) -> PositionSide:
    raw = str(value or "").upper()
    if raw == "LONG":
        return PositionSide.LONG
    if raw == "SHORT":
        return PositionSide.SHORT
    if quantity < 0:
        return PositionSide.SHORT
    if quantity > 0:
        return PositionSide.LONG
    raise ValueError(
        f"Cannot derive position side for account {account_id} symbol {symbol}: "
        "missing or unknown position_side with zero quantity"
    )


def monitor_account_to_kanglong_snapshot(
    account: dict[str, Any],
    *,
    symbol: str,
    leverage: int,
) -> KanglongAccountSnapshot:
    normalized_symbol = symbol.upper()
    account_id = str(account.get("account_id") or "")
    positions: dict[PositionSide, KanglongPositionSnapshot] = {}
    for raw_position in account.get("positions") or []:
        if str(raw_position.get("symbol") or "").upper() != normalized_symbol:
            continue
        signed_qty = _decimal(raw_position.get("position_amt") or raw_position.get("qty"))
        side = _position_side(
            raw_position.get("position_side"),
            signed_qty,
            account_id=account_id,
            symbol=normalized_symbol,
        )
        positions[side] = KanglongPositionSnapshot(
            symbol=normalized_symbol,
            side=side,
            qty=abs(signed_qty),
            entry_price=_decimal(raw_position.get("entry_price")),
            mark_price=_decimal(raw_position.get("mark_price")),
            unrealized_pnl=_decimal(raw_position.get("unrealized_pnl")),
        )
    updated_at = str(account.get("updated_at") or "")
    totals = account.get("totals") or {}
    return KanglongAccountSnapshot(
        account_id=account_id,
        account_name=str(account.get("account_name") or account_id),
        available_balance=_decimal(totals.get("available_balance")),
        equity=_decimal(totals.get("equity")),
        margin=_decimal(totals.get("margin")),
        leverage=int(leverage),
        positions=positions,
        open_orders=list(account.get("open_orders") or []),
        snapshot_version=f"{account_id}:{updated_at}",
    )


def _leverage_for_account(leverage: int | dict[str, int], account_id: str) -> int:
    if isinstance(leverage, dict):
        return max(int(leverage.get(account_id) or 1), 1)
    return max(int(leverage or 1), 1)


def build_snapshot_bundle(
    *,
    symbol: str,
    accounts: list[dict[str, Any]],
    config_version: str,
    symbol_rule_version: str,
    price_version: str,
    leverage: int | dict[str, int],
) -> dict[str, Any]:
    snapshots = [
        monitor_account_to_kanglong_snapshot(
            account,
            symbol=symbol,
            leverage=_leverage_for_account(leverage, str(account.get("account_id") or "")),
        )
        for account in accounts
    ]
    versions = sorted(
        [
            {
                "account_id": snapshot.account_id,
                "snapshot_version": snapshot.snapshot_version,
                "leverage": snapshot.leverage,
            }
            for snapshot in snapshots
        ],
        key=lambda item: (item["account_id"], item["snapshot_version"]),
    )
    fingerprint_payload = {
        "symbol": symbol.upper(),
        "config_version": config_version,
        "symbol_rule_version": symbol_rule_version,
        "price_version": price_version,
        "versions": versions,
    }
    raw = json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":"))
    return {
        "snapshot_bundle_id": hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24],
        "accounts": snapshots,
        "fingerprint": fingerprint_payload,
    }
