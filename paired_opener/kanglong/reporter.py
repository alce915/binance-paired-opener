from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from paired_opener.kanglong.ledger import (
    baseline_from_storage_payload,
    hash_checkpoint,
    hash_ledger_state,
    ledger_entry_from_storage_payload,
)
from paired_opener.kanglong.models import KanglongEvent, ResidualLedgerEntry

REPORT_VERSION = "kanglong_transfer_report_v1"
BATCH_REPORT_VERSION = "kanglong_batch_report_v1"
CONVERSION_UNAVAILABLE = "kanglong_conversion_unavailable"


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _decimal_value(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _group_id_from_operation(operation_id: Any) -> str:
    parts = str(operation_id or "").split(":")
    if len(parts) >= 2 and parts[1]:
        return parts[1]
    return "unknown"


def _generated_at_text(value: datetime | str | None) -> str:
    if value is None:
        return datetime.now(UTC).isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _quote_asset_from_symbol(symbol: str | None) -> str:
    normalized = str(symbol or "").strip().upper()
    for suffix in ("USDC", "USDT", "FDUSD", "BUSD", "USD"):
        if normalized.endswith(suffix):
            return suffix
    return "USDC"


def _rate_for_asset(exchange_rate_snapshot: dict[str, Any] | None, asset: str, quote_asset: str) -> Decimal | None:
    if asset == quote_asset:
        return Decimal("1")
    if not exchange_rate_snapshot:
        return None
    rates = exchange_rate_snapshot.get("rates") if isinstance(exchange_rate_snapshot, dict) else None
    if not isinstance(rates, dict):
        return None
    raw_rate = rates.get(asset) or rates.get(f"{asset}{quote_asset}") or rates.get(f"{asset}/{quote_asset}")
    if raw_rate is None:
        return None
    return _decimal_value(raw_rate)


def _converted_fee_total(
    fee_by_asset: dict[str, Decimal],
    *,
    quote_asset: str,
    exchange_rate_snapshot: dict[str, Any] | None,
) -> tuple[Decimal | None, dict[str, Any]]:
    converted = Decimal("0")
    rates_used: dict[str, str] = {}
    for asset, amount in fee_by_asset.items():
        rate = _rate_for_asset(exchange_rate_snapshot, asset, quote_asset)
        if rate is None:
            return None, {
                "conversion_status": "unavailable",
                "warning_code": CONVERSION_UNAVAILABLE,
                "quote_asset": quote_asset,
            }
        converted += amount * rate
        rates_used[asset] = _decimal_text(rate)
    return converted, {
        "conversion_status": "converted" if any(asset != quote_asset for asset in fee_by_asset) else "not_required",
        "quote_asset": quote_asset,
        "rates": rates_used,
        "snapshot": exchange_rate_snapshot,
    }


def _entries_up_to_checkpoint(entries: list[dict[str, Any]], checkpoint_id: int | None) -> list[dict[str, Any]]:
    if checkpoint_id is None:
        return list(entries)
    return [
        entry
        for entry in entries
        if int(entry.get("checkpoint_id") or 0) <= checkpoint_id
    ]


def _verify_checkpoint_hashes(
    entries: list[dict[str, Any]],
    baselines: list[dict[str, Any]],
    latest_checkpoint: dict[str, Any] | None,
) -> None:
    if latest_checkpoint is None:
        return
    checkpoint_id = int(latest_checkpoint["checkpoint_id"])
    checkpoint_entries = [
        ledger_entry_from_storage_payload(entry)
        for entry in entries
        if int(entry.get("checkpoint_id") or 0) == checkpoint_id
    ]
    all_entries = [
        ledger_entry_from_storage_payload(entry)
        for entry in entries
        if int(entry.get("checkpoint_id") or 0) <= checkpoint_id
    ]
    previous_ledger_hash = str(latest_checkpoint.get("previous_ledger_hash") or "")
    if hash_checkpoint(previous_ledger_hash, checkpoint_entries) != latest_checkpoint.get("ledger_hash"):
        raise ValueError("kanglong_ledger_hash_mismatch")
    baseline_objects = [baseline_from_storage_payload(baseline) for baseline in baselines]
    if hash_ledger_state(baseline_objects, all_entries) != latest_checkpoint.get("ledger_state_hash"):
        raise ValueError("kanglong_ledger_state_hash_mismatch")


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


def summarize_ledger_costs(
    entries: list[dict[str, Any]],
    *,
    latest_checkpoint: dict[str, Any] | None = None,
    symbol: str | None = None,
    exchange_rate_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fee_by_asset: dict[str, Decimal] = {}
    price_wear_by_group: dict[str, Decimal] = {}
    raw_total_fee = Decimal("0")
    total_price_wear = Decimal("0")
    for entry in entries:
        entry_type = str(entry.get("entry_type") or "")
        if entry_type == "fee":
            asset = str(entry.get("fee_asset") or entry.get("asset") or "UNKNOWN")
            fee_amount = abs(_decimal_value(entry.get("fee_amount") or entry.get("amount")))
            raw_total_fee += fee_amount
            fee_by_asset[asset] = fee_by_asset.get(asset, Decimal("0")) + fee_amount
        elif entry_type == "price_wear":
            price_wear = abs(_decimal_value(entry.get("price_wear") or entry.get("amount")))
            total_price_wear += price_wear
            group_id = _group_id_from_operation(entry.get("operation_id"))
            price_wear_by_group[group_id] = price_wear_by_group.get(group_id, Decimal("0")) + price_wear

    quote_asset = _quote_asset_from_symbol(symbol)
    total_fee, conversion = _converted_fee_total(
        fee_by_asset,
        quote_asset=quote_asset,
        exchange_rate_snapshot=exchange_rate_snapshot,
    )
    total_cost = None if total_fee is None else total_fee + total_price_wear
    checkpoint_payload = latest_checkpoint or {}
    summary = {
        "transfer_fee_cost": _decimal_text(total_fee) if total_fee is not None else None,
        "rebalance_fee_cost": "0",
        "transfer_price_diff_pnl": _decimal_text(-total_price_wear),
        "rebalance_price_diff_pnl": "0",
        "transfer_price_diff_loss": _decimal_text(total_price_wear),
        "rebalance_price_diff_loss": "0",
        "total_fee_cost": _decimal_text(total_fee) if total_fee is not None else None,
        "total_price_diff_loss": _decimal_text(total_price_wear),
        "total_cost": _decimal_text(total_cost) if total_cost is not None else None,
        "net_profit_after_cost": _decimal_text(-total_cost) if total_cost is not None else None,
        "fee_by_asset": {asset: _decimal_text(amount) for asset, amount in sorted(fee_by_asset.items())},
        "price_wear_by_group": {
            group_id: _decimal_text(amount)
            for group_id, amount in sorted(price_wear_by_group.items())
        },
        "conversion": conversion,
        "source_checkpoint_id": checkpoint_payload.get("checkpoint_id"),
        "source_ledger_hash": checkpoint_payload.get("ledger_hash"),
        "source_ledger_state_hash": checkpoint_payload.get("ledger_state_hash"),
    }
    if total_fee is None:
        summary["raw_transfer_fee_cost"] = _decimal_text(raw_total_fee)
        summary["warning_code"] = CONVERSION_UNAVAILABLE
    return summary


def summarize_batch_ledger_costs(entries: list[dict[str, Any]]) -> dict[str, Any]:
    accounts: dict[str, dict[str, Any]] = {}
    total_fee = Decimal("0")
    total_adverse = Decimal("0")
    total_improvement = Decimal("0")
    categories = {
        "spread_cost": Decimal("0"),
        "market_impact_cost": Decimal("0"),
        "timing_drift_cost": Decimal("0"),
        "alignment_cost": Decimal("0"),
    }
    for entry in entries:
        payload = entry.get("payload") or {}
        account_id = str(entry.get("account_id") or "unknown")
        leg = str(payload.get("position_side") or "unknown")
        round_index = str(payload.get("round_index") if payload.get("round_index") is not None else "unknown")
        account = accounts.setdefault(
            account_id,
            {"fee_cost": Decimal("0"), "total_adverse_wear": Decimal("0"), "price_improvement": Decimal("0"), "legs": {}},
        )
        leg_payload = account["legs"].setdefault(
            leg,
            {"fee_cost": Decimal("0"), "total_adverse_wear": Decimal("0"), "price_improvement": Decimal("0"), "rounds": {}},
        )
        round_payload = leg_payload["rounds"].setdefault(
            round_index,
            {"fee_cost": Decimal("0"), "total_adverse_wear": Decimal("0"), "price_improvement": Decimal("0")},
        )
        if entry.get("entry_type") == "fee":
            fee = abs(_decimal_value(entry.get("fee_amount") or entry.get("amount")))
            total_fee += fee
            account["fee_cost"] += fee
            leg_payload["fee_cost"] += fee
            round_payload["fee_cost"] += fee
        elif entry.get("entry_type") == "price_wear":
            adverse = max(_decimal_value(payload.get("adverse") or entry.get("price_wear") or entry.get("amount")), Decimal("0"))
            category = str(payload.get("wear_category") or "spread_cost")
            improvement = (
                max(_decimal_value(payload.get("improvement")), Decimal("0"))
                if category == "spread_cost"
                else Decimal("0")
            )
            if category in categories:
                categories[category] += adverse
            total_adverse += adverse
            total_improvement += improvement
            account["total_adverse_wear"] += adverse
            account["price_improvement"] += improvement
            leg_payload["total_adverse_wear"] += adverse
            leg_payload["price_improvement"] += improvement
            round_payload["total_adverse_wear"] += adverse
            round_payload["price_improvement"] += improvement

    def encode(value: Any) -> Any:
        if isinstance(value, Decimal):
            return _decimal_text(value)
        if isinstance(value, dict):
            return {key: encode(item) for key, item in value.items()}
        return value

    return encode(
        {
            "report_version": BATCH_REPORT_VERSION,
            "total_fee_cost": total_fee,
            "total_adverse_wear": total_adverse,
            "total_price_improvement": total_improvement,
            **categories,
            "accounts": accounts,
        }
    )


def build_ledger_report(
    entries: list[dict[str, Any]],
    *,
    baselines: list[dict[str, Any]],
    latest_checkpoint: dict[str, Any] | None,
    summary_status: str,
    symbol: str | None = None,
    generated_at: datetime | str | None = None,
    exchange_rate_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    checkpoint_id = int(latest_checkpoint["checkpoint_id"]) if latest_checkpoint is not None else None
    scoped_entries = _entries_up_to_checkpoint(entries, checkpoint_id)
    _verify_checkpoint_hashes(scoped_entries, baselines, latest_checkpoint)
    costs = summarize_ledger_costs(
        scoped_entries,
        latest_checkpoint=latest_checkpoint,
        symbol=symbol,
        exchange_rate_snapshot=exchange_rate_snapshot,
    )
    batch_costs = summarize_batch_ledger_costs(scoped_entries)
    generated_at_text = _generated_at_text(generated_at)
    report_summary = {
        "report_version": REPORT_VERSION,
        "generated_from_checkpoint_id": costs.get("source_checkpoint_id"),
        "source_ledger_hash": costs.get("source_ledger_hash"),
        "source_ledger_state_hash": costs.get("source_ledger_state_hash"),
        "generated_at": generated_at_text,
        "summary_status": summary_status,
        "total_fee_cost": costs.get("total_fee_cost"),
        "total_price_diff_loss": costs.get("total_price_diff_loss"),
        "total_cost": costs.get("total_cost"),
        "fee_by_asset": costs.get("fee_by_asset") or {},
        "price_wear_by_group": costs.get("price_wear_by_group") or {},
    }
    if costs.get("warning_code"):
        report_summary["warning_code"] = costs["warning_code"]
    return {
        "costs": costs,
        "batch_costs": batch_costs,
        "ledger_report": {
            "report_version": REPORT_VERSION,
            "generated_from_checkpoint_id": costs.get("source_checkpoint_id"),
            "source_checkpoint_id": costs.get("source_checkpoint_id"),
            "source_ledger_hash": costs.get("source_ledger_hash"),
            "source_ledger_state_hash": costs.get("source_ledger_state_hash"),
            "generated_at": generated_at_text,
            "conversion_status": (costs.get("conversion") or {}).get("conversion_status"),
            "warning_code": costs.get("warning_code"),
        },
        "report_summary": report_summary,
    }
