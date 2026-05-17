from __future__ import annotations

import json
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

from paired_opener.config import Settings


@dataclass(frozen=True, slots=True)
class KanglongSymbolConfig:
    per_round_qty_limit: Decimal = Decimal("0.05")
    qty_tolerance: Decimal = Decimal("0.0001")
    max_rounds_per_group: int = 30
    max_chain_groups: int = 100
    max_main_temp_qty: Decimal = Decimal("1.50")
    max_main_temp_notional_ratio: Decimal = Decimal("0.80")
    price_buffer_bps: int = 5
    margin_safety_ratio: Decimal = Decimal("0.20")
    min_liquidation_buffer_ratio: Decimal = Decimal("0.15")
    snapshot_ttl_ms: int = 5000
    price_ttl_ms: int = 2000
    run_lock_ttl_ms: int = 600000
    simulation_result_ttl_ms: int = 60000


def _decimal(value: Any, default: Decimal) -> Decimal:
    if value is None:
        return default
    return Decimal(str(value))


def _int(value: Any, default: int) -> int:
    if value is None:
        return default
    return int(value)


def _apply_overrides(base: KanglongSymbolConfig, payload: dict[str, Any]) -> KanglongSymbolConfig:
    return replace(
        base,
        per_round_qty_limit=_decimal(payload.get("per_round_qty_limit"), base.per_round_qty_limit),
        qty_tolerance=_decimal(payload.get("qty_tolerance"), base.qty_tolerance),
        max_rounds_per_group=_int(payload.get("max_rounds_per_group"), base.max_rounds_per_group),
        max_chain_groups=_int(payload.get("max_chain_groups"), base.max_chain_groups),
        max_main_temp_qty=_decimal(payload.get("max_main_temp_qty"), base.max_main_temp_qty),
        max_main_temp_notional_ratio=_decimal(
            payload.get("max_main_temp_notional_ratio"),
            base.max_main_temp_notional_ratio,
        ),
        price_buffer_bps=_int(payload.get("price_buffer_bps"), base.price_buffer_bps),
        margin_safety_ratio=_decimal(payload.get("margin_safety_ratio"), base.margin_safety_ratio),
        min_liquidation_buffer_ratio=_decimal(
            payload.get("min_liquidation_buffer_ratio"),
            base.min_liquidation_buffer_ratio,
        ),
        snapshot_ttl_ms=_int(payload.get("snapshot_ttl_ms"), base.snapshot_ttl_ms),
        price_ttl_ms=_int(payload.get("price_ttl_ms"), base.price_ttl_ms),
        run_lock_ttl_ms=_int(payload.get("run_lock_ttl_ms"), base.run_lock_ttl_ms),
        simulation_result_ttl_ms=_int(
            payload.get("simulation_result_ttl_ms"),
            base.simulation_result_ttl_ms,
        ),
    )


def load_kanglong_symbol_config(settings: Settings, symbol: str) -> KanglongSymbolConfig:
    normalized = symbol.strip().upper()
    base = KanglongSymbolConfig()
    path = settings.kanglong_symbol_configs_file
    if not path.exists():
        return base
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return base
    if not isinstance(payload, dict):
        return base
    raw = payload.get(normalized)
    if not isinstance(raw, dict):
        return base
    return _apply_overrides(base, raw)
