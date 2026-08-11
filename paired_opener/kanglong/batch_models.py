from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from paired_opener.domain import PositionSide


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    raise TypeError(f"unsupported batch plan value: {type(value)!r}")


def stable_payload_hash(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        default=_json_default,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class KanglongBatchAccountPlan:
    account_id: str
    sequence: int
    target_long_qty: Decimal
    target_short_qty: Decimal
    maker_fee_rate: Decimal
    taker_fee_rate: Decimal
    bracket_max_allowed_leverage: int
    bracket_notional_coef: Decimal
    selected_bracket_effective_cap: Decimal
    current_symbol_leverage: int
    current_symbol_max_notional_value: Decimal
    effective_capacity_leverage: int
    reference_mid_price: Decimal
    capacity_snapshot_id: str
    market_snapshot_id: str
    source_long_remaining_qty: Decimal = Decimal("0")
    source_short_remaining_qty: Decimal = Decimal("0")
    source_ledger_hash: str | None = None
    source_checkpoint_id: int | None = None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "KanglongBatchAccountPlan":
        decimal_fields = {
            "target_long_qty",
            "target_short_qty",
            "maker_fee_rate",
            "taker_fee_rate",
            "bracket_notional_coef",
            "selected_bracket_effective_cap",
            "current_symbol_max_notional_value",
            "reference_mid_price",
            "source_long_remaining_qty",
            "source_short_remaining_qty",
        }
        values = dict(payload)
        for field in decimal_fields:
            values[field] = Decimal(str(values.get(field) or "0"))
        return cls(**values)


@dataclass(frozen=True, slots=True)
class KanglongBatchPlan:
    run_id: str
    operation: Literal["open", "close"]
    symbol: str
    preferred_side: PositionSide
    requested_leverage: int
    per_leg_notional: Decimal
    accounts: tuple[KanglongBatchAccountPlan, ...]
    source_open_run_id: str | None
    credential_revision: str
    lock_scopes: tuple[str, ...]
    completed_prefix_length: int
    input_hash: str
    plan_version: str
    round_count: int = 30
    round_interval_seconds: int = 3
    blocked: bool = False
    blocked_reasons: tuple[str, ...] = ()
    open_capacity_check_applied: bool = True
    warning_codes: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["preferred_side"] = self.preferred_side.value
        return payload

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "KanglongBatchPlan":
        values = dict(payload)
        values["preferred_side"] = PositionSide(str(values["preferred_side"]))
        values["per_leg_notional"] = Decimal(str(values.get("per_leg_notional") or "0"))
        values["accounts"] = tuple(
            KanglongBatchAccountPlan.from_payload(item)
            for item in values.get("accounts") or []
        )
        values["lock_scopes"] = tuple(values.get("lock_scopes") or ())
        values["blocked_reasons"] = tuple(values.get("blocked_reasons") or ())
        values["warning_codes"] = tuple(values.get("warning_codes") or ())
        return cls(**values)
