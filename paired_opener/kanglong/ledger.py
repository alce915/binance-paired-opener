from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

DECIMAL_PLACES = 9
HASH_PREFIX = "sha256:"

KANGLONG_LEDGER_ENTRY_TYPES = {
    "close_position",
    "open_position",
    "fee",
    "price_wear",
    "residual",
    "control",
    "report",
}

_DECIMAL_FIELDS_BASELINE = (
    "wallet_balance",
    "available_balance",
    "equity",
    "margin",
    "margin_deficit",
    "total_unrealized_pnl",
    "long_qty",
    "long_entry_price",
    "long_mark_price",
    "short_qty",
    "short_entry_price",
    "short_mark_price",
)

_DECIMAL_FIELDS_ENTRY = (
    "amount",
    "qty_delta",
    "margin_delta",
    "available_delta",
    "equity_delta",
    "realized_pnl_delta",
    "price_wear",
    "fee_amount",
)

_NUMERIC_JSON_KEY_FRAGMENTS = (
    "amount",
    "available",
    "balance",
    "delta",
    "equity",
    "fee",
    "margin",
    "notional",
    "percent",
    "pnl",
    "price",
    "qty",
    "quantity",
    "rate",
    "wear",
)


def _decimal(value: Decimal | str | int | float | None) -> Decimal:
    if value is None:
        value = "0"
    if isinstance(value, Decimal):
        source = value
    else:
        source = Decimal(str(value))
    quantized = source.quantize(Decimal("1").scaleb(-DECIMAL_PLACES), rounding=ROUND_HALF_UP)
    return abs(quantized) if quantized == 0 else quantized


def canonical_decimal(value: Decimal | str | int | float | None, places: int = DECIMAL_PLACES) -> str:
    source = Decimal("0") if value is None else Decimal(str(value))
    quantized = source.quantize(Decimal("1").scaleb(-int(places)), rounding=ROUND_HALF_UP)
    if quantized == 0:
        quantized = abs(quantized)
    return format(quantized, "f")


def _storage_decimal(value: Decimal | str | int | float | None) -> str:
    text = canonical_decimal(value, DECIMAL_PLACES)
    if "." not in text:
        return text
    return text.rstrip("0").rstrip(".") or "0"


def _is_numeric_json_key(key_hint: str | None) -> bool:
    if not key_hint:
        return False
    normalized = key_hint.lower()
    return any(fragment in normalized for fragment in _NUMERIC_JSON_KEY_FRAGMENTS)


def _normalize_json_value(value: Any, *, key_hint: str | None = None) -> Any:
    if isinstance(value, Decimal):
        return canonical_decimal(value)
    if isinstance(value, dict):
        return {str(key): _normalize_json_value(value[key], key_hint=str(key)) for key in sorted(value)}
    if isinstance(value, list):
        return [_normalize_json_value(item, key_hint=key_hint) for item in value]
    if isinstance(value, tuple):
        return [_normalize_json_value(item, key_hint=key_hint) for item in value]
    if isinstance(value, str) and _is_numeric_json_key(key_hint):
        try:
            return canonical_decimal(Decimal(value))
        except (InvalidOperation, ValueError):
            return value
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(
        _normalize_json_value(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _sha256(value: Any) -> str:
    digest = hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
    return f"{HASH_PREFIX}{digest}"


@dataclass(frozen=True, slots=True)
class KanglongLedgerBaseline:
    run_id: str
    account_id: str
    wallet_balance: Decimal
    available_balance: Decimal
    equity: Decimal
    margin: Decimal
    margin_deficit: Decimal
    total_unrealized_pnl: Decimal
    long_qty: Decimal
    long_entry_price: Decimal
    long_mark_price: Decimal
    long_leverage: int
    short_qty: Decimal
    short_entry_price: Decimal
    short_mark_price: Decimal
    short_leverage: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", str(self.run_id))
        object.__setattr__(self, "account_id", str(self.account_id))
        object.__setattr__(self, "long_leverage", int(self.long_leverage))
        object.__setattr__(self, "short_leverage", int(self.short_leverage))
        for field_name in _DECIMAL_FIELDS_BASELINE:
            object.__setattr__(self, field_name, _decimal(getattr(self, field_name)))

    def to_hash_payload(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "account_id": self.account_id,
            **{field_name: canonical_decimal(getattr(self, field_name)) for field_name in _DECIMAL_FIELDS_BASELINE},
            "long_leverage": self.long_leverage,
            "short_leverage": self.short_leverage,
        }

    def to_storage_payload(self) -> dict[str, Any]:
        payload = {
            "run_id": self.run_id,
            "account_id": self.account_id,
            **{field_name: _storage_decimal(getattr(self, field_name)) for field_name in _DECIMAL_FIELDS_BASELINE},
            "long_leverage": self.long_leverage,
            "short_leverage": self.short_leverage,
        }
        payload["baseline_hash"] = hash_baseline(self)
        return payload


@dataclass(frozen=True, slots=True)
class KanglongLedgerEntry:
    run_id: str
    checkpoint_id: int
    sequence: int
    operation_id: str
    account_id: str | None
    entry_type: str
    asset: str | None = None
    amount: Decimal = Decimal("0")
    qty_delta: Decimal = Decimal("0")
    margin_delta: Decimal = Decimal("0")
    available_delta: Decimal = Decimal("0")
    equity_delta: Decimal = Decimal("0")
    realized_pnl_delta: Decimal = Decimal("0")
    price_wear: Decimal = Decimal("0")
    fee_amount: Decimal = Decimal("0")
    fee_asset: str | None = None
    operation_payload_hash: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        entry_type = str(self.entry_type)
        if entry_type not in KANGLONG_LEDGER_ENTRY_TYPES:
            raise ValueError("kanglong_invalid_ledger_entry_type")
        object.__setattr__(self, "run_id", str(self.run_id))
        object.__setattr__(self, "checkpoint_id", int(self.checkpoint_id))
        object.__setattr__(self, "sequence", int(self.sequence))
        object.__setattr__(self, "operation_id", str(self.operation_id))
        object.__setattr__(self, "account_id", str(self.account_id) if self.account_id is not None else None)
        object.__setattr__(self, "entry_type", entry_type)
        object.__setattr__(self, "asset", str(self.asset) if self.asset is not None else None)
        object.__setattr__(self, "fee_asset", str(self.fee_asset) if self.fee_asset is not None else None)
        object.__setattr__(self, "payload", _normalize_json_value(self.payload or {}))
        for field_name in _DECIMAL_FIELDS_ENTRY:
            object.__setattr__(self, field_name, _decimal(getattr(self, field_name)))
        if not self.operation_payload_hash:
            object.__setattr__(self, "operation_payload_hash", hash_operation_payload(self.payload))

    def to_hash_payload(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "checkpoint_id": self.checkpoint_id,
            "sequence": self.sequence,
            "operation_id": self.operation_id,
            "account_id": self.account_id,
            "entry_type": self.entry_type,
            "asset": self.asset,
            **{field_name: canonical_decimal(getattr(self, field_name)) for field_name in _DECIMAL_FIELDS_ENTRY},
            "fee_asset": self.fee_asset,
            "operation_payload_hash": self.operation_payload_hash,
            "payload": self.payload,
        }

    def to_storage_payload(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "checkpoint_id": self.checkpoint_id,
            "sequence": self.sequence,
            "operation_id": self.operation_id,
            "account_id": self.account_id,
            "entry_type": self.entry_type,
            "asset": self.asset,
            **{field_name: _storage_decimal(getattr(self, field_name)) for field_name in _DECIMAL_FIELDS_ENTRY},
            "fee_asset": self.fee_asset,
            "operation_payload_hash": self.operation_payload_hash,
            "payload": self.payload,
        }


@dataclass(frozen=True, slots=True)
class KanglongCheckpoint:
    run_id: str
    checkpoint_id: int
    previous_ledger_hash: str
    ledger_hash: str
    ledger_state_hash: str
    ledger_entry_count: int

    @classmethod
    def from_entries(
        cls,
        *,
        run_id: str,
        checkpoint_id: int,
        previous_ledger_hash: str,
        entries: list[KanglongLedgerEntry],
        baselines: list[KanglongLedgerBaseline],
    ) -> KanglongCheckpoint:
        return cls(
            run_id=str(run_id),
            checkpoint_id=int(checkpoint_id),
            previous_ledger_hash=str(previous_ledger_hash),
            ledger_hash=hash_checkpoint(previous_ledger_hash, entries),
            ledger_state_hash=hash_ledger_state(baselines, entries),
            ledger_entry_count=len(entries),
        )


def hash_baseline(baseline: KanglongLedgerBaseline) -> str:
    return _sha256({"kind": "kanglong_ledger_baseline_v1", "baseline": baseline.to_hash_payload()})


def hash_operation_payload(payload: dict[str, Any]) -> str:
    return _sha256({"kind": "kanglong_operation_payload_v1", "payload": payload})


def hash_checkpoint(previous_ledger_hash: str, entries: list[KanglongLedgerEntry]) -> str:
    return _sha256(
        {
            "kind": "kanglong_checkpoint_v1",
            "previous_ledger_hash": str(previous_ledger_hash),
            "entries": [entry.to_hash_payload() for entry in entries],
        }
    )


def hash_ledger_state(
    baselines: list[KanglongLedgerBaseline],
    entries: list[KanglongLedgerEntry],
) -> str:
    return _sha256(
        {
            "kind": "kanglong_ledger_state_v1",
            "baselines": [
                baseline.to_hash_payload()
                for baseline in sorted(baselines, key=lambda item: item.account_id)
            ],
            "entries": [entry.to_hash_payload() for entry in entries],
        }
    )


def ledger_entry_from_storage_payload(payload: dict[str, Any]) -> KanglongLedgerEntry:
    source = dict(payload)
    if "payload_json" in source and "payload" not in source:
        source["payload"] = json.loads(source.pop("payload_json") or "{}")
    source.pop("entry_id", None)
    source.pop("created_at", None)
    return KanglongLedgerEntry(
        run_id=source["run_id"],
        checkpoint_id=int(source["checkpoint_id"]),
        sequence=int(source["sequence"]),
        operation_id=str(source["operation_id"]),
        account_id=source.get("account_id"),
        entry_type=str(source["entry_type"]),
        asset=source.get("asset"),
        amount=source.get("amount", "0"),
        qty_delta=source.get("qty_delta", "0"),
        margin_delta=source.get("margin_delta", "0"),
        available_delta=source.get("available_delta", "0"),
        equity_delta=source.get("equity_delta", "0"),
        realized_pnl_delta=source.get("realized_pnl_delta", "0"),
        price_wear=source.get("price_wear", "0"),
        fee_amount=source.get("fee_amount", "0"),
        fee_asset=source.get("fee_asset"),
        operation_payload_hash=source.get("operation_payload_hash"),
        payload=source.get("payload") or {},
    )


def baseline_from_storage_payload(payload: dict[str, Any]) -> KanglongLedgerBaseline:
    source = dict(payload)
    source.pop("baseline_hash", None)
    source.pop("created_at", None)
    return KanglongLedgerBaseline(
        run_id=source["run_id"],
        account_id=source["account_id"],
        wallet_balance=source.get("wallet_balance", "0"),
        available_balance=source.get("available_balance", "0"),
        equity=source.get("equity", "0"),
        margin=source.get("margin", "0"),
        margin_deficit=source.get("margin_deficit", "0"),
        total_unrealized_pnl=source.get("total_unrealized_pnl", "0"),
        long_qty=source.get("long_qty", "0"),
        long_entry_price=source.get("long_entry_price", "0"),
        long_mark_price=source.get("long_mark_price", "0"),
        long_leverage=int(source.get("long_leverage", 1)),
        short_qty=source.get("short_qty", "0"),
        short_entry_price=source.get("short_entry_price", "0"),
        short_mark_price=source.get("short_mark_price", "0"),
        short_leverage=int(source.get("short_leverage", 1)),
    )
