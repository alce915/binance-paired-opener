from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import shutil
import threading
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any


KANGLONG_TEST_TEMPLATE_VERSION = 1

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_STORE_LOCK = threading.RLock()
_TEMPLATE_KNOWN_FIELDS = {
    "id",
    "name",
    "symbol",
    "main_account",
    "subaccounts",
    "created_at",
    "updated_at",
    "template_content_hash",
}
_DOCUMENT_KNOWN_FIELDS = {"version", "templates"}


class TemplateValidationError(ValueError):
    def __init__(self, code: str, field: str, value: Any) -> None:
        self.code = code
        self.field = field
        self.value = value
        super().__init__(str(self))

    def __str__(self) -> str:
        return f"{self.code}: invalid {self.field}={self.value!r}"


class TemplateStoreError(RuntimeError):
    def __init__(self, code: str, detail: dict[str, Any]) -> None:
        self.code = code
        self.detail = detail
        super().__init__(str(self))

    def __str__(self) -> str:
        return f"{self.code}: {self.detail}"


def canonical_decimal_text(value: Any, *, field_name: str = "decimal") -> str:
    try:
        decimal_value = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise TemplateValidationError("kanglong_test_template_invalid_decimal", field_name, value) from exc
    if not decimal_value.is_finite():
        raise TemplateValidationError("kanglong_test_template_invalid_decimal", field_name, value)
    if decimal_value == 0:
        return "0"
    return format(decimal_value.normalize(), "f")


def validate_template_identifier(value: Any, field_name: str = "id") -> str:
    if value is None:
        raise TemplateValidationError("kanglong_test_template_invalid_id", field_name, value)
    text = str(value).strip()
    if not text or _IDENTIFIER_RE.fullmatch(text) is None:
        raise TemplateValidationError("kanglong_test_template_invalid_id", field_name, value)
    return text


def runtime_main_account_id(template_id: str) -> str:
    normalized_template_id = validate_template_identifier(template_id, field_name="template_id")
    return f"tpl:{normalized_template_id}:main"


def runtime_subaccount_id(template_id: str, row_id: str) -> str:
    normalized_template_id = validate_template_identifier(template_id, field_name="template_id")
    normalized_row_id = validate_template_identifier(row_id, field_name="row_id")
    return f"tpl:{normalized_template_id}:sub:{normalized_row_id}"


def template_content_hash(template: dict[str, Any]) -> str:
    normalized = _normalize_hash_payload(template)
    raw = json.dumps(normalized, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def build_template_preview_payload(
    template: dict[str, Any],
    quote: Any,
    orderbook: Any,
    rules: Any,
    config: Any,
) -> dict[str, Any]:
    normalized = _normalize_loaded_template(template)
    symbol = normalized["symbol"]
    quote_bid, quote_ask = _quote_bid_ask(quote, symbol)
    best_bid, best_ask = _orderbook_top_bid_ask(orderbook, symbol)
    mark_price = (quote_bid + quote_ask) / Decimal("2")
    ttl_ms = int(getattr(config, "snapshot_ttl_ms"))
    fee_rate = _decimal_value(getattr(config, "fee_rate"), field_name="fee_rate")

    accounts: list[dict[str, Any]] = []
    rounding_residuals: list[dict[str, Any]] = []
    blocks: list[dict[str, Any]] = []
    main = normalized["main_account"]
    main_account_id = runtime_main_account_id(normalized["id"])
    main_collateral = _decimal_value(main["collateral"], field_name="main_account.collateral")
    accounts.append(
        {
            "id": main_account_id,
            "template_account_id": main["account_id"],
            "role": "main",
            "wallet_balance": _decimal_payload(main_collateral),
            "total_unrealized_pnl": "0",
            "equity": _decimal_payload(main_collateral),
            "available_balance": _decimal_payload(main_collateral),
            "margin": "0",
            "margin_deficit": "0",
            "positions": [],
        }
    )

    for subaccount in normalized["subaccounts"]:
        account_id = runtime_subaccount_id(normalized["id"], subaccount["row_id"])
        raw_qty = _decimal_value(subaccount["qty"], field_name="subaccounts.qty")
        rounded_qty = _round_down_to_step(raw_qty, rules.step_size)
        leverage = int(subaccount["leverage"])
        collateral = _decimal_value(subaccount["collateral"], field_name="subaccounts.collateral")
        long_entry_price = _decimal_value(subaccount["long_entry_price"], field_name="subaccounts.long_entry_price")
        short_entry_price = _decimal_value(subaccount["short_entry_price"], field_name="subaccounts.short_entry_price")

        if rounded_qty != raw_qty:
            rounding_residuals.append(
                {
                    "account_id": account_id,
                    "side": "BOTH",
                    "raw_qty": _decimal_payload(raw_qty),
                    "rounded_qty": _decimal_payload(rounded_qty),
                }
            )
        if rounded_qty <= 0:
            blocks.append({"code": "kanglong_test_template_non_positive_qty", "account_id": account_id})
        if rounded_qty < rules.min_qty:
            blocks.append({"code": "kanglong_test_template_min_qty_not_met", "account_id": account_id})
        if mark_price * rounded_qty < rules.min_notional:
            blocks.append({"code": "kanglong_test_template_min_notional_not_met", "account_id": account_id})
        if leverage > rules.max_leverage:
            blocks.append({"code": "kanglong_test_template_leverage_exceeded", "account_id": account_id})
        for side, entry_price in (("LONG", long_entry_price), ("SHORT", short_entry_price)):
            if not _is_aligned_to_increment(entry_price, rules.tick_size):
                blocks.append(
                    {
                        "code": "kanglong_test_template_invalid_price",
                        "account_id": account_id,
                        "side": side,
                    }
                )

        long_position = _position_snapshot(
            symbol=symbol,
            side="LONG",
            qty=rounded_qty,
            entry_price=long_entry_price,
            mark_price=mark_price,
            leverage=leverage,
        )
        short_position = _position_snapshot(
            symbol=symbol,
            side="SHORT",
            qty=rounded_qty,
            entry_price=short_entry_price,
            mark_price=mark_price,
            leverage=leverage,
        )
        total_unrealized_pnl = _decimal_value(long_position["unrealized_pnl"], field_name="unrealized_pnl") + _decimal_value(
            short_position["unrealized_pnl"],
            field_name="unrealized_pnl",
        )
        margin = _decimal_value(long_position["margin"], field_name="margin") + _decimal_value(
            short_position["margin"],
            field_name="margin",
        )
        equity = collateral + total_unrealized_pnl
        available_balance = max(equity - margin, Decimal("0"))
        margin_deficit = abs(equity - margin) if equity - margin < 0 else Decimal("0")
        accounts.append(
            {
                "id": account_id,
                "template_account_id": subaccount["account_id"],
                "role": "subaccount",
                "wallet_balance": _decimal_payload(collateral),
                "total_unrealized_pnl": _decimal_payload(total_unrealized_pnl),
                "equity": _decimal_payload(equity),
                "available_balance": _decimal_payload(available_balance),
                "margin": _decimal_payload(margin),
                "margin_deficit": _decimal_payload(margin_deficit),
                "positions": [long_position, short_position],
            }
        )

    payload = {
        "template_id": normalized["id"],
        "template_content_hash": normalized["template_content_hash"],
        "symbol": symbol,
        "account_source": "test_template",
        "fee_rate_source": "kanglong_symbol_config",
        "fee_rate": _decimal_payload(fee_rate),
        "mark_price_snapshot": {
            "mark_price": _decimal_payload(mark_price),
            "mark_price_source": "quote_mid",
            "quote_bid_price": _decimal_payload(quote_bid),
            "quote_ask_price": _decimal_payload(quote_ask),
            "ttl_ms": ttl_ms,
        },
        "execution_orderbook_snapshot": {
            "source": "orderbook_top",
            "best_bid_price": _decimal_payload(best_bid),
            "best_ask_price": _decimal_payload(best_ask),
            "ttl_ms": ttl_ms,
        },
        "symbol_rules": {
            "step_size": _decimal_payload(rules.step_size),
            "tick_size": _decimal_payload(rules.tick_size),
            "min_qty": _decimal_payload(rules.min_qty),
            "min_notional": _decimal_payload(rules.min_notional),
            "max_leverage": int(rules.max_leverage),
        },
        "accounts": accounts,
        "rounding_residuals": rounding_residuals,
        "warnings": [],
        "blocks": blocks,
    }
    payload["snapshot_bundle_id"] = _snapshot_bundle_id(payload)
    return payload


def _quote_bid_ask(quote: Any, symbol: str) -> tuple[Decimal, Decimal]:
    bid = _maybe_decimal(_get_value(quote, "bid_price"))
    ask = _maybe_decimal(_get_value(quote, "ask_price"))
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        raise TemplateStoreError(
            "kanglong_test_template_quote_unavailable",
            {
                "symbol": symbol,
                "bid_price": _optional_decimal_payload(bid),
                "ask_price": _optional_decimal_payload(ask),
            },
        )
    return bid, ask


def _orderbook_top_bid_ask(orderbook: Any, symbol: str) -> tuple[Decimal, Decimal]:
    bid = _top_level_price(_get_value(orderbook, "bids"))
    ask = _top_level_price(_get_value(orderbook, "asks"))
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        raise TemplateStoreError(
            "kanglong_test_template_orderbook_unavailable",
            {
                "symbol": symbol,
                "best_bid_price": _optional_decimal_payload(bid),
                "best_ask_price": _optional_decimal_payload(ask),
            },
        )
    return bid, ask


def _top_level_price(levels: Any) -> Decimal | None:
    if not levels:
        return None
    try:
        level = levels[0]
    except (KeyError, TypeError, IndexError):
        return None
    if isinstance(level, dict):
        return _maybe_decimal(level.get("price"))
    if isinstance(level, (list, tuple)) and level:
        return _maybe_decimal(level[0])
    return _maybe_decimal(_get_value(level, "price"))


def _get_value(source: Any, key: str) -> Any:
    if isinstance(source, dict):
        return source.get(key)
    return getattr(source, key, None)


def _maybe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        decimal_value = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not decimal_value.is_finite():
        return None
    return decimal_value


def _round_down_to_step(value: Decimal, step_size: Decimal) -> Decimal:
    if step_size <= 0:
        return value
    return (value / step_size).to_integral_value(rounding=ROUND_DOWN) * step_size


def _is_aligned_to_increment(value: Decimal, increment: Decimal) -> bool:
    if increment <= 0:
        return True
    return value == _round_down_to_step(value, increment)


def _position_snapshot(
    *,
    symbol: str,
    side: str,
    qty: Decimal,
    entry_price: Decimal,
    mark_price: Decimal,
    leverage: int,
) -> dict[str, Any]:
    unrealized_pnl = (mark_price - entry_price) * qty if side == "LONG" else (entry_price - mark_price) * qty
    notional = mark_price * qty
    margin = abs(notional) / Decimal(leverage)
    return {
        "symbol": symbol,
        "position_side": side,
        "qty": _decimal_payload(qty),
        "entry_price": _decimal_payload(entry_price),
        "mark_price": _decimal_payload(mark_price),
        "unrealized_pnl": _decimal_payload(unrealized_pnl),
        "notional": _decimal_payload(notional),
        "margin": _decimal_payload(margin),
    }


def _snapshot_bundle_id(payload: dict[str, Any]) -> str:
    bundle_payload = {
        "template_content_hash": payload["template_content_hash"],
        "mark_price_snapshot": payload["mark_price_snapshot"],
        "execution_orderbook_snapshot": payload["execution_orderbook_snapshot"],
        "symbol_rules": payload["symbol_rules"],
        "accounts": payload["accounts"],
    }
    raw = json.dumps(bundle_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:24]


def _decimal_payload(value: Decimal) -> str:
    return canonical_decimal_text(value)


def _optional_decimal_payload(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return _decimal_payload(value)


class KanglongTemplateStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    @property
    def backup_path(self) -> Path:
        return self.path.with_suffix(self.path.suffix + ".bak")

    def list_templates(self) -> dict[str, Any]:
        document = self._read_document()
        return {
            "version": document["version"],
            "templates": copy.deepcopy(document["templates"]),
            "recoverable_backup": self.backup_path.exists(),
        }

    def get_template(self, template_id: str) -> dict[str, Any]:
        normalized_id = validate_template_identifier(template_id, field_name="template_id")
        for template in self.list_templates()["templates"]:
            if template.get("id") == normalized_id:
                return template
        raise TemplateStoreError("kanglong_test_template_not_found", {"template_id": normalized_id})

    def upsert_template(self, template: dict[str, Any]) -> dict[str, Any]:
        with _STORE_LOCK:
            document = self._read_document()
            templates = document["templates"]
            template_id = validate_template_identifier(template.get("id"), field_name="template_id")
            existing_index = next((index for index, item in enumerate(templates) if item.get("id") == template_id), None)
            existing = templates[existing_index] if existing_index is not None else None
            normalized = _normalize_template(template, existing=existing)
            if existing_index is None:
                templates.append(normalized)
            else:
                templates[existing_index] = normalized
            self._write_document(_document_for_write(document, templates))
            return copy.deepcopy(normalized)

    def clone_template(self, template_id: str, new_id: str | None = None) -> dict[str, Any]:
        with _STORE_LOCK:
            source = self.get_template(template_id)
            document = self._read_document()
            existing_ids = {item.get("id") for item in document["templates"]}
            clone_id = validate_template_identifier(new_id, field_name="template_id") if new_id else _next_clone_id(source["id"], existing_ids)
            if clone_id in existing_ids:
                raise TemplateValidationError("kanglong_test_template_invalid_id", "template_id", clone_id)
            cloned = copy.deepcopy(source)
            cloned["id"] = clone_id
            cloned["name"] = f'{source.get("name", source["id"])} Copy'
            cloned.pop("created_at", None)
            cloned.pop("updated_at", None)
            cloned.pop("template_content_hash", None)
            for subaccount in cloned.get("subaccounts", []):
                subaccount["row_id"] = f"row_{uuid.uuid4().hex[:12]}"
            return self.upsert_template(cloned)

    def delete_template(self, template_id: str) -> dict[str, Any]:
        with _STORE_LOCK:
            normalized_id = validate_template_identifier(template_id, field_name="template_id")
            document = self._read_document()
            remaining = [item for item in document["templates"] if item.get("id") != normalized_id]
            if len(remaining) == len(document["templates"]):
                raise TemplateStoreError("kanglong_test_template_not_found", {"template_id": normalized_id})
            deleted = next(item for item in document["templates"] if item.get("id") == normalized_id)
            self._write_document(_document_for_write(document, remaining))
            return copy.deepcopy(deleted)

    def recover_backup(self) -> dict[str, Any]:
        with _STORE_LOCK:
            if not self.backup_path.exists():
                raise TemplateStoreError(
                    "kanglong_test_template_not_found",
                    {"backup": str(self.backup_path)},
                )
            self._read_document_from_path(self.backup_path)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._temp_path()
            try:
                shutil.copy2(self.backup_path, tmp_path)
                os.replace(tmp_path, self.path)
            finally:
                if tmp_path.exists():
                    tmp_path.unlink()
            return self.list_templates()

    def _read_document(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"version": KANGLONG_TEST_TEMPLATE_VERSION, "templates": []}
        return self._read_document_from_path(self.path)

    def _read_document_from_path(self, path: Path) -> dict[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except json.JSONDecodeError as exc:
            raise TemplateStoreError(
                "kanglong_test_template_store_corrupted",
                {"path": str(path), "error": str(exc)},
            ) from exc
        except OSError as exc:
            raise TemplateStoreError(
                "kanglong_test_template_store_unreadable",
                {"path": str(path), "error": str(exc)},
            ) from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("templates"), list):
            raise TemplateStoreError(
                "kanglong_test_template_store_corrupted",
                {"path": str(path), "error": "expected object with templates list"},
            )
        version = payload.get("version", KANGLONG_TEST_TEMPLATE_VERSION)
        if not isinstance(version, int):
            raise TemplateStoreError(
                "kanglong_test_template_store_corrupted",
                {"path": str(path), "error": "expected integer version"},
            )
        if version > KANGLONG_TEST_TEMPLATE_VERSION:
            raise TemplateStoreError(
                "kanglong_test_template_unsupported_version",
                {"path": str(path), "version": version},
            )
        document = {key: copy.deepcopy(value) for key, value in payload.items() if key not in _DOCUMENT_KNOWN_FIELDS}
        document.update(
            {
                "version": version,
                "templates": [_normalize_loaded_template(item) for item in payload["templates"]],
            }
        )
        return document

    def _write_document(self, document: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._temp_path()
        try:
            tmp_path.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            if self.path.exists():
                shutil.copy2(self.path, self.backup_path)
            os.replace(tmp_path, self.path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

    def _temp_path(self) -> Path:
        return self.path.with_name(f".{self.path.name}.{uuid.uuid4().hex}.tmp")


def _normalize_hash_payload(template: dict[str, Any]) -> dict[str, Any]:
    main_account = _require_mapping(template.get("main_account"), "main_account")
    return {
        "symbol": str(template.get("symbol", "")).strip().upper(),
        "main_account": {
            "account_id": validate_template_identifier(main_account.get("account_id"), field_name="main_account.account_id"),
            "collateral": _collateral_text(main_account.get("collateral", "0"), field_name="main_account.collateral"),
            "leverage": _positive_int(main_account.get("leverage", 0), field_name="main_account.leverage"),
        },
        "subaccounts": sorted(
            (
                {
                    "row_id": validate_template_identifier(item.get("row_id"), field_name="subaccounts.row_id"),
                    "account_id": validate_template_identifier(item.get("account_id"), field_name="subaccounts.account_id"),
                    "collateral": _collateral_text(item.get("collateral", "0"), field_name="subaccounts.collateral"),
                    "leverage": _positive_int(item.get("leverage", 0), field_name="subaccounts.leverage"),
                    "long_entry_price": _positive_decimal_text(
                        item.get("long_entry_price", "0"),
                        field_name="subaccounts.long_entry_price",
                        code="kanglong_test_template_invalid_price",
                    ),
                    "short_entry_price": _positive_decimal_text(
                        item.get("short_entry_price", "0"),
                        field_name="subaccounts.short_entry_price",
                        code="kanglong_test_template_invalid_price",
                    ),
                    "qty": _positive_decimal_text(
                        item.get("qty", "0"),
                        field_name="subaccounts.qty",
                        code="kanglong_test_template_non_positive_qty",
                    ),
                }
                for item in _require_list(template.get("subaccounts"), "subaccounts")
            ),
            key=lambda item: item["row_id"],
        ),
    }


def _document_for_write(document: dict[str, Any], templates: list[dict[str, Any]]) -> dict[str, Any]:
    preserved = {key: copy.deepcopy(value) for key, value in document.items() if key not in _DOCUMENT_KNOWN_FIELDS}
    preserved.update({"version": KANGLONG_TEST_TEMPLATE_VERSION, "templates": templates})
    return preserved


def _normalize_template(template: dict[str, Any], *, existing: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(template, dict):
        raise TemplateValidationError("kanglong_test_template_invalid_template", "template", template)
    now = _fresh_timestamp(existing.get("updated_at") if existing else None)
    template_id = validate_template_identifier(template.get("id"), field_name="template_id")
    created_at = existing.get("created_at") if existing else template.get("created_at")
    normalized = _preserved_template_fields(existing, template)
    normalized.update(
        {
        "id": template_id,
        "name": str(template.get("name", template_id)),
        "symbol": str(template.get("symbol", "")).strip().upper(),
        "main_account": _normalize_main_account(template.get("main_account")),
        "subaccounts": _normalize_subaccounts(template.get("subaccounts")),
        "created_at": str(created_at) if created_at else now,
        "updated_at": now,
        }
    )
    normalized["template_content_hash"] = template_content_hash(normalized)
    return normalized


def _normalize_loaded_template(template: Any) -> dict[str, Any]:
    source = _require_mapping(template, "templates[]")
    timestamp = str(source.get("updated_at") or source.get("created_at") or _fresh_timestamp(None))
    normalized = {key: copy.deepcopy(value) for key, value in source.items() if key not in _TEMPLATE_KNOWN_FIELDS}
    normalized.update(
        {
        "id": validate_template_identifier(source.get("id"), field_name="template_id"),
        "name": str(source.get("name", source.get("id", ""))),
        "symbol": str(source.get("symbol", "")).strip().upper(),
        "main_account": _normalize_main_account(source.get("main_account")),
        "subaccounts": _normalize_subaccounts(source.get("subaccounts")),
        "created_at": str(source.get("created_at") or timestamp),
        "updated_at": timestamp,
        }
    )
    normalized["template_content_hash"] = template_content_hash(normalized)
    return normalized


def _normalize_main_account(value: Any) -> dict[str, Any]:
    account = _require_mapping(value, "main_account")
    normalized = copy.deepcopy(account)
    normalized.update(
        {
        "account_id": validate_template_identifier(account.get("account_id"), field_name="main_account.account_id"),
        "name": str(account.get("name", "")),
        "collateral": _collateral_text(account.get("collateral", "0"), field_name="main_account.collateral"),
        "leverage": _positive_int(account.get("leverage", 0), field_name="main_account.leverage"),
        "positions": copy.deepcopy(account.get("positions", [])),
        }
    )
    return normalized


def _normalize_subaccounts(value: Any) -> list[dict[str, Any]]:
    subaccounts = _require_list(value, "subaccounts")
    normalized: list[dict[str, Any]] = []
    used_row_ids: set[str] = set()
    for index, item in enumerate(subaccounts, start=1):
        subaccount = _require_mapping(item, f"subaccounts[{index - 1}]")
        if "row_id" in subaccount and subaccount.get("row_id") is not None:
            row_id = validate_template_identifier(subaccount.get("row_id"), field_name="subaccounts.row_id")
            if row_id in used_row_ids:
                raise TemplateValidationError("kanglong_test_template_invalid_id", "subaccounts.row_id", row_id)
        else:
            raw_row_id = subaccount.get("account_id") or f"row-{index}"
            row_id = _unique_row_id(validate_template_identifier(raw_row_id, field_name="subaccounts.row_id"), used_row_ids)
        used_row_ids.add(row_id)
        normalized_subaccount = copy.deepcopy(subaccount)
        normalized_subaccount.update(
            {
                "row_id": row_id,
                "account_id": validate_template_identifier(
                    subaccount.get("account_id"),
                    field_name="subaccounts.account_id",
                ),
                "name": str(subaccount.get("name", "")),
                "collateral": _collateral_text(subaccount.get("collateral", "0"), field_name="subaccounts.collateral"),
                "leverage": _positive_int(subaccount.get("leverage", 0), field_name="subaccounts.leverage"),
                "long_entry_price": _positive_decimal_text(
                    subaccount.get("long_entry_price", "0"),
                    field_name="subaccounts.long_entry_price",
                    code="kanglong_test_template_invalid_price",
                ),
                "short_entry_price": _positive_decimal_text(
                    subaccount.get("short_entry_price", "0"),
                    field_name="subaccounts.short_entry_price",
                    code="kanglong_test_template_invalid_price",
                ),
                "qty": _positive_decimal_text(
                    subaccount.get("qty", "0"),
                    field_name="subaccounts.qty",
                    code="kanglong_test_template_non_positive_qty",
                ),
            }
        )
        normalized.append(normalized_subaccount)
    return normalized


def _preserved_template_fields(existing: dict[str, Any] | None, template: dict[str, Any]) -> dict[str, Any]:
    preserved: dict[str, Any] = {}
    if existing:
        preserved.update({key: copy.deepcopy(value) for key, value in existing.items() if key not in _TEMPLATE_KNOWN_FIELDS})
    preserved.update({key: copy.deepcopy(value) for key, value in template.items() if key not in _TEMPLATE_KNOWN_FIELDS})
    return preserved


def _collateral_text(value: Any, *, field_name: str) -> str:
    decimal_value = _decimal_value(value, field_name=field_name)
    if decimal_value < 0:
        raise TemplateValidationError("kanglong_test_template_negative_collateral", field_name, value)
    return canonical_decimal_text(decimal_value, field_name=field_name)


def _positive_decimal_text(value: Any, *, field_name: str, code: str) -> str:
    decimal_value = _decimal_value(value, field_name=field_name)
    if decimal_value <= 0:
        raise TemplateValidationError(code, field_name, value)
    return canonical_decimal_text(decimal_value, field_name=field_name)


def _positive_int(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise TemplateValidationError("kanglong_test_template_invalid_leverage", field_name, value)
    if isinstance(value, int):
        integer_value = value
    elif isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()):
        integer_value = int(value.strip())
    else:
        raise TemplateValidationError("kanglong_test_template_invalid_leverage", field_name, value)
    if integer_value <= 0:
        raise TemplateValidationError("kanglong_test_template_invalid_leverage", field_name, value)
    return integer_value


def _decimal_value(value: Any, *, field_name: str) -> Decimal:
    try:
        decimal_value = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise TemplateValidationError("kanglong_test_template_invalid_decimal", field_name, value) from exc
    if not decimal_value.is_finite():
        raise TemplateValidationError("kanglong_test_template_invalid_decimal", field_name, value)
    return decimal_value


def _unique_row_id(row_id: str, used_row_ids: set[str]) -> str:
    if row_id not in used_row_ids:
        return row_id
    suffix = 2
    while f"{row_id}-{suffix}" in used_row_ids:
        suffix += 1
    return f"{row_id}-{suffix}"


def _require_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TemplateValidationError("kanglong_test_template_invalid_template", field_name, value)
    return value


def _require_list(value: Any, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise TemplateValidationError("kanglong_test_template_invalid_template", field_name, value)
    return value


def _next_clone_id(source_id: str, existing_ids: set[Any]) -> str:
    base = f"{source_id}_copy"
    if base not in existing_ids:
        return base
    suffix = 2
    while f"{base}_{suffix}" in existing_ids:
        suffix += 1
    return f"{base}_{suffix}"


def _fresh_timestamp(previous: Any) -> str:
    now = datetime.now(UTC)
    if previous:
        try:
            previous_dt = datetime.fromisoformat(str(previous))
            if previous_dt.tzinfo is None:
                previous_dt = previous_dt.replace(tzinfo=UTC)
            if now <= previous_dt:
                now = previous_dt + timedelta(microseconds=1)
        except ValueError:
            pass
    return now.isoformat(timespec="microseconds")
