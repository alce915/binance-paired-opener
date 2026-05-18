# Kanglong Test Template Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build persistent Kanglong test scenario templates that replace the Kanglong account pool with synthetic accounts while still using real quote, orderbook, and symbol-rule data for realistic simulation.

**Architecture:** Add a dedicated template store and snapshot provider under `paired_opener/kanglong/`, then branch Kanglong plan input collection by `account_source`. Real gateways are used only for market data in `test_template` mode; synthetic template accounts become frozen run snapshots and later synthetic execution state.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, Decimal arithmetic, atomic JSON file storage, SQLite run-state JSON columns, existing Kanglong planner/executor, vanilla JS frontend, existing i18n registries, pytest, and node-based frontend checks.

---

## Scope Boundaries

- Work in the main checkout `D:\codex\币安自动开单系统`, per user instruction.
- Keep the first version simulation-only.
- Do not allow synthetic template accounts and real trading accounts in the same Kanglong account pool.
- Do not place live orders from template mode.
- Do not build a second Kanglong planner. Convert template input to `KanglongAccountSnapshot` and reuse the current planner.
- Do not call real account-level gateway methods with `tpl:...` account IDs.
- Use real market data only through the user-selected `market_data_account_id`.
- Keep all new user-visible Chinese text in i18n files or registries.
- Store template data in local JSON only; do not sync to cloud or save API keys.

## File Structure

Create:

- `paired_opener/kanglong/test_templates.py`: template dataclasses, canonical Decimal/ID validation, content hash, atomic JSON store, `.bak` recovery, row-id migration, preview snapshot builder entrypoint helpers.
- `tests/test_kanglong_test_templates.py`: unit tests for storage, hash stability, migration, validation, recovery, and preview math.
- `tests/test_kanglong_test_template_api.py`: FastAPI contract tests for list/create/update/clone/delete/recover/preview and plan-mode branching.
- `tests/test_app_kanglong_test_templates.mjs`: frontend modal, account-pool replacement, active-run restore, and no-mixing contract tests.

Modify:

- `paired_opener/config.py`: add a configurable `kanglong_test_templates_file` path defaulting to `data/kanglong_test_templates.json`.
- `paired_opener/kanglong/config.py`: add `fee_rate: Decimal = Decimal("0.0005")` and config override support.
- `paired_opener/kanglong/models.py`: add payload helpers for template account source, synthetic account state, price source, and runtime account map where needed.
- `paired_opener/kanglong/snapshots.py`: reuse or extend snapshot payload conversion so template previews and existing account cards share one `positions[]` shape.
- `paired_opener/kanglong/service.py`: persist `account_source`, `test_template_id`, `template_content_hash`, `market_data_account_id`, `fee_rate_source`, frozen `account_snapshot`, and later `synthetic_account_state` in run state.
- `paired_opener/schemas.py`: add template management schemas and extend `KanglongPlanRequest` with `account_source`, `test_template_id`, `template_content_hash`, and `market_data_account_id`.
- `paired_opener/api.py`: add template routes and split `_collect_kanglong_plan_inputs` into runtime and test-template paths.
- `paired_opener/storage.py`: preserve existing `kanglong_runs` table; use JSON columns for new run metadata and synthetic state. No SQLite migration is needed for template mode.
- `paired_opener/static/index.html`: add the `测试模板` modal entry and modal structure without increasing the main Kanglong workspace height.
- `paired_opener/static/app.js`: add template library/edit/preview/apply state, API calls, account-source isolation, active-run restore, and modal interactions.
- `i18n/messages/zh-CN.json`: add `console.kanglong.test_template.*` and runtime labels.
- `i18n/registry/reasons.json`: add template blocking reason display keys.
- `i18n/registry/runtime.json` or the existing runtime registry file if present: add template runtime error display keys.
- `tests/test_kanglong_i18n_contracts.py`: require every new template code and UI key to be registered.
- `tests/test_kanglong_workflow_contracts.py`: extend run-state and active-run contracts for `test_template` source.
- `tests/test_kanglong_api.py`: ensure legacy runtime mode still behaves the same after request schema expansion.
- `tests/test_app_kanglong_display.mjs`: keep existing Kanglong workspace layout tests passing with the new modal entry.

## Public Contracts

Template store file:

```text
data/kanglong_test_templates.json
```

Runtime account IDs:

```text
tpl:{template_id}:main
tpl:{template_id}:sub:{row_id}
```

Template management routes:

```text
GET    /kanglong/simulation/test-templates
POST   /kanglong/simulation/test-templates
PUT    /kanglong/simulation/test-templates/{template_id}
POST   /kanglong/simulation/test-templates/{template_id}/clone
DELETE /kanglong/simulation/test-templates/{template_id}
POST   /kanglong/simulation/test-templates/{template_id}/preview
POST   /kanglong/simulation/test-templates/store/recover-backup
```

Plan request extension:

```json
{
  "mode": "simulation",
  "symbol": "ETHUSDC",
  "main_account_id": "tpl:tpl_eth_drop_001:main",
  "subaccount_ids": ["tpl:tpl_eth_drop_001:sub:sub-1"],
  "selected_side": null,
  "account_source": "test_template",
  "test_template_id": "tpl_eth_drop_001",
  "template_content_hash": "sha256:...",
  "market_data_account_id": "main"
}
```

Preview response minimum contract:

```json
{
  "template_id": "tpl_eth_drop_001",
  "template_content_hash": "sha256:...",
  "symbol": "ETHUSDC",
  "account_source": "test_template",
  "fee_rate_source": "kanglong_symbol_config",
  "fee_rate": "0.0005",
  "snapshot_bundle_id": "snap-...",
  "mark_price_snapshot": {
    "mark_price": "2443.21",
    "mark_price_source": "quote_mid",
    "quote_bid_price": "2443.20",
    "quote_ask_price": "2443.22",
    "ttl_ms": 5000
  },
  "execution_orderbook_snapshot": {
    "source": "orderbook_top",
    "best_bid_price": "2443.19",
    "best_ask_price": "2443.23",
    "ttl_ms": 5000
  },
  "symbol_rules": {
    "step_size": "0.001",
    "tick_size": "0.01",
    "min_qty": "0.001",
    "min_notional": "5",
    "max_leverage": 125
  },
  "accounts": [],
  "rounding_residuals": [],
  "warnings": [],
  "blocks": []
}
```

## Verification Commands

Run the relevant focused command after each task, then run the full set before final commit:

```powershell
python -m pytest tests/test_kanglong_test_templates.py -q
python -m pytest tests/test_kanglong_test_template_api.py -q
python -m pytest tests/test_kanglong_workflow_contracts.py tests/test_kanglong_api.py -q
python -m pytest tests/test_kanglong_i18n_contracts.py -q
node tests\test_app_kanglong_test_templates.mjs
node tests\test_app_kanglong_display.mjs
node --check paired_opener\static\app.js
```

Memory note for frontend workers: previous reducer bugs came from treating absent payload fields as zero. When restoring active Kanglong runs, preserve existing fields unless the backend explicitly returns replacements.

---

### Task 1: Add Template Store And Hash Contracts

**Files:**
- Create: `paired_opener/kanglong/test_templates.py`
- Modify: `paired_opener/config.py`
- Test: `tests/test_kanglong_test_templates.py`

- [ ] **Step 1: Write failing store/hash tests**

Create `tests/test_kanglong_test_templates.py` with these initial tests:

```python
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from paired_opener.config import Settings
from paired_opener.kanglong.test_templates import (
    KANGLONG_TEST_TEMPLATE_VERSION,
    KanglongTemplateStore,
    TemplateValidationError,
    canonical_decimal_text,
    template_content_hash,
    validate_template_identifier,
)


def test_canonical_decimal_text_is_stable() -> None:
    assert canonical_decimal_text(Decimal("10.000")) == "10"
    assert canonical_decimal_text(Decimal("0.0100")) == "0.01"
    assert canonical_decimal_text(Decimal("123.4500")) == "123.45"


def test_identifier_rejects_runtime_unsafe_characters() -> None:
    validate_template_identifier("tpl_eth_drop_001", field_name="template_id")

    with pytest.raises(TemplateValidationError) as excinfo:
        validate_template_identifier("tpl eth/001", field_name="template_id")

    assert excinfo.value.code == "kanglong_test_template_invalid_id"


def test_template_hash_ignores_display_name_and_decimal_format() -> None:
    first = {
        "id": "tpl_eth_drop_001",
        "name": "展示名称 A",
        "symbol": "ethusdc",
        "main_account": {"account_id": "test-main", "name": "主账号 A", "collateral": "10000.0", "leverage": 75, "positions": []},
        "subaccounts": [
            {
                "row_id": "sub-1",
                "account_id": "test-sub-1",
                "name": "子账号 A",
                "collateral": "5000.00",
                "leverage": 75,
                "long_entry_price": "2440.0",
                "short_entry_price": "2130.00",
                "qty": "10.000",
            }
        ],
    }
    second = {
        **first,
        "name": "展示名称 B",
        "main_account": {**first["main_account"], "name": "主账号 B", "collateral": "10000"},
        "subaccounts": [
            {**first["subaccounts"][0], "name": "子账号 B", "qty": "10"}
        ],
    }

    assert template_content_hash(first) == template_content_hash(second)


def test_store_creates_file_and_backup_on_second_save(tmp_path) -> None:
    path = tmp_path / "kanglong_test_templates.json"
    store = KanglongTemplateStore(path)
    template = {
        "id": "tpl_eth_drop_001",
        "name": "ETH 测试场景",
        "symbol": "ETHUSDC",
        "main_account": {"account_id": "test-main", "name": "测试主账号", "collateral": "10000", "leverage": 75, "positions": []},
        "subaccounts": [
            {
                "row_id": "sub-1",
                "account_id": "test-sub-1",
                "name": "测试子账号 1",
                "collateral": "5000",
                "leverage": 75,
                "long_entry_price": "2440",
                "short_entry_price": "2130",
                "qty": "10",
            }
        ],
    }

    created = store.upsert_template(template)
    updated = store.upsert_template({**created, "name": "ETH 测试场景改名"})

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["version"] == KANGLONG_TEST_TEMPLATE_VERSION
    assert payload["templates"][0]["id"] == "tpl_eth_drop_001"
    assert updated["template_content_hash"].startswith("sha256:")
    assert path.with_suffix(path.suffix + ".bak").exists()
```

Run: `python -m pytest tests/test_kanglong_test_templates.py -q`

Expected: FAIL because `paired_opener.kanglong.test_templates` does not exist.

- [ ] **Step 2: Add settings path**

Modify `paired_opener/config.py` so `Settings` exposes the default store path:

```python
self.kanglong_test_templates_file = Path(
    os.getenv(
        "PAIRED_OPENER_KANGLONG_TEST_TEMPLATES_FILE",
        str(self.data_dir / "kanglong_test_templates.json"),
    )
)
```

If `Settings` currently initializes paths differently, put this beside `kanglong_symbol_configs_file` and use the same `Path` import and environment loading style.

- [ ] **Step 3: Implement `test_templates.py` store primitives**

Create `paired_opener/kanglong/test_templates.py` with these public names:

```python
from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4

KANGLONG_TEST_TEMPLATE_VERSION = 1
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_STORE_LOCK = RLock()


@dataclass(slots=True)
class TemplateValidationError(ValueError):
    code: str
    field: str
    value: str

    def __str__(self) -> str:
        return f"{self.code}:{self.field}:{self.value}"


class TemplateStoreError(RuntimeError):
    def __init__(self, code: str, detail: dict[str, Any] | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.detail = detail or {}


def canonical_decimal_text(value: Decimal | str | int | float) -> str:
    try:
        decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TemplateValidationError("kanglong_test_template_invalid_decimal", "decimal", str(value)) from exc
    normalized = decimal_value.normalize()
    if normalized == normalized.to_integral():
        return format(normalized.quantize(Decimal("1")), "f")
    return format(normalized, "f")


def validate_template_identifier(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or not _IDENTIFIER_RE.match(normalized):
        raise TemplateValidationError("kanglong_test_template_invalid_id", field_name, str(value))
    return normalized
```

Continue the same file with:

```python
def _utc_now_text() -> str:
    return datetime.now(UTC).isoformat()


def _canonical_template_payload(template: dict[str, Any]) -> dict[str, Any]:
    subaccounts = sorted(
        template.get("subaccounts") or [],
        key=lambda item: validate_template_identifier(str(item.get("row_id") or item.get("account_id") or ""), field_name="row_id"),
    )
    return {
        "symbol": str(template.get("symbol") or "").strip().upper(),
        "main_account": {
            "account_id": validate_template_identifier(str(template["main_account"]["account_id"]), field_name="main_account.account_id"),
            "collateral": canonical_decimal_text(template["main_account"]["collateral"]),
            "leverage": int(template["main_account"]["leverage"]),
        },
        "subaccounts": [
            {
                "row_id": validate_template_identifier(str(item.get("row_id") or item.get("account_id")), field_name="subaccounts.row_id"),
                "account_id": validate_template_identifier(str(item["account_id"]), field_name="subaccounts.account_id"),
                "collateral": canonical_decimal_text(item["collateral"]),
                "leverage": int(item["leverage"]),
                "long_entry_price": canonical_decimal_text(item["long_entry_price"]),
                "short_entry_price": canonical_decimal_text(item["short_entry_price"]),
                "qty": canonical_decimal_text(item["qty"]),
            }
            for item in subaccounts
        ],
    }


def template_content_hash(template: dict[str, Any]) -> str:
    encoded = json.dumps(_canonical_template_payload(template), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + sha256(encoded.encode("utf-8")).hexdigest()


def runtime_main_account_id(template_id: str) -> str:
    return f"tpl:{validate_template_identifier(template_id, field_name='template_id')}:main"


def runtime_subaccount_id(template_id: str, row_id: str) -> str:
    return f"tpl:{validate_template_identifier(template_id, field_name='template_id')}:sub:{validate_template_identifier(row_id, field_name='row_id')}"
```

Then add `KanglongTemplateStore` with exact behaviors:

```python
class KanglongTemplateStore:
    def __init__(self, path: Path) -> None:
        self._path = Path(path)

    @property
    def path(self) -> Path:
        return self._path

    @property
    def backup_path(self) -> Path:
        return self._path.with_suffix(self._path.suffix + ".bak")

    def list_templates(self) -> dict[str, Any]:
        payload = self._read_payload()
        return {
            "version": payload["version"],
            "templates": [self._with_hash(item) for item in payload["templates"]],
            "recoverable_backup": self.backup_path.exists(),
        }

    def get_template(self, template_id: str) -> dict[str, Any]:
        normalized = validate_template_identifier(template_id, field_name="template_id")
        for template in self._read_payload()["templates"]:
            if template["id"] == normalized:
                return self._with_hash(template)
        raise TemplateStoreError("kanglong_test_template_not_found", {"template_id": template_id})

    def upsert_template(self, template: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_template(template)
        with _STORE_LOCK:
            payload = self._read_payload()
            templates = [item for item in payload["templates"] if item["id"] != normalized["id"]]
            existing = next((item for item in payload["templates"] if item["id"] == normalized["id"]), None)
            now = _utc_now_text()
            normalized["created_at"] = existing.get("created_at") if existing else now
            normalized["updated_at"] = now
            templates.append(normalized)
            templates.sort(key=lambda item: item["id"])
            self._write_payload({"version": KANGLONG_TEST_TEMPLATE_VERSION, "templates": templates})
        return self._with_hash(normalized)

    def clone_template(self, template_id: str, *, new_id: str | None = None) -> dict[str, Any]:
        source = self.get_template(template_id)
        clone_id = validate_template_identifier(new_id or f"{source['id']}_copy_{uuid4().hex[:8]}", field_name="template_id")
        clone = {key: value for key, value in source.items() if key != "template_content_hash"}
        clone["id"] = clone_id
        clone["name"] = f"{source.get('name') or source['id']} 副本"
        clone["subaccounts"] = [
            {**item, "row_id": f"sub-{index + 1}-{uuid4().hex[:8]}"}
            for index, item in enumerate(clone.get("subaccounts") or [])
        ]
        return self.upsert_template(clone)

    def delete_template(self, template_id: str) -> None:
        normalized = validate_template_identifier(template_id, field_name="template_id")
        with _STORE_LOCK:
            payload = self._read_payload()
            templates = [item for item in payload["templates"] if item["id"] != normalized]
            if len(templates) == len(payload["templates"]):
                raise TemplateStoreError("kanglong_test_template_not_found", {"template_id": template_id})
            self._write_payload({"version": KANGLONG_TEST_TEMPLATE_VERSION, "templates": templates})

    def recover_backup(self) -> dict[str, Any]:
        if not self.backup_path.exists():
            raise TemplateStoreError("kanglong_test_template_not_found", {"backup": str(self.backup_path)})
        backup_payload = json.loads(self.backup_path.read_text(encoding="utf-8"))
        self._validate_store_payload(backup_payload)
        self._write_payload(backup_payload, keep_backup=False)
        return self.list_templates()
```

Complete the private helpers in the same file:

```python
    def _read_payload(self) -> dict[str, Any]:
        if not self._path.exists():
            return {"version": KANGLONG_TEST_TEMPLATE_VERSION, "templates": []}
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8-sig"))
        except json.JSONDecodeError as exc:
            raise TemplateStoreError("kanglong_test_template_store_corrupted", {"path": str(self._path)}) from exc
        self._validate_store_payload(payload)
        return self._migrate_payload(payload)

    def _validate_store_payload(self, payload: dict[str, Any]) -> None:
        version = int(payload.get("version", 0))
        if version > KANGLONG_TEST_TEMPLATE_VERSION:
            raise TemplateStoreError("kanglong_test_template_unsupported_version", {"version": version})
        if not isinstance(payload.get("templates"), list):
            raise TemplateStoreError("kanglong_test_template_store_corrupted", {"path": str(self._path)})

    def _migrate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        migrated = {"version": KANGLONG_TEST_TEMPLATE_VERSION, "templates": []}
        for template in payload.get("templates") or []:
            migrated["templates"].append(self._normalize_template(template))
        return migrated

    def _normalize_template(self, template: dict[str, Any]) -> dict[str, Any]:
        template_id = validate_template_identifier(str(template.get("id") or ""), field_name="template_id")
        symbol = str(template.get("symbol") or "ETHUSDC").strip().upper()
        main = template.get("main_account") or {}
        normalized = {
            "id": template_id,
            "name": str(template.get("name") or template_id),
            "symbol": symbol,
            "main_account": {
                "account_id": validate_template_identifier(str(main.get("account_id") or "test-main"), field_name="main_account.account_id"),
                "name": str(main.get("name") or "测试主账号"),
                "collateral": canonical_decimal_text(main.get("collateral", "0")),
                "leverage": int(main.get("leverage", 75)),
                "positions": [],
            },
            "subaccounts": [],
        }
        for index, item in enumerate(template.get("subaccounts") or []):
            row_source = item.get("row_id") or item.get("account_id") or f"sub-{index + 1}"
            row_id = validate_template_identifier(str(row_source), field_name="subaccounts.row_id")
            normalized["subaccounts"].append(
                {
                    "row_id": row_id,
                    "account_id": validate_template_identifier(str(item.get("account_id") or row_id), field_name="subaccounts.account_id"),
                    "name": str(item.get("name") or row_id),
                    "collateral": canonical_decimal_text(item.get("collateral", "0")),
                    "leverage": int(item.get("leverage", 75)),
                    "long_entry_price": canonical_decimal_text(item.get("long_entry_price", "0")),
                    "short_entry_price": canonical_decimal_text(item.get("short_entry_price", "0")),
                    "qty": canonical_decimal_text(item.get("qty", "0")),
                }
            )
        return normalized

    def _with_hash(self, template: dict[str, Any]) -> dict[str, Any]:
        return {**template, "template_content_hash": template_content_hash(template)}

    def _write_payload(self, payload: dict[str, Any], *, keep_backup: bool = True) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if keep_backup and self._path.exists():
            self.backup_path.write_text(self._path.read_text(encoding="utf-8"), encoding="utf-8")
        fd, tmp_name = tempfile.mkstemp(prefix=self._path.name, suffix=".tmp", dir=str(self._path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
            os.replace(tmp_name, self._path)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
```

- [ ] **Step 4: Run store tests**

Run: `python -m pytest tests/test_kanglong_test_templates.py -q`

Expected: PASS for the initial store/hash tests.

- [ ] **Step 5: Commit store layer**

```powershell
git add paired_opener\config.py paired_opener\kanglong\test_templates.py tests\test_kanglong_test_templates.py
git commit -m "feat: add kanglong test template store"
```

---

### Task 2: Build Preview Snapshots From Templates And Real Market Data

**Files:**
- Modify: `paired_opener/kanglong/test_templates.py`
- Modify: `paired_opener/kanglong/config.py`
- Test: `tests/test_kanglong_test_templates.py`

- [ ] **Step 1: Add failing preview math tests**

Append to `tests/test_kanglong_test_templates.py`:

```python
from paired_opener.domain import SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.test_templates import build_template_preview_payload


class StubQuote:
    bid_price = Decimal("2443.20")
    ask_price = Decimal("2443.22")


class StubOrderBook:
    bids = [(Decimal("2443.19"), Decimal("20"))]
    asks = [(Decimal("2443.23"), Decimal("20"))]


def test_preview_uses_quote_mid_for_mark_price_and_orderbook_for_execution() -> None:
    template = {
        "id": "tpl_eth_drop_001",
        "name": "ETH 测试场景",
        "symbol": "ETHUSDC",
        "main_account": {"account_id": "test-main", "name": "测试主账号", "collateral": "10000", "leverage": 75, "positions": []},
        "subaccounts": [
            {
                "row_id": "sub-1",
                "account_id": "test-sub-1",
                "name": "测试子账号 1",
                "collateral": "5000",
                "leverage": 75,
                "long_entry_price": "2440",
                "short_entry_price": "2130",
                "qty": "10",
            }
        ],
    }

    payload = build_template_preview_payload(
        template=template,
        quote=StubQuote(),
        orderbook=StubOrderBook(),
        rules=SymbolRules(
            symbol="ETHUSDC",
            step_size=Decimal("0.001"),
            tick_size=Decimal("0.01"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        config=KanglongSymbolConfig(fee_rate=Decimal("0.0005")),
    )

    assert payload["mark_price_snapshot"]["mark_price"] == "2443.21"
    assert payload["mark_price_snapshot"]["mark_price_source"] == "quote_mid"
    assert payload["execution_orderbook_snapshot"]["source"] == "orderbook_top"
    sub = payload["accounts"][1]
    assert sub["account_id"] == "tpl:tpl_eth_drop_001:sub:sub-1"
    assert sub["positions"][0]["position_side"] == "LONG"
    assert sub["positions"][0]["unrealized_pnl"] == "32.10"
    assert sub["positions"][1]["position_side"] == "SHORT"
    assert sub["positions"][1]["unrealized_pnl"] == "-3132.10"
    assert sub["margin_deficit"] == "0"
    assert payload["blocks"] == []
```

Run: `python -m pytest tests/test_kanglong_test_templates.py -q`

Expected: FAIL because preview builder and `fee_rate` config do not exist yet.

- [ ] **Step 2: Add fee rate to Kanglong symbol config**

Modify `paired_opener/kanglong/config.py`:

```python
@dataclass(frozen=True, slots=True)
class KanglongSymbolConfig:
    fee_rate: Decimal = Decimal("0.0005")
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
    plan_recheck_price_drift_bps: int = 5
    plan_recheck_qty_tolerance: Decimal = Decimal("0.0001")
```

Add `fee_rate=_decimal(payload.get("fee_rate"), base.fee_rate),` to `_apply_overrides`.

- [ ] **Step 3: Implement preview builder**

Append these public functions to `paired_opener/kanglong/test_templates.py`:

```python
def _round_down_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value // step) * step


def _is_tick_aligned(value: Decimal, tick: Decimal) -> bool:
    if tick <= 0:
        return True
    return value == _round_down_to_step(value, tick)


def _position_payload(symbol: str, side: str, qty: Decimal, entry_price: Decimal, mark_price: Decimal, leverage: int) -> dict[str, str]:
    if side == "LONG":
        unrealized = (mark_price - entry_price) * qty
    else:
        unrealized = (entry_price - mark_price) * qty
    notional = mark_price * qty
    margin = notional / Decimal(leverage)
    return {
        "symbol": symbol,
        "position_side": side,
        "qty": canonical_decimal_text(qty),
        "entry_price": canonical_decimal_text(entry_price),
        "mark_price": canonical_decimal_text(mark_price),
        "unrealized_pnl": canonical_decimal_text(unrealized),
        "notional": canonical_decimal_text(notional),
        "margin": canonical_decimal_text(margin),
    }


def build_template_preview_payload(*, template: dict[str, Any], quote: Any, orderbook: Any, rules: Any, config: Any) -> dict[str, Any]:
    normalized = KanglongTemplateStore(Path("__unused__"))._normalize_template(template)
    bid = Decimal(str(quote.bid_price))
    ask = Decimal(str(quote.ask_price))
    if bid <= 0 or ask <= 0:
        raise TemplateStoreError("kanglong_test_template_quote_unavailable")
    mark_price = (bid + ask) / Decimal("2")
    best_bid_price = Decimal(str(orderbook.bids[0][0])) if getattr(orderbook, "bids", None) else Decimal("0")
    best_ask_price = Decimal(str(orderbook.asks[0][0])) if getattr(orderbook, "asks", None) else Decimal("0")
    if best_bid_price <= 0 or best_ask_price <= 0:
        raise TemplateStoreError("kanglong_test_template_orderbook_unavailable")
    blocks: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    rounding_residuals: list[dict[str, Any]] = []
    accounts: list[dict[str, Any]] = []

    main = normalized["main_account"]
    accounts.append(
        {
            "account_id": runtime_main_account_id(normalized["id"]),
            "template_account_id": main["account_id"],
            "name": main["name"],
            "role": "main",
            "collateral": main["collateral"],
            "wallet_balance": main["collateral"],
            "total_unrealized_pnl": "0",
            "equity": main["collateral"],
            "margin": "0",
            "available_balance": main["collateral"],
            "margin_deficit": "0",
            "positions": [],
        }
    )

    for item in normalized["subaccounts"]:
        leverage = int(item["leverage"])
        raw_qty = Decimal(item["qty"])
        qty = _round_down_to_step(raw_qty, Decimal(str(rules.step_size)))
        if qty != raw_qty:
            rounding_residuals.append({"account_id": item["account_id"], "side": "BOTH", "raw_qty": canonical_decimal_text(raw_qty), "rounded_qty": canonical_decimal_text(qty)})
        long_entry = Decimal(item["long_entry_price"])
        short_entry = Decimal(item["short_entry_price"])
        if qty <= 0:
            blocks.append({"code": "kanglong_test_template_non_positive_qty", "account_id": item["account_id"]})
        if qty < Decimal(str(rules.min_qty)):
            blocks.append({"code": "kanglong_test_template_min_qty_not_met", "account_id": item["account_id"]})
        if mark_price * qty < Decimal(str(rules.min_notional)):
            blocks.append({"code": "kanglong_test_template_min_notional_not_met", "account_id": item["account_id"]})
        if leverage > int(getattr(rules, "max_leverage", 125)):
            blocks.append({"code": "kanglong_test_template_leverage_exceeded", "account_id": item["account_id"]})
        if not _is_tick_aligned(long_entry, Decimal(str(rules.tick_size))) or not _is_tick_aligned(short_entry, Decimal(str(rules.tick_size))):
            blocks.append({"code": "kanglong_test_template_invalid_price", "account_id": item["account_id"]})
        positions = [
            _position_payload(normalized["symbol"], "LONG", qty, long_entry, mark_price, leverage),
            _position_payload(normalized["symbol"], "SHORT", qty, short_entry, mark_price, leverage),
        ]
        total_unrealized = sum(Decimal(position["unrealized_pnl"]) for position in positions)
        margin = sum(Decimal(position["margin"]) for position in positions)
        wallet = Decimal(item["collateral"])
        equity = wallet + total_unrealized
        available = equity - margin
        margin_deficit = abs(available) if available < 0 else Decimal("0")
        accounts.append(
            {
                "account_id": runtime_subaccount_id(normalized["id"], item["row_id"]),
                "template_account_id": item["account_id"],
                "row_id": item["row_id"],
                "name": item["name"],
                "role": "subaccount",
                "collateral": item["collateral"],
                "wallet_balance": item["collateral"],
                "total_unrealized_pnl": canonical_decimal_text(total_unrealized),
                "equity": canonical_decimal_text(equity),
                "margin": canonical_decimal_text(margin),
                "available_balance": canonical_decimal_text(max(available, Decimal("0"))),
                "margin_deficit": canonical_decimal_text(margin_deficit),
                "positions": positions,
            }
        )

    snapshot_basis = {"template_hash": template_content_hash(normalized), "mark_price": canonical_decimal_text(mark_price), "accounts": accounts}
    snapshot_id = sha256(json.dumps(snapshot_basis, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:24]
    return {
        "template_id": normalized["id"],
        "template_content_hash": template_content_hash(normalized),
        "symbol": normalized["symbol"],
        "account_source": "test_template",
        "fee_rate_source": "kanglong_symbol_config",
        "fee_rate": canonical_decimal_text(getattr(config, "fee_rate", Decimal("0.0005"))),
        "snapshot_bundle_id": snapshot_id,
        "mark_price_snapshot": {
            "mark_price": canonical_decimal_text(mark_price),
            "mark_price_source": "quote_mid",
            "quote_bid_price": canonical_decimal_text(bid),
            "quote_ask_price": canonical_decimal_text(ask),
            "ttl_ms": int(getattr(config, "snapshot_ttl_ms", 5000)),
        },
        "execution_orderbook_snapshot": {
            "source": "orderbook_top",
            "best_bid_price": canonical_decimal_text(best_bid_price),
            "best_ask_price": canonical_decimal_text(best_ask_price),
            "ttl_ms": int(getattr(config, "price_ttl_ms", 2000)),
        },
        "symbol_rules": {
            "step_size": canonical_decimal_text(rules.step_size),
            "tick_size": canonical_decimal_text(rules.tick_size),
            "min_qty": canonical_decimal_text(rules.min_qty),
            "min_notional": canonical_decimal_text(rules.min_notional),
            "max_leverage": int(getattr(rules, "max_leverage", 125)),
        },
        "accounts": accounts,
        "rounding_residuals": rounding_residuals,
        "warnings": warnings,
        "blocks": blocks,
    }
```

- [ ] **Step 4: Run preview tests**

Run: `python -m pytest tests/test_kanglong_test_templates.py -q`

Expected: PASS.

- [ ] **Step 5: Commit preview layer**

```powershell
git add paired_opener\kanglong\config.py paired_opener\kanglong\test_templates.py tests\test_kanglong_test_templates.py
git commit -m "feat: preview kanglong test templates"
```

---

### Task 3: Add Template API Contracts

**Files:**
- Modify: `paired_opener/schemas.py`
- Modify: `paired_opener/api.py`
- Test: `tests/test_kanglong_test_template_api.py`

- [ ] **Step 1: Write failing API tests**

Create `tests/test_kanglong_test_template_api.py` with a TestClient fixture matching the repository's existing API test style. Include these tests:

```python
from __future__ import annotations

from decimal import Decimal

from fastapi.testclient import TestClient

from paired_opener.api import app


def _template_payload() -> dict:
    return {
        "id": "tpl_eth_drop_001",
        "name": "ETH 测试场景",
        "symbol": "ETHUSDC",
        "main_account": {"account_id": "test-main", "name": "测试主账号", "collateral": "10000", "leverage": 75, "positions": []},
        "subaccounts": [
            {
                "row_id": "sub-1",
                "account_id": "test-sub-1",
                "name": "测试子账号 1",
                "collateral": "5000",
                "leverage": 75,
                "long_entry_price": "2440",
                "short_entry_price": "2130",
                "qty": "10",
            }
        ],
    }


def test_template_crud_routes_round_trip(client: TestClient) -> None:
    created = client.post("/kanglong/simulation/test-templates", json=_template_payload())
    assert created.status_code == 200
    assert created.json()["template"]["template_content_hash"].startswith("sha256:")

    listed = client.get("/kanglong/simulation/test-templates")
    assert listed.status_code == 200
    assert listed.json()["templates"][0]["id"] == "tpl_eth_drop_001"

    updated = client.put("/kanglong/simulation/test-templates/tpl_eth_drop_001", json={**_template_payload(), "name": "改名"})
    assert updated.status_code == 200
    assert updated.json()["template"]["name"] == "改名"

    cloned = client.post("/kanglong/simulation/test-templates/tpl_eth_drop_001/clone")
    assert cloned.status_code == 200
    assert cloned.json()["template"]["id"] != "tpl_eth_drop_001"

    deleted = client.delete("/kanglong/simulation/test-templates/tpl_eth_drop_001")
    assert deleted.status_code == 200
    assert deleted.json()["status"] == "deleted"
```

Add one preview-route test using the repository's existing fake gateway approach. The assertions must verify:

```python
assert response.json()["account_source"] == "test_template"
assert response.json()["accounts"][0]["account_id"] == "tpl:tpl_eth_drop_001:main"
assert response.json()["accounts"][1]["positions"][0]["mark_price"] == response.json()["mark_price_snapshot"]["mark_price"]
```

Run: `python -m pytest tests/test_kanglong_test_template_api.py -q`

Expected: FAIL because routes and schemas do not exist yet.

- [ ] **Step 2: Add schemas**

Modify `paired_opener/schemas.py`:

```python
class KanglongTemplatePreviewRequest(BaseModel):
    market_data_account_id: str = Field(..., min_length=1)


class KanglongTemplateMutationResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    template: dict[str, Any]


class KanglongTemplateListResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    version: int
    templates: list[dict[str, Any]]
    recoverable_backup: bool = False


class KanglongTemplateDeleteResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    status: str
    template_id: str
```

Extend `KanglongPlanRequest`:

```python
class KanglongPlanRequest(BaseModel):
    mode: str = Field(default="simulation", pattern="^simulation$")
    symbol: str = Field(default=DEFAULT_TRADING_SYMBOL)
    main_account_id: str
    subaccount_ids: list[str] = Field(..., min_length=1)
    selected_side: PositionSide | None = None
    account_source: str = Field(default="runtime", pattern="^(runtime|test_template)$")
    test_template_id: str | None = None
    template_content_hash: str | None = None
    market_data_account_id: str | None = None
```

- [ ] **Step 3: Add API helpers and routes**

Modify `paired_opener/api.py` imports:

```python
from paired_opener.kanglong.test_templates import (
    KanglongTemplateStore,
    TemplateStoreError,
    TemplateValidationError,
    build_template_preview_payload,
)
```

Add helpers near `_validate_kanglong_account_ids`:

```python
def _kanglong_template_store() -> KanglongTemplateStore:
    return KanglongTemplateStore(app.state.settings.kanglong_test_templates_file)


def _raise_template_error(exc: Exception) -> None:
    if isinstance(exc, TemplateValidationError):
        raise HTTPException(status_code=400, detail={"code": exc.code, "field": exc.field, "value": exc.value}) from exc
    if isinstance(exc, TemplateStoreError):
        status_code = 404 if exc.code == "kanglong_test_template_not_found" else 400
        raise HTTPException(status_code=status_code, detail={"code": exc.code, **exc.detail}) from exc
    raise exc
```

Add routes:

```python
@app.get("/kanglong/simulation/test-templates")
async def list_kanglong_test_templates() -> dict:
    try:
        return _kanglong_template_store().list_templates()
    except Exception as exc:
        _raise_template_error(exc)


@app.post("/kanglong/simulation/test-templates")
async def create_kanglong_test_template(template: dict[str, Any]) -> dict:
    try:
        return {"template": _kanglong_template_store().upsert_template(template)}
    except Exception as exc:
        _raise_template_error(exc)


@app.put("/kanglong/simulation/test-templates/{template_id}")
async def update_kanglong_test_template(template_id: str, template: dict[str, Any]) -> dict:
    try:
        return {"template": _kanglong_template_store().upsert_template({**template, "id": template_id})}
    except Exception as exc:
        _raise_template_error(exc)


@app.post("/kanglong/simulation/test-templates/{template_id}/clone")
async def clone_kanglong_test_template(template_id: str) -> dict:
    try:
        return {"template": _kanglong_template_store().clone_template(template_id)}
    except Exception as exc:
        _raise_template_error(exc)


@app.delete("/kanglong/simulation/test-templates/{template_id}")
async def delete_kanglong_test_template(template_id: str) -> dict:
    try:
        _kanglong_template_store().delete_template(template_id)
        return {"status": "deleted", "template_id": template_id}
    except Exception as exc:
        _raise_template_error(exc)


@app.post("/kanglong/simulation/test-templates/store/recover-backup")
async def recover_kanglong_test_template_backup() -> dict:
    try:
        return _kanglong_template_store().recover_backup()
    except Exception as exc:
        _raise_template_error(exc)
```

Add preview route. It must build one real gateway from `market_data_account_id`, call only market-data methods, and close it:

```python
@app.post("/kanglong/simulation/test-templates/{template_id}/preview")
async def preview_kanglong_test_template(template_id: str, request: KanglongTemplatePreviewRequest) -> dict:
    if not request.market_data_account_id.strip():
        raise HTTPException(status_code=400, detail={"code": "kanglong_test_template_market_data_account_required"})
    store = _kanglong_template_store()
    try:
        template = store.get_template(template_id)
        config = load_kanglong_symbol_config(app.state.settings, template["symbol"])
        gateway = app.state.runtime_manager.build_temporary_gateway(request.market_data_account_id)
        try:
            rules = await gateway.get_symbol_rules(template["symbol"])
            quote = await gateway.get_quote(template["symbol"])
            orderbook = await gateway.get_order_book(template["symbol"])
        finally:
            await gateway.close()
        return build_template_preview_payload(
            template=template,
            quote=quote,
            orderbook=orderbook,
            rules=rules,
            config=config,
        )
    except Exception as exc:
        _raise_template_error(exc)
```

- [ ] **Step 4: Run API tests**

Run: `python -m pytest tests/test_kanglong_test_template_api.py -q`

Expected: PASS.

- [ ] **Step 5: Commit template API**

```powershell
git add paired_opener\schemas.py paired_opener\api.py tests\test_kanglong_test_template_api.py
git commit -m "feat: expose kanglong test template api"
```

---

### Task 4: Branch Kanglong Plan Input Collection By Account Source

**Files:**
- Modify: `paired_opener/api.py`
- Modify: `paired_opener/kanglong/test_templates.py`
- Modify: `paired_opener/kanglong/snapshots.py`
- Test: `tests/test_kanglong_test_template_api.py`
- Test: `tests/test_kanglong_workflow_contracts.py`

- [ ] **Step 1: Add failing no-real-account-call test**

Add a test with a fake runtime manager where:

- `build_temporary_gateway("main")` returns a market-data gateway.
- `build_temporary_gateway("tpl:tpl_eth_drop_001:main")` raises `AssertionError("template account reached real gateway")`.
- `POST /kanglong/simulation/plan` in `account_source = "test_template"` succeeds through preview snapshots.

Assertion:

```python
assert payload["report"]["account_snapshot"]["account_source"] == "test_template"
assert payload["report"]["account_snapshot"]["accounts"][0]["account_id"] == "tpl:tpl_eth_drop_001:main"
```

Run: `python -m pytest tests/test_kanglong_test_template_api.py -q`

Expected: FAIL because `_collect_kanglong_plan_inputs` currently builds gateways for every selected account.

- [ ] **Step 2: Add snapshot conversion helper**

In `paired_opener/kanglong/test_templates.py`, add:

```python
from paired_opener.domain import PositionSide
from paired_opener.kanglong.models import KanglongAccountSnapshot, KanglongPositionSnapshot


def preview_account_to_kanglong_snapshot(account: dict[str, Any], *, leverage: int) -> KanglongAccountSnapshot:
    positions: dict[PositionSide, KanglongPositionSnapshot] = {}
    for item in account.get("positions") or []:
        side = PositionSide(str(item["position_side"]).upper())
        positions[side] = KanglongPositionSnapshot(
            symbol=str(item["symbol"]).upper(),
            side=side,
            qty=Decimal(str(item["qty"])),
            entry_price=Decimal(str(item["entry_price"])),
            mark_price=Decimal(str(item["mark_price"])),
            unrealized_pnl=Decimal(str(item["unrealized_pnl"])),
        )
    return KanglongAccountSnapshot(
        account_id=str(account["account_id"]),
        account_name=str(account.get("name") or account["account_id"]),
        available_balance=Decimal(str(account.get("available_balance") or "0")),
        equity=Decimal(str(account.get("equity") or "0")),
        margin=Decimal(str(account.get("margin") or "0")),
        leverage=leverage,
        positions=positions,
        open_orders=[],
        snapshot_version=f"{account['account_id']}:{account.get('template_account_id', '')}:{account.get('row_id', '')}",
    )
```

- [ ] **Step 3: Split `_collect_kanglong_plan_inputs`**

In `paired_opener/api.py`, keep the current runtime path in `_collect_runtime_kanglong_plan_inputs(request)`. Add `_collect_template_kanglong_plan_inputs(request)` with this behavior:

```python
async def _collect_kanglong_plan_inputs(request: KanglongPlanRequest) -> dict:
    if request.account_source == "test_template":
        return await _collect_template_kanglong_plan_inputs(request)
    if request.test_template_id or request.template_content_hash or request.market_data_account_id:
        raise HTTPException(status_code=400, detail={"code": "kanglong_test_template_account_mismatch"})
    return await _collect_runtime_kanglong_plan_inputs(request)
```

Template path rules:

```python
async def _collect_template_kanglong_plan_inputs(request: KanglongPlanRequest) -> dict:
    if not request.test_template_id:
        raise HTTPException(status_code=400, detail={"code": "kanglong_test_template_not_found"})
    if not request.template_content_hash:
        raise HTTPException(status_code=400, detail={"code": "blocked_plan_stale"})
    if not request.market_data_account_id:
        raise HTTPException(status_code=400, detail={"code": "kanglong_test_template_market_data_account_required"})
    template = _kanglong_template_store().get_template(request.test_template_id)
    if template["symbol"] != request.symbol.strip().upper():
        raise HTTPException(status_code=400, detail={"code": "kanglong_test_template_symbol_mismatch"})
    if template["template_content_hash"] != request.template_content_hash:
        raise HTTPException(status_code=409, detail={"code": "blocked_plan_stale"})
    preview = await _preview_template_from_market_data(template, request.market_data_account_id)
    allowed = {account["account_id"] for account in preview["accounts"]}
    requested = {request.main_account_id, *request.subaccount_ids}
    if not requested.issubset(allowed):
        raise HTTPException(status_code=400, detail={"code": "kanglong_test_template_account_mismatch"})
    by_id = {account["account_id"]: account for account in preview["accounts"]}
    main_snapshot = preview_account_to_kanglong_snapshot(by_id[request.main_account_id], leverage=DEFAULT_LEVERAGE)
    subaccount_snapshots = [
        preview_account_to_kanglong_snapshot(by_id[account_id], leverage=DEFAULT_LEVERAGE)
        for account_id in request.subaccount_ids
    ]
    config = load_kanglong_symbol_config(app.state.settings, request.symbol)
    rules = _symbol_rules_from_preview(preview["symbol_rules"])
    return {
        "symbol": request.symbol,
        "main_account_id": request.main_account_id,
        "subaccount_ids": request.subaccount_ids,
        "selected_side": request.selected_side,
        "snapshot_bundle_id": preview["snapshot_bundle_id"],
        "main_snapshot": main_snapshot,
        "subaccount_snapshots": subaccount_snapshots,
        "config": config,
        "rules": rules,
        "close_price": Decimal(preview["execution_orderbook_snapshot"]["best_bid_price"]),
        "open_price": Decimal(preview["execution_orderbook_snapshot"]["best_ask_price"]),
        "fee_rate": Decimal(preview["fee_rate"]),
        "request_metadata": {
            "account_source": "test_template",
            "test_template_id": template["id"],
            "template_content_hash": preview["template_content_hash"],
            "template_input_digest": preview["template_content_hash"],
            "market_data_account_id": request.market_data_account_id,
            "fee_rate_source": preview["fee_rate_source"],
            "fee_rate": preview["fee_rate"],
            "snapshot_bundle_id": preview["snapshot_bundle_id"],
            "template_runtime_account_map": _runtime_account_map_from_preview(preview),
        },
        "account_snapshot_payload": {
            "account_source": "test_template",
            "template_id": template["id"],
            "template_content_hash": preview["template_content_hash"],
            "snapshot_bundle_id": preview["snapshot_bundle_id"],
            "accounts": preview["accounts"],
        },
    }
```

Implement `_preview_template_from_market_data`, `_symbol_rules_from_preview`, and `_runtime_account_map_from_preview` as private helpers in `api.py`. `_preview_template_from_market_data` should reuse the preview route's market-data-only logic.

- [ ] **Step 4: Persist request metadata in service call**

Extend `KanglongSimulationService.create_plan(...)` parameters:

```python
request_metadata: dict[str, Any] | None = None
account_snapshot_payload: dict[str, Any] | None = None
```

When creating the initial draft run, pass a `request` payload:

```python
request_payload = {
    "mode": "simulation",
    "symbol": symbol,
    "main_account_id": main_account_id,
    "subaccount_ids": subaccount_ids,
    **(request_metadata or {"account_source": "runtime"}),
}
```

Put frozen template accounts in the report:

```python
if account_snapshot_payload is not None:
    report["account_snapshot"] = account_snapshot_payload
```

Do this for blocked and successful plan responses so active-run restore always has account data.

- [ ] **Step 5: Run branch tests**

Run:

```powershell
python -m pytest tests/test_kanglong_test_template_api.py -q
python -m pytest tests/test_kanglong_workflow_contracts.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit plan-source branch**

```powershell
git add paired_opener\api.py paired_opener\kanglong\test_templates.py paired_opener\kanglong\service.py tests\test_kanglong_test_template_api.py tests\test_kanglong_workflow_contracts.py
git commit -m "feat: plan kanglong from test template snapshots"
```

---

### Task 5: Preserve Synthetic Run State For Execute, Recover, And Active Restore

**Files:**
- Modify: `paired_opener/kanglong/service.py`
- Modify: `paired_opener/storage.py`
- Modify: `paired_opener/api.py`
- Test: `tests/test_kanglong_workflow_contracts.py`
- Test: `tests/test_kanglong_test_template_api.py`

- [ ] **Step 1: Add failing active-run and stale-plan tests**

Add tests that:

- Create a template plan.
- Verify `/kanglong/simulation/run/active` returns `request.account_source == "test_template"`.
- Verify `report.account_snapshot.accounts` contains `tpl:...` IDs.
- Modify the template after plan creation.
- Confirm/execute returns `blocked_plan_stale` instead of executing.

Expected assertion block:

```python
active = client.get("/kanglong/simulation/run/active").json()
assert active["request"]["account_source"] == "test_template"
assert active["report"]["account_snapshot"]["accounts"][0]["account_id"].startswith("tpl:")
```

Run: `python -m pytest tests/test_kanglong_workflow_contracts.py tests/test_kanglong_test_template_api.py -q`

Expected: FAIL until run-state metadata is persisted and stale validation is enforced.

- [ ] **Step 2: Add stale validation helper**

In `paired_opener/api.py`, add:

```python
def _validate_template_run_not_stale(stored: dict[str, Any]) -> None:
    request_payload = stored.get("request") or {}
    if request_payload.get("account_source") != "test_template":
        return
    template_id = request_payload.get("test_template_id")
    expected_hash = request_payload.get("template_content_hash")
    if not template_id or not expected_hash:
        raise HTTPException(status_code=409, detail={"code": "blocked_plan_stale"})
    try:
        current = _kanglong_template_store().get_template(template_id)
    except Exception as exc:
        raise HTTPException(status_code=409, detail={"code": "blocked_plan_stale"}) from exc
    if current.get("template_content_hash") != expected_hash:
        raise HTTPException(status_code=409, detail={"code": "blocked_plan_stale"})
```

Call this helper at the start of confirm, execute, and recover endpoints after loading `stored`.

- [ ] **Step 3: Recheck execution with market data only**

In `execute_kanglong_simulation_plan`, when `stored["request"]["account_source"] == "test_template"`:

```python
request_payload = stored["request"]
market_data_account_id = request_payload["market_data_account_id"]
gateway = app.state.runtime_manager.build_temporary_gateway(market_data_account_id)
try:
    quote = await gateway.get_quote(stored["symbol"])
    orderbook = await gateway.get_order_book(stored["symbol"])
finally:
    await gateway.close()
execute_kwargs["close_price"] = Decimal(str(orderbook.bids[0][0]))
execute_kwargs["open_price"] = Decimal(str(orderbook.asks[0][0]))
```

Do not rebuild account snapshots from the template during execution. The execution baseline is:

1. `report.synthetic_account_state` when present.
2. Otherwise `report.account_snapshot.accounts`.

- [ ] **Step 4: Persist synthetic account state after execution steps**

Inside `KanglongSimulationService.execute_plan`, after each successful group or round update, merge synthetic state into the report:

```python
report["synthetic_account_state"] = {
    "account_source": "test_template",
    "state_version": f"{run_id}:{group.group_id}:{index}",
    "accounts": _apply_group_result_to_synthetic_accounts(
        previous_accounts,
        group_result,
    ),
    "updated_at": _now_text(),
}
```

Implement `_apply_group_result_to_synthetic_accounts` in `service.py` so it adjusts only the matched group accounts and keeps the payload shape identical to preview accounts. For the first implementation, update quantities and realized/unrealized ledger fields conservatively from simulated matched quantities; leave price recalculation to later preview only when no execution has started.

- [ ] **Step 5: Use one repository update per progress/state/event batch**

When a group or round succeeds, call the existing repository update/event methods in this order inside the current storage lock/transaction pattern:

1. append event rows,
2. update `report.synthetic_account_state`,
3. update progress,
4. update available actions.

If repository methods cannot combine these today, add `SqliteRepository.update_kanglong_run_and_events(...)` with a single `with self._lock, self._connection:` block.

- [ ] **Step 6: Run run-state tests**

Run:

```powershell
python -m pytest tests/test_kanglong_workflow_contracts.py tests/test_kanglong_test_template_api.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit synthetic run-state support**

```powershell
git add paired_opener\api.py paired_opener\kanglong\service.py paired_opener\storage.py tests\test_kanglong_workflow_contracts.py tests\test_kanglong_test_template_api.py
git commit -m "feat: persist kanglong synthetic template state"
```

---

### Task 6: Add i18n And Registry Coverage

**Files:**
- Modify: `i18n/messages/zh-CN.json`
- Modify: `i18n/registry/reasons.json`
- Modify: runtime registry file if present
- Modify: `tests/test_kanglong_i18n_contracts.py`

- [ ] **Step 1: Add failing i18n contract test**

Extend `tests/test_kanglong_i18n_contracts.py` with:

```python
TEMPLATE_MESSAGE_KEYS = [
    "console.kanglong.test_template.button",
    "console.kanglong.test_template.modal_title",
    "console.kanglong.test_template.library_title",
    "console.kanglong.test_template.market_data_account",
    "console.kanglong.test_template.save",
    "console.kanglong.test_template.save_and_apply",
    "console.kanglong.test_template.preview",
    "console.kanglong.test_template.exit_mode",
    "console.kanglong.test_template.applied",
    "console.kanglong.test_template.snapshot_stale",
]

TEMPLATE_REASON_CODES = [
    "kanglong_test_template_not_found",
    "kanglong_test_template_symbol_mismatch",
    "kanglong_test_template_accounts_required",
    "kanglong_test_template_account_mismatch",
    "kanglong_test_template_market_data_account_required",
    "kanglong_test_template_market_data_account_unavailable",
    "kanglong_test_template_invalid_id",
    "kanglong_test_template_invalid_decimal",
    "kanglong_test_template_negative_collateral",
    "kanglong_test_template_invalid_leverage",
    "kanglong_test_template_non_positive_qty",
    "kanglong_test_template_min_qty_not_met",
    "kanglong_test_template_min_notional_not_met",
    "kanglong_test_template_invalid_price",
    "kanglong_test_template_leverage_exceeded",
    "kanglong_test_template_quote_unavailable",
    "kanglong_test_template_orderbook_unavailable",
    "kanglong_test_template_store_corrupted",
    "kanglong_test_template_store_write_conflict",
    "kanglong_test_template_unsupported_version",
    "kanglong_test_template_migration_failed",
    "kanglong_test_template_active_run_exists",
    "blocked_plan_stale",
]
```

Assert every key exists through the repository's existing i18n loader helpers.

Run: `python -m pytest tests/test_kanglong_i18n_contracts.py -q`

Expected: FAIL because keys are missing.

- [ ] **Step 2: Add Chinese messages**

Add these `zh-CN` message values:

```json
{
  "console": {
    "kanglong": {
      "test_template": {
        "button": "测试模板",
        "modal_title": "亢龙测试账号模板",
        "library_title": "模板库",
        "market_data_account": "行情源账号",
        "save": "保存模板",
        "save_and_apply": "保存并应用",
        "preview": "预览快照",
        "exit_mode": "退出测试模板",
        "applied": "已应用测试模板",
        "snapshot_stale": "模板已更新，当前快照已过期"
      }
    }
  }
}
```

Merge into existing JSON without replacing current keys.

- [ ] **Step 3: Add reason registry entries**

Add reason entries with stable labels and messages:

```json
{
  "kanglong_test_template_not_found": {
    "label_key": "reasons.kanglong.test_template_not_found.label",
    "message_key": "reasons.kanglong.test_template_not_found.message"
  },
  "kanglong_test_template_market_data_account_required": {
    "label_key": "reasons.kanglong.test_template_market_data_account_required.label",
    "message_key": "reasons.kanglong.test_template_market_data_account_required.message"
  },
  "blocked_plan_stale": {
    "label_key": "reasons.kanglong.blocked_plan_stale.label",
    "message_key": "reasons.kanglong.blocked_plan_stale.message"
  }
}
```

Create one entry per code listed in Step 1.

- [ ] **Step 4: Run i18n tests**

Run: `python -m pytest tests/test_kanglong_i18n_contracts.py -q`

Expected: PASS.

- [ ] **Step 5: Commit i18n coverage**

```powershell
git add i18n\messages\zh-CN.json i18n\registry\reasons.json tests\test_kanglong_i18n_contracts.py
git commit -m "feat: add kanglong template i18n"
```

---

### Task 7: Build Frontend Template Modal And Account-Pool Replacement

**Files:**
- Modify: `paired_opener/static/index.html`
- Modify: `paired_opener/static/app.js`
- Create: `tests/test_app_kanglong_test_templates.mjs`
- Modify: `tests/test_app_kanglong_display.mjs`

- [ ] **Step 1: Add failing frontend contract test**

Create `tests/test_app_kanglong_test_templates.mjs` using the existing DOM test harness style. Assert:

```javascript
assert(document.querySelector('#kanglongTestTemplateButton'), 'template button exists');
assert(document.querySelector('#kanglongTestTemplateModal'), 'template modal exists');
assert(source.includes('accountSource'), 'app tracks account source');
assert(source.includes('test_template'), 'app knows test template source');
assert(source.includes('/kanglong/simulation/test-templates'), 'app calls template api');
assert(!source.includes('模板已更新，当前快照已过期'), 'stale text is not hard-coded in JS');
```

Add a state-level test that applies a preview payload and verifies:

```javascript
assert.equal(appState.kanglong.accountSource, 'test_template');
assert.equal(appState.kanglong.availableAccounts[0].account_id, 'tpl:tpl_eth_drop_001:main');
assert.equal(appState.kanglong.selectedSubaccountIds.length, 0);
```

Run:

```powershell
node tests\test_app_kanglong_test_templates.mjs
```

Expected: FAIL.

- [ ] **Step 2: Add modal HTML**

In `paired_opener/static/index.html`, add a compact button near the Kanglong account controls:

```html
<button id="kanglongTestTemplateButton" class="btn secondary" type="button" data-i18n="console.kanglong.test_template.button">测试模板</button>
```

Add modal root near existing modal roots:

```html
<div id="kanglongTestTemplateModal" class="modal hidden" role="dialog" aria-modal="true" aria-labelledby="kanglongTestTemplateTitle">
  <div class="modal-panel kanglong-template-modal">
    <header class="modal-header">
      <h2 id="kanglongTestTemplateTitle" data-i18n="console.kanglong.test_template.modal_title">亢龙测试账号模板</h2>
      <button id="kanglongTestTemplateCloseButton" class="icon-button" type="button" aria-label="关闭">×</button>
    </header>
    <section class="kanglong-template-grid">
      <aside id="kanglongTemplateLibrary" class="kanglong-template-library"></aside>
      <section id="kanglongTemplateEditor" class="kanglong-template-editor"></section>
      <section id="kanglongTemplatePreview" class="kanglong-template-preview"></section>
    </section>
    <footer class="modal-footer">
      <button id="kanglongTemplateSaveButton" class="btn secondary" type="button" data-i18n="console.kanglong.test_template.save">保存模板</button>
      <button id="kanglongTemplateSaveApplyButton" class="btn primary" type="button" data-i18n="console.kanglong.test_template.save_and_apply">保存并应用</button>
    </footer>
  </div>
</div>
```

- [ ] **Step 3: Add frontend state**

In `paired_opener/static/app.js`, extend Kanglong state:

```javascript
kanglong: {
  symbol: 'ETHUSDC',
  selectedSide: null,
  selectedMainAccountId: '',
  selectedSubaccountIds: [],
  availableAccounts: [],
  plan: null,
  events: [],
  accountSource: 'runtime',
  testTemplates: [],
  activeTestTemplateId: null,
  activeTemplateContentHash: null,
  marketDataAccountId: null,
  templatePreview: null,
  realAccountPoolSnapshot: null,
}
```

Add constants:

```javascript
const KANGLONG_ACCOUNT_SOURCE_RUNTIME = 'runtime';
const KANGLONG_ACCOUNT_SOURCE_TEST_TEMPLATE = 'test_template';
```

- [ ] **Step 4: Add API client functions**

Add functions:

```javascript
async function fetchKanglongTestTemplates() {
  return apiFetch('/kanglong/simulation/test-templates');
}

async function saveKanglongTestTemplate(template) {
  const method = template.id ? 'PUT' : 'POST';
  const url = template.id
    ? `/kanglong/simulation/test-templates/${encodeURIComponent(template.id)}`
    : '/kanglong/simulation/test-templates';
  return apiFetch(url, { method, body: JSON.stringify(template) });
}

async function previewKanglongTestTemplate(templateId, marketDataAccountId) {
  return apiFetch(`/kanglong/simulation/test-templates/${encodeURIComponent(templateId)}/preview`, {
    method: 'POST',
    body: JSON.stringify({ market_data_account_id: marketDataAccountId }),
  });
}
```

- [ ] **Step 5: Replace account pool when applying preview**

Add:

```javascript
function applyKanglongTemplatePreview(preview) {
  if (state.kanglong.accountSource !== KANGLONG_ACCOUNT_SOURCE_TEST_TEMPLATE) {
    state.kanglong.realAccountPoolSnapshot = [...state.kanglong.availableAccounts];
  }
  state.kanglong.accountSource = KANGLONG_ACCOUNT_SOURCE_TEST_TEMPLATE;
  state.kanglong.activeTestTemplateId = preview.template_id;
  state.kanglong.activeTemplateContentHash = preview.template_content_hash;
  state.kanglong.availableAccounts = preview.accounts;
  state.kanglong.selectedMainAccountId = preview.accounts.find((account) => account.role === 'main')?.account_id || '';
  state.kanglong.selectedSubaccountIds = [];
  state.kanglong.plan = null;
  state.kanglong.events = [];
  renderKanglongWorkspace();
}

function exitKanglongTemplateMode() {
  state.kanglong.accountSource = KANGLONG_ACCOUNT_SOURCE_RUNTIME;
  state.kanglong.activeTestTemplateId = null;
  state.kanglong.activeTemplateContentHash = null;
  state.kanglong.templatePreview = null;
  state.kanglong.availableAccounts = state.kanglong.realAccountPoolSnapshot || [];
  state.kanglong.realAccountPoolSnapshot = null;
  state.kanglong.selectedMainAccountId = '';
  state.kanglong.selectedSubaccountIds = [];
  state.kanglong.plan = null;
  state.kanglong.events = [];
  renderKanglongWorkspace();
}
```

- [ ] **Step 6: Extend plan request payload**

Where the frontend creates `/kanglong/simulation/plan`, include:

```javascript
const payload = {
  mode: 'simulation',
  symbol: state.kanglong.symbol,
  main_account_id: state.kanglong.selectedMainAccountId,
  subaccount_ids: state.kanglong.selectedSubaccountIds,
  selected_side: state.kanglong.selectedSide || null,
  account_source: state.kanglong.accountSource,
};
if (state.kanglong.accountSource === KANGLONG_ACCOUNT_SOURCE_TEST_TEMPLATE) {
  payload.test_template_id = state.kanglong.activeTestTemplateId;
  payload.template_content_hash = state.kanglong.activeTemplateContentHash;
  payload.market_data_account_id = state.kanglong.marketDataAccountId;
}
```

- [ ] **Step 7: Restore active template runs before rendering selected accounts**

In the active-run restoration path, if backend returns `request.account_source === "test_template"`:

```javascript
const accountSnapshot = active.report?.account_snapshot;
if (accountSnapshot?.accounts?.length) {
  state.kanglong.accountSource = KANGLONG_ACCOUNT_SOURCE_TEST_TEMPLATE;
  state.kanglong.activeTestTemplateId = active.request.test_template_id;
  state.kanglong.activeTemplateContentHash = active.request.template_content_hash;
  state.kanglong.marketDataAccountId = active.request.market_data_account_id;
  state.kanglong.availableAccounts = active.report.synthetic_account_state?.accounts || accountSnapshot.accounts;
}
```

Do this before setting selected main/subaccount IDs so `tpl:...` IDs can be resolved.

- [ ] **Step 8: Run frontend tests**

Run:

```powershell
node tests\test_app_kanglong_test_templates.mjs
node tests\test_app_kanglong_display.mjs
node --check paired_opener\static\app.js
```

Expected: PASS.

- [ ] **Step 9: Commit frontend modal**

```powershell
git add paired_opener\static\index.html paired_opener\static\app.js tests\test_app_kanglong_test_templates.mjs tests\test_app_kanglong_display.mjs
git commit -m "feat: add kanglong template modal"
```

---

### Task 8: Final Integration And Regression Pass

**Files:**
- Modify only files needed to fix integration failures from prior tasks.

- [ ] **Step 1: Run backend template tests**

Run:

```powershell
python -m pytest tests/test_kanglong_test_templates.py tests/test_kanglong_test_template_api.py -q
```

Expected: PASS.

- [ ] **Step 2: Run Kanglong workflow regressions**

Run:

```powershell
python -m pytest tests/test_kanglong_workflow_contracts.py tests/test_kanglong_api.py tests/test_kanglong_planner.py tests/test_kanglong_simulator.py -q
```

Expected: PASS.

- [ ] **Step 3: Run i18n regressions**

Run:

```powershell
python -m pytest tests/test_kanglong_i18n_contracts.py -q
```

Expected: PASS.

- [ ] **Step 4: Run frontend regressions**

Run:

```powershell
node tests\test_app_kanglong_test_templates.mjs
node tests\test_app_kanglong_display.mjs
node tests\test_app_simulation_payloads.mjs
node --check paired_opener\static\app.js
```

Expected: PASS.

- [ ] **Step 5: Check formatting and whitespace**

Run:

```powershell
git diff --check
git status --short
```

Expected: no whitespace errors; only intended files are modified.

- [ ] **Step 6: Commit final integration fixes**

If Step 1-5 required edits:

```powershell
git add paired_opener tests i18n
git commit -m "test: cover kanglong template integration"
```

If no edits were required, do not create an empty commit.

## Self-Review Checklist

- Template mode replaces the account pool; it never mixes real and synthetic accounts.
- `market_data_account_id` is mandatory for preview and template-mode plan creation.
- Real gateways are never built with `tpl:...` account IDs.
- Preview uses quote mid for mark/PnL and orderbook bid/ask for execution estimates.
- Manual prices must match tick size; quantities round down by step size and record residuals.
- Template hash ignores display names and Decimal formatting but changes for trading-affecting fields.
- Plan, confirm, execute, recover, and active-run restore use the run-state hash and account source.
- Frozen `report.account_snapshot.accounts` is preserved; execution updates go into `report.synthetic_account_state`.
- All new Chinese UI text and reason/error labels come from i18n or registry files.
- Runtime mode remains backward compatible when `account_source` is omitted.
