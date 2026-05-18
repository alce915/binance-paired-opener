from __future__ import annotations

import json
import threading
import time
from decimal import Decimal

import pytest

from paired_opener.config import Settings
from paired_opener.domain import Quote, SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig, load_kanglong_symbol_config
from paired_opener.kanglong.test_templates import (
    KANGLONG_TEST_TEMPLATE_VERSION,
    KanglongTemplateStore,
    TemplateStoreError,
    TemplateValidationError,
    build_template_preview_payload,
    canonical_decimal_text,
    runtime_main_account_id,
    runtime_subaccount_id,
    template_content_hash,
    validate_template_identifier,
)


def template_payload(template_id: str = "tpl_eth_drop_001") -> dict:
    return {
        "id": template_id,
        "name": "ETH 测试场景",
        "symbol": "ETHUSDC",
        "main_account": {
            "account_id": "test-main",
            "name": "测试主账号",
            "collateral": "10000",
            "leverage": 75,
            "positions": [],
        },
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


def preview_quote(bid: str = "2443.20", ask: str = "2443.22") -> Quote:
    return Quote(symbol="ETHUSDC", bid_price=Decimal(bid), ask_price=Decimal(ask))


def preview_orderbook(bid: str = "2443.19", ask: str = "2443.23") -> dict:
    return {
        "bids": [{"price": Decimal(bid), "qty": Decimal("12")}],
        "asks": [{"price": Decimal(ask), "qty": Decimal("13")}],
    }


def preview_rules(
    *,
    step_size: str = "0.01",
    tick_size: str = "0.01",
    min_qty: str = "0.01",
    min_notional: str = "5",
    max_leverage: int = 125,
) -> SymbolRules:
    return SymbolRules(
        symbol="ETHUSDC",
        tick_size=Decimal(tick_size),
        step_size=Decimal(step_size),
        min_qty=Decimal(min_qty),
        min_notional=Decimal(min_notional),
        max_leverage=max_leverage,
    )


def as_decimal(value) -> Decimal:
    return Decimal(str(value))


def test_canonical_decimal_text_is_stable() -> None:
    assert canonical_decimal_text(Decimal("10.000")) == "10"
    assert canonical_decimal_text(Decimal("0.0100")) == "0.01"
    assert canonical_decimal_text(Decimal("123.4500")) == "123.45"


def test_build_template_preview_payload_uses_quote_mid_and_orderbook_top_for_account_math() -> None:
    template = template_payload()
    payload = build_template_preview_payload(
        template,
        preview_quote(),
        preview_orderbook(),
        preview_rules(),
        KanglongSymbolConfig(fee_rate=Decimal("0.0004")),
    )

    assert payload["template_id"] == "tpl_eth_drop_001"
    assert payload["template_content_hash"] == template_content_hash(template_payload())
    assert payload["symbol"] == "ETHUSDC"
    assert payload["account_source"] == "test_template"
    assert payload["fee_rate_source"] == "kanglong_symbol_config"
    assert as_decimal(payload["fee_rate"]) == Decimal("0.0004")
    assert len(payload["snapshot_bundle_id"]) == 24

    assert as_decimal(payload["mark_price_snapshot"]["mark_price"]) == Decimal("2443.21")
    assert payload["mark_price_snapshot"]["mark_price_source"] == "quote_mid"
    assert as_decimal(payload["mark_price_snapshot"]["quote_bid_price"]) == Decimal("2443.20")
    assert as_decimal(payload["mark_price_snapshot"]["quote_ask_price"]) == Decimal("2443.22")
    assert payload["mark_price_snapshot"]["ttl_ms"] == 5000
    assert payload["execution_orderbook_snapshot"]["source"] == "orderbook_top"
    assert as_decimal(payload["execution_orderbook_snapshot"]["best_bid_price"]) == Decimal("2443.19")
    assert as_decimal(payload["execution_orderbook_snapshot"]["best_ask_price"]) == Decimal("2443.23")
    assert payload["execution_orderbook_snapshot"]["ttl_ms"] == 5000

    assert payload["symbol_rules"] == {
        "step_size": "0.01",
        "tick_size": "0.01",
        "min_qty": "0.01",
        "min_notional": "5",
        "max_leverage": 125,
    }
    assert payload["rounding_residuals"] == []
    assert payload["warnings"] == []
    assert payload["blocks"] == []

    main, sub = payload["accounts"]
    assert "id" not in main
    assert main["account_id"] == "tpl:tpl_eth_drop_001:main"
    assert main["template_account_id"] == "test-main"
    assert main["name"] == template["main_account"]["name"]
    assert main["role"] == "main"
    assert as_decimal(main["collateral"]) == Decimal("10000")
    assert main["positions"] == []
    assert as_decimal(main["wallet_balance"]) == Decimal("10000")
    assert as_decimal(main["total_unrealized_pnl"]) == Decimal("0")
    assert as_decimal(main["equity"]) == Decimal("10000")
    assert as_decimal(main["available_balance"]) == Decimal("10000")
    assert as_decimal(main["margin"]) == Decimal("0")
    assert as_decimal(main["margin_deficit"]) == Decimal("0")

    assert "id" not in sub
    assert sub["account_id"] == "tpl:tpl_eth_drop_001:sub:sub-1"
    assert sub["template_account_id"] == "test-sub-1"
    assert sub["row_id"] == "sub-1"
    assert sub["name"] == template["subaccounts"][0]["name"]
    assert sub["role"] == "subaccount"
    assert as_decimal(sub["collateral"]) == Decimal("5000")
    assert as_decimal(sub["wallet_balance"]) == Decimal("5000")
    assert as_decimal(sub["total_unrealized_pnl"]) == Decimal("-3100")
    assert as_decimal(sub["equity"]) == Decimal("1900")
    assert as_decimal(sub["margin"]) == Decimal("651.5226666666666666666666666")
    assert as_decimal(sub["available_balance"]) == Decimal("1248.477333333333333333333333")
    assert as_decimal(sub["margin_deficit"]) == Decimal("0")

    long_position, short_position = sub["positions"]
    assert long_position["position_side"] == "LONG"
    assert as_decimal(long_position["qty"]) == Decimal("10")
    assert as_decimal(long_position["entry_price"]) == Decimal("2440")
    assert as_decimal(long_position["mark_price"]) == Decimal("2443.21")
    assert as_decimal(long_position["unrealized_pnl"]) == Decimal("32.10")
    assert as_decimal(long_position["notional"]) == Decimal("24432.10")
    assert as_decimal(long_position["margin"]) == Decimal("325.7613333333333333333333333")
    assert short_position["position_side"] == "SHORT"
    assert as_decimal(short_position["unrealized_pnl"]) == Decimal("-3132.10")


def test_load_kanglong_symbol_config_supports_fee_rate_override(tmp_path) -> None:
    config_path = tmp_path / "kanglong_symbol_configs.json"
    config_path.write_text(json.dumps({"ETHUSDC": {"fee_rate": "0.0004"}}), encoding="utf-8")
    settings = Settings(_env_file=None, kanglong_symbol_configs_file=config_path)

    assert load_kanglong_symbol_config(settings, "ethusdc").fee_rate == Decimal("0.0004")


def test_build_template_preview_payload_rounds_qty_down_and_reports_residual() -> None:
    template = template_payload()
    template["subaccounts"][0]["qty"] = "10.09"

    payload = build_template_preview_payload(
        template,
        preview_quote(),
        preview_orderbook(),
        preview_rules(step_size="0.1"),
        KanglongSymbolConfig(),
    )

    sub = payload["accounts"][1]
    assert [as_decimal(position["qty"]) for position in sub["positions"]] == [Decimal("10"), Decimal("10")]
    assert payload["rounding_residuals"] == [
        {
            "account_id": "tpl:tpl_eth_drop_001:sub:sub-1",
            "side": "BOTH",
            "raw_qty": "10.09",
            "rounded_qty": "10",
        }
    ]
    assert payload["blocks"] == []


def test_build_template_preview_payload_blocks_tick_size_misaligned_manual_prices() -> None:
    template = template_payload()
    template["subaccounts"][0]["long_entry_price"] = "2440.01"

    payload = build_template_preview_payload(
        template,
        preview_quote(),
        preview_orderbook(),
        preview_rules(tick_size="0.05"),
        KanglongSymbolConfig(),
    )

    assert any(block["code"] == "kanglong_test_template_invalid_price" for block in payload["blocks"])


def test_build_template_preview_payload_returns_quantity_notional_and_leverage_blocks() -> None:
    template = template_payload()
    template["subaccounts"][0]["qty"] = "0.04"
    template["subaccounts"][0]["leverage"] = 75

    payload = build_template_preview_payload(
        template,
        preview_quote(),
        preview_orderbook(),
        preview_rules(step_size="0.01", min_qty="0.05", min_notional="1000", max_leverage=50),
        KanglongSymbolConfig(),
    )

    codes = [block["code"] for block in payload["blocks"]]
    assert "kanglong_test_template_min_qty_not_met" in codes
    assert "kanglong_test_template_min_notional_not_met" in codes
    assert "kanglong_test_template_leverage_exceeded" in codes


def test_build_template_preview_payload_min_notional_uses_executable_orderbook_price() -> None:
    template = template_payload()
    template["subaccounts"][0]["qty"] = "0.05"

    payload = build_template_preview_payload(
        template,
        preview_quote(bid="99", ask="101"),
        preview_orderbook(bid="99", ask="101"),
        preview_rules(step_size="0.01", min_qty="0.01", min_notional="5"),
        KanglongSymbolConfig(),
    )

    assert "kanglong_test_template_min_notional_not_met" in [block["code"] for block in payload["blocks"]]


def test_build_template_preview_payload_snapshot_bundle_id_changes_with_fee_rate() -> None:
    first = build_template_preview_payload(
        template_payload(),
        preview_quote(),
        preview_orderbook(),
        preview_rules(),
        KanglongSymbolConfig(fee_rate=Decimal("0.0005")),
    )
    second = build_template_preview_payload(
        template_payload(),
        preview_quote(),
        preview_orderbook(),
        preview_rules(),
        KanglongSymbolConfig(fee_rate=Decimal("0.001")),
    )

    assert first["snapshot_bundle_id"] != second["snapshot_bundle_id"]


def test_build_template_preview_payload_snapshot_bundle_id_ignores_display_names() -> None:
    base = template_payload()
    renamed = template_payload()
    renamed["name"] = "Renamed template"
    renamed["main_account"]["name"] = "Renamed main"
    renamed["subaccounts"][0]["name"] = "Renamed sub"

    first = build_template_preview_payload(
        base,
        preview_quote(),
        preview_orderbook(),
        preview_rules(),
        KanglongSymbolConfig(),
    )
    second = build_template_preview_payload(
        renamed,
        preview_quote(),
        preview_orderbook(),
        preview_rules(),
        KanglongSymbolConfig(),
    )

    assert first["template_content_hash"] == second["template_content_hash"]
    assert first["snapshot_bundle_id"] == second["snapshot_bundle_id"]


@pytest.mark.parametrize(
    ("quote", "orderbook", "code"),
    [
        (Quote(symbol="ETHUSDC", bid_price=Decimal("0"), ask_price=Decimal("2443.22")), preview_orderbook(), "kanglong_test_template_quote_unavailable"),
        (preview_quote(), {"bids": [], "asks": [{"price": Decimal("2443.23"), "qty": Decimal("13")}]}, "kanglong_test_template_orderbook_unavailable"),
    ],
)
def test_build_template_preview_payload_raises_for_missing_quote_or_orderbook(quote, orderbook, code) -> None:
    with pytest.raises(TemplateStoreError) as excinfo:
        build_template_preview_payload(
            template_payload(),
            quote,
            orderbook,
            preview_rules(),
            KanglongSymbolConfig(),
        )

    assert excinfo.value.code == code
    assert excinfo.value.detail["symbol"] == "ETHUSDC"


def test_canonical_decimal_text_rejects_invalid_decimal() -> None:
    with pytest.raises(TemplateValidationError) as excinfo:
        canonical_decimal_text("not-a-decimal")

    assert excinfo.value.code == "kanglong_test_template_invalid_decimal"
    assert excinfo.value.field == "decimal"
    assert "kanglong_test_template_invalid_decimal" in str(excinfo.value)


def test_identifier_rejects_runtime_unsafe_characters() -> None:
    assert validate_template_identifier(" tpl_eth_drop_001 ", field_name="template_id") == "tpl_eth_drop_001"

    with pytest.raises(TemplateValidationError) as excinfo:
        validate_template_identifier("tpl eth/001", field_name="template_id")

    assert excinfo.value.code == "kanglong_test_template_invalid_id"
    assert excinfo.value.field == "template_id"


def test_identifier_rejects_missing_values() -> None:
    with pytest.raises(TemplateValidationError) as excinfo:
        validate_template_identifier(None, field_name="template_id")

    assert excinfo.value.code == "kanglong_test_template_invalid_id"


def test_template_hash_ignores_display_name_and_decimal_format() -> None:
    first = {
        "id": "tpl_eth_drop_001",
        "name": "展示名称 A",
        "symbol": "ethusdc",
        "main_account": {
            "account_id": "test-main",
            "name": "主账号 A",
            "collateral": "10000.0",
            "leverage": 75,
            "positions": [],
        },
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
        "subaccounts": [{**first["subaccounts"][0], "name": "子账号 B", "qty": "10"}],
    }

    assert template_content_hash(first) == template_content_hash(second)


def test_runtime_account_ids_are_derived_from_template_ids() -> None:
    assert runtime_main_account_id("tpl_eth_drop_001") == "tpl:tpl_eth_drop_001:main"
    assert runtime_subaccount_id("tpl_eth_drop_001", "sub-1") == "tpl:tpl_eth_drop_001:sub:sub-1"


def test_store_missing_file_lists_empty_templates(tmp_path) -> None:
    store = KanglongTemplateStore(tmp_path / "kanglong_test_templates.json")

    assert store.list_templates() == {
        "version": KANGLONG_TEST_TEMPLATE_VERSION,
        "templates": [],
        "recoverable_backup": False,
    }


def test_list_templates_normalizes_legacy_templates_and_adds_hash(tmp_path) -> None:
    path = tmp_path / "kanglong_test_templates.json"
    legacy = template_payload()
    legacy["main_account"]["collateral"] = "10000.00"
    legacy["subaccounts"][0].pop("row_id")
    legacy["subaccounts"][0]["qty"] = "10.000"
    legacy["subaccounts"].append({**legacy["subaccounts"][0], "account_id": "test-sub-1"})
    path.write_text(
        json.dumps({"version": KANGLONG_TEST_TEMPLATE_VERSION, "templates": [legacy]}),
        encoding="utf-8",
    )
    store = KanglongTemplateStore(path)

    listed = store.list_templates()
    [loaded] = listed["templates"]

    assert listed["version"] == KANGLONG_TEST_TEMPLATE_VERSION
    assert listed["recoverable_backup"] is False
    assert loaded["template_content_hash"].startswith("sha256:")
    assert loaded["main_account"]["collateral"] == "10000"
    assert loaded["subaccounts"][0]["row_id"] == "test-sub-1"
    assert loaded["subaccounts"][1]["row_id"] == "test-sub-1-2"
    assert loaded["subaccounts"][0]["qty"] == "10"


def test_get_template_returns_normalized_legacy_template(tmp_path) -> None:
    path = tmp_path / "kanglong_test_templates.json"
    legacy = template_payload()
    legacy["subaccounts"][0].pop("row_id")
    path.write_text(
        json.dumps({"version": KANGLONG_TEST_TEMPLATE_VERSION, "templates": [legacy]}),
        encoding="utf-8",
    )
    store = KanglongTemplateStore(path)

    loaded = store.get_template("tpl_eth_drop_001")

    assert loaded["template_content_hash"] == template_content_hash(loaded)
    assert loaded["subaccounts"][0]["row_id"] == "test-sub-1"


def test_store_creates_file_and_backup_on_second_save(tmp_path) -> None:
    path = tmp_path / "kanglong_test_templates.json"
    store = KanglongTemplateStore(path)

    created = store.upsert_template(template_payload())
    updated = store.upsert_template({**created, "name": "ETH 测试场景改名"})

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["version"] == KANGLONG_TEST_TEMPLATE_VERSION
    assert payload["templates"][0]["id"] == "tpl_eth_drop_001"
    assert payload["templates"][0]["created_at"] == created["created_at"]
    assert updated["updated_at"] != created["updated_at"]
    assert updated["template_content_hash"].startswith("sha256:")
    assert path.with_suffix(path.suffix + ".bak").exists()
    assert store.list_templates()["recoverable_backup"] is True


def test_concurrent_upserts_from_separate_store_instances_preserve_all_templates(tmp_path, monkeypatch) -> None:
    path = tmp_path / "kanglong_test_templates.json"
    original_read_document = KanglongTemplateStore._read_document

    def slow_read_document(self):
        document = original_read_document(self)
        time.sleep(0.05)
        return document

    monkeypatch.setattr(KanglongTemplateStore, "_read_document", slow_read_document)
    errors: list[BaseException] = []

    def write_template(index: int) -> None:
        try:
            KanglongTemplateStore(path).upsert_template(template_payload(f"tpl_eth_drop_{index}"))
        except BaseException as exc:  # pragma: no cover - assertion reports collected errors
            errors.append(exc)

    threads = [threading.Thread(target=write_template, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert {item["id"] for item in KanglongTemplateStore(path).list_templates()["templates"]} == {
        f"tpl_eth_drop_{index}" for index in range(8)
    }


def test_upsert_preserves_unknown_fields_on_unrelated_templates(tmp_path) -> None:
    path = tmp_path / "kanglong_test_templates.json"
    first = template_payload("tpl_eth_drop_001")
    first["notes"] = "operator note"
    first["ui_collapsed"] = True
    second = template_payload("tpl_btc_drop_001")
    second["symbol"] = "BTCUSDC"
    path.write_text(
        json.dumps({"version": KANGLONG_TEST_TEMPLATE_VERSION, "templates": [first, second]}),
        encoding="utf-8",
    )
    store = KanglongTemplateStore(path)

    store.upsert_template({**second, "name": "BTC renamed"})

    by_id = {item["id"]: item for item in store.list_templates()["templates"]}
    assert by_id["tpl_eth_drop_001"]["notes"] == "operator note"
    assert by_id["tpl_eth_drop_001"]["ui_collapsed"] is True


def test_upsert_preserves_document_metadata_without_exposing_it(tmp_path) -> None:
    path = tmp_path / "kanglong_test_templates.json"
    path.write_text(
        json.dumps(
            {
                "version": KANGLONG_TEST_TEMPLATE_VERSION,
                "store_meta": {"owner": "api"},
                "templates": [template_payload("tpl_eth_drop_001")],
            }
        ),
        encoding="utf-8",
    )
    store = KanglongTemplateStore(path)

    store.upsert_template(template_payload("tpl_btc_drop_001"))

    payload = json.loads(path.read_text(encoding="utf-8"))
    listed = store.list_templates()
    assert payload["store_meta"] == {"owner": "api"}
    assert "store_meta" not in listed
    assert {item["id"] for item in listed["templates"]} == {"tpl_eth_drop_001", "tpl_btc_drop_001"}


def test_delete_preserves_document_metadata(tmp_path) -> None:
    path = tmp_path / "kanglong_test_templates.json"
    path.write_text(
        json.dumps(
            {
                "version": KANGLONG_TEST_TEMPLATE_VERSION,
                "store_meta": {"owner": "api"},
                "templates": [template_payload("tpl_eth_drop_001"), template_payload("tpl_btc_drop_001")],
            }
        ),
        encoding="utf-8",
    )
    store = KanglongTemplateStore(path)

    deleted = store.delete_template("tpl_btc_drop_001")

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert deleted["id"] == "tpl_btc_drop_001"
    assert payload["store_meta"] == {"owner": "api"}
    assert [item["id"] for item in payload["templates"]] == ["tpl_eth_drop_001"]


def test_store_rejects_corrupted_json(tmp_path) -> None:
    path = tmp_path / "kanglong_test_templates.json"
    path.write_text("{", encoding="utf-8")
    store = KanglongTemplateStore(path)

    with pytest.raises(TemplateStoreError) as excinfo:
        store.list_templates()

    assert excinfo.value.code == "kanglong_test_template_store_corrupted"
    assert excinfo.value.detail["path"] == str(path)
    assert "error" in excinfo.value.detail


def test_store_rejects_higher_version(tmp_path) -> None:
    path = tmp_path / "kanglong_test_templates.json"
    path.write_text(json.dumps({"version": KANGLONG_TEST_TEMPLATE_VERSION + 1, "templates": []}), encoding="utf-8")
    store = KanglongTemplateStore(path)

    with pytest.raises(TemplateStoreError) as excinfo:
        store.list_templates()

    assert excinfo.value.code == "kanglong_test_template_unsupported_version"
    assert excinfo.value.detail == {"path": str(path), "version": KANGLONG_TEST_TEMPLATE_VERSION + 1}


def test_get_template_not_found_uses_structured_detail(tmp_path) -> None:
    store = KanglongTemplateStore(tmp_path / "kanglong_test_templates.json")

    with pytest.raises(TemplateStoreError) as excinfo:
        store.get_template("missing_tpl")

    assert excinfo.value.code == "kanglong_test_template_not_found"
    assert excinfo.value.detail == {"template_id": "missing_tpl"}


@pytest.mark.parametrize(
    ("section", "field", "value", "code"),
    [
        ("main", "collateral", "-0.01", "kanglong_test_template_negative_collateral"),
        ("main", "leverage", 0, "kanglong_test_template_invalid_leverage"),
        ("sub", "collateral", "-0.01", "kanglong_test_template_negative_collateral"),
        ("sub", "leverage", 0, "kanglong_test_template_invalid_leverage"),
        ("sub", "qty", "0", "kanglong_test_template_non_positive_qty"),
        ("sub", "long_entry_price", "0", "kanglong_test_template_invalid_price"),
        ("sub", "short_entry_price", "-1", "kanglong_test_template_invalid_price"),
    ],
)
def test_upsert_rejects_invalid_numeric_template_values(tmp_path, section, field, value, code) -> None:
    template = template_payload()
    target = template["main_account"] if section == "main" else template["subaccounts"][0]
    target[field] = value
    store = KanglongTemplateStore(tmp_path / "kanglong_test_templates.json")

    with pytest.raises(TemplateValidationError) as excinfo:
        store.upsert_template(template)

    assert excinfo.value.code == code


@pytest.mark.parametrize(
    ("section", "field", "value", "code"),
    [
        ("main", "collateral", "-0.01", "kanglong_test_template_negative_collateral"),
        ("main", "leverage", -1, "kanglong_test_template_invalid_leverage"),
        ("sub", "collateral", "-0.01", "kanglong_test_template_negative_collateral"),
        ("sub", "leverage", 0, "kanglong_test_template_invalid_leverage"),
        ("sub", "qty", "-0.01", "kanglong_test_template_non_positive_qty"),
        ("sub", "long_entry_price", "-1", "kanglong_test_template_invalid_price"),
        ("sub", "short_entry_price", "0", "kanglong_test_template_invalid_price"),
    ],
)
def test_list_templates_rejects_invalid_numeric_legacy_templates(tmp_path, section, field, value, code) -> None:
    path = tmp_path / "kanglong_test_templates.json"
    template = template_payload()
    target = template["main_account"] if section == "main" else template["subaccounts"][0]
    target[field] = value
    path.write_text(
        json.dumps({"version": KANGLONG_TEST_TEMPLATE_VERSION, "templates": [template]}),
        encoding="utf-8",
    )
    store = KanglongTemplateStore(path)

    with pytest.raises(TemplateValidationError) as excinfo:
        store.list_templates()

    assert excinfo.value.code == code


@pytest.mark.parametrize("value", [True, False, 1.9, "1.9", Decimal("2.5"), "0", 0, -1])
def test_upsert_rejects_non_integer_or_non_positive_leverage(tmp_path, value) -> None:
    template = template_payload()
    template["main_account"]["leverage"] = value
    store = KanglongTemplateStore(tmp_path / "kanglong_test_templates.json")

    with pytest.raises(TemplateValidationError) as excinfo:
        store.upsert_template(template)

    assert excinfo.value.code == "kanglong_test_template_invalid_leverage"


def test_missing_row_id_collision_is_derived_and_uniquified(tmp_path) -> None:
    template = template_payload()
    template["subaccounts"][0].pop("row_id")
    template["subaccounts"].append({**template["subaccounts"][0]})
    store = KanglongTemplateStore(tmp_path / "kanglong_test_templates.json")

    created = store.upsert_template(template)

    assert [item["row_id"] for item in created["subaccounts"]] == ["test-sub-1", "test-sub-1-2"]


def test_explicit_duplicate_row_id_rejects_template(tmp_path) -> None:
    template = template_payload()
    template["subaccounts"].append({**template["subaccounts"][0], "account_id": "test-sub-2"})
    store = KanglongTemplateStore(tmp_path / "kanglong_test_templates.json")

    with pytest.raises(TemplateValidationError) as excinfo:
        store.upsert_template(template)

    assert excinfo.value.code == "kanglong_test_template_invalid_id"
    assert excinfo.value.field == "subaccounts.row_id"


def test_clone_generates_new_template_id_and_row_ids(tmp_path) -> None:
    store = KanglongTemplateStore(tmp_path / "kanglong_test_templates.json")
    created = store.upsert_template(template_payload())

    cloned = store.clone_template(created["id"])

    assert cloned["id"] != created["id"]
    assert cloned["main_account"]["account_id"] == "test-main"
    assert cloned["subaccounts"][0]["row_id"] != created["subaccounts"][0]["row_id"]
    assert {item["id"] for item in store.list_templates()["templates"]} == {created["id"], cloned["id"]}


def test_recover_backup_restores_readable_backup(tmp_path) -> None:
    path = tmp_path / "kanglong_test_templates.json"
    store = KanglongTemplateStore(path)
    created = store.upsert_template(template_payload())
    store.upsert_template({**created, "name": "ETH 测试场景改名"})
    path.write_text("{", encoding="utf-8")

    recovered = store.recover_backup()

    assert recovered["version"] == KANGLONG_TEST_TEMPLATE_VERSION
    assert recovered["recoverable_backup"] is True
    assert recovered["templates"][0]["id"] == "tpl_eth_drop_001"
    assert json.loads(path.read_text(encoding="utf-8"))["templates"][0]["id"] == "tpl_eth_drop_001"


def test_recover_backup_missing_file_uses_not_found_detail(tmp_path) -> None:
    path = tmp_path / "kanglong_test_templates.json"
    store = KanglongTemplateStore(path)

    with pytest.raises(TemplateStoreError) as excinfo:
        store.recover_backup()

    assert excinfo.value.code == "kanglong_test_template_not_found"
    assert excinfo.value.detail == {"backup": str(path.with_suffix(path.suffix + ".bak"))}


def test_settings_exposes_kanglong_test_templates_file(monkeypatch, tmp_path) -> None:
    settings = Settings(_env_file=None)
    assert settings.kanglong_test_templates_file.as_posix().endswith("data/kanglong_test_templates.json")

    custom_path = tmp_path / "templates.json"
    monkeypatch.setenv("PAIRED_OPENER_KANGLONG_TEST_TEMPLATES_FILE", str(custom_path))

    assert Settings(_env_file=None).kanglong_test_templates_file == custom_path
