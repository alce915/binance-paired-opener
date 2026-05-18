from __future__ import annotations

from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from paired_opener import api as api_module
from paired_opener.config import Settings
from paired_opener.domain import Quote, SymbolRules
from paired_opener.kanglong.test_templates import KanglongTemplateStore
from paired_opener.schemas import KanglongPlanRequest


def template_payload(template_id: str = "tpl_eth_drop_001") -> dict:
    return {
        "id": template_id,
        "name": "ETH test template",
        "symbol": "ETHUSDC",
        "main_account": {
            "account_id": "test-main",
            "name": "Test Main",
            "collateral": "10000",
            "leverage": 75,
            "positions": [],
        },
        "subaccounts": [
            {
                "row_id": "sub-1",
                "account_id": "test-sub-1",
                "name": "Test Sub 1",
                "collateral": "5000",
                "leverage": 75,
                "long_entry_price": "2440",
                "short_entry_price": "2130",
                "qty": "10",
            }
        ],
    }


def install_template_settings(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    settings = Settings(_env_file=None, kanglong_test_templates_file=tmp_path / "kanglong_test_templates.json")
    monkeypatch.setattr(api_module.app.state, "settings", settings, raising=False)


def test_kanglong_test_template_crud_round_trip(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    client = TestClient(api_module.app)

    created_response = client.post("/kanglong/simulation/test-templates", json=template_payload())
    assert created_response.status_code == 200
    created = created_response.json()
    assert created["contract_version"]
    assert created["template"]["id"] == "tpl_eth_drop_001"
    assert created["template"]["template_content_hash"].startswith("sha256:")

    listed_response = client.get("/kanglong/simulation/test-templates")
    assert listed_response.status_code == 200
    listed = listed_response.json()
    assert listed["contract_version"]
    assert listed["version"] >= 1
    assert [template["id"] for template in listed["templates"]] == ["tpl_eth_drop_001"]
    assert listed["recoverable_backup"] is False

    renamed = template_payload("ignored_payload_id")
    renamed["name"] = "Renamed template"
    updated_response = client.put("/kanglong/simulation/test-templates/tpl_eth_drop_001", json=renamed)
    assert updated_response.status_code == 200
    updated = updated_response.json()["template"]
    assert updated["id"] == "tpl_eth_drop_001"
    assert updated["name"] == "Renamed template"

    cloned_response = client.post("/kanglong/simulation/test-templates/tpl_eth_drop_001/clone")
    assert cloned_response.status_code == 200
    cloned = cloned_response.json()["template"]
    assert cloned["id"] != "tpl_eth_drop_001"
    assert cloned["template_content_hash"].startswith("sha256:")

    deleted_response = client.delete("/kanglong/simulation/test-templates/tpl_eth_drop_001")
    assert deleted_response.status_code == 200
    assert deleted_response.json() == {
        "contract_version": created["contract_version"],
        "status": "deleted",
        "template_id": "tpl_eth_drop_001",
    }


def test_kanglong_test_template_list_missing_file_returns_empty_envelope(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    response = TestClient(api_module.app).get("/kanglong/simulation/test-templates")

    assert response.status_code == 200
    payload = response.json()
    assert payload["contract_version"]
    assert payload["version"] >= 1
    assert payload["templates"] == []
    assert payload["recoverable_backup"] is False


def test_kanglong_test_template_recover_backup_returns_envelope(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    store = KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file)
    created = store.upsert_template(template_payload())
    store.upsert_template({**created, "name": "Backed up template"})
    api_module.app.state.settings.kanglong_test_templates_file.write_text("{", encoding="utf-8")

    response = TestClient(api_module.app).post("/kanglong/simulation/test-templates/store/recover-backup")

    assert response.status_code == 200
    payload = response.json()
    assert payload["contract_version"]
    assert payload["version"] >= 1
    assert payload["recoverable_backup"] is True
    assert payload["templates"][0]["id"] == "tpl_eth_drop_001"


def test_kanglong_test_template_errors_map_to_http_details(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    client = TestClient(api_module.app)

    validation_response = client.post("/kanglong/simulation/test-templates", json=template_payload("bad id"))
    assert validation_response.status_code == 400
    assert validation_response.json()["detail"] == {
        "code": "kanglong_test_template_invalid_id",
        "field": "template_id",
        "value": "bad id",
    }

    not_found_response = client.delete("/kanglong/simulation/test-templates/missing_tpl")
    assert not_found_response.status_code == 404
    assert not_found_response.json()["detail"] == {
        "code": "kanglong_test_template_not_found",
        "template_id": "missing_tpl",
    }

    api_module.app.state.settings.kanglong_test_templates_file.write_text("{", encoding="utf-8")
    store_response = client.get("/kanglong/simulation/test-templates")
    assert store_response.status_code == 400
    assert store_response.json()["detail"]["code"] == "kanglong_test_template_store_corrupted"


class FakeTemplateMarketGateway:
    def __init__(self):
        self.closed = False
        self.calls = []

    async def get_symbol_rules(self, symbol):
        self.calls.append(("rules", symbol))
        return SymbolRules(symbol, Decimal("0.01"), Decimal("0.01"), Decimal("0.01"), Decimal("5"), 125)

    async def get_quote(self, symbol):
        self.calls.append(("quote", symbol))
        return Quote(symbol, Decimal("2443.20"), Decimal("2443.22"))

    async def get_order_book(self, symbol):
        self.calls.append(("orderbook", symbol))
        return {
            "bids": [{"price": Decimal("2443.19"), "qty": Decimal("12")}],
            "asks": [{"price": Decimal("2443.23"), "qty": Decimal("13")}],
        }

    async def close(self):
        self.closed = True


class FailingTemplateMarketGateway(FakeTemplateMarketGateway):
    async def get_quote(self, symbol):
        self.calls.append(("quote", symbol))
        raise RuntimeError("quote unavailable")


class FakeTemplateRuntimeManager:
    def __init__(self, gateway):
        self.gateway = gateway
        self.build_calls = []

    def build_temporary_gateway(self, account_id: str):
        if account_id.startswith("tpl:"):
            raise AssertionError("preview must not build gateways for template runtime account ids")
        self.build_calls.append(account_id)
        return self.gateway


def test_kanglong_test_template_preview_uses_market_data_account_only(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(template_payload())
    gateway = FakeTemplateMarketGateway()
    runtime_manager = FakeTemplateRuntimeManager(gateway)
    monkeypatch.setattr(api_module.app.state, "runtime_manager", runtime_manager, raising=False)

    response = TestClient(api_module.app).post(
        "/kanglong/simulation/test-templates/tpl_eth_drop_001/preview",
        json={"market_data_account_id": "market-main"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["account_source"] == "test_template"
    assert runtime_manager.build_calls == ["market-main"]
    assert gateway.calls == [
        ("rules", "ETHUSDC"),
        ("quote", "ETHUSDC"),
        ("orderbook", "ETHUSDC"),
    ]
    assert gateway.closed is True

    main, sub = payload["accounts"]
    assert main["account_id"] == "tpl:tpl_eth_drop_001:main"
    assert sub["account_id"] == "tpl:tpl_eth_drop_001:sub:sub-1"
    assert main["template_account_id"] == "test-main"
    assert sub["template_account_id"] == "test-sub-1"
    assert all(account.get("account_id") != "market-main" for account in payload["accounts"])
    assert all("market_data_account_id" not in account for account in payload["accounts"])
    assert sub["positions"][0]["mark_price"] == payload["mark_price_snapshot"]["mark_price"]


def test_kanglong_test_template_preview_closes_gateway_on_market_data_error(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(template_payload())
    gateway = FailingTemplateMarketGateway()
    runtime_manager = FakeTemplateRuntimeManager(gateway)
    monkeypatch.setattr(api_module.app.state, "runtime_manager", runtime_manager, raising=False)

    response = TestClient(api_module.app).post(
        "/kanglong/simulation/test-templates/tpl_eth_drop_001/preview",
        json={"market_data_account_id": "market-main"},
    )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "kanglong_test_template_market_data_account_unavailable"
    assert runtime_manager.build_calls == ["market-main"]
    assert gateway.closed is True


@pytest.mark.parametrize("body", [{"market_data_account_id": "   "}, {}])
def test_kanglong_test_template_preview_rejects_missing_or_blank_market_data_account(monkeypatch, tmp_path, body) -> None:
    install_template_settings(monkeypatch, tmp_path)
    KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(template_payload())

    response = TestClient(api_module.app).post(
        "/kanglong/simulation/test-templates/tpl_eth_drop_001/preview",
        json=body,
    )

    assert response.status_code == 400
    assert response.json()["detail"] == {"code": "kanglong_test_template_market_data_account_required"}


def test_kanglong_plan_request_keeps_runtime_defaults_and_accepts_template_fields() -> None:
    default_request = KanglongPlanRequest(main_account_id="main", subaccount_ids=["sub1"])
    template_request = KanglongPlanRequest(
        main_account_id="main",
        subaccount_ids=["sub1"],
        account_source="test_template",
        test_template_id="tpl_eth_drop_001",
        template_content_hash="sha256:abc",
        market_data_account_id="market-main",
    )

    assert default_request.account_source == "runtime"
    assert default_request.test_template_id is None
    assert default_request.template_content_hash is None
    assert default_request.market_data_account_id is None
    assert template_request.account_source == "test_template"
    assert template_request.test_template_id == "tpl_eth_drop_001"
    assert template_request.template_content_hash == "sha256:abc"
    assert template_request.market_data_account_id == "market-main"
