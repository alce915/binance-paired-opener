from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from paired_opener import api as api_module
from paired_opener.config import Settings
from paired_opener.domain import Quote, SymbolRules
from paired_opener.kanglong.service import KanglongSimulationService
from paired_opener.kanglong.test_templates import KanglongTemplateStore
from paired_opener.schemas import KanglongPlanRequest
from paired_opener.storage import SqliteRepository


def wait_for_kanglong_status(repository: SqliteRepository, run_id: str, statuses: set[str], timeout_s: float = 2.0) -> dict | None:
    deadline = time.monotonic() + timeout_s
    stored = repository.get_kanglong_run(run_id)
    while time.monotonic() < deadline:
        if stored is not None and stored.get("status") in statuses:
            return stored
        time.sleep(0.02)
        stored = repository.get_kanglong_run(run_id)
    return stored


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


def test_kanglong_test_template_create_generates_id_when_visual_form_omits_it(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    payload = template_payload()
    payload.pop("id")

    response = TestClient(api_module.app).post("/kanglong/simulation/test-templates", json=payload)

    assert response.status_code == 200
    created = response.json()["template"]
    assert created["id"].startswith("tpl_")
    assert created["template_content_hash"].startswith("sha256:")
    listed = KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).list_templates()
    assert [template["id"] for template in listed["templates"]] == [created["id"]]


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

    async def get_order_book(self, symbol, limit: int = 10):
        self.calls.append(("orderbook", symbol))
        return {
            "symbol": symbol,
            "bids": [{"price": Decimal("2443.19"), "qty": Decimal("12")}],
            "asks": [{"price": Decimal("2443.23"), "qty": Decimal("13")}],
            "event_time": datetime.now(UTC).isoformat(),
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


def install_template_api_runtime(monkeypatch, tmp_path):
    install_template_settings(monkeypatch, tmp_path)
    repository = SqliteRepository(tmp_path / "kanglong.sqlite3")
    monkeypatch.setattr(api_module.app.state, "kanglong_service", KanglongSimulationService(repository), raising=False)
    gateway = FakeTemplateMarketGateway()
    runtime_manager = FakeTemplateRuntimeManager(gateway)
    monkeypatch.setattr(api_module.app.state, "runtime_manager", runtime_manager, raising=False)
    payload = template_payload()
    payload["subaccounts"][0]["qty"] = "1"
    template = KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(payload)
    return repository, runtime_manager, template


def create_template_backed_plan(client: TestClient, template: dict, *, run_id: str | None = None) -> dict:
    request = {
        "main_account_id": "tpl:tpl_eth_drop_001:main",
        "subaccount_ids": ["tpl:tpl_eth_drop_001:sub:sub-1"],
        "account_source": "test_template",
        "test_template_id": template["id"],
        "template_content_hash": template["template_content_hash"],
        "market_data_account_id": "market-main",
    }
    if run_id is not None:
        request["run_id"] = run_id
    response = client.post("/kanglong/simulation/plan", json=request)
    assert response.status_code == 200
    return response.json()


def stale_template() -> dict:
    updated = template_payload()
    updated["name"] = "Edited after plan creation"
    updated["subaccounts"][0]["collateral"] = "6000"
    return updated


def test_kanglong_active_template_plan_preserves_frozen_synthetic_accounts(monkeypatch, tmp_path) -> None:
    repository, runtime_manager, template = install_template_api_runtime(monkeypatch, tmp_path)
    client = TestClient(api_module.app)
    try:
        created = create_template_backed_plan(client, template)
        response = client.get("/kanglong/simulation/run/active")
    finally:
        repository.close()

    assert created["status"] == "chain_ready"
    assert response.status_code == 200
    payload = response.json()
    assert payload["request"]["account_source"] == "test_template"
    account_ids = [account["account_id"] for account in payload["report"]["account_snapshot"]["accounts"]]
    assert account_ids == ["tpl:tpl_eth_drop_001:main", "tpl:tpl_eth_drop_001:sub:sub-1"]
    assert runtime_manager.build_calls == ["market-main"]


def test_kanglong_template_confirm_blocks_when_template_changed(monkeypatch, tmp_path) -> None:
    repository, _, template = install_template_api_runtime(monkeypatch, tmp_path)
    client = TestClient(api_module.app)
    try:
        created = create_template_backed_plan(client, template)
        KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(stale_template())
        response = client.post(
            f"/kanglong/simulation/plan/{created['run_id']}/confirm",
            json={"plan_version": created["plan_version"], "idempotency_key": "confirm-stale-template-0001"},
        )
        stored = repository.get_kanglong_run(created["run_id"])
    finally:
        repository.close()

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "blocked_plan_stale"
    assert stored is not None
    assert stored["status"] == "chain_ready"


def test_kanglong_template_confirm_idempotency_survives_template_edit(monkeypatch, tmp_path) -> None:
    repository, _, template = install_template_api_runtime(monkeypatch, tmp_path)
    client = TestClient(api_module.app)
    try:
        created = create_template_backed_plan(client, template)
        first = client.post(
            f"/kanglong/simulation/plan/{created['run_id']}/confirm",
            json={"plan_version": created["plan_version"], "idempotency_key": "confirm-template-once-0001"},
        )
        KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(stale_template())
        repeated = client.post(
            f"/kanglong/simulation/plan/{created['run_id']}/confirm",
            json={"plan_version": created["plan_version"], "idempotency_key": "confirm-template-once-0001"},
        )
    finally:
        repository.close()

    assert first.status_code == 200
    assert repeated.status_code == 200
    assert repeated.json() == first.json()


def test_kanglong_template_execute_blocks_when_template_changed_after_confirm(monkeypatch, tmp_path) -> None:
    repository, _, template = install_template_api_runtime(monkeypatch, tmp_path)
    client = TestClient(api_module.app)
    try:
        created = create_template_backed_plan(client, template)
        confirmed = client.post(
            f"/kanglong/simulation/plan/{created['run_id']}/confirm",
            json={"plan_version": created["plan_version"], "idempotency_key": "confirm-before-stale-0001"},
        )
        KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(stale_template())
        response = client.post(
            f"/kanglong/simulation/plan/{created['run_id']}/execute",
            json={"plan_version": created["plan_version"], "idempotency_key": "execute-stale-template-0001"},
        )
        stored = repository.get_kanglong_run(created["run_id"])
    finally:
        repository.close()

    assert confirmed.status_code == 200
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "blocked_plan_stale"
    assert stored is not None
    assert stored["status"] == "plan_confirmed"


def test_kanglong_template_execute_idempotency_survives_template_edit(monkeypatch, tmp_path) -> None:
    repository, _, template = install_template_api_runtime(monkeypatch, tmp_path)
    client = TestClient(api_module.app)
    try:
        created = create_template_backed_plan(client, template)
        confirmed = client.post(
            f"/kanglong/simulation/plan/{created['run_id']}/confirm",
            json={"plan_version": created["plan_version"], "idempotency_key": "confirm-template-exec-once-0001"},
        )
        first = client.post(
            f"/kanglong/simulation/plan/{created['run_id']}/execute",
            json={"plan_version": created["plan_version"], "idempotency_key": "execute-template-once-0001"},
        )
        KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(stale_template())
        repeated = client.post(
            f"/kanglong/simulation/plan/{created['run_id']}/execute",
            json={"plan_version": created["plan_version"], "idempotency_key": "execute-template-once-0001"},
        )
    finally:
        repository.close()

    assert confirmed.status_code == 200
    assert first.status_code == 200
    assert repeated.status_code == 200
    assert repeated.json() == first.json()


def test_kanglong_template_recover_blocks_when_template_changed_without_mutating_or_releasing_locks(
    monkeypatch,
    tmp_path,
) -> None:
    repository, _, template = install_template_api_runtime(monkeypatch, tmp_path)
    client = TestClient(api_module.app)
    lock_scope = "kanglong:ETHUSDC:account:tpl:tpl_eth_drop_001:main"
    try:
        created = create_template_backed_plan(client, template)
        repository.update_kanglong_run(
            created["run_id"],
            status="needs_abort_recover",
            available_actions=["recover"],
        )
        assert repository.acquire_kanglong_locks(run_id=created["run_id"], lock_scopes=[lock_scope], ttl_ms=60_000) is None
        before = repository.get_kanglong_run(created["run_id"])
        KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(stale_template())

        response = client.post(
            f"/kanglong/simulation/run/{created['run_id']}/recover",
            json={"idempotency_key": "recover-stale-template-0001", "release_reason": "operator review"},
        )
        after = repository.get_kanglong_run(created["run_id"])
        lock_conflict = repository.acquire_kanglong_locks(run_id="other-run", lock_scopes=[lock_scope], ttl_ms=60_000)
        events = repository.list_kanglong_events(created["run_id"], after_event_id=0, limit=10)
    finally:
        repository.close()

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "blocked_plan_stale"
    assert before is not None
    assert after is not None
    assert after["status"] == before["status"]
    assert after["report"] == before["report"]
    assert lock_conflict is not None
    assert lock_conflict["run_id"] == created["run_id"]
    assert events["events"] == []


def test_kanglong_template_recover_idempotency_survives_template_edit(monkeypatch, tmp_path) -> None:
    repository, _, template = install_template_api_runtime(monkeypatch, tmp_path)
    client = TestClient(api_module.app)
    try:
        created = create_template_backed_plan(client, template)
        repository.update_kanglong_run(
            created["run_id"],
            status="needs_abort_recover",
            available_actions=["recover"],
        )
        first = client.post(
            f"/kanglong/simulation/run/{created['run_id']}/recover",
            json={"idempotency_key": "recover-template-once-0001", "release_reason": "operator review"},
        )
        KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(stale_template())
        repeated = client.post(
            f"/kanglong/simulation/run/{created['run_id']}/recover",
            json={"idempotency_key": "recover-template-once-0001", "release_reason": "operator review"},
        )
    finally:
        repository.close()

    assert first.status_code == 200
    assert repeated.status_code == 200
    assert repeated.json() == first.json()


def test_kanglong_template_execute_uses_market_data_without_rebuilding_template_accounts(monkeypatch, tmp_path) -> None:
    repository, runtime_manager, template = install_template_api_runtime(monkeypatch, tmp_path)
    client = TestClient(api_module.app)

    async def fail_template_plan_collection(request):
        raise AssertionError("execute must not rebuild account snapshots from the template")

    try:
        created = create_template_backed_plan(client, template)
        confirmed = client.post(
            f"/kanglong/simulation/plan/{created['run_id']}/confirm",
            json={"plan_version": created["plan_version"], "idempotency_key": "confirm-template-exec-0001"},
        )
        monkeypatch.setattr(api_module, "_collect_template_kanglong_plan_inputs", fail_template_plan_collection)
        response = client.post(
            f"/kanglong/simulation/plan/{created['run_id']}/execute",
            json={"plan_version": created["plan_version"], "idempotency_key": "execute-template-market-0001"},
        )
        stored = wait_for_kanglong_status(repository, created["run_id"], {"running", "completed"})
        events = repository.list_kanglong_events(created["run_id"], after_event_id=0, limit=20)["events"]
    finally:
        repository.close()

    assert confirmed.status_code == 200
    assert response.status_code == 200
    assert response.json()["status"] == "execution_starting"
    assert runtime_manager.build_calls == ["market-main", "market-main", "market-main"]
    assert stored is not None
    assert stored["status"] in {"running", "completed"}
    assert any(event["event_type"] == "kanglong_round_completed" for event in events)
    assert not any(event["event_type"] == "kanglong_group_simulated" for event in events)


def test_kanglong_template_execution_snapshots_restore_non_default_leverage(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    repository = SqliteRepository(tmp_path / "kanglong.sqlite3")
    monkeypatch.setattr(api_module.app.state, "kanglong_service", KanglongSimulationService(repository), raising=False)
    runtime_manager = FakeTemplateRuntimeManager(FakeTemplateMarketGateway())
    monkeypatch.setattr(api_module.app.state, "runtime_manager", runtime_manager, raising=False)
    payload = template_payload()
    payload["main_account"]["leverage"] = 50
    payload["subaccounts"][0]["leverage"] = 50
    payload["subaccounts"][0]["qty"] = "1"
    template = KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(payload)
    client = TestClient(api_module.app)
    try:
        created = create_template_backed_plan(client, template, run_id="run-template-leverage")
        stored = repository.get_kanglong_run(created["run_id"])
    finally:
        repository.close()

    assert 50 != api_module.DEFAULT_LEVERAGE
    assert stored is not None
    assert stored["request"]["leverage_by_account_id"] == {
        "tpl:tpl_eth_drop_001:main": 50,
        "tpl:tpl_eth_drop_001:sub:sub-1": 50,
    }
    main_snapshot, subaccount_snapshots = api_module._template_execution_snapshots(stored)
    assert main_snapshot is not None
    assert subaccount_snapshots is not None
    assert main_snapshot.leverage == 50
    assert [snapshot.leverage for snapshot in subaccount_snapshots] == [50]


def test_kanglong_template_execute_blocks_when_frozen_account_snapshot_missing(monkeypatch, tmp_path) -> None:
    repository, runtime_manager, template = install_template_api_runtime(monkeypatch, tmp_path)
    client = TestClient(api_module.app)
    try:
        created = create_template_backed_plan(client, template)
        confirmed = client.post(
            f"/kanglong/simulation/plan/{created['run_id']}/confirm",
            json={"plan_version": created["plan_version"], "idempotency_key": "confirm-missing-snapshot-0001"},
        )
        stored = repository.get_kanglong_run(created["run_id"])
        assert stored is not None
        report = dict(stored["report"])
        report.pop("account_snapshot", None)
        report.pop("synthetic_account_state", None)
        repository.update_kanglong_run(
            created["run_id"],
            status=stored["status"],
            report=report,
        )

        response = client.post(
            f"/kanglong/simulation/plan/{created['run_id']}/execute",
            json={"plan_version": created["plan_version"], "idempotency_key": "execute-missing-snapshot-0001"},
        )
        after = repository.get_kanglong_run(created["run_id"])
        events = repository.list_kanglong_events(created["run_id"], after_event_id=0, limit=10)
    finally:
        repository.close()

    assert confirmed.status_code == 200
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "blocked_plan_recheck_failed"
    assert runtime_manager.build_calls == ["market-main", "market-main"]
    assert after is not None
    assert after["status"] == "plan_confirmed"
    assert "synthetic_account_state" not in after["report"]
    assert events["events"] == []


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


@pytest.mark.asyncio
async def test_collect_template_plan_inputs_uses_preview_accounts_and_market_gateway_only(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    template = KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(template_payload())
    gateway = FakeTemplateMarketGateway()
    runtime_manager = FakeTemplateRuntimeManager(gateway)
    monkeypatch.setattr(api_module.app.state, "runtime_manager", runtime_manager, raising=False)

    payload = await api_module._collect_kanglong_plan_inputs(
        KanglongPlanRequest(
            main_account_id="tpl:tpl_eth_drop_001:main",
            subaccount_ids=["tpl:tpl_eth_drop_001:sub:sub-1"],
            account_source="test_template",
            test_template_id=template["id"],
            template_content_hash=template["template_content_hash"],
            market_data_account_id="market-main",
        )
    )

    assert runtime_manager.build_calls == ["market-main"]
    assert [snapshot.account_id for snapshot in [payload["main_snapshot"], *payload["subaccount_snapshots"]]] == [
        "tpl:tpl_eth_drop_001:main",
        "tpl:tpl_eth_drop_001:sub:sub-1",
    ]
    assert payload["rules"] == SymbolRules("ETHUSDC", Decimal("0.01"), Decimal("0.01"), Decimal("0.01"), Decimal("5"), 125)
    assert payload["close_price"] == Decimal("2443.19")
    assert payload["open_price"] == Decimal("2443.23")
    assert payload["request_metadata"]["account_source"] == "test_template"
    assert payload["request_metadata"]["test_template_id"] == "tpl_eth_drop_001"
    assert payload["request_metadata"]["market_data_account_id"] == "market-main"
    assert payload["request_metadata"]["template_runtime_account_map"] == {
        "test-main": "tpl:tpl_eth_drop_001:main",
        "test-sub-1": "tpl:tpl_eth_drop_001:sub:sub-1",
    }
    assert payload["account_snapshot_payload"]["account_source"] == "test_template"
    assert [account["account_id"] for account in payload["account_snapshot_payload"]["accounts"]] == [
        "tpl:tpl_eth_drop_001:main",
        "tpl:tpl_eth_drop_001:sub:sub-1",
    ]


@pytest.mark.asyncio
async def test_collect_template_plan_inputs_rejects_outside_accounts(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    template = KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(template_payload())
    monkeypatch.setattr(api_module.app.state, "runtime_manager", FakeTemplateRuntimeManager(FakeTemplateMarketGateway()), raising=False)

    with pytest.raises(HTTPException) as exc:
        await api_module._collect_kanglong_plan_inputs(
            KanglongPlanRequest(
                main_account_id="tpl:tpl_eth_drop_001:main",
                subaccount_ids=["external-sub"],
                account_source="test_template",
                test_template_id=template["id"],
                template_content_hash=template["template_content_hash"],
                market_data_account_id="market-main",
            )
        )

    assert exc.value.status_code == 400
    assert exc.value.detail["code"] == "kanglong_test_template_account_mismatch"


@pytest.mark.asyncio
async def test_collect_template_plan_inputs_rejects_subaccount_as_main(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    template = KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(template_payload())
    monkeypatch.setattr(api_module.app.state, "runtime_manager", FakeTemplateRuntimeManager(FakeTemplateMarketGateway()), raising=False)

    with pytest.raises(HTTPException) as exc:
        await api_module._collect_kanglong_plan_inputs(
            KanglongPlanRequest(
                main_account_id="tpl:tpl_eth_drop_001:sub:sub-1",
                subaccount_ids=["tpl:tpl_eth_drop_001:main"],
                account_source="test_template",
                test_template_id=template["id"],
                template_content_hash=template["template_content_hash"],
                market_data_account_id="market-main",
            )
        )

    assert exc.value.status_code == 400
    assert exc.value.detail["code"] == "kanglong_test_template_account_mismatch"


@pytest.mark.asyncio
async def test_collect_template_plan_inputs_rejects_main_account_in_subaccounts(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    template = KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(template_payload())
    monkeypatch.setattr(api_module.app.state, "runtime_manager", FakeTemplateRuntimeManager(FakeTemplateMarketGateway()), raising=False)

    with pytest.raises(HTTPException) as exc:
        await api_module._collect_kanglong_plan_inputs(
            KanglongPlanRequest(
                main_account_id="tpl:tpl_eth_drop_001:main",
                subaccount_ids=["tpl:tpl_eth_drop_001:main"],
                account_source="test_template",
                test_template_id=template["id"],
                template_content_hash=template["template_content_hash"],
                market_data_account_id="market-main",
            )
        )

    assert exc.value.status_code == 400
    assert exc.value.detail["code"] == "kanglong_test_template_account_mismatch"


@pytest.mark.asyncio
async def test_collect_template_plan_inputs_rejects_stale_hash(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    template = KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(template_payload())
    monkeypatch.setattr(api_module.app.state, "runtime_manager", FakeTemplateRuntimeManager(FakeTemplateMarketGateway()), raising=False)

    with pytest.raises(HTTPException) as exc:
        await api_module._collect_kanglong_plan_inputs(
            KanglongPlanRequest(
                main_account_id="tpl:tpl_eth_drop_001:main",
                subaccount_ids=["tpl:tpl_eth_drop_001:sub:sub-1"],
                account_source="test_template",
                test_template_id=template["id"],
                template_content_hash="sha256:stale",
                market_data_account_id="market-main",
            )
        )

    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "blocked_plan_stale"


@pytest.mark.asyncio
async def test_collect_template_plan_inputs_rejects_synthetic_market_data_account_before_gateway(monkeypatch, tmp_path) -> None:
    install_template_settings(monkeypatch, tmp_path)
    template = KanglongTemplateStore(api_module.app.state.settings.kanglong_test_templates_file).upsert_template(template_payload())

    class RecordingRuntimeManager:
        def __init__(self) -> None:
            self.build_calls: list[str] = []

        def build_temporary_gateway(self, account_id: str):
            self.build_calls.append(account_id)
            raise AssertionError("synthetic market data account must be rejected before gateway construction")

    runtime_manager = RecordingRuntimeManager()
    monkeypatch.setattr(api_module.app.state, "runtime_manager", runtime_manager, raising=False)

    with pytest.raises(HTTPException) as exc:
        await api_module._collect_kanglong_plan_inputs(
            KanglongPlanRequest(
                main_account_id="tpl:tpl_eth_drop_001:main",
                subaccount_ids=["tpl:tpl_eth_drop_001:sub:sub-1"],
                account_source="test_template",
                test_template_id=template["id"],
                template_content_hash=template["template_content_hash"],
                market_data_account_id=" tpl:tpl_eth_drop_001:main ",
            )
        )

    assert exc.value.status_code == 400
    assert exc.value.detail["code"] == "kanglong_test_template_market_data_account_unavailable"
    assert runtime_manager.build_calls == []


@pytest.mark.asyncio
async def test_collect_runtime_plan_inputs_rejects_template_fields_before_building_gateways(monkeypatch) -> None:
    runtime_manager = FakeTemplateRuntimeManager(FakeTemplateMarketGateway())
    monkeypatch.setattr(api_module.app.state, "runtime_manager", runtime_manager, raising=False)

    with pytest.raises(HTTPException) as exc:
        await api_module._collect_kanglong_plan_inputs(
            KanglongPlanRequest(
                main_account_id="main",
                subaccount_ids=["sub1"],
                test_template_id="tpl_eth_drop_001",
                template_content_hash="sha256:abc",
                market_data_account_id="market-main",
            )
        )

    assert exc.value.status_code == 400
    assert exc.value.detail["code"] == "kanglong_test_template_account_mismatch"
    assert runtime_manager.build_calls == []
