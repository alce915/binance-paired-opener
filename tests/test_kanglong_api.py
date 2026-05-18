from __future__ import annotations

from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from paired_opener import api as api_module
from paired_opener.classified_gateway import ClassifiedExchangeGateway
from paired_opener.config import Settings
from paired_opener.domain import PositionSide, Quote, SymbolRules
from paired_opener.errors import TradingError
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.service import KanglongSimulationService
from paired_opener.schemas import (
    KanglongActionRequest,
    KanglongPlanRequest,
    KanglongSimulationRunRequest,
)
from paired_opener.storage import SqliteRepository
from tests.test_kanglong_precheck import snapshot


def test_kanglong_request_defaults_to_ethusdc_and_auto_side() -> None:
    request = KanglongSimulationRunRequest(main_account_id="main", subaccount_ids=["sub1"])
    plan_request = KanglongPlanRequest(main_account_id="main", subaccount_ids=["sub1"])

    assert request.symbol == "ETHUSDC"
    assert request.selected_side is None
    assert request.mode == "simulation"
    assert plan_request.symbol == "ETHUSDC"
    assert plan_request.selected_side is None
    assert plan_request.mode == "simulation"


class StubKanglongService:
    def __init__(self) -> None:
        self.plans: dict[str, dict] = {}
        self.list_events_calls: list[tuple[str, int | None, int]] = []

    def create_plan(self, **kwargs) -> dict:
        payload = {
            "run_id": kwargs["run_id"],
            "status": "chain_ready",
            "plan_version": "plan-1",
            "snapshot_bundle_id": kwargs.get("snapshot_bundle_id") or "snap-1",
            "available_actions": ["confirm", "refresh_plan"],
            "report": {"summary": {"group_count": 0}},
        }
        self.plans[payload["run_id"]] = payload
        return payload

    def confirm_plan(self, **kwargs) -> dict:
        return {
            "run_id": kwargs["run_id"],
            "status": "plan_confirmed",
            "plan_version": kwargs["plan_version"],
            "snapshot_bundle_id": "snap-1",
            "available_actions": ["execute", "refresh_plan"],
            "report": {},
        }

    def execute_plan(self, **kwargs) -> dict:
        return {
            "run_id": kwargs["run_id"],
            "status": "completed",
            "plan_version": kwargs["plan_version"],
            "snapshot_bundle_id": "snap-1",
            "available_actions": ["view_report"],
            "report": {},
        }

    def get_run(self, run_id: str) -> dict | None:
        return self.plans.get(run_id)

    def list_events(self, run_id: str, after_event_id: int | None = None, limit: int = 200) -> dict:
        self.list_events_calls.append((run_id, after_event_id, limit))
        return {
            "run_id": run_id,
            "events": [],
            "next_after_event_id": int(after_event_id or 0),
            "latest_event_id": 0,
            "has_more": False,
        }


class FakeKanglongGateway:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self.closed = False
        self.snapshot_called = False
        self.rules_called = False
        self.quote_called = False

    async def get_unified_account_snapshot(self) -> dict:
        self.snapshot_called = True
        return {
            "account_id": self.account_id,
            "account_name": self.account_id.upper(),
            "positions": [
                {
                    "symbol": "ETHUSDC",
                    "position_side": "LONG",
                    "qty": "1.25" if self.account_id == "sub1" else "0",
                    "entry_price": "3000",
                    "mark_price": "3100",
                    "unrealized_pnl": "12.5" if self.account_id == "sub1" else "0",
                }
            ],
            "open_orders": [],
            "totals": {
                "available_balance": "100",
                "equity": "100",
                "margin": "0",
            },
            "updated_at": f"{self.account_id}-snapshot",
        }

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        self.rules_called = True
        return SymbolRules(symbol, Decimal("0.01"), Decimal("0.001"), Decimal("0.001"), Decimal("5"), 125)

    async def get_quote(self, symbol: str) -> Quote:
        self.quote_called = True
        return Quote(symbol, Decimal("3100.00"), Decimal("3100.50"))

    async def close(self) -> None:
        self.closed = True


class FakeRuntimeManager:
    def __init__(self) -> None:
        self.gateways: dict[str, FakeKanglongGateway] = {}
        self.build_calls: list[str] = []

    def list_accounts(self) -> list[dict[str, object]]:
        return [
            {"id": "main", "name": "MAIN", "is_active": True},
            {"id": "sub1", "name": "SUB1", "is_active": False},
            {"id": "sub2", "name": "SUB2", "is_active": False},
        ]

    def build_temporary_gateway(self, account_id: str) -> FakeKanglongGateway:
        self.build_calls.append(account_id)
        gateway = FakeKanglongGateway(account_id)
        self.gateways[account_id] = gateway
        return gateway


class RejectingRuntimeManager:
    def __init__(self) -> None:
        self.build_calls: list[str] = []

    def build_temporary_gateway(self, account_id: str):
        self.build_calls.append(account_id)
        raise AssertionError("duplicate validation should run before gateway construction")


class SnapshotFailureDelegate:
    async def get_unified_account_snapshot(self) -> dict:
        raise ValueError("snapshot failed")


@pytest.mark.asyncio
async def test_kanglong_split_api_plan_confirm_execute() -> None:
    service = StubKanglongService()
    api_module.app.state.kanglong_service = service
    original_collector = api_module._collect_kanglong_plan_inputs

    async def fake_collector(request):
        return {
            "symbol": request.symbol,
            "main_account_id": request.main_account_id,
            "subaccount_ids": request.subaccount_ids,
            "selected_side": request.selected_side,
            "snapshot_bundle_id": "snap-1",
        }

    api_module._collect_kanglong_plan_inputs = fake_collector

    try:
        plan = await api_module.create_kanglong_simulation_plan(
            KanglongPlanRequest(main_account_id="main", subaccount_ids=["sub1"])
        )
        confirmed = await api_module.confirm_kanglong_simulation_plan(
            plan.run_id,
            KanglongActionRequest(plan_version=plan.plan_version, idempotency_key="confirm-0001"),
        )
        executed = await api_module.execute_kanglong_simulation_plan(
            plan.run_id,
            KanglongActionRequest(plan_version=plan.plan_version, idempotency_key="execute-0001"),
        )
        events = await api_module.get_kanglong_simulation_events(plan.run_id, after_event_id=0, limit=50)
    finally:
        api_module._collect_kanglong_plan_inputs = original_collector

    assert plan.status == "chain_ready"
    assert confirmed.status == "plan_confirmed"
    assert executed.status == "completed"
    assert events.latest_event_id == 0


@pytest.mark.asyncio
async def test_classified_gateway_classifies_unified_snapshot_errors() -> None:
    gateway = ClassifiedExchangeGateway(SnapshotFailureDelegate())

    with pytest.raises(TradingError) as exc:
        await gateway.get_unified_account_snapshot()

    assert exc.value.code == "invalid_parameter"
    assert exc.value.context["operation"] == "get_unified_account_snapshot"


@pytest.mark.asyncio
async def test_collect_kanglong_plan_inputs_uses_snapshots_and_closes_gateways(monkeypatch) -> None:
    runtime_manager = FakeRuntimeManager()
    api_module.app.state.runtime_manager = runtime_manager
    api_module.app.state.settings = Settings()
    monkeypatch.setattr(
        api_module,
        "load_kanglong_symbol_config",
        lambda settings, symbol: KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
    )

    payload = await api_module._collect_kanglong_plan_inputs(
        KanglongPlanRequest(main_account_id="main", subaccount_ids=["sub1", "sub2"])
    )

    assert runtime_manager.build_calls == ["main", "sub1", "sub2"]
    assert payload["main_snapshot"].account_id == "main"
    assert [snapshot.account_id for snapshot in payload["subaccount_snapshots"]] == ["sub1", "sub2"]
    assert payload["rules"].symbol == "ETHUSDC"
    assert payload["close_price"] == Decimal("3100.00")
    assert payload["open_price"] == Decimal("3100.50")
    assert all(gateway.snapshot_called for gateway in runtime_manager.gateways.values())
    assert runtime_manager.gateways["main"].rules_called is True
    assert runtime_manager.gateways["main"].quote_called is True
    assert all(gateway.closed for gateway in runtime_manager.gateways.values())


@pytest.mark.asyncio
async def test_kanglong_account_snapshot_route_returns_symbol_positions(monkeypatch) -> None:
    runtime_manager = FakeRuntimeManager()
    api_module.app.state.runtime_manager = runtime_manager
    api_module.app.state.settings = Settings()

    response = await api_module.list_kanglong_simulation_accounts(symbol="ETHUSDC")

    accounts = response["accounts"]
    sub1 = next(account for account in accounts if account["id"] == "sub1")
    assert runtime_manager.build_calls == ["main", "sub1", "sub2"]
    assert sub1["positions"][0]["symbol"] == "ETHUSDC"
    assert sub1["positions"][0]["position_side"] == "LONG"
    assert sub1["positions"][0]["qty"] == "1.25"
    assert all(gateway.snapshot_called for gateway in runtime_manager.gateways.values())
    assert all(gateway.closed for gateway in runtime_manager.gateways.values())


@pytest.mark.asyncio
async def test_create_kanglong_plan_wraps_collector_errors() -> None:
    original_collector = api_module._collect_kanglong_plan_inputs

    async def failing_collector(request):
        raise ValueError("collector failed")

    api_module._collect_kanglong_plan_inputs = failing_collector
    try:
        with pytest.raises(api_module.HTTPException) as exc:
            await api_module.create_kanglong_simulation_plan(
                KanglongPlanRequest(main_account_id="main", subaccount_ids=["sub1"])
            )
    finally:
        api_module._collect_kanglong_plan_inputs = original_collector

    assert exc.value.status_code == 422
    assert exc.value.detail["code"] == "kanglong_plan_failed"
    assert exc.value.detail["source"] == "service"


@pytest.mark.asyncio
async def test_kanglong_plan_rejects_main_account_as_subaccount_before_gateway_build() -> None:
    runtime_manager = RejectingRuntimeManager()
    api_module.app.state.runtime_manager = runtime_manager

    with pytest.raises(api_module.HTTPException) as exc:
        await api_module._collect_kanglong_plan_inputs(
            KanglongPlanRequest(main_account_id="Main", subaccount_ids=["sub1", "main"])
        )

    assert exc.value.status_code == 400
    assert exc.value.detail["code"] == "kanglong_duplicate_account"
    assert runtime_manager.build_calls == []


@pytest.mark.asyncio
async def test_kanglong_plan_rejects_duplicate_subaccounts_before_gateway_build() -> None:
    runtime_manager = RejectingRuntimeManager()
    api_module.app.state.runtime_manager = runtime_manager

    with pytest.raises(api_module.HTTPException) as exc:
        await api_module._collect_kanglong_plan_inputs(
            KanglongPlanRequest(main_account_id="main", subaccount_ids=["Sub1", "sub1"])
        )

    assert exc.value.status_code == 400
    assert exc.value.detail["code"] == "kanglong_duplicate_account"
    assert runtime_manager.build_calls == []


@pytest.mark.parametrize(
    ("main_account_id", "subaccount_ids", "rejected_account_ids"),
    [
        ("tpl:tpl_eth_drop_001:main", ["sub1"], ["tpl:tpl_eth_drop_001:main"]),
        ("main", ["tpl:tpl_eth_drop_001:sub:sub-1"], ["tpl:tpl_eth_drop_001:sub:sub-1"]),
    ],
)
def test_runtime_kanglong_plan_rejects_template_account_ids_before_gateway_build(
    main_account_id: str,
    subaccount_ids: list[str],
    rejected_account_ids: list[str],
) -> None:
    service = StubKanglongService()
    runtime_manager = FakeRuntimeManager()
    api_module.app.state.kanglong_service = service
    api_module.app.state.runtime_manager = runtime_manager
    api_module.app.state.settings = Settings()

    response = TestClient(api_module.app).post(
        "/kanglong/simulation/plan",
        json={
            "main_account_id": main_account_id,
            "subaccount_ids": subaccount_ids,
            "account_source": "runtime",
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == {
        "code": "kanglong_test_template_account_mismatch",
        "account_ids": rejected_account_ids,
    }
    assert runtime_manager.build_calls == []


@pytest.mark.asyncio
async def test_confirm_kanglong_plan_preserves_service_error_metadata() -> None:
    class StalePlanService:
        def confirm_plan(self, **kwargs) -> dict:
            return {
                "run_id": kwargs["run_id"],
                "status": "blocked_plan_stale",
                "plan_version": "plan-current",
                "snapshot_bundle_id": "snap-current",
                "available_actions": ["refresh_plan"],
                "report": {},
                "error_code": "blocked_plan_stale",
                "requested_plan_version": kwargs["plan_version"],
                "current_status": "chain_ready",
            }

    api_module.app.state.kanglong_service = StalePlanService()

    response = await api_module.confirm_kanglong_simulation_plan(
        "run-1",
        KanglongActionRequest(plan_version="plan-old", idempotency_key="confirm-0002"),
    )

    assert response.status == "blocked_plan_stale"
    assert response.error_code == "blocked_plan_stale"
    assert response.requested_plan_version == "plan-old"
    assert response.current_status == "chain_ready"


@pytest.mark.asyncio
async def test_old_kanglong_run_endpoint_is_deprecated() -> None:
    with pytest.raises(api_module.HTTPException) as exc:
        await api_module.run_kanglong_simulation(
            KanglongSimulationRunRequest(main_account_id="main", subaccount_ids=["sub1"])
        )

    assert exc.value.status_code == 410
    assert exc.value.detail["code"] == "kanglong_run_endpoint_deprecated"
    assert exc.value.detail["replacement"] == "/kanglong/simulation/plan"


def test_active_kanglong_run_route_is_not_captured_as_run_id() -> None:
    class CollisionProbeService:
        def __init__(self) -> None:
            self.active_calls = 0
            self.get_run_calls: list[str] = []

        def active_run(self) -> dict:
            self.active_calls += 1
            return {
                "run_id": "active-run",
                "status": "plan_confirmed",
                "plan_version": "plan-active",
                "available_actions": ["execute"],
                "report": {},
            }

        def get_run(self, run_id: str) -> dict:
            self.get_run_calls.append(run_id)
            return {
                "run_id": f"parameterized-{run_id}",
                "status": "chain_ready",
                "available_actions": ["confirm"],
                "report": {},
            }

    service = CollisionProbeService()
    original_service = getattr(api_module.app.state, "kanglong_service", None)
    api_module.app.state.kanglong_service = service
    try:
        response = TestClient(api_module.app).get("/kanglong/simulation/run/active")
    finally:
        if original_service is not None:
            api_module.app.state.kanglong_service = original_service

    assert response.status_code == 200
    assert response.json()["run_id"] == "active-run"
    assert service.active_calls == 1
    assert service.get_run_calls == []


def test_missing_kanglong_run_events_returns_404_without_listing_events() -> None:
    service = StubKanglongService()
    original_service = getattr(api_module.app.state, "kanglong_service", None)
    api_module.app.state.kanglong_service = service
    try:
        response = TestClient(api_module.app).get("/kanglong/simulation/run/missing-run/events")
    finally:
        if original_service is not None:
            api_module.app.state.kanglong_service = original_service

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "kanglong_run_not_found"
    assert response.json()["detail"]["run_id"] == "missing-run"
    assert service.list_events_calls == []


def test_kanglong_service_report_contains_plan_events_and_costs(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)

    try:
        service.create_draft_run(
            run_id="run-1",
            symbol="ETHUSDC",
            main_account_id="main",
            subaccount_ids=["sub1", "sub2"],
        )

        payload = service.simulate(
            run_id="run-1",
            symbol="ETHUSDC",
            main_snapshot=snapshot("main", "0", "0", "0", "0"),
            subaccount_snapshots=[
                snapshot("sub1", "1", "1", "100", "0"),
                snapshot("sub2", "1", "1", "80", "0"),
            ],
            selected_side=PositionSide.LONG,
            config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
            rules=SymbolRules("ETHUSDC", Decimal("0.01"), Decimal("0.001"), Decimal("0.001"), Decimal("5"), 125),
            close_price=Decimal("3100.00"),
            open_price=Decimal("3100.50"),
            fee_rate=Decimal("0.0005"),
        )

        stored = repository.get_kanglong_run("run-1")
    finally:
        repository.close()

    assert payload["result_grade"] == "safe_closed"
    assert payload["report"]["selected_side"] == "LONG"
    assert payload["report"]["groups"][0]["from_account_id"] == "sub1"
    assert payload["report"]["groups"][0]["to_account_id"] == "main"
    assert payload["report"]["costs"]["transfer_fee_cost"] != "0"
    assert stored is not None
    assert stored["status"] == "completed"
    assert stored["report"]["groups"][0]["from_account_id"] == "sub1"
