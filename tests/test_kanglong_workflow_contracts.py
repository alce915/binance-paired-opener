from __future__ import annotations

from decimal import Decimal

import pytest
from fastapi import HTTPException

from paired_opener import api as api_module
from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import KanglongRunStatus
from paired_opener.kanglong.service import KanglongSimulationService
from paired_opener.schemas import (
    KanglongActionRequest,
    KanglongEventsResponse,
    KanglongPlanRequest,
    KanglongPlanResponse,
)
from paired_opener.storage import SqliteRepository
from tests.test_kanglong_precheck import snapshot


def test_kanglong_workflow_status_values_are_stable() -> None:
    assert KanglongRunStatus.DRAFT_PLAN.value == "draft_plan"
    assert KanglongRunStatus.PLAN_CONFIRMED.value == "plan_confirmed"
    assert KanglongRunStatus.EXECUTION_STARTING.value == "execution_starting"
    assert KanglongRunStatus.BLOCKED_PLAN_STALE.value == "blocked_plan_stale"
    assert KanglongRunStatus.BLOCKED_PLAN_RECHECK_FAILED.value == "blocked_plan_recheck_failed"
    assert KanglongRunStatus.PAUSED_PLAN_RECHECK_CHANGED.value == "paused_plan_recheck_changed"


def test_plan_request_defaults_to_simulation_ethusdc_auto_side() -> None:
    request = KanglongPlanRequest(main_account_id="main", subaccount_ids=["sub1"])
    assert request.mode == "simulation"
    assert request.symbol == "ETHUSDC"
    assert request.selected_side is None


def test_action_request_requires_idempotency_key() -> None:
    request = KanglongActionRequest(plan_version="plan-1", idempotency_key="confirm-1")
    assert request.plan_version == "plan-1"
    assert request.idempotency_key == "confirm-1"


def test_plan_response_exposes_actions_and_snapshot_bundle() -> None:
    response = KanglongPlanResponse(
        run_id="run-1",
        status="chain_ready",
        plan_version="plan-1",
        snapshot_bundle_id="snap-1",
        available_actions=["confirm", "refresh_plan"],
        report={"summary": {"group_count": 2}},
    )
    assert response.snapshot_bundle_id == "snap-1"
    assert response.available_actions == ["confirm", "refresh_plan"]


def test_events_response_has_incremental_cursor_fields() -> None:
    response = KanglongEventsResponse(
        run_id="run-1",
        events=[],
        next_after_event_id=10,
        latest_event_id=10,
        has_more=False,
    )
    assert response.next_after_event_id == 10
    assert response.latest_event_id == 10
    assert response.has_more is False


def _rules() -> SymbolRules:
    return SymbolRules("ETHUSDC", Decimal("0.01"), Decimal("0.001"), Decimal("0.001"), Decimal("5"), 125)


def _create_ready_plan(
    service: KanglongSimulationService,
    *,
    run_id: str,
    snapshot_bundle_id: str = "snap-1",
) -> dict:
    return service.create_plan(
        run_id=run_id,
        symbol="ETHUSDC",
        main_snapshot=snapshot("main", "0", "0", "0", "0"),
        subaccount_snapshots=[
            snapshot("sub1", "1", "1", "100", "0"),
            snapshot("sub2", "1", "1", "80", "0"),
        ],
        main_account_id="main",
        subaccount_ids=["sub1", "sub2"],
        selected_side=PositionSide.LONG,
        snapshot_bundle_id=snapshot_bundle_id,
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
        rules=_rules(),
        close_price=Decimal("3100.00"),
        open_price=Decimal("3100.50"),
        fee_rate=Decimal("0.0005"),
    )


def _template_account_snapshot_payload() -> dict:
    return {
        "account_source": "test_template",
        "template_id": "tpl_eth_drop_001",
        "template_content_hash": "sha256:template-v1",
        "accounts": [
            {
                "account_id": "tpl:tpl_eth_drop_001:main",
                "template_account_id": "test-main",
                "name": "Test Main",
                "role": "main",
                "collateral": "10000",
                "wallet_balance": "10000",
                "total_unrealized_pnl": "0",
                "equity": "10000",
                "available_balance": "10000",
                "margin": "0",
                "margin_deficit": "0",
                "positions": [],
            },
            {
                "account_id": "tpl:tpl_eth_drop_001:sub:sub-1",
                "template_account_id": "test-sub-1",
                "row_id": "sub-1",
                "name": "Test Sub 1",
                "role": "subaccount",
                "collateral": "5000",
                "wallet_balance": "5000",
                "total_unrealized_pnl": "100",
                "equity": "5100",
                "available_balance": "4975",
                "margin": "125",
                "margin_deficit": "0",
                "positions": [
                    {
                        "symbol": "ETHUSDC",
                        "position_side": "LONG",
                        "qty": "1",
                        "entry_price": "3000",
                        "mark_price": "3100",
                        "unrealized_pnl": "100",
                        "liquidation_price": "0",
                        "notional": "3100",
                        "leverage": 75,
                        "margin": "41.33333333333333333333333333",
                    },
                    {
                        "symbol": "ETHUSDC",
                        "position_side": "SHORT",
                        "qty": "1",
                        "entry_price": "3100",
                        "mark_price": "3100",
                        "unrealized_pnl": "0",
                        "liquidation_price": "0",
                        "notional": "3100",
                        "leverage": 75,
                        "margin": "41.33333333333333333333333333",
                    },
                ],
            },
        ],
    }


def _create_ready_template_plan(
    service: KanglongSimulationService,
    *,
    run_id: str,
    snapshot_bundle_id: str = "snap-template",
) -> dict:
    return service.create_plan(
        run_id=run_id,
        symbol="ETHUSDC",
        main_snapshot=snapshot("tpl:tpl_eth_drop_001:main", "0", "0", "0", "0"),
        subaccount_snapshots=[snapshot("tpl:tpl_eth_drop_001:sub:sub-1", "1", "1", "100", "0")],
        main_account_id="tpl:tpl_eth_drop_001:main",
        subaccount_ids=["tpl:tpl_eth_drop_001:sub:sub-1"],
        selected_side=PositionSide.LONG,
        snapshot_bundle_id=snapshot_bundle_id,
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
        rules=_rules(),
        close_price=Decimal("3100.00"),
        open_price=Decimal("3100.50"),
        fee_rate=Decimal("0.0005"),
        request_metadata={
            "account_source": "test_template",
            "test_template_id": "tpl_eth_drop_001",
            "template_content_hash": "sha256:template-v1",
            "market_data_account_id": "market-main",
        },
        account_snapshot_payload=_template_account_snapshot_payload(),
    )


def _create_blocked_plan(
    service: KanglongSimulationService,
    *,
    run_id: str,
    snapshot_bundle_id: str = "snap-blocked",
) -> dict:
    return service.create_plan(
        run_id=run_id,
        symbol="ETHUSDC",
        main_snapshot=snapshot("main", "0.01", "0", "0", "0"),
        subaccount_snapshots=[snapshot("sub1", "1", "1", "100", "0")],
        main_account_id="main",
        subaccount_ids=["sub1"],
        selected_side=PositionSide.LONG,
        snapshot_bundle_id=snapshot_bundle_id,
        config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
        rules=_rules(),
        close_price=Decimal("3100.00"),
        open_price=Decimal("3100.50"),
        fee_rate=Decimal("0.0005"),
    )


def _assert_plan_response_shape(response: dict) -> None:
    assert response["run_id"]
    assert isinstance(response["status"], str)
    assert isinstance(response["plan_version"], str)
    assert isinstance(response["snapshot_bundle_id"], str)
    assert isinstance(response["available_actions"], list)
    assert isinstance(response["report"], dict)


def test_service_plan_confirm_execute_records_state_and_events(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)
    try:
        plan = _create_ready_plan(service, run_id="run-1")
        confirmed = service.confirm_plan(
            run_id="run-1",
            plan_version=plan["plan_version"],
            idempotency_key="confirm-0001",
            operator="tester",
            confirmed_warning_codes=[],
        )
        executed = service.execute_plan(
            run_id="run-1",
            plan_version=plan["plan_version"],
            idempotency_key="execute-0001",
            close_price=Decimal("3100.00"),
            open_price=Decimal("3100.50"),
            fee_rate=Decimal("0.0005"),
        )
        events = service.list_events("run-1", after_event_id=0, limit=10)
    finally:
        repository.close()

    assert plan["status"] == "chain_ready"
    assert "confirm" in plan["available_actions"]
    assert plan["report"]["summary"]["round_count"] == 12
    assert plan["report"]["summary"]["planned_release_qty"] == "1"
    assert confirmed["status"] == "plan_confirmed"
    assert "execute" in confirmed["available_actions"]
    assert executed["status"] == "completed"
    assert events["latest_event_id"] > 0


def test_service_execute_persists_synthetic_template_state(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)
    try:
        plan = _create_ready_template_plan(service, run_id="run-template-execute")
        confirmed = service.confirm_plan(
            run_id="run-template-execute",
            plan_version=plan["plan_version"],
            idempotency_key="confirm-template-0001",
            operator="tester",
            confirmed_warning_codes=[],
        )
        executed = service.execute_plan(
            run_id="run-template-execute",
            plan_version=plan["plan_version"],
            idempotency_key="execute-template-0001",
            close_price=Decimal("3100.00"),
            open_price=Decimal("3100.50"),
            fee_rate=Decimal("0.0005"),
        )
        stored = repository.get_kanglong_run("run-template-execute")
    finally:
        repository.close()

    assert confirmed["status"] == "plan_confirmed"
    assert executed["status"] == "completed"
    assert stored is not None
    synthetic_state = stored["report"]["synthetic_account_state"]
    assert synthetic_state["account_source"] == "test_template"
    assert synthetic_state["state_version"].startswith("run-template-execute:")
    account_ids = [account["account_id"] for account in synthetic_state["accounts"]]
    assert account_ids == ["tpl:tpl_eth_drop_001:main", "tpl:tpl_eth_drop_001:sub:sub-1"]
    sub = synthetic_state["accounts"][1]
    main = synthetic_state["accounts"][0]
    assert sub["positions"][0]["qty"] == "1.00"
    assert main["positions"][0]["position_side"] == "LONG"
    assert main["positions"][0]["qty"] == "0.00"


def test_service_active_run_returns_latest_restorable_run_with_actions(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)
    try:
        repository.create_kanglong_run(
            {
                "run_id": "older-open",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub1"],
                "status": KanglongRunStatus.CHAIN_READY.value,
                "plan_version": "plan-older",
                "available_actions": ["confirm", "refresh_plan"],
                "report": {"summary": {"group_count": 1, "round_count": 2, "planned_release_qty": "0.5"}},
                "created_at": "2026-05-17T01:00:00+00:00",
                "updated_at": "2026-05-17T01:00:00+00:00",
            }
        )
        repository.create_kanglong_run(
            {
                "run_id": "latest-open",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub1", "sub2"],
                "status": KanglongRunStatus.PLAN_CONFIRMED.value,
                "plan_version": "plan-latest",
                "available_actions": ["execute", "refresh_plan"],
                "report": {"summary": {"group_count": 2, "round_count": 4, "planned_release_qty": "1"}},
                "created_at": "2026-05-17T02:00:00+00:00",
                "updated_at": "2026-05-17T02:00:00+00:00",
            }
        )
        repository.create_kanglong_run(
            {
                "run_id": "newer-completed",
                "symbol": "ETHUSDC",
                "main_account_id": "main",
                "subaccount_ids": ["sub1"],
                "status": KanglongRunStatus.COMPLETED.value,
                "plan_version": "plan-completed",
                "available_actions": ["view_report"],
                "report": {"summary": {"group_count": 9}},
                "created_at": "2026-05-17T03:00:00+00:00",
                "updated_at": "2026-05-17T03:00:00+00:00",
            }
        )

        active = service.active_run()
    finally:
        repository.close()

    assert active is not None
    assert active["run_id"] == "latest-open"
    assert active["status"] == "plan_confirmed"
    assert active["available_actions"] == ["execute", "refresh_plan"]
    assert active["report_summary"]["group_count"] == 2


def test_service_does_not_confirm_blocked_plan_or_store_idempotency(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)
    try:
        blocked = _create_blocked_plan(service, run_id="blocked-run")
        blocked_response = service.confirm_plan(
            run_id="blocked-run",
            plan_version=blocked["plan_version"],
            idempotency_key="confirm-blocked-0001",
            operator="tester",
            confirmed_warning_codes=[],
        )
        stored_blocked = repository.get_kanglong_run("blocked-run")

        ready = _create_ready_plan(service, run_id="ready-after-blocked", snapshot_bundle_id="snap-ready")
        confirmed = service.confirm_plan(
            run_id="ready-after-blocked",
            plan_version=ready["plan_version"],
            idempotency_key="confirm-blocked-0001",
            operator="tester",
            confirmed_warning_codes=[],
        )
    finally:
        repository.close()

    assert blocked["status"] == "blocked_main_not_flat"
    _assert_plan_response_shape(blocked_response)
    assert blocked_response["status"] != "plan_confirmed"
    assert "execute" not in blocked_response["available_actions"]
    assert stored_blocked is not None
    assert stored_blocked["status"] == "blocked_main_not_flat"
    assert confirmed["status"] == "plan_confirmed"


def test_confirm_missing_run_does_not_poison_idempotency_key(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)
    try:
        missing = service.confirm_plan(
            run_id="missing-run",
            plan_version="plan-missing",
            idempotency_key="confirm-missing-0001",
            operator="tester",
            confirmed_warning_codes=[],
        )
        ready = _create_ready_plan(service, run_id="ready-after-missing", snapshot_bundle_id="snap-ready")
        confirmed = service.confirm_plan(
            run_id="ready-after-missing",
            plan_version=ready["plan_version"],
            idempotency_key="confirm-missing-0001",
            operator="tester",
            confirmed_warning_codes=[],
        )
    finally:
        repository.close()

    _assert_plan_response_shape(missing)
    assert missing["status"] == "kanglong_run_not_found"
    assert missing["plan_version"] == "plan-missing"
    assert missing["snapshot_bundle_id"] == ""
    assert missing["report"] == {}
    assert confirmed["status"] == "plan_confirmed"


def test_execute_before_confirm_does_not_poison_idempotency_key(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)
    try:
        plan = _create_ready_plan(service, run_id="run-execute")
        early_execute = service.execute_plan(
            run_id="run-execute",
            plan_version=plan["plan_version"],
            idempotency_key="execute-early-0001",
            close_price=Decimal("3100.00"),
            open_price=Decimal("3100.50"),
            fee_rate=Decimal("0.0005"),
        )
        confirmed = service.confirm_plan(
            run_id="run-execute",
            plan_version=plan["plan_version"],
            idempotency_key="confirm-execute-0001",
            operator="tester",
            confirmed_warning_codes=[],
        )
        executed = service.execute_plan(
            run_id="run-execute",
            plan_version=plan["plan_version"],
            idempotency_key="execute-early-0001",
            close_price=Decimal("3100.00"),
            open_price=Decimal("3100.50"),
            fee_rate=Decimal("0.0005"),
        )
    finally:
        repository.close()

    _assert_plan_response_shape(early_execute)
    assert early_execute["status"] != "completed"
    assert "execute" not in early_execute["available_actions"]
    assert confirmed["status"] == "plan_confirmed"
    assert executed["status"] == "completed"


def test_execute_blocks_when_recheck_price_drift_exceeds_threshold(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)
    try:
        plan = _create_ready_plan(service, run_id="run-recheck-drift")
        confirmed = service.confirm_plan(
            run_id="run-recheck-drift",
            plan_version=plan["plan_version"],
            idempotency_key="confirm-recheck-0001",
            operator="tester",
            confirmed_warning_codes=[],
        )
        executed = service.execute_plan(
            run_id="run-recheck-drift",
            plan_version=plan["plan_version"],
            idempotency_key="execute-recheck-0001",
            close_price=Decimal("3200.00"),
            open_price=Decimal("3200.50"),
            fee_rate=Decimal("0.0005"),
            recheck_main_snapshot=snapshot("main", "0", "0", "0", "0"),
            recheck_subaccount_snapshots=[
                snapshot("sub1", "1", "1", "100", "0"),
                snapshot("sub2", "1", "1", "80", "0"),
            ],
            recheck_selected_side=PositionSide.LONG,
            recheck_config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
            recheck_snapshot_bundle_id="snap-recheck-drift",
        )
        stored = repository.get_kanglong_run("run-recheck-drift")
    finally:
        repository.close()

    assert confirmed["status"] == "plan_confirmed"
    assert executed["status"] == "blocked_plan_stale"
    assert executed["error_code"] == "blocked_plan_stale"
    assert stored is not None
    assert stored["status"] == "blocked_plan_stale"


def test_service_blocks_plan_when_first_group_exceeds_round_limit(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)
    try:
        plan = service.create_plan(
            run_id="run-round-limit",
            symbol="ETHUSDC",
            main_snapshot=snapshot("main", "0", "0", "0", "0"),
            subaccount_snapshots=[snapshot("sub1", "1", "1", "100", "0")],
            main_account_id="main",
            subaccount_ids=["sub1"],
            selected_side=PositionSide.LONG,
            snapshot_bundle_id="snap-round-limit",
            config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25"), max_rounds_per_group=3),
            rules=_rules(),
            close_price=Decimal("3100.00"),
            open_price=Decimal("3100.50"),
            fee_rate=Decimal("0.0005"),
        )
    finally:
        repository.close()

    assert plan["status"] == "blocked_group_round_limit_exceeded"
    assert plan["available_actions"] == ["refresh_plan"]
    assert plan["report"]["blocks"] == ["blocked_group_round_limit_exceeded"]


def test_service_plan_persists_template_metadata_and_account_snapshot_report(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)
    request_metadata = {
        "account_source": "test_template",
        "test_template_id": "tpl_eth_drop_001",
        "template_content_hash": "sha256:abc",
        "template_input_digest": "digest-1",
        "market_data_account_id": "market-main",
        "fee_rate_source": "kanglong_symbol_config",
        "fee_rate": "0.0005",
        "snapshot_bundle_id": "snap-template",
        "template_runtime_account_map": {"test-main": "tpl:tpl_eth_drop_001:main"},
    }
    account_snapshot_payload = {
        "account_source": "test_template",
        "template_id": "tpl_eth_drop_001",
        "template_content_hash": "sha256:abc",
        "snapshot_bundle_id": "snap-template",
        "accounts": [{"account_id": "tpl:tpl_eth_drop_001:main"}],
    }

    try:
        plan = service.create_plan(
            run_id="run-template-metadata",
            symbol="ETHUSDC",
            main_snapshot=snapshot("tpl:tpl_eth_drop_001:main", "0.01", "0", "0", "0"),
            subaccount_snapshots=[snapshot("tpl:tpl_eth_drop_001:sub:sub-1", "1", "1", "100", "0")],
            main_account_id="tpl:tpl_eth_drop_001:main",
            subaccount_ids=["tpl:tpl_eth_drop_001:sub:sub-1"],
            selected_side=PositionSide.LONG,
            snapshot_bundle_id="snap-template",
            config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
            rules=_rules(),
            close_price=Decimal("3100.00"),
            open_price=Decimal("3100.50"),
            fee_rate=Decimal("0.0005"),
            request_metadata=request_metadata,
            account_snapshot_payload=account_snapshot_payload,
        )
        stored = repository.get_kanglong_run("run-template-metadata")
    finally:
        repository.close()

    assert plan["status"] == "blocked_main_not_flat"
    assert plan["report"]["account_snapshot"] == account_snapshot_payload
    assert stored is not None
    assert stored["request"]["account_source"] == "test_template"
    assert stored["request"]["test_template_id"] == "tpl_eth_drop_001"
    assert stored["request"]["template_runtime_account_map"] == {"test-main": "tpl:tpl_eth_drop_001:main"}
    assert stored["report"]["account_snapshot"] == account_snapshot_payload


@pytest.mark.asyncio
async def test_execute_recheck_uses_template_market_data_path_from_stored_request(monkeypatch) -> None:
    captured_stored: list[dict] = []

    async def fake_market_inputs(stored: dict) -> dict:
        captured_stored.append(stored)
        return {
            "close_price": Decimal("3100.00"),
            "open_price": Decimal("3100.50"),
            "fee_rate": Decimal("0.0005"),
            "recheck_main_snapshot": snapshot("tpl:tpl_eth_drop_001:main", "0", "0", "0", "0"),
            "recheck_subaccount_snapshots": [snapshot("tpl:tpl_eth_drop_001:sub:sub-1", "1", "1", "100", "0")],
            "recheck_selected_side": PositionSide.LONG,
            "recheck_config": KanglongSymbolConfig(),
            "recheck_snapshot_bundle_id": "snap-template-recheck",
        }

    class TemplateExecuteService:
        def get_run(self, run_id: str) -> dict:
            return {
                "run_id": run_id,
                "symbol": "ETHUSDC",
                "status": "plan_confirmed",
                "plan_version": "plan-1",
                "plan": {"selected_side": "LONG"},
                "request": {
                    "mode": "simulation",
                    "symbol": "ETHUSDC",
                    "main_account_id": "tpl:tpl_eth_drop_001:main",
                    "subaccount_ids": ["tpl:tpl_eth_drop_001:sub:sub-1"],
                    "account_source": "test_template",
                    "test_template_id": "tpl_eth_drop_001",
                    "template_content_hash": "sha256:abc",
                    "market_data_account_id": "market-main",
                },
            }

        def execute_plan(self, **kwargs) -> dict:
            return {
                "run_id": kwargs["run_id"],
                "status": "completed",
                "plan_version": kwargs["plan_version"],
                "snapshot_bundle_id": kwargs["recheck_snapshot_bundle_id"],
                "available_actions": ["view_report"],
                "report": {},
                }

    original_collector = api_module._collect_kanglong_plan_inputs
    original_market_inputs = api_module._collect_template_execution_market_inputs
    original_validator = api_module._validate_template_run_not_stale
    original_service = getattr(api_module.app.state, "kanglong_service", None)
    monkeypatch.setattr(api_module, "_collect_kanglong_plan_inputs", lambda request: (_ for _ in ()).throw(AssertionError("full collector not used")))
    monkeypatch.setattr(api_module, "_collect_template_execution_market_inputs", fake_market_inputs)
    monkeypatch.setattr(api_module, "_validate_template_run_not_stale", lambda stored: None)
    monkeypatch.setattr(api_module.app.state, "kanglong_service", TemplateExecuteService(), raising=False)
    try:
        await api_module.execute_kanglong_simulation_plan(
            "run-template",
            KanglongActionRequest(plan_version="plan-1", idempotency_key="execute-template-0001"),
        )
    finally:
        monkeypatch.setattr(api_module, "_collect_kanglong_plan_inputs", original_collector)
        monkeypatch.setattr(api_module, "_collect_template_execution_market_inputs", original_market_inputs)
        monkeypatch.setattr(api_module, "_validate_template_run_not_stale", original_validator)
        if original_service is not None:
            monkeypatch.setattr(api_module.app.state, "kanglong_service", original_service, raising=False)

    assert len(captured_stored) == 1
    assert captured_stored[0]["request"]["account_source"] == "test_template"
    assert captured_stored[0]["request"]["test_template_id"] == "tpl_eth_drop_001"
    assert captured_stored[0]["request"]["template_content_hash"] == "sha256:abc"
    assert captured_stored[0]["request"]["market_data_account_id"] == "market-main"


@pytest.mark.asyncio
async def test_execute_recheck_propagates_template_stale_validation_http_exception(monkeypatch) -> None:
    def stale_template_validator(stored: dict) -> None:
        raise HTTPException(
            status_code=409,
            detail={"code": "blocked_plan_stale", "template_id": stored["request"]["test_template_id"]},
        )

    class TemplateExecuteService:
        def get_run(self, run_id: str) -> dict:
            return {
                "run_id": run_id,
                "symbol": "ETHUSDC",
                "status": "plan_confirmed",
                "plan_version": "plan-1",
                "plan": {"selected_side": "LONG"},
                "request": {
                    "mode": "simulation",
                    "symbol": "ETHUSDC",
                    "main_account_id": "tpl:tpl_eth_drop_001:main",
                    "subaccount_ids": ["tpl:tpl_eth_drop_001:sub:sub-1"],
                    "account_source": "test_template",
                    "test_template_id": "tpl_eth_drop_001",
                    "template_content_hash": "sha256:stale",
                    "market_data_account_id": "market-main",
                },
            }

        def execute_plan(self, **kwargs) -> dict:
            raise AssertionError("execute_plan should not run when recheck collection raises HTTPException")

    original_validator = api_module._validate_template_run_not_stale
    original_service = getattr(api_module.app.state, "kanglong_service", None)
    monkeypatch.setattr(api_module, "_validate_template_run_not_stale", stale_template_validator)
    monkeypatch.setattr(api_module.app.state, "kanglong_service", TemplateExecuteService(), raising=False)
    try:
        with pytest.raises(HTTPException) as exc:
            await api_module.execute_kanglong_simulation_plan(
                "run-template",
                KanglongActionRequest(plan_version="plan-1", idempotency_key="execute-template-0002"),
            )
    finally:
        monkeypatch.setattr(api_module, "_validate_template_run_not_stale", original_validator)
        if original_service is not None:
            monkeypatch.setattr(api_module.app.state, "kanglong_service", original_service, raising=False)

    assert exc.value.status_code == 409
    assert exc.value.detail == {"code": "blocked_plan_stale", "template_id": "tpl_eth_drop_001"}


def test_service_blocks_overlapping_ready_plan_until_lock_is_released(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)
    try:
        first = _create_ready_plan(service, run_id="run-lock-1")
        blocked = _create_ready_plan(service, run_id="run-lock-2", snapshot_bundle_id="snap-lock-2")
        service.confirm_plan(
            run_id="run-lock-1",
            plan_version=first["plan_version"],
            idempotency_key="confirm-lock-0001",
            operator="tester",
            confirmed_warning_codes=[],
        )
        service.execute_plan(
            run_id="run-lock-1",
            plan_version=first["plan_version"],
            idempotency_key="execute-lock-0001",
            close_price=Decimal("3100.00"),
            open_price=Decimal("3100.50"),
            fee_rate=Decimal("0.0005"),
            recheck_main_snapshot=snapshot("main", "0", "0", "0", "0"),
            recheck_subaccount_snapshots=[
                snapshot("sub1", "1", "1", "100", "0"),
                snapshot("sub2", "1", "1", "80", "0"),
            ],
            recheck_selected_side=PositionSide.LONG,
            recheck_config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
            recheck_snapshot_bundle_id="snap-lock-execute",
        )
        after_release = _create_ready_plan(service, run_id="run-lock-3", snapshot_bundle_id="snap-lock-3")
    finally:
        repository.close()

    assert first["status"] == "chain_ready"
    assert blocked["status"] == "blocked_run_lock_exists"
    assert blocked["available_actions"] == ["refresh_plan"]
    assert after_release["status"] == "chain_ready"
