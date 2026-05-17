from __future__ import annotations

from decimal import Decimal

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
