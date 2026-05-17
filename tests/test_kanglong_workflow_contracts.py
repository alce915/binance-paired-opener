from __future__ import annotations

from paired_opener.kanglong.models import KanglongRunStatus
from paired_opener.schemas import (
    KanglongActionRequest,
    KanglongEventsResponse,
    KanglongPlanRequest,
    KanglongPlanResponse,
)


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
