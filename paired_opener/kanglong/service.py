from __future__ import annotations

from dataclasses import fields, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import (
    KanglongAccountSnapshot,
    KanglongGroupPlan,
    KanglongPrecheckResult,
    KanglongResultGrade,
    KanglongRunStatus,
    payload_value,
)
from paired_opener.kanglong.planner import build_kanglong_plan, build_planning_accounts
from paired_opener.kanglong.precheck import run_static_precheck
from paired_opener.kanglong.reporter import summarize_costs
from paired_opener.kanglong.simulator import simulate_group
from paired_opener.storage import SqliteRepository


def _now_text() -> str:
    return datetime.now(UTC).isoformat()


def _payloadify(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _payloadify(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_payloadify(item) for item in value]
    if is_dataclass(value):
        return {
            field.name: _payloadify(getattr(value, field.name))
            for field in fields(value)
        }
    return payload_value(value)


def _group_payload(group: KanglongGroupPlan) -> dict[str, Any]:
    return _payloadify(group)


def _blocked_payload(run_id: str, precheck: KanglongPrecheckResult) -> dict[str, Any]:
    report = {
        "precheck": _payloadify(precheck.details),
        "other_side_preview": _payloadify(precheck.other_side_preview),
    }
    return {
        "run_id": run_id,
        "status": precheck.status.value,
        "result_grade": KanglongResultGrade.UNSAFE_UNCLOSED.value,
        "report": report,
    }


class KanglongSimulationService:
    def __init__(self, repository: SqliteRepository) -> None:
        self._repository = repository

    def create_draft_run(
        self,
        *,
        run_id: str,
        symbol: str,
        main_account_id: str,
        subaccount_ids: list[str],
    ) -> dict[str, Any]:
        payload = {
            "run_id": run_id,
            "symbol": symbol,
            "main_account_id": main_account_id,
            "subaccount_ids": subaccount_ids,
            "status": KanglongRunStatus.DRAFT_PLAN.value,
            "created_at": _now_text(),
            "updated_at": _now_text(),
        }
        self._repository.create_kanglong_run(payload)
        return payload

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self._repository.get_kanglong_run(run_id)

    def simulate(
        self,
        *,
        run_id: str,
        symbol: str,
        main_snapshot: KanglongAccountSnapshot,
        subaccount_snapshots: list[KanglongAccountSnapshot],
        selected_side: PositionSide | None,
        config: KanglongSymbolConfig,
        rules: SymbolRules,
        close_price: Decimal,
        open_price: Decimal,
        fee_rate: Decimal,
    ) -> dict[str, Any]:
        precheck = run_static_precheck(
            main=main_snapshot,
            subaccounts=subaccount_snapshots,
            symbol=symbol,
            manual_side=selected_side,
            config=config,
        )
        if not precheck.ok:
            payload = _blocked_payload(run_id, precheck)
            self._repository.update_kanglong_run(
                run_id,
                status=payload["status"],
                report=payload["report"],
                result_grade=payload["result_grade"],
            )
            return payload
        if precheck.selected_side is None or precheck.first_donor_account_id is None:
            payload = _blocked_payload(run_id, precheck)
            self._repository.update_kanglong_run(
                run_id,
                status=payload["status"],
                report=payload["report"],
                result_grade=payload["result_grade"],
            )
            return payload

        planning_accounts = build_planning_accounts(subaccount_snapshots, precheck.selected_side, config)
        plan = build_kanglong_plan(
            run_id=run_id,
            symbol=symbol,
            selected_side=precheck.selected_side,
            main_account_id=main_snapshot.account_id,
            first_donor_account_id=precheck.first_donor_account_id,
            planned_release_qty=precheck.planned_release_qty,
            accounts=planning_accounts,
            config=config,
        )

        events = []
        residuals = []
        for group in plan.groups:
            result = simulate_group(
                run_id=run_id,
                group=group,
                rules=rules,
                close_price=close_price,
                open_price=open_price,
                fee_rate=fee_rate,
                config=config,
            )
            events.extend(result.events)
            residuals.extend(result.residual_ledger)

        costs = summarize_costs(events, residuals)
        report = {
            "selected_side": precheck.selected_side.value,
            "groups": [_group_payload(group) for group in plan.groups],
            "batch_debt_buffers": _payloadify(plan.batch_debt_buffers),
            "costs": _payloadify(costs),
            "residual_ledger": _payloadify(residuals),
            "events": [_payloadify(event.to_payload()) for event in events],
            "other_side_preview": _payloadify(precheck.other_side_preview),
        }
        payload = {
            "run_id": run_id,
            "status": KanglongRunStatus.COMPLETED.value,
            "result_grade": KanglongResultGrade.SAFE_CLOSED.value,
            "report": report,
        }
        self._repository.update_kanglong_run(
            run_id,
            status=payload["status"],
            report=report,
            result_grade=payload["result_grade"],
        )
        return payload
