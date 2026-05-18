from __future__ import annotations

import json
from dataclasses import fields, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any
from uuid import uuid4

from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import (
    KanglongAccountSnapshot,
    KanglongGroupPlan,
    KanglongPrecheckResult,
    KanglongResultGrade,
    KanglongRunStatus,
    decimal_text,
    payload_value,
)
from paired_opener.kanglong.planner import KanglongGroupRoundLimitExceeded, build_kanglong_plan, build_planning_accounts
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


def _new_plan_version() -> str:
    return f"plan-{uuid4().hex}"


def _request_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(_payloadify(payload), sort_keys=True, separators=(",", ":"))
    return sha256(encoded.encode("utf-8")).hexdigest()


def _group_payload(group: KanglongGroupPlan) -> dict[str, Any]:
    return _payloadify(group)


def _response_base(
    run_id: str | None,
    status: str,
    *,
    plan_version: str | None = "",
    snapshot_bundle_id: str | None = "",
    available_actions: list[str] | None = None,
    report: dict[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "run_id": run_id or "",
        "status": status,
        "plan_version": plan_version or "",
        "snapshot_bundle_id": snapshot_bundle_id or "",
        "available_actions": available_actions or [],
        "report": report or {},
        **extra,
    }


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


def _attach_account_snapshot(report: dict[str, Any], account_snapshot_payload: dict[str, Any] | None) -> dict[str, Any]:
    if account_snapshot_payload is not None:
        report["account_snapshot"] = _payloadify(account_snapshot_payload)
    return report


def _lock_scopes(symbol: str, main_account_id: str, subaccount_ids: list[str]) -> list[str]:
    normalized_symbol = symbol.strip().upper()
    account_ids = [main_account_id, *subaccount_ids]
    return [
        f"kanglong:{normalized_symbol}:account:{account_id.strip().lower()}"
        for account_id in account_ids
    ]


def _price_snapshot(close_price: Decimal, open_price: Decimal, fee_rate: Decimal) -> dict[str, str]:
    return {
        "close_price": decimal_text(close_price),
        "open_price": decimal_text(open_price),
        "fee_rate": decimal_text(fee_rate),
    }


def _price_drift_bps(previous: Decimal, current: Decimal) -> Decimal:
    if previous <= Decimal("0"):
        return Decimal("0")
    return abs(current - previous) / previous * Decimal("10000")


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
        request_payload: dict[str, Any] | None = None,
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
        if request_payload is not None:
            payload["request"] = request_payload
        self._repository.create_kanglong_run(payload)
        return payload

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self._repository.get_kanglong_run(run_id)

    def active_run(self) -> dict[str, Any] | None:
        payload = self._repository.get_active_kanglong_run()
        if payload is None:
            return None
        payload["latest_event_id"] = self._repository.latest_kanglong_event_id(payload["run_id"])
        return payload

    def create_plan(
        self,
        *,
        run_id: str,
        symbol: str,
        main_snapshot: KanglongAccountSnapshot,
        subaccount_snapshots: list[KanglongAccountSnapshot],
        main_account_id: str,
        subaccount_ids: list[str],
        selected_side: PositionSide | None,
        snapshot_bundle_id: str,
        config: KanglongSymbolConfig,
        rules: SymbolRules,
        close_price: Decimal,
        open_price: Decimal,
        fee_rate: Decimal,
        request_metadata: dict[str, Any] | None = None,
        account_snapshot_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        request_payload = {
            "mode": "simulation",
            "symbol": symbol,
            "main_account_id": main_account_id,
            "subaccount_ids": subaccount_ids,
            "selected_side": selected_side.value if selected_side is not None else None,
            "account_source": "runtime",
        }
        if request_metadata:
            request_payload.update(_payloadify(request_metadata))
        self.create_draft_run(
            run_id=run_id,
            symbol=symbol,
            main_account_id=main_account_id,
            subaccount_ids=subaccount_ids,
            request_payload=request_payload,
        )
        precheck = run_static_precheck(
            main=main_snapshot,
            subaccounts=subaccount_snapshots,
            symbol=symbol,
            manual_side=selected_side,
            config=config,
            reference_price=close_price,
            fee_rate=fee_rate,
        )
        plan_version = _new_plan_version()

        if not precheck.ok or precheck.selected_side is None or precheck.first_donor_account_id is None:
            status = (
                precheck.status.value
                if not precheck.ok
                else KanglongRunStatus.BLOCKED_PLAN_RECHECK_FAILED.value
            )
            report = {
                "precheck": _payloadify(precheck.details),
                "other_side_preview": _payloadify(precheck.other_side_preview),
                "warnings": [],
                "blocks": [precheck.reason_code] if precheck.reason_code else [],
            }
            _attach_account_snapshot(report, account_snapshot_payload)
            payload = {
                "run_id": run_id,
                "status": status,
                "result_grade": KanglongResultGrade.UNSAFE_UNCLOSED.value,
                "plan_version": plan_version,
                "snapshot_bundle_id": snapshot_bundle_id,
                "available_actions": ["refresh_plan"],
                "report": report,
            }
            self._repository.update_kanglong_run(
                run_id,
                status=payload["status"],
                report=report,
                result_grade=payload["result_grade"],
                plan_version=plan_version,
                snapshot_bundle_id=snapshot_bundle_id,
                available_actions=payload["available_actions"],
            )
            return payload

        planning_accounts = build_planning_accounts(subaccount_snapshots, precheck.selected_side, config)
        try:
            plan = build_kanglong_plan(
                run_id=run_id,
                symbol=symbol,
                selected_side=precheck.selected_side,
                main_account_id=main_account_id,
                first_donor_account_id=precheck.first_donor_account_id,
                planned_release_qty=precheck.planned_release_qty,
                accounts=planning_accounts,
                config=config,
            )
        except KanglongGroupRoundLimitExceeded as exc:
            status = (
                KanglongRunStatus.BLOCKED_GROUP_ROUND_LIMIT_EXCEEDED
                if exc.group_index == 1
                else KanglongRunStatus.PAUSED_GROUP_ROUND_LIMIT_EXCEEDED
            )
            report = {
                "precheck": _payloadify(precheck.details),
                "other_side_preview": _payloadify(precheck.other_side_preview),
                "warnings": [],
                "blocks": [KanglongRunStatus.BLOCKED_GROUP_ROUND_LIMIT_EXCEEDED.value],
                "round_limit": {
                    "group_index": exc.group_index,
                    "target_qty": decimal_text(exc.target_qty),
                    "per_round_qty_limit": decimal_text(exc.per_round_qty_limit),
                    "required_rounds": exc.required_rounds,
                    "max_rounds_per_group": exc.max_rounds,
                },
            }
            _attach_account_snapshot(report, account_snapshot_payload)
            payload = {
                "run_id": run_id,
                "status": status.value,
                "result_grade": KanglongResultGrade.UNSAFE_UNCLOSED.value,
                "plan_version": plan_version,
                "snapshot_bundle_id": snapshot_bundle_id,
                "available_actions": ["refresh_plan"],
                "report": report,
            }
            self._repository.update_kanglong_run(
                run_id,
                status=payload["status"],
                report=report,
                result_grade=payload["result_grade"],
                plan_version=plan_version,
                snapshot_bundle_id=snapshot_bundle_id,
                available_actions=payload["available_actions"],
            )
            return payload

        lock_conflict = self._repository.acquire_kanglong_locks(
            run_id=run_id,
            lock_scopes=_lock_scopes(symbol, main_account_id, subaccount_ids),
            ttl_ms=config.run_lock_ttl_ms,
        )
        if lock_conflict is not None:
            report = {
                "precheck": _payloadify(precheck.details),
                "other_side_preview": _payloadify(precheck.other_side_preview),
                "warnings": [],
                "blocks": [KanglongRunStatus.BLOCKED_RUN_LOCK_EXISTS.value],
                "lock_conflict": _payloadify(lock_conflict),
            }
            _attach_account_snapshot(report, account_snapshot_payload)
            payload = {
                "run_id": run_id,
                "status": KanglongRunStatus.BLOCKED_RUN_LOCK_EXISTS.value,
                "result_grade": KanglongResultGrade.UNSAFE_UNCLOSED.value,
                "plan_version": plan_version,
                "snapshot_bundle_id": snapshot_bundle_id,
                "available_actions": ["refresh_plan"],
                "report": report,
            }
            self._repository.update_kanglong_run(
                run_id,
                status=payload["status"],
                report=report,
                result_grade=payload["result_grade"],
                plan_version=plan_version,
                snapshot_bundle_id=snapshot_bundle_id,
                available_actions=payload["available_actions"],
            )
            return payload

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
        plan_payload = {
            "plan_version": plan_version,
            "snapshot_bundle_id": snapshot_bundle_id,
            "selected_side": precheck.selected_side.value,
            "groups": [_group_payload(group) for group in plan.groups],
            "batch_debt_buffers": _payloadify(plan.batch_debt_buffers),
        }
        summary = {
            "status": KanglongRunStatus.CHAIN_READY.value,
            "selected_side": precheck.selected_side.value,
            "group_count": len(plan.groups),
            "round_count": sum(len(group.round_qtys) for group in plan.groups),
            "planned_release_qty": decimal_text(precheck.planned_release_qty),
            "estimated_transfer_fee_cost": _payloadify(costs["transfer_fee_cost"]),
            "estimated_transfer_price_diff_loss": _payloadify(costs["transfer_price_diff_loss"]),
        }
        report = {
            "summary": summary,
            "plan": plan_payload,
            "costs": _payloadify(costs),
            "price_snapshot": _price_snapshot(close_price, open_price, fee_rate),
            "other_side_preview": _payloadify(precheck.other_side_preview),
            "warnings": [],
            "blocks": [],
        }
        _attach_account_snapshot(report, account_snapshot_payload)
        available_actions = ["confirm", "refresh_plan"]
        payload = {
            "run_id": run_id,
            "status": KanglongRunStatus.CHAIN_READY.value,
            "plan_version": plan_version,
            "snapshot_bundle_id": snapshot_bundle_id,
            "available_actions": available_actions,
            "report": report,
        }
        self._repository.update_kanglong_run(
            run_id,
            status=payload["status"],
            plan=plan_payload,
            report=report,
            plan_version=plan_version,
            snapshot_bundle_id=snapshot_bundle_id,
            available_actions=available_actions,
            report_summary=summary,
        )
        return payload

    def confirm_plan(
        self,
        *,
        run_id: str,
        plan_version: str,
        idempotency_key: str,
        operator: str,
        confirmed_warning_codes: list[str],
    ) -> dict[str, Any]:
        request_hash = _request_hash(
            {
                "action": "confirm",
                "run_id": run_id,
                "plan_version": plan_version,
                "operator": operator,
                "confirmed_warning_codes": confirmed_warning_codes,
            }
        )
        existing = self._repository.get_kanglong_idempotency(idempotency_key, request_hash)
        if existing is not None:
            if existing["conflict"]:
                return self._idempotency_conflict(run_id)
            return existing["response"]

        stored = self._repository.get_kanglong_run(run_id)
        if stored is None:
            return self._not_found(run_id, plan_version)
        if stored.get("plan_version") != plan_version:
            return self._blocked_plan_stale(run_id, plan_version, stored)
        if stored.get("status") != KanglongRunStatus.CHAIN_READY.value:
            return self._blocked_plan_recheck_failed(run_id, plan_version, stored)

        available_actions = ["execute", "refresh_plan"]
        confirmed_at = _now_text()
        self._repository.update_kanglong_run(
            run_id,
            status=KanglongRunStatus.PLAN_CONFIRMED.value,
            confirmed_at=confirmed_at,
            available_actions=available_actions,
        )
        response = _response_base(
            run_id,
            KanglongRunStatus.PLAN_CONFIRMED.value,
            plan_version=plan_version,
            snapshot_bundle_id=stored.get("snapshot_bundle_id"),
            available_actions=available_actions,
            confirmed_at=confirmed_at,
            report=stored.get("report") or {},
        )
        return self._remember_idempotency(idempotency_key, request_hash, response)

    def execute_plan(
        self,
        *,
        run_id: str,
        plan_version: str,
        idempotency_key: str,
        close_price: Decimal,
        open_price: Decimal,
        fee_rate: Decimal,
        recheck_main_snapshot: KanglongAccountSnapshot | None = None,
        recheck_subaccount_snapshots: list[KanglongAccountSnapshot] | None = None,
        recheck_selected_side: PositionSide | None = None,
        recheck_config: KanglongSymbolConfig | None = None,
        recheck_snapshot_bundle_id: str | None = None,
    ) -> dict[str, Any]:
        request_hash = _request_hash(
            {
                "action": "execute",
                "run_id": run_id,
                "plan_version": plan_version,
                "close_price": close_price,
                "open_price": open_price,
                "fee_rate": fee_rate,
            }
        )
        existing = self._repository.get_kanglong_idempotency(idempotency_key, request_hash)
        if existing is not None:
            if existing["conflict"]:
                return self._idempotency_conflict(run_id)
            return existing["response"]

        stored = self._repository.get_kanglong_run(run_id)
        if stored is None:
            return self._not_found(run_id, plan_version)
        if stored.get("plan_version") != plan_version:
            return self._blocked_plan_stale(run_id, plan_version, stored)
        if stored.get("status") != KanglongRunStatus.PLAN_CONFIRMED.value:
            return self._blocked_plan_recheck_failed(run_id, plan_version, stored)

        recheck_response = self._execute_recheck(
            run_id=run_id,
            requested_plan_version=plan_version,
            stored=stored,
            close_price=close_price,
            open_price=open_price,
            fee_rate=fee_rate,
            recheck_main_snapshot=recheck_main_snapshot,
            recheck_subaccount_snapshots=recheck_subaccount_snapshots,
            recheck_selected_side=recheck_selected_side,
            recheck_config=recheck_config,
            recheck_snapshot_bundle_id=recheck_snapshot_bundle_id,
        )
        if recheck_response is not None:
            return recheck_response

        self._repository.heartbeat_kanglong_locks(
            run_id=run_id,
            ttl_ms=(recheck_config or KanglongSymbolConfig()).run_lock_ttl_ms,
        )
        self._repository.update_kanglong_run(
            run_id,
            status=KanglongRunStatus.EXECUTION_STARTING.value,
            available_actions=[],
        )
        plan = stored.get("plan") or {}
        for group in plan.get("groups") or []:
            group_id = group.get("group_id")
            self._repository.add_kanglong_event(
                run_id,
                "kanglong_group_simulated",
                {
                    "message_key": "events.kanglong.group_simulated",
                    "message_params": {"group_id": group_id},
                    "group_id": group_id,
                    "plan_version": plan_version,
                    "close_price": decimal_text(close_price),
                    "open_price": decimal_text(open_price),
                    "fee_rate": decimal_text(fee_rate),
                },
                group_id=group_id,
            )

        available_actions = ["view_report"]
        self._repository.update_kanglong_run(
            run_id,
            status=KanglongRunStatus.COMPLETED.value,
            result_grade=KanglongResultGrade.SAFE_CLOSED.value,
            available_actions=available_actions,
        )
        self._repository.release_kanglong_locks(run_id)
        response = _response_base(
            run_id,
            KanglongRunStatus.COMPLETED.value,
            plan_version=plan_version,
            snapshot_bundle_id=stored.get("snapshot_bundle_id"),
            available_actions=available_actions,
            report=stored.get("report") or {},
            result_grade=KanglongResultGrade.SAFE_CLOSED.value,
            latest_event_id=self._repository.latest_kanglong_event_id(run_id),
        )
        return self._remember_idempotency(idempotency_key, request_hash, response)

    def _execute_recheck(
        self,
        *,
        run_id: str,
        requested_plan_version: str,
        stored: dict[str, Any],
        close_price: Decimal,
        open_price: Decimal,
        fee_rate: Decimal,
        recheck_main_snapshot: KanglongAccountSnapshot | None,
        recheck_subaccount_snapshots: list[KanglongAccountSnapshot] | None,
        recheck_selected_side: PositionSide | None,
        recheck_config: KanglongSymbolConfig | None,
        recheck_snapshot_bundle_id: str | None,
    ) -> dict[str, Any] | None:
        report = dict(stored.get("report") or {})
        config = recheck_config or KanglongSymbolConfig()
        previous_price_snapshot = report.get("price_snapshot")
        if isinstance(previous_price_snapshot, dict):
            previous_close_price = Decimal(str(previous_price_snapshot.get("close_price") or "0"))
            previous_open_price = Decimal(str(previous_price_snapshot.get("open_price") or "0"))
            close_drift_bps = _price_drift_bps(previous_close_price, close_price)
            open_drift_bps = _price_drift_bps(previous_open_price, open_price)
            max_drift_bps = max(close_drift_bps, open_drift_bps)
            if max_drift_bps > Decimal(config.plan_recheck_price_drift_bps):
                recheck = {
                    "status": KanglongRunStatus.BLOCKED_PLAN_STALE.value,
                    "reason_code": "price_drift_exceeded",
                    "previous_price_snapshot": previous_price_snapshot,
                    "current_price_snapshot": _price_snapshot(close_price, open_price, fee_rate),
                    "max_drift_bps": decimal_text(max_drift_bps),
                    "limit_bps": config.plan_recheck_price_drift_bps,
                    "snapshot_bundle_id": recheck_snapshot_bundle_id,
                }
                return self._mark_execute_recheck_blocked(
                    run_id=run_id,
                    requested_plan_version=requested_plan_version,
                    stored=stored,
                    status=KanglongRunStatus.BLOCKED_PLAN_STALE,
                    report_patch={"execute_recheck": recheck},
                )

        if recheck_main_snapshot is None or recheck_subaccount_snapshots is None:
            return None

        plan_payload = stored.get("plan") or {}
        selected_side_value = plan_payload.get("selected_side")
        stored_selected_side = PositionSide(selected_side_value) if selected_side_value else recheck_selected_side
        precheck = run_static_precheck(
            main=recheck_main_snapshot,
            subaccounts=recheck_subaccount_snapshots,
            symbol=stored.get("symbol") or plan_payload.get("symbol") or "",
            manual_side=recheck_selected_side or stored_selected_side,
            config=config,
            reference_price=close_price,
            fee_rate=fee_rate,
        )
        if not precheck.ok or precheck.selected_side is None or precheck.first_donor_account_id is None:
            return self._mark_execute_recheck_blocked(
                run_id=run_id,
                requested_plan_version=requested_plan_version,
                stored=stored,
                status=KanglongRunStatus.BLOCKED_PLAN_RECHECK_FAILED,
                report_patch={
                    "execute_recheck": {
                        "status": KanglongRunStatus.BLOCKED_PLAN_RECHECK_FAILED.value,
                        "reason_code": precheck.reason_code,
                        "precheck": _payloadify(precheck.details),
                        "snapshot_bundle_id": recheck_snapshot_bundle_id,
                    }
                },
            )
        if stored_selected_side is not None and precheck.selected_side != stored_selected_side:
            return self._mark_execute_recheck_blocked(
                run_id=run_id,
                requested_plan_version=requested_plan_version,
                stored=stored,
                status=KanglongRunStatus.BLOCKED_PLAN_STALE,
                report_patch={
                    "execute_recheck": {
                        "status": KanglongRunStatus.BLOCKED_PLAN_STALE.value,
                        "reason_code": "selected_side_changed",
                        "previous_selected_side": stored_selected_side.value,
                        "current_selected_side": precheck.selected_side.value,
                        "snapshot_bundle_id": recheck_snapshot_bundle_id,
                    }
                },
            )
        previous_release_qty = Decimal(str((report.get("summary") or {}).get("planned_release_qty") or "0"))
        if abs(precheck.planned_release_qty - previous_release_qty) > config.plan_recheck_qty_tolerance:
            return self._mark_execute_recheck_blocked(
                run_id=run_id,
                requested_plan_version=requested_plan_version,
                stored=stored,
                status=KanglongRunStatus.BLOCKED_PLAN_STALE,
                report_patch={
                    "execute_recheck": {
                        "status": KanglongRunStatus.BLOCKED_PLAN_STALE.value,
                        "reason_code": "planned_release_qty_changed",
                        "previous_planned_release_qty": decimal_text(previous_release_qty),
                        "current_planned_release_qty": decimal_text(precheck.planned_release_qty),
                        "tolerance": decimal_text(config.plan_recheck_qty_tolerance),
                        "snapshot_bundle_id": recheck_snapshot_bundle_id,
                    }
                },
            )
        return None

    def _mark_execute_recheck_blocked(
        self,
        *,
        run_id: str,
        requested_plan_version: str,
        stored: dict[str, Any],
        status: KanglongRunStatus,
        report_patch: dict[str, Any],
    ) -> dict[str, Any]:
        report = dict(stored.get("report") or {})
        report.update(_payloadify(report_patch))
        blocks = list(report.get("blocks") or [])
        if status.value not in blocks:
            blocks.append(status.value)
        report["blocks"] = blocks
        available_actions = ["refresh_plan"]
        self._repository.update_kanglong_run(
            run_id,
            status=status.value,
            report=report,
            result_grade=KanglongResultGrade.UNSAFE_UNCLOSED.value,
            available_actions=available_actions,
        )
        self._repository.release_kanglong_locks(run_id)
        return _response_base(
            run_id,
            status.value,
            plan_version=stored.get("plan_version") or requested_plan_version,
            snapshot_bundle_id=stored.get("snapshot_bundle_id"),
            available_actions=available_actions,
            report=report,
            error_code=status.value,
            requested_plan_version=requested_plan_version,
            current_status=stored.get("status"),
        )

    def list_events(
        self,
        run_id: str,
        after_event_id: int | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        return {"run_id": run_id, **self._repository.list_kanglong_events(run_id, after_event_id, limit)}

    def _remember_idempotency(
        self,
        key: str,
        request_hash: str,
        response: dict[str, Any],
    ) -> dict[str, Any]:
        stored = self._repository.remember_kanglong_idempotency(
            key=key,
            request_hash=request_hash,
            response=response,
        )
        if stored["conflict"]:
            return self._idempotency_conflict(
                response.get("run_id"),
                response.get("plan_version"),
                response.get("snapshot_bundle_id"),
            )
        return stored["response"]

    def _idempotency_conflict(
        self,
        run_id: str | None,
        plan_version: str | None = "",
        snapshot_bundle_id: str | None = "",
    ) -> dict[str, Any]:
        return _response_base(
            run_id,
            "idempotency_conflict",
            plan_version=plan_version,
            snapshot_bundle_id=snapshot_bundle_id,
            error_code="idempotency_conflict",
        )

    def _not_found(self, run_id: str, plan_version: str) -> dict[str, Any]:
        return _response_base(
            run_id,
            "kanglong_run_not_found",
            plan_version=plan_version,
            error_code="kanglong_run_not_found",
        )

    def _blocked_plan_stale(
        self,
        run_id: str,
        requested_plan_version: str,
        stored: dict[str, Any],
    ) -> dict[str, Any]:
        return _response_base(
            run_id,
            KanglongRunStatus.BLOCKED_PLAN_STALE.value,
            plan_version=stored.get("plan_version"),
            snapshot_bundle_id=stored.get("snapshot_bundle_id"),
            available_actions=["refresh_plan"],
            report=stored.get("report") or {},
            requested_plan_version=requested_plan_version,
        )

    def _blocked_plan_recheck_failed(
        self,
        run_id: str,
        requested_plan_version: str,
        stored: dict[str, Any],
    ) -> dict[str, Any]:
        return _response_base(
            run_id,
            KanglongRunStatus.BLOCKED_PLAN_RECHECK_FAILED.value,
            plan_version=stored.get("plan_version") or requested_plan_version,
            snapshot_bundle_id=stored.get("snapshot_bundle_id"),
            available_actions=["refresh_plan"],
            report=stored.get("report") or {},
            requested_plan_version=requested_plan_version,
            current_status=stored.get("status"),
        )

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
