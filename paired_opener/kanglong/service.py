from __future__ import annotations

import copy
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

_DEFAULT_SYNTHETIC_LEVERAGE = 1


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


def _display_qty(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.01")), "f")


def _chain_config_account_labels(
    *,
    main_snapshot: KanglongAccountSnapshot,
    subaccount_snapshots: list[KanglongAccountSnapshot],
    account_snapshot_payload: dict[str, Any] | None,
) -> dict[str, str]:
    labels = {
        snapshot.account_id: snapshot.account_name or snapshot.account_id
        for snapshot in [main_snapshot, *subaccount_snapshots]
    }
    for account in (account_snapshot_payload or {}).get("accounts") or []:
        if not isinstance(account, dict):
            continue
        account_id = str(account.get("account_id") or account.get("id") or "").strip()
        if not account_id:
            continue
        label = str(
            account.get("name")
            or account.get("account_name")
            or account.get("label")
            or account.get("template_account_id")
            or account_id
        ).strip()
        labels[account_id] = label or account_id
    return labels


def _chain_config_payload(
    *,
    symbol: str,
    selected_side: PositionSide,
    main_account_id: str,
    main_snapshot: KanglongAccountSnapshot,
    subaccount_snapshots: list[KanglongAccountSnapshot],
    groups: list[KanglongGroupPlan],
    account_snapshot_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    labels = _chain_config_account_labels(
        main_snapshot=main_snapshot,
        subaccount_snapshots=subaccount_snapshots,
        account_snapshot_payload=account_snapshot_payload,
    )
    items = []
    for index, group in enumerate(groups, start=1):
        signed_qty = group.target_qty if group.from_account_id == main_account_id else -group.target_qty
        items.append(
            {
                "index": index,
                "group_id": group.group_id,
                "from_account_id": group.from_account_id,
                "from_account_label": labels.get(group.from_account_id, group.from_account_id),
                "to_account_id": group.to_account_id,
                "to_account_label": labels.get(group.to_account_id, group.to_account_id),
                "qty": decimal_text(group.target_qty),
                "signed_qty": decimal_text(signed_qty),
                "display_qty": _display_qty(signed_qty),
                "round_count": len(group.round_qtys),
            }
        )
    return {
        "symbol": symbol.strip().upper(),
        "side": selected_side.value.lower(),
        "count": len(items),
        "items": items,
    }


def _price_drift_bps(previous: Decimal, current: Decimal) -> Decimal:
    if previous <= Decimal("0"):
        return Decimal("0")
    return abs(current - previous) / previous * Decimal("10000")


def _decimal_payload_value(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _set_decimal_payload(target: dict[str, Any], key: str, value: Decimal) -> None:
    if key in target:
        target[key] = decimal_text(value)


def _find_synthetic_account(accounts: list[dict[str, Any]], account_id: str) -> dict[str, Any] | None:
    return next((account for account in accounts if str(account.get("account_id") or "") == account_id), None)


def _find_synthetic_position(account: dict[str, Any], side: str) -> dict[str, Any] | None:
    return next(
        (
            position
            for position in account.get("positions") or []
            if str(position.get("position_side") or "").strip().upper() == side
        ),
        None,
    )


def _new_synthetic_position(account: dict[str, Any], group: dict[str, Any], qty: Decimal, price: Decimal) -> dict[str, Any]:
    leverage = int(account.get("leverage") or _DEFAULT_SYNTHETIC_LEVERAGE)
    notional = qty * price
    margin = notional / Decimal(max(leverage, 1))
    return {
        "symbol": str(group.get("symbol") or "").strip().upper(),
        "position_side": str(group.get("side") or "").strip().upper(),
        "qty": decimal_text(qty),
        "entry_price": decimal_text(price),
        "mark_price": decimal_text(price),
        "unrealized_pnl": "0",
        "liquidation_price": "0",
        "notional": decimal_text(notional),
        "leverage": leverage,
        "margin": decimal_text(margin),
    }


def _refresh_synthetic_account_totals(account: dict[str, Any]) -> None:
    positions = [position for position in account.get("positions") or [] if isinstance(position, dict)]
    total_unrealized = sum((_decimal_payload_value(position.get("unrealized_pnl")) for position in positions), Decimal("0"))
    total_margin = sum((_decimal_payload_value(position.get("margin")) for position in positions), Decimal("0"))
    wallet_balance = _decimal_payload_value(account.get("wallet_balance") or account.get("collateral"))
    equity = wallet_balance + total_unrealized
    available_balance = max(equity - total_margin, Decimal("0"))
    _set_decimal_payload(account, "total_unrealized_pnl", total_unrealized)
    _set_decimal_payload(account, "margin", total_margin)
    _set_decimal_payload(account, "equity", equity)
    _set_decimal_payload(account, "available_balance", available_balance)
    _set_decimal_payload(account, "margin_deficit", max(total_margin - equity, Decimal("0")))


def _update_existing_position_qty(position: dict[str, Any], qty: Decimal, price: Decimal) -> None:
    previous_qty = _decimal_payload_value(position.get("qty"))
    previous_pnl = _decimal_payload_value(position.get("unrealized_pnl"))
    next_pnl = Decimal("0") if previous_qty <= 0 else previous_pnl * qty / previous_qty
    leverage = int(position.get("leverage") or _DEFAULT_SYNTHETIC_LEVERAGE)
    notional = qty * _decimal_payload_value(position.get("mark_price") or price)
    margin = notional / Decimal(max(leverage, 1))
    position["qty"] = decimal_text(qty)
    _set_decimal_payload(position, "unrealized_pnl", next_pnl)
    _set_decimal_payload(position, "notional", notional)
    _set_decimal_payload(position, "margin", margin)


def _default_execution_rules(symbol: str) -> SymbolRules:
    return SymbolRules(
        symbol.strip().upper() or "ETHUSDC",
        Decimal("0.01"),
        Decimal("0.001"),
        Decimal("0.001"),
        Decimal("5"),
        125,
    )


def _group_plan_from_payload(group: dict[str, Any]) -> KanglongGroupPlan:
    side = PositionSide(str(group.get("side") or PositionSide.LONG.value).strip().upper())
    round_qtys = [_decimal_payload_value(qty) for qty in group.get("round_qtys") or []]
    target_qty = _decimal_payload_value(group.get("target_qty")) or sum(round_qtys, Decimal("0"))
    return KanglongGroupPlan(
        group_id=str(group.get("group_id") or ""),
        from_account_id=str(group.get("from_account_id") or ""),
        to_account_id=str(group.get("to_account_id") or ""),
        symbol=str(group.get("symbol") or "ETHUSDC").strip().upper(),
        side=side,
        target_qty=target_qty,
        round_qtys=round_qtys,
        batch_id=group.get("batch_id"),
    )


def _round_index_from_round_id(round_id: str) -> str:
    try:
        return str(int(str(round_id).rsplit("-", 1)[-1]))
    except (TypeError, ValueError):
        return str(round_id or "")


def _kanglong_trade_events(
    result: Any,
    *,
    group: KanglongGroupPlan,
    plan_version: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in result.events:
        payload = event.to_payload()
        action_label = "平仓" if event.action_type == "single_close" else "开仓"
        payload.update(
            {
                "message_key": "events.kanglong.trade_executed",
                "message_params": {
                    "group_id": event.group_id,
                    "round_id": _round_index_from_round_id(event.round_id),
                    "action_label": action_label,
                    "account_id": event.account_id,
                    "from_account_id": group.from_account_id,
                    "to_account_id": group.to_account_id,
                    "symbol": event.symbol,
                    "side": event.position_side.value,
                    "filled_qty": decimal_text(event.filled_qty),
                    "avg_price": decimal_text(event.avg_price),
                    "fee": decimal_text(event.fee),
                    "status": event.status.value,
                },
                "plan_version": plan_version,
                "from_account_id": group.from_account_id,
                "to_account_id": group.to_account_id,
            }
        )
        rows.append(
            {
                "event_type": "kanglong_trade_executed",
                "group_id": event.group_id,
                "round_id": event.round_id,
                "payload": payload,
            }
        )
    return rows


def _round_process_events_from_result(
    result: Any,
    *,
    plan_version: str,
    close_price: Decimal,
    open_price: Decimal,
    fee_rate: Decimal,
) -> list[dict[str, Any]]:
    matched_by_round: dict[str, Decimal] = {}
    status_by_round: dict[str, str] = {}
    for event in result.events:
        round_id = str(event.round_id or "")
        matched_by_round[round_id] = max(
            matched_by_round.get(round_id, Decimal("0")),
            event.matched_qty,
        )
        status_by_round[round_id] = event.status.value
    events: list[dict[str, Any]] = []
    for round_index, round_id in enumerate(matched_by_round, start=1):
        matched_qty = matched_by_round[round_id]
        matched_qty_text = decimal_text(matched_qty)
        events.append(
            {
                "event_type": "kanglong_round_completed",
                "group_id": result.group_id,
                "round_id": round_id,
                "payload": {
                    "message_key": "events.kanglong.round_completed",
                    "message_params": {
                        "group_id": result.group_id,
                        "round_id": _round_index_from_round_id(round_id) or str(round_index),
                        "matched_qty": matched_qty_text,
                    },
                    "group_id": result.group_id,
                    "round_id": round_id,
                    "round_index": round_index,
                    "plan_version": plan_version,
                    "matched_qty": matched_qty_text,
                    "status": status_by_round.get(round_id, ""),
                    "close_price": decimal_text(close_price),
                    "open_price": decimal_text(open_price),
                    "fee_rate": decimal_text(fee_rate),
                },
            }
        )
    return events


def _synthetic_account_baseline(report: dict[str, Any]) -> list[dict[str, Any]]:
    synthetic_state = report.get("synthetic_account_state")
    if isinstance(synthetic_state, dict) and isinstance(synthetic_state.get("accounts"), list):
        return copy.deepcopy(synthetic_state["accounts"])
    account_snapshot = report.get("account_snapshot")
    if isinstance(account_snapshot, dict) and isinstance(account_snapshot.get("accounts"), list):
        return copy.deepcopy(account_snapshot["accounts"])
    return []


def _apply_group_result_to_synthetic_accounts(
    accounts: list[dict[str, Any]],
    group: dict[str, Any],
    *,
    matched_qty: Decimal,
    close_price: Decimal,
    open_price: Decimal,
) -> list[dict[str, Any]]:
    if matched_qty <= Decimal("0"):
        return accounts
    side = str(group.get("side") or "").strip().upper()
    from_account = _find_synthetic_account(accounts, str(group.get("from_account_id") or ""))
    to_account = _find_synthetic_account(accounts, str(group.get("to_account_id") or ""))
    if from_account is None or to_account is None:
        return accounts
    from_position = _find_synthetic_position(from_account, side)
    if from_position is None:
        return accounts
    effective_matched_qty = min(matched_qty, _decimal_payload_value(from_position.get("qty")))
    if effective_matched_qty <= Decimal("0"):
        return accounts
    next_qty = max(_decimal_payload_value(from_position.get("qty")) - effective_matched_qty, Decimal("0"))
    _update_existing_position_qty(from_position, next_qty, close_price)
    _refresh_synthetic_account_totals(from_account)
    to_position = _find_synthetic_position(to_account, side)
    if to_position is None:
        positions = list(to_account.get("positions") or [])
        to_position = _new_synthetic_position(to_account, group, Decimal("0"), open_price)
        positions.append(to_position)
        to_account["positions"] = positions
    next_qty = _decimal_payload_value(to_position.get("qty")) + effective_matched_qty
    _update_existing_position_qty(to_position, next_qty, open_price)
    _refresh_synthetic_account_totals(to_account)
    return accounts


def _synthetic_group_apply_issue(
    accounts: list[dict[str, Any]],
    group: dict[str, Any],
    *,
    matched_qty: Decimal,
) -> dict[str, Any] | None:
    if matched_qty <= Decimal("0"):
        return None
    side = str(group.get("side") or "").strip().upper()
    from_account_id = str(group.get("from_account_id") or "")
    to_account_id = str(group.get("to_account_id") or "")
    from_account = _find_synthetic_account(accounts, from_account_id)
    to_account = _find_synthetic_account(accounts, to_account_id)
    base = {
        "group_id": group.get("group_id"),
        "from_account_id": from_account_id,
        "to_account_id": to_account_id,
        "side": side,
        "matched_qty": decimal_text(matched_qty),
    }
    if from_account is None:
        return {**base, "reason": "synthetic_donor_missing", "missing_account_id": from_account_id}
    if to_account is None:
        return {**base, "reason": "synthetic_receiver_missing", "missing_account_id": to_account_id}
    from_position = _find_synthetic_position(from_account, side)
    if from_position is None:
        return {**base, "reason": "synthetic_donor_position_missing", "missing_account_id": from_account_id}
    effective_matched_qty = min(matched_qty, _decimal_payload_value(from_position.get("qty")))
    if effective_matched_qty <= Decimal("0"):
        return {**base, "reason": "synthetic_donor_qty_unavailable", "missing_account_id": from_account_id}
    return None


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
            "chain_config": _chain_config_payload(
                symbol=symbol,
                selected_side=precheck.selected_side,
                main_account_id=main_account_id,
                main_snapshot=main_snapshot,
                subaccount_snapshots=subaccount_snapshots,
                groups=plan.groups,
                account_snapshot_payload=account_snapshot_payload,
            ),
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

    def _idempotency_response(
        self,
        *,
        idempotency_key: str,
        request_hash: str,
        run_id: str,
    ) -> dict[str, Any] | None:
        existing = self._repository.get_kanglong_idempotency(idempotency_key, request_hash)
        if existing is None:
            return None
        if existing["conflict"]:
            return self._idempotency_conflict(run_id)
        return existing["response"]

    def confirm_plan_idempotency_response(
        self,
        *,
        run_id: str,
        plan_version: str,
        idempotency_key: str,
        operator: str,
        confirmed_warning_codes: list[str],
    ) -> tuple[str, dict[str, Any] | None]:
        request_hash = _request_hash(
            {
                "action": "confirm",
                "run_id": run_id,
                "plan_version": plan_version,
                "operator": operator,
                "confirmed_warning_codes": confirmed_warning_codes,
            }
        )
        return request_hash, self._idempotency_response(
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            run_id=run_id,
        )

    def confirm_plan(
        self,
        *,
        run_id: str,
        plan_version: str,
        idempotency_key: str,
        operator: str,
        confirmed_warning_codes: list[str],
    ) -> dict[str, Any]:
        request_hash, existing_response = self.confirm_plan_idempotency_response(
            run_id=run_id,
            plan_version=plan_version,
            idempotency_key=idempotency_key,
            operator=operator,
            confirmed_warning_codes=confirmed_warning_codes,
        )
        if existing_response is not None:
            return existing_response

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

    def execute_plan_idempotency_response(
        self,
        *,
        run_id: str,
        plan_version: str,
        idempotency_key: str,
    ) -> tuple[str, dict[str, Any] | None]:
        request_hash = _request_hash(
            {
                "action": "execute",
                "run_id": run_id,
                "plan_version": plan_version,
            }
        )
        return request_hash, self._idempotency_response(
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            run_id=run_id,
        )

    def execute_plan(
        self,
        *,
        run_id: str,
        plan_version: str,
        idempotency_key: str,
        close_price: Decimal,
        open_price: Decimal,
        fee_rate: Decimal,
        rules: SymbolRules | None = None,
        recheck_main_snapshot: KanglongAccountSnapshot | None = None,
        recheck_subaccount_snapshots: list[KanglongAccountSnapshot] | None = None,
        recheck_selected_side: PositionSide | None = None,
        recheck_config: KanglongSymbolConfig | None = None,
        recheck_snapshot_bundle_id: str | None = None,
    ) -> dict[str, Any]:
        request_hash, existing_response = self.execute_plan_idempotency_response(
            run_id=run_id,
            plan_version=plan_version,
            idempotency_key=idempotency_key,
        )
        if existing_response is not None:
            return existing_response

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
        report = copy.deepcopy(stored.get("report") or {})
        request_payload = stored.get("request") or {}
        synthetic_accounts = _synthetic_account_baseline(report) if request_payload.get("account_source") == "test_template" else []
        groups = list(plan.get("groups") or [])
        execution_rules = rules or _default_execution_rules(str(plan.get("symbol") or request_payload.get("symbol") or "ETHUSDC"))
        execution_config = recheck_config or KanglongSymbolConfig()
        for group_index, group in enumerate(groups, start=1):
            group_id = group.get("group_id")
            group_plan = _group_plan_from_payload(group)
            result = simulate_group(
                run_id=run_id,
                group=group_plan,
                rules=execution_rules,
                close_price=close_price,
                open_price=open_price,
                fee_rate=fee_rate,
                config=execution_config,
            )
            if synthetic_accounts:
                synthetic_issue = _synthetic_group_apply_issue(
                    synthetic_accounts,
                    group,
                    matched_qty=result.matched_qty,
                )
                if synthetic_issue is not None:
                    report["synthetic_ledger_error"] = _payloadify(synthetic_issue)
                    blocks = list(report.get("blocks") or [])
                    if "synthetic_ledger_inconsistent" not in blocks:
                        blocks.append("synthetic_ledger_inconsistent")
                    report["blocks"] = blocks
                    progress = {
                        "current_group_id": group_id,
                        "groups_completed": max(group_index - 1, 0),
                        "group_count": len(groups),
                    }
                    available_actions = ["recover"]
                    event_payload = {
                        "message_key": "events.kanglong.synthetic_ledger_failed",
                        "message_params": {
                            "group_id": group_id,
                            "reason": synthetic_issue["reason"],
                        },
                        **_payloadify(synthetic_issue),
                    }
                    self._repository.update_kanglong_run_and_events(
                        run_id,
                        status=KanglongRunStatus.NEEDS_ABORT_RECOVER.value,
                        result_grade=KanglongResultGrade.UNSAFE_UNCLOSED.value,
                        available_actions=available_actions,
                        progress=progress,
                        report=report,
                        events=[
                            {
                                "event_type": "kanglong_synthetic_ledger_failed",
                                "group_id": group_id,
                                "payload": event_payload,
                            }
                        ],
                    )
                    response = _response_base(
                        run_id,
                        KanglongRunStatus.NEEDS_ABORT_RECOVER.value,
                        plan_version=plan_version,
                        snapshot_bundle_id=stored.get("snapshot_bundle_id"),
                        available_actions=available_actions,
                        report=report,
                        result_grade=KanglongResultGrade.UNSAFE_UNCLOSED.value,
                        latest_event_id=self._repository.latest_kanglong_event_id(run_id),
                    )
                    return self._remember_idempotency(idempotency_key, request_hash, response)
                synthetic_accounts = _apply_group_result_to_synthetic_accounts(
                    synthetic_accounts,
                    group,
                    matched_qty=result.matched_qty,
                    close_price=close_price,
                    open_price=open_price,
                )
                report["synthetic_account_state"] = {
                    "account_source": "test_template",
                    "state_version": f"{run_id}:{group_id}:group-{group_index:04d}",
                    "accounts": synthetic_accounts,
                    "updated_at": _now_text(),
                }
            progress = {
                "current_group_id": group_id,
                "groups_completed": group_index,
                "group_count": len(groups),
            }
            trade_events = _kanglong_trade_events(
                result,
                group=group_plan,
                plan_version=plan_version,
            )
            round_events = _round_process_events_from_result(
                result,
                plan_version=plan_version,
                close_price=close_price,
                open_price=open_price,
                fee_rate=fee_rate,
            )
            self._repository.update_kanglong_run_and_events(
                run_id,
                status=KanglongRunStatus.GROUP_COMPLETED.value,
                available_actions=[],
                progress=progress,
                report=report,
                events=[
                    *trade_events,
                    *round_events,
                    {
                        "event_type": "kanglong_group_simulated",
                        "group_id": group_id,
                        "payload": {
                            "message_key": "events.kanglong.group_simulated",
                            "message_params": {"group_id": group_id},
                            "group_id": group_id,
                            "plan_version": plan_version,
                            "close_price": decimal_text(close_price),
                            "open_price": decimal_text(open_price),
                            "fee_rate": decimal_text(fee_rate),
                        },
                    }
                ],
            )

        available_actions = ["view_report"]
        self._repository.update_kanglong_run(
            run_id,
            status=KanglongRunStatus.COMPLETED.value,
            result_grade=KanglongResultGrade.SAFE_CLOSED.value,
            available_actions=available_actions,
            report=report,
        )
        self._repository.release_kanglong_locks(run_id)
        response = _response_base(
            run_id,
            KanglongRunStatus.COMPLETED.value,
            plan_version=plan_version,
            snapshot_bundle_id=stored.get("snapshot_bundle_id"),
            available_actions=available_actions,
            report=report,
            result_grade=KanglongResultGrade.SAFE_CLOSED.value,
            latest_event_id=self._repository.latest_kanglong_event_id(run_id),
        )
        return self._remember_idempotency(idempotency_key, request_hash, response)

    def recover_run_idempotency_response(
        self,
        *,
        run_id: str,
        idempotency_key: str,
        operator: str,
        release_reason: str,
    ) -> tuple[str, dict[str, Any] | None]:
        request_hash = _request_hash(
            {
                "action": "recover",
                "run_id": run_id,
                "operator": operator,
                "release_reason": release_reason,
            }
        )
        return request_hash, self._idempotency_response(
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            run_id=run_id,
        )

    def recover_run(
        self,
        *,
        run_id: str,
        idempotency_key: str,
        operator: str,
        release_reason: str,
    ) -> dict[str, Any]:
        request_hash, existing_response = self.recover_run_idempotency_response(
            run_id=run_id,
            idempotency_key=idempotency_key,
            operator=operator,
            release_reason=release_reason,
        )
        if existing_response is not None:
            return existing_response

        stored = self._repository.get_kanglong_run(run_id)
        if stored is None:
            return self._not_found(run_id, "")
        if stored.get("status") not in {
            KanglongRunStatus.NEEDS_ABORT_RECOVER.value,
            KanglongRunStatus.ABORT_RECOVERING.value,
        }:
            return self._blocked_plan_recheck_failed(run_id, stored.get("plan_version") or "", stored)

        recovered_at = _now_text()
        report = copy.deepcopy(stored.get("report") or {})
        history = list(report.get("abort_recover_history") or [])
        history.append(
            {
                "operator": operator,
                "release_reason": release_reason,
                "previous_status": stored.get("status"),
                "recovered_at": recovered_at,
            }
        )
        report["abort_recover_history"] = history
        available_actions = ["refresh_plan"]
        event_payload = {
            "message_key": "events.kanglong.abort_recovered",
            "operator": operator,
            "release_reason": release_reason,
            "previous_status": stored.get("status"),
            "recovered_at": recovered_at,
        }
        self._repository.update_kanglong_run_and_events(
            run_id,
            status=KanglongRunStatus.ABORTED_RECOVERED.value,
            report=report,
            result_grade=KanglongResultGrade.UNSAFE_UNCLOSED.value,
            available_actions=available_actions,
            events=[{"event_type": "kanglong_abort_recovered", "payload": event_payload}],
        )
        self._repository.release_kanglong_locks(run_id)
        response = _response_base(
            run_id,
            KanglongRunStatus.ABORTED_RECOVERED.value,
            plan_version=stored.get("plan_version"),
            snapshot_bundle_id=stored.get("snapshot_bundle_id"),
            available_actions=available_actions,
            report=report,
            result_grade=KanglongResultGrade.UNSAFE_UNCLOSED.value,
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
