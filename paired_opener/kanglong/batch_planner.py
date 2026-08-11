from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any, Awaitable, Callable, Mapping, Sequence
from uuid import uuid4

from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.batch_models import (
    KanglongBatchAccountPlan,
    KanglongBatchPlan,
    stable_payload_hash,
)
from paired_opener.rounding import quantize_step


class UnsafeBatchRefresh(ValueError):
    pass


_COMPLETED_ACCOUNT_STATUSES = {"completed", "completed_with_dust"}
_REFRESHABLE_ACCOUNT_STATUSES = {"pending", "blocked_precheck"}
_UNSAFE_ACCOUNT_STATUSES = {
    "first_leg",
    "second_leg",
    "aligning",
    "retry_wait",
    "needs_recovery",
}


def _value(source: Any, name: str, default: Any) -> Any:
    if source is None:
        return default
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)


def _snapshot_id(prefix: str, account_id: str, snapshot: Any) -> str:
    explicit = _value(snapshot, f"{prefix}_snapshot_id", "")
    if explicit:
        return str(explicit)
    evidence = {
        "account_id": account_id,
        "assembled_at": str(_value(snapshot, "assembled_at", "")),
        "oldest_component_at": str(_value(snapshot, "oldest_component_at", "")),
        "components": _value(snapshot, "snapshot_components", {}),
    }
    return f"{prefix}-{stable_payload_hash(evidence)[:24]}"


def _lock_scopes(account_ids: Sequence[str], source_open_run_id: str | None = None) -> tuple[str, ...]:
    scopes = {f"kanglong:account:{account_id}" for account_id in account_ids}
    if source_open_run_id:
        scopes.add(f"kanglong:source-open-run:{source_open_run_id}")
    return tuple(sorted(scopes))


class KanglongBatchPlanner:
    def __init__(
        self,
        repository: Any | None = None,
        close_snapshot_loader: Callable[[str, str, bool], Awaitable[Any]] | None = None,
    ) -> None:
        self._repository = repository
        self._close_snapshot_loader = close_snapshot_loader

    async def refresh_close_availability(
        self,
        source_open_run_id: str,
        account_ids: Sequence[str],
        *,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        if self._repository is None:
            raise RuntimeError("kanglong_close_repository_required")
        availability = self._repository.get_kanglong_batch_source_availability(
            source_open_run_id,
            list(account_ids),
        )
        snapshots: dict[str, Any] = {}
        symbol = str(availability.get("symbol") or "")
        if self._close_snapshot_loader is not None:
            for account_id in account_ids:
                snapshot = await self._close_snapshot_loader(account_id, symbol, force_refresh)
                if str(_value(snapshot, "account_status", "")) != "NORMAL":
                    raise ValueError("kanglong_close_account_not_normal")
                if not bool(_value(snapshot, "hedge_mode", False)):
                    raise ValueError("kanglong_close_hedge_mode_required")
                snapshots[account_id] = snapshot
        return {
            **availability,
            "open_capacity_check_applied": False,
            "account_snapshots": snapshots,
        }

    def plan_open(
        self,
        *,
        account_ids: Sequence[str],
        credential_revision: str,
        symbol: str,
        preferred_side: PositionSide,
        leverage: int,
        per_leg_notional: Decimal,
        reference_price: Decimal,
        rules: SymbolRules,
        account_snapshots: Mapping[str, Any] | None = None,
        run_id: str | None = None,
        round_count: int = 30,
        round_interval_seconds: int = 3,
        warning_codes: Sequence[str] = (),
    ) -> KanglongBatchPlan:
        normalized_ids = tuple(str(account_id).strip().lower() for account_id in account_ids)
        if not normalized_ids or len(normalized_ids) != len(set(normalized_ids)):
            raise ValueError("kanglong_batch_account_order_invalid")
        price = Decimal(reference_price)
        notional = Decimal(per_leg_notional)
        if price <= 0 or notional <= 0:
            raise ValueError("kanglong_batch_target_invalid")
        target_qty = quantize_step(notional / price, rules.step_size)
        if target_qty <= 0:
            raise ValueError("kanglong_batch_target_below_step_size")
        snapshots = account_snapshots or {}
        accounts = tuple(
            self._open_account_plan(
                account_id=account_id,
                sequence=sequence,
                target_qty=target_qty,
                reference_price=price,
                requested_leverage=int(leverage),
                snapshot=snapshots.get(account_id),
            )
            for sequence, account_id in enumerate(normalized_ids)
        )
        input_payload = {
            "operation": "open",
            "symbol": symbol.strip().upper(),
            "preferred_side": preferred_side.value,
            "requested_leverage": int(leverage),
            "per_leg_notional": notional,
            "account_ids": normalized_ids,
            "credential_revision": credential_revision,
            "round_count": int(round_count),
            "round_interval_seconds": int(round_interval_seconds),
        }
        return self._finalize_plan(
            KanglongBatchPlan(
                run_id=run_id or f"kanglong-batch-{uuid4().hex}",
                operation="open",
                symbol=symbol.strip().upper(),
                preferred_side=preferred_side,
                requested_leverage=int(leverage),
                per_leg_notional=notional,
                accounts=accounts,
                source_open_run_id=None,
                credential_revision=credential_revision,
                lock_scopes=_lock_scopes(normalized_ids),
                completed_prefix_length=0,
                input_hash=stable_payload_hash(input_payload),
                plan_version="",
                round_count=int(round_count),
                round_interval_seconds=int(round_interval_seconds),
                warning_codes=tuple(dict.fromkeys(str(code) for code in warning_codes)),
            )
        )

    def plan_close(
        self,
        *,
        source_open_run: KanglongBatchPlan | Mapping[str, Any],
        credential_revision: str | None = None,
        preferred_side: PositionSide | None = None,
        run_id: str | None = None,
        account_ids: Sequence[str] | None = None,
        round_count: int = 30,
        round_interval_seconds: int = 3,
        account_snapshot: Any | None = None,
    ) -> KanglongBatchPlan:
        source = (
            source_open_run.to_payload()
            if isinstance(source_open_run, KanglongBatchPlan)
            else dict(source_open_run)
        )
        source_run_id = str(source.get("run_id") or "")
        if not source_run_id:
            raise ValueError("kanglong_source_open_run_required")
        source_accounts = source.get("accounts") or []
        by_id = {str(_value(item, "account_id", "")): item for item in source_accounts}
        ordered_ids = tuple(account_ids or by_id.keys())
        if not ordered_ids or any(account_id not in by_id for account_id in ordered_ids):
            raise ValueError("kanglong_source_account_not_found")
        accounts: list[KanglongBatchAccountPlan] = []
        for sequence, account_id in enumerate(ordered_ids):
            item = by_id[account_id]
            long_remaining = Decimal(str(_value(item, "source_long_remaining_qty", _value(item, "target_long_qty", "0"))))
            short_remaining = Decimal(str(_value(item, "source_short_remaining_qty", _value(item, "target_short_qty", "0"))))
            if long_remaining < 0 or short_remaining < 0:
                raise ValueError("kanglong_source_remaining_invalid")
            accounts.append(
                KanglongBatchAccountPlan(
                    account_id=account_id,
                    sequence=sequence,
                    target_long_qty=long_remaining,
                    target_short_qty=short_remaining,
                    maker_fee_rate=Decimal(str(_value(item, "maker_fee_rate", "0"))),
                    taker_fee_rate=Decimal(str(_value(item, "taker_fee_rate", "0"))),
                    bracket_max_allowed_leverage=int(_value(item, "bracket_max_allowed_leverage", 1)),
                    bracket_notional_coef=Decimal(str(_value(item, "bracket_notional_coef", "1"))),
                    selected_bracket_effective_cap=Decimal(str(_value(item, "selected_bracket_effective_cap", "0"))),
                    current_symbol_leverage=int(_value(item, "current_symbol_leverage", 1)),
                    current_symbol_max_notional_value=Decimal(str(_value(item, "current_symbol_max_notional_value", "0"))),
                    effective_capacity_leverage=int(_value(item, "effective_capacity_leverage", 1)),
                    reference_mid_price=Decimal(str(_value(item, "reference_mid_price", "0"))),
                    capacity_snapshot_id=str(_value(item, "capacity_snapshot_id", "close-not-applicable")),
                    market_snapshot_id=str(_value(item, "market_snapshot_id", _snapshot_id("market", account_id, account_snapshot))),
                    source_long_remaining_qty=long_remaining,
                    source_short_remaining_qty=short_remaining,
                    source_ledger_hash=_value(item, "source_ledger_hash", source.get("ledger_hash")),
                    source_checkpoint_id=_value(item, "source_checkpoint_id", source.get("checkpoint_id")),
                )
            )
        side = preferred_side or PositionSide(str(source.get("preferred_side") or PositionSide.LONG.value))
        revision = credential_revision if credential_revision is not None else str(source.get("credential_revision") or "")
        input_payload = {
            "operation": "close",
            "source_open_run_id": source_run_id,
            "account_ids": ordered_ids,
            "credential_revision": revision,
            "source_ownership": [
                (
                    item.account_id,
                    item.source_long_remaining_qty,
                    item.source_short_remaining_qty,
                    item.source_ledger_hash,
                    item.source_checkpoint_id,
                )
                for item in accounts
            ],
        }
        return self._finalize_plan(
            KanglongBatchPlan(
                run_id=run_id or f"kanglong-batch-{uuid4().hex}",
                operation="close",
                symbol=str(source.get("symbol") or "").strip().upper(),
                preferred_side=side,
                requested_leverage=int(source.get("requested_leverage") or 1),
                per_leg_notional=Decimal("0"),
                accounts=tuple(accounts),
                source_open_run_id=source_run_id,
                credential_revision=revision,
                lock_scopes=_lock_scopes(ordered_ids, source_run_id),
                completed_prefix_length=0,
                input_hash=stable_payload_hash(input_payload),
                plan_version="",
                round_count=int(round_count),
                round_interval_seconds=int(round_interval_seconds),
                blocked=False,
                blocked_reasons=(),
                open_capacity_check_applied=False,
            )
        )

    def refresh_pending_suffix(
        self,
        *,
        stored_plan: KanglongBatchPlan,
        account_statuses: Mapping[str, str],
        refreshed_accounts: Mapping[str, KanglongBatchAccountPlan],
        credential_revision: str,
    ) -> KanglongBatchPlan:
        prefix_length = 0
        saw_pending = False
        rebuilt: list[KanglongBatchAccountPlan] = []
        for original in stored_plan.accounts:
            status = str(account_statuses.get(original.account_id, "pending"))
            if status in _UNSAFE_ACCOUNT_STATUSES:
                raise UnsafeBatchRefresh(f"unsafe account stage: {original.account_id}:{status}")
            if status in _COMPLETED_ACCOUNT_STATUSES and not saw_pending:
                rebuilt.append(original)
                prefix_length += 1
                continue
            saw_pending = True
            if status not in _REFRESHABLE_ACCOUNT_STATUSES:
                raise UnsafeBatchRefresh(f"non-contiguous completed prefix: {original.account_id}:{status}")
            refreshed = refreshed_accounts.get(original.account_id)
            if refreshed is None:
                raise UnsafeBatchRefresh(f"missing refreshed account: {original.account_id}")
            rebuilt.append(replace(refreshed, sequence=original.sequence, account_id=original.account_id))
        changed = replace(
            stored_plan,
            accounts=tuple(rebuilt),
            completed_prefix_length=prefix_length,
            credential_revision=credential_revision,
            input_hash="",
            plan_version="",
        )
        input_hash = stable_payload_hash(
            {
                "previous_input_hash": stored_plan.input_hash,
                "credential_revision": credential_revision,
                "completed_prefix_length": prefix_length,
                "accounts": [item.to_payload() for item in rebuilt],
            }
        )
        return self._finalize_plan(replace(changed, input_hash=input_hash))

    @staticmethod
    def _open_account_plan(
        *,
        account_id: str,
        sequence: int,
        target_qty: Decimal,
        reference_price: Decimal,
        requested_leverage: int,
        snapshot: Any,
    ) -> KanglongBatchAccountPlan:
        return KanglongBatchAccountPlan(
            account_id=account_id,
            sequence=sequence,
            target_long_qty=target_qty,
            target_short_qty=target_qty,
            maker_fee_rate=Decimal(str(_value(snapshot, "maker_fee_rate", "0"))),
            taker_fee_rate=Decimal(str(_value(snapshot, "taker_fee_rate", "0"))),
            bracket_max_allowed_leverage=int(_value(snapshot, "bracket_max_allowed_leverage", requested_leverage)),
            bracket_notional_coef=Decimal(str(_value(snapshot, "bracket_notional_coef", "1"))),
            selected_bracket_effective_cap=Decimal(str(_value(snapshot, "selected_bracket_effective_cap", "0"))),
            current_symbol_leverage=int(_value(snapshot, "current_symbol_leverage", requested_leverage)),
            current_symbol_max_notional_value=Decimal(str(_value(snapshot, "current_symbol_max_notional_value", "0"))),
            effective_capacity_leverage=int(_value(snapshot, "effective_capacity_leverage", requested_leverage)),
            reference_mid_price=reference_price,
            capacity_snapshot_id=_snapshot_id("capacity", account_id, snapshot),
            market_snapshot_id=_snapshot_id("market", account_id, snapshot),
        )

    @staticmethod
    def _finalize_plan(plan: KanglongBatchPlan) -> KanglongBatchPlan:
        version_payload = plan.to_payload()
        version_payload["plan_version"] = ""
        return replace(plan, plan_version=f"plan-{stable_payload_hash(version_payload)}")
