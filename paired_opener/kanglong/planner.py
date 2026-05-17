from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from paired_opener.domain import PositionSide
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.models import (
    KanglongAccountSnapshot,
    KanglongBatchDebtBuffer,
    KanglongGroupPlan,
    KanglongPlan,
    KanglongPlanningAccount,
    PendingDebt,
)


@dataclass(slots=True)
class _CandidatePlan:
    account: KanglongPlanningAccount
    segments: list[tuple[str, Decimal]]
    transfer_qty: Decimal
    covers_all_debt: bool
    covered_debt_count: int


def split_round_qtys(target_qty: Decimal, per_round_qty_limit: Decimal) -> list[Decimal]:
    remaining = target_qty
    rounds: list[Decimal] = []
    while remaining > Decimal("0"):
        qty = min(per_round_qty_limit, remaining)
        rounds.append(qty)
        remaining -= qty
    return rounds


def _group(
    group_index: int,
    from_account_id: str,
    to_account_id: str,
    symbol: str,
    side: PositionSide,
    qty: Decimal,
    config: KanglongSymbolConfig,
    batch_id: str | None = None,
) -> KanglongGroupPlan:
    return KanglongGroupPlan(
        group_id=f"group-{group_index:04d}",
        from_account_id=from_account_id,
        to_account_id=to_account_id,
        symbol=symbol,
        side=side,
        target_qty=qty,
        round_qtys=split_round_qtys(qty, config.per_round_qty_limit),
        batch_id=batch_id,
    )


def _profit_per_qty(account: KanglongPlanningAccount) -> Decimal:
    if account.closeable_qty <= Decimal("0"):
        return Decimal("0")
    return account.unrealized_profit / account.closeable_qty


def _candidate_segments(
    account: KanglongPlanningAccount,
    debts: list[PendingDebt],
    receiver_capacity_remaining: dict[str, Decimal],
    config: KanglongSymbolConfig,
) -> _CandidatePlan | None:
    remaining = account.closeable_qty
    total_pending_debt = sum((debt.qty for debt in debts), Decimal("0"))
    covered_debt_count = 0
    segments: list[tuple[str, Decimal]] = []
    local_capacity = dict(receiver_capacity_remaining)

    for debt in debts:
        if remaining <= config.qty_tolerance:
            break
        receiver_capacity = local_capacity.get(debt.account_id, Decimal("0"))
        segment_qty = min(remaining, debt.qty, receiver_capacity)
        if segment_qty <= config.qty_tolerance:
            break
        segments.append((debt.account_id, segment_qty))
        remaining -= segment_qty
        local_capacity[debt.account_id] = receiver_capacity - segment_qty
        if debt.qty - segment_qty <= config.qty_tolerance:
            covered_debt_count += 1
        else:
            break

    transfer_qty = sum((qty for _, qty in segments), Decimal("0"))
    if transfer_qty <= config.qty_tolerance:
        return None
    return _CandidatePlan(
        account=account,
        segments=segments,
        transfer_qty=transfer_qty,
        covers_all_debt=transfer_qty + config.qty_tolerance >= total_pending_debt,
        covered_debt_count=covered_debt_count,
    )


def _candidate_score(candidate: _CandidatePlan, config: KanglongSymbolConfig) -> tuple[Decimal, Decimal, int, int, int, Decimal, Decimal, str]:
    estimated_net_release_profit = _profit_per_qty(candidate.account) * candidate.transfer_qty
    estimated_round_count = sum(
        len(split_round_qtys(qty, config.per_round_qty_limit))
        for _, qty in candidate.segments
    )
    return (
        -estimated_net_release_profit,
        -candidate.transfer_qty,
        0 if candidate.covers_all_debt else 1,
        -candidate.covered_debt_count,
        estimated_round_count,
        Decimal("0"),
        -candidate.account.risk_buffer,
        candidate.account.account_id,
    )


def _apply_segment_to_debts(debts: list[PendingDebt], receiver_account_id: str, qty: Decimal, tolerance: Decimal) -> None:
    if not debts or debts[0].account_id != receiver_account_id:
        return
    debts[0].qty -= qty
    if debts[0].qty <= tolerance:
        debts.pop(0)


def _opposite_side(side: PositionSide) -> PositionSide:
    return PositionSide.SHORT if side == PositionSide.LONG else PositionSide.LONG


def build_planning_accounts(
    snapshots: list[KanglongAccountSnapshot],
    selected_side: PositionSide,
    config: KanglongSymbolConfig,
) -> list[KanglongPlanningAccount]:
    accounts: list[KanglongPlanningAccount] = []
    opposite_side = _opposite_side(selected_side)
    for snapshot in snapshots:
        closeable_qty = snapshot.qty(selected_side) if snapshot.pnl(selected_side) > Decimal("0") else Decimal("0")
        if closeable_qty <= config.qty_tolerance:
            closeable_qty = Decimal("0")
        accounts.append(
            KanglongPlanningAccount(
                account_id=snapshot.account_id,
                closeable_qty=closeable_qty,
                unrealized_profit=max(snapshot.pnl(selected_side), Decimal("0")),
                receiver_capacity_qty=max(snapshot.qty(opposite_side), Decimal("0")),
                risk_buffer=snapshot.available_balance,
            )
        )
    return accounts


def build_kanglong_plan(
    *,
    run_id: str,
    symbol: str,
    selected_side: PositionSide,
    main_account_id: str,
    first_donor_account_id: str,
    planned_release_qty: Decimal,
    accounts: list[KanglongPlanningAccount],
    config: KanglongSymbolConfig,
) -> KanglongPlan:
    groups: list[KanglongGroupPlan] = []
    batch_buffers: list[KanglongBatchDebtBuffer] = []
    group_index = 1
    receiver_capacity_remaining = {
        account.account_id: account.receiver_capacity_qty
        for account in accounts
    }
    used_donor_ids = {first_donor_account_id}

    groups.append(
        _group(
            group_index,
            first_donor_account_id,
            main_account_id,
            symbol,
            selected_side,
            planned_release_qty,
            config,
        )
    )
    group_index += 1
    debts: list[PendingDebt] = [PendingDebt(first_donor_account_id, planned_release_qty)]

    while debts and len(groups) < config.max_chain_groups:
        pending_account_ids = {debt.account_id for debt in debts}
        candidates: list[_CandidatePlan] = []
        for account in accounts:
            if account.account_id in used_donor_ids or account.account_id in pending_account_ids:
                continue
            if account.has_pending_debt:
                continue
            if account.closeable_qty <= config.qty_tolerance or account.unrealized_profit <= Decimal("0"):
                continue
            candidate = _candidate_segments(account, debts, receiver_capacity_remaining, config)
            if candidate is not None:
                candidates.append(candidate)
        if not candidates:
            break

        selected = min(candidates, key=lambda candidate: _candidate_score(candidate, config))
        batch_id = f"batch-{group_index:04d}" if len(selected.segments) > 1 else None
        completed_group_ids: list[str] = []
        transferred = Decimal("0")

        for receiver_account_id, segment_qty in selected.segments:
            if len(groups) >= config.max_chain_groups:
                break
            group = _group(
                group_index,
                selected.account.account_id,
                receiver_account_id,
                symbol,
                selected_side,
                segment_qty,
                config,
                batch_id=batch_id,
            )
            groups.append(group)
            completed_group_ids.append(group.group_id)
            group_index += 1
            transferred += segment_qty
            receiver_capacity_remaining[receiver_account_id] -= segment_qty
            _apply_segment_to_debts(debts, receiver_account_id, segment_qty, config.qty_tolerance)

        if transferred <= config.qty_tolerance:
            break
        if batch_id is not None:
            batch_buffers.append(
                KanglongBatchDebtBuffer(
                    batch_id=batch_id,
                    donor_account_id=selected.account.account_id,
                    side=selected_side,
                    matched_qty=transferred,
                    completed_group_ids=completed_group_ids,
                )
            )
        debts.append(PendingDebt(selected.account.account_id, transferred))
        used_donor_ids.add(selected.account.account_id)

    for debt in debts:
        if debt.qty <= config.qty_tolerance:
            continue
        if len(groups) >= config.max_chain_groups:
            break
        groups.append(
            _group(
                group_index,
                main_account_id,
                debt.account_id,
                symbol,
                selected_side,
                debt.qty,
                config,
            )
        )
        group_index += 1

    return KanglongPlan(
        run_id=run_id,
        symbol=symbol,
        selected_side=selected_side,
        main_account_id=main_account_id,
        groups=groups,
        batch_debt_buffers=batch_buffers,
    )
