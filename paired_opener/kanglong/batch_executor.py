from __future__ import annotations

import asyncio
import hashlib
import json
import random
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Awaitable, Callable

from paired_opener.domain import OrderSide, PositionSide, SymbolRules
from paired_opener.errors import TradingError
from paired_opener.kanglong.batch_capacity import CapacityPolicy, estimate_account_capacity
from paired_opener.kanglong.batch_models import KanglongBatchAccountPlan, KanglongBatchPlan, stable_payload_hash
from paired_opener.kanglong.batch_planner import KanglongBatchPlanner
from paired_opener.kanglong.config import load_kanglong_symbol_config
from paired_opener.kanglong.ledger import KanglongLedgerEntry, hash_checkpoint, hash_ledger_state
from paired_opener.kanglong.models import KanglongRunStatus, available_actions_for_status
from paired_opener.kanglong.reporter import build_ledger_report
from paired_opener.rounding import normalize_qty
from paired_opener.simulation_matching import MatchResult, OrderbookMatcher, OrderbookSnapshot
from paired_opener.storage import KanglongActionMutation, KanglongLeaseExpectation


def operation_id(
    run_id: str,
    account_id: str,
    round_index: int,
    leg: str,
    economic_attempt: int,
) -> str:
    return f"{run_id}:{account_id}:round-{round_index:04d}:{leg}:attempt-{economic_attempt:04d}"


@dataclass(frozen=True, slots=True)
class RetryDecision:
    delay_seconds: Decimal
    persist_retry_wait: bool
    next_wakeup_at: datetime | None


@dataclass(frozen=True, slots=True)
class TransportRetryPolicy:
    max_attempts: int = 5
    base_delay_ms: int = 500
    max_delay_ms: int = 30_000
    jitter: Callable[[], float] = random.random

    def decide(
        self,
        attempt: int,
        *,
        retry_after_seconds: Decimal | None = None,
        http_status: int | None = None,
        now: datetime | None = None,
    ) -> RetryDecision:
        bounded_attempt = max(int(attempt), 1)
        exponential_ms = min(self.base_delay_ms * (2 ** (bounded_attempt - 1)), self.max_delay_ms)
        backoff = Decimal(str(exponential_ms * max(min(self.jitter(), 1.0), 0.0))) / Decimal("1000")
        retry_after = max(Decimal(retry_after_seconds or "0"), Decimal("0"))
        delay = retry_after if http_status == 418 else max(backoff, retry_after)
        persist = delay > Decimal("30")
        reference = now or datetime.now(UTC)
        return RetryDecision(
            delay_seconds=delay,
            persist_retry_wait=persist,
            next_wakeup_at=reference + timedelta(seconds=float(delay)) if persist else None,
        )

    def on_rate_limit(self, retry_after_seconds: Decimal, *, http_status: int = 429) -> RetryDecision:
        return self.decide(1, retry_after_seconds=retry_after_seconds, http_status=http_status)


class DuplicateOrderbookSnapshot(RuntimeError):
    pass


class KanglongBatchExecutor:
    def __init__(
        self,
        repository: Any,
        runtime_manager: Any,
        credential_revision_provider: Callable[[], str],
        capacity_coordinator: Any,
        settings: Any,
        *,
        close_planner: KanglongBatchPlanner | None = None,
        retry_policy: TransportRetryPolicy | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._repository = repository
        self._runtime_manager = runtime_manager
        self._credential_revision_provider = credential_revision_provider
        self._capacity_coordinator = capacity_coordinator
        self._settings = settings
        self._close_planner = close_planner or KanglongBatchPlanner(repository)
        self._retry_policy = retry_policy or TransportRetryPolicy()
        self._sleep = sleep

    @staticmethod
    def classify_gap(raw_gap: Decimal, quote_price: Decimal, rules: SymbolRules) -> str:
        raw = abs(Decimal(raw_gap))
        if raw == 0:
            return "balanced"
        tradeable = normalize_qty(raw, rules)
        if tradeable > 0 and tradeable * Decimal(quote_price) >= rules.min_notional:
            return "align"
        return "dust"

    async def run_next(self, run_id: str, lease_token: str, fencing_token: str) -> dict[str, Any]:
        try:
            return await self._run_next_impl(run_id, lease_token, fencing_token)
        except DuplicateOrderbookSnapshot as exc:
            stored = self._repository.get_kanglong_run(run_id)
            if stored is None:
                raise
            lease = KanglongLeaseExpectation(lease_token=lease_token, fencing_token=fencing_token)
            return self._worker_action(
                stored,
                next_status=KanglongRunStatus.PAUSED_MARKET_UNSTABLE.value,
                event_type="kanglong_batch_orderbook_rejected",
                lease=lease,
                idempotency_suffix=f"orderbook-rejected:{stable_payload_hash(str(exc))[:16]}",
            )
        except Exception as exc:
            retryable = isinstance(exc, (TimeoutError, ConnectionError)) or (
                isinstance(exc, TradingError) and exc.retryable
            )
            if not retryable:
                raise
            stored = self._repository.get_kanglong_run(run_id)
            account_row = self._repository.get_active_kanglong_batch_account(run_id)
            if stored is None or account_row is None:
                raise
            plan = KanglongBatchPlan.from_payload(stored["plan"])
            account = plan.accounts[int(account_row["sequence"])]
            retry_after = None
            http_status = None
            if isinstance(exc, TradingError):
                retry_after = exc.context.get("retry_after_seconds")
                http_status = exc.context.get("http_status") or exc.raw_code
            return await self._handle_transient_no_fill(
                stored,
                account,
                KanglongLeaseExpectation(lease_token=lease_token, fencing_token=fencing_token),
                retry_after_seconds=Decimal(str(retry_after)) if retry_after is not None else None,
                http_status=int(http_status) if str(http_status or "").isdigit() else None,
            )

    async def _run_next_impl(self, run_id: str, lease_token: str, fencing_token: str) -> dict[str, Any]:
        stored = self._repository.get_kanglong_run(run_id)
        if stored is None or stored.get("run_kind") != "kanglong_batch":
            raise ValueError("kanglong_batch_run_not_found")
        plan = KanglongBatchPlan.from_payload(stored["plan"])
        lease = KanglongLeaseExpectation(lease_token=lease_token, fencing_token=fencing_token)
        renewed = self._repository.renew_kanglong_run_lease(
            run_id=run_id,
            lease_token=lease_token,
            fencing_token=fencing_token,
            ttl_seconds=30,
        )
        if renewed is None:
            raise ValueError("kanglong_stale_fencing_token")
        status = str(stored["status"])
        if status == KanglongRunStatus.EXECUTION_STARTING.value:
            stored = self._worker_action(
                stored,
                next_status=KanglongRunStatus.RUNNING.value,
                event_type="kanglong_batch_execution_started",
                lease=lease,
                idempotency_suffix="worker-start",
            )
        elif status not in {
            KanglongRunStatus.RUNNING.value,
            KanglongRunStatus.PAUSE_PENDING.value,
            KanglongRunStatus.STOP_PENDING.value,
        }:
            return stored

        account_row = self._repository.get_active_kanglong_batch_account(run_id)
        if account_row is None:
            if stored["status"] == KanglongRunStatus.STOP_PENDING.value:
                return self._worker_action(
                    stored,
                    next_status=KanglongRunStatus.STOPPED_BY_USER.value,
                    event_type="kanglong_batch_stopped",
                    lease=lease,
                    idempotency_suffix=f"stop-{stored['progress'].get('action_version', 0)}",
                )
            return self._worker_action(
                stored,
                next_status=self._completion_status(run_id),
                event_type="kanglong_batch_completed",
                lease=lease,
                idempotency_suffix="worker-completed",
            )
        account_plan = plan.accounts[int(account_row["sequence"])]
        progress = dict(stored.get("progress") or {})
        wake_at = progress.get("next_wakeup_at")
        pending = progress.get("batch_pending_operation")
        if pending and pending.get("account_id") == account_plan.account_id:
            if wake_at and datetime.fromisoformat(str(wake_at)) > datetime.now(UTC):
                return stored
            return await self._execute_second_leg(stored, plan, account_plan, account_row, pending, lease)
        if account_row["status"] == "aligning":
            if wake_at and datetime.fromisoformat(str(wake_at)) > datetime.now(UTC):
                return stored
            return await self._execute_alignment(stored, plan, account_plan, account_row, lease)
        if stored["status"] == KanglongRunStatus.PAUSE_PENDING.value:
            return self._worker_action(
                stored,
                next_status=KanglongRunStatus.PAUSED_BY_USER.value,
                event_type="kanglong_batch_paused",
                lease=lease,
                idempotency_suffix=f"pause-{stored['progress'].get('action_version', 0)}",
            )
        if stored["status"] == KanglongRunStatus.STOP_PENDING.value:
            return self._worker_action(
                stored,
                next_status=KanglongRunStatus.STOPPED_BY_USER.value,
                event_type="kanglong_batch_stopped",
                lease=lease,
                idempotency_suffix=f"stop-{stored['progress'].get('action_version', 0)}",
            )
        if wake_at and datetime.fromisoformat(str(wake_at)) > datetime.now(UTC):
            return stored
        if account_row["status"] == "retry_wait":
            progress.pop("next_wakeup_at", None)
        if account_row["status"] in {"pending", "prechecking", "blocked_precheck", "retry_wait"}:
            precheck = await self._precheck_account(stored, plan, account_plan, lease)
            if precheck is not None:
                return precheck
            self._repository.update_kanglong_batch_account(
                run_id=run_id,
                account_id=account_plan.account_id,
                status="first_leg",
                fencing_token=fencing_token,
                expected_status=account_row["status"],
                last_precheck_snapshot_at=datetime.now(UTC).isoformat(),
            )
            account_row = self._repository.get_kanglong_batch_account(run_id, account_plan.account_id)
        return await self._execute_first_leg(stored, plan, account_plan, account_row, lease)

    async def _precheck_account(
        self,
        stored: dict[str, Any],
        plan: KanglongBatchPlan,
        account: KanglongBatchAccountPlan,
        lease: KanglongLeaseExpectation,
    ) -> dict[str, Any] | None:
        current_revision = self._credential_revision_provider()
        if current_revision != plan.credential_revision:
            return self._block_for_plan_refresh(stored, account.account_id, "credential_revision_conflict", lease)
        if plan.operation == "close":
            try:
                availability = await self._close_planner.refresh_close_availability(
                    plan.source_open_run_id or "",
                    [account.account_id],
                    force_refresh=True,
                )
            except Exception as exc:
                if isinstance(exc, (TimeoutError, ConnectionError)) or (
                    isinstance(exc, TradingError) and exc.retryable
                ):
                    raise
                return self._block_for_plan_refresh(stored, account.account_id, "kanglong_close_precheck_failed", lease)
            current = availability["accounts"][0]
            remaining_long = self._remaining_target(plan.run_id, account, PositionSide.LONG)
            remaining_short = self._remaining_target(plan.run_id, account, PositionSide.SHORT)
            if (
                Decimal(str(current["source_long_remaining_qty"])) < remaining_long
                or Decimal(str(current["source_short_remaining_qty"])) < remaining_short
            ):
                return self._block_for_plan_refresh(stored, account.account_id, "kanglong_close_source_changed", lease)
            return None
        try:
            snapshots = await self._capacity_coordinator.refresh_capacity(
                plan.credential_revision,
                [account.account_id],
                plan.symbol,
                force_refresh=True,
            )
            snapshot = snapshots[account.account_id]
            orderbook = await self._fresh_orderbook(account.account_id, plan.symbol)
            mid = self._mid_price(orderbook)
            capacity = estimate_account_capacity(
                per_leg_notional=plan.per_leg_notional,
                requested_leverage=plan.requested_leverage,
                current_symbol_leverage=snapshot.current_symbol_leverage,
                current_symbol_max_notional_value=snapshot.current_symbol_max_notional_value,
                brackets=list(snapshot.brackets),
                available_balance=snapshot.available_balance,
                equity=snapshot.account_equity,
                maker_fee_rate=snapshot.maker_fee_rate,
                taker_fee_rate=snapshot.taker_fee_rate,
                existing_symbol_exposure=snapshot.existing_symbol_exposure,
                policy=CapacityPolicy(),
                capacity_requested_gross_notional=(account.target_long_qty + account.target_short_qty) * mid,
            )
            config = load_kanglong_symbol_config(self._settings, plan.symbol)
            drift_bps = abs(mid - account.reference_mid_price) / account.reference_mid_price * Decimal("10000")
            if (
                snapshot.blocked_reasons
                or not snapshot.all_components_fresh
                or capacity.blocked
                or drift_bps > Decimal(config.plan_recheck_price_drift_bps)
            ):
                reason = capacity.blocked_reason or "kanglong_plan_reference_price_changed"
                return self._block_for_plan_refresh(stored, account.account_id, reason, lease)
        except Exception as exc:
            if isinstance(exc, (TimeoutError, ConnectionError)) or (
                isinstance(exc, TradingError) and exc.retryable
            ):
                raise
            return self._block_for_plan_refresh(stored, account.account_id, "kanglong_open_precheck_failed", lease)
        return None

    def _block_for_plan_refresh(
        self,
        stored: dict[str, Any],
        account_id: str,
        reason: str,
        lease: KanglongLeaseExpectation,
    ) -> dict[str, Any]:
        run_id = stored["run_id"]
        rows = self._repository.list_kanglong_batch_accounts(run_id)
        completed = sum(row["status"] in {"completed", "completed_with_dust"} for row in rows)
        next_status = (
            KanglongRunStatus.PAUSED_PLAN_RECHECK_CHANGED.value
            if completed
            else KanglongRunStatus.BLOCKED_PLAN_STALE.value
        )
        key = f"worker:{run_id}:precheck-blocked:{account_id}:{stable_payload_hash(reason)[:16]}"
        self._repository.commit_kanglong_action(
            run_id=run_id,
            mutation=KanglongActionMutation(
                expected_statuses=(stored["status"],),
                expected_plan_version=stored["plan_version"],
                expected_action_version=None,
                next_status=next_status,
                available_actions=tuple(available_actions_for_status(next_status)),
                events=(
                    {
                        "event_type": "kanglong_batch_precheck_blocked",
                        "payload": {"account_id": account_id, "reason": reason},
                    },
                ),
            ),
            idempotency_key=key,
            request_hash=stable_payload_hash({"key": key, "reason": reason}),
            response={"run_id": run_id},
            lease_expectation=lease,
        )
        row = self._repository.get_kanglong_batch_account(run_id, account_id)
        if row and row["status"] in {"pending", "prechecking", "blocked_precheck", "retry_wait"}:
            self._repository.update_kanglong_batch_account(
                run_id=run_id,
                account_id=account_id,
                status="blocked_precheck",
                fencing_token=lease.fencing_token,
                expected_status=row["status"],
            )
        return self._repository.get_kanglong_run(run_id)

    async def _execute_first_leg(
        self,
        stored: dict[str, Any],
        plan: KanglongBatchPlan,
        account: KanglongBatchAccountPlan,
        account_row: dict[str, Any],
        lease: KanglongLeaseExpectation,
    ) -> dict[str, Any]:
        first_side, _ = self._leg_order(plan, first=True)
        remaining = self._remaining_target(plan.run_id, account, first_side)
        if remaining <= 0:
            self._repository.update_kanglong_batch_account(
                run_id=plan.run_id,
                account_id=account.account_id,
                status="aligning",
                fencing_token=lease.fencing_token,
            )
            return await self._execute_alignment(stored, plan, account, account_row, lease)
        snapshot = await self._fresh_unique_orderbook(stored, account.account_id, plan.symbol)
        rules = await self._runtime_manager.current(account.account_id).gateway.get_symbol_rules(plan.symbol)
        round_index = self._account_round_index(stored, account.account_id)
        target = self._planned_round_target(remaining, round_index, plan.round_count, rules)
        op_id = operation_id(plan.run_id, account.account_id, round_index, "first", 1)
        result = self._match(plan, account, first_side, target, snapshot, rules)
        if result.filled_qty <= 0:
            return await self._handle_transient_no_fill(stored, account, lease)
        pending = {
            "account_id": account.account_id,
            "round_index": round_index,
            "economic_attempt": 1,
            "operation_id": op_id,
            "position_side": first_side.value,
            "match": self._match_payload(result),
            "snapshot_id": self._snapshot_identity(snapshot),
        }
        progress = dict(stored.get("progress") or {})
        progress["batch_pending_operation"] = pending
        progress.setdefault("accepted_orderbooks", {})[account.account_id] = self._snapshot_identity(snapshot)
        response = self._commit_worker_progress(
            stored,
            progress,
            lease,
            idempotency_suffix=f"pending:{op_id}",
            event_type="kanglong_batch_first_leg_pending",
            event_payload={"account_id": account.account_id, "operation_id": op_id},
        )
        self._repository.update_kanglong_batch_account(
            run_id=plan.run_id,
            account_id=account.account_id,
            status="second_leg",
            fencing_token=lease.fencing_token,
        )
        return response

    async def _execute_second_leg(
        self,
        stored: dict[str, Any],
        plan: KanglongBatchPlan,
        account: KanglongBatchAccountPlan,
        account_row: dict[str, Any],
        pending: dict[str, Any],
        lease: KanglongLeaseExpectation,
    ) -> dict[str, Any]:
        first_side = PositionSide(str(pending["position_side"]))
        second_side = PositionSide.SHORT if first_side == PositionSide.LONG else PositionSide.LONG
        first_match = self._match_from_payload(pending["match"])
        second_remaining = self._remaining_target(plan.run_id, account, second_side)
        snapshot = await self._fresh_unique_orderbook(stored, account.account_id, plan.symbol)
        rules = await self._runtime_manager.current(account.account_id).gateway.get_symbol_rules(plan.symbol)
        round_index = int(pending["round_index"])
        target = min(
            first_match.filled_qty,
            self._planned_round_target(second_remaining, round_index, plan.round_count, rules),
        )
        if target <= 0:
            return await self._commit_pair(stored, plan, account, account_row, pending, None, lease)
        second_match = self._match(plan, account, second_side, target, snapshot, rules)
        if second_match.filled_qty <= 0:
            return await self._handle_transient_no_fill(stored, account, lease)
        return await self._commit_pair(stored, plan, account, account_row, pending, second_match, lease, snapshot)

    async def _execute_alignment(
        self,
        stored: dict[str, Any],
        plan: KanglongBatchPlan,
        account: KanglongBatchAccountPlan,
        account_row: dict[str, Any],
        lease: KanglongLeaseExpectation,
    ) -> dict[str, Any]:
        long_qty = self._repository.sum_kanglong_batch_leg_qty(plan.run_id, account.account_id, "LONG")
        short_qty = self._repository.sum_kanglong_batch_leg_qty(plan.run_id, account.account_id, "SHORT")
        long_remaining = max(account.target_long_qty - long_qty, Decimal("0"))
        short_remaining = max(account.target_short_qty - short_qty, Decimal("0"))
        if long_remaining <= 0 and short_remaining <= 0:
            return self._complete_without_checkpoint(stored, plan, account, account_row, lease, Decimal("0"))
        side = PositionSide.LONG if long_remaining >= short_remaining else PositionSide.SHORT
        target = max(long_remaining, short_remaining)
        snapshot = await self._fresh_unique_orderbook(stored, account.account_id, plan.symbol)
        rules = await self._runtime_manager.current(account.account_id).gateway.get_symbol_rules(plan.symbol)
        result = self._match(plan, account, side, target, snapshot, rules)
        if result.filled_qty <= 0:
            return await self._handle_transient_no_fill(stored, account, lease)
        alignment_index = int(
            ((stored.get("progress") or {}).get("batch_alignment_round_indexes") or {}).get(
                account.account_id,
                0,
            )
        )
        round_index = int(plan.round_count) + alignment_index
        pending = {
            "account_id": account.account_id,
            "round_index": round_index,
            "economic_attempt": 1,
            "operation_id": operation_id(plan.run_id, account.account_id, round_index, "alignment", 1),
            "position_side": side.value,
            "match": self._match_payload(result),
            "snapshot_id": self._snapshot_identity(snapshot),
            "alignment": True,
            "alignment_round_index": alignment_index,
        }
        return await self._commit_pair(stored, plan, account, account_row, pending, None, lease, snapshot)

    async def _commit_pair(
        self,
        stored: dict[str, Any],
        plan: KanglongBatchPlan,
        account: KanglongBatchAccountPlan,
        account_row: dict[str, Any],
        pending: dict[str, Any],
        second_match: MatchResult | None,
        lease: KanglongLeaseExpectation,
        second_snapshot: OrderbookSnapshot | None = None,
    ) -> dict[str, Any]:
        latest = self._repository.latest_kanglong_checkpoint(plan.run_id)
        checkpoint_id = int(latest["checkpoint_id"]) + 1 if latest else 1
        previous_hash = str(latest["ledger_hash"]) if latest else "genesis"
        first_match = self._match_from_payload(pending["match"])
        first_side = PositionSide(str(pending["position_side"]))
        round_index = int(pending["round_index"])
        entries = self._fill_entries(
            plan,
            account,
            first_match,
            first_side,
            str(pending["operation_id"]),
            checkpoint_id,
            round_index,
            str(pending["snapshot_id"]),
            alignment=bool(pending.get("alignment")),
            sequence_start=1,
        )
        if second_match is not None:
            second_side = PositionSide.SHORT if first_side == PositionSide.LONG else PositionSide.LONG
            second_id = operation_id(plan.run_id, account.account_id, round_index, "second", int(pending.get("economic_attempt", 1)))
            entries.extend(
                self._fill_entries(
                    plan,
                    account,
                    second_match,
                    second_side,
                    second_id,
                    checkpoint_id,
                    round_index,
                    self._snapshot_identity(second_snapshot),
                    alignment=False,
                    sequence_start=len(entries) + 1,
                )
            )
        existing_long = self._repository.sum_kanglong_batch_leg_qty(plan.run_id, account.account_id, "LONG")
        existing_short = self._repository.sum_kanglong_batch_leg_qty(plan.run_id, account.account_id, "SHORT")
        added_long = sum((abs(item.qty_delta) for item in entries if item.entry_type in {"open_position", "close_position"} and item.payload.get("position_side") == "LONG"), Decimal("0"))
        added_short = sum((abs(item.qty_delta) for item in entries if item.entry_type in {"open_position", "close_position"} and item.payload.get("position_side") == "SHORT"), Decimal("0"))
        total_long = existing_long + added_long
        total_short = existing_short + added_short
        rules = await self._runtime_manager.current(account.account_id).gateway.get_symbol_rules(plan.symbol)
        reference = max(account.reference_mid_price, Decimal("0.000000001"))
        targets_reached = total_long >= account.target_long_qty and total_short >= account.target_short_qty
        raw_gap = abs(total_long - total_short)
        gap_action = self.classify_gap(raw_gap, reference, rules)
        if targets_reached and (plan.operation == "close" or gap_action in {"balanced", "dust"}):
            account_status = "completed_with_dust" if gap_action == "dust" else "completed"
        elif gap_action == "align" or bool(pending.get("alignment")):
            account_status = "aligning"
        else:
            account_status = "first_leg"
        progress = dict(stored.get("progress") or {})
        progress.pop("batch_pending_operation", None)
        progress.pop("next_wakeup_at", None)
        progress["transport_retry_count"] = 0
        if pending.get("alignment"):
            alignment_indexes = dict(progress.get("batch_alignment_round_indexes") or {})
            alignment_indexes[account.account_id] = int(pending.get("alignment_round_index", 0)) + 1
            progress["batch_alignment_round_indexes"] = alignment_indexes
        else:
            round_indexes = dict(progress.get("batch_round_indexes") or {})
            round_indexes[account.account_id] = round_index + 1
            progress["batch_round_indexes"] = round_indexes
            progress["batch_round_index"] = round_index + 1
            if account_status == "first_leg" and plan.round_interval_seconds > 0:
                progress["next_wakeup_at"] = (
                    datetime.now(UTC) + timedelta(seconds=plan.round_interval_seconds)
                ).isoformat()
        if second_snapshot is not None:
            progress.setdefault("accepted_orderbooks", {})[account.account_id] = self._snapshot_identity(second_snapshot)
        all_rows = self._repository.list_kanglong_batch_accounts(plan.run_id)
        is_last = int(account_row["sequence"]) == len(all_rows) - 1
        run_status = (
            KanglongRunStatus.COMPLETED_WITH_DUST_RESIDUAL.value
            if is_last and account_status == "completed_with_dust"
            else KanglongRunStatus.COMPLETED.value
            if is_last and account_status == "completed"
            else KanglongRunStatus.RUNNING.value
        )
        if stored["status"] in {
            KanglongRunStatus.PAUSE_PENDING.value,
            KanglongRunStatus.STOP_PENDING.value,
        }:
            run_status = stored["status"]
        baselines = [
            self._baseline_object(item)
            for item in self._repository.list_kanglong_ledger_baselines(plan.run_id)
        ]
        previous_entries = [
            self._ledger_object(item)
            for item in self._repository.list_kanglong_ledger_entries(plan.run_id)
        ]
        ledger_hash = hash_checkpoint(previous_hash, entries)
        state_hash = hash_ledger_state(baselines, [*previous_entries, *entries])
        stored_baselines = self._repository.list_kanglong_ledger_baselines(plan.run_id)
        stored_entries = self._repository.list_kanglong_ledger_entries(plan.run_id)
        candidate_entries = [entry.to_storage_payload() for entry in entries]
        candidate_checkpoint = {
            "checkpoint_id": checkpoint_id,
            "previous_ledger_hash": previous_hash,
            "ledger_hash": ledger_hash,
            "ledger_state_hash": state_hash,
        }
        report = build_ledger_report(
            [*stored_entries, *candidate_entries],
            baselines=stored_baselines,
            latest_checkpoint=candidate_checkpoint,
            summary_status=run_status,
            symbol=plan.symbol,
        )
        self._repository.commit_kanglong_checkpoint(
            run_id=plan.run_id,
            checkpoint_id=checkpoint_id,
            expected_previous_checkpoint_id=checkpoint_id - 1,
            expected_previous_ledger_hash=previous_hash,
            previous_ledger_hash=previous_hash,
            ledger_hash=ledger_hash,
            ledger_state_hash=state_hash,
            ledger_entries=entries,
            events=[
                {
                    "event_type": "kanglong_batch_checkpoint",
                    "group_id": account.account_id,
                    "round_id": str(round_index),
                    "payload": {
                        "account_id": account.account_id,
                        "account_status": account_status,
                        "raw_gap": raw_gap,
                    },
                }
            ],
            status=run_status,
            available_actions=available_actions_for_status(run_status),
            progress=progress,
            report=report,
            report_summary=report.get("report_summary") or {},
            is_safe=gap_action in {"balanced", "dust"},
            lease_expectation=lease,
            batch_account_transition={
                "account_id": account.account_id,
                "status": account_status,
                "expected_status": account_row["status"],
            },
        )
        return self._repository.get_kanglong_run(plan.run_id)

    def _complete_without_checkpoint(
        self,
        stored: dict[str, Any],
        plan: KanglongBatchPlan,
        account: KanglongBatchAccountPlan,
        account_row: dict[str, Any],
        lease: KanglongLeaseExpectation,
        dust: Decimal,
    ) -> dict[str, Any]:
        account_status = "completed_with_dust" if dust else "completed"
        self._repository.update_kanglong_batch_account(
            run_id=plan.run_id,
            account_id=account.account_id,
            status=account_status,
            fencing_token=lease.fencing_token,
            expected_status=account_row["status"],
        )
        return self._repository.get_kanglong_run(plan.run_id)

    async def _handle_transient_no_fill(
        self,
        stored: dict[str, Any],
        account: KanglongBatchAccountPlan,
        lease: KanglongLeaseExpectation,
        retry_after_seconds: Decimal | None = None,
        http_status: int | None = None,
    ) -> dict[str, Any]:
        progress = dict(stored.get("progress") or {})
        attempts = int(progress.get("transport_retry_count", 0)) + 1
        progress["transport_retry_count"] = attempts
        if attempts >= self._retry_policy.max_attempts:
            checkpoint = self._repository.latest_kanglong_checkpoint(stored["run_id"])
            next_status = (
                KanglongRunStatus.PAUSED_MARKET_UNSTABLE.value
                if checkpoint is None or bool(checkpoint.get("is_safe"))
                else KanglongRunStatus.NEEDS_ABORT_RECOVER.value
            )
            return self._worker_action(
                stored,
                next_status=next_status,
                event_type="kanglong_batch_retry_exhausted",
                lease=lease,
                idempotency_suffix=f"retry-exhausted:{account.account_id}:{attempts}",
                progress=progress,
            )
        decision = self._retry_policy.decide(
            attempts,
            retry_after_seconds=retry_after_seconds,
            http_status=http_status,
        )
        if decision.persist_retry_wait:
            progress["next_wakeup_at"] = decision.next_wakeup_at.isoformat()
            self._repository.update_kanglong_batch_account(
                run_id=stored["run_id"],
                account_id=account.account_id,
                status="retry_wait",
                fencing_token=lease.fencing_token,
            )
        elif decision.delay_seconds > 0:
            await self._sleep(float(decision.delay_seconds))
        return self._commit_worker_progress(
            stored,
            progress,
            lease,
            idempotency_suffix=f"retry:{account.account_id}:{attempts}",
            event_type="kanglong_batch_transport_retry",
            event_payload={"account_id": account.account_id, "attempt": attempts},
        )

    async def _fresh_orderbook(self, account_id: str, symbol: str) -> OrderbookSnapshot:
        gateway = self._runtime_manager.current(account_id).gateway
        raw = await gateway.refresh_order_book(symbol, limit=20)
        return OrderbookSnapshot.from_mapping(raw, source="gateway")

    async def _fresh_unique_orderbook(
        self,
        stored: dict[str, Any],
        account_id: str,
        symbol: str,
    ) -> OrderbookSnapshot:
        snapshot = await self._fresh_orderbook(account_id, symbol)
        previous = ((stored.get("progress") or {}).get("accepted_orderbooks") or {}).get(account_id)
        current = self._snapshot_identity(snapshot)
        if previous:
            previous_kind, previous_value = str(previous).split(":", 1)
            current_kind, current_value = current.split(":", 1)
            if current_kind == previous_kind == "update" and int(current_value) <= int(previous_value):
                raise DuplicateOrderbookSnapshot(current)
            if current == previous:
                raise DuplicateOrderbookSnapshot(current)
        return snapshot

    @staticmethod
    def _snapshot_identity(snapshot: OrderbookSnapshot | None) -> str:
        if snapshot is None:
            return "none:0"
        if snapshot.update_id is not None:
            return f"update:{snapshot.update_id}"
        payload = snapshot.to_mapping()
        payload.pop("event_time", None)
        digest = stable_payload_hash(payload)[:24]
        return f"event:{int(snapshot.event_time.timestamp() * 1000)}:{digest}"

    @staticmethod
    def _mid_price(snapshot: OrderbookSnapshot) -> Decimal:
        if not snapshot.bids or not snapshot.asks:
            raise ValueError("kanglong_orderbook_insufficient_depth")
        return (snapshot.bids[0].price + snapshot.asks[0].price) / Decimal("2")

    @staticmethod
    def _leg_order(plan: KanglongBatchPlan, *, first: bool) -> tuple[PositionSide, OrderSide]:
        side = plan.preferred_side if first else (
            PositionSide.SHORT if plan.preferred_side == PositionSide.LONG else PositionSide.LONG
        )
        if plan.operation == "open":
            order_side = OrderSide.BUY if side == PositionSide.LONG else OrderSide.SELL
        else:
            order_side = OrderSide.SELL if side == PositionSide.LONG else OrderSide.BUY
        return side, order_side

    @staticmethod
    def _order_side(plan: KanglongBatchPlan, side: PositionSide) -> OrderSide:
        if plan.operation == "open":
            return OrderSide.BUY if side == PositionSide.LONG else OrderSide.SELL
        return OrderSide.SELL if side == PositionSide.LONG else OrderSide.BUY

    @staticmethod
    def _account_round_index(stored: dict[str, Any], account_id: str) -> int:
        progress = stored.get("progress") or {}
        indexes = progress.get("batch_round_indexes") or {}
        return int(indexes.get(account_id, 0))

    @staticmethod
    def _planned_round_target(
        remaining: Decimal,
        round_index: int,
        round_count: int,
        rules: SymbolRules,
    ) -> Decimal:
        if remaining <= 0:
            return Decimal("0")
        remaining_steps = int(remaining / rules.step_size)
        if remaining_steps <= 0:
            return remaining
        rounds_left = max(int(round_count) - int(round_index), 1)
        step_count = max((remaining_steps + rounds_left - 1) // rounds_left, 1)
        return min(remaining, Decimal(step_count) * rules.step_size)

    def _match(
        self,
        plan: KanglongBatchPlan,
        account: KanglongBatchAccountPlan,
        side: PositionSide,
        target: Decimal,
        snapshot: OrderbookSnapshot,
        rules: SymbolRules,
    ) -> MatchResult:
        matcher = OrderbookMatcher(
            maker_fee_rate=account.maker_fee_rate,
            taker_fee_rate=account.taker_fee_rate,
        )
        return matcher.match_orderbook_snapshot(
            snapshot,
            order_side=self._order_side(plan, side),
            position_side=side,
            target_qty=normalize_qty(target, rules),
            rules=rules,
            liquidity="taker",
        )

    def _remaining_target(
        self,
        run_id: str,
        account: KanglongBatchAccountPlan,
        side: PositionSide,
    ) -> Decimal:
        filled = self._repository.sum_kanglong_batch_leg_qty(run_id, account.account_id, side.value)
        target = account.target_long_qty if side == PositionSide.LONG else account.target_short_qty
        return max(target - filled, Decimal("0"))

    def _fill_entries(
        self,
        plan: KanglongBatchPlan,
        account: KanglongBatchAccountPlan,
        match: MatchResult,
        side: PositionSide,
        op_id: str,
        checkpoint_id: int,
        round_index: int,
        snapshot_id: str,
        *,
        alignment: bool,
        sequence_start: int,
    ) -> list[KanglongLedgerEntry]:
        entry_type = "open_position" if plan.operation == "open" else "close_position"
        qty_delta = match.filled_qty if plan.operation == "open" else -match.filled_qty
        base_payload = {
            "operation": plan.operation,
            "position_side": side.value,
            "order_side": match.side.value,
            "round_index": round_index,
            "snapshot_id": snapshot_id,
            "avg_price": match.avg_price,
            "liquidity": match.liquidity,
            "source_open_run_id": plan.source_open_run_id,
        }
        adverse, improvement = self._wear(match, account.reference_mid_price)
        wear_categories = {
            "spread_cost": Decimal("0") if alignment else adverse,
            "market_impact_cost": Decimal("0"),
            "timing_drift_cost": Decimal("0"),
            "alignment_cost": adverse if alignment else Decimal("0"),
        }
        entries = [
            KanglongLedgerEntry(
                run_id=plan.run_id,
                checkpoint_id=checkpoint_id,
                sequence=sequence_start,
                operation_id=op_id,
                account_id=account.account_id,
                entry_type=entry_type,
                qty_delta=qty_delta,
                payload={**base_payload, "price_improvement": improvement},
            ),
            KanglongLedgerEntry(
                run_id=plan.run_id,
                checkpoint_id=checkpoint_id,
                sequence=sequence_start + 1,
                operation_id=f"{op_id}:fee",
                account_id=account.account_id,
                entry_type="fee",
                amount=match.fee,
                fee_amount=match.fee,
                fee_asset=self._quote_asset(plan.symbol),
                payload=base_payload,
            ),
        ]
        for offset, (category, amount) in enumerate(wear_categories.items(), start=2):
            entries.append(
                KanglongLedgerEntry(
                    run_id=plan.run_id,
                    checkpoint_id=checkpoint_id,
                    sequence=sequence_start + offset,
                    operation_id=f"{op_id}:wear:{category}",
                    account_id=account.account_id,
                    entry_type="price_wear",
                    amount=amount,
                    price_wear=amount,
                    payload={**base_payload, "wear_category": category, "adverse": amount, "improvement": improvement},
                )
            )
        return entries

    @staticmethod
    def _wear(match: MatchResult, reference_mid: Decimal) -> tuple[Decimal, Decimal]:
        if match.side == OrderSide.BUY:
            cost = (match.avg_price - reference_mid) * match.filled_qty
        else:
            cost = (reference_mid - match.avg_price) * match.filled_qty
        return max(cost, Decimal("0")), max(-cost, Decimal("0"))

    @staticmethod
    def _quote_asset(symbol: str) -> str:
        for suffix in ("USDC", "USDT", "FDUSD", "BUSD", "USD"):
            if symbol.upper().endswith(suffix):
                return suffix
        return "USDC"

    @staticmethod
    def _match_payload(match: MatchResult) -> dict[str, Any]:
        payload = asdict(match)
        payload["side"] = match.side.value
        payload["position_side"] = match.position_side.value
        return payload

    @staticmethod
    def _match_from_payload(payload: dict[str, Any]) -> MatchResult:
        values = dict(payload)
        for name in (
            "requested_qty", "filled_qty", "avg_price", "notional", "fee",
            "residual_qty", "slippage_bps", "wait_seconds_consumed",
        ):
            values[name] = Decimal(str(values.get(name) or "0"))
        values["side"] = OrderSide(str(values["side"]))
        values["position_side"] = PositionSide(str(values["position_side"]))
        return MatchResult(**values)

    def _commit_worker_progress(
        self,
        stored: dict[str, Any],
        progress: dict[str, Any],
        lease: KanglongLeaseExpectation,
        *,
        idempotency_suffix: str,
        event_type: str,
        event_payload: dict[str, Any],
    ) -> dict[str, Any]:
        key = f"worker:{stored['run_id']}:{idempotency_suffix}"
        response = self._repository.commit_kanglong_action(
            run_id=stored["run_id"],
            mutation=KanglongActionMutation(
                expected_statuses=(stored["status"],),
                expected_plan_version=stored["plan_version"],
                expected_action_version=None,
                next_status=stored["status"],
                available_actions=tuple(stored.get("available_actions") or available_actions_for_status(stored["status"])),
                progress=progress,
                events=({"event_type": event_type, "payload": event_payload},),
            ),
            idempotency_key=key,
            request_hash=stable_payload_hash({"key": key, "progress": progress}),
            response={"run_id": stored["run_id"]},
            lease_expectation=lease,
        )
        return {**self._repository.get_kanglong_run(stored["run_id"]), **response}

    def _worker_action(
        self,
        stored: dict[str, Any],
        *,
        next_status: str,
        event_type: str,
        lease: KanglongLeaseExpectation,
        idempotency_suffix: str,
        progress: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        key = f"worker:{stored['run_id']}:{idempotency_suffix}"
        response = self._repository.commit_kanglong_action(
            run_id=stored["run_id"],
            mutation=KanglongActionMutation(
                expected_statuses=(stored["status"],),
                expected_plan_version=stored["plan_version"],
                expected_action_version=None,
                next_status=next_status,
                available_actions=tuple(available_actions_for_status(next_status)),
                progress=progress,
                events=({"event_type": event_type, "payload": {}},),
            ),
            idempotency_key=key,
            request_hash=stable_payload_hash({"key": key, "next_status": next_status, "progress": progress}),
            response={"run_id": stored["run_id"]},
            lease_expectation=lease,
        )
        return {**self._repository.get_kanglong_run(stored["run_id"]), **response}

    def _completion_status(self, run_id: str) -> str:
        rows = self._repository.list_kanglong_batch_accounts(run_id)
        if any(row["status"] == "completed_with_dust" for row in rows):
            return KanglongRunStatus.COMPLETED_WITH_DUST_RESIDUAL.value
        return KanglongRunStatus.COMPLETED.value

    @staticmethod
    def _baseline_object(payload: dict[str, Any]):
        from paired_opener.kanglong.ledger import baseline_from_storage_payload

        return baseline_from_storage_payload(payload)

    @staticmethod
    def _ledger_object(payload: dict[str, Any]):
        from paired_opener.kanglong.ledger import ledger_entry_from_storage_payload

        return ledger_entry_from_storage_payload(payload)
