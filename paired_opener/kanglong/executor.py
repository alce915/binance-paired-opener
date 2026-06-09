from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Callable

from paired_opener.domain import OrderSide, PositionSide, SymbolRules
from paired_opener.kanglong.ledger import KanglongLedgerEntry, hash_operation_payload
from paired_opener.kanglong.models import available_actions_for_status
from paired_opener.rounding import normalize_qty
from paired_opener.simulation_matching import MarketDataProvider, MarketDataStaleError, MatchResult, OrderbookMatcher


GENESIS_LEDGER_HASH = "sha256:genesis"


@dataclass(frozen=True, slots=True)
class KanglongTransferExecutorConfig:
    round_interval_seconds: int = 3
    max_supplemental_rounds_per_group: int = 50
    max_supplemental_rounds_per_run: int = 300
    max_consecutive_unfilled: int = 5
    max_events_per_run: int = 20000


class KanglongTransferExecutor:
    def __init__(
        self,
        *,
        repository: Any,
        market_data: MarketDataProvider,
        matcher: OrderbookMatcher,
        rules: SymbolRules,
        clock: Callable[[], datetime] | None = None,
        config: KanglongTransferExecutorConfig | None = None,
        fee_policy_snapshot: dict[str, Any] | None = None,
    ) -> None:
        self._repository = repository
        self._market_data = market_data
        self._matcher = matcher
        self._rules = rules
        self._clock = clock or (lambda: datetime.now(UTC))
        self._config = config or KanglongTransferExecutorConfig()
        self._fee_policy_snapshot = dict(fee_policy_snapshot or {})

    async def run_next(self, run_id: str) -> dict[str, Any]:
        run = self._repository.get_kanglong_run(run_id)
        if run is None:
            raise ValueError("kanglong_run_not_found")
        if run["status"] == "stop_pending":
            return self._stop_without_market_orders(run)
        if self._repository.latest_kanglong_event_id(run_id) >= self._config.max_events_per_run:
            return self._pause_market_unstable(
                run,
                group=None,
                round_id=None,
                progress=dict(run.get("progress") or {}),
                event_type="kanglong_event_limit_reached",
                warning_code="kanglong_event_limit_reached",
                payload={"message_key": "events.kanglong.event_limit_reached"},
            )

        plan = run.get("plan") or {}
        progress = dict(run.get("progress") or {})
        group_index = int(progress.get("group_index", 0))
        groups = list(plan.get("groups") or [])
        if group_index >= len(groups):
            self._repository.update_kanglong_run(
                run_id,
                status="completed",
                available_actions=available_actions_for_status("completed"),
                progress=progress,
                report_summary={"summary_status": "completed"},
            )
            return {"run_id": run_id, "status": "completed", "matched_qty": "0"}

        group = groups[group_index]
        group_id = str(group["group_id"])
        side = _position_side(group.get("side") or plan.get("direction") or "LONG")
        round_index = int(progress.get("round_index", 0))
        residual_qty = Decimal(str(progress.get("residual_qty", "0")))
        round_qtys = [Decimal(str(qty)) for qty in group.get("round_qtys") or []]
        if residual_qty > Decimal("0"):
            target_qty = residual_qty
            round_attempt = int(progress.get("round_attempt", 1)) + 1
            is_supplemental = True
        else:
            if round_index >= len(round_qtys):
                next_progress = {**progress, "group_index": group_index + 1, "round_index": 0}
                self._repository.update_kanglong_run(
                    run_id,
                    status="completed" if group_index + 1 >= len(groups) else "running",
                    available_actions=available_actions_for_status("completed" if group_index + 1 >= len(groups) else "running"),
                    progress=next_progress,
                    report_summary={"summary_status": "completed" if group_index + 1 >= len(groups) else "running"},
                )
                return {
                    "run_id": run_id,
                    "status": "completed" if group_index + 1 >= len(groups) else "running",
                    "matched_qty": "0",
                }
            target_qty = round_qtys[round_index]
            round_attempt = int(progress.get("round_attempt", 1))
            is_supplemental = False
        target_qty = normalize_qty(target_qty, self._rules)
        round_id = _round_id(group_id, round_index, is_supplemental, round_attempt)
        operation_id = _operation_id(run_id, group_id, round_id, round_attempt)
        operation_payload = {
            "run_id": run_id,
            "group_id": group_id,
            "round_id": round_id,
            "round_attempt": round_attempt,
            "from_account_id": group.get("from_account_id"),
            "to_account_id": group.get("to_account_id"),
            "symbol": group.get("symbol") or plan.get("symbol") or self._rules.symbol,
            "side": side.value,
            "target_qty": str(target_qty),
            "fee_policy_snapshot": self._fee_policy_snapshot,
        }
        operation_payload_hash = hash_operation_payload(operation_payload)
        existing_entries = [
            entry
            for entry in self._repository.list_kanglong_ledger_entries(run_id)
            if entry.get("operation_id") == operation_id
        ]
        if existing_entries:
            existing_hashes = {entry.get("operation_payload_hash") for entry in existing_entries}
            if existing_hashes == {operation_payload_hash}:
                return {"run_id": run_id, "status": run["status"], "checkpoint_id": progress.get("checkpoint_id"), "idempotent": True}
            self._repository.update_kanglong_run_and_events(
                run_id,
                status="needs_abort_recover",
                available_actions=available_actions_for_status("needs_abort_recover"),
                progress=progress,
                events=[
                    {
                        "event_type": "kanglong_operation_payload_mismatch",
                        "group_id": group_id,
                        "round_id": round_id,
                        "payload": {
                            "message_key": "events.kanglong.operation_payload_mismatch",
                            "operation_id": operation_id,
                        },
                    }
                ],
            )
            return {"run_id": run_id, "status": "needs_abort_recover", "matched_qty": "0"}

        close_side, open_side = _transfer_order_sides(side)
        try:
            close_snapshot = await self._market_data.get_orderbook(str(operation_payload["symbol"]))
        except MarketDataStaleError:
            return self._pause_market_unstable(
                run,
                group=group,
                round_id=round_id,
                progress=progress,
                event_type="kanglong_market_data_stale",
                warning_code="kanglong_market_data_stale",
                payload={
                    "message_key": "events.kanglong.market_data_stale",
                    "operation_id": operation_id,
                },
            )
        close_match = self._matcher.match_orderbook_snapshot(
            close_snapshot,
            order_side=close_side,
            position_side=side,
            target_qty=target_qty,
            rules=self._rules,
            liquidity="taker",
        )
        if close_match.filled_qty <= Decimal("0"):
            return self._commit_zero_fill(run, group, round_id, operation_id, operation_payload_hash, progress, target_qty)

        try:
            open_snapshot = await self._market_data.get_orderbook(str(operation_payload["symbol"]))
        except MarketDataStaleError:
            return self._pause_market_unstable(
                run,
                group=group,
                round_id=round_id,
                progress=progress,
                event_type="kanglong_market_data_stale",
                warning_code="kanglong_market_data_stale",
                payload={
                    "message_key": "events.kanglong.market_data_stale",
                    "operation_id": operation_id,
                },
            )
        open_match = self._matcher.match_orderbook_snapshot(
            open_snapshot,
            order_side=open_side,
            position_side=side,
            target_qty=target_qty,
            rules=self._rules,
            liquidity="taker",
        )
        aligned_qty = normalize_qty(min(close_match.filled_qty, open_match.filled_qty), self._rules)
        if aligned_qty <= Decimal("0"):
            return self._commit_zero_fill(run, group, round_id, operation_id, operation_payload_hash, progress, target_qty)

        checkpoint = self._latest_checkpoint(run["run_id"])
        next_checkpoint_id = checkpoint["checkpoint_id"] + 1
        residual_after = normalize_qty(max(target_qty - aligned_qty, Decimal("0")), self._rules)
        next_progress = self._next_progress(
            progress,
            group=group,
            group_index=group_index,
            round_index=round_index,
            checkpoint_id=next_checkpoint_id,
            matched_qty=aligned_qty,
            residual_qty=residual_after,
            is_supplemental=is_supplemental,
        )
        pause_after_round = run["status"] == "pause_pending"
        final_status = self._status_after_round(
            group_index=group_index,
            group_count=len(groups),
            round_index=round_index,
            round_count=len(round_qtys),
            residual_qty=residual_after,
            pause_after_round=pause_after_round,
        )
        available_actions = available_actions_for_status(final_status)
        entries = self._ledger_entries(
            run_id=run_id,
            checkpoint_id=next_checkpoint_id,
            operation_id=operation_id,
            operation_payload_hash=operation_payload_hash,
            group=group,
            side=side,
            aligned_qty=aligned_qty,
            close_match=close_match,
            open_match=open_match,
        )
        events = [
            {
                "event_type": "kanglong_trade_executed",
                "group_id": group["group_id"],
                "round_id": round_id,
                "payload": {
                    "message_key": "events.kanglong.trade_executed",
                    "operation_id": operation_id,
                    "leg": "close",
                    "account_id": group.get("from_account_id"),
                    "filled_qty": _decimal_text(aligned_qty),
                    "avg_price": _decimal_text(close_match.avg_price),
                },
            },
            {
                "event_type": "kanglong_trade_executed",
                "group_id": group["group_id"],
                "round_id": round_id,
                "payload": {
                    "message_key": "events.kanglong.trade_executed",
                    "operation_id": operation_id,
                    "leg": "open",
                    "account_id": group.get("to_account_id"),
                    "filled_qty": _decimal_text(aligned_qty),
                    "avg_price": _decimal_text(open_match.avg_price),
                },
            },
            {
                "event_type": "kanglong_round_completed",
                "group_id": group["group_id"],
                "round_id": round_id,
                "payload": {
                    "message_key": "events.kanglong.round_completed",
                    "operation_id": operation_id,
                    "matched_qty": _decimal_text(aligned_qty),
                    "residual_qty": _decimal_text(residual_after),
                },
            },
        ]
        committed = self._repository.commit_kanglong_checkpoint(
            run_id=run_id,
            checkpoint_id=next_checkpoint_id,
            expected_previous_checkpoint_id=checkpoint["checkpoint_id"],
            expected_previous_ledger_hash=checkpoint["ledger_hash"],
            previous_ledger_hash=checkpoint["ledger_hash"],
            ledger_hash="recomputed",
            ledger_state_hash="recomputed",
            ledger_entries=entries,
            events=events,
            status=final_status,
            available_actions=available_actions,
            progress=next_progress,
            report_summary={"summary_status": final_status, "matched_qty": _decimal_text(aligned_qty)},
        )
        return {
            "run_id": run_id,
            "status": final_status,
            "checkpoint_id": committed["checkpoint_id"],
            "matched_qty": _decimal_text(aligned_qty),
            "residual_qty": _decimal_text(residual_after),
        }

    def _commit_zero_fill(
        self,
        run: dict[str, Any],
        group: dict[str, Any],
        round_id: str,
        operation_id: str,
        operation_payload_hash: str,
        progress: dict[str, Any],
        target_qty: Decimal,
    ) -> dict[str, Any]:
        checkpoint = self._latest_checkpoint(run["run_id"])
        next_checkpoint_id = checkpoint["checkpoint_id"] + 1
        consecutive = int(progress.get("consecutive_unfilled", 0)) + 1
        final_status = "paused_market_unstable" if consecutive >= self._config.max_consecutive_unfilled else "running"
        next_progress = {
            **progress,
            "checkpoint_id": next_checkpoint_id,
            "current_group_id": group.get("group_id"),
            "consecutive_unfilled": consecutive,
            "scheduled_reason": "market_unstable" if final_status == "paused_market_unstable" else "round_interval",
            "next_wake_at": (self._clock() + timedelta(seconds=self._config.round_interval_seconds)).isoformat(),
        }
        committed = self._repository.commit_kanglong_checkpoint(
            run_id=run["run_id"],
            checkpoint_id=next_checkpoint_id,
            expected_previous_checkpoint_id=checkpoint["checkpoint_id"],
            expected_previous_ledger_hash=checkpoint["ledger_hash"],
            previous_ledger_hash=checkpoint["ledger_hash"],
            ledger_hash="recomputed",
            ledger_state_hash="recomputed",
            ledger_entries=[],
            events=[
                {
                    "event_type": "kanglong_round_zero_fill",
                    "group_id": group.get("group_id"),
                    "round_id": round_id,
                    "payload": {
                        "message_key": "events.kanglong.round_zero_fill",
                        "operation_id": operation_id,
                        "operation_payload_hash": operation_payload_hash,
                        "target_qty": _decimal_text(target_qty),
                        "consecutive_unfilled": consecutive,
                    },
                }
            ],
            status=final_status,
            available_actions=available_actions_for_status(final_status),
            progress=next_progress,
            report_summary={"summary_status": final_status},
        )
        return {
            "run_id": run["run_id"],
            "status": final_status,
            "checkpoint_id": committed["checkpoint_id"],
            "matched_qty": "0",
        }

    def _stop_without_market_orders(self, run: dict[str, Any]) -> dict[str, Any]:
        checkpoint = self._latest_checkpoint(run["run_id"])
        next_checkpoint_id = checkpoint["checkpoint_id"] + 1
        progress = {
            **(run.get("progress") or {}),
            "checkpoint_id": next_checkpoint_id,
            "control_request": {"action": "stop", "consumed_at": self._clock().isoformat()},
        }
        entry = KanglongLedgerEntry(
            run_id=run["run_id"],
            checkpoint_id=next_checkpoint_id,
            sequence=1,
            operation_id=f"{run['run_id']}:control:stop:{next_checkpoint_id}",
            account_id=None,
            entry_type="control",
            operation_payload_hash=hash_operation_payload({"action": "stop", "checkpoint_id": next_checkpoint_id}),
            payload={"action": "stop"},
        )
        committed = self._repository.commit_kanglong_checkpoint(
            run_id=run["run_id"],
            checkpoint_id=next_checkpoint_id,
            expected_previous_checkpoint_id=checkpoint["checkpoint_id"],
            expected_previous_ledger_hash=checkpoint["ledger_hash"],
            previous_ledger_hash=checkpoint["ledger_hash"],
            ledger_hash="recomputed",
            ledger_state_hash="recomputed",
            ledger_entries=[entry],
            events=[
                {
                    "event_type": "kanglong_run_stopped",
                    "payload": {
                        "message_key": "events.kanglong.run_stopped",
                        "checkpoint_id": next_checkpoint_id,
                    },
                }
            ],
            status="stopped_by_user",
            available_actions=available_actions_for_status("stopped_by_user"),
            progress=progress,
            report_summary={"summary_status": "stopped_by_user"},
        )
        return {
            "run_id": run["run_id"],
            "status": "stopped_by_user",
            "checkpoint_id": committed["checkpoint_id"],
            "matched_qty": "0",
        }

    def _pause_market_unstable(
        self,
        run: dict[str, Any],
        *,
        group: dict[str, Any] | None,
        round_id: str | None,
        progress: dict[str, Any],
        event_type: str,
        warning_code: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        checkpoint = self._latest_checkpoint(run["run_id"])
        next_checkpoint_id = checkpoint["checkpoint_id"] + 1
        next_progress = {
            **progress,
            "checkpoint_id": next_checkpoint_id,
            "current_group_id": group.get("group_id") if group else progress.get("current_group_id"),
            "scheduled_reason": warning_code,
            "next_wake_at": (self._clock() + timedelta(seconds=self._config.round_interval_seconds)).isoformat(),
        }
        committed = self._repository.commit_kanglong_checkpoint(
            run_id=run["run_id"],
            checkpoint_id=next_checkpoint_id,
            expected_previous_checkpoint_id=checkpoint["checkpoint_id"],
            expected_previous_ledger_hash=checkpoint["ledger_hash"],
            previous_ledger_hash=checkpoint["ledger_hash"],
            ledger_hash="recomputed",
            ledger_state_hash="recomputed",
            ledger_entries=[],
            events=[
                {
                    "event_type": event_type,
                    "group_id": group.get("group_id") if group else None,
                    "round_id": round_id,
                    "payload": {**payload, "warning_code": warning_code},
                }
            ],
            status="paused_market_unstable",
            available_actions=available_actions_for_status("paused_market_unstable"),
            progress=next_progress,
            report_summary={"summary_status": "paused_market_unstable", "warning_code": warning_code},
        )
        return {
            "run_id": run["run_id"],
            "status": "paused_market_unstable",
            "checkpoint_id": committed["checkpoint_id"],
            "matched_qty": "0",
        }

    def _ledger_entries(
        self,
        *,
        run_id: str,
        checkpoint_id: int,
        operation_id: str,
        operation_payload_hash: str,
        group: dict[str, Any],
        side: PositionSide,
        aligned_qty: Decimal,
        close_match: MatchResult,
        open_match: MatchResult,
    ) -> list[KanglongLedgerEntry]:
        close_fee = _scaled_fee(close_match, aligned_qty)
        open_fee = _scaled_fee(open_match, aligned_qty)
        close_notional = close_match.avg_price * aligned_qty
        open_notional = open_match.avg_price * aligned_qty
        price_wear = _price_wear(side, close_match.avg_price, open_match.avg_price, aligned_qty)
        entries = [
            KanglongLedgerEntry(
                run_id=run_id,
                checkpoint_id=checkpoint_id,
                sequence=1,
                operation_id=operation_id,
                account_id=str(group.get("from_account_id")),
                entry_type="close_position",
                asset=side.value,
                qty_delta=-aligned_qty,
                realized_pnl_delta=Decimal("0"),
                operation_payload_hash=operation_payload_hash,
                payload={"leg": "close", "notional": str(close_notional)},
            ),
            KanglongLedgerEntry(
                run_id=run_id,
                checkpoint_id=checkpoint_id,
                sequence=2,
                operation_id=operation_id,
                account_id=str(group.get("to_account_id")),
                entry_type="open_position",
                asset=side.value,
                qty_delta=aligned_qty,
                margin_delta=open_notional / Decimal("75"),
                operation_payload_hash=operation_payload_hash,
                payload={"leg": "open", "notional": str(open_notional)},
            ),
            KanglongLedgerEntry(
                run_id=run_id,
                checkpoint_id=checkpoint_id,
                sequence=3,
                operation_id=operation_id,
                account_id=str(group.get("from_account_id")),
                entry_type="fee",
                asset="USDC",
                amount=-close_fee,
                available_delta=-close_fee,
                equity_delta=-close_fee,
                fee_amount=close_fee,
                fee_asset="USDC",
                operation_payload_hash=operation_payload_hash,
                payload={"leg": "close"},
            ),
            KanglongLedgerEntry(
                run_id=run_id,
                checkpoint_id=checkpoint_id,
                sequence=4,
                operation_id=operation_id,
                account_id=str(group.get("to_account_id")),
                entry_type="fee",
                asset="USDC",
                amount=-open_fee,
                available_delta=-open_fee,
                equity_delta=-open_fee,
                fee_amount=open_fee,
                fee_asset="USDC",
                operation_payload_hash=operation_payload_hash,
                payload={"leg": "open"},
            ),
        ]
        if price_wear > Decimal("0"):
            entries.append(
                KanglongLedgerEntry(
                    run_id=run_id,
                    checkpoint_id=checkpoint_id,
                    sequence=5,
                    operation_id=operation_id,
                    account_id=str(group.get("to_account_id")),
                    entry_type="price_wear",
                    asset="USDC",
                    amount=-price_wear,
                    available_delta=-price_wear,
                    equity_delta=-price_wear,
                    price_wear=price_wear,
                    operation_payload_hash=operation_payload_hash,
                    payload={"close_price": str(close_match.avg_price), "open_price": str(open_match.avg_price)},
                )
            )
        return entries

    def _latest_checkpoint(self, run_id: str) -> dict[str, Any]:
        checkpoint = self._repository.latest_kanglong_checkpoint(run_id)
        if checkpoint is None:
            return {"checkpoint_id": 0, "ledger_hash": GENESIS_LEDGER_HASH}
        return {"checkpoint_id": int(checkpoint["checkpoint_id"]), "ledger_hash": str(checkpoint["ledger_hash"])}

    def _next_progress(
        self,
        progress: dict[str, Any],
        *,
        group: dict[str, Any],
        group_index: int,
        round_index: int,
        checkpoint_id: int,
        matched_qty: Decimal,
        residual_qty: Decimal,
        is_supplemental: bool,
    ) -> dict[str, Any]:
        supplemental_rounds = int(progress.get("supplemental_rounds", 0)) + (1 if residual_qty > 0 else 0)
        next_round_index = round_index if residual_qty > 0 else round_index + 1
        return {
            **progress,
            "checkpoint_id": checkpoint_id,
            "current_group_id": group.get("group_id"),
            "group_index": group_index,
            "round_index": next_round_index,
            "round_attempt": int(progress.get("round_attempt", 1)) + (1 if residual_qty > 0 or is_supplemental else 0),
            "matched_qty": _decimal_text(matched_qty),
            "residual_qty": _decimal_text(residual_qty),
            "supplemental_rounds": supplemental_rounds,
            "consecutive_unfilled": 0,
            "scheduled_reason": "supplemental_residual" if residual_qty > 0 else "round_interval",
            "next_wake_at": (self._clock() + timedelta(seconds=self._config.round_interval_seconds)).isoformat(),
        }

    def _status_after_round(
        self,
        *,
        group_index: int,
        group_count: int,
        round_index: int,
        round_count: int,
        residual_qty: Decimal,
        pause_after_round: bool,
    ) -> str:
        if pause_after_round:
            return "paused_by_user"
        if residual_qty > Decimal("0"):
            return "running"
        if group_index + 1 >= group_count and round_index + 1 >= round_count:
            return "completed"
        return "running"


def _position_side(value: Any) -> PositionSide:
    normalized = str(value or "").upper()
    if normalized in {"LONG", "LONG | 做多开仓"}:
        return PositionSide.LONG
    if normalized in {"SHORT", "SHORT | 做空开仓"}:
        return PositionSide.SHORT
    raise ValueError("kanglong_invalid_transfer_setting")


def _transfer_order_sides(side: PositionSide) -> tuple[OrderSide, OrderSide]:
    if side == PositionSide.LONG:
        return OrderSide.SELL, OrderSide.BUY
    return OrderSide.BUY, OrderSide.SELL


def _round_id(group_id: str, round_index: int, is_supplemental: bool, round_attempt: int) -> str:
    if is_supplemental:
        return f"{group_id}-supplemental-{round_attempt:04d}"
    return f"{group_id}-round-{round_index + 1:04d}"


def _operation_id(run_id: str, group_id: str, round_id: str, round_attempt: int) -> str:
    return f"{run_id}:{group_id}:{round_id}:{round_attempt}"


def _scaled_fee(match: MatchResult, aligned_qty: Decimal) -> Decimal:
    if match.filled_qty <= Decimal("0"):
        return Decimal("0")
    return match.fee * aligned_qty / match.filled_qty


def _price_wear(side: PositionSide, close_price: Decimal, open_price: Decimal, qty: Decimal) -> Decimal:
    if side == PositionSide.LONG:
        return max(open_price - close_price, Decimal("0")) * qty
    return max(close_price - open_price, Decimal("0")) * qty


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text
