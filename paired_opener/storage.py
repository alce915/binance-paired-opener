from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from app_i18n.runtime import CONTRACT_VERSION, DEFAULT_ACCOUNT_NAME, redact_debug_text
from paired_opener.domain import OpenSession, RecoveryStatus, RoundExecution, SessionStatus, SessionStopReason
from paired_opener.kanglong.ledger import (
    KanglongLedgerBaseline,
    KanglongLedgerEntry,
    baseline_from_storage_payload,
    hash_checkpoint,
    hash_ledger_state,
    ledger_entry_from_storage_payload,
)


def _json_dumps(payload: Any) -> str:
    def encode(value: Any) -> Any:
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        if hasattr(value, "value"):
            return value.value
        return value

    return json.dumps(payload, default=encode, ensure_ascii=True, sort_keys=True)


def _json_load(payload: str, default: Any) -> Any:
    try:
        return json.loads(payload) if payload else default
    except json.JSONDecodeError:
        return default


def _kanglong_report_summary(payload: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    if "report_summary" in payload:
        return payload.get("report_summary") or {}
    summary = report.get("summary") if isinstance(report, dict) else None
    return summary if isinstance(summary, dict) else {}


_ACTIVE_KANGLONG_RUN_STATUSES = (
    "draft_plan",
    "chain_ready",
    "plan_confirmed",
    "execution_starting",
    "running",
    "pause_pending",
    "stop_pending",
    "paused_by_user",
    "paused_market_unstable",
    "paused_plan_stale",
    "group_ready",
    "group_completed",
    "paused_group_round_limit_exceeded",
    "paused_group_not_executable",
    "paused_plan_recheck_changed",
    "needs_abort_recover",
    "abort_recovering",
    "unsafe_dust_residual",
)


class SqliteRepository:
    def __init__(
        self,
        database_path: Path,
        *,
        session_event_retention_days: int = 30,
        session_event_retention_per_session: int = 2_000,
    ) -> None:
        database_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(database_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._session_event_retention_days = max(int(session_event_retention_days), 0)
        self._session_event_retention_per_session = max(int(session_event_retention_per_session), 0)
        self._initialize()

    def _initialize(self) -> None:
        account_name_default = DEFAULT_ACCOUNT_NAME.replace("'", "''")
        with self._connection:
            self._connection.executescript(
                f"""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    session_kind TEXT NOT NULL DEFAULT 'paired_open',
                    account_id TEXT NOT NULL DEFAULT 'default',
                    account_name TEXT NOT NULL DEFAULT '{account_name_default}',
                    symbol TEXT NOT NULL,
                    trend_bias TEXT NOT NULL,
                    leverage INTEGER NOT NULL,
                    round_count INTEGER NOT NULL,
                    round_qty TEXT NOT NULL,
                    poll_interval_ms INTEGER NOT NULL,
                    order_ttl_ms INTEGER NOT NULL,
                    max_zero_fill_retries INTEGER NOT NULL,
                    market_fallback_attempts INTEGER NOT NULL,
                    execution_profile TEXT NOT NULL DEFAULT 'balanced',
                    market_fallback_max_ratio TEXT NOT NULL DEFAULT '1',
                    market_fallback_min_residual_qty TEXT NOT NULL DEFAULT '0',
                    max_reprice_ticks INTEGER,
                    max_spread_bps INTEGER,
                    max_reference_deviation_bps INTEGER,
                    round_interval_seconds INTEGER NOT NULL DEFAULT 3,
                    open_mode TEXT,
                    close_mode TEXT,
                    selected_position_side TEXT,
                    target_open_qty TEXT NOT NULL DEFAULT '0',
                    target_close_qty TEXT NOT NULL DEFAULT '0',
                    planned_round_qtys_json TEXT NOT NULL DEFAULT '[]',
                    final_round_qty TEXT NOT NULL DEFAULT '0',
                    extension_round_cap_qty TEXT NOT NULL DEFAULT '0',
                    max_extension_rounds INTEGER NOT NULL DEFAULT 5,
                    max_session_duration_seconds INTEGER NOT NULL DEFAULT 1800,
                    created_by TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_error TEXT,
                    last_error_category TEXT,
                    last_error_strategy TEXT,
                    last_error_code TEXT,
                    last_error_operator_action TEXT,
                    last_error_params_json TEXT,
                    last_error_raw_message TEXT,
                    last_error_contract_version TEXT,
                    recovery_status TEXT,
                    recovery_summary TEXT,
                    recovery_checked_at TEXT,
                    recovery_details_json TEXT,
                    stage2_carryover_qty TEXT NOT NULL DEFAULT '0',
                    final_alignment_status TEXT NOT NULL DEFAULT 'not_needed',
                    final_unaligned_qty TEXT NOT NULL DEFAULT '0',
                    completed_with_final_alignment INTEGER NOT NULL DEFAULT 0,
                    session_deadline_at TEXT,
                    extension_rounds_used INTEGER NOT NULL DEFAULT 0,
                    remaining_extension_rounds INTEGER NOT NULL DEFAULT 0,
                    stop_reason TEXT,
                    residual_source TEXT
                );
                CREATE TABLE IF NOT EXISTS rounds (
                    session_id TEXT NOT NULL,
                    round_index INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    stage1_filled_qty TEXT NOT NULL,
                    stage2_filled_qty TEXT NOT NULL,
                    stage1_zero_fill_retries INTEGER NOT NULL,
                    stage2_zero_fill_retries INTEGER NOT NULL,
                    market_fallback_used INTEGER NOT NULL,
                    notes_json TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    PRIMARY KEY (session_id, round_index)
                );
                CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    round_index INTEGER,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kanglong_runs (
                    run_id TEXT PRIMARY KEY,
                    engine_version INTEGER NOT NULL DEFAULT 1,
                    symbol TEXT NOT NULL,
                    main_account_id TEXT NOT NULL,
                    subaccount_ids_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result_grade TEXT,
                    request_json TEXT NOT NULL,
                    plan_json TEXT NOT NULL DEFAULT '{{}}',
                    report_json TEXT NOT NULL DEFAULT '{{}}',
                    plan_version TEXT,
                    snapshot_bundle_id TEXT,
                    confirmed_at TEXT,
                    available_actions_json TEXT NOT NULL DEFAULT '[]',
                    progress_json TEXT NOT NULL DEFAULT '{{}}',
                    report_summary_json TEXT NOT NULL DEFAULT '{{}}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kanglong_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    checkpoint_id INTEGER,
                    group_id TEXT,
                    round_id TEXT,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kanglong_locks (
                    lock_scope TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    worker_id TEXT,
                    lease_token TEXT,
                    fencing_token TEXT,
                    worker_epoch INTEGER NOT NULL DEFAULT 0,
                    heartbeat_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kanglong_idempotency (
                    idempotency_key TEXT PRIMARY KEY,
                    request_hash TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kanglong_ledger_baselines (
                    run_id TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    wallet_balance TEXT NOT NULL DEFAULT '0',
                    available_balance TEXT NOT NULL DEFAULT '0',
                    equity TEXT NOT NULL DEFAULT '0',
                    margin TEXT NOT NULL DEFAULT '0',
                    margin_deficit TEXT NOT NULL DEFAULT '0',
                    total_unrealized_pnl TEXT NOT NULL DEFAULT '0',
                    long_qty TEXT NOT NULL DEFAULT '0',
                    long_entry_price TEXT NOT NULL DEFAULT '0',
                    long_mark_price TEXT NOT NULL DEFAULT '0',
                    long_leverage INTEGER NOT NULL DEFAULT 1,
                    short_qty TEXT NOT NULL DEFAULT '0',
                    short_entry_price TEXT NOT NULL DEFAULT '0',
                    short_mark_price TEXT NOT NULL DEFAULT '0',
                    short_leverage INTEGER NOT NULL DEFAULT 1,
                    baseline_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, account_id)
                );
                CREATE TABLE IF NOT EXISTS kanglong_run_checkpoints (
                    run_id TEXT NOT NULL,
                    checkpoint_id INTEGER NOT NULL,
                    previous_ledger_hash TEXT NOT NULL,
                    ledger_hash TEXT NOT NULL,
                    ledger_state_hash TEXT NOT NULL,
                    ledger_entry_count INTEGER NOT NULL DEFAULT 0,
                    events_high_watermark INTEGER NOT NULL DEFAULT 0,
                    is_safe INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, checkpoint_id)
                );
                CREATE TABLE IF NOT EXISTS kanglong_ledger_entries (
                    entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    checkpoint_id INTEGER NOT NULL,
                    sequence INTEGER NOT NULL,
                    operation_id TEXT NOT NULL,
                    account_id TEXT,
                    entry_type TEXT NOT NULL,
                    asset TEXT,
                    amount TEXT NOT NULL DEFAULT '0',
                    qty_delta TEXT NOT NULL DEFAULT '0',
                    margin_delta TEXT NOT NULL DEFAULT '0',
                    available_delta TEXT NOT NULL DEFAULT '0',
                    equity_delta TEXT NOT NULL DEFAULT '0',
                    realized_pnl_delta TEXT NOT NULL DEFAULT '0',
                    price_wear TEXT NOT NULL DEFAULT '0',
                    fee_amount TEXT NOT NULL DEFAULT '0',
                    fee_asset TEXT,
                    operation_payload_hash TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{{}}',
                    created_at TEXT NOT NULL,
                    UNIQUE (run_id, checkpoint_id, sequence),
                    UNIQUE (run_id, operation_id, sequence)
                );
                """
            )
            self._ensure_column("sessions", "session_kind", "TEXT NOT NULL DEFAULT 'paired_open'")
            self._ensure_column("sessions", "account_id", "TEXT NOT NULL DEFAULT 'default'")
            self._ensure_column("sessions", "account_name", f"TEXT NOT NULL DEFAULT '{account_name_default}'")
            self._ensure_column("sessions", "round_interval_seconds", "INTEGER NOT NULL DEFAULT 3")
            self._ensure_column("sessions", "execution_profile", "TEXT NOT NULL DEFAULT 'balanced'")
            self._ensure_column("sessions", "market_fallback_max_ratio", "TEXT NOT NULL DEFAULT '1'")
            self._ensure_column("sessions", "market_fallback_min_residual_qty", "TEXT NOT NULL DEFAULT '0'")
            self._ensure_column("sessions", "max_reprice_ticks", "INTEGER")
            self._ensure_column("sessions", "max_spread_bps", "INTEGER")
            self._ensure_column("sessions", "max_reference_deviation_bps", "INTEGER")
            self._ensure_column("sessions", "open_mode", "TEXT")
            self._ensure_column("sessions", "close_mode", "TEXT")
            self._ensure_column("sessions", "selected_position_side", "TEXT")
            self._ensure_column("sessions", "target_open_qty", "TEXT NOT NULL DEFAULT '0'")
            self._ensure_column("sessions", "target_close_qty", "TEXT NOT NULL DEFAULT '0'")
            self._ensure_column("sessions", "planned_round_qtys_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column("sessions", "final_round_qty", "TEXT NOT NULL DEFAULT '0'")
            self._ensure_column("sessions", "extension_round_cap_qty", "TEXT NOT NULL DEFAULT '0'")
            self._ensure_column("sessions", "max_extension_rounds", "INTEGER NOT NULL DEFAULT 5")
            self._ensure_column("sessions", "max_session_duration_seconds", "INTEGER NOT NULL DEFAULT 1800")
            self._ensure_column("sessions", "stage2_carryover_qty", "TEXT NOT NULL DEFAULT '0'")
            self._ensure_column("sessions", "final_alignment_status", "TEXT NOT NULL DEFAULT 'not_needed'")
            self._ensure_column("sessions", "final_unaligned_qty", "TEXT NOT NULL DEFAULT '0'")
            self._ensure_column("sessions", "completed_with_final_alignment", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("sessions", "session_deadline_at", "TEXT")
            self._ensure_column("sessions", "extension_rounds_used", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("sessions", "remaining_extension_rounds", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("sessions", "stop_reason", "TEXT")
            self._ensure_column("sessions", "residual_source", "TEXT")
            self._ensure_column("sessions", "last_error_category", "TEXT")
            self._ensure_column("sessions", "last_error_strategy", "TEXT")
            self._ensure_column("sessions", "last_error_code", "TEXT")
            self._ensure_column("sessions", "last_error_operator_action", "TEXT")
            self._ensure_column("sessions", "last_error_params_json", "TEXT")
            self._ensure_column("sessions", "last_error_raw_message", "TEXT")
            self._ensure_column("sessions", "last_error_contract_version", f"TEXT NOT NULL DEFAULT '{CONTRACT_VERSION}'")
            self._ensure_column("sessions", "recovery_status", "TEXT")
            self._ensure_column("sessions", "recovery_summary", "TEXT")
            self._ensure_column("sessions", "recovery_checked_at", "TEXT")
            self._ensure_column("sessions", "recovery_details_json", "TEXT")
            self._ensure_column("kanglong_runs", "plan_version", "TEXT")
            self._ensure_column("kanglong_runs", "engine_version", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column("kanglong_runs", "snapshot_bundle_id", "TEXT")
            self._ensure_column("kanglong_runs", "confirmed_at", "TEXT")
            self._ensure_column("kanglong_runs", "available_actions_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column("kanglong_runs", "progress_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("kanglong_runs", "report_summary_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("kanglong_events", "checkpoint_id", "INTEGER")
            self._ensure_column("kanglong_locks", "worker_id", "TEXT")
            self._ensure_column("kanglong_locks", "lease_token", "TEXT")
            self._ensure_column("kanglong_locks", "fencing_token", "TEXT")
            self._ensure_column("kanglong_locks", "worker_epoch", "INTEGER NOT NULL DEFAULT 0")

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        columns = {row["name"] for row in self._connection.execute(f"PRAGMA table_info({table})").fetchall()}
        if column in columns:
            return
        self._connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def create_kanglong_run(self, payload: dict[str, Any]) -> None:
        now = datetime.now(UTC).isoformat()
        created_at = payload.get("created_at") or now
        updated_at = payload.get("updated_at") or now
        request_payload = payload.get("request") or {
            "mode": "simulation",
            "symbol": payload["symbol"],
            "main_account_id": payload["main_account_id"],
            "subaccount_ids": payload["subaccount_ids"],
        }
        report_payload = payload.get("report") or {}
        report_summary = _kanglong_report_summary(payload, report_payload)
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO kanglong_runs (
                    run_id, engine_version, symbol, main_account_id, subaccount_ids_json, status,
                    result_grade, request_json, plan_json, report_json, plan_version,
                    snapshot_bundle_id, confirmed_at, available_actions_json, progress_json,
                    report_summary_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["run_id"],
                    int(payload.get("engine_version", 2)),
                    payload["symbol"],
                    payload["main_account_id"],
                    _json_dumps(payload["subaccount_ids"]),
                    payload["status"],
                    payload.get("result_grade"),
                    _json_dumps(request_payload),
                    _json_dumps(payload.get("plan") or {}),
                    _json_dumps(report_payload),
                    payload.get("plan_version"),
                    payload.get("snapshot_bundle_id"),
                    payload.get("confirmed_at"),
                    _json_dumps(payload.get("available_actions") or []),
                    _json_dumps(payload.get("progress") or {}),
                    _json_dumps(report_summary),
                    created_at,
                    updated_at,
                ),
            )

    def get_kanglong_run(self, run_id: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            "SELECT * FROM kanglong_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        return self._deserialize_kanglong_run_row(row)

    def get_active_kanglong_run(self) -> dict[str, Any] | None:
        placeholders = ", ".join("?" for _ in _ACTIVE_KANGLONG_RUN_STATUSES)
        row = self._connection.execute(
            f"""
            SELECT * FROM kanglong_runs
            WHERE status IN ({placeholders})
              AND engine_version >= 2
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            _ACTIVE_KANGLONG_RUN_STATUSES,
        ).fetchone()
        if row is None:
            return None
        return self._deserialize_kanglong_run_row(row)

    def update_kanglong_run_request(self, run_id: str, request: dict[str, Any]) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE kanglong_runs
                SET request_json = ?,
                    updated_at = ?
                WHERE run_id = ?
                """,
                (_json_dumps(request), datetime.now(UTC).isoformat(), run_id),
            )

    def update_kanglong_run(
        self,
        run_id: str,
        *,
        status: str,
        plan: dict[str, Any] | None = None,
        report: dict[str, Any] | None = None,
        result_grade: str | None = None,
        plan_version: str | None = None,
        snapshot_bundle_id: str | None = None,
        confirmed_at: str | None = None,
        available_actions: list[str] | None = None,
        progress: dict[str, Any] | None = None,
        report_summary: dict[str, Any] | None = None,
    ) -> None:
        with self._lock, self._connection:
            current = self._connection.execute(
                """
                SELECT plan_json, report_json, result_grade, plan_version,
                       snapshot_bundle_id, confirmed_at, available_actions_json,
                       progress_json, report_summary_json
                FROM kanglong_runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if current is None:
                return
            next_report_summary_json = current["report_summary_json"]
            if report_summary is not None:
                next_report_summary_json = _json_dumps(report_summary)
            elif report is not None:
                next_report_summary_json = _json_dumps(_kanglong_report_summary({}, report))
            self._connection.execute(
                """
                UPDATE kanglong_runs
                SET status = ?,
                    result_grade = ?,
                    plan_json = ?,
                    report_json = ?,
                    plan_version = ?,
                    snapshot_bundle_id = ?,
                    confirmed_at = ?,
                    available_actions_json = ?,
                    progress_json = ?,
                    report_summary_json = ?,
                    updated_at = ?
                WHERE run_id = ?
                """,
                (
                    status,
                    result_grade if result_grade is not None else current["result_grade"],
                    _json_dumps(plan) if plan is not None else current["plan_json"],
                    _json_dumps(report) if report is not None else current["report_json"],
                    plan_version if plan_version is not None else current["plan_version"],
                    snapshot_bundle_id if snapshot_bundle_id is not None else current["snapshot_bundle_id"],
                    confirmed_at if confirmed_at is not None else current["confirmed_at"],
                    _json_dumps(available_actions) if available_actions is not None else current["available_actions_json"],
                    _json_dumps(progress) if progress is not None else current["progress_json"],
                    next_report_summary_json,
                    datetime.now(UTC).isoformat(),
                    run_id,
                ),
            )

    def update_kanglong_run_and_events(
        self,
        run_id: str,
        *,
        status: str,
        events: list[dict[str, Any]],
        plan: dict[str, Any] | None = None,
        report: dict[str, Any] | None = None,
        result_grade: str | None = None,
        plan_version: str | None = None,
        snapshot_bundle_id: str | None = None,
        confirmed_at: str | None = None,
        available_actions: list[str] | None = None,
        progress: dict[str, Any] | None = None,
        report_summary: dict[str, Any] | None = None,
    ) -> list[int]:
        event_ids: list[int] = []
        with self._lock, self._connection:
            current = self._connection.execute(
                """
                SELECT plan_json, report_json, result_grade, plan_version,
                       snapshot_bundle_id, confirmed_at, available_actions_json,
                       progress_json, report_summary_json
                FROM kanglong_runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if current is None:
                return event_ids
            next_report_summary_json = current["report_summary_json"]
            if report_summary is not None:
                next_report_summary_json = _json_dumps(report_summary)
            elif report is not None:
                next_report_summary_json = _json_dumps(_kanglong_report_summary({}, report))
            self._connection.execute(
                """
                UPDATE kanglong_runs
                SET status = ?,
                    result_grade = ?,
                    plan_json = ?,
                    report_json = ?,
                    plan_version = ?,
                    snapshot_bundle_id = ?,
                    confirmed_at = ?,
                    available_actions_json = ?,
                    progress_json = ?,
                    report_summary_json = ?,
                    updated_at = ?
                WHERE run_id = ?
                """,
                (
                    status,
                    result_grade if result_grade is not None else current["result_grade"],
                    _json_dumps(plan) if plan is not None else current["plan_json"],
                    _json_dumps(report) if report is not None else current["report_json"],
                    plan_version if plan_version is not None else current["plan_version"],
                    snapshot_bundle_id if snapshot_bundle_id is not None else current["snapshot_bundle_id"],
                    confirmed_at if confirmed_at is not None else current["confirmed_at"],
                    _json_dumps(available_actions) if available_actions is not None else current["available_actions_json"],
                    _json_dumps(progress) if progress is not None else current["progress_json"],
                    next_report_summary_json,
                    datetime.now(UTC).isoformat(),
                    run_id,
                ),
            )
            created_at = datetime.now(UTC).isoformat()
            for event in events:
                cursor = self._connection.execute(
                    """
                    INSERT INTO kanglong_events (run_id, checkpoint_id, group_id, round_id, event_type, payload_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        event.get("checkpoint_id"),
                        event.get("group_id"),
                        event.get("round_id"),
                        event["event_type"],
                        _json_dumps(event.get("payload") or {}),
                        created_at,
                    ),
                )
                event_ids.append(int(cursor.lastrowid))
        return event_ids

    def add_kanglong_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        checkpoint_id: int | None = None,
        group_id: str | None = None,
        round_id: str | None = None,
    ) -> int:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO kanglong_events (run_id, checkpoint_id, group_id, round_id, event_type, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    checkpoint_id,
                    group_id,
                    round_id,
                    event_type,
                    _json_dumps(payload),
                    datetime.now(UTC).isoformat(),
                ),
            )
            return int(cursor.lastrowid)

    def list_kanglong_events(
        self,
        run_id: str,
        after_event_id: int | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        bounded_limit = max(1, min(int(limit), 1000))
        rows = self._connection.execute(
            """
            SELECT * FROM kanglong_events
            WHERE run_id = ? AND event_id > ?
            ORDER BY event_id ASC
            LIMIT ?
            """,
            (run_id, int(after_event_id or 0), bounded_limit + 1),
        ).fetchall()
        visible_rows = rows[:bounded_limit]
        events = [self._deserialize_kanglong_event_row(row) for row in visible_rows]
        next_after_event_id = events[-1]["event_id"] if events else int(after_event_id or 0)
        return {
            "events": events,
            "next_after_event_id": next_after_event_id,
            "latest_event_id": self.latest_kanglong_event_id(run_id),
            "has_more": len(rows) > bounded_limit,
        }

    def latest_kanglong_event_id(self, run_id: str) -> int:
        row = self._connection.execute(
            "SELECT COALESCE(MAX(event_id), 0) AS latest_event_id FROM kanglong_events WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        return int(row["latest_event_id"] or 0) if row is not None else 0

    def save_kanglong_ledger_baselines(
        self,
        run_id: str,
        baselines: list[dict[str, Any] | KanglongLedgerBaseline],
    ) -> None:
        now = datetime.now(UTC).isoformat()
        fields = [
            "wallet_balance",
            "available_balance",
            "equity",
            "margin",
            "margin_deficit",
            "total_unrealized_pnl",
            "long_qty",
            "long_entry_price",
            "long_mark_price",
            "short_qty",
            "short_entry_price",
            "short_mark_price",
        ]
        with self._lock, self._connection:
            self._connection.execute(
                "DELETE FROM kanglong_ledger_baselines WHERE run_id = ?",
                (run_id,),
            )
            for raw_baseline in baselines:
                if isinstance(raw_baseline, KanglongLedgerBaseline):
                    baseline = raw_baseline
                    if baseline.run_id != run_id:
                        raise ValueError("kanglong_operation_payload_mismatch")
                else:
                    baseline_payload = dict(raw_baseline)
                    baseline_payload.setdefault("run_id", run_id)
                    baseline = baseline_from_storage_payload(baseline_payload)
                payload = baseline.to_storage_payload()
                self._connection.execute(
                    """
                    INSERT INTO kanglong_ledger_baselines (
                        run_id, account_id, wallet_balance, available_balance, equity,
                        margin, margin_deficit, total_unrealized_pnl,
                        long_qty, long_entry_price, long_mark_price, long_leverage,
                        short_qty, short_entry_price, short_mark_price, short_leverage,
                        baseline_hash, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        str(payload["account_id"]),
                        *(str(payload.get(field, "0")) for field in fields[:9]),
                        int(payload.get("long_leverage", 1)),
                        *(str(payload.get(field, "0")) for field in fields[9:]),
                        int(payload.get("short_leverage", 1)),
                        str(payload["baseline_hash"]),
                        now,
                    ),
                )

    def list_kanglong_ledger_baselines(self, run_id: str) -> list[dict[str, Any]]:
        rows = self._connection.execute(
            "SELECT * FROM kanglong_ledger_baselines WHERE run_id = ? ORDER BY account_id ASC",
            (run_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def latest_kanglong_checkpoint(self, run_id: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            """
            SELECT * FROM kanglong_run_checkpoints
            WHERE run_id = ?
            ORDER BY checkpoint_id DESC
            LIMIT 1
            """,
            (run_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    def list_kanglong_ledger_entries(self, run_id: str, checkpoint_id: int | None = None) -> list[dict[str, Any]]:
        if checkpoint_id is None:
            rows = self._connection.execute(
                """
                SELECT * FROM kanglong_ledger_entries
                WHERE run_id = ?
                ORDER BY checkpoint_id ASC, sequence ASC, entry_id ASC
                """,
                (run_id,),
            ).fetchall()
        else:
            rows = self._connection.execute(
                """
                SELECT * FROM kanglong_ledger_entries
                WHERE run_id = ? AND checkpoint_id = ?
                ORDER BY sequence ASC, entry_id ASC
                """,
                (run_id, int(checkpoint_id)),
            ).fetchall()
        entries = []
        for row in rows:
            payload = dict(row)
            payload["payload"] = _json_load(payload.pop("payload_json", "{}"), {})
            entries.append(payload)
        return entries

    def commit_kanglong_checkpoint(
        self,
        *,
        run_id: str,
        checkpoint_id: int,
        expected_previous_checkpoint_id: int,
        expected_previous_ledger_hash: str,
        previous_ledger_hash: str,
        ledger_hash: str,
        ledger_state_hash: str,
        ledger_entries: list[dict[str, Any] | KanglongLedgerEntry],
        events: list[dict[str, Any]],
        status: str | None = None,
        available_actions: list[str] | None = None,
        progress: dict[str, Any] | None = None,
        report_summary: dict[str, Any] | None = None,
        is_safe: bool = True,
    ) -> dict[str, Any]:
        now = datetime.now(UTC).isoformat()
        with self._lock, self._connection:
            latest = self._connection.execute(
                """
                SELECT * FROM kanglong_run_checkpoints
                WHERE run_id = ?
                ORDER BY checkpoint_id DESC
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
            latest_checkpoint_id = int(latest["checkpoint_id"]) if latest is not None else 0
            latest_ledger_hash = str(latest["ledger_hash"]) if latest is not None else str(expected_previous_ledger_hash)
            if latest_checkpoint_id != int(expected_previous_checkpoint_id):
                raise ValueError("kanglong_stale_checkpoint")
            if latest is not None and latest_ledger_hash != expected_previous_ledger_hash:
                raise ValueError("kanglong_ledger_hash_mismatch")
            if previous_ledger_hash != expected_previous_ledger_hash:
                raise ValueError("kanglong_ledger_hash_mismatch")
            if int(checkpoint_id) != latest_checkpoint_id + 1:
                raise ValueError("kanglong_stale_checkpoint")

            baseline_rows = self._connection.execute(
                "SELECT * FROM kanglong_ledger_baselines WHERE run_id = ? ORDER BY account_id ASC",
                (run_id,),
            ).fetchall()
            baselines = [baseline_from_storage_payload(dict(row)) for row in baseline_rows]
            previous_rows = self._connection.execute(
                """
                SELECT * FROM kanglong_ledger_entries
                WHERE run_id = ?
                ORDER BY checkpoint_id ASC, sequence ASC, entry_id ASC
                """,
                (run_id,),
            ).fetchall()
            previous_entries = []
            for row in previous_rows:
                previous_entry_payload = dict(row)
                previous_entry_payload["payload"] = _json_load(
                    previous_entry_payload.pop("payload_json", "{}"),
                    {},
                )
                previous_entries.append(ledger_entry_from_storage_payload(previous_entry_payload))

            normalized_entries: list[KanglongLedgerEntry] = []
            for index, raw_entry in enumerate(ledger_entries, start=1):
                if isinstance(raw_entry, KanglongLedgerEntry):
                    entry = raw_entry
                    if entry.run_id != run_id or entry.checkpoint_id != int(checkpoint_id):
                        raise ValueError("kanglong_operation_payload_mismatch")
                else:
                    entry_payload = dict(raw_entry)
                    sequence = int(entry_payload.get("sequence", index))
                    entry_payload.setdefault("run_id", run_id)
                    entry_payload.setdefault("checkpoint_id", int(checkpoint_id))
                    entry_payload.setdefault(
                        "operation_id",
                        f"{run_id}:checkpoint:{checkpoint_id}:entry:{sequence}",
                    )
                    entry_payload["sequence"] = sequence
                    entry = ledger_entry_from_storage_payload(entry_payload)
                normalized_entries.append(entry)

            computed_ledger_hash = hash_checkpoint(previous_ledger_hash, normalized_entries)
            computed_ledger_state_hash = hash_ledger_state(baselines, [*previous_entries, *normalized_entries])

            for entry in normalized_entries:
                storage_payload = entry.to_storage_payload()
                self._connection.execute(
                    """
                    INSERT INTO kanglong_ledger_entries (
                        run_id, checkpoint_id, sequence, operation_id, account_id,
                        entry_type, asset, amount, qty_delta, margin_delta,
                        available_delta, equity_delta, realized_pnl_delta, price_wear,
                        fee_amount, fee_asset, operation_payload_hash, payload_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        int(checkpoint_id),
                        int(storage_payload["sequence"]),
                        str(storage_payload["operation_id"]),
                        storage_payload.get("account_id"),
                        str(storage_payload["entry_type"]),
                        storage_payload.get("asset"),
                        str(storage_payload.get("amount", "0")),
                        str(storage_payload.get("qty_delta", "0")),
                        str(storage_payload.get("margin_delta", "0")),
                        str(storage_payload.get("available_delta", "0")),
                        str(storage_payload.get("equity_delta", "0")),
                        str(storage_payload.get("realized_pnl_delta", "0")),
                        str(storage_payload.get("price_wear", "0")),
                        str(storage_payload.get("fee_amount", "0")),
                        storage_payload.get("fee_asset"),
                        storage_payload.get("operation_payload_hash"),
                        _json_dumps(storage_payload.get("payload") or {}),
                        now,
                    ),
                )

            event_ids: list[int] = []
            for event in events:
                cursor = self._connection.execute(
                    """
                    INSERT INTO kanglong_events (run_id, checkpoint_id, group_id, round_id, event_type, payload_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        int(checkpoint_id),
                        event.get("group_id"),
                        event.get("round_id"),
                        str(event["event_type"]),
                        _json_dumps(event.get("payload") or {}),
                        now,
                    ),
                )
                event_ids.append(int(cursor.lastrowid))
            events_high_watermark = event_ids[-1] if event_ids else self.latest_kanglong_event_id(run_id)

            self._connection.execute(
                """
                INSERT INTO kanglong_run_checkpoints (
                    run_id, checkpoint_id, previous_ledger_hash, ledger_hash,
                    ledger_state_hash, ledger_entry_count, events_high_watermark,
                    is_safe, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    int(checkpoint_id),
                    previous_ledger_hash,
                    computed_ledger_hash,
                    computed_ledger_state_hash,
                    len(normalized_entries),
                    int(events_high_watermark),
                    1 if is_safe else 0,
                    now,
                ),
            )

            if status is not None or available_actions is not None or progress is not None or report_summary is not None:
                current = self._connection.execute(
                    """
                    SELECT status, available_actions_json, progress_json, report_summary_json
                    FROM kanglong_runs
                    WHERE run_id = ?
                    """,
                    (run_id,),
                ).fetchone()
                if current is not None:
                    self._connection.execute(
                        """
                        UPDATE kanglong_runs
                        SET status = ?,
                            available_actions_json = ?,
                            progress_json = ?,
                            report_summary_json = ?,
                            updated_at = ?
                        WHERE run_id = ?
                        """,
                        (
                            status if status is not None else current["status"],
                            _json_dumps(available_actions) if available_actions is not None else current["available_actions_json"],
                            _json_dumps(progress) if progress is not None else current["progress_json"],
                            _json_dumps(report_summary) if report_summary is not None else current["report_summary_json"],
                            now,
                            run_id,
                        ),
                    )
        return {
            "run_id": run_id,
            "checkpoint_id": int(checkpoint_id),
            "ledger_hash": computed_ledger_hash,
            "ledger_state_hash": computed_ledger_state_hash,
            "event_ids": event_ids,
        }

    def acquire_kanglong_locks(
        self,
        *,
        run_id: str,
        lock_scopes: list[str] | set[str] | tuple[str, ...],
        ttl_ms: int,
    ) -> dict[str, Any] | None:
        scopes = sorted({scope for scope in lock_scopes if scope})
        if not scopes:
            return None
        now = datetime.now(UTC)
        now_text = now.isoformat()
        expires_at = (now + timedelta(milliseconds=max(int(ttl_ms), 1000))).isoformat()
        with self._lock, self._connection:
            self._connection.execute(
                """
                DELETE FROM kanglong_locks
                WHERE expires_at <= ?
                  AND (lease_token IS NULL OR lease_token = '')
                """,
                (now_text,),
            )
            for scope in scopes:
                row = self._connection.execute(
                    "SELECT * FROM kanglong_locks WHERE lock_scope = ?",
                    (scope,),
                ).fetchone()
                if row is not None and row["run_id"] != run_id and row["status"] == "active":
                    return dict(row)
            for scope in scopes:
                self._connection.execute(
                    """
                    INSERT INTO kanglong_locks (lock_scope, run_id, status, heartbeat_at, expires_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(lock_scope) DO UPDATE SET
                        run_id = excluded.run_id,
                        status = excluded.status,
                        heartbeat_at = excluded.heartbeat_at,
                        expires_at = excluded.expires_at
                    """,
                    (scope, run_id, "active", now_text, expires_at),
                )
        return None

    def heartbeat_kanglong_locks(self, *, run_id: str, ttl_ms: int) -> None:
        now = datetime.now(UTC)
        now_text = now.isoformat()
        expires_at = (now + timedelta(milliseconds=max(int(ttl_ms), 1000))).isoformat()
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE kanglong_locks
                SET heartbeat_at = ?, expires_at = ?
                WHERE run_id = ? AND status = 'active'
                """,
                (now_text, expires_at, run_id),
            )

    def release_kanglong_locks(self, run_id: str) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                DELETE FROM kanglong_locks
                WHERE run_id = ?
                  AND (lease_token IS NULL OR lease_token = '')
                """,
                (run_id,),
            )

    def acquire_kanglong_run_lease(self, *, run_id: str, worker_id: str, ttl_seconds: int) -> dict[str, Any]:
        lock_scope = f"kanglong:run:{run_id}:lease"
        now = datetime.now(UTC)
        now_text = now.isoformat()
        expires_at = (now + timedelta(seconds=max(int(ttl_seconds), 1))).isoformat()
        with self._lock, self._connection:
            row = self._connection.execute(
                "SELECT * FROM kanglong_locks WHERE lock_scope = ?",
                (lock_scope,),
            ).fetchone()
            if row is not None and row["status"] == "active" and str(row["expires_at"]) > now_text:
                payload = dict(row)
                payload["conflict"] = True
                payload["lock_expires_at"] = payload.get("expires_at")
                return payload
            worker_epoch = (int(row["worker_epoch"] or 0) + 1) if row is not None else 1
            lease_token = f"lease-{uuid4().hex}"
            fencing_token = f"fence-{run_id}-{worker_epoch}-{uuid4().hex}"
            self._connection.execute(
                """
                INSERT INTO kanglong_locks (
                    lock_scope, run_id, status, worker_id, lease_token,
                    fencing_token, worker_epoch, heartbeat_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(lock_scope) DO UPDATE SET
                    run_id = excluded.run_id,
                    status = excluded.status,
                    worker_id = excluded.worker_id,
                    lease_token = excluded.lease_token,
                    fencing_token = excluded.fencing_token,
                    worker_epoch = excluded.worker_epoch,
                    heartbeat_at = excluded.heartbeat_at,
                    expires_at = excluded.expires_at
                """,
                (
                    lock_scope,
                    run_id,
                    "active",
                    worker_id,
                    lease_token,
                    fencing_token,
                    worker_epoch,
                    now_text,
                    expires_at,
                ),
            )
        return {
            "conflict": False,
            "run_id": run_id,
            "worker_id": worker_id,
            "lease_token": lease_token,
            "fencing_token": fencing_token,
            "worker_epoch": worker_epoch,
            "lock_expires_at": expires_at,
        }

    def renew_kanglong_run_lease(
        self,
        *,
        run_id: str,
        lease_token: str,
        fencing_token: str,
        ttl_seconds: int,
    ) -> dict[str, Any] | None:
        lock_scope = f"kanglong:run:{run_id}:lease"
        now = datetime.now(UTC)
        now_text = now.isoformat()
        expires_at = (now + timedelta(seconds=max(int(ttl_seconds), 1))).isoformat()
        with self._lock, self._connection:
            row = self._connection.execute(
                """
                SELECT * FROM kanglong_locks
                WHERE lock_scope = ?
                  AND run_id = ?
                  AND lease_token = ?
                  AND fencing_token = ?
                  AND status = 'active'
                  AND expires_at > ?
                """,
                (lock_scope, run_id, lease_token, fencing_token, now_text),
            ).fetchone()
            if row is None:
                return None
            self._connection.execute(
                """
                UPDATE kanglong_locks
                SET heartbeat_at = ?, expires_at = ?
                WHERE lock_scope = ?
                """,
                (now_text, expires_at, lock_scope),
            )
            payload = dict(row)
            payload["heartbeat_at"] = now_text
            payload["expires_at"] = expires_at
            payload["lock_expires_at"] = expires_at
            return payload

    def release_kanglong_run_lease(self, *, run_id: str, lease_token: str, fencing_token: str) -> bool:
        lock_scope = f"kanglong:run:{run_id}:lease"
        now_text = datetime.now(UTC).isoformat()
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                UPDATE kanglong_locks
                SET status = 'released',
                    heartbeat_at = ?,
                    expires_at = ?
                WHERE lock_scope = ?
                  AND run_id = ?
                  AND lease_token = ?
                  AND fencing_token = ?
                  AND status = 'active'
                """,
                (now_text, now_text, lock_scope, run_id, lease_token, fencing_token),
            )
            return cursor.rowcount > 0

    def request_kanglong_control_action(
        self,
        *,
        run_id: str,
        action: str,
        expected_action_version: int,
    ) -> dict[str, Any]:
        normalized_action = action.strip().lower()
        if normalized_action not in {"pause", "stop"}:
            raise ValueError("kanglong_invalid_control_action")
        with self._lock, self._connection:
            row = self._connection.execute(
                "SELECT * FROM kanglong_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise ValueError("kanglong_run_not_found")
            progress = _json_load(row["progress_json"], {})
            current_version = int(progress.get("action_version", 0))
            if current_version != int(expected_action_version):
                raise ValueError("kanglong_stale_action_version")
            current_action = (progress.get("control_request") or {}).get("action")
            if current_action == "stop" and normalized_action == "pause":
                return {
                    "run_id": run_id,
                    "status": row["status"],
                    "progress": progress,
                    "available_actions": _json_load(row["available_actions_json"], []),
                }
            next_version = current_version + 1
            next_status = "stop_pending" if normalized_action == "stop" else "pause_pending"
            next_progress = {
                **progress,
                "action_version": next_version,
                "control_request": {
                    "action": normalized_action,
                    "requested_at": datetime.now(UTC).isoformat(),
                    "action_version": next_version,
                },
            }
            self._connection.execute(
                """
                UPDATE kanglong_runs
                SET status = ?,
                    progress_json = ?,
                    updated_at = ?
                WHERE run_id = ?
                """,
                (next_status, _json_dumps(next_progress), datetime.now(UTC).isoformat(), run_id),
            )
        return {
            "run_id": run_id,
            "status": next_status,
            "progress": next_progress,
            "available_actions": _json_load(row["available_actions_json"], []),
        }

    def remember_kanglong_idempotency(
        self,
        *,
        key: str,
        request_hash: str,
        response: dict[str, Any],
        expires_at: str | None = None,
    ) -> dict[str, Any]:
        with self._lock, self._connection:
            existing = self._connection.execute(
                "SELECT * FROM kanglong_idempotency WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
            if existing is not None:
                stored_response = _json_load(existing["response_json"], {})
                return {
                    "conflict": existing["request_hash"] != request_hash,
                    "response": stored_response,
                }
            now = datetime.now(UTC).isoformat()
            expiry = expires_at or now
            self._connection.execute(
                """
                INSERT INTO kanglong_idempotency (idempotency_key, request_hash, response_json, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (key, request_hash, _json_dumps(response), now, expiry),
            )
        return {"conflict": False, "response": response}

    def get_kanglong_idempotency(self, key: str, request_hash: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            "SELECT * FROM kanglong_idempotency WHERE idempotency_key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        return {
            "conflict": row["request_hash"] != request_hash,
            "response": _json_load(row["response_json"], {}),
        }

    def create_session(self, session: OpenSession) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO sessions (
                    session_id, session_kind, account_id, account_name, symbol, trend_bias, leverage, round_count, round_qty,
                    poll_interval_ms, order_ttl_ms, max_zero_fill_retries, market_fallback_attempts,
                    execution_profile, market_fallback_max_ratio, market_fallback_min_residual_qty,
                    max_reprice_ticks, max_spread_bps, max_reference_deviation_bps,
                    round_interval_seconds, open_mode, close_mode, selected_position_side, target_open_qty, target_close_qty,
                    planned_round_qtys_json, final_round_qty, extension_round_cap_qty, max_extension_rounds, max_session_duration_seconds,
                    created_by, status, created_at, updated_at, last_error,
                    last_error_category, last_error_strategy, last_error_code, last_error_operator_action,
                    last_error_params_json, last_error_raw_message, last_error_contract_version,
                    stage2_carryover_qty, final_alignment_status, final_unaligned_qty, completed_with_final_alignment,
                    session_deadline_at, extension_rounds_used, remaining_extension_rounds, stop_reason, residual_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session.session_id,
                    session.spec.session_kind.value,
                    session.account_id,
                    session.account_name,
                    session.spec.symbol,
                    session.spec.trend_bias.value,
                    session.spec.leverage,
                    session.spec.round_count,
                    str(session.spec.round_qty),
                    session.spec.poll_interval_ms,
                    session.spec.order_ttl_ms,
                    session.spec.max_zero_fill_retries,
                    session.spec.market_fallback_attempts,
                    session.spec.execution_profile.value,
                    str(session.spec.market_fallback_max_ratio),
                    str(session.spec.market_fallback_min_residual_qty),
                    session.spec.max_reprice_ticks,
                    session.spec.max_spread_bps,
                    session.spec.max_reference_deviation_bps,
                    session.spec.round_interval_seconds,
                    session.spec.open_mode.value if session.spec.open_mode else None,
                    session.spec.close_mode.value if session.spec.close_mode else None,
                    session.spec.selected_position_side.value if session.spec.selected_position_side else None,
                    str(session.spec.target_open_qty),
                    str(session.spec.target_close_qty),
                    json.dumps([str(item) for item in session.spec.planned_round_qtys], ensure_ascii=True),
                    str(session.spec.final_round_qty),
                    str(session.spec.extension_round_cap_qty),
                    int(session.spec.max_extension_rounds),
                    int(session.spec.max_session_duration_seconds),
                    session.spec.created_by,
                    session.status.value,
                    session.created_at.isoformat(),
                    session.updated_at.isoformat(),
                    session.last_error,
                    session.last_error_category,
                    session.last_error_strategy,
                    session.last_error_code,
                    session.last_error_operator_action,
                    _json_dumps(session.last_error_params),
                    redact_debug_text(session.last_error_raw_message),
                    session.last_error_contract_version or CONTRACT_VERSION,
                    str(session.stage2_carryover_qty),
                    session.final_alignment_status.value,
                    str(session.final_unaligned_qty),
                    int(session.completed_with_final_alignment),
                    session.session_deadline_at.isoformat() if session.session_deadline_at else None,
                    int(session.extension_rounds_used),
                    int(session.remaining_extension_rounds),
                    session.stop_reason.value if isinstance(session.stop_reason, SessionStopReason) else session.stop_reason,
                    session.residual_source,
                ),
            )

    def update_session_status(
        self,
        session_id: str,
        status: SessionStatus,
        *,
        last_error: str | None = None,
        last_error_category: str | None = None,
        last_error_strategy: str | None = None,
        last_error_code: str | None = None,
        last_error_operator_action: str | None = None,
        last_error_params: dict[str, Any] | None = None,
        last_error_raw_message: str | None = None,
        last_error_contract_version: str | None = None,
        clear_recovery: bool = False,
    ) -> None:
        encoded_error_params = _json_dumps(last_error_params or {})
        redacted_raw_message = redact_debug_text(last_error_raw_message)
        error_contract_version = last_error_contract_version or CONTRACT_VERSION
        with self._lock, self._connection:
            if clear_recovery:
                self._connection.execute(
                    """
                    UPDATE sessions
                    SET status = ?,
                        updated_at = ?,
                        last_error = ?,
                        last_error_category = ?,
                        last_error_strategy = ?,
                        last_error_code = ?,
                        last_error_operator_action = ?,
                        last_error_params_json = ?,
                        last_error_raw_message = ?,
                        last_error_contract_version = ?,
                        recovery_status = NULL,
                        recovery_summary = NULL,
                        recovery_checked_at = NULL,
                        recovery_details_json = NULL
                    WHERE session_id = ?
                    """,
                    (
                        status.value,
                        datetime.now(UTC).isoformat(),
                        last_error,
                        last_error_category,
                        last_error_strategy,
                        last_error_code,
                        last_error_operator_action,
                        encoded_error_params,
                        redacted_raw_message,
                        error_contract_version,
                        session_id,
                    ),
                )
            else:
                self._connection.execute(
                    """
                    UPDATE sessions
                    SET status = ?,
                        updated_at = ?,
                        last_error = ?,
                        last_error_category = ?,
                        last_error_strategy = ?,
                        last_error_code = ?,
                        last_error_operator_action = ?,
                        last_error_params_json = ?,
                        last_error_raw_message = ?,
                        last_error_contract_version = ?
                    WHERE session_id = ?
                    """,
                    (
                        status.value,
                        datetime.now(UTC).isoformat(),
                        last_error,
                        last_error_category,
                        last_error_strategy,
                        last_error_code,
                        last_error_operator_action,
                        encoded_error_params,
                        redacted_raw_message,
                        error_contract_version,
                        session_id,
                    ),
                )

    def update_session_recovery(
        self,
        session_id: str,
        recovery_status: RecoveryStatus | str | None,
        recovery_summary: str | None,
        recovery_checked_at: datetime | str | None,
        recovery_details: dict[str, Any] | None = None,
    ) -> None:
        with self._lock, self._connection:
            status_value = recovery_status.value if isinstance(recovery_status, RecoveryStatus) else recovery_status
            checked_at_value = recovery_checked_at.isoformat() if isinstance(recovery_checked_at, datetime) else recovery_checked_at
            details_value = None if recovery_details is None else _json_dumps(recovery_details)
            self._connection.execute(
                """
                UPDATE sessions
                SET recovery_status = ?,
                    recovery_summary = ?,
                    recovery_checked_at = ?,
                    recovery_details_json = ?,
                    updated_at = ?
                WHERE session_id = ?
                """,
                (
                    status_value,
                    recovery_summary,
                    checked_at_value,
                    details_value,
                    datetime.now(UTC).isoformat(),
                    session_id,
                ),
            )
    def update_session_runtime(self, session: OpenSession) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE sessions
                SET updated_at = ?,
                    stage2_carryover_qty = ?,
                    final_alignment_status = ?,
                    final_unaligned_qty = ?,
                    completed_with_final_alignment = ?,
                    session_deadline_at = ?,
                    extension_rounds_used = ?,
                    remaining_extension_rounds = ?,
                    stop_reason = ?,
                    residual_source = ?
                WHERE session_id = ?
                """,
                (
                    datetime.now(UTC).isoformat(),
                    str(session.stage2_carryover_qty),
                    session.final_alignment_status.value,
                    str(session.final_unaligned_qty),
                    int(session.completed_with_final_alignment),
                    session.session_deadline_at.isoformat() if session.session_deadline_at else None,
                    int(session.extension_rounds_used),
                    int(session.remaining_extension_rounds),
                    session.stop_reason.value if isinstance(session.stop_reason, SessionStopReason) else session.stop_reason,
                    session.residual_source,
                    session.session_id,
                ),
            )

    def upsert_round(self, execution: RoundExecution) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO rounds (
                    session_id, round_index, status, stage1_filled_qty, stage2_filled_qty,
                    stage1_zero_fill_retries, stage2_zero_fill_retries, market_fallback_used,
                    notes_json, started_at, ended_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id, round_index) DO UPDATE SET
                    status = excluded.status,
                    stage1_filled_qty = excluded.stage1_filled_qty,
                    stage2_filled_qty = excluded.stage2_filled_qty,
                    stage1_zero_fill_retries = excluded.stage1_zero_fill_retries,
                    stage2_zero_fill_retries = excluded.stage2_zero_fill_retries,
                    market_fallback_used = excluded.market_fallback_used,
                    notes_json = excluded.notes_json,
                    started_at = excluded.started_at,
                    ended_at = excluded.ended_at
                """,
                (
                    execution.session_id,
                    execution.round_index,
                    execution.status.value,
                    str(execution.stage1_filled_qty),
                    str(execution.stage2_filled_qty),
                    execution.stage1_zero_fill_retries,
                    execution.stage2_zero_fill_retries,
                    int(execution.market_fallback_used),
                    _json_dumps(execution.notes),
                    execution.started_at.isoformat(),
                    execution.ended_at.isoformat() if execution.ended_at else None,
                ),
            )

    def add_event(
        self,
        session_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        round_index: int | None = None,
        created_at: datetime | None = None,
    ) -> None:
        event_created_at = (created_at or datetime.now(UTC)).isoformat()
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT INTO events (session_id, round_index, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
                (session_id, round_index, event_type, _json_dumps(payload), event_created_at),
            )
            self._prune_session_events_locked(session_id)

    def prune_event_retention(self, *, now: datetime | None = None) -> None:
        cutoff_iso = None
        if self._session_event_retention_days > 0:
            cutoff_iso = ((now or datetime.now(UTC)) - timedelta(days=self._session_event_retention_days)).isoformat()
        with self._lock, self._connection:
            if cutoff_iso is not None:
                self._connection.execute(
                    "DELETE FROM events WHERE created_at < ?",
                    (cutoff_iso,),
                )
            if self._session_event_retention_per_session > 0:
                rows = self._connection.execute(
                    "SELECT DISTINCT session_id FROM events"
                ).fetchall()
                for row in rows:
                    self._prune_session_events_locked(row["session_id"])

    def _prune_session_events_locked(self, session_id: str) -> None:
        if self._session_event_retention_days > 0:
            cutoff_iso = (datetime.now(UTC) - timedelta(days=self._session_event_retention_days)).isoformat()
            self._connection.execute(
                "DELETE FROM events WHERE session_id = ? AND created_at < ?",
                (session_id, cutoff_iso),
            )
        if self._session_event_retention_per_session <= 0:
            return
        self._connection.execute(
            """
            DELETE FROM events
            WHERE session_id = ?
              AND event_id NOT IN (
                  SELECT event_id FROM (
                      SELECT event_id
                      FROM events
                      WHERE session_id = ?
                      ORDER BY event_id DESC
                      LIMIT ?
                  )
              )
            """,
            (session_id, session_id, self._session_event_retention_per_session),
        )

    def get_session_record(self, session_id: str, account_id: str | None = None) -> dict[str, Any] | None:
        if account_id is None:
            row = self._connection.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,)).fetchone()
        else:
            row = self._connection.execute(
                "SELECT * FROM sessions WHERE session_id = ? AND account_id = ?",
                (session_id, account_id),
            ).fetchone()
        if row is None:
            return None
        return self._deserialize_session_row(row)

    def get_session(self, session_id: str, account_id: str | None = None) -> dict[str, Any] | None:
        session = self.get_session_record(session_id, account_id)
        if session is None:
            return None
        session["rounds"] = self.list_rounds(session_id)
        session["events"] = self.list_events(session_id)
        return session


    def list_incomplete_sessions(self, account_id: str | None = None) -> list[dict[str, Any]]:
        params: tuple[Any, ...]
        if account_id is None:
            query = "SELECT * FROM sessions WHERE status IN (?, ?, ?) ORDER BY created_at ASC"
            params = (
                SessionStatus.PENDING.value,
                SessionStatus.RUNNING.value,
                SessionStatus.PAUSED.value,
            )
        else:
            query = "SELECT * FROM sessions WHERE account_id = ? AND status IN (?, ?, ?) ORDER BY created_at ASC"
            params = (
                account_id,
                SessionStatus.PENDING.value,
                SessionStatus.RUNNING.value,
                SessionStatus.PAUSED.value,
            )
        rows = self._connection.execute(query, params).fetchall()
        return [self._deserialize_session_row(row) for row in rows]
    def fail_incomplete_sessions(self, reason: str) -> list[str]:
        with self._lock, self._connection:
            rows = self._connection.execute(
                "SELECT session_id FROM sessions WHERE status IN (?, ?, ?)",
                (
                    SessionStatus.PENDING.value,
                    SessionStatus.RUNNING.value,
                    SessionStatus.PAUSED.value,
                ),
            ).fetchall()
            session_ids = [row["session_id"] for row in rows]
            if not session_ids:
                return []
            now = datetime.now(UTC).isoformat()
            self._connection.executemany(
                """
                UPDATE sessions
                SET status = ?,
                    updated_at = ?,
                    last_error = ?,
                    last_error_category = NULL,
                    last_error_strategy = NULL,
                    last_error_code = NULL,
                    last_error_operator_action = NULL,
                    last_error_params_json = '{}',
                    last_error_raw_message = NULL,
                    last_error_contract_version = ?
                WHERE session_id = ?
                """,
                [(SessionStatus.EXCEPTION.value, now, reason, CONTRACT_VERSION, session_id) for session_id in session_ids],
            )
            return session_ids

    def list_sessions(self, account_id: str | None = None) -> list[dict[str, Any]]:
        if account_id is None:
            rows = self._connection.execute("SELECT * FROM sessions ORDER BY created_at DESC").fetchall()
        else:
            rows = self._connection.execute(
                "SELECT * FROM sessions WHERE account_id = ? ORDER BY created_at DESC",
                (account_id,),
            ).fetchall()
        return [self._deserialize_session_row(row) for row in rows]

    def list_rounds(self, session_id: str) -> list[dict[str, Any]]:
        rows = self._connection.execute(
            "SELECT * FROM rounds WHERE session_id = ? ORDER BY round_index ASC",
            (session_id,),
        ).fetchall()
        return [self._deserialize_round_row(row) for row in rows]

    def list_rounds_by_indexes(self, session_id: str, round_indexes: list[int] | set[int]) -> list[dict[str, Any]]:
        normalized_indexes = sorted({int(round_index) for round_index in round_indexes})
        if not normalized_indexes:
            return []
        placeholders = ",".join("?" for _ in normalized_indexes)
        rows = self._connection.execute(
            f"SELECT * FROM rounds WHERE session_id = ? AND round_index IN ({placeholders}) ORDER BY round_index ASC",
            (session_id, *normalized_indexes),
        ).fetchall()
        return [self._deserialize_round_row(row) for row in rows]

    def list_events(self, session_id: str, after_event_id: int | None = None) -> list[dict[str, Any]]:
        if after_event_id is None:
            rows = self._connection.execute(
                "SELECT * FROM events WHERE session_id = ? ORDER BY event_id ASC",
                (session_id,),
            ).fetchall()
        else:
            rows = self._connection.execute(
                "SELECT * FROM events WHERE session_id = ? AND event_id > ? ORDER BY event_id ASC",
                (session_id, int(after_event_id)),
            ).fetchall()
        return [self._deserialize_event_row(row) for row in rows]

    def latest_event_id(self, session_id: str) -> int:
        row = self._connection.execute(
            "SELECT COALESCE(MAX(event_id), 0) AS latest_event_id FROM events WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        return int(row["latest_event_id"] or 0) if row is not None else 0

    def has_active_symbol_session(self, symbol: str, account_id: str | None = None) -> bool:
        if account_id is None:
            row = self._connection.execute(
                "SELECT 1 FROM sessions WHERE symbol = ? AND status IN (?, ?, ?) LIMIT 1",
                (
                    symbol,
                    SessionStatus.PENDING.value,
                    SessionStatus.RUNNING.value,
                    SessionStatus.PAUSED.value,
                ),
            ).fetchone()
        else:
            row = self._connection.execute(
                "SELECT 1 FROM sessions WHERE symbol = ? AND account_id = ? AND status IN (?, ?, ?) LIMIT 1",
                (
                    symbol,
                    account_id,
                    SessionStatus.PENDING.value,
                    SessionStatus.RUNNING.value,
                    SessionStatus.PAUSED.value,
                ),
            ).fetchone()
        return row is not None

    def has_active_sessions(self, account_id: str | None = None) -> bool:
        if account_id is None:
            row = self._connection.execute(
                "SELECT 1 FROM sessions WHERE status IN (?, ?, ?) LIMIT 1",
                (
                    SessionStatus.PENDING.value,
                    SessionStatus.RUNNING.value,
                    SessionStatus.PAUSED.value,
                ),
            ).fetchone()
        else:
            row = self._connection.execute(
                "SELECT 1 FROM sessions WHERE account_id = ? AND status IN (?, ?, ?) LIMIT 1",
                (
                    account_id,
                    SessionStatus.PENDING.value,
                    SessionStatus.RUNNING.value,
                    SessionStatus.PAUSED.value,
                ),
            ).fetchone()
        return row is not None

    def _deserialize_round_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = dict(row)
        payload["notes"] = _json_load(payload.pop("notes_json", "{}"), {})
        payload["market_fallback_used"] = bool(payload.get("market_fallback_used"))
        return payload

    def _deserialize_event_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = dict(row)
        payload["payload"] = _json_load(payload.pop("payload_json", "{}"), {})
        return payload

    def _deserialize_kanglong_event_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return self._deserialize_event_row(row)

    def _deserialize_kanglong_run_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = dict(row)
        payload["subaccount_ids"] = _json_load(payload.pop("subaccount_ids_json", "[]"), [])
        payload["request"] = _json_load(payload.pop("request_json", "{}"), {})
        payload["plan"] = _json_load(payload.pop("plan_json", "{}"), {})
        payload["report"] = _json_load(payload.pop("report_json", "{}"), {})
        payload["available_actions"] = _json_load(payload.pop("available_actions_json", "[]"), [])
        payload["progress"] = _json_load(payload.pop("progress_json", "{}"), {})
        report_summary = _json_load(payload.pop("report_summary_json", "{}"), {})
        if not report_summary:
            report_summary = _kanglong_report_summary({}, payload["report"])
        payload["report_summary"] = report_summary
        if int(payload.get("engine_version") or 1) < 2:
            payload["status"] = "legacy_readonly"
            payload["available_actions"] = ["refresh_plan", "view_report"]
        return payload

    def _deserialize_session_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = dict(row)
        payload["completed_with_final_alignment"] = bool(payload.get("completed_with_final_alignment"))
        payload["last_error_params"] = _json_load(payload.pop("last_error_params_json", "{}"), {})
        recovery_status = payload.get("recovery_status")
        if recovery_status:
            payload["recovery_status"] = RecoveryStatus(recovery_status)
        details_payload = _json_load(payload.pop("recovery_details_json", "{}"), {})
        payload["recovery_details"] = details_payload
        payload["planned_round_qtys"] = _json_load(payload.pop("planned_round_qtys_json", "[]"), [])
        stop_reason = payload.get("stop_reason")
        if stop_reason:
            payload["stop_reason"] = SessionStopReason(stop_reason)
        return payload







