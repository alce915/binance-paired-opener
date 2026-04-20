from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from paired_opener.domain import OpenSession, RecoveryStatus, RoundExecution, SessionStatus


def _json_dumps(payload: dict[str, Any]) -> str:
    def encode(value: Any) -> Any:
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        if hasattr(value, "value"):
            return value.value
        return value

    return json.dumps(payload, default=encode, ensure_ascii=True, sort_keys=True)


def _json_loads(payload: str) -> dict[str, Any]:
    try:
        return json.loads(payload) if payload else {}
    except json.JSONDecodeError:
        return {}


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
        with self._connection:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    session_kind TEXT NOT NULL DEFAULT 'paired_open',
                    account_id TEXT NOT NULL DEFAULT 'default',
                    account_name TEXT NOT NULL DEFAULT '默认账户',
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
                    created_by TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_error TEXT,
                    last_error_category TEXT,
                    last_error_strategy TEXT,
                    last_error_code TEXT,
                    last_error_operator_action TEXT,
                    recovery_status TEXT,
                    recovery_summary TEXT,
                    recovery_checked_at TEXT,
                    recovery_details_json TEXT,
                    stage2_carryover_qty TEXT NOT NULL DEFAULT '0',
                    final_alignment_status TEXT NOT NULL DEFAULT 'not_needed',
                    final_unaligned_qty TEXT NOT NULL DEFAULT '0',
                    completed_with_final_alignment INTEGER NOT NULL DEFAULT 0
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
                """
            )
            self._ensure_column("sessions", "session_kind", "TEXT NOT NULL DEFAULT 'paired_open'")
            self._ensure_column("sessions", "account_id", "TEXT NOT NULL DEFAULT 'default'")
            self._ensure_column("sessions", "account_name", "TEXT NOT NULL DEFAULT '默认账户'")
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
            self._ensure_column("sessions", "stage2_carryover_qty", "TEXT NOT NULL DEFAULT '0'")
            self._ensure_column("sessions", "final_alignment_status", "TEXT NOT NULL DEFAULT 'not_needed'")
            self._ensure_column("sessions", "final_unaligned_qty", "TEXT NOT NULL DEFAULT '0'")
            self._ensure_column("sessions", "completed_with_final_alignment", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("sessions", "last_error_category", "TEXT")
            self._ensure_column("sessions", "last_error_strategy", "TEXT")
            self._ensure_column("sessions", "last_error_code", "TEXT")
            self._ensure_column("sessions", "last_error_operator_action", "TEXT")
            self._ensure_column("sessions", "recovery_status", "TEXT")
            self._ensure_column("sessions", "recovery_summary", "TEXT")
            self._ensure_column("sessions", "recovery_checked_at", "TEXT")
            self._ensure_column("sessions", "recovery_details_json", "TEXT")

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        columns = {row["name"] for row in self._connection.execute(f"PRAGMA table_info({table})").fetchall()}
        if column in columns:
            return
        self._connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def create_session(self, session: OpenSession) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO sessions (
                    session_id, session_kind, account_id, account_name, symbol, trend_bias, leverage, round_count, round_qty,
                    poll_interval_ms, order_ttl_ms, max_zero_fill_retries, market_fallback_attempts,
                    execution_profile, market_fallback_max_ratio, market_fallback_min_residual_qty,
                    max_reprice_ticks, max_spread_bps, max_reference_deviation_bps,
                    round_interval_seconds, open_mode, close_mode, selected_position_side, target_open_qty, target_close_qty, created_by, status, created_at, updated_at, last_error,
                    last_error_category, last_error_strategy, last_error_code, last_error_operator_action,
                    stage2_carryover_qty, final_alignment_status, final_unaligned_qty, completed_with_final_alignment
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    session.spec.created_by,
                    session.status.value,
                    session.created_at.isoformat(),
                    session.updated_at.isoformat(),
                    session.last_error,
                    session.last_error_category,
                    session.last_error_strategy,
                    session.last_error_code,
                    session.last_error_operator_action,
                    str(session.stage2_carryover_qty),
                    session.final_alignment_status.value,
                    str(session.final_unaligned_qty),
                    int(session.completed_with_final_alignment),
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
        clear_recovery: bool = False,
    ) -> None:
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
                        last_error_operator_action = ?
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
                    completed_with_final_alignment = ?
                WHERE session_id = ?
                """,
                (
                    datetime.now(UTC).isoformat(),
                    str(session.stage2_carryover_qty),
                    session.final_alignment_status.value,
                    str(session.final_unaligned_qty),
                    int(session.completed_with_final_alignment),
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
                    last_error_operator_action = NULL
                WHERE session_id = ?
                """,
                [(SessionStatus.EXCEPTION.value, now, reason, session_id) for session_id in session_ids],
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
        payload["notes"] = _json_loads(payload.pop("notes_json", "{}"))
        payload["market_fallback_used"] = bool(payload.get("market_fallback_used"))
        return payload

    def _deserialize_event_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = dict(row)
        payload["payload"] = _json_loads(payload.pop("payload_json", "{}"))
        return payload

    def _deserialize_session_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = dict(row)
        payload["completed_with_final_alignment"] = bool(payload.get("completed_with_final_alignment"))
        recovery_status = payload.get("recovery_status")
        if recovery_status:
            payload["recovery_status"] = RecoveryStatus(recovery_status)
        details_payload = _json_loads(payload.pop("recovery_details_json", "{}"))
        payload["recovery_details"] = details_payload
        return payload







