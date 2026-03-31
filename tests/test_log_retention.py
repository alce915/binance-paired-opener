from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from paired_opener.log_retention import rotate_log_file
from paired_opener.storage import SqliteRepository


def test_repository_prunes_session_events_by_count(tmp_path: Path) -> None:
    repository = SqliteRepository(
        tmp_path / "events-count.db",
        session_event_retention_days=30,
        session_event_retention_per_session=3,
    )

    for index in range(5):
        repository.add_event("session-a", "tick", {"index": index})

    events = repository.list_events("session-a")

    assert len(events) == 3
    assert [event["payload"]["index"] for event in events] == [2, 3, 4]


def test_repository_prunes_session_events_by_age(tmp_path: Path) -> None:
    repository = SqliteRepository(
        tmp_path / "events-age.db",
        session_event_retention_days=30,
        session_event_retention_per_session=10,
    )
    now = datetime.now(UTC)

    repository.add_event(
        "session-a",
        "old",
        {"index": 1},
        created_at=now - timedelta(days=31),
    )
    repository.add_event(
        "session-a",
        "fresh",
        {"index": 2},
        created_at=now - timedelta(days=1),
    )

    repository.prune_event_retention(now=now)
    events = repository.list_events("session-a")

    assert len(events) == 1
    assert events[0]["payload"]["index"] == 2


def test_rotate_log_file_rolls_existing_backups(tmp_path: Path) -> None:
    log_file = tmp_path / "api.runtime.log"
    log_file.write_text("current", encoding="utf-8")
    (tmp_path / "api.runtime.log.1").write_text("older-1", encoding="utf-8")
    (tmp_path / "api.runtime.log.2").write_text("older-2", encoding="utf-8")

    rotate_log_file(log_file, max_bytes=4, backup_count=2)

    assert not log_file.exists()
    assert (tmp_path / "api.runtime.log.1").read_text(encoding="utf-8") == "current"
    assert (tmp_path / "api.runtime.log.2").read_text(encoding="utf-8") == "older-1"


def test_rotate_log_file_is_noop_below_threshold(tmp_path: Path) -> None:
    log_file = tmp_path / "monitor.runtime.log"
    log_file.write_text("small", encoding="utf-8")

    rotate_log_file(log_file, max_bytes=100, backup_count=5)

    assert log_file.read_text(encoding="utf-8") == "small"
    assert not (tmp_path / "monitor.runtime.log.1").exists()
