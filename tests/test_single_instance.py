from __future__ import annotations

from pathlib import Path

import pytest

from paired_opener.single_instance import ServiceInstanceAlreadyRunning, SingleInstanceGuard


def test_second_instance_for_same_data_directory_fails_closed(tmp_path: Path) -> None:
    first = SingleInstanceGuard.acquire(tmp_path)
    try:
        with pytest.raises(ServiceInstanceAlreadyRunning):
            SingleInstanceGuard.acquire(tmp_path)
    finally:
        first.close()


def test_lock_is_released_when_guard_closes(tmp_path: Path) -> None:
    first = SingleInstanceGuard.acquire(tmp_path)
    lock_path = first.path
    first.close()
    second = SingleInstanceGuard.acquire(tmp_path)
    try:
        assert second.path == lock_path
    finally:
        second.close()


def test_different_data_directories_do_not_conflict(tmp_path: Path) -> None:
    first = SingleInstanceGuard.acquire(tmp_path / "one")
    second = SingleInstanceGuard.acquire(tmp_path / "two")
    first.close()
    second.close()
