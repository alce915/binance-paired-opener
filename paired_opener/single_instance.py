from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO


class ServiceInstanceAlreadyRunning(RuntimeError):
    """Another service process owns the data-directory lock."""


class SingleInstanceGuard:
    def __init__(self, path: Path, handle: BinaryIO) -> None:
        self.path = path
        self._handle = handle
        self._closed = False

    @classmethod
    def acquire(cls, data_dir: Path) -> "SingleInstanceGuard":
        resolved = Path(data_dir).resolve()
        resolved.mkdir(parents=True, exist_ok=True)
        path = resolved / ".paired-opener.instance.lock"
        handle = path.open("a+b")
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
                os.fsync(handle.fileno())
            handle.seek(0)
            _lock_handle(handle)
            handle.seek(0)
            handle.truncate()
            handle.write(str(os.getpid()).encode("ascii"))
            handle.flush()
        except OSError as exc:
            handle.close()
            raise ServiceInstanceAlreadyRunning("service data directory is already in use") from exc
        return cls(path, handle)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._handle.seek(0)
            _unlock_handle(self._handle)
        finally:
            self._handle.close()

    def __enter__(self) -> "SingleInstanceGuard":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def _lock_handle(handle: BinaryIO) -> None:
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_handle(handle: BinaryIO) -> None:
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def require_single_worker(workers: int | None = None) -> int:
    configured = workers
    if configured is None:
        raw = os.getenv("WEB_CONCURRENCY", "1").strip() or "1"
        try:
            configured = int(raw)
        except ValueError as exc:
            raise RuntimeError("WEB_CONCURRENCY must be an integer") from exc
    if configured != 1:
        raise RuntimeError("paired opener requires workers=1")
    return 1
