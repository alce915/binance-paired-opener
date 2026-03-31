from __future__ import annotations

import argparse
from pathlib import Path

from paired_opener.config import Settings


def rotate_log_file(path: Path, *, max_bytes: int, backup_count: int) -> None:
    if max_bytes <= 0 or backup_count <= 0:
        return
    if not path.exists() or path.stat().st_size <= max_bytes:
        return

    oldest = path.with_name(f"{path.name}.{backup_count}")
    if oldest.exists():
        oldest.unlink()

    for index in range(backup_count - 1, 0, -1):
        source = path.with_name(f"{path.name}.{index}")
        if not source.exists():
            continue
        target = path.with_name(f"{path.name}.{index + 1}")
        if target.exists():
            target.unlink()
        source.replace(target)

    first_backup = path.with_name(f"{path.name}.1")
    if first_backup.exists():
        first_backup.unlink()
    path.replace(first_backup)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Rotate runtime log files when they exceed a size threshold.')
    parser.add_argument('--file', required=True, help='Path to the runtime log file.')
    parser.add_argument('--max-bytes', type=int, default=None, help='Maximum size in bytes before rotation.')
    parser.add_argument('--backup-count', type=int, default=None, help='Number of rotated backups to keep.')
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    settings = Settings()
    rotate_log_file(
        Path(args.file),
        max_bytes=args.max_bytes or settings.runtime_log_max_bytes,
        backup_count=args.backup_count or settings.runtime_log_backup_count,
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
