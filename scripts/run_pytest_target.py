from __future__ import annotations

import argparse
from pathlib import Path

import pytest
import _pytest.pathlib
import _pytest.tmpdir


def _patched_getbasetemp(self):  # type: ignore[no-untyped-def]
    if self._basetemp is not None:
        return self._basetemp
    if self._given_basetemp is not None:
        basetemp = self._given_basetemp
        basetemp.mkdir(parents=True, exist_ok=True)
        self._basetemp = basetemp
        return basetemp
    return _ORIGINAL_GETBASETEMP(self)


def _patched_make_numbered_dir(root: Path, prefix: str, mode: int = 0o700) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    for _ in range(100):
        existing_numbers: list[int] = []
        for entry in root.iterdir():
            if not entry.name.lower().startswith(prefix.lower()):
                continue
            suffix = entry.name[len(prefix) :]
            if suffix.isdigit():
                existing_numbers.append(int(suffix))
        new_number = (max(existing_numbers) + 1) if existing_numbers else 0
        new_path = root / f"{prefix}{new_number}"
        try:
            new_path.mkdir(parents=False, exist_ok=False)
        except FileExistsError:
            continue
        return new_path
    raise OSError(f"could not create numbered dir with prefix {prefix} in {root}")


def _disable_cleanup(_root: Path) -> None:
    return None


_ORIGINAL_GETBASETEMP = _pytest.tmpdir.TempPathFactory.getbasetemp
_pytest.tmpdir.TempPathFactory.getbasetemp = _patched_getbasetemp
_pytest.pathlib.make_numbered_dir = _patched_make_numbered_dir
_pytest.tmpdir.make_numbered_dir = _patched_make_numbered_dir
_pytest.pathlib.cleanup_dead_symlinks = _disable_cleanup
_pytest.tmpdir.cleanup_dead_symlinks = _disable_cleanup


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a pytest target with Windows-safe tempdir patching.")
    parser.add_argument("target", help="pytest target path")
    parser.add_argument("--basetemp", required=True, help="base temp directory for pytest tmp_path")
    parser.add_argument("extra", nargs="*", help="extra pytest args")
    args = parser.parse_args()

    pytest_args = ["-q", "--basetemp", args.basetemp, "-p", "no:cacheprovider", args.target, *args.extra]
    return int(pytest.main(pytest_args))


if __name__ == "__main__":
    raise SystemExit(main())
