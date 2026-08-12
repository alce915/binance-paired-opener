from __future__ import annotations

import fnmatch
import tomllib
from pathlib import Path


def test_all_web_assets_are_included_in_wheel_package_data() -> None:
    root = Path(__file__).resolve().parents[1]
    config = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    patterns = config["tool"]["setuptools"]["package-data"]["paired_opener"]
    assets = [path.relative_to(root / "paired_opener").as_posix() for path in (root / "paired_opener" / "static").iterdir() if path.is_file()]

    missing = [
        asset
        for asset in assets
        if not any(fnmatch.fnmatchcase(asset, pattern) for pattern in patterns)
    ]
    assert missing == []
