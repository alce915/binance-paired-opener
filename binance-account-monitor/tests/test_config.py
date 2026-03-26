from __future__ import annotations

import json
from pathlib import Path

import pytest

from monitor_app.config import Settings


def test_load_monitor_accounts_from_hierarchical_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "binance_monitor_accounts.json").write_text(
        json.dumps(
            {
                "main_accounts": [
                    {
                        "main_id": "group_a",
                        "name": "Group A",
                        "children": [
                            {
                                "account_id": "sub1",
                                "name": "Sub One",
                                "api_key": "k1",
                                "api_secret": "s1",
                            },
                            {
                                "account_id": "sub2",
                                "name": "Sub Two",
                                "api_key": "k2",
                                "api_secret": "s2",
                                "use_testnet": True,
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = Settings(_env_file=None)
    settings.load_monitor_accounts()

    assert set(settings.monitor_accounts) == {"group_a.sub1", "group_a.sub2"}
    assert settings.monitor_accounts["group_a.sub1"].display_name == "Group A / Sub One"
    assert settings.monitor_accounts["group_a.sub2"].use_testnet is True
    assert settings.monitor_main_accounts["group_a"].name == "Group A"


def test_load_monitor_accounts_rejects_duplicate_main_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "binance_monitor_accounts.json").write_text(
        json.dumps(
            {
                "main_accounts": [
                    {"main_id": "group_a", "name": "A", "children": [{"account_id": "sub1", "name": "One", "api_key": "k1", "api_secret": "s1"}]},
                    {"main_id": "group_a", "name": "B", "children": [{"account_id": "sub2", "name": "Two", "api_key": "k2", "api_secret": "s2"}]},
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = Settings(_env_file=None)

    with pytest.raises(ValueError, match="Duplicate main_id"):
        settings.load_monitor_accounts()