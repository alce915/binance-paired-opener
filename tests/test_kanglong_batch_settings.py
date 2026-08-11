from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from paired_opener.domain import PositionSide
from paired_opener.kanglong.batch_settings import KanglongBatchDefaults, KanglongBatchDefaultsStore


def test_defaults_round_trip_utf8(tmp_path: Path) -> None:
    store = KanglongBatchDefaultsStore(tmp_path / "defaults.json")
    saved = store.save(
        KanglongBatchDefaults(
            symbol="ETHUSDC",
            preferred_side=PositionSide.LONG,
            leverage=100,
            per_leg_notional=Decimal("250000"),
        )
    )
    loaded = store.load()
    assert loaded == saved
    assert loaded.leverage == 100
    assert "ETHUSDC" in store.path.read_text(encoding="utf-8")


def test_missing_defaults_file_uses_production_defaults(tmp_path: Path) -> None:
    defaults = KanglongBatchDefaultsStore(tmp_path / "missing.json").load()
    assert defaults.symbol == "ETHUSDC"
    assert defaults.leverage == 100
    assert defaults.per_leg_notional == Decimal("250000")
    assert defaults.round_count == 30
