from __future__ import annotations

import json
import os
import uuid
from decimal import Decimal
from pathlib import Path

from pydantic import BaseModel, Field

from paired_opener.domain import PositionSide


class KanglongBatchDefaults(BaseModel):
    symbol: str = Field(default="ETHUSDC", min_length=1, max_length=32)
    preferred_side: PositionSide = PositionSide.LONG
    leverage: int = Field(default=100, ge=1, le=125)
    per_leg_notional: Decimal = Field(default=Decimal("250000"), gt=0)
    round_count: int = Field(default=30, ge=1, le=500)
    round_interval_seconds: int = Field(default=3, ge=0, le=3600)


class KanglongBatchDefaultsStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def load(self) -> KanglongBatchDefaults:
        if not self.path.exists():
            return KanglongBatchDefaults()
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return KanglongBatchDefaults.model_validate(payload)

    def save(self, defaults: KanglongBatchDefaults) -> KanglongBatchDefaults:
        normalized = defaults.model_copy(update={"symbol": defaults.symbol.strip().upper()})
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = Path(f"{self.path}.tmp.{uuid.uuid4().hex}")
        try:
            encoded = json.dumps(
                normalized.model_dump(mode="json"),
                ensure_ascii=False,
                indent=2,
            ).encode("utf-8")
            with temp_path.open("wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_path, self.path)
        finally:
            temp_path.unlink(missing_ok=True)
        return normalized
