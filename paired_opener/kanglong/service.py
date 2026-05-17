from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from paired_opener.kanglong.models import KanglongRunStatus
from paired_opener.storage import SqliteRepository


def _now_text() -> str:
    return datetime.now(UTC).isoformat()


class KanglongSimulationService:
    def __init__(self, repository: SqliteRepository) -> None:
        self._repository = repository

    def create_draft_run(
        self,
        *,
        run_id: str,
        symbol: str,
        main_account_id: str,
        subaccount_ids: list[str],
    ) -> dict[str, Any]:
        payload = {
            "run_id": run_id,
            "symbol": symbol,
            "main_account_id": main_account_id,
            "subaccount_ids": subaccount_ids,
            "status": KanglongRunStatus.DRAFT_PLAN.value,
            "created_at": _now_text(),
            "updated_at": _now_text(),
        }
        self._repository.create_kanglong_run(payload)
        return payload

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self._repository.get_kanglong_run(run_id)
