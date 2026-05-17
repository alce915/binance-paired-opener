from __future__ import annotations

import pytest

from paired_opener import api as api_module
from paired_opener.schemas import KanglongSimulationRunRequest


def test_kanglong_request_defaults_to_ethusdc_and_auto_side() -> None:
    request = KanglongSimulationRunRequest(main_account_id="main", subaccount_ids=["sub1"])

    assert request.symbol == "ETHUSDC"
    assert request.selected_side is None
    assert request.mode == "simulation"


class StubKanglongService:
    def __init__(self) -> None:
        self.runs: dict[str, dict] = {}

    def create_draft_run(self, *, run_id: str, symbol: str, main_account_id: str, subaccount_ids: list[str]) -> dict:
        payload = {
            "run_id": run_id,
            "symbol": symbol,
            "main_account_id": main_account_id,
            "subaccount_ids": subaccount_ids,
            "status": "draft_plan",
        }
        self.runs[run_id] = payload
        return payload

    def get_run(self, run_id: str) -> dict | None:
        return self.runs.get(run_id)


@pytest.mark.asyncio
async def test_kanglong_api_creates_simulation_only_draft_run() -> None:
    service = StubKanglongService()
    api_module.app.state.kanglong_service = service

    response = await api_module.run_kanglong_simulation(
        KanglongSimulationRunRequest(main_account_id="main", subaccount_ids=["sub1", "sub2"])
    )
    stored = await api_module.get_kanglong_simulation(response.run_id)

    assert response.status == "draft_plan"
    assert response.result_grade is None
    assert stored is not None
    assert stored["main_account_id"] == "main"
    assert stored["subaccount_ids"] == ["sub1", "sub2"]
