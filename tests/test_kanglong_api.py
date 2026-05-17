from __future__ import annotations

from decimal import Decimal

import pytest

from paired_opener import api as api_module
from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.config import KanglongSymbolConfig
from paired_opener.kanglong.service import KanglongSimulationService
from paired_opener.schemas import KanglongSimulationRunRequest
from paired_opener.storage import SqliteRepository
from tests.test_kanglong_precheck import snapshot


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


def test_kanglong_service_report_contains_plan_events_and_costs(tmp_path) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    service = KanglongSimulationService(repository)

    try:
        service.create_draft_run(
            run_id="run-1",
            symbol="ETHUSDC",
            main_account_id="main",
            subaccount_ids=["sub1", "sub2"],
        )

        payload = service.simulate(
            run_id="run-1",
            symbol="ETHUSDC",
            main_snapshot=snapshot("main", "0", "0", "0", "0"),
            subaccount_snapshots=[
                snapshot("sub1", "1", "1", "100", "0"),
                snapshot("sub2", "1", "1", "80", "0"),
            ],
            selected_side=PositionSide.LONG,
            config=KanglongSymbolConfig(per_round_qty_limit=Decimal("0.25")),
            rules=SymbolRules("ETHUSDC", Decimal("0.01"), Decimal("0.001"), Decimal("0.001"), Decimal("5"), 125),
            close_price=Decimal("3100.00"),
            open_price=Decimal("3100.50"),
            fee_rate=Decimal("0.0005"),
        )

        stored = repository.get_kanglong_run("run-1")
    finally:
        repository.close()

    assert payload["result_grade"] == "safe_closed"
    assert payload["report"]["selected_side"] == "LONG"
    assert payload["report"]["groups"][0]["from_account_id"] == "sub1"
    assert payload["report"]["groups"][0]["to_account_id"] == "main"
    assert payload["report"]["costs"]["transfer_fee_cost"] != "0"
    assert stored is not None
    assert stored["status"] == "completed"
    assert stored["report"]["groups"][0]["from_account_id"] == "sub1"
