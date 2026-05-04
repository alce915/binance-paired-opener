from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from paired_opener import api as api_module
from paired_opener.domain import PositionSide, SessionKind, SingleOpenMode
from paired_opener.schemas import SimulationAccountSettingsRequest, SimulationRunRequest, SimulationTemplateRequest
from paired_opener.simulation import SimulationError


class StubRealService:
    def __init__(self, active: bool = False) -> None:
        self.active = active

    def has_active_sessions(self) -> bool:
        return self.active


class StubSimulationService:
    def __init__(self) -> None:
        self.active = False
        self.last_run = None
        self.last_settings = None
        self.fail_reset = False
        self.fail_clear = False

    def is_active(self) -> bool:
        return self.active

    async def get_account(self) -> dict:
        return {"totals": {"wallet_balance": "7000"}}

    async def update_account_settings(self, **kwargs) -> dict:
        self.last_settings = kwargs
        return {"settings": kwargs}

    async def reset_account(self) -> dict:
        if self.fail_reset:
            raise SimulationError("simulation is running")
        return {"run_id": "reset-run"}

    async def run(self, request: SimulationRunRequest) -> dict:
        self.last_run = request
        return {"run_id": "sim-run", "status": "completed", "stop_reason": "filled"}

    async def start_run(self, request: SimulationRunRequest) -> dict:
        self.last_run = request
        return {"run_id": "sim-run", "status": "running", "stage": "starting", "requested": True}

    async def abort(self) -> dict:
        return {"status": "aborting", "requested": True, "requested_action": "abort"}

    async def active_run(self) -> dict:
        return {"active": True, "run_id": "sim-run", "status": "running", "stage": "running"}

    async def run_updates(self, run_id: str, *, after_event_id: int = 0) -> dict:
        return {"run_id": run_id, "events": [], "latest_event_id": after_event_id, "account": {}}

    async def list_history(self, *, page: int, page_size: int) -> dict:
        return {"page": page, "page_size": page_size, "items": []}

    async def clear_history(self) -> dict:
        if self.fail_clear:
            raise SimulationError("simulation is running")
        return {"cleared": True}

    async def get_history_detail(self, run_id: str) -> dict:
        return {"run_id": run_id}

    async def rerun(self, run_id: str) -> dict:
        return {"run_id": "rerun", "rerun_source_run_id": run_id}

    async def start_rerun(self, run_id: str) -> dict:
        return {"run_id": "rerun", "status": "running", "rerun_source_run_id": run_id}

    async def export_history_csv(self) -> str:
        return "run_id,status\n"

    async def list_templates(self) -> dict:
        return {"items": []}

    async def save_template(self, *, name: str, payload: dict) -> dict:
        return {"template_id": "template", "name": name, "payload": payload}

    async def delete_template(self, template_id: str) -> dict:
        return {"deleted": True, "template_id": template_id}


def install_runtime(monkeypatch: pytest.MonkeyPatch, *, active_real: bool = False) -> StubSimulationService:
    simulation = StubSimulationService()
    runtime = SimpleNamespace(service=StubRealService(active_real), simulation=simulation)
    monkeypatch.setattr(api_module, "current_runtime", lambda _app: runtime)
    return simulation


@pytest.mark.asyncio
async def test_simulation_api_routes_delegate_to_simulation_service(monkeypatch: pytest.MonkeyPatch) -> None:
    simulation = install_runtime(monkeypatch)

    account = await api_module.get_simulation_account()
    settings = await api_module.update_simulation_account_settings(
        SimulationAccountSettingsRequest(initial_balance=Decimal("8000"), maker_fee_rate=Decimal("0"), taker_fee_rate=Decimal("0.0005"))
    )
    run = await api_module.run_simulation(
        SimulationRunRequest(
            session_kind=SessionKind.SINGLE_OPEN,
            symbol="BTCUSDT",
            open_mode=SingleOpenMode.REGULAR,
            selected_position_side=PositionSide.LONG,
            open_qty=Decimal("0.010"),
            leverage=10,
            round_count=1,
            round_interval_seconds=0,
        )
    )
    abort = await api_module.abort_simulation()
    template = await api_module.save_simulation_template(SimulationTemplateRequest(name="quick test", payload={"session_kind": "single_open"}))

    assert account["totals"]["wallet_balance"] == "7000"
    assert settings["settings"]["initial_balance"] == Decimal("8000")
    assert run["run_id"] == "sim-run"
    assert run["status"] == "running"
    assert simulation.last_run.session_kind == SessionKind.SINGLE_OPEN
    assert abort.requested is True
    assert template["name"] == "quick test"


@pytest.mark.asyncio
async def test_simulation_run_is_blocked_when_real_session_is_active(monkeypatch: pytest.MonkeyPatch) -> None:
    install_runtime(monkeypatch, active_real=True)

    with pytest.raises(HTTPException) as exc_info:
        await api_module.run_simulation(
            SimulationRunRequest(
                session_kind=SessionKind.SINGLE_OPEN,
                symbol="BTCUSDT",
                open_mode=SingleOpenMode.REGULAR,
                selected_position_side=PositionSide.LONG,
                open_qty=Decimal("0.010"),
                leverage=10,
                round_count=1,
                round_interval_seconds=0,
            )
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["code"] == "execution_conflict"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["reset", "clear_history"])
async def test_simulation_mutating_routes_return_structured_conflict_when_run_is_active(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    simulation = install_runtime(monkeypatch)
    if operation == "reset":
        simulation.fail_reset = True
        call = api_module.reset_simulation_account
    else:
        simulation.fail_clear = True
        call = api_module.clear_simulation_history

    with pytest.raises(HTTPException) as exc_info:
        await call()

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["code"] == "execution_conflict"
