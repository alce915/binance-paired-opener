from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from paired_opener import api as api_module
from paired_opener.config import AccountConfig, Settings
from paired_opener.domain import PositionSide, SymbolRules
from paired_opener.kanglong.batch_capacity import CapacitySnapshotCoordinator
from paired_opener.kanglong.batch_settings import KanglongBatchDefaultsStore
from paired_opener.kanglong.batch_planner import KanglongBatchPlanner
from paired_opener.schemas import KanglongBatchCapacityPreviewRequest
from paired_opener.storage import SqliteRepository
from tests.test_kanglong_batch_capacity import FakeRuntimeManager


class RevisionStore:
    def current_revision(self) -> str:
        return "revision-api"


class FakeRegistry:
    def __init__(self) -> None:
        self.started: list[str] = []

    def start(self, run_id: str) -> bool:
        self.started.append(run_id)
        return True

    wake = start


def _local_client(token: str, port: int) -> TestClient:
    return TestClient(
        api_module.app,
        base_url="http://127.0.0.1:8000",
        client=("127.0.0.1", port),
        headers={
            "Host": "127.0.0.1:8000",
            "Origin": "http://127.0.0.1:8000",
            "X-Local-Management-Token": token,
        },
    )


def test_batch_defaults_api_round_trip(tmp_path: Path, monkeypatch) -> None:
    token = "defaults-local-token"
    store = KanglongBatchDefaultsStore(tmp_path / "defaults.json")
    monkeypatch.setattr(api_module.app.state, "kanglong_batch_defaults_store", store, raising=False)
    monkeypatch.setattr(api_module.app.state, "local_management_token", token, raising=False)
    client = _local_client(token, 50129)
    initial = client.get("/config/kanglong-batch-defaults")
    assert initial.status_code == 200
    assert initial.json()["leverage"] == 100
    updated = client.put(
        "/config/kanglong-batch-defaults",
        json={
            "symbol": "btcusdc",
            "preferred_side": "SHORT",
            "leverage": 80,
            "per_leg_notional": "100000",
            "round_count": 20,
            "round_interval_seconds": 5,
        },
    )
    assert updated.status_code == 200
    assert updated.json()["symbol"] == "BTCUSDC"
    assert store.load().leverage == 80


def test_capacity_preview_uses_canonical_account_order_and_echoes_input(tmp_path: Path, monkeypatch) -> None:
    settings = Settings(
        _env_file=None,
        kanglong_batch_defaults_file=tmp_path / "defaults.json",
    )
    settings.accounts = {
        "a1": AccountConfig("a1", "账号 1", "KEY-a1-123456", "SECRET-a1-123456"),
        "a2": AccountConfig("a2", "账号 2", "KEY-a2-123456", "SECRET-a2-123456"),
    }
    settings.active_account_id = "a1"
    runtimes = FakeRuntimeManager(["a1", "a2"])
    token = "capacity-local-token"
    monkeypatch.setattr(api_module.app.state, "settings", settings, raising=False)
    monkeypatch.setattr(api_module.app.state, "runtime_manager", runtimes, raising=False)
    monkeypatch.setattr(api_module.app.state, "account_credential_store", RevisionStore(), raising=False)
    monkeypatch.setattr(
        api_module.app.state,
        "capacity_snapshot_coordinator",
        CapacitySnapshotCoordinator(runtimes),
        raising=False,
    )
    monkeypatch.setattr(api_module.app.state, "local_management_token", token, raising=False)
    client = _local_client(token, 50130)
    response = client.post(
        "/kanglong/batch-simulation/capacity-preview",
        json={
            "operation": "open",
            "symbol": "ETHUSDC",
            "preferred_side": "LONG",
            "leverage": 100,
            "per_leg_notional": "250000",
            "account_ids": ["a2", "a1"],
            "round_count": 30,
            "round_interval_seconds": 3,
            "request_seq": 7,
            "input_hash": "input-hash-0007",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["request_seq"] == 7
    assert payload["input_hash"] == "input-hash-0007"
    assert [account["account_id"] for account in payload["accounts"]] == ["a1", "a2"]
    assert payload["batch_requested_gross_notional"] == "1000000"
    assert payload["calculation_version"]
    assert payload["accounts"][0]["snapshot_components"]["order_book"]["ttl_ms"] > 0


def test_capacity_preview_rejects_close_operation(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(api_module.app.state, "local_management_token", "capacity-local-token", raising=False)
    client = TestClient(
        api_module.app,
        base_url="http://127.0.0.1:8000",
        client=("127.0.0.1", 50131),
        headers={
            "Host": "127.0.0.1:8000",
            "Origin": "http://127.0.0.1:8000",
            "X-Local-Management-Token": "capacity-local-token",
        },
    )
    response = client.post(
        "/kanglong/batch-simulation/capacity-preview",
        json={
            "operation": "close",
            "symbol": "ETHUSDC",
            "preferred_side": "LONG",
            "account_ids": ["a1"],
            "source_open_run_id": "open-run",
            "request_seq": 1,
            "input_hash": "input-hash-close",
        },
    )
    assert response.status_code == 422


def test_stale_snapshot_component_blocks_capacity() -> None:
    runtimes = FakeRuntimeManager(["a1"])
    snapshot = asyncio.run(
        CapacitySnapshotCoordinator(runtimes).get_snapshot("revision-api", "a1", "ETHUSDC")
    )
    components = {name: dict(component) for name, component in snapshot.snapshot_components.items()}
    components["order_book"]["valid"] = False
    stale = replace(snapshot, snapshot_components=components, all_components_fresh=False)
    request = KanglongBatchCapacityPreviewRequest(
        symbol="ETHUSDC",
        preferred_side=PositionSide.LONG,
        account_ids=["a1"],
        request_seq=1,
        input_hash="stale-input-hash",
    )
    payload, estimate = api_module._capacity_account_payload(stale, request)
    assert estimate is None
    assert payload["capacity_known"] is False
    assert payload["blocked"] is True
    assert "capacity_component_stale:order_book" in payload["blocked_reasons"]


def test_plan_confirm_execute_is_idempotent_and_uses_one_start_event(tmp_path: Path, monkeypatch) -> None:
    settings = Settings(_env_file=None, kanglong_symbol_configs_file=tmp_path / "symbols.json")
    settings.accounts = {
        "a1": AccountConfig("a1", "账号 1", "KEY-a1-123456", "SECRET-a1-123456"),
    }
    settings.active_account_id = "a1"
    runtimes = FakeRuntimeManager(["a1"])
    coordinator = CapacitySnapshotCoordinator(runtimes)
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    registry = FakeRegistry()
    token = "batch-actions-token"
    monkeypatch.setattr(api_module.app.state, "settings", settings, raising=False)
    monkeypatch.setattr(api_module.app.state, "runtime_manager", runtimes, raising=False)
    monkeypatch.setattr(api_module.app.state, "kanglong_readonly_runtime_manager", runtimes, raising=False)
    monkeypatch.setattr(api_module.app.state, "account_credential_store", RevisionStore(), raising=False)
    monkeypatch.setattr(api_module.app.state, "capacity_snapshot_coordinator", coordinator, raising=False)
    monkeypatch.setattr(api_module.app.state, "repository", repository, raising=False)
    monkeypatch.setattr(
        api_module.app.state,
        "kanglong_batch_planner",
        KanglongBatchPlanner(repository),
        raising=False,
    )
    monkeypatch.setattr(api_module.app.state, "kanglong_execution_task_registry", registry, raising=False)
    monkeypatch.setattr(api_module.app.state, "local_management_token", token, raising=False)
    client = _local_client(token, 50132)
    try:
        created = client.post(
            "/kanglong/batch-simulation/plan",
            json={
                "operation": "open",
                "symbol": "ETHUSDC",
                "preferred_side": "LONG",
                "leverage": 100,
                "per_leg_notional": "1000",
                "account_ids": ["a1"],
                "round_count": 30,
                "round_interval_seconds": 3,
            },
        )
        assert created.status_code == 200, created.text
        run_id = created.json()["run_id"]
        confirmed = client.post(
            f"/kanglong/batch-simulation/plan/{run_id}/confirm",
            json={"plan_version": created.json()["plan_version"], "idempotency_key": "confirm-api-0001"},
        )
        assert confirmed.status_code == 200, confirmed.text
        confirm_payload = confirmed.json()

        async def shifted_quote(symbol):
            from paired_opener.domain import Quote

            return Quote(symbol, Decimal("110"), Decimal("111"), api_module.datetime.now(api_module.UTC))

        runtimes.gateways["a1"].get_quote = shifted_quote
        repeated_confirm = client.post(
            f"/kanglong/batch-simulation/plan/{run_id}/confirm",
            json={"plan_version": created.json()["plan_version"], "idempotency_key": "confirm-api-0001"},
        )
        assert repeated_confirm.status_code == 200
        executed = client.post(
            f"/kanglong/batch-simulation/plan/{run_id}/execute",
            json={"plan_version": confirm_payload["plan_version"], "idempotency_key": "execute-api-0001"},
        )
        repeated_execute = client.post(
            f"/kanglong/batch-simulation/plan/{run_id}/execute",
            json={"plan_version": confirm_payload["plan_version"], "idempotency_key": "execute-api-0001"},
        )
        events = repository.list_kanglong_events(run_id)["events"]
    finally:
        repository.close()
    assert repeated_confirm.json() == confirm_payload
    assert executed.status_code == repeated_execute.status_code == 200
    assert repeated_execute.json() == executed.json()
    assert sum(event["event_type"] == "kanglong_batch_execution_starting" for event in events) == 1
    assert registry.started == [run_id, run_id]


def test_confirm_marks_original_plan_stale_when_market_recheck_changes(tmp_path: Path, monkeypatch) -> None:
    settings = Settings(_env_file=None, kanglong_symbol_configs_file=tmp_path / "symbols.json")
    settings.accounts = {
        "a1": AccountConfig("a1", "账号 1", "KEY-a1-123456", "SECRET-a1-123456"),
    }
    settings.active_account_id = "a1"
    runtimes = FakeRuntimeManager(["a1"])
    coordinator = CapacitySnapshotCoordinator(runtimes)
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    token = "batch-stale-token"
    monkeypatch.setattr(api_module.app.state, "settings", settings, raising=False)
    monkeypatch.setattr(api_module.app.state, "runtime_manager", runtimes, raising=False)
    monkeypatch.setattr(api_module.app.state, "kanglong_readonly_runtime_manager", runtimes, raising=False)
    monkeypatch.setattr(api_module.app.state, "account_credential_store", RevisionStore(), raising=False)
    monkeypatch.setattr(api_module.app.state, "capacity_snapshot_coordinator", coordinator, raising=False)
    monkeypatch.setattr(api_module.app.state, "repository", repository, raising=False)
    monkeypatch.setattr(
        api_module.app.state,
        "kanglong_batch_planner",
        KanglongBatchPlanner(repository),
        raising=False,
    )
    monkeypatch.setattr(api_module.app.state, "local_management_token", token, raising=False)
    client = _local_client(token, 50136)
    try:
        created = client.post(
            "/kanglong/batch-simulation/plan",
            json={
                "operation": "open",
                "symbol": "ETHUSDC",
                "preferred_side": "LONG",
                "leverage": 100,
                "per_leg_notional": "1000",
                "account_ids": ["a1"],
            },
        )
        run_id = created.json()["run_id"]

        async def shifted_quote(symbol):
            from paired_opener.domain import Quote

            return Quote(symbol, Decimal("110"), Decimal("111"), api_module.datetime.now(api_module.UTC))

        runtimes.gateways["a1"].get_quote = shifted_quote
        response = client.post(
            f"/kanglong/batch-simulation/plan/{run_id}/confirm",
            json={
                "plan_version": created.json()["plan_version"],
                "idempotency_key": "confirm-stale-api-0001",
            },
        )
        stored = repository.get_kanglong_run(run_id)
    finally:
        repository.close()

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "blocked_plan_stale"
    assert stored["status"] == "blocked_plan_stale"
    assert stored["plan_version"] == created.json()["plan_version"]


def test_control_rejects_stale_action_version_without_state_change(tmp_path: Path, monkeypatch) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    plan = KanglongBatchPlanner().plan_open(
        account_ids=["a1"],
        credential_revision="revision-api",
        symbol="ETHUSDC",
        preferred_side=PositionSide.LONG,
        leverage=100,
        per_leg_notional="1000",
        reference_price="100",
        rules=SymbolRules(
            symbol="ETHUSDC",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        run_id="paused-api",
    )
    repository.save_batch_plan(plan, status="paused_by_user")
    repository._connection.execute(
        "UPDATE kanglong_runs SET progress_json = ? WHERE run_id = ?",
        ('{"action_version":2}', plan.run_id),
    )
    repository._connection.commit()
    token = "batch-control-token"
    monkeypatch.setattr(api_module.app.state, "repository", repository, raising=False)
    monkeypatch.setattr(api_module.app.state, "kanglong_execution_task_registry", FakeRegistry(), raising=False)
    monkeypatch.setattr(api_module.app.state, "account_credential_store", RevisionStore(), raising=False)
    monkeypatch.setattr(api_module.app.state, "local_management_token", token, raising=False)
    client = _local_client(token, 50133)
    try:
        response = client.post(
            f"/kanglong/batch-simulation/run/{plan.run_id}/resume",
            json={
                "plan_version": plan.plan_version,
                "expected_action_version": 1,
                "idempotency_key": "resume-api-0001",
            },
        )
        stored = repository.get_kanglong_run(plan.run_id)
    finally:
        repository.close()
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "action_version_conflict"
    assert stored["status"] == "paused_by_user"


def test_pause_wakes_worker_during_round_interval(tmp_path: Path, monkeypatch) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    plan = KanglongBatchPlanner().plan_open(
        account_ids=["a1"],
        credential_revision="revision-api",
        symbol="ETHUSDC",
        preferred_side=PositionSide.LONG,
        leverage=100,
        per_leg_notional="1000",
        reference_price="100",
        rules=SymbolRules(
            symbol="ETHUSDC",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        run_id="pause-wake-api",
    )
    repository.save_batch_plan(plan, status="running")
    registry = FakeRegistry()
    token = "pause-wake-token"
    monkeypatch.setattr(api_module.app.state, "repository", repository, raising=False)
    monkeypatch.setattr(api_module.app.state, "kanglong_execution_task_registry", registry, raising=False)
    monkeypatch.setattr(api_module.app.state, "account_credential_store", RevisionStore(), raising=False)
    monkeypatch.setattr(api_module.app.state, "local_management_token", token, raising=False)
    client = _local_client(token, 50137)
    try:
        response = client.post(
            f"/kanglong/batch-simulation/run/{plan.run_id}/pause",
            json={
                "plan_version": plan.plan_version,
                "expected_action_version": 0,
                "idempotency_key": "pause-wake-api-0001",
            },
        )
    finally:
        repository.close()

    assert response.status_code == 200
    assert response.json()["status"] == "pause_pending"
    assert registry.started == [plan.run_id]


def test_stop_can_override_pause_pending(tmp_path: Path, monkeypatch) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    plan = KanglongBatchPlanner().plan_open(
        account_ids=["a1"], credential_revision="revision-api", symbol="ETHUSDC",
        preferred_side=PositionSide.LONG, leverage=100, per_leg_notional="1000",
        reference_price="100", rules=SymbolRules(
            symbol="ETHUSDC", tick_size=Decimal("0.01"), step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"), min_notional=Decimal("5"), max_leverage=125,
        ), run_id="stop-overrides-pause-api",
    )
    repository.save_batch_plan(plan, status="pause_pending")
    registry = FakeRegistry()
    token = "stop-overrides-pause-token"
    monkeypatch.setattr(api_module.app.state, "repository", repository, raising=False)
    monkeypatch.setattr(api_module.app.state, "kanglong_execution_task_registry", registry, raising=False)
    monkeypatch.setattr(api_module.app.state, "account_credential_store", RevisionStore(), raising=False)
    monkeypatch.setattr(api_module.app.state, "local_management_token", token, raising=False)
    client = _local_client(token, 50138)
    try:
        response = client.post(
            f"/kanglong/batch-simulation/run/{plan.run_id}/stop",
            json={"plan_version": plan.plan_version, "expected_action_version": 0,
                  "idempotency_key": "stop-overrides-pause-0001"},
        )
    finally:
        repository.close()

    assert response.status_code == 200
    assert response.json()["status"] == "stop_pending"
    assert registry.started == [plan.run_id]


def test_batch_recover_records_reason_and_releases_locks(tmp_path: Path, monkeypatch) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    plan = KanglongBatchPlanner().plan_open(
        account_ids=["a1"], credential_revision="revision-api", symbol="ETHUSDC",
        preferred_side=PositionSide.LONG, leverage=100, per_leg_notional="1000",
        reference_price="100", rules=SymbolRules(
            symbol="ETHUSDC", tick_size=Decimal("0.01"), step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"), min_notional=Decimal("5"), max_leverage=125,
        ), run_id="batch-abort-recover-api",
    )
    repository.save_batch_plan(plan, status="needs_abort_recover")
    repository._connection.execute(
        "UPDATE kanglong_batch_accounts SET status = 'second_leg' WHERE run_id = ? AND account_id = ?",
        (plan.run_id, "a1"),
    )
    repository._connection.commit()
    repository.acquire_kanglong_locks(
        run_id=plan.run_id, lock_scopes=["kanglong:account:a1"], ttl_ms=60_000,
    )
    registry = FakeRegistry()
    token = "batch-recover-token"
    monkeypatch.setattr(api_module.app.state, "repository", repository, raising=False)
    monkeypatch.setattr(api_module.app.state, "kanglong_execution_task_registry", registry, raising=False)
    monkeypatch.setattr(api_module.app.state, "account_credential_store", RevisionStore(), raising=False)
    monkeypatch.setattr(api_module.app.state, "local_management_token", token, raising=False)
    client = _local_client(token, 50139)
    try:
        response = client.post(
            f"/kanglong/batch-simulation/run/{plan.run_id}/recover",
            json={"plan_version": plan.plan_version, "expected_action_version": 0,
                  "idempotency_key": "batch-abort-recover-0001", "operator": "tester",
                  "release_reason": "operator reviewed unsafe checkpoint"},
        )
        stored = repository.get_kanglong_run(plan.run_id)
        account = repository.get_kanglong_batch_account(plan.run_id, "a1")
        events = repository.list_kanglong_events(plan.run_id)["events"]
        lock = repository.get_kanglong_lock("kanglong:account:a1")
        repeated = client.post(
            f"/kanglong/batch-simulation/run/{plan.run_id}/recover",
            json={"plan_version": plan.plan_version, "expected_action_version": 0,
                  "idempotency_key": "batch-abort-recover-0001", "operator": "tester",
                  "release_reason": "operator reviewed unsafe checkpoint"},
        )
    finally:
        repository.close()

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "aborted_recovered"
    assert response.json()["available_actions"] == ["view_report"]
    assert stored["progress"]["abort_recover"]["release_reason"] == "operator reviewed unsafe checkpoint"
    assert stored["report"]["abort_recover_history"][0]["operator"] == "tester"
    assert [event["event_type"] for event in events[-2:]] == [
        "kanglong_batch_abort_recovering", "kanglong_batch_aborted_recovered",
    ]
    assert [event["payload"]["message_key"] for event in events[-2:]] == [
        "events.kanglong.batch_abort_recovering", "events.kanglong.batch_aborted_recovered",
    ]
    assert account["status"] == "needs_recovery"
    assert lock is None
    assert registry.started == []

    assert repeated.status_code == 200
    assert repeated.json()["status"] == "aborted_recovered"
    assert repeated.json()["available_actions"] == ["view_report"]


def test_batch_api_exposes_recover_only_for_abort_recovery_state(tmp_path: Path, monkeypatch) -> None:
    repository = SqliteRepository(tmp_path / "db.sqlite3")
    plan = KanglongBatchPlanner().plan_open(
        account_ids=["a1"],
        credential_revision="revision-api",
        symbol="ETHUSDC",
        preferred_side=PositionSide.LONG,
        leverage=100,
        per_leg_notional="1000",
        reference_price="100",
        rules=SymbolRules(
            symbol="ETHUSDC",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=125,
        ),
        run_id="recover-actions-api",
    )
    repository.save_batch_plan(plan, status="paused_market_unstable")
    monkeypatch.setattr(api_module.app.state, "repository", repository, raising=False)
    try:
        paused = api_module._kanglong_batch_run_payload(repository.get_kanglong_run(plan.run_id))
        repository.update_kanglong_run(
            plan.run_id,
            status="needs_abort_recover",
            available_actions=["recover", "view_report"],
        )
        unsafe = api_module._kanglong_batch_run_payload(repository.get_kanglong_run(plan.run_id))
    finally:
        repository.close()
    assert "recover" not in paused["available_actions"]
    assert "recover" in unsafe["available_actions"]
