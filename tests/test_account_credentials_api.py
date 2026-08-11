from __future__ import annotations

import asyncio
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paired_opener import api as api_module
from paired_opener.account_credentials import (
    AccountCredentialStore,
    CredentialImportPreviewStore,
)
from paired_opener.account_runtime import (
    AccountCredentialCommitCoordinator,
    AccountCredentialsLockedByActiveBatch,
    AccountRuntimeManager,
)
from paired_opener.config import AccountConfig, Settings
from paired_opener import config as config_module
from paired_opener.storage import SqliteRepository


class FakeProtector:
    def protect(self, value: bytes) -> bytes:
        return b"protected:" + value[::-1]

    def unprotect(self, value: bytes) -> bytes:
        if not value.startswith(b"protected:"):
            raise ValueError("invalid ciphertext")
        return value[10:][::-1]


class FakeRuntime:
    def __init__(self, account: AccountConfig) -> None:
        self.account = account
        self.is_closed = False
        self.verify_calls = 0

    async def verify_read_only_access(self) -> None:
        self.verify_calls += 1

    async def aclose(self) -> None:
        self.is_closed = True


class MutableMutationGuard:
    def __init__(self) -> None:
        self.locked = False

    def ensure_mutation_allowed(self) -> None:
        if self.locked:
            raise AccountCredentialsLockedByActiveBatch


@pytest.fixture
def credential_api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    settings = Settings(
        _env_file=None,
        database_path=tmp_path / "runtime.db",
        active_account_file=tmp_path / "active-account.json",
        binance_accounts_secure_file=tmp_path / "accounts.secure.json",
    )
    initial = AccountConfig("a1", "账号 1", "KEY-123456", "SECRET-123456")
    settings.accounts = {"a1": initial}
    settings.active_account_id = "a1"
    repository = SqliteRepository(settings.database_path)
    store = AccountCredentialStore(
        settings.binance_accounts_secure_file,
        FakeProtector(),
        permission_hardener=lambda _path: None,
    )
    prepared = store.prepare([initial])
    store.commit(prepared, expected_revision=store.current_revision())
    runtime_manager = AccountRuntimeManager(
        settings,
        repository,
        runtime_factory=FakeRuntime,
    )
    guard = MutableMutationGuard()
    coordinator = AccountCredentialCommitCoordinator(store, runtime_manager, guard)
    token = "local-management-token-for-tests"

    monkeypatch.setattr(api_module.app.state, "settings", settings, raising=False)
    monkeypatch.setattr(api_module.app.state, "repository", repository, raising=False)
    monkeypatch.setattr(api_module.app.state, "account_credential_store", store, raising=False)
    monkeypatch.setattr(api_module.app.state, "runtime_manager", runtime_manager, raising=False)
    monkeypatch.setattr(api_module.app.state, "account_mutation_guard", guard, raising=False)
    monkeypatch.setattr(api_module.app.state, "account_credential_commit_coordinator", coordinator, raising=False)
    monkeypatch.setattr(api_module.app.state, "credential_imports", CredentialImportPreviewStore(), raising=False)
    monkeypatch.setattr(api_module.app.state, "local_management_token", token, raising=False)
    monkeypatch.setattr(api_module.app.state, "account_credentials_migration_required", False, raising=False)

    client = TestClient(
        api_module.app,
        base_url="http://127.0.0.1:8000",
        client=("127.0.0.1", 50123),
    )
    headers = {
        "Host": "127.0.0.1:8000",
        "Origin": "http://127.0.0.1:8000",
        "X-Local-Management-Token": token,
    }
    client.headers.update(headers)
    yield client, headers, store, runtime_manager, guard
    asyncio.run(runtime_manager.aclose())
    repository.close()


def _account_payload(account_id: str, *, name: str | None = None) -> dict[str, object]:
    return {
        "account_id": account_id,
        "name": name or f"账号 {account_id}",
        "api_key": f"KEY-{account_id}-123456",
        "api_secret": f"SECRET-{account_id}-123456",
        "credential_type": "hmac",
        "account_mode": "portfolio_margin",
        "enabled": True,
    }


def test_account_list_masks_key_and_never_returns_secret(credential_api) -> None:
    client, _, _, _, _ = credential_api
    response = client.get("/config/account-credentials")
    assert response.status_code == 200
    assert "SECRET-123456" not in response.text
    assert '"api_secret"' not in response.text
    assert response.json()["accounts"][0]["api_key_masked"] == "KEY-…3456"


def test_local_management_checks_host_origin_client_and_token(credential_api) -> None:
    client, headers, _, _, _ = credential_api
    payload = _account_payload("a2")
    for invalid in (
        {**headers, "X-Local-Management-Token": "wrong-token"},
        {**headers, "Origin": "http://evil.example"},
        {**headers, "Host": "evil.example"},
    ):
        response = client.post("/config/account-credentials", json=payload, headers=invalid)
        assert response.status_code == 403
        assert response.json()["detail"]["code"] == "local_management_forbidden"

    remote = TestClient(
        api_module.app,
        base_url="http://127.0.0.1:8000",
        client=("203.0.113.10", 50124),
        headers=headers,
    )
    response = remote.post("/config/account-credentials", json=payload)
    assert response.status_code == 403


def test_local_management_allows_browser_get_without_origin_but_not_write(credential_api) -> None:
    client, headers, _, _, _ = credential_api
    browser_get_headers = {
        **{key: value for key, value in headers.items() if key.lower() != "origin"},
        "Origin": "",
    }

    response = client.get("/config/account-credentials", headers=browser_get_headers)
    assert response.status_code == 200

    rejected = client.post(
        "/config/account-credentials",
        json=_account_payload("a2"),
        headers=browser_get_headers,
    )
    assert rejected.status_code == 403
    assert rejected.json()["detail"]["code"] == "local_management_forbidden"


def test_import_preview_does_not_write_then_commit_is_atomic(credential_api) -> None:
    client, headers, store, runtime_manager, _ = credential_api
    before = store.path.read_bytes()
    old_runtime = runtime_manager.current("a1")
    response = client.post(
        "/config/account-credentials/import/preview",
        json={"accounts": [_account_payload("a2")], "mode": "merge"},
        headers=headers,
    )
    assert response.status_code == 200
    preview = response.json()
    assert store.path.read_bytes() == before
    assert "api_secret" not in preview["final_accounts"][0]
    assert preview["changes"]["added_account_ids"] == ["a2"]

    committed = client.post(
        "/config/account-credentials/import/commit",
        json={"preview_token": preview["preview_token"]},
        headers=headers,
    )
    assert committed.status_code == 200
    assert [account.account_id for account in store.load()] == ["a1", "a2"]
    assert runtime_manager.current("a2").account.name == "账号 a2"
    assert old_runtime.is_closed is True


def test_replace_import_removes_missing_accounts(credential_api) -> None:
    client, headers, store, _, _ = credential_api
    preview = client.post(
        "/config/account-credentials/import/preview",
        json={"accounts": [_account_payload("a2")], "mode": "replace"},
        headers=headers,
    ).json()
    response = client.post(
        "/config/account-credentials/import/commit",
        json={"preview_token": preview["preview_token"]},
        headers=headers,
    )
    assert response.status_code == 200
    assert [account.account_id for account in store.load()] == ["a2"]


def test_reorder_rejected_when_mutation_guard_is_locked(credential_api) -> None:
    client, headers, store, runtime_manager, guard = credential_api
    old_runtime = runtime_manager.current("a1")
    before = store.path.read_bytes()
    guard.locked = True
    response = client.put(
        "/config/account-credentials/order",
        json={"account_ids": ["a1"]},
        headers=headers,
    )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "account_credentials_locked_by_active_batch"
    assert store.path.read_bytes() == before
    assert runtime_manager.current("a1") is old_runtime
    assert old_runtime.is_closed is False


def test_import_body_limit_is_enforced_before_json_decode(credential_api) -> None:
    client, headers, _, _, _ = credential_api
    response = client.post(
        "/config/account-credentials/import/preview",
        content=b"{" + b"x" * (256 * 1024) + b"}",
        headers={**headers, "Content-Type": "application/json"},
    )
    assert response.status_code == 413
    assert response.json()["detail"]["code"] == "credential_import_too_large"


def test_hmac_only_validation_is_structured(credential_api) -> None:
    client, headers, _, _, _ = credential_api
    payload = _account_payload("a2")
    payload["credential_type"] = "rsa"
    response = client.post("/config/account-credentials", json=payload, headers=headers)
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "credential_type_not_supported"


def test_preview_revision_conflict_preserves_newer_configuration(credential_api) -> None:
    client, headers, store, _, _ = credential_api
    preview = client.post(
        "/config/account-credentials/import/preview",
        json={"accounts": [_account_payload("a2")], "mode": "merge"},
        headers=headers,
    ).json()
    store.upsert(AccountConfig("a3", "账号 3", "KEY-a3-123456", "SECRET-a3-123456"))
    response = client.post(
        "/config/account-credentials/import/commit",
        json={"preview_token": preview["preview_token"]},
        headers=headers,
    )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "credential_revision_conflict"
    assert [account.account_id for account in store.load()] == ["a1", "a3"]


def test_runtime_verification_failure_preserves_store_and_runtime(credential_api) -> None:
    client, headers, store, runtime_manager, _ = credential_api
    before = store.path.read_bytes()
    old_runtime = runtime_manager.current("a1")

    class RejectingRuntime(FakeRuntime):
        async def verify_read_only_access(self) -> None:
            if self.account.account_id == "a2":
                raise RuntimeError("read-only verification rejected")
            await super().verify_read_only_access()

    runtime_manager._runtime_factory = RejectingRuntime
    preview = client.post(
        "/config/account-credentials/import/preview",
        json={"accounts": [_account_payload("a2")], "mode": "merge"},
        headers=headers,
    ).json()
    response = client.post(
        "/config/account-credentials/import/commit",
        json={"preview_token": preview["preview_token"]},
        headers=headers,
    )
    assert response.status_code == 400
    assert store.path.read_bytes() == before
    assert runtime_manager.current("a1") is old_runtime


def test_bootstrap_token_is_no_store_and_not_a_cookie(credential_api) -> None:
    client, _, _, _, _ = credential_api
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store, max-age=0"
    assert "set-cookie" not in response.headers
    assert "local-management-token-for-tests" in response.text


def _lifespan_settings(tmp_path: Path, **overrides) -> Settings:
    return Settings(
        _env_file=None,
        data_dir=tmp_path / "data",
        database_path=tmp_path / "data" / "service.db",
        active_account_file=tmp_path / "config" / "active.json",
        symbol_whitelist_file=tmp_path / "config" / "symbols.json",
        binance_accounts_secure_file=tmp_path / "config" / "accounts.secure.json",
        **overrides,
    )


def test_service_starts_in_setup_mode_without_any_account(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _lifespan_settings(tmp_path)
    monkeypatch.setattr(config_module, "ENV_FILES", ())
    monkeypatch.setattr(api_module, "Settings", lambda: settings)
    with TestClient(
        api_module.app,
        base_url="http://127.0.0.1:8000",
        client=("127.0.0.1", 50125),
    ) as client:
        assert client.get("/").status_code == 200
        response = client.get("/symbols/ETHUSDC")
        assert response.status_code == 503
        assert response.json()["detail"]["code"] == "account_credentials_not_configured"


def test_bootstrap_token_rotates_between_service_starts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _lifespan_settings(tmp_path)
    monkeypatch.setattr(config_module, "ENV_FILES", ())
    monkeypatch.setattr(api_module, "Settings", lambda: settings)

    def read_token() -> str:
        with TestClient(
            api_module.app,
            base_url="http://127.0.0.1:8000",
            client=("127.0.0.1", 50126),
        ) as client:
            response = client.get("/")
            assert "set-cookie" not in response.headers
            match = re.search(r'"localManagementToken":"([^"]+)"', response.text)
            assert match is not None
            return match.group(1)

    assert read_token() != read_token()


def test_legacy_credentials_remain_available_with_migration_hint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _lifespan_settings(
        tmp_path,
        binance_api_key="LEGACY-KEY-123456",
        binance_api_secret="LEGACY-SECRET-123456",
    )
    monkeypatch.setattr(config_module, "ENV_FILES", ())
    monkeypatch.setattr(api_module, "Settings", lambda: settings)
    with TestClient(
        api_module.app,
        base_url="http://127.0.0.1:8000",
        client=("127.0.0.1", 50127),
    ) as client:
        html = client.get("/").text
        token_match = re.search(r'"localManagementToken":"([^"]+)"', html)
        assert token_match is not None
        response = client.get(
            "/config/account-credentials",
            headers={
                "Host": "127.0.0.1:8000",
                "Origin": "http://127.0.0.1:8000",
                "X-Local-Management-Token": token_match.group(1),
            },
        )
        assert response.status_code == 200
        assert response.json()["migration_required"] is True
        assert response.json()["accounts"]
        assert "LEGACY-SECRET-123456" not in response.text
