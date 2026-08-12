from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import paired_opener.account_credentials as account_credentials
from paired_opener.account_credentials import (
    AccountCredentialStore,
    CredentialRevisionConflict,
    CredentialStoreState,
    CredentialStoreUnavailable,
    WindowsDpapiProtector,
    _windows_acl_hardener,
    mask_api_key,
)
from paired_opener.config import AccountConfig


class FakeProtector:
    def protect(self, value: bytes) -> bytes:
        return b"cipher:" + value[::-1]

    def unprotect(self, value: bytes) -> bytes:
        if not value.startswith(b"cipher:"):
            raise ValueError("invalid ciphertext")
        return value[7:][::-1]


def _credential(account_id: str, api_key: str, api_secret: str) -> AccountConfig:
    return AccountConfig(
        account_id=account_id,
        name=f"账号 {account_id}",
        api_key=api_key,
        api_secret=api_secret,
    )


def _store(path: Path, *, hardener=None) -> AccountCredentialStore:
    return AccountCredentialStore(
        path,
        FakeProtector(),
        permission_hardener=hardener or (lambda _path: None),
    )


def _seeded_store(tmp_path: Path) -> AccountCredentialStore:
    store = _store(tmp_path / "accounts.secure.json")
    prepared = store.prepare([_credential("a1", "KEY-123456", "SECRET-123456")])
    store.commit(prepared, expected_revision=store.current_revision())
    return store


def test_store_persists_only_ciphertext(tmp_path: Path) -> None:
    path = tmp_path / "accounts.secure.json"
    store = _store(path)
    prepared = store.prepare([_credential("a1", "KEY-123456", "SECRET-123456")])
    store.commit(prepared, expected_revision=store.current_revision())
    raw = path.read_text(encoding="utf-8")
    assert "SECRET-123456" not in raw
    assert "KEY-123456" not in raw
    assert store.load()[0].account_id == "a1"


def test_mask_key_does_not_reveal_full_value() -> None:
    masked = mask_api_key("ABCDEFGH12345678")
    assert masked == "ABCD…5678"


def test_corrupt_ciphertext_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "accounts.secure.json"
    path.write_text('{"version":1,"ciphertext":"broken"}', encoding="utf-8")
    with pytest.raises(CredentialStoreUnavailable):
        _store(path).load()


def test_missing_secure_file_is_unconfigured_not_corrupt(tmp_path: Path) -> None:
    store = _store(tmp_path / "accounts.secure.json")
    assert store.load() == []
    assert store.state() is CredentialStoreState.UNCONFIGURED


def test_commit_rejects_stale_revision_without_changing_file(tmp_path: Path) -> None:
    store = _seeded_store(tmp_path)
    stale_revision = store.current_revision()

    first = store.prepare([_credential("a1", "OTHER-KEY-12", "OTHER-SECRET-12")])
    store.commit(first, expected_revision=stale_revision)
    before = store.path.read_bytes()

    prepared = store.prepare([_credential("a1", "NEW-KEY-123", "NEW-SECRET-123")])
    with pytest.raises(CredentialRevisionConflict):
        store.commit(prepared, expected_revision=stale_revision)
    assert store.path.read_bytes() == before
    assert not prepared.temp_path.exists()


def test_outer_and_inner_revision_mismatch_fails_closed(tmp_path: Path) -> None:
    store = _seeded_store(tmp_path)
    payload = json.loads(store.path.read_text(encoding="utf-8"))
    payload["credential_revision"] = "spliced-revision"
    store.path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(CredentialStoreUnavailable):
        store.load()


def test_prepare_permission_failure_removes_temp_and_preserves_old_file(tmp_path: Path) -> None:
    path = tmp_path / "accounts.secure.json"
    stable = _store(path)
    prepared = stable.prepare([_credential("a1", "KEY-123456", "SECRET-123456")])
    stable.commit(prepared, expected_revision=stable.current_revision())
    before = path.read_bytes()

    def fail_hardening(_path: Path) -> None:
        raise OSError("ACL failed")

    failing = _store(path, hardener=fail_hardening)
    with pytest.raises(CredentialStoreUnavailable):
        failing.prepare([_credential("a2", "KEY-234567", "SECRET-234567")])
    assert path.read_bytes() == before
    assert not path.with_suffix(path.suffix + ".tmp").exists()


def test_prepare_hardens_parent_directory_before_temporary_file(tmp_path: Path) -> None:
    path = tmp_path / "secure" / "accounts.secure.json"
    hardened: list[Path] = []
    store = _store(path, hardener=hardened.append)

    prepared = store.prepare([_credential("a1", "KEY-123456", "SECRET-123456")])
    try:
        assert hardened == [path.parent, prepared.temp_path]
    finally:
        prepared.discard()


def test_windows_acl_hardener_fails_closed_when_acl_verification_fails(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(account_credentials, "os", type("WindowsOs", (), {"name": "nt"})())
    calls: list[list[str]] = []

    def failed_run(command, **_kwargs):
        calls.append(command)
        return type("Result", (), {"returncode": 1})()

    monkeypatch.setattr("paired_opener.account_credentials.subprocess.run", failed_run)
    with pytest.raises(OSError, match="cannot restrict credential file ACL"):
        _windows_acl_hardener(tmp_path / "accounts.secure.json")

    assert len(calls) == 1
    assert calls[0][:5] == [
        "powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand"
    ]


def test_upsert_delete_and_reorder_keep_explicit_order(tmp_path: Path) -> None:
    store = _store(tmp_path / "accounts.secure.json")
    store.upsert(_credential("a1", "KEY-123456", "SECRET-123456"))
    store.upsert(_credential("a2", "KEY-234567", "SECRET-234567"))
    store.reorder(["a2", "a1"])
    assert [account.account_id for account in store.load()] == ["a2", "a1"]
    store.delete("a2")
    assert [account.account_id for account in store.load()] == ["a1"]


def test_unknown_ciphertext_version_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "accounts.secure.json"
    path.write_text('{"version":2,"encoding":"dpapi-current-user","ciphertext":"AA=="}', encoding="utf-8")
    with pytest.raises(CredentialStoreUnavailable):
        _store(path).load()


@pytest.mark.skipif(os.name != "nt", reason="Windows DPAPI is only available on Windows")
def test_windows_dpapi_round_trip() -> None:
    protector = WindowsDpapiProtector()
    plaintext = "仅限当前 Windows 用户的测试凭据".encode("utf-8")
    ciphertext = protector.protect(plaintext)
    assert ciphertext != plaintext
    assert protector.unprotect(ciphertext) == plaintext
