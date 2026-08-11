from __future__ import annotations

import base64
import binascii
import ctypes
import getpass
import hashlib
import json
import os
import re
import subprocess
import secrets
import threading
import uuid
from ctypes import wintypes
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Callable, Protocol

from paired_opener.config import AccountConfig


_FILE_VERSION = 1
_FILE_ENCODING = "dpapi-current-user"
_UNCONFIGURED_REVISION = "unconfigured"
_ACCOUNT_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
_SID_PATTERN = re.compile(r"S-\d-(?:\d+-)+\d+")


class CredentialStoreUnavailable(RuntimeError):
    """The secure credential store cannot be trusted or accessed."""


class CredentialRevisionConflict(RuntimeError):
    """The secure credential store changed after the caller read it."""


class CredentialPreviewInvalid(RuntimeError):
    """A credential import preview is missing, expired, or already consumed."""


class CredentialStoreState(str, Enum):
    UNCONFIGURED = "unconfigured"
    READY = "ready"
    UNAVAILABLE = "unavailable"


class SecretProtector(Protocol):
    def protect(self, value: bytes) -> bytes: ...

    def unprotect(self, value: bytes) -> bytes: ...


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_ubyte))]


def _blob_from_bytes(value: bytes) -> tuple[_DataBlob, ctypes.Array[ctypes.c_char]]:
    buffer = ctypes.create_string_buffer(value, len(value))
    blob = _DataBlob(len(value), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)))
    return blob, buffer


def _crypt_protect_data(value: bytes, *, description: str) -> bytes:
    if os.name != "nt" or not hasattr(ctypes, "windll"):
        raise CredentialStoreUnavailable("Windows DPAPI is unavailable")
    input_blob, input_buffer = _blob_from_bytes(value)
    output_blob = _DataBlob()
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    crypt32.CryptProtectData.argtypes = [
        ctypes.POINTER(_DataBlob),
        wintypes.LPCWSTR,
        ctypes.POINTER(_DataBlob),
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(_DataBlob),
    ]
    crypt32.CryptProtectData.restype = wintypes.BOOL
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    success = crypt32.CryptProtectData(
        ctypes.byref(input_blob),
        description,
        None,
        None,
        None,
        0x1,  # CRYPTPROTECT_UI_FORBIDDEN; scope remains CurrentUser.
        ctypes.byref(output_blob),
    )
    _ = input_buffer
    if not success:
        raise CredentialStoreUnavailable("Windows DPAPI encryption failed") from ctypes.WinError()
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        kernel32.LocalFree(ctypes.cast(output_blob.pbData, ctypes.c_void_p))


def _crypt_unprotect_data(value: bytes) -> bytes:
    if os.name != "nt" or not hasattr(ctypes, "windll"):
        raise CredentialStoreUnavailable("Windows DPAPI is unavailable")
    input_blob, input_buffer = _blob_from_bytes(value)
    output_blob = _DataBlob()
    description = wintypes.LPWSTR()
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    crypt32.CryptUnprotectData.argtypes = [
        ctypes.POINTER(_DataBlob),
        ctypes.POINTER(wintypes.LPWSTR),
        ctypes.POINTER(_DataBlob),
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(_DataBlob),
    ]
    crypt32.CryptUnprotectData.restype = wintypes.BOOL
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    success = crypt32.CryptUnprotectData(
        ctypes.byref(input_blob),
        ctypes.byref(description),
        None,
        None,
        None,
        0x1,
        ctypes.byref(output_blob),
    )
    _ = input_buffer
    if not success:
        raise CredentialStoreUnavailable("Windows DPAPI decryption failed") from ctypes.WinError()
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        if description:
            kernel32.LocalFree(ctypes.cast(description, ctypes.c_void_p))
        kernel32.LocalFree(ctypes.cast(output_blob.pbData, ctypes.c_void_p))


class WindowsDpapiProtector:
    def protect(self, value: bytes) -> bytes:
        return _crypt_protect_data(value, description="Binance Paired Opener accounts")

    def unprotect(self, value: bytes) -> bytes:
        return _crypt_unprotect_data(value)


def mask_api_key(value: str) -> str:
    normalized = value.strip()
    if len(normalized) <= 8:
        return "…" if normalized else ""
    return f"{normalized[:4]}…{normalized[-4:]}"


def _windows_acl_hardener(path: Path) -> None:
    if os.name != "nt":
        raise OSError("Windows ACL is unavailable")
    creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    identity = subprocess.run(
        ["whoami", "/user", "/fo", "csv", "/nh"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        creationflags=creation_flags,
    )
    sid_match = _SID_PATTERN.search(identity.stdout)
    if identity.returncode != 0 or sid_match is None:
        raise OSError(f"cannot resolve current user SID for {getpass.getuser()}")
    result = subprocess.run(
        [
            "icacls",
            str(path),
            "/inheritance:r",
            "/grant:r",
            f"*{sid_match.group(0)}:(F)",
            "*S-1-5-18:(F)",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        creationflags=creation_flags,
    )
    if result.returncode != 0:
        raise OSError("cannot restrict credential file ACL")


@dataclass(frozen=True, slots=True)
class PreparedCredentialWrite:
    target_path: Path
    temp_path: Path
    credential_revision: str

    def discard(self) -> None:
        self.temp_path.unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class VerifiedCredentialCandidate:
    accounts: tuple[AccountConfig, ...]
    credential_revision: str
    content_hash: str
    expires_at: datetime
    preview_token: str
    changes: dict[str, list[str]]


class CredentialImportPreviewStore:
    def __init__(self, *, ttl: timedelta = timedelta(minutes=5)) -> None:
        self._ttl = ttl
        self._previews: dict[str, VerifiedCredentialCandidate] = {}
        self._lock = threading.Lock()

    def create(
        self,
        *,
        existing: list[AccountConfig],
        imported: list[AccountConfig],
        mode: str,
        credential_revision: str,
    ) -> VerifiedCredentialCandidate:
        if mode not in {"merge", "replace"}:
            raise ValueError("unsupported import mode")
        imported_by_id = {account.account_id: account for account in imported}
        if len(imported_by_id) != len(imported):
            raise ValueError("duplicate account_id")
        existing_by_id = {account.account_id: account for account in existing}
        if mode == "replace":
            final_accounts = list(imported)
        else:
            final_accounts = [imported_by_id.get(account.account_id, account) for account in existing]
            final_accounts.extend(account for account in imported if account.account_id not in existing_by_id)

        added = [account.account_id for account in final_accounts if account.account_id not in existing_by_id]
        updated = [
            account.account_id
            for account in final_accounts
            if account.account_id in existing_by_id and account != existing_by_id[account.account_id]
        ]
        unchanged = [
            account.account_id
            for account in final_accounts
            if account.account_id in existing_by_id and account == existing_by_id[account.account_id]
        ]
        final_ids = {account.account_id for account in final_accounts}
        removed = [account.account_id for account in existing if account.account_id not in final_ids]
        changes = {
            "added_account_ids": added,
            "updated_account_ids": updated,
            "unchanged_account_ids": unchanged,
            "removed_account_ids": removed,
        }
        content_hash = _credential_content_hash(final_accounts)
        token = secrets.token_urlsafe(32)
        candidate = VerifiedCredentialCandidate(
            accounts=tuple(final_accounts),
            credential_revision=credential_revision,
            content_hash=content_hash,
            expires_at=datetime.now(UTC) + self._ttl,
            preview_token=token,
            changes=changes,
        )
        with self._lock:
            self._prune_expired_locked(datetime.now(UTC))
            self._previews[token] = candidate
        return candidate

    def consume_verified_preview(self, token: str) -> VerifiedCredentialCandidate:
        with self._lock:
            candidate = self._previews.pop(token, None)
        if candidate is None or candidate.expires_at <= datetime.now(UTC):
            raise CredentialPreviewInvalid("credential preview is invalid")
        if candidate.content_hash != _credential_content_hash(list(candidate.accounts)):
            raise CredentialPreviewInvalid("credential preview content changed")
        return candidate

    def _prune_expired_locked(self, now: datetime) -> None:
        expired = [token for token, candidate in self._previews.items() if candidate.expires_at <= now]
        for token in expired:
            self._previews.pop(token, None)


def _credential_content_hash(accounts: list[AccountConfig]) -> str:
    payload = [
        {
            "account_id": account.account_id,
            "name": account.name,
            "api_key": account.api_key,
            "api_secret": account.api_secret,
            "credential_type": account.credential_type,
            "account_mode": account.account_mode,
            "enabled": account.enabled,
            "use_testnet": account.use_testnet,
            "rest_base_url": account.rest_base_url,
            "ws_base_url": account.ws_base_url,
        }
        for account in accounts
    ]
    canonical = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class AccountCredentialStore:
    def __init__(
        self,
        path: Path,
        protector: SecretProtector,
        *,
        permission_hardener: Callable[[Path], None] | None = None,
    ) -> None:
        self._path = Path(path)
        self._protector = protector
        self._permission_hardener = permission_hardener or _windows_acl_hardener
        self._lock = threading.RLock()

    @property
    def path(self) -> Path:
        return self._path

    def state(self) -> CredentialStoreState:
        with self._lock:
            if not self._path.exists():
                return CredentialStoreState.UNCONFIGURED
            try:
                self._read_secure_payload()
            except CredentialStoreUnavailable:
                return CredentialStoreState.UNAVAILABLE
            return CredentialStoreState.READY

    def load(self) -> list[AccountConfig]:
        with self._lock:
            if not self._path.exists():
                return []
            _, accounts = self._read_secure_payload()
            return accounts

    def current_revision(self) -> str:
        with self._lock:
            if not self._path.exists():
                return _UNCONFIGURED_REVISION
            revision, _ = self._read_secure_payload()
            return revision

    def prepare(self, records: list[AccountConfig]) -> PreparedCredentialWrite:
        with self._lock:
            validated = self._validate_records(records)
            credential_revision = uuid.uuid4().hex
            plaintext = json.dumps(
                {
                    "version": _FILE_VERSION,
                    "credential_revision": credential_revision,
                    "accounts": [self._account_payload(account) for account in validated],
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            try:
                encrypted = self._protector.protect(plaintext)
            except Exception as exc:
                if isinstance(exc, CredentialStoreUnavailable):
                    raise
                raise CredentialStoreUnavailable("credential encryption failed") from exc

            payload = json.dumps(
                {
                    "version": _FILE_VERSION,
                    "encoding": _FILE_ENCODING,
                    "credential_revision": credential_revision,
                    "ciphertext": base64.b64encode(encrypted).decode("ascii"),
                },
                ensure_ascii=True,
                indent=2,
            ).encode("utf-8")
            temp_path = Path(f"{self._path}.tmp.{uuid.uuid4().hex}")
            try:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                # Hardening only the temporary file is insufficient: without a
                # protected parent directory another local user could replace it.
                self._permission_hardener(self._path.parent)
                with temp_path.open("wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                self._permission_hardener(temp_path)
            except Exception as exc:
                temp_path.unlink(missing_ok=True)
                raise CredentialStoreUnavailable("cannot prepare secure credential file") from exc
            return PreparedCredentialWrite(
                target_path=self._path,
                temp_path=temp_path,
                credential_revision=credential_revision,
            )

    def commit(self, prepared: PreparedCredentialWrite, *, expected_revision: str) -> str:
        with self._lock:
            if prepared.target_path != self._path or not prepared.temp_path.exists():
                raise CredentialStoreUnavailable("prepared credential write is unavailable")
            try:
                current_revision = self.current_revision()
            except Exception:
                prepared.temp_path.unlink(missing_ok=True)
                raise
            if current_revision != expected_revision:
                prepared.temp_path.unlink(missing_ok=True)
                raise CredentialRevisionConflict("credential revision changed")
            try:
                os.replace(prepared.temp_path, self._path)
            except OSError as exc:
                prepared.temp_path.unlink(missing_ok=True)
                raise CredentialStoreUnavailable("cannot commit secure credential file") from exc
            return prepared.credential_revision

    def upsert(self, record: AccountConfig) -> str:
        with self._lock:
            expected_revision = self.current_revision()
            accounts = self.load()
            for index, account in enumerate(accounts):
                if account.account_id == record.account_id:
                    accounts[index] = record
                    break
            else:
                accounts.append(record)
            return self.commit(self.prepare(accounts), expected_revision=expected_revision)

    def delete(self, account_id: str) -> str:
        with self._lock:
            expected_revision = self.current_revision()
            accounts = self.load()
            remaining = [account for account in accounts if account.account_id != account_id]
            if len(remaining) == len(accounts):
                raise KeyError(account_id)
            return self.commit(self.prepare(remaining), expected_revision=expected_revision)

    def reorder(self, account_ids: list[str]) -> str:
        with self._lock:
            expected_revision = self.current_revision()
            accounts = self.load()
            by_id = {account.account_id: account for account in accounts}
            if len(account_ids) != len(set(account_ids)) or set(account_ids) != set(by_id):
                raise ValueError("account_ids must contain every account exactly once")
            reordered = [by_id[account_id] for account_id in account_ids]
            return self.commit(self.prepare(reordered), expected_revision=expected_revision)

    def _read_secure_payload(self) -> tuple[str, list[AccountConfig]]:
        try:
            outer = json.loads(self._path.read_text(encoding="utf-8"))
            if not isinstance(outer, dict):
                raise ValueError("outer payload must be an object")
            if outer.get("version") != _FILE_VERSION or outer.get("encoding") != _FILE_ENCODING:
                raise ValueError("unsupported credential file format")
            outer_revision = str(outer["credential_revision"])
            encrypted = base64.b64decode(str(outer["ciphertext"]), validate=True)
            plaintext = self._protector.unprotect(encrypted)
            inner = json.loads(plaintext.decode("utf-8"))
            if not isinstance(inner, dict) or inner.get("version") != _FILE_VERSION:
                raise ValueError("unsupported protected payload format")
            inner_revision = str(inner["credential_revision"])
            if not outer_revision or outer_revision != inner_revision:
                raise ValueError("credential revision mismatch")
            raw_accounts = inner.get("accounts")
            if not isinstance(raw_accounts, list):
                raise ValueError("accounts must be a list")
            accounts = [self._account_from_payload(item) for item in raw_accounts]
            return outer_revision, self._validate_records(accounts)
        except CredentialStoreUnavailable:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError, binascii.Error, KeyError, TypeError, ValueError) as exc:
            raise CredentialStoreUnavailable("secure credential file is invalid") from exc
        except Exception as exc:
            raise CredentialStoreUnavailable("secure credential file cannot be decrypted") from exc

    @staticmethod
    def _validate_records(records: list[AccountConfig]) -> list[AccountConfig]:
        if len(records) > 100:
            raise ValueError("at most 100 accounts are allowed")
        seen: set[str] = set()
        validated: list[AccountConfig] = []
        for account in records:
            if not _ACCOUNT_ID_PATTERN.fullmatch(account.account_id):
                raise ValueError("invalid account_id")
            if account.account_id in seen:
                raise ValueError("duplicate account_id")
            if not 1 <= len(account.name.strip()) <= 100:
                raise ValueError("invalid account name")
            if not 8 <= len(account.api_key) <= 256 or not 8 <= len(account.api_secret) <= 256:
                raise ValueError("invalid API credential length")
            if account.credential_type != "hmac":
                raise ValueError("unsupported credential type")
            if account.account_mode != "portfolio_margin":
                raise ValueError("unsupported account mode")
            seen.add(account.account_id)
            validated.append(account)
        return validated

    @staticmethod
    def _account_payload(account: AccountConfig) -> dict[str, object]:
        return {
            "account_id": account.account_id,
            "name": account.name,
            "api_key": account.api_key,
            "api_secret": account.api_secret,
            "credential_type": account.credential_type,
            "account_mode": account.account_mode,
            "enabled": account.enabled,
            "use_testnet": account.use_testnet,
            "rest_base_url": account.rest_base_url,
            "ws_base_url": account.ws_base_url,
        }

    @staticmethod
    def _account_from_payload(payload: object) -> AccountConfig:
        if not isinstance(payload, dict):
            raise ValueError("account must be an object")
        if payload.get("credential_type") != "hmac":
            raise ValueError("unsupported credential type")
        if payload.get("account_mode") != "portfolio_margin":
            raise ValueError("unsupported account mode")
        return AccountConfig(
            account_id=str(payload["account_id"]),
            name=str(payload["name"]),
            api_key=str(payload["api_key"]),
            api_secret=str(payload["api_secret"]),
            credential_type=str(payload["credential_type"]),
            account_mode=str(payload["account_mode"]),
            enabled=bool(payload.get("enabled", True)),
            use_testnet=bool(payload.get("use_testnet", False)),
            rest_base_url=str(payload.get("rest_base_url", "")),
            ws_base_url=str(payload.get("ws_base_url", "")),
        )
