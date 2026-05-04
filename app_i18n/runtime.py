from __future__ import annotations

import hashlib
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Mapping


PROJECT_ROOT = Path(__file__).resolve().parent.parent
I18N_ROOT = PROJECT_ROOT / "i18n"
MESSAGES_DIR = I18N_ROOT / "messages"
REGISTRY_DIR = I18N_ROOT / "registry"

CONTRACT_VERSION = "2026-04-21"
DEFAULT_LOCALE = "zh-CN"
DEFAULT_TIMEZONE = "Asia/Shanghai"
DEFAULT_ACCOUNT_ID = "default"
DEFAULT_ACCOUNT_NAME = "默认账户"

_API_KEY_RE = re.compile(r"(?i)(api[_-]?key\s*[:=]\s*)([^\s,;]+)")
_SECRET_RE = re.compile(r"(?i)(api[_-]?secret\s*[:=]\s*)([^\s,;]+)")
_TOKEN_RE = re.compile(r"(?i)(token\s*[:=]\s*)([^\s,;]+)")
_LONG_IDENTIFIER_RE = re.compile(r"(?<![A-Za-z0-9])[A-Za-z0-9_-]{16,}(?![A-Za-z0-9])")


class _SafeFormatDict(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def messages(locale: str = DEFAULT_LOCALE) -> dict[str, str]:
    return {str(key): str(value) for key, value in _load_json(MESSAGES_DIR / f"{locale}.json").items()}


@lru_cache(maxsize=1)
def reason_registry() -> dict[str, dict[str, Any]]:
    return _load_json(REGISTRY_DIR / "reasons.json")


@lru_cache(maxsize=1)
def event_registry() -> dict[str, dict[str, Any]]:
    return _load_json(REGISTRY_DIR / "events.json")


@lru_cache(maxsize=1)
def log_registry() -> dict[str, dict[str, Any]]:
    return _load_json(REGISTRY_DIR / "logs.json")


@lru_cache(maxsize=1)
def precheck_registry() -> dict[str, dict[str, Any]]:
    return _load_json(REGISTRY_DIR / "precheck.json")


def _catalog_digest_payload() -> dict[str, Any]:
    return {
        "messages": messages(),
        "reasons": reason_registry(),
        "events": event_registry(),
        "logs": log_registry(),
        "precheck": precheck_registry(),
    }


@lru_cache(maxsize=1)
def catalog_version() -> str:
    payload = json.dumps(_catalog_digest_payload(), ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


CATALOG_VERSION = catalog_version()


def slice_messages(namespaces: Iterable[str] | None = None) -> dict[str, str]:
    catalog = messages()
    if not namespaces:
        return dict(catalog)
    prefixes = tuple(f"{namespace}." for namespace in namespaces)
    return {key: value for key, value in catalog.items() if key.startswith(prefixes)}


def format_copy(key: str, params: Mapping[str, Any] | None = None) -> str:
    template = messages().get(key)
    if template is None:
        return key
    safe_params = {name: value for name, value in dict(params or {}).items() if value is not None}
    try:
        return template.format_map(_SafeFormatDict(safe_params))
    except Exception:
        return template


def format_reason(code: str, params: Mapping[str, Any] | None = None) -> str:
    entry = reason_registry().get(code)
    if not entry:
        fallback_params = dict(params or {})
        if "message" in fallback_params:
            return str(fallback_params["message"])
        return format_copy("common.unknown_error")
    return format_copy(str(entry["key"]), params)


def format_runtime_message(message_code: str | None, message_params: Mapping[str, Any] | None = None, *, fallback: str = "") -> str:
    if message_code:
        rendered = format_copy(message_code, message_params)
        if rendered != message_code:
            return rendered
    return fallback


def redact_debug_text(raw_message: str | None) -> str | None:
    if not raw_message:
        return None
    redacted = str(raw_message)
    redacted = _API_KEY_RE.sub(r"\1[redacted]", redacted)
    redacted = _SECRET_RE.sub(r"\1[redacted]", redacted)
    redacted = _TOKEN_RE.sub(r"\1[redacted]", redacted)
    return _LONG_IDENTIFIER_RE.sub("[redacted]", redacted)


def make_api_detail(
    *,
    code: str,
    params: Mapping[str, Any] | None = None,
    category: str,
    strategy: str,
    source: str,
    raw_code: int | str | None = None,
    raw_message: str | None = None,
    operator_action: str | None = None,
    context: Mapping[str, Any] | None = None,
    message: str | None = None,
    precheck: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rendered_message = message or format_reason(code, params)
    payload: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "catalog_version": CATALOG_VERSION,
        "code": code,
        "params": dict(params or {}),
        "message": rendered_message,
        "category": category,
        "strategy": strategy,
        "raw_code": raw_code,
        "retryable": strategy == "retry",
        "operator_action": operator_action,
        "source": source,
        "context": dict(context or {}),
    }
    redacted = redact_debug_text(raw_message)
    if redacted:
        payload["raw_message"] = redacted
    if precheck is not None:
        payload["precheck"] = dict(precheck)
    return payload


def make_precheck_item(
    *,
    code: str,
    status: str,
    label_key: str,
    message_key: str | None = None,
    message_params: Mapping[str, Any] | None = None,
    details: Mapping[str, Any] | None = None,
    legacy_message: str | None = None,
    legacy_label: str | None = None,
) -> dict[str, Any]:
    resolved_message_key = message_key or "runtime.precheck_legacy_message"
    resolved_params = dict(message_params or {})
    if legacy_message is not None and "message" not in resolved_params and message_key is None:
        resolved_params["message"] = legacy_message
    resolved_label = format_copy(label_key)
    if resolved_label == label_key and legacy_label:
        resolved_label = legacy_label
    resolved_message = format_copy(resolved_message_key, resolved_params)
    return {
        "contract_version": CONTRACT_VERSION,
        "code": code,
        "status": status,
        "label_key": label_key,
        "label": resolved_label,
        "message_key": resolved_message_key,
        "message_params": resolved_params,
        "message": resolved_message,
        "details": dict(details or {}),
    }


def frontend_bootstrap_payload(*, namespaces: Iterable[str] | None = None) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "catalog_version": CATALOG_VERSION,
        "default_locale": DEFAULT_LOCALE,
        "default_timezone": DEFAULT_TIMEZONE,
        "messages": slice_messages(namespaces),
        "registries": {
            "reasons": reason_registry(),
            "events": event_registry(),
            "logs": log_registry(),
            "precheck": precheck_registry(),
        },
    }
