from __future__ import annotations

import importlib
import sys
from decimal import Decimal
from pathlib import Path

from fastapi import HTTPException

import pytest

from paired_opener import api as api_module
from paired_opener import monitor_api as monitor_api_module
from paired_opener.domain import OpenSession, SessionSpec, SessionStatus, TrendBias
from paired_opener.errors import ErrorCategory, ErrorStrategy, TradingError
from paired_opener.storage import SqliteRepository


def test_root_catalog_bootstrap_payload_exposes_shared_contract() -> None:
    from app_i18n.runtime import CONTRACT_VERSION, catalog_version, format_copy, format_reason, frontend_bootstrap_payload

    payload = frontend_bootstrap_payload(namespaces=("common", "reasons", "runtime", "console"))
    joined_messages = " ".join(payload["messages"].values())

    assert payload["contract_version"] == CONTRACT_VERSION
    assert payload["catalog_version"] == catalog_version()
    assert payload["messages"]["common.default_account_name"] == "默认账户"
    assert payload["messages"]["console.mode_labels.paired_open"] == "双向开仓"
    assert payload["messages"]["runtime.connection_disconnected"] == "已断开"
    assert payload["registries"]["reasons"]["session_not_found"]["key"] == "reasons.session_not_found"
    assert format_copy("common.default_account_name") == "默认账户"
    assert format_reason("session_not_found", {"session_id": "session-1"}) == "未找到会话 session-1。"
    assert "榛樿" not in joined_messages
    assert "鍙屽悜" not in joined_messages
    assert "宸叉柇寮€" not in joined_messages


def test_root_catalog_includes_precheck_fail_read_error_message() -> None:
    from app_i18n.runtime import format_copy, messages

    catalog = messages()

    assert catalog["precheck.system_open_orders.fail_read_error"] == "系统挂单状态读取失败：{error}"
    assert (
        format_copy("precheck.system_open_orders.fail_read_error", {"error": "读取失败"})
        == "系统挂单状态读取失败：读取失败"
    )


def test_make_precheck_item_keeps_missing_catalog_key_visible(monkeypatch: pytest.MonkeyPatch) -> None:
    from app_i18n import runtime as runtime_module

    def fake_format_copy(key: str, params: dict[str, object] | None = None) -> str:
        if key == "precheck.labels.system_open_orders":
            return "系统挂单冲突"
        return key

    monkeypatch.setattr(runtime_module, "format_copy", fake_format_copy)

    item = runtime_module.make_precheck_item(
        code="system_open_orders",
        status="fail",
        label_key="precheck.labels.system_open_orders",
        message_key="precheck.system_open_orders.fail_read_error",
        message_params={"error": "读取失败"},
        legacy_message="旧版自由文本错误",
    )

    assert item["label"] == "系统挂单冲突"
    assert item["message"] == "precheck.system_open_orders.fail_read_error"


def test_render_index_html_injects_console_namespace_messages() -> None:
    settings = api_module.Settings(_env_file=None)
    html = api_module._render_index_html(settings)

    assert "window.__APP_I18N__" in html
    assert "console.mode_labels.paired_open" in html
    assert html.index("window.__APP_I18N__") < html.index('<script src="/static/app.js')


def test_render_monitor_html_injects_shared_i18n_bootstrap_without_mojibake() -> None:
    settings = api_module.Settings(_env_file=None)
    html = api_module._render_monitor_html(settings)

    assert "window.__APP_I18N__" in html
    assert "Binance Account Monitor" in html
    assert "独立账户监控页" in html
    assert "榛樿" not in html
    assert "鍙屽悜" not in html
    assert "鐙珛" not in html
    bootstrap_index = html.index("window.__APP_I18N__")
    inline_script_index = html.index("const APP_CONFIG = window.__APP_CONFIG__ || {};")
    assert bootstrap_index < inline_script_index


def test_paired_monitor_api_bootstrap_precedes_inline_script() -> None:
    html = monitor_api_module._render_monitor_html()

    bootstrap_index = html.index("window.__APP_I18N__")
    inline_script_index = html.index("const APP_CONFIG = window.__APP_CONFIG__ || {};")

    assert bootstrap_index < inline_script_index


@pytest.mark.asyncio
async def test_standalone_monitor_index_sets_html_cache_headers() -> None:
    standalone_root = Path(__file__).resolve().parents[1] / "binance-account-monitor"
    if str(standalone_root) not in sys.path:
        sys.path.insert(0, str(standalone_root))
    standalone_api_module = importlib.import_module("monitor_app.api")

    response = await standalone_api_module.index()

    assert response.headers["Cache-Control"] == "no-cache"


def test_trading_error_to_detail_uses_structured_contract_and_redacts_debug_message() -> None:
    from app_i18n.runtime import CONTRACT_VERSION

    error = TradingError(
        "未找到会话 session-1。",
        category=ErrorCategory.INVALID_PARAMETER,
        strategy=ErrorStrategy.TERMINATE,
        source="api",
        code="session_not_found",
        params={"session_id": "session-1"},
        raw_message="apiKey=shhh-secret-value-123456 session=session-1",
    )

    detail = error.to_detail()

    assert detail["contract_version"] == CONTRACT_VERSION
    assert detail["code"] == "session_not_found"
    assert detail["params"] == {"session_id": "session-1"}
    assert detail["message"] == "未找到会话 session-1。"
    assert "shhh-secret-value-123456" not in str(detail.get("raw_message", ""))


def test_repository_persists_structured_error_fields(tmp_path) -> None:
    from app_i18n.runtime import CONTRACT_VERSION

    repository = SqliteRepository(tmp_path / "session.db")
    try:
        session = OpenSession.create(
            SessionSpec(
                symbol="BTCUSDT",
                trend_bias=TrendBias.LONG,
                leverage=10,
                round_count=1,
                round_qty=Decimal("0.010"),
                poll_interval_ms=50,
                order_ttl_ms=3000,
                max_zero_fill_retries=1,
                market_fallback_attempts=1,
                created_by="test",
            )
        )
        repository.create_session(session)

        repository.update_session_status(
            session.session_id,
            SessionStatus.EXCEPTION,
            last_error="未找到会话 session-1。",
            last_error_category="invalid_parameter",
            last_error_strategy="terminate",
            last_error_code="session_not_found",
            last_error_operator_action="检查会话编号后重试。",
            last_error_params={"session_id": "session-1"},
            last_error_raw_message="apiKey=super-secret-value-123456 session=session-1",
            last_error_contract_version=CONTRACT_VERSION,
        )

        payload = repository.get_session(session.session_id)

        assert payload is not None
        assert payload["last_error_code"] == "session_not_found"
        assert payload["last_error_params"] == {"session_id": "session-1"}
        assert payload["last_error_contract_version"] == CONTRACT_VERSION
        assert "super-secret-value-123456" not in str(payload["last_error_raw_message"])
    finally:
        repository.close()


@pytest.mark.asyncio
async def test_missing_session_http_error_uses_structured_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    from app_i18n.runtime import CONTRACT_VERSION

    class MissingSessionService:
        def get_session(self, session_id: str) -> dict:
            raise KeyError(session_id)

    runtime = type("Runtime", (), {"service": MissingSessionService()})()
    monkeypatch.setattr(api_module, "current_runtime", lambda _app: runtime)

    with pytest.raises(HTTPException) as exc_info:
        await api_module.get_session("missing-session")

    detail = exc_info.value.detail
    assert exc_info.value.status_code == 404
    assert detail["contract_version"] == CONTRACT_VERSION
    assert detail["code"] == "session_not_found"
    assert detail["params"] == {"session_id": "missing-session"}
