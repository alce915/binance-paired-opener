from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse

from app_i18n.runtime import DEFAULT_LOCALE, DEFAULT_TIMEZONE, frontend_bootstrap_payload
from paired_opener.account_monitor import AccountMonitorController
from paired_opener.config import settings
from paired_opener.market_stream import format_sse

STATIC_DIR = Path(__file__).with_name("static")
HTML_CACHE_HEADERS = {"Cache-Control": "no-cache"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    monitor_settings = settings.model_copy(deep=True)
    monitor_settings.load_accounts(include_accounts_file=True)
    monitor = AccountMonitorController(monitor_settings)
    app.state.monitor = monitor
    try:
        yield
    finally:
        await monitor.close()


app = FastAPI(title=settings.monitor_app_name, lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1024)


def _inject_bootstrap_before_scripts(html: str, bootstrap: str) -> str:
    first_script_index = html.find("<script")
    if first_script_index != -1:
        line_start = html.rfind("\n", 0, first_script_index)
        insert_at = 0 if line_start == -1 else line_start + 1
        indent = html[insert_at:first_script_index]
        return f"{html[:insert_at]}{indent}{bootstrap}\n{html[insert_at:]}"
    if "</body>" in html:
        return html.replace("</body>", f"{bootstrap}\n</body>", 1)
    return f"{html}\n{bootstrap}"


def _render_monitor_html() -> str:
    html = STATIC_DIR.joinpath("monitor.html").read_text(encoding="utf-8")
    bootstrap = (
        f'<script>window.__APP_CONFIG__ = {json.dumps({"locale": DEFAULT_LOCALE, "timezone": DEFAULT_TIMEZONE}, ensure_ascii=False)};'
        f'window.__APP_I18N__ = {json.dumps(frontend_bootstrap_payload(namespaces=("common", "runtime", "events", "reasons")), ensure_ascii=False)};</script>'
    )
    return _inject_bootstrap_before_scripts(html, bootstrap)


@app.get("/", include_in_schema=False)
async def index() -> HTMLResponse:
    return HTMLResponse(_render_monitor_html(), headers=HTML_CACHE_HEADERS)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/accounts")
async def get_accounts(account_ids: str | None = Query(default=None)) -> dict:
    monitor: AccountMonitorController = app.state.monitor
    selected_ids = _parse_account_ids(account_ids)
    return monitor.current_snapshot(selected_ids)


@app.get("/stream/accounts")
async def stream_accounts(account_ids: str | None = Query(default=None)) -> StreamingResponse:
    monitor: AccountMonitorController = app.state.monitor
    selected_ids = _parse_account_ids(account_ids)

    async def event_generator():
        queue = await monitor.subscribe(selected_ids)
        try:
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=15)
                    yield format_sse(message["event"], message["data"])
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            monitor.unsubscribe(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream", headers=HTML_CACHE_HEADERS)


def _parse_account_ids(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    account_ids = [item.strip().lower() for item in raw.split(",") if item.strip()]
    return account_ids or None
