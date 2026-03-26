from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from paired_opener.account_runtime import AccountRuntimeManager
from paired_opener.config import Settings, settings
from paired_opener.domain import ExchangeStateError, SessionConflictError
from paired_opener.schemas import (
    AccountListResponse,
    AccountSelectRequest,
    AccountSelectResponse,
    AccountSummary,
    CloseSessionRequest,
    MarketConnectRequest,
    OpenSessionRequest,
    SingleCloseSessionRequest,
    SingleOpenSessionRequest,
    SessionActionResponse,
    SessionDetail,
    SessionSummary,
    SimulationRequest,
    SymbolInfoResponse,
    WhitelistResponse,
    WhitelistUpdateRequest,
)
from paired_opener.market_stream import format_sse
from paired_opener.storage import SqliteRepository


@asynccontextmanager
async def lifespan(app: FastAPI):
    app_settings = Settings()
    app_settings.load_persisted_whitelist()
    app_settings.load_accounts(include_accounts_file=False)
    repository = SqliteRepository(app_settings.database_path)
    runtime_manager = AccountRuntimeManager(app_settings, repository)
    app.state.settings = app_settings
    app.state.repository = repository
    app.state.runtime_manager = runtime_manager
    try:
        yield
    finally:
        await runtime_manager.close()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.mount('/static', StaticFiles(directory=Path(__file__).with_name('static')), name='static')


def current_runtime(app: FastAPI):
    return app.state.runtime_manager.current()


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(Path(__file__).with_name("static").joinpath("index.html"))


@app.get("/static/app.js", include_in_schema=False)
async def static_app_js() -> FileResponse:
    return FileResponse(Path(__file__).with_name("static").joinpath("app.js"), media_type="application/javascript")


@app.get("/static/monitor.html", include_in_schema=False)
async def static_monitor_html() -> FileResponse:
    return FileResponse(Path(__file__).with_name("static").joinpath("monitor.html"))


@app.get("/stream/events")
async def stream_events() -> StreamingResponse:
    market = current_runtime(app).market

    async def event_generator():
        queue = await market.subscribe()
        try:
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=15)
                    yield format_sse(message["event"], message["data"])
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            market.unsubscribe(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/market/connect")
async def connect_market(request: MarketConnectRequest) -> dict:
    market = current_runtime(app).market
    try:
        return await market.connect(request.symbol)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/market/disconnect")
async def disconnect_market() -> dict:
    market = current_runtime(app).market
    return await market.disconnect()


@app.post("/simulation/run")
async def run_simulation(request: SimulationRequest) -> dict:
    market = current_runtime(app).market
    try:
        return await market.run_simulation(
            symbol=request.symbol,
            trend_bias=request.trend_bias,
            open_amount=request.open_amount,
            leverage=request.leverage,
            round_count=request.round_count,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/sessions/open", response_model=SessionSummary)
async def create_session(request: OpenSessionRequest) -> SessionSummary:
    service = current_runtime(app).service
    try:
        session = await service.create_open_session(request)
    except (ValueError, SessionConflictError, ExchangeStateError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    payload = service.get_session(session.session_id)
    return SessionSummary.model_validate(payload)


@app.post("/sessions/close", response_model=SessionSummary)
async def create_close_session(request: CloseSessionRequest) -> SessionSummary:
    service = current_runtime(app).service
    try:
        session = await service.create_close_session(request)
    except (ValueError, SessionConflictError, ExchangeStateError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    payload = service.get_session(session.session_id)
    return SessionSummary.model_validate(payload)


@app.post("/sessions/single-open", response_model=SessionSummary)
async def create_single_open_session(request: SingleOpenSessionRequest) -> SessionSummary:
    service = current_runtime(app).service
    try:
        session = await service.create_single_open_session(request)
    except (ValueError, SessionConflictError, ExchangeStateError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    payload = service.get_session(session.session_id)
    return SessionSummary.model_validate(payload)


@app.post("/sessions/single-close", response_model=SessionSummary)
async def create_single_close_session(request: SingleCloseSessionRequest) -> SessionSummary:
    service = current_runtime(app).service
    try:
        session = await service.create_single_close_session(request)
    except (ValueError, SessionConflictError, ExchangeStateError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    payload = service.get_session(session.session_id)
    return SessionSummary.model_validate(payload)


@app.get("/sessions", response_model=list[SessionSummary])
async def list_sessions() -> list[SessionSummary]:
    service = current_runtime(app).service
    return [SessionSummary.model_validate(item) for item in service.list_sessions()]


@app.get("/sessions/{session_id}", response_model=SessionDetail)
async def get_session(session_id: str) -> SessionDetail:
    service = current_runtime(app).service
    try:
        return SessionDetail.model_validate(service.get_session(session_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Session not found") from exc


@app.post("/sessions/{session_id}/pause", response_model=SessionActionResponse)
async def pause_session(session_id: str) -> SessionActionResponse:
    service = current_runtime(app).service
    try:
        status = await service.pause_session(session_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Session not found") from exc
    return SessionActionResponse(
        session_id=session_id,
        status=status,
        requested=True,
        requested_action="pause",
        message="已收到暂停请求，将在安全边界暂停",
    )


@app.post("/sessions/{session_id}/resume", response_model=SessionActionResponse)
async def resume_session(session_id: str) -> SessionActionResponse:
    service = current_runtime(app).service
    try:
        status = await service.resume_session(session_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Session not found") from exc
    return SessionActionResponse(
        session_id=session_id,
        status=status,
        requested=True,
        requested_action="resume",
        message="已收到恢复请求",
    )


@app.post("/sessions/{session_id}/abort", response_model=SessionActionResponse)
async def abort_session(session_id: str) -> SessionActionResponse:
    service = current_runtime(app).service
    try:
        status = await service.abort_session(session_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Session not found") from exc
    return SessionActionResponse(
        session_id=session_id,
        status=status,
        requested=True,
        requested_action="abort",
        message="已收到中断请求，当前将安全收口后退出",
    )


@app.get("/config/whitelist", response_model=WhitelistResponse)
async def get_whitelist() -> WhitelistResponse:
    service = current_runtime(app).service
    return WhitelistResponse(symbols=service.get_whitelist())


@app.put("/config/whitelist", response_model=WhitelistResponse)
async def update_whitelist(request: WhitelistUpdateRequest) -> WhitelistResponse:
    service = current_runtime(app).service
    try:
        symbols = await service.update_whitelist(request.symbols)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return WhitelistResponse(symbols=symbols)


@app.get("/config/accounts", response_model=AccountListResponse)
async def get_accounts() -> AccountListResponse:
    runtime_manager: AccountRuntimeManager = app.state.runtime_manager
    accounts = [AccountSummary.model_validate(item) for item in runtime_manager.list_accounts()]
    return AccountListResponse(accounts=accounts)


@app.post("/config/accounts/select", response_model=AccountSelectResponse)
async def select_account(request: AccountSelectRequest) -> AccountSelectResponse:
    runtime_manager: AccountRuntimeManager = app.state.runtime_manager
    try:
        payload = await runtime_manager.switch_account(request.account_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return AccountSelectResponse(account=AccountSummary.model_validate(payload))


@app.get("/symbols/{symbol}", response_model=SymbolInfoResponse)
async def get_symbol_info(symbol: str) -> SymbolInfoResponse:
    service = current_runtime(app).service
    try:
        payload = await service.get_symbol_info(symbol)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SymbolInfoResponse.model_validate(payload)




