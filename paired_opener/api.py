from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from paired_opener.account_runtime import AccountRuntimeManager
from paired_opener.config import Settings, settings
from paired_opener.domain import ExchangeStateError, SessionConflictError
from paired_opener.errors import TradingError, ensure_trading_error, http_status_for_error, invalid_parameter_error
from paired_opener.market_stream import format_sse
from paired_opener.schemas import (
    AccountListResponse,
    AccountSelectRequest,
    AccountSelectResponse,
    AccountSummary,
    CloseSessionRequest,
    MarketConnectRequest,
    OpenSessionRequest,
    SessionActionResponse,
    SessionDetail,
    SessionPrecheckRequest,
    SessionPrecheckResponse,
    SessionSummary,
    SessionUpdatesResponse,
    SimulationRunRequest,
    SingleCloseSessionRequest,
    SingleOpenSessionRequest,
    SymbolInfoResponse,
    WhitelistResponse,
    WhitelistUpdateRequest,
)
from paired_opener.service import SessionPrecheckFailed
from paired_opener.storage import SqliteRepository

STATIC_DIR = Path(__file__).with_name('static')
HTML_CACHE_HEADERS = {'Cache-Control': 'no-store, max-age=0'}
STATIC_CACHE_HEADERS = {'Cache-Control': 'public, max-age=300'}


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
app.add_middleware(GZipMiddleware, minimum_size=1024)


def current_runtime(app: FastAPI):
    return app.state.runtime_manager.current()


def _static_file_response(name: str, *, media_type: str | None = None, cache_headers: dict[str, str] | None = None) -> FileResponse:
    return FileResponse(STATIC_DIR.joinpath(name), media_type=media_type, headers=cache_headers or STATIC_CACHE_HEADERS)


def _raise_api_error(
    exc: Exception,
    *,
    code: str,
    source: str = 'api',
    context: dict[str, object] | None = None,
    precheck: dict | None = None,
) -> None:
    if isinstance(exc, SessionConflictError):
        error = invalid_parameter_error(str(exc), source=source, code=code, context=context)
    else:
        error = ensure_trading_error(exc, source=source, code=code, context=context)
    raise HTTPException(status_code=http_status_for_error(error), detail=error.to_detail(precheck=precheck)) from exc


@app.get('/', include_in_schema=False)
async def index() -> FileResponse:
    return _static_file_response('index.html', cache_headers=HTML_CACHE_HEADERS)


@app.get('/static/app.js', include_in_schema=False)
async def static_app_js() -> FileResponse:
    return _static_file_response('app.js', media_type='application/javascript', cache_headers=HTML_CACHE_HEADERS)


@app.get('/static/monitor.html', include_in_schema=False)
async def static_monitor_html() -> FileResponse:
    return _static_file_response('monitor.html', cache_headers=HTML_CACHE_HEADERS)

app.mount('/static', StaticFiles(directory=STATIC_DIR), name='static')


@app.get('/stream/events')
async def stream_events() -> StreamingResponse:
    market = current_runtime(app).market

    async def event_generator():
        queue = await market.subscribe()
        try:
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=15)
                    yield format_sse(message['event'], message['data'])
                except asyncio.TimeoutError:
                    yield ': keep-alive\n\n'
        finally:
            market.unsubscribe(queue)

    return StreamingResponse(event_generator(), media_type='text/event-stream', headers=HTML_CACHE_HEADERS)


@app.post('/market/connect')
async def connect_market(request: MarketConnectRequest) -> dict:
    market = current_runtime(app).market
    try:
        return await market.connect(request.symbol)
    except Exception as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')


@app.post('/market/disconnect')
async def disconnect_market() -> dict:
    market = current_runtime(app).market
    return await market.disconnect()


@app.post('/simulation/run')
async def run_simulation(request: SimulationRunRequest) -> dict:
    market = current_runtime(app).market
    try:
        return await market.run_simulation(
            session_kind=request.session_kind,
            symbol=request.symbol,
            trend_bias=request.trend_bias,
            open_mode=request.open_mode,
            close_mode=request.close_mode,
            selected_position_side=request.selected_position_side,
            open_amount=request.open_amount,
            open_qty=request.open_qty,
            close_qty=request.close_qty,
            leverage=request.leverage,
            round_count=request.round_count,
            round_interval_seconds=request.round_interval_seconds,
        )
    except Exception as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')


@app.post('/sessions/precheck', response_model=SessionPrecheckResponse)
async def precheck_session(request: SessionPrecheckRequest) -> SessionPrecheckResponse:
    service = current_runtime(app).service
    try:
        payload = await service.precheck_request(request)
    except Exception as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')
    return SessionPrecheckResponse.model_validate(payload)


@app.post('/sessions/open', response_model=SessionSummary)
async def create_session(request: OpenSessionRequest) -> SessionSummary:
    service = current_runtime(app).service
    try:
        session = await service.create_open_session(request)
    except SessionPrecheckFailed as exc:
        _raise_api_error(exc, code='session_precheck_failed', source='service', precheck=exc.precheck)
    except (TradingError, ValueError, SessionConflictError, ExchangeStateError) as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')
    payload = service.get_session(session.session_id)
    return SessionSummary.model_validate(payload)


@app.post('/sessions/close', response_model=SessionSummary)
async def create_close_session(request: CloseSessionRequest) -> SessionSummary:
    service = current_runtime(app).service
    try:
        session = await service.create_close_session(request)
    except SessionPrecheckFailed as exc:
        _raise_api_error(exc, code='session_precheck_failed', source='service', precheck=exc.precheck)
    except (TradingError, ValueError, SessionConflictError, ExchangeStateError) as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')
    payload = service.get_session(session.session_id)
    return SessionSummary.model_validate(payload)


@app.post('/sessions/single-open', response_model=SessionSummary)
async def create_single_open_session(request: SingleOpenSessionRequest) -> SessionSummary:
    service = current_runtime(app).service
    try:
        session = await service.create_single_open_session(request)
    except SessionPrecheckFailed as exc:
        _raise_api_error(exc, code='session_precheck_failed', source='service', precheck=exc.precheck)
    except (TradingError, ValueError, SessionConflictError, ExchangeStateError) as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')
    payload = service.get_session(session.session_id)
    return SessionSummary.model_validate(payload)


@app.post('/sessions/single-close', response_model=SessionSummary)
async def create_single_close_session(request: SingleCloseSessionRequest) -> SessionSummary:
    service = current_runtime(app).service
    try:
        session = await service.create_single_close_session(request)
    except SessionPrecheckFailed as exc:
        _raise_api_error(exc, code='session_precheck_failed', source='service', precheck=exc.precheck)
    except (TradingError, ValueError, SessionConflictError, ExchangeStateError) as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')
    payload = service.get_session(session.session_id)
    return SessionSummary.model_validate(payload)


@app.get('/sessions', response_model=list[SessionSummary])
async def list_sessions() -> list[SessionSummary]:
    service = current_runtime(app).service
    return [SessionSummary.model_validate(item) for item in service.list_sessions()]


@app.get('/sessions/{session_id}', response_model=SessionDetail)
async def get_session(session_id: str) -> SessionDetail:
    service = current_runtime(app).service
    try:
        return SessionDetail.model_validate(service.get_session(session_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail='Session not found') from exc


@app.get('/sessions/{session_id}/updates', response_model=SessionUpdatesResponse)
async def get_session_updates(session_id: str, after_event_id: int = Query(default=0, ge=0)) -> SessionUpdatesResponse:
    service = current_runtime(app).service
    try:
        payload = service.get_session_updates(session_id, after_event_id=after_event_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail='Session not found') from exc
    return SessionUpdatesResponse(
        session=SessionSummary.model_validate(payload['session']),
        changed_rounds=payload['changed_rounds'],
        events=payload['events'],
        latest_event_id=payload['latest_event_id'],
    )


@app.post('/sessions/{session_id}/pause', response_model=SessionActionResponse)
async def pause_session(session_id: str) -> SessionActionResponse:
    service = current_runtime(app).service
    try:
        status = await service.pause_session(session_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail='Session not found') from exc
    return SessionActionResponse(
        session_id=session_id,
        status=status,
        requested=True,
        requested_action='pause',
        message='已请求暂停当前会话。',
    )


@app.post('/sessions/{session_id}/resume', response_model=SessionActionResponse)
async def resume_session(session_id: str) -> SessionActionResponse:
    service = current_runtime(app).service
    try:
        status = await service.resume_session(session_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail='Session not found') from exc
    return SessionActionResponse(
        session_id=session_id,
        status=status,
        requested=True,
        requested_action='resume',
        message='已请求恢复当前会话。',
    )


@app.post('/sessions/{session_id}/abort', response_model=SessionActionResponse)
async def abort_session(session_id: str) -> SessionActionResponse:
    service = current_runtime(app).service
    try:
        status = await service.abort_session(session_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail='Session not found') from exc
    return SessionActionResponse(
        session_id=session_id,
        status=status,
        requested=True,
        requested_action='abort',
        message='已请求安全中止当前会话。',
    )


@app.get('/config/whitelist', response_model=WhitelistResponse)
async def get_whitelist() -> WhitelistResponse:
    service = current_runtime(app).service
    return WhitelistResponse(symbols=service.get_whitelist())


@app.put('/config/whitelist', response_model=WhitelistResponse)
async def update_whitelist(request: WhitelistUpdateRequest) -> WhitelistResponse:
    service = current_runtime(app).service
    try:
        symbols = await service.update_whitelist(request.symbols)
    except Exception as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')
    return WhitelistResponse(symbols=symbols)


@app.get('/config/accounts', response_model=AccountListResponse)
async def get_accounts() -> AccountListResponse:
    runtime_manager: AccountRuntimeManager = app.state.runtime_manager
    accounts = [AccountSummary.model_validate(item) for item in runtime_manager.list_accounts()]
    return AccountListResponse(accounts=accounts)


@app.post('/config/accounts/select', response_model=AccountSelectResponse)
async def select_account(request: AccountSelectRequest) -> AccountSelectResponse:
    runtime_manager: AccountRuntimeManager = app.state.runtime_manager
    try:
        payload = await runtime_manager.switch_account(request.account_id)
    except ValueError as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')
    return AccountSelectResponse(account=AccountSummary.model_validate(payload))


@app.get('/symbols/{symbol}', response_model=SymbolInfoResponse)
async def get_symbol_info(symbol: str) -> SymbolInfoResponse:
    service = current_runtime(app).service
    try:
        payload = await service.get_symbol_info(symbol)
    except Exception as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')
    return SymbolInfoResponse.model_validate(payload)



