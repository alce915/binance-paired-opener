from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app_i18n.runtime import DEFAULT_LOCALE, DEFAULT_TIMEZONE, format_copy, frontend_bootstrap_payload, make_api_detail
from paired_opener.account_runtime import AccountRuntimeManager
from paired_opener.config import DEFAULT_LEVERAGE, DEFAULT_TRADING_SYMBOL, Settings, settings
from paired_opener.domain import ExchangeStateError, SessionConflictError
from paired_opener.errors import TradingError, ensure_trading_error, http_status_for_error, invalid_parameter_error
from paired_opener.kanglong.config import load_kanglong_symbol_config
from paired_opener.kanglong.service import KanglongSimulationService
from paired_opener.kanglong.snapshots import build_snapshot_bundle
from paired_opener.kanglong.test_templates import (
    KanglongTemplateStore,
    TemplateStoreError,
    TemplateValidationError,
    build_template_preview_payload,
    preview_account_to_kanglong_snapshot,
    symbol_rules_from_preview_payload,
)
from paired_opener.market_stream import format_sse
from paired_opener.schemas import (
    AccountListResponse,
    AccountSelectRequest,
    AccountSelectResponse,
    AccountSummary,
    CloseSessionRequest,
    KanglongActionRequest,
    KanglongEventsResponse,
    KanglongPlanRequest,
    KanglongPlanResponse,
    KanglongSimulationRunRequest,
    KanglongTemplateDeleteResponse,
    KanglongTemplateListResponse,
    KanglongTemplateMutationResponse,
    KanglongTemplatePreviewRequest,
    MarketConnectRequest,
    OpenSessionRequest,
    SessionActionResponse,
    SessionDetail,
    SessionPrecheckRequest,
    SessionPrecheckResponse,
    SessionSummary,
    SessionUpdatesResponse,
    SimulationActionResponse,
    SimulationAccountSettingsRequest,
    SimulationRunRequest,
    SimulationTemplateRequest,
    SingleCloseSessionRequest,
    SingleOpenSessionRequest,
    SymbolInfoResponse,
    WhitelistResponse,
    WhitelistUpdateRequest,
)
from paired_opener.service import SessionPrecheckFailed
from paired_opener.simulation import SimulationError
from paired_opener.storage import SqliteRepository

STATIC_DIR = Path(__file__).with_name('static')
HTML_CACHE_HEADERS = {'Cache-Control': 'no-store, max-age=0'}
STATIC_CACHE_HEADERS = {'Cache-Control': 'public, max-age=300'}


@asynccontextmanager
async def lifespan(app: FastAPI):
    app_settings = Settings()
    app_settings.load_persisted_whitelist()
    app_settings.load_accounts(include_accounts_file=False)
    repository = SqliteRepository(
        app_settings.database_path,
        session_event_retention_days=app_settings.session_event_retention_days,
        session_event_retention_per_session=app_settings.session_event_retention_per_session,
    )
    repository.prune_event_retention()
    runtime_manager = AccountRuntimeManager(app_settings, repository)
    app.state.settings = app_settings
    app.state.repository = repository
    app.state.runtime_manager = runtime_manager
    app.state.kanglong_service = KanglongSimulationService(repository)
    await runtime_manager.initialize_startup_recovery()
    try:
        yield
    finally:
        await runtime_manager.close()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1024)


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    path = request.url.path
    if path.startswith("/kanglong/simulation/test-templates/") and path.endswith("/preview"):
        for error in exc.errors():
            if "market_data_account_id" in error.get("loc", ()):
                return JSONResponse(
                    status_code=400,
                    content={"detail": {"code": "kanglong_test_template_market_data_account_required"}},
                )
    return JSONResponse(status_code=422, content={"detail": jsonable_encoder(exc.errors())})


def current_runtime(app: FastAPI):
    return app.state.runtime_manager.current()


def _normalize_kanglong_account_id(account_id: str) -> str:
    return account_id.strip().lower()


def _validate_kanglong_account_ids(request: KanglongPlanRequest) -> None:
    main_account_id = _normalize_kanglong_account_id(request.main_account_id)
    seen_subaccounts: set[str] = set()
    for subaccount_id in request.subaccount_ids:
        normalized = _normalize_kanglong_account_id(subaccount_id)
        if normalized == main_account_id or normalized in seen_subaccounts:
            raise HTTPException(
                status_code=400,
                detail={"code": "kanglong_duplicate_account", "account_id": subaccount_id},
            )
        seen_subaccounts.add(normalized)


def _kanglong_template_store() -> KanglongTemplateStore:
    return KanglongTemplateStore(app.state.settings.kanglong_test_templates_file)


def _raise_kanglong_template_error(exc: Exception) -> None:
    if isinstance(exc, TemplateValidationError):
        raise HTTPException(
            status_code=400,
            detail={"code": exc.code, "field": exc.field, "value": exc.value},
        ) from exc
    if isinstance(exc, TemplateStoreError):
        status_code = 404 if exc.code == "kanglong_test_template_not_found" else 400
        raise HTTPException(status_code=status_code, detail={"code": exc.code, **exc.detail}) from exc
    raise exc


def _api_text(value: object) -> str:
    if isinstance(value, Decimal):
        return str(value)
    return str(value or "")


def _position_side_from_payload(position: dict, qty: Decimal) -> str:
    raw = str(position.get("position_side") or position.get("side") or "").strip().upper()
    if raw in {"LONG", "SHORT"}:
        return raw
    return "SHORT" if qty < Decimal("0") else "LONG"


def _kanglong_account_positions_payload(snapshot: dict, symbol: str) -> list[dict]:
    normalized_symbol = symbol.strip().upper()
    positions: list[dict] = []
    for raw_position in snapshot.get("positions") or []:
        if not isinstance(raw_position, dict):
            continue
        position_symbol = str(raw_position.get("symbol") or raw_position.get("contract") or "").strip().upper()
        if position_symbol and position_symbol != normalized_symbol:
            continue
        try:
            signed_qty = Decimal(str(raw_position.get("position_amt") or raw_position.get("positionAmt") or raw_position.get("qty") or "0"))
        except Exception:
            signed_qty = Decimal("0")
        side = _position_side_from_payload(raw_position, signed_qty)
        qty = abs(signed_qty)
        notional = raw_position.get("notional")
        if notional is None:
            try:
                mark_price = Decimal(str(raw_position.get("mark_price") or raw_position.get("markPrice") or "0"))
                notional = qty * mark_price
            except Exception:
                notional = "0"
        positions.append(
            {
                "symbol": position_symbol or normalized_symbol,
                "position_side": side,
                "qty": _api_text(qty),
                "entry_price": _api_text(raw_position.get("entry_price") or raw_position.get("entryPrice")),
                "mark_price": _api_text(raw_position.get("mark_price") or raw_position.get("markPrice")),
                "unrealized_pnl": _api_text(raw_position.get("unrealized_pnl") or raw_position.get("unrealizedPnl")),
                "liquidation_price": _api_text(raw_position.get("liquidation_price") or raw_position.get("liquidationPrice")),
                "notional": _api_text(notional),
                "leverage": raw_position.get("leverage"),
                "margin": _api_text(raw_position.get("margin")),
            }
        )
    return positions


def _kanglong_account_totals_payload(snapshot: dict) -> dict:
    totals = snapshot.get("totals") or {}
    if not isinstance(totals, dict):
        return {}
    return {str(key): _api_text(value) for key, value in totals.items()}


async def _collect_kanglong_plan_inputs(request: KanglongPlanRequest) -> dict:
    if request.account_source == "test_template":
        return await _collect_template_kanglong_plan_inputs(request)
    return await _collect_runtime_kanglong_plan_inputs(request)


def _reject_runtime_template_fields(request: KanglongPlanRequest) -> None:
    if request.test_template_id or request.template_content_hash or request.market_data_account_id:
        raise HTTPException(
            status_code=400,
            detail={"code": "kanglong_test_template_account_mismatch"},
        )


async def _collect_runtime_kanglong_plan_inputs(request: KanglongPlanRequest) -> dict:
    _validate_kanglong_account_ids(request)
    _reject_runtime_template_fields(request)
    runtime_manager: AccountRuntimeManager = app.state.runtime_manager
    account_ids = [request.main_account_id, *request.subaccount_ids]
    gateways = []
    account_payloads = []
    operation_failed = False
    try:
        for account_id in account_ids:
            gateway = runtime_manager.build_temporary_gateway(account_id)
            gateways.append(gateway)
            account_payloads.append(await gateway.get_unified_account_snapshot())
        main_gateway = gateways[0]
        rules = await main_gateway.get_symbol_rules(request.symbol)
        quote = await main_gateway.get_quote(request.symbol)
    except Exception:
        operation_failed = True
        raise
    finally:
        close_error: Exception | None = None
        for gateway in gateways:
            try:
                await gateway.close()
            except Exception as exc:
                if not operation_failed and close_error is None:
                    close_error = exc
        if close_error is not None:
            raise close_error
    config = load_kanglong_symbol_config(app.state.settings, request.symbol)
    snapshot_bundle = build_snapshot_bundle(
        symbol=request.symbol,
        accounts=account_payloads,
        config_version="default",
        symbol_rule_version=request.symbol,
        price_version=f"{quote.bid_price}:{quote.ask_price}",
        leverage=DEFAULT_LEVERAGE,
    )
    snapshots = snapshot_bundle["accounts"]
    return {
        "symbol": request.symbol,
        "main_account_id": request.main_account_id,
        "subaccount_ids": request.subaccount_ids,
        "selected_side": request.selected_side,
        "snapshot_bundle_id": snapshot_bundle["snapshot_bundle_id"],
        "main_snapshot": snapshots[0],
        "subaccount_snapshots": snapshots[1:],
        "config": config,
        "rules": rules,
        "close_price": Decimal(str(quote.bid_price)),
        "open_price": Decimal(str(quote.ask_price)),
        "fee_rate": Decimal("0.0005"),
        "request_metadata": {"account_source": "runtime"},
    }


def _raise_blocked_plan_stale(**detail: Any) -> None:
    raise HTTPException(status_code=409, detail={"code": "blocked_plan_stale", **detail})


def _require_template_plan_field(value: str | None, *, code: str, status_code: int = 400) -> str:
    text = str(value or "").strip()
    if not text:
        raise HTTPException(status_code=status_code, detail={"code": code})
    return text


def _template_input_digest(preview_payload: dict[str, Any]) -> str:
    digest_payload = {
        "template_content_hash": preview_payload.get("template_content_hash"),
        "fee_rate": preview_payload.get("fee_rate"),
        "mark_price_snapshot": preview_payload.get("mark_price_snapshot"),
        "execution_orderbook_snapshot": preview_payload.get("execution_orderbook_snapshot"),
        "symbol_rules": preview_payload.get("symbol_rules"),
    }
    raw = json.dumps(digest_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def _preview_account_leverage_map(template: dict[str, Any], preview_payload: dict[str, Any]) -> dict[str, int]:
    row_leverage = {str(item.get("row_id")): int(item.get("leverage")) for item in template.get("subaccounts") or []}
    leverage_by_account_id: dict[str, int] = {}
    for account in preview_payload.get("accounts") or []:
        account_id = str(account.get("account_id") or "")
        if account.get("role") == "main":
            leverage_by_account_id[account_id] = int((template.get("main_account") or {}).get("leverage") or DEFAULT_LEVERAGE)
        else:
            leverage_by_account_id[account_id] = row_leverage.get(str(account.get("row_id")), DEFAULT_LEVERAGE)
    return leverage_by_account_id


def _template_runtime_account_map(preview_payload: dict[str, Any]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for account in preview_payload.get("accounts") or []:
        template_account_id = str(account.get("template_account_id") or "")
        account_id = str(account.get("account_id") or "")
        if template_account_id and account_id:
            mapping[template_account_id] = account_id
    return mapping


def _template_account_snapshot_payload(
    *,
    template: dict[str, Any],
    preview_payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "account_source": "test_template",
        "template_id": template["id"],
        "template_content_hash": template["template_content_hash"],
        "snapshot_bundle_id": preview_payload["snapshot_bundle_id"],
        "accounts": preview_payload.get("accounts") or [],
    }


def _is_template_runtime_account_id(account_id: str) -> bool:
    return account_id.strip().lower().startswith("tpl:")


def _validate_template_account_role_shape(request: KanglongPlanRequest, accounts_by_id: dict[str, dict[str, Any]]) -> None:
    requested_account_ids = [request.main_account_id, *request.subaccount_ids]
    mismatched_account_ids = [
        account_id
        for account_id in requested_account_ids
        if account_id not in accounts_by_id
    ]
    main_account = accounts_by_id.get(request.main_account_id)
    if main_account is not None and main_account.get("role") != "main":
        mismatched_account_ids.append(request.main_account_id)
    seen_subaccount_ids: set[str] = set()
    for subaccount_id in request.subaccount_ids:
        subaccount = accounts_by_id.get(subaccount_id)
        if (
            subaccount is None
            or subaccount.get("role") != "subaccount"
            or subaccount_id == request.main_account_id
            or subaccount_id in seen_subaccount_ids
        ):
            mismatched_account_ids.append(subaccount_id)
        seen_subaccount_ids.add(subaccount_id)
    if mismatched_account_ids:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "kanglong_test_template_account_mismatch",
                "account_ids": list(dict.fromkeys(mismatched_account_ids)),
            },
        )


async def _collect_template_kanglong_plan_inputs(request: KanglongPlanRequest) -> dict:
    template_id = _require_template_plan_field(
        request.test_template_id,
        code="kanglong_test_template_not_found",
        status_code=404,
    )
    template_content_hash = _require_template_plan_field(
        request.template_content_hash,
        code="blocked_plan_stale",
        status_code=409,
    )
    market_data_account_id = _require_template_plan_field(
        request.market_data_account_id,
        code="kanglong_test_template_market_data_account_required",
    )
    try:
        template = _kanglong_template_store().get_template(template_id)
    except (TemplateValidationError, TemplateStoreError) as exc:
        _raise_kanglong_template_error(exc)
    if request.symbol.strip().upper() != template["symbol"]:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "kanglong_test_template_symbol_mismatch",
                "template_symbol": template["symbol"],
                "request_symbol": request.symbol,
            },
        )
    if template_content_hash != template["template_content_hash"]:
        _raise_blocked_plan_stale(
            template_id=template["id"],
            current_template_content_hash=template["template_content_hash"],
            requested_template_content_hash=template_content_hash,
        )

    preview_payload = await _preview_template_from_market_data(template, market_data_account_id)
    accounts = preview_payload.get("accounts") or []
    accounts_by_id = {str(account.get("account_id") or ""): account for account in accounts}
    requested_account_ids = [request.main_account_id, *request.subaccount_ids]
    _validate_template_account_role_shape(request, accounts_by_id)

    leverage_by_account_id = _preview_account_leverage_map(template, preview_payload)
    snapshots_by_id = {
        account_id: preview_account_to_kanglong_snapshot(
            accounts_by_id[account_id],
            leverage=leverage_by_account_id.get(account_id, DEFAULT_LEVERAGE),
        )
        for account_id in requested_account_ids
    }
    best_bid = Decimal(str(preview_payload["execution_orderbook_snapshot"]["best_bid_price"]))
    best_ask = Decimal(str(preview_payload["execution_orderbook_snapshot"]["best_ask_price"]))
    fee_rate = Decimal(str(preview_payload["fee_rate"]))
    snapshot_payload = _template_account_snapshot_payload(template=template, preview_payload=preview_payload)
    request_metadata = {
        "account_source": "test_template",
        "test_template_id": template["id"],
        "template_content_hash": template["template_content_hash"],
        "template_input_digest": _template_input_digest(preview_payload),
        "market_data_account_id": market_data_account_id,
        "fee_rate_source": preview_payload.get("fee_rate_source"),
        "fee_rate": preview_payload.get("fee_rate"),
        "snapshot_bundle_id": preview_payload["snapshot_bundle_id"],
        "template_runtime_account_map": _template_runtime_account_map(preview_payload),
    }
    return {
        "symbol": request.symbol,
        "main_account_id": request.main_account_id,
        "subaccount_ids": request.subaccount_ids,
        "selected_side": request.selected_side,
        "snapshot_bundle_id": preview_payload["snapshot_bundle_id"],
        "main_snapshot": snapshots_by_id[request.main_account_id],
        "subaccount_snapshots": [snapshots_by_id[account_id] for account_id in request.subaccount_ids],
        "config": load_kanglong_symbol_config(app.state.settings, request.symbol),
        "rules": symbol_rules_from_preview_payload(preview_payload["symbol_rules"], symbol=request.symbol),
        "close_price": best_bid,
        "open_price": best_ask,
        "fee_rate": fee_rate,
        "request_metadata": request_metadata,
        "account_snapshot_payload": snapshot_payload,
    }


def _static_file_response(name: str, *, media_type: str | None = None, cache_headers: dict[str, str] | None = None) -> FileResponse:
    return FileResponse(STATIC_DIR.joinpath(name), media_type=media_type, headers=cache_headers or STATIC_CACHE_HEADERS)


def _inject_bootstrap_before_scripts(html: str, bootstrap: str) -> str:
    first_script_index = html.find('<script')
    if first_script_index != -1:
        line_start = html.rfind('\n', 0, first_script_index)
        insert_at = 0 if line_start == -1 else line_start + 1
        indent = html[insert_at:first_script_index]
        return f'{html[:insert_at]}{indent}{bootstrap}\n{html[insert_at:]}'
    if '</body>' in html:
        return html.replace('</body>', f'{bootstrap}\n</body>', 1)
    return f'{html}\n{bootstrap}'


def _render_html(name: str, app_settings: Settings) -> str:
    html = STATIC_DIR.joinpath(name).read_text(encoding='utf-8')
    config_payload = json.dumps(
        {
            'frontend_execution_log_lines': app_settings.frontend_execution_log_lines,
            'sse_queue_maxsize': app_settings.sse_queue_maxsize,
            'locale': DEFAULT_LOCALE,
            'timezone': DEFAULT_TIMEZONE,
        },
        ensure_ascii=False,
        separators=(',', ':'),
    )
    i18n_payload = json.dumps(
        frontend_bootstrap_payload(namespaces=('common', 'console', 'reasons', 'runtime', 'events', 'precheck', 'log')),
        ensure_ascii=False,
        separators=(',', ':'),
    )
    bootstrap = (
        f'<script>window.__APP_CONFIG__ = {config_payload};'
        f'window.__APP_I18N__ = {i18n_payload};</script>'
    )
    return _inject_bootstrap_before_scripts(html, bootstrap)


def _render_index_html(app_settings: Settings) -> str:
    return _render_html('index.html', app_settings)


def _render_monitor_html(app_settings: Settings) -> str:
    return _render_html('monitor.html', app_settings)


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


def _missing_session_http_error(session_id: str) -> HTTPException:
    return HTTPException(
        status_code=404,
        detail=make_api_detail(
            code='session_not_found',
            params={'session_id': session_id},
            category='invalid_parameter',
            strategy='terminate',
            source='api',
            message=format_copy('reasons.session_not_found', {'session_id': session_id}),
        ),
    )


def _raise_simulation_conflict() -> None:
    raise HTTPException(
        status_code=409,
        detail=make_api_detail(
            code='execution_conflict',
            params={},
            category='invalid_state',
            strategy='retry_later',
            source='api',
            message=format_copy('runtime.execution_conflict_simulation_active'),
        ),
    )


def _raise_real_session_conflict() -> None:
    raise HTTPException(
        status_code=409,
        detail=make_api_detail(
            code='execution_conflict',
            params={},
            category='invalid_state',
            strategy='retry_later',
            source='api',
            message=format_copy('runtime.execution_conflict_real_active'),
        ),
    )


def _raise_simulation_busy_conflict(message: str | None = None) -> None:
    raise HTTPException(
        status_code=409,
        detail=make_api_detail(
            code='execution_conflict',
            params={},
            category='invalid_state',
            strategy='retry_later',
            source='api',
            message=message or format_copy('runtime.simulation_busy'),
        ),
    )


@app.get('/', include_in_schema=False)
async def index() -> HTMLResponse:
    return HTMLResponse(_render_index_html(app.state.settings), headers=HTML_CACHE_HEADERS)


@app.get('/static/app.js', include_in_schema=False)
async def static_app_js() -> FileResponse:
    return _static_file_response('app.js', media_type='application/javascript', cache_headers=HTML_CACHE_HEADERS)


@app.get('/static/monitor.html', include_in_schema=False)
async def static_monitor_html() -> HTMLResponse:
    return HTMLResponse(_render_monitor_html(app.state.settings), headers=HTML_CACHE_HEADERS)

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
    runtime = current_runtime(app)
    if getattr(runtime, 'service', None) is not None and runtime.service.has_active_sessions():
        _raise_real_session_conflict()
    if getattr(runtime, 'simulation', None) is not None:
        try:
            return await runtime.simulation.start_run(request)
        except SimulationError as exc:
            _raise_simulation_busy_conflict(str(exc))
        except Exception as exc:
            _raise_api_error(exc, code='trading_request_failed', source='service')
    market = runtime.market
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
            execution_profile=request.execution_profile.value if request.execution_profile is not None else None,
            market_fallback_max_ratio=request.market_fallback_max_ratio,
            market_fallback_min_residual_qty=request.market_fallback_min_residual_qty,
            max_reprice_ticks=request.max_reprice_ticks,
            max_spread_bps=request.max_spread_bps,
            max_reference_deviation_bps=request.max_reference_deviation_bps,
        )
    except Exception as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')


@app.post("/kanglong/simulation/plan", response_model=KanglongPlanResponse)
async def create_kanglong_simulation_plan(request: KanglongPlanRequest) -> KanglongPlanResponse:
    if request.mode != "simulation":
        raise HTTPException(status_code=400, detail={"code": "kanglong_live_mode_not_supported"})
    run_id = str(uuid4())
    try:
        inputs = await _collect_kanglong_plan_inputs(request)
        payload = app.state.kanglong_service.create_plan(run_id=run_id, **inputs)
    except HTTPException:
        raise
    except Exception as exc:
        _raise_api_error(exc, code="kanglong_plan_failed", source="service")
    return KanglongPlanResponse.model_validate(payload)


@app.get("/kanglong/simulation/test-templates", response_model=KanglongTemplateListResponse)
async def list_kanglong_test_templates() -> KanglongTemplateListResponse:
    try:
        payload = _kanglong_template_store().list_templates()
    except (TemplateValidationError, TemplateStoreError) as exc:
        _raise_kanglong_template_error(exc)
    return KanglongTemplateListResponse.model_validate(payload)


@app.post("/kanglong/simulation/test-templates", response_model=KanglongTemplateMutationResponse)
async def create_kanglong_test_template(template: dict[str, Any]) -> KanglongTemplateMutationResponse:
    try:
        saved = _kanglong_template_store().upsert_template(template)
    except (TemplateValidationError, TemplateStoreError) as exc:
        _raise_kanglong_template_error(exc)
    return KanglongTemplateMutationResponse(template=saved)


@app.put("/kanglong/simulation/test-templates/{template_id}", response_model=KanglongTemplateMutationResponse)
async def update_kanglong_test_template(template_id: str, template: dict[str, Any]) -> KanglongTemplateMutationResponse:
    try:
        saved = _kanglong_template_store().upsert_template({**template, "id": template_id})
    except (TemplateValidationError, TemplateStoreError) as exc:
        _raise_kanglong_template_error(exc)
    return KanglongTemplateMutationResponse(template=saved)


@app.post("/kanglong/simulation/test-templates/{template_id}/clone", response_model=KanglongTemplateMutationResponse)
async def clone_kanglong_test_template(template_id: str) -> KanglongTemplateMutationResponse:
    try:
        cloned = _kanglong_template_store().clone_template(template_id)
    except (TemplateValidationError, TemplateStoreError) as exc:
        _raise_kanglong_template_error(exc)
    return KanglongTemplateMutationResponse(template=cloned)


@app.delete("/kanglong/simulation/test-templates/{template_id}", response_model=KanglongTemplateDeleteResponse)
async def delete_kanglong_test_template(template_id: str) -> KanglongTemplateDeleteResponse:
    try:
        deleted = _kanglong_template_store().delete_template(template_id)
    except (TemplateValidationError, TemplateStoreError) as exc:
        _raise_kanglong_template_error(exc)
    return KanglongTemplateDeleteResponse(status="deleted", template_id=str(deleted.get("id") or template_id))


@app.post("/kanglong/simulation/test-templates/store/recover-backup", response_model=KanglongTemplateListResponse)
async def recover_kanglong_test_template_backup() -> KanglongTemplateListResponse:
    try:
        payload = _kanglong_template_store().recover_backup()
    except (TemplateValidationError, TemplateStoreError) as exc:
        _raise_kanglong_template_error(exc)
    return KanglongTemplateListResponse.model_validate(payload)


async def _preview_template_from_market_data(template: dict[str, Any], market_data_account_id: str) -> dict[str, Any]:
    market_data_account_id = str(market_data_account_id or "").strip()
    if not market_data_account_id:
        raise HTTPException(
            status_code=400,
            detail={"code": "kanglong_test_template_market_data_account_required"},
        )
    if _is_template_runtime_account_id(market_data_account_id):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "kanglong_test_template_market_data_account_unavailable",
                "account_id": market_data_account_id,
            },
        )
    runtime_manager: AccountRuntimeManager = app.state.runtime_manager
    gateway = None
    template_error: TemplateValidationError | TemplateStoreError | None = None
    market_error: Exception | None = None
    preview_payload: dict[str, Any] | None = None
    try:
        gateway = runtime_manager.build_temporary_gateway(market_data_account_id)
        rules = await gateway.get_symbol_rules(template["symbol"])
        quote = await gateway.get_quote(template["symbol"])
        orderbook = await gateway.get_order_book(template["symbol"])
        config = load_kanglong_symbol_config(app.state.settings, template["symbol"])
        preview_payload = build_template_preview_payload(template, quote, orderbook, rules, config)
    except (TemplateValidationError, TemplateStoreError) as exc:
        template_error = exc
    except Exception as exc:
        market_error = exc
    finally:
        if gateway is not None:
            try:
                await gateway.close()
            except Exception as exc:
                if template_error is None and market_error is None:
                    market_error = exc
    if template_error is not None:
        _raise_kanglong_template_error(template_error)
    if market_error is not None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "kanglong_test_template_market_data_account_unavailable",
                "account_id": market_data_account_id,
            },
        ) from market_error
    return preview_payload or {}


@app.post("/kanglong/simulation/test-templates/{template_id}/preview")
async def preview_kanglong_test_template(template_id: str, request: KanglongTemplatePreviewRequest) -> dict[str, Any]:
    market_data_account_id = request.market_data_account_id.strip()
    if not market_data_account_id:
        raise HTTPException(
            status_code=400,
            detail={"code": "kanglong_test_template_market_data_account_required"},
        )
    try:
        template = _kanglong_template_store().get_template(template_id)
    except (TemplateValidationError, TemplateStoreError) as exc:
        _raise_kanglong_template_error(exc)
    return await _preview_template_from_market_data(template, market_data_account_id)


@app.get("/kanglong/simulation/accounts", response_model=AccountListResponse)
async def list_kanglong_simulation_accounts(symbol: str = Query(default=DEFAULT_TRADING_SYMBOL)) -> dict:
    normalized_symbol = symbol.strip().upper() or DEFAULT_TRADING_SYMBOL
    runtime_manager: AccountRuntimeManager = app.state.runtime_manager
    accounts = []
    for account in runtime_manager.list_accounts():
        account_id = str(account.get("id") or account.get("account_id") or "").strip().lower()
        payload = {
            "id": account_id,
            "name": account.get("name") or account.get("account_name") or account_id,
            "is_active": bool(account.get("is_active")),
            "positions": [],
            "totals": {},
            "snapshot_version": None,
            "risk_unknown": False,
            "status": "ok",
        }
        gateway = None
        try:
            gateway = runtime_manager.build_temporary_gateway(account_id)
            snapshot = await gateway.get_unified_account_snapshot()
            payload["positions"] = _kanglong_account_positions_payload(snapshot, normalized_symbol)
            payload["totals"] = _kanglong_account_totals_payload(snapshot)
            payload["snapshot_version"] = str(snapshot.get("updated_at") or snapshot.get("snapshot_version") or "")
            payload["name"] = snapshot.get("account_name") or payload["name"]
        except Exception as exc:
            payload["risk_unknown"] = True
            payload["status"] = "snapshot_failed"
            payload["error"] = str(exc)
        finally:
            if gateway is not None:
                try:
                    await gateway.close()
                except Exception:
                    payload["risk_unknown"] = True
                    payload["status"] = "snapshot_close_failed"
        accounts.append(payload)
    return {"accounts": accounts}


@app.post("/kanglong/simulation/plan/{run_id}/confirm", response_model=KanglongPlanResponse)
async def confirm_kanglong_simulation_plan(run_id: str, request: KanglongActionRequest) -> KanglongPlanResponse:
    payload = app.state.kanglong_service.confirm_plan(
        run_id=run_id,
        plan_version=request.plan_version,
        idempotency_key=request.idempotency_key,
        operator=request.operator,
        confirmed_warning_codes=request.confirmed_warning_codes,
    )
    return KanglongPlanResponse.model_validate(payload)


@app.post("/kanglong/simulation/plan/{run_id}/execute", response_model=KanglongPlanResponse)
async def execute_kanglong_simulation_plan(run_id: str, request: KanglongActionRequest) -> KanglongPlanResponse:
    service = app.state.kanglong_service
    stored = service.get_run(run_id)
    execute_kwargs = {
        "run_id": run_id,
        "plan_version": request.plan_version,
        "idempotency_key": request.idempotency_key,
        "close_price": Decimal("0"),
        "open_price": Decimal("0"),
        "fee_rate": Decimal("0"),
    }
    if (
        stored is not None
        and stored.get("plan_version") == request.plan_version
        and stored.get("status") == "plan_confirmed"
    ):
        stored_request = stored.get("request") or {}
        if stored_request.get("main_account_id") and stored_request.get("subaccount_ids"):
            selected_side = (stored.get("plan") or {}).get("selected_side")
            plan_request = KanglongPlanRequest(
                mode=stored_request.get("mode") or "simulation",
                symbol=stored_request.get("symbol") or stored.get("symbol"),
                main_account_id=stored_request["main_account_id"],
                subaccount_ids=list(stored_request["subaccount_ids"]),
                selected_side=selected_side,
                account_source=stored_request.get("account_source") or "runtime",
                test_template_id=stored_request.get("test_template_id"),
                template_content_hash=stored_request.get("template_content_hash"),
                market_data_account_id=stored_request.get("market_data_account_id"),
            )
            try:
                inputs = await _collect_kanglong_plan_inputs(plan_request)
            except HTTPException:
                raise
            except Exception as exc:
                _raise_api_error(exc, code="kanglong_plan_failed", source="service")
            execute_kwargs.update(
                {
                    "close_price": inputs["close_price"],
                    "open_price": inputs["open_price"],
                    "fee_rate": inputs["fee_rate"],
                    "recheck_main_snapshot": inputs["main_snapshot"],
                    "recheck_subaccount_snapshots": inputs["subaccount_snapshots"],
                    "recheck_selected_side": inputs["selected_side"],
                    "recheck_config": inputs["config"],
                    "recheck_snapshot_bundle_id": inputs["snapshot_bundle_id"],
                }
            )
    payload = service.execute_plan(**execute_kwargs)
    return KanglongPlanResponse.model_validate(payload)


@app.get("/kanglong/simulation/run/{run_id}/events", response_model=KanglongEventsResponse)
async def get_kanglong_simulation_events(
    run_id: str,
    after_event_id: int = 0,
    limit: int = 200,
) -> KanglongEventsResponse:
    if app.state.kanglong_service.get_run(run_id) is None:
        raise HTTPException(status_code=404, detail={"code": "kanglong_run_not_found", "run_id": run_id})
    payload = app.state.kanglong_service.list_events(run_id, after_event_id=after_event_id, limit=limit)
    return KanglongEventsResponse.model_validate(payload)


@app.post("/kanglong/simulation/run", response_model=KanglongPlanResponse)
async def run_kanglong_simulation(request: KanglongSimulationRunRequest) -> KanglongPlanResponse:
    raise HTTPException(
        status_code=410,
        detail={"code": "kanglong_run_endpoint_deprecated", "replacement": "/kanglong/simulation/plan"},
    )


@app.get("/kanglong/simulation/run/active")
async def get_active_kanglong_simulation() -> dict:
    payload = app.state.kanglong_service.active_run()
    if payload is None:
        return {"status": "idle", "available_actions": ["create_plan"]}
    return payload


@app.get('/kanglong/simulation/run/{run_id}')
async def get_kanglong_simulation(run_id: str) -> dict:
    payload = app.state.kanglong_service.get_run(run_id)
    if payload is None:
        raise HTTPException(status_code=404, detail={"code": "kanglong_run_not_found", "run_id": run_id})
    return payload


@app.post('/simulation/abort', response_model=SimulationActionResponse)
async def abort_simulation() -> SimulationActionResponse:
    runtime = current_runtime(app)
    if getattr(runtime, 'simulation', None) is not None:
        payload = await runtime.simulation.abort()
    else:
        payload = await runtime.market.abort_simulation()
    return SimulationActionResponse.model_validate(payload)


@app.get('/simulation/run/active')
async def get_active_simulation_run() -> dict:
    return await current_runtime(app).simulation.active_run()


@app.get('/simulation/run/{run_id}/updates')
async def get_simulation_run_updates(run_id: str, after_event_id: int = Query(default=0, ge=0)) -> dict:
    try:
        return await current_runtime(app).simulation.run_updates(run_id, after_event_id=after_event_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail={'code': 'simulation_run_not_found', 'run_id': run_id}) from exc


@app.get('/simulation/account')
async def get_simulation_account() -> dict:
    return await current_runtime(app).simulation.get_account()


@app.post('/simulation/account/settings')
async def update_simulation_account_settings(request: SimulationAccountSettingsRequest) -> dict:
    try:
        return await current_runtime(app).simulation.update_account_settings(
            initial_balance=request.initial_balance,
            maker_fee_rate=request.maker_fee_rate,
            taker_fee_rate=request.taker_fee_rate,
        )
    except SimulationError as exc:
        _raise_simulation_busy_conflict(str(exc))
    except Exception as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')


@app.post('/simulation/account/reset')
async def reset_simulation_account() -> dict:
    try:
        return await current_runtime(app).simulation.reset_account()
    except SimulationError as exc:
        _raise_simulation_busy_conflict(str(exc))
    except Exception as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')


@app.get('/simulation/history')
async def list_simulation_history(page: int = Query(default=1, ge=1), page_size: int = Query(default=20, ge=1, le=200)) -> dict:
    return await current_runtime(app).simulation.list_history(page=page, page_size=page_size)


@app.delete('/simulation/history')
async def clear_simulation_history() -> dict:
    try:
        return await current_runtime(app).simulation.clear_history()
    except SimulationError as exc:
        _raise_simulation_busy_conflict(str(exc))
    except Exception as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')


@app.get('/simulation/history/export.csv')
async def export_simulation_history_csv() -> Response:
    payload = await current_runtime(app).simulation.export_history_csv()
    return Response(
        content=payload,
        media_type='text/csv; charset=utf-8',
        headers={'Content-Disposition': 'attachment; filename="simulation-history.csv"', **HTML_CACHE_HEADERS},
    )


@app.get('/simulation/history/{run_id}')
async def get_simulation_history_detail(run_id: str) -> dict:
    try:
        return await current_runtime(app).simulation.get_history_detail(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail={'code': 'simulation_run_not_found', 'run_id': run_id}) from exc


@app.post('/simulation/history/{run_id}/rerun')
async def rerun_simulation_history(run_id: str) -> dict:
    runtime = current_runtime(app)
    if runtime.service.has_active_sessions():
        _raise_real_session_conflict()
    try:
        return await runtime.simulation.start_rerun(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail={'code': 'simulation_run_not_found', 'run_id': run_id}) from exc
    except SimulationError as exc:
        _raise_simulation_busy_conflict(str(exc))
    except Exception as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')


@app.get('/simulation/templates')
async def list_simulation_templates() -> dict:
    return await current_runtime(app).simulation.list_templates()


@app.post('/simulation/templates')
async def save_simulation_template(request: SimulationTemplateRequest) -> dict:
    return await current_runtime(app).simulation.save_template(name=request.name, payload=request.payload)


@app.delete('/simulation/templates/{template_id}')
async def delete_simulation_template(template_id: str) -> dict:
    return await current_runtime(app).simulation.delete_template(template_id)


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
    runtime = current_runtime(app)
    if runtime.simulation.is_active():
        _raise_simulation_conflict()
    service = runtime.service
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
    runtime = current_runtime(app)
    if runtime.simulation.is_active():
        _raise_simulation_conflict()
    service = runtime.service
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
    runtime = current_runtime(app)
    if runtime.simulation.is_active():
        _raise_simulation_conflict()
    service = runtime.service
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
    runtime = current_runtime(app)
    if runtime.simulation.is_active():
        _raise_simulation_conflict()
    service = runtime.service
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
        raise _missing_session_http_error(session_id) from exc


@app.get('/sessions/{session_id}/updates', response_model=SessionUpdatesResponse)
async def get_session_updates(session_id: str, after_event_id: int = Query(default=0, ge=0)) -> SessionUpdatesResponse:
    service = current_runtime(app).service
    try:
        payload = service.get_session_updates(session_id, after_event_id=after_event_id)
    except KeyError as exc:
        raise _missing_session_http_error(session_id) from exc
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
        raise _missing_session_http_error(session_id) from exc
    return SessionActionResponse(
        session_id=session_id,
        status=status,
        requested=True,
        requested_action='pause',
        message_code='runtime.session_pause_requested',
        message=format_copy('runtime.session_pause_requested'),
    )


@app.post('/sessions/{session_id}/resume', response_model=SessionActionResponse)
async def resume_session(session_id: str) -> SessionActionResponse:
    service = current_runtime(app).service
    try:
        status = await service.resume_session(session_id)
    except KeyError as exc:
        raise _missing_session_http_error(session_id) from exc
    except (TradingError, ValueError, SessionConflictError, ExchangeStateError) as exc:
        _raise_api_error(exc, code='trading_request_failed', source='service')
    return SessionActionResponse(
        session_id=session_id,
        status=status,
        requested=True,
        requested_action='resume',
        message_code='runtime.session_resume_requested',
        message=format_copy('runtime.session_resume_requested'),
    )


@app.post('/sessions/{session_id}/abort', response_model=SessionActionResponse)
async def abort_session(session_id: str) -> SessionActionResponse:
    service = current_runtime(app).service
    try:
        status = await service.abort_session(session_id)
    except KeyError as exc:
        raise _missing_session_http_error(session_id) from exc
    return SessionActionResponse(
        session_id=session_id,
        status=status,
        requested=True,
        requested_action='abort',
        message_code='runtime.session_abort_requested',
        message=format_copy('runtime.session_abort_requested'),
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




