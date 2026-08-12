from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import inspect
import json
import secrets
from contextlib import asynccontextmanager
from dataclasses import fields, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app_i18n.runtime import CONTRACT_VERSION, DEFAULT_LOCALE, DEFAULT_TIMEZONE, format_copy, frontend_bootstrap_payload, make_api_detail
from paired_opener.account_credentials import (
    AccountCredentialStore,
    CredentialImportPreviewStore,
    CredentialPreviewInvalid,
    CredentialRevisionConflict,
    CredentialStoreUnavailable,
    VerifiedCredentialCandidate,
    WindowsDpapiProtector,
    mask_api_key,
)
from paired_opener.account_runtime import (
    AccountCredentialCommitCoordinator,
    AccountCredentialsLockedByActiveBatch,
    AccountCredentialsNotConfigured,
    AccountRuntimeManager,
    KanglongReadOnlyRuntimeManager,
    RepositoryAccountMutationGuard,
)
from paired_opener.config import AccountConfig, DEFAULT_LEVERAGE, DEFAULT_TRADING_SYMBOL, Settings, settings
from paired_opener.domain import ExchangeStateError, PositionSide, SessionConflictError, SymbolRules
from paired_opener.errors import TradingError, ensure_trading_error, http_status_for_error, invalid_parameter_error
from paired_opener.kanglong.config import KanglongSymbolConfig, load_kanglong_symbol_config
from paired_opener.kanglong.batch_capacity import (
    CALCULATION_VERSION,
    CapacityPolicy,
    CapacitySnapshot,
    CapacitySnapshotCoordinator,
    estimate_account_capacity,
    estimate_batch_capacity,
)
from paired_opener.kanglong.batch_executor import KanglongBatchExecutor
from paired_opener.kanglong.batch_models import KanglongBatchAccountPlan, KanglongBatchPlan, stable_payload_hash
from paired_opener.kanglong.batch_planner import KanglongBatchPlanner, UnsafeBatchRefresh
from paired_opener.kanglong.batch_settings import KanglongBatchDefaults, KanglongBatchDefaultsStore
from paired_opener.kanglong.models import KanglongRunStatus, available_actions_for_status
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
from paired_opener.kanglong.task_registry import (
    KanglongCompatibilityTaskRegistry,
    KanglongExecutionTaskRegistry,
)
from paired_opener.market_stream import format_sse
from paired_opener.schemas import (
    AccountListResponse,
    AccountCredentialCreateRequest,
    AccountCredentialImportCommitRequest,
    AccountCredentialImportPreviewRequest,
    AccountCredentialOrderRequest,
    AccountCredentialUpdateRequest,
    AccountSelectRequest,
    AccountSelectResponse,
    AccountSummary,
    CloseSessionRequest,
    KanglongActionRequest,
    KanglongBatchCapacityPreviewRequest,
    KanglongBatchPlanRequest,
    KanglongBatchRecoverRequest,
    KanglongBatchRunResponse,
    KanglongControlRequest,
    KanglongEventsResponse,
    KanglongPlanRequest,
    KanglongPlanResponse,
    KanglongRecoverRequest,
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
from paired_opener.simulation_matching import GatewayMarketDataProvider
from paired_opener.single_instance import SingleInstanceGuard
from paired_opener.storage import KanglongActionMutation, SqliteRepository

STATIC_DIR = Path(__file__).with_name('static')
HTML_CACHE_HEADERS = {'Cache-Control': 'no-store, max-age=0'}
STATIC_CACHE_HEADERS = {'Cache-Control': 'public, max-age=300'}
_compatibility_execution_registry = KanglongCompatibilityTaskRegistry()


class RequestBodyTooLarge(RuntimeError):
    pass


class CredentialImportBodyLimitMiddleware:
    LIMIT = 256 * 1024
    PATH = "/config/account-credentials/import/preview"

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http" or scope.get("path") != self.PATH:
            await self.app(scope, receive, send)
            return
        total = 0
        buffered: list[dict[str, Any]] = []
        while True:
            message = await receive()
            if message.get("type") == "http.request":
                total += len(message.get("body", b""))
                if total > self.LIMIT:
                    response = JSONResponse(
                        status_code=413,
                        content={"detail": {"code": "credential_import_too_large"}},
                    )
                    await response(scope, receive, send)
                    return
            buffered.append(message)
            if message.get("type") != "http.request" or not message.get("more_body", False):
                break

        async def replay_receive():
            if buffered:
                return buffered.pop(0)
            return {"type": "http.request", "body": b"", "more_body": False}

        await self.app(scope, replay_receive, send)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app_settings = Settings()
    instance_guard = SingleInstanceGuard.acquire(app_settings.data_dir)
    repository: SqliteRepository | None = None
    runtime_manager: AccountRuntimeManager | None = None
    execution_registry: KanglongExecutionTaskRegistry | None = None
    try:
        app_settings.load_persisted_whitelist()
        credential_store = AccountCredentialStore(
            app_settings.binance_accounts_secure_file,
            WindowsDpapiProtector(),
        )
        migration_required = False
        if credential_store.path.exists():
            accounts = credential_store.load()
            app_settings.accounts = {account.account_id: account for account in accounts}
            if app_settings.active_account_id not in app_settings.accounts:
                app_settings.active_account_id = next(iter(app_settings.accounts), "")
        else:
            app_settings.load_accounts(include_accounts_file=False, allow_empty=True)
            migration_required = bool(app_settings.accounts)

        repository = SqliteRepository(
            app_settings.database_path,
            session_event_retention_days=app_settings.session_event_retention_days,
            session_event_retention_per_session=app_settings.session_event_retention_per_session,
        )
        repository.prune_event_retention()
        runtime_manager = AccountRuntimeManager(app_settings, repository)
        kanglong_readonly_runtime_manager = KanglongReadOnlyRuntimeManager(runtime_manager)
        mutation_guard = RepositoryAccountMutationGuard(repository)
        app.state.settings = app_settings
        app.state.repository = repository
        app.state.runtime_manager = runtime_manager
        app.state.kanglong_readonly_runtime_manager = kanglong_readonly_runtime_manager
        app.state.kanglong_service = KanglongSimulationService(repository)
        app.state.kanglong_batch_defaults_store = KanglongBatchDefaultsStore(
            app_settings.kanglong_batch_defaults_file,
        )
        app.state.capacity_snapshot_coordinator = CapacitySnapshotCoordinator(
            kanglong_readonly_runtime_manager,
            fast_ttl_ms=app_settings.kanglong_capacity_fast_ttl_ms,
            slow_ttl_ms=app_settings.kanglong_capacity_slow_ttl_ms,
            private_concurrency=app_settings.kanglong_capacity_private_concurrency,
        )
        async def close_snapshot_loader(account_id: str, symbol: str, force_refresh: bool) -> Any:
            snapshots = await app.state.capacity_snapshot_coordinator.refresh_capacity(
                credential_store.current_revision(),
                [account_id],
                symbol,
                force_refresh=force_refresh,
            )
            return snapshots[account_id]

        app.state.kanglong_batch_planner = KanglongBatchPlanner(
            repository,
            close_snapshot_loader=close_snapshot_loader,
        )
        app.state.kanglong_batch_executor = KanglongBatchExecutor(
            repository,
            kanglong_readonly_runtime_manager,
            credential_store.current_revision,
            app.state.capacity_snapshot_coordinator,
            app_settings,
            close_planner=app.state.kanglong_batch_planner,
        )
        execution_registry = KanglongExecutionTaskRegistry(
            repository,
            app.state.kanglong_batch_executor,
            transfer_worker=_registry_transfer_worker,
        )
        app.state.kanglong_execution_task_registry = execution_registry
        app.state.account_credential_store = credential_store
        app.state.account_mutation_guard = mutation_guard
        app.state.account_credential_commit_coordinator = AccountCredentialCommitCoordinator(
            credential_store,
            runtime_manager,
            mutation_guard,
        )
        app.state.credential_imports = CredentialImportPreviewStore()
        app.state.account_credentials_migration_required = migration_required
        app.state.local_management_token = secrets.token_urlsafe(32)
        await runtime_manager.initialize_startup_recovery()
        await execution_registry.initialize_startup_recovery()
        yield
    finally:
        if execution_registry is not None:
            await execution_registry.aclose(grace_seconds=15)
        if runtime_manager is not None:
            await runtime_manager.aclose()
        if repository is not None:
            repository.close()
        instance_guard.close()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(CredentialImportBodyLimitMiddleware)


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    path = request.url.path
    if path.startswith("/config/account-credentials"):
        for error in exc.errors():
            if "credential_type" in error.get("loc", ()):
                return JSONResponse(
                    status_code=422,
                    content={"detail": {"code": "credential_type_not_supported"}},
                )
    if path.startswith("/kanglong/simulation/test-templates/") and path.endswith("/preview"):
        for error in exc.errors():
            if "market_data_account_id" in error.get("loc", ()):
                return JSONResponse(
                    status_code=400,
                    content={"detail": {"code": "kanglong_test_template_market_data_account_required"}},
                )
    return JSONResponse(status_code=422, content={"detail": jsonable_encoder(exc.errors())})


def current_runtime(app: FastAPI):
    try:
        return app.state.runtime_manager.current()
    except AccountCredentialsNotConfigured as exc:
        raise HTTPException(
            status_code=503,
            detail={"code": "account_credentials_not_configured"},
        ) from exc


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


def _template_with_generated_id(template: dict[str, Any]) -> dict[str, Any]:
    if str(template.get("id") or "").strip():
        return template
    return {**template, "id": f"tpl_{uuid4().hex[:12]}"}


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


def _raise_blocked_plan_stale(**detail: Any) -> None:
    raise HTTPException(status_code=409, detail={"code": "blocked_plan_stale", **detail})


def _validate_template_run_not_stale(stored: dict[str, Any]) -> None:
    request_payload = stored.get("request") or {}
    if request_payload.get("account_source") != "test_template":
        return
    template_id = request_payload.get("test_template_id")
    template_content_hash = request_payload.get("template_content_hash")
    if not template_id or not template_content_hash:
        _raise_blocked_plan_stale(run_id=stored.get("run_id"))
    try:
        template = _kanglong_template_store().get_template(str(template_id))
    except Exception as exc:
        if isinstance(exc, (TemplateStoreError, TemplateValidationError)):
            _raise_blocked_plan_stale(run_id=stored.get("run_id"), template_id=template_id)
        raise
    if template.get("template_content_hash") != template_content_hash:
        _raise_blocked_plan_stale(
            run_id=stored.get("run_id"),
            template_id=template_id,
            current_template_content_hash=template.get("template_content_hash"),
            requested_template_content_hash=template_content_hash,
        )


def _kanglong_execution_market_data_account_id(stored: dict[str, Any] | None, execute_kwargs: dict[str, Any]) -> str:
    explicit = str(execute_kwargs.get("market_data_account_id") or "").strip()
    if explicit:
        return explicit
    request_payload = (stored or {}).get("request") or {}
    if request_payload.get("account_source") == "test_template":
        return str(request_payload.get("market_data_account_id") or "").strip()
    return str(request_payload.get("main_account_id") or (stored or {}).get("main_account_id") or "").strip()


async def _run_kanglong_market_execution_in_background(service: Any, execute_kwargs: dict[str, Any]) -> None:
    run_market_execution = getattr(service, "run_market_execution", None)
    if not callable(run_market_execution):
        return
    runner_kwargs = dict(execute_kwargs)
    gateway = None
    try:
        if runner_kwargs.get("market_data") is None:
            get_run = getattr(service, "get_run", None)
            stored = get_run(str(runner_kwargs.get("run_id") or "")) if callable(get_run) else None
            market_data_account_id = _kanglong_execution_market_data_account_id(stored, runner_kwargs)
            runtime_manager: AccountRuntimeManager | None = getattr(app.state, "runtime_manager", None)
            if runtime_manager is None or not market_data_account_id:
                raise RuntimeError("kanglong_market_data_account_unavailable")
            gateway = runtime_manager.build_temporary_gateway(market_data_account_id)
            runner_kwargs["market_data"] = GatewayMarketDataProvider(gateway)
        result = run_market_execution(**runner_kwargs)
        if inspect.isawaitable(result):
            await result
    finally:
        if gateway is not None:
            await gateway.close()


async def _complete_kanglong_execution_in_background(service: Any, execute_kwargs: dict[str, Any]) -> None:
    try:
        run_market_execution = getattr(service, "run_market_execution", None)
        if callable(run_market_execution):
            await _run_kanglong_market_execution_in_background(service, execute_kwargs)
            return
        complete_execution = getattr(service, "complete_started_execution", None)
        if not callable(complete_execution):
            return
        await asyncio.to_thread(complete_execution, **execute_kwargs)
    except Exception as exc:
        mark_failed = getattr(service, "mark_execution_failed", None)
        if callable(mark_failed):
            try:
                await asyncio.to_thread(
                    mark_failed,
                    run_id=execute_kwargs["run_id"],
                    plan_version=execute_kwargs["plan_version"],
                    error=exc,
                )
            except Exception as mark_exc:
                print(f"Kanglong background failure marker failed: {mark_exc}", flush=True)
        print(f"Kanglong background execution failed: {exc}", flush=True)


def _schedule_kanglong_execution(service: Any, execute_kwargs: dict[str, Any]) -> None:
    run_id = str(execute_kwargs.get("run_id") or "")
    if not run_id:
        return
    registry = getattr(app.state, "kanglong_execution_task_registry", None)
    if registry is not None:
        registry.start(run_id)
        return
    _compatibility_execution_registry.start(
        run_id,
        lambda: _complete_kanglong_execution_in_background(service, dict(execute_kwargs)),
    )


async def _registry_transfer_worker(run_id: str) -> None:
    service = app.state.kanglong_service
    stored = service.get_run(run_id)
    if stored is None:
        return
    execute_kwargs = _started_kanglong_execution_kwargs(
        run_id=run_id,
        request=KanglongActionRequest(
            plan_version=str(stored.get("plan_version") or ""),
            idempotency_key=f"registry-{run_id}",
        ),
        stored=stored,
    )
    if execute_kwargs is not None:
        await _complete_kanglong_execution_in_background(service, execute_kwargs)


def _api_text(value: object) -> str:
    if isinstance(value, Decimal):
        return str(value)
    return str(value or "")


def _decimal_from_payload(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value if value is not None else default))
    except Exception:
        return Decimal(default)


def _symbol_rules_from_payload(payload: Any, *, fallback_symbol: str) -> SymbolRules | None:
    if not isinstance(payload, dict):
        return None
    return SymbolRules(
        str(payload.get("symbol") or fallback_symbol or "ETHUSDC").strip().upper(),
        _decimal_from_payload(payload.get("tick_size"), "0.01"),
        _decimal_from_payload(payload.get("step_size"), "0.001"),
        _decimal_from_payload(payload.get("min_qty"), "0.001"),
        _decimal_from_payload(payload.get("min_notional"), "5"),
        int(payload.get("max_leverage") or 125),
    )


def _kanglong_config_from_payload(payload: Any) -> KanglongSymbolConfig | None:
    if not isinstance(payload, dict):
        return None
    values: dict[str, Any] = {}
    for field in fields(KanglongSymbolConfig):
        if field.name not in payload:
            continue
        raw_value = payload[field.name]
        if isinstance(field.default, Decimal):
            values[field.name] = _decimal_from_payload(raw_value, str(field.default))
        else:
            values[field.name] = int(raw_value)
    return KanglongSymbolConfig(**values)


def _started_kanglong_execution_kwargs(
    *,
    run_id: str,
    request: KanglongActionRequest,
    stored: dict[str, Any],
) -> dict[str, Any] | None:
    report = stored.get("report") or {}
    context = report.get("execution_context") if isinstance(report, dict) else None
    if not isinstance(context, dict):
        return None
    price_snapshot = context.get("price_snapshot") or {}
    if not isinstance(price_snapshot, dict):
        return None
    stored_request = stored.get("request") or {}
    symbol = str(stored_request.get("symbol") or stored.get("symbol") or "ETHUSDC").strip().upper()
    execute_kwargs: dict[str, Any] = {
        "run_id": run_id,
        "plan_version": request.plan_version,
        "confirmed_plan_hash": request.confirmed_plan_hash,
        "idempotency_key": request.idempotency_key,
        "close_price": _decimal_from_payload(price_snapshot.get("close_price")),
        "open_price": _decimal_from_payload(price_snapshot.get("open_price")),
        "fee_rate": _decimal_from_payload(price_snapshot.get("fee_rate")),
    }
    rules = _symbol_rules_from_payload(context.get("rules"), fallback_symbol=symbol)
    config = _kanglong_config_from_payload(context.get("config"))
    if rules is not None:
        execute_kwargs["rules"] = rules
    if config is not None:
        execute_kwargs["recheck_config"] = config
    return execute_kwargs


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
    synthetic_account_ids = [
        account_id
        for account_id in [request.main_account_id, *request.subaccount_ids]
        if _is_template_runtime_account_id(account_id)
    ]
    if synthetic_account_ids:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "kanglong_test_template_account_mismatch",
                "account_ids": list(dict.fromkeys(synthetic_account_ids)),
            },
        )


def _kanglong_transfer_settings_from_request(request: KanglongPlanRequest) -> dict[str, Any] | None:
    if request.order_side is not None and request.selected_side is not None and request.order_side != request.selected_side:
        raise HTTPException(
            status_code=400,
            detail={"code": "kanglong_invalid_transfer_setting", "field": "order_side"},
        )
    transfer_field_names = {
        "transfer_mode",
        "leverage",
        "order_side",
        "transfer_percent",
        "round_count",
        "round_interval_seconds",
    }
    if not (request.model_fields_set & transfer_field_names):
        return None
    return {
        "symbol": request.symbol,
        "direction": request.selected_side.value if request.selected_side is not None else None,
        "mode": request.transfer_mode,
        "order_side": request.order_side.value if request.order_side is not None else None,
        "leverage": request.leverage,
        "transfer_percent": request.transfer_percent,
        "round_count": request.round_count,
        "round_interval_seconds": request.round_interval_seconds,
    }


async def _collect_runtime_kanglong_plan_inputs(request: KanglongPlanRequest) -> dict:
    _validate_kanglong_account_ids(request)
    _reject_runtime_template_fields(request)
    runtime_manager: AccountRuntimeManager = app.state.runtime_manager
    account_ids = [request.main_account_id, *request.subaccount_ids]
    gateways = []
    account_payloads = []
    leverages_by_account_id: dict[str, int] = {}
    operation_failed = False
    try:
        for account_id in account_ids:
            gateway = runtime_manager.build_temporary_gateway(account_id)
            gateways.append(gateway)
            account_payloads.append(await gateway.get_unified_account_snapshot())
        main_gateway = gateways[0]
        rules = await main_gateway.get_symbol_rules(request.symbol)
        quote = await main_gateway.get_quote(request.symbol)
        for account_id, gateway in zip(account_ids, gateways, strict=True):
            account_leverage = await gateway.get_symbol_leverage(request.symbol)
            leverages_by_account_id[account_id] = max(int(account_leverage or 1), 1)
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
        leverage=leverages_by_account_id,
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
        "transfer_settings": _kanglong_transfer_settings_from_request(request),
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
        "leverage_by_account_id": leverage_by_account_id,
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
        "transfer_settings": _kanglong_transfer_settings_from_request(request),
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


def _render_html(name: str, app_settings: Settings, *, local_management_token: str | None = None) -> str:
    html = STATIC_DIR.joinpath(name).read_text(encoding='utf-8')
    config_payload = json.dumps(
        {
            'frontend_execution_log_lines': app_settings.frontend_execution_log_lines,
            'sse_queue_maxsize': app_settings.sse_queue_maxsize,
            'locale': DEFAULT_LOCALE,
            'timezone': DEFAULT_TIMEZONE,
            'localManagementToken': local_management_token,
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


def _render_index_html(app_settings: Settings, *, local_management_token: str | None = None) -> str:
    return _render_html('index.html', app_settings, local_management_token=local_management_token)


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
    return HTMLResponse(
        _render_index_html(
            app.state.settings,
            local_management_token=getattr(app.state, "local_management_token", None),
        ),
        headers=HTML_CACHE_HEADERS,
    )


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
        saved = _kanglong_template_store().upsert_template(_template_with_generated_id(template))
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


def _orderbook_level_price(levels: Any) -> Decimal | None:
    if not isinstance(levels, list) or not levels:
        return None
    first = levels[0]
    if isinstance(first, dict):
        value = first.get("price")
    elif isinstance(first, (list, tuple)) and first:
        value = first[0]
    else:
        value = first
    try:
        price = Decimal(str(value))
    except Exception:
        return None
    return price if price > 0 else None


def _orderbook_best_bid_ask(orderbook: Any) -> tuple[Decimal, Decimal]:
    bids = orderbook.get("bids") if isinstance(orderbook, dict) else getattr(orderbook, "bids", None)
    asks = orderbook.get("asks") if isinstance(orderbook, dict) else getattr(orderbook, "asks", None)
    bid = _orderbook_level_price(bids)
    ask = _orderbook_level_price(asks)
    if bid is None or ask is None:
        raise HTTPException(
            status_code=400,
            detail={"code": "kanglong_test_template_market_data_account_unavailable"},
        )
    return bid, ask


def _template_execution_baseline_accounts(report: dict[str, Any]) -> list[dict[str, Any]]:
    synthetic_state = report.get("synthetic_account_state")
    if isinstance(synthetic_state, dict) and isinstance(synthetic_state.get("accounts"), list):
        return list(synthetic_state["accounts"])
    account_snapshot = report.get("account_snapshot")
    if isinstance(account_snapshot, dict) and isinstance(account_snapshot.get("accounts"), list):
        return list(account_snapshot["accounts"])
    return []


def _template_execution_snapshots(stored: dict[str, Any]) -> tuple[Any | None, list[Any] | None]:
    request_payload = stored.get("request") or {}
    accounts = _template_execution_baseline_accounts(stored.get("report") or {})
    if not accounts:
        return None, None
    accounts_by_id = {str(account.get("account_id") or ""): account for account in accounts if isinstance(account, dict)}
    leverage_by_account_id = request_payload.get("leverage_by_account_id") or {}

    def snapshot_for(account_id: str):
        account = accounts_by_id.get(account_id)
        if account is None:
            return None
        leverage = leverage_by_account_id.get(account_id)
        if leverage is None:
            leverage = next(
                (
                    position.get("leverage")
                    for position in account.get("positions") or []
                    if isinstance(position, dict) and position.get("leverage") is not None
                ),
                DEFAULT_LEVERAGE,
            )
        return preview_account_to_kanglong_snapshot(account, leverage=int(leverage))

    main_snapshot = snapshot_for(str(request_payload.get("main_account_id") or ""))
    subaccount_snapshots = [
        snapshot
        for account_id in request_payload.get("subaccount_ids") or []
        if (snapshot := snapshot_for(str(account_id))) is not None
    ]
    if main_snapshot is None or len(subaccount_snapshots) != len(request_payload.get("subaccount_ids") or []):
        return None, None
    return main_snapshot, subaccount_snapshots


async def _collect_template_execution_market_inputs(stored: dict[str, Any]) -> dict[str, Any]:
    request_payload = stored.get("request") or {}
    market_data_account_id = str(request_payload.get("market_data_account_id") or "").strip()
    if not market_data_account_id:
        raise HTTPException(
            status_code=400,
            detail={"code": "kanglong_test_template_market_data_account_required"},
        )
    if _is_template_runtime_account_id(market_data_account_id):
        raise HTTPException(
            status_code=400,
            detail={"code": "kanglong_test_template_market_data_account_unavailable"},
        )
    symbol = str(request_payload.get("symbol") or stored.get("symbol") or DEFAULT_TRADING_SYMBOL).strip().upper()
    runtime_manager: AccountRuntimeManager = app.state.runtime_manager
    gateway = None
    operation_failed = False
    try:
        gateway = runtime_manager.build_temporary_gateway(market_data_account_id)
        rules = await gateway.get_symbol_rules(symbol)
        await gateway.get_quote(symbol)
        orderbook = await gateway.get_order_book(symbol)
    except HTTPException:
        operation_failed = True
        raise
    except Exception as exc:
        operation_failed = True
        raise HTTPException(
            status_code=400,
            detail={"code": "kanglong_test_template_market_data_account_unavailable"},
        ) from exc
    finally:
        if gateway is not None:
            try:
                await gateway.close()
            except Exception as exc:
                if not operation_failed:
                    raise HTTPException(
                        status_code=400,
                        detail={"code": "kanglong_test_template_market_data_account_unavailable"},
                    ) from exc
    close_price, open_price = _orderbook_best_bid_ask(orderbook)
    config = load_kanglong_symbol_config(app.state.settings, symbol)
    main_snapshot, subaccount_snapshots = _template_execution_snapshots(stored)
    if main_snapshot is None or subaccount_snapshots is None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "blocked_plan_recheck_failed",
                "reason_code": "template_account_snapshot_missing",
                "run_id": stored.get("run_id"),
            },
        )
    selected_side_value = (stored.get("plan") or {}).get("selected_side")
    return {
        "close_price": close_price,
        "open_price": open_price,
        "fee_rate": Decimal(str(config.fee_rate)),
        "recheck_main_snapshot": main_snapshot,
        "recheck_subaccount_snapshots": subaccount_snapshots,
        "recheck_selected_side": PositionSide(selected_side_value) if selected_side_value else None,
        "recheck_config": config,
        "recheck_snapshot_bundle_id": stored.get("snapshot_bundle_id"),
        "rules": rules,
    }


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
    service = app.state.kanglong_service
    idempotency_lookup = getattr(service, "confirm_plan_idempotency_response", None)
    if callable(idempotency_lookup):
        _, idempotency_response = idempotency_lookup(
            run_id=run_id,
            plan_version=request.plan_version,
            plan_input_hash=request.plan_input_hash,
            idempotency_key=request.idempotency_key,
            operator=request.operator,
            confirmed_warning_codes=request.confirmed_warning_codes,
        )
        if idempotency_response is not None:
            return KanglongPlanResponse.model_validate(idempotency_response)
    get_run = getattr(service, "get_run", None)
    stored = get_run(run_id) if callable(get_run) else None
    if stored is not None:
        _validate_template_run_not_stale(stored)
    payload = service.confirm_plan(
        run_id=run_id,
        plan_version=request.plan_version,
        plan_input_hash=request.plan_input_hash,
        idempotency_key=request.idempotency_key,
        operator=request.operator,
        confirmed_warning_codes=request.confirmed_warning_codes,
    )
    return KanglongPlanResponse.model_validate(payload)


@app.post("/kanglong/simulation/plan/{run_id}/execute", response_model=KanglongPlanResponse)
async def execute_kanglong_simulation_plan(run_id: str, request: KanglongActionRequest) -> KanglongPlanResponse:
    service = app.state.kanglong_service
    stored = service.get_run(run_id)
    idempotency_lookup = getattr(service, "execute_plan_idempotency_response", None)
    if callable(idempotency_lookup):
        _, idempotency_response = idempotency_lookup(
            run_id=run_id,
            plan_version=request.plan_version,
            confirmed_plan_hash=request.confirmed_plan_hash,
            idempotency_key=request.idempotency_key,
        )
        if idempotency_response is not None:
            if (
                idempotency_response.get("status") == "execution_starting"
                and stored is not None
                and stored.get("status") == "execution_starting"
            ):
                execute_kwargs = _started_kanglong_execution_kwargs(
                    run_id=run_id,
                    request=request,
                    stored=stored,
                )
                if execute_kwargs is not None and callable(getattr(service, "complete_started_execution", None)):
                    _schedule_kanglong_execution(service, execute_kwargs)
            return KanglongPlanResponse.model_validate(idempotency_response)
    if stored is not None:
        _validate_template_run_not_stale(stored)
    execute_kwargs = {
        "run_id": run_id,
        "plan_version": request.plan_version,
        "confirmed_plan_hash": request.confirmed_plan_hash,
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
            if stored_request.get("account_source") == "test_template":
                try:
                    inputs = await _collect_template_execution_market_inputs(stored)
                except HTTPException:
                    raise
                except Exception as exc:
                    _raise_api_error(exc, code="kanglong_plan_failed", source="service")
                execute_kwargs.update(
                    {
                        "close_price": inputs["close_price"],
                        "open_price": inputs["open_price"],
                        "fee_rate": inputs["fee_rate"],
                        "rules": inputs.get("rules"),
                        "recheck_main_snapshot": inputs["recheck_main_snapshot"],
                        "recheck_subaccount_snapshots": inputs["recheck_subaccount_snapshots"],
                        "recheck_selected_side": inputs["recheck_selected_side"],
                        "recheck_config": inputs["recheck_config"],
                        "recheck_snapshot_bundle_id": inputs["recheck_snapshot_bundle_id"],
                    }
                )
            else:
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
                    transfer_percent=Decimal(str((stored_request.get("transfer_settings") or {}).get("transfer_percent", "100"))),
                    round_count=int((stored_request.get("transfer_settings") or {}).get("round_count", 30)),
                    round_interval_seconds=int((stored_request.get("transfer_settings") or {}).get("round_interval_seconds", 3)),
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
                        "rules": inputs.get("rules"),
                        "recheck_main_snapshot": inputs["main_snapshot"],
                        "recheck_subaccount_snapshots": inputs["subaccount_snapshots"],
                        "recheck_selected_side": inputs["selected_side"],
                        "recheck_config": inputs["config"],
                        "recheck_snapshot_bundle_id": inputs["snapshot_bundle_id"],
                    }
                )
    start_execute = getattr(service, "start_execute_plan", None)
    complete_execute = getattr(service, "complete_started_execution", None)
    if callable(start_execute) and callable(complete_execute):
        payload = start_execute(**execute_kwargs)
        if payload.get("status") == "execution_starting":
            _schedule_kanglong_execution(service, execute_kwargs)
    else:
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


def _schedule_kanglong_execution_from_stored(
    service: Any,
    *,
    run_id: str,
    plan_version: str,
    idempotency_key: str,
) -> None:
    get_run = getattr(service, "get_run", None)
    if not callable(get_run) or not callable(getattr(service, "run_market_execution", None)):
        return
    stored = get_run(run_id)
    if stored is None:
        return
    execute_kwargs = _started_kanglong_execution_kwargs(
        run_id=run_id,
        request=KanglongActionRequest(plan_version=plan_version, idempotency_key=idempotency_key),
        stored=stored,
    )
    if execute_kwargs is not None:
        _schedule_kanglong_execution(service, execute_kwargs)


async def _control_kanglong_simulation_run(
    run_id: str,
    request: KanglongControlRequest,
    *,
    action: str,
) -> KanglongPlanResponse:
    service = app.state.kanglong_service
    payload = service.control_run(
        run_id=run_id,
        plan_version=request.plan_version,
        action=action,
        expected_action_version=request.expected_action_version,
        idempotency_key=request.idempotency_key,
        operator=request.operator,
    )
    if action in {"resume", "stop"} and payload.get("status") in {"running", "stop_pending"}:
        _schedule_kanglong_execution_from_stored(
            service,
            run_id=run_id,
            plan_version=request.plan_version,
            idempotency_key=request.idempotency_key,
        )
    return KanglongPlanResponse.model_validate(payload)


@app.post("/kanglong/simulation/run/{run_id}/pause", response_model=KanglongPlanResponse)
async def pause_kanglong_simulation_run(run_id: str, request: KanglongControlRequest) -> KanglongPlanResponse:
    return await _control_kanglong_simulation_run(run_id, request, action="pause")


@app.post("/kanglong/simulation/run/{run_id}/resume", response_model=KanglongPlanResponse)
async def resume_kanglong_simulation_run(run_id: str, request: KanglongControlRequest) -> KanglongPlanResponse:
    return await _control_kanglong_simulation_run(run_id, request, action="resume")


@app.post("/kanglong/simulation/run/{run_id}/stop", response_model=KanglongPlanResponse)
async def stop_kanglong_simulation_run(run_id: str, request: KanglongControlRequest) -> KanglongPlanResponse:
    return await _control_kanglong_simulation_run(run_id, request, action="stop")


@app.post("/kanglong/simulation/run/{run_id}/recover", response_model=KanglongPlanResponse)
async def recover_kanglong_simulation_run(run_id: str, request: KanglongRecoverRequest) -> KanglongPlanResponse:
    service = app.state.kanglong_service
    idempotency_lookup = getattr(service, "recover_run_idempotency_response", None)
    if callable(idempotency_lookup):
        _, idempotency_response = idempotency_lookup(
            run_id=run_id,
            idempotency_key=request.idempotency_key,
            operator=request.operator,
            release_reason=request.release_reason,
        )
        if idempotency_response is not None:
            return KanglongPlanResponse.model_validate(idempotency_response)
    stored = service.get_run(run_id)
    if stored is not None:
        _validate_template_run_not_stale(stored)
    payload = service.recover_run(
        run_id=run_id,
        idempotency_key=request.idempotency_key,
        operator=request.operator,
        release_reason=request.release_reason,
    )
    return KanglongPlanResponse.model_validate(payload)


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
async def create_session(request: OpenSessionRequest, raw: Request) -> SessionSummary:
    require_local_management_request(raw)
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
async def create_close_session(request: CloseSessionRequest, raw: Request) -> SessionSummary:
    require_local_management_request(raw)
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
async def create_single_open_session(request: SingleOpenSessionRequest, raw: Request) -> SessionSummary:
    require_local_management_request(raw)
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
async def create_single_close_session(request: SingleCloseSessionRequest, raw: Request) -> SessionSummary:
    require_local_management_request(raw)
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
async def pause_session(session_id: str, raw: Request) -> SessionActionResponse:
    require_local_management_request(raw)
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
async def resume_session(session_id: str, raw: Request) -> SessionActionResponse:
    require_local_management_request(raw)
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
async def abort_session(session_id: str, raw: Request) -> SessionActionResponse:
    require_local_management_request(raw)
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


def _loopback_host(value: str | None) -> bool:
    if not value:
        return False
    normalized = value.strip().lower()
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def require_local_management_request(request: Request) -> None:
    client_host = request.client.host if request.client is not None else None
    host_header = request.headers.get("host", "")
    host_name = urlsplit(f"//{host_header}").hostname
    origin_header = request.headers.get("origin", "")
    origin = urlsplit(origin_header)
    token = request.headers.get("x-local-management-token", "")
    expected_token = str(getattr(app.state, "local_management_token", ""))
    browser_safe_read_without_origin = not origin_header and request.method.upper() in {"GET", "HEAD"}
    same_origin = browser_safe_read_without_origin or (
        origin.scheme in {"http", "https"}
        and origin.hostname is not None
        and _loopback_host(origin.hostname)
        and origin.netloc.lower() == host_header.lower()
    )
    if not (
        _loopback_host(client_host)
        and _loopback_host(host_name)
        and same_origin
        and expected_token
        and secrets.compare_digest(token, expected_token)
    ):
        raise HTTPException(status_code=403, detail={"code": "local_management_forbidden"})


def _credential_accounts() -> list[AccountConfig]:
    app_settings: Settings = app.state.settings
    return list(app_settings.accounts.values())


def _credential_summary(account: AccountConfig, order: int) -> dict[str, object]:
    return {
        "account_id": account.account_id,
        "name": account.name,
        "api_key_masked": mask_api_key(account.api_key),
        "has_api_secret": bool(account.api_secret),
        "account_mode": account.account_mode,
        "enabled": account.enabled,
        "order": order,
    }


def _credential_list_payload(accounts: list[AccountConfig] | None = None) -> dict[str, object]:
    records = accounts if accounts is not None else _credential_accounts()
    store: AccountCredentialStore = app.state.account_credential_store
    return {
        "accounts": [_credential_summary(account, order) for order, account in enumerate(records)],
        "credential_revision": store.current_revision(),
        "migration_required": bool(getattr(app.state, "account_credentials_migration_required", False)),
    }


def _canonical_batch_accounts(account_ids: list[str]) -> list[AccountConfig]:
    requested = {account_id.strip().lower() for account_id in account_ids}
    accounts = _credential_accounts()
    known_ids = {account.account_id for account in accounts}
    unknown_ids = sorted(requested - known_ids)
    if unknown_ids:
        raise HTTPException(
            status_code=422,
            detail={"code": "kanglong_batch_account_not_found", "account_ids": unknown_ids},
        )
    disabled_ids = [
        account.account_id
        for account in accounts
        if account.account_id in requested and not account.enabled
    ]
    if disabled_ids:
        raise HTTPException(
            status_code=422,
            detail={"code": "kanglong_batch_account_disabled", "account_ids": disabled_ids},
        )
    return [account for account in accounts if account.account_id in requested]


def _unknown_capacity_payload(snapshot: CapacitySnapshot, reasons: list[str]) -> dict[str, Any]:
    return {
        "account_id": snapshot.account_id,
        "capacity_known": False,
        "blocked": True,
        "blocked_reason": reasons[0],
        "blocked_reasons": reasons,
        "warnings": [],
        "assembled_at": snapshot.assembled_at,
        "oldest_component_at": snapshot.oldest_component_at,
        "snapshot_components": snapshot.snapshot_components,
        "calculation_version": CALCULATION_VERSION,
    }


def _capacity_account_payload(
    snapshot: CapacitySnapshot,
    request: KanglongBatchCapacityPreviewRequest,
) -> tuple[dict[str, Any], Any | None]:
    invalid_components = [
        name
        for name, component in snapshot.snapshot_components.items()
        if not component.get("valid", False)
    ]
    reasons = list(snapshot.blocked_reasons)
    reasons.extend(f"capacity_component_stale:{name}" for name in invalid_components)
    if not snapshot.brackets:
        reasons.append("capacity_leverage_bracket_missing")
    if reasons:
        unique_reasons = list(dict.fromkeys(reasons))
        return _unknown_capacity_payload(snapshot, unique_reasons), None

    estimate = estimate_account_capacity(
        per_leg_notional=request.per_leg_notional,
        requested_leverage=request.leverage,
        current_symbol_leverage=snapshot.current_symbol_leverage,
        current_symbol_max_notional_value=snapshot.current_symbol_max_notional_value,
        brackets=list(snapshot.brackets),
        available_balance=snapshot.available_balance,
        equity=snapshot.account_equity,
        maker_fee_rate=snapshot.maker_fee_rate,
        taker_fee_rate=snapshot.taker_fee_rate,
        existing_symbol_exposure=snapshot.existing_symbol_exposure,
        policy=CapacityPolicy(),
    )
    payload = estimate.to_payload()
    payload.update(
        {
            "account_id": snapshot.account_id,
            "capacity_known": True,
            "blocked_reasons": [estimate.blocked_reason] if estimate.blocked_reason else [],
            "assembled_at": snapshot.assembled_at,
            "oldest_component_at": snapshot.oldest_component_at,
            "snapshot_components": snapshot.snapshot_components,
            "calculation_version": CALCULATION_VERSION,
        }
    )
    return payload, estimate


def _decimal_response_strings(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {key: _decimal_response_strings(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decimal_response_strings(item) for item in value]
    if isinstance(value, tuple):
        return [_decimal_response_strings(item) for item in value]
    return value


def _account_from_create(request: AccountCredentialCreateRequest) -> AccountConfig:
    return AccountConfig(
        account_id=request.account_id.strip().lower(),
        name=request.name.strip(),
        api_key=request.api_key.strip(),
        api_secret=request.api_secret.strip(),
        credential_type=request.credential_type,
        account_mode=request.account_mode,
        enabled=request.enabled,
    )


def _raise_credential_api_error(exc: BaseException) -> None:
    if isinstance(exc, AccountCredentialsLockedByActiveBatch):
        raise HTTPException(
            status_code=409,
            detail={"code": "account_credentials_locked_by_active_batch"},
        ) from exc
    if isinstance(exc, CredentialRevisionConflict):
        raise HTTPException(status_code=409, detail={"code": "credential_revision_conflict"}) from exc
    if isinstance(exc, CredentialPreviewInvalid):
        raise HTTPException(status_code=409, detail={"code": "credential_preview_invalid"}) from exc
    if isinstance(exc, CredentialStoreUnavailable):
        raise HTTPException(status_code=503, detail={"code": "account_credentials_unavailable"}) from exc
    raise HTTPException(status_code=400, detail={"code": "account_credential_operation_failed"}) from exc


def _manual_credential_candidate(
    accounts: list[AccountConfig],
    *,
    credential_revision: str,
) -> VerifiedCredentialCandidate:
    content_hash = hashlib.sha256(
        json.dumps(
            [(account.account_id, account.api_key, account.api_secret, account.enabled) for account in accounts],
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return VerifiedCredentialCandidate(
        accounts=tuple(accounts),
        credential_revision=credential_revision,
        content_hash=content_hash,
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
        preview_token="",
        changes={
            "added_account_ids": [],
            "updated_account_ids": [],
            "unchanged_account_ids": [],
            "removed_account_ids": [],
        },
    )


async def _commit_credential_candidate(candidate: VerifiedCredentialCandidate) -> dict[str, object]:
    runtime_manager: AccountRuntimeManager = app.state.runtime_manager
    store: AccountCredentialStore = app.state.account_credential_store
    try:
        prepared_runtime = await runtime_manager.prepare_accounts(list(candidate.accounts))
        try:
            prepared_write = store.prepare(list(candidate.accounts))
        except BaseException:
            await runtime_manager.close_replaced(tuple(prepared_runtime.runtimes.values()))
            raise
        coordinator: AccountCredentialCommitCoordinator = app.state.account_credential_commit_coordinator
        await coordinator.commit(
            candidate=candidate,
            prepared_write=prepared_write,
            prepared_runtime=prepared_runtime,
        )
    except BaseException as exc:
        if isinstance(exc, asyncio.CancelledError):
            raise
        _raise_credential_api_error(exc)
    app.state.account_credentials_migration_required = False
    return _credential_list_payload()


@app.get('/config/kanglong-batch-defaults')
async def get_kanglong_batch_defaults(raw: Request) -> dict[str, Any]:
    require_local_management_request(raw)
    store: KanglongBatchDefaultsStore = app.state.kanglong_batch_defaults_store
    return store.load().model_dump(mode="json")


@app.put('/config/kanglong-batch-defaults')
async def update_kanglong_batch_defaults(
    request: KanglongBatchDefaults,
    raw: Request,
) -> dict[str, Any]:
    require_local_management_request(raw)
    store: KanglongBatchDefaultsStore = app.state.kanglong_batch_defaults_store
    return store.save(request).model_dump(mode="json")


@app.post('/kanglong/batch-simulation/capacity-preview')
async def preview_kanglong_batch_capacity(
    request: KanglongBatchCapacityPreviewRequest,
    raw: Request,
) -> dict[str, Any]:
    require_local_management_request(raw)
    accounts = _canonical_batch_accounts(request.account_ids)
    store: AccountCredentialStore = app.state.account_credential_store
    coordinator: CapacitySnapshotCoordinator = app.state.capacity_snapshot_coordinator
    credential_revision = store.current_revision()
    try:
        snapshots = await coordinator.refresh_capacity(
            credential_revision,
            [account.account_id for account in accounts],
            request.symbol,
        )
    except BaseException as exc:
        if isinstance(exc, asyncio.CancelledError):
            raise
        raise HTTPException(
            status_code=503,
            detail={"code": "kanglong_capacity_snapshot_unavailable"},
        ) from exc

    account_payloads: list[dict[str, Any]] = []
    estimates: list[tuple[str, Any]] = []
    for account in accounts:
        payload, estimate = _capacity_account_payload(snapshots[account.account_id], request)
        payload["account_name"] = account.name
        account_payloads.append(payload)
        if estimate is not None:
            estimates.append((account.account_id, estimate))

    batch = estimate_batch_capacity(estimates)
    if len(estimates) != len(account_payloads):
        batch["batch_blocked"] = True
        batch["batch_capacity_known"] = False
    else:
        batch["batch_capacity_known"] = True
    payload = {
        "operation": request.operation,
        "symbol": request.symbol.strip().upper(),
        "preferred_side": request.preferred_side,
        "leverage": request.leverage,
        "per_leg_notional": request.per_leg_notional,
        "round_count": request.round_count,
        "round_interval_seconds": request.round_interval_seconds,
        "credential_revision": credential_revision,
        "request_seq": request.request_seq,
        "input_hash": request.input_hash,
        "calculation_version": CALCULATION_VERSION,
        "accounts": account_payloads,
        **batch,
    }
    return _decimal_response_strings(payload)


def _kanglong_batch_run_payload(stored: dict[str, Any]) -> dict[str, Any]:
    status = str(stored["status"])
    actions = list(stored.get("available_actions") or available_actions_for_status(status))
    if status == KanglongRunStatus.ABORTED_RECOVERED.value:
        actions = ["view_report"]
    if status != KanglongRunStatus.NEEDS_ABORT_RECOVER.value:
        actions = [action for action in actions if action != "recover"]
    return _decimal_response_strings(
        {
            "contract_version": CONTRACT_VERSION,
            "run_id": stored["run_id"],
            "status": status,
            "plan_version": stored.get("plan_version") or "",
            "action_version": int((stored.get("progress") or {}).get("action_version", 0)),
            "available_actions": actions,
            "report": stored.get("report") or {},
            "plan": stored.get("plan") or {},
            "accounts": app.state.repository.list_kanglong_batch_accounts(stored["run_id"]),
            "latest_event_id": app.state.repository.latest_kanglong_event_id(stored["run_id"]),
        }
    )


def _batch_quote_mid(snapshot: CapacitySnapshot) -> Decimal:
    details = (snapshot.snapshot_components.get("quote") or {}).get("details") or {}
    bid = Decimal(str(details.get("bid_price") or "0"))
    ask = Decimal(str(details.get("ask_price") or "0"))
    if bid <= 0 or ask <= 0:
        raise HTTPException(status_code=503, detail={"code": "kanglong_capacity_quote_unavailable"})
    return (bid + ask) / Decimal("2")


async def _build_kanglong_batch_plan(
    request: KanglongBatchPlanRequest,
    *,
    run_id: str | None = None,
    force_refresh: bool,
) -> KanglongBatchPlan:
    accounts = _canonical_batch_accounts(request.account_ids)
    account_ids = [account.account_id for account in accounts]
    credential_store: AccountCredentialStore = app.state.account_credential_store
    revision = credential_store.current_revision()
    planner: KanglongBatchPlanner = app.state.kanglong_batch_planner
    if request.operation == "close":
        availability = await planner.refresh_close_availability(
            request.source_open_run_id or "",
            account_ids,
            force_refresh=force_refresh,
        )
        source = app.state.repository.get_kanglong_run(request.source_open_run_id or "")
        if source is None:
            raise HTTPException(status_code=404, detail={"code": "kanglong_source_open_run_not_found"})
        source_accounts = {
            str(item.get("account_id") or ""): item
            for item in (source.get("plan") or {}).get("accounts") or []
        }
        snapshots = availability.get("account_snapshots") or {}
        merged_accounts: list[dict[str, Any]] = []
        for item in availability["accounts"]:
            account_id = item["account_id"]
            frozen = source_accounts.get(account_id, {})
            snapshot = snapshots.get(account_id)
            merged_accounts.append(
                {
                    **frozen,
                    **item,
                    "maker_fee_rate": getattr(snapshot, "maker_fee_rate", frozen.get("maker_fee_rate", "0")),
                    "taker_fee_rate": getattr(snapshot, "taker_fee_rate", frozen.get("taker_fee_rate", "0")),
                    "market_snapshot_id": (
                        f"market-{stable_payload_hash(getattr(snapshot, 'snapshot_components', {}))[:24]}"
                        if snapshot is not None
                        else frozen.get("market_snapshot_id", "close-market-unavailable")
                    ),
                }
            )
        source_payload = {
            "run_id": source["run_id"],
            "symbol": source["symbol"],
            "preferred_side": request.preferred_side.value,
            "requested_leverage": (source.get("plan") or {}).get("requested_leverage", 1),
            "credential_revision": revision,
            "accounts": merged_accounts,
            "ledger_hash": availability.get("source_ledger_hash"),
            "checkpoint_id": availability.get("source_checkpoint_id"),
        }
        return planner.plan_close(
            source_open_run=source_payload,
            credential_revision=revision,
            preferred_side=request.preferred_side,
            run_id=run_id,
            account_ids=account_ids,
            round_count=request.round_count,
            round_interval_seconds=request.round_interval_seconds,
        )

    coordinator: CapacitySnapshotCoordinator = app.state.capacity_snapshot_coordinator
    snapshots = await coordinator.refresh_capacity(
        revision,
        account_ids,
        request.symbol,
        force_refresh=force_refresh,
    )
    reference_price = _batch_quote_mid(snapshots[account_ids[0]])
    planner_snapshots: dict[str, dict[str, Any]] = {}
    blocked: list[dict[str, Any]] = []
    warning_codes: list[str] = []
    for account_id in account_ids:
        snapshot = snapshots[account_id]
        if snapshot.blocked_reasons or not snapshot.all_components_fresh or not snapshot.brackets:
            blocked.append(
                {"account_id": account_id, "reason": (snapshot.blocked_reasons or ("capacity_unknown",))[0]}
            )
            continue
        estimate = estimate_account_capacity(
            per_leg_notional=request.per_leg_notional,
            requested_leverage=request.leverage,
            current_symbol_leverage=snapshot.current_symbol_leverage,
            current_symbol_max_notional_value=snapshot.current_symbol_max_notional_value,
            brackets=list(snapshot.brackets),
            available_balance=snapshot.available_balance,
            equity=snapshot.account_equity,
            maker_fee_rate=snapshot.maker_fee_rate,
            taker_fee_rate=snapshot.taker_fee_rate,
            existing_symbol_exposure=snapshot.existing_symbol_exposure,
            policy=CapacityPolicy(),
        )
        if estimate.blocked:
            blocked.append({"account_id": account_id, "reason": estimate.blocked_reason})
        warning_codes.extend(f"{account_id}:{warning}" for warning in estimate.warnings)
        planner_snapshots[account_id] = {
            **estimate.to_payload(),
            "maker_fee_rate": snapshot.maker_fee_rate,
            "taker_fee_rate": snapshot.taker_fee_rate,
            "assembled_at": snapshot.assembled_at,
            "oldest_component_at": snapshot.oldest_component_at,
            "snapshot_components": snapshot.snapshot_components,
        }
    if blocked:
        raise HTTPException(
            status_code=409,
            detail={"code": "kanglong_batch_capacity_blocked", "accounts": blocked},
        )
    batch_runtime_manager = getattr(
        app.state,
        "kanglong_readonly_runtime_manager",
        app.state.runtime_manager,
    )
    rules = await batch_runtime_manager.current(account_ids[0]).gateway.get_symbol_rules(
        request.symbol.strip().upper()
    )
    return planner.plan_open(
        account_ids=account_ids,
        credential_revision=revision,
        symbol=request.symbol,
        preferred_side=request.preferred_side,
        leverage=request.leverage,
        per_leg_notional=request.per_leg_notional,
        reference_price=reference_price,
        rules=rules,
        account_snapshots=planner_snapshots,
        run_id=run_id,
        round_count=request.round_count,
        round_interval_seconds=request.round_interval_seconds,
        warning_codes=warning_codes,
    )


def _mark_kanglong_batch_plan_stale(stored: dict[str, Any], reasons: list[dict[str, Any]]) -> None:
    app.state.repository.update_kanglong_run(
        stored["run_id"],
        status=KanglongRunStatus.BLOCKED_PLAN_STALE.value,
        available_actions=available_actions_for_status(KanglongRunStatus.BLOCKED_PLAN_STALE),
    )
    raise HTTPException(
        status_code=409,
        detail={"code": "blocked_plan_stale", "reasons": reasons},
    )


async def _recheck_kanglong_batch_plan_for_confirmation(stored: dict[str, Any]) -> list[str]:
    plan = KanglongBatchPlan.from_payload(stored["plan"])
    reasons: list[dict[str, Any]] = []
    warning_codes: list[str] = []
    if plan.credential_revision != app.state.account_credential_store.current_revision():
        _mark_kanglong_batch_plan_stale(stored, [{"reason": "credential_revision_conflict"}])
    if plan.operation == "close":
        availability = await app.state.kanglong_batch_planner.refresh_close_availability(
            plan.source_open_run_id or "",
            [account.account_id for account in plan.accounts],
            force_refresh=True,
        )
        current_by_id = {item["account_id"]: item for item in availability["accounts"]}
        snapshots = availability.get("account_snapshots") or {}
        for account in plan.accounts:
            current = current_by_id.get(account.account_id)
            if current is None or any(
                Decimal(str(current[field])) != frozen
                for field, frozen in (
                    ("source_long_remaining_qty", account.source_long_remaining_qty),
                    ("source_short_remaining_qty", account.source_short_remaining_qty),
                )
            ) or (
                str((current or {}).get("source_ledger_hash")) != str(account.source_ledger_hash)
                or int((current or {}).get("source_checkpoint_id") or 0)
                != int(account.source_checkpoint_id or 0)
            ):
                reasons.append({"account_id": account.account_id, "reason": "kanglong_close_source_changed"})
                continue
            snapshot = snapshots.get(account.account_id)
            if snapshot is not None and (
                Decimal(str(snapshot.maker_fee_rate)) != account.maker_fee_rate
                or Decimal(str(snapshot.taker_fee_rate)) != account.taker_fee_rate
            ):
                reasons.append({"account_id": account.account_id, "reason": "commission_rate_changed"})
    else:
        coordinator: CapacitySnapshotCoordinator = app.state.capacity_snapshot_coordinator
        snapshots = await coordinator.refresh_capacity(
            plan.credential_revision,
            [account.account_id for account in plan.accounts],
            plan.symbol,
            force_refresh=True,
        )
        config = load_kanglong_symbol_config(app.state.settings, plan.symbol)
        for account in plan.accounts:
            snapshot = snapshots[account.account_id]
            mid = _batch_quote_mid(snapshot)
            estimate = estimate_account_capacity(
                per_leg_notional=plan.per_leg_notional,
                requested_leverage=plan.requested_leverage,
                current_symbol_leverage=snapshot.current_symbol_leverage,
                current_symbol_max_notional_value=snapshot.current_symbol_max_notional_value,
                brackets=list(snapshot.brackets),
                available_balance=snapshot.available_balance,
                equity=snapshot.account_equity,
                maker_fee_rate=snapshot.maker_fee_rate,
                taker_fee_rate=snapshot.taker_fee_rate,
                existing_symbol_exposure=snapshot.existing_symbol_exposure,
                policy=CapacityPolicy(),
                capacity_requested_gross_notional=(account.target_long_qty + account.target_short_qty) * mid,
            )
            drift_bps = abs(mid - account.reference_mid_price) / account.reference_mid_price * Decimal("10000")
            frozen_changed = any(
                (
                    Decimal(str(current)) != Decimal(str(frozen))
                    if not isinstance(frozen, int)
                    else int(current) != frozen
                )
                for current, frozen in (
                    (snapshot.maker_fee_rate, account.maker_fee_rate),
                    (snapshot.taker_fee_rate, account.taker_fee_rate),
                    (snapshot.current_symbol_leverage, account.current_symbol_leverage),
                    (snapshot.current_symbol_max_notional_value, account.current_symbol_max_notional_value),
                    (estimate.bracket_max_allowed_leverage, account.bracket_max_allowed_leverage),
                    (estimate.bracket_notional_coef, account.bracket_notional_coef),
                    (estimate.selected_bracket_effective_cap, account.selected_bracket_effective_cap),
                    (estimate.effective_capacity_leverage, account.effective_capacity_leverage),
                )
            )
            if (
                snapshot.blocked_reasons
                or not snapshot.all_components_fresh
                or estimate.blocked
                or drift_bps > Decimal(config.plan_recheck_price_drift_bps)
                or frozen_changed
            ):
                reasons.append({"account_id": account.account_id, "reason": "kanglong_plan_recheck_changed"})
            warning_codes.extend(f"{account.account_id}:{warning}" for warning in estimate.warnings)
    if reasons:
        _mark_kanglong_batch_plan_stale(stored, reasons)
    return list(dict.fromkeys(warning_codes))


def _batch_plan_request_from_stored(stored: dict[str, Any]) -> KanglongBatchPlanRequest:
    request = stored.get("request") or {}
    return KanglongBatchPlanRequest.model_validate(
        {
            "operation": request.get("operation"),
            "symbol": request.get("symbol") or stored.get("symbol"),
            "preferred_side": request.get("preferred_side"),
            "leverage": request.get("leverage", 100),
            "per_leg_notional": request.get("per_leg_notional", "250000") or "250000",
            "account_ids": request.get("account_ids") or [],
            "source_open_run_id": request.get("source_open_run_id"),
            "round_count": request.get("round_count", 30),
            "round_interval_seconds": request.get("round_interval_seconds", 3),
        }
    )


def _raise_batch_action_error(exc: BaseException) -> None:
    code = str(exc)
    if code in {
        "idempotency_key_conflict",
        "plan_version_conflict",
        "action_version_conflict",
        "credential_revision_conflict",
        "kanglong_action_status_conflict",
        "kanglong_batch_lock_conflict",
        "kanglong_close_source_changed",
        "kanglong_stale_fencing_token",
    }:
        raise HTTPException(status_code=409, detail={"code": code}) from exc
    if code in {"kanglong_run_not_found", "kanglong_batch_run_not_found"}:
        raise HTTPException(status_code=404, detail={"code": code}) from exc
    raise HTTPException(status_code=400, detail={"code": "kanglong_batch_action_failed"}) from exc


@app.post('/kanglong/batch-simulation/plan', response_model=KanglongBatchRunResponse)
async def create_kanglong_batch_plan(
    request: KanglongBatchPlanRequest,
    raw: Request,
) -> KanglongBatchRunResponse:
    require_local_management_request(raw)
    plan = await _build_kanglong_batch_plan(request, force_refresh=False)
    app.state.repository.save_kanglong_batch_plan(plan, status=KanglongRunStatus.CHAIN_READY.value)
    stored = app.state.repository.get_kanglong_run(plan.run_id)
    return KanglongBatchRunResponse.model_validate(_kanglong_batch_run_payload(stored))


@app.post('/kanglong/batch-simulation/plan/{run_id}/confirm', response_model=KanglongBatchRunResponse)
async def confirm_kanglong_batch_plan(
    run_id: str,
    request: KanglongActionRequest,
    raw: Request,
) -> KanglongBatchRunResponse:
    require_local_management_request(raw)
    stored = app.state.repository.get_kanglong_run(run_id)
    if stored is None or stored.get("run_kind") != "kanglong_batch":
        raise HTTPException(status_code=404, detail={"code": "kanglong_batch_run_not_found"})
    if stored["plan_version"] != request.plan_version:
        raise HTTPException(status_code=409, detail={"code": "plan_version_conflict"})
    key_hash = stable_payload_hash({"action": "confirm", **request.model_dump(mode="json")})
    remembered = app.state.repository.get_live_kanglong_idempotency(
        request.idempotency_key,
        key_hash,
    )
    if remembered is not None:
        if remembered["conflict"]:
            raise HTTPException(status_code=409, detail={"code": "idempotency_key_conflict"})
        return KanglongBatchRunResponse.model_validate(
            {
                **_kanglong_batch_run_payload(stored),
                **remembered["response"],
            }
        )
    current_warning_codes = await _recheck_kanglong_batch_plan_for_confirmation(stored)
    missing_warnings = sorted(set(current_warning_codes) - set(request.confirmed_warning_codes))
    if missing_warnings:
        raise HTTPException(
            status_code=409,
            detail={"code": "kanglong_batch_warning_confirmation_required", "warning_codes": missing_warnings},
        )
    try:
        response = app.state.repository.commit_kanglong_action(
            run_id=run_id,
            mutation=KanglongActionMutation(
                expected_statuses=(KanglongRunStatus.CHAIN_READY.value,),
                expected_plan_version=request.plan_version,
                expected_action_version=None,
                next_status=KanglongRunStatus.PLAN_CONFIRMED.value,
                available_actions=tuple(available_actions_for_status(KanglongRunStatus.PLAN_CONFIRMED)),
                confirmed_at=datetime.now(UTC).isoformat(),
                events=({"event_type": "kanglong_batch_plan_confirmed", "payload": {}},),
                increment_action_version=True,
            ),
            idempotency_key=request.idempotency_key,
            request_hash=key_hash,
            response={"run_id": run_id, "plan_version": stored["plan_version"]},
        )
    except BaseException as exc:
        _raise_batch_action_error(exc)
    latest = app.state.repository.get_kanglong_run(run_id)
    return KanglongBatchRunResponse.model_validate({**_kanglong_batch_run_payload(latest), **response})


@app.post('/kanglong/batch-simulation/plan/{run_id}/execute', response_model=KanglongBatchRunResponse)
async def execute_kanglong_batch_plan(
    run_id: str,
    request: KanglongActionRequest,
    raw: Request,
) -> KanglongBatchRunResponse:
    require_local_management_request(raw)
    key_hash = stable_payload_hash({"action": "execute", **request.model_dump(mode="json")})
    try:
        response = app.state.repository.commit_kanglong_action(
            run_id=run_id,
            mutation=KanglongActionMutation(
                expected_statuses=(KanglongRunStatus.PLAN_CONFIRMED.value,),
                expected_plan_version=request.plan_version,
                expected_action_version=None,
                next_status=KanglongRunStatus.EXECUTION_STARTING.value,
                available_actions=tuple(available_actions_for_status(KanglongRunStatus.EXECUTION_STARTING)),
                events=({"event_type": "kanglong_batch_execution_starting", "payload": {}},),
                increment_action_version=True,
                acquire_frozen_locks=True,
                current_credential_revision=app.state.account_credential_store.current_revision(),
                lock_ttl_ms=600_000,
            ),
            idempotency_key=request.idempotency_key,
            request_hash=key_hash,
            response={"run_id": run_id, "plan_version": request.plan_version},
        )
    except BaseException as exc:
        _raise_batch_action_error(exc)
    app.state.kanglong_execution_task_registry.start(run_id)
    latest = app.state.repository.get_kanglong_run(run_id)
    return KanglongBatchRunResponse.model_validate({**_kanglong_batch_run_payload(latest), **response})


async def _control_kanglong_batch_run(
    run_id: str,
    request: KanglongControlRequest | KanglongBatchRecoverRequest,
    *,
    action: str,
) -> KanglongBatchRunResponse:
    stored = app.state.repository.get_kanglong_run(run_id)
    if stored is None or stored.get("run_kind") != "kanglong_batch":
        raise HTTPException(status_code=404, detail={"code": "kanglong_batch_run_not_found"})
    transitions = {
        "pause": ((KanglongRunStatus.RUNNING.value,), KanglongRunStatus.PAUSE_PENDING.value),
        "resume": (
            (KanglongRunStatus.PAUSED_BY_USER.value, KanglongRunStatus.PAUSED_MARKET_UNSTABLE.value),
            KanglongRunStatus.RUNNING.value,
        ),
        "stop": (
            (
                KanglongRunStatus.RUNNING.value,
                KanglongRunStatus.PAUSE_PENDING.value,
                KanglongRunStatus.PAUSED_BY_USER.value,
                KanglongRunStatus.PAUSED_MARKET_UNSTABLE.value,
                KanglongRunStatus.PAUSED_PLAN_RECHECK_CHANGED.value,
            ),
            KanglongRunStatus.STOP_PENDING.value,
        ),
        "recover": ((KanglongRunStatus.NEEDS_ABORT_RECOVER.value,), KanglongRunStatus.EXECUTION_STARTING.value),
    }
    expected, next_status = transitions[action]
    key_hash = stable_payload_hash({"action": action, **request.model_dump(mode="json")})
    progress = None
    report = None
    events = (
        {
            "event_type": f"kanglong_batch_{action}_requested",
            "payload": {"operator": request.operator},
        },
    )
    release_frozen_locks = False
    mark_accounts_needs_recovery = False
    if action == "recover":
        recovered_at = datetime.now(UTC).isoformat()
        recovery_record = {
            "operator": request.operator,
            "release_reason": request.release_reason,
            "previous_status": stored["status"],
            "recovered_at": recovered_at,
        }
        progress = dict(stored.get("progress") or {})
        progress["abort_recover"] = recovery_record
        report = dict(stored.get("report") or {})
        history = list(report.get("abort_recover_history") or [])
        history.append(recovery_record)
        report["abort_recover_history"] = history
        next_status = KanglongRunStatus.ABORTED_RECOVERED.value
        events = (
            {
                "event_type": "kanglong_batch_abort_recovering",
                "payload": {
                    **recovery_record,
                    "message_key": "events.kanglong.batch_abort_recovering",
                },
            },
            {
                "event_type": "kanglong_batch_aborted_recovered",
                "payload": {
                    **recovery_record,
                    "message_key": "events.kanglong.batch_aborted_recovered",
                },
            },
        )
        release_frozen_locks = True
        mark_accounts_needs_recovery = True
    try:
        response = app.state.repository.commit_kanglong_action(
            run_id=run_id,
            mutation=KanglongActionMutation(
                expected_statuses=expected,
                expected_plan_version=request.plan_version,
                expected_action_version=request.expected_action_version,
                next_status=next_status,
                available_actions=(
                    ("view_report",)
                    if action == "recover"
                    else tuple(available_actions_for_status(next_status))
                ),
                progress=progress,
                report=report,
                result_grade="unsafe_unclosed" if action == "recover" else None,
                events=events,
                increment_action_version=True,
                acquire_frozen_locks=action in {"resume", "stop"},
                current_credential_revision=(
                    app.state.account_credential_store.current_revision()
                    if action in {"resume", "stop"}
                    else None
                ),
                lock_ttl_ms=600_000,
                release_frozen_locks=release_frozen_locks,
                mark_active_batch_accounts_needs_recovery=mark_accounts_needs_recovery,
            ),
            idempotency_key=request.idempotency_key,
            request_hash=key_hash,
            response={"run_id": run_id, "plan_version": request.plan_version},
        )
    except BaseException as exc:
        _raise_batch_action_error(exc)
    if action in {"pause", "resume", "stop"}:
        app.state.kanglong_execution_task_registry.wake(run_id)
    latest = app.state.repository.get_kanglong_run(run_id)
    return KanglongBatchRunResponse.model_validate({**_kanglong_batch_run_payload(latest), **response})


@app.post('/kanglong/batch-simulation/run/{run_id}/pause', response_model=KanglongBatchRunResponse)
async def pause_kanglong_batch_run(run_id: str, request: KanglongControlRequest, raw: Request):
    require_local_management_request(raw)
    return await _control_kanglong_batch_run(run_id, request, action="pause")


@app.post('/kanglong/batch-simulation/run/{run_id}/resume', response_model=KanglongBatchRunResponse)
async def resume_kanglong_batch_run(run_id: str, request: KanglongControlRequest, raw: Request):
    require_local_management_request(raw)
    return await _control_kanglong_batch_run(run_id, request, action="resume")


@app.post('/kanglong/batch-simulation/run/{run_id}/stop', response_model=KanglongBatchRunResponse)
async def stop_kanglong_batch_run(run_id: str, request: KanglongControlRequest, raw: Request):
    require_local_management_request(raw)
    return await _control_kanglong_batch_run(run_id, request, action="stop")


@app.post('/kanglong/batch-simulation/run/{run_id}/recover', response_model=KanglongBatchRunResponse)
async def recover_kanglong_batch_run(run_id: str, request: KanglongBatchRecoverRequest, raw: Request):
    require_local_management_request(raw)
    return await _control_kanglong_batch_run(run_id, request, action="recover")


@app.post('/kanglong/batch-simulation/run/{run_id}/refresh-plan', response_model=KanglongBatchRunResponse)
async def refresh_kanglong_batch_plan(
    run_id: str,
    request: KanglongControlRequest,
    raw: Request,
) -> KanglongBatchRunResponse:
    require_local_management_request(raw)
    stored = app.state.repository.get_kanglong_run(run_id)
    if stored is None or stored.get("run_kind") != "kanglong_batch":
        raise HTTPException(status_code=404, detail={"code": "kanglong_batch_run_not_found"})
    key_hash = stable_payload_hash({"action": "refresh_plan", **request.model_dump(mode="json")})
    remembered = app.state.repository.get_live_kanglong_idempotency(
        request.idempotency_key,
        key_hash,
    )
    if remembered is not None:
        if remembered["conflict"]:
            raise HTTPException(status_code=409, detail={"code": "idempotency_key_conflict"})
        return KanglongBatchRunResponse.model_validate(
            {**_kanglong_batch_run_payload(stored), **remembered["response"]}
        )
    candidate = await _build_kanglong_batch_plan(
        _batch_plan_request_from_stored(stored),
        run_id=run_id,
        force_refresh=True,
    )
    statuses = {
        row["account_id"]: row["status"]
        for row in app.state.repository.list_kanglong_batch_accounts(run_id)
    }
    original = KanglongBatchPlan.from_payload(stored["plan"])
    try:
        refreshed = app.state.kanglong_batch_planner.refresh_pending_suffix(
            stored_plan=original,
            account_statuses=statuses,
            refreshed_accounts={item.account_id: item for item in candidate.accounts},
            credential_revision=candidate.credential_revision,
        )
    except UnsafeBatchRefresh as exc:
        app.state.repository.update_kanglong_run(
            run_id,
            status=KanglongRunStatus.NEEDS_ABORT_RECOVER.value,
            available_actions=available_actions_for_status(KanglongRunStatus.NEEDS_ABORT_RECOVER),
        )
        raise HTTPException(status_code=409, detail={"code": "kanglong_batch_refresh_unsafe"}) from exc
    try:
        response = app.state.repository.commit_kanglong_action(
            run_id=run_id,
            mutation=KanglongActionMutation(
                expected_statuses=(
                    KanglongRunStatus.BLOCKED_PLAN_STALE.value,
                    KanglongRunStatus.PAUSED_PLAN_RECHECK_CHANGED.value,
                    KanglongRunStatus.CHAIN_READY.value,
                    KanglongRunStatus.PLAN_CONFIRMED.value,
                ),
                expected_plan_version=request.plan_version,
                expected_action_version=request.expected_action_version,
                next_status=KanglongRunStatus.CHAIN_READY.value,
                available_actions=tuple(available_actions_for_status(KanglongRunStatus.CHAIN_READY)),
                plan=refreshed.to_payload(),
                events=({"event_type": "kanglong_batch_plan_refreshed", "payload": {}},),
                increment_action_version=True,
            ),
            idempotency_key=request.idempotency_key,
            request_hash=key_hash,
            response={"run_id": run_id, "plan_version": refreshed.plan_version},
        )
    except BaseException as exc:
        _raise_batch_action_error(exc)
    latest = app.state.repository.get_kanglong_run(run_id)
    return KanglongBatchRunResponse.model_validate({**_kanglong_batch_run_payload(latest), **response})


@app.post('/kanglong/batch-simulation/run/{run_id}/{action}', response_model=KanglongBatchRunResponse)
async def control_kanglong_batch_run_generic(
    run_id: str,
    action: str,
    payload: dict[str, Any],
    raw: Request,
) -> KanglongBatchRunResponse:
    require_local_management_request(raw)
    normalized = action.strip().lower()
    if normalized == "refresh_plan":
        return await refresh_kanglong_batch_plan(
            run_id,
            KanglongControlRequest.model_validate(payload),
            raw,
        )
    if normalized == "recover":
        request = KanglongBatchRecoverRequest.model_validate(payload)
    elif normalized in {"pause", "resume", "stop"}:
        request = KanglongControlRequest.model_validate(payload)
    else:
        raise HTTPException(status_code=404, detail={"code": "kanglong_batch_action_not_found"})
    return await _control_kanglong_batch_run(run_id, request, action=normalized)


@app.get('/kanglong/batch-simulation/run/{run_id}', response_model=KanglongBatchRunResponse)
async def get_kanglong_batch_run(run_id: str) -> KanglongBatchRunResponse:
    stored = app.state.repository.get_kanglong_run(run_id)
    if stored is None or stored.get("run_kind") != "kanglong_batch":
        raise HTTPException(status_code=404, detail={"code": "kanglong_batch_run_not_found"})
    return KanglongBatchRunResponse.model_validate(_kanglong_batch_run_payload(stored))


@app.get('/kanglong/batch-simulation/run/{run_id}/events')
async def get_kanglong_batch_events(run_id: str, after_event_id: int = 0, limit: int = 200) -> dict[str, Any]:
    stored = app.state.repository.get_kanglong_run(run_id)
    if stored is None or stored.get("run_kind") != "kanglong_batch":
        raise HTTPException(status_code=404, detail={"code": "kanglong_batch_run_not_found"})
    return app.state.repository.list_kanglong_events(run_id, after_event_id=after_event_id, limit=limit)


@app.get('/kanglong/batch-simulation/open-runs')
async def list_open_kanglong_batch_runs() -> dict[str, Any]:
    return {
        "runs": [
            _kanglong_batch_run_payload(stored)
            for stored in app.state.repository.list_active_kanglong_batch_runs()
        ]
    }


@app.get('/config/account-credentials')
async def get_account_credentials(raw: Request) -> dict[str, object]:
    require_local_management_request(raw)
    try:
        return _credential_list_payload()
    except BaseException as exc:
        _raise_credential_api_error(exc)


@app.post('/config/account-credentials')
async def create_account_credential(
    request: AccountCredentialCreateRequest,
    raw: Request,
) -> dict[str, object]:
    require_local_management_request(raw)
    accounts = _credential_accounts()
    candidate_account = _account_from_create(request)
    if any(account.account_id == candidate_account.account_id for account in accounts):
        raise HTTPException(status_code=409, detail={"code": "account_credential_already_exists"})
    store: AccountCredentialStore = app.state.account_credential_store
    candidate = _manual_credential_candidate(
        [*accounts, candidate_account],
        credential_revision=store.current_revision(),
    )
    return await _commit_credential_candidate(candidate)


@app.post('/config/account-credentials/import/preview')
async def preview_account_credential_import(
    request: AccountCredentialImportPreviewRequest,
    raw: Request,
) -> dict[str, object]:
    require_local_management_request(raw)
    imported = [_account_from_create(account) for account in request.accounts]
    store: AccountCredentialStore = app.state.account_credential_store
    previews: CredentialImportPreviewStore = app.state.credential_imports
    try:
        candidate = previews.create(
            existing=_credential_accounts(),
            imported=imported,
            mode=request.mode,
            credential_revision=store.current_revision(),
        )
    except BaseException as exc:
        _raise_credential_api_error(exc)
    return {
        "preview_token": candidate.preview_token,
        "credential_revision": candidate.credential_revision,
        "expires_at": candidate.expires_at.isoformat(),
        "final_accounts": [
            _credential_summary(account, order)
            for order, account in enumerate(candidate.accounts)
        ],
        "changes": candidate.changes,
    }


@app.post('/config/account-credentials/import/commit')
async def commit_account_credential_import(
    request: AccountCredentialImportCommitRequest,
    raw: Request,
) -> dict[str, object]:
    require_local_management_request(raw)
    previews: CredentialImportPreviewStore = app.state.credential_imports
    try:
        candidate = previews.consume_verified_preview(request.preview_token)
    except BaseException as exc:
        _raise_credential_api_error(exc)
    return await _commit_credential_candidate(candidate)


@app.put('/config/account-credentials/order')
async def reorder_account_credentials(
    request: AccountCredentialOrderRequest,
    raw: Request,
) -> dict[str, object]:
    require_local_management_request(raw)
    accounts = _credential_accounts()
    by_id = {account.account_id: account for account in accounts}
    normalized_ids = [account_id.strip().lower() for account_id in request.account_ids]
    if len(normalized_ids) != len(set(normalized_ids)) or set(normalized_ids) != set(by_id):
        raise HTTPException(status_code=422, detail={"code": "account_credential_order_invalid"})
    store: AccountCredentialStore = app.state.account_credential_store
    candidate = _manual_credential_candidate(
        [by_id[account_id] for account_id in normalized_ids],
        credential_revision=store.current_revision(),
    )
    return await _commit_credential_candidate(candidate)


@app.post('/config/account-credentials/{account_id}/verify')
async def verify_account_credential(account_id: str, raw: Request) -> dict[str, object]:
    require_local_management_request(raw)
    normalized = account_id.strip().lower()
    account = next((item for item in _credential_accounts() if item.account_id == normalized), None)
    if account is None:
        raise HTTPException(status_code=404, detail={"code": "account_credential_not_found"})
    runtime_manager: AccountRuntimeManager = app.state.runtime_manager
    try:
        prepared = await runtime_manager.prepare_accounts([replace(account, enabled=True)])
        await runtime_manager.close_replaced(tuple(prepared.runtimes.values()))
    except BaseException as exc:
        if isinstance(exc, asyncio.CancelledError):
            raise
        _raise_credential_api_error(exc)
    return {"account_id": normalized, "verified": True, "account_mode": "portfolio_margin"}


@app.put('/config/account-credentials/{account_id}')
async def update_account_credential(
    account_id: str,
    request: AccountCredentialUpdateRequest,
    raw: Request,
) -> dict[str, object]:
    require_local_management_request(raw)
    normalized = account_id.strip().lower()
    accounts = _credential_accounts()
    index = next((idx for idx, account in enumerate(accounts) if account.account_id == normalized), None)
    if index is None:
        raise HTTPException(status_code=404, detail={"code": "account_credential_not_found"})
    changes = request.model_dump(exclude_unset=True)
    if changes.get("name") is not None:
        changes["name"] = str(changes["name"]).strip()
    if changes.get("api_key") is not None:
        changes["api_key"] = str(changes["api_key"]).strip()
    if changes.get("api_secret") is not None:
        changes["api_secret"] = str(changes["api_secret"]).strip()
    accounts[index] = replace(accounts[index], **changes)
    store: AccountCredentialStore = app.state.account_credential_store
    candidate = _manual_credential_candidate(accounts, credential_revision=store.current_revision())
    return await _commit_credential_candidate(candidate)


@app.delete('/config/account-credentials/{account_id}')
async def delete_account_credential(account_id: str, raw: Request) -> dict[str, object]:
    require_local_management_request(raw)
    normalized = account_id.strip().lower()
    accounts = _credential_accounts()
    remaining = [account for account in accounts if account.account_id != normalized]
    if len(remaining) == len(accounts):
        raise HTTPException(status_code=404, detail={"code": "account_credential_not_found"})
    store: AccountCredentialStore = app.state.account_credential_store
    candidate = _manual_credential_candidate(remaining, credential_revision=store.current_revision())
    return await _commit_credential_candidate(candidate)


@app.get('/config/whitelist', response_model=WhitelistResponse)
async def get_whitelist() -> WhitelistResponse:
    service = current_runtime(app).service
    return WhitelistResponse(symbols=service.get_whitelist())


@app.put('/config/whitelist', response_model=WhitelistResponse)
async def update_whitelist(request: WhitelistUpdateRequest, raw: Request) -> WhitelistResponse:
    require_local_management_request(raw)
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
async def select_account(request: AccountSelectRequest, raw: Request) -> AccountSelectResponse:
    require_local_management_request(raw)
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




