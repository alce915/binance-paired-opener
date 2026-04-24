const logsBody = document.getElementById("logsBody");
const asksContainer = document.getElementById("asksContainer");
const bidsContainer = document.getElementById("bidsContainer");
const connectionToggle = document.getElementById("connectionToggle");
const accountSelect = document.getElementById("accountSelect");
const accountBadge = document.getElementById("accountBadge");
const orderBookInput = document.getElementById("orderBookInput");
const executionSymbol = document.getElementById("executionSymbol");
const closeExecutionSymbol = document.getElementById("closeExecutionSymbol");
const singleOpenExecutionSymbol = document.getElementById("singleOpenExecutionSymbol");
const confirmSymbolBtn = document.getElementById("confirmSymbolBtn");
const editWhitelistBtn = document.getElementById("editWhitelistBtn");
const positionsList = document.getElementById("positionsList");
const createBtn = document.getElementById("createBtn");
const createCloseBtn = document.getElementById("createCloseBtn");
const createSingleOpenBtn = document.getElementById("createSingleOpenBtn");
const createSingleCloseBtn = document.getElementById("createSingleCloseBtn");
const simulateBtn = document.getElementById("simulateBtn");
const executionSummaryBanner = document.getElementById("executionSummaryBanner");
const executionSummaryText = document.getElementById("executionSummaryText");
const executionRiskBanner = document.getElementById("executionRiskBanner");
const executionRiskText = document.getElementById("executionRiskText");
const recoverableSessionBanner = document.getElementById("recoverableSessionBanner");
const recoverableSessionText = document.getElementById("recoverableSessionText");
const recoverSessionBtn = document.getElementById("recoverSessionBtn");
const dismissRecoverSessionBtn = document.getElementById("dismissRecoverSessionBtn");
const minNotionalHint = document.getElementById("minNotionalHint");
const closeValidationHint = document.getElementById("closeValidationHint");
const singleOpenValidationHint = document.getElementById("singleOpenValidationHint");
const singleOpenLeverageInput = document.getElementById("singleOpenLeverage");
const singleCloseValidationHint = document.getElementById("singleCloseValidationHint");
const modeButtons = {
  paired_open: document.getElementById("modePairedOpen"),
  paired_close: document.getElementById("modePairedClose"),
  single_open: document.getElementById("modeSingleOpen"),
  single_close: document.getElementById("modeSingleClose"),
};
const modePanels = {
  paired_open: document.getElementById("pairedOpenPanel"),
  paired_close: document.getElementById("pairedClosePanel"),
  single_open: document.getElementById("singleOpenPanel"),
  single_close: document.getElementById("singleClosePanel"),
};
let eventSource = null;
let executionMode = "paired_open";
let activeSymbol = executionSymbol.value || "BTCUSDT";
const APP_CONFIG = window.__APP_CONFIG__ || {};
const APP_I18N = window.__APP_I18N__ || {};
const I18N_MESSAGES = APP_I18N.messages || {};
const I18N_REGISTRIES = APP_I18N.registries || {};
const APP_LOCALE = APP_CONFIG.locale || APP_I18N.default_locale || "zh-CN";
const APP_TIMEZONE = APP_CONFIG.timezone || APP_I18N.default_timezone || "Asia/Shanghai";
const DEFAULT_ACCOUNT_NAME = I18N_MESSAGES["common.default_account_name"] || "默认账户";
let currentAccount = { id: "default", name: DEFAULT_ACCOUNT_NAME };
let availableAccounts = [];
let whitelistSymbols = [];
let temporaryCustomSymbol = null;
let latestReferencePrice = 0;
let latestAvailableBalance = null;
let currentPositions = [];
const latestOpenOrderCountsBySymbol = new Map();
let currentSymbolInfo = { symbol: activeSymbol, min_notional: 0, allowed: true };
let symbolInfoReady = false;
let precheckTimer = null;
let precheckAbortController = null;
let latestPrecheckToken = 0;
const latestPrecheckResultByMode = new Map();
const latestResolvedPrecheckPayloadByMode = new Map();
const inFlightPrecheckPayloadByMode = new Map();
const precheckTimersByMode = new Map();
const precheckAbortControllersByMode = new Map();
const latestPrecheckTokensByMode = new Map();
let precheckPaused = false;
let lastAutoPrecheckAt = 0;
let activeSessionId = null;
let activeSessionPoller = null;
let activeSessionState = null;
let latestSessionEventId = 0;
const seenSessionEventIds = new Set();
let executionActionInFlightCount = 0;
let simulationRunInFlight = false;
let simulationAbortInFlight = false;
let sessionAbortInFlight = false;
let latestExecutionStatsState = null;
let activeExecutionSummary = null;
let topRiskBanner = null;
let recoverableSessionState = null;
let recoverableSessionDismissed = false;
let latestResidualSideLabel = "--";
const modeHintStateByMode = new Map();
const precheckFreshnessStateByMode = new Map();
["paired_open", "paired_close", "single_open", "single_close"].forEach((mode) => {
  modeHintStateByMode.set(mode, { canCreate: false, canSimulate: false });
  precheckFreshnessStateByMode.set(mode, { fresh: false, reason: "pending" });
});
const MAX_LOG_LINES = Number(APP_CONFIG.frontend_execution_log_lines || 200);
const DEFAULT_REAL_ACTION_LABELS = {
  paired_open: "创建真实开单会话",
  paired_close: "创建真实平仓会话",
  single_open: "创建真实单向开仓会话",
  single_close: "创建真实单向平仓会话",
};
const DEFAULT_SIMULATE_LABEL = copyOrDefault("runtime.simulation_run", "模拟执行");
const EXECUTION_TERMINATE_LABEL = copyOrDefault("runtime.execution_running_click_abort", "执行中...点击终止");
const SIMULATION_TERMINATE_LABEL = copyOrDefault("runtime.simulation_running_click_abort", "模拟中...点击终止");
const EXECUTION_ABORTING_LABEL = copyOrDefault("runtime.execution_aborting", "终止中...");
const LOG_LEVEL_LABELS = {
  info: copyOrDefault("console.log_levels.info", "提示"),
  success: copyOrDefault("console.log_levels.success", "成功"),
  warn: copyOrDefault("console.log_levels.warn", "警告"),
  error: copyOrDefault("console.log_levels.error", "错误"),
};
const CONNECTION_STATUS_LABELS = {
  connected: I18N_MESSAGES["runtime.connection_connected"] || "已连接",
  connecting: I18N_MESSAGES["runtime.connection_connecting"] || "连接中",
  disconnected: I18N_MESSAGES["runtime.connection_disconnected"] || "已断开",
  error: I18N_MESSAGES["runtime.connection_error"] || "异常",
  idle: I18N_MESSAGES["runtime.connection_idle"] || "空闲",
};
const orderBookRowCache = { sell: [], buy: [] };
const positionRowCache = new Map();
let pendingOrderbookPayload = null;
let pendingAccountOverviewPayload = null;
const pendingLogEntries = [];
let renderFramePending = false;

function resolveActionAvailability(hintState = {}, runtimeState = {}) {
  const locked = Boolean(runtimeState.requestInFlight || runtimeState.hasActiveSession);
  return {
    canCreate: Boolean(hintState.canCreate) && !locked,
    canSimulate: Boolean(hintState.canSimulate) && !locked,
    locked,
  };
}

function normalizeSessionKind(kind) {
  return ["paired_close", "single_open", "single_close"].includes(String(kind || ""))
    ? String(kind)
    : "paired_open";
}

function isTerminalSimulationStatus(status) {
  return ["idle", "completed", "completed_with_skips", "blocked", "aborted", "exception"].includes(String(status || "idle"));
}

function hasActiveSimulationRun() {
  if (simulationRunInFlight || simulationAbortInFlight) return true;
  return Boolean(latestExecutionStatsState && !isTerminalSimulationStatus(latestExecutionStatsState.status));
}

function activeSessionKind() {
  if (!hasActiveExecutionSession()) return null;
  const sessionKind = activeSessionState?.session_kind;
  return sessionKind ? normalizeSessionKind(sessionKind) : "paired_open";
}

function currentExecutionLockState() {
  return {
    requestInFlight: executionActionInFlightCount > 0,
    hasActiveSession: hasActiveExecutionSession(),
    hasActiveSimulation: hasActiveSimulationRun(),
  };
}

function executionButtonForMode(mode) {
  switch (normalizeSessionKind(mode)) {
    case "paired_close":
      return createCloseBtn;
    case "single_open":
      return createSingleOpenBtn;
    case "single_close":
      return createSingleCloseBtn;
    default:
      return createBtn;
  }
}

function eachExecutionInput(callback) {
  const controls = new Set([
    connectionToggle,
    orderBookInput,
    confirmSymbolBtn,
    editWhitelistBtn,
    accountSelect,
    executionSymbol,
    closeExecutionSymbol,
    singleOpenExecutionSymbol,
    document.getElementById("singleCloseExecutionSymbol"),
    ...Object.values(modeButtons),
    ...document.querySelectorAll(".mode-panel input, .mode-panel select"),
  ]);
  controls.forEach((element) => {
    if (element) callback(element);
  });
}

function setExecutionInputLock(locked) {
  eachExecutionInput((element) => {
    if (locked) {
      if (element.dataset.executionLocked !== "true") {
        element.dataset.executionLocked = "true";
        element.dataset.executionLockedPrev = element.disabled ? "1" : "0";
      }
      element.disabled = true;
      return;
    }
    if (element.dataset.executionLocked === "true") {
      element.disabled = element.dataset.executionLockedPrev === "1";
      delete element.dataset.executionLocked;
      delete element.dataset.executionLockedPrev;
    }
  });
}

function setBannerState(element, textElement, config = null) {
  if (!element || !textElement) return;
  element.classList.remove("summary", "info", "warn", "error", "hidden");
  if (!config || !config.message) {
    element.classList.add("hidden");
    textElement.textContent = "";
    return;
  }
  element.classList.add(config.tone || "info");
  textElement.textContent = config.message;
}

function updateTopRiskBanner(level, message) {
  if (!message) return;
  const tone = level === "error" ? "error" : level === "warn" ? "warn" : "info";
  topRiskBanner = { tone, message };
}

function clearTopRiskBanner() {
  topRiskBanner = null;
}

function renderRiskBanner() {
  setBannerState(executionRiskBanner, executionRiskText, topRiskBanner);
}

function formatExecutionStatus(status) {
  switch (String(status || "idle")) {
    case "running":
      return copyOrDefault("runtime.execution_status_running", "执行中");
    case "aborting":
      return copyOrDefault("runtime.execution_status_aborting", "终止中");
    case "aborted":
      return copyOrDefault("runtime.execution_status_aborted", "已终止");
    case "completed":
      return copyOrDefault("runtime.execution_status_completed", "已完成");
    case "completed_with_skips":
      return copyOrDefault("runtime.execution_status_completed_with_skips", "已完成（含跳过）");
    case "blocked":
      return copyOrDefault("runtime.execution_status_blocked", "已阻断");
    case "paused":
      return copyOrDefault("runtime.execution_status_paused", "已暂停");
    case "pending":
      return copyOrDefault("runtime.execution_status_pending", "待执行");
    case "exception":
      return copyOrDefault("runtime.execution_status_exception", "执行异常");
    default:
      return copyOrDefault("runtime.execution_status_idle", "空闲");
  }
}

function formatStopReason(reason) {
  switch (String(reason || "")) {
    case "filled":
      return "已完成";
    case "below_min_notional":
      return "残量低于最小下单金额";
    case "insufficient_balance":
      return "余额不足";
    case "insufficient_position":
      return "持仓不足";
    case "price_guard_blocked":
      return "价格保护阻断";
    case "quote_stale":
      return "行情已过期";
    case "open_order_conflict_detected":
      return "检测到挂单冲突";
    case "target_guard_blocked":
      return "目标保护阻断";
    case "max_extension_rounds_reached":
      return "补充轮已耗尽";
    case "max_session_duration_reached":
      return "已超过最长执行时长";
    default:
      return "--";
  }
}

function summarizeExecutionSummary(summary) {
  if (!summary) return "";
  const segments = [
    `${copyOrDefault("console.summary.mode", "模式")}：${formatModeLabel(summary.mode || executionMode)}`,
    `${copyOrDefault("console.summary.rounds", "轮次")}：${summary.roundsCompleted || 0} / ${summary.roundsTotal || 0}`,
    `${copyOrDefault("console.summary.status", "状态")}：${formatExecutionStatus(summary.status)}`,
    `${copyOrDefault("console.summary.carryover", "累计残量")}：${formatNumber(summary.carryoverQty || 0, 6)}`,
    `${copyOrDefault("console.summary.residual_side", "残量归属")}：${summary.residualSide || "--"}`,
    `${copyOrDefault("console.summary.alignment", "最终对齐")}：${formatAlignmentStatus(summary.finalAlignmentStatus)}`,
  ];
  if (Array.isArray(summary.plannedRoundQtys) && summary.plannedRoundQtys.length) {
    segments.push(`计划轮量：${summary.plannedRoundQtys.join(" / ")}`);
  }
  if (Number(summary.maxExtensionRounds || 0) > 0) {
    segments.push(`补充轮：${Number(summary.extensionRoundsUsed || 0)} / ${Number(summary.maxExtensionRounds || 0)}`);
  }
  if (summary.stopReason) {
    segments.push(`停止原因：${formatStopReason(summary.stopReason)}`);
  }
  if (summary.sessionDeadlineAt) {
    segments.push(`截止：${new Date(summary.sessionDeadlineAt).toLocaleTimeString(APP_LOCALE, { hour12: false, timeZone: APP_TIMEZONE })}`);
  }
  if (summary.abortRequested) {
    segments.push(copyOrDefault("console.summary.abort_requested", "已请求终止"));
  }
  return segments.join(" | ");
}

function renderExecutionSummaryBanner() {
  setBannerState(
    executionSummaryBanner,
    executionSummaryText,
    activeExecutionSummary ? { tone: "summary", message: summarizeExecutionSummary(activeExecutionSummary) } : null,
  );
}

function updateAbortStateLabel(status = "idle", abortRequested = false) {
  const statAbortState = document.getElementById("statAbortState");
  if (!statAbortState) return;
  if (abortRequested || status === "aborting") {
    statAbortState.textContent = copyOrDefault("runtime.execution_abort_requested", "已请求终止");
    return;
  }
  if (status === "aborted") {
    statAbortState.textContent = copyOrDefault("runtime.execution_status_aborted", "已终止");
    return;
  }
  statAbortState.textContent = copyOrDefault("runtime.execution_abort_not_requested", "未请求");
}

function buildExecutionSummary(source = {}, overrides = {}) {
  return {
    mode: normalizeSessionKind(overrides.mode || source.session_kind || source.mode || executionMode),
    status: String(overrides.status || source.status || "idle"),
    roundsCompleted: Number(overrides.roundsCompleted ?? source.rounds_completed ?? 0),
    roundsTotal: Number(overrides.roundsTotal ?? source.rounds_total ?? source.round_count ?? 0),
    carryoverQty: Number(overrides.carryoverQty ?? resolveResidualQty(source) ?? 0),
    residualSide: overrides.residualSide || latestResidualSideLabel || "--",
    finalAlignmentStatus: String(overrides.finalAlignmentStatus || source.final_alignment_status || "not_needed"),
    plannedRoundQtys: overrides.plannedRoundQtys || source.planned_round_qtys || [],
    extensionRoundsUsed: Number(overrides.extensionRoundsUsed ?? source.extension_rounds_used ?? 0),
    remainingExtensionRounds: Number(overrides.remainingExtensionRounds ?? source.remaining_extension_rounds ?? 0),
    maxExtensionRounds: Number(overrides.maxExtensionRounds ?? source.max_extension_rounds ?? 0),
    sessionDeadlineAt: overrides.sessionDeadlineAt || source.session_deadline_at || null,
    stopReason: String(overrides.stopReason || source.stop_reason || ""),
    abortRequested: Boolean(overrides.abortRequested),
  };
}

function updateExecutionSummary(summary = null) {
  activeExecutionSummary = summary;
  renderExecutionSummaryBanner();
}

function describePrecheckFreshness(decision) {
  if (!decision || !decision.runnable || decision.reason === "not_runnable" || decision.reason === "no_snapshot") {
    return {
      fresh: false,
      reason: "pending",
      label: copyOrDefault("runtime.precheck_status_pending", "待校验"),
      message: copyOrDefault("runtime.precheck_status_pending", "待校验"),
    };
  }
  if (decision.reason === "fresh") {
    return {
      fresh: true,
      reason: "fresh",
      label: copyOrDefault("runtime.precheck_status_fresh", "已校验"),
      message: copyOrDefault("runtime.precheck_status_fresh", "已校验"),
    };
  }
  const staleMessageKey = (() => {
    switch (decision.reason) {
      case "params_changed":
        return "runtime.precheck_stale_params_changed";
      case "context_stale":
      case "context_interval":
        return "runtime.precheck_stale_context_changed";
      case "price_drift":
      case "no_price_baseline":
        return "runtime.precheck_stale_price_drift";
      default:
        return "runtime.precheck_stale_interval";
    }
  })();
  return {
    fresh: false,
    reason: decision.reason,
    label: copyOrDefault("runtime.precheck_status_stale", "需重新确认"),
    message: copyOrDefault(staleMessageKey, "预检已过期，请重新确认。"),
  };
}

function syncPrecheckFreshnessState(mode = executionMode) {
  const freshness = describePrecheckFreshness(getModeValidationDecision(mode));
  precheckFreshnessStateByMode.set(mode, freshness);
  if (mode === executionMode) {
    const statPrecheckFreshness = document.getElementById("statPrecheckFreshness");
    if (statPrecheckFreshness) {
      statPrecheckFreshness.textContent = freshness.label;
    }
  }
  return freshness;
}

function selectRecoverableSession(sessions = []) {
  const candidates = Array.isArray(sessions) ? sessions : [];
  const active = candidates
    .filter((session) => !isTerminalSession(session.status))
    .sort((left, right) => new Date(right.updated_at || 0) - new Date(left.updated_at || 0));
  if (active.length) return active[0];
  const recoverable = candidates
    .filter((session) => String(session.status) === "exception" && String(session.recovery_status || "") === "recoverable")
    .sort((left, right) => new Date(right.updated_at || 0) - new Date(left.updated_at || 0));
  return recoverable[0] || null;
}

function renderRecoverableSessionBanner() {
  if (recoverableSessionDismissed || !recoverableSessionState) {
    setBannerState(recoverableSessionBanner, recoverableSessionText, null);
    return;
  }
  const session = recoverableSessionState;
  const promptKey = String(session.status) === "exception"
    ? "runtime.recover_session_resume_prompt"
    : "runtime.recover_session_monitor_prompt";
  const prompt = copyOrDefault(promptKey, "检测到未完成执行，是否恢复监控？", {
    session_id: session.session_id,
    symbol: session.symbol,
  });
  setBannerState(recoverableSessionBanner, recoverableSessionText, { tone: "info", message: prompt });
}

function copyOrDefault(key, fallback, params = {}) {
  const rendered = formatCopy(key, params);
  return rendered === key ? fallback : rendered;
}

function statusLabel(status) {
  return CONNECTION_STATUS_LABELS[String(status || "idle")] || String(status || "idle");
}

function formatCopy(key, params = {}) {
  const template = I18N_MESSAGES[key];
  if (typeof template !== "string") return key;
  return template.replace(/\{(\w+)\}/g, (_, name) => {
    const value = params[name];
    return value === undefined || value === null ? `{${name}}` : String(value);
  });
}

function unknownErrorMessage() {
  return I18N_MESSAGES["common.unknown_error"] || "未知错误";
}

function formatReason(code, params = {}, fallback = "") {
  const reasonEntry = I18N_REGISTRIES.reasons?.[code];
  if (reasonEntry?.key) {
    const rendered = formatCopy(reasonEntry.key, params);
    if (rendered !== reasonEntry.key) {
      return rendered;
    }
  }
  return fallback || unknownErrorMessage();
}

function resolveStructuredMessage(source = {}, fallback = "") {
  const safeFallback = fallback || unknownErrorMessage();
  if (source && typeof source === "object") {
    if (source.message_code) {
      const rendered = formatCopy(source.message_code, source.message_params || {});
      if (rendered !== source.message_code) {
        return rendered;
      }
    }
    if (source.message_key) {
      const rendered = formatCopy(source.message_key, source.message_params || {});
      if (rendered !== source.message_key) {
        return rendered;
      }
    }
    if (source.code) {
      return formatReason(source.code, source.params || {}, safeFallback);
    }
  }
  return safeFallback;
}

function userVisibleErrorMessage(error, fallback = "") {
  const safeFallback = fallback || unknownErrorMessage();
  if (error && typeof error === "object") {
    if (error.precheck) {
      return summarizePrecheckMessage(error.precheck, safeFallback) || safeFallback;
    }
    if (error.detail && typeof error.detail === "object") {
      return resolveStructuredMessage(error.detail, safeFallback);
    }
    if (error.code) {
      return formatReason(error.code, error.params || {}, safeFallback);
    }
  }
  return safeFallback;
}

function resolveLogMessage(source = {}, fallback = "") {
  const safeFallback = fallback || copyOrDefault("runtime.execution_message_unavailable", "日志信息暂不可用");
  if (source && typeof source === "object") {
    if (source.messageCode) {
      const rendered = formatCopy(source.messageCode, source.messageParams || {});
      if (rendered !== source.messageCode) {
        return rendered;
      }
    }
    if (source.trustedMessage === true && source.message) {
      return String(source.message);
    }
  }
  return safeFallback;
}

function nowTime() {
  return new Date().toLocaleTimeString(APP_LOCALE, { hour12: false, timeZone: APP_TIMEZONE });
}

function request(path, options = {}) {
  return fetch(path, options).then(async (response) => {
    const text = await response.text();
    if (!response.ok) {
      const safeFallback = unknownErrorMessage();
      let message = safeFallback;
      let precheck = null;
      let validationDetail = null;
      let errorDetail = null;
      try {
        const payload = JSON.parse(text);
        if (payload && typeof payload === "object") {
          if (Array.isArray(payload.detail)) {
            validationDetail = payload.detail;
            message = formatReason("invalid_parameter", {}, safeFallback);
          } else if (payload.detail && typeof payload.detail === "object") {
            errorDetail = payload.detail;
            message = resolveStructuredMessage(payload.detail, safeFallback);
            precheck = payload.detail.precheck || null;
          } else {
            message = resolveStructuredMessage(payload, safeFallback);
            precheck = payload.precheck || null;
          }
        }
      } catch {}
      const error = new Error(message);
      error.rawResponseText = text;
      if (errorDetail) {
        error.detail = errorDetail;
        error.code = errorDetail.code || null;
        error.params = errorDetail.params || {};
      }
      if (precheck) error.precheck = precheck;
      if (validationDetail) error.validationDetail = validationDetail;
      throw error;
    }
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  });
}

function formatNumber(value, digits = 8) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "0";
  return numeric.toLocaleString(APP_LOCALE, {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  });
}

function formatMoney(value, digits = 2) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "0.00";
  return numeric.toLocaleString(APP_LOCALE, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}
function formatDisplayPrice(value, digits = 2) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) return "--";
  return formatNumber(numeric, digits);
}

function resolveResidualQty(source = {}) {
  if (source.final_unaligned_qty !== undefined && source.final_unaligned_qty !== null) {
    return source.final_unaligned_qty;
  }
  if (source.stage2_carryover_qty !== undefined && source.stage2_carryover_qty !== null) {
    return source.stage2_carryover_qty;
  }
  if (source.carryover_qty !== undefined && source.carryover_qty !== null) {
    return source.carryover_qty;
  }
  return 0;
}

function normalizeSymbol(value) {
  return (value || "BTCUSDT").trim().toUpperCase();
}

function inferBaseAsset(symbol) {
  const normalized = normalizeSymbol(symbol);
  const knownQuoteAssets = ["USDT", "USDC", "BUSD", "FDUSD", "TUSD", "BTC", "ETH", "BNB", "EUR", "TRY"];
  for (const quoteAsset of knownQuoteAssets) {
    if (normalized.endsWith(quoteAsset) && normalized.length > quoteAsset.length) {
      return normalized.slice(0, normalized.length - quoteAsset.length);
    }
  }
  return normalized;
}

function updateSymbolUnits(symbol) {
  const baseAsset = inferBaseAsset(symbol);
  ["openRoundQtyUnit", "closeQtyUnit", "closeRoundQtyUnit", "singleOpenQtyUnit", "singleOpenRoundQtyUnit", "singleCloseQtyUnit", "singleCloseRoundQtyUnit"].forEach((id) => {
    const element = document.getElementById(id);
    if (element) element.textContent = baseAsset;
  });
}

function syncTrendSelectTone(selectElement) {
  if (!selectElement) return;
  selectElement.classList.remove("trend-long", "trend-short");
  selectElement.style.color = "";
  selectElement.style.borderColor = "";
  selectElement.style.backgroundColor = "";
  selectElement.style.fontWeight = "700";
  if (selectElement.value === "long") {
    selectElement.classList.add("trend-long");
    selectElement.style.color = "#21986f";
    selectElement.style.borderColor = "rgba(33, 152, 111, 0.55)";
    selectElement.style.backgroundColor = "#f8fffb";
  } else if (selectElement.value === "short") {
    selectElement.classList.add("trend-short");
    selectElement.style.color = "#c6514d";
    selectElement.style.borderColor = "rgba(198, 81, 77, 0.55)";
    selectElement.style.backgroundColor = "#fff9f9";
  }
}

function syncPositionSideTone(selectElement) {
  if (!selectElement) return;
  selectElement.classList.remove("side-long", "side-short");
  selectElement.style.color = "";
  selectElement.style.borderColor = "";
  selectElement.style.backgroundColor = "";
  selectElement.style.fontWeight = "700";
  if (selectElement.value === "LONG") {
    selectElement.classList.add("side-long");
    selectElement.style.color = "#21986f";
    selectElement.style.borderColor = "rgba(33, 152, 111, 0.55)";
    selectElement.style.backgroundColor = "#f8fffb";
  } else if (selectElement.value === "SHORT") {
    selectElement.classList.add("side-short");
    selectElement.style.color = "#c6514d";
    selectElement.style.borderColor = "rgba(198, 81, 77, 0.55)";
    selectElement.style.backgroundColor = "#fff9f9";
  }
}

function formatModeLabel(mode) {
  switch (String(mode || "paired_open")) {
    case "paired_close":
      return copyOrDefault("console.mode_labels.paired_close", "双向平仓");
    case "single_open":
      return copyOrDefault("console.mode_labels.single_open", "单向开仓");
    case "single_close":
      return copyOrDefault("console.mode_labels.single_close", "单向平仓");
    default:
      return copyOrDefault("console.mode_labels.paired_open", "双向开仓");
  }
}

function precheckTone(precheck) {
  if (!precheck) return "";
  if (precheck.ok === false) return "error";
  const checks = Array.isArray(precheck.checks) ? precheck.checks : [];
  if (checks.some((item) => String(item.status) === "warn")) return "";
  return "success";
}

function optionalPositiveValue(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const numeric = Number(raw);
  if (!Number.isFinite(numeric) || numeric <= 0) return null;
  return raw;
}

function summarizePrecheckMessage(precheck, fallbackMessage) {
  if (!precheck) return fallbackMessage;
  const summary = String(resolveStructuredMessage(precheck, fallbackMessage || unknownErrorMessage()) || "").trim();
  const checks = Array.isArray(precheck.checks) ? precheck.checks : [];
  const warning = checks.find((item) => String(item.status) === "warn");
  const warningMessage = warning ? resolveStructuredMessage(warning, "") : "";
  if (warningMessage && warningMessage !== summary) {
    return `${summary} ${warningMessage}`;
  }
  return summary || fallbackMessage;
}

function buildPrecheckPayload(mode = executionMode) {
  switch (mode) {
    case "paired_close":
      return {
        session_kind: "paired_close",
        symbol: closeExecutionSymbol.value,
        trend_bias: document.getElementById("closeTrend").value,
        close_qty: optionalPositiveValue(document.getElementById("closeQty").value),
        round_count: Number(document.getElementById("closeRounds").value),
      };
    case "single_open": {
      const openMode = document.getElementById("singleOpenMode").value;
      return {
        session_kind: "single_open",
        symbol: document.getElementById("singleOpenExecutionSymbol").value,
        open_mode: openMode,
        selected_position_side: openMode === "align" ? null : (document.getElementById("singleOpenOrder").value || null),
        open_qty: optionalPositiveValue(document.getElementById("singleOpenQty").value),
        leverage: Number(document.getElementById("singleOpenLeverage").value),
        round_count: Number(document.getElementById("singleOpenRounds").value),
      };
    }
    case "single_close": {
      const closeMode = document.getElementById("singleCloseMode").value;
      return {
        session_kind: "single_close",
        symbol: document.getElementById("singleCloseExecutionSymbol").value,
        close_mode: closeMode,
        selected_position_side: closeMode === "align" ? null : (document.getElementById("singleCloseOrder").value || null),
        close_qty: optionalPositiveValue(document.getElementById("singleCloseQty").value),
        round_count: Number(document.getElementById("singleCloseRounds").value),
      };
    }
    default:
      return {
        session_kind: "paired_open",
        symbol: executionSymbol.value,
        trend_bias: document.getElementById("trend").value,
        leverage: Number(document.getElementById("leverage").value),
        round_count: Number(document.getElementById("calcRounds").value),
        round_qty: optionalPositiveValue(document.getElementById("roundQty").value),
      };
  }
}

function canRunPrecheck(mode, payload) {
  if (!payload || !payload.symbol) return false;
  switch (mode) {
    case "paired_close":
      return Boolean(payload.trend_bias && payload.close_qty && Number(payload.round_count) > 0);
    case "single_open":
      if (!payload.open_mode || !payload.open_qty || Number(payload.round_count) <= 0 || Number(payload.leverage) <= 0) {
        return false;
      }
      return payload.open_mode === "align" ? true : Boolean(payload.selected_position_side);
    case "single_close":
      if (!payload.close_mode || !payload.close_qty || Number(payload.round_count) <= 0) {
        return false;
      }
      return payload.close_mode === "align" ? true : Boolean(payload.selected_position_side);
    default:
      return Boolean(payload.trend_bias && payload.round_qty && Number(payload.round_count) > 0 && Number(payload.leverage) > 0);
  }
}

function setPrecheckPaused(paused) {
  precheckPaused = Boolean(paused);
  if (!precheckPaused) {
    return;
  }
  if (precheckTimer) {
    clearTimeout(precheckTimer);
    precheckTimer = null;
  }
  if (precheckAbortController) {
    precheckAbortController.abort();
    precheckAbortController = null;
  }
  precheckTimersByMode.forEach((timerId) => clearTimeout(timerId));
  precheckTimersByMode.clear();
  precheckAbortControllersByMode.forEach((controller) => controller.abort());
  precheckAbortControllersByMode.clear();
  inFlightPrecheckPayloadByMode.clear();
}
function isTerminalSession(status) {
  return ["completed", "completed_with_skips", "aborted", "exception"].includes(String(status || ""));
}

function formatAlignmentStatus(status) {
  switch (String(status || "not_needed")) {
    case "carryover_pending":
      return copyOrDefault("console.alignment.carryover_pending", "待最终对齐");
    case "market_aligned":
      return copyOrDefault("console.alignment.market_aligned", "市价对齐完成");
    case "flattened_both_sides":
      return copyOrDefault("console.alignment.flattened_both_sides", "双边清仓对齐");
    case "failed":
      return copyOrDefault("console.alignment.failed", "最终对齐失败");
    default:
      return copyOrDefault("console.alignment.not_needed", "未触发");
  }
}

function setCurrentAccount(accountId, accountName, syncSelect = true) {
  currentAccount = {
    id: String(accountId || currentAccount.id || "default").trim().toLowerCase(),
    name: String(accountName || currentAccount.name || DEFAULT_ACCOUNT_NAME).trim() || DEFAULT_ACCOUNT_NAME,
  };
  accountBadge.textContent = currentAccount.name;
  if (syncSelect && availableAccounts.length > 0) {
    accountSelect.value = currentAccount.id;
  }
}

function renderAccountOptions(accounts) {
  availableAccounts = Array.isArray(accounts) ? accounts : [];
  accountSelect.innerHTML = "";
  availableAccounts.forEach((account) => {
    const option = document.createElement("option");
    option.value = account.id;
    option.textContent = account.name;
    accountSelect.appendChild(option);
  });
  accountBadge.hidden = true;
  accountSelect.hidden = availableAccounts.length === 0;
  accountSelect.disabled = availableAccounts.length <= 1;
  const activeAccount = availableAccounts.find((account) => account.is_active) || availableAccounts[0];
  if (activeAccount) {
    setCurrentAccount(activeAccount.id, activeAccount.name, true);
  }
}

async function loadAccounts() {
  const payload = await request("/config/accounts");
  renderAccountOptions(payload.accounts || []);
  return payload.accounts || [];
}

function rebuildSymbolOptions(selectedSymbol = activeSymbol) {
  const normalizedSelected = normalizeSymbol(selectedSymbol);
  const options = [...whitelistSymbols];
  if (temporaryCustomSymbol && !options.includes(temporaryCustomSymbol)) {
    options.push(temporaryCustomSymbol);
  }
  if (!options.length && normalizedSelected) {
    options.push(normalizedSelected);
  }
  options.sort((left, right) => left.localeCompare(right));
  orderBookInput.innerHTML = "";
  options.forEach((symbol) => {
    const option = document.createElement("option");
    option.value = symbol;
    option.textContent = symbol === temporaryCustomSymbol && !whitelistSymbols.includes(symbol)
      ? `${symbol} ${copyOrDefault("console.symbol_custom_suffix", "(自定义)")}` : symbol;
    orderBookInput.appendChild(option);
  });
  if (options.includes(normalizedSelected)) {
    orderBookInput.value = normalizedSelected;
  } else if (options.length) {
    orderBookInput.value = options[0];
  }
}

async function loadWhitelist(options = {}) {
  const { preferWhitelistDefault = false } = options;
  const payload = await request("/config/whitelist");
  whitelistSymbols = (payload.symbols || []).map((symbol) => normalizeSymbol(symbol)).filter(Boolean);
  const currentSymbol = normalizeSymbol(executionSymbol.value || activeSymbol);
  const selectedSymbol =
    preferWhitelistDefault && whitelistSymbols.length && !whitelistSymbols.includes(currentSymbol)
      ? whitelistSymbols[0]
      : currentSymbol;
  temporaryCustomSymbol = whitelistSymbols.includes(selectedSymbol) ? null : selectedSymbol;
  if (selectedSymbol !== currentSymbol) {
    setActiveSymbol(selectedSymbol, false, { suppressRecalc: true, suppressPrecheck: true });
  }
  rebuildSymbolOptions(selectedSymbol);
  return whitelistSymbols;
}

function renderLevels(container, levels, side) {
  const cache = orderBookRowCache[side] || [];
  orderBookRowCache[side] = cache;
  const emptyState = container.querySelector(".orderbook-empty");
  if (!Array.isArray(levels) || !levels.length) {
    if (!emptyState) {
      const placeholder = document.createElement("div");
      placeholder.className = "empty-state orderbook-empty";
      placeholder.textContent = side === "sell"
        ? copyOrDefault("console.orderbook.load_asks", "开启连接后加载卖盘")
        : copyOrDefault("console.orderbook.load_bids", "开启连接后加载买盘");
      container.appendChild(placeholder);
    }
    cache.forEach((row) => {
      row.style.display = "none";
    });
    return;
  }
  if (emptyState) {
    emptyState.remove();
  }
  levels.forEach((level, index) => {
    let row = cache[index];
    if (!row) {
      row = document.createElement("div");
      row.className = `level-row ${side}`;
      const price = document.createElement("div");
      price.className = `level-price ${side}`;
      const qty = document.createElement("div");
      qty.className = "level-qty mono";
      const ratio = document.createElement("div");
      ratio.className = "level-bar-value mono";
      row.appendChild(price);
      row.appendChild(qty);
      row.appendChild(ratio);
      row._priceEl = price;
      row._qtyEl = qty;
      row._ratioEl = ratio;
      cache[index] = row;
      container.appendChild(row);
    }
    row.style.display = "";
    const depthRatio = Math.max(0, Math.min(1, Number(level.depth_ratio || 0)));
    const sideLabel = side === "sell"
      ? copyOrDefault("console.orderbook.side_sell", "卖")
      : copyOrDefault("console.orderbook.side_buy", "买");
    const priceText = `${sideLabel}${index + 1} ${formatNumber(level.price, 2)}`;
    const qtyText = formatNumber(level.qty, 6);
    const ratioText = `${Math.round(depthRatio * 100)}%`;
    row.style.setProperty("--depth", depthRatio);
    if (row._priceEl.textContent !== priceText) row._priceEl.textContent = priceText;
    if (row._qtyEl.textContent !== qtyText) row._qtyEl.textContent = qtyText;
    if (row._ratioEl.textContent !== ratioText) row._ratioEl.textContent = ratioText;
  });
  for (let index = levels.length; index < cache.length; index += 1) {
    cache[index].style.display = "none";
  }
}

function setEmptyState(container, className, text) {
  const empty = document.createElement("div");
  empty.className = className;
  empty.textContent = text;
  container.replaceChildren(empty);
}

function appendLog(level, message, createdAt, options = {}) {
  const line = document.createElement("div");
  line.className = "log-line";
  const time = createdAt ? new Date(createdAt).toLocaleTimeString(APP_LOCALE, { hour12: false, timeZone: APP_TIMEZONE }) : nowTime();
  const renderedMessage = resolveLogMessage({
    message,
    messageCode: options.messageCode,
    messageParams: options.messageParams,
    trustedMessage: options.trustedMessage === true,
  });
  const timeNode = document.createElement("div");
  timeNode.className = "log-time mono";
  timeNode.textContent = time;
  const badgeNode = document.createElement("div");
  badgeNode.className = `log-badge ${level}`;
  badgeNode.textContent = LOG_LEVEL_LABELS[level] || level;
  const messageNode = document.createElement("div");
  messageNode.className = "log-message";
  messageNode.textContent = renderedMessage;
  line.appendChild(timeNode);
  line.appendChild(badgeNode);
  line.appendChild(messageNode);
  logsBody.prepend(line);
  while (logsBody.children.length > MAX_LOG_LINES) {
    logsBody.removeChild(logsBody.lastElementChild);
  }
  if (options?.messageParams?.residual_side) {
    latestResidualSideLabel = String(options.messageParams.residual_side);
    if (activeExecutionSummary) {
      activeExecutionSummary = { ...activeExecutionSummary, residualSide: latestResidualSideLabel };
      renderExecutionSummaryBanner();
    }
    const statResidualSide = document.getElementById("statResidualSide");
    if (statResidualSide) {
      statResidualSide.textContent = latestResidualSideLabel;
    }
  }
  if (level === "warn" || level === "error") {
    updateTopRiskBanner(level, renderedMessage);
    renderRiskBanner();
  }
}

function setConnectionState(state) {
  const connected = Boolean(state.connected);
  const status = String(state.status || "disconnected");
  const badge = document.getElementById("connectionBadge");
  const switchLabel = document.getElementById("switchLabel");
  const footerDot = document.getElementById("footerDot");
  const footerStatus = document.getElementById("footerStatus");
  const statConnection = document.getElementById("statConnection");
  setCurrentAccount(state.account_id, state.account_name);
  setActiveSymbol(state.symbol || activeSymbol);
  if (connected) {
    badge.className = "badge success";
    switchLabel.className = "badge success";
    badge.textContent = statusLabel("connected");
    switchLabel.textContent = copyOrDefault("console.switch.on", "已开启");
    footerDot.classList.add("live");
  } else if (status === "connecting") {
    badge.className = "badge warn";
    switchLabel.className = "badge warn";
    badge.textContent = statusLabel("connecting");
    switchLabel.textContent = statusLabel("connecting");
    footerDot.classList.remove("live");
  } else if (status === "error") {
    badge.className = "badge error";
    switchLabel.className = "badge error";
    badge.textContent = statusLabel("error");
    switchLabel.textContent = statusLabel("error");
    footerDot.classList.remove("live");
  } else {
    badge.className = "badge warn";
    switchLabel.className = "badge warn";
    badge.textContent = copyOrDefault("console.connection.not_connected", "未连接");
    switchLabel.textContent = statusLabel("disconnected");
    footerDot.classList.remove("live");
  }
  footerStatus.textContent = `${connected ? statusLabel("connected") : statusLabel(status)} ${state.symbol || activeSymbol}`;
  statConnection.textContent = CONNECTION_STATUS_LABELS[status] || status;
  connectionToggle.checked = connected;
}
function refreshDerivedStats({ totalNotional = 0, perRoundNotional = 0, estimatedQty = 0, minNotional = Number(currentSymbolInfo.min_notional || 0) } = {}) {
  document.getElementById("statTotalNotional").textContent = formatNumber(totalNotional || 0, 4);
  document.getElementById("statPerRound").textContent = formatNumber(perRoundNotional || 0, 4);
  document.getElementById("statLastQty").textContent = formatNumber(estimatedQty || 0, 8);
  document.getElementById("statMinNotional").textContent = formatNumber(minNotional || 0, 4);
}

function syncPairedOpenDerivedPanel(derived = {}) {
  const normalizedRoundQty = Number(derived.normalized_round_qty || 0);
  const totalNotional = Number(derived.total_notional || 0);
  const perRoundNotional = Number(derived.per_round_notional || 0);
  const totalMargin = Number(derived.implied_margin_amount || 0);
  const rounds = Math.max(Number(document.getElementById("calcRounds")?.value || 0), 1);
  const marginPerRound = rounds > 0 ? totalMargin / rounds : 0;

  const roundQtyInput = document.getElementById("roundQty");
  const marginPerRoundEl = document.getElementById("marginPerRound");
  const totalNotionalEl = document.getElementById("totalNotional");
  const notionalPerRoundEl = document.getElementById("notionalPerRound");

  if (roundQtyInput) roundQtyInput.value = normalizedRoundQty > 0 ? normalizedRoundQty.toFixed(6) : "0";
  if (marginPerRoundEl) marginPerRoundEl.textContent = formatMoney(marginPerRound);
  if (totalNotionalEl) totalNotionalEl.textContent = formatMoney(totalNotional);
  if (notionalPerRoundEl) notionalPerRoundEl.textContent = formatMoney(perRoundNotional);
}

function syncSingleOpenDerivedPanel(derived = {}) {
  const normalizedRoundQty = Number(derived.normalized_round_qty || 0);
  const totalNotional = Number(derived.total_notional || 0);
  const perRoundNotional = Number(derived.per_round_notional || 0);
  const totalMargin = Number(derived.implied_margin_amount || 0);
  const rounds = Math.max(Number(document.getElementById("singleOpenRounds")?.value || 0), 1);
  const marginPerRound = rounds > 0 ? totalMargin / rounds : 0;

  const roundQtyInput = document.getElementById("singleOpenRoundQty");
  const marginPerRoundEl = document.getElementById("singleOpenMarginPerRound");
  const totalNotionalEl = document.getElementById("singleOpenTotalNotional");
  const notionalPerRoundEl = document.getElementById("singleOpenNotionalPerRound");

  if (roundQtyInput) roundQtyInput.value = normalizedRoundQty > 0 ? normalizedRoundQty.toFixed(6) : "0";
  if (marginPerRoundEl) marginPerRoundEl.textContent = formatMoney(marginPerRound);
  if (totalNotionalEl) totalNotionalEl.textContent = formatMoney(totalNotional);
  if (notionalPerRoundEl) notionalPerRoundEl.textContent = formatMoney(perRoundNotional);
}

function updateExecutionStats(stats) {
  latestExecutionStatsState = stats;
  if (isTerminalSimulationStatus(stats.status)) {
    simulationRunInFlight = false;
    simulationAbortInFlight = false;
  }
  document.getElementById("statSimStatus").textContent = stats.status || "idle";
  document.getElementById("statRounds").textContent = `${stats.rounds_completed || 0} / ${stats.rounds_total || 0}`;
  document.getElementById("statTotalNotional").textContent = formatNumber(stats.total_notional || 0, 4);
  document.getElementById("statPerRound").textContent = formatNumber(stats.notional_per_round || 0, 4);
  document.getElementById("statLastQty").textContent = formatNumber(stats.last_qty || 0, 8);
  document.getElementById("statMode").textContent = formatModeLabel(stats.mode || executionMode);
  if (stats.min_notional !== undefined) {
    document.getElementById("statMinNotional").textContent = formatNumber(stats.min_notional || 0, 4);
  }
  if (
    stats.carryover_qty !== undefined
    || stats.stage2_carryover_qty !== undefined
    || stats.final_unaligned_qty !== undefined
  ) {
    document.getElementById("statCarryoverQty").textContent = formatNumber(resolveResidualQty(stats), 6);
  }
  if (stats.final_alignment_status !== undefined) {
    document.getElementById("statFinalAlignment").textContent = formatAlignmentStatus(stats.final_alignment_status);
  }
  updateAbortStateLabel(stats.status, simulationAbortInFlight);
  if (stats.status === "running" || stats.status === "aborting" || !isTerminalSimulationStatus(stats.status)) {
    updateExecutionSummary(buildExecutionSummary(stats, { abortRequested: simulationAbortInFlight || stats.status === "aborting" }));
  } else if (!hasActiveExecutionSession()) {
    updateExecutionSummary(buildExecutionSummary(stats, { abortRequested: false }));
  }
  refreshExecutionActionButtons();
}

function positionQty(symbol, positionSide) {
  return currentPositions
    .filter((position) => position.symbol === symbol && String(position.position_side) === positionSide)
    .reduce((total, position) => total + Number(position.qty || 0), 0);
}

function maxCloseableQtyForSymbol(symbol) {
  return Math.min(positionQty(symbol, "LONG"), positionQty(symbol, "SHORT"));
}

function renderAccountOverview(payload) {
  const totals = payload.totals || {};
  setCurrentAccount(payload.account_id, payload.account_name);
  const equity = document.getElementById("overviewEquity");
  const margin = document.getElementById("overviewMarginUsed");
  const availableBalance = document.getElementById("overviewAvailableBalance");
  const unrealizedPnl = document.getElementById("overviewUnrealizedPnl");
  latestAvailableBalance = payload.status === "ok" ? Number(totals.available_balance || 0) : null;
  currentPositions = Array.isArray(payload.positions) ? payload.positions : [];

  equity.textContent = payload.status === "idle" ? "--" : formatNumber(totals.equity || 0, 2);
  margin.textContent = payload.status === "idle" ? "--" : formatNumber(totals.margin || 0, 2);
  availableBalance.textContent = payload.status === "idle" ? "--" : formatNumber(totals.available_balance || 0, 2);
  unrealizedPnl.textContent = payload.status === "idle" ? "--" : formatNumber(totals.unrealized_pnl || 0, 2);

  const applyMetricTone = (element, rawValue) => {
    element.classList.remove("positive", "negative", "zero");
    const value = Number(rawValue || 0);
    if (value > 0) {
      element.classList.add("positive");
    } else if (value < 0) {
      element.classList.add("negative");
    } else {
      element.classList.add("zero");
    }
  };

  if (payload.status === "idle") {
    [equity, margin, availableBalance, unrealizedPnl].forEach((element) => {
      element.classList.remove("positive", "negative", "zero");
    });
  } else {
    applyMetricTone(equity, totals.equity);
    applyMetricTone(margin, totals.margin);
    applyMetricTone(availableBalance, totals.available_balance);
    applyMetricTone(unrealizedPnl, totals.unrealized_pnl);
  }

  document.getElementById("positionsCount").textContent = String(currentPositions.length);
  const emptyNode = positionsList.querySelector(".empty-state");
  const syncCurrentModeFromAccountOverview = () => {
    maybeScheduleCurrentModePrecheck("account_update");
  };
  if (!currentPositions.length) {
    positionRowCache.forEach((row) => row.remove());
    positionRowCache.clear();
    const message = payload.status === "loading"
      ? (I18N_MESSAGES["runtime.positions_loading"] || "正在加载持仓")
      : (I18N_MESSAGES["runtime.positions_empty"] || "暂无持仓");
    const detail = resolveStructuredMessage(payload, I18N_MESSAGES["runtime.positions_empty_detail"] || "连接行情流后会在这里显示持仓。");
    const placeholder = document.createElement("div");
    placeholder.className = "empty-state";
    placeholder.style.minHeight = "220px";
    placeholder.style.marginTop = "0";
    const wrapper = document.createElement("div");
    const title = document.createElement("div");
    title.style.fontSize = "36px";
    title.style.marginBottom = "10px";
    title.textContent = copyOrDefault("console.positions_title", "持仓");
    const body = document.createElement("div");
    body.textContent = message;
    const detailNode = document.createElement("div");
    detailNode.style.marginTop = "6px";
    detailNode.style.fontSize = "13px";
    detailNode.textContent = detail;
    wrapper.appendChild(title);
    wrapper.appendChild(body);
    wrapper.appendChild(detailNode);
    placeholder.replaceChildren(wrapper);
    positionsList.replaceChildren(placeholder);
    refreshSingleOpenOrderOptions();
    refreshSingleClosePositionOptions();
    syncCurrentModeFromAccountOverview();
    return;
  }

  if (emptyNode) {
    emptyNode.remove();
  }
  const nextKeys = new Set();
  const fragment = document.createDocumentFragment();
  currentPositions.forEach((position) => {
    const key = `${position.symbol}:${position.position_side}`;
    nextKeys.add(key);
    let row = positionRowCache.get(key);
    if (!row) {
      row = document.createElement("div");
      row.className = "position-row";
      positionRowCache.set(key, row);
    }
    const sideClass = String(position.position_side || "").toLowerCase() === "short" ? "short" : "long";
    const pnlValue = Number(position.unrealized_pnl || 0);
    const pnlClass = pnlValue > 0 ? "positive" : pnlValue < 0 ? "negative" : "zero";
    const leverageText = Number(position.leverage || 0) > 0 ? `${position.leverage}x` : "--";
    const notional = Number(position.notional || 0) || ((Number(position.qty || 0) || 0) * (Number(position.entry_price || 0) || 0));
    const markPriceText = formatDisplayPrice(position.mark_price, 2);
    const liquidationPriceText = formatDisplayPrice(position.liquidation_price, 2);
    const signature = JSON.stringify([
      position.symbol,
      position.position_side,
      position.qty,
      position.entry_price,
      position.unrealized_pnl,
      notional,
      position.mark_price,
      position.liquidation_price,
      leverageText,
      pnlClass,
      sideClass,
      markPriceText,
      liquidationPriceText,
    ]);
    if (row.dataset.signature !== signature) {
      row.dataset.signature = signature;
      row.innerHTML = `
        <div class="position-row-head">
          <div class="position-symbol">${position.symbol}<span class="position-leverage-inline">${leverageText}</span></div>
          <span class="position-side ${sideClass}">${position.position_side === "SHORT" ? copyOrDefault("console.position_side.short", "空") : copyOrDefault("console.position_side.long", "多")}</span>
        </div>
        <div class="position-meta">
          <div>${copyOrDefault("console.position_fields.qty", "数量")}<strong class="mono">${formatNumber(position.qty || 0, 6)}</strong></div>
          <div>${copyOrDefault("console.position_fields.notional", "名义价值")}<strong class="mono">${formatNumber(notional, 2)}</strong></div>
          <div>${copyOrDefault("console.position_fields.entry_price", "开仓均价")}<strong class="mono">${formatNumber(position.entry_price || 0, 2)}</strong></div>
          <div>${copyOrDefault("console.position_fields.mark_price", "标记价格")}<strong class="mono">${markPriceText}</strong></div>
          <div>${copyOrDefault("console.position_fields.unrealized_pnl", "未实现盈亏")}<strong class="mono ${pnlClass}">${formatNumber(position.unrealized_pnl || 0, 4)}</strong></div>
          <div>${copyOrDefault("console.position_fields.liquidation_price", "爆仓价格")}<strong class="mono">${liquidationPriceText}</strong></div>
        </div>
      `;
    }
    fragment.appendChild(row);
  });
  positionRowCache.forEach((row, key) => {
    if (!nextKeys.has(key)) {
      row.remove();
      positionRowCache.delete(key);
    }
  });
  positionsList.replaceChildren(fragment);

  refreshSingleOpenOrderOptions();
    refreshSingleClosePositionOptions();
    syncCurrentModeFromAccountOverview();
}
function updateOpenValidationHint({ canCreate, canSimulate = true, message, tone }) {
  minNotionalHint.className = `validation-hint ${tone || ""}`;
  minNotionalHint.textContent = message;
  modeHintStateByMode.set("paired_open", { canCreate, canSimulate });
  refreshExecutionActionButtons();
}

function updateCloseValidationHint({ canCreate, message, tone }) {
  closeValidationHint.className = `validation-hint ${tone || ""}`;
  closeValidationHint.textContent = message;
  modeHintStateByMode.set("paired_close", { canCreate, canSimulate: false });
  refreshExecutionActionButtons();
}

function updateSingleOpenValidationHint({ canCreate, message, tone }) {
  singleOpenValidationHint.className = `validation-hint ${tone || ""}`;
  singleOpenValidationHint.textContent = message;
  modeHintStateByMode.set("single_open", { canCreate, canSimulate: false });
  refreshExecutionActionButtons();
}

function updateSingleCloseValidationHint({ canCreate, message, tone }) {
  singleCloseValidationHint.className = `validation-hint ${tone || ""}`;
  singleCloseValidationHint.textContent = message;
  modeHintStateByMode.set("single_close", { canCreate, canSimulate: false });
  refreshExecutionActionButtons();
}

function hasActiveExecutionSession() {
  if (activeSessionId) return true;
  if (activeSessionState && !isTerminalSession(activeSessionState.status)) return true;
  return false;
}

function refreshExecutionActionButtons() {
  const runtimeState = currentExecutionLockState();
  const activeSessionOwner = activeSessionKind();
  const locked = runtimeState.requestInFlight || runtimeState.hasActiveSession || runtimeState.hasActiveSimulation;
  setExecutionInputLock(locked);
  syncPrecheckFreshnessState("paired_open");
  syncPrecheckFreshnessState("paired_close");
  syncPrecheckFreshnessState("single_open");
  syncPrecheckFreshnessState("single_close");

  createBtn.textContent = DEFAULT_REAL_ACTION_LABELS.paired_open;
  createCloseBtn.textContent = DEFAULT_REAL_ACTION_LABELS.paired_close;
  createSingleOpenBtn.textContent = DEFAULT_REAL_ACTION_LABELS.single_open;
  createSingleCloseBtn.textContent = DEFAULT_REAL_ACTION_LABELS.single_close;
  simulateBtn.textContent = DEFAULT_SIMULATE_LABEL;

  const assignRealButtonState = (mode) => {
    const button = executionButtonForMode(mode);
    const hintState = modeHintStateByMode.get(mode) || { canCreate: false, canSimulate: false };
    const freshness = precheckFreshnessStateByMode.get(mode) || { fresh: false };
    const baseState = resolveActionAvailability(hintState, { requestInFlight: runtimeState.requestInFlight, hasActiveSession: false });
    if (runtimeState.hasActiveSession) {
      const owner = activeSessionOwner === mode;
      button.textContent = owner ? (sessionAbortInFlight ? EXECUTION_ABORTING_LABEL : EXECUTION_TERMINATE_LABEL) : DEFAULT_REAL_ACTION_LABELS[mode];
      button.disabled = owner ? sessionAbortInFlight : true;
      return;
    }
    if (runtimeState.hasActiveSimulation) {
      button.disabled = true;
      return;
    }
    button.disabled = !(baseState.canCreate && freshness.fresh);
  };

  assignRealButtonState("paired_open");
  assignRealButtonState("paired_close");
  assignRealButtonState("single_open");
  assignRealButtonState("single_close");

  const pairedOpenHint = modeHintStateByMode.get("paired_open") || { canCreate: false, canSimulate: false };
  const pairedOpenFreshness = precheckFreshnessStateByMode.get("paired_open") || { fresh: false };
  const pairedOpenAvailability = resolveActionAvailability(pairedOpenHint, {
    requestInFlight: runtimeState.requestInFlight && !runtimeState.hasActiveSimulation,
    hasActiveSession: false,
  });
  if (runtimeState.hasActiveSimulation) {
    simulateBtn.textContent = simulationAbortInFlight ? EXECUTION_ABORTING_LABEL : SIMULATION_TERMINATE_LABEL;
    simulateBtn.disabled = simulationAbortInFlight;
  } else if (runtimeState.hasActiveSession) {
    simulateBtn.disabled = true;
  } else {
    simulateBtn.disabled = !(pairedOpenAvailability.canSimulate && pairedOpenFreshness.fresh);
  }

  renderExecutionSummaryBanner();
  renderRiskBanner();
  renderRecoverableSessionBanner();
}

async function withExecutionActionLock(action) {
  executionActionInFlightCount += 1;
  refreshExecutionActionButtons();
  try {
    return await action();
  } finally {
    executionActionInFlightCount = Math.max(0, executionActionInFlightCount - 1);
    refreshExecutionActionButtons();
  }
}

function setHintStateForMode(mode, { canCreate = false, canSimulate = false, message = "", tone = "" } = {}) {
  switch (mode) {
    case "paired_close":
      updateCloseValidationHint({ canCreate, tone, message });
      break;
    case "single_open":
      updateSingleOpenValidationHint({ canCreate, tone, message });
      break;
    case "single_close":
      updateSingleCloseValidationHint({ canCreate, tone, message });
      break;
    default:
      updateOpenValidationHint({ canCreate, canSimulate, tone, message });
      break;
  }
}

function clearHintStateForMode(mode) {
  setHintStateForMode(mode, {
    canCreate: false,
    canSimulate: false,
    tone: "",
    message: "",
  });
}

function firstFailingPrecheckItem(precheck) {
  const checks = Array.isArray(precheck?.checks) ? precheck.checks : [];
  const failure = checks.find((item) => String(item.status) === "fail") || null;
  if (failure) {
    failure.message = resolveStructuredMessage(failure, unknownErrorMessage());
  }
  return failure;
}

function buildModeSuccessHint(mode, precheck) {
  const derived = precheck?.derived || {};
  switch (mode) {
    case "paired_close": {
      const maxCloseableQty = Math.min(Number(derived.long_qty || 0), Number(derived.short_qty || 0));
      const perRoundNotional = Number(derived.per_round_notional || 0);
      return `当前可双向平仓数量 ${formatNumber(maxCloseableQty, 6)}，每轮名义平仓金额 ${formatMoney(perRoundNotional)}，可以平仓。`;
    }
    case "single_open": {
      const openMode = document.getElementById("singleOpenMode")?.value || "regular";
      const selectedSide = String(derived.selected_position_side || document.getElementById("singleOpenOrder")?.value || "LONG");
      const openQty = Number(document.getElementById("singleOpenQty")?.value || 0);
      const leverage = Math.max(Number(derived.current_leverage || document.getElementById("singleOpenLeverage")?.value || 1), 1);
      const hasExistingPosition = Number(derived.long_qty || 0) > 0 || Number(derived.short_qty || 0) > 0;
      const perRoundNotional = Number(derived.per_round_notional || 0);
      if (openMode === "align") {
        return hasExistingPosition
          ? `将按订单对齐模式补齐 ${selectedSide}，数量 ${formatNumber(openQty, 6)}，当前交易对已有持仓，杠杆已锁定为 ${leverage}x。`
          : `将按订单对齐模式补齐 ${selectedSide}，数量 ${formatNumber(openQty, 6)}，当前杠杆 ${leverage}x。`;
      }
      return hasExistingPosition
        ? `将按常规模式开 ${selectedSide}，当前交易对已有持仓，杠杆已锁定为 ${leverage}x，每轮开仓金额约 ${formatMoney(perRoundNotional)}。`
        : `将按常规模式开 ${selectedSide}，当前杠杆 ${leverage}x，每轮开仓金额约 ${formatMoney(perRoundNotional)}。`;
    }
    case "single_close": {
      const closeMode = document.getElementById("singleCloseMode")?.value || "regular";
      const selectedSide = String(derived.selected_position_side || document.getElementById("singleCloseOrder")?.value || "");
      const closeQty = Number(document.getElementById("singleCloseQty")?.value || 0);
      const availableQty = selectedSide === "LONG"
        ? Number(derived.long_qty || 0)
        : selectedSide === "SHORT"
          ? Number(derived.short_qty || 0)
          : 0;
      const perRoundNotional = Number(derived.per_round_notional || 0);
      if (closeMode === "align") {
        return `订单对齐模式已锁定 ${selectedSide}，差值平仓数量 ${formatNumber(closeQty, 6)}。`;
      }
      return `当前可用持仓数量 ${formatNumber(availableQty, 6)}，每轮名义平仓金额 ${formatMoney(perRoundNotional)}。`;
    }
    default: {
      const minNotional = Number(derived.min_notional ?? currentSymbolInfo.min_notional ?? 0);
      const perRoundNotional = Number(derived.per_round_notional || 0);
      return `最小下单金额 ${formatMoney(minNotional)}，当前每轮开单金额 ${formatMoney(perRoundNotional)}，可以开单。`;
    }
  }
}
function currentSymbolPositions() {
  return currentPositions.filter((position) => position.symbol === activeSymbol && Number(position.qty || 0) > 0);
}

function resolveSymbolLeverage(symbol) {
  const matching = currentPositions.filter((position) => position.symbol === symbol && Number(position.leverage || 0) > 0);
  if (matching.length) {
    return Math.max(...matching.map((position) => Number(position.leverage || 1)));
  }
  return Math.max(Number(currentSymbolInfo.current_leverage || 1), 1);
}
function refreshSingleOpenOrderOptions() {
  const orderSelect = document.getElementById("singleOpenOrder");
  if (!orderSelect) return;
  const existingValue = orderSelect.value || "LONG";
  orderSelect.innerHTML = "";
  [
    { value: "LONG", label: "LONG | 做多开仓" },
    { value: "SHORT", label: "SHORT | 做空开仓" },
  ].forEach((item) => {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = item.label;
    orderSelect.appendChild(option);
  });
  orderSelect.value = existingValue === "SHORT" ? "SHORT" : "LONG";
  syncPositionSideTone(orderSelect);
}

function recalculateSingleOpenAmount() {
  const mode = document.getElementById("singleOpenMode")?.value || "regular";
  const orderSelect = document.getElementById("singleOpenOrder");
  const qtyInput = document.getElementById("singleOpenQty");
  const leverageInput = singleOpenLeverageInput;
  const rounds = Math.max(Number(document.getElementById("singleOpenRounds")?.value) || 1, 1);
  const positions = currentSymbolPositions();
  const longQty = positions.filter((position) => String(position.position_side) === "LONG").reduce((sum, position) => sum + Number(position.qty || 0), 0);
  const shortQty = positions.filter((position) => String(position.position_side) === "SHORT").reduce((sum, position) => sum + Number(position.qty || 0), 0);
  const hasExistingPosition = positions.length > 0;
  let selectedSide = String(orderSelect?.value || "LONG");

  if (leverageInput) {
    const currentLeverage = Math.max(resolveSymbolLeverage(activeSymbol), 1);
    const wasLocked = leverageInput.dataset.locked === "true";
    if (hasExistingPosition) {
      leverageInput.value = String(currentLeverage);
      leverageInput.disabled = true;
      leverageInput.readOnly = false;
      leverageInput.classList.add("locked-field");
      leverageInput.dataset.locked = "true";
    } else {
      leverageInput.disabled = false;
      leverageInput.readOnly = false;
      leverageInput.classList.remove("locked-field");
      if (wasLocked || Number(leverageInput.value || 0) <= 0) {
        leverageInput.value = String(currentLeverage);
      }
      leverageInput.dataset.locked = "false";
    }
  }
  const leverage = Math.max(Number(leverageInput?.value || 1), 1);

  if (mode === "align") {
    if (longQty === shortQty) {
      selectedSide = longQty <= shortQty ? "LONG" : "SHORT";
      if (orderSelect) {
        orderSelect.value = selectedSide;
        orderSelect.disabled = true;
        syncPositionSideTone(orderSelect);
      }
      if (qtyInput) {
        qtyInput.disabled = true;
        qtyInput.value = "0";
      }
      document.getElementById("singleOpenRoundQty").value = "0";
      document.getElementById("singleOpenMarginPerRound").textContent = formatMoney(0);
      document.getElementById("singleOpenTotalNotional").textContent = formatMoney(0);
      document.getElementById("singleOpenNotionalPerRound").textContent = formatMoney(0);
      if (executionMode === "single_open") {
        refreshDerivedStats({ totalNotional: 0, perRoundNotional: 0, estimatedQty: 0 });
      }
      updateSingleOpenValidationHint({ canCreate: false, tone: "error", message: "当前双边持仓数量已对齐，无需单向开仓。" });
      return;
    }
    selectedSide = longQty < shortQty ? "LONG" : "SHORT";
    if (orderSelect) {
      orderSelect.value = selectedSide;
      orderSelect.disabled = true;
      syncPositionSideTone(orderSelect);
    }
    if (qtyInput) {
      qtyInput.value = Math.abs(longQty - shortQty).toFixed(6);
      qtyInput.disabled = true;
    }
  } else {
    if (orderSelect) {
      orderSelect.disabled = false;
      syncPositionSideTone(orderSelect);
    }
    if (qtyInput) qtyInput.disabled = false;
  }

  const openQty = Number(qtyInput?.value || 0);
  const perRoundQty = openQty / rounds;
  const totalNotional = openQty * latestReferencePrice;
  const perRoundNotional = perRoundQty * latestReferencePrice;
  const impliedOpenAmount = leverage > 0 ? totalNotional / leverage : totalNotional;
  const marginPerRound = rounds > 0 ? impliedOpenAmount / rounds : 0;
  const deferHintToPrecheck = symbolInfoReady && canRunPrecheck("single_open", buildPrecheckPayload("single_open"));

  document.getElementById("singleOpenRoundQty").value = perRoundQty > 0 ? perRoundQty.toFixed(6) : "0";
  document.getElementById("singleOpenMarginPerRound").textContent = formatMoney(marginPerRound);
  document.getElementById("singleOpenTotalNotional").textContent = formatMoney(totalNotional);
  document.getElementById("singleOpenNotionalPerRound").textContent = formatMoney(perRoundNotional);
  if (executionMode === "single_open") {
    refreshDerivedStats({ totalNotional, perRoundNotional, estimatedQty: perRoundQty });
  }

  if (!deferHintToPrecheck) {
    latestPrecheckResultByMode.delete("single_open");
    latestResolvedPrecheckPayloadByMode.delete("single_open");
    clearHintStateForMode("single_open");
    return;
  }
}
function refreshSingleClosePositionOptions() {  const orderSelect = document.getElementById("singleCloseOrder");
  if (!orderSelect) return;
  const baseAsset = inferBaseAsset(activeSymbol);
  const positions = currentSymbolPositions();
  const existingValue = orderSelect.value;
  orderSelect.innerHTML = "";
  positions.forEach((position) => {
    const option = document.createElement("option");
    option.value = String(position.position_side || "");
    option.textContent = `${position.position_side} | ${formatNumber(position.qty || 0, 6)} ${baseAsset}`;
    orderSelect.appendChild(option);
  });
  if (!positions.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "当前交易对没有持仓单";
    orderSelect.appendChild(option);
    orderSelect.value = "";
    orderSelect.disabled = true;
    syncPositionSideTone(orderSelect);
    return;
  }
  orderSelect.disabled = false;
  if (positions.some((position) => String(position.position_side) === existingValue)) {
    orderSelect.value = existingValue;
  } else {
    orderSelect.value = String(positions[0].position_side || "");
  }
  syncPositionSideTone(orderSelect);
}

function recalculateSingleCloseAmount() {
  const mode = document.getElementById("singleCloseMode")?.value || "regular";
  const orderSelect = document.getElementById("singleCloseOrder");
  const qtyInput = document.getElementById("singleCloseQty");
  const rounds = Math.max(Number(document.getElementById("singleCloseRounds")?.value) || 1, 1);
  const positions = currentSymbolPositions();
  const longQty = positions.filter((position) => String(position.position_side) === "LONG").reduce((sum, position) => sum + Number(position.qty || 0), 0);
  const shortQty = positions.filter((position) => String(position.position_side) === "SHORT").reduce((sum, position) => sum + Number(position.qty || 0), 0);
  let selectedSide = String(orderSelect?.value || "");
  let availableQty = positions.filter((position) => String(position.position_side) === selectedSide).reduce((sum, position) => sum + Number(position.qty || 0), 0);

  if (!positions.length) {
    if (orderSelect) {
      orderSelect.disabled = true;
      orderSelect.value = "";
      syncPositionSideTone(orderSelect);
    }
    if (qtyInput) {
      qtyInput.disabled = true;
      qtyInput.value = "0";
    }
    document.getElementById("singleCloseRoundQty").value = "0";
    document.getElementById("singleCloseAvailableQty").textContent = formatNumber(0, 6);
    document.getElementById("singleCloseTotalNotional").textContent = formatMoney(0);
    document.getElementById("singleCloseNotionalPerRound").textContent = formatMoney(0);
    if (executionMode === "single_close") {
      refreshDerivedStats({ totalNotional: 0, perRoundNotional: 0, estimatedQty: 0 });
    }
    updateSingleCloseValidationHint({ canCreate: false, tone: "error", message: "当前交易对不存在持仓" });
    return;
  }

  if (mode === "align") {
    if (longQty === shortQty) {
      if (orderSelect) orderSelect.disabled = true;
      if (qtyInput) qtyInput.disabled = true;
      if (qtyInput) qtyInput.value = "0";
      document.getElementById("singleCloseRoundQty").value = "0";
      document.getElementById("singleCloseAvailableQty").textContent = formatNumber(0, 6);
      document.getElementById("singleCloseTotalNotional").textContent = formatMoney(0);
      document.getElementById("singleCloseNotionalPerRound").textContent = formatMoney(0);
      if (executionMode === "single_close") {
        refreshDerivedStats({ totalNotional: 0, perRoundNotional: 0, estimatedQty: 0 });
      }
      updateSingleCloseValidationHint({ canCreate: false, tone: "error", message: "当前双边持仓数量已对齐，无需单向平仓。" });
      return;
    }
    selectedSide = longQty > shortQty ? "LONG" : "SHORT";
    availableQty = Math.max(longQty, shortQty);
    if (orderSelect) {
      orderSelect.value = selectedSide;
      orderSelect.disabled = true;
      syncPositionSideTone(orderSelect);
    }
    if (qtyInput) {
      qtyInput.value = Math.abs(longQty - shortQty).toFixed(6);
      qtyInput.disabled = true;
    }
  } else {
    if (orderSelect) {
      orderSelect.disabled = positions.length === 0;
      syncPositionSideTone(orderSelect);
    }
    if (qtyInput) qtyInput.disabled = false;
  }

  const closeQty = Number(qtyInput?.value || 0);
  const perRoundQty = closeQty / rounds;
  const totalNotional = closeQty * latestReferencePrice;
  const perRoundNotional = perRoundQty * latestReferencePrice;
  document.getElementById("singleCloseRoundQty").value = perRoundQty > 0 ? perRoundQty.toFixed(6) : "0";
  document.getElementById("singleCloseAvailableQty").textContent = formatNumber(availableQty, 6);
  document.getElementById("singleCloseTotalNotional").textContent = formatMoney(totalNotional);
  document.getElementById("singleCloseNotionalPerRound").textContent = formatMoney(perRoundNotional);
  if (executionMode === "single_close") {
    refreshDerivedStats({ totalNotional, perRoundNotional, estimatedQty: perRoundQty });
  }

  const deferHintToPrecheck = symbolInfoReady && canRunPrecheck("single_close", buildPrecheckPayload("single_close"));
  if (!deferHintToPrecheck) {
    latestPrecheckResultByMode.delete("single_close");
    latestResolvedPrecheckPayloadByMode.delete("single_close");
    clearHintStateForMode("single_close");
    return;
  }
}
function recalculateOpenAmount() {
  const margin = Number(document.getElementById("calcMargin").value) || 0;
  const leverage = Number(document.getElementById("leverage").value) || 0;
  const rounds = Math.max(Number(document.getElementById("calcRounds").value) || 1, 1);
  const marginPerRound = margin / rounds;
  const totalNotional = margin * leverage;
  const notionalPerRound = totalNotional / rounds;
  const roundQty = latestReferencePrice > 0 ? notionalPerRound / latestReferencePrice : 0;
  const deferHintToPrecheck = symbolInfoReady && canRunPrecheck("paired_open", buildPrecheckPayload("paired_open"));

  document.getElementById("marginPerRound").textContent = formatMoney(marginPerRound);
  document.getElementById("totalNotional").textContent = formatMoney(totalNotional);
  document.getElementById("notionalPerRound").textContent = formatMoney(notionalPerRound);
  document.getElementById("roundQty").value = roundQty > 0 ? roundQty.toFixed(6) : "0";
  if (executionMode === "paired_open") {
    refreshDerivedStats({ totalNotional, perRoundNotional: notionalPerRound, estimatedQty: roundQty });
  }
  document.getElementById("statTotalNotional").textContent = formatMoney(totalNotional);
  document.getElementById("statPerRound").textContent = formatMoney(notionalPerRound);
  document.getElementById("statLastQty").textContent = formatNumber(roundQty, 8);

  if (!deferHintToPrecheck) {
    latestPrecheckResultByMode.delete("paired_open");
    latestResolvedPrecheckPayloadByMode.delete("paired_open");
    clearHintStateForMode("paired_open");
    return;
  }
}
function recalculateCloseAmount() {
  const closeQty = Number(document.getElementById("closeQty").value) || 0;
  const rounds = Math.max(Number(document.getElementById("closeRounds").value) || 1, 1);
  const perRoundQty = closeQty / rounds;
  const totalNotional = closeQty * latestReferencePrice;
  const perRoundNotional = perRoundQty * latestReferencePrice;
  const maxCloseableQty = maxCloseableQtyForSymbol(activeSymbol);

  document.getElementById("closeRoundQty").value = perRoundQty > 0 ? perRoundQty.toFixed(6) : "0";
  document.getElementById("closeTotalNotional").textContent = formatMoney(totalNotional);
  document.getElementById("closeNotionalPerRound").textContent = formatMoney(perRoundNotional);
  document.getElementById("maxCloseableQty").textContent = formatNumber(maxCloseableQty, 6);
  if (executionMode === "paired_close") {
    refreshDerivedStats({ totalNotional, perRoundNotional, estimatedQty: perRoundQty });
  }

  const deferHintToPrecheck = symbolInfoReady && canRunPrecheck("paired_close", buildPrecheckPayload("paired_close"));
  if (!deferHintToPrecheck) {
    latestPrecheckResultByMode.delete("paired_close");
    latestResolvedPrecheckPayloadByMode.delete("paired_close");
    clearHintStateForMode("paired_close");
    return;
  }
}
function summarizeSessionEvent(event) {
  const payload = event.payload || {};
  const registryEntry = I18N_REGISTRIES.events?.[event.event_type];
  if (registryEntry?.key) {
    const params = {
      ...payload,
      final_alignment_status: payload.final_alignment_status ? formatAlignmentStatus(payload.final_alignment_status) : payload.final_alignment_status,
    };
    return {
      level: registryEntry.level || "info",
      message: formatCopy(registryEntry.key, params),
    };
  }
  switch (event.event_type) {
    case "session_created":
      return { level: "info", message: `\u771f\u5b9e\u5f00\u4ed3\u4f1a\u8bdd\u5df2\u521b\u5efa\uff1a ${payload.symbol} | ${payload.trend_bias} | ${payload.round_count} 轮` };
    case "session_preflight_failed":
      return { level: "error", message: `\u771f\u5b9e\u5f00\u4ed3\u9884\u68c0\u5931\u8d25\uff1a ${payload.error || "未知错误"}` };
    case "round_started":
      return { level: "info", message: `第 ${payload.round_index} 轮开始执行开仓` };
    case "stage1_fill":
    case "stage1_late_fill":
      return { level: "success", message: `第 ${payload.round_index} 轮 \u9636\u6bb51 成交 ${payload.filled_qty}` };
    case "stage2_fill":
    case "stage2_late_fill":
      return { level: "success", message: `第 ${payload.round_index} 轮 \u9636\u6bb52 成交 ${payload.filled_qty}，剩余 ${payload.remaining_qty}` };
    case "stage2_zero_fill_retry":
      return { level: "warn", message: `第 ${payload.round_index} 轮 \u9636\u6bb52 零成交重试，第 ${payload.retry} 次` };
    case "stage2_below_min_carryover":
      return { level: "warn", message: `第 ${payload.round_index} 轮 \u9636\u6bb52 剩余 ${payload.remaining_qty} 金额低于最小下单金额，未完成数量顺延到下一轮` };
    case "stage2_carryover_persisted":
      return { level: "warn", message: `第 ${payload.round_index} 轮保留未完成数量 ${payload.carryover_qty}` };
    case "round_completed":
      return { level: "success", message: `第 ${payload.round_index} 轮开仓完成，未完成数量 ${payload.stage2_remaining_qty || "0"}` };
    case "round_skipped":
      return { level: "warn", message: `第 ${payload.round_index} 轮因 \u9636\u6bb51 连续零成交被跳过` };
    case "final_alignment_started":
      return { level: "warn", message: `开始最终市价对齐，当前未完成数量 ${payload.carryover_qty}` };
    case "final_alignment_market_reduce":
      return { level: "warn", message: `最终对齐减仓 ${payload.position_side} ${payload.qty}` };
    case "final_alignment_flatten_both_sides":
      return { level: "warn", message: "少侧不足最小减仓量，双边市价清仓对齐" };
    case "final_alignment_completed":
      return { level: "success", message: `\u6700\u7ec8\u5bf9\u9f50\u5b8c\u6210\uff1a ${payload.mode || "完成"}` };
    case "final_alignment_failed":
      return { level: "error", message: `最终对齐失败: ${payload.error || "未知错误"}` };
    case "session_completed":
      return { level: "success", message: `真实开仓会话完成，最终对齐结果 ${formatAlignmentStatus(payload.final_alignment_status)}` };
    case "session_failed":
      return { level: "error", message: `\u771f\u5b9e\u5f00\u4ed3\u4f1a\u8bdd\u5931\u8d25\uff1a ${payload.error || "未知错误"}` };
    case "close_session_created":
      return { level: "info", message: `\u771f\u5b9e\u53cc\u5411\u5e73\u4ed3\u4f1a\u8bdd\u5df2\u521b\u5efa\uff1a ${payload.symbol} | ${payload.trend_bias} | ${payload.round_count} 轮 | 数量 ${payload.close_qty}` };
    case "close_session_preflight_failed":
      return { level: "error", message: `\u771f\u5b9e\u53cc\u5411\u5e73\u4ed3\u9884\u68c0\u5931\u8d25\uff1a ${payload.error || "未知错误"}` };
    case "close_round_started":
      return { level: "info", message: `第 ${payload.round_index} 轮开始执行双向平仓` };
    case "close_round_skipped":
      return { level: "warn", message: `第 ${payload.round_index} 轮无可双向平仓持仓，已跳过` };
    case "close_stage1_fill":
    case "close_stage1_late_fill":
      return { level: "success", message: `第 ${payload.round_index} 轮 \u9636\u6bb51 平仓成交 ${payload.filled_qty}` };
    case "close_stage2_fill":
    case "close_stage2_late_fill":
      return { level: "success", message: `第 ${payload.round_index} 轮 \u9636\u6bb52 平仓成交 ${payload.filled_qty}` };
    case "close_stage2_zero_fill_retry":
      return { level: "warn", message: `第 ${payload.round_index} 轮 \u9636\u6bb52 平仓零成交重试，第 ${payload.retry} 次` };
    case "close_round_completed":
      return { level: "success", message: `第 ${payload.round_index} 轮双向平仓完成` };
    case "close_round_interval_wait":
      return { level: "info", message: `等待 ${payload.wait_seconds} 秒后进入下一轮双向平仓` };
    case "close_session_completed":
      return { level: "success", message: "真实双向平仓会话完成" };
    case "close_session_failed":
      return { level: "error", message: `\u771f\u5b9e\u53cc\u5411\u5e73\u4ed3\u4f1a\u8bdd\u5931\u8d25\uff1a ${payload.error || "未知错误"}` };
    case "single_open_session_created":
      return { level: "info", message: `\u771f\u5b9e\u5355\u5411\u5f00\u4ed3\u4f1a\u8bdd\u5df2\u521b\u5efa\uff1a ${payload.symbol} | ${payload.selected_position_side} | ${payload.round_count} 轮 | 数量 ${payload.open_qty}` };
    case "single_open_session_preflight_failed":
      return { level: "error", message: `\u771f\u5b9e\u5355\u5411\u5f00\u4ed3\u9884\u68c0\u5931\u8d25\uff1a ${payload.error || "未知错误"}` };
    case "single_open_round_started":
      return { level: "info", message: payload.is_extension_round ? `开始执行第 ${payload.extension_round_index || 1} 个单向开仓补充轮` : `第 ${payload.round_index} 轮开始执行单向开仓` };
    case "single_open_fill":
    case "single_open_late_fill":
      return { level: "success", message: `第 ${payload.round_index} 轮单向开仓成交 ${payload.filled_qty}` };
    case "single_open_zero_fill_retry":
      return { level: "warn", message: `第 ${payload.round_index} 轮单向开仓零成交重试，第 ${payload.retry} 次` };
    case "single_open_round_completed":
      return { level: "success", message: payload.is_extension_round ? `单向开仓补充轮完成，计划量 ${payload.planned_qty || payload.filled_qty || "0"}` : `第 ${payload.round_index} 轮单向开仓完成` };
    case "single_open_round_skipped":
      return { level: "warn", message: `第 ${payload.round_index} 轮单向开仓已跳过` };
    case "single_open_round_interval_wait":
      return { level: "info", message: `等待 ${payload.wait_seconds} 秒后进入下一轮单向开仓` };
    case "single_open_market_fallback":
      return { level: "warn", message: `第 ${payload.round_index} 轮单向开仓已转市价补单 ${payload.filled_qty}` };
    case "single_open_session_completed":
      return { level: "success", message: "真实单向开仓会话完成" };
    case "single_open_session_failed":
      return { level: "error", message: `\u771f\u5b9e\u5355\u5411\u5f00\u4ed3\u4f1a\u8bdd\u5931\u8d25\uff1a ${payload.error || "未知错误"}` };
    case "single_close_session_created":      return { level: "info", message: `\u771f\u5b9e\u5355\u5411\u5e73\u4ed3\u4f1a\u8bdd\u5df2\u521b\u5efa\uff1a ${payload.symbol} | ${payload.selected_position_side} | ${payload.round_count} 轮 | 数量 ${payload.close_qty}` };
    case "single_close_session_preflight_failed":
      return { level: "error", message: `\u771f\u5b9e\u5355\u5411\u5e73\u4ed3\u9884\u68c0\u5931\u8d25\uff1a ${payload.error || "未知错误"}` };
    case "single_close_round_started":
      return { level: "info", message: payload.is_extension_round ? `开始执行第 ${payload.extension_round_index || 1} 个单向平仓补充轮` : `第 ${payload.round_index} 轮开始执行单向平仓` };
    case "single_close_fill":
    case "single_close_late_fill":
      return { level: "success", message: `第 ${payload.round_index} 轮单向平仓成交 ${payload.filled_qty}` };
    case "single_close_zero_fill_retry":
      return { level: "warn", message: `第 ${payload.round_index} 轮单向平仓零成交重试，第 ${payload.retry} 次` };
    case "single_close_round_completed":
      return { level: "success", message: payload.is_extension_round ? `单向平仓补充轮完成，计划量 ${payload.planned_qty || payload.filled_qty || "0"}` : `第 ${payload.round_index} 轮单向平仓完成` };
    case "single_close_round_skipped":
      return { level: "warn", message: `第 ${payload.round_index} 轮无可平持仓，已跳过` };
    case "single_close_round_interval_wait":
      return { level: "info", message: `等待 ${payload.wait_seconds} 秒后进入下一轮单向平仓` };
    case "single_close_market_fallback":
      return { level: "warn", message: `第 ${payload.round_index} 轮单向平仓已转市价补单 ${payload.filled_qty}` };
    case "single_close_session_completed":
      return { level: "success", message: "真实单向平仓会话完成" };
    case "single_close_session_failed":
      return { level: "error", message: `\u771f\u5b9e\u5355\u5411\u5e73\u4ed3\u4f1a\u8bdd\u5931\u8d25\uff1a ${payload.error || "未知错误"}` };
    case "quote_stale":
      return { level: "warn", message: `第 ${payload.round_index} 轮报价已过期，已停止继续执行` };
    case "extension_round_conflict_detected":
      return { level: "warn", message: `补充轮前检测到${payload.order_class === "manual" ? "人工" : "系统"}挂单冲突，已停止继续执行` };
    default:
      return null;
  }
}

function renderSessionEvents(events) {
  (events || []).forEach((event) => {
    if (seenSessionEventIds.has(event.event_id)) return;
    seenSessionEventIds.add(event.event_id);
    const summary = summarizeSessionEvent(event);
    if (!summary) return;
    appendLog(summary.level, summary.message, event.created_at, { trustedMessage: true });
  });
}

function updateRealSessionStats(session) {
  activeSessionState = session;
  if (isTerminalSession(session.status)) {
    sessionAbortInFlight = false;
  }
  const regularTerminalRounds = Array.isArray(session.rounds)
    ? session.rounds.filter((round) => {
      const status = String(round.status || "");
      const isExtensionRound = Boolean(round?.notes?.is_extension_round);
      return ["round_completed", "stage1_skipped"].includes(status) && !isExtensionRound;
    }).length
    : 0;
  const latestRound = Array.isArray(session.rounds) && session.rounds.length
    ? session.rounds[session.rounds.length - 1]
    : null;
  const currentPlannedQty = latestRound?.notes?.current_planned_qty ?? session.round_qty ?? 0;
  document.getElementById("statSessionStatus").textContent = session.status || "idle";
  document.getElementById("statMode").textContent = formatModeLabel(session.session_kind || executionMode);
  document.getElementById("statRounds").textContent = `${regularTerminalRounds} / ${session.round_count || 0}`;
  document.getElementById("statCarryoverQty").textContent = formatNumber(resolveResidualQty(session), 6);
  document.getElementById("statFinalAlignment").textContent = formatAlignmentStatus(session.final_alignment_status);
  document.getElementById("statLastQty").textContent = formatNumber(currentPlannedQty || 0, 8);
  document.getElementById("statResidualSide").textContent = latestResidualSideLabel || "--";
  updateAbortStateLabel(session.status, sessionAbortInFlight);
  updateExecutionSummary(buildExecutionSummary(session, {
    roundsCompleted: regularTerminalRounds,
    roundsTotal: session.round_count || 0,
    abortRequested: sessionAbortInFlight || session.status === "aborting",
  }));
  refreshExecutionActionButtons();
}

function stopSessionPolling(clearSessionId = true) {
  if (activeSessionPoller) {
    clearInterval(activeSessionPoller);
    activeSessionPoller = null;
  }
  if (clearSessionId) {
    activeSessionId = null;
    activeSessionState = null;
    latestSessionEventId = 0;
    sessionAbortInFlight = false;
    if (!hasActiveSimulationRun()) {
      updateExecutionSummary(latestExecutionStatsState && !isTerminalSimulationStatus(latestExecutionStatsState.status)
        ? buildExecutionSummary(latestExecutionStatsState, { abortRequested: simulationAbortInFlight || latestExecutionStatsState.status === "aborting" })
        : null);
    }
  }
  refreshExecutionActionButtons();
}

function mergeChangedRounds(existingRounds, changedRounds) {
  const rounds = Array.isArray(existingRounds) ? [...existingRounds] : [];
  const byIndex = new Map(rounds.map((round) => [Number(round.round_index || 0), round]));
  (changedRounds || []).forEach((round) => {
    byIndex.set(Number(round.round_index || 0), round);
  });
  return [...byIndex.entries()]
    .sort((left, right) => left[0] - right[0])
    .map((entry) => entry[1]);
}

async function loadActiveSessionSnapshot() {
  if (!activeSessionId) return;
  const session = await request(`/sessions/${encodeURIComponent(activeSessionId)}`);
  activeSessionState = session;
  latestSessionEventId = Array.isArray(session.events)
    ? session.events.reduce((maxId, event) => Math.max(maxId, Number(event.event_id || 0)), 0)
    : 0;
  updateRealSessionStats(session);
  renderSessionEvents(session.events || []);
  if (isTerminalSession(session.status)) {
    stopSessionPolling();
  }
}

async function pollActiveSession() {
  if (!activeSessionId) return;
  try {
    if (!activeSessionState) {
      await loadActiveSessionSnapshot();
      return;
    }
    const payload = await request(`/sessions/${encodeURIComponent(activeSessionId)}/updates?after_event_id=${latestSessionEventId}`);
    activeSessionState = {
      ...activeSessionState,
      ...(payload.session || {}),
      rounds: mergeChangedRounds(activeSessionState.rounds || [], payload.changed_rounds || []),
    };
    latestSessionEventId = Math.max(latestSessionEventId, Number(payload.latest_event_id || 0));
    updateRealSessionStats(activeSessionState);
    renderSessionEvents(payload.events || []);
    if (isTerminalSession(activeSessionState.status)) {
      stopSessionPolling();
    }
  } catch (error) {
    try {
      await loadActiveSessionSnapshot();
    } catch (fallbackError) {
      appendLog("error", "", undefined, {
        messageCode: "runtime.session_refresh_failed",
        messageParams: { error: userVisibleErrorMessage(fallbackError || error) },
      });
      stopSessionPolling();
    }
  }
}

function startSessionPolling(sessionId) {
  stopSessionPolling(false);
  activeSessionId = sessionId;
  activeSessionState = null;
  latestSessionEventId = 0;
  seenSessionEventIds.clear();
  recoverableSessionState = null;
  renderRecoverableSessionBanner();
  refreshExecutionActionButtons();
  loadActiveSessionSnapshot().catch((error) => {
    appendLog("error", "", undefined, {
      messageCode: "runtime.session_refresh_failed",
      messageParams: { error: userVisibleErrorMessage(error) },
    });
    stopSessionPolling();
  });
  activeSessionPoller = setInterval(pollActiveSession, 2000);
}

function buildRealExecutionConfirmation(mode) {
  const normalizedMode = normalizeSessionKind(mode);
  switch (normalizedMode) {
    case "paired_close":
      return [
        copyOrDefault("runtime.real_execution_confirm_title", "确认发起实盘执行？"),
        `${copyOrDefault("runtime.confirm_symbol", "交易对")}：${closeExecutionSymbol.value}`,
        `${copyOrDefault("runtime.confirm_mode", "模式")}：${formatModeLabel(normalizedMode)}`,
        `${copyOrDefault("runtime.confirm_direction", "方向")}：${document.getElementById("closeTrend").value}`,
        `${copyOrDefault("runtime.confirm_total_qty", "预计总数量")}：${document.getElementById("closeQty").value || "0"}`,
        `${copyOrDefault("runtime.confirm_rounds", "轮数")}：${document.getElementById("closeRounds").value || "0"}`,
        `${copyOrDefault("runtime.confirm_profile", "执行策略")}：${copyOrDefault("console.execution_profile.default", "默认")}`,
      ].join("\n");
    case "single_open":
      return [
        copyOrDefault("runtime.real_execution_confirm_title", "确认发起实盘执行？"),
        `${copyOrDefault("runtime.confirm_symbol", "交易对")}：${document.getElementById("singleOpenExecutionSymbol").value}`,
        `${copyOrDefault("runtime.confirm_mode", "模式")}：${formatModeLabel(normalizedMode)}`,
        `${copyOrDefault("runtime.confirm_direction", "方向")}：${document.getElementById("singleOpenOrder").value || "--"}`,
        `${copyOrDefault("runtime.confirm_total_qty", "预计总数量")}：${document.getElementById("singleOpenQty").value || "0"}`,
        `${copyOrDefault("runtime.confirm_rounds", "轮数")}：${document.getElementById("singleOpenRounds").value || "0"}`,
        `${copyOrDefault("runtime.confirm_leverage", "杠杆")}：${document.getElementById("singleOpenLeverage").value || "1"}x`,
        `${copyOrDefault("runtime.confirm_profile", "执行策略")}：${copyOrDefault("console.execution_profile.default", "默认")}`,
      ].join("\n");
    case "single_close":
      return [
        copyOrDefault("runtime.real_execution_confirm_title", "确认发起实盘执行？"),
        `${copyOrDefault("runtime.confirm_symbol", "交易对")}：${document.getElementById("singleCloseExecutionSymbol").value}`,
        `${copyOrDefault("runtime.confirm_mode", "模式")}：${formatModeLabel(normalizedMode)}`,
        `${copyOrDefault("runtime.confirm_direction", "方向")}：${document.getElementById("singleCloseOrder").value || "--"}`,
        `${copyOrDefault("runtime.confirm_total_qty", "预计总数量")}：${document.getElementById("singleCloseQty").value || "0"}`,
        `${copyOrDefault("runtime.confirm_rounds", "轮数")}：${document.getElementById("singleCloseRounds").value || "0"}`,
        `${copyOrDefault("runtime.confirm_profile", "执行策略")}：${copyOrDefault("console.execution_profile.default", "默认")}`,
      ].join("\n");
    default:
      return [
        copyOrDefault("runtime.real_execution_confirm_title", "确认发起实盘执行？"),
        `${copyOrDefault("runtime.confirm_symbol", "交易对")}：${executionSymbol.value}`,
        `${copyOrDefault("runtime.confirm_mode", "模式")}：${formatModeLabel(normalizedMode)}`,
        `${copyOrDefault("runtime.confirm_direction", "方向")}：${document.getElementById("trend").value}`,
        `${copyOrDefault("runtime.confirm_total_qty", "预计总数量")}：${document.getElementById("roundQty").value || "0"}`,
        `${copyOrDefault("runtime.confirm_rounds", "轮数")}：${document.getElementById("calcRounds").value || "0"}`,
        `${copyOrDefault("runtime.confirm_leverage", "杠杆")}：${document.getElementById("leverage").value || "1"}x`,
        `${copyOrDefault("runtime.confirm_profile", "执行策略")}：${copyOrDefault("console.execution_profile.default", "默认")}`,
      ].join("\n");
  }
}

function confirmSimulationAbort() {
  return window.confirm(copyOrDefault("runtime.simulation_abort_confirm", "确认终止当前模拟吗？"));
}

function confirmSessionAbort() {
  return window.confirm(
    copyOrDefault(
      "runtime.session_abort_confirm",
      "确认终止当前实盘执行吗？系统只会停止后续轮次，不会回滚已经发出的订单。",
    ),
  );
}

async function requestSimulationAbort() {
  if (simulationAbortInFlight) return;
  if (!confirmSimulationAbort()) return;
  simulationAbortInFlight = true;
  refreshExecutionActionButtons();
  try {
    const payload = await request("/simulation/abort", { method: "POST" });
    appendLog(payload.requested ? "warn" : "info", "", undefined, {
      messageCode: payload.message_code,
      messageParams: payload.message_params,
    });
  } catch (error) {
    appendLog("error", "", undefined, {
      messageCode: "runtime.simulation_run_failed",
      messageParams: { error: userVisibleErrorMessage(error) },
    });
    simulationAbortInFlight = false;
  } finally {
    refreshExecutionActionButtons();
  }
}

async function requestSessionAbort() {
  if (sessionAbortInFlight || !activeSessionId) return;
  if (!confirmSessionAbort()) return;
  sessionAbortInFlight = true;
  refreshExecutionActionButtons();
  try {
    const payload = await request(`/sessions/${encodeURIComponent(activeSessionId)}/abort`, { method: "POST" });
    appendLog("warn", "", undefined, {
      messageCode: payload.message_code || "runtime.session_abort_requested",
      messageParams: payload.message_params,
    });
  } catch (error) {
    appendLog("error", "", undefined, {
      messageCode: "runtime.session_create_failed",
      messageParams: { error: userVisibleErrorMessage(error) },
    });
    sessionAbortInFlight = false;
  } finally {
    refreshExecutionActionButtons();
  }
}

async function detectRecoverableSession() {
  try {
    const sessions = await request("/sessions");
    recoverableSessionState = selectRecoverableSession(sessions || []);
    recoverableSessionDismissed = false;
    renderRecoverableSessionBanner();
  } catch (error) {
    appendLog("error", "", undefined, {
      messageCode: "runtime.session_restore_failed",
      messageParams: { error: userVisibleErrorMessage(error) },
    });
  }
}

async function restoreRecoverableSession() {
  if (!recoverableSessionState) return;
  const session = recoverableSessionState;
  try {
    setExecutionMode(normalizeSessionKind(session.session_kind));
    if (String(session.status) === "exception") {
      const payload = await request(`/sessions/${encodeURIComponent(session.session_id)}/resume`, { method: "POST" });
      appendLog("success", "", undefined, {
        messageCode: payload.message_code || "runtime.session_resume_requested",
        messageParams: payload.message_params,
      });
    } else {
      appendLog("info", "", undefined, {
        messageCode: "runtime.session_monitor_restored",
        messageParams: { session_id: session.session_id },
      });
    }
    startSessionPolling(session.session_id);
    recoverableSessionState = null;
    renderRecoverableSessionBanner();
  } catch (error) {
    appendLog("error", "", undefined, {
      messageCode: "runtime.session_restore_failed",
      messageParams: { error: userVisibleErrorMessage(error) },
    });
  }
}

async function refreshSymbolInfo(symbol, { applyState = true } = {}) {
  const symbolInfo = await request(`/symbols/${encodeURIComponent(symbol)}`);
  if (applyState) {
    setSymbolInfo(symbolInfo);
  }
  return symbolInfo;
}

function openSse() {
  if (eventSource) return;
  eventSource = new EventSource("/stream/events");
  eventSource.addEventListener("connection_status", (event) => {
    const payload = JSON.parse(event.data);
    setConnectionState(payload);
    document.getElementById("streamClock").textContent = nowTime();
  });
  eventSource.addEventListener("orderbook", (event) => {
    pendingOrderbookPayload = JSON.parse(event.data);
    queueUiRender();
  });
  eventSource.addEventListener("execution_log", (event) => {
    pendingLogEntries.push(JSON.parse(event.data));
    queueUiRender();
  });
  eventSource.addEventListener("execution_stats", (event) => {
    const payload = JSON.parse(event.data);
    updateExecutionStats(payload);
  });
  eventSource.addEventListener("account_overview", (event) => {
    pendingAccountOverviewPayload = JSON.parse(event.data);
    queueUiRender();
  });
  eventSource.onerror = () => {
    document.getElementById("streamClock").textContent = nowTime();
  };
}

function closeSse() {
  if (eventSource) {
    eventSource.close();
    eventSource = null;
  }
}

async function switchSymbol(nextSymbol, shouldReconnect = connectionToggle.checked) {
  const targetSymbol = normalizeSymbol(nextSymbol);
  if (!targetSymbol) {
    rebuildSymbolOptions(activeSymbol);
    appendLog("warn", "", undefined, { messageCode: "runtime.invalid_symbol_input" });
    return false;
  }
  if (targetSymbol === activeSymbol) {
    rebuildSymbolOptions(activeSymbol);
    return true;
  }
  const previousSymbol = activeSymbol;
  const previousTemporaryCustomSymbol = temporaryCustomSymbol;
  const previousSymbolInfo = { ...currentSymbolInfo };
  try {
    const symbolInfo = await refreshSymbolInfo(targetSymbol, { applyState: false });
    temporaryCustomSymbol = symbolInfo.allowed ? null : targetSymbol;
    applySymbolContext(targetSymbol, symbolInfo, { syncInput: true });
    if (shouldReconnect) {
      openSse();
      await request("/market/connect", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ symbol: targetSymbol })
      });
    }
    appendLog("info", "", undefined, { messageCode: "runtime.symbol_switched", messageParams: { symbol: targetSymbol } });
    if (symbolInfo.allowed === false) {
      appendLog("warn", "", undefined, { messageCode: "runtime.symbol_not_whitelisted", messageParams: { symbol: targetSymbol } });
    }
    return true;
  } catch (error) {
    temporaryCustomSymbol = previousTemporaryCustomSymbol;
    applySymbolContext(previousSymbol, previousSymbolInfo, { syncInput: true });
    appendLog("error", "", undefined, {
      messageCode: "runtime.symbol_switch_failed",
      messageParams: { symbol: targetSymbol, error: userVisibleErrorMessage(error) },
    });
    return false;
  }
}

connectionToggle.addEventListener("change", async (event) => {
  const symbol = executionSymbol.value || activeSymbol || "BTCUSDT";
  try {
    if (event.target.checked) {
      await refreshSymbolInfo(symbol);
      openSse();
      await request("/market/connect", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ symbol })
      });
    } else {
      await request("/market/disconnect", { method: "POST" });
      closeSse();
      setConnectionState({
        connected: false,
        status: "disconnected",
        symbol,
        account_id: currentAccount.id,
        account_name: currentAccount.name,
        message_code: "runtime.connection_disconnected",
        message: copyOrDefault("runtime.connection_disconnected", "已断开"),
      });
    }
  } catch (error) {
    appendLog("error", "", undefined, {
      messageCode: "runtime.market_action_failed",
      messageParams: { error: userVisibleErrorMessage(error) },
    });
    event.target.checked = false;
  }
});

accountSelect.addEventListener("change", async (event) => {
  const nextAccountId = String(event.target.value || "").trim().toLowerCase();
  const previousAccount = { ...currentAccount };
  const shouldReconnect = connectionToggle.checked;
  if (!nextAccountId || nextAccountId === previousAccount.id) {
    return;
  }
  try {
    const payload = await request("/config/accounts/select", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ account_id: nextAccountId })
    });
    setCurrentAccount(payload.account.id, payload.account.name, true);
    try {
      await refreshSymbolInfo(activeSymbol);
      if (shouldReconnect) {
        await request("/market/connect", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ symbol: activeSymbol })
        });
        closeSse();
        openSse();
      } else {
        closeSse();
        setConnectionState({
          connected: false,
          status: "disconnected",
          symbol: activeSymbol,
          account_id: payload.account.id,
          account_name: payload.account.name,
          message_code: "runtime.connection_disconnected",
          message: copyOrDefault("runtime.connection_disconnected", "已断开"),
        });
      }
      appendLog("success", "", undefined, { messageCode: "runtime.account_switched", messageParams: { account_name: payload.account.name } });
      maybeScheduleCurrentModePrecheck("mode_switch");
    } catch (error) {
      connectionToggle.checked = false;
        setConnectionState({
          connected: false,
          status: "error",
          symbol: activeSymbol,
          account_id: payload.account.id,
          account_name: payload.account.name,
          message: userVisibleErrorMessage(error)
        });
      appendLog("error", "", undefined, {
        messageCode: "runtime.account_switch_partial_failure",
        messageParams: { account_name: payload.account.name, symbol: activeSymbol, error: userVisibleErrorMessage(error) },
      });
    }
  } catch (error) {
    setCurrentAccount(previousAccount.id, previousAccount.name, true);
    appendLog("error", "", undefined, { messageCode: "runtime.account_switch_failed", messageParams: { error: userVisibleErrorMessage(error) } });
  }
});

editWhitelistBtn.addEventListener("click", async () => {
  try {
    const initialValue = whitelistSymbols.join(", ");
    const input = window.prompt("\u7f16\u8f91\u767d\u540d\u5355\u4ea4\u6613\u5bf9\uff0c\u4f7f\u7528\u82f1\u6587\u9017\u53f7\u5206\u9694", initialValue);
    if (input === null) return;
    const symbols = input.split(",").map((item) => normalizeSymbol(item)).filter(Boolean);
    const payload = await request("/config/whitelist", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ symbols })
    });
    whitelistSymbols = (payload.symbols || []).map((symbol) => normalizeSymbol(symbol)).filter(Boolean);
    const currentSymbol = normalizeSymbol(executionSymbol.value);
    temporaryCustomSymbol = whitelistSymbols.includes(currentSymbol) ? null : currentSymbol;
    rebuildSymbolOptions(currentSymbol);
    appendLog("success", "", undefined, {
      messageCode: "runtime.whitelist_updated",
      messageParams: { symbols: (payload.symbols || []).join(", ") },
    });
    await refreshSymbolInfo(currentSymbol);
    if (!(payload.symbols || []).includes(currentSymbol)) {
      appendLog("warn", "", undefined, { messageCode: "runtime.symbol_not_whitelisted", messageParams: { symbol: currentSymbol } });
    }
  } catch (error) {
    appendLog("error", "", undefined, {
      messageCode: "runtime.whitelist_update_failed",
      messageParams: { error: userVisibleErrorMessage(error) },
    });
  }
});

confirmSymbolBtn.addEventListener("click", async () => {
  const currentSymbol = normalizeSymbol(executionSymbol.value || activeSymbol);
  const input = window.prompt("\u8f93\u5165\u81ea\u5b9a\u4e49\u4ea4\u6613\u5bf9", currentSymbol);
  if (input === null) {
    rebuildSymbolOptions(activeSymbol);
    return;
  }
  await switchSymbol(input, connectionToggle.checked);
});

orderBookInput.addEventListener("change", async (event) => {
  await switchSymbol(event.target.value, connectionToggle.checked);
});

recoverSessionBtn?.addEventListener("click", async () => {
  await restoreRecoverableSession();
});

dismissRecoverSessionBtn?.addEventListener("click", () => {
  recoverableSessionDismissed = true;
  renderRecoverableSessionBanner();
});

simulateBtn.addEventListener("click", async () => {
  if (hasActiveSimulationRun()) {
    await requestSimulationAbort();
    return;
  }
  simulationRunInFlight = true;
  refreshExecutionActionButtons();
  try {
    openSse();
    await refreshSymbolInfo(executionSymbol.value);
    await request("/simulation/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        symbol: executionSymbol.value,
        trend_bias: document.getElementById("trend").value,
        open_amount: document.getElementById("calcMargin").value,
        leverage: Number(document.getElementById("leverage").value),
        round_count: Number(document.getElementById("calcRounds").value)
      })
    });
  } catch (error) {
    appendLog("error", "", undefined, {
      messageCode: "runtime.simulation_run_failed",
      messageParams: { error: userVisibleErrorMessage(error) },
    });
    simulationRunInFlight = false;
  } finally {
    refreshExecutionActionButtons();
  }
});

createBtn.addEventListener("click", async () => {
  if (activeSessionKind() === "paired_open") {
    await requestSessionAbort();
    return;
  }
  if (!window.confirm(buildRealExecutionConfirmation("paired_open"))) {
    return;
  }
  await withExecutionActionLock(async () => {
    setPrecheckPaused(true);
    try {
      const payload = await request("/sessions/open", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          symbol: executionSymbol.value,
          trend_bias: document.getElementById("trend").value,
          leverage: Number(document.getElementById("leverage").value),
          round_count: Number(document.getElementById("calcRounds").value),
          round_qty: document.getElementById("roundQty").value,
          round_interval_seconds: Number(document.getElementById("roundIntervalSeconds").value)
        })
      });
      appendLog("success", "", undefined, {
        messageCode: "runtime.session_created",
        messageParams: { session_id: payload.session_id },
      });
      startSessionPolling(payload.session_id);
    } catch (error) {
      if (error.precheck) applyPrecheckResult("paired_open", error.precheck);
      appendLog("error", "", undefined, {
        messageCode: "runtime.session_create_failed",
        messageParams: { error: userVisibleErrorMessage(error) },
      });
    } finally {
      setPrecheckPaused(false);
    }
  });
});

createCloseBtn.addEventListener("click", async () => {
  if (activeSessionKind() === "paired_close") {
    await requestSessionAbort();
    return;
  }
  if (!window.confirm(buildRealExecutionConfirmation("paired_close"))) {
    return;
  }
  await withExecutionActionLock(async () => {
    setPrecheckPaused(true);
    try {
      const payload = await request("/sessions/close", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          symbol: closeExecutionSymbol.value,
          trend_bias: document.getElementById("closeTrend").value,
          close_qty: document.getElementById("closeQty").value,
          round_count: Number(document.getElementById("closeRounds").value),
          round_interval_seconds: Number(document.getElementById("closeRoundIntervalSeconds").value)
        })
      });
      appendLog("success", "", undefined, {
        messageCode: "runtime.session_created",
        messageParams: { session_id: payload.session_id },
      });
      startSessionPolling(payload.session_id);
    } catch (error) {
      if (error.precheck) applyPrecheckResult("paired_close", error.precheck);
      appendLog("error", "", undefined, {
        messageCode: "runtime.session_create_failed",
        messageParams: { error: userVisibleErrorMessage(error) },
      });
    } finally {
      setPrecheckPaused(false);
    }
  });
});

createSingleOpenBtn.addEventListener("click", async () => {
  if (activeSessionKind() === "single_open") {
    await requestSessionAbort();
    return;
  }
  if (!window.confirm(buildRealExecutionConfirmation("single_open"))) {
    return;
  }
  await withExecutionActionLock(async () => {
    setPrecheckPaused(true);
    try {
      const payload = await request("/sessions/single-open", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          symbol: document.getElementById("singleOpenExecutionSymbol").value,
          open_mode: document.getElementById("singleOpenMode").value,
          selected_position_side: document.getElementById("singleOpenMode").value === "align" ? null : (document.getElementById("singleOpenOrder").value || null),
          open_qty: document.getElementById("singleOpenQty").value,
          leverage: Number(document.getElementById("singleOpenLeverage").value),
          round_count: Number(document.getElementById("singleOpenRounds").value),
          round_interval_seconds: Number(document.getElementById("singleOpenRoundIntervalSeconds").value)
        })
      });
      appendLog("success", "", undefined, {
        messageCode: "runtime.session_created",
        messageParams: { session_id: payload.session_id },
      });
      startSessionPolling(payload.session_id);
    } catch (error) {
      if (error.precheck) applyPrecheckResult("single_open", error.precheck);
      appendLog("error", "", undefined, {
        messageCode: "runtime.session_create_failed",
        messageParams: { error: userVisibleErrorMessage(error) },
      });
    } finally {
      setPrecheckPaused(false);
    }
  });
});

createSingleCloseBtn.addEventListener("click", async () => {
  if (activeSessionKind() === "single_close") {
    await requestSessionAbort();
    return;
  }
  if (!window.confirm(buildRealExecutionConfirmation("single_close"))) {
    return;
  }
  await withExecutionActionLock(async () => {
    setPrecheckPaused(true);
    try {
      const payload = await request("/sessions/single-close", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          symbol: document.getElementById("singleCloseExecutionSymbol").value,
          close_mode: document.getElementById("singleCloseMode").value,
          selected_position_side: document.getElementById("singleCloseMode").value === "align" ? null : (document.getElementById("singleCloseOrder").value || null),
          close_qty: document.getElementById("singleCloseQty").value,
          round_count: Number(document.getElementById("singleCloseRounds").value),
          round_interval_seconds: Number(document.getElementById("singleCloseRoundIntervalSeconds").value)
        })
      });
      appendLog("success", "", undefined, {
        messageCode: "runtime.session_created",
        messageParams: { session_id: payload.session_id },
      });
      startSessionPolling(payload.session_id);
    } catch (error) {
      if (error.precheck) applyPrecheckResult("single_close", error.precheck);
      appendLog("error", "", undefined, {
        messageCode: "runtime.session_create_failed",
        messageParams: { error: userVisibleErrorMessage(error) },
      });
    } finally {
      setPrecheckPaused(false);
    }
  });
});

const modeValidationSnapshots = new Map();
const PRECHECK_INTERVAL_MS = 10000;
const PRECHECK_PRICE_DRIFT_THRESHOLD = 0.003;

function shouldSilentlyRefreshPairedOpen(mode, trigger) {
  const snapshot = modeValidationSnapshots.get(mode);
  return mode === "paired_open" && (trigger === "price_drift" || trigger === "interval") && Boolean(snapshot?.precheckResult);
}

function buildModeParamsKey(mode = executionMode) {
  switch (mode) {
    case "paired_close":
      return JSON.stringify({
        mode,
        accountId: currentAccount.id,
        symbol: closeExecutionSymbol.value,
        trend_bias: document.getElementById("closeTrend")?.value || "",
        close_qty: document.getElementById("closeQty")?.value || "",
        round_count: Number(document.getElementById("closeRounds")?.value || 0),
      });
    case "single_open":
      return JSON.stringify({
        mode,
        accountId: currentAccount.id,
        symbol: document.getElementById("singleOpenExecutionSymbol")?.value || "",
        open_mode: document.getElementById("singleOpenMode")?.value || "",
        selected_position_side: document.getElementById("singleOpenMode")?.value === "align" ? "ALIGN" : (document.getElementById("singleOpenOrder")?.value || ""),
        open_qty: document.getElementById("singleOpenQty")?.value || "",
        leverage: Number(document.getElementById("singleOpenLeverage")?.value || 0),
        round_count: Number(document.getElementById("singleOpenRounds")?.value || 0),
      });
    case "single_close":
      return JSON.stringify({
        mode,
        accountId: currentAccount.id,
        symbol: document.getElementById("singleCloseExecutionSymbol")?.value || "",
        close_mode: document.getElementById("singleCloseMode")?.value || "",
        selected_position_side: document.getElementById("singleCloseMode")?.value === "align" ? "ALIGN" : (document.getElementById("singleCloseOrder")?.value || ""),
        close_qty: document.getElementById("singleCloseQty")?.value || "",
        round_count: Number(document.getElementById("singleCloseRounds")?.value || 0),
      });
    default:
      return JSON.stringify({
        mode,
        accountId: currentAccount.id,
        symbol: executionSymbol.value,
        trend_bias: document.getElementById("trend")?.value || "",
        leverage: Number(document.getElementById("leverage")?.value || 0),
        round_count: Number(document.getElementById("calcRounds")?.value || 0),
        open_amount: document.getElementById("calcMargin")?.value || "",
      });
  }
}

function buildModeContextKey(mode = executionMode) {
  const currentMode = mode || executionMode;
  const symbol = currentMode === "paired_close"
    ? normalizeSymbol(closeExecutionSymbol.value)
    : currentMode === "single_open"
      ? normalizeSymbol(document.getElementById("singleOpenExecutionSymbol")?.value || activeSymbol)
      : currentMode === "single_close"
        ? normalizeSymbol(document.getElementById("singleCloseExecutionSymbol")?.value || activeSymbol)
        : normalizeSymbol(executionSymbol.value || activeSymbol);
  const longQty = Number(positionQty(symbol, "LONG") || 0);
  const shortQty = Number(positionQty(symbol, "SHORT") || 0);
  const openOrderCounts = latestOpenOrderCountsBySymbol.get(symbol) || { system: 0, manual: 0 };
  const baseContext = {
    mode: currentMode,
    accountId: currentAccount.id,
    symbol,
    system_open_order_count: Number(openOrderCounts.system || 0),
    manual_open_order_count: Number(openOrderCounts.manual || 0),
  };
  if (currentMode === "paired_open" || currentMode === "single_open") {
    return JSON.stringify({
      ...baseContext,
      available_balance: Number(latestAvailableBalance ?? 0),
      long_qty: longQty,
      short_qty: shortQty,
    });
  }
  return JSON.stringify({
    ...baseContext,
    long_qty: longQty,
    short_qty: shortQty,
  });
}

function getModeValidationPrice(mode = executionMode) {
  const currentPrice = Number(latestReferencePrice || 0);
  if (currentPrice > 0) return currentPrice;
  const snapshot = modeValidationSnapshots.get(mode);
  return Number(snapshot?.validatedPrice || 0);
}

function extractValidatedPrice(mode, fallbackPrice = 0) {
  const currentPrice = Number(fallbackPrice || 0);
  if (currentPrice > 0) {
    return currentPrice;
  }
  const livePrice = Number(latestReferencePrice || 0);
  if (livePrice > 0) {
    return livePrice;
  }
  const snapshot = modeValidationSnapshots.get(mode);
  return Number(snapshot?.validatedPrice || 0);
}

function captureModeDisplaySnapshot(mode) {
  switch (mode) {
    case "paired_close":
      return {
        roundQty: document.getElementById("closeRoundQty")?.value || "0",
        totalNotional: document.getElementById("closeTotalNotional")?.textContent || "0.00",
        perRoundNotional: document.getElementById("closeNotionalPerRound")?.textContent || "0.00",
        maxCloseableQty: document.getElementById("maxCloseableQty")?.textContent || "0",
      };
    case "single_open":
      return {
        roundQty: document.getElementById("singleOpenRoundQty")?.value || "0",
        marginPerRound: document.getElementById("singleOpenMarginPerRound")?.textContent || "0.00",
        totalNotional: document.getElementById("singleOpenTotalNotional")?.textContent || "0.00",
        perRoundNotional: document.getElementById("singleOpenNotionalPerRound")?.textContent || "0.00",
      };
    case "single_close":
      return {
        roundQty: document.getElementById("singleCloseRoundQty")?.value || "0",
        availableQty: document.getElementById("singleCloseAvailableQty")?.textContent || "0",
        totalNotional: document.getElementById("singleCloseTotalNotional")?.textContent || "0.00",
        perRoundNotional: document.getElementById("singleCloseNotionalPerRound")?.textContent || "0.00",
      };
    default:
      return {
        roundQty: document.getElementById("roundQty")?.value || "0",
        marginPerRound: document.getElementById("marginPerRound")?.textContent || "0.00",
        totalNotional: document.getElementById("totalNotional")?.textContent || "0.00",
        perRoundNotional: document.getElementById("notionalPerRound")?.textContent || "0.00",
      };
  }
}

function applyModeDisplaySnapshot(mode, snapshot) {
  if (!snapshot) return;
  switch (mode) {
    case "paired_close": {
      const roundQtyInput = document.getElementById("closeRoundQty");
      const totalNotionalEl = document.getElementById("closeTotalNotional");
      const perRoundNotionalEl = document.getElementById("closeNotionalPerRound");
      const maxCloseableQtyEl = document.getElementById("maxCloseableQty");
      if (roundQtyInput) roundQtyInput.value = snapshot.roundQty || "0";
      if (totalNotionalEl) totalNotionalEl.textContent = snapshot.totalNotional || "0.00";
      if (perRoundNotionalEl) perRoundNotionalEl.textContent = snapshot.perRoundNotional || "0.00";
      if (maxCloseableQtyEl) maxCloseableQtyEl.textContent = snapshot.maxCloseableQty || "0";
      break;
    }
    case "single_open": {
      const roundQtyInput = document.getElementById("singleOpenRoundQty");
      const marginPerRoundEl = document.getElementById("singleOpenMarginPerRound");
      const totalNotionalEl = document.getElementById("singleOpenTotalNotional");
      const perRoundNotionalEl = document.getElementById("singleOpenNotionalPerRound");
      if (roundQtyInput) roundQtyInput.value = snapshot.roundQty || "0";
      if (marginPerRoundEl) marginPerRoundEl.textContent = snapshot.marginPerRound || "0.00";
      if (totalNotionalEl) totalNotionalEl.textContent = snapshot.totalNotional || "0.00";
      if (perRoundNotionalEl) perRoundNotionalEl.textContent = snapshot.perRoundNotional || "0.00";
      break;
    }
    case "single_close": {
      const roundQtyInput = document.getElementById("singleCloseRoundQty");
      const availableQtyEl = document.getElementById("singleCloseAvailableQty");
      const totalNotionalEl = document.getElementById("singleCloseTotalNotional");
      const perRoundNotionalEl = document.getElementById("singleCloseNotionalPerRound");
      if (roundQtyInput) roundQtyInput.value = snapshot.roundQty || "0";
      if (availableQtyEl) availableQtyEl.textContent = snapshot.availableQty || "0";
      if (totalNotionalEl) totalNotionalEl.textContent = snapshot.totalNotional || "0.00";
      if (perRoundNotionalEl) perRoundNotionalEl.textContent = snapshot.perRoundNotional || "0.00";
      break;
    }
    default: {
      const roundQtyInput = document.getElementById("roundQty");
      const marginPerRoundEl = document.getElementById("marginPerRound");
      const totalNotionalEl = document.getElementById("totalNotional");
      const perRoundNotionalEl = document.getElementById("notionalPerRound");
      if (roundQtyInput) roundQtyInput.value = snapshot.roundQty || "0";
      if (marginPerRoundEl) marginPerRoundEl.textContent = snapshot.marginPerRound || "0.00";
      if (totalNotionalEl) totalNotionalEl.textContent = snapshot.totalNotional || "0.00";
      if (perRoundNotionalEl) perRoundNotionalEl.textContent = snapshot.perRoundNotional || "0.00";
      break;
    }
  }
}

function recalculateMode(mode = executionMode) {
  if (mode === "paired_open") {
    recalculateOpenAmount();
  } else if (mode === "paired_close") {
    recalculateCloseAmount();
  } else if (mode === "single_open") {
    recalculateSingleOpenAmount();
  } else if (mode === "single_close") {
    recalculateSingleCloseAmount();
  }
}

function getModeValidationDecision(mode = executionMode) {
  const payload = buildPrecheckPayload(mode);
  const runnable = symbolInfoReady && canRunPrecheck(mode, payload);
  const paramsKey = buildModeParamsKey(mode);
  const contextKey = buildModeContextKey(mode);
  const snapshot = modeValidationSnapshots.get(mode) || null;
  if (!runnable) {
    return { runnable: false, payload, paramsKey, contextKey, snapshot, reason: "not_runnable" };
  }
  if (!snapshot) {
    return { runnable: true, payload, paramsKey, contextKey, snapshot: null, reason: "no_snapshot" };
  }
  if (snapshot.paramsKey !== paramsKey) {
    return { runnable: true, payload, paramsKey, contextKey, snapshot, reason: "params_changed" };
  }
  const contextChanged = snapshot.contextKey !== contextKey;
  const currentPrice = Number(getModeValidationPrice(mode) || 0);
  const validatedPrice = Number(snapshot.validatedPrice || 0);
  if (currentPrice > 0 && validatedPrice > 0) {
    const drift = Math.abs(currentPrice - validatedPrice) / validatedPrice;
    if (drift > PRECHECK_PRICE_DRIFT_THRESHOLD) {
      return { runnable: true, payload, paramsKey, contextKey, snapshot, reason: "price_drift", drift };
    }
  } else if (currentPrice > 0 && validatedPrice <= 0) {
    return { runnable: true, payload, paramsKey, contextKey, snapshot, reason: "no_price_baseline" };
  }
  if (Date.now() - Number(snapshot.validatedAt || 0) >= PRECHECK_INTERVAL_MS) {
    return {
      runnable: true,
      payload,
      paramsKey,
      contextKey,
      snapshot,
      reason: contextChanged ? "context_interval" : "interval_elapsed",
    };
  }
  if (contextChanged) {
    return { runnable: true, payload, paramsKey, contextKey, snapshot, reason: "context_stale" };
  }
  return { runnable: true, payload, paramsKey, contextKey, snapshot, reason: "fresh" };
}

function storeModeValidationSnapshot(mode, paramsKey, precheck, contextKey = buildModeContextKey(mode), validationPrice = 0) {
  modeValidationSnapshots.set(mode, {
    paramsKey,
    contextKey,
    validatedPrice: extractValidatedPrice(mode, validationPrice),
    validatedAt: Date.now(),
    precheckResult: precheck,
    displaySnapshot: captureModeDisplaySnapshot(mode),
  });
}
function restoreModeValidationSnapshot(mode = executionMode) {
  const decision = getModeValidationDecision(mode);
  if (!decision.snapshot || decision.reason !== "fresh") {
    return false;
  }
  if (decision.snapshot.precheckResult) {
    applyPrecheckResult(mode, decision.snapshot.precheckResult);
  }
  applyModeDisplaySnapshot(mode, decision.snapshot.displaySnapshot);
  return true;
}

function maybeScheduleCurrentModePrecheck(trigger = "price_tick") {
  if (precheckPaused) return;
  const decision = getModeValidationDecision(executionMode);
  if (!decision.runnable) {
    return;
  }
  const shouldRun =
    decision.reason === "no_snapshot" ||
    decision.reason === "params_changed" ||
    decision.reason === "no_price_baseline" ||
    decision.reason === "price_drift" ||
    decision.reason === "interval_elapsed" ||
    decision.reason === "context_interval" ||
    decision.reason === "context_stale";
  if (shouldRun) {
    const scheduleTrigger =
      decision.reason === "price_drift"
        ? "price_drift"
        : decision.reason === "context_interval"
          ? "account_update"
          : decision.reason === "context_stale"
            ? trigger
          : decision.reason === "interval_elapsed"
            ? "interval"
            : trigger;
    schedulePrecheck(executionMode, 0, scheduleTrigger);
  }
}

function queueUiRender() {
  if (renderFramePending) return;
  renderFramePending = true;
  requestAnimationFrame(() => {
    renderFramePending = false;
    if (pendingOrderbookPayload) {
      const payload = pendingOrderbookPayload;
      pendingOrderbookPayload = null;
      renderLevels(asksContainer, payload.asks || [], "sell");
      renderLevels(bidsContainer, payload.bids || [], "buy");
      const bestAsk = Number(payload.asks?.[0]?.price || 0);
      const bestBid = Number(payload.bids?.[0]?.price || 0);
      latestReferencePrice = bestAsk > 0 && bestBid > 0 ? (bestAsk + bestBid) / 2 : (bestAsk || bestBid || 0);
      if (symbolInfoReady) {
        maybeScheduleCurrentModePrecheck("price_tick");
      }
      document.getElementById("streamClock").textContent = nowTime();
    }
    if (pendingAccountOverviewPayload) {
      const payload = pendingAccountOverviewPayload;
      pendingAccountOverviewPayload = null;
      renderAccountOverview(payload);
      document.getElementById("streamClock").textContent = nowTime();
    }
    if (pendingLogEntries.length) {
      const entries = pendingLogEntries.splice(0, pendingLogEntries.length);
      entries.forEach((entry) => {
        appendLog(entry.level || "info", entry.message || "", entry.created_at, {
          messageCode: entry.message_code,
          messageParams: entry.message_params,
        });
      });
      document.getElementById("streamClock").textContent = nowTime();
    }
  });
}

function buildPrecheckPayload(mode = executionMode) {
  switch (mode) {
    case "paired_close":
      return {
        session_kind: "paired_close",
        symbol: closeExecutionSymbol.value,
        trend_bias: document.getElementById("closeTrend").value,
        close_qty: optionalPositiveValue(document.getElementById("closeQty").value),
        round_count: Number(document.getElementById("closeRounds").value),
      };
    case "single_open": {
      const openMode = document.getElementById("singleOpenMode").value;
      return {
        session_kind: "single_open",
        symbol: document.getElementById("singleOpenExecutionSymbol").value,
        open_mode: openMode,
        selected_position_side: openMode === "align" ? null : (document.getElementById("singleOpenOrder").value || null),
        open_qty: optionalPositiveValue(document.getElementById("singleOpenQty").value),
        leverage: Number(document.getElementById("singleOpenLeverage").value),
        round_count: Number(document.getElementById("singleOpenRounds").value),
      };
    }
    case "single_close": {
      const closeMode = document.getElementById("singleCloseMode").value;
      return {
        session_kind: "single_close",
        symbol: document.getElementById("singleCloseExecutionSymbol").value,
        close_mode: closeMode,
        selected_position_side: closeMode === "align" ? null : (document.getElementById("singleCloseOrder").value || null),
        close_qty: optionalPositiveValue(document.getElementById("singleCloseQty").value),
        round_count: Number(document.getElementById("singleCloseRounds").value),
      };
    }
    default: {
      const margin = Number(document.getElementById("calcMargin")?.value || 0);
      const leverage = Number(document.getElementById("leverage")?.value || 0);
      const rounds = Math.max(Number(document.getElementById("calcRounds")?.value || 0), 1);
      const totalNotional = margin * leverage;
      const notionalPerRound = rounds > 0 ? totalNotional / rounds : 0;
      const roundQty = latestReferencePrice > 0 ? (notionalPerRound / latestReferencePrice) : 0;
      return {
        session_kind: "paired_open",
        symbol: executionSymbol.value,
        trend_bias: document.getElementById("trend").value,
        leverage,
        round_count: Number(document.getElementById("calcRounds").value),
        round_qty: roundQty > 0 ? String(roundQty) : null,
      };
    }
  }
}

function applyPrecheckResult(mode, precheck) {
  if (!precheck) return;
  latestPrecheckResultByMode.set(mode, precheck);
  const derived = precheck.derived || {};
  if (mode === "paired_open") {
    syncPairedOpenDerivedPanel(derived);
  } else if (mode === "single_open") {
    syncSingleOpenDerivedPanel(derived);
  }
  if (mode === executionMode) {
    refreshDerivedStats({
      totalNotional: Number(derived.total_notional || 0),
      perRoundNotional: Number(derived.per_round_notional || 0),
      estimatedQty: Number(derived.normalized_round_qty || 0),
      minNotional: Number((derived.min_notional ?? currentSymbolInfo.min_notional) || 0),
    });
    document.getElementById("statMode").textContent = formatModeLabel(mode);
    document.getElementById("statCarryoverQty").textContent = formatNumber(resolveResidualQty(derived), 6);
    document.getElementById("statFinalAlignment").textContent = formatAlignmentStatus(derived.final_alignment_status);
  }
  const failure = firstFailingPrecheckItem(precheck);
  if (failure) {
    setHintStateForMode(mode, {
      canCreate: false,
      canSimulate: false,
      tone: "error",
      message: failure.message || summarizePrecheckMessage(precheck, "参数校验未通过。"),
    });
    return;
  }
  setHintStateForMode(mode, {
    canCreate: Boolean(precheck.ok),
    canSimulate: Boolean(precheck.ok),
    tone: Boolean(precheck.ok) ? "success" : "",
    message: buildModeSuccessHint(mode, precheck),
  });
  syncPrecheckFreshnessState(mode);
}

function shouldSilentlyRefreshMode(mode, trigger) {
  const snapshot = modeValidationSnapshots.get(mode);
  if (!snapshot?.precheckResult) return false;
  return trigger === "price_drift" || trigger === "interval" || trigger === "account_update" || trigger === "mode_switch";
}

async function runPrecheck(mode = executionMode, trigger = "user_input") {
  if (precheckPaused || mode !== executionMode) return;
  const payload = buildPrecheckPayload(mode);
  if (!canRunPrecheck(mode, payload)) {
    syncPrecheckFreshnessState(mode);
    return;
  }
  const requestKey = JSON.stringify({ mode, accountId: currentAccount.id, payload });
  const paramsKey = buildModeParamsKey(mode);
  const contextKey = buildModeContextKey(mode);
  if (inFlightPrecheckPayloadByMode.get(mode) === requestKey) {
    return;
  }
  const currentController = precheckAbortControllersByMode.get(mode);
  if (currentController) {
    currentController.abort();
  }
  const controller = new AbortController();
  const validationPrice = Number(getModeValidationPrice(mode) || 0);
  precheckAbortControllersByMode.set(mode, controller);
  inFlightPrecheckPayloadByMode.set(mode, requestKey);
  const token = (latestPrecheckTokensByMode.get(mode) || 0) + 1;
  latestPrecheckTokensByMode.set(mode, token);
  const silentRefresh = shouldSilentlyRefreshMode(mode, trigger);
  if (!silentRefresh) {
    setHintStateForMode(mode, {
      canCreate: false,
      canSimulate: false,
      tone: "",
      message: mode === "paired_close" || mode === "single_close" ? "正在校验平仓参数..." : "正在校验开仓参数...",
    });
    precheckFreshnessStateByMode.set(mode, {
      fresh: false,
      reason: "pending",
      label: copyOrDefault("runtime.precheck_status_pending", "待校验"),
      message: copyOrDefault("runtime.precheck_status_pending", "待校验"),
    });
  }
  try {
    const precheck = await request("/sessions/precheck", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    if (controller.signal.aborted || token !== (latestPrecheckTokensByMode.get(mode) || 0) || mode !== executionMode) return;
    latestResolvedPrecheckPayloadByMode.set(mode, requestKey);
    storeModeValidationSnapshot(mode, paramsKey, precheck, contextKey, validationPrice);
    applyPrecheckResult(mode, precheck);
  } catch (error) {
    if (controller.signal.aborted || error?.name === "AbortError") {
      return;
    }
    if (token !== (latestPrecheckTokensByMode.get(mode) || 0) || mode !== executionMode) return;
    latestPrecheckResultByMode.delete(mode);
    const precheck = error.precheck || null;
    if (error.validationDetail) {
      return;
    }
    if (precheck) {
      latestResolvedPrecheckPayloadByMode.set(mode, requestKey);
      storeModeValidationSnapshot(mode, paramsKey, precheck, contextKey, validationPrice);
      applyPrecheckResult(mode, precheck);
      return;
    }
    const message = copyOrDefault("runtime.precheck_request_failed", "预检失败：{error}", {
      error: userVisibleErrorMessage(error),
    });
    setHintStateForMode(mode, {
      canCreate: false,
      canSimulate: false,
      tone: "error",
      message,
    });
    updateTopRiskBanner("error", message);
    renderRiskBanner();
  } finally {
    if (inFlightPrecheckPayloadByMode.get(mode) === requestKey) {
      inFlightPrecheckPayloadByMode.delete(mode);
    }
    if (precheckAbortControllersByMode.get(mode) === controller) {
      precheckAbortControllersByMode.delete(mode);
    }
    syncPrecheckFreshnessState(mode);
  }
}

function schedulePrecheck(mode = executionMode, delay = 400, trigger = "user_input") {
  if (precheckPaused) return;
  const currentTimer = precheckTimersByMode.get(mode);
  if (currentTimer) {
    clearTimeout(currentTimer);
  }
  const timerId = setTimeout(() => {
    if (precheckTimersByMode.get(mode) === timerId) {
      precheckTimersByMode.delete(mode);
    }
    if (mode !== executionMode) return;
    runPrecheck(mode, trigger);
  }, delay);
  precheckTimersByMode.set(mode, timerId);
}
function applySymbolContext(symbol, info, options = {}) {
  const { syncInput = true } = options;
  const normalizedSymbol = normalizeSymbol(symbol);
  const symbolChanged = normalizedSymbol !== activeSymbol;
  activeSymbol = normalizedSymbol;
  currentSymbolInfo = info || { symbol: normalizedSymbol, min_notional: 0, allowed: true };
  symbolInfoReady = Boolean(info);
  if (symbolChanged) {
    latestReferencePrice = 0;
  }
  document.getElementById("statsSymbol").textContent = activeSymbol;
  executionSymbol.value = activeSymbol;
  closeExecutionSymbol.value = activeSymbol;
  if (singleOpenExecutionSymbol) singleOpenExecutionSymbol.value = activeSymbol;
  const singleCloseSymbolInput = document.getElementById("singleCloseExecutionSymbol");
  if (singleCloseSymbolInput) singleCloseSymbolInput.value = activeSymbol;
  document.getElementById("statMinNotional").textContent = formatNumber(currentSymbolInfo.min_notional || 0, 4);
  updateSymbolUnits(activeSymbol);
  if (syncInput) rebuildSymbolOptions(activeSymbol);
  refreshSingleOpenOrderOptions();
  refreshSingleClosePositionOptions();
  recalculateMode(executionMode);
  const footerStatus = document.getElementById("footerStatus");
  footerStatus.textContent = `${statusLabel(connectionToggle.checked ? "connected" : "disconnected")} ${activeSymbol}`;
  maybeScheduleCurrentModePrecheck("mode_switch");
}

function setExecutionMode(mode) {
  executionMode = mode;
  Object.entries(modeButtons).forEach(([key, button]) => {
    if (button) button.classList.toggle("active", key === mode);
  });
  Object.entries(modePanels).forEach(([key, panel]) => {
    if (panel) panel.classList.toggle("hidden", key !== mode);
  });
  document.getElementById("statMode").textContent = formatModeLabel(mode);
  const restored = restoreModeValidationSnapshot(mode);
  if (!restored) {
    recalculateMode(mode);
  }
  syncPrecheckFreshnessState(mode);
  refreshExecutionActionButtons();
  maybeScheduleCurrentModePrecheck("mode_switch");
}

function setActiveSymbol(symbol, syncInput = true, options = {}) {
  const { suppressRecalc = false, suppressPrecheck = false } = options;
  const normalizedSymbol = normalizeSymbol(symbol);
  const symbolChanged = normalizedSymbol !== activeSymbol;
  activeSymbol = normalizedSymbol;
  if (symbolChanged) {
    latestReferencePrice = 0;
  }
  document.getElementById("statsSymbol").textContent = activeSymbol;
  executionSymbol.value = activeSymbol;
  closeExecutionSymbol.value = activeSymbol;
  if (singleOpenExecutionSymbol) singleOpenExecutionSymbol.value = activeSymbol;
  const singleCloseSymbolInput = document.getElementById("singleCloseExecutionSymbol");
  if (singleCloseSymbolInput) singleCloseSymbolInput.value = activeSymbol;
  updateSymbolUnits(activeSymbol);
  if (syncInput) rebuildSymbolOptions(activeSymbol);
  refreshSingleOpenOrderOptions();
  refreshSingleClosePositionOptions();
  if (!suppressRecalc && (symbolChanged || !document.getElementById("roundQty")?.value)) {
    recalculateMode(executionMode);
  }
  const footerStatus = document.getElementById("footerStatus");
  footerStatus.textContent = `${statusLabel(connectionToggle.checked ? "connected" : "disconnected")} ${activeSymbol}`;
  if (!suppressPrecheck) {
    maybeScheduleCurrentModePrecheck("mode_switch");
  }
}

function setSymbolInfo(info, options = {}) {
  const { suppressRecalc = false, suppressPrecheck = false } = options;
  currentSymbolInfo = info || { symbol: activeSymbol, min_notional: 0, allowed: true };
  symbolInfoReady = Boolean(info);
  document.getElementById("statMinNotional").textContent = formatNumber(currentSymbolInfo.min_notional || 0, 4);
  if (!suppressRecalc) {
    recalculateMode(executionMode);
  }
  if (!suppressPrecheck) {
    maybeScheduleCurrentModePrecheck("mode_switch");
  }
}

Object.entries(modeButtons).forEach(([mode, button]) => {
  button.addEventListener("click", () => setExecutionMode(mode));
});

["calcMargin", "leverage", "calcRounds"].forEach((id) => {
  document.getElementById(id).addEventListener("input", () => {
    recalculateOpenAmount();
    schedulePrecheck("paired_open", 400, "user_input");
  });
});
["closeQty", "closeRounds"].forEach((id) => {
  document.getElementById(id).addEventListener("input", () => {
    recalculateCloseAmount();
    schedulePrecheck("paired_close", 400, "user_input");
  });
});
["singleOpenQty", "singleOpenRounds", "singleOpenLeverage"].forEach((id) => {
  document.getElementById(id)?.addEventListener("input", () => {
    recalculateSingleOpenAmount();
    schedulePrecheck("single_open", 400, "user_input");
  });
});
document.getElementById("singleOpenMode")?.addEventListener("change", () => {
  recalculateSingleOpenAmount();
  schedulePrecheck("single_open", 400, "user_input");
});
document.getElementById("singleOpenOrder")?.addEventListener("change", (event) => {
  syncPositionSideTone(event.target);
  recalculateSingleOpenAmount();
  schedulePrecheck("single_open", 400, "user_input");
});
["singleCloseQty", "singleCloseRounds"].forEach((id) => {
  document.getElementById(id)?.addEventListener("input", () => {
    recalculateSingleCloseAmount();
    schedulePrecheck("single_close", 400, "user_input");
  });
});
document.getElementById("singleCloseMode")?.addEventListener("change", () => {
  recalculateSingleCloseAmount();
  schedulePrecheck("single_close", 400, "user_input");
});
document.getElementById("singleCloseOrder")?.addEventListener("change", (event) => {
  syncPositionSideTone(event.target);
  recalculateSingleCloseAmount();
  schedulePrecheck("single_close", 400, "user_input");
});
document.getElementById("trend")?.addEventListener("change", (event) => {
  syncTrendSelectTone(event.target);
  schedulePrecheck("paired_open", 400, "user_input");
});
document.getElementById("closeTrend")?.addEventListener("change", (event) => {
  syncTrendSelectTone(event.target);
  schedulePrecheck("paired_close", 400, "user_input");
});

setEmptyState(asksContainer, "empty-state orderbook-empty", I18N_MESSAGES["runtime.orderbook_empty_asks"] || "开启连接后加载卖盘");
setEmptyState(bidsContainer, "empty-state orderbook-empty", I18N_MESSAGES["runtime.orderbook_empty_bids"] || "开启连接后加载买盘");
setActiveSymbol(activeSymbol, false);
renderAccountOverview({
  status: "idle",
  message_code: "runtime.connection_disconnected",
  message: copyOrDefault("runtime.connection_disconnected", "已断开"),
  totals: {},
  positions: [],
  account_id: currentAccount.id,
  account_name: currentAccount.name,
});
updateExecutionStats({
  mode: "paired_open",
  status: "idle",
  rounds_total: 0,
  rounds_completed: 0,
  total_notional: "0",
  notional_per_round: "0",
  last_qty: "0",
  min_notional: "0",
  carryover_qty: "0",
  final_alignment_status: "not_needed",
});
syncTrendSelectTone(document.getElementById("trend"));
syncTrendSelectTone(document.getElementById("closeTrend"));
syncPositionSideTone(document.getElementById("singleOpenOrder"));
syncPositionSideTone(document.getElementById("singleCloseOrder"));
renderExecutionSummaryBanner();
renderRiskBanner();
renderRecoverableSessionBanner();
refreshExecutionActionButtons();
setExecutionMode("paired_open");
appendLog("info", "", undefined, { messageCode: "runtime.console_ready" });
Promise.allSettled([
  loadAccounts(),
  loadWhitelist({ preferWhitelistDefault: true }),
]).then((results) => {
  const [accountsResult, whitelistResult] = results;
  if (accountsResult.status === "rejected") {
    appendLog("error", "", undefined, {
      messageCode: "runtime.accounts_load_failed",
      messageParams: { error: userVisibleErrorMessage(accountsResult.reason) },
    });
  }
  if (whitelistResult.status === "rejected") {
    temporaryCustomSymbol = activeSymbol;
    rebuildSymbolOptions(activeSymbol);
    appendLog("error", "", undefined, {
      messageCode: "runtime.whitelist_load_failed",
      messageParams: { error: userVisibleErrorMessage(whitelistResult.reason) },
    });
  }
  return refreshSymbolInfo(activeSymbol);
}).then(() => {
  detectRecoverableSession().catch(() => {});
}).catch((error) => {
    appendLog("error", "", undefined, {
      messageCode: "runtime.symbol_info_load_failed",
      messageParams: { error: userVisibleErrorMessage(error) },
    });
  detectRecoverableSession().catch(() => {});
});
setInterval(() => {
  maybeScheduleCurrentModePrecheck("interval");
}, PRECHECK_INTERVAL_MS);













