const logsBody = document.getElementById("logsBody");
const asksContainer = document.getElementById("asksContainer");
const appRoot = document.getElementById("appRoot");
const navRealBtn = document.getElementById("navRealBtn");
const navSimulationBtn = document.getElementById("navSimulationBtn");
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
const simulationRunButtons = [
  simulateBtn,
  document.getElementById("simulateCloseBtn"),
  document.getElementById("simulateSingleOpenBtn"),
  document.getElementById("simulateSingleCloseBtn"),
].filter(Boolean);
const simUnrealizedPnl = document.getElementById("simUnrealizedPnl");
const simAvailableBalance = document.getElementById("simAvailableBalance");
const simMarginUsed = document.getElementById("simMarginUsed");
const simEquity = document.getElementById("simEquity");
const simPositionsCount = document.getElementById("simPositionsCount");
const simPositionsList = document.getElementById("simPositionsList");
const simInitialBalance = document.getElementById("simInitialBalance");
const simMakerFee = document.getElementById("simMakerFee");
const simTakerFee = document.getElementById("simTakerFee");
const saveSimSettingsBtn = document.getElementById("saveSimSettingsBtn");
const resetSimAccountBtn = document.getElementById("resetSimAccountBtn");
const resetSimAccountInlineBtn = document.getElementById("resetSimAccountInlineBtn");
const exportSimHistoryBtn = document.getElementById("exportSimHistoryBtn");
const clearSimHistoryBtn = document.getElementById("clearSimHistoryBtn");
const simHistoryList = document.getElementById("simHistoryList");
const simHistoryCount = document.getElementById("simHistoryCount");
const executionSummaryBanner = document.getElementById("executionSummaryBanner");
const executionSummaryText = document.getElementById("executionSummaryText");
const executionRiskBanner = document.getElementById("executionRiskBanner");
const executionRiskText = document.getElementById("executionRiskText");
const recoverableSessionBanner = document.getElementById("recoverableSessionBanner");
const recoverableSessionText = document.getElementById("recoverableSessionText");
const recoverSessionBtn = document.getElementById("recoverSessionBtn");
const dismissRecoverSessionBtn = document.getElementById("dismissRecoverSessionBtn");
const activeRealSessionsPanel = document.getElementById("activeRealSessionsPanel");
const activeRealSessionsList = document.getElementById("activeRealSessionsList");
const activeRealSessionsCount = document.getElementById("activeRealSessionsCount");
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
let appPage = "real";
let activeSymbol = executionSymbol.value || "BTCUSDC";
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
let latestSimulationAvailableBalance = null;
let latestSimulationMakerFeeRate = 0;
let latestSimulationTakerFeeRate = 0;
let currentPositions = [];
let currentSimulationPositions = [];
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
const activeRealSessions = new Map();
let activeSessionsPoller = null;
let executionActionInFlightCount = 0;
let simulationRunInFlight = false;
let simulationAbortInFlight = false;
let sessionAbortInFlight = false;
const sessionAbortInFlightIds = new Set();
let latestExecutionStatsState = null;
let activeExecutionSummary = null;
const executionSummaryByPage = { real: null, simulation: null };
const executionStatsByPage = { real: null, simulation: null };
const seenSimulationRunLogKeys = new Set();
const seenSimulationEventIds = new Set();
let activeSimulationRunId = null;
let activeSimulationPoller = null;
let latestSimulationEventId = 0;
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
  paired_open: copyOrDefault("console.actions.create_real_paired_open", "创建真实开单会话"),
  paired_close: copyOrDefault("console.actions.create_real_paired_close", "创建真实平仓会话"),
  single_open: copyOrDefault("console.actions.create_real_single_open", "创建真实单向开仓会话"),
  single_close: copyOrDefault("console.actions.create_real_single_close", "创建真实单向平仓会话"),
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

const EXECUTION_FORM_FIELD_IDS = Object.freeze([
  "executionSymbol",
  "trend",
  "calcMargin",
  "leverage",
  "calcRounds",
  "roundIntervalSeconds",
  "closeExecutionSymbol",
  "closeTrend",
  "closeQty",
  "closeRounds",
  "closeRoundIntervalSeconds",
  "singleOpenExecutionSymbol",
  "singleOpenMode",
  "singleOpenOrder",
  "singleOpenQty",
  "singleOpenLeverage",
  "singleOpenRounds",
  "singleOpenRoundIntervalSeconds",
  "singleCloseExecutionSymbol",
  "singleCloseMode",
  "singleCloseOrder",
  "singleCloseQty",
  "singleCloseRounds",
  "singleCloseRoundIntervalSeconds",
]);
const EXECUTION_SYMBOL_FIELD_BY_MODE = Object.freeze({
  paired_open: "executionSymbol",
  paired_close: "closeExecutionSymbol",
  single_open: "singleOpenExecutionSymbol",
  single_close: "singleCloseExecutionSymbol",
});
const executionPageFormStates = { real: null, simulation: null };
let restoringExecutionPageState = false;
let executionPageSwitchQueue = Promise.resolve();

function normalizeAppPage(page) {
  return page === "simulation" ? "simulation" : "real";
}

function cloneExecutionFormState(state) {
  if (!state) return null;
  return {
    mode: normalizeSessionKind(state.mode),
    activeSymbol: state.activeSymbol || activeSymbol,
    fields: { ...(state.fields || {}) },
  };
}

function captureExecutionFormState(mode = executionMode) {
  const fields = {};
  EXECUTION_FORM_FIELD_IDS.forEach((id) => {
    const element = document.getElementById(id);
    if (element && "value" in element) {
      fields[id] = element.value;
    }
  });
  return {
    mode: normalizeSessionKind(mode),
    activeSymbol,
    fields,
  };
}

function initializeExecutionPageFormStates() {
  const currentState = captureExecutionFormState(executionMode);
  if (!executionPageFormStates.real) {
    executionPageFormStates.real = cloneExecutionFormState(currentState);
  }
  if (!executionPageFormStates.simulation) {
    executionPageFormStates.simulation = cloneExecutionFormState(currentState);
  }
}

function saveExecutionPageFormState(page = appPage) {
  executionPageFormStates[normalizeAppPage(page)] = captureExecutionFormState(executionMode);
}

function symbolForExecutionFormState(state) {
  const mode = normalizeSessionKind(state?.mode);
  const symbolFieldId = EXECUTION_SYMBOL_FIELD_BY_MODE[mode] || "executionSymbol";
  return normalizeSymbol(state?.fields?.[symbolFieldId] || state?.activeSymbol || activeSymbol);
}

function syncExecutionPageFormStateSymbols(previousSymbol, nextSymbol) {
  const previous = normalizeSymbol(previousSymbol);
  const next = normalizeSymbol(nextSymbol);
  if (!previous || !next || previous === next || restoringExecutionPageState) return;
  const state = executionPageFormStates[normalizeAppPage(appPage)];
  if (!state?.fields) return;
  Object.values(EXECUTION_SYMBOL_FIELD_BY_MODE).forEach((fieldId) => {
    if (normalizeSymbol(state.fields[fieldId]) === previous) {
      state.fields[fieldId] = next;
    }
  });
  if (normalizeSymbol(state.activeSymbol) === previous) {
    state.activeSymbol = next;
  }
}

function applyExecutionFormFields(normalizedState) {
  Object.entries(normalizedState.fields || {}).forEach(([id, value]) => {
    const element = document.getElementById(id);
    if (element && "value" in element) {
      element.value = value;
    }
  });
  setExecutionMode(normalizedState.mode);
}

async function applyExecutionFormState(state) {
  if (!state) return false;
  const normalizedState = cloneExecutionFormState(state);
  const nextSymbol = symbolForExecutionFormState(normalizedState);
  if (nextSymbol && nextSymbol !== normalizeSymbol(activeSymbol) && typeof switchSymbol === "function") {
    const switched = await switchSymbol(nextSymbol, connectionToggle.checked);
    if (!switched) return false;
  } else if (nextSymbol && typeof refreshSymbolInfo === "function") {
    await refreshSymbolInfo(nextSymbol);
  }
  restoringExecutionPageState = true;
  try {
    applyExecutionFormFields(normalizedState);
  } finally {
    restoringExecutionPageState = false;
  }
  recalculateMode(normalizedState.mode);
  refreshExecutionActionButtons();
  return true;
}

function normalizeSessionKind(kind) {
  return ["paired_close", "single_open", "single_close"].includes(String(kind || ""))
    ? String(kind)
    : "paired_open";
}

function isTerminalSimulationStatus(status) {
  return ["idle", "completed", "completed_with_skips", "blocked", "aborted", "exception", "interrupted"].includes(String(status || "idle"));
}

function hasActiveSimulationRun() {
  if (simulationRunInFlight || simulationAbortInFlight) return true;
  if (typeof activeSimulationRunId !== "undefined" && activeSimulationRunId) return true;
  return Boolean(latestExecutionStatsState && !isTerminalSimulationStatus(latestExecutionStatsState.status));
}

function symbolForMode(mode) {
  switch (normalizeSessionKind(mode)) {
    case "paired_close":
      return normalizeSymbol(closeExecutionSymbol?.value || activeSymbol);
    case "single_open":
      return normalizeSymbol(document.getElementById("singleOpenExecutionSymbol")?.value || activeSymbol);
    case "single_close":
      return normalizeSymbol(document.getElementById("singleCloseExecutionSymbol")?.value || activeSymbol);
    default:
      return normalizeSymbol(executionSymbol?.value || activeSymbol);
  }
}

function activeRealSessionList() {
  const byId = new Map(activeRealSessions);
  if (activeSessionId && activeSessionState && !isTerminalSession(activeSessionState.status)) {
    byId.set(activeSessionId, activeSessionState);
  }
  return [...byId.values()]
    .filter((session) => session && session.session_id && !isTerminalSession(session.status))
    .sort((left, right) => new Date(right.updated_at || 0) - new Date(left.updated_at || 0));
}

function hasActiveRealSessionForSymbol(symbol) {
  const normalizedSymbol = normalizeSymbol(symbol);
  if (!normalizedSymbol) return false;
  return activeRealSessionList().some((session) => normalizeSymbol(session.symbol || "") === normalizedSymbol);
}

function syncFocusedAbortFlag() {
  sessionAbortInFlight = Boolean(activeSessionId && sessionAbortInFlightIds.has(activeSessionId));
}

function setSessionAbortPending(sessionId, pending) {
  if (!sessionId) return;
  if (pending) {
    sessionAbortInFlightIds.add(sessionId);
  } else {
    sessionAbortInFlightIds.delete(sessionId);
  }
  syncFocusedAbortFlag();
}

function upsertActiveRealSession(session) {
  if (!session?.session_id) return;
  if (isTerminalSession(session.status)) {
    activeRealSessions.delete(session.session_id);
    setSessionAbortPending(session.session_id, false);
    return;
  }
  activeRealSessions.set(session.session_id, session);
}

function syncActiveRealSessions(sessions = []) {
  const activeIds = new Set();
  (Array.isArray(sessions) ? sessions : []).forEach((session) => {
    if (!session?.session_id) return;
    if (isTerminalSession(session.status)) {
      activeRealSessions.delete(session.session_id);
      setSessionAbortPending(session.session_id, false);
      return;
    }
    activeIds.add(session.session_id);
    activeRealSessions.set(session.session_id, session);
  });
  [...activeRealSessions.keys()].forEach((sessionId) => {
    if (!activeIds.has(sessionId)) {
      activeRealSessions.delete(sessionId);
      setSessionAbortPending(sessionId, false);
    }
  });
}

function renderActiveRealSessionsPanel() {
  if (!activeRealSessionsPanel || !activeRealSessionsList) return;
  const sessions = activeRealSessionList();
  if (activeRealSessionsCount) activeRealSessionsCount.textContent = String(sessions.length);
  activeRealSessionsPanel.classList.toggle("hidden", sessions.length === 0);
  activeRealSessionsList.innerHTML = "";
  sessions.forEach((session) => {
    const item = document.createElement("div");
    item.className = "active-session-item";
    item.dataset.sessionId = session.session_id;
    item.classList.toggle("active", session.session_id === activeSessionId);
    const meta = document.createElement("div");
    meta.className = "active-session-meta";
    const title = document.createElement("strong");
    title.textContent = `${formatModeLabel(session.session_kind)} · ${session.symbol || "--"}`;
    const detail = document.createElement("span");
    detail.textContent = `${session.status || "--"} · ${session.session_id}`;
    meta.append(title, detail);
    const focusButton = document.createElement("button");
    focusButton.type = "button";
    focusButton.className = "inline-btn";
    focusButton.dataset.focusSessionId = session.session_id;
    focusButton.textContent = session.session_id === activeSessionId
      ? copyOrDefault("console.action.monitoring", "监控中")
      : copyOrDefault("console.action.monitor", "查看");
    focusButton.disabled = session.session_id === activeSessionId;
    const abortButton = document.createElement("button");
    abortButton.type = "button";
    abortButton.className = "inline-btn secondary";
    abortButton.dataset.abortSessionId = session.session_id;
    abortButton.textContent = sessionAbortInFlightIds.has(session.session_id)
      ? EXECUTION_ABORTING_LABEL
      : copyOrDefault("console.action.abort", "终止");
    abortButton.disabled = sessionAbortInFlightIds.has(session.session_id);
    item.append(meta, focusButton, abortButton);
    activeRealSessionsList.appendChild(item);
  });
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
  setBannerState(executionRiskBanner, executionRiskText, null);
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
      return copyOrDefault("runtime.stop_reason.filled", "已完成");
    case "below_min_notional":
      return copyOrDefault("runtime.stop_reason.below_min_notional", "残量低于最小下单金额");
    case "insufficient_balance":
      return copyOrDefault("runtime.stop_reason.insufficient_balance", "余额不足");
    case "insufficient_position":
      return copyOrDefault("runtime.stop_reason.insufficient_position", "持仓不足");
    case "price_guard_blocked":
      return copyOrDefault("runtime.stop_reason.price_guard_blocked", "价格保护阻断");
    case "quote_stale":
      return copyOrDefault("runtime.stop_reason.quote_stale", "行情已过期");
    case "open_order_conflict_detected":
      return copyOrDefault("runtime.stop_reason.open_order_conflict_detected", "检测到挂单冲突");
    case "target_guard_blocked":
      return copyOrDefault("runtime.stop_reason.target_guard_blocked", "目标保护阻断");
    case "max_extension_rounds_reached":
      return copyOrDefault("runtime.stop_reason.max_extension_rounds_reached", "补充轮已耗尽");
    case "max_session_duration_reached":
      return copyOrDefault("runtime.stop_reason.max_session_duration_reached", "已超过最长执行时长");
    case "max_simulation_duration_reached":
      return copyOrDefault("runtime.stop_reason.max_simulation_duration_reached", "模拟运行时长达到上限");
    case "interrupted":
      return copyOrDefault("runtime.stop_reason.interrupted", "服务重启中断");
    case "running":
      return copyOrDefault("runtime.simulation_stage.running", "执行中");
    case "user_aborted":
      return copyOrDefault("runtime.stop_reason.user_aborted", "用户已终止");
    case "insufficient_sim_balance":
      return copyOrDefault("runtime.stop_reason.insufficient_sim_balance", "模拟余额不足");
    case "insufficient_sim_position":
      return copyOrDefault("runtime.stop_reason.insufficient_sim_position", "模拟持仓不足");
    case "stale_orderbook":
      return copyOrDefault("runtime.stop_reason.stale_orderbook", "盘口已过期");
    case "min_notional_blocked":
      return copyOrDefault("runtime.stop_reason.min_notional_blocked", "低于最小下单金额");
    case "limit_order_unfilled":
      return copyOrDefault("runtime.stop_reason.limit_order_unfilled", "限价挂单未成交");
    case "simulation_account_invariant_failed":
      return copyOrDefault("runtime.stop_reason.simulation_account_invariant_failed", "模拟账户校验失败");
    case "exception":
      return copyOrDefault("runtime.stop_reason.exception", "执行异常");
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
    segments.push(copyOrDefault("console.summary.planned_round_qtys", "计划轮量：{qtys}", { qtys: summary.plannedRoundQtys.join(" / ") }));
  }
  if (Number(summary.maxExtensionRounds || 0) > 0) {
    segments.push(copyOrDefault("console.summary.extension_rounds", "补充轮：{used} / {max}", {
      used: Number(summary.extensionRoundsUsed || 0),
      max: Number(summary.maxExtensionRounds || 0),
    }));
  }
  if (summary.stopReason) {
    segments.push(copyOrDefault("console.summary.stop_reason", "停止原因：{reason}", { reason: formatStopReason(summary.stopReason) }));
  }
  if (summary.sessionDeadlineAt) {
    segments.push(copyOrDefault("console.summary.deadline", "截止：{time}", {
      time: new Date(summary.sessionDeadlineAt).toLocaleTimeString(APP_LOCALE, { hour12: false, timeZone: APP_TIMEZONE }),
    }));
  }
  if (summary.abortRequested) {
    segments.push(copyOrDefault("console.summary.abort_requested", "已请求终止"));
  }
  return segments.join(" | ");
}

function renderExecutionSummaryBanner() {
  const page = normalizeAppPage(appPage);
  activeExecutionSummary = page === "real" ? executionSummaryByPage.real || null : null;
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

function updateExecutionSummary(summary = null, page = appPage) {
  const normalizedPage = normalizeAppPage(page);
  executionSummaryByPage[normalizedPage] = summary;
  if (normalizedPage === normalizeAppPage(appPage)) {
    renderExecutionSummaryBanner();
  }
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
  if (rendered !== key) return rendered;
  return String(fallback || "").replace(/\{(\w+)\}/g, (_, name) => {
    const value = params[name];
    return value === undefined || value === null ? `{${name}}` : String(value);
  });
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
  const safeFallback = fallback || source.fallbackMessage || copyOrDefault("runtime.execution_message_unavailable", "日志信息暂不可用");
  if (source && typeof source === "object") {
    const messageCode = source.messageCode || source.message_code;
    const messageParams = source.messageParams || source.message_params || {};
    if (messageCode) {
      const rendered = formatCopy(messageCode, messageParams);
      if (rendered !== messageCode) {
        return rendered;
      }
    }
    if (source.trustedMessage === true && source.message) {
      return String(source.message);
    }
    if (source.fallbackMessage) {
      return String(source.fallbackMessage);
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

function parseDisplayNumber(value) {
  const normalized = String(value ?? "").replace(/,/g, "").trim();
  const numeric = Number(normalized);
  return Number.isFinite(numeric) ? numeric : 0;
}

function readStatNumber(id) {
  return parseDisplayNumber(document.getElementById(id)?.textContent || "0");
}

function formatMoney(value, digits = 2) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "0.00";
  return numeric.toLocaleString(APP_LOCALE, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function applyMetricTone(element, rawValue) {
  if (!element?.classList) return;
  element.classList.remove("positive", "negative", "zero");
  const value = Number(rawValue || 0);
  if (value > 0) {
    element.classList.add("positive");
  } else if (value < 0) {
    element.classList.add("negative");
  } else {
    element.classList.add("zero");
  }
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
  return (value || "BTCUSDC").trim().toUpperCase();
}

function payloadMatchesActiveSymbol(payload = {}) {
  const rawSymbol = String(payload.symbol || "").trim();
  if (!rawSymbol) return true;
  return normalizeSymbol(rawSymbol) === normalizeSymbol(activeSymbol);
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

function canRunSimulation(mode, payload) {
  if (!payload || !payload.symbol) return false;
  switch (normalizeSessionKind(mode)) {
    case "paired_close":
      if (!payload.trend_bias || !payload.close_qty || Number(payload.round_count) <= 0) return false;
      if (typeof appPage !== "undefined" && appPage === "simulation") {
        const maxCloseableQty = maxCloseableQtyForSymbol(payload.symbol);
        return maxCloseableQty > 0 && Number(payload.close_qty) <= maxCloseableQty;
      }
      return true;
    case "single_open":
      if (!payload.open_mode || !payload.open_qty || Number(payload.round_count) <= 0 || Number(payload.leverage) <= 0) return false;
      return payload.open_mode === "align" ? true : Boolean(payload.selected_position_side);
    case "single_close":
      if (!payload.close_mode || !payload.close_qty || Number(payload.round_count) <= 0) return false;
      return payload.close_mode === "align" ? true : Boolean(payload.selected_position_side);
    default:
      return Boolean(payload.trend_bias && payload.open_amount && Number(payload.round_count) > 0 && Number(payload.leverage) > 0);
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
    fallbackMessage: options.fallbackMessage,
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
    const summaryPage = normalizeAppPage(appPage);
    const pageSummary = executionSummaryByPage[summaryPage];
    if (pageSummary) {
      executionSummaryByPage[summaryPage] = { ...pageSummary, residualSide: latestResidualSideLabel };
      renderExecutionSummaryBanner();
    }
    const statResidualSide = document.getElementById("statResidualSide");
    if (statResidualSide) {
      statResidualSide.textContent = latestResidualSideLabel;
    }
  }
  if (executionRiskBanner && (level === "warn" || level === "error")) {
    updateTopRiskBanner(level, renderedMessage);
    renderRiskBanner();
  }
}

function setConnectionState(state) {
  const connected = Boolean(state.connected);
  const status = String(state.status || "disconnected");
  const stateSymbol = normalizeSymbol(state.symbol || "");
  const displaySymbol = stateSymbol || activeSymbol;
  const badge = document.getElementById("connectionBadge");
  const simulationBadge = document.getElementById("simulationConnectionBadge");
  const switchLabel = document.getElementById("switchLabel");
  const footerDot = document.getElementById("footerDot");
  const footerStatus = document.getElementById("footerStatus");
  setCurrentAccount(state.account_id, state.account_name);
  if ((connected || status === "connecting") && stateSymbol && connectionToggle.checked) {
    setActiveSymbol(stateSymbol);
  }
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
  footerStatus.textContent = `${connected ? statusLabel("connected") : statusLabel(status)} ${displaySymbol}`;
  if (simulationBadge) {
    simulationBadge.className = badge.className;
    simulationBadge.textContent = badge.textContent;
  }
  connectionToggle.checked = connected;
}
function refreshDerivedStats({ totalNotional = 0, perRoundNotional = 0, estimatedQty = 0, minNotional = Number(currentSymbolInfo.min_notional || 0) } = {}) {
  document.getElementById("statTotalNotional").textContent = formatNumber(totalNotional || 0, 4);
  document.getElementById("statPerRound").textContent = formatNumber(perRoundNotional || 0, 4);
  document.getElementById("statLastQty").textContent = formatNumber(estimatedQty || 0, 8);
  document.getElementById("statMinNotional").textContent = formatNumber(minNotional || 0, 4);
}

function renderExecutionStatsSnapshot(snapshot = {}) {
  const page = normalizeAppPage(snapshot?.page || appPage);
  const displaySnapshot = {
    page,
    status: "idle",
    mode: "paired_open",
    roundsCompleted: 0,
    roundsTotal: 0,
    totalNotional: 0,
    perRoundNotional: 0,
    minNotional: 0,
    carryoverQty: 0,
    finalAlignmentStatus: "not_needed",
    lastQty: 0,
    residualSide: "--",
    abortRequested: false,
    ...(snapshot || {}),
  };
  if (page === "simulation") {
    document.getElementById("statSimStatus").textContent = displaySnapshot.status || "idle";
  } else {
    document.getElementById("statSessionStatus").textContent = displaySnapshot.status || "idle";
  }
  document.getElementById("statMode").textContent = formatModeLabel(displaySnapshot.mode || "paired_open");
  document.getElementById("statRounds").textContent = displaySnapshot.roundsLabel || `${displaySnapshot.roundsCompleted || 0} / ${displaySnapshot.roundsTotal || 0}`;
  document.getElementById("statTotalNotional").textContent = formatNumber(displaySnapshot.totalNotional || 0, 4);
  document.getElementById("statPerRound").textContent = formatNumber(displaySnapshot.perRoundNotional || 0, 4);
  document.getElementById("statMinNotional").textContent = formatNumber(displaySnapshot.minNotional || 0, 4);
  document.getElementById("statCarryoverQty").textContent = formatNumber(displaySnapshot.carryoverQty || 0, 6);
  document.getElementById("statFinalAlignment").textContent = formatAlignmentStatus(displaySnapshot.finalAlignmentStatus || "not_needed");
  document.getElementById("statLastQty").textContent = formatNumber(displaySnapshot.lastQty || 0, 8);
  document.getElementById("statResidualSide").textContent = displaySnapshot.residualSide || "--";
  updateAbortStateLabel(displaySnapshot.status, Boolean(displaySnapshot.abortRequested));
}

function renderCurrentPageExecutionStats() {
  const page = normalizeAppPage(appPage);
  renderExecutionStatsSnapshot(executionStatsByPage[page] || { page });
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

function buildRoundsLabelFromStats(stats = {}, roundsCompleted = 0, roundsTotal = 0) {
  if (stats.extension_rounds_unlimited) {
    return `${roundsCompleted || 0} / ${copyOrDefault("runtime.rounds_unlimited", "不限")}`;
  }
  return `${roundsCompleted || 0} / ${roundsTotal || 0}`;
}

function updateExecutionStats(stats) {
  latestExecutionStatsState = stats;
  if (isTerminalSimulationStatus(stats.status)) {
    simulationRunInFlight = false;
    simulationAbortInFlight = false;
  }
  const roundsCompleted = stats.rounds_completed || 0;
  const roundsTotal = stats.rounds_total || 0;
  const snapshot = {
    page: "simulation",
    status: stats.status || "idle",
    mode: stats.mode || executionMode,
    roundsCompleted,
    roundsTotal,
    roundsLabel: buildRoundsLabelFromStats(stats, roundsCompleted, roundsTotal),
    totalNotional: stats.total_notional || 0,
    perRoundNotional: stats.notional_per_round || 0,
    minNotional: stats.min_notional,
    carryoverQty: resolveResidualQty(stats),
    finalAlignmentStatus: stats.final_alignment_status,
    lastQty: stats.last_qty || 0,
    abortRequested: simulationAbortInFlight || stats.status === "aborting",
  };
  executionStatsByPage.simulation = snapshot;
  if (normalizeAppPage(appPage) === "simulation") {
    renderExecutionStatsSnapshot(snapshot);
  }
  if (stats.status === "running" || stats.status === "aborting" || !isTerminalSimulationStatus(stats.status)) {
    updateExecutionSummary(buildExecutionSummary(stats, { abortRequested: simulationAbortInFlight || stats.status === "aborting" }), "simulation");
  } else if (!hasActiveExecutionSession()) {
    updateExecutionSummary(buildExecutionSummary(stats, { abortRequested: false }), "simulation");
  }
  refreshExecutionActionButtons();
}

function displaySimulationStopReason(payload = {}) {
  const rawReason = payload.message_params?.stop_reason ?? payload.messageParams?.stop_reason ?? payload.stop_reason ?? "";
  const formatted = formatStopReason(rawReason);
  return formatted && formatted !== "--" ? formatted : (rawReason || payload.status || "--");
}

function simulationRunFallbackMessage(payload = {}) {
  const stopReason = displaySimulationStopReason(payload);
  return copyOrDefault("runtime.simulation_run_finished", "模拟结束：{stop_reason}", { stop_reason: stopReason });
}

function simulationLogLevelForStatus(status) {
  const normalized = String(status || "");
  if (normalized === "completed") return "success";
  if (["blocked", "exception"].includes(normalized)) return "error";
  if (["completed_with_skips", "aborted"].includes(normalized)) return "warn";
  return "info";
}

function normalizeSimulationMessageParams(messageCode, params = {}) {
  const normalized = { ...(params || {}) };
  if (normalized.mode) {
    normalized.mode = formatModeLabel(normalized.mode);
  }
  if (normalized.stop_reason) {
    normalized.stop_reason = formatStopReason(normalized.stop_reason);
  }
  return normalized;
}

function updateSimulationRunStatsFromPayload(payload = {}, requestPayload = {}) {
  const existing = executionStatsByPage.simulation || {};
  const status = String(payload.status || "running");
  const mode = normalizeSessionKind(payload.session_kind || requestPayload.session_kind || existing.mode || executionMode);
  const roundsTotal = Number(payload.rounds_total ?? payload.round_count ?? requestPayload.round_count ?? existing.roundsTotal ?? 0);
  let roundsCompleted = Number(payload.rounds_completed ?? payload.completed_rounds ?? existing.roundsCompleted ?? 0);
  if (!roundsCompleted && isTerminalSimulationStatus(status) && !["idle", "blocked", "exception", "aborted"].includes(status)) {
    roundsCompleted = roundsTotal;
  }
  const filledQty = parseDisplayNumber(payload.filled_qty);
  const avgFillPrice = parseDisplayNumber(payload.avg_fill_price);
  const calculatedNotional = filledQty > 0 && avgFillPrice > 0 ? filledQty * avgFillPrice : 0;
  const totalNotional = parseDisplayNumber(payload.total_notional) || calculatedNotional || Number(existing.totalNotional || 0) || readStatNumber("statTotalNotional");
  const perRoundNotional = parseDisplayNumber(payload.notional_per_round)
    || Number(existing.perRoundNotional || 0)
    || readStatNumber("statPerRound")
    || (roundsTotal > 0 ? totalNotional / roundsTotal : 0);
  const lastQty = parseDisplayNumber(payload.last_qty)
    || Number(existing.lastQty || 0)
    || readStatNumber("statLastQty");
  const snapshot = {
    page: "simulation",
    status,
    mode,
    roundsCompleted,
    roundsTotal,
    roundsLabel: buildRoundsLabelFromStats(payload, roundsCompleted, roundsTotal),
    totalNotional,
    perRoundNotional,
    minNotional: parseDisplayNumber(payload.min_notional) || Number(existing.minNotional || currentSymbolInfo.min_notional || 0),
    carryoverQty: resolveResidualQty(payload) || Number(existing.carryoverQty || 0),
    finalAlignmentStatus: payload.final_alignment_status || existing.finalAlignmentStatus || "not_needed",
    lastQty,
    abortRequested: simulationAbortInFlight || status === "aborting",
  };
  latestExecutionStatsState = { ...payload, status };
  executionStatsByPage.simulation = snapshot;
  if (isTerminalSimulationStatus(status)) {
    simulationRunInFlight = false;
    simulationAbortInFlight = false;
  }
  if (normalizeAppPage(appPage) === "simulation") {
    renderExecutionStatsSnapshot(snapshot);
  }
  updateExecutionSummary(buildExecutionSummary({
    ...payload,
    session_kind: mode,
    rounds_total: roundsTotal,
    rounds_completed: roundsCompleted,
    total_notional: totalNotional,
    notional_per_round: perRoundNotional,
    last_qty: lastQty,
    min_notional: snapshot.minNotional,
    status,
  }, { abortRequested: snapshot.abortRequested }), "simulation");
  refreshExecutionActionButtons();
}

function appendSimulationRunLog(payload = {}, requestPayload = {}) {
  const eventId = Number(payload.event_id || 0);
  if (eventId > 0) {
    if (seenSimulationEventIds.has(eventId)) return;
    seenSimulationEventIds.add(eventId);
    latestSimulationEventId = Math.max(latestSimulationEventId, eventId);
  }
  if (!payload.message_code && !payload.message && !payload.stop_reason) return;
  const runKey = payload.run_id
    ? `${payload.run_id}:${payload.status || ""}:${payload.message_code || ""}`
    : `${requestPayload.session_kind || executionMode}:${payload.status || "running"}:${Date.now()}`;
  if (payload.run_id && seenSimulationRunLogKeys.has(runKey)) return;
  if (payload.run_id) seenSimulationRunLogKeys.add(runKey);
  const messageCode = payload.message_code || "runtime.simulation_run_finished";
  const stopReason = displaySimulationStopReason(payload);
  const messageParams = messageCode === "runtime.simulation_run_finished"
    ? normalizeSimulationMessageParams(messageCode, { ...(payload.message_params || {}), stop_reason: stopReason })
    : normalizeSimulationMessageParams(messageCode, payload.message_params || { stop_reason: stopReason });
  appendLog(
    simulationLogLevelForStatus(payload.status),
    "",
    payload.created_at,
    {
      messageCode,
      messageParams,
      fallbackMessage: simulationRunFallbackMessage(payload),
    },
  );
}

function applySimulationEvent(event = {}) {
  const eventId = Number(event.event_id || 0);
  if (eventId > 0) {
    if (seenSimulationEventIds.has(eventId)) return;
    seenSimulationEventIds.add(eventId);
    latestSimulationEventId = Math.max(latestSimulationEventId, eventId);
  }
  const payload = {
    ...(event.payload || {}),
    event_id: eventId || undefined,
    run_id: event.run_id || event.payload?.run_id,
    status: event.payload?.status || event.status,
    session_kind: event.payload?.session_kind,
    message_code: event.message_code || event.payload?.message_code,
    message_params: normalizeSimulationMessageParams(
      event.message_code || event.payload?.message_code,
      event.message_params || event.payload?.message_params || {},
    ),
    created_at: event.created_at || event.payload?.created_at,
  };
  const eventType = event.event_type || payload.event_type;
  if (eventType === "simulation_round_progress") {
    updateExecutionStats(payload);
  } else if (payload.event_type === "simulation_run" || eventType === "simulation_finished" || eventType === "simulation_started") {
    updateSimulationRunStatsFromPayload(payload);
  }
  appendLog(event.level || simulationLogLevelForStatus(payload.status), "", payload.created_at, {
    messageCode: payload.message_code,
    messageParams: payload.message_params,
    fallbackMessage: payload.message || simulationRunFallbackMessage(payload),
  });
}

function stopSimulationRunPolling() {
  if (activeSimulationPoller) {
    clearInterval(activeSimulationPoller);
    activeSimulationPoller = null;
  }
}

function startSimulationRunPolling(runId) {
  if (!runId) return;
  activeSimulationRunId = runId;
  stopSimulationRunPolling();
  activeSimulationPoller = setInterval(() => {
    pollSimulationRunUpdates(runId).catch((error) => {
      appendLog("warn", "", undefined, {
        messageCode: "runtime.simulation_run_failed",
        messageParams: { error: userVisibleErrorMessage(error) },
      });
    });
  }, 2000);
}

async function pollSimulationRunUpdates(runId = activeSimulationRunId) {
  if (!runId) return null;
  const updates = await request(`/simulation/run/${encodeURIComponent(runId)}/updates?after_event_id=${latestSimulationEventId}`);
  if (updates.run_id && updates.run_id !== runId) return updates;
  (updates.events || []).forEach(applySimulationEvent);
  if (updates.account) {
    renderSimulationAccount(updates.account);
  }
  const resultPayload = updates.result || updates.run?.result || {};
  const summaryPayload = {
    ...resultPayload,
    run_id: updates.run_id || updates.run?.run_id || runId,
    status: updates.status || updates.run?.status || resultPayload.status,
    stage: updates.stage || updates.run?.stage || resultPayload.stage,
    stop_reason: updates.stop_reason || updates.run?.stop_reason || resultPayload.stop_reason,
    session_kind: updates.session_kind || updates.run?.session_kind || resultPayload.session_kind,
    symbol: updates.symbol || updates.run?.symbol || resultPayload.symbol,
    rounds_completed: updates.rounds_completed ?? updates.run?.rounds_completed ?? resultPayload.rounds_completed,
    rounds_total: updates.rounds_total ?? updates.run?.rounds_total ?? resultPayload.rounds_total,
    message_code: resultPayload.message_code,
    message_params: resultPayload.message_params,
  };
  updateSimulationRunStatsFromPayload(summaryPayload);
  latestSimulationEventId = Math.max(latestSimulationEventId, Number(updates.latest_event_id || 0));
  if (isTerminalSimulationStatus(summaryPayload.status)) {
    activeSimulationRunId = null;
    simulationRunInFlight = false;
    simulationAbortInFlight = false;
    stopSimulationRunPolling();
    await refreshSimulationHistory();
  }
  refreshExecutionActionButtons();
  return updates;
}

async function refreshActiveSimulationRun() {
  const active = await request("/simulation/run/active");
  if (!active.active || !active.run_id) {
    activeSimulationRunId = null;
    if (!simulationAbortInFlight) simulationRunInFlight = false;
    stopSimulationRunPolling();
    refreshExecutionActionButtons();
    return active;
  }
  activeSimulationRunId = active.run_id;
  simulationRunInFlight = true;
  (active.events || []).forEach(applySimulationEvent);
  if (active.account) {
    renderSimulationAccount(active.account);
  } else {
    await refreshSimulationAccount();
  }
  updateSimulationRunStatsFromPayload({
    ...(active.result || active.run?.result || {}),
    run_id: active.run_id,
    status: active.status,
    stage: active.stage,
    session_kind: active.session_kind,
    symbol: active.symbol,
    rounds_completed: active.rounds_completed,
    rounds_total: active.rounds_total,
  });
  latestSimulationEventId = Math.max(latestSimulationEventId, Number(active.latest_event_id || 0));
  startSimulationRunPolling(active.run_id);
  refreshExecutionActionButtons();
  return active;
}

function seedSimulationRunStats(requestPayload = {}) {
  updateSimulationRunStatsFromPayload(
    {
      status: "running",
      session_kind: requestPayload.session_kind || executionMode,
      symbol: requestPayload.symbol || activeSymbol,
      rounds_total: Number(requestPayload.round_count || 0),
      rounds_completed: 0,
      total_notional: readStatNumber("statTotalNotional"),
      notional_per_round: readStatNumber("statPerRound"),
      last_qty: readStatNumber("statLastQty"),
      min_notional: Number(currentSymbolInfo.min_notional || 0),
      message_code: "runtime.simulation_run_started",
      message_params: {
        mode: formatModeLabel(requestPayload.session_kind || executionMode),
        symbol: requestPayload.symbol || activeSymbol,
        round_count: Number(requestPayload.round_count || 0),
      },
    },
    requestPayload,
  );
}

function currentExecutionPositions() {
  return appPage === "simulation" ? currentSimulationPositions : currentPositions;
}

function positionQty(symbol, positionSide) {
  return currentExecutionPositions()
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

function updateCloseValidationHint({ canCreate, canSimulate = false, message, tone }) {
  closeValidationHint.className = `validation-hint ${tone || ""}`;
  closeValidationHint.textContent = message;
  modeHintStateByMode.set("paired_close", { canCreate, canSimulate });
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
  if (activeRealSessionList().length > 0) return true;
  return false;
}

function refreshExecutionActionButtons() {
  const runtimeState = currentExecutionLockState();
  const locked = runtimeState.requestInFlight || runtimeState.hasActiveSimulation;
  const simButtons = Array.isArray(typeof simulationRunButtons === "undefined" ? null : simulationRunButtons)
    ? simulationRunButtons
    : [simulateBtn].filter(Boolean);
  setExecutionInputLock(locked);
  [
    saveSimSettingsBtn,
    resetSimAccountBtn,
    typeof resetSimAccountInlineBtn !== "undefined" ? resetSimAccountInlineBtn : null,
    clearSimHistoryBtn,
  ].forEach((button) => {
    if (button) button.disabled = runtimeState.hasActiveSimulation || runtimeState.requestInFlight;
  });
  syncPrecheckFreshnessState("paired_open");
  syncPrecheckFreshnessState("paired_close");
  syncPrecheckFreshnessState("single_open");
  syncPrecheckFreshnessState("single_close");

  createBtn.textContent = DEFAULT_REAL_ACTION_LABELS.paired_open;
  createCloseBtn.textContent = DEFAULT_REAL_ACTION_LABELS.paired_close;
  createSingleOpenBtn.textContent = DEFAULT_REAL_ACTION_LABELS.single_open;
  createSingleCloseBtn.textContent = DEFAULT_REAL_ACTION_LABELS.single_close;
  simButtons.forEach((button) => {
    button.textContent = DEFAULT_SIMULATE_LABEL;
  });

  const assignRealButtonState = (mode) => {
    const button = executionButtonForMode(mode);
    const hintState = modeHintStateByMode.get(mode) || { canCreate: false, canSimulate: false };
    const freshness = precheckFreshnessStateByMode.get(mode) || { fresh: false };
    const baseState = resolveActionAvailability(hintState, { requestInFlight: runtimeState.requestInFlight, hasActiveSession: false });
    if (runtimeState.hasActiveSimulation) {
      button.disabled = true;
      return;
    }
    const sameSymbolActive = hasActiveRealSessionForSymbol(symbolForMode(mode));
    button.disabled = sameSymbolActive || !(baseState.canCreate && freshness.fresh);
  };

  assignRealButtonState("paired_open");
  assignRealButtonState("paired_close");
  assignRealButtonState("single_open");
  assignRealButtonState("single_close");

  const simulationPayload = typeof buildSimulationRunPayload === "function" ? buildSimulationRunPayload(executionMode) : {};
  const simulationReady = typeof canRunSimulation === "function" ? canRunSimulation(executionMode, simulationPayload) : true;
  const currentAppPage = typeof appPage === "undefined" ? "simulation" : appPage;
  if (runtimeState.hasActiveSimulation) {
    simButtons.forEach((button) => {
      button.textContent = simulationAbortInFlight ? EXECUTION_ABORTING_LABEL : SIMULATION_TERMINATE_LABEL;
      button.disabled = simulationAbortInFlight;
    });
  } else if (runtimeState.hasActiveSession) {
    simButtons.forEach((button) => {
      button.disabled = true;
    });
  } else {
    simButtons.forEach((button) => {
      button.disabled = currentAppPage !== "simulation" || !simulationReady || runtimeState.requestInFlight;
    });
  }

  renderExecutionSummaryBanner();
  renderRiskBanner();
  renderRecoverableSessionBanner();
  renderActiveRealSessionsPanel();
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
      updateCloseValidationHint({ canCreate, canSimulate, tone, message });
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
  return currentExecutionPositions().filter((position) => position.symbol === activeSymbol && Number(position.qty || 0) > 0);
}

function resolveSymbolLeverage(symbol) {
  const matching = currentExecutionPositions().filter((position) => position.symbol === symbol && Number(position.leverage || 0) > 0);
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
  const perLegNotionalPerRound = notionalPerRound / 2;
  const roundQty = latestReferencePrice > 0 ? perLegNotionalPerRound / latestReferencePrice : 0;
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

  if (typeof appPage !== "undefined" && appPage === "simulation") {
    latestPrecheckResultByMode.delete("paired_open");
    latestResolvedPrecheckPayloadByMode.delete("paired_open");
    if (margin <= 0 || leverage <= 0 || rounds <= 0 || roundQty <= 0) {
      clearHintStateForMode("paired_open");
      return;
    }
    const estimatedMargin = margin;
    const estimatedFee = totalNotional * Math.max(Number(latestSimulationMakerFeeRate || 0), 0);
    const estimatedRequired = estimatedMargin + estimatedFee;
    if (latestSimulationAvailableBalance !== null && estimatedRequired > latestSimulationAvailableBalance) {
      updateOpenValidationHint({
        canCreate: false,
        canSimulate: false,
        tone: "error",
        message: copyOrDefault("console.simulation.open_required_exceeds_available", "模拟账户预计需要 {required}（保证金 {margin} + 手续费 {fee}），超过可用余额 {available}。", {
          required: formatMoney(estimatedRequired),
          margin: formatMoney(estimatedMargin),
          fee: formatMoney(estimatedFee),
          available: formatMoney(latestSimulationAvailableBalance),
        }),
      });
      return;
    }
    updateOpenValidationHint({
      canCreate: false,
      canSimulate: true,
      tone: "success",
      message: copyOrDefault("console.simulation.open_precheck_ok", "模拟账户可用余额 {available}，预计保证金 {margin}，预估手续费 {fee}，可以模拟开单。", {
        available: formatMoney(latestSimulationAvailableBalance ?? 0),
        margin: formatMoney(estimatedMargin),
        fee: formatMoney(estimatedFee),
      }),
    });
    return;
  }

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

  if (typeof appPage !== "undefined" && appPage === "simulation") {
    latestPrecheckResultByMode.delete("paired_close");
    latestResolvedPrecheckPayloadByMode.delete("paired_close");
    const minNotional = Number(currentSymbolInfo.min_notional || 0);
    if (closeQty <= 0 || rounds <= 0 || perRoundQty <= 0) {
      updateCloseValidationHint({
        canCreate: false,
        canSimulate: false,
        tone: "error",
        message: copyOrDefault("console.simulation.close_qty_required", "请输入模拟平仓数量和轮次。"),
      });
      return;
    }
    if (maxCloseableQty <= 0) {
      updateCloseValidationHint({
        canCreate: false,
        canSimulate: false,
        tone: "error",
        message: copyOrDefault("console.simulation.paired_close_no_position", "当前模拟持仓没有可双向平仓数量，无法模拟平仓。"),
      });
      return;
    }
    if (closeQty > maxCloseableQty) {
      updateCloseValidationHint({
        canCreate: false,
        canSimulate: false,
        tone: "error",
        message: copyOrDefault("console.simulation.paired_close_qty_exceeds_position", "模拟平仓数量 {qty} 超过可双向平仓数量 {available}，无法模拟平仓。", {
          qty: formatNumber(closeQty, 6),
          available: formatNumber(maxCloseableQty, 6),
        }),
      });
      return;
    }
    if (minNotional > 0 && perRoundNotional < minNotional) {
      updateCloseValidationHint({
        canCreate: false,
        canSimulate: false,
        tone: "error",
        message: copyOrDefault("console.simulation.paired_close_below_min_notional", "每轮名义平仓金额 {per_round} 低于最小下单金额 {min_notional}，无法模拟平仓。", {
          per_round: formatMoney(perRoundNotional),
          min_notional: formatMoney(minNotional),
        }),
      });
      return;
    }
    updateCloseValidationHint({
      canCreate: false,
      canSimulate: true,
      tone: "success",
      message: copyOrDefault("console.simulation.paired_close_precheck_ok", "当前可双向平仓数量 {available}，每轮名义平仓金额 {per_round}，可以模拟平仓。", {
        available: formatNumber(maxCloseableQty, 6),
        per_round: formatMoney(perRoundNotional),
      }),
    });
    return;
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
  const eventType = String(event.event_type || "");
  const registryEntry = I18N_REGISTRIES.events?.[eventType]
    || (eventType.endsWith("_market_fallback") ? I18N_REGISTRIES.events?.market_fallback_applied : null);
  if (!registryEntry?.key) return null;
  const params = {
    ...payload,
    event_type: eventType,
    label: payload.label || eventType.replace(/_market_fallback$/, ""),
    final_alignment_status: payload.final_alignment_status ? formatAlignmentStatus(payload.final_alignment_status) : payload.final_alignment_status,
    order_class_label: payload.order_class === "manual"
      ? copyOrDefault("events.order_class.manual", "manual")
      : copyOrDefault("events.order_class.system", "system"),
  };
  return {
    level: registryEntry.level || "info",
    message: formatCopy(registryEntry.key, params),
  };
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
    activeRealSessions.delete(session.session_id);
    setSessionAbortPending(session.session_id, false);
  } else {
    upsertActiveRealSession(session);
  }
  syncFocusedAbortFlag();
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
  const snapshot = {
    page: "real",
    status: session.status || "idle",
    mode: session.session_kind || executionMode,
    roundsCompleted: regularTerminalRounds,
    roundsTotal: session.round_count || 0,
    carryoverQty: resolveResidualQty(session),
    finalAlignmentStatus: session.final_alignment_status,
    lastQty: currentPlannedQty || 0,
    residualSide: latestResidualSideLabel || "--",
    abortRequested: sessionAbortInFlight || session.status === "aborting",
  };
  executionStatsByPage.real = snapshot;
  if (normalizeAppPage(appPage) === "real") {
    renderExecutionStatsSnapshot(snapshot);
  }
  updateExecutionSummary(buildExecutionSummary(session, {
    roundsCompleted: regularTerminalRounds,
    roundsTotal: session.round_count || 0,
    abortRequested: sessionAbortInFlight || session.status === "aborting",
  }), "real");
  refreshExecutionActionButtons();
}

function focusNextActiveSession(excludeSessionId = null) {
  const next = activeRealSessionList().find((session) => session.session_id !== excludeSessionId);
  if (next) {
    startSessionPolling(next.session_id);
    return true;
  }
  stopSessionPolling();
  return false;
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
    seenSessionEventIds.clear();
    syncFocusedAbortFlag();
    if (!hasActiveSimulationRun()) {
      updateExecutionSummary(null, "real");
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

async function loadActiveSessionSnapshot(sessionId = activeSessionId) {
  const requestedSessionId = sessionId;
  if (!requestedSessionId) return false;
  const session = await request(`/sessions/${encodeURIComponent(requestedSessionId)}`);
  if (activeSessionId !== requestedSessionId) return false;
  activeSessionState = session;
  latestSessionEventId = Array.isArray(session.events)
    ? session.events.reduce((maxId, event) => Math.max(maxId, Number(event.event_id || 0)), 0)
    : 0;
  updateRealSessionStats(session);
  renderSessionEvents(session.events || []);
  if (isTerminalSession(session.status)) {
    focusNextActiveSession(requestedSessionId);
  }
  return true;
}

async function pollActiveSession(sessionId = activeSessionId) {
  const requestedSessionId = sessionId;
  if (!requestedSessionId || activeSessionId !== requestedSessionId) return;
  try {
    if (!activeSessionState || activeSessionState.session_id !== requestedSessionId) {
      await loadActiveSessionSnapshot(requestedSessionId);
      return;
    }
    const payload = await request(`/sessions/${encodeURIComponent(requestedSessionId)}/updates?after_event_id=${latestSessionEventId}`);
    if (activeSessionId !== requestedSessionId || !activeSessionState || activeSessionState.session_id !== requestedSessionId) return;
    activeSessionState = {
      ...activeSessionState,
      ...(payload.session || {}),
      rounds: mergeChangedRounds(activeSessionState.rounds || [], payload.changed_rounds || []),
    };
    latestSessionEventId = Math.max(latestSessionEventId, Number(payload.latest_event_id || 0));
    updateRealSessionStats(activeSessionState);
    renderSessionEvents(payload.events || []);
    if (isTerminalSession(activeSessionState.status)) {
      focusNextActiveSession(requestedSessionId);
    }
  } catch (error) {
    if (activeSessionId !== requestedSessionId) return;
    try {
      await loadActiveSessionSnapshot(requestedSessionId);
    } catch (fallbackError) {
      if (activeSessionId !== requestedSessionId) return;
      appendLog("error", "", undefined, {
        messageCode: "runtime.session_refresh_failed",
        messageParams: { error: userVisibleErrorMessage(fallbackError || error) },
      });
      focusNextActiveSession(requestedSessionId);
    }
  }
}

function startSessionPolling(sessionId, sessionPayload = null) {
  stopSessionPolling(false);
  if (sessionPayload) {
    upsertActiveRealSession(sessionPayload);
  }
  activeSessionId = sessionId;
  activeSessionState = sessionPayload || activeRealSessions.get(sessionId) || null;
  latestSessionEventId = 0;
  seenSessionEventIds.clear();
  syncFocusedAbortFlag();
  recoverableSessionState = null;
  renderRecoverableSessionBanner();
  ensureActiveRealSessionsPolling();
  refreshExecutionActionButtons();
  loadActiveSessionSnapshot(sessionId).catch((error) => {
    if (activeSessionId !== sessionId) return;
    appendLog("error", "", undefined, {
      messageCode: "runtime.session_refresh_failed",
      messageParams: { error: userVisibleErrorMessage(error) },
    });
    focusNextActiveSession(sessionId);
  });
  activeSessionPoller = setInterval(() => pollActiveSession(sessionId), 2000);
}

async function refreshActiveRealSessions({ focusIfMissing = true } = {}) {
  const sessions = await request("/sessions");
  syncActiveRealSessions(sessions || []);
  renderActiveRealSessionsPanel();
  if (focusIfMissing && (!activeSessionId || !activeRealSessions.has(activeSessionId))) {
    const next = activeRealSessionList()[0];
    if (next) {
      startSessionPolling(next.session_id);
    } else {
      stopSessionPolling();
    }
  }
  refreshExecutionActionButtons();
  return sessions;
}

function ensureActiveRealSessionsPolling() {
  if (activeSessionsPoller) return;
  activeSessionsPoller = setInterval(() => {
    refreshActiveRealSessions().catch((error) => {
      appendLog("error", "", undefined, {
        messageCode: "runtime.session_refresh_failed",
        messageParams: { error: userVisibleErrorMessage(error) },
      });
    });
  }, 5000);
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

function applyAppPageChrome(page) {
  appPage = normalizeAppPage(page);
  appRoot?.classList.toggle("app-view-real", appPage === "real");
  appRoot?.classList.toggle("app-view-simulation", appPage === "simulation");
  navRealBtn?.classList.toggle("active", appPage === "real");
  navSimulationBtn?.classList.toggle("active", appPage === "simulation");
}

function setAppPage(page) {
  const nextPage = normalizeAppPage(page);
  executionPageSwitchQueue = executionPageSwitchQueue
    .catch(() => false)
    .then(() => applyAppPage(nextPage));
  return executionPageSwitchQueue;
}

async function applyAppPage(page) {
  const nextPage = normalizeAppPage(page);
  const previousPage = appPage;
  initializeExecutionPageFormStates();
  if (!restoringExecutionPageState) {
    saveExecutionPageFormState(appPage);
  }
  const previousState = cloneExecutionFormState(executionPageFormStates[previousPage]);
  applyAppPageChrome(nextPage);
  let applied = false;
  try {
    applied = await applyExecutionFormState(executionPageFormStates[appPage]);
  } catch (error) {
    appendLog("error", "", undefined, {
      messageCode: "runtime.symbol_switch_failed",
      messageParams: { symbol: symbolForExecutionFormState(executionPageFormStates[appPage]), error: userVisibleErrorMessage(error) },
    });
  }
  if (!applied) {
    applyAppPageChrome(previousPage);
    restoringExecutionPageState = true;
    try {
      applyExecutionFormFields(previousState);
    } finally {
      restoringExecutionPageState = false;
    }
    appendLog("error", "", undefined, {
      messageCode: "runtime.symbol_switch_failed",
      messageParams: { symbol: symbolForExecutionFormState(executionPageFormStates[nextPage]), error: "symbol switch failed" },
    });
    recalculateMode(previousState.mode);
    refreshExecutionActionButtons();
    return false;
  }
  refreshExecutionActionButtons();
  renderCurrentPageExecutionStats();
  renderExecutionSummaryBanner();
  renderRiskBanner();
  if (appPage === "simulation") {
    openSse();
    refreshSimulationAccount();
    refreshSimulationHistory();
  } else {
    maybeScheduleCurrentModePrecheck("page_switch");
  }
  return true;
}

function buildSimulationRunPayload(mode = executionMode) {
  switch (normalizeSessionKind(mode)) {
    case "paired_close":
      return {
        session_kind: "paired_close",
        symbol: closeExecutionSymbol.value,
        trend_bias: document.getElementById("closeTrend").value,
        close_qty: optionalPositiveValue(document.getElementById("closeQty").value),
        round_count: Number(document.getElementById("closeRounds").value),
        round_interval_seconds: Number(document.getElementById("closeRoundIntervalSeconds").value),
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
        round_interval_seconds: Number(document.getElementById("singleOpenRoundIntervalSeconds").value),
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
        round_interval_seconds: Number(document.getElementById("singleCloseRoundIntervalSeconds").value),
      };
    }
    default:
      return {
        session_kind: "paired_open",
        symbol: executionSymbol.value,
        trend_bias: document.getElementById("trend").value,
        open_amount: optionalPositiveValue(document.getElementById("calcMargin").value),
        leverage: Number(document.getElementById("leverage").value),
        round_count: Number(document.getElementById("calcRounds").value),
        round_interval_seconds: Number(document.getElementById("roundIntervalSeconds").value),
      };
  }
}

function renderSimulationAccount(account) {
  if (!account) return;
  const totals = account.totals || {};
  currentSimulationPositions = Array.isArray(account.positions) ? account.positions : [];
  latestSimulationAvailableBalance = Number(totals.available_balance ?? 0);
  if (simEquity) {
    simEquity.textContent = formatNumber(totals.equity, 2);
    applyMetricTone(simEquity, totals.equity);
  }
  if (simMarginUsed) {
    simMarginUsed.textContent = formatNumber(totals.margin, 2);
    applyMetricTone(simMarginUsed, totals.margin);
  }
  if (simAvailableBalance) {
    simAvailableBalance.textContent = formatNumber(totals.available_balance, 2);
    applyMetricTone(simAvailableBalance, totals.available_balance);
  }
  if (simUnrealizedPnl) {
    simUnrealizedPnl.textContent = formatNumber(totals.unrealized_pnl, 2);
    applyMetricTone(simUnrealizedPnl, totals.unrealized_pnl);
  }
  const settings = account.settings || {};
  latestSimulationMakerFeeRate = Number(settings.maker_fee_rate || 0);
  latestSimulationTakerFeeRate = Number(settings.taker_fee_rate || 0);
  if (simInitialBalance && document.activeElement !== simInitialBalance) simInitialBalance.value = settings.initial_balance || "7000";
  if (simMakerFee && document.activeElement !== simMakerFee) simMakerFee.value = settings.maker_fee_rate || "0";
  if (simTakerFee && document.activeElement !== simTakerFee) simTakerFee.value = settings.taker_fee_rate || "0.0005";
  renderSimulationPositions(currentSimulationPositions);
  refreshSingleOpenOrderOptions();
  refreshSingleClosePositionOptions();
  if (appPage === "simulation") {
    recalculateMode(executionMode);
  }
}

function renderSimulationPositions(positions) {
  if (!simPositionsList) return;
  if (simPositionsCount) simPositionsCount.textContent = String(positions.length);
  simPositionsList.replaceChildren();
  if (!positions.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.style.minHeight = "160px";
    empty.textContent = copyOrDefault("console.simulation.positions_empty", "暂无模拟持仓");
    simPositionsList.appendChild(empty);
    return;
  }
  positions.forEach((position) => {
    const item = document.createElement("div");
    item.className = "position-row";
    const sideClass = String(position.position_side || "").toLowerCase() === "short" ? "short" : "long";
    const leverageText = Number(position.leverage || 0) > 0 ? `${position.leverage}x` : "--";
    const qty = Number(position.qty || 0) || 0;
    const entryPrice = Number(position.entry_price || 0) || 0;
    const notional = Number(position.notional || 0) || qty * entryPrice;
    const pnlValue = Number(position.unrealized_pnl || 0);
    const pnlClass = pnlValue > 0 ? "positive" : (pnlValue < 0 ? "negative" : "zero");

    const head = document.createElement("div");
    head.className = "position-row-head";
    const symbol = document.createElement("div");
    symbol.className = "position-symbol";
    symbol.append(document.createTextNode(position.symbol || "--"));
    const leverage = document.createElement("span");
    leverage.className = "position-leverage-inline";
    leverage.textContent = leverageText;
    symbol.appendChild(leverage);
    const side = document.createElement("span");
    side.className = `position-side ${sideClass}`;
    side.textContent = String(position.position_side || "").toUpperCase() === "SHORT"
      ? copyOrDefault("console.position_side.short", "空")
      : copyOrDefault("console.position_side.long", "多");
    head.append(symbol, side);

    const meta = document.createElement("div");
    meta.className = "position-meta";
    [
      [copyOrDefault("console.position_fields.qty", "数量"), formatNumber(position.qty || 0, 6)],
      [copyOrDefault("console.position_fields.notional", "名义价值"), formatNumber(notional, 2)],
      [copyOrDefault("console.position_fields.entry_price", "开仓均价"), formatNumber(position.entry_price || 0, 2)],
      [copyOrDefault("console.position_fields.mark_price", "标记价格"), position.mark_price ? formatDisplayPrice(position.mark_price, 2) : "--"],
      [copyOrDefault("console.position_fields.unrealized_pnl", "未实现盈亏"), formatNumber(position.unrealized_pnl || 0, 4), pnlClass],
      [copyOrDefault("console.simulation.position_margin", "保证金"), formatNumber(position.margin || 0, 4)],
    ].forEach(([label, value, valueClass]) => {
      const cell = document.createElement("div");
      cell.appendChild(document.createTextNode(label));
      const valueNode = document.createElement("strong");
      valueNode.className = `mono${valueClass ? ` ${valueClass}` : ""}`;
      valueNode.textContent = String(value ?? "--");
      cell.appendChild(valueNode);
      meta.appendChild(cell);
    });
    item.append(head, meta);
    simPositionsList.appendChild(item);
  });
}

function renderSimulationHistory(history) {
  if (!simHistoryList) return;
  const items = history?.items || [];
  if (simHistoryCount) simHistoryCount.textContent = String(history?.total || items.length || 0);
  simHistoryList.innerHTML = "";
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.style.minHeight = "140px";
    empty.textContent = copyOrDefault("console.simulation.history_empty", "暂无模拟历史");
    simHistoryList.appendChild(empty);
    return;
  }
  items.forEach((item) => {
    const result = item.result || {};
    const isReset = item.event_type === "account_reset";
    const canReplay = item.event_type === "simulation_run";
    const node = document.createElement("div");
    node.className = "history-item";
    const header = document.createElement("div");
    header.className = "history-item-header";
    const title = document.createElement("strong");
    title.textContent = `${isReset ? copyOrDefault("console.simulation.account_reset", "账户重置") : formatModeLabel(item.session_kind)} · ${item.symbol || "--"}`;
    const time = document.createElement("div");
    time.className = "history-item-time";
    time.textContent = new Date(item.created_at).toLocaleString(APP_LOCALE, { hour12: false, timeZone: APP_TIMEZONE });
    header.append(title, time);
    const status = document.createElement("div");
    status.textContent = copyOrDefault("console.simulation.history_status", "状态：{status} / {stop_reason}", {
      status: item.status || "--",
      stop_reason: formatStopReason(item.stop_reason) || item.stop_reason || "--",
    });
    const fill = document.createElement("div");
    const fillQty = document.createElement("span");
    fillQty.className = "mono";
    fillQty.textContent = String(result.filled_qty || "0");
    const fee = document.createElement("span");
    fee.className = "mono";
    fee.textContent = String(result.fee || "0");
    fill.append(
      document.createTextNode(copyOrDefault("console.simulation.history_fill_prefix", "成交：")),
      fillQty,
      document.createTextNode(copyOrDefault("console.simulation.history_fee_prefix", "  手续费：")),
      fee,
    );
    node.append(header, status, fill);
    if (canReplay) {
      const actions = document.createElement("div");
      actions.className = "history-actions";
      const rerun = document.createElement("button");
      rerun.className = "inline-btn secondary";
      rerun.dataset.rerun = item.run_id;
      rerun.type = "button";
      rerun.textContent = copyOrDefault("console.action.rerun_current_market", "当前行情重跑");
      const copy = document.createElement("button");
      copy.className = "inline-btn success";
      copy.dataset.copyReal = item.run_id;
      copy.type = "button";
      copy.textContent = copyOrDefault("console.action.copy_to_real", "复制到实盘");
      actions.append(rerun, copy);
      node.appendChild(actions);
    }
    simHistoryList.appendChild(node);
  });
}

async function refreshSimulationAccount() {
  try {
    renderSimulationAccount(await request("/simulation/account"));
  } catch (error) {
    appendLog("error", "", undefined, { messageCode: "runtime.simulation_run_failed", messageParams: { error: userVisibleErrorMessage(error) } });
  }
}

async function refreshSimulationHistory() {
  try {
    renderSimulationHistory(await request("/simulation/history?page=1&page_size=20"));
  } catch (error) {
    appendLog("error", "", undefined, { messageCode: "runtime.simulation_run_failed", messageParams: { error: userVisibleErrorMessage(error) } });
  }
}

async function syncSimulationPayloadSymbolContext(payload = {}) {
  const symbol = normalizeSymbol(payload.symbol || "");
  if (!symbol || symbol === activeSymbol) {
    return true;
  }
  const switched = await switchSymbol(symbol, connectionToggle.checked);
  if (!switched) {
    throw new Error(copyOrDefault("runtime.symbol_switch_failed", "交易对切换失败"));
  }
  return true;
}

async function applySimulationPayloadToForm(payload = {}) {
  const mode = normalizeSessionKind(payload.session_kind);
  await syncSimulationPayloadSymbolContext(payload);
  setExecutionMode(mode);
  if (mode === "paired_open") {
    executionSymbol.value = payload.symbol || executionSymbol.value;
    document.getElementById("trend").value = payload.trend_bias || "long";
    document.getElementById("calcMargin").value = payload.open_amount || document.getElementById("calcMargin").value;
    document.getElementById("leverage").value = payload.leverage || document.getElementById("leverage").value;
    document.getElementById("calcRounds").value = payload.round_count || document.getElementById("calcRounds").value;
    document.getElementById("roundIntervalSeconds").value = payload.round_interval_seconds ?? document.getElementById("roundIntervalSeconds").value;
  } else if (mode === "paired_close") {
    closeExecutionSymbol.value = payload.symbol || closeExecutionSymbol.value;
    document.getElementById("closeTrend").value = payload.trend_bias || "long";
    document.getElementById("closeQty").value = payload.close_qty || document.getElementById("closeQty").value;
    document.getElementById("closeRounds").value = payload.round_count || document.getElementById("closeRounds").value;
    document.getElementById("closeRoundIntervalSeconds").value = payload.round_interval_seconds ?? document.getElementById("closeRoundIntervalSeconds").value;
  } else if (mode === "single_open") {
    document.getElementById("singleOpenExecutionSymbol").value = payload.symbol || activeSymbol;
    document.getElementById("singleOpenMode").value = payload.open_mode || "regular";
    document.getElementById("singleOpenOrder").value = payload.selected_position_side || "LONG";
    document.getElementById("singleOpenQty").value = payload.open_qty || document.getElementById("singleOpenQty").value;
    document.getElementById("singleOpenLeverage").value = payload.leverage || document.getElementById("singleOpenLeverage").value;
    document.getElementById("singleOpenRounds").value = payload.round_count || document.getElementById("singleOpenRounds").value;
    document.getElementById("singleOpenRoundIntervalSeconds").value = payload.round_interval_seconds ?? document.getElementById("singleOpenRoundIntervalSeconds").value;
  } else if (mode === "single_close") {
    document.getElementById("singleCloseExecutionSymbol").value = payload.symbol || activeSymbol;
    document.getElementById("singleCloseMode").value = payload.close_mode || "regular";
    document.getElementById("singleCloseOrder").value = payload.selected_position_side || document.getElementById("singleCloseOrder").value;
    document.getElementById("singleCloseQty").value = payload.close_qty || document.getElementById("singleCloseQty").value;
    document.getElementById("singleCloseRounds").value = payload.round_count || document.getElementById("singleCloseRounds").value;
    document.getElementById("singleCloseRoundIntervalSeconds").value = payload.round_interval_seconds ?? document.getElementById("singleCloseRoundIntervalSeconds").value;
  }
  recalculateMode(mode);
  maybeScheduleCurrentModePrecheck("mode_switch");
  refreshExecutionActionButtons();
}

async function requestSimulationRunForCurrentMode() {
  if (hasActiveSimulationRun()) {
    await requestSimulationAbort();
    return;
  }
  simulationRunInFlight = true;
  refreshExecutionActionButtons();
  try {
    openSse();
    await refreshSymbolInfo(activeSymbol);
    const requestPayload = buildSimulationRunPayload(executionMode);
    seedSimulationRunStats(requestPayload);
    const payload = await request("/simulation/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(requestPayload),
    });
    if (typeof activeSimulationRunId !== "undefined") activeSimulationRunId = payload.run_id || null;
    updateSimulationRunStatsFromPayload(payload, requestPayload);
    if (payload.run_id) {
      appendSimulationRunLog(payload, requestPayload);
    }
    if (isTerminalSimulationStatus(payload.status)) {
      simulationAbortInFlight = false;
    }
    if (payload.run_id && typeof startSimulationRunPolling === "function" && typeof pollSimulationRunUpdates === "function") {
      startSimulationRunPolling(payload.run_id);
      await pollSimulationRunUpdates(payload.run_id);
    } else if (!payload.run_id) {
      appendSimulationRunLog(payload, requestPayload);
    }
    await refreshSimulationAccount();
    await refreshSimulationHistory();
  } catch (error) {
    appendLog("error", "", undefined, {
      messageCode: "runtime.simulation_run_failed",
      messageParams: { error: userVisibleErrorMessage(error) },
    });
  } finally {
    if (typeof activeSimulationRunId === "undefined" || !activeSimulationRunId) {
      simulationRunInFlight = false;
    }
    refreshExecutionActionButtons();
  }
}

async function copySimulationRunToRealForm(runId) {
  const detail = await request(`/simulation/history/${encodeURIComponent(runId)}`);
  const payload = detail.request || {};
  const mode = normalizeSessionKind(payload.session_kind);
  await setAppPage("real");
  await syncSimulationPayloadSymbolContext(payload);
  await applySimulationPayloadToForm({ ...payload, session_kind: mode });
  appendLog("success", "", undefined, { messageCode: "runtime.simulation_copied_to_real_form" });
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
    if (payload.run_id) {
      activeSimulationRunId = payload.run_id;
      startSimulationRunPolling(payload.run_id);
      await pollSimulationRunUpdates(payload.run_id);
    }
    simulationAbortInFlight = false;
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

async function requestSessionAbort(sessionId = activeSessionId) {
  if (!sessionId || sessionAbortInFlightIds.has(sessionId)) return;
  if (!confirmSessionAbort()) return;
  setSessionAbortPending(sessionId, true);
  refreshExecutionActionButtons();
  try {
    const payload = await request(`/sessions/${encodeURIComponent(sessionId)}/abort`, { method: "POST" });
    appendLog("warn", "", undefined, {
      messageCode: payload.message_code || "runtime.session_abort_requested",
      messageParams: payload.message_params,
    });
    if (!payload.requested || isTerminalSession(payload.status)) {
      setSessionAbortPending(sessionId, false);
    }
  } catch (error) {
    appendLog("error", "", undefined, {
      messageCode: "runtime.session_create_failed",
      messageParams: { error: userVisibleErrorMessage(error) },
    });
    setSessionAbortPending(sessionId, false);
  } finally {
    refreshExecutionActionButtons();
  }
}

async function detectRecoverableSession() {
  try {
    const sessions = await request("/sessions");
    syncActiveRealSessions(sessions || []);
    const activeSessions = activeRealSessionList();
    if (activeSessions.length) {
      ensureActiveRealSessionsPolling();
      if (!activeSessionId || !activeRealSessions.has(activeSessionId)) {
        startSessionPolling(activeSessions[0].session_id, activeSessions[0]);
      }
    }
    recoverableSessionState = selectRecoverableSession(sessions || []);
    recoverableSessionDismissed = false;
    renderRecoverableSessionBanner();
    renderActiveRealSessionsPanel();
    refreshExecutionActionButtons();
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
    startSessionPolling(session.session_id, session);
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
  eventSource.addEventListener("simulation_account", (event) => {
    renderSimulationAccount(JSON.parse(event.data));
  });
  eventSource.addEventListener("simulation_run", (event) => {
    const payload = JSON.parse(event.data);
    if (payload?.event_type === "simulation_run" && payload.status) {
      if (payload.run_id && !isTerminalSimulationStatus(payload.status)) {
        activeSimulationRunId = payload.run_id;
        startSimulationRunPolling(payload.run_id);
      }
      updateSimulationRunStatsFromPayload(payload);
      appendSimulationRunLog(payload);
    }
    refreshSimulationAccount();
    if (isTerminalSimulationStatus(payload?.status)) {
      activeSimulationRunId = null;
      stopSimulationRunPolling();
      refreshSimulationHistory();
    }
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
  const symbol = executionSymbol.value || activeSymbol || "BTCUSDC";
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

activeRealSessionsList?.addEventListener("click", async (event) => {
  const abortButton = event.target?.closest?.("button[data-abort-session-id]");
  if (abortButton) {
    await requestSessionAbort(abortButton.dataset.abortSessionId);
    return;
  }
  const focusButton = event.target?.closest?.("button[data-focus-session-id]");
  const item = event.target?.closest?.("[data-session-id]");
  const sessionId = focusButton?.dataset.focusSessionId || item?.dataset.sessionId;
  if (sessionId) {
    startSessionPolling(sessionId, activeRealSessions.get(sessionId) || null);
  }
});

simulationRunButtons.forEach((button) => {
  button.addEventListener("click", requestSimulationRunForCurrentMode);
});

createBtn.addEventListener("click", async () => {
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
      startSessionPolling(payload.session_id, payload);
      refreshActiveRealSessions({ focusIfMissing: false }).catch(() => {});
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
      startSessionPolling(payload.session_id, payload);
      refreshActiveRealSessions({ focusIfMissing: false }).catch(() => {});
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
      startSessionPolling(payload.session_id, payload);
      refreshActiveRealSessions({ focusIfMissing: false }).catch(() => {});
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
      startSessionPolling(payload.session_id, payload);
      refreshActiveRealSessions({ focusIfMissing: false }).catch(() => {});
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
      if (payloadMatchesActiveSymbol(payload)) {
        renderLevels(asksContainer, payload.asks || [], "sell");
        renderLevels(bidsContainer, payload.bids || [], "buy");
        const bestAsk = Number(payload.asks?.[0]?.price || 0);
        const bestBid = Number(payload.bids?.[0]?.price || 0);
        const previousReferencePrice = Number(latestReferencePrice || 0);
        latestReferencePrice = bestAsk > 0 && bestBid > 0 ? (bestAsk + bestBid) / 2 : (bestAsk || bestBid || 0);
        if (symbolInfoReady) {
          if (latestReferencePrice > 0 && previousReferencePrice <= 0) {
            recalculateMode(executionMode);
          }
          maybeScheduleCurrentModePrecheck("price_tick");
        }
        document.getElementById("streamClock").textContent = nowTime();
      }
    }
    if (pendingAccountOverviewPayload) {
      const payload = pendingAccountOverviewPayload;
      pendingAccountOverviewPayload = null;
      if (payloadMatchesActiveSymbol(payload)) {
        renderAccountOverview(payload);
        document.getElementById("streamClock").textContent = nowTime();
      }
    }
    if (pendingLogEntries.length) {
      const entries = pendingLogEntries.splice(0, pendingLogEntries.length);
      entries.forEach((entry) => {
        const eventId = Number(entry.event_id || 0);
        if (entry.run_id && eventId > 0) {
          if (seenSimulationEventIds.has(eventId)) return;
          seenSimulationEventIds.add(eventId);
          latestSimulationEventId = Math.max(latestSimulationEventId, eventId);
        }
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
      const perLegNotionalPerRound = notionalPerRound / 2;
      const roundQty = latestReferencePrice > 0 ? (perLegNotionalPerRound / latestReferencePrice) : 0;
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
  if (appPage === "simulation") {
    latestPrecheckResultByMode.delete(mode);
    latestResolvedPrecheckPayloadByMode.delete(mode);
    recalculateMode(mode);
    return;
  }
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
  if (typeof appPage !== "undefined" && appPage === "simulation") return;
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
  const previousSymbol = activeSymbol;
  const symbolChanged = normalizedSymbol !== previousSymbol;
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
  if (symbolChanged) {
    syncExecutionPageFormStateSymbols(previousSymbol, activeSymbol);
  }
  refreshSingleOpenOrderOptions();
  refreshSingleClosePositionOptions();
  recalculateMode(executionMode);
  const footerStatus = document.getElementById("footerStatus");
  footerStatus.textContent = `${statusLabel(connectionToggle.checked ? "connected" : "disconnected")} ${activeSymbol}`;
  maybeScheduleCurrentModePrecheck("mode_switch");
}

function setExecutionMode(mode) {
  const normalizedMode = normalizeSessionKind(mode);
  executionMode = normalizedMode;
  const currentPageState = executionPageFormStates[normalizeAppPage(appPage)];
  if (currentPageState && !restoringExecutionPageState) {
    currentPageState.mode = normalizedMode;
  }
  Object.entries(modeButtons).forEach(([key, button]) => {
    if (button) button.classList.toggle("active", key === normalizedMode);
  });
  Object.entries(modePanels).forEach(([key, panel]) => {
    if (panel) panel.classList.toggle("hidden", key !== normalizedMode);
  });
  document.getElementById("statMode").textContent = formatModeLabel(normalizedMode);
  const restored = restoreModeValidationSnapshot(normalizedMode);
  if (!restored) {
    recalculateMode(normalizedMode);
  }
  syncPrecheckFreshnessState(normalizedMode);
  refreshExecutionActionButtons();
  if (!restoringExecutionPageState) {
    maybeScheduleCurrentModePrecheck("mode_switch");
  }
}

function setActiveSymbol(symbol, syncInput = true, options = {}) {
  const { suppressRecalc = false, suppressPrecheck = false } = options;
  const normalizedSymbol = normalizeSymbol(symbol);
  const previousSymbol = activeSymbol;
  const symbolChanged = normalizedSymbol !== previousSymbol;
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
  if (symbolChanged) {
    syncExecutionPageFormStateSymbols(previousSymbol, activeSymbol);
  }
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

navRealBtn?.addEventListener("click", () => {
  setAppPage("real").catch((error) => appendLog("error", "", undefined, {
    messageCode: "runtime.symbol_switch_failed",
    messageParams: { symbol: activeSymbol, error: userVisibleErrorMessage(error) },
  }));
});
navSimulationBtn?.addEventListener("click", () => {
  setAppPage("simulation").catch((error) => appendLog("error", "", undefined, {
    messageCode: "runtime.symbol_switch_failed",
    messageParams: { symbol: activeSymbol, error: userVisibleErrorMessage(error) },
  }));
});

saveSimSettingsBtn?.addEventListener("click", async () => {
  try {
    await request("/simulation/account/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        initial_balance: simInitialBalance?.value,
        maker_fee_rate: simMakerFee?.value,
        taker_fee_rate: simTakerFee?.value,
      }),
    });
    await refreshSimulationAccount();
    appendLog("success", "", undefined, { messageCode: "console.simulation.account_settings_saved" });
  } catch (error) {
    appendLog("error", "", undefined, { messageCode: "runtime.simulation_run_failed", messageParams: { error: userVisibleErrorMessage(error) } });
  }
});

async function resetSimulationAccount() {
  if (!window.confirm(copyOrDefault("runtime.simulation_account_reset_confirm", "确认重置模拟账户？历史记录会保留。"))) return;
  try {
    await request("/simulation/account/reset", { method: "POST" });
    await refreshSimulationAccount();
    await refreshSimulationHistory();
    appendLog("warn", "", undefined, { messageCode: "runtime.simulation_account_reset_done" });
  } catch (error) {
    appendLog("error", "", undefined, { messageCode: "runtime.simulation_run_failed", messageParams: { error: userVisibleErrorMessage(error) } });
  }
}

resetSimAccountBtn?.addEventListener("click", async () => {
  try {
    await resetSimulationAccount();
  } catch (error) {
    appendLog("error", "", undefined, { messageCode: "runtime.simulation_run_failed", messageParams: { error: userVisibleErrorMessage(error) } });
  }
});
resetSimAccountInlineBtn?.addEventListener("click", resetSimulationAccount);

clearSimHistoryBtn?.addEventListener("click", async () => {
  if (!window.confirm(copyOrDefault("runtime.simulation_history_clear_confirm", "确认清空模拟历史？当前模拟账户资金和仓位不会变化。"))) return;
  try {
    await request("/simulation/history", { method: "DELETE" });
    await refreshSimulationHistory();
    appendLog("warn", "", undefined, { messageCode: "runtime.simulation_history_cleared" });
  } catch (error) {
    appendLog("error", "", undefined, { messageCode: "runtime.simulation_run_failed", messageParams: { error: userVisibleErrorMessage(error) } });
  }
});

exportSimHistoryBtn?.addEventListener("click", () => {
  window.location.href = "/simulation/history/export.csv";
});

simHistoryList?.addEventListener("click", async (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  const rerunId = target.dataset.rerun;
  const copyId = target.dataset.copyReal;
  try {
    if (rerunId) {
      await request(`/simulation/history/${encodeURIComponent(rerunId)}/rerun`, { method: "POST" });
      await refreshSimulationAccount();
      await refreshSimulationHistory();
      appendLog("success", "", undefined, { messageCode: "runtime.simulation_rerun_started" });
    } else if (copyId) {
      await copySimulationRunToRealForm(copyId);
    }
  } catch (error) {
    appendLog("error", "", undefined, { messageCode: "runtime.simulation_run_failed", messageParams: { error: userVisibleErrorMessage(error) } });
  }
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
}).catch((error) => {
    appendLog("error", "", undefined, {
      messageCode: "runtime.symbol_info_load_failed",
      messageParams: { error: userVisibleErrorMessage(error) },
    });
}).finally(() => {
  initializeExecutionPageFormStates();
  detectRecoverableSession().catch(() => {});
  refreshActiveSimulationRun().catch(() => {});
});
setInterval(() => {
  maybeScheduleCurrentModePrecheck("interval");
}, PRECHECK_INTERVAL_MS);













