import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";

const appPath = path.join(process.cwd(), "paired_opener", "static", "app.js");
const appSource = fs.readFileSync(appPath, "utf8");

function extract(pattern, label) {
  const match = appSource.match(pattern);
  assert.ok(match, `${label} should exist in app.js`);
  return match[0];
}

function extractOptional(pattern) {
  const match = appSource.match(pattern);
  return match ? match[0] : "";
}

function loadErrorHelpers(fetchImpl) {
  const sources = {
    copyOrDefault: extract(/function copyOrDefault\(key, fallback, params = \{\}\) \{[\s\S]*?\n\}/, "copyOrDefault"),
    formatCopy: extract(/function formatCopy\(key, params = \{\}\) \{[\s\S]*?\n\}/, "formatCopy"),
    unknownErrorMessage: extract(/function unknownErrorMessage\(\) \{[\s\S]*?\n\}/, "unknownErrorMessage"),
    formatReason: extract(/function formatReason\(code, params = \{\}, fallback = ""\) \{[\s\S]*?\n\}/, "formatReason"),
    resolveStructuredMessage: extract(
      /function resolveStructuredMessage\(source = \{\}, fallback = ""\) \{[\s\S]*?\n\}/,
      "resolveStructuredMessage",
    ),
    userVisibleErrorMessage: extract(
      /function userVisibleErrorMessage\(error, fallback = ""\) \{[\s\S]*?\n\}/,
      "userVisibleErrorMessage",
    ),
    resolveLogMessage: extract(/function resolveLogMessage\(source = \{\}, fallback = ""\) \{[\s\S]*?\n\}/, "resolveLogMessage"),
    resolveActionAvailability: extract(
      /function resolveActionAvailability\(hintState = \{\}, runtimeState = \{\}\) \{[\s\S]*?\n\}/,
      "resolveActionAvailability",
    ),
    request: extract(/function request\(path, options = \{\}\) \{[\s\S]*?\n\}/, "request"),
  };

  const sandbox = {
    I18N_MESSAGES: {
      "common.unknown_error": "Unknown error",
      "runtime.execution_message_unavailable": "Structured log unavailable",
      "runtime.simulation_run_finished": "Simulation finished: {stop_reason}",
      "log.simulation.single_open_started": "Sim start: {symbol}",
      "reasons.session_not_found": "Session {session_id} not found",
    },
    I18N_REGISTRIES: {
      reasons: {
        session_not_found: { key: "reasons.session_not_found" },
      },
    },
    summarizePrecheckMessage(precheck, fallback) {
      return precheck?.summary || fallback;
    },
    fetch: fetchImpl,
    Error,
    JSON,
    Promise,
    String,
    Array,
    Object,
    console,
  };

  vm.runInNewContext(
    `
${sources.formatCopy}
${sources.copyOrDefault}
${sources.unknownErrorMessage}
${sources.formatReason}
${sources.resolveStructuredMessage}
${sources.userVisibleErrorMessage}
${sources.resolveLogMessage}
${sources.resolveActionAvailability}
${sources.request}
this.resolveStructuredMessage = resolveStructuredMessage;
this.userVisibleErrorMessage = userVisibleErrorMessage;
this.resolveLogMessage = resolveLogMessage;
this.resolveActionAvailability = resolveActionAvailability;
this.request = request;
this.copyOrDefault = copyOrDefault;
`,
    sandbox,
  );

  return {
    copyOrDefault: sandbox.copyOrDefault,
    resolveStructuredMessage: sandbox.resolveStructuredMessage,
    userVisibleErrorMessage: sandbox.userVisibleErrorMessage,
    resolveLogMessage: sandbox.resolveLogMessage,
    resolveActionAvailability: sandbox.resolveActionAvailability,
    request: sandbox.request,
  };
}

function loadExecutionControlHelpers() {
  const sources = {
    resolveActionAvailability: extract(
      /function resolveActionAvailability\(hintState = \{\}, runtimeState = \{\}\) \{[\s\S]*?\n\}/,
      "resolveActionAvailability",
    ),
    isTerminalSession: extract(/function isTerminalSession\(status\) \{[\s\S]*?\n\}/, "isTerminalSession"),
    normalizeSessionKind: extract(/function normalizeSessionKind\(kind\) \{[\s\S]*?\n\}/, "normalizeSessionKind"),
    isTerminalSimulationStatus: extract(
      /function isTerminalSimulationStatus\(status\) \{[\s\S]*?\n\}/,
      "isTerminalSimulationStatus",
    ),
    hasActiveExecutionSession: extract(/function hasActiveExecutionSession\(\) \{[\s\S]*?\n\}/, "hasActiveExecutionSession"),
    hasActiveSimulationRun: extract(/function hasActiveSimulationRun\(\) \{[\s\S]*?\n\}/, "hasActiveSimulationRun"),
    currentExecutionLockState: extract(/function currentExecutionLockState\(\) \{[\s\S]*?\n\}/, "currentExecutionLockState"),
    executionButtonForMode: extract(/function executionButtonForMode\(mode\) \{[\s\S]*?\n\}/, "executionButtonForMode"),
    activeRealSessionList: extractOptional(/function activeRealSessionList\(\) \{[\s\S]*?\n\}/),
    hasActiveRealSessionForSymbol: extractOptional(/function hasActiveRealSessionForSymbol\(symbol\) \{[\s\S]*?\n\}/),
    activeSessionKind: extractOptional(/function activeSessionKind\(\) \{[\s\S]*?\n\}/),
    symbolForMode: extract(/function symbolForMode\(mode\) \{[\s\S]*?\n\}/, "symbolForMode"),
    activeSessionMatchesMode: extractOptional(/function activeSessionMatchesMode\(mode\) \{[\s\S]*?\n\}/),
    refreshExecutionActionButtons: extract(
      /function refreshExecutionActionButtons\(\) \{[\s\S]*?\n\}/,
      "refreshExecutionActionButtons",
    ),
    describePrecheckFreshness: extract(
      /function describePrecheckFreshness\(decision\) \{[\s\S]*?\n\}/,
      "describePrecheckFreshness",
    ),
    selectRecoverableSession: extract(
      /function selectRecoverableSession\(sessions = \[\]\) \{[\s\S]*?\n\}/,
      "selectRecoverableSession",
    ),
  };

  const sandbox = {
    modeHintStateByMode: new Map([
      ["paired_open", { canCreate: true, canSimulate: true }],
      ["paired_close", { canCreate: true, canSimulate: false }],
      ["single_open", { canCreate: true, canSimulate: false }],
      ["single_close", { canCreate: true, canSimulate: false }],
    ]),
    precheckFreshnessStateByMode: new Map([
      ["paired_open", { fresh: true }],
      ["paired_close", { fresh: true }],
      ["single_open", { fresh: true }],
      ["single_close", { fresh: true }],
    ]),
    executionActionInFlightCount: 0,
    activeSessionId: null,
    activeSessionState: null,
    activeRealSessions: new Map(),
    simulationRunInFlight: false,
    simulationAbortInFlight: false,
    sessionAbortInFlight: false,
    sessionAbortInFlightIds: new Set(),
    latestExecutionStatsState: null,
    activeSymbol: "BTCUSDC",
    executionSymbol: { value: "BTCUSDC" },
    closeExecutionSymbol: { value: "ETHUSDC" },
    saveSimSettingsBtn: { disabled: false },
    resetSimAccountBtn: { disabled: false },
    clearSimHistoryBtn: { disabled: false },
    createBtn: { disabled: false, textContent: "" },
    simulateBtn: { disabled: false, textContent: "" },
    createCloseBtn: { disabled: false, textContent: "" },
    createSingleOpenBtn: { disabled: false, textContent: "" },
    createSingleCloseBtn: { disabled: false, textContent: "" },
    DEFAULT_REAL_ACTION_LABELS: {
      paired_open: "Start paired open",
      paired_close: "Start paired close",
      single_open: "Start single open",
      single_close: "Start single close",
    },
    DEFAULT_SIMULATE_LABEL: "Run simulation",
    EXECUTION_TERMINATE_LABEL: "Running... click to abort",
    SIMULATION_TERMINATE_LABEL: "Simulation running... click to abort",
    EXECUTION_ABORTING_LABEL: "Aborting...",
    setExecutionInputLock() {},
    syncPrecheckFreshnessState(mode) {
      return sandbox.precheckFreshnessStateByMode.get(mode) || { fresh: false };
    },
    renderExecutionSummaryBanner() {},
    renderRiskBanner() {},
    renderRecoverableSessionBanner() {},
    renderActiveRealSessionsPanel() {},
    copyOrDefault(_key, fallback) {
      return fallback;
    },
    normalizeSymbol(value) {
      return String(value || "").trim().toUpperCase();
    },
    document: {
      getElementById(id) {
        return { value: id.includes("single") ? "ETHUSDC" : "BTCUSDC" };
      },
    },
    getModeValidationDecision() {
      return { runnable: true, reason: "fresh" };
    },
    Boolean,
    String,
    Number,
    Map,
    Date,
  };

  vm.runInNewContext(
    `
${sources.resolveActionAvailability}
${sources.isTerminalSession}
${sources.normalizeSessionKind}
${sources.isTerminalSimulationStatus}
${sources.hasActiveExecutionSession}
${sources.hasActiveSimulationRun}
${sources.activeRealSessionList}
${sources.hasActiveRealSessionForSymbol}
${sources.currentExecutionLockState}
${sources.executionButtonForMode}
${sources.activeSessionKind}
${sources.symbolForMode}
${sources.activeSessionMatchesMode}
${sources.describePrecheckFreshness}
${sources.selectRecoverableSession}
${sources.refreshExecutionActionButtons}
this.refreshExecutionActionButtons = refreshExecutionActionButtons;
this.describePrecheckFreshness = describePrecheckFreshness;
this.selectRecoverableSession = selectRecoverableSession;
`,
    sandbox,
  );

  return sandbox;
}

function loadSimulationAbortHelper(responsePayload) {
  const requestSimulationAbortSource = extract(
    /async function requestSimulationAbort\(\) \{[\s\S]*?\n\}/,
    "requestSimulationAbort",
  );
  const events = [];
  const sandbox = {
    simulationAbortInFlight: false,
    confirmSimulationAbort() {
      return true;
    },
    refreshExecutionActionButtons() {
      events.push({ name: "buttons", aborting: sandbox.simulationAbortInFlight });
    },
    async request(pathName, options) {
      events.push({ name: "request", pathName, method: options?.method });
      return responsePayload;
    },
    appendLog(level, message, _createdAt, options = {}) {
      events.push({ name: "log", level, message, messageCode: options.messageCode });
    },
    isTerminalSimulationStatus(status) {
      return ["idle", "completed", "completed_with_skips", "blocked", "aborted", "exception"].includes(String(status || "idle"));
    },
    userVisibleErrorMessage(error) {
      return String(error?.message || error || "");
    },
  };

  vm.runInNewContext(
    `
${requestSimulationAbortSource}
this.requestSimulationAbort = requestSimulationAbort;
`,
    sandbox,
  );

  return { events, sandbox, requestSimulationAbort: sandbox.requestSimulationAbort };
}

function loadSimulationRunHelper(responsePayload) {
  const requestSimulationRunSource = extract(
    /async function requestSimulationRunForCurrentMode\(\) \{[\s\S]*?\n\}/,
    "requestSimulationRunForCurrentMode",
  );
  const events = [];
  const sandbox = {
    simulationRunInFlight: false,
    simulationAbortInFlight: false,
    latestExecutionStatsState: null,
    executionMode: "single_open",
    hasActiveSimulationRun() {
      return Boolean(sandbox.simulationRunInFlight || sandbox.simulationAbortInFlight);
    },
    async requestSimulationAbort() {
      events.push({ name: "abort" });
    },
    refreshExecutionActionButtons() {
      events.push({
        name: "buttons",
        running: sandbox.simulationRunInFlight,
        aborting: sandbox.simulationAbortInFlight,
      });
    },
    openSse() {},
    async refreshSymbolInfo() {},
    activeSymbol: "BTCUSDC",
    async request(pathName, options) {
      events.push({ name: "request", pathName, method: options?.method });
      sandbox.simulationAbortInFlight = true;
      return responsePayload;
    },
    buildSimulationRunPayload() {
      return { session_kind: "single_open", symbol: "BTCUSDC", open_qty: "0.01", leverage: 10, round_count: 1 };
    },
    appendLog(level, message) {
      events.push({ name: "log", level, message });
    },
    isTerminalSimulationStatus(status) {
      return ["idle", "completed", "completed_with_skips", "blocked", "aborted", "exception"].includes(String(status || "idle"));
    },
    async refreshSimulationAccount() {},
    async refreshSimulationHistory() {},
    userVisibleErrorMessage(error) {
      return String(error?.message || error || "");
    },
    JSON,
  };

  vm.runInNewContext(
    `
${requestSimulationRunSource}
this.requestSimulationRunForCurrentMode = requestSimulationRunForCurrentMode;
`,
    sandbox,
  );

  return { events, sandbox, requestSimulationRunForCurrentMode: sandbox.requestSimulationRunForCurrentMode };
}

function loadPrecheckSchedulingHelpers(decision) {
  const sources = {
    maybeScheduleCurrentModePrecheck: extract(
      /function maybeScheduleCurrentModePrecheck\(trigger = "price_tick"\) \{[\s\S]*?\n\}/,
      "maybeScheduleCurrentModePrecheck",
    ),
  };

  const scheduled = [];
  const sandbox = {
    precheckPaused: false,
    executionMode: "paired_open",
    getModeValidationDecision() {
      return decision;
    },
    schedulePrecheck(mode, delay, trigger) {
      scheduled.push({ mode, delay, trigger });
    },
  };

  vm.runInNewContext(
    `
${sources.maybeScheduleCurrentModePrecheck}
this.maybeScheduleCurrentModePrecheck = maybeScheduleCurrentModePrecheck;
`,
    sandbox,
  );

  return {
    maybeScheduleCurrentModePrecheck: sandbox.maybeScheduleCurrentModePrecheck,
    scheduled,
  };
}

function loadRunPrecheckOrderHelpers() {
  const sources = {
    runPrecheck: extract(/async function runPrecheck\(mode = executionMode, trigger = "user_input"\) \{[\s\S]*?\n\}/, "runPrecheck"),
  };

  const events = [];
  let snapshotStored = false;
  const sandbox = {
    precheckPaused: false,
    executionMode: "paired_open",
    currentAccount: { id: "account-1" },
    inFlightPrecheckPayloadByMode: new Map(),
    precheckAbortControllersByMode: new Map(),
    latestPrecheckTokensByMode: new Map(),
    latestResolvedPrecheckPayloadByMode: new Map(),
    precheckFreshnessStateByMode: new Map(),
    buildPrecheckPayload() {
      return {
        session_kind: "paired_open",
        symbol: "BTCUSDT",
        trend_bias: "long_short",
        leverage: 50,
        round_count: 10,
        round_qty: "0.006",
      };
    },
    canRunPrecheck() {
      return true;
    },
    buildModeParamsKey() {
      return "params-key";
    },
    buildModeContextKey() {
      return "context-key";
    },
    getModeValidationPrice() {
      return 100;
    },
    shouldSilentlyRefreshMode() {
      return false;
    },
    setHintStateForMode(_mode, state) {
      events.push({ name: "setHint", state });
    },
    copyOrDefault(_key, fallback) {
      return fallback;
    },
    async request() {
      return {
        ok: true,
        derived: {
          min_notional: 50,
          per_round_notional: 500,
        },
      };
    },
    applyPrecheckResult(_mode, precheck) {
      events.push({ name: "apply", ok: Boolean(precheck?.ok), snapshotStored });
    },
    storeModeValidationSnapshot() {
      snapshotStored = true;
      events.push({ name: "store" });
    },
    syncPrecheckFreshnessState() {
      events.push({ name: "sync", snapshotStored });
    },
    updateTopRiskBanner() {},
    renderRiskBanner() {},
    userVisibleErrorMessage(error) {
      return String(error?.message || error || "");
    },
    AbortController,
    JSON,
    Map,
    Number,
    Boolean,
    String,
    Error,
    Promise,
  };

  vm.runInNewContext(
    `
${sources.runPrecheck}
this.runPrecheck = runPrecheck;
`,
    sandbox,
  );

  return {
    events,
    runPrecheck: sandbox.runPrecheck,
  };
}

function loadSymbolWhitelistHelpers({ initialSymbol = "BTCUSDT", whitelist = ["BTCUSDC", "ETHUSDC"] } = {}) {
  const sources = {
    normalizeSymbol: extract(/function normalizeSymbol\(value\) \{[\s\S]*?\n\}/, "normalizeSymbol"),
    rebuildSymbolOptions: extract(/function rebuildSymbolOptions\(selectedSymbol = activeSymbol\) \{[\s\S]*?\n\}/, "rebuildSymbolOptions"),
    loadWhitelist: extract(/async function loadWhitelist\([^)]*\) \{[\s\S]*?\n\}/, "loadWhitelist"),
  };

  const orderBookInput = {
    options: [],
    value: "",
    set innerHTML(_value) {
      this.options = [];
    },
    get innerHTML() {
      return "";
    },
    appendChild(option) {
      this.options.push(option);
    },
  };

  const sandbox = {
    orderBookInput,
    executionSymbol: { value: initialSymbol },
    activeSymbol: initialSymbol,
    whitelistSymbols: [],
    temporaryCustomSymbol: null,
    document: {
      createElement(tagName) {
        assert.equal(tagName, "option");
        return { value: "", textContent: "" };
      },
    },
    async request(pathName) {
      assert.equal(pathName, "/config/whitelist");
      return { symbols: whitelist };
    },
    copyOrDefault(_key, fallback) {
      return fallback;
    },
    setActiveSymbol(symbol) {
      const normalized = sandbox.normalizeSymbol(symbol);
      sandbox.activeSymbol = normalized;
      sandbox.executionSymbol.value = normalized;
    },
    String,
    Boolean,
    Array,
  };

  vm.runInNewContext(
    `
${sources.normalizeSymbol}
${sources.rebuildSymbolOptions}
${sources.loadWhitelist}
this.normalizeSymbol = normalizeSymbol;
this.loadWhitelist = loadWhitelist;
this.state = () => ({
  activeSymbol,
  executionSymbolValue: executionSymbol.value,
  whitelistSymbols,
  temporaryCustomSymbol,
  selectedSymbol: orderBookInput.value,
  options: orderBookInput.options.map((option) => ({ value: option.value, textContent: option.textContent })),
});
`,
    sandbox,
  );

  return {
    loadWhitelist: sandbox.loadWhitelist,
    state: sandbox.state,
  };
}

{
  const { copyOrDefault } = loadErrorHelpers(async () => {
    throw new Error("fetch should not be called in helper-only tests");
  });

  assert.equal(
    copyOrDefault("runtime.missing_key", "可用 {available}，手续费 {fee}", { available: "8,000", fee: "4.00" }),
    "可用 8,000，手续费 4.00",
    "copyOrDefault should format fallback strings when a catalog key is absent",
  );
}

{
  const { resolveStructuredMessage } = loadErrorHelpers(async () => {
    throw new Error("fetch should not be called in helper-only tests");
  });

  assert.equal(
    resolveStructuredMessage({ message: "legacy backend message" }, "Safe fallback"),
    "Safe fallback",
    "resolveStructuredMessage should not render raw backend messages",
  );

  assert.equal(
    resolveStructuredMessage({ code: "session_not_found", params: { session_id: "session-1" }, message: "legacy" }, "Safe fallback"),
    "Session session-1 not found",
    "resolveStructuredMessage should prefer structured reason codes over raw messages",
  );
}

{
  const { userVisibleErrorMessage } = loadErrorHelpers(async () => ({
    ok: true,
    async text() {
      return "";
    },
  }));

  assert.equal(
    userVisibleErrorMessage(new Error("network down"), "Operation failed"),
    "Operation failed",
    "userVisibleErrorMessage should not surface raw client-side exception text",
  );

  assert.equal(
    userVisibleErrorMessage({ detail: { code: "session_not_found", params: { session_id: "session-2" } } }, "Operation failed"),
    "Session session-2 not found",
    "userVisibleErrorMessage should preserve structured server messages",
  );
}

{
  const { resolveLogMessage } = loadErrorHelpers(async () => ({
    ok: true,
    async text() {
      return "";
    },
  }));

  assert.equal(
    resolveLogMessage({ messageCode: "log.simulation.single_open_started", messageParams: { symbol: "BTCUSDT" } }, "Fallback"),
    "Sim start: BTCUSDT",
    "resolveLogMessage should render structured log entries",
  );

  assert.equal(
    resolveLogMessage({ message_code: "runtime.simulation_run_finished", message_params: { stop_reason: "filled" } }, "Fallback"),
    "Simulation finished: filled",
    "resolveLogMessage should render snake_case structured log entries from SSE payloads",
  );

  assert.equal(
    resolveLogMessage({ messageCode: "log.simulation.missing_key", message: "legacy log text" }, "Fallback"),
    "Fallback",
    "resolveLogMessage should not render unresolved log keys",
  );

  assert.equal(
    resolveLogMessage({ message: "legacy log text" }, "Fallback"),
    "Fallback",
    "resolveLogMessage should not render raw legacy log text",
  );
}

{
  const { resolveActionAvailability } = loadErrorHelpers(async () => ({
    ok: true,
    async text() {
      return "";
    },
  }));

  const unlocked = resolveActionAvailability({ canCreate: true, canSimulate: true }, {});
  assert.equal(unlocked.canCreate, true);
  assert.equal(unlocked.canSimulate, true);
  assert.equal(unlocked.locked, false);

  const inflight = resolveActionAvailability({ canCreate: true, canSimulate: true }, { requestInFlight: true });
  assert.equal(inflight.canCreate, false);
  assert.equal(inflight.canSimulate, false);
  assert.equal(inflight.locked, true);
}

{
  const sandbox = loadExecutionControlHelpers();

  sandbox.refreshExecutionActionButtons();
  assert.equal(sandbox.createBtn.disabled, false);
  assert.equal(sandbox.simulateBtn.disabled, false);
  assert.equal(sandbox.createBtn.textContent, "Start paired open");
  assert.equal(sandbox.simulateBtn.textContent, "Run simulation");

  sandbox.activeSessionId = "session-1";
  sandbox.activeSessionState = { session_id: "session-1", status: "running", session_kind: "paired_open", symbol: "BTCUSDC" };
  sandbox.refreshExecutionActionButtons();
  assert.equal(sandbox.createBtn.disabled, true, "same-symbol real-session creation should be blocked");
  assert.equal(sandbox.createBtn.textContent, "Start paired open");
  assert.equal(sandbox.simulateBtn.disabled, true);
  assert.equal(sandbox.createCloseBtn.disabled, false, "other real-session buttons should stay enabled for parallel real sessions");

  sandbox.activeSessionId = null;
  sandbox.activeSessionState = null;
  sandbox.simulationRunInFlight = true;
  sandbox.refreshExecutionActionButtons();
  assert.equal(sandbox.simulateBtn.disabled, false, "simulate button should remain clickable for abort");
  assert.equal(sandbox.simulateBtn.textContent, "Simulation running... click to abort");
  assert.equal(sandbox.createBtn.disabled, true);

  sandbox.simulationRunInFlight = false;
  sandbox.sessionAbortInFlightIds.add("session-2");
  sandbox.activeSessionId = "session-2";
  sandbox.activeSessionState = { session_id: "session-2", status: "running", session_kind: "single_close", symbol: "ETHUSDC" };
  sandbox.refreshExecutionActionButtons();
  assert.equal(sandbox.createSingleCloseBtn.disabled, true, "same-symbol create button remains blocked while abort is pending");
  assert.equal(sandbox.createSingleCloseBtn.textContent, "Start single close");
}

{
  const sandbox = loadExecutionControlHelpers();
  sandbox.activeRealSessions.set("old-session", {
    session_id: "old-session",
    status: "running",
    session_kind: "paired_open",
    symbol: "BTCUSDC",
  });
  sandbox.executionSymbol.value = "ETHUSDC";
  sandbox.activeSymbol = "ETHUSDC";
  sandbox.refreshExecutionActionButtons();

  assert.equal(sandbox.simulateBtn.disabled, true, "simulation should stay disabled while any real session remains active");
  assert.equal(sandbox.createBtn.textContent, "Start paired open", "form buttons should not become abort controls for a different symbol");
  assert.equal(sandbox.createCloseBtn.disabled, false, "other real-session buttons should remain available for parallel starts");
}

{
  const { requestSimulationAbort, sandbox } = loadSimulationAbortHelper({
    requested: false,
    status: "idle",
    message_code: "runtime.simulation_abort_not_running",
  });

  sandbox.simulationAbortInFlight = false;
  await requestSimulationAbort();

  assert.equal(sandbox.simulationAbortInFlight, false, "idle abort responses should not leave the simulation abort flag stuck");
}

{
  const { requestSimulationAbort, sandbox } = loadSimulationAbortHelper({
    requested: true,
    status: "aborting",
    message_code: "runtime.simulation_abort_requested",
  });

  await requestSimulationAbort();

  assert.equal(sandbox.simulationAbortInFlight, false, "accepted abort responses should not leave the simulation abort flag stuck");
}

{
  const { requestSimulationRunForCurrentMode, sandbox } = loadSimulationRunHelper({
    run_id: "sim-run",
    status: "aborted",
    stop_reason: "user_aborted",
  });

  await requestSimulationRunForCurrentMode();

  assert.equal(sandbox.simulationRunInFlight, false);
  assert.equal(sandbox.simulationAbortInFlight, false, "terminal simulation run responses should clear accepted abort state");
}

{
  const resetStart = appSource.indexOf('resetSimAccountBtn?.addEventListener("click"');
  const clearStart = appSource.indexOf('clearSimHistoryBtn?.addEventListener("click"');
  const exportStart = appSource.indexOf('exportSimHistoryBtn?.addEventListener("click"', clearStart);
  const resetSource = appSource.slice(resetStart, clearStart);
  const clearSource = appSource.slice(clearStart, exportStart);

  assert.ok(resetSource.includes("try") && resetSource.includes("catch"), "reset simulation account handler should catch structured API errors");
  assert.ok(clearSource.includes("try") && clearSource.includes("catch"), "clear simulation history handler should catch structured API errors");
}

{
  const connectionStart = appSource.indexOf("function setConnectionState");
  const connectionEnd = appSource.indexOf("\nfunction refreshDerivedStats", connectionStart);
  assert.notEqual(connectionStart, -1, "setConnectionState should exist");
  assert.notEqual(connectionEnd, -1, "setConnectionState block should be locatable");
  const connectionSource = appSource.slice(connectionStart, connectionEnd);
  assert.ok(
    connectionSource.includes('if ((connected || status === "connecting") && stateSymbol && connectionToggle.checked)'),
    "passive SSE connection state should not rewrite the current execution symbol",
  );

  const loadStart = appSource.indexOf("async function loadActiveSessionSnapshot");
  const pollStart = appSource.indexOf("async function pollActiveSession", loadStart);
  const startPollingStart = appSource.indexOf("function startSessionPolling", pollStart);
  const refreshSessionsStart = appSource.indexOf("async function refreshActiveRealSessions", startPollingStart);
  assert.notEqual(loadStart, -1, "loadActiveSessionSnapshot should exist");
  assert.notEqual(pollStart, -1, "pollActiveSession should exist");
  assert.notEqual(startPollingStart, -1, "startSessionPolling should exist");
  assert.notEqual(refreshSessionsStart, -1, "refreshActiveRealSessions should delimit startSessionPolling");

  const loadSource = appSource.slice(loadStart, pollStart);
  const pollSource = appSource.slice(pollStart, startPollingStart);
  const startPollingSource = appSource.slice(startPollingStart, refreshSessionsStart);
  assert.ok(loadSource.includes("sessionId = activeSessionId"), "session snapshot loading should bind the requested session id");
  assert.ok(loadSource.includes("activeSessionId !== requestedSessionId"), "stale snapshot responses should be discarded");
  assert.ok(pollSource.includes("activeSessionId !== requestedSessionId"), "stale polling responses should be discarded");
  assert.ok(startPollingSource.includes("loadActiveSessionSnapshot(sessionId)"), "initial snapshot should request the focused session explicitly");
  assert.ok(
    startPollingSource.includes("setInterval(() => pollActiveSession(sessionId), 2000)"),
    "poller interval should keep polling the session it was created for",
  );

  const queueStart = appSource.indexOf("function queueUiRender");
  const queueEnd = appSource.indexOf("\nfunction buildPrecheckPayload", queueStart);
  assert.notEqual(queueStart, -1, "queueUiRender should exist");
  assert.notEqual(queueEnd, -1, "queueUiRender block should be locatable");
  const queueSource = appSource.slice(queueStart, queueEnd);
  assert.ok(queueSource.includes("payloadMatchesActiveSymbol(payload)"), "SSE market payloads should be symbol-scoped before updating UI state");
  assert.ok(queueSource.includes("latestReferencePrice"), "orderbook payload handling should still update the active symbol reference price");

  const activePanelStart = appSource.indexOf("function renderActiveRealSessionsPanel");
  const lockStateStart = appSource.indexOf("function currentExecutionLockState", activePanelStart);
  const activePanelSource = appSource.slice(activePanelStart, lockStateStart);
  assert.ok(activePanelSource.includes("dataset.focusSessionId"), "active real sessions should expose a focus control");
  assert.ok(activePanelSource.includes("dataset.abortSessionId"), "active real sessions should keep an abort control");

  const activeListStart = appSource.indexOf('activeRealSessionsList?.addEventListener("click"');
  const simulationButtonsStart = appSource.indexOf("simulationRunButtons.forEach", activeListStart);
  const activeListSource = appSource.slice(activeListStart, simulationButtonsStart);
  assert.ok(activeListSource.includes("data-focus-session-id"), "active session list clicks should support focusing a session");
  assert.ok(activeListSource.includes("startSessionPolling(sessionId"), "focusing an active session should start its monitor poller");

  const eventSummaryStart = appSource.indexOf("function summarizeSessionEvent");
  const eventRenderStart = appSource.indexOf("function renderSessionEvents", eventSummaryStart);
  const eventSummarySource = appSource.slice(eventSummaryStart, eventRenderStart);
  assert.ok(eventSummarySource.includes("I18N_REGISTRIES.events"), "session event summaries should resolve through the event registry");
  assert.ok(!eventSummarySource.includes("switch (event.event_type)"), "session event summaries should not keep hardcoded event text branches");

  assert.ok(
    !appSource.includes("syncAllExecutionPageFormStateSymbols"),
    "real and simulation form state should never be symbol-synced across pages",
  );

  const applyStateStart = appSource.indexOf("async function applyExecutionFormState");
  const normalizeSessionKindStart = appSource.indexOf("function normalizeSessionKind", applyStateStart);
  assert.notEqual(applyStateStart, -1, "applyExecutionFormState should exist");
  assert.notEqual(normalizeSessionKindStart, -1, "applyExecutionFormState block should be locatable");
  const applyStateSource = appSource.slice(applyStateStart, normalizeSessionKindStart);
  assert.ok(
    applyStateSource.indexOf("switchSymbol(nextSymbol") !== -1
      && applyStateSource.indexOf("switchSymbol(nextSymbol") < applyStateSource.indexOf("applyExecutionFormFields(normalizedState)"),
    "page restoration should refresh the symbol context before writing saved form values",
  );

  const copySimulationStart = appSource.indexOf("async function copySimulationRunToRealForm");
  const confirmSimulationAbortStart = appSource.indexOf("function confirmSimulationAbort", copySimulationStart);
  assert.notEqual(copySimulationStart, -1, "copySimulationRunToRealForm should exist");
  assert.notEqual(confirmSimulationAbortStart, -1, "copySimulationRunToRealForm block should be locatable");
  const copySimulationSource = appSource.slice(copySimulationStart, confirmSimulationAbortStart);
  assert.ok(
    copySimulationSource.indexOf('await setAppPage("real")') !== -1
      && copySimulationSource.indexOf('await setAppPage("real")') < copySimulationSource.indexOf("await syncSimulationPayloadSymbolContext(payload)"),
    "copying simulation parameters to real should switch page before applying the copied symbol context",
  );
}

{
  const sandbox = loadExecutionControlHelpers();
  const fresh = sandbox.describePrecheckFreshness({ runnable: true, reason: "fresh" });
  assert.equal(fresh.fresh, true);
  assert.equal(fresh.reason, "fresh");

  const stale = sandbox.describePrecheckFreshness({ runnable: true, reason: "params_changed" });
  assert.equal(stale.fresh, false);
  assert.equal(stale.reason, "params_changed");
  assert.equal(stale.label, "需重新确认");
}

{
  const sandbox = loadExecutionControlHelpers();
  const selected = sandbox.selectRecoverableSession([
    {
      session_id: "session-recoverable",
      status: "exception",
      recovery_status: "recoverable",
      updated_at: "2026-04-21T10:00:00Z",
    },
    {
      session_id: "session-running",
      status: "running",
      recovery_status: null,
      updated_at: "2026-04-21T09:00:00Z",
    },
  ]);
  assert.equal(selected.session_id, "session-running", "active sessions should be preferred over recoverable exceptions");
}

{
  const { maybeScheduleCurrentModePrecheck, scheduled } = loadPrecheckSchedulingHelpers({
    runnable: true,
    reason: "context_stale",
  });

  maybeScheduleCurrentModePrecheck("account_update");

  assert.deepEqual(
    scheduled,
    [{ mode: "paired_open", delay: 0, trigger: "account_update" }],
    "context_stale prechecks should refresh immediately after account updates",
  );
}

{
  const { loadWhitelist, state } = loadSymbolWhitelistHelpers({
    initialSymbol: "BTCUSDT",
    whitelist: ["BTCUSDC", "ETHUSDC"],
  });

  await loadWhitelist({ preferWhitelistDefault: true });

  assert.equal(state().activeSymbol, "BTCUSDC");
  assert.equal(state().executionSymbolValue, "BTCUSDC");
  assert.equal(state().selectedSymbol, "BTCUSDC");
  assert.equal(state().temporaryCustomSymbol, null);
  assert.deepEqual(
    state().options.map((option) => option.value),
    ["BTCUSDC", "ETHUSDC"],
    "initial whitelist load should not keep the HTML default BTCUSDT as a custom symbol",
  );
}

{
  const { loadWhitelist, state } = loadSymbolWhitelistHelpers({
    initialSymbol: "BTCUSDT",
    whitelist: ["BTCUSDC", "ETHUSDC"],
  });

  await loadWhitelist();

  assert.equal(state().activeSymbol, "BTCUSDT");
  assert.equal(state().temporaryCustomSymbol, "BTCUSDT");
  assert.deepEqual(
    state().options.map((option) => option.value),
    ["BTCUSDC", "BTCUSDT", "ETHUSDC"],
    "non-initial whitelist refresh should preserve an explicit custom symbol",
  );
}

{
  const { runPrecheck, events } = loadRunPrecheckOrderHelpers();

  await runPrecheck("paired_open", "user_input");

  const applyEvent = events.find((event) => event.name === "apply");
  assert.equal(
    applyEvent?.snapshotStored,
    true,
    "successful precheck should store the validation snapshot before refreshing success hints and action buttons",
  );
}

{
  const { request } = loadErrorHelpers(async () => ({
    ok: false,
    async text() {
      return JSON.stringify({ message: "legacy backend message" });
    },
  }));

  await assert.rejects(
    () => request("/api/test"),
    (error) => error instanceof Error && error.message === "Unknown error",
    "request should downgrade unstructured backend errors to a safe fallback message",
  );
}

{
  const { request } = loadErrorHelpers(async () => ({
    ok: false,
    async text() {
      return JSON.stringify({
        detail: {
          code: "session_not_found",
          params: { session_id: "session-9" },
          raw_message: "Session not found",
        },
      });
    },
  }));

  await assert.rejects(
    () => request("/api/test"),
    (error) =>
      error instanceof Error &&
      error.message === "Session session-9 not found" &&
      error.code === "session_not_found" &&
      error.params?.session_id === "session-9",
    "request should preserve structured error details while formatting the user-visible message",
  );
}
