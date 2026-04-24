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
`,
    sandbox,
  );

  return {
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
    activeSessionKind: extract(/function activeSessionKind\(\) \{[\s\S]*?\n\}/, "activeSessionKind"),
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
    simulationRunInFlight: false,
    simulationAbortInFlight: false,
    sessionAbortInFlight: false,
    latestExecutionStatsState: null,
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
    copyOrDefault(_key, fallback) {
      return fallback;
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
${sources.currentExecutionLockState}
${sources.executionButtonForMode}
${sources.activeSessionKind}
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
  sandbox.activeSessionState = { status: "running", session_kind: "paired_open" };
  sandbox.refreshExecutionActionButtons();
  assert.equal(sandbox.createBtn.disabled, false, "owner real-session button should remain clickable for abort");
  assert.equal(sandbox.createBtn.textContent, "Running... click to abort");
  assert.equal(sandbox.simulateBtn.disabled, true);
  assert.equal(sandbox.createCloseBtn.disabled, true);

  sandbox.activeSessionId = null;
  sandbox.activeSessionState = null;
  sandbox.simulationRunInFlight = true;
  sandbox.refreshExecutionActionButtons();
  assert.equal(sandbox.simulateBtn.disabled, false, "simulate button should remain clickable for abort");
  assert.equal(sandbox.simulateBtn.textContent, "Simulation running... click to abort");
  assert.equal(sandbox.createBtn.disabled, true);

  sandbox.simulationRunInFlight = false;
  sandbox.sessionAbortInFlight = true;
  sandbox.activeSessionId = "session-2";
  sandbox.activeSessionState = { status: "running", session_kind: "single_close" };
  sandbox.refreshExecutionActionButtons();
  assert.equal(sandbox.createSingleCloseBtn.disabled, true);
  assert.equal(sandbox.createSingleCloseBtn.textContent, "Aborting...");
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
