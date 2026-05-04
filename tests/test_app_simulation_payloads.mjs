import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";

const appPath = path.join(process.cwd(), "paired_opener", "static", "app.js");
const appSource = fs.readFileSync(appPath, "utf8");
const indexPath = path.join(process.cwd(), "paired_opener", "static", "index.html");
const indexSource = fs.readFileSync(indexPath, "utf8");

function extract(pattern, label) {
  const match = appSource.match(pattern);
  assert.ok(match, `${label} should exist in app.js`);
  return match[0];
}

function extractOptional(pattern) {
  const match = appSource.match(pattern);
  return match ? match[0] : "";
}

function loadSimulationPayloadHelpers() {
  const helperSource = extractOptional(
    /async function syncSimulationPayloadSymbolContext\(payload = \{\}\) \{[\s\S]*?\n\}/,
  );
  const applySource = extract(
    /(?:async )?function applySimulationPayloadToForm\(payload = \{\}\) \{[\s\S]*?\n\}/,
    "applySimulationPayloadToForm",
  );
  const copySource = extract(
    /async function copySimulationRunToRealForm\(runId\) \{[\s\S]*?\n\}/,
    "copySimulationRunToRealForm",
  );
  const events = [];
  const elements = new Map();

  function getElement(id) {
    if (!elements.has(id)) {
      elements.set(id, { value: "", textContent: "", disabled: false });
    }
    return elements.get(id);
  }

  const sandbox = {
    activeSymbol: "BTCUSDC",
    connectionToggle: { checked: false },
    executionSymbol: getElement("executionSymbol"),
    closeExecutionSymbol: getElement("closeExecutionSymbol"),
    document: {
      getElementById: getElement,
    },
    normalizeSymbol(value) {
      return String(value || "").trim().toUpperCase();
    },
    normalizeSessionKind(kind) {
      return kind || "paired_open";
    },
    async switchSymbol(symbol, shouldReconnect) {
      events.push({ name: "switch", symbol, shouldReconnect });
      sandbox.activeSymbol = symbol;
      return true;
    },
    setAppPage(page) {
      events.push({ name: "page", page, activeSymbol: sandbox.activeSymbol });
    },
    setExecutionMode(mode) {
      events.push({ name: "mode", mode, activeSymbol: sandbox.activeSymbol });
    },
    recalculateMode(mode) {
      events.push({ name: "recalculate", mode, activeSymbol: sandbox.activeSymbol });
    },
    refreshExecutionActionButtons() {
      events.push({ name: "buttons", activeSymbol: sandbox.activeSymbol });
    },
    maybeScheduleCurrentModePrecheck(trigger) {
      events.push({ name: "precheck", trigger, activeSymbol: sandbox.activeSymbol });
    },
    appendLog(level, message) {
      events.push({ name: "log", level, message, activeSymbol: sandbox.activeSymbol });
    },
    async request(pathName) {
      assert.equal(pathName, "/simulation/history/run-1");
      return {
        request: {
          session_kind: "single_open",
          symbol: "ETHUSDC",
          open_mode: "regular",
          selected_position_side: "LONG",
          open_qty: "0.010",
          leverage: 20,
          round_count: 3,
          round_interval_seconds: 12,
        },
      };
    },
    String,
    Number,
    Promise,
  };

  vm.runInNewContext(
    `
${helperSource}
${applySource}
${copySource}
this.applySimulationPayloadToForm = applySimulationPayloadToForm;
this.copySimulationRunToRealForm = copySimulationRunToRealForm;
`,
    sandbox,
  );

  return {
    events,
    elements,
    applySimulationPayloadToForm: sandbox.applySimulationPayloadToForm,
    copySimulationRunToRealForm: sandbox.copySimulationRunToRealForm,
  };
}

function loadSimulationStatsUpdateHelper() {
  const start = appSource.indexOf("function updateSimulationRunStatsFromPayload");
  const end = appSource.indexOf("\nfunction appendSimulationRunLog", start);
  assert.notEqual(start, -1, "updateSimulationRunStatsFromPayload should exist");
  assert.notEqual(end, -1, "updateSimulationRunStatsFromPayload should end before appendSimulationRunLog");
  const source = appSource.slice(start, end);
  const rendered = [];
  const summaries = [];
  const sandbox = {
    executionStatsByPage: {
      simulation: {
        mode: "paired_open",
        roundsCompleted: 7,
        roundsTotal: 30,
        totalNotional: 3000,
        perRoundNotional: 100,
        minNotional: 50,
        carryoverQty: 0,
        finalAlignmentStatus: "not_needed",
        lastQty: 0.1,
      },
    },
    latestExecutionStatsState: null,
    simulationRunInFlight: true,
    simulationAbortInFlight: false,
    currentSymbolInfo: { min_notional: 50 },
    appPage: "simulation",
    executionMode: "paired_open",
    normalizeSessionKind(kind) {
      return kind || "paired_open";
    },
    isTerminalSimulationStatus(status) {
      return ["idle", "completed", "completed_with_skips", "blocked", "aborted", "exception", "interrupted"].includes(String(status || "idle"));
    },
    parseDisplayNumber(value) {
      const numeric = Number(String(value ?? "").replace(/,/g, ""));
      return Number.isFinite(numeric) ? numeric : 0;
    },
    readStatNumber() {
      return 0;
    },
    buildRoundsLabelFromStats(_stats, roundsCompleted = 0, roundsTotal = 0) {
      return `${roundsCompleted || 0} / ${roundsTotal || 0}`;
    },
    resolveResidualQty() {
      return 0;
    },
    normalizeAppPage(page) {
      return page || "real";
    },
    renderExecutionStatsSnapshot(snapshot) {
      rendered.push(snapshot);
    },
    updateExecutionSummary(summary, page) {
      summaries.push({ summary, page });
    },
    buildExecutionSummary(stats) {
      return stats;
    },
    refreshExecutionActionButtons() {},
    Number,
    String,
  };

  vm.runInNewContext(
    `
${source}
this.updateSimulationRunStatsFromPayload = updateSimulationRunStatsFromPayload;
`,
    sandbox,
  );

  return {
    rendered,
    summaries,
    sandbox,
    updateSimulationRunStatsFromPayload: sandbox.updateSimulationRunStatsFromPayload,
  };
}

function loadPairedOpenRecalculateHelper() {
  const source = extract(/function recalculateOpenAmount\(\) \{[\s\S]*?\n\}/, "recalculateOpenAmount");
  const elements = new Map();
  function getElement(id) {
    if (!elements.has(id)) {
      elements.set(id, { value: "", textContent: "", className: "" });
    }
    return elements.get(id);
  }
  getElement("calcMargin").value = "6500";
  getElement("leverage").value = "75";
  getElement("calcRounds").value = "10";
  getElement("roundQty").value = "";
  getElement("minNotionalHint").textContent = "开仓总金额超过可用余额的98%，无法开单。";
  const sandbox = {
    appPage: "simulation",
    executionMode: "paired_open",
    latestReferencePrice: 77920,
    latestSimulationAvailableBalance: 8000,
    latestSimulationMakerFeeRate: 0.0001,
    latestSimulationTakerFeeRate: 0.0005,
    symbolInfoReady: true,
    modeHintStateByMode: new Map([["paired_open", { canCreate: false, canSimulate: false }]]),
    latestPrecheckResultByMode: new Map(),
    latestResolvedPrecheckPayloadByMode: new Map(),
    document: { getElementById: getElement },
    formatMoney(value) {
      return Number(value).toFixed(2);
    },
    formatNumber(value) {
      return String(value);
    },
    refreshDerivedStats() {},
    canRunPrecheck() {
      return true;
    },
    buildPrecheckPayload() {
      return { symbol: "BTCUSDC", trend_bias: "long", round_qty: "0.312821", round_count: 10, leverage: 75 };
    },
    clearHintStateForMode(mode) {
      sandbox.modeHintStateByMode.set(mode, { canCreate: false, canSimulate: false });
    },
    updateOpenValidationHint({ canCreate, canSimulate = true, message, tone }) {
      const hint = getElement("minNotionalHint");
      hint.className = `validation-hint ${tone || ""}`;
      hint.textContent = message;
      sandbox.modeHintStateByMode.set("paired_open", { canCreate, canSimulate });
    },
    copyOrDefault(_key, fallback, params = {}) {
      return String(fallback).replace(/\{([^{}]+)\}/g, (_match, name) => String(params[name] ?? `{${name}}`));
    },
    Number,
    Math,
  };
  vm.runInNewContext(
    `
${source}
this.recalculateOpenAmount = recalculateOpenAmount;
`,
    sandbox,
  );
  return { elements, sandbox, recalculateOpenAmount: sandbox.recalculateOpenAmount };
}

function loadOrderbookRenderHelper() {
  const payloadMatchesSource = extract(
    /function payloadMatchesActiveSymbol\(payload = \{\}\) \{[\s\S]*?\n\}/,
    "payloadMatchesActiveSymbol",
  );
  const queueStart = appSource.indexOf("function queueUiRender()");
  const queueEnd = appSource.indexOf("\nfunction buildPrecheckPayload", queueStart);
  assert.ok(queueStart >= 0 && queueEnd > queueStart, "queueUiRender block should exist in app.js");
  const queueSource = appSource.slice(queueStart, queueEnd);
  const events = [];
  const elements = new Map();
  function getElement(id) {
    if (!elements.has(id)) {
      elements.set(id, { textContent: "", value: "" });
    }
    return elements.get(id);
  }
  const sandbox = {
    events,
    elements,
    console,
    Number,
    String,
  };
  vm.runInNewContext(
    `
var activeSymbol = "ETHUSDC";
var executionMode = "paired_open";
var symbolInfoReady = true;
var latestReferencePrice = 0;
var renderFramePending = false;
var pendingOrderbookPayload = {
  symbol: "ETHUSDC",
  asks: [{ price: "102", qty: "1" }],
  bids: [{ price: "100", qty: "1" }]
};
var pendingAccountOverviewPayload = null;
var pendingLogEntries = [];
var asksContainer = {};
var bidsContainer = {};
function normalizeSymbol(value) {
  return String(value || "").trim().toUpperCase();
}
function requestAnimationFrame(callback) {
  callback();
}
function renderLevels(container, levels, side) {
  events.push({ name: "levels", side, levels });
}
function recalculateMode(mode) {
  events.push({ name: "recalculate", mode, price: latestReferencePrice });
}
function maybeScheduleCurrentModePrecheck(trigger) {
  events.push({ name: "precheck", trigger, price: latestReferencePrice });
}
function renderAccountOverview(payload) {
  events.push({ name: "account", payload });
}
function appendLog(level, message, createdAt, meta) {
  events.push({ name: "log", level, message, createdAt, meta });
}
function nowTime() {
  return "12:00:00";
}
var document = { getElementById: (${getElement.toString()}) };
${payloadMatchesSource}
${queueSource}
this.queueUiRender = queueUiRender;
this.getLatestReferencePrice = () => latestReferencePrice;
`,
    sandbox,
  );
  return {
    events,
    elements,
    queueUiRender: sandbox.queueUiRender,
    getLatestReferencePrice: sandbox.getLatestReferencePrice,
  };
}

function loadPairedCloseRecalculateHelper() {
  const source = `
${extractOptional(/function currentExecutionPositions\(\) \{[\s\S]*?\n\}/)}
${extract(/function positionQty\(symbol, positionSide\) \{[\s\S]*?\n\}/, "positionQty")}
${extract(/function maxCloseableQtyForSymbol\(symbol\) \{[\s\S]*?\n\}/, "maxCloseableQtyForSymbol")}
${extract(/function recalculateCloseAmount\(\) \{[\s\S]*?\n\}/, "recalculateCloseAmount")}
`;
  const elements = new Map();
  function getElement(id) {
    if (!elements.has(id)) {
      elements.set(id, { value: "", textContent: "", className: "" });
    }
    return elements.get(id);
  }
  getElement("closeQty").value = "4.144";
  getElement("closeRounds").value = "10";
  getElement("closeRoundQty").value = "";
  getElement("closeValidationHint").textContent = "";
  const sandbox = {
    appPage: "simulation",
    executionMode: "paired_close",
    activeSymbol: "BTCUSDC",
    latestReferencePrice: 77774,
    symbolInfoReady: true,
    currentSymbolInfo: { min_notional: 5 },
    currentPositions: [],
    currentSimulationPositions: [
      { symbol: "BTCUSDC", position_side: "LONG", qty: "4.144", leverage: 50 },
      { symbol: "BTCUSDC", position_side: "SHORT", qty: "4.144", leverage: 50 },
    ],
    modeHintStateByMode: new Map([["paired_close", { canCreate: false, canSimulate: false }]]),
    latestPrecheckResultByMode: new Map(),
    latestResolvedPrecheckPayloadByMode: new Map(),
    document: { getElementById: getElement },
    formatMoney(value) {
      return Number(value).toFixed(2);
    },
    formatNumber(value, digits = 6) {
      return Number(value).toFixed(digits);
    },
    refreshDerivedStats(snapshot) {
      sandbox.derivedStats = snapshot;
    },
    canRunPrecheck() {
      return true;
    },
    buildPrecheckPayload() {
      return { symbol: "BTCUSDC", trend_bias: "long", close_qty: "4.144", round_count: 10 };
    },
    clearHintStateForMode(mode) {
      sandbox.modeHintStateByMode.set(mode, { canCreate: false, canSimulate: false });
    },
    updateCloseValidationHint({ canCreate, canSimulate = false, message, tone }) {
      const hint = getElement("closeValidationHint");
      hint.className = `validation-hint ${tone || ""}`;
      hint.textContent = message;
      sandbox.modeHintStateByMode.set("paired_close", { canCreate, canSimulate });
    },
    copyOrDefault(_key, fallback, params = {}) {
      return String(fallback).replace(/\{([^{}]+)\}/g, (_match, name) => String(params[name] ?? `{${name}}`));
    },
    Number,
    Math,
  };
  vm.runInNewContext(
    `
${source}
this.recalculateCloseAmount = recalculateCloseAmount;
`,
    sandbox,
  );
  return { elements, sandbox, recalculateCloseAmount: sandbox.recalculateCloseAmount };
}

function loadPageScopedFormStateHelpers() {
  const start = appSource.indexOf("const EXECUTION_FORM_FIELD_IDS");
  const end = appSource.indexOf("\nfunction normalizeSessionKind", start);
  assert.notEqual(start, -1, "page-scoped execution form helpers should exist");
  assert.notEqual(end, -1, "page-scoped helpers should be defined before normalizeSessionKind");
  const source = appSource.slice(start, end);
  const pageStart = appSource.indexOf("function applyAppPageChrome") >= 0
    ? appSource.indexOf("function applyAppPageChrome")
    : appSource.indexOf("async function setAppPage");
  const pageEnd = appSource.indexOf("\nfunction buildSimulationRunPayload", pageStart);
  assert.notEqual(pageStart, -1, "setAppPage should exist");
  assert.notEqual(pageEnd, -1, "setAppPage should be defined before buildSimulationRunPayload");
  const pageSource = appSource.slice(pageStart, pageEnd);
  const modeSource = extract(/function setExecutionMode\(mode\) \{[\s\S]*?\n\}/, "setExecutionMode");
  const elements = new Map();
  const events = [];

  function classListFor(id) {
    return {
      toggle(className, enabled) {
        events.push({ name: "class", id, className, enabled });
      },
    };
  }

  function getElement(id) {
    if (!elements.has(id)) {
      elements.set(id, { value: "", textContent: "", disabled: false, classList: classListFor(id) });
    }
    return elements.get(id);
  }

  [
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
    "statMode",
  ].forEach((id) => getElement(id));

  const sandbox = {
    appPage: "real",
    executionMode: "paired_open",
    activeSymbol: "BTCUSDC",
    connectionToggle: { checked: false },
    appRoot: { classList: classListFor("appRoot") },
    navRealBtn: { classList: classListFor("navRealBtn") },
    navSimulationBtn: { classList: classListFor("navSimulationBtn") },
    modeButtons: {
      paired_open: { classList: classListFor("modePairedOpen") },
      paired_close: { classList: classListFor("modePairedClose") },
      single_open: { classList: classListFor("modeSingleOpen") },
      single_close: { classList: classListFor("modeSingleClose") },
    },
    modePanels: {
      paired_open: { classList: classListFor("pairedOpenPanel") },
      paired_close: { classList: classListFor("pairedClosePanel") },
      single_open: { classList: classListFor("singleOpenPanel") },
      single_close: { classList: classListFor("singleClosePanel") },
    },
    executionSymbol: getElement("executionSymbol"),
    closeExecutionSymbol: getElement("closeExecutionSymbol"),
    document: { getElementById: getElement },
    normalizeSessionKind(kind) {
      return ["paired_close", "single_open", "single_close"].includes(String(kind || ""))
        ? String(kind)
        : "paired_open";
    },
    normalizeSymbol(value) {
      return String(value || "").trim().toUpperCase();
    },
    formatModeLabel(mode) {
      return mode;
    },
    setActiveSymbol(symbol) {
      events.push({ name: "symbol", symbol });
      sandbox.activeSymbol = symbol;
    },
    switchSymbolShouldSucceed: true,
    switchSymbolQueue: [],
    async switchSymbol(symbol) {
      events.push({ name: "switch", symbol });
      const queuedSwitch = sandbox.switchSymbolQueue.shift();
      if (queuedSwitch) {
        const queuedResult = await queuedSwitch;
        if (!queuedResult) return false;
      }
      if (!sandbox.switchSymbolShouldSucceed) return false;
      sandbox.activeSymbol = symbol;
      return true;
    },
    restoreModeValidationSnapshot() {
      return false;
    },
    recalculateMode(mode) {
      events.push({ name: "recalculate", mode, page: sandbox.appPage });
    },
    syncPrecheckFreshnessState(mode) {
      events.push({ name: "freshness", mode, page: sandbox.appPage });
    },
    refreshExecutionActionButtons() {
      events.push({ name: "buttons", page: sandbox.appPage });
    },
    renderCurrentPageExecutionStats() {
      events.push({ name: "stats", page: sandbox.appPage });
    },
    renderExecutionSummaryBanner() {
      events.push({ name: "summary", page: sandbox.appPage });
    },
    renderRiskBanner() {
      events.push({ name: "risk", page: sandbox.appPage });
    },
    maybeScheduleCurrentModePrecheck(trigger) {
      events.push({ name: "precheck", trigger, page: sandbox.appPage });
    },
    openSse() {
      events.push({ name: "sse" });
    },
    refreshSimulationAccount() {
      events.push({ name: "sim-account" });
    },
    refreshSimulationHistory() {
      events.push({ name: "sim-history" });
    },
    refreshSimulationTemplates() {
      events.push({ name: "sim-templates" });
    },
    appendLog(level, message, _createdAt, options = {}) {
      events.push({ name: "log", level, message, messageCode: options.messageCode, params: options.messageParams });
    },
    userVisibleErrorMessage(error) {
      return String(error?.message || error || "");
    },
    Promise,
  };

  vm.runInNewContext(
    `
${source}
${pageSource}
${modeSource}
this.initializeExecutionPageFormStates = initializeExecutionPageFormStates;
this.setAppPage = setAppPage;
this.setExecutionMode = setExecutionMode;
this.syncExecutionPageFormStateSymbols = syncExecutionPageFormStateSymbols;
`,
    sandbox,
  );

  return { elements, events, sandbox };
}

function loadWhitelistHelper() {
  const rebuildSource = extract(/function rebuildSymbolOptions\(selectedSymbol = activeSymbol\) \{[\s\S]*?\n\}/, "rebuildSymbolOptions");
  const loadSource = extract(/async function loadWhitelist\(options = \{\}\) \{[\s\S]*?\n\}/, "loadWhitelist");
  const optionNodes = [];
  const sandbox = {
    activeSymbol: "BTCUSDT",
    temporaryCustomSymbol: null,
    whitelistSymbols: [],
    executionSymbol: { value: "BTCUSDT" },
    EXECUTION_SYMBOL_FIELD_BY_MODE: {
      paired_open: "executionSymbol",
      paired_close: "closeExecutionSymbol",
      single_open: "singleOpenExecutionSymbol",
      single_close: "singleCloseExecutionSymbol",
    },
    executionPageFormStates: {},
    orderBookInput: {
      innerHTML: "",
      value: "",
      appendChild(option) {
        optionNodes.push(option);
      },
    },
    document: {
      createElement(tagName) {
        return { tagName, value: "", textContent: "" };
      },
    },
    async request(pathName) {
      assert.equal(pathName, "/config/whitelist");
      return { symbols: ["BTCUSDC", "ETHUSDC"] };
    },
    normalizeSymbol(value) {
      return String(value || "").trim().toUpperCase();
    },
    setActiveSymbol(symbol) {
      sandbox.activeSymbol = symbol;
      sandbox.executionSymbol.value = symbol;
    },
    copyOrDefault(_key, fallback) {
      return fallback;
    },
    String,
    Array,
  };

  vm.runInNewContext(
    `
${rebuildSource}
${loadSource}
this.loadWhitelist = loadWhitelist;
`,
    sandbox,
  );

  return { optionNodes, sandbox, loadWhitelist: sandbox.loadWhitelist };
}

function loadSimulationAccountRenderHelper() {
  const toneSource = extract(/function applyMetricTone\(element, rawValue\) \{[\s\S]*?\n\}/, "applyMetricTone");
  const source = extract(/function renderSimulationAccount\(account\) \{[\s\S]*?\n\}/, "renderSimulationAccount");
  const elements = new Map();

  function makeElement() {
    const classes = new Set();
    return {
      value: "",
      textContent: "",
      classList: {
        add(...names) {
          names.forEach((name) => classes.add(name));
        },
        remove(...names) {
          names.forEach((name) => classes.delete(name));
        },
        contains(name) {
          return classes.has(name);
        },
      },
    };
  }

  function getElement(id) {
    if (!elements.has(id)) {
      elements.set(id, makeElement());
    }
    return elements.get(id);
  }

  const sandbox = {
    appPage: "real",
    executionMode: "paired_open",
    latestSimulationAvailableBalance: null,
    latestSimulationMakerFeeRate: 0,
    latestSimulationTakerFeeRate: 0,
    currentSimulationPositions: [],
    simUnrealizedPnl: getElement("simUnrealizedPnl"),
    simAvailableBalance: getElement("simAvailableBalance"),
    simMarginUsed: getElement("simMarginUsed"),
    simEquity: getElement("simEquity"),
    simInitialBalance: getElement("simInitialBalance"),
    simMakerFee: getElement("simMakerFee"),
    simTakerFee: getElement("simTakerFee"),
    document: { activeElement: null, getElementById: getElement },
    formatNumber(value, digits = 8) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "0";
      return numeric.toLocaleString("en-US", {
        minimumFractionDigits: 0,
        maximumFractionDigits: digits,
      });
    },
    renderSimulationPositions() {},
    refreshSingleOpenOrderOptions() {},
    refreshSingleClosePositionOptions() {},
    recalculateMode() {},
    Number,
  };

  vm.runInNewContext(
    `
${toneSource}
${source}
this.renderSimulationAccount = renderSimulationAccount;
`,
    sandbox,
  );
  return { elements, sandbox, renderSimulationAccount: sandbox.renderSimulationAccount };
}

{
  const { events, applySimulationPayloadToForm } = loadSimulationPayloadHelpers();

  await applySimulationPayloadToForm({
    session_kind: "single_open",
    symbol: "ETHUSDC",
    open_mode: "regular",
    selected_position_side: "LONG",
    open_qty: "0.010",
    leverage: 20,
    round_count: 3,
  });

  assert.equal(events[0]?.name, "switch");
  assert.equal(events[0]?.symbol, "ETHUSDC");
  assert.equal(events.find((event) => event.name === "recalculate")?.activeSymbol, "ETHUSDC");
}

{
  const { events, elements, copySimulationRunToRealForm } = loadSimulationPayloadHelpers();

  await copySimulationRunToRealForm("run-1");

  assert.equal(events[0]?.name, "page");
  assert.equal(events[0]?.page, "real");
  assert.equal(events[1]?.name, "switch");
  assert.equal(events[1]?.symbol, "ETHUSDC");
  assert.equal(events.find((event) => event.name === "switch")?.symbol, "ETHUSDC");
  assert.equal(events.find((event) => event.name === "precheck")?.activeSymbol, "ETHUSDC");
  assert.equal(elements.get("singleOpenRoundIntervalSeconds")?.value, 12);
}

{
  const { elements, sandbox, recalculateOpenAmount } = loadPairedOpenRecalculateHelper();

  recalculateOpenAmount();

  assert.equal(sandbox.modeHintStateByMode.get("paired_open")?.canSimulate, true);
  assert.equal(elements.get("roundQty")?.value, "0.312821");
  assert.match(elements.get("minNotionalHint")?.textContent || "", /可以模拟开单/);
  assert.match(
    elements.get("minNotionalHint")?.textContent || "",
    /预估手续费 48\.75/,
    "simulation paired-open fee estimate should use maker fee rate, not taker fee rate",
  );
}

{
  const { elements, sandbox, recalculateCloseAmount } = loadPairedCloseRecalculateHelper();

  recalculateCloseAmount();

  assert.equal(elements.get("maxCloseableQty").textContent, "4.144000");
  assert.equal(elements.get("closeRoundQty").value, "0.414400");
  assert.equal(elements.get("closeValidationHint").className, "validation-hint success");
  assert.match(elements.get("closeValidationHint").textContent, /可以模拟平仓/);
  assert.equal(sandbox.modeHintStateByMode.get("paired_close")?.canSimulate, true);
}

{
  const { elements, sandbox } = loadPageScopedFormStateHelpers();

  elements.get("executionSymbol").value = "BTCUSDC";
  elements.get("calcMargin").value = "50";
  elements.get("leverage").value = "10";
  elements.get("calcRounds").value = "3";
  sandbox.initializeExecutionPageFormStates();

  elements.get("calcMargin").value = "100";
  elements.get("leverage").value = "20";
  await sandbox.setAppPage("simulation");
  assert.equal(elements.get("calcMargin").value, "50");
  assert.equal(elements.get("leverage").value, "10");

  elements.get("calcMargin").value = "6500";
  elements.get("leverage").value = "75";
  sandbox.setExecutionMode("single_open");
  await sandbox.setAppPage("real");
  assert.equal(elements.get("calcMargin").value, "100");
  assert.equal(elements.get("leverage").value, "20");
  assert.equal(sandbox.executionMode, "paired_open");

  await sandbox.setAppPage("simulation");
  assert.equal(elements.get("calcMargin").value, "6500");
  assert.equal(elements.get("leverage").value, "75");
  assert.equal(sandbox.executionMode, "single_open");
}

{
  const { elements, events, sandbox } = loadPageScopedFormStateHelpers();

  elements.get("executionSymbol").value = "BTCUSDC";
  sandbox.initializeExecutionPageFormStates();

  await sandbox.setAppPage("simulation");
  elements.get("executionSymbol").value = "BTCUSDC";
  await sandbox.setAppPage("real");
  elements.get("executionSymbol").value = "ETHUSDC";
  sandbox.activeSymbol = "ETHUSDC";
  sandbox.syncExecutionPageFormStateSymbols("BTCUSDC", "ETHUSDC");

  await sandbox.setAppPage("simulation");

  assert.equal(elements.get("executionSymbol").value, "BTCUSDC", "real symbol changes should not rewrite simulation saved form state");
  assert.equal(
    events.some((event) => event.name === "switch" && event.symbol === "BTCUSDC"),
    true,
    `restoring a page with a different symbol should switch symbol context; events=${JSON.stringify(events)}`,
  );
}

{
  const { elements, sandbox } = loadPageScopedFormStateHelpers();

  elements.get("executionSymbol").value = "BTCUSDC";
  sandbox.initializeExecutionPageFormStates();

  await sandbox.setAppPage("simulation");
  elements.get("executionSymbol").value = "ETHUSDC";
  sandbox.activeSymbol = "ETHUSDC";
  await sandbox.setAppPage("real");
  assert.equal(sandbox.appPage, "real");
  assert.equal(elements.get("executionSymbol").value, "BTCUSDC");
  assert.equal(sandbox.activeSymbol, "BTCUSDC");

  let releaseFirstSwitch;
  sandbox.switchSymbolQueue.push(new Promise((resolve) => {
    releaseFirstSwitch = resolve;
  }));
  const simulationSwitch = sandbox.setAppPage("simulation");
  await Promise.resolve();
  const realSwitch = sandbox.setAppPage("real");
  releaseFirstSwitch(true);
  await Promise.all([simulationSwitch, realSwitch]);

  assert.equal(sandbox.appPage, "real", "the latest page switch should win after an earlier slow switch resolves");
  assert.equal(elements.get("executionSymbol").value, "BTCUSDC", "slow previous switch should not overwrite the latest page form");
  assert.equal(sandbox.activeSymbol, "BTCUSDC", "slow previous switch should not leave the latest page on the old symbol context");
}

{
  const { elements, events, sandbox } = loadPageScopedFormStateHelpers();

  elements.get("executionSymbol").value = "BTCUSDC";
  sandbox.initializeExecutionPageFormStates();

  await sandbox.setAppPage("simulation");
  elements.get("executionSymbol").value = "ETHUSDC";
  sandbox.activeSymbol = "ETHUSDC";
  await sandbox.setAppPage("real");
  assert.equal(sandbox.appPage, "real");
  assert.equal(elements.get("executionSymbol").value, "BTCUSDC");

  sandbox.switchSymbolShouldSucceed = false;
  sandbox.activeSymbol = "BTCUSDC";
  await sandbox.setAppPage("simulation");

  assert.equal(sandbox.appPage, "real", "failed symbol restore should keep the previous page active");
  assert.equal(elements.get("executionSymbol").value, "BTCUSDC", "failed symbol restore should roll form fields back");
  assert.equal(sandbox.activeSymbol, "BTCUSDC", "failed symbol restore should keep the previous symbol context");
  assert.equal(
    events.some((event) => event.name === "log" && event.messageCode === "runtime.symbol_switch_failed"),
    true,
    `failed page restore should log a symbol switch failure; events=${JSON.stringify(events)}`,
  );
}

{
  const { events, queueUiRender, getLatestReferencePrice } = loadOrderbookRenderHelper();

  queueUiRender();

  assert.equal(getLatestReferencePrice(), 101);
  assert.equal(
    events.some((event) => event.name === "recalculate" && event.mode === "paired_open" && event.price === 101),
    true,
    `first orderbook price after a symbol switch should recalculate derived quantities; events=${JSON.stringify(events)}`,
  );
  assert.equal(
    events.some((event) => event.name === "precheck" && event.trigger === "price_tick"),
    true,
    "orderbook updates should still schedule a price precheck",
  );
}

{
  assert.ok(!indexSource.includes('id="executionRiskBanner"'), "execution risk banner should not render on real or simulation pages");
  assert.match(indexSource, /id="executionSummaryBanner"[^>]*data-real-only/s, "execution summary should only render on the real page");
  assert.ok(!indexSource.includes('id="statConnection"'), "execution stats should not include a duplicated connection status row");
  assert.match(indexSource, /data-simulation-only[^>]*>\s*<strong>模拟状态<\/strong><span id="statSimStatus"/s);
  assert.match(indexSource, /data-real-only[^>]*>\s*<strong>真实会话状态<\/strong><span id="statSessionStatus"/s);
  assert.match(indexSource, /id="activeRealSessionsPanel"[^>]*data-real-only/s);
  assert.match(indexSource, /id="recoverableSessionBanner"[^>]*data-real-only/s);
  assert.match(indexSource, /id="simulationConnectionBadge"[^>]*>未连接<\/span>/, "simulation account header should mirror the account connection badge");
  assert.ok(!indexSource.includes('<span class="badge success">不会真实下单</span>'), "simulation account header should not use the no-real-order badge");
  assert.match(indexSource, /id="simulationAccountCard"/, "simulation account should have an explicit account card");
  assert.match(indexSource, /id="simulationSettingsCard"/, "simulation settings should be rendered as a separate card");
  assert.match(indexSource, /id="executionLogCard"/, "execution log should have an explicit layout anchor");
  assert.match(indexSource, /id="simulationHistoryCard"/, "simulation history should have an explicit layout anchor");
  assert.ok(
    indexSource.indexOf('id="simulationSettingsCard"') > indexSource.indexOf('id="simulationAccountCard"'),
    "simulation settings card should be separated from the simulation account card",
  );
  assert.ok(
    indexSource.indexOf('id="executionLogCard"') > indexSource.indexOf('<section class="dashboard"'),
    "execution log should live in the dashboard flow below the primary trading cards",
  );
  assert.ok(
    indexSource.indexOf('id="simulationSettingsCard"') > indexSource.indexOf('id="executionLogCard"'),
    "simulation settings should render below the execution log",
  );
  assert.ok(
    indexSource.indexOf('id="simulationHistoryCard"') > indexSource.indexOf('id="executionLogCard"'),
    "simulation history should render below the execution log",
  );
  assert.ok(!indexSource.includes('value="BTCUSDT"'), "static execution form defaults should not prefer non-whitelisted BTCUSDT");
  assert.ok(!indexSource.includes(">BTCUSDT<"), "static status/footer defaults should not display BTCUSDT");
}

{
  const { sandbox, optionNodes, loadWhitelist } = loadWhitelistHelper();

  await loadWhitelist({ preferWhitelistDefault: true });

  assert.equal(sandbox.activeSymbol, "BTCUSDC");
  assert.equal(sandbox.executionSymbol.value, "BTCUSDC");
  assert.deepEqual(optionNodes.map((option) => option.value), ["BTCUSDC", "ETHUSDC"]);
  assert.deepEqual(sandbox.executionPageFormStates, {}, "whitelist loading should not mutate page-scoped saved form state");
}

{
  const { elements, renderSimulationAccount } = loadSimulationAccountRenderHelper();

  renderSimulationAccount({
    totals: {
      wallet_balance: "8000.1234",
      available_balance: "7999.9876",
      margin: "0",
      equity: "8000.1234",
      unrealized_pnl: "-12.3456",
    },
    settings: { initial_balance: "8000", maker_fee_rate: "0", taker_fee_rate: "0.0005" },
    positions: [],
  });

  assert.equal(elements.get("simEquity").textContent, "8,000.12");
  assert.equal(elements.get("simAvailableBalance").textContent, "7,999.99");
  assert.equal(elements.get("simAvailableBalance").classList.contains("positive"), true);
  assert.equal(elements.get("simEquity").classList.contains("positive"), true);
  assert.equal(elements.get("simMarginUsed").classList.contains("zero"), true);
  assert.equal(elements.get("simUnrealizedPnl").textContent, "-12.35");
  assert.equal(elements.get("simUnrealizedPnl").classList.contains("negative"), true);
}

{
  const positionsStart = appSource.indexOf("function renderSimulationPositions");
  const positionsEnd = appSource.indexOf("\nfunction renderSimulationHistory", positionsStart);
  const positionsSource = appSource.slice(positionsStart, positionsEnd);
  const historyStart = appSource.indexOf("function renderSimulationHistory");
  const historyEnd = appSource.indexOf("\nasync function refreshSimulationAccount", historyStart);
  const historySource = appSource.slice(historyStart, historyEnd);
  assert.ok(
    !historySource.includes("node.innerHTML"),
    "simulation history should render untrusted run fields with textContent instead of innerHTML",
  );
  assert.ok(
    !positionsSource.includes("item.innerHTML"),
    "simulation positions should render untrusted symbol fields with textContent instead of innerHTML",
  );
  assert.ok(
    positionsSource.includes('item.className = "position-row"'),
    "simulation positions should reuse the real position-row card style",
  );
  assert.ok(
    positionsSource.includes("position-meta"),
    "simulation positions should render the same structured metric grid as real positions",
  );
}

{
  const { rendered, sandbox, updateSimulationRunStatsFromPayload } = loadSimulationStatsUpdateHelper();

  updateSimulationRunStatsFromPayload({
    status: "running",
    stage: "waiting_fill",
    heartbeat_at: "2026-05-01T00:00:00Z",
  });

  assert.equal(
    sandbox.executionStatsByPage.simulation.roundsCompleted,
    7,
    "stage heartbeat without rounds_completed should preserve existing completed rounds",
  );
  assert.equal(rendered.at(-1)?.roundsLabel, "7 / 30");
}

{
  const appendStart = appSource.indexOf("function appendSimulationRunLog");
  const appendEnd = appSource.indexOf("\nfunction applySimulationEvent", appendStart);
  const appendSource = appSource.slice(appendStart, appendEnd);
  const seedStart = appSource.indexOf("function seedSimulationRunStats");
  const seedEnd = appSource.indexOf("\nfunction currentExecutionPositions", seedStart);
  const seedSource = appSource.slice(seedStart, seedEnd);
  assert.match(
    appendSource,
    /seenSimulationEventIds\.has\(eventId\)/,
    "simulation run logs should dedupe persisted events by event_id",
  );
  assert.ok(
    !seedSource.includes("appendLog("),
    "seeding simulation running stats should not append a duplicate start log",
  );
  assert.match(
    appSource,
    /eventSource\.addEventListener\("simulation_run", \(event\) => \{[\s\S]*?updateSimulationRunStatsFromPayload\(payload\);[\s\S]*?appendSimulationRunLog\(payload\);/,
    "simulation_run SSE events should update execution stats and append a visible log entry",
  );
  assert.match(
    appSource,
    /seedSimulationRunStats\(requestPayload\);[\s\S]*?request\("\/simulation\/run"/,
    "starting a simulation should seed running stats before waiting for the final response",
  );
}

console.log("app simulation payload tests passed");
