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
let currentAccount = { id: "default", name: "默认账户" };
let availableAccounts = [];
let whitelistSymbols = [];
let temporaryCustomSymbol = null;
let latestReferencePrice = 0;
let latestAvailableBalance = null;
let currentPositions = [];
let currentSymbolInfo = { symbol: activeSymbol, min_notional: 0, allowed: true };
let symbolInfoReady = false;
let precheckTimer = null;
let precheckAbortController = null;
let latestPrecheckToken = 0;
const latestPrecheckByMode = new Map();
const inFlightPrecheckPayloadByMode = new Map();
let precheckPaused = false;
let activeSessionId = null;
let activeSessionPoller = null;
let activeSessionState = null;
let latestSessionEventId = 0;
const seenSessionEventIds = new Set();
const MAX_LOG_LINES = 200;
const orderBookRowCache = { sell: [], buy: [] };
const positionRowCache = new Map();
let pendingOrderbookPayload = null;
let pendingAccountOverviewPayload = null;
const pendingLogEntries = [];
let renderFramePending = false;

function nowTime() {
  return new Date().toLocaleTimeString("zh-CN", { hour12: false });
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
      recalculateOpenAmount();
      recalculateCloseAmount();
      recalculateSingleCloseAmount();
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
        appendLog(entry.level || "info", entry.message || "", entry.created_at);
      });
      document.getElementById("streamClock").textContent = nowTime();
    }
  });
}


function request(path, options = {}) {
  return fetch(path, options).then(async (response) => {
    const text = await response.text();
    if (!response.ok) {
      let message = text;
      let precheck = null;
      let validationDetail = null;
      try {
        const payload = JSON.parse(text);
        if (payload && typeof payload === "object") {
          if (Array.isArray(payload.detail)) {
            validationDetail = payload.detail;
            message = payload.message || text;
          } else if (payload.detail && typeof payload.detail === "object") {
            message = payload.detail.message || payload.message || text;
            precheck = payload.detail.precheck || null;
          } else {
            message = payload.detail || payload.message || text;
            precheck = payload.precheck || null;
          }
        }
      } catch {}
      const error = new Error(message);
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
  return numeric.toLocaleString("zh-CN", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  });
}

function formatDisplayPrice(value, digits = 2) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) return "--";
  return formatNumber(numeric, digits);
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
      return "双向平仓";
    case "single_open":
      return "单向开仓";
    case "single_close":
      return "单向平仓";
    default:
      return "双向开仓";
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
  const summary = String(precheck.summary || fallbackMessage || "").trim();
  const checks = Array.isArray(precheck.checks) ? precheck.checks : [];
  const warning = checks.find((item) => String(item.status) === "warn");
  if (warning && warning.message && warning.message !== summary) {
    return `${summary} ${warning.message}`;
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

function applyPrecheckResult(mode, precheck) {
  if (!precheck) return;
  latestPrecheckByMode.set(mode, precheck);
  const derived = precheck.derived || {};
  if (mode === executionMode) {
    refreshDerivedStats({
      totalNotional: Number(derived.total_notional || 0),
      perRoundNotional: Number(derived.per_round_notional || 0),
      estimatedQty: Number(derived.normalized_round_qty || 0),
      minNotional: Number((derived.min_notional ?? currentSymbolInfo.min_notional) || 0),
    });
    document.getElementById("statMode").textContent = formatModeLabel(mode);
    document.getElementById("statCarryoverQty").textContent = formatNumber(derived.carryover_qty || 0, 6);
    document.getElementById("statFinalAlignment").textContent = formatAlignmentStatus(derived.final_alignment_status);
  }
  const checks = Array.isArray(precheck.checks) ? precheck.checks : [];
  const hasFailures = checks.some((item) => String(item.status) === "fail");
  const hasWarnings = checks.some((item) => String(item.status) === "warn");
  if (!hasFailures && hasWarnings) {
    switch (mode) {
      case "paired_close":
        createCloseBtn.disabled = !Boolean(precheck.ok);
        break;
      case "single_open":
        createSingleOpenBtn.disabled = !Boolean(precheck.ok);
        break;
      case "single_close":
        createSingleCloseBtn.disabled = !Boolean(precheck.ok);
        break;
      default:
        createBtn.disabled = !Boolean(precheck.ok);
        simulateBtn.disabled = false;
        break;
    }
    return;
  }
  const tone = precheckTone(precheck);
  const message = summarizePrecheckMessage(precheck, "????");
  switch (mode) {
    case "paired_close":
      updateCloseValidationHint({ canCreate: Boolean(precheck.ok), tone, message });
      break;
    case "single_open":
      updateSingleOpenValidationHint({ canCreate: Boolean(precheck.ok), tone, message });
      break;
    case "single_close":
      updateSingleCloseValidationHint({ canCreate: Boolean(precheck.ok), tone, message });
      break;
    default:
      updateOpenValidationHint({ canCreate: Boolean(precheck.ok), canSimulate: !simulateBtn.disabled, tone, message });
      break;
  }
}

async function runPrecheck(mode = executionMode) {
  if (precheckPaused) return;
  const payload = buildPrecheckPayload(mode);
  if (!canRunPrecheck(mode, payload)) {
    return;
  }
  const payloadKey = JSON.stringify(payload);
  if (latestPrecheckByMode.get(mode) === payloadKey || inFlightPrecheckPayloadByMode.get(mode) === payloadKey) {
    return;
  }
  if (precheckAbortController) {
    precheckAbortController.abort();
  }
  const controller = new AbortController();
  precheckAbortController = controller;
  inFlightPrecheckPayloadByMode.set(mode, payloadKey);
  const token = ++latestPrecheckToken;
  try {
    const precheck = await request("/sessions/precheck", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    if (controller.signal.aborted || token !== latestPrecheckToken) return;
    latestPrecheckByMode.set(mode, payloadKey);
    applyPrecheckResult(mode, precheck);
  } catch (error) {
    if (controller.signal.aborted || error?.name === "AbortError") {
      return;
    }
    if (token !== latestPrecheckToken) return;
    latestPrecheckByMode.delete(mode);
    const precheck = error.precheck || null;
    if (error.validationDetail) {
      return;
    }
    if (precheck) {
      applyPrecheckResult(mode, precheck);
      return;
    }
    const message = `Precheck failed: ${String(error)}`;
    switch (mode) {
      case "paired_close":
        updateCloseValidationHint({ canCreate: false, tone: "error", message });
        break;
      case "single_open":
        updateSingleOpenValidationHint({ canCreate: false, tone: "error", message });
        break;
      case "single_close":
        updateSingleCloseValidationHint({ canCreate: false, tone: "error", message });
        break;
      default:
        updateOpenValidationHint({ canCreate: false, canSimulate: !simulateBtn.disabled, tone: "error", message });
        break;
    }
  } finally {
    if (inFlightPrecheckPayloadByMode.get(mode) === payloadKey) {
      inFlightPrecheckPayloadByMode.delete(mode);
    }
    if (precheckAbortController === controller) {
      precheckAbortController = null;
    }
  }
}

function schedulePrecheck(mode = executionMode, delay = 400) {
  if (precheckPaused) return;
  if (precheckTimer) {
    clearTimeout(precheckTimer);
  }
  precheckTimer = setTimeout(() => {
    precheckTimer = null;
    runPrecheck(mode);
  }, delay);
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
  inFlightPrecheckPayloadByMode.clear();
}

function isTerminalSession(status) {
  return ["completed", "completed_with_skips", "aborted", "exception"].includes(String(status || ""));
}

function formatAlignmentStatus(status) {
  switch (String(status || "not_needed")) {
    case "carryover_pending":
      return "待最终对齐";
    case "market_aligned":
      return "市价对齐完成";
    case "flattened_both_sides":
      return "双边清仓对齐";
    case "failed":
      return "最终对齐失败";
    default:
      return "未触发";
  }
}

function setCurrentAccount(accountId, accountName, syncSelect = true) {
  currentAccount = {
    id: String(accountId || currentAccount.id || "default").trim().toLowerCase(),
    name: String(accountName || currentAccount.name || "默认账户").trim() || "默认账户",
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
      ? `${symbol} (自定义)` : symbol;
    orderBookInput.appendChild(option);
  });
  if (options.includes(normalizedSelected)) {
    orderBookInput.value = normalizedSelected;
  } else if (options.length) {
    orderBookInput.value = options[0];
  }
}

async function loadWhitelist() {
  const payload = await request("/config/whitelist");
  whitelistSymbols = (payload.symbols || []).map((symbol) => normalizeSymbol(symbol)).filter(Boolean);
  const currentSymbol = normalizeSymbol(executionSymbol.value || activeSymbol);
  temporaryCustomSymbol = whitelistSymbols.includes(currentSymbol) ? null : currentSymbol;
  rebuildSymbolOptions(currentSymbol);
  return whitelistSymbols;
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
  if (mode === "paired_open") {
    recalculateOpenAmount();
  } else if (mode === "paired_close") {
    recalculateCloseAmount();
  } else if (mode === "single_open") {
    recalculateSingleOpenAmount();
  } else if (mode === "single_close") {
    recalculateSingleCloseAmount();
  }
  schedulePrecheck(mode, 0);
}

function setActiveSymbol(symbol, syncInput = true) {
  activeSymbol = normalizeSymbol(symbol);
  latestReferencePrice = 0;
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
  recalculateOpenAmount();
  recalculateCloseAmount();
  recalculateSingleOpenAmount();
  recalculateSingleCloseAmount();
  const footerStatus = document.getElementById("footerStatus");
  footerStatus.textContent = `${connectionToggle.checked ? "已连接" : "已断开"} ${activeSymbol}`;
  schedulePrecheck();
}

function setSymbolInfo(info) {
  currentSymbolInfo = info || { symbol: activeSymbol, min_notional: 0, allowed: true };
  symbolInfoReady = Boolean(info);
  document.getElementById("statMinNotional").textContent = formatNumber(currentSymbolInfo.min_notional || 0, 4);
  recalculateOpenAmount();
  recalculateCloseAmount();
  recalculateSingleOpenAmount();
  recalculateSingleCloseAmount();
  schedulePrecheck();
}

function renderLevels(container, levels, side) {
  const cache = orderBookRowCache[side] || [];
  orderBookRowCache[side] = cache;
  const emptyState = container.querySelector(".orderbook-empty");
  if (!Array.isArray(levels) || !levels.length) {
    if (!emptyState) {
      const placeholder = document.createElement("div");
      placeholder.className = "empty-state orderbook-empty";
      placeholder.textContent = side === "sell" ? "Sell levels will appear after connect" : "Buy levels will appear after connect";
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
    const priceText = `${side === "sell" ? "卖" : "买"}${index + 1} ${formatNumber(level.price, 2)}`;
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

function appendLog(level, message, createdAt) {
  const line = document.createElement("div");
  line.className = "log-line";
  const time = createdAt ? new Date(createdAt).toLocaleTimeString("zh-CN", { hour12: false }) : nowTime();
  line.innerHTML = `
    <div class="log-time mono">${time}</div>
    <div class="log-badge ${level}">${level}</div>
    <div class="log-message">${message}</div>
  `;
  logsBody.prepend(line);
  while (logsBody.children.length > MAX_LOG_LINES) {
    logsBody.removeChild(logsBody.lastElementChild);
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
    badge.textContent = "已连接";
    switchLabel.textContent = "已开启";
    footerDot.classList.add("live");
  } else if (status === "connecting") {
    badge.className = "badge warn";
    switchLabel.className = "badge warn";
    badge.textContent = "连接中";
    switchLabel.textContent = "连接中";
    footerDot.classList.remove("live");
  } else if (status === "error") {
    badge.className = "badge error";
    switchLabel.className = "badge error";
    badge.textContent = "异常";
    switchLabel.textContent = "异常";
    footerDot.classList.remove("live");
  } else {
    badge.className = "badge warn";
    switchLabel.className = "badge warn";
    badge.textContent = "未连接";
    switchLabel.textContent = "已断开";
    footerDot.classList.remove("live");
  }
  footerStatus.textContent = `${connected ? "已连接" : status === "connecting" ? "连接中" : status === "error" ? "异常" : "已断开"} ${state.symbol || activeSymbol}`;
  statConnection.textContent = status;
  connectionToggle.checked = connected;
}
function refreshDerivedStats({ totalNotional = 0, perRoundNotional = 0, estimatedQty = 0, minNotional = Number(currentSymbolInfo.min_notional || 0) } = {}) {
  document.getElementById("statTotalNotional").textContent = formatNumber(totalNotional || 0, 4);
  document.getElementById("statPerRound").textContent = formatNumber(perRoundNotional || 0, 4);
  document.getElementById("statLastQty").textContent = formatNumber(estimatedQty || 0, 8);
  document.getElementById("statMinNotional").textContent = formatNumber(minNotional || 0, 4);
}

function updateExecutionStats(stats) {
  document.getElementById("statSimStatus").textContent = stats.status || "idle";
  document.getElementById("statRounds").textContent = `${stats.rounds_completed || 0} / ${stats.rounds_total || 0}`;
  document.getElementById("statTotalNotional").textContent = formatNumber(stats.total_notional || 0, 4);
  document.getElementById("statPerRound").textContent = formatNumber(stats.notional_per_round || 0, 4);
  document.getElementById("statLastQty").textContent = formatNumber(stats.last_qty || 0, 8);
  document.getElementById("statMode").textContent = formatModeLabel(stats.mode || executionMode);
  if (stats.min_notional !== undefined) {
    document.getElementById("statMinNotional").textContent = formatNumber(stats.min_notional || 0, 4);
  }
  if (stats.carryover_qty !== undefined) {
    document.getElementById("statCarryoverQty").textContent = formatNumber(stats.carryover_qty || 0, 6);
  }
  if (stats.final_alignment_status !== undefined) {
    document.getElementById("statFinalAlignment").textContent = formatAlignmentStatus(stats.final_alignment_status);
  }
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
  if (!currentPositions.length) {
    positionRowCache.forEach((row) => row.remove());
    positionRowCache.clear();
    const message = payload.status === "loading" ? "Loading positions" : "No open positions";
    const detail = payload.message || "Positions will appear here after the stream connects.";
    const placeholder = document.createElement("div");
    placeholder.className = "empty-state";
    placeholder.style.minHeight = "220px";
    placeholder.style.marginTop = "0";
    placeholder.innerHTML = `<div><div style="font-size: 36px; margin-bottom: 10px;">??</div><div>${message}</div><div style="margin-top: 6px; font-size: 13px;">${detail}</div></div>`;
    positionsList.replaceChildren(placeholder);
    refreshSingleOpenOrderOptions();
    refreshSingleClosePositionOptions();
    recalculateOpenAmount();
    recalculateCloseAmount();
    recalculateSingleOpenAmount();
    recalculateSingleCloseAmount();
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
          <span class="position-side ${sideClass}">${position.position_side === "SHORT" ? "空" : "多"}</span>
        </div>
        <div class="position-meta">
          <div>数量<strong class="mono">${formatNumber(position.qty || 0, 6)}</strong></div>
          <div>名义价值<strong class="mono">${formatNumber(notional, 2)}</strong></div>
          <div>开仓均价<strong class="mono">${formatNumber(position.entry_price || 0, 2)}</strong></div>
          <div>标记价格<strong class="mono">${markPriceText}</strong></div>
          <div>未实现盈亏<strong class="mono ${pnlClass}">${formatNumber(position.unrealized_pnl || 0, 4)}</strong></div>
          <div>爆仓价格<strong class="mono">${liquidationPriceText}</strong></div>
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
  recalculateOpenAmount();
  recalculateCloseAmount();
  recalculateSingleOpenAmount();
  recalculateSingleCloseAmount();
}
function updateOpenValidationHint({ canCreate, canSimulate = true, message, tone }) {
  minNotionalHint.className = `validation-hint ${tone || ""}`;
  minNotionalHint.textContent = message;
  createBtn.disabled = !canCreate;
  simulateBtn.disabled = !canSimulate;
}

function updateCloseValidationHint({ canCreate, message, tone }) {
  closeValidationHint.className = `validation-hint ${tone || ""}`;
  closeValidationHint.textContent = message;
  createCloseBtn.disabled = !canCreate;
}

function updateSingleOpenValidationHint({ canCreate, message, tone }) {
  singleOpenValidationHint.className = `validation-hint ${tone || ""}`;
  singleOpenValidationHint.textContent = message;
  createSingleOpenBtn.disabled = !canCreate;
}

function updateSingleCloseValidationHint({ canCreate, message, tone }) {
  singleCloseValidationHint.className = `validation-hint ${tone || ""}`;
  singleCloseValidationHint.textContent = message;
  createSingleCloseBtn.disabled = !canCreate;
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
      document.getElementById("singleOpenMarginPerRound").textContent = formatNumber(0, 4);
      document.getElementById("singleOpenTotalNotional").textContent = formatNumber(0, 4);
      document.getElementById("singleOpenNotionalPerRound").textContent = formatNumber(0, 4);
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
  const minNotional = Number(currentSymbolInfo.min_notional || 0);
  const impliedOpenAmount = leverage > 0 ? totalNotional / leverage : totalNotional;
  const marginPerRound = rounds > 0 ? impliedOpenAmount / rounds : 0;
  const maxOpenAmount = latestAvailableBalance === null ? null : latestAvailableBalance * 0.95;

  document.getElementById("singleOpenRoundQty").value = perRoundQty > 0 ? perRoundQty.toFixed(6) : "0";
  document.getElementById("singleOpenMarginPerRound").textContent = formatNumber(marginPerRound, 4);
  document.getElementById("singleOpenTotalNotional").textContent = formatNumber(totalNotional, 4);
  document.getElementById("singleOpenNotionalPerRound").textContent = formatNumber(perRoundNotional, 4);
  if (executionMode === "single_open") {
    refreshDerivedStats({ totalNotional, perRoundNotional, estimatedQty: perRoundQty });
  }

  if (!symbolInfoReady) {
    updateSingleOpenValidationHint({ canCreate: false, tone: "", message: "等待交易对规则加载完成后再计算单向开仓参数。" });
    return;
  }
  if (currentSymbolInfo.allowed === false) {
    updateSingleOpenValidationHint({ canCreate: false, tone: "error", message: `${activeSymbol} 不在白名单中，无法创建真实单向开仓会话。` });
    return;
  }
  if (Number(currentSymbolInfo.max_leverage || 0) > 0 && leverage > Number(currentSymbolInfo.max_leverage)) {
    updateSingleOpenValidationHint({ canCreate: false, tone: "error", message: `杠杆 ${leverage}x 超过该交易对最大杠杆 ${currentSymbolInfo.max_leverage}x。` });
    return;
  }
  if (!selectedSide) {
    updateSingleOpenValidationHint({ canCreate: false, tone: "error", message: "请选择开仓方向。" });
    return;
  }
  if (openQty <= 0) {
    updateSingleOpenValidationHint({ canCreate: false, tone: "", message: "请输入有效的单向开仓数量。" });
    return;
  }
  if (perRoundQty <= 0) {
    updateSingleOpenValidationHint({ canCreate: false, tone: "error", message: "每轮数量归一化后为 0，无法单向开仓。" });
    return;
  }
  if (perRoundNotional < minNotional) {
    updateSingleOpenValidationHint({ canCreate: false, tone: "error", message: `每轮开仓名义金额 ${formatNumber(perRoundNotional, 4)} 低于交易所最小下单金额 ${formatNumber(minNotional, 4)}。` });
    return;
  }
  if (maxOpenAmount !== null && impliedOpenAmount > maxOpenAmount) {
    updateSingleOpenValidationHint({ canCreate: false, tone: "error", message: `开单金额 ${formatNumber(impliedOpenAmount, 4)} 超过当前可用余额 ${formatNumber(latestAvailableBalance, 4)} 的 95% 上限 ${formatNumber(maxOpenAmount, 4)}。` });
    return;
  }
  if (mode === "align") {
    updateSingleOpenValidationHint({ canCreate: true, tone: "success", message: hasExistingPosition ? `将按订单对齐模式补齐 ${selectedSide}，数量 ${formatNumber(openQty, 6)}，当前交易对已有持仓，杠杆已锁定为 ${leverage}x。` : `将按订单对齐模式补齐 ${selectedSide}，数量 ${formatNumber(openQty, 6)}，当前杠杆 ${leverage}x。` });
    return;
  }
  updateSingleOpenValidationHint({ canCreate: true, tone: "success", message: hasExistingPosition ? `将按常规模式开 ${selectedSide}，当前交易对已有持仓，杠杆已锁定为 ${leverage}x，每轮开仓金额约 ${formatNumber(perRoundNotional, 4)}。` : `将按常规模式开 ${selectedSide}，当前杠杆 ${leverage}x，每轮开仓金额约 ${formatNumber(perRoundNotional, 4)}。` });
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

  if (mode === "align") {
    if (longQty === shortQty) {
      if (orderSelect) orderSelect.disabled = true;
      if (qtyInput) qtyInput.disabled = true;
      if (qtyInput) qtyInput.value = "0";
      document.getElementById("singleCloseRoundQty").value = "0";
      document.getElementById("singleCloseAvailableQty").textContent = formatNumber(0, 6);
      document.getElementById("singleCloseTotalNotional").textContent = formatNumber(0, 4);
      document.getElementById("singleCloseNotionalPerRound").textContent = formatNumber(0, 4);
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
    if (orderSelect) { orderSelect.disabled = positions.length === 0; syncPositionSideTone(orderSelect); }
    if (qtyInput) qtyInput.disabled = false;
  }

  const closeQty = Number(qtyInput?.value || 0);
  const perRoundQty = closeQty / rounds;
  const totalNotional = closeQty * latestReferencePrice;
  const perRoundNotional = perRoundQty * latestReferencePrice;
  const minNotional = Number(currentSymbolInfo.min_notional || 0);
  document.getElementById("singleCloseRoundQty").value = perRoundQty > 0 ? perRoundQty.toFixed(6) : "0";
  document.getElementById("singleCloseAvailableQty").textContent = formatNumber(availableQty, 6);
  document.getElementById("singleCloseTotalNotional").textContent = formatNumber(totalNotional, 4);
  document.getElementById("singleCloseNotionalPerRound").textContent = formatNumber(perRoundNotional, 4);
  if (executionMode === "single_close") {
    refreshDerivedStats({ totalNotional, perRoundNotional, estimatedQty: perRoundQty });
  }

  if (!symbolInfoReady) {
    updateSingleCloseValidationHint({ canCreate: false, tone: "", message: "正在加载交易对规则，暂时无法确认单向平仓参数。" });
    return;
  }
  if (!positions.length) {
    updateSingleCloseValidationHint({ canCreate: false, tone: "error", message: "当前交易对没有持仓单，无法创建单向平仓会话。" });
    return;
  }
  if (!selectedSide) {
    updateSingleCloseValidationHint({ canCreate: false, tone: "error", message: "请选择要平仓的持仓单。" });
    return;
  }
  if (closeQty <= 0) {
    updateSingleCloseValidationHint({ canCreate: false, tone: "", message: "请输入平仓数量，或切换到订单对齐模式。" });
    return;
  }
  if (closeQty > availableQty) {
    updateSingleCloseValidationHint({ canCreate: false, tone: "error", message: `平仓数量 ${formatNumber(closeQty, 6)} 超过所选持仓数量 ${formatNumber(availableQty, 6)}。` });
    return;
  }
  if (perRoundQty <= 0) {
    updateSingleCloseValidationHint({ canCreate: false, tone: "error", message: "每轮数量归一化后为 0，无法单向平仓。" });
    return;
  }
  if (perRoundNotional < minNotional) {
    updateSingleCloseValidationHint({ canCreate: false, tone: "error", message: `每轮平仓名义金额 ${formatNumber(perRoundNotional, 4)} 低于交易所最小下单金额 ${formatNumber(minNotional, 4)}。` });
    return;
  }
  if (mode === "align") {
    updateSingleCloseValidationHint({ canCreate: true, tone: "success", message: `订单对齐模式已锁定 ${selectedSide}，差值平仓数量 ${formatNumber(closeQty, 6)}。` });
    return;
  }
  updateSingleCloseValidationHint({ canCreate: true, tone: "success", message: `当前可用持仓数量 ${formatNumber(availableQty, 6)}，每轮名义平仓金额 ${formatNumber(perRoundNotional, 4)}。` });
}

function recalculateOpenAmount() {
  const margin = Number(document.getElementById("calcMargin").value) || 0;
  const leverage = Number(document.getElementById("leverage").value) || 0;
  const rounds = Math.max(Number(document.getElementById("calcRounds").value) || 1, 1);
  const marginPerRound = margin / rounds;
  const totalNotional = margin * leverage;
  const notionalPerRound = totalNotional / rounds;
  const roundQty = latestReferencePrice > 0 ? notionalPerRound / latestReferencePrice : 0;
  const minNotional = Number(currentSymbolInfo.min_notional || 0);
  const balanceLimit = latestAvailableBalance === null ? null : { available: latestAvailableBalance, maxOpenAmount: latestAvailableBalance * 0.95 };

  document.getElementById("marginPerRound").textContent = formatNumber(marginPerRound, 4);
  document.getElementById("totalNotional").textContent = formatNumber(totalNotional, 4);
  document.getElementById("notionalPerRound").textContent = formatNumber(notionalPerRound, 4);
  document.getElementById("roundQty").value = roundQty > 0 ? roundQty.toFixed(6) : "0";
  if (executionMode === "paired_open") {
    refreshDerivedStats({ totalNotional, perRoundNotional: notionalPerRound, estimatedQty: roundQty });
  }
  document.getElementById("statTotalNotional").textContent = formatNumber(totalNotional, 4);
  document.getElementById("statPerRound").textContent = formatNumber(notionalPerRound, 4);
  document.getElementById("statLastQty").textContent = formatNumber(roundQty, 8);

  if (balanceLimit && margin > balanceLimit.maxOpenAmount) {
    updateOpenValidationHint({
      canCreate: false,
      canSimulate: false,
      tone: "error",
      message: `开单金额 ${formatNumber(margin, 4)} 超过当前可用余额 ${formatNumber(balanceLimit.available, 4)} 的 95% 上限 ${formatNumber(balanceLimit.maxOpenAmount, 4)}，无法开单或模拟执行。`,
    });
    return;
  }
  if (!symbolInfoReady) {
    updateOpenValidationHint({ canCreate: false, tone: "", message: "正在加载交易对规则，暂时无法确认最小开单金额。" });
    return;
  }
  if (currentSymbolInfo.allowed === false) {
    updateOpenValidationHint({ canCreate: false, tone: "error", message: `${activeSymbol} 不在当前白名单内，真实开单会被拒绝。` });
    return;
  }
  if (roundQty <= 0) {
    updateOpenValidationHint({ canCreate: false, tone: "", message: `等待订单簿价格更新后计算每轮数量。最小下单金额 ${formatNumber(minNotional, 4)}。` });
    return;
  }
  if (notionalPerRound < minNotional) {
    updateOpenValidationHint({ canCreate: false, tone: "error", message: `每轮开单金额 ${formatNumber(notionalPerRound, 4)} 低于交易所最小下单金额 ${formatNumber(minNotional, 4)}，无法开单。` });
    return;
  }
  updateOpenValidationHint({ canCreate: true, tone: "success", message: `最小下单金额 ${formatNumber(minNotional, 4)}，当前每轮开单金额 ${formatNumber(notionalPerRound, 4)}，可以开单。` });
}

function recalculateCloseAmount() {
  const closeQty = Number(document.getElementById("closeQty").value) || 0;
  const rounds = Math.max(Number(document.getElementById("closeRounds").value) || 1, 1);
  const perRoundQty = closeQty / rounds;
  const totalNotional = closeQty * latestReferencePrice;
  const perRoundNotional = perRoundQty * latestReferencePrice;
  const maxCloseableQty = maxCloseableQtyForSymbol(activeSymbol);
  const minNotional = Number(currentSymbolInfo.min_notional || 0);

  document.getElementById("closeRoundQty").value = perRoundQty > 0 ? perRoundQty.toFixed(6) : "0";
  document.getElementById("closeTotalNotional").textContent = formatNumber(totalNotional, 4);
  document.getElementById("closeNotionalPerRound").textContent = formatNumber(perRoundNotional, 4);
  document.getElementById("maxCloseableQty").textContent = formatNumber(maxCloseableQty, 6);
  if (executionMode === "paired_close") {
    refreshDerivedStats({ totalNotional, perRoundNotional, estimatedQty: perRoundQty });
  }

  if (!symbolInfoReady) {
    updateCloseValidationHint({ canCreate: false, tone: "", message: "正在加载交易对规则，暂时无法确认双向平仓参数。" });
    return;
  }
  if (closeQty <= 0) {
    updateCloseValidationHint({ canCreate: false, tone: "", message: "请输入平仓数量。" });
    return;
  }
  if (maxCloseableQty <= 0) {
    updateCloseValidationHint({ canCreate: false, tone: "error", message: "当前账户不存在可双向平仓的双边持仓。" });
    return;
  }
  if (closeQty > maxCloseableQty) {
    updateCloseValidationHint({ canCreate: false, tone: "error", message: `平仓数量 ${formatNumber(closeQty, 6)} 超过当前可双向平仓数量 ${formatNumber(maxCloseableQty, 6)}。` });
    return;
  }
  if (perRoundQty <= 0) {
    updateCloseValidationHint({ canCreate: false, tone: "error", message: "每轮数量归一化后为 0，无法平仓。" });
    return;
  }
  if (perRoundNotional < minNotional) {
    updateCloseValidationHint({ canCreate: false, tone: "error", message: `每轮平仓名义金额 ${formatNumber(perRoundNotional, 4)} 低于交易所最小下单金额 ${formatNumber(minNotional, 4)}。` });
    return;
  }
  updateCloseValidationHint({ canCreate: true, tone: "success", message: `当前可双向平仓数量 ${formatNumber(maxCloseableQty, 6)}，每轮名义平仓金额 ${formatNumber(perRoundNotional, 4)}，可以平仓。` });
}

function summarizeSessionEvent(event) {
  const payload = event.payload || {};
  switch (event.event_type) {
    case "session_created":
      return { level: "info", message: `真实开仓会话已创建: ${payload.symbol} | ${payload.trend_bias} | ${payload.round_count} 轮` };
    case "session_preflight_failed":
      return { level: "error", message: `真实开仓预检失败: ${payload.error || "未知错误"}` };
    case "round_started":
      return { level: "info", message: `第 ${payload.round_index} 轮开始执行开仓` };
    case "stage1_fill":
    case "stage1_late_fill":
      return { level: "success", message: `第 ${payload.round_index} 轮 Stage1 成交 ${payload.filled_qty}` };
    case "stage2_fill":
    case "stage2_late_fill":
      return { level: "success", message: `第 ${payload.round_index} 轮 Stage2 成交 ${payload.filled_qty}，剩余 ${payload.remaining_qty}` };
    case "stage2_zero_fill_retry":
      return { level: "warn", message: `第 ${payload.round_index} 轮 Stage2 零成交重试，第 ${payload.retry} 次` };
    case "stage2_below_min_carryover":
      return { level: "warn", message: `第 ${payload.round_index} 轮 Stage2 剩余 ${payload.remaining_qty} 金额低于最小下单金额，残量结转到下一轮` };
    case "stage2_carryover_persisted":
      return { level: "warn", message: `第 ${payload.round_index} 轮保留待补残量 ${payload.carryover_qty}` };
    case "round_completed":
      return { level: "success", message: `第 ${payload.round_index} 轮开仓完成，残量 ${payload.stage2_remaining_qty || "0"}` };
    case "round_skipped":
      return { level: "warn", message: `第 ${payload.round_index} 轮因 Stage1 连续零成交被跳过` };
    case "final_alignment_started":
      return { level: "warn", message: `开始最终市价对齐，当前残量 ${payload.carryover_qty}` };
    case "final_alignment_market_reduce":
      return { level: "warn", message: `最终对齐减仓 ${payload.position_side} ${payload.qty}` };
    case "final_alignment_flatten_both_sides":
      return { level: "warn", message: "少侧不足最小减仓量，双边市价清仓对齐" };
    case "final_alignment_completed":
      return { level: "success", message: `最终对齐完成: ${payload.mode || "完成"}` };
    case "final_alignment_failed":
      return { level: "error", message: `最终对齐失败: ${payload.error || "未知错误"}` };
    case "session_completed":
      return { level: "success", message: `真实开仓会话完成，最终对齐结果 ${formatAlignmentStatus(payload.final_alignment_status)}` };
    case "session_failed":
      return { level: "error", message: `真实开仓会话失败: ${payload.error || "未知错误"}` };
    case "close_session_created":
      return { level: "info", message: `真实双向平仓会话已创建: ${payload.symbol} | ${payload.trend_bias} | ${payload.round_count} 轮 | 数量 ${payload.close_qty}` };
    case "close_session_preflight_failed":
      return { level: "error", message: `真实双向平仓预检失败: ${payload.error || "未知错误"}` };
    case "close_round_started":
      return { level: "info", message: `第 ${payload.round_index} 轮开始执行双向平仓` };
    case "close_round_skipped":
      return { level: "warn", message: `第 ${payload.round_index} 轮无可双向平仓持仓，已跳过` };
    case "close_stage1_fill":
    case "close_stage1_late_fill":
      return { level: "success", message: `第 ${payload.round_index} 轮 Stage1 平仓成交 ${payload.filled_qty}` };
    case "close_stage2_fill":
    case "close_stage2_late_fill":
      return { level: "success", message: `第 ${payload.round_index} 轮 Stage2 平仓成交 ${payload.filled_qty}` };
    case "close_stage2_zero_fill_retry":
      return { level: "warn", message: `第 ${payload.round_index} 轮 Stage2 平仓零成交重试，第 ${payload.retry} 次` };
    case "close_round_completed":
      return { level: "success", message: `第 ${payload.round_index} 轮双向平仓完成` };
    case "close_round_interval_wait":
      return { level: "info", message: `等待 ${payload.wait_seconds} 秒后进入下一轮双向平仓` };
    case "close_session_completed":
      return { level: "success", message: "真实双向平仓会话完成" };
    case "close_session_failed":
      return { level: "error", message: `真实双向平仓会话失败: ${payload.error || "未知错误"}` };
    case "single_open_session_created":
      return { level: "info", message: `真实单向开仓会话已创建: ${payload.symbol} | ${payload.selected_position_side} | ${payload.round_count} 轮 | 数量 ${payload.open_qty}` };
    case "single_open_session_preflight_failed":
      return { level: "error", message: `真实单向开仓预检失败: ${payload.error || "未知错误"}` };
    case "single_open_round_started":
      return { level: "info", message: `第 ${payload.round_index} 轮开始执行单向开仓` };
    case "single_open_fill":
    case "single_open_late_fill":
      return { level: "success", message: `第 ${payload.round_index} 轮单向开仓成交 ${payload.filled_qty}` };
    case "single_open_zero_fill_retry":
      return { level: "warn", message: `第 ${payload.round_index} 轮单向开仓零成交重试，第 ${payload.retry} 次` };
    case "single_open_round_completed":
      return { level: "success", message: `第 ${payload.round_index} 轮单向开仓完成` };
    case "single_open_round_skipped":
      return { level: "warn", message: `第 ${payload.round_index} 轮单向开仓已跳过` };
    case "single_open_round_interval_wait":
      return { level: "info", message: `等待 ${payload.wait_seconds} 秒后进入下一轮单向开仓` };
    case "single_open_market_fallback":
      return { level: "warn", message: `第 ${payload.round_index} 轮单向开仓已转市价补单 ${payload.filled_qty}` };
    case "single_open_session_completed":
      return { level: "success", message: "真实单向开仓会话完成" };
    case "single_open_session_failed":
      return { level: "error", message: `真实单向开仓会话失败: ${payload.error || "未知错误"}` };
    case "single_close_session_created":      return { level: "info", message: `真实单向平仓会话已创建: ${payload.symbol} | ${payload.selected_position_side} | ${payload.round_count} 轮 | 数量 ${payload.close_qty}` };
    case "single_close_session_preflight_failed":
      return { level: "error", message: `真实单向平仓预检失败: ${payload.error || "未知错误"}` };
    case "single_close_round_started":
      return { level: "info", message: `第 ${payload.round_index} 轮开始执行单向平仓` };
    case "single_close_fill":
    case "single_close_late_fill":
      return { level: "success", message: `第 ${payload.round_index} 轮单向平仓成交 ${payload.filled_qty}` };
    case "single_close_zero_fill_retry":
      return { level: "warn", message: `第 ${payload.round_index} 轮单向平仓零成交重试，第 ${payload.retry} 次` };
    case "single_close_round_completed":
      return { level: "success", message: `第 ${payload.round_index} 轮单向平仓完成` };
    case "single_close_round_skipped":
      return { level: "warn", message: `第 ${payload.round_index} 轮无可平持仓，已跳过` };
    case "single_close_round_interval_wait":
      return { level: "info", message: `等待 ${payload.wait_seconds} 秒后进入下一轮单向平仓` };
    case "single_close_market_fallback":
      return { level: "warn", message: `第 ${payload.round_index} 轮单向平仓已转市价补单 ${payload.filled_qty}` };
    case "single_close_session_completed":
      return { level: "success", message: "真实单向平仓会话完成" };
    case "single_close_session_failed":
      return { level: "error", message: `真实单向平仓会话失败: ${payload.error || "未知错误"}` };
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
    appendLog(summary.level, summary.message, event.created_at);
  });
}

function updateRealSessionStats(session) {
  const terminalRounds = Array.isArray(session.rounds)
    ? session.rounds.filter((round) => ["round_completed", "stage1_skipped"].includes(String(round.status || ""))).length
    : 0;
  document.getElementById("statSessionStatus").textContent = session.status || "idle";
  document.getElementById("statMode").textContent = formatModeLabel(session.session_kind || executionMode);
  document.getElementById("statRounds").textContent = `${terminalRounds} / ${session.round_count || 0}`;
  document.getElementById("statCarryoverQty").textContent = formatNumber(session.stage2_carryover_qty || 0, 6);
  document.getElementById("statFinalAlignment").textContent = formatAlignmentStatus(session.final_alignment_status);
  document.getElementById("statLastQty").textContent = formatNumber(session.round_qty || 0, 8);
  accountSelect.disabled = !isTerminalSession(session.status);
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
    accountSelect.disabled = availableAccounts.length <= 1;
  }
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
      appendLog("error", `Session refresh failed: ${String(fallbackError || error)}`);
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
  accountSelect.disabled = true;
  loadActiveSessionSnapshot().catch((error) => {
    appendLog("error", `Session refresh failed: ${String(error)}`);
    stopSessionPolling();
  });
  activeSessionPoller = setInterval(pollActiveSession, 2000);
}

async function refreshSymbolInfo(symbol) {
  const symbolInfo = await request(`/symbols/${encodeURIComponent(symbol)}`);
  setSymbolInfo(symbolInfo);
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
    appendLog("warn", "Please enter a valid symbol");
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
    const symbolInfo = await refreshSymbolInfo(targetSymbol);
    temporaryCustomSymbol = symbolInfo.allowed ? null : targetSymbol;
    setActiveSymbol(targetSymbol, true);
    setSymbolInfo(symbolInfo);
    if (shouldReconnect) {
      openSse();
      await request("/market/connect", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ symbol: targetSymbol })
      });
    }
    appendLog("info", `Symbol switched to ${targetSymbol}`);
    if (symbolInfo.allowed === false) {
      appendLog("warn", `${targetSymbol} exists on Binance USD-M futures, but it is not in the current whitelist, so real trading will fail.`);
    }
    return true;
  } catch (error) {
    temporaryCustomSymbol = previousTemporaryCustomSymbol;
    setActiveSymbol(previousSymbol, true);
    setSymbolInfo(previousSymbolInfo);
    appendLog("error", `Failed to switch symbol ${targetSymbol}: ${String(error)}`);
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
        message: "Disconnected",
      });
    }
  } catch (error) {
    appendLog("error", String(error));
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
    closeSse();
    const payload = await request("/config/accounts/select", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ account_id: nextAccountId })
    });
    setCurrentAccount(payload.account.id, payload.account.name, true);
    openSse();
    try {
      await refreshSymbolInfo(activeSymbol);
      if (shouldReconnect) {
        await request("/market/connect", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ symbol: activeSymbol })
        });
      } else {
        setConnectionState({
          connected: false,
          status: "disconnected",
          symbol: activeSymbol,
          account_id: payload.account.id,
          account_name: payload.account.name,
          message: "Disconnected",
        });
      }
      appendLog("success", `Current account switched to ${payload.account.name}`);
      schedulePrecheck();
    } catch (error) {
      connectionToggle.checked = false;
      setConnectionState({
        connected: false,
        status: "error",
        symbol: activeSymbol,
        account_id: payload.account.id,
        account_name: payload.account.name,
        message: String(error)
      });
      appendLog("error", `Account switched to ${payload.account.name}, but loading ${activeSymbol} failed: ${String(error)}`);
    }
  } catch (error) {
    setCurrentAccount(previousAccount.id, previousAccount.name, true);
    openSse();
    appendLog("error", `Failed to switch account: ${String(error)}`);
  }
});

editWhitelistBtn.addEventListener("click", async () => {
  try {
    const initialValue = whitelistSymbols.join(", ");
    const input = window.prompt("Edit whitelist symbols, separated by commas", initialValue);
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
    appendLog("success", `Whitelist updated: ${(payload.symbols || []).join(", ")}`);
    await refreshSymbolInfo(currentSymbol);
    if (!(payload.symbols || []).includes(currentSymbol)) {
      appendLog("warn", `${currentSymbol} is no longer in the whitelist, so real trading will fail.`);
    }
  } catch (error) {
    appendLog("error", `Failed to update whitelist: ${String(error)}`);
  }
});

confirmSymbolBtn.addEventListener("click", async () => {
  const currentSymbol = normalizeSymbol(executionSymbol.value || activeSymbol);
  const input = window.prompt("Enter a custom symbol", currentSymbol);
  if (input === null) {
    rebuildSymbolOptions(activeSymbol);
    return;
  }
  await switchSymbol(input, connectionToggle.checked);
});

orderBookInput.addEventListener("change", async (event) => {
  await switchSymbol(event.target.value, connectionToggle.checked);
});

simulateBtn.addEventListener("click", async () => {
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
    appendLog("error", String(error));
  }
});

createBtn.addEventListener("click", async () => {
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
    appendLog("success", `Open session created: ${payload.session_id}`);
    startSessionPolling(payload.session_id);
  } catch (error) {
    if (error.precheck) applyPrecheckResult("paired_open", error.precheck);
    appendLog("error", String(error));
  } finally {
    setPrecheckPaused(false);
  }
});

createCloseBtn.addEventListener("click", async () => {
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
    appendLog("success", `Close session created: ${payload.session_id}`);
    startSessionPolling(payload.session_id);
  } catch (error) {
    if (error.precheck) applyPrecheckResult("paired_close", error.precheck);
    appendLog("error", String(error));
  } finally {
    setPrecheckPaused(false);
  }
});

createSingleOpenBtn.addEventListener("click", async () => {
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
    appendLog("success", `Single-side open session created: ${payload.session_id}`);
    startSessionPolling(payload.session_id);
  } catch (error) {
    if (error.precheck) applyPrecheckResult("single_open", error.precheck);
    appendLog("error", String(error));
  } finally {
    setPrecheckPaused(false);
  }
});

createSingleCloseBtn.addEventListener("click", async () => {
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
    appendLog("success", `Single-side close session created: ${payload.session_id}`);
    startSessionPolling(payload.session_id);
  } catch (error) {
    if (error.precheck) applyPrecheckResult("single_close", error.precheck);
    appendLog("error", String(error));
  } finally {
    setPrecheckPaused(false);
  }
});

Object.entries(modeButtons).forEach(([mode, button]) => {
  button.addEventListener("click", () => setExecutionMode(mode));
});

["calcMargin", "leverage", "calcRounds"].forEach((id) => {
  document.getElementById(id).addEventListener("input", () => {
    recalculateOpenAmount();
    schedulePrecheck("paired_open");
  });
});
["closeQty", "closeRounds"].forEach((id) => {
  document.getElementById(id).addEventListener("input", () => {
    recalculateCloseAmount();
    schedulePrecheck("paired_close");
  });
});
["singleOpenQty", "singleOpenRounds", "singleOpenLeverage"].forEach((id) => {
  document.getElementById(id)?.addEventListener("input", () => {
    recalculateSingleOpenAmount();
    schedulePrecheck("single_open");
  });
});
document.getElementById("singleOpenMode")?.addEventListener("change", () => {
  recalculateSingleOpenAmount();
  schedulePrecheck("single_open");
});
document.getElementById("singleOpenOrder")?.addEventListener("change", (event) => {
  syncPositionSideTone(event.target);
  recalculateSingleOpenAmount();
  schedulePrecheck("single_open");
});
["singleCloseQty", "singleCloseRounds"].forEach((id) => {
  document.getElementById(id)?.addEventListener("input", () => {
    recalculateSingleCloseAmount();
    schedulePrecheck("single_close");
  });
});
document.getElementById("singleCloseMode")?.addEventListener("change", () => {
  recalculateSingleCloseAmount();
  schedulePrecheck("single_close");
});
document.getElementById("singleCloseOrder")?.addEventListener("change", (event) => {
  syncPositionSideTone(event.target);
  recalculateSingleCloseAmount();
  schedulePrecheck("single_close");
});
document.getElementById("trend")?.addEventListener("change", (event) => {
  syncTrendSelectTone(event.target);
  schedulePrecheck("paired_open");
});
document.getElementById("closeTrend")?.addEventListener("change", (event) => {
  syncTrendSelectTone(event.target);
  schedulePrecheck("paired_close");
});

asksContainer.innerHTML = '<div class="empty-state orderbook-empty">Enable the stream to load asks</div>';
bidsContainer.innerHTML = '<div class="empty-state orderbook-empty">Enable the stream to load bids</div>';
setActiveSymbol(activeSymbol, false);
renderAccountOverview({ status: "idle", message: "Disconnected", totals: {}, positions: [], account_id: currentAccount.id, account_name: currentAccount.name });
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
setExecutionMode("paired_open");
appendLog("info", "Console ready. Pick a symbol from the whitelist or switch to a custom symbol.");
Promise.allSettled([
  loadAccounts(),
  loadWhitelist(),
  refreshSymbolInfo(activeSymbol),
]).then((results) => {
  const [accountsResult, whitelistResult, symbolInfoResult] = results;
  if (accountsResult.status === "rejected") {
    appendLog("error", `Failed to load account list: ${String(accountsResult.reason)}`);
  }
  if (whitelistResult.status === "rejected") {
    temporaryCustomSymbol = activeSymbol;
    rebuildSymbolOptions(activeSymbol);
    appendLog("error", `Failed to load whitelist: ${String(whitelistResult.reason)}`);
  }
  if (symbolInfoResult.status === "rejected") {
    appendLog("error", `Failed to load symbol rules: ${String(symbolInfoResult.reason)}`);
  }
});
