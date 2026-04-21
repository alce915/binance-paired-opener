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

function loadResolveResidualQty() {
  const source = extract(/function resolveResidualQty\(source = \{\}\) \{[\s\S]*?\n\}/, "resolveResidualQty");
  const sandbox = {};
  vm.runInNewContext(`${source}; this.resolveResidualQty = resolveResidualQty;`, sandbox);
  return sandbox.resolveResidualQty;
}

function loadStatsFunctions() {
  const resolveSource = extract(/function resolveResidualQty\(source = \{\}\) \{[\s\S]*?\n\}/, "resolveResidualQty");
  const executionSource = extract(/function updateExecutionStats\(stats\) \{[\s\S]*?\n\}/, "updateExecutionStats");
  const sessionSource = extract(/function updateRealSessionStats\(session\) \{[\s\S]*?\n\}/, "updateRealSessionStats");

  const elements = new Map();
  const sandbox = {
    document: {
      getElementById(id) {
        if (!elements.has(id)) {
          elements.set(id, { textContent: "", classList: { add() {}, remove() {} } });
        }
        return elements.get(id);
      },
    },
    executionMode: "single_open",
    latestExecutionStatsState: null,
    simulationRunInFlight: false,
    simulationAbortInFlight: false,
    activeSessionState: null,
    activeExecutionSummary: null,
    latestResidualSideLabel: "--",
    formatNumber(value) {
      return `formatted:${value}`;
    },
    formatModeLabel(value) {
      return `mode:${value}`;
    },
    formatAlignmentStatus(value) {
      return `alignment:${value}`;
    },
    updateAbortStateLabel() {},
    buildExecutionSummary(source, overrides = {}) {
      return { source, overrides };
    },
    updateExecutionSummary(summary) {
      sandbox.activeExecutionSummary = summary;
    },
    hasActiveExecutionSession() {
      return false;
    },
    isTerminalSession(status) {
      return ["completed", "completed_with_skips", "aborted", "exception"].includes(String(status || ""));
    },
    isTerminalSimulationStatus(status) {
      return ["idle", "completed", "completed_with_skips", "blocked", "aborted", "exception"].includes(String(status || "idle"));
    },
    refreshExecutionActionButtons() {},
    Array,
    String,
    Number,
  };

  vm.runInNewContext(
    `
${resolveSource}
${executionSource}
${sessionSource}
this.resolveResidualQty = resolveResidualQty;
this.updateExecutionStats = updateExecutionStats;
this.updateRealSessionStats = updateRealSessionStats;
`,
    sandbox,
  );

  return {
    elements,
    updateExecutionStats: sandbox.updateExecutionStats,
    updateRealSessionStats: sandbox.updateRealSessionStats,
  };
}

function loadLabelFunctions() {
  const modeSource = extract(/function formatModeLabel\(mode\) \{[\s\S]*?\n\}/, "formatModeLabel");
  const alignmentSource = extract(/function formatAlignmentStatus\(status\) \{[\s\S]*?\n\}/, "formatAlignmentStatus");
  const connectionSource = extract(/const CONNECTION_STATUS_LABELS = \{[\s\S]*?\n\};/, "CONNECTION_STATUS_LABELS");
  const copySource = extract(/function copyOrDefault\(key, fallback, params = \{\}\) \{[\s\S]*?\n\}/, "copyOrDefault");
  const formatCopySource = extract(/function formatCopy\(key, params = \{\}\) \{[\s\S]*?\n\}/, "formatCopy");

  const messages = {
    "console.mode_labels.paired_open": "Paired open",
    "console.mode_labels.paired_close": "Paired close",
    "console.mode_labels.single_open": "Single open",
    "console.mode_labels.single_close": "Single close",
    "console.alignment.not_needed": "No alignment",
    "console.alignment.market_aligned": "Market aligned",
    "console.alignment.flattened_both_sides": "Flattened both sides",
    "console.alignment.failed": "Alignment failed",
    "console.alignment.carryover_pending": "Carryover pending",
    "runtime.connection_connected": "Connected",
    "runtime.connection_connecting": "Connecting",
    "runtime.connection_disconnected": "Disconnected",
    "runtime.connection_error": "Error",
    "runtime.connection_idle": "Idle",
  };
  const sandbox = {
    I18N_MESSAGES: messages,
  };
  vm.runInNewContext(
    `
${formatCopySource}
${copySource}
${connectionSource}
${modeSource}
${alignmentSource}
this.formatModeLabel = formatModeLabel;
this.formatAlignmentStatus = formatAlignmentStatus;
this.CONNECTION_STATUS_LABELS = CONNECTION_STATUS_LABELS;
`,
    sandbox,
  );
  return {
    formatModeLabel: sandbox.formatModeLabel,
    formatAlignmentStatus: sandbox.formatAlignmentStatus,
    CONNECTION_STATUS_LABELS: sandbox.CONNECTION_STATUS_LABELS,
  };
}

const resolveResidualQty = loadResolveResidualQty();

assert.equal(
  resolveResidualQty({
    final_unaligned_qty: "0.005",
    stage2_carryover_qty: "0.002",
    carryover_qty: "0.001",
  }),
  "0.005",
);
assert.equal(resolveResidualQty({ stage2_carryover_qty: "0.003", carryover_qty: "0.001" }), "0.003");
assert.equal(resolveResidualQty({ carryover_qty: "0.002" }), "0.002");
assert.equal(resolveResidualQty({}), 0);

{
  const { elements, updateExecutionStats } = loadStatsFunctions();
  updateExecutionStats({
    status: "completed_with_skips",
    rounds_completed: 1,
    rounds_total: 2,
    total_notional: "100",
    notional_per_round: "50",
    last_qty: "0.01",
    mode: "single_open",
    final_unaligned_qty: "0.009",
    carryover_qty: "0.001",
    final_alignment_status: "not_needed",
  });
  assert.equal(elements.get("statCarryoverQty").textContent, "formatted:0.009");
  assert.equal(elements.get("statFinalAlignment").textContent, "alignment:not_needed");
}

{
  const { elements, updateRealSessionStats } = loadStatsFunctions();
  updateRealSessionStats({
    status: "completed_with_skips",
    session_kind: "single_open",
    round_count: 2,
    round_qty: "0.01",
    rounds: [{ status: "round_completed" }],
    stage2_carryover_qty: "0.003",
    final_unaligned_qty: "0",
    final_alignment_status: "not_needed",
  });
  assert.equal(elements.get("statCarryoverQty").textContent, "formatted:0");
  assert.equal(elements.get("statFinalAlignment").textContent, "alignment:not_needed");
}

{
  const { formatModeLabel, formatAlignmentStatus, CONNECTION_STATUS_LABELS } = loadLabelFunctions();
  assert.equal(formatModeLabel("paired_open"), "Paired open");
  assert.equal(formatModeLabel("single_close"), "Single close");
  assert.equal(formatAlignmentStatus("market_aligned"), "Market aligned");
  assert.equal(formatAlignmentStatus("not_needed"), "No alignment");
  assert.equal(CONNECTION_STATUS_LABELS.connected, "Connected");
  assert.equal(CONNECTION_STATUS_LABELS.error, "Error");
}
