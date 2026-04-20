import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";

const appPath = path.join(process.cwd(), "paired_opener", "static", "app.js");
const appSource = fs.readFileSync(appPath, "utf8");

function loadResolveResidualQty() {
  const match = appSource.match(/function resolveResidualQty\(source = \{\}\) \{[\s\S]*?\n\}/);
  assert.ok(match, "resolveResidualQty should exist in app.js");
  const sandbox = {};
  vm.runInNewContext(`${match[0]}; this.resolveResidualQty = resolveResidualQty;`, sandbox);
  return sandbox.resolveResidualQty;
}

function loadStatsFunctions() {
  const resolveMatch = appSource.match(/function resolveResidualQty\(source = \{\}\) \{[\s\S]*?\n\}/);
  const executionMatch = appSource.match(/function updateExecutionStats\(stats\) \{[\s\S]*?\n\}/);
  const sessionMatch = appSource.match(/function updateRealSessionStats\(session\) \{[\s\S]*?\n\}/);
  assert.ok(resolveMatch, "resolveResidualQty should exist in app.js");
  assert.ok(executionMatch, "updateExecutionStats should exist in app.js");
  assert.ok(sessionMatch, "updateRealSessionStats should exist in app.js");

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
    accountSelect: { disabled: false },
    executionMode: "single_open",
    formatNumber(value) {
      return `formatted:${value}`;
    },
    formatModeLabel(value) {
      return `mode:${value}`;
    },
    formatAlignmentStatus(value) {
      return `alignment:${value}`;
    },
    isTerminalSession(status) {
      return ["completed", "completed_with_skips", "aborted", "exception"].includes(String(status || ""));
    },
    Array,
    String,
    Number,
  };
  vm.runInNewContext(
    `${resolveMatch[0]}\n${executionMatch[0]}\n${sessionMatch[0]}\nthis.resolveResidualQty = resolveResidualQty;\nthis.updateExecutionStats = updateExecutionStats;\nthis.updateRealSessionStats = updateRealSessionStats;`,
    sandbox,
  );
  return {
    elements,
    updateExecutionStats: sandbox.updateExecutionStats,
    updateRealSessionStats: sandbox.updateRealSessionStats,
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

assert.equal(
  resolveResidualQty({
    final_unaligned_qty: "0",
    stage2_carryover_qty: "0.003",
    carryover_qty: "0.001",
  }),
  "0",
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
  const mergedSession = {
    status: "completed_with_skips",
    session_kind: "single_open",
    round_count: 2,
    round_qty: "0.01",
    rounds: [{ status: "round_completed" }],
    stage2_carryover_qty: "0.003",
    final_unaligned_qty: "0",
    final_alignment_status: "not_needed",
  };
  updateRealSessionStats(mergedSession);
  assert.equal(elements.get("statCarryoverQty").textContent, "formatted:0");
  assert.equal(elements.get("statFinalAlignment").textContent, "alignment:not_needed");
}
