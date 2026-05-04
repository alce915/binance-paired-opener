import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";

function compileResolveStructuredMessage(htmlPath) {
  const source = fs.readFileSync(htmlPath, "utf8");
  const match = source.match(/function resolveStructuredMessage\(payload, fallback = '-'\) \{[\s\S]*?\n    \}/);
  assert.ok(match, `resolveStructuredMessage should exist in ${htmlPath}`);

  const messages = {
    "runtime.monitor_all_healthy": "全部账户监控正常",
  };
  const sandbox = {
    formatCopy(key, params = {}) {
      const template = messages[key] || key;
      return template.replace(/\{(\w+)\}/g, (_, name) => {
        const value = params[name];
        return value === undefined || value === null ? `{${name}}` : String(value);
      });
    },
  };

  vm.runInNewContext(`${match[0]}; this.resolveStructuredMessage = resolveStructuredMessage;`, sandbox);
  return sandbox.resolveStructuredMessage;
}

for (const htmlPath of [
  path.join(process.cwd(), "paired_opener", "static", "monitor.html"),
  path.join(process.cwd(), "binance-account-monitor", "monitor_app", "static", "monitor.html"),
]) {
  const resolveStructuredMessage = compileResolveStructuredMessage(htmlPath);

  assert.equal(
    resolveStructuredMessage({ message_code: "runtime.monitor_all_healthy" }, "-"),
    "全部账户监控正常",
    `${htmlPath} should render structured message codes`,
  );
  assert.equal(
    resolveStructuredMessage({ message: "legacy backend message" }, "-"),
    "-",
    `${htmlPath} should not render raw backend messages`,
  );
  assert.equal(
    resolveStructuredMessage({ message_code: "runtime.missing_key" }, "-"),
    "-",
    `${htmlPath} should not render unresolved catalog keys`,
  );
}
