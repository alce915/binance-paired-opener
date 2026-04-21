# 开发日志（2026-04-21）

## 版本概览

- 远端分支：`origin/main`
- 远端最新提交：`5211ef78568255211463d331bdf4f2da38a01a5a`
- 提交消息：`fix: align execution policy simulation and recovery flow`

本次版本的主目标，是把“单向/双向开平仓”的真实执行、模拟执行、恢复判定、前端残量展示和回归工具链，收敛到同一套更稳定的语义上，并把多轮代码审查中发现的问题逐项修掉。

## 本轮完成内容

### 1. 执行策略与共享语义

- 新增 `paired_opener/execution_policy.py`
- 抽出并统一了执行策略解析：
  - `maker_first`
  - `balanced`
- 补齐了这组策略字段在配置、Schema、SessionSpec、持久化和返回结构里的贯通：
  - `execution_profile`
  - `market_fallback_max_ratio`
  - `market_fallback_min_residual_qty`
  - `max_reprice_ticks`
  - `max_spread_bps`
  - `max_reference_deviation_bps`

### 2. 真实执行链路修正

主要涉及：

- `paired_opener/engine.py`
- `paired_opener/service.py`
- `paired_opener/binance.py`
- `paired_opener/exchange.py`
- `paired_opener/classified_gateway.py`

本轮修掉的重点包括：

- regular 单向开仓不再错误地提前“假支持” one-way 账户
- create 路径里的 Hedge Mode 严格确认后移，不再在本地校验失败前就改交易所状态
- `/sessions/precheck` 保持只读，不再因为 strict hedge fallback 去触发写接口
- 启动恢复时：
  - 已经逻辑完成的 open session 不再误判成 `RECOVERABLE`
  - 单向开仓轮次耗尽但带残量的会话，会直接收口到终态，不再卡成 `EXCEPTION`
- 用户流健康判断改成“连接态 + task 存活”联合判断，避免：
  - 冷启动第一单误判退化
  - 重连窗口把陈旧缓存当成健康数据

### 3. 模拟执行与真实执行对齐

主要涉及：

- `paired_opener/market_stream.py`

这部分是本轮改动最多的地方，核心是把“模拟结果不能比实盘更乐观”这件事尽量做实。已完成的修正包括：

- `maker_first` 默认 fallback 在模拟里真正生效，而不是只回显解析结果
- price guard 在模拟里按真实执行语义生效
- 多轮模拟时，每轮都会重置 price guard 参考价
- 命中 price guard 时：
  - 已完成的轮次/阶段进度会被保留
  - 不再把“部分完成后被拦”错误表示成纯 `blocked`
- `paired_open` 的模拟拆成真实的 `stage1 -> stage2` 语义：
  - `stage1` 先模拟
  - 再用 `stage1_filled_qty + carryover` 推导 `stage2`
  - `stage2` quote 和 guard 时机也对齐到了真实链路

### 4. 残量语义与前端展示

主要涉及：

- `paired_opener/schemas.py`
- `paired_opener/static/app.js`
- `paired_opener/static/index.html`
- `paired_opener/api.py`

已完成的前端/接口收口：

- `/sessions/{id}/updates` 的 `SessionSummary` 现在会携带：
  - `stage2_carryover_qty`
  - `final_alignment_status`
  - `final_unaligned_qty`
- 前端残量展示统一优先使用 `final_unaligned_qty`
- 如果 `final_unaligned_qty` 不存在，才退回旧字段
- UI 文案已统一往“未完成数量/残量”方向调整
- `app.js` 静态资源版本号已更新，避免浏览器继续命中旧缓存

### 5. 服务脚本与回归工具

主要涉及：

- `scripts/restart_service.ps1`
- `scripts/run_regression_tests.ps1`
- `运行回归测试.bat`

本轮新增了可双击运行的回归入口，便于在当前机器这种 Python/环境行为不稳定的情况下，由本机桌面直接发起统一回归并把结果写入固定路径。

输出位置：

- `.codex-runtime/regression/latest.log`
- `.codex-runtime/regression/latest.status.txt`
- `.codex-runtime/regression/latest.summary.txt`

## 本轮通过的回归结果

最终结果来自：

- `D:\codex\币安自动开单系统\.codex-runtime\regression\latest.status.txt`
- `D:\codex\币安自动开单系统\.codex-runtime\regression\latest.summary.txt`
- `D:\codex\币安自动开单系统\.codex-runtime\regression\latest.log`

结论：

- `pytest`：`86 passed in 40.09s`
- Node 前端脚本：通过
- 总状态：`PASSED`

说明：

- 日志里还有一个 `pytest` 退出后的临时目录清理 `PermissionError`
- 该问题不影响本次测试退出码，最终回归判断仍然是通过

## 当前工作区状态说明

这个点非常重要，下一窗口开发前请先注意：

- 远端 `origin/main` 已经包含本次版本，提交是 `5211ef7`
- 但**当前这个本地目录的 `.git` 目录仍然不可写**
- 因此本次 commit/push 不是在当前目录原地完成的，而是通过“临时可写副本 + git-sync + 8800 代理”推上去的

这导致当前目录存在一个“代码内容是新的，但本地 Git 元数据不是最新提交态”的现象：

- 当前目录 `git status` 仍是脏的
- 当前目录 `git log` 仍停在更早的本地提交
- 但远端 `main` 已经是 `5211ef7`

## 给下一窗口的建议

如果下一窗口要继续开发，建议优先按下面顺序处理：

1. 先判断是否继续沿用当前目录

- 如果只是继续读代码、改代码，可以继续在当前目录工作
- 但不要把当前目录的 Git 状态误认为“还没提交”

2. 如果下一步需要继续提交

优先选一个干净、`.git` 可写的副本继续工作，而不是直接在当前目录上做 Git 操作。

3. 如果下一窗口要做功能开发

建议先从这几个文件入手理解当前版本：

- `paired_opener/execution_policy.py`
- `paired_opener/service.py`
- `paired_opener/engine.py`
- `paired_opener/market_stream.py`
- `paired_opener/static/app.js`

4. 如果下一窗口要做问题排查

先看这几个输出：

- `D:\codex\币安自动开单系统\.codex-runtime\regression\latest.log`
- `D:\codex\币安自动开单系统\.codex-runtime\regression\latest.summary.txt`
- `D:\codex\币安自动开单系统\.codex-runtime\regression\latest.status.txt`

## 一句话交接

这版已经完成提交并推到远端 `main@5211ef7`，核心是把执行策略、模拟语义、恢复判定、残量展示和回归入口统一收口；接力开发时，最需要小心的是“当前目录本地 Git 仍是脏的，但远端代码已经是最新版本”。
