# 亢龙移仓模拟重构设计

## 背景

当前亢龙移仓已经具备账号池、检测链路、确认链路、执行日志和测试模板能力，但执行结果仍不符合移仓模拟的目标。核心问题是现有 `paired_opener/kanglong/simulator.py` 使用检测阶段拿到的固定 `close_price`、`open_price` 和 `fee_rate` 静态生成结果，每轮直接把 `matched_qty` 视为 `submitted_qty`，因此长链路会在一瞬间完成，缺少模拟盘已有的实时盘口撮合、限价等待、补充订单、手续费和价格波动过程。

这次重构的目标不是给亢龙新增一套独立交易引擎，而是让亢龙移仓执行过程复用模拟盘的正式市场数据模拟机制。模拟盘的双向开仓是在同一个账号内同步多空开仓并对齐仓位；亢龙移仓是在不同账号之间同步单方向平仓和开仓。两者的腿类型不同，但每轮的盘口撮合、成交量对齐、补充订单、间隔等待、费用统计和断点恢复规则要保持一致。

本设计以 2026-06-03 采访确认结果为准。当前阶段只开放模拟移仓，不开放真实下单入口；但状态机、事件、checkpoint 和报告结构要为未来真实移仓适配层保留复用空间。

## 目标

- 在账号池和检测链路之间新增移仓设置区域，让用户在检测链路前配置移仓百分比、开仓轮次和轮次间隔。
- 检测链路按用户输入的百分比规划链路，链路配置展示实际会执行的百分比缩放后数量。
- `确认链路` 按钮在检测通过后立即可用，由后端 `available_actions` 和前端当前响应同步驱动，不需要刷新页面。
- `开始模拟移仓` 进入逐组、逐轮、逐盘口撮合的模拟过程，不再静态瞬间完成。
- 移仓执行复用模拟盘的订单撮合、限价等待、补充订单、手续费、滑点和轮次间隔机制。
- 每轮移仓严格保证源账号平仓腿和目标账号开仓腿按同一成交数量对齐；不能出现单腿成功后另一腿缺失仍被当作完成。
- 长链路支持断点续传、自动恢复、安全暂停、继续、停止和异常恢复。
- 执行过程中统计手续费、价格波动导致的价差磨损、滑点和残余数量，并按总览、分组、分轮展示。
- 执行日志使用账号展示名，不展示 `tpl:tpl_...:sub:...` 这类内部合成账号 ID，除非后端确实没有任何可读标签。

## 非目标

- 本阶段不真实下单，不新增真实移仓 UI/API 入口。
- 本阶段不在执行过程中刷新真实账号仓位并重新规划；真实账号模式只使用执行开始时的模拟账本快照。
- 本阶段不新增移仓专属的价格偏移阈值；价格、盘口、限价等待和未成交处理沿用模拟盘规则。
- 本阶段不允许用户手工编辑 planner 生成的账号链路顺序。
- 本阶段不让测试模板账号和真实账号混用在同一条移仓链路中。

## 工程边界

实施计划必须先抽出共享的市场模拟能力，再接入亢龙。亢龙不能直接调用 `SimulationService` 的私有方法来修改单账号模拟账户状态，因为 `SimulationService` 当前绑定了单个模拟账户、SQLite 表和前端模拟盘生命周期。推荐边界是：

- `MarketDataProvider`：按 run 的行情源读取 symbol rules、orderbook 和盘口新鲜度。
- `FeeProvider`：返回和模拟盘一致的 maker/taker 费率。
- `OrderbookMatcher`：只负责对某个 orderbook 快照做数量撮合，返回成交量、均价、手续费、滑点、深度层数、残余量和流动性角色，不写账本。
- `PairedLegMatcher`：负责两条腿的 dry-match、成交量截断和配对结果生成，不写账号账本。
- `SimulationLedger`：负责测试模板账本和真实账号快照账本的读写、checkpoint 和 rollback。
- `KanglongTransferExecutor`：负责按 group/round 调度、调用共享撮合能力、写亢龙事件、更新 checkpoint 和控制状态。

共享模块要由模拟盘和亢龙共同使用。迁移完成后，模拟盘现有双向开仓、双向平仓、单向开仓、单向平仓测试必须继续通过，用来证明抽取没有改变模拟盘行为。

实施顺序固定为先抽取、后接入：

1. 先从 `paired_opener/simulation.py` 抽出共享撮合模块，并把模拟盘回接到新模块。
2. 跑通现有模拟盘单向开仓、单向平仓、双向开仓、双向平仓和补充轮测试，确认行为不变。
3. 再实现亢龙多账号模拟账本和 `KanglongTransferExecutor`。
4. 最后接入亢龙 UI、日志、成本面板和浏览器冒烟。

本阶段只保留未来真实移仓需要的数据形状，不创建真实移仓执行适配器、真实下单 endpoint 或真实下单按钮。实施计划中如果出现真实交易网关写单逻辑，视为越界。

## 移仓设置 UI

移仓设置区域放在账号池/已选账号区域和检测链路区域之间。它使用和模拟盘参数区一致的表单风格，但锁定字段要用只读 input/select 呈现，避免用户误以为可以修改。

锁定字段：

- `执行交易对`：跟随页面交易对选择变化，例如 `ETHUSDC`。
- `开仓模式`：固定为 `移仓`。
- `开仓订单`：跟随移仓方向变化。方向为多时展示 `LONG | 做多开仓`，方向为空时展示 `SHORT | 做空开仓`。
- `杠杆倍数`：固定展示 `75X`。
- `每轮数量（自动计算，ETH 数量）`：不可编辑。检测前显示本地估算或待计算状态；检测后显示服务端按百分比、轮次、交易规则取整后的实际每轮上限。

可编辑字段：

- `开仓数量（百分比）`：替代原来的 ETH 数量输入。默认 `100%`，允许 `1%` 到 `100%`，检测链路时按当前可移仓的盈利方向可平仓数量计算目标移仓量。
- `开仓轮次`：正整数。它不是总执行上限，而是用于计算每轮单方向移仓上限。
- `轮次间隔（秒）`：正数或零。计划轮次和补充轮次都使用同一个间隔。

前端实现必须给移仓设置区和关键控件提供稳定测试锚点，避免后续 UI 样式调整破坏回归测试。第一版固定使用以下 `data-testid`：

- `kanglong-transfer-settings`
- `kanglong-transfer-symbol`
- `kanglong-transfer-mode`
- `kanglong-transfer-order`
- `kanglong-transfer-leverage`
- `kanglong-transfer-percent`
- `kanglong-transfer-round-count`
- `kanglong-transfer-interval-seconds`
- `kanglong-transfer-per-round-qty`
- `kanglong-detect-status`
- `kanglong-confirm-chain`
- `kanglong-start-transfer`
- `kanglong-pause-run`
- `kanglong-resume-run`
- `kanglong-stop-run`
- `kanglong-recover-run`

任何会影响计划的字段变化都必须立即让已检测链路和已确认状态失效，包括交易对、移仓方向、主账号、子账号集合、测试模板、开仓数量百分比、开仓轮次和轮次间隔。失效后 `确认链路` 和 `开始模拟移仓` 禁用，用户必须重新检测。

前端检测请求必须带 `plan_input_hash` 或单调递增的 `request_seq`。检测响应返回时，前端只允许当前输入 hash 或最新序号的响应更新链路、`available_actions` 和按钮状态。用户修改参数后，任何旧检测响应都必须被丢弃，不能重新点亮 `确认链路`。

服务端也必须持久化并校验计划输入 hash。检测成功时保存 `plan_input_hash`；确认链路时请求必须携带当前 `plan_input_hash`，后端将其固化为 `confirmed_plan_hash` 和 `plan_version`；开始执行时请求必须携带 `confirmed_plan_hash`，后端只允许与 run 当前确认版本一致的请求启动。多标签页、页面刷新或旧按钮请求携带旧 hash 时，后端必须返回结构化冲突，不得继续确认或执行旧计划。

## 链路规划语义

百分比基准是当前可移仓的盈利方向可平仓数量。若起始账号在所选方向可平仓 `30 ETH`，用户填写 `50%`，则本次目标移仓量为 `15 ETH`，链路中的每条配置都按 planner 结果和目标量缩放。

数量计算公式固定为：

```text
target_qty_raw = closeable_profit_qty * transfer_percent / 100
target_qty = normalize_qty(target_qty_raw, symbol_rules)
per_round_cap_qty = normalize_qty(target_qty / round_count, symbol_rules)
```

`target_qty` 必须在进入 planner 前确定。planner 使用缩放后的 `target_qty` 做容量、轮次、残差和链路闭合判断，不能先按 100% 规划再在展示层缩放 group 数量。若 `target_qty` 或 `per_round_cap_qty` 归一化后低于交易所最小数量或最小名义金额，检测阶段直接阻断并展示原因。

链路展示必须显示实际执行配置，例如：

```text
账号链式顺序配置
交易对: ETHUSDC
方向: short
共 6 条配置

1. 测试子账号 1 -> 测试主账号，数量:-55.50
2. 测试子账号 2 -> 测试子账号 1，数量:-55.50
3. 测试主账号 -> 测试子账号 2，数量:55.50
```

数量符号沿用现有链路表达：释放方向为负，承接方向为正。展示层使用账号名称或账号展示 ID，不能把内部合成 ID 当作主文案。

开仓轮次用于定义每轮单方向移仓上限。示例：目标移仓 `30 ETH`，开仓轮次 `30`，则每轮上限为 `1 ETH`。如果某一组目标量为 `7.3 ETH`，同样按 `1 ETH` 上限分成多轮，最后一轮按剩余量执行。轮次数量不是完成判定的硬上限；计划轮次结束后仍有可交易残余时，进入补充轮。

补充轮沿用模拟盘补充订单机制：使用同一个每轮上限、同一个轮次间隔和同一套盘口撮合规则，直到目标量完成、剩余量低于交易所最小可交易数量/名义金额、连续无成交或盘口异常触发暂停、达到模拟盘运行安全上限，或用户主动暂停/停止。

补充轮必须有硬上限，避免长链路无限运行。实施计划需要给出配置项，至少包括：

- `max_supplemental_rounds_per_group`：默认 `50`。
- `max_supplemental_rounds_per_run`：默认 `300`。
- `max_consecutive_unfilled_rounds`：默认 `5`。
- `max_run_duration_seconds`：默认 `21600`，即 6 小时。
- `max_events_per_run`：默认 `20000`。

用户输入和运行安全上限第一版固定为：

- `transfer_percent`：默认 `100`，范围 `1` 到 `100`，最多保留 2 位小数。
- `round_count`：默认 `30`，范围 `1` 到 `500`。
- `round_interval_seconds`：默认 `3`，范围 `0` 到 `3600`，允许 0 表示不额外等待。
- `max_supplemental_rounds_per_group`、`max_supplemental_rounds_per_run`、`max_consecutive_unfilled_rounds`、`max_run_duration_seconds` 和 `max_events_per_run` 是服务端常量或配置项，不在当前 UI 暴露。

任一输入或安全上限校验失败时，API 必须返回结构化错误：

```text
{
  "error_code": "kanglong_invalid_transfer_setting",
  "field": "round_count",
  "min": "1",
  "max": "500",
  "message_key": "kanglong.errors.invalid_round_count",
  "available_actions": ["refresh_plan"]
}
```

达到硬上限但仍有可交易残余时，run 不能标记为完成，应进入 `paused_market_unstable` 或 `needs_abort_recover`，并在报告里说明剩余数量、最后一次成交时间和触发的上限。

达到 `max_events_per_run` 时不能静默截断日志，也不能继续执行但丢弃低价值事件。后端必须先写入一条最终 warning 事件和报告摘要，说明事件上限、已迁移数量、剩余数量和最后安全 checkpoint，然后进入 `paused_market_unstable`，`available_actions` 至少包含 `view_report`、`stop` 和 `recover`。若最终 warning 因事件上限无法写入，必须进入 `needs_abort_recover` 并保留可诊断错误。

## 执行机制

亢龙不再使用静态 `simulate_group()` 直接完成分组。实施时需要把模拟盘 `SimulationService` 中的盘口撮合和配对执行能力抽成共享模块，亢龙通过移仓适配器调用同一套机制。`paired_opener/kanglong/simulator.py` 可以保留为兼容入口，但不能再用固定价格直接把 `matched_qty` 设为 `submitted_qty`；它要么委托新 executor，要么在迁移后只保留纯数据转换函数。

每个分组每轮执行两条腿：

- 源账号平仓腿：在 `from_account` 对所选方向执行单方向平仓。
- 目标账号开仓腿：在 `to_account` 对同一方向执行单方向开仓。

每轮对齐流程采用 dry-match 后原子落账：

1. 读取本轮盘口并按模拟盘规则尝试源账号平仓腿，得到 `close_filled_qty`、`close_avg_price`、`close_fee`、`liquidity_role` 和滑点。
2. 目标账号开仓腿的目标数量等于第一腿实际成交量，而不是原计划数量。
3. 按同一盘口源规则执行目标账号开仓腿，得到 `open_filled_qty`、`open_avg_price`、`open_fee`、`liquidity_role` 和滑点。
4. 本轮有效配对数量为 `min(close_filled_qty, open_filled_qty)`，按交易对 step size 归一化。
5. 两条腿都截断到同一个有效配对数量，重新按截断比例计算 notional、fee、slippage 和 residual。
6. 只有两条腿都能解释且有效配对数量大于 0 时，才在同一个 ledger transaction 中写入模拟账本、事件和成本统计。
7. 如果任一腿 dry-match 失败、账本写入失败或有效配对数量无法解释，本轮回滚到执行前 checkpoint，不推进链路。

第一腿 dry-match 结果在第二腿失败时不能先写入账本；第二腿成交少于第一腿时，第一腿也必须按第二腿实际成交量截断后再落账。每轮事件要同时保留 `requested_qty`、`filled_qty`、`matched_qty` 和 `residual_qty`，让报告能解释为什么本轮没有完全成交。

有效配对数量为 0 时，不写账号账本，但必须写入非账本进度事件和 warning 事件，记录计划数量、两腿实际成交量、剩余量、未成交原因和连续未成交计数。前端必须能看到该轮“未成交，将按间隔继续/进入补充轮/暂停”的日志，不能表现为卡住。

模拟阶段必须保证每轮 commit 后账本处于可解释状态。未来真实移仓无法回滚交易所真实成交，但仍复用相同事件模型；真实适配层要把未对齐情况转成补偿或人工恢复流程，而不是改变 planner 和报告语义。

测试模板和真实账号模拟使用同一套执行规则。测试模板使用模板生成的合成账本，真实账号模式使用执行开始时从真实账号快照派生出的模拟账本；两者都使用真实市场 orderbook，不真实下单。

## 市场数据与账号状态

每次运行只使用一个市场数据来源。测试模板模式使用模板的 `market_data_account_id` 读取 symbol rules、quote 和 orderbook；真实账号模拟模式使用主账号网关读取同一套市场数据。两条腿必须在同一个 run 的市场数据上下文里执行，不能一条腿来自测试行情源、另一条腿来自主账号行情源。

同轮 orderbook 策略固定为沿用当前模拟盘配对逻辑：第一腿从同一个 `MarketDataProvider` 读取 fresh orderbook 并 dry-match；第二腿以第一腿实际成交量为目标，再从同一个 provider 读取 fresh orderbook 并 dry-match。两腿不共享账号状态副作用，但允许市场价格在两次读取之间变化，以便模拟真实执行中的价格扰动。事件必须分别记录两腿的 `orderbook_snapshot_id`、`orderbook_captured_at`、`liquidity_role` 和 `wait_seconds_consumed`，方便复盘价格波动和限价等待造成的差异。

测试和回归不能依赖实时市场波动。共享市场模块必须提供 `DeterministicMarketDataProvider` 或等价测试实现，按预置序列返回 symbol rules、orderbook、captured_at 和 freshness 结果。后端单元测试和合约测试默认使用确定性行情回放；真实网关 orderbook 只用于手动验收冒烟或显式标记的集成测试。

确定性行情 fixture 第一版最小结构固定为：

```text
{
  "symbol": "ETHUSDC",
  "symbol_rules": {
    "step_size": "0.001",
    "tick_size": "0.01",
    "min_qty": "0.001",
    "min_notional": "5",
    "quote_asset": "USDC"
  },
  "orderbooks": [
    {
      "snapshot_id": "ob-1",
      "captured_at": "2026-06-03T00:00:00Z",
      "fresh": true,
      "bids": [["1863.44", "10"]],
      "asks": [["1863.51", "10"]]
    }
  ],
  "freshness": [{"snapshot_id": "ob-1", "fresh": true}],
  "fee_roles": [{"snapshot_id": "ob-1", "liquidity_role": "taker", "fee_rate": "0.0004", "fee_asset": "USDC"}],
  "conversion_prices": [{"asset": "BNB", "quote_asset": "USDC", "price": "600", "captured_at": "2026-06-03T00:00:00Z"}]
}
```

测试 fixture 中的 `orderbooks` 必须按读取顺序消费；消费完后默认返回 stale 或显式配置的 terminal snapshot，避免测试因为隐式重复最后一档盘口而误通过。

确定性行情回放至少覆盖：

- 两条腿都完全成交。
- 第一腿成交、第二腿部分成交，最终按第二腿截断。
- 第一腿成交、第二腿 0 成交，账本无单腿副作用。
- 两条腿都 0 成交，写 warning/progress 事件并推进未成交计数。
- orderbook 过期，进入暂停或异常恢复。
- 两次 fresh orderbook 之间价格跳动，正确统计价差 PnL 和价差磨损。
- maker/taker 角色不同，手续费和流动性角色分别落账。

执行开始前做轻量预检：

- `plan_version` 和已确认版本一致。
- 交易对规则可用，且与检测阶段 symbol 一致。
- orderbook 新鲜度满足模拟盘规则。
- 运行锁可获取或可续期。
- 已确认链路没有因为参数变化失效。

执行过程中不刷新真实账号仓位，不用新快照重算链路。若用户需要以最新账号状态执行，必须重新检测链路并确认。

运行锁策略固定如下，实施计划不能留到编码阶段再临时决定：

- `execution_starting` 和 `running` 必须持有带 fencing token 的独占运行锁，并按 heartbeat 续期；所有 ledger/event/checkpoint 写入都必须校验当前 token。
- `paused_by_user`、`paused_market_unstable`、`stopped_by_user`、`completed`、`completed_with_dust_residual` 和 `aborted_recovered` 只能在安全 checkpoint 后释放运行锁。
- `needs_abort_recover` 不允许运行 worker 继续写入。后端必须让旧锁失效或标记为 fenced，恢复动作重新获取新锁后先校验 checkpoint、ledger 和事件一致性，再决定进入 `aborted_recovered` 或生成人工恢复结果。
- live 状态下 heartbeat 过期时，恢复扫描必须先用 checkpoint 安全性判定：安全 checkpoint 可进入可恢复暂停态或重新调度；非安全 checkpoint 必须进入 `needs_abort_recover`。
- 页面刷新只能恢复展示和轮询，不能凭前端状态重新创建 worker；是否继续执行由后端锁、checkpoint 和 `available_actions` 决定。

长链路不能依赖进程内 `sleep` 作为唯一调度状态。每次进入轮次间隔、补充轮等待或暂停前，都必须把以下调度字段写入 `progress_json` 或 checkpoint：

- `next_wake_at`：下一次允许继续执行的时间；立即继续时为空。
- `scheduled_reason`：`round_interval`、`supplemental_interval`、`market_retry`、`user_pause` 或 `none`。
- `worker_epoch`：每次 worker 接管 run 时递增。
- `lease_token`：本次 worker 持有的锁 token，写 ledger/event/checkpoint 时必须匹配。
- `lock_expires_at`：当前租约过期时间。

`kanglong_locks` 需要支持 `fencing_token`、`worker_epoch` 和 `lease_token`。服务重启扫描只处理 `next_wake_at <= now` 或 heartbeat 过期的 run；扫描器重新获取锁并递增 `worker_epoch` 后，旧 worker 即使醒来也会因为 token 不匹配而无法写入。页面刷新只读取这些字段展示进度，不触发新的执行线程。

后台 worker 重试幂等必须独立于 API `idempotency_key`。每一次轮次尝试都必须生成稳定 `operation_id`，它表示一次轮次尝试的幂等 envelope，而不是单条 ledger entry：

```text
operation_id = run_id + group_id + round_id + round_attempt
```

`kanglong_ledger_entries` 每条记录都携带 `operation_id`、`leg_id`、`entry_type`、`account_id`、`asset` 和 `sequence`，并对 `run_id + operation_id + sequence` 建唯一约束。worker 崩溃后重跑同一个安全 checkpoint 之前的轮次时，存储层必须识别已提交的 `operation_id` 并返回同一轮次结果，而不是重复写账本。若同一 `operation_id` 对应的 operation payload hash 不一致，run 必须进入 `needs_abort_recover`，不能覆盖旧记录。

## 状态机与操作按钮

页面按钮由后端 `available_actions` 驱动，前端只负责按响应立即刷新状态。检测成功后响应包含 `confirm`，页面应立即点亮 `确认链路`；确认成功后响应包含 `execute`，页面应立即点亮 `开始模拟移仓`。

核心状态：

- `execution_starting`：执行启动中，已保存可恢复的 `execution_context`。
- `running`：正在按组/轮执行。
- `paused_by_user`：用户在安全 checkpoint 暂停，可继续。
- `paused_market_unstable`：盘口过期、连续无成交或市场异常暂停，可在条件恢复后继续。
- `paused_plan_stale`：确认链路或执行上下文已过期，必须重新检测或进入恢复流程。
- `stopped_by_user`：用户在安全 checkpoint 停止，不自动恢复。
- `completed`：目标百分比数量完全移仓，且无不可解释残余。
- `completed_with_dust_residual`：剩余量低于交易所最小数量或最小名义金额，无法继续交易，报告明确展示 dust residual。
- `needs_abort_recover`：账本、锁、事件或腿对齐状态不安全，需要人工恢复。
- `aborted_recovered`：人工恢复完成，本次 run 不能继续，只能重新检测。

现有代码中的 `KanglongRunStatus` 和 storage 状态需要显式迁移或映射。实施计划必须先确定状态兼容方案：

```text
chain_ready / plan_confirmed -> 沿用现有状态
execution_starting -> 沿用现有状态
running -> 新增，或映射到 group_ready / round_simulated 但 API 对外展示 running
paused_by_user -> 新增
paused_market_unstable -> 新增，不能复用 blocked_plan_stale
paused_plan_stale -> 可映射 blocked_plan_stale，但 API 文案要区分暂停和阻断
stopped_by_user -> 新增
completed -> 沿用 completed
completed_with_dust_residual -> 新增，不能继续使用 unsafe_dust_residual 作为成功态
needs_abort_recover -> 沿用现有状态
aborted_recovered -> 沿用现有状态
```

如果实施时为了兼容旧数据暂时保留旧枚举名，API 响应必须提供稳定的 `display_status` 或 `run_phase`，前端按钮只读 `available_actions`，不要根据旧枚举自行推断。

旧 run 兼容策略固定为只读历史兼容：没有 `checkpoint_id`、`engine_version` 或新 ledger/checkpoint 表记录的旧 run，可以继续查看原有报告和事件，但不能执行 `resume`、`stop`、`recover` 或补写为新执行引擎 run。旧 run 的 `available_actions` 必须为空或只包含“重新检测”类导航动作；如果用户要继续迁移，必须基于当前输入重新检测链路并创建新 run。

`available_actions` 矩阵固定由后端生成，前端不得自行从状态枚举推断：

```text
draft_plan / blocked_* -> refresh_plan
chain_ready -> confirm, refresh_plan
plan_confirmed -> execute, refresh_plan
execution_starting -> view_report
running -> pause, stop, view_report
pause_pending -> stop, view_report
stop_pending -> view_report
paused_by_user -> resume, stop, view_report
paused_market_unstable -> resume, stop, recover, view_report
paused_plan_stale -> refresh_plan, recover, view_report
stopped_by_user -> view_report, refresh_plan
completed -> view_report
completed_with_dust_residual -> view_report
needs_abort_recover -> recover, view_report
aborted_recovered -> refresh_plan, view_report
legacy_readonly -> refresh_plan, view_report
```

若某个状态因为锁冲突、事件上限、旧数据兼容或计划过期需要减少动作，只能在后端收窄 `available_actions`，不能让前端加动作。

按钮：

- `检测账号状态`
- `确认链路`
- `开始模拟移仓`
- `暂停`
- `继续`
- `停止`
- `异常恢复`

`暂停`、`继续`、`停止` 和 `异常恢复` 都必须是幂等动作。请求需要携带 `idempotency_key` 和当前 `checkpoint_id` 或 `action_version`；后端只接受匹配当前 checkpoint 的动作。旧 checkpoint 发来的继续、停止或恢复请求必须返回结构化冲突结果，不能改变 run 状态。

暂停和停止只在安全 checkpoint 生效。安全 checkpoint 至少包括：当前轮两条腿已对齐并写入账本、事件已落库、成本统计已更新、运行锁 heartbeat 正常。服务重启或页面刷新后，如果 run 处于安全 checkpoint 且状态允许自动恢复，后端可以从 `execution_context` 继续；如果恢复检查发现上一轮处于未对齐或未落库状态，必须进入 `needs_abort_recover`。

如果用户在一轮两条腿执行中点击暂停或停止，动作不能打断当前轮的原子落账，但必须立即进入待生效状态并让前端可见。控制动作需要写入 `progress_json.control_request`：

- `requested_action`：`pause` 或 `stop`。
- `requested_at`：用户请求时间。
- `requested_checkpoint_id`：用户发起动作时看到的 checkpoint。
- `requested_action_version`：用户发起动作时看到的 action version。
- `requested_by`：第一版可固定为 `local_user`。

worker 每轮提交到安全 checkpoint 后必须先读取并消费 `control_request`，再决定进入下一轮、等待间隔、`paused_by_user` 或 `stopped_by_user`。待生效期间 API 响应和前端状态要展示 `pause_pending` 或 `stop_pending`，避免用户误以为按钮无效。旧 checkpoint 的控制请求仍然返回冲突，不写入 `control_request`。

`control_request` 冲突优先级固定如下：

- `stop` 优先级高于 `pause`。已有 `pause_pending` 时收到合法 `stop`，升级为 `stop_pending`。
- 已有 `stop_pending` 时不能被 `pause`、`resume` 或新的 `pause` 降级；重复 `stop` 返回当前 pending 状态。
- 已有 `pause_pending` 时重复 `pause` 返回当前 pending 状态；`resume` 只能在进入 `paused_by_user` 后生效，不能取消执行中的 `pause_pending`。
- 所有 `control_request` 写入必须用 `action_version` compare-and-set；版本不匹配返回结构化冲突，不改变 pending 状态。

`停止` 是终止动作，不是暂停动作。`stopped_by_user` 不能自动继续，也不能通过 `resume` 重新进入同一 run；报告必须展示已迁移数量、剩余数量、停止前累计手续费、价差磨损、滑点、最后安全 checkpoint、停止时间和触发人。若用户想处理剩余数量，前端只能引导重新检测并创建新 run，不能把停止 run 当作可继续 run。

## Checkpoint 与断点续传

`execution_context` 是恢复的唯一事实来源，必须持久化在 run 报告或专门字段中。实现上使用 `progress_json`、专门 checkpoint 字段和新表保存 compact checkpoint；`report` 只保存展示摘要，避免大账本反复写入报告导致历史报告膨胀和恢复慢。

第一版数据模型固定为：

- `kanglong_runs.engine_version`：新增列。新执行引擎创建的 run 固定为 `2`；没有该字段或值低于 `2` 的历史 run 按旧 run 只读兼容策略处理。
- `kanglong_runs.progress_json`：保存当前 `checkpoint_id`、group/round 进度、`available_actions`、`run_phase`、`next_wake_at`、`scheduled_reason`、`worker_epoch`、`lease_token`、`lock_expires_at` 和小型恢复摘要。
- `kanglong_ledger_baselines`：新增表，保存 run 开始时每个账号的初始模拟账本基线。第一版固定使用该表作为唯一 baseline 路径，不使用 `checkpoint_id = 0` 作为替代实现。字段至少包括 `run_id`、`account_id`、`asset`、`wallet_balance`、`available_balance`、`equity`、`margin`、`margin_deficit`、`total_unrealized_pnl`、`position_side`、`position_qty`、`entry_price`、`mark_price`、`leverage`、`baseline_sequence`、`baseline_hash` 和 `created_at`。
- `kanglong_run_checkpoints`：新增表，按 `run_id + checkpoint_id` 保存 compact `execution_context`、`previous_ledger_hash`、`ledger_hash`、`ledger_state_hash`、`events_high_watermark`、`created_at` 和 `is_safe`。
- `kanglong_ledger_entries`：新增 append-only 表。主键使用 `entry_id`，并建立 `run_id + checkpoint_id + sequence` 和 `run_id + operation_id + sequence` 唯一约束；字段至少包括 `run_id`、`checkpoint_id`、`operation_id`、`operation_payload_hash`、`sequence`、`group_id`、`round_id`、`round_attempt`、`leg_id`、`account_id`、`entry_type`、`asset`、`delta`、`balance_after`、`available_balance_after`、`equity_after`、`margin_after`、`margin_delta`、`position_side`、`position_delta`、`fee_asset`、`fee_delta`、`price_diff_pnl`、`payload_json` 和 `created_at`。
- `kanglong_events.checkpoint_id`：新增真实列。历史事件允许为空或标记为 `legacy`，但 `engine_version >= 2` 的执行相关事件必须通过写入层强制携带非空真实 checkpoint；不能把 `checkpoint_id` 只写入 `payload_json` 作为第一版折中。新引擎事件缺少 checkpoint 时，写入必须失败并让 run 进入可诊断错误路径。
- `kanglong_runs.report_summary_json`：只存总览摘要，不存完整账本。摘要必须携带 `report_version`、`generated_from_checkpoint_id`、`source_ledger_hash`、`source_ledger_state_hash`、`generated_at` 和 `summary_status`。恢复或重新聚合报告时，如果 `source_ledger_hash` 或 `source_ledger_state_hash` 与当前 checkpoint 不一致，必须重新生成摘要，不能展示过期摘要。

`kanglong_ledger_entries.entry_type` 第一版固定为：

- `position_delta`
- `wallet_delta`
- `fee_delta`
- `price_diff_pnl`
- `slippage_cost`
- `residual_marker`

`ledger_hash` 必须是链式 hash，而不是只对当前 checkpoint 的局部条目取 hash。第一个执行 checkpoint 的 `previous_ledger_hash` 来自 `kanglong_ledger_baselines.baseline_hash`；之后每个 checkpoint 使用上一安全 checkpoint 的 `ledger_hash` 作为 `previous_ledger_hash`。计算规则固定为：

```text
ledger_hash = hash(previous_ledger_hash + canonical_ledger_entries_for_checkpoint)
```

`ledger_state_hash` 是从 baseline 加所有 ledger entries 重建出的完整账号状态 hash，用于恢复校验。规范 JSON 必须使用固定字段顺序、字符串化 Decimal 和 UTF-8；ledger entries 必须按 `run_id + checkpoint_id + sequence` 排序。恢复时不能按数据库默认返回顺序计算 hash。

`execution_context` 至少包含：

- `run_id`
- `engine_version`
- `plan_version`
- `snapshot_bundle_id`
- `market_data_source`
- `symbol_rules`
- `fee_policy_snapshot`：本次 run 使用的手续费核算来源、版本和费率快照，只用于审计和模拟扣费，不作为用户可调参数。
- `account_labels_snapshot`：执行开始时冻结的账号展示名映射，用于所有后续事件和报告。
- `request_params`：百分比、轮次、间隔、方向、账号集合。
- `plan_groups`
- `current_group_index`
- `current_round_index`
- `current_round_kind`：planned 或 supplemental。
- `baseline_hash`
- `ledger_head_checkpoint_id`
- `previous_ledger_hash`
- `ledger_hash`
- `ledger_state_hash`
- `ledger_entry_count`
- `residual_summary`
- `cost_summary`
- `latest_event_id`
- `last_safe_checkpoint_at`
- `available_actions`

`execution_context` 不能重复保存完整 `synthetic_ledger` 或完整账号账本快照。恢复时以 `kanglong_ledger_entries` 为账本事实来源，通过 `ledger_head_checkpoint_id`、`ledger_hash` 和 `ledger_entry_count` 校验一致性；`residual_summary` 只保存剩余数量、dust 判定和下一轮调度所需的小型摘要。若实现需要快速展示账号余额，只能在 `report_summary_json` 中保存派生摘要，不能把它作为恢复依据。

checkpoint 写入顺序固定为：先写 ledger transaction，再写事件，再写 progress/checkpoint，再更新 run 状态和 `available_actions`。这些写入必须在同一个 SQLite transaction 内完成，并由一个存储层方法统一提交，例如 `commit_kanglong_checkpoint()`。该方法负责校验 `lease_token`、写入 ledger entries、写入 events、写入 checkpoint、更新 progress/status/actions，并在任一步失败时整体 rollback。恢复时必须验证 baseline、ledger entries、events、checkpoint 和 run progress 的 `checkpoint_id`、`ledger_hash` 与 `events_high_watermark` 一致；不一致时进入 `needs_abort_recover`，不能猜测补齐。

所有进入 ledger、checkpoint hash、成本报告和事件 payload 的 Decimal 都必须在写入前完成量化，避免同一数值在不同路径出现不同字符串表达。第一版量化规则固定为：

- 数量：按交易对 `step_size` 归一化。
- 价格：按交易对 `tick_size` 归一化。
- notional：按 quote asset 精度归一化；若交易规则没有精度，沿用项目现有 `_money` 精度。
- fee：按 `fee_asset` 精度归一化；缺少资产精度时沿用项目现有 `_money` 精度。
- price_diff_pnl、price_diff_loss、slippage_cost：按报告 quote asset 精度归一化。
- hash 输入中的 Decimal 使用固定字符串格式，不使用科学计数法，不保留无意义前导 `+`，零值统一为 `0`。

账号展示名分两层冻结。检测链路成功时冻结 `plan_labels_snapshot`，只用于链路配置展示和确认页；执行开始时从确认后的计划复制生成 `account_labels_snapshot`，事件和报告只能使用执行快照渲染账号名称。模板改名、账号池刷新或真实账号备注变化不能改变已生成 run 的历史日志。

事件读取支持分页和增量读取。长链路默认只渲染最新日志，保留全部、警告、错误、当前组、成本事件等过滤器，并通过 `after_event_id` 翻页或补齐，避免一次渲染数千条日志。

## 成本与磨损统计

成本统计分为预估和实际：

- 预估值来自检测链路时的账户快照、交易对规则和 orderbook 快照。
- 实际值来自每一轮真实 orderbook 模拟撮合结果。

统计维度：

- 总览：总手续费、总价差磨损、总滑点、总成交数量、剩余数量、完成状态。
- 分组：每个 `group_id` 的目标数量、已迁移数量、补充轮数量、手续费、价差磨损、dust residual。
- 分轮：每个 `round_id` 的计划数量、配对成交数量、平仓均价、开仓均价、手续费、滑点、价差 PnL 和价差磨损。

报告汇总币种第一版固定为交易对 quote asset；例如 `ETHUSDC` 使用 `USDC`。每条 ledger entry 必须保留原始 `asset`、`fee_asset`、原始扣费数量和报告换算结果。若手续费资产与 quote asset 不同，必须记录 `conversion_price`、`conversion_price_source` 和 `conversion_at`；测试模板可使用确定性行情回放提供的换算价。无法换算时，报告总览显示原始资产分项并标记 `conversion_status = unavailable`，不能把不同资产直接相加。

价差 PnL 的方向规则：

```text
LONG:  close_open_price_diff_pnl = (close_avg_price - open_avg_price) * matched_qty
SHORT: close_open_price_diff_pnl = (open_avg_price - close_avg_price) * matched_qty
```

`price_diff_loss` 取不利方向的非负损耗值；`price_diff_pnl` 保留正负号，方便复盘价格波动到底是收益还是磨损。手续费按统一账户的成交扣费语义处理：每条腿成交后，手续费作为该账号的账本变动直接扣除，并写入同一个 round checkpoint。报告里的手续费来自每笔模拟成交的 fee 结果，不使用亢龙配置里的固定 fee rate 作为执行结果。

执行阶段不暴露可变的“手续费设置”。共享 `FeeProvider` 只负责给模拟成交提供和统一账户一致的手续费核算口径，并在 run 开始时保存 `fee_policy_snapshot` 供审计和恢复校验。亢龙配置中的 `fee_rate` 只允许用于旧报告兼容或检测阶段临时预估 fallback；一旦 orderbook matcher 返回 maker/taker 结果，实际账本和报告必须使用撮合结果的 `fee_rate`、`fee_asset`、`fee_amount` 和 `liquidity_role`。

执行日志每轮至少展示：

```text
第 12/30 轮完成：ETHUSDC 配对成交 1.000，平仓均价 1863.44，开仓均价 1863.51，手续费 0.932000000，价差磨损 0.070，剩余 4.000。
```

未成交轮展示为警告，说明会按轮次间隔继续进入下一轮或补充轮。

## API 与数据模型调整

检测链路请求需要增加：

```text
plan_input_hash
transfer_percent
round_count
round_interval_seconds
execution_mode = simulation_transfer
```

检测链路响应需要增加：

```text
transfer_settings
plan_input_hash
plan_version
computed_target_qty
computed_per_round_cap_qty
estimated_costs
chain_config
available_actions
```

确认链路请求需要携带：

```text
plan_input_hash
plan_version
```

确认链路响应需要返回：

```text
confirmed_plan_hash
plan_version
available_actions
```

执行 run 需要增加或强化：

```text
engine_version
execution_context
checkpoint_id
action_version
plan_input_hash
confirmed_plan_hash
plan_version
baseline_hash
previous_ledger_hash
ledger_hash
ledger_state_hash
ledger_entry_count
operation_id
operation_payload_hash
lease_token
worker_epoch
next_wake_at
scheduled_reason
lock_expires_at
control_request
progress.current_group_index
progress.current_round_index
progress.current_round_kind
progress.completed_groups
progress.completed_rounds
progress.supplemental_rounds
progress.migrated_qty
progress.remaining_qty
cost_summary
report_summary.report_version
report_summary.generated_from_checkpoint_id
report_summary.source_ledger_hash
report_summary.source_ledger_state_hash
report_summary.summary_status
available_actions
display_status
run_phase
```

控制类接口需要增加或保持：

```text
POST /kanglong/simulation/run/{run_id}/pause
POST /kanglong/simulation/run/{run_id}/resume
POST /kanglong/simulation/run/{run_id}/stop
POST /kanglong/simulation/run/{run_id}/recover
```

这些接口都必须接收 `idempotency_key`、当前 `checkpoint_id`、`action_version` 和客户端看到的 `run_phase`。响应中必须返回最新 `available_actions`、`checkpoint_id`、`action_version`、`run_phase`、`display_status`、`control_request`、`lease_token`、`worker_epoch`、`next_wake_at` 和 `lock_expires_at`。如果动作进入待生效状态，响应必须明确返回 `run_phase = pause_pending` 或 `run_phase = stop_pending`。

错误码第一版固定包含：

```text
kanglong_invalid_transfer_setting
kanglong_stale_plan_input_hash
kanglong_stale_confirmed_plan_hash
kanglong_stale_checkpoint
kanglong_stale_action_version
kanglong_lease_conflict
kanglong_operation_payload_mismatch
kanglong_ledger_hash_mismatch
kanglong_ledger_state_hash_mismatch
kanglong_market_data_stale
kanglong_event_limit_reached
kanglong_conversion_unavailable
kanglong_legacy_run_readonly
```

所有结构化错误都必须返回 `error_code`、`message_key`、`run_phase`、`available_actions`、`checkpoint_id` 和 `action_version`。与字段范围相关的错误还要返回 `field`、`min`、`max` 或 `allowed_values`。

事件 payload 需要携带账号展示字段：

```text
account_id
account_label
from_account_id
from_account_label
to_account_id
to_account_label
```

日志渲染优先使用 `account_label`、`from_account_label` 和 `to_account_label`。内部账号 ID 只作为展开详情或排障字段显示。

## 测试策略

后端测试：

- 百分比默认 `100%`，允许 `1%` 到 `100%`，超出范围阻断。
- `transfer_percent`、`round_count` 和 `round_interval_seconds` 使用固定默认值、最大值和结构化错误响应。
- `target_qty` 在 planner 前按百分比和交易规则归一化，不能后置缩放已生成的 group。
- `per_round_cap_qty` 使用归一化后的 `target_qty / round_count`，低于最小交易规则时检测阻断。
- 检测链路按百分比缩放目标数量，并在链路配置展示实际执行数量。
- 服务端持久化 `plan_input_hash`，确认链路校验当前 hash 并生成 `confirmed_plan_hash`；旧 hash 的确认或执行请求返回冲突。
- 开仓轮次只定义每轮上限，计划轮次后仍有可交易残余时进入补充轮。
- 补充轮达到 `max_supplemental_rounds_per_group`、`max_supplemental_rounds_per_run`、`max_consecutive_unfilled_rounds` 或 `max_run_duration_seconds` 时不会标记完成。
- 达到 `max_events_per_run` 时写最终 warning/report 后进入 `paused_market_unstable`；不能静默截断事件。
- 后端单元测试使用确定性行情回放，不依赖真实 orderbook 波动。
- 确定性行情 fixture 按固定结构提供 `symbol_rules`、`orderbooks`、`freshness`、`fee_roles` 和 `conversion_prices`，并按读取顺序消费 orderbook。
- 确定性行情覆盖全成交、第二腿部分成交、零成交、orderbook 过期、两次 fresh orderbook 价格跳动和 maker/taker 费率不同。
- 共享撮合模块抽出后，先回接模拟盘并保持现有模拟盘执行结果、手续费、滑点和补充轮行为不变。
- 同轮 orderbook 策略固定为第一腿 fresh match、第二腿按第一腿成交量再 fresh match，且两腿都记录独立 orderbook snapshot。
- 亢龙移仓调用共享模拟撮合能力，不再用固定价格直接完成。
- 源账号平仓腿和目标账号开仓腿按 `min(close_filled_qty, open_filled_qty)` 严格对齐。
- 第一腿 dry-match 成功但第二腿失败时，模拟账本没有任何单腿副作用。
- 有效配对数量为 0 时不会写账本，但会写 warning/progress 事件并推进连续未成交计数。
- 任一腿失败时回滚本轮 checkpoint，不推进 group/round。
- `kanglong_run_checkpoints`、`kanglong_ledger_entries` 和执行事件的 `checkpoint_id` 能互相校验。
- `kanglong_events.checkpoint_id` 是真实列；旧事件允许 legacy 空值，新引擎执行事件缺少 checkpoint 时写入失败，不能只依赖 `payload_json`。
- `kanglong_ledger_entries` 支持同一 checkpoint/account 下的多条 append-only 账本项，并按固定 sequence 计算 `ledger_hash`。
- `kanglong_ledger_baselines` 能重建 run 初始模拟账本；缺少基线时新引擎 run 不能恢复，第一版不支持 `checkpoint_id=0` baseline 替代路径。
- baseline 和 ledger entries 会记录 `available_balance`、`equity`、`margin`、`margin_deficit`、`total_unrealized_pnl`、`margin_delta`、`available_balance_after` 和 `equity_after`，恢复后保证金与可用余额一致。
- `ledger_hash` 使用 `previous_ledger_hash` 链式计算，篡改早期 checkpoint 会导致后续恢复校验失败。
- `ledger_state_hash` 能从 baseline 加所有 ledger entries 重建并校验完整账号状态。
- `execution_context` 不保存完整 `synthetic_ledger`；恢复流程从 `kanglong_ledger_entries` 重建账本，并校验 `ledger_hash` 和 `ledger_entry_count`。
- `commit_kanglong_checkpoint()` 在同一个 SQLite transaction 内提交 ledger、events、checkpoint、progress 和 run 状态；任一步失败都会整体 rollback。
- checkpoint_id 不一致时进入 `needs_abort_recover`。
- 盘口过期、连续无成交、用户暂停、用户停止和服务重启分别进入正确状态。
- 运行锁 fencing token 过期或不匹配时，旧 worker 无法继续写 ledger、event 或 checkpoint。
- `next_wake_at`、`worker_epoch`、`lease_token` 和 `lock_expires_at` 持久化后，服务重启不会重复执行同一轮，也不会丢失等待中的 run。
- 同一轮次 envelope `operation_id` 的 worker 重试不会重复写账本；同一 `operation_id` 的 operation payload hash 不一致时进入 `needs_abort_recover`。
- `paused_by_user`、`paused_market_unstable` 和 `stopped_by_user` 只在安全 checkpoint 后释放锁；`needs_abort_recover` 会阻止 worker 继续写入。
- 轮次执行中收到 pause/stop 时进入 `pause_pending` 或 `stop_pending`，当前轮安全提交后立即停到对应状态。
- `control_request` 遵守 stop 高于 pause 的优先级；pending stop 不可降级，重复同动作返回当前 pending，所有 pending 写入都使用 `action_version` compare-and-set。
- pause/resume/stop/recover 带旧 `checkpoint_id` 或旧 `action_version` 时返回冲突，不改变 run。
- `execution_context` 能恢复 `execution_starting` 和安全 checkpoint 后的 `running`。
- 非安全状态恢复进入 `needs_abort_recover`。
- 没有新 checkpoint/ledger 结构的旧 run 只能只读查看，不能继续、停止、恢复或补写成新引擎 run。
- 用户停止后报告展示已迁移数量、剩余数量、累计手续费、价差磨损、滑点、最后安全 checkpoint 和停止时间，且 `resume` 不可用。
- 手续费、价差 PnL、价差磨损、滑点和 dust residual 的总览/分组/分轮聚合正确。
- 手续费资产与 quote asset 不同时，报告保留原始资产、换算价格、换算时间和 `conversion_status`，不能直接跨资产相加。
- `report_summary_json` 携带 `report_version`、`generated_from_checkpoint_id`、`source_ledger_hash` 和 `source_ledger_state_hash`；来源 hash 不一致时重新生成摘要。
- 所有核心错误场景返回固定 `error_code`、`message_key`、`run_phase`、`available_actions`、`checkpoint_id` 和 `action_version`。
- Decimal 写入 ledger、hash、事件和报告前完成统一量化；等价数值在不同路径生成相同规范字符串。
- 执行阶段手续费来自共享 fee provider 和撮合结果，不再使用亢龙固定 `fee_rate`。
- 测试模板账本和真实账号快照账本在相同 orderbook 下执行规则一致。
- 检测链路使用 `plan_labels_snapshot` 展示链路；run 开始后使用 `account_labels_snapshot`，模板改名、账号池刷新或真实账号备注变化不影响已生成事件里的账号展示名。
- 抽出共享撮合模块后，现有模拟盘双向开仓、双向平仓、单向开仓、单向平仓测试保持通过。

前端测试：

- 移仓设置区域出现在账号池和检测链路之间。
- 移仓设置区和控制按钮使用约定的 `data-testid`，测试不依赖易变 CSS 选择器或中文文案层级。
- 锁定字段跟随交易对和移仓方向变化，用户不能编辑。
- 用户修改百分比、轮次、间隔、交易对、方向、主账号、子账号或模板后，确认状态立即失效。
- 旧检测响应晚返回时，不能覆盖新输入状态，也不能重新点亮 `确认链路`。
- 检测成功后 `确认链路` 不需要刷新即可可用。
- 确认成功后 `开始模拟移仓` 不需要刷新即可可用。
- pause/resume/stop/recover 成功后按钮只按最新 `available_actions` 刷新，旧响应不能覆盖新状态。
- 每个 `run_phase` 使用后端 `available_actions` 矩阵渲染按钮，前端不根据状态枚举自行加动作。
- `pause_pending` 和 `stop_pending` 有明确展示状态；pending 期间重复动作或降级动作显示后端返回的结构化冲突/当前 pending。
- 开始执行后日志按轮追加，不再一次性显示所有组已完成。
- 零成交轮显示 warning 日志和剩余数量，页面不表现为无反馈等待。
- 长链路日志默认展示最新项，过滤器和增量读取可用。
- 执行日志不展示 `tpl:tpl_...:sub:...` 作为主账号名或子账号名。
- 成本面板能展示预估值、实际值、分组和分轮明细。

验收冒烟：

- 用测试模板构造 6 个以上账号、30 轮、3 秒间隔，点击开始后能看到轮次按时间推进。
- 构造盘口无成交场景，日志出现未成交警告并进入下一轮或补充轮，而不是直接完成。
- 刷新页面后能恢复当前 run、当前组、当前轮、日志和可用按钮。
- 服务重启后安全 checkpoint 能继续，非安全状态进入异常恢复。

## 自查结论

- 本设计聚焦亢龙移仓模拟重构，不进入真实下单实现。
- 用户确认的百分比、轮次、补充订单、模拟盘复用、断点恢复和成本统计均已覆盖。
- 旧的静态 `simulate_group()` 问题被明确替换为共享模拟撮合机制。
- 状态、按钮和日志规则能解释用户遇到的刷新后才可点击、瞬间完成和账号 ID 乱码问题。
- 当前范围足够形成单独实施计划，不需要拆成多个 spec。
