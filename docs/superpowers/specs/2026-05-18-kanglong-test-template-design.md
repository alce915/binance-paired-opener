# 亢龙测试账号模板设计

## 背景

亢龙有悔移仓模拟需要反复验证不同账号资金、仓位和价格场景。仅依赖真实 Binance 账号快照会让测试受真实资产状态限制；仅在前端录入假账号又无法进入现有检测链路、订单簿定价和执行日志机制。

因此第一版增加持久化的“测试场景模板”。模板用于构造一组可被后端识别的测试账号快照，并在加载、检测和模拟执行时读取当前真实行情与订单簿，尽量贴近正式流程，但永远不真实下单。

## 已确认决策

- 模板粒度为完整测试场景，不做单账号模板库。
- 模板持久化到本地 JSON 文件。
- 加载模板后进入测试模板模式，账号池完全替换为模板账号。
- 测试模板账号不允许和真实账号混用。
- 模板编辑入口使用弹窗，不挤占亢龙主页面空间。
- 模板保存输入参数；标记价格、未实现盈亏、名义价值、保证金占用和可用余额在加载或检测时按真实行情重算。
- 后续移仓模拟继续复用现有开平仓、订单簿、链路规划、事件日志和消耗统计机制。
- 用户录入的“保证金”统一落库为 `collateral`，第一版口径为测试账号的场景钱包余额；`equity`、`margin_used`、`available_balance` 和 `margin_deficit` 由系统按当前行情和未实现盈亏重算。
- 每次生成 plan 时必须锁定 `template_content_hash`，后续 confirm、execute 和 recover 以 run state 中的模板来源和 hash 做后端校验，不依赖前端重复传参。
- 测试模板账号只能进入 synthetic/simulation gateway；真实 gateway 只能用于读取交易对规则、quote 和订单簿，不能按模板账号查仓、撤单或下单。
- 测试模板运行时账号 ID 必须使用独立命名空间，避免和真实账号 ID 归一化后碰撞。

## 目标

- 支持创建、编辑、复制、删除和加载亢龙测试场景模板。
- 支持手动设置主账号保证金、子账号保证金、开仓价格、持仓数量、杠杆倍数等参数。
- 支持根据输入参数自动生成双向开仓的配平仓位。
- 加载模板后在亢龙账号池中展示测试账号、仓位、保证金和实时重算后的盈亏。
- 检测链路、确认链路和开始模拟移仓都能使用测试模板快照。
- 保持第一版仍为模拟盘，不触发真实交易所下单。
- 保留真实订单簿读取和价格计算，让测试环境尽量靠近正式移仓流程。

## 非目标

- 第一版不支持测试模板账号与真实账号混合参与同一条链路。
- 第一版不保存真实 API key、真实账号配置或任何敏感凭证。
- 第一版不把模板同步到云端或多机器共享。
- 第一版不自动生成复杂行情路径；行情来自当前真实订单簿和当前参考价格。
- 第一版不做模板版本审计，只保存 `created_at` 和 `updated_at`。
- 第一版不真实下单，也不绕过现有模拟执行层。
- 第一版不允许编辑模板后自动更新已经应用到账户池的测试账号；必须重新应用模板。
- 第一版不支持复杂订单簿路径回放；每次预览、检测和执行复检都读取当前真实行情。

## 持久化

模板保存到：

```text
data/kanglong_test_templates.json
```

文件结构：

```json
{
  "version": 1,
  "templates": [
    {
      "id": "tpl_eth_drop_001",
      "name": "ETH 下跌子账号多头亏损场景",
      "symbol": "ETHUSDC",
      "created_at": "2026-05-18T00:00:00+08:00",
      "updated_at": "2026-05-18T00:00:00+08:00",
      "main_account": {
        "account_id": "test-main",
        "name": "测试主账号",
        "collateral": "10000",
        "leverage": 75,
        "positions": []
      },
      "subaccounts": [
        {
          "account_id": "test-sub-1",
          "name": "测试子账号 1",
          "collateral": "5000",
          "leverage": 75,
          "long_entry_price": "2440",
          "short_entry_price": "2130",
          "qty": "10"
        }
      ]
    }
  ]
}
```

存储规则：

- 所有数量、价格、保证金、权益和可用余额以字符串保存，后端读取后转为 Decimal。
- 用户输入的保证金字段统一命名为 `collateral`。预览和检测时把它解释为场景钱包余额，派生 `wallet_balance = collateral`、`equity = collateral + total_unrealized_pnl`、`margin_used`、`available_balance` 和 `margin_deficit`，避免 UI 字段和后端资金口径不一致。
- `main_account.positions` 第一版默认为空；如果未来允许主账号模板仓位，仍必须经过亢龙主账号初始空仓预检。
- `subaccounts[].qty` 表示自动生成 LONG/SHORT 双向配平仓位的数量。
- `long_entry_price` 和 `short_entry_price` 分别用于计算 LONG/SHORT 未实现盈亏。
- 每次模板保存后生成稳定的 `template_content_hash`。hash 基于规范化 JSON 内容计算，忽略字段顺序，但只包含会影响快照、计划和风险判断的字段。
- 写入 JSON 文件必须使用原子写：先写入同目录 `.tmp` 文件，校验可读后替换正式文件；替换前保留 `.bak`。Windows 下同一时间只允许一个模板写操作，避免并发保存导致文件损坏。
- 模板账号保存时可以使用用户可读的 `account_id`，但应用到运行时快照前必须改写为 `tpl:{template_id}:main` 和 `tpl:{template_id}:sub:{row_id}`。后端必须校验这些运行时 ID 与真实账号 ID 归一化后不冲突。
- `template_content_hash` 只覆盖会影响模板快照和计划的字段：交易对、账号标识、保证金、杠杆、开仓价、数量。账号展示名称不进入 hash；仅修改名称、列表排序、UI 折叠状态等不影响计划的字段不得改变 hash。
- `template_id`、模板账号 `account_id` 和子账号 `row_id` 只能包含 `[a-zA-Z0-9_-]`。展示名称可以使用中文，但不得参与运行时账号 ID。保存子账号时生成稳定 `row_id`，后续改名或排序不能改变该子账号的运行时 ID。

## 后端接口

新增模板管理接口：

```text
GET    /kanglong/simulation/test-templates
POST   /kanglong/simulation/test-templates
PUT    /kanglong/simulation/test-templates/{template_id}
POST   /kanglong/simulation/test-templates/{template_id}/clone
DELETE /kanglong/simulation/test-templates/{template_id}
POST   /kanglong/simulation/test-templates/{template_id}/preview
```

`preview` 返回按当前真实行情重算后的账号快照，用于弹窗预览和应用前校验。

`preview` 响应契约：

```json
{
  "template_id": "tpl_eth_drop_001",
  "template_content_hash": "sha256:...",
  "symbol": "ETHUSDC",
  "account_source": "test_template",
  "snapshot_bundle_id": "snap-...",
  "mark_price_snapshot": {
    "mark_price": "2443.21",
    "mark_price_source": "quote_mid",
    "quote_bid_price": "2443.20",
    "quote_ask_price": "2443.22",
    "captured_at": "2026-05-18T00:00:00+08:00",
    "ttl_ms": 5000
  },
  "execution_orderbook_snapshot": {
    "source": "orderbook_top",
    "best_bid_price": "2443.19",
    "best_ask_price": "2443.23",
    "captured_at": "2026-05-18T00:00:00+08:00",
    "ttl_ms": 5000
  },
  "symbol_rules": {
    "step_size": "0.001",
    "tick_size": "0.01",
    "min_qty": "0.001",
    "min_notional": "5",
    "max_leverage": 125
  },
  "accounts": [
    {
      "account_id": "tpl:tpl_eth_drop_001:main",
      "template_account_id": "test-main",
      "name": "测试主账号",
      "role": "main",
      "collateral": "10000",
      "wallet_balance": "10000",
      "total_unrealized_pnl": "0",
      "equity": "10000",
      "margin": "0",
      "available_balance": "10000",
      "margin_deficit": "0",
      "positions": []
    }
  ],
  "rounding_residuals": [],
  "warnings": [],
  "blocks": []
}
```

规则：

- `accounts` 必须是前端应用模板时替换账号池的唯一来源，前端不得自行重新拼装测试账号。
- `snapshot_bundle_id` 覆盖模板输入、运行时账号 ID、交易对规则版本、价格快照和生成出的账号快照。
- `warnings` 和 `blocks` 只携带结构化 code 和 params；展示文本由语言包或 registry 渲染。
- `mark_price_source` 第一版默认使用 `quote_mid`。只有后端已经实现交易所 mark price 市场数据接口并通过测试后，才允许返回 `exchange_mark_price`；否则不得在响应中伪造交易所标记价来源。
- `mark_price_snapshot` 只服务持仓展示、PnL 和风险预估；`execution_orderbook_snapshot` 只服务执行价格预估和移仓损耗统计。两者不得共用字段或互相兜底。
- `symbol_rules.max_leverage` 必须来自交易对规则或交易所规则；模板输入杠杆超过该值时，`blocks` 返回 `kanglong_test_template_leverage_exceeded`，不能等到 plan 阶段才失败。

亢龙计划接口扩展请求字段：

```json
{
  "mode": "simulation",
  "symbol": "ETHUSDC",
  "main_account_id": "tpl:tpl_eth_drop_001:main",
  "subaccount_ids": ["tpl:tpl_eth_drop_001:sub:test-sub-1"],
  "selected_side": null,
  "account_source": "test_template",
  "test_template_id": "tpl_eth_drop_001",
  "template_content_hash": "sha256:..."
}
```

规则：

- `account_source` 省略或为 `runtime` 时，沿用真实账号快照。
- `account_source = test_template` 时，后端只允许使用该模板内账号，并且必须校验 `template_content_hash` 与当前模板内容一致。
- 如果请求中出现模板之外的账号 ID，返回阻断错误 `kanglong_test_template_account_mismatch`。
- 如果模板不存在、交易对不匹配或模板账号为空，返回明确错误码。
- plan 请求中的 `main_account_id` 和 `subaccount_ids` 必须使用 `preview.accounts[].account_id` 返回的运行时账号 ID；模板原始账号 ID 只用于 `template_runtime_account_map` 审计和 UI 显示。
- plan 创建时把 `account_source`、`test_template_id`、`template_content_hash`、模板输入摘要和生成出的 `snapshot_bundle_id` 一起写入 run state。
- confirm、execute 和 recover 不要求前端重复传 `account_source` 或 `test_template_id`；后端必须从 run state 读取模板来源并执行校验。若当前模板被编辑、删除或 hash 不一致，返回 `blocked_plan_stale`，要求用户重新应用模板并重新检测。

schema 与采集分流：

- `KanglongPlanRequest` 需要新增可选字段 `account_source`、`test_template_id`、`template_content_hash`。
- `account_source = runtime` 时沿用当前真实账号采集路径，可以调用 `runtime_manager.build_temporary_gateway(account_id)`。
- `account_source = test_template` 时必须走测试模板快照提供者，按模板和当前行情生成 `KanglongAccountSnapshot`，不得调用真实账号 gateway 的查仓、撤单或下单方法。
- `test_template` 模式下真实 gateway 仅能用于账号无关的市场数据方法：`get_symbol_rules`、`get_quote`、`get_order_book`，并且调用时不传测试账号 ID。
- `_collect_kanglong_plan_inputs` 或后续同等采集函数必须以 `account_source` 作为第一层分支，避免测试账号 ID 被误送入真实账号运行时。

run state 字段落点：

- 第一版不新增独立列，统一写入 `kanglong_runs.request_json`，避免为模板模式增加一次 SQLite schema 迁移。
- `request_json.account_source` 固定为 `runtime` 或 `test_template`。
- `request_json.test_template_id`、`request_json.template_content_hash`、`request_json.template_input_digest` 只在测试模板模式下存在。
- `request_json.snapshot_bundle_id` 必须和 `kanglong_runs.snapshot_bundle_id` 一致。
- `request_json.template_runtime_account_map` 保存模板账号 ID 到运行时账号 ID 的映射，用于日志、恢复和 UI 展示。
- `report.account_snapshot.accounts` 或同等 run state 字段必须保存最近一次用于 plan 的模板账号快照，至少包含 `preview.accounts` 中的账号、持仓、资金、`template_content_hash` 和 `snapshot_bundle_id`。
- 后续如果需要按模板查询历史运行，再考虑把 `account_source`、`test_template_id` 单独升为列。

execute / recover 复检规则：

- execute 阶段重建复检请求时，必须从 `kanglong_runs.request_json.account_source` 读取账号来源，不得只使用 `main_account_id`、`subaccount_ids` 重新构造默认 runtime 请求。
- `runtime` 模式下执行现有真实账号快照复检。
- `test_template` 模式下读取 run state 中的 `test_template_id`、`template_content_hash` 和 `template_runtime_account_map`，重新生成模板快照并校验 hash；模板缺失、hash 漂移或运行时账号映射不一致时返回 `blocked_plan_stale`。
- recover 阶段同样从 run state 读取模板来源；人工恢复审计需要记录模板 ID、模板 hash、复检前后 `snapshot_bundle_id` 和操作者。

active run 恢复规则：

- `/kanglong/simulation/run/active` 返回 active run 时必须包含 `request.account_source` 和当前 run 使用的账号来源。
- 当 `account_source = test_template` 时，响应必须直接携带 `account_snapshot.accounts`，或携带足够信息让前端自动重新调用 preview 并校验同一个 `template_content_hash`。
- 前端恢复 active run 时，如果发现 `account_source = test_template`，必须先用模板账号快照替换 `availableAccounts`，再恢复主账号、子账号选择和执行日志；不得用真实账号池渲染 `tpl:...` 账号。
- 如果模板已删除或 hash 漂移导致无法恢复账号池，active run 响应进入 `blocked_plan_stale` 或 `needs_abort_recover`，并只允许刷新计划或人工恢复动作。

## 行情与计算

模板保存的是输入参数，不保存最终快照。每次加载、预览、检测和执行前复检都重新读取：

- 交易对规则：step size、tick size、最小名义价值、最大杠杆。
- 订单簿：当前 bid/ask。
- 标记价格：第一版使用 quote mid。只有后端新增交易所 mark price 市场数据接口后，才可以优先使用交易所 mark price。前端必须展示 `mark_price_source`，让用户知道该价格是交易所标记价还是参考价。
- 执行价格：用于模拟平仓/开仓成交，必须使用真实订单簿的 bid/ask 或订单簿深度，不得直接用 mid price 代替成交价。

计算口径：

```text
mark_price = 当前参考价格
long_unrealized_pnl = (mark_price - long_entry_price) * qty
short_unrealized_pnl = (short_entry_price - mark_price) * qty
notional = mark_price * qty
position_margin = notional / leverage
margin = sum(position_margin)
wallet_balance = collateral
total_unrealized_pnl = sum(long_unrealized_pnl + short_unrealized_pnl)
equity = wallet_balance + total_unrealized_pnl
available_balance = max(equity - margin, 0)
margin_deficit = max(margin - equity, 0)
```

`mark_price` 只用于持仓展示、未实现盈亏和风险估算。移仓执行中的 close/open 成交价必须由模拟执行适配器基于订单簿计算，并进入手续费、价差损耗和执行日志。

执行价格第一版实现口径：

- plan / preview 阶段可以读取 quote bid/ask 计算 `mark_price`，但执行价格预估必须读取 orderbook 顶档或深度。
- execute 阶段每个 group 开始前重新调用 `get_order_book`。若执行适配器已支持深度撮合，则按本轮 `submitted_qty` 穿透深度计算平均成交价；若当前实现仍只支持顶档，则必须使用 orderbook 顶档并把 `execution_price_source = orderbook_top` 写入报告和事件日志。
- LONG 平仓使用 bid 侧，LONG 开仓使用 ask 侧；SHORT 平仓使用 ask 侧，SHORT 开仓使用 bid 侧。
- 任何情况下都不能把 `mark_price` 或 quote mid 当作成交价。
- orderbook 为空、过期或深度不足时阻断当前检测或当前组执行，返回结构化原因，不自动降级到 quote 或 mid price。

数量和价格规则：

- 所有计算使用 Decimal。
- 生成仓位数量按交易对 step size 向下取整。
- 生成价格按交易对 tick size 归一化。
- 取整产生的差额在预览报告中展示为 `rounding_residuals`，但不写入真实执行账本。
- 杠杆必须在 preview 阶段按 `symbol_rules.max_leverage` 校验。超过上限时不生成可执行计划，只返回阻断原因。

## 测试模板模式

前端新增 `kanglongState.accountSource`：

```text
runtime       使用真实账号池
test_template 使用当前测试模板账号池
```

进入测试模板模式时：

- 清空当前检测链路、确认状态、执行日志和选中账号。
- 用模板生成的账号快照替换 `availableAccounts`。
- 主账号默认选中模板主账号。
- 子账号池只显示模板子账号。
- 页面展示当前模板名和“测试模板模式”状态。
- `刷新账号状态` 不访问真实账号接口，而是刷新当前模板预览快照。

退出测试模板模式时：

- 清空当前检测链路、确认状态和执行日志。
- 丢弃模板账号池。
- 重新加载真实账号池和真实快照。

安全限制：

- 测试模板模式下，所有亢龙请求必须带 `account_source = test_template` 和 `test_template_id`。
- 后端必须校验请求账号全集等于或包含于模板账号集合。
- 测试模板模式不允许切换到真实账号池后继续使用旧 plan。
- 真实账号模式不允许引用 `test_template_id`。
- 应用模板后如果用户在弹窗中编辑并保存同一个模板，当前账号池不自动变化；页面必须显示“模板已更新，当前快照已过期”，并禁用确认/执行，直到用户重新应用模板并重新检测。
- 无 active run 引用当前模板时，删除当前已应用模板必须二次确认；确认删除后立即退出测试模板模式、清空当前 plan/日志/选择，并恢复真实账号池。
- 如果存在引用该模板的 active run，删除和影响 hash 的编辑必须被阻断，返回 `kanglong_test_template_active_run_exists`，提示用户先完成、终止或恢复当前 run。
- 如果 active run 已进入 `plan_confirmed`、`execution_starting` 或更后状态，模板删除只能在 abort/recover 完成后进行。
- 不影响 hash 的字段可以保存，但不能改变当前已应用快照；UI 仍应提示“当前快照来自旧版本预览”。

## 弹窗 UI

亢龙主页面只新增一个轻量入口：

- 按钮：`测试模板`
- 状态提示：未加载模板 / 已加载模板名称 / 测试模板模式
- 操作：退出测试模板模式

弹窗分区：

1. 模板库
   - 列表展示模板名、交易对、子账号数量、更新时间。
   - 支持加载、复制、删除。

2. 模板编辑
   - 模板名。
   - 交易对。
   - 主账号保证金、杠杆。
   - 子账号表格：名称、保证金、杠杆、LONG 开仓价、SHORT 开仓价、持仓数量。

3. 批量生成
   - 子账号数量。
   - 每个子账号保证金。
   - LONG 开仓价。
   - SHORT 开仓价。
   - 持仓数量。
   - 杠杆。
   - 点击后生成配平子账号行。

4. 预览
   - 当前真实参考价格。
   - 每个账号 LONG/SHORT 数量、开仓价、标记价、未实现盈亏、保证金占用、可用余额。
   - 风险提示：余额不足、数量被 step size 压缩、交易对规则缺失、订单簿不可用。

5. 底部操作
   - 保存模板。
   - 保存并应用。
   - 关闭。

弹窗文本必须进入语言包，使用 `console.kanglong.test_template.*` 命名空间。模板模式状态、预览警告、错误提示和阻断原因也必须进入语言包或 registry：前端展示文本使用 `console.kanglong.test_template.*`，运行错误使用 `runtime.kanglong.*`，阻断原因使用 `reasons.kanglong.*`，不得在 JS 或后端响应中硬编码中文。

## 计划与执行复用

测试模板模式下不新增另一套 planner。后端把模板输入转换为 `KanglongAccountSnapshot` 后，继续走现有流程：

```text
模板输入
-> 当前行情和订单簿
-> 测试账号快照 bundle
-> 预检
-> planner
-> plan / confirm / execute
-> 统一事件日志
-> 消耗统计
```

执行层仍使用模拟适配器，不真实下单。成交价、手续费、价差和失败场景沿用现有模拟执行规则；如果现有模拟执行规则读取订单簿，则测试模板模式也读取同一订单簿。

模板账号永远不能通过 `runtime_manager.build_temporary_gateway(test_account_id)` 构造真实交易所 gateway。实现时应提供独立的测试模板快照提供者或 synthetic gateway，只输出 `KanglongAccountSnapshot` 和模拟执行需要的账号状态。真实 gateway 只允许在账号无关的市场数据路径中使用，例如读取 symbol rules、quote、orderbook。

模板账号的 `snapshot_version` 必须由规范化模板输入、运行时账号 ID、价格快照、交易对规则版本和派生持仓内容计算。`snapshot_bundle_id` 必须基于这些模板 `snapshot_version` 生成，不能只依赖模板 ID 或更新时间。

## 错误处理

- JSON 文件不存在：返回空模板列表，并在首次保存时创建文件。
- JSON 文件损坏：返回 `kanglong_test_template_store_corrupted`，不自动覆盖原文件。
- JSON 写入中断：保留 `.bak` 和损坏文件，返回恢复提示，不自动丢弃用户数据。
- 如果 `.bak` 可读，列表接口返回 `recoverable_backup = true` 和备份时间；第一版至少提供后端恢复动作 `POST /kanglong/simulation/test-templates/store/recover-backup`，恢复前需要用户确认。
- 模板交易对与当前页面交易对不同：加载时切换页面交易对；检测时以模板交易对为准。
- quote 不可用：预览和检测阻断，错误码 `kanglong_test_template_quote_unavailable`。
- orderbook 不可用：预览、检测和执行复检阻断，错误码 `kanglong_test_template_orderbook_unavailable`。
- 模板账号数量不足：阻断，错误码 `kanglong_test_template_accounts_required`。
- 模板杠杆超过交易对最大杠杆：预览和检测阻断，错误码 `kanglong_test_template_leverage_exceeded`。
- 资金重算后可用余额不足：预览显示警告；如果触发亢龙现有风险上限，则检测阶段阻断。
- plan 创建后模板内容变化、删除或 hash 不一致：confirm/execute/recover 阶段返回 `blocked_plan_stale`。
- 存在引用模板的 active run 时删除或影响 hash 的编辑：返回 `kanglong_test_template_active_run_exists`。

## 测试计划

- 模板 JSON 文件不存在时，列表接口返回空列表。
- 创建模板后，JSON 文件包含完整场景并保留 Decimal 字符串。
- 保存模板时通过 `.tmp` 原子替换，并生成 `.bak`；并发保存不会破坏正式 JSON。
- 更新、复制、删除模板只影响目标模板。
- preview 使用当前 quote 重算 `mark_price`、LONG/SHORT 未实现盈亏、notional、margin、equity 和 available balance。
- preview 使用 `collateral` 作为 wallet balance，并派生 equity 和 available balance，不接受前端传入的 available balance 作为最终值。
- preview 在保证金不足时返回 `margin_deficit`，不能只把 available balance 压成 0。
- preview 返回 `template_content_hash`、`snapshot_bundle_id`、运行时账号 ID、模板账号 ID 映射、`mark_price_snapshot`、`execution_orderbook_snapshot`、交易对规则、warnings 和 blocks。
- preview 账号池必须使用 `tpl:{template_id}:...` 运行时 ID，且不会和真实账号 ID 冲突。
- preview 校验模板 ID、账号 ID 和 row ID 字符集，只允许 `[a-zA-Z0-9_-]` 进入运行时账号 ID。
- preview 在杠杆超过 `symbol_rules.max_leverage` 时阻断为 `kanglong_test_template_leverage_exceeded`。
- preview 在 quote 或 orderbook 不可用时分别返回 `kanglong_test_template_quote_unavailable` 或 `kanglong_test_template_orderbook_unavailable`。
- execution 使用订单簿 bid/ask 或深度生成成交价，不把 `mark_price`、quote mid 或单独 quote 当作成交价。
- execution 在事件日志和成本报告中记录 `execution_price_source`，至少区分 `orderbook_depth` 和 `orderbook_top`。
- preview 按交易对 step size / tick size 归一化数量和价格。
- 加载测试模板后，前端账号池完全替换为模板账号，真实账号不再出现。
- 退出测试模板模式后，真实账号池恢复。
- `account_source = test_template` 时，plan 接口拒绝模板外账号。
- `account_source = runtime` 时，plan 接口拒绝 `test_template_id`。
- `account_source = test_template` 时，plan 和 execute recheck 不会调用真实账号 gateway 的账号级方法。
- `/kanglong/simulation/run/active` 能恢复测试模板账号池；前端不会用真实账号池渲染 `tpl:...` 账号。
- plan 写入模板 hash 后，模板被编辑或删除时 confirm/execute 阻断为 `blocked_plan_stale`。
- active run 引用模板时，删除模板或修改影响 hash 的字段会被阻断。
- `.bak` 可读时，模板存储恢复接口能恢复备份且不会静默覆盖当前损坏文件。
- 模板账号路径不会调用真实账号 gateway 的查仓、撤单或下单方法。
- 模板模式下检测、确认、执行仍产出现有亢龙事件日志结构。
- 所有新增中文展示文本、预览警告、错误码、阻断原因、`mark_price_source` 和 `execution_price_source` 展示文案来自语言包或 registry，不在 JS 中硬编码。
- 所有 `kanglong_test_template_*` 错误码、`blocked_plan_stale` 扩展原因、`kanglong_test_template_leverage_exceeded`、`kanglong_test_template_orderbook_unavailable`、`kanglong_test_template_active_run_exists` 都在 i18n messages 或 registry 中有覆盖测试。

## 实施注意

- 模板存储模块应独立于 `api.py`，避免继续膨胀 API 文件。
- 模板快照转换应复用 `build_snapshot_bundle` 和现有 Decimal 规则。
- 模板来源、模板 hash 和模板输入摘要应进入 run state，与 `snapshot_bundle_id`、`plan_version` 一起用于复检和审计。
- 实现时优先新增 `paired_opener/kanglong/test_templates.py` 或同级存储/快照模块，不要把模板 JSON 读写、hash、预览转换逻辑堆进 `api.py`。
- 若第一版执行适配器暂时不能按 orderbook depth 穿透成交，必须在文档化字段和 UI 中明确展示 `orderbook_top`，避免把顶档模拟误认为深度撮合。
- 前端弹窗状态应与亢龙账号池状态分离，关闭弹窗不应丢失已应用模板。
- 保存模板不应自动应用；只有“保存并应用”或“加载”才进入测试模板模式。
- 每次应用模板都必须 invalidate 当前 plan，避免旧链路继续执行。
- 当前工作区已有未提交的 UI 间距和标记价修复改动，实施本功能前应先提交或明确保留这些改动，避免混入模板功能提交。
