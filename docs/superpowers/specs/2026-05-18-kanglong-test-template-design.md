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
- 用户录入的“保证金”统一落库为 `collateral`，表示该测试账号用于本场景的权益/资金上限；`margin_used` 和 `available_balance` 由系统按当前行情重算。
- 每次生成 plan 时必须锁定 `template_content_hash`，后续 confirm、execute 和 recover 以 run state 中的模板来源和 hash 做后端校验，不依赖前端重复传参。
- 测试模板账号只能进入 synthetic/simulation gateway；真实 gateway 只能用于读取交易对规则、quote 和订单簿，不能按模板账号查仓、撤单或下单。

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
- 用户输入的保证金字段统一命名为 `collateral`。预览和检测时派生 `equity = collateral`、`margin_used` 和 `available_balance`，避免 UI 字段和后端资金口径不一致。
- `main_account.positions` 第一版默认为空；如果未来允许主账号模板仓位，仍必须经过亢龙主账号初始空仓预检。
- `subaccounts[].qty` 表示自动生成 LONG/SHORT 双向配平仓位的数量。
- `long_entry_price` 和 `short_entry_price` 分别用于计算 LONG/SHORT 未实现盈亏。
- 每次模板保存后生成稳定的 `template_content_hash`。hash 基于规范化 JSON 内容计算，忽略字段顺序，但包含交易对、账号、保证金、杠杆、价格和数量等会影响快照的字段。
- 写入 JSON 文件必须使用原子写：先写入同目录 `.tmp` 文件，校验可读后替换正式文件；替换前保留 `.bak`。Windows 下同一时间只允许一个模板写操作，避免并发保存导致文件损坏。

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

亢龙计划接口扩展请求字段：

```json
{
  "mode": "simulation",
  "symbol": "ETHUSDC",
  "main_account_id": "test-main",
  "subaccount_ids": ["test-sub-1"],
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
- plan 创建时把 `account_source`、`test_template_id`、`template_content_hash`、模板输入摘要和生成出的 `snapshot_bundle_id` 一起写入 run state。
- confirm、execute 和 recover 不要求前端重复传 `account_source` 或 `test_template_id`；后端必须从 run state 读取模板来源并执行校验。若当前模板被编辑、删除或 hash 不一致，返回 `blocked_plan_stale`，要求用户重新应用模板并重新检测。

## 行情与计算

模板保存的是输入参数，不保存最终快照。每次加载、预览、检测和执行前复检都重新读取：

- 交易对规则：step size、tick size、最小名义价值、最大杠杆。
- 订单簿：当前 bid/ask。
- 标记价格：默认使用 mid price；如现有持仓展示已有更保守的参考价格规则，则复用现有规则。
- 执行价格：用于模拟平仓/开仓成交，必须使用真实订单簿的 bid/ask 或订单簿深度，不得直接用 mid price 代替成交价。

计算口径：

```text
mark_price = 当前参考价格
long_unrealized_pnl = (mark_price - long_entry_price) * qty
short_unrealized_pnl = (short_entry_price - mark_price) * qty
notional = mark_price * qty
position_margin = notional / leverage
margin = sum(position_margin)
equity = collateral
available_balance = max(collateral - margin, 0)
```

`mark_price` 只用于持仓展示、未实现盈亏和风险估算。移仓执行中的 close/open 成交价必须由模拟执行适配器基于订单簿计算，并进入手续费、价差损耗和执行日志。

数量和价格规则：

- 所有计算使用 Decimal。
- 生成仓位数量按交易对 step size 向下取整。
- 生成价格按交易对 tick size 归一化。
- 取整产生的差额在预览报告中展示为 `rounding_residuals`，但不写入真实执行账本。

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
- 删除当前已应用模板时必须二次确认。确认删除后立即退出测试模板模式、清空当前 plan/日志/选择，并恢复真实账号池。

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

## 错误处理

- JSON 文件不存在：返回空模板列表，并在首次保存时创建文件。
- JSON 文件损坏：返回 `kanglong_test_template_store_corrupted`，不自动覆盖原文件。
- JSON 写入中断：保留 `.bak` 和损坏文件，返回恢复提示，不自动丢弃用户数据。
- 模板交易对与当前页面交易对不同：加载时切换页面交易对；检测时以模板交易对为准。
- 订单簿不可用：预览和检测阻断，错误码 `kanglong_test_template_quote_unavailable`。
- 模板账号数量不足：阻断，错误码 `kanglong_test_template_accounts_required`。
- 资金重算后可用余额不足：预览显示警告；如果触发亢龙现有风险上限，则检测阶段阻断。
- plan 创建后模板内容变化、删除或 hash 不一致：confirm/execute/recover 阶段返回 `blocked_plan_stale`。

## 测试计划

- 模板 JSON 文件不存在时，列表接口返回空列表。
- 创建模板后，JSON 文件包含完整场景并保留 Decimal 字符串。
- 保存模板时通过 `.tmp` 原子替换，并生成 `.bak`；并发保存不会破坏正式 JSON。
- 更新、复制、删除模板只影响目标模板。
- preview 使用当前 quote 重算 `mark_price`、LONG/SHORT 未实现盈亏、notional、margin 和 available balance。
- preview 使用 `collateral` 派生 equity 和 available balance，不接受前端传入的 available balance 作为最终值。
- execution 使用订单簿 bid/ask 或深度生成成交价，不把 `mark_price` 当作成交价。
- preview 按交易对 step size / tick size 归一化数量和价格。
- 加载测试模板后，前端账号池完全替换为模板账号，真实账号不再出现。
- 退出测试模板模式后，真实账号池恢复。
- `account_source = test_template` 时，plan 接口拒绝模板外账号。
- `account_source = runtime` 时，plan 接口拒绝 `test_template_id`。
- plan 写入模板 hash 后，模板被编辑或删除时 confirm/execute 阻断为 `blocked_plan_stale`。
- 模板账号路径不会调用真实账号 gateway 的查仓、撤单或下单方法。
- 模板模式下检测、确认、执行仍产出现有亢龙事件日志结构。
- 所有新增中文展示文本、预览警告、错误码和阻断原因来自语言包或 registry，不在 JS 中硬编码。

## 实施注意

- 模板存储模块应独立于 `api.py`，避免继续膨胀 API 文件。
- 模板快照转换应复用 `build_snapshot_bundle` 和现有 Decimal 规则。
- 模板来源、模板 hash 和模板输入摘要应进入 run state，与 `snapshot_bundle_id`、`plan_version` 一起用于复检和审计。
- 前端弹窗状态应与亢龙账号池状态分离，关闭弹窗不应丢失已应用模板。
- 保存模板不应自动应用；只有“保存并应用”或“加载”才进入测试模板模式。
- 每次应用模板都必须 invalidate 当前 plan，避免旧链路继续执行。
- 当前工作区已有未提交的 UI 间距和标记价修复改动，实施本功能前应先提交或明确保留这些改动，避免混入模板功能提交。
