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
        "equity": "10000",
        "available_balance": "10000",
        "leverage": 75,
        "positions": []
      },
      "subaccounts": [
        {
          "account_id": "test-sub-1",
          "name": "测试子账号 1",
          "equity": "5000",
          "available_balance": "3000",
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
- `main_account.positions` 第一版默认为空；如果未来允许主账号模板仓位，仍必须经过亢龙主账号初始空仓预检。
- `subaccounts[].qty` 表示自动生成 LONG/SHORT 双向配平仓位的数量。
- `long_entry_price` 和 `short_entry_price` 分别用于计算 LONG/SHORT 未实现盈亏。

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
  "test_template_id": "tpl_eth_drop_001"
}
```

规则：

- `account_source` 省略或为 `runtime` 时，沿用真实账号快照。
- `account_source = test_template` 时，后端只允许使用该模板内账号。
- 如果请求中出现模板之外的账号 ID，返回阻断错误 `kanglong_test_template_account_mismatch`。
- 如果模板不存在、交易对不匹配或模板账号为空，返回明确错误码。

## 行情与计算

模板保存的是输入参数，不保存最终快照。每次加载、预览、检测和执行前复检都重新读取：

- 交易对规则：step size、tick size、最小名义价值、最大杠杆。
- 订单簿：当前 bid/ask。
- 参考价格：默认使用 mid price；如现有执行层已有更保守的参考价格规则，则复用现有规则。

计算口径：

```text
mark_price = 当前参考价格
long_unrealized_pnl = (mark_price - long_entry_price) * qty
short_unrealized_pnl = (short_entry_price - mark_price) * qty
notional = mark_price * qty
position_margin = notional / leverage
margin = sum(position_margin)
available_balance = max(equity - margin, 0)
```

若模板显式输入 `available_balance`，第一版仍以重算值为准；原始输入值只作为生成初始 equity 的辅助字段，避免模板显示和检测链路使用两套资金口径。

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

弹窗文本必须进入语言包，使用 `console.kanglong.test_template.*` 命名空间。

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

## 错误处理

- JSON 文件不存在：返回空模板列表，并在首次保存时创建文件。
- JSON 文件损坏：返回 `kanglong_test_template_store_corrupted`，不自动覆盖原文件。
- 模板交易对与当前页面交易对不同：加载时切换页面交易对；检测时以模板交易对为准。
- 订单簿不可用：预览和检测阻断，错误码 `kanglong_test_template_quote_unavailable`。
- 模板账号数量不足：阻断，错误码 `kanglong_test_template_accounts_required`。
- 资金重算后可用余额不足：预览显示警告；如果触发亢龙现有风险上限，则检测阶段阻断。

## 测试计划

- 模板 JSON 文件不存在时，列表接口返回空列表。
- 创建模板后，JSON 文件包含完整场景并保留 Decimal 字符串。
- 更新、复制、删除模板只影响目标模板。
- preview 使用当前 quote 重算 `mark_price`、LONG/SHORT 未实现盈亏、notional、margin 和 available balance。
- preview 按交易对 step size / tick size 归一化数量和价格。
- 加载测试模板后，前端账号池完全替换为模板账号，真实账号不再出现。
- 退出测试模板模式后，真实账号池恢复。
- `account_source = test_template` 时，plan 接口拒绝模板外账号。
- `account_source = runtime` 时，plan 接口拒绝 `test_template_id`。
- 模板模式下检测、确认、执行仍产出现有亢龙事件日志结构。
- 所有新增中文展示文本来自语言包，不在 JS 中硬编码。

## 实施注意

- 模板存储模块应独立于 `api.py`，避免继续膨胀 API 文件。
- 模板快照转换应复用 `build_snapshot_bundle` 和现有 Decimal 规则。
- 前端弹窗状态应与亢龙账号池状态分离，关闭弹窗不应丢失已应用模板。
- 保存模板不应自动应用；只有“保存并应用”或“加载”才进入测试模板模式。
- 每次应用模板都必须 invalidate 当前 plan，避免旧链路继续执行。
- 当前工作区已有未提交的 UI 间距和标记价修复改动，实施本功能前应先提交或明确保留这些改动，避免混入模板功能提交。
