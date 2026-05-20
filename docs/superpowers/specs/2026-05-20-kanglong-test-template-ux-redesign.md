# 亢龙测试账号模板弹窗体验改造设计

## 背景

当前“测试模板”入口的作用是创建一组亢龙模拟移仓专用的测试账号快照。用户可以用它设置主账号保证金、子账号保证金、杠杆、开仓价格和持仓数量，然后让系统按当前真实行情生成接近实盘流程的模拟账号池。

现有弹窗已经具备模板持久化和应用能力，但界面主要暴露原始 JSON 编辑区。这个形态对开发调试有用，对日常测试不直观：用户很难知道先选行情源、再填写模板、再预览、最后保存并应用；也很难确认生成的账号、仓位、盈亏和风险是否符合预期。

因此本次改造只调整测试模板的前端体验和交互组织，让它从“JSON 调试器”变成“可视化测试场景编辑器”。底层仍复用现有模板存储、preview、应用模板、检测链路和模拟执行机制。

## 目标

- 弹窗打开后，用户可以直接通过表单完成测试账号模板创建，不需要理解 JSON 结构。
- 用户可以直观看到模板库、当前编辑内容和预览快照，明确下一步该做什么。
- 保存模板和保存并应用的区别清晰：保存只入库，保存并应用会替换亢龙页面账号池。
- 测试模板模式继续完全替换账号池，不允许与真实账号混用。
- 行情源账号继续只用于读取真实 symbol rules、quote 和 orderbook，不参与主账号或子账号选择。
- 生成仓位后的标记价格、未实现盈亏、保证金占用、可用余额等数据按当前真实行情实时预览。
- 所有可见中文文本进入语言包，不在 `paired_opener/static/app.js` 中硬编码。

## 非目标

- 不新增另一套模板后端模型；第一版继续输出并保存现有模板 JSON 结构。
- 不让测试账号参与真实下单。
- 不允许真实账号和测试模板账号在同一次亢龙链路中混用。
- 不在本次改造中新增复杂行情路径模拟；价格仍来自当前真实 quote/orderbook。
- 不改变亢龙 planner、执行器、abort/recover 和成本统计的核心策略。

## 推荐方案

采用“可视化表单为主，JSON 高级模式为辅”的弹窗。

弹窗默认进入可视化模式，用户按字段填写模板名称、交易对、行情源账号、主账号资金、子账号仓位参数，并实时获得右侧预览。JSON 编辑能力保留在“高级”区域，默认折叠，只用于调试和批量粘贴；普通测试流程不需要打开 JSON。

默认草稿使用当前亢龙默认参数：交易对 `ETHUSDC`，杠杆 `75x`，主账号空仓，主账号保证金为空等待用户输入。子账号区域默认提供 1 行空白草稿，方便用户直接填写；批量生成器默认不自动生成子账号，避免用户误以为已经应用到账号池。

可视化表单是默认唯一数据源。高级 JSON 展开后先展示当前表单转换出的 JSON；用户修改 JSON 后，必须点击“校验并导入表单”。导入成功后才覆盖表单状态并标记预览过期；导入失败时只显示 JSON 错误，不影响现有表单草稿。

这个方案兼顾易用性和当前实现成本：前端负责把表单状态转换成现有模板 JSON，后端接口可以大部分复用，风险边界也不改变。

## 弹窗布局

弹窗使用三栏布局，宽屏下从左到右为：

1. 模板库
2. 模板编辑
3. 预览快照

窄屏或浏览器宽度不足时改为上下堆叠：模板库在上方，编辑区居中，预览区在下方。弹窗本身保持足够宽度，不挤压亢龙主页面布局。

### 模板库

模板库用于管理已保存的测试场景：

- 顶部放“新建模板”按钮。
- 列表项展示模板名称、交易对、子账号数量、最后更新时间。
- 当前选中模板高亮。
- 每个模板提供复制、删除、应用三个轻量操作。
- 模板为空时显示空状态，提示用户可以从右侧表单开始创建。

模板库不展示原始 JSON，不承担字段编辑职责。

模板库中的“应用”按钮与编辑区“保存并应用”使用同一套安全流程：先读取该模板的最新内容，加载到表单后用表单当前的行情源账号执行 preview，确认 `template_content_hash` 与 preview 一致且不存在阻断后，才允许替换亢龙页面账号池。模板库直接应用不会跳过 preview，也不会使用前端本地缓存的旧快照。

行情源账号的优先级固定为：模板保存的 `market_data_account_id` 是默认值；弹窗表单当前选择值是 preview 和应用时的实际值；如果用户在弹窗里修改行情源并点击保存或保存并应用，新值写回模板。模板库列表里的直接“应用”动作等价于先加载模板，再使用模板保存的默认行情源执行 preview。

### 模板编辑

编辑区按操作顺序分组，避免用户迷路：

1. 基础信息
   - 模板名称
   - 交易对
   - 行情源账号
   - 行情源状态

2. 主账号
   - 主账号名称
   - 保证金
   - 杠杆
   - 生成后默认空仓

3. 子账号
   - 子账号列表使用紧凑行布局，不使用过高卡片。
   - 每行展示名称、保证金、杠杆、LONG 开仓价、SHORT 开仓价、数量。
   - 每行提供复制、删除、上移、下移操作。
   - 行内字段校验直接显示在对应字段下方。

4. 批量生成
   - 子账号数量
   - 单账号保证金
   - 杠杆
   - LONG 开仓价
   - SHORT 开仓价
   - 持仓数量
   - 点击后一次性生成多行配平子账号。

5. 高级 JSON
   - 默认折叠。
   - 展开后显示当前表单转换出的 JSON。
   - 支持从 JSON 反向导入表单，但必须点击“校验并导入表单”并通过校验后才能覆盖表单、保存或预览。

### 预览快照

预览区展示“当前模板如果应用到亢龙页面，会得到什么账号池”：

- 顶部展示当前参考价格、价格来源、orderbook 更新时间和交易对规则状态。
- 主账号使用紧凑账号卡，突出保证金、可用余额和空仓状态。
- 子账号按行展示 LONG/SHORT 双向仓位。
- 每个方向展示数量、开仓均价、标记价格、未实现盈亏、名义价值和保证金占用。
- 风险提示分为警告和阻断。阻断存在时禁用“保存并应用”和“检测账号状态”。
- 预览为空或行情源不可用时，展示明确原因，而不是空白区域。

预览卡片沿用亢龙账号池中已经调整过的低高度排版：标签移动到标题行右侧，持仓指标用小型指标格展示，减少纵向空间占用。

preview 请求必须防止竞态覆盖。每次请求带前端递增的 `request_seq`，页面只接受最新序号的响应；旧响应返回后直接丢弃，不得覆盖当前表单、预览状态或按钮状态。任意影响模板内容、行情源账号或交易对的字段变化后，立即把预览标记为过期。

## 用户流程

### 新建模板

1. 用户点击亢龙页面的“测试模板”按钮。
2. 弹窗打开，默认创建一个未保存草稿。
3. 用户填写模板名称、交易对和行情源账号。
4. 用户填写主账号保证金和杠杆。
5. 用户手动添加子账号，或使用批量生成器生成子账号。
6. 系统调用 preview，读取真实行情并计算账号快照。
7. 用户确认预览结果。
8. 用户点击“保存模板”或“保存并应用”。

### 编辑模板

1. 用户在模板库中选择一个模板。
2. 中间表单加载模板内容。
3. 修改字段后，右侧预览标记为需要刷新。
4. 用户点击刷新预览或系统在防抖后自动刷新。
5. 保存后更新模板库的更新时间。

如果当前草稿存在未保存改动，用户切换模板、关闭弹窗、删除模板、点击模板库应用或退出测试模板模式时，必须先看到确认提示。默认行为是保留当前草稿并取消跳转；只有用户确认放弃改动后，才执行对应动作。

### 保存并应用

1. 前端先保存模板，拿到最新 `template_content_hash`。
2. 前端立即请求 preview，确认无阻断。
3. 前端用 preview 返回的账号快照替换亢龙页面账号池。
4. 亢龙页面进入测试模板模式，显示模板名称和测试账号来源。
5. 原来的检测链路、确认链路和开始模拟移仓流程继续复用现有入口。

如果 preview 返回 warning 但没有 block，保存并应用允许继续，但必须弹出一次确认，列出 warning code 和摘要。用户确认后才替换账号池，并在前端状态中记录本次 warning 已确认；后续字段变化或重新 preview 后，该确认状态失效。

warning 确认状态必须绑定到具体预览快照，绑定键为 `template_content_hash + snapshot_bundle_id/request_seq + warning_codes`。任意一项变化后，旧确认都不能复用到新的保存并应用动作。

### 退出测试模板模式

1. 用户点击页面上的退出测试模板模式操作。
2. 页面清空当前测试模板账号池、检测链路、确认状态和执行日志。
3. 页面重新加载真实账号池。
4. 已保存模板仍保留在模板库中。

## 数据与状态规则

表单状态只是一层 UI 适配，最终仍转换成现有模板结构：

```json
{
  "name": "ETH 下跌场景",
  "symbol": "ETHUSDC",
  "market_data_account_id": "main",
  "main_account": {
    "account_id": "test-main",
    "name": "测试主账号",
    "collateral": "10000",
    "leverage": 75,
    "positions": []
  },
  "subaccounts": [
    {
      "row_id": "sub-1",
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
```

运行时约束：

- 主账号第一版保持空仓，表单不提供主账号持仓输入。
- 子账号生成的是双向配平仓位，LONG/SHORT 数量一致。
- `collateral` 表示测试账号场景钱包余额。
- `market_data_account_id` 随模板保存，表示默认行情源账号。它只用于读取真实 symbol rules、quote 和 orderbook，不进入测试账号池、不参与 planner、不作为主账号或子账号候选。
- 所有数量、价格、保证金和盈亏字段以字符串保存，后端按 Decimal 处理。
- 保存模板不改变亢龙页面账号池。
- 保存并应用必须使用最新 preview 返回的账号快照，不允许前端自行拼装运行时账号。
- 已应用模板如果之后被编辑，当前账号池不自动变化，页面提示当前快照已过期，要求重新应用并重新检测链路。

如果模板保存的 `market_data_account_id` 在当前运行环境不存在、断连或无法读取行情，弹窗保留模板内容，但预览进入可重试错误状态，并要求用户重新选择行情源账号后才能保存并应用。修改行情源账号会影响本地草稿状态，但不改变主账号或子账号列表。

空字符串和未填写值不能被自动解释为 0。保证金字段中，只有用户显式输入 `0` 才表示 0；空值表示未填写，不能保存、不能 preview、不能应用。价格、数量和杠杆字段不接受 0，也不接受空值。

## 校验规则

前端做即时校验，后端继续作为最终校验来源。

基础校验：

- 模板名称不能为空。
- 交易对不能为空，并统一转换为大写。
- 行情源账号必须存在并且当前可读取行情。
- 主账号保证金必须大于或等于 0。
- 主账号杠杆必须大于 0 且不超过交易对允许上限。
- 至少需要 1 个子账号。
- 子账号名称不能为空。
- 子账号保证金必须大于或等于 0。
- 子账号杠杆必须大于 0 且不超过交易对允许上限。
- LONG 开仓价、SHORT 开仓价和数量必须大于 0。

行情与规则校验：

- 开仓价必须符合 tick size。
- 数量按 step size 向下取整后不能低于 min qty。
- 标记价格乘数量不能低于 min notional。
- orderbook 为空、过期或深度不足时，预览显示阻断原因。
- 杠杆超过交易对上限时，预览显示阻断原因。

应用校验：

- 保存并应用前必须拥有最新 `template_content_hash`。
- 如果模板内容在预览和应用之间变化，阻断并提示重新预览。
- 如果存在 active run 引用当前模板，影响 hash 的编辑、删除和重新应用必须被阻断。
- 模板库直接应用、编辑区保存并应用、JSON 导入后应用都必须走同一套 preview 和 hash 校验，不允许存在快捷路径。

active run 引用当前模板时，弹窗必须展示 active run id、状态和处理提示。当前模板的保存、删除、应用和保存并应用按钮禁用；复制为新模板允许继续，因为复制会生成新的 template id 和新的内容 hash，不影响正在运行的 run。只修改不影响 hash 的展示名称时也不能自动刷新已应用账号池。

## 交互反馈

弹窗顶部显示当前草稿状态：

- 未保存
- 已保存
- 预览已过期
- 预览可应用
- 存在阻断
- 存在未保存改动
- 警告待确认

按钮状态：

- “保存模板”在表单基础校验通过时可用。
- “保存并应用”在保存成功、preview 成功且无阻断时可用。
- “检测账号状态”仍在亢龙主页面触发，不放进弹窗里执行。
- 删除模板需要确认，当前已应用模板删除时需要二次确认并退出测试模板模式。
- “校验并导入表单”只在高级 JSON 内容与当前表单转换结果不一致时可用，导入成功后预览状态变为过期。
- active run 引用当前模板时，保存、删除、应用和保存并应用按钮禁用，只保留复制和关闭。

错误展示：

- 字段错误展示在字段附近。
- 后端阻断展示在预览区顶部。
- 网络或行情源错误展示为可重试状态。
- 成功保存或应用使用页面已有 toast 机制。
- warning 不阻断保存模板；保存并应用时需要一次显式确认。

## 文案与语言包

所有新增文案进入 `i18n/messages/zh-CN.json`，建议命名空间：

```text
console.kanglong.test_template.modal.title
console.kanglong.test_template.library.title
console.kanglong.test_template.library.empty
console.kanglong.test_template.editor.title
console.kanglong.test_template.editor.basic
console.kanglong.test_template.editor.main_account
console.kanglong.test_template.editor.subaccounts
console.kanglong.test_template.editor.batch_generator
console.kanglong.test_template.editor.advanced_json
console.kanglong.test_template.preview.title
console.kanglong.test_template.preview.empty
console.kanglong.test_template.preview.refresh
console.kanglong.test_template.actions.new
console.kanglong.test_template.actions.save
console.kanglong.test_template.actions.save_apply
console.kanglong.test_template.actions.apply
console.kanglong.test_template.actions.clone
console.kanglong.test_template.actions.delete
console.kanglong.test_template.actions.validate_import
console.kanglong.test_template.status.unsaved
console.kanglong.test_template.status.saved
console.kanglong.test_template.status.preview_stale
console.kanglong.test_template.status.preview_ready
console.kanglong.test_template.status.blocked
console.kanglong.test_template.status.dirty
console.kanglong.test_template.status.warning_pending
console.kanglong.test_template.status.active_run_locked
console.kanglong.test_template.validation.market_data_unavailable
console.kanglong.test_template.validation.empty_numeric
console.kanglong.test_template.validation.warning_confirm_required
console.kanglong.test_template.validation.*
```

运行时错误、阻断原因和事件日志继续使用现有 `runtime.kanglong.*`、`reasons.kanglong.*` 和 registry 机制。

## 文件边界

预期实现集中在前端模板弹窗和现有测试模板接口适配：

- `paired_opener/static/app.js`
  - 增加可视化表单状态。
  - 增加模板 JSON 与表单状态的双向转换。
  - 增加 preview 刷新、保存、保存并应用流程。
  - 保留高级 JSON 模式。

- `paired_opener/static/styles.css`
  - 调整弹窗三栏布局。
  - 增加紧凑子账号编辑行、预览快照和状态条样式。

- `i18n/messages/zh-CN.json`
  - 增加所有新增前端文案。

- `tests/test_app_kanglong_test_templates.mjs`
  - 增加表单转换、校验、保存并应用、JSON 高级模式回归测试。

- `tests/test_kanglong_i18n_contracts.py`
  - 增加新增文案键覆盖检查。

后端边界以当前实现为准。如果现有模板 schema、存储校验或 API 响应尚未持久化 `market_data_account_id`，实施计划必须同步修改后端模板模型、读写校验、preview 入参和 API 测试；不能只在前端保存该字段。除 `market_data_account_id` 和 preview 必要字段外，本次不新增新的后端业务模型。

## 测试计划

单元与契约测试：

- 表单默认值能生成合法模板 JSON。
- 默认草稿使用 `ETHUSDC`、`75x`、主账号空仓和 1 行空白子账号草稿。
- 空保证金不能保存、不能 preview；显式输入 `0` 才按 0 处理。
- 批量生成能创建指定数量的配平子账号。
- 表单字段修改后 preview 状态变为过期。
- 多个 preview 请求并发返回时，只有最新 `request_seq` 的响应能更新预览。
- 保存模板不会替换账号池。
- 保存并应用会使用 preview 返回的账号快照替换账号池。
- 模板库直接应用会重新读取模板、重新 preview，并校验 hash 后再替换账号池。
- JSON 高级模式导入合法 JSON 后能回填表单。
- JSON 高级模式导入非法 JSON 时显示错误且不能保存。
- JSON 高级模式编辑但未导入时，不会污染当前表单草稿。
- 已应用模板 hash 漂移时阻断确认链路。
- 有 warning 无 block 时，保存并应用需要确认；确认绑定 `template_content_hash + snapshot_bundle_id/request_seq + warning_codes`，字段变化或重新 preview 后确认失效。
- 存在未保存改动时，切换模板、关闭弹窗、删除模板和直接应用模板都会触发确认。
- 模板保存的行情源账号不存在或不可用时，模板可编辑但不可应用。
- 修改行情源账号后保存或保存并应用，会把新的 `market_data_account_id` 写回模板。
- active run 引用当前模板时，保存、删除、应用和保存并应用禁用，但复制为新模板仍可用。
- 新增语言包 key 不缺失。

界面验证：

- 8000 端口页面打开后，亢龙页面“测试模板”弹窗可正常打开。
- 模板库为空时空状态不让用户误解。
- 窄屏下三栏布局能堆叠，不出现按钮或文字重叠。
- 预览区在行情源不可用时展示明确错误。
- 子账号数量较多时，编辑区和预览区可以滚动，不撑破弹窗。

回归验证：

- 现有亢龙账号选择、检测链路、确认链路和执行日志不受影响。
- 真实账号模式下不出现测试模板账号。
- 测试模板模式退出后能恢复真实账号池。

## 实施阶段

### 阶段一：可视化弹窗骨架

建立三栏布局、模板库、编辑区和预览区的基础结构；不改变后端接口。

### 阶段二：表单状态与模板 JSON 转换

实现可视化表单的默认值、字段校验、批量生成、表单到 JSON、JSON 到表单。

### 阶段三：预览、保存和应用流程

接入现有模板 API，完成 preview 刷新、保存模板、保存并应用、退出测试模板模式。

### 阶段四：高级 JSON 与体验打磨

保留 JSON 调试能力，补齐空状态、阻断状态、加载状态、响应式布局和语言包覆盖。

## 自查结论

- 本设计没有改变亢龙移仓策略，仅改善测试模板的创建和应用体验。
- 真实账号与测试模板账号仍保持隔离，不引入混用风险。
- 保存、预览、应用三个动作的状态边界明确，能避免用户误以为“保存模板”等于“替换账号池”。
- 所有新增可见文本都要求进入语言包，符合当前中文开发环境要求。
- 当前范围足够形成单独实施计划，不需要拆成多个独立 spec。
