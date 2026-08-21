# ARB-0002 跨平台永续资金费率错位套利

> 迁移说明：本文件来自 `crypto-arb-observer`。文中的 Redis、PostgreSQL、服务名、代码路径和“已实现”状态均描述来源项目，不是 `crypto-market-info` 的实现规范；本项目以 [`../../market-data-storage.md`](../../market-data-storage.md) 和 [`../../implementation-design.md`](../../implementation-design.md) 为准。

状态：funding scanner、固定结算窗口采集和PostgreSQL replay/CLI代码已实现。固定窗口采集已通过相关普通/race测试，但尚未完成真实交易所窗口或Production验收。
证据等级：E1 -> E2 验证中
范围：只读数据抓取、funding 错位观察和复盘记录
不包含：下单、账户余额、API Key、保证金调仓、实盘风控

> [!IMPORTANT]
> ARB-0002结算窗口、T-5深度选择和135点正式采样的业务规则，只以[ARB-0002结算窗口数据采集规则](arb-0002-settlement-window-collection.md)为准。本文的candidate/active只描述`arb-opportunity`机会生命周期，不是结算窗口采集状态。

本文档是 `crypto-arb-observer` 的第二个策略分支设计和第一版边界说明。当前代码已经实现 `ARB-0022` 现货盘口价差观察分支；`ARB-0002` 已按可 review 小块接入最小 funding 数据域、scanner runtime、Redis latest、PostgreSQL funding opportunity/summary helper 和 `arb-doctor` funding 只读检查。最新接力状态以 `docs/ai/current-state.md` 为准；运行状态以实时 dev 命令和 `arb-doctor` 输出为准。

当前第一版最小实现与本文完整设计的关系：

- 已实现：Binance / OKX funding latest provider、Redis latest、PostgreSQL observations、`arb-opportunity` funding scanner、scan summary/opportunity store和Doctor funding只读检查。
- 已实现但尚未live验收：固定结算窗口runtime、四个perp-book Manager、135点sampler和窗口期funding/mark WebSocket。该采集路径默认关闭。
- 已实现：纯pair engine、精确PostgreSQL sample query和`arb-funding-replay`只读JSON/text报告；pair matrix按需计算，不默认入库。
- 当前未实现：funding universe Redis keys、`health:funding:summary`专用summary key、`capacity_checked` event wiring、paper PnL、API key、账户/仓位读取和下单。
- 本文后文仍保留完整设计和后续阶段目标；当它们与当前最小实现范围不同，按段落中的“当前第一版”说明、`docs/data/redis.md`、`docs/ai/current-state.md` 和实际代码执行。

本文后文所称“第一版”主要指`rate_spread_only` funding opportunity路径。固定窗口采集是独立的数据采集链路；replay只读消费持久化facts，不接入V1 event grade，也不构成真实收益验证。

完整机会定义见 [机会库 ARB-0002](../opportunities/opportunity-library.md#arb-0002跨平台永续资金费率错位套利)。

第一版硬边界：

- `funding.depth_enabled` 默认 false。
- 固定窗口runtime和sampler已经实现；`funding.settlement_sampling_enabled`默认false，真实运行需要单独授权。
- 启动落在`T-5`到`T+2`时整窗跳过；不恢复、不补采、不维护restart debt。
- `T-5`只做一次top1/top100选择，`T-3`到`T+2`按固定135点采样，窗口内不重新判断深度；窗口外不持续维护结算采集数据。
- 每个capture wave使用单个`pgx.Batch`持久化；只保留同一进程内已抓数据的pending write retry。
- 采集层没有candidate/active、adaptive depth controller、depth checkpoint、周期schedule refresh或authoritative reconciliation。
- 不创建 `capacity_checked` event。
- 不创建 `paper_pnl_checked` event。
- `carry_net_return_after_n_windows` / `break_even_windows` 等字段可以预留为空，但第一版不得作为触发机会或验收条件。
- 第一版唯一允许输出的 `opportunity_grade` 是 `rate_spread_only`。

## 0. 评审后修订原则

本稿经过评审后，明确区分三层结果，避免把“可观测 funding 错位”直接表述成“已覆盖完整开平仓成本的可交易套利”：

| 层级 | 枚举值 | 中文说明 |
|---|---|---|
| 资金费率错位观察 | `rate_spread_only` | 只证明不同 venue 的同标的 funding rate 存在差异，不验证盘口容量 |
| 容量检查候选 | `capacity_checked` | 在 funding 差异之外，已有永续盘口并能估算可执行容量 |
| 纸面 PnL 候选 | `paper_pnl_checked` | 明确持有窗口、开平仓成本、buffer 和结算后回放路径 |

第一版优先达成 `rate_spread_only`。Phase 5C/6虽已提供default-off盘口和settlement sampling基础，但尚未接入V1 `capacity_checked` event；只有后续完成容量证据与event wiring后才允许升级。`paper_pnl_checked`属于后续E3回放或模拟执行阶段，不作为E2的硬要求。

## 1. 背景和目标

`ARB-0002` 的机会来源是同一标的在不同永续合约平台上的资金费率错位。当前项目仍处于验证阶段，因此本设计只做以下事情：

1. 抓取 Binance / OKX 等平台的 USDT 永续合约元数据。
2. 抓取当前或下一期 funding rate、mark price、index price 和下一次结算时间。
3. 默认运行路径不启用永续盘口；已实现的Phase 5C/6仅在显式开启时为settlement sampling提供固定allowlist/fixed-depth盘口，不改变V1 `rate_spread_only`机会路径。
4. 对同一统一标的的不同 venue 进行两两比较。
5. 计算“做多低资金费率侧、做空高资金费率侧”的 funding diff 和 one-window observation。
6. 将持续满足阈值的 `rate_spread_only` 机会写入 PostgreSQL 事件和快照，方便复盘。

目标不是证明可以立即实盘赚钱，而是把 `ARB-0002` 从 `E1` 推进到 `E2`：

- 已接入具体交易所数据。
- 只读扫描能稳定复现资金费率错位。
- 每次机会都有数据时间、成本假设、无效原因和复盘快照。
- 即使没有 active opportunity，也有低频 scan summary 能证明扫描范围、最大错位和失败原因。

## 2. 与当前架构的关系

保持现有观察框架：

```text
交易所 API / WebSocket
-> arb-market-data
-> Redis latest-only 状态
-> arb-opportunity 周期扫描
-> PostgreSQL opportunity event / snapshot
```

当前 OKX / Binance 现货价差逻辑不改。`ARB-0002` 作为一条新的数据域和策略域接入：

```text
现货价差域:
spot metadata / spot book -> spot opportunity scanner

资金费率域:
perp metadata / funding latest / mark/index price -> funding opportunity scanner
```

第一版可以复用现有两个长期运行服务，并扩展现有诊断 / 测试工具：

| 组件 | 类型 | 新增职责中文说明 |
|---|---|
| `arb-market-data` | 长期运行服务 | 增加永续元数据、funding rate、mark/index price 抓取 |
| `arb-opportunity` | 长期运行服务 | 增加 funding scanner，周期读取 Redis funding latest 并计算机会 |
| `arb-doctor` | 诊断工具 | 增加 funding 数据新鲜度、scanner 健康和快照完整性检查 |
| `arb-test-injector` | 测试工具 | 增加 fake funding 数据注入，用于验证 event/snapshot 链路 |

后续若 trading 热路径需要更低延迟，再把行情和策略计算改成事件驱动；本设计暂不处理。

## 3. 命名规则

### 3.1 Venue 命名

| 字段值 | 中文说明 |
|---|---|
| `binance_usdt_perp` | Binance USDT-M 永续合约 |
| `okx_usdt_swap` | OKX USDT 保证金永续合约 |

后续可扩展：

| 字段值 | 中文说明 |
|---|---|
| `bybit_usdt_perp` | Bybit USDT 永续合约 |
| `deribit_usdc_perp` | Deribit USDC 永续合约 |
| `hyperliquid_perp` | Hyperliquid 永续合约 |

### 3.2 Symbol 命名

统一标的使用：

```text
BASE-QUOTE-PERP
```

示例：

| 原始来源 | 原始 symbol | 统一 symbol | 中文说明 |
|---|---|---|---|
| Binance USDT-M | `BTCUSDT` | `BTC-USDT-PERP` | BTC / USDT 永续 |
| OKX Swap | `BTC-USDT-SWAP` | `BTC-USDT-PERP` | BTC / USDT 永续 |

设计原则：

1. `normalized_symbol` 表示经济敞口一致，而不是交易所原始字符串一致。
2. `raw_symbol` 保留交易所原始代码。
3. `settle_asset` 必须显式记录，第一版只允许 `USDT`。
4. 币本位、USDC 本位、反向合约暂不混入同一 universe。

## 4. 数据流设计

### 4.1 元数据刷新流程

```text
Funding provider
-> 拉取交易所永续合约列表
-> 归一化 symbol / base / quote / settle / quantity_unit / contract value 字段
-> 刷新market-data进程内poll-usable/verified metadata snapshot
-> 按持久化间隔写PostgreSQL perp_instrument_metadata
```

当前没有独立Redis funding universe key。非空`funding.symbol_allowlist`是静态扫描集合；空allowlist时，funding provider poll从verified metadata构造可用latest，scanner再从Redis现存`funding:*:latest` keys动态求enabled venues共同symbol。

刷新频率：

| 配置项 | 建议默认值 | 中文说明 |
|---|---:|---|
| `funding.metadata_refresh_interval_sec` | `3600` | 永续合约元数据刷新间隔，第一版每小时一次 |

### 4.2 Funding 数据刷新流程

```text
Funding provider
-> 拉取 funding rate / next funding time / mark price / index price
-> 写 Redis funding latest key
-> 可按间隔写 PostgreSQL funding_rate_observations
-> 写 venue health
```

刷新频率：

| 配置项 | 建议默认值 | 中文说明 |
|---|---:|---|
| `funding.funding_poll_interval_ms` | `5000` | funding rate 轮询间隔 |
| `funding.funding_observation_persist_interval_ms` | `30000` | funding 原始观测落库间隔 |

### 4.3 永续盘口刷新流程

结算窗口采集已实现但默认关闭，业务流程以[结算窗口数据采集规则](arb-0002-settlement-window-collection.md)为准：

```text
T-5启动当前窗口funding/mark session
-> 按进入/维持规则统一选择前K个top100，其余top1
-> 四个Manager只维护本窗口固定目标
-> T-3到T+2按135点读取funding/mark/book latest并批量写库
-> T+2关闭session和盘口目标，完整窗口保存深度状态
```

窗口外不为settlement collection长期维护top1，也没有adaptive controller、采集candidate/active、checkpoint或重启恢复。当前V1 funding scanner不读取这些book创建`capacity_checked` event，因此第一版机会仍只能标为“资金费率错位观察”。新窗口实现尚未完成live或Production验收。

### 4.4 Funding 机会扫描流程

```text
arb-opportunity funding scanner
-> 非空allowlist使用静态symbol集合；空allowlist列举funding:*:latest并求enabled venues共同symbol
-> 对每个 normalized_symbol 读取所有 venue funding latest
-> 对每对不同 venue 生成 long / short 方向
-> 读取 funding latest 中的 mark/index price
-> 计算 gross funding diff 和 one-window observation
-> 通过阈值后进入 candidate
-> 持续满足阈值后创建 active event 和 open snapshot
-> 持续期间写 max / periodic snapshot
-> 失效后写 close snapshot
-> 每隔固定间隔写 funding_scan_summaries，记录无机会时的扫描证据
```

## 5. Redis 数据模型

Redis 继续只保存 latest-only 状态，不保存历史。

### 5.1 当前Universe发现（无专用Funding Universe Key）

当前实现不写任何专用Redis funding universe/readiness key，也不得把这类未来概念作为runtime依赖：

- `funding.symbol_allowlist`非空：scanner使用配置中的静态symbol集合。
- allowlist为空：`FundingLatestLister`列举`funding:*:latest`，只保留enabled venues均有latest的normalized symbol。
- metadata readiness和provider降级状态通过进程内snapshot及service/venue health表达，不由专用universe key表达。

### 5.2 Funding Latest Key

```text
funding:{venue_code}:{normalized_symbol}:latest
```

示例：

```text
funding:binance_usdt_perp:BTC-USDT-PERP:latest
funding:okx_usdt_swap:BTC-USDT-PERP:latest
```

Value JSON 字段：

| 字段 | 类型 | 中文说明 |
|---|---|---|
| `venue_code` | string | 交易场所代码 |
| `raw_symbol` | string | 交易所原始合约代码 |
| `normalized_symbol` | string | 系统统一永续标的 |
| `base_asset` | string | 基础资产，例如 `BTC` |
| `quote_asset` | string | 计价资产，例如 `USDT` |
| `settle_asset` | string | 结算资产，例如 `USDT` |
| `funding_rate` | decimal string | 本条数据用于判断的资金费率，必须结合 `funding_rate_kind` 解释 |
| `funding_rate_kind` | string | 费率语义：`predicted_next`、`current_window`、`settled_previous`、`unknown` |
| `predicted_funding_rate` | decimal string / null | 预测资金费率；交易所不提供时为空 |
| `previous_funding_rate` | decimal string / null | 上一期资金费率；交易所不提供时为空 |
| `source_rate_field` | string | 原始 API 字段名，例如 `fundingRate`、`nextFundingRate` |
| `is_final` | boolean | 是否已经是最终结算值；第一版 candidate 必须为 false |
| `funding_interval_hours` | decimal string | 资金费结算周期小时数 |
| `funding_period_start` | string / null | 当前 funding 周期开始时间；交易所不提供时为空 |
| `funding_period_end` | string / null | 当前 funding 周期结束时间，通常等于下一次 funding time |
| `next_funding_time` | string / null | 下一次资金费结算时间 |
| `funding_window_key` | string | funding 窗口键，格式为 venue + symbol + next funding time |
| `mark_price` | decimal string / null | 标记价格 |
| `index_price` | decimal string / null | 指数价格 |
| `source_event_ts` | string / null | 交易所数据时间 |
| `local_receive_ts` | string | 本地收到数据的时间 |
| `redis_write_ts` | string | 写入 Redis 的时间 |
| `source_sequence_id` | string / null | 数据源序列号；没有则为空 |
| `source_channel` | string | 数据来源通道，例如 REST endpoint 或 WebSocket channel |
| `raw_payload_hash` | string | 原始响应哈希，用于审计和去重 |
| `raw_payload_json` | object / null | 原始响应内容；默认关闭，仅 debug 时保存 |

TTL：

| 配置项 | 建议默认值 | 中文说明 |
|---|---:|---|
| `funding.funding_latest_ttl_ms` | `30000` | funding latest 在 Redis 中的过期时间 |

Scanner 第一版只允许以下数据进入 candidate：

```text
funding_rate_kind in ('predicted_next', 'current_window')
is_final = false
next_funding_time is not null
```

`settled_previous` 只能进入历史回放或 E3 验证，不触发新的 active opportunity。

### 5.3 Perp Ticker Key

```text
perp:{venue_code}:{normalized_symbol}:ticker
```

Value JSON 字段：

| 字段 | 类型 | 中文说明 |
|---|---|---|
| `venue_code` | string | 交易场所代码 |
| `raw_symbol` | string | 交易所原始合约代码 |
| `normalized_symbol` | string | 系统统一永续标的 |
| `last_price` | decimal string / null | 最新成交价 |
| `mark_price` | decimal string / null | 标记价格 |
| `index_price` | decimal string / null | 指数价格 |
| `best_bid_price` | decimal string / null | 最优买价 |
| `best_bid_qty` | decimal string / null | 最优买量 |
| `best_ask_price` | decimal string / null | 最优卖价 |
| `best_ask_qty` | decimal string / null | 最优卖量 |
| `open_interest` | decimal string / null | 未平仓量；交易所不提供时为空 |
| `volume_24h_base` | decimal string / null | 24 小时基础资产成交量 |
| `volume_24h_quote` | decimal string / null | 24 小时计价资产成交额 |
| `source_event_ts` | string / null | 交易所数据时间 |
| `local_receive_ts` | string | 本地收到数据的时间 |
| `redis_write_ts` | string | 写入 Redis 的时间 |
| `raw_payload_hash` | string | 原始响应哈希 |

### 5.4 Perp Orderbook Key

```text
md:{venue_code}:{normalized_symbol}:book:{depth_level}
```

当前Phase 5A/5C contract只允许`depth_level=1`或`100`，由`arb0002.PerpBookLatestKey`及`Set/Get/DeletePerpBookLatest`读写；TTL来自`funding.book_ttl_ms`。Value是ready-only `arb0002.PerpBookLatest`，包含source depth、数量单位/合约换算、统一后的`source_quantity/base_quantity`、sync/sequence和三个事实时间戳。完整字段以来源项目的 Redis 数据字典和实际model为准，不在本迁移副本中重复维护第二份字段表。

当前固定窗口runtime默认关闭。四个venue/depth Manager复用既有worker、collector和shared WebSocket transport，但只接收`T-5`确定的本窗口top1/top100目标；窗口关闭时清空目标。采集层不再有长期top1或adaptive controller。

## 6. PostgreSQL 数据模型

### 6.1 `perp_instrument_metadata`

用途：保存永续合约元数据。

| 字段 | 类型 | 是否为空 | 中文说明 |
|---|---|---:|---|
| `id` | bigserial | 否 | 主键 |
| `venue_code` | text | 否 | 交易场所代码 |
| `venue_type` | text | 否 | 场所类型，第一版为 `CEX` |
| `market_type` | text | 否 | 市场类型，第一版为 `perpetual` |
| `raw_symbol` | text | 否 | 交易所原始合约代码 |
| `normalized_symbol` | text | 否 | 系统统一永续标的 |
| `base_asset` | text | 否 | 基础资产 |
| `quote_asset` | text | 否 | 计价资产 |
| `settle_asset` | text | 否 | 结算资产 |
| `contract_type` | text | 否 | 合约类型，第一版固定为 `perpetual` |
| `contract_size` | numeric | 否 | 兼容字段，第一版等同于 `contract_value * contract_multiplier` |
| `contract_value` | numeric | 否 | 单张合约面值 |
| `contract_value_currency` | text | 否 | 合约面值计价币种，例如 `BTC`、`USDT` |
| `contract_multiplier` | numeric | 否 | 合约乘数，默认 1 |
| `quantity_unit` | text | 否 | 原始盘口数量单位：`base` 或 `contract` |
| `linear_contract` | boolean | 否 | 是否线性合约；第一版只允许 true |
| `funding_interval_hours` | numeric | 否 | 资金费结算周期小时数 |
| `trading_enabled` | boolean | 否 | 是否处于可交易状态 |
| `source_status` | text | 否 | 交易所返回的原始状态 |
| `price_tick_size` | numeric | 否 | 最小价格变动单位 |
| `quantity_step_size` | numeric | 否 | 最小数量变动单位 |
| `min_base_quantity` | numeric | 否 | 最小基础资产数量 |
| `min_quote_notional` | numeric | 是 | 最小名义金额；交易所不提供时为空 |
| `max_leverage` | numeric | 是 | 最大杠杆；只做风险展示，不用于下单 |
| `raw_metadata_json` | jsonb | 否 | 原始元数据 JSON |
| `metadata_updated_at` | timestamptz | 否 | 元数据更新时间 |
| `created_at` | timestamptz | 否 | 首次入库时间 |
| `updated_at` | timestamptz | 否 | 最近更新时间 |

建议约束和索引：

| 名称 | 中文说明 |
|---|---|
| unique `(venue_code, raw_symbol)` | 同一 venue 的原始合约不能重复 |
| index `(normalized_symbol)` | 加速按统一标的查询 |
| index `(venue_code, normalized_symbol)` | 加速构建 funding universe |
| index `(trading_enabled)` | 加速过滤可交易合约 |

### 6.2 `funding_rate_observations`

用途：低频保存 funding 原始观测，便于回放和确认数据源稳定性。

| 字段 | 类型 | 是否为空 | 中文说明 |
|---|---|---:|---|
| `id` | bigserial | 否 | 主键 |
| `venue_code` | text | 否 | 交易场所代码 |
| `raw_symbol` | text | 否 | 交易所原始合约代码 |
| `normalized_symbol` | text | 否 | 系统统一永续标的 |
| `base_asset` | text | 否 | 基础资产 |
| `quote_asset` | text | 否 | 计价资产 |
| `settle_asset` | text | 否 | 结算资产 |
| `funding_window_key` | text | 否 | funding 窗口键，格式为 venue + symbol + next funding time |
| `funding_rate` | numeric | 否 | 本条数据用于判断或回放的资金费率，必须结合 `funding_rate_kind` 解释 |
| `funding_rate_kind` | text | 否 | 费率语义：`predicted_next`、`current_window`、`settled_previous`、`unknown` |
| `is_final` | boolean | 否 | 是否已经是最终结算值 |
| `predicted_funding_rate` | numeric | 是 | 预测资金费率；没有则为空 |
| `previous_funding_rate` | numeric | 是 | 上一期资金费率；没有则为空 |
| `source_rate_field` | text | 否 | 原始 API 字段名 |
| `funding_interval_hours` | numeric | 否 | 资金费结算周期 |
| `funding_period_start` | timestamptz | 是 | funding 周期开始时间 |
| `funding_period_end` | timestamptz | 是 | funding 周期结束时间 |
| `next_funding_time` | timestamptz | 是 | 下一次资金费结算时间 |
| `mark_price` | numeric | 是 | 标记价格 |
| `index_price` | numeric | 是 | 指数价格 |
| `source_event_ts` | timestamptz | 是 | 交易所数据时间 |
| `local_receive_ts` | timestamptz | 否 | 本地接收时间 |
| `redis_write_ts` | timestamptz | 是 | 写入 Redis 时间 |
| `source_channel` | text | 否 | 数据来源通道 |
| `raw_payload_hash` | text | 否 | 原始响应哈希 |
| `raw_payload_json` | jsonb | 是 | 原始响应内容；默认不保存 |
| `created_at` | timestamptz | 否 | 入库时间 |

建议索引：

| 名称 | 中文说明 |
|---|---|
| index `(normalized_symbol, created_at DESC)` | 加速标的历史查询 |
| index `(venue_code, normalized_symbol, created_at DESC)` | 加速单 venue 历史查询 |
| index `(next_funding_time)` | 加速按结算窗口查询 |
| index `(funding_window_key, created_at DESC)` | 加速同一 funding window 的预测值和结算值回放 |
| index `(venue_code, normalized_symbol, next_funding_time)` | 加速按交易所、标的和结算窗口查询 |

### 6.3 `funding_scan_summaries`

用途：低频保存 funding scanner 的扫描汇总。即使没有 active opportunity，也能证明系统扫描过哪些 pair、最大错位是多少，以及无效原因是什么。

| 字段 | 类型 | 是否为空 | 中文说明 |
|---|---|---:|---|
| `id` | bigserial | 否 | 主键 |
| `scan_at` | timestamptz | 否 | 扫描时间 |
| `duration_ms` | bigint | 否 | 本轮扫描耗时 |
| `symbols_count` | integer | 否 | 本轮 symbol 数量 |
| `venues_count` | integer | 否 | 本轮 venue 数量 |
| `expected_pairs_count` | integer | 否 | 理论方向组合数 |
| `pairs_evaluated_count` | integer | 否 | 实际进入 engine 的 pair 数量 |
| `valid_candidates_count` | integer | 否 | 通过阈值的 pair 数量 |
| `invalid_counts_json` | jsonb | 否 | 按 invalid reason 聚合的计数 |
| `max_funding_rate_diff` | numeric | 是 | 本轮最大资金费率差 |
| `max_one_window_net_observation` | numeric | 是 | 本轮最大单窗口观测净值 |
| `max_carry_net_return_after_n_windows` | numeric | 是 | 本轮最大多窗口摊销净值；未启用时为空 |
| `top_candidates_json` | jsonb | 是 | 前 N 个最接近机会的 pair，用于无机会复盘 |
| `config_snapshot_json` | jsonb | 否 | 本轮配置快照 |
| `created_at` | timestamptz | 否 | 入库时间 |

建议索引：

| 名称 | 中文说明 |
|---|---|
| index `(scan_at DESC)` | 加速查看最近扫描 |

写入频率建议：

| 策略 | 中文说明 |
|---|---|
| 低频写入 | 例如每 30 秒或 60 秒写一条 summary，而不是每轮扫描都写 |
| 按 funding window 聚合 | 对临近同一结算窗口的扫描可以聚合保存，避免无机会时产生过量数据 |
| Redis 写最新状态 | 高频 scan 细节仍可写 Redis health，PostgreSQL 只保存复盘需要的摘要 |

### 6.4 `funding_opportunity_events`

用途：保存资金费率套利机会生命周期。

| 字段 | 类型 | 是否为空 | 中文说明 |
|---|---|---:|---|
| `id` | bigserial | 否 | 主键 |
| `opportunity_key` | text | 否 | 机会唯一键，包含 symbol、long venue、short venue 和首次出现时间 |
| `normalized_symbol` | text | 否 | 系统统一永续标的 |
| `base_asset` | text | 否 | 基础资产 |
| `quote_asset` | text | 否 | 计价资产 |
| `settle_asset` | text | 否 | 结算资产 |
| `long_venue_code` | text | 否 | 做多永续的一侧 |
| `short_venue_code` | text | 否 | 做空永续的一侧 |
| `long_funding_window_key` | text | 否 | 做多侧 funding 窗口键 |
| `short_funding_window_key` | text | 否 | 做空侧 funding 窗口键 |
| `common_funding_window_key` | text | 是 | 两边 funding 时间足够接近时的共同窗口键；否则为空 |
| `status` | text | 否 | 事件状态，`active` 或 `closed` |
| `started_at` | timestamptz | 否 | 机会首次满足阈值的时间 |
| `ended_at` | timestamptz | 是 | 机会关闭时间 |
| `duration_ms` | bigint | 是 | 机会持续毫秒数 |
| `close_reason` | text | 是 | 关闭原因 |
| `opportunity_grade` | text | 否 | 机会等级：`rate_spread_only`、`capacity_checked`、`paper_pnl_checked` |
| `capacity_verified` | boolean | 否 | 是否已通过盘口估算容量 |
| `capacity_source` | text | 是 | 容量来源，例如 `perp_orderbook_depth`；未验证时为空 |
| `sample_count` | bigint | 否 | active 期间有效样本数量 |
| `max_funding_rate_diff` | numeric | 否 | 最大资金费率差 |
| `avg_funding_rate_diff` | numeric | 否 | 平均资金费率差 |
| `max_one_window_net_observation` | numeric | 否 | 最大单 funding 窗口观测净值，不扣完整开平仓手续费 |
| `avg_one_window_net_observation` | numeric | 否 | 平均单 funding 窗口观测净值 |
| `max_carry_net_return_after_n_windows` | numeric | 是 | 最大多窗口摊销净收益；未启用 carry 模型时为空 |
| `avg_carry_net_return_after_n_windows` | numeric | 是 | 平均多窗口摊销净收益；未启用 carry 模型时为空 |
| `min_break_even_windows` | integer | 是 | 最小回本 funding 窗口数 |
| `max_estimated_capacity_notional` | numeric | 是 | 最大估算容量名义金额；`rate_spread_only` 时为空表示未验证容量 |
| `avg_estimated_capacity_notional` | numeric | 是 | 平均估算容量名义金额；`rate_spread_only` 时为空表示未验证容量 |
| `long_taker_fee` | numeric | 否 | 做多侧 taker fee 假设 |
| `short_taker_fee` | numeric | 否 | 做空侧 taker fee 假设 |
| `estimated_close_fee` | numeric | 否 | 平仓手续费假设 |
| `slippage_buffer` | numeric | 否 | 滑点缓冲 |
| `basis_buffer` | numeric | 否 | 价差或 mark/index 偏离风险缓冲 |
| `strategy_min_rate_diff` | numeric | 否 | 最小资金费率差阈值 |
| `strategy_min_one_window_net_observation` | numeric | 否 | 单窗口观测净值阈值，只扣 slippage / basis buffer，不扣完整开平仓手续费 |
| `strategy_min_duration_ms` | integer | 否 | 机会持续时间阈值 |
| `strategy_min_executable_notional` | numeric | 否 | 最小可执行名义金额 |
| `max_funding_data_age_ms` | integer | 否 | funding 数据最大允许年龄 |
| `max_funding_receive_diff_ms` | integer | 否 | 两边 funding 数据接收时间最大差异 |
| `max_book_age_ms` | integer | 否 | 盘口数据最大允许年龄 |
| `max_next_funding_time_diff_ms` | integer | 否 | 两边下一次结算时间最大差异 |
| `min_time_to_funding_ms` | integer | 否 | 距离结算的最短允许时间 |
| `max_time_to_funding_ms` | integer | 否 | 距离结算的最长允许时间 |
| `max_abs_entry_mark_spread` | numeric | 是 | 两边 mark price 最大允许绝对偏离 |
| `max_abs_entry_index_spread` | numeric | 是 | 两边 index price 最大允许绝对偏离 |
| `require_index_price` | boolean | 否 | 是否强制要求 index price |
| `depth_enabled` | boolean | 否 | 本事件创建时是否启用盘口容量检查 |
| `depth_level` | integer | 否 | 盘口档位 |
| `config_snapshot_json` | jsonb | 否 | 创建机会时使用的配置快照 |
| `created_at` | timestamptz | 否 | 入库时间 |
| `updated_at` | timestamptz | 否 | 更新时间 |

第一版 `funding.depth_enabled=false` 时：

- `opportunity_grade` 必须为 `rate_spread_only`。
- `capacity_verified` 必须为 false。
- `capacity_source` 必须为 null。
- `max_carry_net_return_after_n_windows` / `avg_carry_net_return_after_n_windows` / `min_break_even_windows` 必须为 null。
- `max_estimated_capacity_notional` / `avg_estimated_capacity_notional` 必须为 null。
- `depth_enabled` 必须为 false；`depth_level` 可以保留默认值，但不得触发盘口订阅或容量检查。

建议约束：

| 名称 | 中文说明 |
|---|---|
| partial unique `(normalized_symbol, long_venue_code, short_venue_code) where status = 'active'` | 同一方向同时只能有一个 active funding 机会 |

### 6.5 `funding_opportunity_snapshots`

用途：保存 funding 机会打开、最大值、周期复盘和关闭时的完整计算快照。

| 字段 | 类型 | 是否为空 | 中文说明 |
|---|---|---:|---|
| `id` | bigserial | 否 | 主键 |
| `event_id` | bigint | 否 | 关联的 funding opportunity event |
| `snapshot_type` | text | 否 | 快照类型：`open`、`max`、`periodic`、`close`、`invalid` |
| `snapshot_at` | timestamptz | 否 | 快照时间 |
| `normalized_symbol` | text | 否 | 系统统一永续标的 |
| `base_asset` | text | 否 | 基础资产 |
| `quote_asset` | text | 否 | 计价资产 |
| `settle_asset` | text | 否 | 结算资产 |
| `long_venue_code` | text | 否 | 做多永续的一侧 |
| `short_venue_code` | text | 否 | 做空永续的一侧 |
| `long_funding_window_key` | text | 否 | 做多侧 funding 窗口键 |
| `short_funding_window_key` | text | 否 | 做空侧 funding 窗口键 |
| `common_funding_window_key` | text | 是 | 两边 funding 时间足够接近时的共同窗口键；否则为空 |
| `opportunity_grade` | text | 否 | 机会等级 |
| `capacity_verified` | boolean | 否 | 是否已验证容量 |
| `capacity_source` | text | 是 | 容量来源 |
| `long_funding_json` | jsonb | 否 | 做多侧 funding latest 完整 JSON |
| `short_funding_json` | jsonb | 否 | 做空侧 funding latest 完整 JSON |
| `long_book_json` | jsonb | 是 | 做多侧永续盘口 JSON；未抓盘口时为空 |
| `short_book_json` | jsonb | 是 | 做空侧永续盘口 JSON；未抓盘口时为空 |
| `long_funding_rate` | numeric | 否 | 做多侧资金费率 |
| `short_funding_rate` | numeric | 否 | 做空侧资金费率 |
| `funding_rate_diff` | numeric | 否 | 资金费率差，等于 `short_funding_rate - long_funding_rate` |
| `long_funding_rate_kind` | text | 否 | 做多侧 funding rate 语义 |
| `short_funding_rate_kind` | text | 否 | 做空侧 funding rate 语义 |
| `long_is_final` | boolean | 否 | 做多侧 funding 是否已最终结算 |
| `short_is_final` | boolean | 否 | 做空侧 funding 是否已最终结算 |
| `long_predicted_funding_rate` | numeric | 是 | 做多侧预测资金费率 |
| `short_predicted_funding_rate` | numeric | 是 | 做空侧预测资金费率 |
| `long_mark_price` | numeric | 是 | 做多侧标记价格 |
| `short_mark_price` | numeric | 是 | 做空侧标记价格 |
| `long_index_price` | numeric | 是 | 做多侧指数价格 |
| `short_index_price` | numeric | 是 | 做空侧指数价格 |
| `entry_mark_spread` | numeric | 是 | 两边 mark price 相对偏差 |
| `entry_index_spread` | numeric | 是 | 两边 index price 相对偏差 |
| `next_funding_time` | timestamptz | 是 | 采用的共同或最近 funding 结算时间 |
| `long_next_funding_time` | timestamptz | 是 | 做多侧下一次 funding 结算时间 |
| `short_next_funding_time` | timestamptz | 是 | 做空侧下一次 funding 结算时间 |
| `time_to_funding_ms` | bigint | 是 | 距离下一次 funding 的毫秒数 |
| `funding_time_diff_ms` | bigint | 是 | 两边 funding 结算时间差 |
| `gross_funding_diff` | numeric | 否 | 原始资金费率差 |
| `one_window_net_observation` | numeric | 否 | 单窗口观测净值，只扣观测 buffer |
| `expected_hold_windows` | integer | 是 | 多窗口 carry 模型假设持有窗口数 |
| `carry_net_return_after_n_windows` | numeric | 是 | 多窗口摊销后的预计净收益率 |
| `break_even_windows` | integer | 是 | 按当前 funding 差覆盖开平仓成本所需窗口数 |
| `long_open_fee` | numeric | 否 | 做多侧开仓手续费率估计 |
| `short_open_fee` | numeric | 否 | 做空侧开仓手续费率估计 |
| `estimated_close_fee` | numeric | 否 | 两边合计平仓手续费率估计 |
| `slippage_buffer` | numeric | 否 | 滑点缓冲 |
| `basis_buffer` | numeric | 否 | basis 风险缓冲 |
| `executable_base_qty` | numeric | 是 | 根据盘口估算的可执行基础资产数量 |
| `estimated_capacity_notional` | numeric | 是 | 根据盘口估算的可执行名义金额 |
| `long_book_age_ms` | bigint | 是 | 做多侧盘口年龄 |
| `short_book_age_ms` | bigint | 是 | 做空侧盘口年龄 |
| `long_funding_age_ms` | bigint | 否 | 做多侧 funding 数据年龄 |
| `short_funding_age_ms` | bigint | 否 | 做空侧 funding 数据年龄 |
| `funding_receive_diff_ms` | bigint | 否 | 两边 funding 数据本地接收时间差 |
| `calculation_valid` | boolean | 否 | 本次计算是否有效 |
| `invalid_reason` | text | 是 | 无效原因 |
| `created_at` | timestamptz | 否 | 入库时间 |

第一版 `funding.depth_enabled=false` 时：

- `opportunity_grade` 必须为 `rate_spread_only`。
- `capacity_verified` 必须为 false。
- `capacity_source` 必须为 null。
- `long_book_json` / `short_book_json` 必须为 null。
- `expected_hold_windows` / `carry_net_return_after_n_windows` / `break_even_windows` 可以为 null，且不得参与 candidate 触发。
- `executable_base_qty` / `estimated_capacity_notional` / `long_book_age_ms` / `short_book_age_ms` 必须为 null。

建议索引：

| 名称 | 中文说明 |
|---|---|
| index `(event_id)` | 加速按事件查询快照 |
| index `(normalized_symbol, snapshot_at DESC)` | 加速按标的查询历史快照 |
| index `(snapshot_type)` | 加速查询 open / max / close |

## 7. Funding 计算规则

### 7.1 Funding 收益方向

通用规则：

```text
做多某永续的 funding 收益 = - funding_rate
做空某永续的 funding 收益 = + funding_rate
```

因此对一个方向：

```text
long venue = 资金费率较低的一侧
short venue = 资金费率较高的一侧
gross_return = short_funding_rate - long_funding_rate
```

示例：

| Venue | Funding Rate | 理论动作 | 中文说明 |
|---|---:|---|---|
| Binance | `0.00030` | short | 资金费高，做空收资金费 |
| OKX | `0.00005` | long | 资金费低，做多支付较少资金费 |

```text
gross_return = 0.00030 - 0.00005 = 0.00025
```

如果 funding rate 为负：

| Venue | Funding Rate | 理论动作 | 中文说明 |
|---|---:|---|---|
| Binance | `-0.00020` | long | 负 funding，做多收资金费 |
| OKX | `0.00005` | short | 正 funding，做空收资金费 |

```text
gross_return = 0.00005 - (-0.00020) = 0.00025
```

### 7.2 不同 funding 周期归一化

第一版建议优先要求两边 `funding_interval_hours` 相同，通常为 8 小时。

若后续允许不同周期，按小时归一化：

```text
short_rate_per_hour = short_funding_rate / short_interval_hours
long_rate_per_hour = long_funding_rate / long_interval_hours
gross_return_for_window = (short_rate_per_hour - long_rate_per_hour) * common_window_hours
```

第一版无效规则：

| 无效原因 | 中文说明 |
|---|---|
| `funding_interval_mismatch` | 两边 funding 周期不同，第一版暂不比较 |

### 7.3 结算时间对齐

Funding arbitrage 必须关心下一次结算时间。第一版要求：

```text
abs(long_next_funding_time - short_next_funding_time) <= funding.max_next_funding_time_diff_ms
```

建议默认：

| 配置项 | 默认值 | 中文说明 |
|---|---:|---|
| `funding.max_next_funding_time_diff_ms` | `300000` | 两边下一次 funding 时间最多相差 5 分钟 |

如果结算时间不一致，第一版不把它当成稳定机会，因为一边可能先结算，另一边 funding rate 在后续窗口变动。

### 7.4 时间窗口过滤

建议加入距离结算时间的上下限：

| 配置项 | 默认值 | 中文说明 |
|---|---:|---|
| `funding.min_time_to_funding_ms` | `60000` | 距离结算少于 1 分钟时不新建机会，避免过期数据 |
| `funding.max_time_to_funding_ms` | `28800000` | 距离结算超过 8 小时时不作为当前窗口机会 |

说明：如果交易所提供的是下一期预测 funding，距离结算太远时只作为观察，不应直接按可收敛收益处理。

### 7.5 成本模型

第一版把收益拆成三层，避免把 E2 观察值误写成已经覆盖完整开平仓成本的交易收益。

#### 7.5.1 单窗口资金费率差

```text
gross_funding_diff = short_funding_rate - long_funding_rate
```

含义：只表示两个 venue 当前 funding rate 的差，不扣任何交易成本。

#### 7.5.2 单窗口观测净值

```text
one_window_net_observation =
  gross_funding_diff
  - slippage_buffer
  - basis_buffer
```

含义：用于 E2 观察，只扣观测层面的保守 buffer，不扣完整开平仓手续费。这样可以回答“资金费率错位是否足够明显”，但不能回答“开仓后马上平仓是否赚钱”。

#### 7.5.3 多窗口 carry 纸面收益

```text
open_fee_cost =
  long_open_taker_fee
  + short_open_taker_fee

close_fee_cost =
  long_close_taker_fee
  + short_close_taker_fee

carry_gross_return =
  gross_funding_diff * expected_hold_windows

carry_net_return_after_n_windows =
  carry_gross_return
  - open_fee_cost
  - close_fee_cost
  - slippage_buffer
  - basis_buffer
  - borrow_or_margin_cost
```

含义：用于 E3 或更接近交易模拟的纸面 PnL。开平仓成本应摊到多个 funding 窗口，而不是默认全部压到单个窗口。

回本窗口数：

```text
break_even_windows =
  ceil((open_fee_cost + close_fee_cost + slippage_buffer + basis_buffer + borrow_or_margin_cost) / gross_funding_diff)
```

如果 `gross_funding_diff <= 0`，则 `break_even_windows = null`。

字段说明：

| 字段 | 中文说明 |
|---|---|
| `gross_funding_diff` | 原始资金费率差 |
| `one_window_net_observation` | 单窗口观测净值，适合 E2 |
| `expected_hold_windows` | 假设持有多少个 funding 窗口 |
| `carry_net_return_after_n_windows` | 多窗口摊销后的纸面净收益率 |
| `break_even_windows` | 按当前 funding 差覆盖开平仓成本所需窗口数 |
| `long_open_taker_fee` | 做多侧开仓手续费 |
| `short_open_taker_fee` | 做空侧开仓手续费 |
| `long_close_taker_fee` | 做多侧平仓手续费 |
| `short_close_taker_fee` | 做空侧平仓手续费 |
| `borrow_or_margin_cost` | 借贷或保证金资金成本；第一版可为 0 或 null |
| `slippage_buffer` | 盘口滑点、撮合延迟和手续费档位误差的保守缓冲 |
| `basis_buffer` | 两个交易所 mark/index/成交价偏离导致平仓价差不利变化的缓冲 |

第一版默认不开 maker 假设，因为 maker 成交概率和逆向选择需要额外模型。

#### 7.5.4 结算后回放路径

本节仅描述后续 E3 回放设计。第一版不得实现 E3 回放，不得创建 `paper_pnl_checked` event；第一版只保留支持后续回放所需的数据字段。回放按 `funding_window_key` 聚合：

1. 找到窗口内最后一个 `funding_rate_kind in ('predicted_next', 'current_window')` 且 `is_final=false` 的观测值。
2. 找到同一窗口结算后第一个 `funding_rate_kind='settled_previous'` 或 `is_final=true` 的观测值。
3. 计算预测误差、实际 funding diff 和理论 realized funding return。
4. 若当时有盘口快照，再用 open snapshot 的容量和价格估算纸面持仓收益。

这条路径用于 E3，不改变第一版 E2 的只读观察定位。

### 7.6 盘口容量估算

若永续盘口可用：

1. 做多侧开仓需要吃 ask。
2. 做空侧开仓需要打 bid。
3. 取两边前 `depth_level` 档。
4. 将交易所张数统一转换为基础资产数量。
5. 取共同可执行基础资产数量。
6. 计算两边加权均价和估算名义金额。

```text
long_available_base_qty = sum(long_asks[0:depth_level])
short_available_base_qty = sum(short_bids[0:depth_level])
executable_base_qty = min(long_available_base_qty, short_available_base_qty)

long_cost_notional = weighted_notional(long_asks, executable_base_qty)
short_sell_notional = weighted_notional(short_bids, executable_base_qty)
estimated_capacity_notional = min(long_cost_notional, short_sell_notional)
```

容量过滤：

| 配置项 | 默认值 | 中文说明 |
|---|---:|---|
| `funding.strategy_min_executable_notional` | `100` | 最小可执行名义金额，低于该值不记录为 `capacity_checked` 机会 |

如果盘口不可用：

| 字段 | 处理方式 | 中文说明 |
|---|---|---|
| `estimated_capacity_notional` | null | 不估算容量 |
| `executable_base_qty` | null | 不估算数量 |
| `capacity_verified` | false | 容量未验证 |

以下是长期等级规则；第一版硬边界下仅 `depth_enabled=false` -> `rate_spread_only` 生效。其他行仅为后续阶段设计，不得在第一版实现。

事件等级规则：

| 条件 | 输出等级 | 中文说明 |
|---|---|---|
| `depth_enabled=false` | `rate_spread_only` | 可以创建 funding 错位观察 event，但容量字段必须为空 |
| `depth_enabled=true` 且盘口齐全 | `capacity_checked` | 通过容量和最小名义金额检查后创建容量候选 |
| `depth_enabled=true` 但盘口缺失 | `rate_spread_only` 或 invalid | 若配置允许降级，则只记录错位观察；否则记为缺盘口无效 |
| 完成结算后回放 | `paper_pnl_checked` | 后续 E3 阶段使用，不是第一版硬要求 |

`funding.strategy_min_executable_notional` 只对 `capacity_checked` 生效。没有盘口时可以记录 funding 差，但不得将机会标记为“容量已验证”。

### 7.7 Mark / Index 偏离过滤

资金费率差不是完全无风险收益。两边永续的 mark price、index price、盘口成交价可能不同。

建议计算：

```text
entry_mark_spread = short_mark_price / long_mark_price - 1
entry_index_spread = short_index_price / long_index_price - 1
```

过滤规则：

| 配置项 | 默认值 | 中文说明 |
|---|---:|---|
| `funding.max_abs_entry_mark_spread` | `0.002` | 两边 mark price 相对偏离超过 20 bps 时不记录 |
| `funding.max_abs_entry_index_spread` | `0.002` | 两边 index price 相对偏离超过 20 bps 时不记录 |

如果某交易所缺少 index price，可以只使用 mark price，但快照必须记录缺失字段。

## 8. 无效原因枚举

新增 `FundingInvalidReason`：

| 枚举值 | 中文说明 |
|---|---|
| `missing_long_funding` | 缺少做多侧 funding 数据 |
| `missing_short_funding` | 缺少做空侧 funding 数据 |
| `stale_long_funding` | 做多侧 funding 数据过期 |
| `stale_short_funding` | 做空侧 funding 数据过期 |
| `unsupported_funding_rate_kind` | funding rate 语义不适合创建新机会，例如已结算或未知 |
| `final_funding_rate_not_candidate` | 数据是最终结算值，只能进入历史回放，不能触发新机会 |
| `funding_receive_diff_too_large` | 两边 funding 数据接收时间差过大 |
| `next_funding_time_missing` | 缺少下一次 funding 结算时间 |
| `next_funding_time_diff_too_large` | 两边下一次 funding 结算时间差过大 |
| `time_to_funding_too_short` | 距离 funding 结算太近 |
| `time_to_funding_too_long` | 距离 funding 结算太远 |
| `funding_interval_mismatch` | 两边 funding 周期不一致 |
| `settle_asset_mismatch` | 两边结算资产不一致 |
| `base_asset_mismatch` | 两边基础资产不一致 |
| `quote_asset_mismatch` | 两边计价资产不一致 |
| `normalized_symbol_mismatch` | 两边统一标的不一致 |
| `mark_price_missing` | 缺少 mark price |
| `index_price_missing` | 缺少 index price，但配置要求必须有 |
| `zero_or_negative_price` | 价格小于或等于 0 |
| `entry_mark_spread_too_large` | 两边 mark price 偏离过大 |
| `entry_index_spread_too_large` | 两边 index price 偏离过大 |
| `missing_long_book` | 缺少做多侧盘口 |
| `missing_short_book` | 缺少做空侧盘口 |
| `stale_long_book` | 做多侧盘口过期 |
| `stale_short_book` | 做空侧盘口过期 |
| `invalid_book_depth` | 盘口深度无效 |
| `insufficient_notional` | 可执行名义金额不足 |
| `one_window_observation_below_threshold` | 单窗口观测净值低于阈值 |
| `symbol_disabled` | 该标的不在启用列表或已被 block |
| `venue_disabled` | 交易场所未启用 |
| `trading_disabled` | 交易所元数据显示合约不可交易 |
| `unsupported_contract_quantity_unit` | 合约数量单位无法安全转换为基础资产数量 |

Close reason 映射：

| close_reason | 对应无效原因中文说明 |
|---|---|
| `funding_below_threshold` | 收益低于阈值 |
| `stale_funding_data` | funding 数据过期或时间差过大 |
| `stale_market_data` | 盘口、ticker 或 mark/index 数据过期 |
| `missing_market_data` | 缺少 funding、盘口或 ticker |
| `insufficient_notional` | 容量不足 |
| `funding_window_rollover` | funding 窗口切换，需要关闭旧机会并重新评估 |
| `symbol_disabled` | 标的被禁用 |
| `venue_disabled` | venue 被禁用 |
| `service_shutdown` | 服务退出时关闭 active 机会 |

## 9. Opportunity 生命周期

本章只描述`arb-opportunity` funding scanner的机会事件生命周期。这里的candidate/active与结算窗口盘口采集无关，不能把它们恢复成采集准备态或深度状态。

### 9.1 Candidate

第一次满足阈值时进入 candidate，不立即落库为 event。

Candidate 字段：

| 字段 | 中文说明 |
|---|---|
| `first_seen` | 第一次满足阈值时间 |
| `last_seen` | 最近一次满足阈值时间 |
| `best_calc` | candidate 期间最好的计算结果 |
| `long_funding_window_key` | 做多侧 funding 窗口键，来自做多侧 latest |
| `short_funding_window_key` | 做空侧 funding 窗口键，来自做空侧 latest |
| `common_funding_window_key` | 两边 funding 时间足够接近时的共同窗口键；否则为空 |

Candidate 必须按 long / short 两边分别保存窗口键，不能只保存单个 `funding_window_key`。只要后续扫描中任一侧窗口键变化，就视为 funding window rollover：旧 candidate 立即失效并重新计时，不能继续累积 `funding.strategy_min_duration_ms`。

### 9.2 Active

满足以下条件后创建 active event：

1. `funding_rate_diff >= funding.strategy_min_rate_diff`
2. `one_window_net_observation >= funding.strategy_min_one_window_net_observation`
3. candidate 持续时间 `>= funding.strategy_min_duration_ms`
4. 当前计算结果的 `long_funding_window_key` 和 `short_funding_window_key` 与 candidate 中保存的窗口键一致
5. funding 数据新鲜度和 rate kind 检查通过
6. 如果创建 `capacity_checked` event，则盘口数据和容量检查必须通过

如果盘口未启用或盘口缺失但允许降级，只能创建 `rate_spread_only` event，并且：

| 字段 | 值 |
|---|---|
| `opportunity_grade` | `rate_spread_only` |
| `capacity_verified` | `false` |
| `estimated_capacity_notional` | null |
| `executable_base_qty` | null |

创建 active 时写：

| 写入内容 | 中文说明 |
|---|---|
| `funding_opportunity_events` | 创建 active event |
| `funding_opportunity_snapshots` open | 写 open 快照 |

### 9.3 Update

Active 持续期间：

| 行为 | 中文说明 |
|---|---|
| 更新 sample_count | 统计有效样本数量 |
| 更新 max / avg | 第一版更新 funding diff 和 one-window observation；carry return / capacity 字段可保留为空，仅后续等级启用 |
| 写 max snapshot | `one_window_net_observation` 或对应等级指标刷新最大值时写入 |
| 写 periodic snapshot | 按周期写复盘快照 |

### 9.4 Close

以下情况关闭：

| 条件 | 中文说明 |
|---|---|
| 收益低于阈值 | 机会不再满足策略要求 |
| funding 数据过期 | 数据源不可靠 |
| 盘口或 mark 数据过期 | 容量或 basis 无法估算 |
| funding 窗口变化 | 做多侧或做空侧任一 funding window key 已切换 |
| 服务退出 | 关闭所有 active |

关闭时：

1. 更新 event 为 `closed`。
2. 写 close snapshot。
3. close snapshot 中保留导致关闭的 invalid reason。

## 10. 配置设计

当前`config/config.dev.yaml`已经包含`funding`段。以下是default-off关键示例，不代表实时运行状态：

```yaml
funding:
  enabled: false
  enabled_venues:
    - binance_usdt_perp
    - okx_usdt_swap
  enabled_quote_assets:
    - USDT
  enabled_settle_assets:
    - USDT
  symbol_allowlist: []
  symbol_blocklist: []
  metadata_refresh_interval_sec: 3600
  funding_poll_interval_ms: 5000
  funding_latest_ttl_ms: 300000
  funding_observation_persist_interval_ms: 30000
  depth_enabled: false
  depth_level: 1
  depth_mode: adaptive_full_market
  binance_snapshot_min_interval_ms: 2000
  book_ttl_ms: 5000
  depth_buffer_max_updates: 4096
  settlement_sampling_enabled: false
  settlement_top100_max_pairs: 20
  settlement_depth_entry_min_rate_diff: "0.0001"
  settlement_depth_maintain_min_rate_diff: "0"
  settlement_sampler_tick_ms: 100
  settlement_sample_max_lateness_ms: 500
  max_book_age_ms: 5000
  scan_interval_ms: 1000
  strategy_min_rate_diff: "0.0001"
  strategy_min_one_window_net_observation: "0.00002"
  strategy_min_duration_ms: 30000
  strategy_min_executable_notional: "100"
  max_funding_data_age_ms: 300000
  max_funding_receive_diff_ms: 5000
  max_next_funding_time_diff_ms: 300000
  min_time_to_funding_ms: 60000
  max_time_to_funding_ms: 28800000
  max_abs_entry_mark_spread: "0.002"
  max_abs_entry_index_spread: "0.002"
  require_index_price: false
  slippage_buffer: "0.0002"
  basis_buffer: "0.0002"
  snapshot_periodic_interval_ms: 60000
  debug_raw_payload: false
  venue_taker_fee:
    binance_usdt_perp: "0.0004"
    okx_usdt_swap: "0.0005"
  rate_limit:
    max_requests_per_minute:
      binance_usdt_perp: 120
      okx_usdt_swap: 120
    max_concurrent_requests_per_venue: 2
    retry_max_attempts: 3
    retry_base_delay_ms: 500
    retry_max_delay_ms: 10000
    backoff_on_http_status:
      - 429
      - 418
      - 500
      - 502
      - 503
      - 504
```

正文引用嵌套配置时统一使用 `funding.xxx` 形式，例如 `funding.max_next_funding_time_diff_ms`。Go 配置加载建议使用 `yaml.Decoder.KnownFields(true)`，至少要用测试覆盖关键字段拼错、decimal 字段非法值、默认值加载、YAML 覆盖、allowlist / blocklist 过滤规则。

`funding.depth_mode` 目前支持：

* `fixed_allowlist`：保留独立的固定allowlist盘口runner；
* `adaptive_full_market`：名称为历史兼容。和`settlement_sampling_enabled=true`一起使用时启动固定结算窗口runtime及四个venue/depth Manager，不再启动adaptive controller。

库默认值仍为 `fixed_allowlist`；`config/config.dev.yaml` 暂存为 `adaptive_full_market`，并保持 `depth_enabled=false`、`settlement_sampling_enabled=false`，用于保留分阶段接入状态。

`settlement_sampling_enabled=false`时，独立funding opportunity模式可周期写`funding:*:latest`，`arb-opportunity`再从这些key构造双边扫描集合。`settlement_sampling_enabled=true`时，main跳过这个常驻funding poll，由窗口runtime只在窗口内维护funding/mark数据。

固定窗口runtime在`T-5`启动当前窗口funding/mark session、选择一次top1/top100并向四个Manager下发互斥目标；`T-3`到`T+2`的135点不改变深度，`T+2`清空目标。它不读取depth checkpoint，不做当前窗口恢复，也不依赖常驻funding poll。

`funding.binance_snapshot_min_interval_ms`供Binance depth100 snapshot路径使用，合法下限为2000ms。

字段中文说明：

| 配置字段 | 中文说明 |
|---|---|
| `enabled` | 是否启用 funding 扫描 |
| `enabled_venues` | 启用的永续交易所 |
| `enabled_quote_assets` | 启用的计价资产 |
| `enabled_settle_assets` | 启用的结算资产 |
| `symbol_allowlist` | 只扫描这些统一永续标的；为空时进入全局 USDT 永续观察模式，由 Redis latest 动态构造扫描 universe |
| `symbol_blocklist` | 禁止扫描的统一永续标的；优先级高于白名单 |
| `metadata_refresh_interval_sec` | 元数据刷新间隔 |
| `funding_poll_interval_ms` | funding 数据轮询间隔 |
| `funding_latest_ttl_ms` | Redis funding latest TTL |
| `funding_observation_persist_interval_ms` | funding 原始观测落库间隔 |
| `depth_enabled` | 是否启动perp-book runtime；默认false |
| `depth_level` | 独立fixed allowlist模式的目标深度；固定窗口模式按每币选择top1或top100 |
| `depth_mode` | `adaptive_full_market`是兼容名称；settlement sampling下表示固定窗口Manager runtime，不表示adaptive controller |
| `settlement_top100_max_pairs` | 一个窗口最多选择多少个top100币种；默认20，可配置为其他正整数 |
| `settlement_depth_entry_min_rate_diff` | 上一完整窗口为top1或无状态时的进入阈值；严格`>`才合格 |
| `settlement_depth_maintain_min_rate_diff` | 上一完整窗口为top100时的维持阈值；严格`>`才合格 |
| `binance_snapshot_min_interval_ms` | Binance depth100 snapshot最小间隔，合法下限2000ms |
| `book_ttl_ms` | 永续盘口 Redis TTL |
| `depth_buffer_max_updates` | Binance snapshot前增量buffer最大条数，必须为正 |
| `settlement_sampling_enabled` | 是否启动固定结算窗口runtime；默认false；真实运行需要明确授权 |
| `max_book_age_ms` | 盘口最大允许年龄 |
| `scan_interval_ms` | funding scanner 扫描间隔 |
| `strategy_min_rate_diff` | 最小资金费率差 |
| `strategy_min_one_window_net_observation` | 单窗口观测净值阈值，只扣 slippage / basis buffer，不扣完整开平仓手续费 |
| `strategy_min_duration_ms` | 机会必须持续多久才创建 active event |
| `strategy_min_executable_notional` | 最小可执行名义金额 |
| `max_funding_data_age_ms` | funding 数据最大允许年龄 |
| `max_funding_receive_diff_ms` | 两边 funding 数据接收时间最大差异 |
| `max_next_funding_time_diff_ms` | 两边下一次 funding 结算时间最大差异 |
| `min_time_to_funding_ms` | 距离 funding 结算的最短允许时间 |
| `max_time_to_funding_ms` | 距离 funding 结算的最长允许时间 |
| `max_abs_entry_mark_spread` | 两边 mark price 最大允许绝对偏离 |
| `max_abs_entry_index_spread` | 两边 index price 最大允许绝对偏离 |
| `require_index_price` | 是否强制要求 index price 存在 |
| `slippage_buffer` | 滑点缓冲 |
| `basis_buffer` | basis 风险缓冲 |
| `snapshot_periodic_interval_ms` | active 机会周期快照间隔 |
| `debug_raw_payload` | 是否保存原始 payload |
| `venue_taker_fee` | 各 venue 的 taker fee 假设 |
| `rate_limit.max_requests_per_minute` | 每个 venue 每分钟最大请求数 |
| `rate_limit.max_concurrent_requests_per_venue` | 每个 venue 最大并发请求数 |
| `rate_limit.retry_max_attempts` | 单次请求最大重试次数 |
| `rate_limit.retry_base_delay_ms` | 重试初始退避时间 |
| `rate_limit.retry_max_delay_ms` | 重试最大退避时间 |
| `rate_limit.backoff_on_http_status` | 触发退避的 HTTP 状态码 |

## 11. Provider 设计

### 11.1 Provider 接口

第一版必须新增 funding provider 接口：

| 方法 | 中文说明 |
|---|---|
| `VenueCode()` | 返回 venue code |
| `FetchPerpInstruments(ctx)` | 拉取永续合约元数据 |
| `FetchFundingRates(ctx, instruments)` | 拉取 funding latest 数据 |

Phase 5C盘口runtime边界：

当前perp-book runtime没有把`RunPerpBooks`加入funding provider接口；`arb-market-data/perp_book_runner.go`使用现有metadata provider构造Binance/OKX public runtime。固定窗口runtime在这个边界上复用shared transport、Binance snapshot queue和四个target Manager，没有新增重复的provider方法。

Provider 输出必须完成：

1. 原始 symbol 到 normalized symbol 的转换。
2. 保留`quantity_unit`和contract value系列metadata；Phase 5A/5C已在构造`PerpBookLatest`时把每档`source_quantity`精确归一化为`base_quantity`。V1 funding opportunity仍不据此创建`capacity_checked` event。
3. decimal 字符串解析。
4. 原始 payload hash。
5. debug 开关控制 raw payload 是否保留。

Provider 运行规则：

1. REST API 支持批量拉取时优先批量拉取，不按 symbol 逐个请求。
2. 每个 venue 必须经过独立的 `funding.rate_limit` 限频器，不能用全局锁阻塞其他 venue 或现货 market-data。
3. 429、418、5xx 等状态码触发指数退避。
4. metadata refresh 失败不清空旧 universe，除非旧 universe 已过 TTL 或配置显式禁用。
5. 某个 venue funding 拉取失败时，只标记该 venue stale，不影响其他 venue 的 latest 数据。
6. 当前第一版最小实现把 funding provider 状态写入 `health:service:arb-market-data` 的 `funding_*` 字段；`health:funding:summary` 属于后续完整 health summary 设计，不作为第一版运行验证阻断。
7. funding 轮询不得阻塞现有 spot orderbook 写入；实现上使用独立 goroutine、bounded queue 或互斥很短的 latest 状态更新。

来源项目的代码结构遵守其代码地图中的 `src/arb-core/strategies/` 规则。第一版 `ARB-0002` 策略专属逻辑默认放在：

```text
src/arb-core/strategies/arb0002/
```

第一版建议文件：

```text
src/arb-core/strategies/arb0002/model.go
src/arb-core/strategies/arb0002/engine.go
src/arb-core/strategies/arb0002/scanner.go
src/arb-core/strategies/arb0002/redis_keys.go
src/arb-core/strategies/arb0002/store.go
src/arb-core/strategies/arb0002/providers_binance.go
src/arb-core/strategies/arb0002/providers_okx.go
src/arb-core/strategies/arb0002/invalid_reason.go
src/arb-core/strategies/arb0002/*_test.go
```

不要新增以下顶层 funding domain：

```text
src/arb-core/fundingmodel
src/arb-core/fundingproviders
src/arb-core/fundingengine
src/arb-core/fundingstate
src/arb-core/fundingstore
```

如果 provider / state / store 后续确实需要拆子包，也应先拆在策略目录内部，例如：

```text
src/arb-core/strategies/arb0002/providers
src/arb-core/strategies/arb0002/state
src/arb-core/strategies/arb0002/store
```

第一版纯模型、纯计算、invalid reason 和测试优先保持在 `arb0002` 包内，避免过早拆分。只有当某段 funding 能力被两个及以上策略复用时，才能作为单独重构任务提取到通用 domain，并同步更新 `docs/development/code-map.md`。

运行时可以仍在 `arb-market-data` 和 `arb-opportunity` 两个服务中启动，但 health 和日志必须按 spot / funding 分域展示。

### 11.2 Binance USDT-M Provider

第一版需要的数据：

| 数据 | 中文说明 |
|---|---|
| 合约元数据 | 交易对状态、tick size、step size、最小名义金额、合约类型 |
| funding rate | 当前资金费率和下一次 funding 时间 |
| mark price | 标记价格、指数价格 |

已实现但default-off的Phase 5C数据：

| 数据 | 中文说明 |
|---|---|
| depth | Binance public diff + snapshot维护depth1/100；窗口runtime通过Manager下发本窗口固定目标，depth100复用统一WS API snapshot queue/worker bridge |

注意：

1. Binance 原始 symbol 常见为 `BTCUSDT`。
2. Provider 归一化为 `BTC-USDT-PERP`。
3. 第一版只接 USDT-M linear contract。

### 11.3 OKX USDT Swap Provider

第一版需要的数据：

| 数据 | 中文说明 |
|---|---|
| 合约元数据 | instrument 状态、contract value、tick size、lot size、settle currency |
| funding rate | 当前或预测资金费率、下一次 funding 时间 |
| mark price | 标记价格、指数价格 |

已实现但default-off的Phase 5C数据：

| 数据 | 中文说明 |
|---|---|
| depth | OKX public books/bbo维护depth1/100；窗口runtime通过Manager下发本窗口固定目标，并复用worker/shared transport、native snapshot recovery和shared rolling pacer |

注意：

1. OKX 原始 symbol 常见为 `BTC-USDT-SWAP`。
2. Provider 归一化为 `BTC-USDT-PERP`。
3. 第一版只支持 USDT linear swap。
4. 当前Phase 5C处理OKX depth时，盘口数量可能是合约张数，必须在构造ready latest前用合约面值精确转换为base quantity。

OKX已实现的book quantity归一化，以及后续capacity复用边界：

```text
if quantity_unit == 'base':
  base_qty = raw_qty

if quantity_unit == 'contract' and contract_value_currency == base_asset:
  base_qty = contracts * contract_value * contract_multiplier

otherwise:
  reject instrument as unsupported
```

如果`ctVal`、`ctMult`、`ctValCcy`、`settleCcy`无法证明上述转换，当前Phase 5A/5C contract必须fail closed，不能生成合法ready `PerpBookLatest`。V1 funding-only scanner仍可在depth disabled时输出`rate_spread_only`，但不得据此创建`capacity_checked` event；尚未实现的是capacity event wiring，不是book quantity归一化。

## 12. Engine 设计

funding engine 必须作为 `ARB-0002` 策略专属纯计算逻辑实现，默认放在 `src/arb-core/strategies/arb0002/engine.go`。不要把 funding 逻辑混入现货 `engine.EvaluateBooks`，也不要新增顶层 `src/arb-core/fundingengine` 包。

### 12.1 输入结构 `FundingEvaluateInput`

| 字段 | 中文说明 |
|---|---|
| `LongFunding` | 做多侧 funding latest |
| `ShortFunding` | 做空侧 funding latest |
| `LongBook` | 做多侧永续盘口；后续 `capacity_checked` 阶段使用，第一版为空 |
| `ShortBook` | 做空侧永续盘口；后续 `capacity_checked` 阶段使用，第一版为空 |
| `ComputedAt` | 本次计算时间 |
| `DepthLevel` | 盘口档位；第一版保留默认值但不启用 |
| `LongFeeRate` | 做多侧开仓 taker fee |
| `ShortFeeRate` | 做空侧开仓 taker fee |
| `CloseFeeRate` | 两边平仓 fee 假设，或按 venue 分开传入 |
| `SlippageBuffer` | 滑点缓冲 |
| `BasisBuffer` | basis 风险缓冲 |
| `MaxFundingDataAge` | funding 最大数据年龄 |
| `MaxFundingReceiveDiff` | funding 本地接收时间最大差异 |
| `MaxBookAge` | 盘口最大数据年龄；第一版不参与失败判断 |
| `MaxNextFundingTimeDiff` | 下一次结算时间最大差异 |
| `MinTimeToFunding` | 距离结算的最短允许时间 |
| `MaxTimeToFunding` | 距离结算的最长允许时间 |
| `MinRateDiff` | 最小资金费率差 |
| `MinOneWindowNetObservation` | 单窗口观测净值阈值 |
| `MinExecutableNotional` | 最小可执行名义金额；仅后续 `capacity_checked` 阶段生效 |
| `MaxAbsEntryMarkSpread` | 最大 mark price 偏离 |
| `MaxAbsEntryIndexSpread` | 最大 index price 偏离 |
| `RequireIndexPrice` | 是否强制要求 index price |

第一版 `funding.depth_enabled=false` 时：

- `LongBook` / `ShortBook` 必须为空。
- `DepthLevel`、`MaxBookAge`、`MinExecutableNotional` 可以保留配置默认值，但 engine 不得因为缺少盘口而失败。
- 缺少永续盘口不得生成 `missing_long_book`、`missing_short_book`、`stale_long_book`、`stale_short_book` 等 invalid reason。

### 12.2 输出结构 `FundingOpportunityCalculation`

| 字段 | 中文说明 |
|---|---|
| `FundingRateDiff` | 资金费率差 |
| `GrossFundingDiff` | 原始资金费率差 |
| `OneWindowNetObservation` | 单窗口观测净值 |
| `ExpectedHoldWindows` | 多窗口 carry 模型假设持有窗口数；第一版可为空 |
| `CarryNetReturnAfterNWindows` | 多窗口摊销后的纸面净收益；后续 E3 使用，第一版可为空 |
| `BreakEvenWindows` | 覆盖开平仓成本所需 funding 窗口数；后续 E3 使用，第一版可为空 |
| `EstimatedFeeCost` | 预计手续费成本；仅用于后续 carry / break-even，第一版可为空 |
| `SlippageBuffer` | 滑点缓冲 |
| `BasisBuffer` | basis 风险缓冲 |
| `ExecutableBaseQty` | 可执行基础资产数量；后续 `capacity_checked` 阶段使用，第一版必须为空 |
| `EstimatedCapacityNotional` | 可执行名义金额；后续 `capacity_checked` 阶段使用，第一版必须为空 |
| `EntryMarkSpread` | 两边 mark price 相对偏离 |
| `EntryIndexSpread` | 两边 index price 相对偏离 |
| `TimeToFundingMS` | 距离 funding 结算的毫秒数 |
| `FundingTimeDiffMS` | 两边 funding 结算时间差 |
| `LongFundingWindowKey` | 做多侧 funding 窗口键 |
| `ShortFundingWindowKey` | 做空侧 funding 窗口键 |
| `CommonFundingWindowKey` | 两边 funding 时间足够接近时的共同窗口键；否则为空 |
| `LongFundingAgeMS` | 做多侧 funding 数据年龄 |
| `ShortFundingAgeMS` | 做空侧 funding 数据年龄 |
| `LongBookAgeMS` | 做多侧盘口年龄；第一版必须为空 |
| `ShortBookAgeMS` | 做空侧盘口年龄；第一版必须为空 |
| `CalculationValid` | 计算是否有效 |
| `InvalidReason` | 无效原因 |

第一版 `funding.depth_enabled=false` 时：

- `ExpectedHoldWindows` / `CarryNetReturnAfterNWindows` / `BreakEvenWindows` / `EstimatedFeeCost` 可以为 null，且不得参与 candidate 触发。
- `ExecutableBaseQty` / `EstimatedCapacityNotional` 必须为 null。
- `LongBookAgeMS` / `ShortBookAgeMS` 必须为 null。
- `CalculationValid` 不得因为缺少 `LongBook` / `ShortBook` 而失败。

### 12.3 方向生成

Scanner 可以对每对 venue 生成两个方向，也可以直接按 funding rate 排序生成最佳方向。

推荐第一版：

```text
对 venue A / venue B：
1. 计算 A long + B short
2. 计算 B long + A short
3. 两个方向都经过 engine
4. 第一版只有 `one_window_net_observation` 通过阈值的方向进入 candidate；carry return 只在后续 E3 / `paper_pnl_checked` 阶段使用
```

这样可以保持和现有 spot opportunity scanner 一样的“有向组合”思路。

## 13. Health 和 Doctor 设计

### 13.1 Redis Health Key

当前第一版最小实现沿用现有 service health hash，并在其中暴露 funding 子字段：

```text
health:service:arb-market-data
health:service:arb-opportunity
```

当前 `arb-market-data` funding provider loop 写入 / 暴露的最小字段包括：

| 字段 | 中文说明 |
|---|---|
| `funding_enabled` | funding 配置是否启用 |
| `funding_poll_loop_enabled` | 常驻funding provider poll是否启用；固定结算窗口模式必须false |
| `funding_last_poll_ts` | 最近一次成功 poll 时间 |
| `funding_last_poll_duration_ms` | 最近一次 poll 耗时 |
| `funding_latest_write_count_total` | funding latest 累计写入数量 |
| `funding_last_error` | 最近一次 funding provider / Redis 写入错误摘要 |
| `funding_last_successful_venues` | 最近一次成功 venue 列表 |
| `funding_unsupported_venues` | 配置中当前不支持的 funding venue |

当前 `arb-opportunity` funding scanner loop 写入 / 暴露的最小字段包括：

| 字段 | 中文说明 |
|---|---|
| `funding_enabled` | funding 配置是否启用 |
| `funding_scan_loop_enabled` | funding scan loop 是否启用 |
| `funding_last_scan_ts` | 最近一次成功 scan 时间 |
| `funding_last_scan_duration_ms` | 最近一次 scan 耗时 |
| `funding_last_scan_valid_candidates_count` | 最近一次 scan 通过阈值的候选数量；0 不代表故障 |
| `funding_last_scan_error` | 最近一次 funding scanner 错误摘要 |

后续完整 health 设计可新增专用 summary key：

```text
health:venue:{venue_code}
health:service:arb-market-data
health:service:arb-opportunity
health:funding:summary
```

`health:funding:summary` 当前第一版未实现，不作为 dev Doctor funding 验证阻断。后续若实现，建议字段如下：

| 字段 | 中文说明 |
|---|---|
| `symbols_total` | funding universe 中的 symbol 数量 |
| `venues_total` | funding universe 中的 venue 数量 |
| `expected_funding_count` | 理论应该存在的 funding latest 数量 |
| `fresh_funding_count` | 新鲜 funding latest 数量 |
| `missing_funding_count` | 缺失 funding latest 数量 |
| `stale_funding_count` | 过期 funding latest 数量 |
| `expected_book_count` | 理论应该存在的永续盘口数量；第一版可省略或为 0 |
| `fresh_book_count` | 新鲜永续盘口数量；第一版可省略或为 0 |
| `missing_book_count` | 缺失永续盘口数量；第一版可省略或为 0 |
| `stale_book_count` | 过期永续盘口数量；第一版可省略或为 0 |
| `symbols_with_all_venues_fresh` | 所有 venue 数据都新鲜的 symbol 数量 |
| `oldest_funding_age_ms` | 最旧 funding 数据年龄 |
| `oldest_book_age_ms` | 最旧盘口数据年龄；第一版可省略或为 null |
| `last_scan_ts` | funding scanner 最近扫描时间 |
| `last_scan_expected_pairs_count` | 最近一轮理论方向组合数 |
| `last_scan_pairs_evaluated_count` | 最近一轮实际评估组合数 |
| `last_scan_valid_candidates_count` | 最近一轮通过阈值候选数 |
| `last_scan_max_funding_rate_diff` | 最近一轮最大资金费率差 |
| `last_scan_top_invalid_reason` | 最近一轮最多的无效原因 |
| `updated_at` | summary 更新时间 |

第一版 `funding.depth_enabled=false` 时：

- book 相关 summary 字段可以省略，或固定为 0 / null。
- `funding.books_freshness` 必须是 `skipped`。
- Doctor 不得因为缺少永续盘口 book 而输出 warning / error。
- depth disabled时不要求存在`md:{venue_code}:{normalized_symbol}:book:{1|100}` Redis key。

### 13.2 Doctor 新增检查

当前第一版最小 `arb-doctor` funding checks 已实现以下 ID；dev 运行验证应以这些 ID 为准：

| 检查 ID | 中文说明 |
|---|---|
| `funding.config_enabled` | funding 配置是否启用；disabled 时跳过下游 funding checks |
| `funding.market_data_health` | `arb-market-data` service health 中 funding provider poll loop 是否启用、新鲜且无错误 |
| `funding.opportunity_health` | `arb-opportunity` service health 中 funding scanner loop 是否启用、新鲜且无错误 |
| `funding.redis_latest_keys` | 根据当前 funding config 推导的有限 expected latest keys 是否存在、新鲜、JSON 可解析且字段匹配 |
| `funding.scan_summary_recent` | PostgreSQL 中是否有最近 `funding_scan_summaries` 行；无机会时也可证明 scanner 已运行 |
| `funding.opportunity_tables` | funding opportunity event / snapshot 表是否可查询；0 events / snapshots 不代表故障 |

后续完整 Doctor 设计可在当前最小 ID 之上新增或重命名更细粒度检查。以下 ID 是后续设计目标，不是当前第一版 dev 验证阻断项：

| 后续检查 ID | 中文说明 |
|---|---|
| `funding.config` | funding 配置是否可加载且字段完整 |
| `funding.metadata_ready` | Redis funding metadata 是否准备好 |
| `funding.universe_symbols` | funding universe symbol 是否非空 |
| `funding.universe_venues` | funding universe venue 是否不少于 2 个 |
| `funding.latest_freshness` | funding latest 是否齐全且新鲜 |
| `funding.books_freshness` | 永续盘口是否齐全且新鲜；如果 depth disabled 则 skipped |
| `funding.scan_summary` | PostgreSQL 中是否有最近 scan summary，可用于无机会复盘 |
| `funding.recent_db_activity` | funding event / snapshot 表是否可查询 |
| `funding.active_consistency` | active event 是否和内存健康字段一致 |
| `funding.snapshot_integrity` | funding snapshot 是否存在孤儿、缺字段、JSON 异常 |

Doctor 结论规则：

| 状态 | 中文说明 |
|---|---|
| `ok` | 数据齐全、服务扫描正常 |
| `warning` | 数据缺失、过期、无近期扫描或快照轻微异常 |
| `error` | Postgres / Redis / 配置不可用，或服务完全不扫描 |

## 14. 测试计划

### 14.1 Unit Tests

第一版必须测试：

| 测试 | 中文说明 |
|---|---|
| funding rate 正负号测试 | 验证 long 低 funding、short 高 funding 的收益方向 |
| 收益分层测试 | 只验证 gross diff 和 one-window observation；不验证 carry net return / break-even windows |
| rate kind 测试 | 验证已结算或 unknown funding rate 不能触发 candidate |
| funding 时间对齐测试 | 验证 next funding time 差异过大时无效 |
| funding 数据过期测试 | 验证 stale funding 被拒绝 |
| mark/index 偏离测试 | 验证 entry spread 过大时无效 |
| invalid reason 映射测试 | 验证无效原因到 close reason 的映射 |
| 配置 strict 解析测试 | 验证默认值、YAML 覆盖、decimal 非法值、未知字段和 allow/block 规则 |
| 第一版后续字段禁用测试 | 验证 carry / break-even 字段为空或不参与 candidate，capacity 字段为空且 `capacity_verified=false` |
| 第一版缺盘口不失败测试 | 验证 `depth_enabled=false` 时缺少 `LongBook` / `ShortBook` 不导致 calculation invalid |

后续 `capacity_checked` 阶段测试：

| 测试 | 中文说明 |
|---|---|
| 容量估算测试 | 验证使用共同可执行数量和加权价格 |
| 合约张数转换测试 | 验证 OKX 等张数盘口能转换为基础资产数量 |
| book freshness 测试 | 验证 missing / stale book 在启用 depth 后才触发 warning、invalid 或 skipped 以外状态 |

后续 E3 / `paper_pnl_checked` 阶段测试：

| 测试 | 中文说明 |
|---|---|
| carry net return 公式测试 | 验证多窗口摊销后的纸面净收益 |
| break-even windows 公式测试 | 验证覆盖开平仓成本所需 funding 窗口数 |
| 结算后回放测试 | 验证预测 funding 与结算后 funding 的差异和 paper PnL 回放 |

### 14.2 Provider Parse Tests

| 测试 | 中文说明 |
|---|---|
| Binance 元数据解析 | 验证 raw symbol、tick、step、min notional、状态解析 |
| OKX 元数据解析 | 验证 ctVal、lot size、settle asset、状态解析 |
| Binance funding 解析 | 验证 funding rate、next funding time、mark/index 解析 |
| OKX funding 解析 | 验证 funding rate、next funding time、mark/index 解析 |
| debug raw payload 测试 | 验证 debug=false 时不保存 raw JSON，但保存 hash |

### 14.3 Integration Tests

| 测试 | 中文说明 |
|---|---|
| fake funding injector open/max/close | 注入虚假 funding 错位，验证 event 和 snapshot |
| Redis latest TTL | 验证 funding latest 过期后 scanner 关闭 active |
| 无机会 scan summary | 注入低于阈值的 pair，验证 scan summary 记录最大 diff 和 invalid counts |
| migration checksum | 验证新增 migration 可重复执行且 checksum 稳定 |
| doctor funding checks | 验证 funding health 在正常和异常情况下输出正确 recommendation |

## 15. 实现里程碑

AI 执行约束：以下阶段是路线图，不代表单次任务范围。默认一次只做一个可 review 的子任务；不要一次完成整个阶段，除非用户明确要求。

进度说明：本节保留原始分阶段路线图和后续完整目标。实际已完成小块、未验证事项和下一步接力任务以 `docs/ai/current-state.md` 为准；如果本节某个阶段交付项已经被拆分为更小实现，后续 AI 不应因为路线图仍列出完整目标而扩大当前任务范围。

### 阶段一：数据结构和配置

阶段一拆分为以下可独立 review 的子任务：

| 子任务 | 交付 |
|---|---|
| 阶段一 A：funding config | funding 配置结构、默认值、配置解析测试 |
| 阶段一 B：funding / perp model | funding latest、metadata、calculation、invalid reason 等 model |
| 阶段一 C：migration skeleton | 新增表的 migration 初稿和数据字典草稿 |
| 阶段一 D：Redis funding latest store | Redis funding latest key 的读写和 TTL 测试 |
| 阶段一 E：funding engine unit tests | funding diff、one-window observation、rate kind、时间窗口测试 |

整阶段交付（完成 A-E 后）：

1. 新增 funding 配置结构。
2. 新增 funding / perp model。
3. 新增 PostgreSQL migration。
4. 新增 Redis funding latest 读写方法。
5. 新增 `funding_scan_summaries` schema。
6. 新增 funding engine 单元测试。

整阶段完成标准（完成 A-E 后）：

| 标准 | 中文说明 |
|---|---|
| `go test ./...` 通过 | 不破坏现有现货扫描逻辑 |
| migration 可执行 | 新表创建成功 |
| funding engine 覆盖核心公式 | 正负 funding 和成本模型无歧义 |
| scan summary schema 可查询 | 无机会时也有复盘载体 |

### 阶段二：Provider 和 Redis 写入

交付：

1. Binance USDT-M funding provider。
2. OKX USDT swap funding provider。
3. funding universe 生成。
4. Redis funding latest 写入。
5. `funding_rate_observations` 低频落库。
6. funding summary health 写入。

完成标准：

| 标准 | 中文说明 |
|---|---|
| Redis 中 5 个 allowlist symbol 都有 funding latest | 数据链路跑通 |
| funding latest TTL 正常刷新 | 服务持续抓取 |
| metadata 写入 Postgres | 可复盘合约元数据 |
| observations 覆盖至少一个 funding window | 可开始验证预测值和结算值关系 |

### 阶段三：Funding Scanner

交付：

1. `arb-opportunity` 增加 funding scanner。
2. candidate / active / close 生命周期。
3. funding event / snapshot 写入。
4. funding scan summary 写入。
5. fake funding injector。

完成标准：

| 标准 | 中文说明 |
|---|---|
| fake funding 可触发 open / max / close | 写入链路可验证 |
| 真实市场可持续扫描 | 没机会也能输出健康扫描状态 |
| 无机会时 scan summary 有最大 diff 和 invalid counts | E2 无机会证明成立 |
| snapshot 包含完整中文字段设计对应数据 | 后续可复盘 |

### 阶段四：Doctor 和文档

交付：

1. Doctor 增加 funding 检查。
2. README / runbook 增加 funding 启停和查询命令。
3. 数据字典增加新增表说明。

完成标准：

| 标准 | 中文说明 |
|---|---|
| `arb-doctor` 能区分 funding 数据缺失和无机会 | 运维判断清晰 |
| runbook 能指导启动和查询 | 后续开发不依赖口头记忆 |

## 16. 第一版不做的内容

| 不做事项 | 中文说明 |
|---|---|
| 真实下单 | 不连接私有 API，不放 API key |
| 账户余额 | 不读取真实余额，不计算实际仓位 |
| 强平价 | 不计算真实强平价，只记录 mark/index 和风险提示 |
| 保证金优化 | 不做跨平台资金调拨和保证金占用优化 |
| maker 成交 | 不假设 maker rebate 或 maker 成交概率 |
| 多交易所净额调仓 | 不做仓位再平衡 |
| 自动告警交易 | 可以后续告警，但第一版不触发交易动作 |

## 17. E2 验证标准

当满足以下条件时，`ARB-0002` 可以从 `E1` 标记为 `E2`：

1. 至少两个真实 venue 的 funding latest 能稳定抓取。
2. 至少 5 个主流 symbol 进入 funding universe。
3. Redis funding latest 新鲜度可由 doctor 验证。
4. Funding scanner 持续运行，并写入 `funding_scan_summaries`。
5. PostgreSQL 中存在 `funding_rate_observations`，覆盖至少一个完整 funding window。
6. 若存在机会，写入 funding event / snapshot；若不存在机会，scan summary 能证明所有 pair 的失败原因。
7. 每条 opportunity snapshot 都包含 rate kind、funding time、手续费假设、buffer、one-window observation、容量验证状态和 invalid reason；carry return / capacity 字段如未启用必须为 null 或明确 false。
8. 如果第一版 `funding.depth_enabled=false`，E2 只升级到 `rate_spread_only`；容量验证和 `capacity_checked` 是下一阶段目标。
9. 文档明确声明该结果是只读观察、纸面机会、非交易信号。

## 18. 风险边界

即使扫描到正的 one-window observation，也只能说明“资金费率错位可观测”，不能说明已经锁定收益。主要风险：

| 风险 | 中文说明 |
|---|---|
| funding 反转 | 下一次结算前 funding rate 可能变化 |
| 结算时间错位 | 一边先结算，另一边后结算，导致收益窗口不一致 |
| mark/index 差异 | 两个平台价格指数不同，可能产生非中性风险 |
| 开平仓滑点 | 盘口深度不足会吞噬 funding 收益 |
| 单边流动性消失 | 实盘时一边成交另一边失败 |
| 交易所风控 | 限频、限仓、维护、撮合异常 |
| 保证金风险 | 实盘需要保证金和强平模型，第一版不覆盖 |

因此第一版输出统一标记为：

```text
只读观察 / 纸面机会 / 非交易信号
```
