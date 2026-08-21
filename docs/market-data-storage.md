# 市场数据存储数据字典

本数据字典定义当前阶段的六个核心表：交易标的表 `instrument`、分钟完整盘口表 `order_book_minute`、分钟内秒级差量表 `order_book_second_delta`、整点资金费率表 `funding_rate_hourly`、收益路线表 `yield_route` 和收益观测表 `yield_observation`。

这些表在整个采集进程中的位置及以后增加其他数据时的扩展原则见[系统总体架构](architecture.md)。

所有时间均为 UTC。盘口快照和差量中的价格、数量均为整数：价格使用 `price_tick`，数量使用 `qty_lot`；不得使用字符串价格或二进制浮点数。用于定义换算单位的元数据使用十进制定点数 `Decimal`。

`instrument_id` 是交易流的唯一标识，必须区分交易所、市场类型、标的、结算币种及合约版本。例如，Binance 现货 `BTCUSDT`、Binance U 本位永续 `BTCUSDT`、OKX 现货 `BTC-USDT` 和 OKX 永续 `BTC-USDT-SWAP` 必须使用不同的 `instrument_id`。其定义保存在 `instrument` 表中，不在各事实表中重复保存。

## 1. 交易标的表

表名：`instrument`

用途：保存交易所产品代码与统一交易流 ID 的映射，以及还原整数价格、整数数量和分析现货/合约价差所需的最少元数据。

主键：`instrument_id`

| 字段名 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `instrument_id` | 交易标的 ID | `UInt32` | 是 | 交易流的唯一标识。交易所产品定义、结算方式或合约版本发生变化时，分配新的 ID。 |
| `exchange` | 交易所 | `String` | 是 | 交易场所，例如 `Binance`、`OKX`。 |
| `market_type` | 市场类型 | `String` | 是 | 当前取值为 `spot`（现货）、`perpetual`（永续）或 `delivery`（交割合约）。 |
| `exchange_symbol` | 交易所产品代码 | `String` | 是 | 交易所原始产品代码，例如 `BTCUSDT`、`BTC-USDT-SWAP`。 |
| `base_asset` | 基础币种 | `String` | 是 | 被交易的资产，例如 BTC/USDT 中的 BTC。 |
| `quote_asset` | 报价币种 | `String` | 是 | 表示价格使用什么资产计价，例如 BTC/USDT 中的 USDT。 |
| `settle_asset` | 结算币种 | `Nullable(String)` | 否 | 保证金、盈亏和资金费使用的资产。现货为空；USDT 本位合约通常为 USDT；币本位合约可能与报价币种不同。 |
| `contract_multiplier` | 合约乘数 | `Decimal` | 是 | 合约数量换算参数；现货固定为 `1`。跨市场比较数量时，查询端结合该值及合约类型换算为统一基础币数量。 |
| `price_tick_size` | 最小价格单位 | `Decimal` | 是 | 一个 `price_tick` 对应的实际价格，用于还原盘口价格，并支持 tick 离散规则分析。不得使用二进制浮点数。 |
| `quantity_step_size` | 最小数量单位 | `Decimal` | 是 | 一个 `qty_lot` 对应的实际数量，用于还原盘口数量，并支持 lot 离散规则分析。不得使用二进制浮点数。 |
| `expiry_time` | 到期时间 | `Nullable(DateTime)` | 否 | 交割合约的 UTC 到期时间；现货和永续为空。用于计算剩余期限和年化基差。 |

`quote_asset` 与 `settle_asset` 表达不同含义。例如，BTC 币本位合约可以使用 USD 报价，但使用 BTC 作为保证金和盈亏结算资产，因此其 `quote_asset` 为 USD、`settle_asset` 为 BTC。

本表不维护启用状态、创建时间、更新时间或生效区间。若上述交易定义发生变化，应创建新的 `instrument_id`，避免用新规则解释旧行情。

## 2. 分钟完整盘口表

表名：`order_book_minute`

用途：保存每个交易流在每分钟第 0 秒的买卖各 50 档完整盘口。该记录是恢复本分钟内任意秒盘口的起点。

唯一键：`(instrument_id, minute_time)`

| 字段名 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `id` | 分钟盘口 ID | `UInt64` | 是 | 分钟完整盘口的唯一 ID，供差量表关联。按照下方公式由 `instrument_id` 与 UTC 分钟序号确定性生成。 |
| `instrument_id` | 交易流 ID | `UInt32` | 是 | 唯一标识交易所、市场类型、标的、结算币种及合约版本。 |
| `minute_time` | 分钟时间 | `DateTime` | 是 | UTC 分钟起始时间，秒和毫秒固定为 0。 |
| `valid_bitmap` | 秒有效位图 | `UInt64` | 是 | 低 60 位分别代表本分钟第 0 至 59 秒是否有有效盘口。位值为 `1` 表示有效，`0` 表示无效或缺失。 |

分钟 ID 的生成公式固定为：

```text
unix_minute = floor(UTC Unix 时间戳 / 60)
id = (unix_minute << 32) | instrument_id
```

`instrument_id` 占低 32 位，UTC 分钟序号占高 32 位。该公式也允许 ClickHouse 从 `minute_id` 推导月份，用于差量表按月分区。

### 买盘字段

| 字段范围 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `bid_price_01` | 买 1 价 | `Int64` | 是 | 本分钟第 0 秒的最高买价，保存为 `price_tick`。 |
| `bid_qty_01` | 买 1 量 | `UInt64` | 是 | 买 1 价对应的数量，保存为 `qty_lot`。 |
| `bid_price_02` | 买 2 价 | `Int64` | 是 | 第二高买价，保存为 `price_tick`。 |
| `bid_qty_02` | 买 2 量 | `UInt64` | 是 | 买 2 价对应的数量，保存为 `qty_lot`。 |
| `……` | 买 3 价、买 3 量……买 49 价、买 49 量 | `Int64` / `UInt64` | 是 | 命名规则分别为 `bid_price_03`、`bid_qty_03`……`bid_price_49`、`bid_qty_49`；含义与买 1 价、买 1 量相同。 |
| `bid_price_50` | 买 50 价 | `Int64` | 是 | 第五十高买价，保存为 `price_tick`。 |
| `bid_qty_50` | 买 50 量 | `UInt64` | 是 | 买 50 价对应的数量，保存为 `qty_lot`。 |

买盘的有效价格必须满足：

```text
bid_price_01 > bid_price_02 > …… > bid_price_50
```

### 卖盘字段

| 字段范围 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `ask_price_01` | 卖 1 价 | `Int64` | 是 | 本分钟第 0 秒的最低卖价，保存为 `price_tick`。 |
| `ask_qty_01` | 卖 1 量 | `UInt64` | 是 | 卖 1 价对应的数量，保存为 `qty_lot`。 |
| `ask_price_02` | 卖 2 价 | `Int64` | 是 | 第二低卖价，保存为 `price_tick`。 |
| `ask_qty_02` | 卖 2 量 | `UInt64` | 是 | 卖 2 价对应的数量，保存为 `qty_lot`。 |
| `……` | 卖 3 价、卖 3 量……卖 49 价、卖 49 量 | `Int64` / `UInt64` | 是 | 命名规则分别为 `ask_price_03`、`ask_qty_03`……`ask_price_49`、`ask_qty_49`；含义与卖 1 价、卖 1 量相同。 |
| `ask_price_50` | 卖 50 价 | `Int64` | 是 | 第五十低卖价，保存为 `price_tick`。 |
| `ask_qty_50` | 卖 50 量 | `UInt64` | 是 | 卖 50 价对应的数量，保存为 `qty_lot`。 |

卖盘的有效价格必须满足：

```text
ask_price_01 < ask_price_02 < …… < ask_price_50
```

如果某一侧不足 50 档，从第一个缺失档开始，该档及后续档位的价格和数量一律为 `0`。

`price_tick` 以交易流定义的最小价格单位换算：

```text
price_tick = 实际价格 / price_tick_size
实际价格 = price_tick × price_tick_size
```

`qty_lot` 以交易流定义的最小数量单位换算：

```text
qty_lot = 实际数量 / quantity_step_size
实际数量 = qty_lot × quantity_step_size
```

期货交易流的 `qty_lot` 保留交易所原始合约数量语义；需要比较跨市场可成交量时，由查询端结合合约乘数转换。

## 3. 分钟内秒级差量表

表名：`order_book_second_delta`

用途：保存同一分钟内第 1 至 59 秒相对上一个有效采样状态的最终盘口变化。连续有效时，它就是相对前一秒的变化；经过无效区间后的首个有效秒，则相对本分钟上一个有效状态。差量按价格记录，不按买 1、买 2 等档位编号记录。

唯一键：`(minute_id, second_offset)`

| 字段名 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `minute_id` | 分钟盘口 ID | `UInt64` | 是 | 关联 `order_book_minute.id`。不重复保存交易流和分钟时间。 |
| `second_offset` | 分钟内秒偏移 | `UInt8` | 是 | 当前差量所属秒，取值范围为 `1` 至 `59`；第 0 秒由分钟完整盘口表表示。 |
| `bid_change_prices` | 买盘变化价格数组 | `Array(Int64)` | 是 | 本秒发生新增、修改或删除的买盘价格，元素为 `price_tick`。 |
| `bid_change_qtys` | 买盘变化数量数组 | `Array(UInt64)` | 是 | 与 `bid_change_prices` 同下标对应的最终 `qty_lot`。数量为 `0` 表示删除该价格。 |
| `ask_change_prices` | 卖盘变化价格数组 | `Array(Int64)` | 是 | 本秒发生新增、修改或删除的卖盘价格，元素为 `price_tick`。 |
| `ask_change_qtys` | 卖盘变化数量数组 | `Array(UInt64)` | 是 | 与 `ask_change_prices` 同下标对应的最终 `qty_lot`。数量为 `0` 表示删除该价格。 |

数组必须满足：

```text
length(bid_change_prices) = length(bid_change_qtys)
length(ask_change_prices) = length(ask_change_qtys)
```

同一个秒内，同一方向、同一价格只能保留一个结果；若交易所推送多次变化，只保留该秒采样时刻的最终数量。

数量保存的是更新后的绝对值，而不是与前一秒相比的增减值。例如某价格的数量从 `1.2` 变为 `1.5`，差量中保存 `1.5` 对应的 `qty_lot`，不保存 `+0.3`。

若某个价格从保存的前 50 档状态中移除，写入其价格和数量 `0`。该价格即使仍存在于交易所更深的盘口中，也视为从本系统保存的前 50 档状态中删除。

若某一秒有效但盘口没有变化，不写差量行；其有效性由分钟完整盘口表的 `valid_bitmap` 对应位表示。若某一秒无效或缺失，同样不写差量行，但 `valid_bitmap` 对应位必须为 `0`。

无效区间后的首个有效秒必须相对本分钟上一个已保存的有效状态生成差量，使查询端跳过无效秒后仍能恢复该秒。如果第 0 秒无有效完整盘口，则本分钟没有恢复起点，整分钟不保存。

## 4. 整点资金费率表

表名：`funding_rate_hourly`

用途：每个 UTC 整点保存永续合约当时可获得的一条资金费率。优先保存实际结算费率；没有实际结算值时保存估算费率。

唯一键：`(instrument_id, hour_time)`

| 字段名 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `instrument_id` | 交易标的 ID | `UInt32` | 是 | 关联 `instrument.instrument_id`，且对应永续合约。 |
| `hour_time` | 采集整点 | `DateTime` | 是 | 本次记录的 UTC 整点时间，分钟、秒和毫秒固定为 0。 |
| `funding_time` | 资金费结算时间 | `DateTime64(3, 'UTC')` | 是 | 当前费率对应的 UTC 结算时点，保留交易所返回的毫秒精度。估算值表示目标结算时间，实际值表示本次实际结算时间。 |
| `rate` | 资金费率 | `Decimal` | 是 | 估算资金费率或实际结算资金费率，不使用二进制浮点数。 |
| `is_actual` | 是否实际结算值 | `Boolean` | 是 | `1` 表示实际结算费率，`0` 表示估算费率。 |

写入规则：

1. 估算费率来自交易所公开 WebSocket。每个 UTC 整点从内存中的最新有效推送写入 `rate`，并设置 `is_actual = 0`；不使用 REST 轮询估算费率；
2. WebSocket 推送必须同时保存其对应的 `funding_time`。结算整点应使用结算前已经收到、且目标为该结算时点的估算值，不能被结算后下一周期的估算值抢先替换；
3. 实际结算费率只通过交易所历史 REST 接口确认。首次查询不得早于目标 `funding_time + 2 分钟`；
4. 同一交易所的实际费率 REST 查询必须由单个 worker 串行执行，请求之间至少间隔 1 秒。不同 instrument 不得在整点集中并发请求；
5. 首次未取得实际值时，按结算后第 5、15、60 分钟重新入队；仍未取得时可低频补查，但不能恢复为每分钟遍历全部 instrument；
6. 取得实际值后，使用交易所返回的精确 `funding_time`，以相同 `(instrument_id, hour_time)` 写入实际版本并设置 `is_actual = 1`；
7. 已保存的实际值不得被后续估算值覆盖；
8. 按照“一时间点只保留一个费率”的规则，结算整点不同时保存针对下一结算周期的新估算值。
9. 进程启动时只扫描最近 24 小时内 `funding_time` 已到的记录；同一 `(instrument_id, funding_time)` 只要存在实际版本就不补查，否则去重后交给对应交易所的同一个串行 worker。错过多个历史重试时点时只立即查询一次，不形成启动请求突发。

`hour_time` 是本表的整点逻辑键，继续保持秒和毫秒为 0；`funding_time` 是交易所定义的实际或目标结算时刻，不得为了匹配 `hour_time` 而截断。当前不记录 REST 请求时间或取得实际值的时间，因为它们不参与套利分析。

本表不保存资金费率上下限、结算标记价格、结算状态、资金费间隔或分别独立的估算/实际费率字段。需要研究资金费规则边界、指数结算或标记价格偏差时，应另建低频规则或指数数据表，不扩充本表。

## 5. 收益路线与收益观测

表名：`yield_route`、`yield_observation`

两表为一对多关系：`yield_route` 保存收益产品及资产路径的稳定定义，`yield_observation` 通过 `yield_route_id` 保存该路线在某个 UTC 时刻、某个额度档位的完整收益快照。路线主键为 `yield_route_id`；观测逻辑键为 `(yield_route_id, observation_time, tier_no)`，并按 `observation_time` 的月份分区。

收益数据量较低，每次有效采集直接保存完整快照，不使用盘口的分钟锚点和秒级差量编码。时间统一使用 UTC，利率、金额、费用、额度和兑换比例均使用 `Decimal`，不得经过二进制浮点数。

收益路线和观测独立于盘口及资金费率表：收益率中不合并永续资金费率，也不重复保存做空市场深度。完整字段字典和筛选语义见 [ARB-0016 收益数据采集设计](arbitrage/strategies/arb-0016-yield-data.md)；TRX 的 JustLend 与原生质押采集、区块锚点和写入规则见 [ARB-0016 TRX 收益采集实现设计](arbitrage/strategies/arb-0016-trx-yield-implementation.md)。

## 6. 盘口恢复关系

查询某交易流在某分钟内第 `n` 秒（`0 ≤ n ≤ 59`）的盘口时：

1. 按 `(instrument_id, minute_time)` 读取 `order_book_minute` 的完整盘口；
2. 检查 `valid_bitmap` 的第 `n` 位，若为 `0` 则该秒盘口无效；
3. 读取同一 `minute_id` 且 `second_offset ≤ n` 的全部差量；
4. 按 `second_offset` 从小到大应用差量：数量为 `0` 则删除该价格，数量大于 `0` 则新增或覆盖该价格的数量；
5. 买盘按价格从高到低、卖盘按价格从低到高排序，各取前 50 档。

分钟完整盘口和差量表之间不跨分钟依赖；下一分钟第 0 秒的完整盘口是新的独立恢复起点。
