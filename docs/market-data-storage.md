# 市场数据存储数据字典

本数据字典定义当前阶段的六个市场与收益核心表：交易标的表 `instrument`、分钟完整盘口表 `order_book_minute`、分钟内秒级差量表 `order_book_second_delta`、整点资金费率表 `funding_rate_hourly`、收益路线表 `yield_route` 和收益观测表 `yield_observation`；另定义三个永续共有交易对元数据表 `instrument_canonical_mapping`、`perpetual_universe_run` 和 `perpetual_universe_member`。

这些表在整个采集进程中的位置及以后增加其他数据时的扩展原则见[系统总体架构](architecture.md)。

所有时间均为 UTC。盘口快照和差量中的价格、数量均为整数：价格使用 `price_tick`，数量使用 `qty_lot`；不得使用字符串价格或二进制浮点数。用于定义换算单位的元数据使用十进制定点数 `Decimal`。

`instrument_id` 是交易流的唯一标识，必须区分交易所、市场类型、标的、结算币种及合约版本。例如，Binance 现货 `BTCUSDT`、Binance U 本位永续 `BTCUSDT`、OKX 永续 `BTC-USDT-SWAP` 和 Bybit 线性永续 `BTCUSDT` 必须使用不同的 `instrument_id`。其定义保存在 `instrument` 表中，不在各事实表中重复保存。

## 1. 交易标的表

表名：`instrument`

用途：保存交易所产品代码与统一交易流 ID 的映射，以及还原整数价格、整数数量和分析现货/合约价差所需的最少元数据。

主键：`instrument_id`

| 字段名 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `instrument_id` | 交易标的 ID | `UInt32` | 是 | 交易流的唯一标识。交易所产品定义、结算方式或合约版本发生变化时，分配新的 ID。 |
| `exchange` | 交易所 | `String` | 是 | 交易场所，例如 `Binance`、`OKX`、`Bybit`。 |
| `market_type` | 市场类型 | `String` | 是 | 当前取值为 `spot`（现货）、`perpetual`（永续）或 `delivery`（交割合约）。 |
| `exchange_symbol` | 交易所产品代码 | `String` | 是 | 交易所原始产品代码，例如 `BTCUSDT`、`BTC-USDT-SWAP`。 |
| `venue_contract_version` | 场内合约版本 | `String` | 新登记衍生品是 | 标识同一交易所代码背后的具体合约实例。Binance 永续使用 `onboardDate`，OKX 永续使用 `listTime`，Bybit 永续使用 `symbolId:launchTime`；现货及迁移前历史行可为空。该字段变化必须分配新 `instrument_id`。 |
| `base_asset` | 基础币种 | `String` | 是 | 被交易的资产，例如 BTC/USDT 中的 BTC。 |
| `quote_asset` | 报价币种 | `String` | 是 | 表示价格使用什么资产计价，例如 BTC/USDT 中的 USDT。 |
| `settle_asset` | 结算币种 | `Nullable(String)` | 否 | 保证金、盈亏和资金费使用的资产。现货为空；USDT 本位合约通常为 USDT；币本位合约可能与报价币种不同。 |
| `contract_multiplier` | 合约乘数 | `Decimal` | 是 | 合约数量换算参数；现货固定为 `1`。跨市场比较数量时，查询端结合该值及合约类型换算为统一基础币数量。 |
| `price_tick_size` | 最小价格单位 | `Decimal` | 是 | 一个 `price_tick` 对应的实际价格，用于还原盘口价格，并支持 tick 离散规则分析。不得使用二进制浮点数。 |
| `quantity_step_size` | 最小数量单位 | `Decimal` | 是 | 一个 `qty_lot` 对应的实际数量，用于还原盘口数量，并支持 lot 离散规则分析。不得使用二进制浮点数。 |
| `expiry_time` | 到期时间 | `Nullable(DateTime)` | 否 | 交割合约的 UTC 到期时间；现货和永续为空。用于计算剩余期限和年化基差。 |

`quote_asset` 与 `settle_asset` 表达不同含义。例如，BTC 币本位合约可以使用 USD 报价，但使用 BTC 作为保证金和盈亏结算资产，因此其 `quote_asset` 为 USD、`settle_asset` 为 BTC。

本表不维护启用状态、创建时间、更新时间或生效区间。若上述交易定义发生变化，应创建新的 `instrument_id`，避免用新规则解释旧行情。`venue_contract_version` 通过幂等 `ADD COLUMN IF NOT EXISTS ... DEFAULT ''` 兼容旧表；旧行的空字符串只代表迁移前未知版本，不能用于登记新的衍生品定义。首次部署该迁移时，现有 Binance、OKX 永续会以有版本的新定义取得新 ID，旧盘口和资金费率仍留在旧 ID 下，不覆盖也不重写。

注册器先完整校验一批全部定义，再为所有新定义执行一次 batch insert；调用内的重试复用已分配 ID。相同完整定义复用历史 ID；历史 tick、lot 或乘数改变，即使场内上市版本不变，也仍分配新 ID。同一注册批次不能为同一 `(exchange, market_type, exchange_symbol, venue_contract_version)` 提交两种不同定义，这表示当前 catalog 自相矛盾；这项检查不禁止历史定义并存。

## 2. 分钟完整盘口表

表名：`order_book_minute`

用途：保存每个交易流在每分钟第 0 秒的完整盘口。新采集保存买卖各 10 档；旧历史保存买卖各 50 档，由 `stored_depth` 标明。该记录是恢复本分钟内任意秒盘口的起点。

唯一键：`(instrument_id, minute_time)`

| 字段名 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `id` | 分钟盘口 ID | `UInt64` | 是 | 分钟完整盘口的唯一 ID，供差量表关联。按照下方公式由 `instrument_id` 与 UTC 分钟序号确定性生成。 |
| `instrument_id` | 交易流 ID | `UInt32` | 是 | 唯一标识交易所、市场类型、标的、结算币种及合约版本。 |
| `minute_time` | 分钟时间 | `DateTime` | 是 | UTC 分钟起始时间，秒和毫秒固定为 0。 |
| `valid_bitmap` | 秒有效位图 | `UInt64` | 是 | 低 60 位分别代表本分钟第 0 至 59 秒是否有有效盘口。位值为 `1` 表示有效，`0` 表示无效或缺失。 |
| `stored_depth` | 保存档数上限（每侧） | `UInt8` | 是 | 仅支持 `10` 或 `50`。新 writer 显式写 `10`；列默认值固定为 `50`，用于旧 part 与旧 collector。实际挂单不足此深度时仍以零补齐。 |

兼容迁移执行 `ADD COLUMN IF NOT EXISTS stored_depth UInt8 DEFAULT 50 AFTER valid_bitmap`。物理快照继续保留每侧 50 档的价格和数量列，新数据的第 11 至 50 档必须全部为零；压缩后主要新增占用来自前 10 档及其差量。不删除列、不重写旧 part、不改变 instrument 或分钟唯一键。新建库也采用同样结构和默认值，避免旧 writer 回退运行时被误标为 10 档。

回放必须先加载完整的历史起点及差量，再按本分钟实际保存深度返回，并在输出 `BookSnapshot.StoredDepth` 中标明上限。不能只读旧起点的前 10 档：未发生数量变化的旧第 11 档也可能因前档删除进入前 10。尚未迁移的旧表可只读回放，缺少该列时按 50 解释；新查询不会为此执行 DDL。新分钟在 UTC 分钟边界建立 10 档独立起点，不与旧分钟共用差量链。回滚只能在新分钟开始新起点，不能重写已存在分钟为另一深度。

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

买盘非零有效价格必须严格递减（新数据到第 10 档，旧数据最多到第 50 档）：

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

卖盘非零有效价格必须严格递增（新数据到第 10 档，旧数据最多到第 50 档）：

```text
ask_price_01 < ask_price_02 < …… < ask_price_50
```

如果某一侧不足 `stored_depth` 档，从第一个缺失档开始，该档及后续物理档位的价格和数量一律为 `0`。`stored_depth=10` 时第 11 至 50 档强制为零；writer 拒绝深度不受支持、超出深度的非零快照或差量恢复后超过该深度的状态。

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

若某个价格从保存的前 `stored_depth` 档状态中移除，写入其价格和数量 `0`。该价格即使仍存在于交易所更深的盘口中，也视为从本系统保存的状态中删除。新数据先截取每侧 10 档再计算差量，不能通过截短旧 50 档差量数组得到 10 档差量。

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
9. 进程启动时只扫描最近 24 小时内 `funding_time` 已到的记录；同一 `(instrument_id, funding_time)` 只要存在实际版本就不补查，否则去重后交给对应交易所的同一个串行 worker。错过多个历史重试时点时只立即查询一次，不形成启动请求突发。发生合约版本字段迁移后，补查集合还包含与当前启用流在交易所、市场、代码及资产身份上兼容的旧 `instrument_id`，但实时盘口和估算资金费率只写当前版本 ID。

`hour_time` 是本表的整点逻辑键，继续保持秒和毫秒为 0；`funding_time` 是交易所定义的实际或目标结算时刻，不得为了匹配 `hour_time` 而截断。当前不记录 REST 请求时间或取得实际值的时间，因为它们不参与套利分析。

本表不保存资金费率上下限、结算标记价格、结算状态、资金费间隔或分别独立的估算/实际费率字段。需要研究资金费规则边界、指数结算或标记价格偏差时，应另建低频规则或指数数据表，不扩充本表。

## 5. 收益路线与收益观测

表名：`yield_route`、`yield_observation`

两表为一对多关系：`yield_route` 保存收益产品及资产路径的稳定定义，`yield_observation` 通过 `yield_route_id` 保存该路线在某个 UTC 时刻、某个额度档位的完整收益快照。路线主键为 `yield_route_id`；观测逻辑键为 `(yield_route_id, observation_time, tier_no)`，并按 `observation_time` 的月份分区。

收益数据量较低，每次有效采集直接保存完整快照，不使用盘口的分钟锚点和秒级差量编码。时间统一使用 UTC，利率、金额、费用、额度和兑换比例均使用 `Decimal`，不得经过二进制浮点数。

收益路线和观测独立于盘口及资金费率表：收益率中不合并永续资金费率，也不重复保存做空市场深度。`unbonding_seconds` 为 `Nullable(UInt64)`：`0` 表示明确没有制度性等待，正整数表示协议规定的固定等待秒数，`NULL` 表示等待信息未知或无法预先确定固定秒数；该字段不保证流动性充足或实际即时到账。完整字段字典和筛选语义见 [ARB-0016 收益数据采集设计](arbitrage/strategies/arb-0016-yield-data.md)；TRX 的 JustLend 与原生质押采集、区块锚点和写入规则见 [ARB-0016 TRX 收益采集实现设计](arbitrage/strategies/arb-0016-trx-yield-implementation.md)；SOL 第一阶段的 bSOL、JitoSOL、mSOL、验证者白名单和 Marinade Native 见 [ARB-0016 SOL 收益采集第一阶段实现设计](arbitrage/strategies/arb-0016-sol-yield-phase-1.md)。

[AVAX 第一阶段实现](arbitrage/strategies/arb-0016-avax-yield-phase-1.md)复用这两张表保存 OKX、Aave V3/V4 的固定来源历史曲线，不修改 DDL。Aave 历史日期标签对应来源平均 APY，不能当即时利率；同一路线不混写当前快照或其他聚合窗口。历史没有给出的费用、额度及状态保留未知，不用当前值填充。

[AVAX 第二阶段实现](arbitrage/strategies/arb-0016-avax-yield-phase-2.md)在同一 `yield_observation` 表增加以下两列。模型、DDL、writer 和旧表幂等迁移已实现，并已迁移生产库：

| 字段 | 物理类型 | 语义 |
|---|---|---|
| `pool_cash` | `Nullable(Decimal(38,18))` | 指定池合约的底层现金余额，单位 `redeem_asset_key`；不代表保证可立即赎回金额，也不复用 `remaining_capacity`。 |
| `redemption_window_seconds` | `Nullable(UInt64)` | 完成退出等待后的有限申领窗口长度；正数表示有限窗口，0 表示实读零长度、没有可用窗口，未知/不适用时 NULL；0 不是无限期。 |

两列都是同一路线、同一区块的观测属性，不增加第三张表。新版 `InitSchema` 同时包含建表定义及旧表的 `ADD COLUMN IF NOT EXISTS` 迁移；writer 使用显式列名，不依赖新旧表的物理列顺序。旧数据和旧 collector 默认 NULL，不回填，不改变逻辑键或分区。第二阶段读取固定 finalized block hash 的合约状态，`finality=finalized`，不是把无区块的页面/APY 或多个 latest 读数拼在同一锚点上。

## 6. 盘口恢复关系

查询某交易流在某分钟内第 `n` 秒（`0 ≤ n ≤ 59`）的盘口时：

1. 按 `(instrument_id, minute_time)` 读取 `order_book_minute` 的完整盘口；
2. 检查 `valid_bitmap` 的第 `n` 位，若为 `0` 则该秒盘口无效；
3. 读取同一 `minute_id` 且 `second_offset ≤ n` 的全部差量；
4. 按 `second_offset` 从小到大应用差量：数量为 `0` 则删除该价格，数量大于 `0` 则新增或覆盖该价格的数量；
5. 买盘按价格从高到低、卖盘按价格从低到高排序，返回本分钟保存的全部深度（新 10 档、旧 50 档）；返回 `StoredDepth`，稀疏盘口可以少于此上限，超出上限则报错。

分钟完整盘口和差量表之间不跨分钟依赖；下一分钟第 0 秒的完整盘口是新的独立恢复起点。

## 7. 整分钟完成屏障与批量写入

采样器在每个 UTC 分钟切换时发送一个不可变的 `CompletedMinute{MinuteTime, Batches}`。`Batches` 按 instrument ID 排序，只包含第 0 秒有有效 anchor 的交易流；即使没有有效 anchor，也发送一个空 envelope，明确该分钟已收齐。一次全部来源的采样超过下一秒边界时，采样器终止并丢弃当前未完成分钟。

完成分钟的 channel 最多缓存 2 个 envelope；`MINUTE_QUEUE_CAPACITY` 的计量单位是已入队和 writer 正在处理但尚未完成的 instrument batch，空 envelope 按 1 计。未配置时使用 `max(512, 2 × sample_sources)`，显式容量不足两个完整分钟时拒绝启动。queue 满、最老未写完分钟达到 45 秒或写入重试失败均终止采集，不静默丢弃后继续运行。

writer 在第一条 insert 前校验整个 envelope，再按最多 100 个 instrument 分块，不跨分钟拼块。每块先批量写入全部 `order_book_second_delta`，再批量写入对应 `order_book_minute` 可见行。后者成功前，该块的新分钟不具备恢复入口。部分失败时重试完全相同的块；两表使用既有逻辑键和 `ReplacingMergeTree`，查询使用 `FINAL`，因此重复写入不改变回放结果。空 envelope 不写事实表。该机制不承诺跨表事务。

## 8. 规范化交易对映射历史

表名：`instrument_canonical_mapping`；逻辑键：`(instrument_id, mapping_revision)`；引擎：`ReplacingMergeTree`，按逻辑键排序。

| 字段 | 类型 | 含义 |
|---|---|---|
| `instrument_id` | `UInt32` | 已登记的场内 USDT 线性永续交易流。 |
| `mapping_revision` | `FixedString(64)` | 映射算法版本和严格字典规范化 JSON 的 SHA-256 小写十六进制摘要。 |
| `canonical_market_key` | `String` | `<canonical_base_asset>-USDT-PERP`，用于跨交易所分组。 |
| `canonical_base_asset` | `String` | 满足 `[A-Z0-9][A-Z0-9._]{0,31}` 的规范化基础资产。 |
| `canonical_quote_asset` / `canonical_settle_asset` | `String` | 本版均为 `USDT`。 |
| `canonical_base_units_per_venue_base_unit` | `Decimal(38,18)` | 1 个场内 BaseAsset 单位对应的 canonical base 数量，严格大于 0。 |
| `mapping_kind` | `LowCardinality(String)` | `identity` 或 `alias`；identity 必须保持原始 base，且 factor 为 1。 |
| `recorded_at` | `DateTime64(3, 'UTC')` | 该 revision 行首次准备写入的时间，重试保持不变。 |

异名字典按 `(exchange, market_type, exchange_symbol, venue_contract_version)` 精确匹配，不支持通配版本。它不覆盖原始 instrument 字段和盘口数值。查询跨交易所数量和价格时必须使用：

```text
raw_price = price_tick × price_tick_size
raw_size = qty_lot × quantity_step_size
canonical_base_quantity = raw_size × contract_multiplier × canonical_base_units_per_venue_base_unit
canonical_price = raw_price ÷ canonical_base_units_per_venue_base_unit
```

每次启动为历史已登记的全部 USDT 永续及本次新入选 instrument 写入当前 revision。writer 先读取既有 `(instrument_id, mapping_revision)`：语义一致时保留原行，不一致时失败；同一次写入的重试复用原始行及时间。错误 alias 删除后，新 revision 下对应的 identity mapping 仍可解释不再入选的历史 instrument。

映射查询必须指定当前或历史 `run_id`，或者显式 `mapping_revision`；`recorded_at` 不能决定权威解释。字典 A → B → A 回滚时，当前 run 引用 A，早于 B 的 A 行仍是正确解释。精确 canonical JSON、hash 前缀和选择规则见[永续共有交易对设计](perpetual-common-universe.md)。

## 9. 每次启动的 universe 快照

`perpetual_universe_run` 保存一份启动计划，逻辑键为 `run_id`；`perpetual_universe_member` 保存成员，逻辑键为 `(run_id, instrument_id)`。两表均为 `ReplacingMergeTree`，按各自逻辑键排序。

| run 字段 | 类型 | 含义 |
|---|---|---|
| `run_id` | `UUID` | 每次非 dry-run 启动生成的新 ID，写入重试复用。 |
| `selection_revision` / `mapping_revision` | `FixedString(64)` | 此次选择和解释使用的精确 revision。 |
| `started_at` | `DateTime64(3, 'UTC')` | 本次启动计划时间，重试不变。 |
| `selection_config_json` | `String` | 参与选择的规范化配置；venue 是稳定排序的任意长度数组，包含模式、include/exclude 与容量，不包含凭据。 |
| `enabled_venues` | `Array(String)` | 稳定排序的启用交易所。 |
| `canonical_include` / `canonical_exclude` | `Array(String)` | 稳定排序的规范化交易对选择条件。 |
| `canonical_group_count` / `instrument_count` | `UInt32` | 最终规范化组数及场内 instrument 总数。 |

member 只有 `run_id UUID`、`instrument_id UInt32`、`canonical_market_key String` 三列。每个 canonical group 至少属于两个不同 venue，同一个 group 不得重复同一 venue。

启动先登记 instrument、写 mapping，再无网络副作用地构造和校验所有 runtime。其后先批量写全部 member，最后写 run 行作为可见提交标记，成功后才启动网络采集。没有 run 行的孤立 member 一律忽略；重试已有 run ID 必须保持整个 run 及 membership 完全一致。

当前健康检查的 expected instruments 来自进程当前 run 的 member，不取全部历史 mapping 的并集，也不按数据库最近写入时间猜测当前进程。该快照只在启动时生成，metadata 或配置变化要到下次启动才重新选择。

`perp-check` 通过只读连接按 run 成员统计。盘口范围从 `started_at` 之后第一个完整 UTC 分钟开始，到查询时刻或同库下一次已提交 run 开始之前的完整分钟结束；旧 run 被后续 run 替代时只输出历史范围并标记 pending，不能把后续进程的数据累计为旧 run 的运行时长。差量活跃度只统计存在分钟可见标记的 delta，排除孤立差量；随机回放使用固定 seed 对有效秒做无放回 reservoir sampling，每个有效秒入选机会相同。

资金费率按小时桶统计，当前 run 的初始小时可能包含原有行，不能证明其插入进程；旧 run 的最后一个不完整小时也不计入。验收同时报告估算/实际行数并检查小时数量、陈旧状态和 24 小时窗口内是否出现实际确认。`system.parts` 的压缩空间属于整个数据库，不是单次 run 的增量。`passed_checks` 只表示这些数据库和回放检查通过，`full_soak_verified` 始终为 false；重叠进程、连续运行、CPU/内存/网络、限速响应和采样/写入 deadline 仍须独立运行证据，不能从事实表没有 `run_id` 的数据推断。
