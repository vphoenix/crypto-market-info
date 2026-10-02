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

## 10. 期权采集

2026-09-21 已按 [ARB-0009 R4 设计](arbitrage/strategies/arb-0009-options-collection-design.md) 实现现有机器上的固定合约清单采集：Deribit BTC/ETH币本位、USDC线性期权及同到期期货，保存10档与必要元数据供以后分析。[离线基础](arbitrage/strategies/arb-0009-options-phase-1.md) 与[实时实现](arbitrage/strategies/arb-0009-options-live.md)共享规格、规则与盘口事实表，使用各自提交表。真实公共来源已在临时库完成验证，现有常驻采集服务尚未启用这些表。

- 已扩展 `instrument` 的类型与版本校验，并实现定类型经济规格、组合腿和交易规则。实时run/成员、所选元数据复核证据与状态、指数观测及分钟发布见10.3；完整结算/费用/保证金规则、原生combo和全链BBO按需扩展。行权价币种、权利金币、结算币、native amount 和合约张数不能混用。
- 经济/编码规格保持不可变；下单tick、分段tick、最小量与增量放入独立 `derivative_trading_rule`，逐秒引用当时规则。该链路 `contract_multiplier=1` 表示保留native amount，USD名义和combo必须经专项换算，不能直接套第8节USDT永续的基础币换算公式。
- 已提供 `derivative_book_minute`、`derivative_book_second_delta`、`derivative_book_quality_minute` 的显式建表与离线读写接口。新数据仍只保存每秒前10档，`stored_depth=10`；允许已知空边/空书，combo允许零价和负价，因此不复用现有正价/双边盘口的校验语义。
- 新分钟起点用等长typed价格/数量数组，各侧长度0–10，真实数量大于0；价格0可为真实combo档位，只有差量数量0表示删除。没有有效第0秒锚点的分钟不生成可回放盘口，但保留独立质量记录。
- 每秒质量按分钟60槽保存；所选合约BBO可直接从L2提取，无需先建全链quote表。真实源时间、接收时间、采样截止、连接代次、缺失原因与盘口状态分开表达。
- 实时分钟需关联本次run及不可变 `batch_id`，先写数据后发布提交标记；跨表部分写入不可见，重试复用原内容与身份。当前 `derivative_book_foundation_commit` 仍只接受离线来源，不能冒充实时提交。所选指数和低频元数据使用定类型字段，不进入资金费率或收益表。
- 低频实时来源按本次请求范围核验完整性，保存来源时间/采集时间、URL和payload hash；规则区分生效与知悉时间，失败不刷新历史事实。盘口的 `delta_bitmap` 标明应存在差量的秒，提交后缺行必须返回incomplete，不能解释成无变化。
- 现有10档/旧50档历史的编码、物理列和回放路径保持本字典第2–3节语义；不得先截旧50档起点再应用旧差量。

### 10.1 已实现的离线基础表（显式初始化）

`DerivativeSchemaStatements`/`InitDerivativeSchema` 提供以下六表；原 `InitSchema` 不调用它们，app仅在期权任务启用时通过 `InitOptionsLiveSchema` 初始化。均为 `ReplacingMergeTree`，以下是实际代码的逻辑键和数据口径。热路径没有JSON、字符串价格或二进制浮点数。

| 表 | 排序/去重键 | 实际字段与约束 |
| --- | --- | --- |
| `derivative_contract_spec` | `instrument_id` | 源ID、创建时间、经济定义hash、normalization_version、linear/reversed、native amount种类与币种、Decimal(38,18) contract_size、index、结算语义ID、Nullable期权类型/strike/strike_currency、等长组合腿ID/有符号比例/Decimal换算系数数组、首次证据hash。与instrument经济版本一起不可变；本阶段不单独建option和combo子表 |
| `derivative_trading_rule` | `trading_rule_id FixedString(64)` | instrument、Decimal tick/min amount/step、等长分段阈值/tick数组、observed_at/known_from/effective_from、明确first_observed或published口径、来源URL与payload hash；时间DateTime64(6,UTC)；规则变化新增ID |
| `derivative_book_minute` | `(instrument_id,minute_time,batch_id)` | id、encoding_version=1、stored_depth=10、signed、valid_bitmap、delta_bitmap、四个买卖price Int64/qty UInt64数组，每侧0–10档；价0不是填充；按月分区 |
| `derivative_book_second_delta` | `(minute_id,second_offset,batch_id)` | 秒1–59，买卖各price Int64与qty UInt64等长数组，qty=0删除；按minute_id内分钟推导月份分区 |
| `derivative_book_quality_minute` | `(instrument_id,minute_time,batch_id)` | signed、整份该instrument分钟内容hash；sampled/stream_valid/replay_valid/market_known/market_open位图；所有时间、epoch、序号、交易规则、市场状态依据、原因和实际档数按60槽保存；时间统一Nullable DateTime64(6,UTC)，UUID/hash未知为NULL；按月分区 |
| `derivative_book_foundation_commit` | `(run_id,minute_time)` | batch_id、origin仅fixture/synthetic、evidence_hash、prepared_at、完整instrument_ids与member_hashes数组、anchor_count与delta_count；按月分区；最后写以发布离线分钟 |

质量原因的UInt8编码由 `model.DerivativeReason` 定义：0=有效、1=未就绪、2=断线、3=断序、4=解析错误、5=元数据未知、6=采样迟滞、7=缺锚点、8=资源超限、9=时钟异常、10=明确关闭或已到期；市场状态依据0=未知、1=生命周期接纳时间、2=目录发布时间。market_known=0时market_open不能当作明确关闭。当前实时实现使用依据2，不宣称具有独立生命周期频道的实时确认。

分钟内容摘要包含锚点、全部差量和60槽质量，使用确定性二进制编码；nil和空价量数组等价，时间按UTC值编码。batch摘要再包括run、分钟、origin、证据hash、准备时间和有序成员摘要；重试不得修改这些字段。读取完整成员再校验各行存在性、delta_bitmap与内容摘要，失效秒和缺锚点返回明确质量，提交后缺数据返回incomplete。离线提交标记只证明指定离线批次完整，不能证明实际采集已运行。

每批最多448个instrument，预校验后每100个instrument分块写入差量、快照、质量，全部成功后写提交标记；进程内互斥，但不声称支持多writer。离线期权时间不改写成查询时间。真实原始数值按1e-8编码，source_contract_size不再重复乘到native amount。

### 10.2 第一阶段离线测量

2026-09-21 的整日10档合成测试通过：单流1440分钟的快照、差量、质量及提交标记合计，活跃输入为2,282,784压缩字节，静默输入为1,121,074压缩字节。最终查询代码在相同整日样本上抽取100个分钟，完整单成员批次读取与回放P95分别为31.49 ms、31.54 ms。测量口径、分表占用及测试记录见[离线实现记录](arbitrage/strategies/arb-0009-options-phase-1.md#整日合成容量与查询测量)。这些是离线合成结果；真实活跃/冷门交易流的日量与查询测量在接入后随持续采集记录，不再设置七天运行准入要求。

### 10.3 实时新增四表

`InitOptionsLiveSchema` 显式初始化原六表与以下四表。期权关闭时不执行这些DDL；新表同为 `ReplacingMergeTree`，不是JSON大表。

| 表 | 排序/去重键 | 实际字段与约束 |
| --- | --- | --- |
| `options_live_run` | `run_id UUID` | `run_hash FixedString(64)`、UTC微秒起始时间、REST/WS URL、选择方法；有序等长成员instrument ID/原始symbol/经济定义hash/index ID数组、唯一有序index ID集合；选择参考symbol、Int64买卖价格tick、源/接收时间、hash等长数组。一个run最多32个合约、4个指数，校验C/P及同到期期货配对 |
| `options_metadata_observation` | `(run_id,attempt_id,instrument_id)`，按观测月分区 | scope、symbol、URL、UTC微秒requested/observed时间、payload hash、status、definition hash、rule ID、state、active、scope_complete及UInt32 raw/accepted/excluded计数、row_hash。相同响应的成员共享attempt UUID，成功scope满足raw=accepted+excluded；request/parse_error不能伪造完整计数，missing/definition_changed来自完整scope但不能携带当前有效规则或状态 |
| `options_index_minute` | `(index_id,minute_time,batch_id)`，按分钟月分区 | row_hash与固定60槽：`prices Array(Nullable(Decimal(38,18)))`、source/received UTC微秒Nullable时间数组、UUID epoch数组、UInt8 state数组。采样时间由minute+槽位推导 |
| `options_live_minute_commit` | `(run_id,minute_time)`，按分钟月分区 | batch_id/run_hash、UTC微秒prepared_at、有序instrument_ids/member_hashes、UInt32 anchor_count/delta_count、index_ids/index_hashes。整个分钟的数据写成功后才发布；内容不同的同键重试拒绝 |

指数state为0=缺失、1=本秒收到、2=沿用此前观测、3=断线、4=无效。1/2必须有正价格、真实源/接收时间与非零epoch；其他状态价格和时间均为NULL，不能拿断线前价格冒充当前值。held只表示健康连接上暂未变化，不改写原始源时间；指数失效与盘口有效性独立。

元数据响应没有独立来源时间时，`observed_at` 明确采用完整响应解析后的采集时间；所有实时scope保留URL、请求窗口、hash和解析计数。规则和证据写入成功后才经有序入口发布，质量行中的rule/state发布时间不能早于观测，也不能晚于对应采样秒。当前规则引用必须能在本run的成功元数据行中核验，读取也会检查；缺证据、缺指数或缺盘口行返回incomplete。

实时批次ID绑定run hash、有序完整成员和指数摘要，使用不同于离线信封的身份域。读取 `LoadOptionsMinute` 只认实时提交表，核对持久化run/规格、完整成员、规则及元数据证据后逐秒回放；`LoadDerivativeBookEnvelope`仍只认fixture/synthetic离线提交表。已存在的历史50档使用原回放路径。

秒入口的256条/16MiB上限、时钟异常或持续写入失败会结束当前run并重试，尚未完成/提交的分钟形成缺口。逐秒冻结迟于T+250ms则保存无效质量；缺第0秒锚点时该分钟不可回放。停止最多45秒排空完整分钟，末尾不完整分钟不提交。实际短窗口容量与查询测量见[实时记录](arbitrage/strategies/arb-0009-options-live.md)。

## 11. Ethereum DEX 协议兑换实验（2026-09-22）

本次新增五张专项表，DDL 入口为 `DEXSchemaStatements` / `InitDEXSchema`；`collector --init-dex-schema` 只建表并退出。当前 `crypto_market_info` 已实际建表，DEX 持续采集默认关闭。完整范围、使用方法与验收记录见 [DEX 实现说明](dex-arbitrage-implementation.md)。原盘口、期权、资金费率和收益模型不改。

通用列：`chain_id UInt64`、`block_number UInt64`、`block_hash/manifest_hash/batch_id/payload_hash FixedString(32)`、`block_time DateTime64(6,'UTC')`。地址使用 `FixedString(20)`；原子金额、费用用 `UInt256`，未知数量为 `Nullable(UInt256)`，应用通过 `big.Int` 读写，不经浮点。所有表按 `toYYYYMM(block_time)` 分区，不设自动删除 TTL。

| 表 | 额外字段及语义 | 引擎、排序/去重键 |
| --- | --- | --- |
| `dex_block` | parent_hash、base_fee_wei、fee_recipient；received_at/available_at UTC微秒时间；capture_mode、canonical、finality、revision；log/receipt/quote三项coverage；expected_quotes/actual_quotes/log_count/receipt_count、三项成员SHA-256、committed | `ReplacingMergeTree(revision)`；chain+manifest+height+hash |
| `dex_sky_state` | module_id；tin/tout/buf/dai_cash/usdc_pocket_cash/pocket_allowance；vat_live/dai_join_live/dai_join_ward/usds_join_ward；identity_ok/state_complete/reason | `ReplacingMergeTree`；chain+manifest+height+hash+batch+module |
| `dex_route_quote` | quote_id、quote_role、route_id、quote_mode、token_in/out、requested_amount_raw；amount_in/out_raw、dust_dai/usds；v3_input/output_token、v3_input/output_raw、sqrt_price_after_x96、ticks_crossed、quoter_gas_estimate；status/reason/available_at | `ReplacingMergeTree`；chain+manifest+height+hash+batch+quote_id |
| `dex_log` | tx_hash、tx_index、log_index、emitter、topics Array(FixedString(32))、ABI data String（二进制）、event_type、removed | `ReplacingMergeTree`；chain+manifest+height+hash+batch+tx+log_index |
| `dex_tx_receipt` | tx_hash/index、sender/recipient、has_recipient、tx_type、value_raw、input_selector Nullable(FixedString(4))、calldata_hash、status、gas_used、effective_gas_price、receipt_log_count、receipt_hash、available_at | `ReplacingMergeTree(available_at)`；chain+height+hash+tx |

`capture_mode` 为 `live`（当时新块）、`backfill`（补头/日志，不补历史金额报价）、`research`（显式历史样本，不能计入实时完整覆盖或确认新窗口）。`finality` 为 head/safe/finalized/orphaned；三项覆盖为 missing/partial/complete。空日志请求成功是 complete 且 count=0，请求失败是 missing，绝不混用。quote status 为 ok/unknown，unknown 必须有具体原因，缺少任一必要值不得填成零。

`quote_id` 是 batch、role、route、输入输出币、模式及请求原子金额的确定性摘要。每批固定56条策略金额观测和2条参考观测，失败档也保存 unknown。quote成员摘要按quote_id排序，日志按log_index排序，回执按tx_hash排序，再对定类型序列计算SHA-256；真实读回测试检查UInt256、NULL、时间及摘要完全一致。

写入顺序为 Sky/quotes/logs/receipts，再写 dex_block 的 committed 标记；进程内写入失败保留同一不可变批次重试，未提交事实不可见。查询**先对每个block逻辑键 argMax(tuple(...),revision)，再筛canonical/committed/finality**；不能先筛旧canonical行。报价只按最新完成行选择的batch读取，并校验成员数和成员摘要。finality/重组版本保留原batch、成员、received_at与available_at，孤块事实保留而统计排除。

已经提交但缺失的日志另行补采：先写日志事实，再追加coverage完成revision，保留原quote成员与可见时间；补采的原始证据记录真实获取时间。相同hash的每条日志使用确定性定类型日志响应作为payload证据，完整RPC信封由该区块的proof引用；不能因RPC id或请求时间变化而改变同一日志事实。日志补写同样保留不可变pending直到完成。回执独立batch按available_at去重，补齐目标交易集合后更新receipt coverage/count/members，不让已完成块占住补采窗口。这些是补采完成版本；纯finality版本不重新生成数据。

原始RPC响应及manifest按SHA-256寻址写入 `DEX_EVIDENCE_DIR/<hash前2位>/<hash>.json.gz`，临时文件、fsync、原子rename完成后才提交表引用；calldata证据保存原始字节（该文件不一定是JSON）。RPC证据保存脱敏source_id（主机名）与UTC获取时间，不保存RPC URL的用户名、路径或查询凭据。事实行及manifest在库内均有hash，原始body在文件中；不使用通用JSON业务大表。证据目录与ClickHouse数据共同备份，不能独立清理引用文件。

报告连接使用服务端 `readonly=1`，不执行数据库引导或DDL。成本补报只写报告目录的证据与JSON/CSV，不修改生产事实。参考1 WETH卖出价格不参与可执行gas成本计算；成本必须用同hash、实际gas数量的USDC→WETH exact-output，Quoter内部gas不作为交易总gas。

日规模存储/查询验收见[容量记录](../research/2026-09-22-dex-implementation/validation.md#日规模合成容量)：真实样本衍生的两份7,200块合成数据分别占40,849,739及34,423,988压缩列字节，另列查询耗时；不包含gzip证据、marks和真实行情变化的额外熵，不当作实采日量。

## Across 稳定币中继（2026-10-02 已实现）

独立研究库 `crypto_market_info_across` 已建立七表：`across_capture`、`across_deposit`、`across_deposit_update`、`across_fill`、`across_refund`、`across_tx_receipt`、`across_order_probe`。入口 `AcrossSchemaStatements` / `InitAcrossSchema`，命令 `cmd/across-data`。字段见 [DDL](across-stablecoin-data-schema.sql)，操作和实际限制见 [实现说明](across-stablecoin-data-implementation.md)。

金额原子单位 UInt256，估值价格/数量 Decimal(38,18)，时间 DateTime64(6,UTC)；协议 bytes32 和 EVM address 分别为32/20字节。原始响应存有SHA256的本地gzip证据，不使用通用JSON业务表。事实包含capture_id并按原区块时间分月，capture/probe分别按开始/请求时间分月，不设TTL。

先归档证据、写事实，再提交capture；读取先取最新revision，再筛canonical/committed并核验六组成员计数/摘要。重试内容不变；重组revision只改canonical/finality/说明，旧成功版本不会重新出现。跨capture按不可变链上事实去重，采集时间和payload格式不同不算协议冲突。capture错误、成功空日志和未知ABI保持不同状态。

原始/更新条款、真实live首见、计划/实际后续probe各自保存；原始响应先收到不代表解码已可用。重启/补采不能制造历史live可见性。聚合退款仅通过同交易Transfer核验地址到账，不伪造逐单归属。整笔gas按交易去重，未知费用/过期价格保留NULL。库存及成本情景只在离线报告计算。已通过[独立代码审核与必要验证](../discuss/0013-across-stablecoin-code-review.md)，完整验收见[记录](../research/2026-10-02-across-implementation/validation.md)。
