# USDT 线性永续共有交易对自动采集设计

## 1. 文档状态与目标

本文定义 Binance、OKX 和 Bybit 的 USDT 线性永续交易流如何在启动时自动发现、规范化、求共有集合并安全启动采集，也定义以后增加交易所时必须遵守的公共接口和不变量。

本次完成后的选择规则是：

> 一个规范化交易对只要出现在至少两个已启用交易所，就进入采集集合；程序采集该交易对在所有已启用且提供它的交易所上的盘口和资金费率。

例如：

| 规范化交易对 | Binance | OKX | Bybit | 结果 |
|---|---:|---:|---:|---|
| `BTC-USDT-PERP` | 有 | 有 | 有 | 三家都采集 |
| `DOGE-USDT-PERP` | 有 | 无 | 有 | 采集 Binance、Bybit |
| `ONLY-USDT-PERP` | 无 | 无 | 有 | 不采集 |

“至少两个”统计的是不同交易所数量，不是 instrument 行数或 WebSocket topic 数。增加第四家交易所时只增加该交易所适配器和注册项，公共选择算法不增加新的两两组合分支。

本文是现有 [Bybit USDT 线性永续采集设计](bybit-usdt-perpetual-market-data.md)之后的扩容设计。实现前，Bybit 文档中的“一 instrument 一条盘口连接”仍描述当前代码；实现本文后，以本文的分片连接规则取代该部分，Bybit 的消息、序列、限频和失效规则继续有效。

## 2. 范围与非目标

本次范围包括：

- 启动时完整读取所有已启用交易所的永续 metadata；
- 只接受正在交易的 USDT 报价、USDT 结算线性永续；
- 生成与交易所无关的规范化交易对身份；
- 使用受版本控制的显式字典处理异名但属于同一底层资产的合约；
- 应用自动、禁用、手工 include 和 exclude 配置；
- 按“不同交易所数量不少于 2”得到最终 universe；
- 对最终 universe 中每个场内 instrument 启动盘口，并在 `FUNDING_ENABLED=true` 时启动资金费率采集；
- 将盘口 WebSocket 从“一 instrument 一连接”改为按交易所分片、多 topic 共用连接，避免全量启动形成大量常驻连接；
- 保留并扩展当前 REST/WS 请求门控、冷却和带抖动重连；
- 将分钟写入从逐 instrument 串行提交改成按分钟分块批量提交，避免全量 universe 在分钟边界形成数百次独立 ClickHouse insert；
- 保存 instrument 到规范化交易对的映射，使以后能够核验异名匹配和单位换算。

本次不包括：

- 现货 universe 自动发现；现有 `BINANCE_SPOT_SYMBOLS` 和 `OKX_SPOT_SYMBOLS` 行为不变；
- USDC 本位、币本位、反向或交割合约；
- 根据成交量、持仓量、币种热度或主观质量选择交易对；
- 运行中热增删交易流；第一版只在进程启动时计算一次，metadata 变化在下次重启时生效；
- 修改每秒前 10 档、分钟快照加秒级差量的存储编码；
- 直接计算套利机会或执行交易。

## 3. 当前行为与必须改变的部分

改造前程序分别读取 `BINANCE_PERP_SYMBOLS`、`OKX_PERP_SYMBOLS` 和 `BYBIT_PERP_SYMBOLS`，只从 metadata 中选择配置列出的原始 symbol。生产配置三家都只列出 BTC，因此只有 BTC 永续被采集。

以下逻辑保持不变：

- 除下列 Binance quote eligibility 补强及 REST/WS 桥接边界修正外，各交易所 adapter 对 metadata、盘口和资金费率的协议解析；
- `instrument_id` 仍标识具体交易所、市场、原始 symbol 和场内合约版本；
- Binance 使用 WebSocket diff 加 1000 档 REST 快照建立本地盘口；
- OKX 使用 `books` 的 400 档 snapshot/update；
- Bybit 使用 `orderbook.1000` 的 snapshot/delta；
- 断档、队列溢出、解析失败或连接失效时 fail closed；
- 本地保留深档，采样器每秒只读取前 10 档；
- 资金费率估算、实际值确认、分钟写入和查询回放逻辑。

以下部分必须改变：

1. 永续 metadata 必须先按交易所形成完整 catalog，再统一选择，不能逐家读取后立即注册；
2. Binance 永续候选除了 `PERPETUAL` 和 `marginAsset=USDT`，还必须明确要求 `quoteAsset=USDT`；
3. symbol 配置需要区分 `auto`、禁用和显式列表，不能继续把未设置、空值和 `-` 都折叠成同一种空切片；
4. Bybit 连接预算必须按最终选择结果计算，不能在 metadata 读取前按配置字符串数量计算；
5. 三家盘口需要按稳定顺序分片并在一条连接中订阅多个 topic；否则全量 universe 会产生数百条连接；
6. Binance 盘口 WebSocket 也要加入共享建连门控；当前只有其 REST 快照有共享门控；
7. Binance 和 OKX 资金费率的大量 topic 订阅要像现有 Bybit 一样分批发送、分别确认；Binance 等待前批 ACK 后发送下一批，OKX/Bybit 允许多批同时等待 ACK，但各批/arg 独立校验和超时。
8. 当前 sampler 在分钟边界把每个 instrument 分别入队，并由单 worker 对每个 instrument 分别写 ClickHouse；全量模式要保留原编码，但把同一分钟的多个 `MinuteBatch` 合并成分块批量写入。

Binance 永续 REST/WS 桥接同时修正一个既有边界错误：首个 diff 的 `u` 等于快照 `lastUpdateId` 时仍可建立桥接，不能当作过期事件丢弃后错过后续连续的 `pu`。该事件的数量已包含在快照内，不重复覆盖快照。此修正遵循 [Binance 本地盘口规则](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)，不改变落库编码。

本设计不改变价格/数量编码单位。实盘已发现“当前下单步长不一定覆盖旧挂单价格”的前置限制：SOLUSDT 当前 `tickSize=0.01`，深档仍有 `105.002`；[官方公告](https://www.binance.com/en/support/announcement/detail/8bfe2e9b86734cd08f3cbcdb82b79f51) 允许变更前订单按原步长继续匹配。现有严格整数编码会拒绝该快照，不能丢档、取整或将其计入有效覆盖。若要完整支持这类产品，需独立确认编码单位与下单规则分离的模型设计；在此前不得宣称满足本文全量验收条件。当前受影响范围与运行状态见[运行说明](runtime-operations.md#6-三家共有永续全量验收)。

## 4. 名词和公共模型

### 4.1 场内 instrument

场内 instrument 是交易所 metadata 返回并经过 adapter 严格解析的 `model.Instrument`。它保留交易所原始身份和原始计量语义，例如：

- `Exchange=Binance`、`ExchangeSymbol=1000PEPEUSDT`；
- `Exchange=OKX`、`ExchangeSymbol=PEPE-USDT-SWAP`；
- 交易所自己的 `BaseAsset`、`ContractMultiplier`、tick size 和 quantity step。

共有交易对匹配不能替换这些字段，也不能让两个交易所共用同一个 `instrument_id`。

### 4.2 规范化交易对

规范化交易对只用于跨交易所分组，第一版格式为：

```text
<canonical_base_asset>-USDT-PERP
```

例如 `BTC-USDT-PERP`、`PEPE-USDT-PERP`。内部不要把该字符串拆来拆去判断语义，应使用定类型键：

```go
type PerpetualMarketKey struct {
    BaseAsset   string
    QuoteAsset  string
    SettleAsset string
    MarketType  model.MarketType
}
```

字符串只用于配置、日志和持久化展示。`MarketType` 固定为 `perpetual`，第一版 quote 和 settle 都固定为 `USDT`，但字段仍完整保留，避免以后扩展时把不同结算口径误合并。

canonical base 必须匹配 `[A-Z0-9][A-Z0-9._]{0,31}`，不允许 `-`、空白或小写；canonical key 的唯一文本格式是 `canonical_base + "-USDT-PERP"`。公共 `ParsePerpetualMarketKey` 只移除一次固定后缀 `-USDT-PERP`，校验剩余 base 后构造定类型键；其他代码只能调用该 parser/formatter，不得自行 split 或大小写转换。交易所返回的 base 不满足 grammar 时，必须由显式 alias 给出合法 canonical base，否则该 catalog 失败。

### 4.3 VenueCatalog 与选择结果

公共选择器接收交易所 catalog 列表，而不是接收 Binance/OKX/Bybit 三组固定参数：

```go
type VenueCatalog struct {
    Venue       string
    Instruments []model.Instrument
}

type SelectedInstrument struct {
    Instrument                    model.Instrument
    MarketKey                     PerpetualMarketKey
    CanonicalBaseUnitsPerVenueBaseUnit decimal.Decimal
    MappingKind                   string // identity 或 alias
}
```

未来增加交易所时，adapter 仍负责交易所协议，app 增加一个 venue spec；`BuildCommonPerpetualUniverse([]VenueCatalog, ...)` 本身不变。

## 5. 异名合约字典

### 5.1 为什么需要字典

名称不同不能仅靠字符串规则判断是否属于同一资产。例如 `1000PEPE` 通常表示一个场内价格单位对应 1000 个 PEPE，但不能普遍假定所有以数字开头的资产都可删除前缀。自动删除 `1000` 会同时带来误匹配和价格、数量差 1000 倍的风险。

默认规则因此是：

- `BaseAsset` 完全相同才得到相同的 canonical base；
- 任何异名合并都必须来自显式字典；
- 禁止使用去数字前缀、模糊匹配或相似字符串自动合并。

### 5.2 文件和字段

字典保存在仓库内的 `config/perpetual-asset-aliases.json`，默认由 `PERP_ASSET_ALIASES_FILE` 指向。文件参与版本控制，部署时与二进制一起验收；程序启动时记录 mapping revision。revision 定义为规范化 JSON 之前加固定算法标识 `perpetual-canonical-v1` 后计算的 SHA-256，因此字典内容或规范化算法版本变化都会产生新 revision。

结构示例：

```json
{
  "schema_version": 1,
  "aliases": [
    {
      "exchange": "Binance",
      "market_type": "perpetual",
      "exchange_symbol": "1000PEPEUSDT",
      "venue_contract_version": "<exact version from catalog metadata>",
      "expected_base_asset": "1000PEPE",
      "expected_quote_asset": "USDT",
      "expected_settle_asset": "USDT",
      "canonical_base_asset": "PEPE",
      "canonical_base_units_per_venue_base_unit": "1000",
      "reason": "one venue base unit represents 1000 PEPE"
    }
  ]
}
```

字典键固定为 `(exchange, market_type, exchange_symbol, venue_contract_version)`。第一版只支持与 catalog metadata 完全相等的合约版本，不提供空值、通配符或“最新版本”匹配。这样同一交易所复用 symbol 重新上市时，可以为不同合约版本保留不同映射。值中的 expected 字段是断言，不是备注；当前 catalog 命中该键但任何断言不同都必须阻止启动。数量因子必须是严格解析的正十进制定点数，不能经过二进制浮点数。

字典必须满足：

- schema version 受支持；
- 不允许未知字段、空字符串、前后空白、重复四元组键或重复 JSON key；
- `venue_contract_version` 必须是 metadata 生成 instrument 身份时使用的非空精确版本；
- `market_type` 第一版只能是 `perpetual`；
- expected quote、expected settle 第一版只能是 `USDT`；
- canonical base 必须是规范的大写资产代码；
- 单位因子必须大于 0 且不超过 `Decimal(38,18)`；
- 当前 catalog 或既有 instrument 中以完整四元组命中的 entry 必须完全满足断言；同 symbol 的其他版本不算命中；
- 未命中的历史或已下架 entry 允许保留，但必须在启动摘要中计数并告警；
- 显式 include 引用不存在或不再满足两家条件的交易对仍然启动失败，不能因陈旧 entry 规则而被忽略。

### 5.3 单位含义

`canonical_base_units_per_venue_base_unit` 的方向固定为：

```text
1 个交易所 BaseAsset 单位 = factor 个 canonical base 单位
```

假设：

```text
raw_price = price_tick × price_tick_size
raw_size  = qty_lot × quantity_step_size
factor    = canonical_base_units_per_venue_base_unit
```

则以后跨交易所比较时必须使用：

```text
canonical_base_quantity = raw_size × contract_multiplier × factor
canonical_price         = raw_price ÷ factor
```

OKX 的 `contract_multiplier=ctVal×ctMult` 表示每个场内合约数量对应多少个交易所 BaseAsset；字典的 factor 表示该 BaseAsset 对应多少 canonical base。两者是不同层级，不能互相覆盖。

本次盘口表继续保存交易所原始 price tick 和 quantity lot，不在采集热路径中改写数值。字典只决定分组和记录换算关系；未来比较层必须使用上述公式。

### 5.4 冲突与修正

同一交易所在同一个规范化交易对下最多只能有一个 live instrument。若别名导致同一交易所的两个 live instrument 都映射到 `PEPE-USDT-PERP`，选择器必须失败，不得：

- 把同一交易所的两行计成“两家”；
- 任意挑其中一行；
- 同时采集两行并让下游误认为它们是不同交易所。

操作人员应通过该交易所的 exclude 配置排除一个原始 symbol，或修正字典。

别名修改只在下次启动生效，不修改既有 `instrument_id` 和历史盘口事实。新映射追加写入第 9 节的映射历史表；查询必须通过当前 run、历史 run 或显式 mapping revision 选择解释，不能把写入时间最新的映射当作权威。

## 6. 配置语义

### 6.1 每家交易所的启用状态

保留现有三个变量名，但使用能够区分状态的新解析器：

| 配置值 | 语义 |
|---|---|
| `auto` | 启用该交易所，并把其全部合格 USDT 线性永续放入候选集 |
| `-` | 禁用该交易所的永续采集，不请求其永续 metadata |
| 逗号分隔原始 symbol | 启用该交易所，但只把这些精确原始 symbol 放入候选集 |

对应变量仍是：

```text
BINANCE_PERP_SYMBOLS
OKX_PERP_SYMBOLS
BYBIT_PERP_SYMBOLS
```

三个变量未设置时保留当前默认值：Binance 为 `BTCUSDT`，OKX 为 `BTC-USDT-SWAP`，Bybit 为 `-`。显式空字符串是配置错误，不再等同于禁用；需要禁用时必须写 `-`。这保留了现有单币测试方式，也使现有生产 unit 在代码升级后仍只采集 BTC，不会因默认值变化突然扩大全量。正式切换自动模式时，生产 unit 必须显式把三项改为 `auto`。

没有任何永续交易所启用时，永续采集整体关闭，现有 yield-only 运行方式继续允许。只启用一家永续交易所时启动失败，因为它不可能证明“至少两家共有”。`auto` 不等于“已经选中所有产品”；产品仍需通过统一资格、别名、exclude 和至少两家条件。

### 6.2 include 与 exclude

新增配置：

| 变量 | 层级 | 空值/`-` | 非空值 |
|---|---|---|---|
| `PERP_UNIVERSE_INCLUDE` | 规范化交易对 | 不限制 | 只允许列出的 canonical key，例如 `BTC-USDT-PERP,ETH-USDT-PERP` |
| `PERP_UNIVERSE_EXCLUDE` | 规范化交易对 | 不排除 | 整组排除列出的 canonical key |
| `BINANCE_PERP_EXCLUDE_SYMBOLS` | Binance 原始 symbol | 不排除 | 只排除 Binance 的精确原始 symbol |
| `OKX_PERP_EXCLUDE_SYMBOLS` | OKX 原始 symbol | 不排除 | 只排除 OKX 的精确原始 symbol |
| `BYBIT_PERP_EXCLUDE_SYMBOLS` | Bybit 原始 symbol | 不排除 | 只排除 Bybit 的精确原始 symbol |

`PERP_ASSET_ALIASES_FILE` 未设置时使用仓库相对路径 `config/perpetual-asset-aliases.json`；显式空字符串、`-`、文件不存在或不可读都属于启动错误。需要“没有 alias”时仍保留该文件并使用空 `aliases` 数组，以便 revision 和审计语义始终存在。`auto` 必须是去除外围空白后的精确小写 token；原始 symbol 和 canonical key 均区分大小写并要求各自的规范形式。

处理顺序固定为：

1. 读取并严格校验完整 catalog；
2. 应用每家交易所的禁用、`auto` 或原始 symbol include；
3. 应用每家交易所的原始 symbol exclude；
4. 应用别名字典并检查同一交易所 canonical 冲突；
5. 按 canonical key 分组并保留不同交易所数量不少于 2 的组；
6. 应用 `PERP_UNIVERSE_INCLUDE`；
7. 应用 `PERP_UNIVERSE_EXCLUDE`，exclude 优先；
8. 校验每个显式 include 最终都存在，否则启动失败；
9. 对 canonical key、交易所和原始 symbol 做稳定排序。

include 和 exclude 同时包含同一个 canonical key 属于配置冲突，直接失败。同一交易所的原始 symbol include 列表和该交易所 exclude 同时包含相同 symbol 也属于配置冲突，不能依赖处理顺序决定结果。

显式原始 symbol include 在该交易所 metadata 中不存在、重复或不合格时直接失败。venue exclude 和 canonical exclude 都是长期 denylist，未命中当前 live catalog/universe 的陈旧项只在启动摘要中告警，不阻止重启。`auto` 模式中的单交易所独有产品正常被忽略。

## 7. Universe 构建与启动原子性

启动顺序必须是：

```text
加载配置和别名字典
  -> 完整读取所有已启用交易所 catalog
  -> 统一资格校验
  -> 原始 include/exclude
  -> canonical 映射与冲突校验
  -> 按不同交易所数量 >= 2 选择
  -> canonical include/exclude
  -> 稳定排序
  -> 计算连接、文件描述符和写入容量预算
  -> 一次性注册全部场内 instrument
  -> 写 canonical mapping
  -> 无网络副作用地构造并完整校验全部 Book、manager、盘口 runtime 和 funding runtime
  -> 写当前 run 的全部 universe member
  -> 写 run 行作为本次启动计划的可见提交标记
  -> 同时启动 sampler、批量 writer 和受门控的网络 runtime
```

任何已启用交易所的 metadata 请求、分页或严格解析失败，都阻止本次启动。不能使用另外两家的完整 catalog 加上一家的残缺 catalog 计算 universe，因为这会把真实的三家共有错误降成两家共有或漏掉产品。

完整选择和容量检查通过前，不允许：

- 注册部分 instrument；
- 写 canonical mapping；
- 建立盘口或资金费率 WebSocket；
- 以已加载的前几页 metadata 生成部分 universe。

API 返回顺序不能影响结果。注册顺序固定为 `(canonical key, exchange, exchange_symbol, venue_contract_version)`，从而使首次分配的 `instrument_id`、分片和启动节奏可重复。

`RegisterInstruments` 在全量模式下必须先校验全部定义和当前批次内的身份冲突，再把所有新 instrument 用一个 ClickHouse batch 写入，不能循环执行数百个独立 insert。历史相同场内身份的 tick、lot 等定义变化仍按既有版本规则登记新 ID，不覆盖旧定义；当前批次同一身份出现互相矛盾的定义则失败。Book、manager 和 runtime 的构造函数只能建立本地状态并验证依赖，不得拨号、发订阅或启动 goroutine。若注册、mapping、runtime 构造校验或 run 提交失败，本次不启动网络 runtime，也不产生本次启动的市场事实；已经被 ClickHouse 接受的幂等 metadata 行允许在下次启动复用。这里保证的是“不会基于部分 universe 启动网络”，不是 ClickHouse 跨表事务。

## 8. 全量盘口连接与限频

### 8.1 为什么要改变一 instrument 一连接

改造前三家盘口 runtime 都以单个 instrument 为单位。自动 universe 可能产生数百个场内 instrument；仅增加拨号间隔能够避免瞬间建连，但不能减少常驻 TCP/WebSocket 数量，也会接近本机文件描述符和交易所连接预算。

因此本次自动全量模式必须同时引入“每家交易所一个 book manager，manager 按稳定顺序把多个 topic 分片到若干 WebSocket 连接”的结构。旧项目中的 Binance/OKX 分片实现可以作为协议和测试参考，但不能迁移其中的 Redis publisher、TTL、套利窗口或丢旧消息逻辑。

第一版使用以下配置和代码硬上限：

| 配置 | 默认值 | 第一版代码硬上限 |
|---|---:|---:|
| `BINANCE_PERP_BOOK_TOPICS_PER_CONNECTION` | 200 | 200 |
| `OKX_PERP_BOOK_TOPICS_PER_CONNECTION` | 20 | 20 |
| `BYBIT_PERP_BOOK_TOPICS_PER_CONNECTION` | 20 | 20 |

配置有效范围是 `1..硬上限`。这些是项目的保守运行上限，不宣称是交易所协议允许的最大值；提高代码硬上限必须重新核验官方规则、消息带宽和实盘 soak。Binance 官方支持在一个连接中订阅多个 stream；OKX 和 Bybit 的订阅请求也支持多个参数。Bybit 仍须满足单连接 `args` JSON 长度不超过 21,000 字符。

### 8.2 共享连接下的故障边界

每个 instrument 仍拥有独立的：

- `Book`；
- 序列状态；
- Binance diff buffer 和 REST snapshot 状态；
- OKX/Bybit snapshot 是否已建立的状态；
- 订阅 generation、确认和有效状态。

每个 shard 只有一个共享的有界消息队列，容量为 `max(4096, 64 × shard_topic_count)`；不为数百个 instrument 永久各分配一个 4096 长度 channel。Binance 仅为当前正在请求 REST snapshot 的 instrument 延迟分配最多 4096 条 diff 的桥接 buffer；OKX/Bybit 的确认前消息使用 shard 总量有界的 pre-ack buffer。所有 shard 队列、pre-ack 和临时 diff buffer 的容量总和还必须通过第 8.5 节的全局内存预算。

共享连接的 reader 先按原始 symbol/topic 路由，再交给对应 instrument collector。规则如下：

- 未知 symbol/topic、重复路由或消息身份不匹配使整个 shard 失效；
- WebSocket 断开、共享 reader 队列溢出或无法归属的解析错误使整个 shard 的所有 Book 立即 invalid；
- 单个 instrument 的序列断档只使该 instrument invalid；Binance 为该 instrument 重新快照，OKX/Bybit 在同一连接上受控重新订阅该 topic；
- 单个 instrument 重新建立 snapshot 前，其他 instrument 可以继续保持有效；
- 任一 resync 控制操作失败或确认超时，则关闭并重建整个 shard；
- 任何队列都不能丢掉旧消息只保留最新消息。

所有连接都只有一个串行 writer，订阅、取消订阅和 ping/pong 写入不能由多个 goroutine 并发。每条消息进入处理器时带当前本地 generation；开始 resync 时先增加 generation 并使 Book invalid，旧 generation 的缓存全部丢弃。

### 8.3 Binance

Binance book manager 连接配置的 public `/ws` endpoint 后，每个 shard 最多订阅 200 个 `<symbol>@depth@100ms` stream。发送时按实际序列化后的整条 JSON 长度切分为不超过 4,000 字节的 `SUBSCRIBE` 批次，每批带唯一整数 ID 并逐批等待 ACK；单 topic 都无法满足字节预算时启动校验失败。ACK 成功前对应 topic 保持 invalid，提前到达的 diff 直接丢弃；确认后才进入 snapshot scheduler。第一版不把全部 stream 拼进 URL，避免 URL 长度成为另一项限制。消息按 payload 中的原始 symbol 路由，每个 instrument 仍执行现有的 WebSocket diff 与 1000 档 REST snapshot 桥接。

4,000 字节是本项目根据 2026-09-10 公共端点实测设置的保守发送上限，并非声称官方文档公开了该常量：超过 4,096 字节的请求当时返回嵌套 `error`，报告在第 4,096 字节处 JSON 截断，并以 close 1008 关闭。200 topics 的连接容量与每条控制请求的字节容量是两个独立预算。错误解析同时接受官方平级 `code/msg` 和实测嵌套 `error.code/error.msg`，不能将错误帧误报为行情字段缺失。

所有 Binance 盘口 shard 和 funding 连接共享新的 WebSocket 建连门控；相邻拨号至少间隔 1 秒并保留正向随机抖动。现有 REST snapshot 门控继续保持相邻请求至少 1 秒，同一时刻只启动一个 snapshot 请求。

若最终有 `N` 个 Binance instrument，首次完成全部本地盘口的理论下限约为 `N` 秒，不把这个过程伪装成瞬时 ready。为避免队尾 target 在等待 snapshot gate 的数分钟内不断缓存，snapshot scheduler 固定执行以下状态机：

1. target 在排队期间保持 invalid，收到的 diff 直接丢弃，不建立 bridge buffer；
2. scheduler 先等待交易所 HTTP 冷却和 1 秒 snapshot pace gate；只有下一步可以立即发出 REST 请求时，才为 target 增加 generation、清空旧状态并开启新的有界 diff buffer；
3. arm buffer 后立即发出单次 1000 档 REST 请求；响应期间只缓存该 generation 的 diff；
4. 以响应 `lastUpdateId` 按现有 Binance 规则丢弃旧 diff、找到覆盖边界并连续应用；成功后 Book 才变为 valid；
5. 请求失败、429/418、无法桥接或 buffer 溢出时终止该 generation 并清空 buffer，target 进入带抖动的退避重试队列；
6. 初次队列和尚未尝试过的 targets 优先于重试队列；重试队列使用轮转公平顺序，单一高 churn 或持续失败的 target 不能阻塞其他 targets；
7. 同一交易所任意时刻最多一个 snapshot 请求和一个临时 diff buffer 处于 in-flight；共享冷却期间不 arm buffer。

这要求 HTTP 客户端把“等待共享冷却”和“实际发送请求”之间的边界暴露给 snapshot scheduler；不能先开启 buffer，再在通用 HTTP 层内部等待长时间 `Retry-After`。

### 8.4 OKX 与 Bybit

OKX shard 每条连接最多 20 个 targets，一条 `SUBSCRIBE` 请求携带全部 `books` args。OKX 对每个 arg 分别返回带 `channel/instId` 的 ACK；每个 target 在自己的 ACK 和第一条 snapshot 都到达后独立变为 valid，ACK 前数据按 target 放入 shard 总量有界的 pre-ack buffer。全部 args 必须在订阅超时内确认，缺少或失败的 ACK 使 shard 重建。

Bybit shard 每条连接最多 20 个 targets，一条带唯一 `req_id` 的 `SUBSCRIBE` 请求携带全部 `orderbook.1000.{symbol}` topics，且编码后的 `args` 数组不超过 21,000 字符。Bybit 对该 `req_id` 返回批级 ACK；ACK 前消息进入 shard 总量有界的 pre-ack buffer，ACK 成功后按接收顺序回放整批，但每个 target 仍须先收到自己的 snapshot 才能 valid。批 ACK 失败、超时或 pre-ack 溢出使 shard 重建。

OKX/Bybit 单 target resync 固定执行：增加 generation 并 invalid，发送单 topic unsubscribe，等待匹配 ACK，再发送带新 request ID 的单 topic subscribe，等待匹配 ACK；在新 generation 的第一条 snapshot 前忽略 delta，snapshot 建立后才恢复。任一步失败或超时都关闭 shard，避免旧订阅的晚到消息污染新 generation。

OKX 继续共用 500 毫秒建连门控；Bybit 继续共用 1 秒建连门控。各交易所还必须执行第 8.5 节的控制消息门控。

### 8.5 连接和本机容量预算

预算必须在最终 universe 产生后计算：

```text
venue_book_connections = ceil(venue_selected_instruments / topics_per_book_connection)
venue_ws_connections   = venue_book_connections + funding_connection_if_enabled
total_ws_connections   = sum(venue_ws_connections) + unchanged_spot_connections
```

启动前必须同时检查：

- 每条连接的 topic 数和订阅 payload 不超过协议上限；
- Bybit 同一 IP 的 market-data 连接硬限制；
- 每家最终 instrument 数不超过对应的 `*_PERP_MAX_INSTRUMENTS=500`，三家合计不超过 `PERP_MAX_TOTAL_INSTRUMENTS=1000`；
- 配置的项目级 `PERP_MAX_TOTAL_WS_CONNECTIONS`，默认 128；
- 进程文件描述符上限要给 ClickHouse、HTTP、日志和系统保留至少 20% 余量；
- 全部共享队列、pre-ack 和临时 diff buffer 的 event slot 总和不超过 `PERP_MAX_TOTAL_BUFFERED_EVENTS=1000000`，dry run 按实际 Go 数据结构给出队列 envelope 的预计字节数下界，明确 `payload_memory_included=false`；该值不含动态 JSON payload、深档 map 等分配，不能作为总内存预算结论，总 RSS 仍须实测；
- 全部 spot 加 perpetual 的 sample sources 不超过 `MARKET_DATA_MAX_SAMPLE_SOURCES=1100`；分钟写入队列至少能够容纳两个完整分钟边界。`MINUTE_QUEUE_CAPACITY` 的单位是尚未写完的 instrument `MinuteBatch` 数，而不是 channel envelope 数；未显式设置时自动取 `max(512, 2 × total_sample_sources)`，显式正值小于 `2 × total_sample_sources` 时启动失败，显式空字符串非法。承载完成分钟的 channel 固定最多缓存 2 个 envelope，并另用加权计数器限制已入队及 writer 已接收但尚未完成的 `MinuteBatch` 总数。

`*_PERP_MAX_INSTRUMENTS` 和 `PERP_MAX_TOTAL_INSTRUMENTS` 是经过实盘验收后才能提高的生产部署容量上限，不是从交易所 symbol 数推导的协议上限。独立验收库可显式设置待验证的更高上限，必须单独记录配置和资源保护；未通过 24 小时验收不得把该上限推广到生产。这样未来新上市产品在重启时不会仅因仍低于连接数上限，就未经容量验证自动扩大内存、网络和磁盘负载。

订阅控制消息另受以下门控约束：

- Binance 每连接由同一 writer 统计客户端发出的 JSON control、ping 和 pong，相邻发送至少 250 毫秒、合计最多 4 条/秒；默认 WebSocket 自动 pong 必须改由该 writer 调度。book/funding 每条请求同时满足最多 200 个 topics 和完整 JSON 不超过 4,000 字节，必要时同连接多批发送、逐批 ACK；
- OKX 每连接相邻 subscribe/unsubscribe 至少 250 毫秒，并使用滚动窗口最多 400 次/小时，低于官方 480 次/小时限制；book 每批最多 20 个 args，funding 同样每批最多 20 个 args；
- Bybit 每连接相邻 JSON control 至少 250 毫秒，每批同时满足最多 20 个 book topics 或资金费现有 21,000 字符限制；
- ping/pong 与订阅写入共用同一个串行 writer；门控实现必须为心跳保留发送时机，不能因大量 resync 控制操作使连接超过 silence timeout。

任一预算不满足时启动失败，错误必须打印最终 universe 数、每家 instrument 数、每家 shard 数、funding 连接数、总连接数和失败的限制。不能静默截取前 N 个交易对。

交易所限制可能调整；实现和部署验收时应以官方文档为准。当前依据包括 [Binance 多 stream 订阅](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams)、[OKX WebSocket 连接与订阅规则](https://www.okx.com/docs-v5/en/#overview-websocket)及 [Bybit WebSocket 连接和 args 限制](https://bybit-exchange.github.io/docs/v5/ws/connect)。

### 8.6 每秒采样和分钟批量写入

盘口编码不变，但 sampler 与 writer 之间增加明确的整分钟完成屏障：

```go
type CompletedMinute struct {
    MinuteTime time.Time
    Batches    []model.MinuteBatch
}
```

`Batches` 一经发送即不可修改，并按 instrument ID 稳定排序。writer 只接收 `CompletedMinute`，不再自行猜测同一 `minute_time` 的最后一个 instrument 是否已经到达。具体调度为：

1. sampler 仍在 UTC 秒边界按稳定 instrument ID 顺序读取每个 Book；一次全 source 采样若运行到下一个秒边界，属于采样容量失败，当前未完成分钟不得伪装完整，sampler 返回终止错误让进程明确重启；
2. 分钟切换时，sampler 在自身锁内一次性收集所有具有第 0 秒有效 anchor 的 instrument `MinuteBatch`，形成一个 `CompletedMinute`；即使没有有效 batch，也为该分钟恰好发送一个空 envelope，明确表示该分钟已经收齐；
3. envelope 入队时占用一个 channel 槽和 `max(1, len(Batches))` 加权容量；加权统计包含 writer 正在处理的 envelope，完成整个 envelope 后才释放对应容量。channel 已有 2 个 envelope、或加权总量将超过 `MINUTE_QUEUE_CAPACITY` 时立即终止；
4. writer 收到完整 envelope 后，以 `MINUTE_WRITE_BATCH_INSTRUMENTS=100` 为固定默认和第一版硬上限分块，不跨分钟拼块；空 envelope 不写事实表，只更新运行指标；
5. 每个块先用一次 ClickHouse batch insert 写入所有 instrument 的秒级 delta，再用一次 batch insert 写入所有 minute visibility rows；
6. delta 成功而 minute rows 失败时重试相同块。表的既有逻辑键和 ReplacingMergeTree 语义保证重试后回放结果不变；
7. writer 必须在下一个分钟边界前排空前一分钟，运行验收要求整分钟所有块写入耗时 p99 小于 30 秒；
8. queue 满或最老待写 minute 的排队时间达到 45 秒是终止错误，不再只记录 `minute writer queue full; dropping minute` 后继续运行。

`PERP_MAX_TOTAL_INSTRUMENTS`、`MARKET_DATA_MAX_SAMPLE_SOURCES`、两分钟 queue 容量、30 秒写入 p99 和 1 秒采样 deadline 共同构成持续吞吐预算。只通过连接数预算不能在生产启用更大 universe；提高生产 instrument/source 上限前必须用独立验收环境新的 24 小时实测证明采样和写入仍满足这两个 deadline。

## 9. Canonical 映射持久化

语义映射历史和每次启动的 active universe 是两个概念，不能用 alias revision 推断当前采集集合。本次新增三个小型 metadata 表，都不进入每秒热写路径。

### 9.1 映射历史

```sql
CREATE TABLE IF NOT EXISTS <db>.instrument_canonical_mapping
(
    instrument_id UInt32,
    mapping_revision FixedString(64),
    canonical_market_key String,
    canonical_base_asset String,
    canonical_quote_asset String,
    canonical_settle_asset String,
    canonical_base_units_per_venue_base_unit Decimal(38,18),
    mapping_kind LowCardinality(String),
    recorded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (instrument_id, mapping_revision)
```

`mapping_revision` 的 canonical bytes 固定为：validated alias entries 按 `(exchange, market_type, exchange_symbol, venue_contract_version)` 排序后，由定字段顺序的 Go struct 使用 `encoding/json` 无缩进编码；外层字段顺序固定为 `schema_version,aliases`，无尾随换行；最终计算 `SHA256("perpetual-canonical-v1\n" + canonical_json)` 并保存小写十六进制。改变算法时必须提高前缀版本。

writer 在插入前读取已有 `(instrument_id,mapping_revision)`；已存在且其余字段一致时跳过，不一致时失败。一次批次重试复用首次生成的同一 `recorded_at` 和完全相同的行，因此 ClickHouse 中即使暂时存在物理重复，`FINAL` 语义仍幂等，`recorded_at` 始终表示该进程首次准备这条 revision 的时间。

每次成功构建 universe 后，程序为“数据库中已有的全部 USDT 线性永续 instrument，加上本次最终选中的新 instrument”计算并写入当前 revision 的 mapping。这样即使一次错误 alias 被删除后某个 instrument 不再入选，它也会得到新的 identity mapping，而不会让错误 mapping 永远成为该 instrument 的最新解释。未被选中、从未产生事实且未登记的 catalog 候选不需要写表。

mapping 必须在本次新增 instrument 注册成功后批量写入；任何 mapping 写入失败都阻止网络 runtime 启动。不存在脱离 run/revision 上下文的“每个 instrument 最新 mapping”：解释当前进程事实时，必须用当前 `perpetual_universe_run.mapping_revision` 连接 mapping；复盘历史选择时必须指定历史 `run_id` 并取该 run 的 revision，或直接显式指定 `mapping_revision`。`recorded_at` 只用于审计某行首次准备写入的时间，不能用于决定权威映射。这样字典从 A 改为 B 后再回滚到内容相同、revision 也相同的 A 时，即使 A 行的时间早于 B，当前 run 仍明确使用 A。实现该表时同步更新 `docs/market-data-storage.md`。

### 9.2 每次启动的 universe

每次非 dry-run 启动生成一个随机 UUID `run_id`。`selection_revision` 覆盖选择算法版本、alias revision、按 venue 名稳定排序的任意长度 `[]VenueSelectionConfig`、canonical include/exclude 以及稳定排序后的最终 `(canonical key, exchange, exchange_symbol, venue_contract_version)` 列表；它使用与 mapping revision 相同的稳定 JSON 编码规则和 `perpetual-universe-v1` 前缀计算 SHA-256。当前 Binance、OKX、Bybit 只是该数组的三个 registry entry；以后增加第四家时必须自动进入配置 JSON 和 hash，不能修改固定三家字段后才生效。相同选择可以有相同 selection revision，但每次进程启动的 run ID 不同。

```sql
CREATE TABLE IF NOT EXISTS <db>.perpetual_universe_member
(
    run_id UUID,
    instrument_id UInt32,
    canonical_market_key String
)
ENGINE = ReplacingMergeTree
ORDER BY (run_id, instrument_id);

CREATE TABLE IF NOT EXISTS <db>.perpetual_universe_run
(
    run_id UUID,
    selection_revision FixedString(64),
    mapping_revision FixedString(64),
    started_at DateTime64(3, 'UTC'),
    selection_config_json String,
    enabled_venues Array(String),
    canonical_include Array(String),
    canonical_exclude Array(String),
    canonical_group_count UInt32,
    instrument_count UInt32
)
ENGINE = ReplacingMergeTree
ORDER BY run_id;
```

`selection_config_json` 保存参与 hash 的 canonical 配置，其中 venue 配置编码为按 venue 名稳定排序的数组；每个 entry 包含 disabled/auto/explicit 状态、显式原始 include 和 venue exclude，外层另含 canonical include/exclude 和所有容量上限。它不得保存密码、代理凭据或其他无关环境变量。

写入顺序固定为 mapping、无网络副作用地构造并完整校验所有 runtime、当前 run 的全部 member、最后写 run 行。run 行是该次选择快照的可见提交标记；只有 runtime 已可启动且 member 批次完整成功后才能出现。失败重试复用同一 run ID 和 started_at；没有对应 run 行的孤立 member 行一律忽略。run 行写入成功后立即启动 sampler、writer 和网络 runtime；构造函数不得在 run 提交前建立网络连接或启动后台任务。

健康检查使用当前进程日志中的 run ID 关联 `perpetual_universe_run/member` 得到 expected instruments，不能把 `instrument_canonical_mapping` 的历次并集当成 active universe。这里的“原子”是网络采集不会基于部分选择启动，不承诺三个 ClickHouse 表之间具有跨表事务；失败遗留的幂等 metadata 或孤立 member 行不会产生市场事实，下次启动可以安全复用或忽略。

## 10. 资金费率的一致性

资金费率 instrument 集合必须从最终盘口选择结果直接派生，不能单独再算一次 universe：

- `FUNDING_ENABLED=false` 时只关闭资金费率，不改变盘口 universe；
- 启用时，每家 funding runtime 的 instruments 必须等于该交易所最终盘口 instruments；
- alias 只用于选择，交易所消息仍按原始 symbol 校验和路由；
- 估算值和实际值仍写具体 `instrument_id`，不写 canonical key 代替；
- Binance funding 每批最多 200 个 topics，且整条编码后 JSON 不超过 4,000 字节，以请求 ID 逐批等待批级 ACK；属于该批的 ACK 前数据进入有界 pre-ack 缓存，确认后才发布；
- OKX funding 每批最多 20 个 args，按返回的 `channel/instId` 等待每个 arg 的 ACK；Bybit 保留现有 21,000 字符分批、批级 ACK 和 pre-ack 规则；
- 三家 funding 订阅和重连都使用第 8.5 节相应的建连与 control-operation gate；任一批失败、确认超时或缓存溢出使该 venue 的全部 funding estimates unavailable 并重建连接；
- 某个交易所 funding 连接失败只使该交易所对应估算 unavailable，不改变盘口有效性；
- 单个 funding topic 静默超过 freshness 阈值时只把该 instrument 的估算标为 unavailable，不因为一个低活跃产品没有新数据而重连整家 funding。Bybit 已建立 snapshot 的字段缓存可保留用于合成后续真实 delta；只有真实 ticker/funding 消息及其源时间才能恢复可用性，ping/pong 不得推进源时间或复活估算。连接失效、ACK 失败和队列溢出仍失效整条连接的估算；
- 实际结算 REST worker 继续按交易所串行并共用现有限频 gate。

[Bybit ticker 文档](https://bybit-exchange.github.io/docs/v5/websocket/public/ticker) 规定 snapshot/delta 以及缺省字段代表未改变；列出的推送频率不能当作每个 topic 必有心跳的保证。单 topic 静默隔离不改变资金费率严格解析、来源时间和不使用过期估算的不变量。

## 11. 运行可观测性

collector 新增 `-print-perp-universe` dry-run 参数。它加载真实配置和别名字典、访问已启用交易所的 metadata、执行完整选择与容量计算，然后向标准输出写稳定排序的 JSON，但不连接 ClickHouse、不登记 instrument、不建立 WebSocket，也不修改任何外部状态。全部 metadata、字典、选择和预算校验成功时退出码为 0，任一步失败时错误写标准错误且退出码非 0。部署切换到 `auto` 前必须先运行该命令。

dry-run JSON 包含完整最终 membership，以及只供人工检查、绝不参与自动选择的 `potential_aliases`。第一版至少报告“未通过 exact base 分组、但去除纯数字数量前缀后与另一交易所 base 相同”的候选，并同时给出双方原始 symbol、base、contract multiplier 和 tick/quantity step。实现阶段必须用三家当前真实 catalog 审核这些候选：能从场内 metadata/官方合约说明确认底层和单位的写入初始 alias 文件，不能确认的保留为未匹配并在部署记录中说明，不能因为名称相似直接合并。

启动时必须输出一条结构化 universe 摘要，至少包含：

- alias revision；
- run ID 和 selection revision；
- 已启用和已禁用交易所；
- 每家 metadata 合格候选数；
- 每家显式 include、exclude 后的候选数；
- identity 映射数、alias 命中数、未命中陈旧 alias 数；
- canonical 分组总数、因少于两家被忽略的组数、最终组数；
- 每家最终 instrument 数；
- 每家 book shard 数、funding 连接数和预计总 WebSocket 数；
- Binance 最短预计 snapshot 启动时间；
- 项目连接预算和文件描述符预算结果。

不得把数百个完整 symbol 全部重复打印到正常日志。完整选择结果在 debug 日志中按稳定顺序输出，正常日志只输出计数、配置 exclude 和异常冲突。

每个 book manager 和 funding runtime 增加只读 `HealthSnapshot()`；app 每 60 秒输出一条 `perpetual_runtime_health` 结构化日志，携带当前 run ID 和下列汇总。第一版不新增 HTTP 管理端口。现有健康说明不能再要求操作人员按三个手工 symbol 列表逐一检查；expected instruments 来自当前 run 的 `perpetual_universe_member`，实时状态来自进程内 health snapshot，不能从 canonical mapping 猜测。日志分别报告：

秒级采样若因 GC、调度或单本盘口锁竞争越过本秒截止时间，只把尚未完成的来源在该秒标为无效，并增加 `SampleOverruns`、`MissedSamples`、`LastOverrunAt` 和 `LastOverrunID`；不得退出整个 collector。已在截止时间前取得的来源仍可作为该秒有效样本。这样保留 `valid_bitmap` 的缺失语义，也避免一次局部超时触发所有 WebSocket 冷启动。

- expected instruments；
- 已建立有效盘口的 instruments；
- resyncing/invalid instruments；
- 每家已连接 shard；
- 最老有效来源更新时间（`OldestSourceTime`），不是每秒 sampler 的采样时间；
- 最近 metadata/universe 构建时间。

第一版 universe 只在启动时计算。新上市、下架或 metadata 改变不会热增删 runtime；运行说明必须明确“重启才重新发现”。运行中产品停止推送时继续沿用现有 invalid 和重连规则，不能把旧盘口当有效数据。

## 12. 实现分层

实现按以下职责分层；文件名允许在不改变边界的前提下调整：

```text
internal/universe/
  key.go                 canonical key 定类型与严格解析
  aliases.go             JSON 字典、revision 和单位因子校验
  perpetual.go           通用 >=2 venue 选择器

internal/app/
  perpetual_venues.go    venue spec registry 和启动装配

internal/exchange/<venue>/
  book_manager.go        该交易所多 topic 分片、路由和重连

internal/storage/clickhouse/
  canonical_mapping.go   映射历史批量写入/查询
  writer.go              同一分钟多个 instrument 的分块批量写入
```

venue spec 至少提供：

```go
type PerpetualVenueSpec struct {
    Name             string
    Selection        VenueSelectionConfig
    FetchCatalog     func(context.Context) ([]model.Instrument, error)
    BuildBookManager func([]model.Instrument, map[uint32]*orderbook.Book) component
    Funding          FundingRuntimeFactory
    ConnectionBudget func(selected int, funding bool) (ConnectionBudget, error)
}
```

这只是 app 装配层的统一描述；不要把 Binance REST/WS 桥接、OKX checksum、Bybit `u/seq` 等协议差异塞进公共 universe 包。

## 13. 测试要求

### 13.1 Universe 和配置

- 两家有、第三家无：选择两家；
- 三家都有：选择三家；
- 未来第四家加入后仍按不同交易所计数 `>=2`；
- 同一交易所重复 instrument 不增加 venue 计数并触发冲突；
- 只有一家有：`auto` 模式正常忽略；
- 一个已启用交易所 metadata 失败或分页不完整：整个启动在注册前失败；
- API 返回随机顺序仍产生完全相同的选择、注册和分片顺序；
- Binance `quoteAsset!=USDT` 即使 margin 为 USDT 也不能进入候选；
- spot 配置和 spot 选择测试结果不变；
- 未设置保持旧默认，显式空值失败，`auto`、`-`、原始 symbol 列表三态严格区分；
- 三家都禁用时 spot/yield-only 可以运行，只启用一家永续交易所时失败；
- canonical include 不存在、不满两家或与 exclude 冲突时失败；
- 同一 raw symbol 同时出现在 venue include/exclude 时失败，陈旧 venue exclude 只告警；
- venue raw exclude 后若仍有两家则保留剩余两家，否则整组不采集。

### 13.2 别名和单位

- 同名 base 使用 identity mapping 和 factor 1；
- `1000PEPE` 显式映射为 `PEPE` 且 factor 1000；
- 不配置字典时 `1000PEPE` 不会靠字符串猜测匹配 `PEPE`；
- 重复 key、未知字段、空值、非正或超精度 factor 失败；
- 同一 exchange/symbol 的两个 `venue_contract_version` 可以配置不同 factor，当前版本只命中自己的精确四元组；缺少当前版本 entry 时使用 identity，不得命中旧版本或模糊猜测；
- 命中 entry 的 expected 字段与 metadata 不同则失败；
- 未命中的陈旧 entry 只告警；
- alias 造成同一 venue canonical 冲突时失败；
- 第 5.3 节价格、数量公式用十进制定点数精确验证；
- OKX contract multiplier 与 alias factor 分别相乘，不互相覆盖；
- 修正 alias 产生新 mapping revision，旧盘口事实和旧 mapping 行保留；
- mapping revision 经历 A → B → A 回滚时，当前 run 通过自身 revision 使用 A，不受 B 的较晚 `recorded_at` 影响；
- canonical JSON 排序、算法前缀和 SHA-256 fixture 固定；
- 同一 mapping revision 重启不增加有差异的逻辑行；
- 同一 alias revision 下 explicit/auto、include/exclude 或 membership 变化会产生新的 selection revision 和 run ID；
- 加入第四家 venue 后，其排序后的 `VenueSelectionConfig` 自动进入 `selection_config_json` 和 selection revision；
- run 行最后写入，缺少 run 行的孤立 member 不会成为 active universe。
- 任一 runtime 构造或本地依赖校验失败时不写 run 行，也不拨号或启动 goroutine；

### 13.3 分片、序列和失败

- 每家选择数位于 `0、1、上限、上限+1、2×上限` 时分片正确且顺序稳定；
- Binance/Bybit 批级 ACK 与 OKX 逐 arg ACK 分别按设计激活，任何 target 在自己的 snapshot 前都不 ready；
- ACK 失败、缺失、重复、超时和 shard 总量 pre-ack 溢出按设计失效；
- 未知 topic 或共享 reader 溢出使整个 shard invalid；
- 单 instrument 断档只使该 instrument invalid，重新建立 snapshot 后恢复；
- OKX/Bybit resync 严格执行新 generation、unsubscribe ACK、subscribe ACK、snapshot 顺序，旧消息不能污染新状态；
- shard 断线使 shard 内所有 Book invalid，其他 shard 不受影响；
- Binance 多 shard 共用 REST snapshot gate，相邻 1000 档请求至少 1 秒；
- Binance target 等待 snapshot slot 时不缓存，arm 后才缓存；初次队列优先和轮转重试不会饥饿其他 target；
- Binance、OKX、Bybit 的所有 book/funding shard 共享各自建连 gate；
- 三家 control-operation gate 的间隔、滚动窗口、心跳保留和 funding 分批均通过确定性测试；
- 同时断网恢复时重连仍经过共享门控和随机抖动；
- 最终 universe 的 instrument、sample source、连接、FD、event slot 或 queue 任一预算超过限制时在注册和拨号前失败；
- 分钟边界为每分钟恰好发送一个不可变 `CompletedMinute`；零有效 anchor、部分 instrument 无 anchor 和满 universe 时，writer 都能明确知道该分钟已收齐并只写 envelope 中的有效 batch；
- 完整 envelope 内的全部 instrument 使用有限次数的分块 ClickHouse insert，部分重试不改变最终回放结果；
- 自动 queue 容量不小于两分钟 instrument 数，显式不足时启动失败；
- queue 满、45 秒 backlog 或一次采样跨过下一秒都成为终止错误，不静默形成看似完整的数据；
- 每 60 秒 health snapshot 的 expected/ready/invalid/shard 数与当前 run membership 一致；
- 现有三家 sequence、sampler、writer 和 replay 测试继续通过。

### 13.4 实盘验收

生产全量启用前依次完成：

1. 只构建 catalog/universe 的 dry run，记录三家候选数、最终组数、alias 命中、未匹配候选和预计连接数；逐项审核数字数量前缀等潜在 alias，确认项写入初始字典，不能确认的保持不匹配并记录原因；
2. 独立开发库启用 canonical include，仅采集 BTC、ETH 和至少一个显式 alias 产品；
3. 验证 alias 产品的原始价格、数量、contract multiplier 和换算公式；
4. 扩到最终 universe，至少连续运行 24 小时；
5. 记录每家重连次数、sequence/resync 次数、队列峰值、每秒采样耗时、分钟批量写入耗时、CPU、内存、网络、文件描述符和 ClickHouse 写入错误；采样 p99 必须稳定小于 1 秒；
6. 分别对活跃与非活跃产品验证 100 次随机秒回放；
7. 测量 24 小时压缩后新增磁盘占用，据此更新容量预测和告警阈值；
8. 确认没有 HTTP 418/429、Bybit `access too frequent`、连接预算错误或持续 invalid shard 后再切换生产 unit 为三家 `auto`。

初始字典及单位核验依据见 [config/perpetual-asset-aliases.md](../config/perpetual-asset-aliases.md)。`collector -print-perp-catalogs` 只输出严格解析后的场内目录，用于维护带合约版本的精确规则；不会使用字典、连接数据库或订阅盘口。`perp-check -database <db> -run-id <uuid> -min-duration 24h` 只读检查当前 run 成员的有效秒、资金费率、物理压缩空间和每家活跃/低活跃产品各 100 次随机秒回放。它的 `passed_checks` 仅表示数据库及回放检查通过，不替代连续进程运行、主机资源、请求限制和 sampler/writer deadline 的验收。

## 14. 部署与回退

升级分两步，避免代码部署本身自动扩流：

1. 部署支持三态配置、自动 universe、alias、分片和新 mapping 表的二进制；生产 unit 仍保留当前 BTC 原始 symbol 列表，验证行为与升级前一致；
2. 完成第 13.4 节后，把三家 `*_PERP_SYMBOLS` 改成 `auto`，先运行 dry run，再重启 collector 正式启用。

回退时把三个变量恢复为精确 BTC 原始 symbol 列表并重启。自动模式已登记的其他 instrument 和历史事实不删除；它们停止产生新时间点，只作为历史保留。回退不执行表删除、数据迁移或 instrument ID 复用。

## 15. 完成条件

满足以下条件才算完成：

1. 最终 universe 对任意数量交易所统一执行“不同交易所数量不少于 2”；
2. 三家 USDT 线性永续 eligibility 严格一致，残缺 metadata 不产生部分 universe；
3. 异名合约只通过显式字典匹配，单位因子可核验且映射 revision 已持久化；
4. 三家最终盘口集合与启用时的资金费率集合一致；
5. 全量连接采用分片多 topic transport，不按 instrument 建立数百条连接；
6. REST 快照、WebSocket 建连、订阅和重连不会产生请求风暴；
7. 任意断档和过载都 fail closed，不生成看似有效的盘口；
8. 超过连接或本机容量预算时明确失败，不静默漏采；
9. BTC 显式列表兼容模式、spot、存储编码和查询回放行为不回归；
10. 24 小时全量实盘验收给出磁盘、CPU、内存、网络、连接与回放数据；
11. 实现完成时同步更新 `README.md`、`docs/architecture.md`、`docs/implementation-design.md`、`docs/market-data-storage.md`、`docs/runtime-operations.md` 和 `docs/bybit-usdt-perpetual-market-data.md`，删除其中“一 instrument 一连接”、手工 symbol 健康检查及旧 Bybit 连接预算等已被取代的描述。
