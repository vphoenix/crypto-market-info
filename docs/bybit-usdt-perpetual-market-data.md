# Bybit USDT 线性永续市场数据采集设计

日期：2026-09-05

## 结论

第一阶段只增加 Bybit 的 USDT 结算线性永续，不处理 Kraken Derivatives、Bybit 现货、USDC 永续、反向永续和交割合约。

Bybit 接入后保存的数据与 Binance、OKX 永续一致：

- 每秒前 50 档标准化订单簿；
- 每分钟第 0 秒完整快照及分钟内秒级差量；
- 每个 UTC 小时的最新有效资金费率估算；
- 到达结算时间后由历史接口确认的实际资金费率。

现有 `Book`、sampler、分钟缓冲、ClickHouse 盘口事实表、`funding_rate_hourly`、资金费率 scheduler 和确认 worker 全部复用，盘口及资金费率的编码规则不变。新增工作主要位于 `internal/exchange/bybit`、配置和应用装配层。

但这不只是机械地改接口名。除适配器外，还需要一次 instrument 版本身份迁移和一个向后兼容的 HTTP 响应扩展。Bybit 有六项必须单独处理的差异：

1. `category=linear` 还包含 USDC 合约和 USDT 交割合约，必须继续按响应字段筛选；
2. 订单簿由同一 WebSocket 内的 `snapshot` 和 `delta` 组成，`u=1` 或新的 `snapshot` 都要求无条件覆盖本地盘口；
3. ticker 的 `delta` 会省略未变化字段，不能把缺失的 `fundingRate` 或 `nextFundingTime` 当作空值；
4. REST 即使返回 HTTP 2xx，也必须继续检查 `retCode`；Bybit 的 HTTP 403 限频语义还要求扩展通用 HTTP 冷却配置。
5. Bybit 提供 `symbolId` 和 `launchTime`，现有 instrument 定义却没有显式合约版本字段；若同 symbol、同规格下架后重上，旧逻辑会错误复用历史 ID；
6. metadata 启动限频不能只靠进程内 gate 后退出，否则 systemd 30 秒重启会绕过 Bybit 要求的 10 分钟冷却。

## 范围和非目标

### 本次包含

- 发现并登记已配置的 Bybit USDT 线性永续 instrument；
- 采集 `orderbook.1000.{symbol}`，在内存保留 1000 档并向现有 sampler 提供前 50 档；
- 采集 `tickers.{symbol}` 中的估算资金费率和下一结算时间；
- 调用资金费率历史接口确认实际结算值；
- 接入现有的启动补查、失效标记、重连、采样、存储和回放流程；
- 增加离线协议测试和最小实盘验收。

### 本次不包含

- Kraken Derivatives；
- Bybit 现货、USDC 永续、反向永续、交割合约和 pre-market 合约；
- 私有 API、API key、账户数据或交易执行；
- RPI 订单。Bybit 标准公开订单簿明确不包含 RPI，本项目保存的是该公开订单簿的前 50 档；
- 合并或重构 Binance、OKX 适配器；
- 新表、新消息队列或新的资金费率调度模型；`instrument` 会增加一个版本身份列，但盘口和资金费率事实表不变。

## 官方协议依据

- [Instruments Info](https://bybit-exchange.github.io/docs/v5/market/instrument)：`linear` 当前超过 500 个 instrument，必须分页；响应提供 `contractType`、`status`、`settleCoin`、`tickSize`、`qtyStep` 和 `fundingInterval`。
- [枚举定义](https://bybit-exchange.github.io/docs/v5/enum)：`linear` 是产品大类，`LinearPerpetual` 才是线性永续合约类型。
- [公开 WebSocket 连接](https://bybit-exchange.github.io/docs/v5/ws/connect)：线性产品使用 `/v5/public/linear`，公共频道无需鉴权，心跳使用 JSON `ping`，并限制建连频率。
- [Orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)：1000 档每 200 ms 推送；首次和重置消息为 `snapshot`，后续为绝对数量 `delta`；数量 0 表示删除；`u=1` 表示服务重启后的全量覆盖。
- [REST Orderbook](https://bybit-exchange.github.io/docs/v5/market/orderbook)：REST 的 `u` 顺序产生并与 1000 档 WebSocket 的 `u` 对应，可用于验收和排障核验。
- [Ticker](https://bybit-exchange.github.io/docs/v5/websocket/public/ticker)：衍生品 ticker 每 100 ms 推送，`delta` 中未出现的字段表示未变化。
- [Funding Rate History](https://bybit-exchange.github.io/docs/v5/market/history-fund-rate)：历史接口返回 `fundingRate` 和毫秒级 `fundingRateTimestamp`，不同 symbol 可以有不同结算间隔。
- [限频规则](https://bybit-exchange.github.io/docs/v5/rate-limit)与[错误码](https://bybit-exchange.github.io/docs/v5/error)：HTTP 403 可能表示 IP 访问过频，需停止 HTTP 请求至少 10 分钟；JSON `retCode=10006` 和 HTTP 429 也表示限频。

## Instrument 发现和标准化

### 请求和分页

启动时调用：

```text
GET /v5/market/instruments-info?category=linear&limit=1000
```

若 `result.nextPageCursor` 非空，将解码后的 cursor 作为 `url.Values` 的一个值重新编码后继续请求，直到 cursor 为空；不能直接拼接，也不能手工二次转义。实现必须拒绝重复 cursor，避免异常响应造成无限循环。不能依赖接口默认的 500 条，因为官方已经说明 `linear` instrument 数量超过 500。

请求可以附加 `status=Trading` 减少返回量，但响应端仍要完整校验，不把请求参数当作数据证明。

### 精确筛选条件

只有同时满足以下条件的响应行才能进入可选 instrument 集合：

```text
result.category == "linear"
contractType   == "LinearPerpetual"
status         == "Trading"
quoteCoin      == "USDT"
settleCoin     == "USDT"
isPreListing   == false
```

同时检查 `quoteCoin` 和 `settleCoin`，是为了把“USDT 报价且 USDT 结算”写成显式身份约束，而不是依赖 symbol 后缀。

配置中出现但筛选结果不存在的 symbol 必须令启动失败，不能静默跳过或误选同名的其他产品。

### 字段映射

| Bybit 字段 | 标准字段 | 规则 |
|---|---|---|
| 固定值 | `exchange` | `Bybit` |
| 固定值 | `market_type` | `perpetual` |
| `symbol` | `exchange_symbol` | 原样保存，不自行拆 symbol |
| `symbolId` + `launchTime` | `venue_contract_version` | 两个字段都必须存在且为正整数，规范化成稳定字符串；不把版本拼进交易所 symbol |
| `baseCoin` | `base_asset` | 原样保存 |
| `quoteCoin` | `quote_asset` | 必须为 `USDT` |
| `settleCoin` | `settle_asset` | 必须为 `USDT` |
| 固定值 `1` | `contract_multiplier` | USDT 线性合约的订单簿数量按基础币数量标准化 |
| `priceFilter.tickSize` | `price_tick_size` | 严格十进制解析，必须为正数且 scale 不超过 18 |
| `lotSizeFilter.qtyStep` | `quantity_step_size` | 严格十进制解析，必须为正数且 scale 不超过 18 |
| 固定空值 | `expiry_time` | 永续合约没有到期时刻；不能使用可能存在的 delisting 时间填充 |

`fundingInterval` 必须解析为正整数分钟且可被 60 整除，但本阶段不写入公共 `Instrument` 或数据库，也不与 ticker 建立隐藏的跨接口状态。实际调度继续以每条 ticker 给出的 `nextFundingTime` 为准，所以 1、4、8 小时等不同结算间隔不需要修改公共 worker。

REST 外层必须严格验证：JSON 可解析、`retCode == 0`、`result` 非空、`result.category == linear`、`list` 是非空数组、同一 symbol 不重复。`isPreListing` 在线类型中使用 `*bool` 或等价的 presence-aware 类型，字段缺失必须报错，不能让 Go 的 `bool` 零值把缺失伪装成 `false`。其他用于身份、精度和结算间隔的必需字段也要区分“缺失”和“零值”。所有价格、数量和费率字段都从字符串直接进入十进制定点数路径，不经过 `float64`。

## 合约版本身份和迁移

这是本次唯一的表结构变化，也是加入 Bybit 时发现的现有模型缺口。`instrument_id` 必须区分合约版本，但当前 `SameDefinition` 只比较交易所、市场类型、symbol、资产、乘数、tick、lot 和 expiry；这些字段不能区分“同 symbol、同规格的重新上市”。Bybit 官方提供 `symbolId` 和 `launchTime`，也没有承诺 symbol 永不复用，因此不能忽略这个问题。

### 模型和表结构

为 `model.Instrument` 增加：

```text
VenueContractVersion string
```

为 `instrument` 表增加：

```sql
venue_contract_version String
```

新建表 DDL 直接包含该列；`InitSchema` 还要执行幂等的 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS venue_contract_version String DEFAULT ''` 兼容已有数据库。历史行取得空字符串默认值。该字段进入 `SameDefinition` 的精确比较、instrument 的 SELECT/INSERT、fixtures、注册测试和数据字典。它是交易所版本身份的规范化字符串，不是展示名称，也不能替代原始 `exchange_symbol`。

新采集的衍生品定义必须具有非空版本：

- Bybit USDT 永续：presence-aware 且均大于 0 的 `symbolId:launchTime`；
- Binance 永续：[exchange info](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information) 中 presence-aware 且大于 0 的 `onboardDate`；
- OKX SWAP：[public instruments](https://app.okx.com/docs-v5/en/) 中 presence-aware 且大于 0 的 `listTime`。

现货没有“合约版本”，本阶段允许为空。历史衍生品行也允许为空，以便读取已有数据库；空值只代表迁移前 legacy 身份，不能成为以后合法的新衍生品版本。所有新登记的启用衍生品必须在 metadata parser 和注册前检查中同时校验版本非空。不能用当前 metadata 猜填旧行，因为数据库可能已经跨过一次无法证明的下架/重上。

### 首次部署行为

首次引入该字段时，当前启用的 Binance、OKX 永续会因为版本身份从空变为非空而各自登记新的 `instrument_id`；Bybit 从第一条记录开始就使用非空版本。旧盘口和资金费率事实仍关联旧 ID，不改写、不搬迁。部署时必须明确记录这个 ID 切换点。

为避免 ID 切换使最近 24 小时待确认资金费率被遗漏，启动补查建立独立的 `backfillInstruments` 查找集合：除当前注册 instrument 外，再从 instrument 表纳入与当前配置具有相同 `(exchange, market_type, exchange_symbol)` 且 base/quote/settle 身份相容的历史永续版本，只把实际值写回任务原来的旧 `instrument_id`。scheduler、ticker 和盘口仍只能使用本次注册的当前 ID，不能把旧版本加入实时采集。这个改动只扩大 backfill 的 instrument 查找集合，不改变 24 小时窗口、任务键、串行 worker 或重试时点。

架构文档中的新鲜度查询也要按 `(exchange, market_type, exchange_symbol)` 只把最高 `instrument_id` 称为“latest registered version”，再与当前运行配置交叉核对后才视为当前启用版本；旧版本仍显示为历史，但不要求继续产生新数据。这个规则依赖“新版本 ID 单调增加”的现有注册方式，不能替代缺失的 active/effective 字段。

这次迁移只能保证从部署切换点起，新采集的三家永续具有显式版本身份。迁移前旧 ID 内是否已经混入同 symbol、同规格的多次上市，在缺少历史版本字段的情况下无法恢复，必须记录为不可逆的历史限制，不能事后猜分。

## 订单簿设计

### 订阅深度和连接模型

每个已配置 instrument 维持一条盘口 WebSocket，订阅：

```text
orderbook.1000.{symbol}
```

选择 1000 档而不是 50 档，是因为项目不能只保留输出的 50 档：最佳价位被删除后，需要更深的本地状态补入前 50。1000 档是 Bybit 官方支持的最深标准频道，每 200 ms 推送，仍明显快于当前每秒采样目标；相较 200 档，它给高 churn 盘口更大的本地缓冲，内存增加可控，而且 Bybit REST 盘口的 `u` 明确对应 1000 档 WebSocket，便于实盘验收和排障时交叉核验。内存 `Book` 的 `retainedDepth` 对 Bybit 固定为 1000，落库仍只有前 50 档；REST 只用于人工核验，不进入实时重同步链路。

第一阶段沿用现有“一 instrument 一条盘口连接”的故障隔离方式，不把多个盘口合并到共享连接。盘口连接与单条资金费率连接共用一个 Bybit 建连门控，相邻建连至少间隔 1 秒，并保留带随机抖动的指数退避。这样即使同时启动或网络恢复，也不会接近官方的 500 次/5 分钟建连限制。

### 消息映射

| Bybit 字段 | 本地含义 | 规则 |
|---|---|---|
| `topic` | 订阅身份 | 必须精确等于 `orderbook.1000.{配置 symbol}` |
| `type` | 更新类型 | 只接受 `snapshot` 或 `delta` |
| `data.s` | symbol | 必须与 instrument 一致 |
| `data.u` | `Book.Sequence` | 正整数 update ID |
| `data.seq` | 交叉频道顺序 | 只做合法性和单调性观测，不作为本地连续性序列 |
| 顶层 `cts` | `SourceTime` | 撮合引擎产生数据的毫秒 UTC 时间 |
| `data.b` / `data.a` | 买卖盘变化 | 每行必须恰为 `[price, qty]` 两个十进制字符串 |

`price_tick = price / tickSize`，`qty_lot = qty / qtyStep`。除法必须整除，结果必须分别落入正 `int64` 和非负 `uint64`；snapshot 中数量必须大于 0，delta 中 0 才表示删除。单条消息同侧出现重复价格、价格不对齐 tick、数量不对齐 lot、盘口锁定或交叉，都视为协议/数据错误。

使用 `cts` 而不是顶层 `ts` 作为标准 `SourceTime`，因为前者是撮合引擎时间；`ts` 仍严格解析并可用于日志和延迟指标，但不进入现有存储模型。

### 状态机和连续性

连接建立后先将 `Book` 标为 `resyncing`。处理规则如下：

1. 收到 `snapshot`：无论当前是否已有有效盘口，完整替换本地 1000 档状态，并把 `u` 设为当前 sequence；
2. 尚未收到 snapshot 前收到 delta：连接失败并重连；
3. 收到 delta：要求 `u == last_u + 1`，再把绝对数量变化原子应用到本地盘口；
4. 收到 `u=1`：只允许消息类型为 snapshot，并按步骤 1 覆盖；
5. `u` 回退、重复或跳号：立即把盘口标为 invalid，断开并等待新连接的 snapshot；
6. 任意时刻收到新的 snapshot：它是服务端给出的重新同步边界，不要求与旧 `u` 连续；
7. 解析错误、订阅失败、读超时、更新队列溢出或应用后盘口无效：均停止当前连接，invalid 期间 sampler 通过 `valid_bitmap` 记录缺失，不能沿用旧盘口；
8. 重连后不调用 REST 盘口接口。Bybit WebSocket 自身首先下发 snapshot，因此不需要设计 REST/WS 序列桥接。

`seq` 用于比较不同深度频道生成先后；本阶段每个 instrument 只订阅一个深度，不能拿它代替 `u`。实现可以记录最后的 `seq` 并拒绝同一连接内回退，但不能要求 `seq == last_seq + 1`。

官方 WebSocket 文档只把 `u` 定义为 Update ID，REST 盘口文档说明它顺序产生且与 1000 档 WebSocket 对应，但没有明确承诺相邻 WebSocket 消息一定满足 `last_u + 1`。这里的逐一连续要求，是为满足项目 fail-closed 断档不变量采用的待验证协议假设。上线前必须对真实 1000 档流做长时 soak 并记录相邻 `u` 差值。如果观测到可重复的合法跳号，停止上线并重新确认可验证的连续性方案；不能为了提高在线率直接放宽成“只要递增”。

### 心跳和控制消息

- 每 20 秒发送 `{"op":"ping"}`；
- 任意合法数据或 `pong` 都刷新读超时，默认 silence timeout 为 45 秒；
- `success=true, op=subscribe` 才表示订阅成功；`success=false`、未知控制响应或失败 topic 都令连接失败；
- `success=true, op=ping, ret_msg=pong` 只作为心跳响应，不进入数据 parser；
- 使用有界更新队列，满队列不能丢消息后继续产生“有效”盘口。

## 资金费率设计

### 估算值 WebSocket

每个交易所继续只使用一条资金费率连接。Bybit 连接订阅所有启用永续的：

```text
tickers.{symbol}
```

订阅 topic 按请求分批，使每个 `args` JSON 数组不超过官方的 21,000 字符限制；所有批次必须收到成功响应，任一失败都重建整条连接。

映射为现有 `FundingEstimate`：

| Bybit 字段 | 标准字段 |
|---|---|
| instrument 映射 | `InstrumentID` |
| `data.nextFundingTime` | `FundingTime`，毫秒 UTC |
| `data.fundingRate` | `Rate`，严格十进制 |
| 顶层 `ts` | `SourceTime`，毫秒 UTC |

Bybit ticker 同时有 snapshot 和 delta，delta 会省略没有变化的字段。因此 runtime 必须按 symbol 维护连接内缓存：

- snapshot 重置该 symbol 的缓存；
- delta 只覆盖实际出现的字段；
- snapshot 之前收到 delta 属于无基线状态，连接失败并重连；
- 只有缓存同时具有合法的 `fundingRate`、`nextFundingTime` 和 `fundingIntervalHour` 时，才生成新估算；
- metadata 的 `fundingInterval` 必须是可被 60 整除的正整数分钟，ticker 的 `fundingIntervalHour` 必须是正整数小时；本阶段不把 interval 加入公共 `Instrument`，也不在两个端点间保存隐式共享状态。实际调度唯一信任每条状态给出的 `nextFundingTime`；
- 只有该 symbol 已收到完整 snapshot 且缓存仍完整有效时，每一条合法 ticker 才使用当前消息的 `ts` 生成估算，即使该 delta 完全没有 funding 字段；这表示“Bybit 在该次 ticker 状态下仍维持这个值”，并防止公共 2 分钟 freshness 窗口把长期未变化的费率误判为陈旧；
- 同一 `(instrument_id, funding_time)` 的旧 source time 会被现有 `EstimateStore` 忽略；
- 连接断开时，对该连接覆盖的全部 instrument 调用 `MarkUnavailable`。

每次产生完整估算后，继续调用现有 `ConfirmationWorker.Schedule`。重复 ticker 不会产生重复任务，因为 worker 已按 `(instrument_id, funding_time)` 去重。UTC 整点写估算、2/5/15/60 分钟确认、结算边界优先保留上一目标等规则均不变。

数据库保存的是 Bybit 针对自身结算周期公布的原始 funding rate，不把 1 小时费率与 8 小时费率换算成同一周期。查询或套利分析层在跨 instrument 比较前必须结合相邻 `funding_time` 或当时有效的产品规则统一周期口径；本次不新增 funding interval 规则历史表。

### 实际值 REST

确认接口：

```text
GET /v5/market/funding/history
    ?category=linear
    &symbol={symbol}
    &endTime={target_ms+1}
    &limit=1
```

只传 `endTime` 是官方允许的；`target_ms + 1` 不是官方要求，而是本地防御性选择，用于避免目标毫秒恰好落在未明确说明开闭区间的边界。fixture 只负责锁定本地 URL 和精确匹配行为；真实接口的边界语义必须在跨结算点验收中确认。响应必须再次校验 `result.category == linear`。非空结果中出现其他 symbol 属于响应身份错误，不能跳过后伪装成尚未结算；身份正确但找不到 `fundingRateTimestamp == target_ms` 的精确目标时才返回 `found=false`，由现有 worker 按既定时点重试。不能把“最新一条”直接当作目标实际值。

匹配成功后映射为：

```text
HourTime   = target.UTC().Truncate(time.Hour)
FundingTime = fundingRateTimestamp 的毫秒 UTC
Rate       = fundingRate 的严格十进制值
IsActual   = true
```

单次 provider 调用的 `MaxAttempts` 保持为 1，避免公共 HTTP 重试与确认 worker 的 2/5/15/60 分钟计划叠加。启动时仍只补查 ClickHouse 中最近 24 小时“已有估算但无实际值”的记录；这次不扩展成交易所全量历史导入，也不补造 collector 停机期间从未产生的估算行。

## REST 错误和限频

所有 Bybit REST 端点必须先检查 HTTP，再检查统一 JSON envelope：

- `retCode == 0` 才允许解析 `result`；
- 非零 `retCode` 返回包含 code、message 和 endpoint 的错误，不把空 `result` 当作“没有数据”；
- `retCode=10006` 读取 `X-Bapi-Limit-Reset-Timestamp`，按其毫秒时间戳触发客户端共享冷却并返回错误；响应头缺失、非法或不晚于当前时间时使用 1 分钟 fallback；
- HTTP 429 使用已有 `Retry-After`/fallback 冷却；
- HTTP 403 对 Bybit 按官方 IP 限频要求触发至少 10 分钟共享冷却，且本次调用立即失败，不做短间隔重试。

这需要对 `internal/exchange/http.go` 做两项向后兼容的小扩展：

1. 增加返回 payload、status 和响应头的 `GetResponse`（名称可按实现调整），现有 `Get` 继续包装它并保持原签名；Bybit 用响应头处理 HTTP 2xx 下的 `retCode=10006`；
2. 让 `HTTPRetryConfig` 可配置“哪些 HTTP 状态属于限频”，并支持按状态设置独立 fallback。默认仍为 Binance 当前使用的 429/418 和既有 1 分钟 fallback；Bybit client 额外加入 403，并只把 403 的 fallback 设为 10 分钟。实现可以使用 `map[status]duration` 或等价策略，但不能用单一字段把 Bybit 的 429 fallback 也意外改成 10 分钟。

不能把 403 加入全局默认，否则会改变其他数据源的鉴权/权限错误语义。

Bybit metadata 和资金费率历史调用共用同一 REST 冷却 gate。metadata 的启动请求可保留有限 5xx 重试；实际资金费率 provider 仍为单次请求。

HTTP 403、429 和 body `retCode=10006` 都返回同一种可由 `errors.As` 检查的 typed rate-limit error，并携带绝对 `RetryAt`；通用 HTTP 层遇到它立即返回，不把限频计入 `MaxAttempts` 内做短间隔重试。metadata 分页加载遇到该错误时，不退出进程，而是在同一进程内 context-aware 等待共享 gate 后只重试当前页；已取得的前页不能先注册，只有完整分页成功后才原子进入选择/注册。这样 10 分钟冷却不会被 systemd 的 30 秒重启周期绕过。等待期间若进程收到取消信号，应立即退出。永久地区性 403 可能使启动一直停在每次至少间隔 10 分钟的重试中，但不会形成重启风暴，日志必须保留具体 403 响应供运维识别。

实际资金费率 provider 遇到限频仍把 typed error 返回给公共 worker；后续任务请求在进入 HTTP 前先等待同一个 gate，因此不会在冷却期继续访问。非限频的永久 4xx、解析错误或用尽 5xx 重试仍沿用当前 fail-fast/worker 重试语义。

## 配置和应用装配

新增配置：

| 环境变量 | 默认值 | 含义 |
|---|---|---|
| `BYBIT_PERP_SYMBOLS` | `-` | 逗号分隔、精确大小写的 USDT 线性永续 symbol；默认不启用，避免部署升级后自动增加流量 |
| `BYBIT_REST_URL` | `https://api.bybit.com` | 公共 REST 根地址 |
| `BYBIT_WS_URL` | `wss://stream.bybit.com/v5/public/linear` | 盘口和 ticker 公共 WebSocket 根地址 |

`internal/app/app.go` 当前使用 `if Binance { ... } else { OKX ... }`，加入第三个交易所后必须改为显式 `switch instrument.Exchange`，未知交易所直接报错，不能让它落入 OKX 分支。

装配层还需：

- 调用 Bybit metadata 并将选中的 instrument 加入统一注册列表；
- Bybit `Book` 的 retained depth 设为 1000；
- 为每个 Bybit instrument 创建盘口 runtime；
- 建立 `bybitFundingInstruments`、Bybit 确认 worker 和单条 ticker runtime；
- 把 `confirmationWorkers["Bybit"]` 注册给现有启动补查；
- Bybit 盘口与 ticker runtime 共用同一个 1 秒建连 gate；
- 启动时计算本进程的 linear WebSocket 连接预算：`盘口 instrument 数 +（启用资金费率时为 1）` 不得超过官方的 1000 条上限，超过则启动失败并要求后续改为多 topic 盘口连接；该校验不能感知同一 IP 上的其他进程，部署仍应只运行一个 collector；
- 除 typed rate-limit error 按上一节在同一进程等待外，metadata 失败或配置 symbol 不存在时沿用当前 fail-fast 启动语义。

## 保持不变的公共逻辑

以下逻辑不应因 Bybit 接入而出现条件分支：

- UTC 时间标准化；
- `price_tick` / `qty_lot` 定点整数表示；
- 每秒采样和 `valid_bitmap`；
- 分钟第 0 秒快照、第 1 至 59 秒相对上一有效状态的价格键差量；
- 前 50 档落库、分钟批量写入及回放查询；
- `instrument_id` 注册及已纳入 instrument 定义的合约规格变化处理；
- 每小时估算值写入、实际值版本优先；
- 每交易所一个串行资金费率确认 worker；
- 最近 24 小时待确认记录的启动补查。

盘口与资金费率事实表 DDL 不变；`instrument` 只增加 `venue_contract_version`。因此必须同步更新 `docs/market-data-storage.md` 的 instrument 定义，但其中的盘口/资金费率编码规则不变。实现完成时还要更新架构/运行文档中的已支持来源、启用配置和 latest registered version 查询。

## 预计文件范围

新增：

```text
internal/exchange/bybit/client.go
internal/exchange/bybit/metadata.go
internal/exchange/bybit/depth.go
internal/exchange/bybit/collector.go
internal/exchange/bybit/runtime.go
internal/exchange/bybit/funding_stream.go
internal/exchange/bybit/funding.go
internal/exchange/bybit/*_test.go
```

修改：

```text
internal/config/config.go
internal/config/config_test.go
internal/app/app.go
internal/app/app_test.go
internal/model/model.go
internal/model/model_test.go
internal/exchange/http.go
internal/exchange/http_test.go
internal/exchange/binance/metadata.go
internal/exchange/binance/*_test.go
internal/exchange/okx/metadata.go
internal/exchange/okx/*_test.go
internal/storage/clickhouse/schema.go
internal/storage/clickhouse/instrument.go
internal/storage/clickhouse/*_test.go
docs/architecture.md
docs/implementation-design.md
docs/market-data-storage.md
docs/runtime-operations.md
README.md
```

除上述 instrument 版本字段外，不修改 sampler、盘口/资金费率 storage schema 和 replay。

## 测试计划

### Metadata

- 多页 cursor 正常合并，最后一页空 cursor 停止；
- cursor 含 `=`、`&` 等字符时经 `url.Values` 恰好编码一次；
- 超过 500 条时不会漏掉后页配置 symbol；
- 重复 cursor、重复 symbol、null result/list、非零 `retCode` 失败；
- 同一响应混有 USDC 永续、USDT 交割、PendingOpen、PreLaunch 时，只保留精确 USDT Trading `LinearPerpetual`；
- `symbolId`、`launchTime` 缺失、非正数时失败；规范化版本稳定且与 `exchange_symbol` 分离；
- `tickSize`、`qtyStep` 缺失、零值、非法十进制或 scale 超限失败；`fundingInterval` 缺失、非正整数或不能整除 60 失败；
- 配置 symbol 不存在时启动失败。

### Instrument 版本迁移

- 新建 DDL 包含 `venue_contract_version`，旧库 `ALTER ... IF NOT EXISTS` 可重复执行；
- 旧空版本 instrument 仍可读取，但新 Bybit/Binance/OKX 永续定义缺少版本时不能注册；
- 完全相同版本复用 ID，不同版本即使其他规格相同也分配新 ID；
- Binance `onboardDate`、OKX `listTime`、Bybit `symbolId:launchTime` 都走 presence-aware 正整数校验；
- ID 首次切换后，实时 scheduler 只包含新 ID，而最近 24 小时旧 ID pending 任务通过独立 backfill map 查询并写回旧 ID；
- latest registered version 健康查询只对当前配置 route 做新鲜度判断，旧 ID 不产生误报警。

### 订单簿

- snapshot 最多建立 1000 档本地状态，sampler 只读前 50；低流动性侧允许少于 1000 档但至少有一档；
- delta 插入、更新、数量 0 删除均正确；
- 同一消息同价位重复、tick/lot 不整除、symbol/topic 不匹配失败；
- delta 在 snapshot 前失败；
- `u` 连续时更新，重复/回退/跳号时失效；
- 任意新 snapshot 和 `u=1` snapshot 都完整覆盖旧状态；
- `seq` 可以跳号但不能回退；
- 队列溢出、silence timeout、订阅失败使盘口 invalid，重连 snapshot 前采样无效；
- 多 runtime 同时启动/重连时，共享 gate 满足 1 秒间隔；
- snapshot 加全部 delta 可以精确恢复每一秒的前 50 档，无变化秒与无效秒可区分。

### 资金费率

- ticker snapshot 建立缓存，后续只含一个字段的 delta 正确合并；
- delta 在 snapshot 前失败并清空连接状态；
- 不含 funding 字段但状态已完整的 delta 仍以当前 `ts` 刷新 estimate；
- 完整字段建立前不发布估算；
- snapshot 会清空旧缓存，不能继承上一个 snapshot 遗漏的字段；
- 费率、时间、interval 非法或 symbol/topic 不匹配失败；
- 1、4、8 小时结算间隔都由 `nextFundingTime` 正确调度；
- 结算边界后 ticker 切到下一目标时，整点仍选择结算前针对当前目标的最后估算；
- 断线后全部 Bybit estimate 不可用，重连拿到完整状态后恢复；
- 历史接口只接受正确 category/symbol 下的精确 `fundingRateTimestamp`，上一条、下一条或其他 symbol 不能误记；
- 非零 `retCode`、HTTP 403/429、空数组和非法十进制分别按设计处理；
- metadata 首次 403、随后成功时在同一进程等待至少 RetryAt 后重试当前页；等待可被 context 取消，且完整分页前不暴露部分结果；
- `retCode=10006` 的 reset header 和 1 分钟 fallback 都生成 typed rate-limit error；
- 启动补查能路由到 `confirmationWorkers["Bybit"]`，重复任务仍只查询一次。

### 回归和验收

- `go test -buildvcs=false ./...`；
- `go test -race -buildvcs=false ./...`；
- `go vet -buildvcs=false ./...`；
- 使用本地 HTTP/WebSocket fixture 完成故障注入，不让单元测试依赖真实 Bybit；
- 开发环境只启用一个 Bybit symbol，至少运行跨过一个完整资金费率结算点；
- 检查完整有效分钟的 `valid_bitmap` 为 60 个有效秒，随机回放秒与采集 fixture 的前 50 档一致；
- 人工制造 `u` 跳号，确认重同步期间没有看似有效的盘口；对真实 1000 档流做长时 soak，记录所有相邻 `u` 差值并验证连续性假设；
- 确认结算小时最终出现 `is_actual=1` 且不会被后续估算覆盖；
- 分别选择代表性的活跃和非活跃 Bybit 永续，记录 24 小时压缩占用、写入错误、重连次数和 100 次随机秒回放耗时，与现有 Binance/OKX 同量级比较。

## 完成条件

只有以下条件同时满足，Bybit 才能从“实现完成”进入生产启用：

1. 精确过滤 USDT Trading `LinearPerpetual`，并通过分页与严格解析测试；
2. `u` 断档不会继续生成有效盘口，新 snapshot 可恢复；
3. ticker 稀疏 delta 不会把缺失字段当成空值或错误目标；
4. 实际资金费率只按精确毫秒目标确认；
5. HTTP 和 WebSocket 限频处理不会形成重连/重试风暴；
6. 当前启用的三家永续均使用非空合约版本身份，首次 ID 迁移不会漏掉旧 ID 的待确认资金费率；
7. 除明确记录的 instrument ID 切换外，现有 Binance、OKX 盘口和资金费率行为不变；
8. 实盘验收覆盖至少一个结算边界和一次人为断档恢复。
