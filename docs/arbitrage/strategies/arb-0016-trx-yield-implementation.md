# ARB-0016 TRX 收益采集实现设计

## 1. 范围

本文说明第一版如何在现有 Go 单进程采集程序中加入两类 TRX 收益数据：

1. JustLend：sTRX、jTRX、TRX → sTRX → jsTRX 组合路线、JustLend V2 TRX Vault；
2. TRON 原生质押：当前票数排名前 127 的 SR/SR Partner 对投票人的理论 APR。

两类采集器共用 [ARB-0016 收益数据模型](arb-0016-yield-data.md)中的 `yield_route` 和 `yield_observation`，不增加第三张收益表。Gate、Bitfinex、交易执行、钱包、私钥、链上交易和历史区块回扫均不在第一版范围。

第一版从程序启用时开始积累历史：JustLend 每小时保存一次完整快照，TRON 原生质押每 6 小时保存一次完整快照。已有官方接口不提供可验证历史的部分不伪造回填。

## 2. 最小程序结构

```text
JustLend REST ──> justlend.Collector ─┐
                                      ├─> yield.Runner ─> ClickHouse
TRON HTTP ─────> tron.Collector ──────┘
```

建议只增加以下目录和文件：

```text
internal/yield/
  model.go              采集批次、路线定义、观测和校验
  runner.go             启动即采一次，之后按固定间隔采集
  sink.go               WriteYieldBatch 接口
  justlend/client.go     四个公开 REST 请求及响应解析
  justlend/collector.go  生成四条 TRX 路线
  tron/client.go         witness、brokerage、参数和固化区块请求
  tron/calculator.go     原生质押 APR 公式
  tron/collector.go      生成前 127 名路线

internal/storage/clickhouse/yield.go
  路线登记和观测批量写入
```

不建立插件系统，不拆新进程，不引入消息队列，也不把收益采集接入每秒盘口采样器。

## 3. 公共采集和写入接口

采集器不分配数据库 ID，只返回稳定的路线定义和观测。ClickHouse writer 负责复用或分配 `yield_route_id`：

```go
type CollectedYield struct {
    Route       YieldRouteDefinition
    Observation YieldObservation
}

type Batch struct {
    Source      string
    CollectedAt time.Time
    Items       []CollectedYield
}

type Collector interface {
    Collect(context.Context) (Batch, error)
}

type Sink interface {
    WriteYieldBatch(context.Context, Batch) error
}
```

同一个 `Batch` 内的 `CollectedAt` 必须相同。`ObservationTime` 可以使用来源时间；来源没有时间时等于 `CollectedAt`。两个时间在 model 校验前都转为 UTC 并截断到毫秒，避免写库重试因纳秒差异形成另一个逻辑版本。所有金额和利率在 JSON 解码后立即转为 `shopspring/decimal.Decimal`，禁止先经过 `float64`。

`yield.Runner` 的行为保持简单：

1. 程序启动后立即采集一次；
2. JustLend 成功后等待 1 小时，TRON 成功后等待 6 小时；
3. 网络或解析失败时记录错误，10 分钟后重新采集；
4. 写入失败时保留已经校验的内存批次，10 分钟后只重试写入，不重新生成时间；旧批次写成功时若已经超过正常间隔，立即再采一份当前快照；
5. 同一采集器上一轮未结束时不启动下一轮；
6. 收到 `context` 取消后退出，不在退出期间开启新请求。

每个 Runner 最多只保留一个待写批次；待写批次未成功写入前不继续拉取新数据，避免数据库故障期间在个人工具里再实现磁盘队列。

## 4. ClickHouse 表和写入

字段语义以收益数据模型为准，物理类型采用现有项目已经使用的 UTC 和定点数规则。两张表使用以下结构：

```sql
CREATE TABLE IF NOT EXISTS yield_route
(
    yield_route_id UInt32,
    provider_type LowCardinality(String),
    provider LowCardinality(String),
    product_code String,
    product_name String,
    yield_type LowCardinality(String),
    deposit_asset_key String,
    position_asset_key String,
    redeem_asset_key String,
    network Nullable(String),
    contract_address Nullable(String),
    price_exposure_asset Nullable(String),
    income_source LowCardinality(String),
    source_url String,
    collection_enabled Bool
)
ENGINE = ReplacingMergeTree
ORDER BY yield_route_id;

CREATE TABLE IF NOT EXISTS yield_observation
(
    yield_route_id UInt32,
    observation_time DateTime64(3, 'UTC'),
    collected_at DateTime64(3, 'UTC'),
    tier_no UInt16,
    tier_min_amount Decimal(38, 18),
    tier_max_amount Nullable(Decimal(38, 18)),
    tier_mode LowCardinality(String),
    rate Nullable(Decimal(38, 18)),
    rate_kind LowCardinality(String),
    rate_origin LowCardinality(String),
    rate_mode LowCardinality(String),
    reward_asset_keys Array(String),
    reward_component_rates Array(Nullable(Decimal(38, 18))),
    entry_fee_rate Nullable(Decimal(38, 18)),
    exit_fee_rate Nullable(Decimal(38, 18)),
    fixed_penalty_rate Nullable(Decimal(38, 18)),
    performance_fee_rate Nullable(Decimal(38, 18)),
    entry_fee_amount Nullable(Decimal(38, 18)),
    exit_fee_amount Nullable(Decimal(38, 18)),
    fixed_fee_asset_key Nullable(String),
    lock_seconds UInt64,
    unbonding_seconds UInt64,
    rule_principal_loss_mode LowCardinality(String),
    fixed_principal_loss_rate Nullable(Decimal(38, 18)),
    rule_eligibility LowCardinality(String),
    eligibility_reason Nullable(String),
    exposure_ratio Nullable(Decimal(38, 18)),
    capacity Nullable(Decimal(38, 18)),
    remaining_capacity Nullable(Decimal(38, 18)),
    tvl Nullable(Decimal(38, 18)),
    availability LowCardinality(String),
    block_height Nullable(UInt64),
    block_hash Nullable(String),
    finality Nullable(String),
    source_payload_hash Nullable(String),
    CONSTRAINT reward_lengths CHECK length(reward_asset_keys) = length(reward_component_rates)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(observation_time)
ORDER BY (yield_route_id, observation_time, tier_no);
```

路线登记使用稳定身份：

```text
(provider, product_code, network, contract_address,
 deposit_asset_key, position_asset_key, redeem_asset_key)
```

两个 Runner 共用同一个 ClickHouse yield writer。writer 在 schema 初始化后用 `FINAL` 一次加载已有路线，建立 identity ↔ ID 内存索引和当前最大 ID；路线登记由同一个 mutex 串行执行，并在锁内再次查内存索引。身份完全相同则复用 ID，否则使用当前最大 ID 加一，`0` 保留为无效值。这样无需数据库序列，也不会因 JustLend 与 TRON 同时首次写入而撞号。

第一版不更新已有路线的显示名称；核心定义与已有行冲突时直接报错，避免为简单的展示变化设计版本系统。新路线用一个 ClickHouse batch 插入；插入失败时丢弃本次尚未提交的内存索引变化，并在下次分配 ID 前从 `FINAL` 重新加载，处理“服务端已写入但客户端收到超时”的情况。

写入顺序固定为：

1. 在不需要数据库 ID 的情况下校验整批路线定义和观测，包括枚举、十进制范围、时间、数组长度和必填字段；
2. 在 route registry mutex 内登记本批次路线并取得 ID；
3. 赋 ID 后再次校验 observation 与 route identity 的对应关系；
4. 用一个 ClickHouse batch 写入全部 `yield_observation`；
5. 写入失败时复用内存中的原批次重试，不能重新生成时间。

ClickHouse 没有跨表事务。路线先写成功而观测失败只会留下暂时没有观测的路线，可以接受；下一次重试会复用相同路线 ID。观测查询使用 `FINAL`，消除写入重试产生的相同键版本。

本项目中 `tvl` 统一保存为 `deposit_asset_key` 的数量，不保存美元值：sTRX 保存池中 TRX，jTRX/jsTRX 换算成 TRX，V2 TRX Vault 保存 TRX 等值。API 的美元 TVL 不写入该字段，避免同列混合单位。

## 5. JustLend 采集器

### 5.1 请求

基础地址为 `https://openapi.just.network`，只请求四个无需认证的 GET 接口：

| 接口 | 用途 |
|---|---|
| `/lend/strx` | sTRX APY、TRX/sTRX 兑换率和池规模 |
| `/lend/jtoken` | 从完整列表中选择地址固定的 jTRX 和 jsTRX |
| `/mining/apy` | 读取 jTRX、jsTRX 当前是否有额外 USDD 激励 |
| `/v2/index/vault/list?deposit=TRX&allPage=1&allPageSize=20` | 按固定 Vault 地址选择 TRX Vault |

V1 响应必须满足 HTTP 成功且 `code == 0`；V2 必须满足 HTTP 成功且 `code == 200`。业务错误虽然可能返回 HTTP 200，仍视为采集失败。所有十进制字段必须是普通十进制字符串；缺字段、科学计数法、负 APY、无效地址或数组结构变化均返回解析错误，不写半条数据。

第一版选择简单的全批成功：四个接口任一网络或解析失败，本轮四条路线都不写。这会比按接口拆分写入产生更多缺口，但能让一次成功批次始终包含同一轮取得的完整输入，也不需要依赖图和部分提交逻辑。

官方 API 不返回链上区块位置。JustLend 观测的 `block_height`、`block_hash` 和 `finality` 留空，不能用另一次 TRON 请求的区块号伪装成 API 快照位置。V1 没有来源时间，使用采集完成时间；V2 使用顶层 `timestamp` 作为 `observation_time`。

### 5.2 固定路线

第一版只认以下四条路线；不能仅按 symbol 匹配，必须同时核对合约地址：

| `product_code` | 路线 | 核心合约 |
|---|---|---|
| `strx` | TRX → sTRX → TRX | `TU3kjFuhtEo42tsCBtfYUAZxoqQ4yuSLQ5` |
| `jtrx` | TRX → jTRX → TRX | `TE2RzoSV3wFK99w6J9UnnZ4vLfXYoxvRwP` |
| `strx-jstrx` | TRX → sTRX → jsTRX → sTRX → TRX | `TJQ9rbVe9ei3nNtyGgBL22Fuu2xYjZaLAQ` |
| `trx-v2-vault` | TRX → V2 Vault 份额 → TRX | `THpxp8RpCUGk55dV7oL1LfxDeP9QvouxmM` |

四条路线共同填写 `provider_type=protocol`、`provider=JustLend`、`network=tron-mainnet`、`price_exposure_asset=TRX`、`source_url=https://docs.justlend.org/developers/apis/`、`collection_enabled=true`。各自的资产路径为：

| `product_code` | `deposit_asset_key` | `position_asset_key` | `redeem_asset_key` |
|---|---|---|---|
| `strx` | 原生 TRX | sTRX 合约资产 | 原生 TRX |
| `jtrx` | 原生 TRX | jTRX 合约资产 | 原生 TRX |
| `strx-jstrx` | 原生 TRX | jsTRX 合约资产 | 原生 TRX |
| `trx-v2-vault` | 原生 TRX | jTRXv2 Vault 份额资产 | 原生 TRX |

资产 key 使用明确网络和合约，例如：

```text
tron:mainnet:native:TRX
tron:mainnet:trc20:TU3kjFuhtEo42tsCBtfYUAZxoqQ4yuSLQ5
tron:mainnet:trc20:TE2RzoSV3wFK99w6J9UnnZ4vLfXYoxvRwP
```

V2 虽在合约内部使用 WTRX，但官方 TRX Vault 通过 `TrxProviderProxy` 对用户透明地包装和解包；本路线的存入和赎回资产仍记原生 TRX，收益持仓资产记 Vault 份额合约。

不能只核对产品 symbol 和产品地址，还必须验证资产链路：

- sTRX：`symbol=sTRX`、sTRX decimals 为 18、underlying decimals 为 6；
- jTRX：`symbol=jTRX`、`underlyingSymbol=TRX`、底层地址为原生 TRX 占位地址 `T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb`、decimals 为 6；
- jsTRX：`symbol=jsTRX`、`underlyingSymbol=sTRX`、底层地址等于 sTRX 合约、decimals 为 18；
- V2 TRX Vault：`vaultAddress` 为固定地址、`vaultSymbol=jTRXv2`、`assetSymbol=WTRX`、`assetAddress=TNUC9Qb1rRpS5CbWLmNMxXBjyFoydXjWFR`、decimals 为 6。

任一项不符都视为接口语义变化，整轮失败，不能把另一底层资产误记成 TRX 路线。sTRX、jTRX、jsTRX 和 Vault 份额本身都统一使用 `tron:mainnet:trc20:<address>`，避免同一合约出现两种 asset key。

### 5.3 字段映射

所有四条路线均为单档：`tier_no=1`、`tier_min_amount=0`、`tier_max_amount=NULL`、`tier_mode=none`、`rate_mode=variable`、`lock_seconds=0`。路线和理论筛选固定值如下：

| 路线 | `yield_type` | `income_source` | `rule_principal_loss_mode` | `rule_eligibility` | 原因 |
|---|---|---|---|---|---|
| sTRX | `liquid_staking` | `combined` | `none` | `candidate` | 正常规则内收益变化不扣减本金；合约和运营风险留待人工判断 |
| jTRX | `lending` | `borrow_interest` | `variable` | `rejected` | `collateral_bad_debt` |
| sTRX → jsTRX | `lending` | `combined` | `variable` | `rejected` | `collateral_bad_debt` |
| V2 TRX Vault | `lending` | `borrow_interest` | `variable` | `rejected` | `collateral_bad_debt` |

jTRX、jsTRX 和 V2 Vault 仍然采集，因为本项目先记录高收益再筛选；`rejected` 只表示它们不满足当前“理论上无不确定本金损失”的自动候选条件。三条借贷路线的 `fixed_principal_loss_rate=NULL`，拒绝原因写 `eligibility_reason=collateral_bad_debt`；sTRX 的拒绝原因为空。除 V2 `performance_fee_rate` 外，第一版没有从来源确认的固定进入费、退出费、提前罚金或固定金额费用，对应字段留空；不能擅自写 0 表示来源保证免费。

| 路线 | `rate` | `rate_kind/origin` | `reward_asset_keys/component_rates` | `exposure_ratio` | `tvl` |
|---|---|---|---|---|---|
| sTRX | `stakeInfo.supplyRate` | `apy/reported` | `[TRX] / [supplyRate]` | `stakeInfo.exchangeRate`，TRX/sTRX | `stakeInfo.totalUnderlying` |
| jTRX | `supplyRate + USDD mining APY` | `apy/derived` | 无激励时 `[TRX]/[supplyRate]`；有激励时 `[TRX,USDD]/[supplyRate,miningAPY]` | jTRX 的 `exchangeRate`，TRX/jTRX | `totalSupply × exchangeRate` |
| sTRX → jsTRX | `sTRX.supplyRate + jsTRX.supplyRate + USDD mining APY` | `apy/derived` | TRX 分项为前两项之和；USDD 激励非零时追加 USDD 分项 | `jsTRX.exchangeRate × sTRX.exchangeRate`，TRX/jsTRX | `jsTRX.totalSupply × jsTRX.exchangeRate × sTRX.exchangeRate` |
| V2 TRX Vault | Vault 的 `apy` | `apy/reported` | `[TRX] / [apy]` | API 未提供份额兑换率，留空 | `totalSupplyAmount` |

`/mining/apy` 中对应市场缺失或值为 0 表示当前没有激励，按 0 处理。组合路线的加法沿用 JustLend 对供应 APY 分项相加的公开口径；不加入永续资金费率。若未来接口明确提供组合后的单一 APY，应改用来源值并把 `rate_origin` 改成 `reported`。

上表为易读简写；实际 `reward_asset_keys` 必须使用完整 asset key，不能写裸 `TRX` 或 `USDD` symbol。

`/mining/apy` 的 `USDD` 在本采集器中固定映射为 JustLend 当前活跃 jUSDD 市场所使用的底层合约：

```text
tron:mainnet:trc20:TXDk8mbtRbXeYuMNS83CfKPaYYT8XWv9Hz
```

只检查 jTRX 和 jsTRX 对应的 mining 内层对象；其中除 `USDD` 外出现任何奖励 key（即使值为 0）都视为接口语义变化并让整轮失败，避免 Go 解码器忽略新奖励币后少算收益。其他市场的奖励种类不影响本批次。

sTRX 和组合路线的 `unbonding_seconds` 固定为 `14 × 86400`；jTRX、V2 Vault 为 0。jTRX/jsTRX 的 `cash` 是当前赎回流动性，不是申购剩余额度，不能写入 `remaining_capacity`。第一版四条路线的 `capacity` 和 `remaining_capacity` 均留空。

V2 的 `performanceFee` 是百分数 0–100，写入前除以 100，例如 `"10.00"` 写成 `0.10`。来源公布的 Vault `apy` 原样保存，采集器不自行再次扣除绩效费；是否已净额化由以后分析时依据届时官方定义处理。第一版不另抓 V2 mining，因此该 `rate` 只代表 Vault 列表接口公布的 APY，不声称包含接口之外的全部激励。

V2 的 `exposure_ratio=NULL` 是第一版已知限制：官方聚合接口没有返回 Vault 份额兑换率。若以后需要按份额精确计算对冲数量，再用 ERC-4626 `convertToAssets` 补齐；第一版不为这一列增加合约调用。

四个接口必须全部成功解析后才生成一个 JustLend 批次。若完整且成功的 jToken/Vault 列表中找不到固定合约，写一条 `availability=unavailable`、`rate=NULL` 的观测；同一固定地址出现多次属于响应歧义，整轮失败。单例 `/lend/strx` 缺少必填结构属于响应错误，整轮 JustLend 批次不写。其他正常观测写 `availability=available`。

`source_payload_hash` 使用 SHA-256 并散列完整原始响应：sTRX、V2 分别散列各自响应；jTRX 固定连接完整 jToken 和 mining 响应；组合路线固定连接完整 sTRX、jToken 和 mining 响应。每段都加 endpoint 名和长度，避免简单拼接歧义。

## 6. TRON 原生质押采集器

### 6.1 请求顺序

基础地址默认 `https://api.trongrid.io`，并允许通过配置替换为自建或其他兼容节点。每轮按以下顺序串行执行：

1. `GET /wallet/getnextmaintenancetime`，保存返回的下一维护时间 `num`；若距离维护不足 2 分钟，本轮先不采集；
2. `POST /walletsolidity/getblock?visible=true`，请求体为 `{"detail":false}`，取得批次开始时的固化区块头；
3. `GET /walletsolidity/getpaginatednowwitnesslist?offset=0&limit=127&visible=true`，取得已固化视图中按票数排序的前 127 名；
4. `GET /wallet/getchainparameters`，按 `key` 取得 `getMaintenanceTimeInterval`、`getWitnessPayPerBlock`、`getWitness127PayPerBlock` 和 `getUnfreezeDelayDays`，禁止按数组位置读取；
5. 对 127 个地址逐个调用 `POST /walletsolidity/getBrokerage`，请求体为 `{"address":"...","visible":true}`；
6. 再次调用同一个 header-only `getblock`，取得批次结束时的固化区块头；
7. 再次调用 `GET /wallet/getnextmaintenancetime`，要求 `num` 与第 1 步相同；不同表示 FullNode 采集期间跨过维护边界，整批丢弃。

brokerage 不在 witness 列表里，因此确实需要 127 次轻量请求，但仍然只有一个循环和一个解析器。请求串行执行并共用 200 毫秒最小间隔，每 6 小时约用半分钟；不为这点流量增加 worker pool。

一次成功的原生质押快照必须恰好包含 127 名、127 个 brokerage、所需链参数和相同的维护周期。任意一项失败则整批不写，10 分钟后由 Runner 重试；不拿上一轮佣金或票数冒充当前值。这样同一个 `observation_time` 下的 127 行天然代表完整排名快照。

`wallet/getchainparameters` 和下一维护时间没有 SolidityNode 对应接口，其他状态读取使用 `walletsolidity`。令维护周期为链参数中的 `getMaintenanceTimeInterval`，开始和结束两个固化区块时间必须同时位于半开区间 `[nextMaintenance - interval, nextMaintenance)`，且结束锚点的高度和时间均不得早于开始锚点；否则说明 SolidityNode 仍落在上一维护周期、在 127 次请求期间追过边界或节点视图倒退，整批丢弃。这个检查与两次相同的 `nextMaintenance` 一起使用，不能互相替代。

观测的区块高度、哈希和时间取第 6 步的结束固化区块，`finality=finalized_anchor`。这些字段只表示批次结束时的固化锚点和上界，不表示前面的多次非原子请求都来自该块，也不能用于按该高度重放原始输入。

这里保留 header-only 的 `getblock(detail=false)`，而不使用 `getnowblock`：对官方主网接口的实测中，`getnowblock` 返回了整块交易，`getblock` 的 `detail=false` 只返回区块头，语义限制已经通过 `finalized_anchor` 明确表达。

解析阶段还必须校验：127 个 Base58 地址互不重复；`voteCount` 为正数并按响应顺序不增；总票数 `T>0`；brokerage 是 0–100 的整数；四个链参数非负；维护周期恰为 21,600,000 毫秒；固化区块高度、ID 和时间有效。任一校验失败都不生成批次。

### 6.2 路线身份

每个前 127 名地址生成一条路线，但不生成独立代码：

```text
provider_type = native
provider = TRON
product_code = sr:<Base58 address>
product_name = TRON vote for <URL or shortened address>
yield_type = native_staking
deposit_asset_key = tron:mainnet:native:TRX
position_asset_key = tron:mainnet:native:TRX
redeem_asset_key = tron:mainnet:native:TRX
network = tron-mainnet
contract_address = NULL
price_exposure_asset = TRX
income_source = issuance
source_url = https://developers.tron.network/docs/reward-calculation
collection_enabled = true
```

SR 跌出前 127 后不删除路线。每个成功快照只查询该快照时间下的 127 行；不能把某条路线更早的最后一行当成当前收益。若它以后重新进入前 127，稳定的地址会复用原 `yield_route_id`。`collection_enabled=true` 只表示适配器仍支持这条路线，不表示该 SR 当前一定在前 127。

### 6.3 APR 公式

TRON 原生奖励不会自动复投，因此保存 `rate_kind=apr`、`rate_origin=derived`。设：

```text
B = 每日理论区块数，当前规则为 28,792
V = getWitness127PayPerBlock / 1,000,000，单位 TRX/块
P = getWitnessPayPerBlock / 1,000,000，单位 TRX/块
T = 前 127 名 voteCount 总和
S = 当前 SR 的 voteCount
C = brokerage / 100
```

每质押并投票 1 TRX 可获得 1 票。所有前 127 名都有投票奖励，只有前 27 名有出块奖励：

```text
vote_daily_per_trx  = B × V / T × (1 - C)

block_daily_per_trx = 0                                      # 排名 28–127
block_daily_per_trx = B × P / 27 / S × (1 - C)              # 排名 1–27

apr = (vote_daily_per_trx + block_daily_per_trx) × 365
```

该公式是按当前网络规则和计划出块数计算的理论前瞻 APR，不用累计 `totalProduced/totalMissed` 反推历史收益。`B=28,792` 必须作为有官方依据的代码常量并用官方示例测试；奖励金额仍每轮读取链参数，因为可由治理修改。第一版同时要求 `getMaintenanceTimeInterval == 21,600,000` 毫秒；若治理修改维护周期，停止写入并更新公式，不能继续套用 28,792。

每条观测填写：

```text
tier_no = 1
tier_min_amount = 0
tier_max_amount = NULL
tier_mode = none
rate_mode = variable
rate_kind = apr
rate_origin = derived
reward_asset_keys = [tron:mainnet:native:TRX]
reward_component_rates = [apr]
entry_fee_rate = NULL
exit_fee_rate = NULL
fixed_penalty_rate = NULL
performance_fee_rate = NULL
entry_fee_amount = NULL
exit_fee_amount = NULL
fixed_fee_asset_key = NULL
lock_seconds = 0
unbonding_seconds = getUnfreezeDelayDays × 86400
rule_principal_loss_mode = none
fixed_principal_loss_rate = NULL
rule_eligibility = candidate
eligibility_reason = NULL
exposure_ratio = 1
tvl = S
availability = available
```

`source_payload_hash` 对本行真正使用的完整原始响应按请求顺序和长度前缀散列：第一次 maintenance、开始 block header、完整 witness、完整 chain-parameters、该 SR 的完整 brokerage、结束 block header、第二次 maintenance。URL 只作为显示名称的一部分，路线身份始终使用地址。

## 7. HTTP、配置和程序接入

现有 `exchange.Get` 已处理超时、429/418 冷却和有限 5xx 重试。为 TRON 增加一个复用同一重试策略的 `PostJSON`，不复制另一套重试器。JustLend 和 TRON 使用不同的 `RequestGate`，互不阻塞；TRON 的 127 次 brokerage 共用一个 gate。

第一版只增加以下配置：

```text
JUSTLEND_YIELD_ENABLED=false
JUSTLEND_BASE_URL=https://openapi.just.network
TRON_STAKING_YIELD_ENABLED=false
TRON_HTTP_URL=https://api.trongrid.io
```

采集间隔和失败重试先使用代码默认值，不增加更多环境变量。URL 可配置是为了测试和切换兼容节点。两个开关默认关闭，完成测试后由部署配置明确开启。

`app.Run` 在 ClickHouse schema 初始化后装配启用的 Runner。收益 Runner 内部吞掉单轮网络/解析/写入错误并记录日志，只有配置无效或 `context` 取消才结束；因此 JustLend 或 TronGrid 短时不可用不会关闭 Binance/OKX 盘口采集。

## 8. 校验、日志和测试

日志只记录 `source`、endpoint、采集阶段、耗时、路线数和错误，不记录完整大响应。解析失败的响应最多记录截断后的 512 字节，沿用现有 HTTP 工具的限制。

最低单元测试：

1. JustLend V1/V2 成功码不同，HTTP 200 业务错误不能写库；
2. sTRX、jTRX、jsTRX、V2 Vault 的产品地址、底层资产地址、symbol 和 decimals 必须一起匹配；
3. 所有十进制值直接从字符串解析，科学计数法和 `float64` 路径被拒绝；
4. mining APY 为 0、非零和市场 key 缺失时，奖励数组及 jTRX/jsTRX 总 APY 正确；
5. jsTRX 组合 APY、组合兑换率和 TRX TVL 计算正确；
6. V2 `performanceFee` 从百分数正确转成比例，借贷三条路线均为 `variable/rejected`；
7. witness 必须恰好 127 名、地址唯一、票数为正且不增；重复地址、乱序、零票和零总票整批失败；
8. 链参数按 key 查找且范围正确，缺参数、负值或维护周期不再是 6 小时均整批失败；
9. brokerage 为 0、20、100 时公式正确，越界值失败，排名 27 有出块奖励、排名 28 没有；
10. 使用官方奖励示例验证 APR 公式的中间结果；
11. 127 个 brokerage 中任意一个失败、两次下一维护时间不同或固化 anchor 无效时不产生残缺批次；专门覆盖 FullNode 已越过维护点而 SolidityNode 在批次中从上一周期追到当前周期的失败用例；
12. 相同路线身份在重试和重启后复用 ID，新 SR 地址得到新 ID；
13. JustLend 与 TRON 并发登记新路线时 ID 不重复，路由插入超时后会从 `FINAL` 重载；
14. 非法 observation 不会先写入 route；observation batch 重试保持原 `observation_time` 和 `collected_at`；
15. 查询 `FINAL` 后同一 `(yield_route_id, observation_time, tier_no)` 只有一个逻辑版本；
16. Runner 单轮失败不会退出，也不会影响现有盘口组件；待写批次成功补写后会按是否过期决定立即采集。

ClickHouse 集成测试只需验证建表、路线复用、四条 JustLend 批量写入和 127 条 TRON 批量写入，不启动真实网络请求。真实接口只做一个手工 smoke test，避免 CI 依赖第三方服务。

## 9. 实现顺序和完成标准

按下面顺序实现，减少同时改动的范围：

1. 增加两张表、公共 model、校验和 ClickHouse writer；
2. 实现 JustLend client/collector，先让四条路线持续写入；
3. 增加通用 `PostJSON`，实现 TRON client 和 APR calculator；
4. 接入两个 Runner 和配置开关；
5. 运行单元测试、ClickHouse 集成测试和各一次公开接口 smoke test。

完成标准是：程序连续运行时每小时得到一组完整 JustLend TRX 观测、每 6 小时得到恰好 127 条同时间且未跨维护周期的 TRON 原生质押观测；重启或两个来源并发写入不会为相同路线分配新 ID，也不会分配重复 ID；来源失败只造成明确缺口，不会沿用旧利率，也不会停止现有行情采集。

## 10. 官方资料

- [JustLend 公开 API](https://docs.justlend.org/developers/apis/)
- [JustLend 主网合约与底层资产地址](https://docs.justlend.org/developers/deployed_contracts/)
- [JustLend V2 与 TRX Vault](https://docs.justlend.org/developers/justlend_v2/)
- [TRON 奖励计算](https://developers.tron.network/docs/reward-calculation)
- [TRON Super Representative 角色与排名](https://developers.tron.network/docs/super-representatives)
- [TRON HTTP API 与 SolidityNode](https://developers.tron.network/docs/api)
- [TRON GetBrokerage](https://developers.tron.network/reference/wallet-getbrokerage)
- [TRON GetChainParameters](https://developers.tron.network/reference/wallet-getchainparameters)
