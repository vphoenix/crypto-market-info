# ARB-0016 SOL 收益采集第二阶段实现设计

## 1. 第二阶段做什么

第二阶段继续沿用第一阶段的单进程、固定产品、低频采集方式，只增加五条收益路线和一组已有功能的配置说明：

| 模块 | 第二阶段产品 | 保存内容 | 主来源 |
|---|---|---|---|
| LST 通用 Stake Pool | laineSOL、JupSOL、hSOL | 当前兑换率、TVL、链上费用、近似 APR | Solana finalized RPC |
| SOL 单币借贷 | Kamino Main SOL | 最近 30 天逐小时基础存款 APY、兑换率、TVL、额度和状态 | Kamino 官方历史 API |
| SOL 单币借贷 | Save Main SOL | 当前基础存款 APY、最近 30 天逐日历史 APY、兑换率和容量 | Save 官方 API |
| 原生验证者质押 | 用户加入现有 vote account 白名单的验证者 | 继续由第一阶段 collector 保存已完成 epoch 的 APY、佣金和 active stake | Marinade validators API |

仍然只采集公开数据，不执行存款、质押、赎回、借贷或交易，不使用私钥，也不把永续资金费率写入收益表。

“保持简单”在第二阶段具体表示：

- 继续使用现有 `yield.Runner`、ClickHouse writer 和 `yield_route`、`yield_observation` 两张表；
- 固定五个产品的 program、market、reserve、pool 和 mint，不动态扫描全链产品；
- 每条路线一个 Runner，一条来源失败不影响其他路线；
- HTTP 和 RPC 均顺序请求，不增加消息队列、任务表、水位表或新服务；
- 不引入 Node.js、Rust 辅助进程或完整 Solana SDK；
- 不抓奖励币激励、金额相关的即时退出报价，也不做自动风险评分。

第二阶段不实现 marginfi 和 Drift。两者当前的官方集成主路径都是 SDK 或链上账户读取，没有找到与 Kamino、Save 同等级的、稳定且公开的历史存款 APY HTTP 接口。现在接入需要额外运行 TypeScript SDK，或在 Go 中维护各自快速变化的 Anchor 账户布局和利率公式，这不符合本项目当前的简单实现边界。以后出现官方稳定 HTTP 接口，或确实决定接受链上解析维护成本时，再单独设计；不是把网页内部接口当作正式来源。

## 2. Runner 与唯一写入者

新增五个 Runner，全部复用现有 `yield.Runner`，采集间隔仍为 6 小时、失败后 10 分钟重试：

```text
Solana RPC ──> stakepool.Reader ──> laineSOL Collector ─┐
                  ├────────────────> JupSOL Collector ──┤
                  └────────────────> hSOL Collector ────┤
Kamino API + RPC identity ─────────> Kamino SOL ────────┼─> yield.Runner
Save API + RPC identity/anchor ────> Save SOL ──────────┘       │
                                                                v
                                                   yield_route / yield_observation
```

每条经济路线只有一个写入者：

| 路线 | 唯一写入者 |
|---|---|
| laineSOL | 通用 Stake Pool collector |
| JupSOL | 通用 Stake Pool collector |
| hSOL | 通用 Stake Pool collector |
| Kamino Main SOL lending | Kamino collector |
| Save Main SOL lending | Save collector |

JupSOL 和第一阶段的 JitoSOL 是两个不同产品。JupSOL 由 Jupiter/Sanctum 路线写入；JitoSOL 继续只由 Jito 专用 collector 写入，不能因为名称相近而复用地址或产生重复行。

原生验证者质押不新增 collector。第二阶段只补充白名单用法，继续由第一阶段的 `solvalidator.Collector` 为每个配置的 vote account 独占写入。

## 3. 数据库和模型不变

第二阶段不新增表、不新增字段、不修改 DDL，也不新增枚举值。五条新路线都能直接使用现有模型：

- LST 使用 `yield_type=liquid_staking`；
- Kamino、Save 使用 `yield_type=lending`；
- 基础借贷利息使用 `income_source=borrow_interest`；
- 所有金额、利率、兑换率和费用继续使用 `shopspring/decimal`；
- API JSON 数字必须经 `json.Decoder.UseNumber` 和十进制字符串解析，不能经过 `float64`。

共同观测字段：

```text
tier_no = 1
tier_min_amount = 0
tier_max_amount = NULL
tier_mode = none
rate_mode = variable
lock_seconds = 0
rule_principal_loss_mode = none
rule_eligibility = candidate
fixed_principal_loss_rate = NULL
eligibility_reason = NULL
```

这里严格沿用已经确认的理论筛选口径：正常规则下，浮动收益只表示未来收入变化，不表示本金损失。Kamino 和 Save 的借贷池不能再写成 `variable/rejected/collateral_bad_debt`；合约漏洞、坏账处置、预言机失效、治理恶意或流动性枯竭属于以后人工判断的理论外风险。

五条路线都直接位于 Solana mainnet，不使用桥、映射 SOL 或二层网络。三条 LST 的 `deposit_asset_key` 和 `redeem_asset_key` 是原生 SOL：

```text
solana:mainnet:native:SOL
```

Kamino、Save reserve 实际接受 SPL Wrapped SOL，因此两条借贷路线使用精确到 mint 的 WSOL：

```text
solana:mainnet:spl:So11111111111111111111111111111111111111112
```

WSOL 与原生 SOL 的标准 wrap/unwrap 是确定的 1:1 路径，不经过桥或二层；wrap/unwrap 和链上交易费不属于借贷 reserve 公布的 APY，当前收益观测不伪造这类费用。两条路线的 `price_exposure_asset` 仍是原生 SOL，便于与现有 SOL 行情和空头市场关联。

## 4. 通用 Stake Pool 扩展

### 4.1 固定产品身份

三条 LST 路线使用以下固定地址：

| 产品 | program | pool | mint |
|---|---|---|---|
| laineSOL | `SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy` | `2qyEeSAWKfU18AFthrF7JA8z8ZCi1yt76Tqs917vwQTV` | `LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZQti1xfWMLy6X` |
| JupSOL | `SPMBzsVUuoHA4Jm6KunbsotaahvVikZs1JyTW6iJvbn` | `8VpRhuxa7sUUepdY3kQiTmX9rS5vx4WgaXiAnXq4KCtr` | `jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v` |
| hSOL | `SP12tWFxD9oJsVWNavTTBZvMbA6gkAmxtVgxdqvyvhY` | `3wK2g8ZdzAH8FJ7PKr2RcvGh7V9VYson5hrVsJM5Lmws` | `he1iusmfkpAdwvxLNGV8Y1iSbj4rUy6yMhEA3fotn9A` |

Sanctum 文档说明上述 `SPMB...`、`SP12...` 与原始 `SPoo...` 当前运行相同的 Stake Pool 程序代码，但以后可能分叉。因此 Reader 只解析当前相同的 Borsh 字段顺序，同时必须对每个产品校验配置中指定的唯一 program；不能只建立一个宽泛白名单后接受任意 owner。未来任一部署改变字段、长度上限或 owner 时，该路线应停止写入，而不是继续按旧布局产生错误 APY。

2026-08-26 对 finalized 账户重新核验后，bSOL、JitoSOL、laineSOL、JupSOL、hSOL 的账户物理长度（RPC `space` 和解码后的 data）均为 611 字节。此前记录的 599、597、604、600、596 不是账户长度，而是现有解析器分别消费完已知 Borsh 字段后的 offset。Borsh 中的 `Option` 和 `FutureEpoch` 使有效序列化前缀长度变化，剩余部分是最大 packed size 预分配出的尾部。

官方 Stake Pool processor 使用 unchecked Borsh 读取，并以 `borsh::to_writer(&mut account_data[..], ...)` 只覆盖新序列化前缀，不保证在字段由 `Some` 变为 `None` 后清零剩余预分配字节。因此解析器必须严格解析和校验已知前缀，但不能要求未使用尾部全为 0，也不能把尾部非零本身判为未来字段。

资产键：

```text
laineSOL = solana:mainnet:spl:LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZQti1xfWMLy6X
JupSOL   = solana:mainnet:spl:jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v
hSOL     = solana:mainnet:spl:he1iusmfkpAdwvxLNGV8Y1iSbj4rUy6yMhEA3fotn9A
```

路线定义：

| 字段 | laineSOL | JupSOL | hSOL |
|---|---|---|---|
| `provider` | `Laine` | `Jupiter` | `Helius` |
| `product_code` | `lainesol` | `jupsol` | `hsol` |
| `product_name` | `Laine laineSOL` | `Jupiter JupSOL` | `Helius hSOL` |
| `position_asset_key` | laineSOL 资产键 | JupSOL 资产键 | hSOL 资产键 |
| `contract_address` | 对应 pool | 对应 pool | 对应 pool |
| `price_exposure_asset` | SOL | SOL | SOL |
| `income_source` | `combined` | `combined` | `combined` |
| `source_url` | Solana mainnet RPC | Solana mainnet RPC | Solana mainnet RPC |

### 4.2 最小代码改动

将现有硬编码 bSOL 的实现小幅通用化：

```go
type PoolConfig struct {
    Program string
    Address string
    Mint    string
}

type StakePoolProduct struct {
    PoolConfig
    Provider, ProductCode, ProductName, PositionAssetKey string
}
```

`Reader.Read` 将当前固定检查：

```go
account.Owner == StakePoolProgram
```

改为：

```go
account.Owner == config.Program
```

`Program`、`Address`、`Mint` 为空或不是合法 pubkey 时立即失败。产品配置全部写在代码常量中，不从环境变量加载。现有 bSOL 和 JitoSOL 校验调用也补上原始 `StakePoolProgram`，保持行为不变。

`ParseStakePool` 保留并明确以下边界：

- 账户物理长度必须恰好是当前部署的 611 字节，截断或超过该长度都拒绝；
- 严格解析当前已知 Borsh 前缀，所有 `Option` / `FutureEpoch` tag、逐字段 bounds、account type 和数值均按现有布局校验；
- 读完当前已知最后字段后，允许任意内容的剩余预分配尾部，不把它当作新字段解析；
- 固定 owner、固定 pool mint 和当前 epoch 校验仍然全部保留。

如果任一固定部署以后改变 owner、物理长度或使已知前缀无法按当前布局解析，该路线停止写入。不能为了继续采集而放宽 program、长度、tag 或字段边界。

新增一个固定产品的 `StakePoolCollector`，把现有 `BSOLCollector` 的计算和字段映射搬进去。bSOL 也切换为这个 collector，但必须保持原路线身份、字段口径和 Runner source 不变。这样新增三条 LST 只增加三组常量和三个 Runner，不复制计算代码。

不得为了这四个通用产品建立动态注册中心、配置文件格式或插件接口。

### 4.3 观测映射

三条新 LST 与第一阶段 bSOL 使用完全相同的计算：

```text
current_ratio   = total_lamports / pool_token_supply
previous_ratio  = last_epoch_total_lamports / last_epoch_pool_token_supply
period_return   = current_ratio / previous_ratio - 1
approximate_apr = period_return × 182.625
```

字段映射：

| 字段 | 值 |
|---|---|
| `observation_time` | finalized pool account context slot 对应的 block time |
| `rate` / `rate_kind` / `rate_origin` | `approximate_apr` / `apr` / `derived` |
| `reward_asset_keys` / `reward_component_rates` | `[SOL]` / `[approximate_apr]` |
| `exposure_ratio` | `current_ratio`，SOL/LST |
| `tvl` | `total_lamports / 1_000_000_000`，单位 SOL |
| `entry_fee_rate` | 当前 `sol_deposit_fee` |
| `exit_fee_rate` | 当前 `stake_withdrawal_fee` |
| `performance_fee_rate` | 当前 `epoch_fee`；APR 已来自净兑换率变化，查询时不能再次扣除 |
| `unbonding_seconds` | `NULL`；正常 stake withdrawal 后仍受 epoch cooldown 影响，不能写固定秒数 |
| `availability` | `unknown` |
| `block_height` / `block_hash` / `finality` | 本次 finalized block / `finalized` |
| `source_payload_hash` | account、epoch、block 三个完整 RPC 响应的固定顺序 hash |

任何一个总量为 0、当前 epoch 未更新、费用分数非法、兑换率非正或 APR 为负时，整条路线本轮失败，不写部分观测。

## 5. 借贷路线的共同口径

第二阶段只记录“借出 SOL 后，由借款人支付并计入 SOL 存款余额的基础利息”：

```text
yield_type = lending
income_source = borrow_interest
rate_kind = apy
rate_origin = reported
reward_asset_keys = [solana:mainnet:spl:So11111111111111111111111111111111111111112]
reward_component_rates = [rate]
price_exposure_asset = SOL
unbonding_seconds = 0
```

这里的 reserve 存入、持仓和赎回数量以 WSOL 路径定义，表中的收益和 TVL 仍按 1 WSOL = 1 SOL 的链上面值使用 SOL 数量口径。查询端如需从原生 SOL 开始计算完整持有期收益，应在分析层另加标准 wrap/unwrap 的交易费，不能把它混进来源公布的 APY。

Kamino 的 KMNO 等激励和 Save 响应中的奖励数组本阶段不并入 `rate`，也不加入奖励数组。原因不是判定它们有风险，而是奖励币价格暴露、奖励历史和基础借贷利息不是同一口径；先保存可直接由 SOL 空头对冲本金价格的基础利息。以后采集奖励时，应把每个奖励币及其独立 APY 明确写入现有数组，不能先换算成美元再混成 SOL 利息。

协议没有固定锁仓和正常退出等待，因此 `lock_seconds=0`、`unbonding_seconds=0`。但能否立即提出任意金额取决于当时未借出的流动性，这不是固定等待规则。没有金额相关退出报价时不能把 `availability=available` 解释为“大额可立即全额退出”。

来源没有明确给出的进入费、退出费和绩效费一律留空，不能擅自写 0。

## 6. Kamino Main SOL collector

### 6.1 固定身份与来源

```text
program = KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD
market  = 7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF
reserve = d4A2prbA2whesmvHaL88BH6Ewn5N4bTSU2Ze8P6Bc4Q
mint    = So11111111111111111111111111111111111111112
```

路线定义：

```text
provider_type = protocol
provider = Kamino
product_code = main-sol
product_name = Kamino Main SOL lending
deposit_asset_key = solana:mainnet:spl:So11111111111111111111111111111111111111112
position_asset_key = solana:mainnet:protocol-position:kamino:d4A2prbA2whesmvHaL88BH6Ewn5N4bTSU2Ze8P6Bc4Q
redeem_asset_key = solana:mainnet:spl:So11111111111111111111111111111111111111112
network = solana-mainnet
contract_address = d4A2prbA2whesmvHaL88BH6Ewn5N4bTSU2Ze8P6Bc4Q
source_url = https://api.kamino.finance/kamino-market/7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF/reserves/d4A2prbA2whesmvHaL88BH6Ewn5N4bTSU2Ze8P6Bc4Q/metrics/history
```

`position_asset_key` 表示该固定 reserve 的内部 collateral accounting unit，不宣称它是用户钱包中可自由转移的普通 SOL。这样可以用历史响应中的兑换率记录每个内部单位对应的 SOL 本金，并避免把不同 Kamino market 的 SOL 存款混成一个位置资产。

### 6.2 每轮请求与校验

每轮顺序执行：

1. finalized RPC `getAccountInfo(reserve)`，校验账户存在、非 executable、owner 等于固定 Kamino program；
2. 请求最近 30 天逐小时历史：

```text
GET /kamino-market/{market}/reserves/{reserve}/metrics/history
    ?env=mainnet-beta
    &start=<collected_at - 30 days, RFC3339>
    &end=<collected_at, RFC3339>
    &frequency=hour
```

身份 RPC 只保护当前轮没有指向错误合约。API 历史点不返回对应 slot，不能把当前 identity check 的 slot 冒充成过去观测的链上位置；所有 Kamino 历史行的 `block_height`、`block_hash`、`finality` 都为空。

严格校验：

- 顶层 `reserve` 必须等于固定 reserve；
- 历史点为 1–1024 个，时间严格递增且不得重复；
- 每点 `symbol=SOL`、`mintAddress` 等于固定 SOL mint、`decimals=9`；
- `supplyInterestAPY` 非负，`exchangeRate` 大于 0，`totalSupply`、`reserveDepositLimit` 非负；
- 最新点不能晚于采集时间 5 分钟，也不能旧于 72 小时；
- 未识别的 `status` 不能猜测，映射为 `availability=unknown`。

每点字段：

| 字段 | 来源或计算 |
|---|---|
| `observation_time` | `timestamp` |
| `rate` | `metrics.supplyInterestAPY`，API 已是比例，`0.075` 表示 7.5% |
| `exposure_ratio` | `1 / metrics.exchangeRate`，单位 SOL/内部 collateral unit；该 API 的 `exchangeRate` 是 collateral unit/SOL |
| `tvl` | `metrics.totalSupply`，单位 SOL；不能误用以美元计价的 `depositTvl` |
| `capacity` | `reserveDepositLimit / 1_000_000_000`，单位 SOL |
| `remaining_capacity` | `max(capacity - totalSupply, 0)` |
| `availability` | `Active` 且未标记 UI deprecated 时为 `available`；`Inactive` 为 `unavailable`；其他为 `unknown` |
| `source_payload_hash` | 本次完整历史 API 响应 hash |

`rate` 只取 `supplyInterestAPY`，不取 `borrowInterestAPY`，也不从借款 APY自行反推存款 APY。

## 7. Save Main SOL collector

### 7.1 固定身份与来源

```text
program         = So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo
market          = 4UpD2fh7xH3VP9QQaXtsS1YY3bxzWhtfpks7FatyKvdY
reserve         = 8PbodeaosQP19SjYFx855UMqWxH2HynZLdBXmsrbac36
liquidity mint  = So11111111111111111111111111111111111111112
collateral mint = 5h6ssFpeDeRbzsEHDbTQNH7nVGgsKrZydxdSTnLm6QdV
```

路线定义：

```text
provider_type = protocol
provider = Save
product_code = main-sol
product_name = Save Main SOL lending
deposit_asset_key = solana:mainnet:spl:So11111111111111111111111111111111111111112
position_asset_key = solana:mainnet:spl:5h6ssFpeDeRbzsEHDbTQNH7nVGgsKrZydxdSTnLm6QdV
redeem_asset_key = solana:mainnet:spl:So11111111111111111111111111111111111111112
network = solana-mainnet
contract_address = 8PbodeaosQP19SjYFx855UMqWxH2HynZLdBXmsrbac36
source_url = https://api.solend.fi/v1/reserves
```

产品当前品牌使用 Save，公开 API 域名仍是 `api.solend.fi`。路线 provider 使用 `Save`，source URL 保留真实 API 域名。

### 7.2 每轮请求

每轮顺序执行：

1. `GET /v1/reserves?scope=reserve&ids={reserve}`；
2. finalized RPC `getAccountInfo(reserve)`，校验 owner 是固定 Save program；
3. 按当前响应的 `reserve.lastUpdate.slot` 调用 finalized `getBlock`，取得当前点的 block time、height 和 hash；
4. `GET /v1/reserves/historical-interest-rates?ids={reserve}&span=30d`。

当前接口的外层必须严格为 `{results:[...], next:null}`，要求 `len(results)==1` 且 `next==null`。对 `results[0]` 校验：

- `reserve.pubkey` 和 `reserve.address` 都等于固定 reserve；
- `reserve.lendingMarket` 等于固定 market；
- `reserve.liquidity.mintPubkey` 等于固定 WSOL mint，`mintDecimals=9`；
- `reserve.collateral.mintPubkey` 等于固定 cSOL mint。

`lastUpdate.stale` 只表示该链上 reserve 状态是否等待刷新，不代表产品是否允许存取；只接受 `0` 或 `1`，当前点的 `availability` 始终写 `unknown`。

当前点字段：

| 字段 | 来源或计算 |
|---|---|
| `observation_time` | `lastUpdate.slot` 对应 finalized block time |
| `rate` | `rates.supplyInterest / 100`；该字段是百分数字符串，`1.82` 表示 1.82% |
| `exposure_ratio` | `cTokenExchangeRate`，SOL/cSOL |
| `tvl` | `(availableAmount + borrowedAmountWads / 1e18 - accumulatedProtocolFeesWads / 1e18) / 1e9`，单位 SOL |
| `capacity` | `depositLimit / 1e9`，单位 SOL |
| `remaining_capacity` | `max(capacity - tvl, 0)` |
| `block_height` / `block_hash` / `finality` | 对应 source 自带 slot 的 block / `finalized_anchor` |
| `source_payload_hash` | 当前 reserves 响应与 block 响应的固定顺序 hash |

历史接口外层必须恰好只有一个 key，且 key 等于固定 reserve；其 value 数组点数为 1–64。每个点的 `reserveID` 也必须等于固定 reserve，时间严格递增、不重复，最新点在 72 小时内。历史字段映射：

| 字段 | 来源 |
|---|---|
| `observation_time` | `timestamp`，Unix 秒 UTC |
| `rate` | `supplyAPY`，已经是比例，不再除以 100 |
| `exposure_ratio` | `cTokenExchangeRate` |
| `tvl` / `capacity` / `remaining_capacity` | `NULL`；历史接口没有这些值 |
| `availability` | `unknown` |
| block 三字段 | `NULL`；历史接口没有对应 slot |
| `source_payload_hash` | 完整历史响应 hash |

如果当前 block time 恰好与某个逐日历史点相同，只保留字段更完整的当前点，避免同一 batch 出现相同 `(route, observation_time, tier_no)` 而使整轮校验失败。

当前和历史两套接口的利率单位不同，必须分别测试，不能共用一个“自动猜单位”的函数。响应里即使出现奖励数组，本阶段也只保存基础 `supplyInterest` / `supplyAPY`。

当前点还要校验以下恒等关系。两边统一换算成 SOL 后，允许的绝对差不超过 1 lamport（`0.000000001 SOL`）：

```text
collateral.mintTotalSupply × cTokenExchangeRate / 1e9
= (availableAmount + borrowedAmountWads / 1e18
   - accumulatedProtocolFeesWads / 1e18) / 1e9
```

这保证 `tvl` 与 cSOL 的供应量、`exposure_ratio` 使用同一净本金口径；1 lamport 容差只吸收官方字段各自取整造成的微小差异，仍会拒绝未扣累计协议费造成的明显偏差。不能套用旧 SDK 中未扣累计协议费的 gross deposits 公式。

## 8. 原生验证者白名单

第一阶段代码已经支持任意固定 vote account，因此第二阶段不改 collector 和数据库。只在 README 中说明：

- `SOL_VALIDATOR_VOTE_ACCOUNTS` 仍由用户人工维护；
- 只能填写已从验证者官方资料核实的 vote account，不能填写 identity account、Stake Pool validator-list account 或 LST mint；
- Helius 和 Laine 可以分别使用其官方 vote account作为配置示例；
- JupSOL 是多验证者 Stake Pool，不存在一个可替代整个 JupSOL 路线的单一 Jupiter vote account，因此不能把 JupSOL validator-list 地址填进原生验证者 collector；
- 不把任何第三方验证者默认硬编码为启用状态。

示例只放注释或文档，不改变默认 `-`：

```text
Helius = he1iusunGwqrNtafDtLdhsUQDFvo13z9sUa36PauBtk
Laine  = GE6atKoWiQ2pt3zL7N13pjNHjdLVys8LinG8qeJLcAiL
```

## 9. 配置、代码位置和启动

继续使用：

```text
SOL_YIELD_ENABLED=false
SOLANA_RPC_URL=https://api.mainnet.solana.com
SOL_VALIDATOR_VOTE_ACCOUNTS=-
```

新增两个仅用于端点覆盖的配置：

```text
KAMINO_BASE_URL=https://api.kamino.finance
SAVE_BASE_URL=https://api.solend.fi
```

`SOL_YIELD_ENABLED=true` 时同时启动第一、第二阶段全部固定 SOL 收益路线；不再增加第二个总开关，也不增加单产品开关。两个 base URL 可覆盖只用于测试、代理或排障，固定 program、market、reserve、pool、mint 和计算公式不能从环境变量替换。

预计修改或新增：

```text
internal/yield/solana/stakepool.go
internal/yield/solana/collector.go
internal/yield/solana/*_test.go
internal/yield/kamino/collector.go
internal/yield/kamino/collector_test.go
internal/yield/save/collector.go
internal/yield/save/collector_test.go
internal/config/config.go
internal/config/config_test.go
internal/app/app.go
README.md
docs/arbitrage/README.md
docs/arbitrage/strategies/arb-0016-yield-data.md
docs/arbitrage/strategies/arb-0016-sol-yield-phase-1.md
```

不增加 Go 依赖，不修改 ClickHouse schema。

## 10. 失败、重试和历史重复

- 每个产品一个 Runner；一个产品失败只形成自己的缺口；
- Runner 继续立即首采、成功后等待 6 小时、失败后等待 10 分钟；
- 写库失败时继续保留原 pending batch 重试，不重新抓取；
- Kamino 最近 30 天和 Save 最近 30 天会有重叠，依靠现有 `(yield_route_id, observation_time, tier_no)` 逻辑键与 `ReplacingMergeTree FINAL` 合并；
- 不为低频、最多约 1024 行的批次建立水位表；
- API 历史缺口不能用当前 APY 伪造，RPC identity 失败也不能绕过；
- `source_payload_hash` 必须填写，不能因为历史行的 block 字段为空而省略。

## 11. 测试和完成标准

### 11.1 单元测试

至少覆盖：

1. Stake Pool Reader 按每个 `PoolConfig.Program` 校验 owner；611 字节 fixture 中不同长度的有效 Borsh 前缀以及非零预分配尾部能解析，截断、超过 611 字节、非法 tag、错误 program、pool mint 和 epoch 都拒绝；
2. bSOL 路线身份和数值在通用化前后不变；JitoSOL 仍只有 Jito 专用写入者；
3. laineSOL、JupSOL、hSOL 的 program、pool、mint、资产键和路线字段固定正确；
4. LST 的 APR、兑换率、TVL和三种费用只走整数/Decimal 路径；
5. Kamino 严格解析 30 天窗口，拒绝错误 reserve、mint、重复/乱序/过期时间、非正兑换率和非法 Decimal；
6. Kamino `exposure_ratio=1/exchangeRate`，`depositTvl` 不会被误写成 SOL TVL；
7. Save 当前百分数字符串必须除以 100，历史 `supplyAPY` 不再除以 100；
8. Save 当前 `{results,next}`、历史动态 reserve key、每点 `reserveID`、固定 market/reserve/两种 mint、decimals、owner 和 source slot block anchor 全部校验；净 TVL 与 cSOL 供应量乘兑换率的绝对差不超过 1 lamport；`stale` 不会被误当成可存取状态；当前点与历史点同秒时只保留当前点；
9. Kamino、Save 的奖励激励不混入基础 SOL 利息；借贷路线固定为 `none/candidate`；
10. API 历史行 block 三字段为空，直接链上 LST 和 Save 当前点的区块锚点完整；
11. 所有实时观测都有 64 位小写 `source_payload_hash`；
12. 同一批历史点可通过现有 `Batch.NormalizeAndValidateForLiveCollection`，重复时间会被拒绝；
13. app 测试确认五个新增 collector 注册为五个独立 component，不为既有 Runner 重复编写并发框架测试。

HTTP 测试使用 `httptest.Server`，RPC 测试使用固定 JSON fixture，不在普通 `go test ./...` 中依赖公网。

### 11.2 完成标准

实现完成需同时满足：

```bash
gofmt -w <modified go files>
go vet ./...
go test ./...
```

并进行一次人工 smoke test，确认：

- 五条新路线均出现观测；
- LST 行有 finalized 区块锚点；
- Kamino 历史行没有伪造区块锚点；
- Save 当前行有 `finalized_anchor`，历史行无区块锚点；
- 利率数量级与官方响应一致，尤其没有把 1.82% 写成 182%；
- 查询 `yield_route FINAL` 后没有重复经济路线；
- 日志没有持续重试或单轮异常大批量写入。

## 12. 第二阶段明确不做

- 不实现 marginfi、Drift 的 SDK 或链上账户解析；
- 不采集 Kamino Vault、Multiply、循环贷、杠杆策略或 Save isolated pools；
- 不采集 KMNO、SLND 等奖励币激励；
- 不采集 LST 的 DEX 即时退出滑点和金额分档报价；
- 不动态发现全部 Sanctum LST；
- 不把原生验证者、LST 与借贷叠加成复合策略行；
- 不写永续资金费率，不重复保存 OKX、Binance 的做空深度；
- 不建立风险审查表，不自动评价合约、运营方或验证者安全性；
- 不执行任何交易或链上写操作。

## 13. 官方资料

以下资料用于固定第二阶段的来源、地址与口径；实现前若地址与链上 owner 不一致，应停止而不是自动改用搜索结果：

- Kamino 历史 reserve metrics：<https://kamino.com/docs/build/api-reference/borrow/market-data/market-reserve-metrics-history>
- Kamino 市场数据说明：<https://kamino.com/docs/build/borrow/market-data-metrics>
- Kamino SOL reserve 示例：<https://kamino.com/docs/build/borrow/recipes/borrow-via-api>
- Kamino program 地址：<https://github.com/Kamino-Finance/klend-sdk>
- Save REST API：<https://api.solend.fi/>
- Save 开发者 REST API 说明：<https://dev.solend.fi/docs/api/>
- Save 链上 APY 公式：<https://docs.save.finance/developers/integration-guide>
- Sanctum 三种 Stake Pool deployment：<https://learn.sanctum.so/docs/creating-your-own-lst-with-sanctum/why-are-there-3-stake-pool-deployments>
- Sanctum program 地址：<https://learn.sanctum.so/docs/for-developers/deployed-programs>
- Sanctum LST 固定注册资料：<https://github.com/igneous-labs/sanctum-lst-list>
- Laine 官方 stake 页面：<https://stake.laine.one/>
- Helius hSOL 说明：<https://www.helius.dev/blog/what-is-hsol>
- Jupiter JupSOL FAQ：<https://docs.jup.ag/user-docs/earn/stake-sol/faq>

资料核对日期：2026-08-26。
