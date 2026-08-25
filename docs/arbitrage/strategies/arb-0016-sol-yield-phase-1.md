# ARB-0016 SOL 收益采集第一阶段实现设计

## 1. 第一阶段做什么

第一阶段按已经确认的四块实现，不再缩减范围：

| 模块 | 第一阶段产品 | 保存内容 | 主来源 |
|---|---|---|---|
| LST 通用 Stake Pool | bSOL；同一 Reader 也供 JitoSOL 校验 | 当前兑换率、TVL、链上费用、近似 APR | Solana finalized RPC |
| JitoSOL、mSOL 专用校验与历史 | JitoSOL、mSOL | 官方 APY 历史；JitoSOL 兑换率和 TVL；mSOL 兑换率 | Jito、Marinade 官方 API |
| SOL 原生验证者质押 | 配置中明确列出的 vote account | 每个已完成 epoch 的验证者 APY、佣金和 active stake | Marinade 官方 validators API |
| Marinade Native | Marinade Native | 官方滚动 APY 历史 | Marinade 官方 APY API |

程序仍然只采集公开数据，不创建 stake account，不申购、赎回或交易，不使用私钥，也不把永续资金费率写入收益表。

这里的“简单实现”具体指：

- 继续使用现有单进程、`yield.Runner`、ClickHouse writer 和两张收益表；
- LST 只读代码内固定地址，不动态发现池子；
- 原生验证者只读配置中的 vote account，不自动排名或选择验证者；
- HTTP 和 RPC 都是低频顺序请求，不增加消息队列、任务表或微服务；
- 不做多来源自动回退、收益口径融合、全链事件回放或自动风险评分。

第一阶段直接落库的通用池产品只有 bSOL。JitoSOL 虽然也是标准 SPL Stake Pool，但其 APY 和历史由 Jito 专用 collector 独占；mSOL 使用 Marinade 自有程序，不能按 SPL Stake Pool 布局解析。以后增加另一个标准 Stake Pool，只需核实官方 program、pool、mint 后在固定产品列表增加一项，不改变表和 Runner。

## 2. 五类 Runner 与唯一写入者

四块功能最终运行五类 Runner；配置多个原生验证者时，每个 vote account 各有一个 Runner：

```text
Solana RPC ──> stakepool.Reader ──> bSOL Collector ───────────┐
                   └─────────────> Jito 身份校验              │
Jito API ─────────────────────────> JitoSOL Collector ────────┤
Marinade APY API ─────────────────> mSOL Collector ───────────┤
Marinade validators API ──────────> Validator Collector × N ──┼─> yield.Runner
Marinade APY API ─────────────────> Marinade Native Collector ┘       │
                                                                      v
                                                         yield_route / yield_observation
```

每条经济路线只能有一个写入者：

| 路线 | 唯一写入者 |
|---|---|
| bSOL | 通用 Stake Pool collector |
| JitoSOL | Jito 专用 collector |
| mSOL | mSOL 专用 collector |
| `validator:<vote account>` | 该 vote account 的原生验证者 collector |
| Marinade Native | Marinade Native collector |

通用 Reader 可以校验 Jito pool，但不能替 JitoSOL 写观测；Marinade Native 底层即使委托到了已配置验证者，也仍是另一条产品路径，不算重复。第一阶段用固定列表和单元测试保证所有权，不新增通用插件或来源仲裁框架。

专用 API 失败时只形成该来源的历史缺口，不回退为另一个口径，也不拿当前兑换率伪造过去 APY。

## 3. 需要修改的现有模型

不新增表，只做两个必要的小改动。

### 3.1 一个 Batch 可以带同一路线的多个历史点

现有 `Batch.NormalizeAndValidate` 的批内唯一键是：

```text
(route identity, tier_no)
```

这会错误拒绝一个 API 响应中的多条历史观测。改成：

```text
(route identity, observation_time, tier_no)
```

同一路线、同一来源时刻、同一档位仍必须拒绝重复。数据库逻辑键本来就是 `(yield_route_id, observation_time, tier_no)`，不需要改表键。

### 3.2 `unbonding_seconds` 改为可空

Solana 的激活和退出等待取决于 epoch 及全网 stake 变化，不能诚实地写成固定秒数。因此：

```text
YieldObservation.UnbondingSeconds: uint64 -> *uint64
ClickHouse: UInt64 -> Nullable(UInt64)
```

统一语义为：

- `0`：正常退出没有强制等待；
- 正整数：来源给出了固定秒数；
- `NULL`：存在等待，但不能预先确定为固定秒数。

schema 初始化在建表语句后增加幂等迁移：

```sql
ALTER TABLE <database>.yield_observation
    MODIFY COLUMN unbonding_seconds Nullable(UInt64)
```

原有 TRX 的 `0` 和 `14 × 86400` 保持非空且数值不变。实现时同步修改 `docs/arbitrage/strategies/arb-0016-yield-data.md` 和 `docs/market-data-storage.md`。

## 4. 固定资产与产品身份

共同资产键：

```text
SOL     = solana:mainnet:native:SOL
bSOL    = solana:mainnet:spl:bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1
JitoSOL = solana:mainnet:spl:J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn
mSOL    = solana:mainnet:spl:mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So
```

固定地址：

| 产品 | program 或管理地址 | pool/state 地址 |
|---|---|---|
| SPL Stake Pool | `SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy` | 每个产品不同 |
| bSOL | 上述 SPL program | `stk9ApL5HeVAwPLr3TLhDXdZS8ptVu7zp6ov8HFDuMi` |
| JitoSOL | 上述 SPL program | `Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb` |
| mSOL | `MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD` | `8szGkuLTAux9XMgZ2vtY39jVSowEcpBfFfD8hXSEqdGC` |
| Marinade Native | stake authority `stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq` | 不产生 LST |

这些资产都位于 Solana mainnet，不经过桥或二层，也不是其他网络上的映射 SOL。地址只用于准确识别路线，并不替代以后对升级权限、合约、运营方和二级市场流动性的人工安全判断。

共同观测字段：

```text
tier_no = 1
tier_min_amount = 0
tier_max_amount = NULL
tier_mode = none
rate_mode = variable
lock_seconds = 0
unbonding_seconds = NULL
rule_principal_loss_mode = none
rule_eligibility = candidate
fixed_principal_loss_rate = NULL
```

这里把正常运行中的收益变化、validator 离线少赚和不可预知的 cooldown 时长，与规则内本金损失区分开。第一阶段按当前正常经济规则记为 `none/candidate`；极端网络攻击、网络重启时的非自动 slashing 决定、合约漏洞或治理恶意属于后续人工风险。如果 Solana 启用日常自动 slashing，必须先把相关路线改为 `variable/rejected`，再继续采集。

## 5. LST 通用 Stake Pool collector

### 5.1 最小实现

新增一个很小的 Solana JSON-RPC client 和一个只解析 SPL `StakePool` 状态的 Reader，不引入完整 Solana SDK，也不实现通用 Borsh 框架。

Reader 顺序调用：

1. `getAccountInfo(pool, encoding=base64, commitment=finalized)`；
2. `getEpochInfo(commitment=finalized, minContextSlot=context.slot)`；
3. `getBlock(context.slot, transactionDetails=none, rewards=false, commitment=finalized)`。

解析器只读取第一阶段实际使用的字段：

```text
pool_mint, total_lamports, pool_token_supply, last_update_epoch,
epoch_fee, stake_withdrawal_fee, sol_deposit_authority,
sol_deposit_fee, last_epoch_pool_token_supply,
last_epoch_total_lamports
```

必须校验：

- account owner 是固定 SPL Stake Pool program；
- account type 是 `StakePool`；账户物理长度必须恰好是当前部署的 611 字节，`Option`/`FutureEpoch` 使其中的有效 Borsh 前缀长度变化；逐字段解析不得越界且所有 tag 必须合法，读完当前已知最后字段后允许任意内容的剩余预分配尾部。官方 unchecked 读取和前缀写回不保证清零该尾部，因此不能把尾部非零当作新字段或错误；
- pool mint 与固定产品配置一致；
- `last_update_epoch == getEpochInfo.epoch`；
- 当前和上一个 epoch 的 lamports、token supply 都大于 0；
- 所有 fee 的 numerator/denominator 合法，费率不超过 1；
- block 返回的 slot 对应 `blockHeight`、`blockhash` 和 `blockTime` 完整存在。

Solana pubkey 只需一个包内小型 base58 解码函数，用来比较固定地址与状态中的 32 字节 pubkey；不为此建立新的资产注册系统。

### 5.2 bSOL 映射

路线定义：

```text
provider_type = protocol
provider = BlazeStake
product_code = bsol
product_name = BlazeStake bSOL
yield_type = liquid_staking
deposit_asset_key = SOL
position_asset_key = bSOL
redeem_asset_key = SOL
network = solana-mainnet
contract_address = stk9ApL5HeVAwPLr3TLhDXdZS8ptVu7zp6ov8HFDuMi
price_exposure_asset = SOL
income_source = combined
source_url = https://api.mainnet.solana.com
```

全程使用整数和 Decimal：

```text
current_ratio   = total_lamports / pool_token_supply
previous_ratio  = last_epoch_total_lamports / last_epoch_pool_token_supply
period_return   = current_ratio / previous_ratio - 1
approximate_apr = period_return × 182.625
```

`182.625 = 365.25 / 2`，与 SPL Stake Pool 官方源码中的近似 APR 测试一致。这个值是按默认两天 epoch 年化的近似值，不是精确预测。如果比率非正、计算结果为负或 pool 本 epoch 未更新，则整轮失败，不写一个看似正常的 APR。

| 观测字段 | 值 |
|---|---|
| `observation_time` | finalized context slot 对应 `blockTime` |
| `rate` / `rate_kind` / `rate_origin` | `approximate_apr` / `apr` / `derived` |
| `reward_asset_keys` | `[SOL]` |
| `reward_component_rates` | `[approximate_apr]` |
| `exposure_ratio` | `current_ratio`，SOL/bSOL |
| `tvl` | `total_lamports / 1_000_000_000`，单位 SOL |
| `entry_fee_rate` | 当前 `sol_deposit_fee` |
| `exit_fee_rate` | 当前 `stake_withdrawal_fee`；路线按取回 stake account 后正常 cooldown 到 SOL 定义 |
| `performance_fee_rate` | 当前 `epoch_fee`；APR 已由净兑换率变化得到，查询时不能再次扣除 |
| `availability` | `unknown`；只读 pool 状态不能证明任意金额此刻都可完整进出 |
| `block_height` / `block_hash` / `finality` | 本次 block 的值 / `finalized` |
| `source_payload_hash` | epoch、account 和 block 三个完整 RPC 响应的固定顺序 hash |

第一阶段的固定通用产品列表只包含 bSOL。Jito pool 交给下一节专用 collector，mSOL 不属于这种状态布局。

## 6. JitoSOL 与 mSOL 专用历史 collector

### 6.1 共同规则

两条路线各自使用一个 Runner。每轮先做固定身份校验，再抓官方历史接口：

- JitoSOL：通用 Stake Pool Reader 校验 program、pool 和 mint；Reader 只校验，不产生 Jito 行；
- mSOL：RPC 检查固定 state 仍由 Marinade program 拥有，固定 mint 仍由 SPL Token program 拥有且 decimals 为 9；第一阶段不解析整个 Marinade 自定义状态。

身份校验是当前采集轮的保护条件，不把当前 finalized block 冒充成旧 API 历史点的链上位置。专用 API 没有返回历史点对应 slot，因此专用历史行的 `block_height`、`block_hash` 和 `finality` 全部为空。RPC 校验的当前锚点只写日志，不参与历史值计算。

身份校验或历史 API 任一失败，本轮不写；不回退到通用池近似 APR，也不拿当前兑换率回填历史。

### 6.2 JitoSOL

路线定义：

```text
provider_type = protocol
provider = Jito
product_code = jitosol
product_name = JitoSOL
yield_type = liquid_staking
deposit_asset_key = SOL
position_asset_key = JitoSOL
redeem_asset_key = SOL
network = solana-mainnet
contract_address = Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb
price_exposure_asset = SOL
income_source = combined
source_url = https://kobe.mainnet.jito.network/api/v1/stake_pool_stats
```

每轮顺序请求：

```text
GET /api/v1/stake_pool_stats
GET /api/v1/jitosol_sol_ratio
```

分别取得 `apy[]`、`tvl[]` 和 `ratios[]`，按完全相同的 RFC3339 `date` 求交集，不能按数组下标拼接。共同时间点必须为 1–128 个，内部不得重复，最新点不得比采集时间旧 72 小时，也不得晚于采集时间 5 分钟。

每个共同点写一行：

| 字段 | 来源 |
|---|---|
| `observation_time` | `date` |
| `rate` / `rate_kind` / `rate_origin` | `apy.data` / `apy` / `reported` |
| `exposure_ratio` | `ratios.data`，SOL/JitoSOL |
| `tvl` | `tvl.data / 1_000_000_000`，单位 SOL |
| `reward_asset_keys` / `reward_component_rates` | `[SOL]` / `[apy.data]` |
| `availability` | `unknown` |
| `source_payload_hash` | 两个完整 API 响应的固定顺序 hash |

Jito API 的 APY 已是比例，例如 `0.051` 表示 5.1%；TVL 是 lamports。历史接口没有给出同一历史时刻的申购、退出和绩效费用，相关费用字段留空，不能把当前链状态费用复制到旧点。

### 6.3 mSOL

路线定义：

```text
provider_type = protocol
provider = Marinade
product_code = msol
product_name = Marinade mSOL
yield_type = liquid_staking
deposit_asset_key = SOL
position_asset_key = mSOL
redeem_asset_key = SOL
network = solana-mainnet
contract_address = 8szGkuLTAux9XMgZ2vtY39jVSowEcpBfFfD8hXSEqdGC
price_exposure_asset = SOL
income_source = combined
source_url = https://apy.marinade.finance/v1/rolling-apy/liquid-pool-token/MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD
```

每轮请求最近 30 天范围，滚动窗口固定为官方前端使用的 14 天：

```text
GET /v1/rolling-apy/liquid-pool-token/MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD
    ?window=1209600&from=<now-30d>&to=<now>
```

响应中的 `times[]`、`values[]`、`labels[]` 长度必须相同且为 1–64。每一项必须满足：

- `times[i]` 是不重复、递增的 Unix 秒；
- `values[i]` 是 `[0, 1)` 的 APY 比例；
- `labels[i].upperTime == times[i]`；
- `lowerTime < upperTime`，`lowerPrice` 和 `upperPrice` 都大于 0；
- 最新点距采集时间不超过 72 小时，未来偏差不超过 5 分钟。

每个点写：

| 字段 | 来源 |
|---|---|
| `observation_time` | `times[i]` |
| `rate` / `rate_kind` / `rate_origin` | `values[i]` / `apy` / `reported` |
| `exposure_ratio` | `labels[i].upperPrice`，SOL/mSOL |
| `reward_asset_keys` / `reward_component_rates` | `[SOL]` / `[values[i]]` |
| `tvl`、费用、额度、availability | `NULL` 或 `unknown`，接口没有提供 |
| `source_payload_hash` | 完整 API 响应 hash |

这里的每个值都是“截至该时刻的 14 日滚动 APY”，不是把 14 个历史点误当成 14 日窗口，也不再二次年化。

## 7. SOL 原生验证者质押

### 7.1 范围与请求

验证者不写死品牌名称，由配置提供 vote account 白名单。没有默认地址；个人使用通常配置 3–10 个即可，但程序不设置人为上限。启动时拒绝空字符串、非法 Solana pubkey 和重复地址。

每个 vote account 建一个独立 Runner。这样一个验证者接口失败只影响自己，也不需要为 N 个地址建立复杂批次状态。每轮只发一次请求：

```text
GET https://validators-api.marinade.finance/validators
    ?epochs=10&limit=1&query_vote_accounts=<vote_account>
```

必须严格得到 `total_count=1`、一个 validator，且返回的 `vote_account` 与请求完全相同。只接受 `epoch_stats` 中 1–10 个已经结束的 epoch：

- `epoch_end_at`、`apr`、`apy`、`commission_effective` 和 `activated_stake` 均存在；
- epoch 和 `epoch_end_at` 都不重复且不在未来；来源当前按新到旧返回，collector 校验后按 `epoch_end_at` 排序再写；
- APR、APY 都在 `[0, 1)`，APY 不小于 APR；
- `commission_effective` 在 0–100；存在 `mev_commission_bps` 时必须在 0–10000；
- `activated_stake` 是正的十进制整数字符串；
- 最新已完成 epoch 距采集时间不超过 7 天。

当前未结束 epoch 的 APY 通常为空，直接忽略，不能年化半个 epoch。

### 7.2 路线和字段

每个 vote account 是一条稳定路线：

```text
provider_type = native
provider = Solana
product_code = validator:<vote_account>
product_name = Solana validator <API info_name 或缩短地址>
yield_type = native_staking
deposit_asset_key = SOL
position_asset_key = SOL
redeem_asset_key = SOL
network = solana-mainnet
contract_address = <vote_account>
price_exposure_asset = SOL
income_source = combined
source_url = https://validators-api.marinade.finance/validators
```

`info_name` 只是可变展示信息，绝不能作为身份；vote account 同时进入 `product_code` 和 `contract_address`。

每个已完成 epoch 写一行：

| 字段 | 来源 |
|---|---|
| `observation_time` | `epoch_end_at` |
| `rate` / `rate_kind` / `rate_origin` | `apy` / `apy` / `reported` |
| `reward_asset_keys` / `reward_component_rates` | `[SOL]` / `[apy]` |
| `performance_fee_rate` | `commission_effective / 100` |
| `exposure_ratio` | `1` |
| `tvl` | `activated_stake / 1_000_000_000`，单位 SOL |
| `availability` | `unknown`；历史表现不等于当前可委托状态 |
| block 字段 | 全空；这是 Marinade 聚合 API，不伪造 RPC 锚点 |
| `source_payload_hash` | 该 vote account 的完整响应 hash |

保存的 APY 已是委托人收益口径，不能再次扣除 `performance_fee_rate`。`mev_commission_bps` 与普通 validator commission 不是同一种费用，现有表也没有分项费用结构；第一阶段只校验并由 payload hash 留证，不强行合成一个总费率。API 同时返回 APR，但一条观测只保存一种主利率，本阶段保存 APY，APR 只做响应校验。

## 8. Marinade Native

这是一条使用 Solana 原生 stake account、由 Marinade 管理委托策略的独立路线，不是 mSOL，也不拆成底层每个 validator 的重复观测。用户仍持有 withdraw authority，不产生桥币或 LST。

路线定义：

```text
provider_type = protocol
provider = Marinade
product_code = marinade-native
product_name = Marinade Native
yield_type = native_staking
deposit_asset_key = SOL
position_asset_key = SOL
redeem_asset_key = SOL
network = solana-mainnet
contract_address = stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq
price_exposure_asset = SOL
income_source = combined
source_url = https://apy.marinade.finance/v1/rolling-apy/marinade-native
```

请求同样使用最近 30 天范围和 14 日滚动窗口：

```text
GET /v1/rolling-apy/marinade-native
    ?window=1209600&from=<now-30d>&to=<now>
```

`times[]` 和 `values[]` 必须等长且为 1–64，时间、APY、最新点和未来偏差校验与 mSOL 相同。该接口也返回 `labels[]`，但其价格字段不用于 Marinade Native 路线，第一阶段忽略，不能当成 SOL/SOL 兑换率。

每个点保存 `rate=values[i]`、`rate_kind=apy`、`rate_origin=reported`、`reward_asset_keys=[SOL]`、`reward_component_rates=[values[i]]`、`exposure_ratio=1`。TVL、额度和 API 未返回的费用留空，`availability=unknown`，block 字段为空，完整响应参与 payload hash。

Marinade 文档中的即时退出报价会随市场变化，本阶段不记录为固定损失。正常 delayed unstake 的等待按 epoch 变化，所以 `unbonding_seconds=NULL`。如果以后采集实时退出报价，应作为单独市场报价数据，而不是改写收益 APY。

## 9. 数值、历史和失败规则

所有 JSON 数字统一使用 `json.Decoder.UseNumber`，再用 `decimal.NewFromString`；lamports、epoch、slot 和时间先严格解析为整数。任何收益率、比例和费用都不能经过 `float64`。

历史接口每 6 小时重取短历史窗口并 append 写入。重复的 `(route, observation_time, tier_no)` 由现有 ClickHouse 逻辑键合并，不增加游标、checkpoint 或补采任务表。按当前上限，个人工具的数据量很小。

每个 Runner 都沿用现有行为：

1. 启动立即采集；
2. HTTP、RPC、身份或完整性校验任一失败时不写半批；
3. 写库失败时保留已校验 Batch，只重试写入；
4. 正常间隔 6 小时，失败重试 10 分钟；
5. 一个 Runner 持续失败不终止其他 Runner；
6. 不写旧值冒充新值。

API 历史行只 hash 生成该行数值的完整 API 响应。bSOL 的链上行 hash 全部相关 RPC 响应。Jito/mSOL 的当前链身份检查不生成历史数值，其 block 锚点只用于本轮校验日志，不能填进旧 API 点。

## 10. 配置和文件

新增最少配置：

```text
SOL_YIELD_ENABLED=false
SOLANA_RPC_URL=https://api.mainnet.solana.com
SOL_VALIDATOR_VOTE_ACCOUNTS=-
JITO_SOL_BASE_URL=https://kobe.mainnet.jito.network
MARINADE_APY_BASE_URL=https://apy.marinade.finance
MARINADE_VALIDATORS_BASE_URL=https://validators-api.marinade.finance
```

`SOL_YIELD_ENABLED=true` 时启动 bSOL、JitoSOL、mSOL 和 Marinade Native；`SOL_VALIDATOR_VOTE_ACCOUNTS` 非空时再为每个地址启动一个原生验证者 Runner。base URL 可覆盖只用于测试和排障，program、pool、state、mint 和计算公式不允许从环境变量随意替换。

新增代码集中在四个小包：

```text
internal/yield/solana/       # JSON-RPC、StakePool Reader、bSOL collector
internal/yield/jito/         # Jito 历史接口和 collector
internal/yield/marinade/     # rolling APY、mSOL、Marinade Native
internal/yield/solvalidator/ # validators API 和单 vote-account collector
```

每个包可以合并 client 和 collector 文件；不要求为了形式拆很多层。现有 `model.go`、config、app、ClickHouse schema/writer 和相应测试只做接线及第 3 节的小改。

## 11. 测试与完成标准

单元测试使用固定 fixture 和 `httptest.Server`，不在自动测试中依赖外网。至少覆盖：

1. SPL Stake Pool 的 611 字节账户中，不同长度的有效 Borsh 前缀和非零预分配尾部正常解析；错误 owner/type/mint、截断或超过 611 字节、非法 tag、陈旧 epoch、非法 fee 全部失败；
2. bSOL 兑换率、TVL、182.625 近似 APR 和三种费用全部走 Decimal，finalized block 锚点正确；
3. generic 产品列表不写 JitoSOL，mSOL 不经过 SPL parser；
4. Jito 三组时间顺序不同仍按时间求交集；缺数组、重复点、过期、未来点和超过 128 点整批失败；
5. mSOL 三个数组严格对齐，`upperTime` 和 `upperPrice` 正确映射；Marinade Native 不误用 label price；
6. 同一路线不同 observation time 可在一个 Batch；同路线同时间同 tier 仍拒绝；
7. 每个 validator 只接受请求的 vote account，只写已完成 epoch；当前 epoch、重复 epoch、非法 commission、非法 stake 和过期历史失败；
8. validator APY 不重复扣佣金，也不把 APR 另写成第二条观测；
9. JitoSOL 只有专用 collector 写，当前链校验不产生第二条同路线观测；
10. Marinade Native 不展开为验证者路线；
11. 所有实时观测都有 payload hash；API 历史点 block 字段为空，bSOL 链上点有真实 finalized 锚点；
12. `unbonding_seconds` 迁移保留原 TRX 值并允许 SOL 写 NULL；
13. 多个 validator Runner 中一个失败不影响其他来源；
14. 配置默认关闭，vote account 校验、去重和接线正确。

实现顺序：

1. 修改 Batch 唯一键和 nullable `unbonding_seconds`；
2. 实现 Solana RPC、StakePool Reader 和 bSOL；
3. 实现 JitoSOL 与 mSOL 专用历史；
4. 实现单 vote-account 原生验证者 collector；
5. 实现 Marinade Native；
6. 接入 config/app，更新数据字典，运行 `gofmt`、`go test ./...` 和一次人工官方接口 smoke test。

完成标准是：四块都能独立稳定运行并持续形成历史；同一经济路线只有一个写入者；来源失败产生明确缺口；链上数据有 finalized 锚点，聚合 API 不伪造锚点；没有永续资金费率、账户、私钥和交易执行。

## 12. 官方来源

- [SPL Stake Pool 官方源码与状态布局](https://github.com/solana-program/stake-pool)
- [Solana Stake Pool 说明](https://github.com/solana-labs/solana-program-library/blob/master/docs/src/stake-pool/overview.md)
- [Solana stake account、warmup 与 cooldown](https://solana.com/docs/references/staking/stake-accounts)
- [Solana 质押和 slashing 说明](https://solana.com/staking)
- [Jito 已部署 program、pool 和 mint](https://www.jito.network/docs/jitosol/jitosol-liquid-staking/security/deployed-programs/)
- [Jito stake pool stats API](https://kobe.mainnet.jito.network/api/v1/stake_pool_stats)
- [JitoSOL/SOL ratio API](https://kobe.mainnet.jito.network/api/v1/jitosol_sol_ratio)
- [BlazeStake 主网地址](https://stake-docs.solblaze.org/developers/addresses)
- [Marinade program、state 和 mSOL mint](https://docs.marinade.finance/developers/contract-addresses)
- [Marinade mSOL 兑换率说明](https://github.com/marinade-finance/liquid-staking-referral-program)
- [mSOL rolling APY API](https://apy.marinade.finance/v1/rolling-apy/liquid-pool-token/MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD)
- [Marinade Native 原理、权限与退出](https://docs.marinade.finance/marinade-protocol/protocol-overview/marinade-native)
- [Marinade Native API、stake authority 与应急退出](https://docs.marinade.finance/marinade-protocol/protocol-overview/marinade-native/marinade-native-api-and-sdk)
- [Marinade validators API 源项目](https://github.com/marinade-finance/delegation-strategy-2)
- [Marinade validators API](https://validators-api.marinade.finance/validators)
- [Marinade Native rolling APY API](https://apy.marinade.finance/v1/rolling-apy/marinade-native)
