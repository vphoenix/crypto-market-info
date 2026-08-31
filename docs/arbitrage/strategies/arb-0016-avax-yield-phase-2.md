# ARB-0016 AVAX 收益采集第二阶段实现设计

状态：代码及双方实现复审通过，已完成生产库迁移并部署；三条路线首批数据已成功写入。接口核验及评审日期：2026-08-27（北京时间）。

“第二阶段”是此前安排的 **BENQI sAVAX、Ankr ankrAVAX、BENQI AVAX 借贷**。第一阶段的 OKX、Aave V3/V4 保持不变；原生委托属于第三阶段。

## 1. 最小方案

| 路线 | 本期自动保存 | 利率如何取得 |
|---|---|---|
| AVAX → BENQI sAVAX → AVAX | 兑换率、池内现金、规模、额度、费用和退出期 | 先积累兑换率历史，`rate=NULL` |
| AVAX → Ankr ankrAVAX → AVAX | 兑换率、池内现金 | 先积累兑换率历史，`rate=NULL` |
| AVAX → BENQI qiAVAX → AVAX，只存不借 | 基础存款 APR、计息兑换率、池内现金、规模、存入状态 | 链上每秒基础存款利率换算 APR |

三条都按每小时采一个当前链上快照，不抓页面宣传 APY。LST 的空利率不表示零收益或采集失败；其收入体现在每份凭证对应 AVAX 数量的变化中。以后查询两个时点的兑换率，就能计算这段时间的持有收益。

继续复用 `yield_route`、`yield_observation` 两表，只给观测表增加两个可空字段：池内现金余额 `pool_cash`、赎回申领窗口 `redemption_window_seconds`。不新增服务、表、队列、通用 ABI 框架或历史索引器。

不包含：原生节点委托、sAVAX 再存借贷、QI/AVAX 额外流动性激励、LP、杠杆循环、DEX 即时卖出报价、做空行情、永续资金费率、账户/私钥、申购或赎回交易、安全审查表。本期是采数据，不保证产品或退出通道安全。

## 2. 固定路线和资产身份

来源：[BENQI 质押地址](https://docs.benqi.fi/resources/contracts/liquid-staking)、[BENQI Core Markets](https://docs.benqi.fi/resources/contracts/core-markets)、[Ankr AVAX 合约](https://www.ankr.com/docs/staking-for-developers/smart-contract-api/avax-api/)。地址比较和入库统一小写。

```text
chain_id       = 43114
network        = avalanche-c-mainnet
sAVAX          = 0x2b2c81e08f1af8835a78bb2a90ae924ace0ea4be
ankrAVAX       = 0xc3344870d52688874b06d844e0c36cc39fc727f6
Ankr pool      = 0x7baa1e3bfe49db8361680785182b80bb420a836d
qiAVAX         = 0x5c0401e81bc07ca70fad469b451682c0d747ef1c
BENQI controller = 0x486af39519b4dc9a7fccd318217352830e8ad9b4

N = eip155:43114:native:AVAX
S = eip155:43114:erc20:<sAVAX>
A = eip155:43114:erc20:<ankrAVAX>
Q = eip155:43114:erc20:<qiAVAX>
```

`N/S/A/Q` 只是本文缩写，实现时展开为完整资产键。特别注意：本期 BENQI 市场接收原生 AVAX，不是 WAVAX；不能沿用 Aave 的 WAVAX 资产键。qiAVAX 精度为 8，其他两种凭证精度为 18。

| 路线字段 | BENQI sAVAX | Ankr ankrAVAX | BENQI AVAX 借贷 |
|---|---|---|---|
| `provider` | `BENQI` | `Ankr` | `BENQI` |
| `product_code` | `avalanche-savax-staking` | `avalanche-ankravax-staking` | `avalanche-qiavax-supply` |
| `product_name` | BENQI sAVAX 质押兑换率 | Ankr ankrAVAX 质押兑换率 | BENQI AVAX 基础借贷收益 |
| `yield_type` | `liquid_staking` | `liquid_staking` | `lending` |
| `position_asset_key` | `S` | `A` | `Q` |
| `contract_address` | sAVAX | Ankr pool | qiAVAX |
| `income_source` | `issuance` | `issuance` | `borrow_interest` |

共同字段：`provider_type=protocol`、`deposit_asset_key=redeem_asset_key=N`、`price_exposure_asset=AVAX`、`collection_enabled=true`；`network` 见上。`source_url` 使用各自上述官方合约页面，不保存可能带凭据的自定义 RPC URL。

C-chain 是本期读取和存入/兑回资产所在网络，不表示所有底层 AVAX 都留在 C-chain。BENQI 官方说明涉及 MPC 管理的 C/P-chain 转移，Ankr 涉及运营方后台和质押地址；这些托管与回流依赖留待人工检查，不能因为链上有兑换率就判定一定能兑回。[BENQI 架构](https://docs.benqi.fi/benqi-liquid-staking/architecture)、[Ankr 工作流程](https://www.ankr.com/docs/staking-for-developers/dev-details/avax-liquid-staking-mechanics/)

## 3. 共用采集流程：所有数据固定在同一个区块

默认 RPC：`https://api.avax.network/ext/bc/C/rpc`。只用公开只读 JSON-RPC，不依赖浏览器接口、区块浏览器 API key 或付费归档节点。[C-chain RPC 文档](https://build.avax.network/docs/rpcs/c-chain/api)

每条路线使用一个现有 `yield.Runner`，启动立即采集，正常间隔 `1h`，失败间隔 `10m`。三条 Runner 互相独立，共用一个轻量 RPC client 和请求 gate（请求起始间隔至少 `1s`）；不与盘口链路共用限频器。

每次 `Collect`：

1. 第一个 JSON-RPC batch 请求 `eth_chainId` 和 `eth_getBlockByNumber("finalized", false)`；校验主网 43114，取得区块高度、哈希、时间。
2. 第二个 batch 读取本路线下文列出的全部合约字段/现金。每个请求都显式固定到第一个请求取得的区块哈希：

   ```json
   {"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"<固定合约地址>","data":"<selector 和参数>"},{"blockHash":"<该 finalized 区块哈希>","requireCanonical":true}]}
   ```

   `eth_getBalance` 同样使用该 block hash 对象。默认公共 RPC 已实测支持这两种请求，不使用多个 `latest` 读数拼接快照。自定义节点不支持时该路线报错，不降级成无锚点读取。
3. 所有必需读数解析、身份和数值校验成功后，生成恰好一条完整观测。`observation_time=区块 timestamp`（UTC），`collected_at=本次完整读取结束的 UTC 时间 T`，`block_height/hash` 对应该区块，`finality=finalized`。
4. 区块时间必须在 `[T-10分钟, T+1分钟]` 内；节点返回过期 finalized 头不能更新采集时间冒充新数据。兑换率本身没有变化则可以正常保存。
5. 通过 `Batch.NormalizeAndValidateForLiveCollection` 后交给现有 writer。

RPC batch 最多 40 项；本期每条远低于此限制，不引入自动拆批。HTTP 超时 `20s`，复用 `exchange.PostJSON`、现有有限重试和 gate，不无限重试。

自定义 `AVALANCHE_RPC_URL` 可能在路径、query 或 userinfo 中带凭据。现有 `exchange.PostJSON` 的错误会含完整 URL/响应片段，Runner 又会直接记录错误；因此**必须在新增 `avalanche.Client` 边界做局部脱敏**：对外只返回固定阶段/方法名、固定错误类别及可安全取得的数值 HTTP/RPC code；不能直接返回或 `%w` 包装含 URL 的底层错误，也不输出 provider 的 `message/data/body`、未通过校验的来源文本或原始请求 URL。无法安全取得状态码时，固定返回“该阶段 RPC 请求失败”等描述即可；取消/超时可保留安全的 context 错误类别。不改整个 HTTP 或日志框架，不增加凭据管理系统。

必须严格校验：

- JSON-RPC `2.0`、ID 唯一且与请求集合完全一致；结果可以乱序，但缺项、重复项、未知 ID、`error`、`null`、尾随第二段 JSON 都使本路线整轮失败。HTTP 200 不等于成功。
- 合约返回按固定 ABI 解码：单值必须恰为 32 bytes；bool 只接受 0/1，address 高 12 bytes 必须为 0；`markets` 恰为三个 word。返回空 `0x` 不是数字 0。
- RPC quantity（高度、时间、余额）与 ABI word 分开解析。用 `math/big.Int` 解析 uint256，用受限 uint64 保存高度/秒数；不得经过 `float64`。哈希必须是 32 bytes，地址必须是 20 bytes。
- 只校验使用的字段，允许区块响应新增无关字段。不自动发现新市场、替换地址或猜测新的精度。合约调用异常或返回结构变化时报告失败，留待人工复核。
- 正数兑换率、非负金额/利率、费用比例在 `[0,1]`。整数缩放用 `decimal.NewFromBigInt` / `Shift`；有除法时先使用大整数算商，或显式指定足够精度，不能使用默认 16 位的 `Decimal.Div`。写入前检查值小于 `10^20`，再显式截断至 18 位；截断后兑换率仍须大于 0。

失败不写一行“当前数据全 NULL”，不沿用上次兑换率配新时间。单条协议/解析失败不阻止另外两条及第一阶段；但三条共用的 RPC 整体故障或限流可能令本期三条同时缺数，不能声称消除了共同节点依赖，第一阶段的独立 HTTP 来源仍继续运行。

## 4. 各路线读取和换算

### 4.1 BENQI sAVAX

全部 getter 对 **sAVAX 代理地址** 调用，不把实现地址当资产地址。当前已验证实现为 `0xb791c7a42fd0d10f90deaa906a8735f79719fa53`；公式、费用与历史数组行为核对其[已验证源码](https://api.routescan.io/v2/network/mainnet/evm/43114/etherscan/api?module=contract&action=getsourcecode&address=0xb791c7a42fd0d10f90deaa906a8735f79719fa53)，不照搬较旧 GitHub 版本的奖励方法签名。

| 读取方法 | Selector | 用途/换算 |
|---|---|---|
| `decimals()` | `0x313ce567` | 必须为 18 |
| `getPooledAvaxByShares(10^18)` | `0x4a36d6c1` | 返回整数 `R`，`exposure_ratio=R / 10^18` |
| `totalPooledAvax()` | `0x629e8056` | `tvl=返回值 / 10^18`，单位 AVAX，含其账面质押资产 |
| `totalPooledAvaxCap()` | `0x5cd47487` | 原生 AVAX 最小单位的入池上限，见下文 |
| `protocolRewardShare()` | `0x6e34637c` | `performance_fee_rate=返回值 / 10^18` |
| `cooldownPeriod()` | `0x04646a49` | `unbonding_seconds` |
| `redeemPeriod()` | `0x40a233a6` | `redemption_window_seconds` |
| `paused()` | `0x5c975abb` | 暂停状态 |
| `mintingPaused()` | `0xe1a283d6` | 新铸造暂停状态 |
| `eth_getBalance(sAVAX, blockHash)` | RPC 方法 | `pool_cash=返回值 / 10^18`，仅该 C-chain 合约现金 |

有参数的 ABI 编码为 selector 后接一个 32-byte 左补零整数；不用 JavaScript number/Go float 构造 `10^18`。

额度规则：

- `cap_raw=2^256-1` 是无限上限哨兵，**先识别哨兵**，`capacity/remaining_capacity=NULL`；不能直接塞入 Decimal 而溢出，也不能伪造为 0。
- 其他 cap 按 18 位缩放；`capacity=cap`，`remaining_capacity=max(cap-tvl,0)`。奖励可能使 `tvl>cap`，这是额度用尽，不是解析失败。有限 cap 超出数据库范围则报错。
- `availability`：任一暂停标记为真 → `paused`；否则有限剩余额度为 0 → `unavailable`；其余 → `available`。这里只表达上述存入规则状态，不承诺能立即全额退出。

`rate=NULL`、`rate_kind=unknown`，不把链上的兑换率数值当 APY。`protocolRewardShare` 已在奖励进入 `totalPooledAvax` 前扣除；按兑换率计算持有收益时不能再次扣绩效费。

本轮链上固定等待为 1,296,000 秒（15 天），申领窗口为 172,800 秒（2 天）；以后每轮读取，不硬编码。窗口是“冷却结束后多久内可申领”，不能加到冷却期上写成固定等待 17 天。合约允许将 `redeemPeriod` 设置为 0，此时没有可用申领窗口，必须保存 0 而不是拒绝整条快照或改成 NULL；`availability` 仍只表达存入状态，但该次 `rule_eligibility=unknown`，见第 5 节。逾期应走退回凭证/重新申请流程，不假设立即取到 AVAX；冷却期间与申领窗口的计息规则不同，历史持有兑换率不等于整段退出流程净收益。[官方退出规则](https://docs.benqi.fi/benqi-liquid-staking/overview)

### 4.2 Ankr ankrAVAX

只读 ankrAVAX 的 `decimals()`（必须为 18）、`ratio()`（`0x71ca337d`），以及 `eth_getBalance(Ankr pool, blockHash)`。ratio 必须大于 0。

**ratio 的方向与本表正好相反：**原值除以 `10^18` 是 ankrAVAX/AVAX，而 `exposure_ratio` 必须是 AVAX/ankrAVAX。

```text
ratio_raw = ratio()
redeem_rate_raw = floor(10^36 / ratio_raw)        # 大整数除法
exposure_ratio = redeem_rate_raw / 10^18
pool_cash = Ankr pool 的原生余额 / 10^18
```

该公式与官方[兑换率提供器](https://www.ankr.com/docs/staking-for-developers/oracles/redemption-price-oracle/) `0xd70c8aac058e6dafe3446f78091325f9e29bcee4` 的 `getRate()` 及其[源码](https://api.routescan.io/v2/network/mainnet/evm/43114/etherscan/api?module=contract&action=getsourcecode&address=0xd70c8aac058e6dafe3446f78091325f9e29bcee4)一致；本轮已交叉实读验证，运行时不再增加这个重复来源依赖。

以下值明确保持未知：

- `rate=NULL`、`rate_kind=unknown`。官方 REST 虽返回 `apy`、`apyBasis=WEEK`，但名称与文档简单年化公式有歧义，且没有来源时间/区块；不混进同块观测，也不另造一条宣传 APY 路线。[REST 指标说明](https://www.ankr.com/docs/staking-for-developers/restful-api/staking-metrics/)
- `unbonding_seconds=NULL`、`redemption_window_seconds=NULL`。开发文档说等验证周期结束，周期约 4 周；[FAQ](https://www.ankr.com/docs/liquid-staking/avax/faq/)写依请求时点约 0–35 天。两者都不能变成每笔固定等待 28 天或固定申领窗口。
- 所有费用字段为 `NULL`。文档说明奖励服务费 10%，但本期未找到同块可读的费用 getter；该规则留在文档，不写成每个区块实测的费率。兑换率体现的净奖励不能再重复扣这 10%。
- `capacity/remaining_capacity/tvl=NULL`、`availability=unknown`。合约现金不能当作申购额度、总底层资产或可立即赎回额；继承的 `paused()` 并不足以证明完整业务是否可用，因此不采它推断状态。

官方规则及本轮源码核验的最低申购为 1 AVAX，且有整数倍限制。本期没有申购门槛/步长字段，先留在文档，不把这条要求冒充利率阶梯或为了它再加表。只采 ankrAVAX，不采旧的 aAVAXb。[Ankr 规则](https://www.ankr.com/docs/staking-for-developers/dev-details/avax-liquid-staking-mechanics/)

### 4.3 BENQI AVAX 基础借贷

| 对象/读取方法 | Selector | 用途/换算 |
|---|---|---|
| qiAVAX `decimals()` | `0x313ce567` | 必须为 8 |
| qiAVAX `isQiToken()` | `0x840bbeac` | 必须为 true |
| qiAVAX `comptroller()` | `0x5fe3b567` | 必须等于第 2 节固定 controller |
| qiAVAX `supplyRatePerTimestamp()` | `0xd3bd2c72` | 每秒基础供给利率 `s`，18 位定点 |
| qiAVAX `exchangeRateCurrent()` | `0xbd6d894d` | 模拟计息后的兑换率整数 `E` |
| qiAVAX `totalSupply()` | `0x18160ddd` | qiAVAX 最小单位的总供给 `Tq` |
| qiAVAX `getCash()` | `0x3b1d21a2` | `pool_cash=返回值 / 10^18` AVAX |
| controller `markets(qiAVAX)` | `0x8e8f294b` | 校验三元组，读取第一个 bool `isListed` |
| controller `mintGuardianPaused(qiAVAX)` | `0x731f0c2b` | 存入暂停标记 |

```text
rate = s × 31536000 / 10^18
rate_kind = apr
rate_origin = derived
exposure_ratio = E / 10^(18 + 18 - 8) = E / 10^28
tvl = E × Tq / 10^36                    # 单位 AVAX，按当前兑换率的总存款权益
```

年固定按 365 天换算。这是 getter 返回的每秒基础存款利率的简单年化，不是借款利率，不是每区块利率，不套用 Compound 的假设区块数，也不复刻前端可能包含激励的 APY。供给利率已经考虑储备分成，不再减一次 reserve factor。

`exchangeRateCurrent` 虽在 ABI 中不是 `view`，仍只通过 `eth_call` 模拟，**不发送交易**。它将截至区块时间的未入账利息计入兑换率；`exchangeRateStored` 可能停在较早的计息点，本期不使用。其他 getter 独立读取同块状态，不把一次模拟的内存修改误认为会影响下一次 RPC。利率保存 `supplyRatePerTimestamp` 的返回事实，不声称它是模拟后再次计算的利率。[BENQI QiToken 源码](https://github.com/Benqi-fi/BENQI-Smart-Contracts/blob/master/lending/QiToken.sol)

`E×Tq` 先用大整数相乘，不能先把 `E` 截断到 18 位小数再算 TVL。只有最终映射入库时才截断。

`availability`：未上市 → `closed`；否则暂停存入 → `paused`；其余 → `available`。现金为 0 不等于禁止存入，也不代表可以全额赎回。`capacity/remaining_capacity=NULL`，不把 `getCash` 填到剩余申购额度里。这里只存不借，不读取用户抵押率或清算状态；额外 QI/AVAX 激励完全不计入 `rate` 或奖励数组。

## 5. 共同字段、两项表结构补充

| 观测字段 | 本期规则 |
|---|---|
| `tier_no / tier_min_amount / tier_max_amount / tier_mode` | `1 / 0 / NULL / none`；没有利率分档 |
| `rate_mode` | `variable` |
| `rate_origin` | sAVAX、Ankr 为 `reported`（无 rate 时不参与解释）；BENQI 借贷为 `derived` |
| `reward_asset_keys / reward_component_rates` | `[N] / [NULL]`（两种 LST）；`[N] / [rate]`（借贷基础利息）；LST 的 AVAX 奖励体现在兑换率中，不是凭空增加另一笔现金分红 |
| `lock_seconds` | `0`，不存在本期所定义凭证持有人的固定强制锁仓；不等于立即得到原生 AVAX |
| `unbonding_seconds` | sAVAX 按链上 cooldown；Ankr 为 `NULL`；借贷为 `0`，表示没有协议设定的冷却计时器，实际退出仍取决于现金等条件 |
| `redemption_window_seconds` | sAVAX 按链上 redeemPeriod；其余 `NULL`，不推断为无限窗口或 0 |
| `entry_fee_rate / exit_fee_rate` | sAVAX 为 `0 / 0`（仅协议正常进出费用）；Ankr、借贷为 `NULL` |
| `performance_fee_rate` | 仅 sAVAX 读取；其余 `NULL`，不以 reserve factor 代替此字段 |
| 固定罚金、固定费用金额/币种、固定本金损失率 | `NULL`；链上 gas 不伪装成固定费用 0 |
| `rule_principal_loss_mode / rule_eligibility` | sAVAX、借贷按正常协议兑付/只存不借口径为 `none / candidate`；Ankr 为 `unknown / unknown`，原因 `redemption_rules_not_fully_reviewed`，不妨碍记录兑换率 |

sAVAX 的零进出费是本次已核验实现/规则的静态解释，不是额外读到了费用 getter，也不代表自动审计所有未来代理升级；发现实现或规则变化后须人工复核该映射。本期不为此增加代理升级监控或 codehash 系统。

一个明确例外：sAVAX 当轮 `redeemPeriod=0` 时，`rule_principal_loss_mode` 仍为 `none`（没有窗口不等于本金罚没），但 `rule_eligibility=unknown`、`eligibility_reason=redemption_window_empty`，不能把当时缺少正常 AVAX 申领窗口的观测当作通过初筛。窗口恢复正数后，按该轮正常规则填写 `candidate`，原因留空；不引入跨轮状态机。

`candidate` 只沿用本项目的规则内初筛，不表示已审查合约、坏账、治理、MPC、运营方或 C/P-chain 回流安全；不以 DEX 折价卖出作为无损退出依据。不新增自动风险打分。

两个新增字段与现有观测是相同路线、相同区块、相同完整性语义，可放在原表；没必要另建“流动性表”。

| 新字段 | 类型 | 精确定义 |
|---|---|---|
| `pool_cash` | `Nullable(Decimal(38,18))` | 本路线指定池合约内的底层现金余额，单位 `redeem_asset_key`。0 是实读无现金，NULL 是未采/不适用。包含可能另有用途或已被其他请求占用的余额；不是保证可立即赎回量，不是 TVL，不是申购额度。 |
| `redemption_window_seconds` | `Nullable(UInt64)` | 发起退出并完成等待后，协议规定的有限申领窗口长度。正数表示明确有限窗口；0 表示实读零长度、没有可用申领窗口，不是无限期；NULL 表示未采、不适用或无法确认。 |

现金对象固定：sAVAX 合约、Ankr pool、qiAVAX 合约；不能混入别的托管地址、DEX 池子或 P-chain 质押余额。现金可以大于/小于 TVL，没有通用 `pool_cash<=tvl` 约束，更不能由它断言某人一定能取款。

`SchemaStatements` 的建表语句已加入字段，并为现存数据库追加幂等迁移（下列库名仅为示例，**本轮不对生产库执行**）：

```sql
ALTER TABLE crypto_market_info.yield_observation
    ADD COLUMN IF NOT EXISTS pool_cash Nullable(Decimal(38, 18)),
    ADD COLUMN IF NOT EXISTS redemption_window_seconds Nullable(UInt64);
```

代码必须使用配置中的数据库名生成语句，不能硬编码示例库名。旧记录/旧 collector 默认 `NULL`，不回写历史，不改主键、分区、排序键或引擎。不删除或重建现有表。仅有 `CREATE TABLE IF NOT EXISTS` 不会补旧表的列，必须保留上述 ALTER 路径。

已同步修改 `YieldObservation` 的 `PoolCash`、`RedemptionWindowSeconds` 指针字段、校验、writer 显式 INSERT 列和 Append 参数。现金非负且 Decimal 范围有效，申领窗口按 uint64 校验并允许 0。新字段不会影响第一阶段、SOL/TRX 等现有 collector 的行为。

## 6. 证据、入库和失败重试

每轮哈希使用现有 `yield.HashPayloads`，按固定顺序加入四份原始字节：`anchor-request`、`anchor-response`、`state-request`、`state-response`。第一个 batch 同时包含 chain ID 和 finalized 区块；第二个包含所有字段调用及固定 block hash。不得用解析后的对象重新序列化代替原始响应，也不混入 `collected_at`、RPC 凭据或本机时间。

每条观测的 `source_payload_hash` 必填。响应顺序变化可能改变 hash，但 hash 不是唯一键，不能用它做去重。哈希不能恢复原文，本期不增加原始 JSON 热表或冷存档系统。

沿用 `WriteYieldBatch`：先登记/复用路线 ID，再写完整观测。逻辑键仍是 `(yield_route_id, observation_time, tier_no)`，查询使用 `FINAL`。同一路线只有一个串行 Runner，不能同时启动另一个写入者。

写入失败保留同一个 pending Batch，重试不重新读取链头、不改时间或 hash。两表没有事务，允许先登记路线而本轮观测暂未写入；不能误报整体成功。来源读取失败则没有 Batch，下一次 Runner 重试重新读当前 finalized 区块。

## 7. 历史能保存到什么程度

本期明确交付的是**从启动日起积累的每小时链上历史**。不承诺第一次运行即可得到完整过去 APY，也不把当前费用、现金、退出规则回填到过去。

- BENQI 官方说实时读合约、历史走 subgraph，但文档列出的历史入口不等于已经验证可用的完整利率曲线。本期不接旧 hosted subgraph。[数据获取说明](https://docs.benqi.fi/developers/faq)
- sAVAX 的 `historicalExchangeRateTimestamps` 是供赎回使用的短期数组，会清理早于 `redeemPeriod+172800秒` 的条目，且没有公开数组长度 getter；不能误当作上线以来完整 APY 接口，不做试探越界遍历。
- 默认公共 RPC 的较远历史状态读取本轮未成功验证。将来确有回补需求且有可靠历史节点后，可按历史 block hash 重读相同字段；本期不建设归档节点、扫日志器、游标或独立回补进程。
- 进程停机形成的缺口如实保留，不用前值填补。跨缺口的两个真实兑换率仍可计算整个区间累计变化，但不能知道区间内每小时路径。

后续查询层计算 LST 持有收益的口径：

```text
持有期收益比例 = R1 / R0 - 1
该区间简单年化 APR = (R1 / R0 - 1) × 31536000 / 实际相隔秒数
```

`R0/R1` 必须是同一路线、两条真实观测的 `exposure_ratio`。建议显示至少 7 天实际区间及起止时间；不足两点不计算。查询计算继续用 Decimal/大整数；不使用 `Float64` 或把简单年化标成 APY。不把该结果重新写入 `rate`，以免同路线混入历史窗口统计与即时观测。兑换率若下降，要保留原始有效读数及查询得到的负收益，不能改成 0 或丢掉样本。

这种计算反映持有凭证的兑换价值增长，不包括个人申购/退出时点、窗口内停息、gas、DEX 折价或对冲成本，也不是未来收益承诺。本期只附查询方法，不增设收益计算服务。

生产库已完成迁移，可查询原始历史：

```sql
SELECT r.product_code, o.observation_time, o.rate, o.rate_kind,
       o.exposure_ratio, o.pool_cash, o.tvl, o.availability,
       o.unbonding_seconds, o.redemption_window_seconds,
       o.block_height, o.block_hash, o.finality, o.source_payload_hash
FROM (SELECT * FROM crypto_market_info.yield_observation FINAL) AS o
INNER JOIN (SELECT * FROM crypto_market_info.yield_route FINAL) AS r
    ON o.yield_route_id = r.yield_route_id
WHERE r.product_code IN ('avalanche-savax-staking',
    'avalanche-ankravax-staking', 'avalanche-qiavax-supply')
ORDER BY r.product_code, o.observation_time;
```

## 8. 编程文件与验收

最小改动位置：

```text
internal/yield/avalanche/client.go + client_test.go   # 只读 RPC、固定 ABI 基础类型、锚点
internal/yield/benqi/staking.go + staking_test.go     # sAVAX
internal/yield/benqi/lending.go + lending_test.go     # qiAVAX 基础借贷
internal/yield/ankr/collector.go + collector_test.go  # ankrAVAX
internal/yield/model.go + model_test.go
internal/storage/clickhouse/schema.go + schema_test.go
internal/storage/clickhouse/yield.go + 相关集成测试
internal/config/config.go + config_test.go
internal/app/app.go + app_test.go
```

不引入 go-ethereum 全套 SDK；本期只需无参 getter、一个 uint256 参数、一个 address 参数及上述固定返回类型。selector 固定为常量并用测试核对，不做运行时 ABI 下载或代码生成。

只增加 `AVALANCHE_RPC_URL`（默认第 3 节公共地址）；复用 `AVAX_YIELD_ENABLED` 总开关，默认仍为 `false`。启用时第一阶段三条加本期三条，共六个 Runner。测试通过构造器注入 `httptest` 地址，不为每条路线、地址、周期新增环境变量。RPC 短暂不可用由各 Runner 重试，不能阻止第一阶段继续运行。

实现验收至少覆盖：

1. 单元测试完全离线：真实样本制作的小 fixture + `httptest`；HTTP/RPC 错误、ID 缺失/重复/乱序、部分成功、非法 bool/address/word/quantity、空结果均有断言。另以 URL 路径/query/userinfo 和错误响应中带假 key 的用例验证：HTTP 失败、RPC error、非法响应、传输失败时，Collector 返回错误及 Runner 日志均不泄露 URL、假 key 或原始错误文本。
2. 错误 chain ID、过期/未来区块、无 finalized、hash 格式错误、节点不支持 block-hash 调用均不入库；所有第二批请求含相同 hash 和 `requireCanonical=true`。
3. sAVAX 正确读出 18 位兑换率与绩效费；无限 cap 先识别，有限 cap 被奖励超出时剩余额度为 0；暂停和额度耗尽仍写完整状态，15 天与 2 天分别入列。申领窗口为 0 也入库，并将初筛记为 unknown；下一轮恢复正数后不残留旧原因。
4. Ankr ratio 倒数、零分母、超范围、最末位截断正确；`rate=NULL` 不是 0；现金不进入额度/TVL，费用和等待保持未知。
5. qiAVAX 8 位精度、`10^28` 兑换率、每秒 APR、TVL 一次最终截断均正确；供给利率为 0 合法；不使用 borrow rate、Stored 兑换率或额外奖励。
6. 所有金额全程整数/Decimal，范围边界及不足 18 位的极小兑换率正确处理；正的兑换率较上次下降不被采集器过滤。
7. hash 覆盖两批完整请求/响应，任一输入改变会改变 hash；三条来源分别失败互不影响，不补写伪造的当前值。
8. 两个新字段的 NULL/0/正数语义、INSERT 列/参数对应和查回精度；新表建表及旧结构幂等 ALTER 各运行两次成功，旧行仍为 NULL，旧 collector 仍可写入。
9. writer/Runner 重试复用路线 ID、观测时间和 hash；同批重复写后 `FINAL` 只出现一条逻辑记录；三条路线不能串成同一身份。
10. `AVAX_YIELD_ENABLED` 默认关闭；打开装配六条且每条一个 Runner，新增 RPC 配置不影响第一阶段。运行 `go test ./...`、`go test -race ./...`；ClickHouse 集成测试只使用临时测试库，不动生产历史。

联调再用默认公共 RPC 只读采三条，每条一行，同一行区块/字段可核对；写临时 ClickHouse 库，确认 hash、两个新列及 NULL 值。生产迁移及启动须另行部署，执行前遵循现有[运行说明](../../runtime-operations.md)，本轮不做。

本轮实读可用于数值 fixture 校对，不作为未来固定预期值：

| UTC 来源时间 / 区块 | 关键输入 | 应得到的结果 |
|---|---|---|
| 2026-08-26 16:57:19 / 93742828 | sAVAX `R=1281376817186495921` | `exposure_ratio=1.281376817186495921` |
| 同上 | qiAVAX `s=267399646`、`E=233222643389531741806027720` | `rate=0.008432715236256000` APR；`exposure_ratio=0.023322264338953174` |
| 2026-08-26 16:53:55 / 93742640 | Ankr `ratio=707043246512935099` | `exposure_ratio=1.414340643138729653` |

两个区块哈希分别为 `0x4c621d56b2a6749290b40b6e31b78cd7ecafe02bbcd723ff6c82ea6fa2063d48`、`0x1062e758cb290e7ef7abb2ed5a448cc81afdb632c53d57c8fa403c664fcb65fa`。不同采集轮之间可以不同块，不能把两者混在同一条观测。

## 9. 设计评审记录

2026-08-27，作者与独立审阅 Agent `avax_phase1_doc_review` 完成“首轮指摘 → 修订讨论 → 完整复审”，双方认可本版可以交给编程 Agent 实施，无剩余实质阻断项。

两项必要指摘已修正：

1. `redeemPeriod=0` 是合约允许的退出规则状态，不能当解析错误丢弃；现在照实保存，初筛记为 unknown，窗口恢复后按当轮规则恢复，字典与测试要求同步。
2. 仅不存 RPC URL 仍不足以防泄漏：现有 HTTP 错误会进入 Runner 日志；现在要求在新增客户端局部脱敏，并覆盖含假 key 的错误与日志测试，不扩展全项目凭据框架。

同时明确了共享 RPC 的共同故障边界、静态费用解释不等于自动审计升级。研究 Agent `avax_yield_research` 另行核对并认可 Ankr 的地址、倒数、现金及未知字段口径。作者复核了真实 RPC 数值、四项大整数换算示例及本地文档链接；文档差异检查通过。

两表、两个新增字段、现有 Runner/writer 的范围不变。该次仅完成设计与文档检查，未运行新 collector、未执行 DDL；后续实现验收另行记录，不与设计评审混淆。

## 10. 实现评审与验收记录

2026-08-27，编程 Agent `avax_phase1_impl` 完成实现，主 Agent 独立审查并运行测试，经过指摘、修订和复测后，双方认可当前程序，无剩余阻断项。`avax_phase1_doc_review` 另行审查了 RPC 客户端；`avax_yield_research` 补充了独立 Ankr、模型、配置、DDL 和六源装配测试，编程 Agent 与主 Agent 均已复核。

交付范围与设计一致：三个定类型 collector、一个共用只读 RPC client、原观测表的两列兼容迁移，以及现有开关下的六条 Runner 装配；未增加服务、交易功能或 SDK 依赖。

实现复审中修订了四处测试问题：

1. BENQI 有限额度不再仅断言剩余额度非负，还精确核对 `150-100=50`、额度超出/用尽时为 0。
2. 旧表迁移前先查询并断言两列确实不存在，避免旧结构 fixture 因 DDL 排版变化而失去验证意义。
3. 隔离库查询原先把普通 `time.Time` 作为位置参数，驱动默认按秒绑定，导致带毫秒的记录查不到。改用 `fromUnixTimestamp64Milli(?)` 和整数毫秒，并回读核对来源时间与采集时间；保留原 123ms 测试值，没有修改正常的二进制 batch writer。
4. 共享 gate 测试不再逐对比较 HTTP 服务端到达间隔，改用本地 transport、完整四次请求及总预约时长校验，消除调度造成的误报；生产限频器不变。修订后的 RPC 包连续 20 次竞态测试通过。

最终验证通过：

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
CLICKHOUSE_INTEGRATION=1 go test -count=1 ./internal/storage/clickhouse \
  -run '^TestClickHouseAVAXPhase2ColumnsMigrationAndRoundTrip$' -v
CLICKHOUSE_INTEGRATION=1 go test -count=1 ./internal/storage/clickhouse \
  -run 'TestClickHouse(AVAXYieldCollectorsHistoryAndRetry|YieldTablesRouteReuseAndBatchWrites|DDLWriteReplayAndDayMeasurement)$' -v
```

新迁移测试分别使用新表与缺少两列的真实旧结构：反复初始化成功，旧行/旧来源的新字段仍为 NULL；现金的 NULL、0、1 wei、Decimal 上界，以及窗口的 NULL、0、172800 均精确查回。登记后取消写入、重载 registry、重复写同批数据，路线 ID、时间、hash 不变，`FINAL` 保留预期的 4 条路线、6 条观测。旧 AVAX 来源、通用收益写入和盘口压缩/回放集成测试也通过。

主 Agent 另用默认公共 RPC 实际采集，再写入唯一命名的临时 ClickHouse 库：

| 路线 | UTC 来源时间 / finalized 区块 | 实际保存的关键值 |
|---|---|---|
| BENQI sAVAX | 2026-08-26 17:30:08 / 93744605 | `exposure_ratio=1.281380313446060415`，`rate=NULL` |
| Ankr ankrAVAX | 2026-08-26 17:30:09 / 93744606 | `exposure_ratio=1.414522133743464898`，`rate=NULL` |
| BENQI AVAX 借贷 | 2026-08-26 17:30:11 / 93744608 | `rate=0.008208024484464`（APR 比例），`exposure_ratio=0.02332227662441892` |

每条路线的同批数据重复写两遍，`FINAL` 仍为 3 条路线、3 条观测；兑换率、现金、窗口、利率空值、hash 和 finalized 锚点回读通过。以上只是该次接口/存储验收值，不是固定预期或未来收益预测。

本轮临时验收库已清理，临时实读程序未留在产品代码中。2026-08-27 已原子替换线上二进制并由既有守护循环重启；生产 `yield_observation` 已有两列，三条路线首次成功写入同一 finalized 区块 `93809132`，每行均有 64 位 payload hash。旧二进制保留为本机回退副本；持续抓取现已启用。
